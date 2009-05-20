%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_disk_queue).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([publish/3, publish_with_seq/4, deliver/1, phantom_deliver/1, ack/2,
	 tx_publish/2, tx_commit/3, tx_commit_with_seqs/3, tx_cancel/1,
	 requeue/2, requeue_with_seqs/2, purge/1]).

-export([stop/0, stop_and_obliterate/0, to_disk_only_mode/0, to_ram_disk_mode/0]).

-include("rabbit.hrl").

-define(WRITE_OK_SIZE_BITS,       8).
-define(WRITE_OK,                 255).
-define(INTEGER_SIZE_BYTES,       8).
-define(INTEGER_SIZE_BITS,        (8 * ?INTEGER_SIZE_BYTES)).
-define(MSG_LOC_NAME,             rabbit_disk_queue_msg_location).
-define(FILE_SUMMARY_ETS_NAME,    rabbit_disk_queue_file_summary).
-define(SEQUENCE_ETS_NAME,        rabbit_disk_queue_sequences).
-define(FILE_EXTENSION,           ".rdq").
-define(FILE_EXTENSION_TMP,       ".rdt").
-define(FILE_EXTENSION_DETS,      ".dets").
-define(FILE_PACKING_ADJUSTMENT,  (1 + (2* (?INTEGER_SIZE_BYTES)))).

-define(SERVER, ?MODULE).

-define(MAX_READ_FILE_HANDLES, 256).

-record(dqstate, {msg_location_dets,       %% where are messages?
		  msg_location_ets,        %% as above, but for ets version
                  operation_mode,          %% ram_disk | disk_only
		  file_summary,            %% what's in the files?
		  sequences,               %% next read and write for each q
		  current_file_num,        %% current file name as number
		  current_file_name,       %% current file name
		  current_file_handle,     %% current file handle
		  current_offset,          %% current offset within current file
		  file_size_limit,         %% how big can our files get?
		  read_file_handles,       %% file handles for reading (LRU)
		  read_file_handles_limit  %% how many file handles can we open?
		 }).

%% The components:
%%
%% MsgLocation: this is a dets table which contains:
%%              {MsgId, RefCount, File, Offset, TotalSize}
%% FileSummary: this is an ets table which contains:
%%              {File, ValidTotalSize, ContiguousTop, Left, Right}
%% Sequences:   this is an ets table which contains:
%%              {Q, ReadSeqId, WriteSeqId}
%% rabbit_disk_queue: this is an mnesia table which contains:
%%              #dq_msg_loc { queue_and_seq_id = {Q, SeqId},
%% 			      is_delivered = IsDelivered,
%% 			      msg_id = MsgId,
%%                            next_seq_id = SeqId
%% 			    }
%%

%% The basic idea is that messages are appended to the current file up
%% until that file becomes too big (> file_size_limit). At that point,
%% the file is closed and a new file is created on the _right_ of the
%% old file which is used for new messages. Files are named
%% numerically ascending, thus the file with the lowest name is the
%% eldest file.
%%
%% We need to keep track of which messages are in which files (this is
%% the MsgLocation table); how much useful data is in each file and
%% which files are on the left and right of each other. This is the
%% purpose of the FileSummary table.
%%
%% As messages are removed from files, holes appear in these
%% files. The field ValidTotalSize contains the total amount of useful
%% data left in the file, whilst ContiguousTop contains the amount of
%% valid data right at the start of each file. These are needed for
%% garbage collection.
%%
%% On publish, we write the message to disk, record the changes to
%% FileSummary and MsgLocation, and, should this be either a plain
%% publish, or followed by a tx_commit, we record the message in the
%% mnesia table. Sequences exists to enforce ordering of messages as
%% they are published within a queue.
%%
%% On delivery, we read the next message to be read from disk
%% (according to the ReadSeqId for the given queue) and record in the
%% mnesia table that the message has been delivered.
%%
%% On ack we remove the relevant entry from MsgLocation, update
%% FileSummary and delete from the mnesia table.
%%
%% In order to avoid extra mnesia searching, we return the SeqId
%% during delivery which must be returned in ack - it is not possible
%% to ack from MsgId alone.

%% As messages are ack'd, holes develop in the files. When we discover
%% that either a file is now empty or that it can be combined with the
%% useful data in either its left or right file, we compact the two
%% files together. This keeps disk utilisation high and aids
%% performance.
%%
%% Given the compaction between two files, the left file is considered
%% the ultimate destination for the good data in the right file. If
%% necessary, the good data in the left file which is fragmented
%% throughout the file is written out to a temporary file, then read
%% back in to form a contiguous chunk of good data at the start of the
%% left file. Thus the left file is garbage collected and
%% compacted. Then the good data from the right file is copied onto
%% the end of the left file. MsgLocation and FileSummary tables are
%% updated.
%%
%% On startup, we scan the files we discover, dealing with the
%% possibilites of a crash have occured during a compaction (this
%% consists of tidyup - the compaction is deliberately designed such
%% that data is duplicated on disk rather than risking it being lost),
%% and rebuild the dets and ets tables (MsgLocation, FileSummary,
%% Sequences) from what we find. We ensure that the messages we have
%% discovered on disk match exactly with the messages recorded in the
%% mnesia table.

%% MsgLocation is deliberately a dets table, and the mnesia table is
%% set to be a disk_only_table in order to ensure that we are not RAM
%% constrained. However, for performance reasons, it is possible to
%% call to_ram_disk_mode/0 which will alter the mnesia table to
%% disc_copies and convert MsgLocation to an ets table. This results
%% in a massive performance improvement, at the expense of greater RAM
%% usage. The idea is that when memory gets tight, we switch to
%% disk_only mode but otherwise try to run in ram_disk mode.

%% So, with this design, messages move to the left. Eventually, they
%% should end up in a contiguous block on the left and are then never
%% rewritten. But this isn't quite the case. If in a file there is one
%% message that is being ignored, for some reason, and messages in the
%% file to the right and in the current block are being read all the
%% time then it will repeatedly be the case that the good data from
%% both files can be combined and will be written out to a new
%% file. Whenever this happens, our shunned message will be rewritten.
%%
%% So, provided that we combine messages in the right order,
%% (i.e. left file, bottom to top, right file, bottom to top),
%% eventually our shunned message will end up at the bottom of the
%% left file. The compaction/combining algorithm is smart enough to
%% read in good data from the left file that is scattered throughout
%% (i.e. C and D in the below diagram), then truncate the file to just
%% above B (i.e. truncate to the limit of the good contiguous region
%% at the start of the file), then write C and D on top and then write
%% E, F and G from the right file on top. Thus contiguous blocks of
%% good data at the bottom of files are not rewritten (yes, this is
%% the data the size of which is tracked by the ContiguousTop
%% variable. Judicious use of a mirror is required).
%%
%% +-------+    +-------+         +-------+
%% |   X   |    |   G   |	  |   G   |
%% +-------+    +-------+	  +-------+
%% |   D   |    |   X   |	  |   F   |
%% +-------+    +-------+	  +-------+
%% |   X   |    |   X   |	  |   E   |
%% +-------+    +-------+	  +-------+
%% |   C   |    |   F   |   ===>  |   D   |
%% +-------+    +-------+	  +-------+
%% |   X   |    |   X   |	  |   C   |
%% +-------+    +-------+	  +-------+
%% |   B   |    |   X   |	  |   B   |
%% +-------+    +-------+	  +-------+
%% |   A   |    |   E   |	  |   A   |
%% +-------+    +-------+         +-------+
%%   left         right             left
%%
%% From this reasoning, we do have a bound on the number of times the
%% message is rewritten. From when it is inserted, there can be no
%% files inserted between it and the head of the queue, and the worst
%% case is that everytime it is rewritten, it moves one position lower
%% in the file (for it to stay at the same position requires that
%% there are no holes beneath it, which means truncate would be used
%% and so it would not be rewritten at all). Thus this seems to
%% suggest the limit is the number of messages ahead of it in the
%% queue, though it's likely that that's pessimistic, given the
%% requirements for compaction/combination of files.
%%
%% The other property is that we have is the bound on the lowest
%% utilisation, which should be 50% - worst case is that all files are
%% fractionally over half full and can't be combined (equivalent is
%% alternating full files and files with only one tiny message in
%% them).

%% ---- SPECS ----

-ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).

-spec(start_link/1 :: (non_neg_integer()) ->
	      {'ok', pid()} | 'ignore' | {'error', any()}).
-spec(publish/3 :: (queue_name(), msg_id(), binary()) -> 'ok').
-spec(publish_with_seq/4 :: (queue_name(), msg_id(), seq_id(), binary()) -> 'ok').
-spec(deliver/1 :: (queue_name()) ->
	     {'empty' | {msg_id(), binary(), non_neg_integer(),
			 bool(), {msg_id(), seq_id()}}}).
-spec(phantom_deliver/1 :: (queue_name()) ->
	     { 'empty' | {msg_id(), bool(), {msg_id(), seq_id()}}}).
-spec(ack/2 :: (queue_name(), [{msg_id(), seq_id()}]) -> 'ok').
-spec(tx_publish/2 :: (msg_id(), binary()) -> 'ok').
-spec(tx_commit/3 :: (queue_name(), [msg_id()], [seq_id()]) -> 'ok').
-spec(tx_commit_with_seqs/3 :: (queue_name(), [{msg_id(), seq_id()}], [seq_id()]) -> 'ok').
-spec(tx_cancel/1 :: ([msg_id()]) -> 'ok').
-spec(requeue/2 :: (queue_name(), [{msg_id(), seq_id()}]) -> 'ok').
-spec(requeue_with_seqs/2 :: (queue_name(), [{{msg_id(), seq_id()}, seq_id()}]) -> 'ok').
-spec(purge/1 :: (queue_name()) -> non_neg_integer()).
-spec(stop/0 :: () -> 'ok').
-spec(stop_and_obliterate/0 :: () -> 'ok').
-spec(to_ram_disk_mode/0 :: () -> 'ok').
-spec(to_disk_only_mode/0 :: () -> 'ok').

-endif.

%% ---- PUBLIC API ----

start_link(FileSizeLimit) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
			  [FileSizeLimit, ?MAX_READ_FILE_HANDLES], []).

publish(Q, MsgId, Msg) when is_binary(Msg) ->
    gen_server:cast(?SERVER, {publish, Q, MsgId, Msg}).

publish_with_seq(Q, MsgId, SeqId, Msg) when is_binary(Msg) ->
    gen_server:cast(?SERVER, {publish_with_seq, Q, MsgId, SeqId, Msg}).

deliver(Q) ->
    gen_server:call(?SERVER, {deliver, Q}, infinity).

phantom_deliver(Q) ->
    gen_server:call(?SERVER, {phantom_deliver, Q}).

ack(Q, MsgSeqIds) when is_list(MsgSeqIds) ->
    gen_server:cast(?SERVER, {ack, Q, MsgSeqIds}).

tx_publish(MsgId, Msg) when is_binary(Msg) ->
    gen_server:cast(?SERVER, {tx_publish, MsgId, Msg}).

tx_commit(Q, PubMsgIds, AckSeqIds) when is_list(PubMsgIds) andalso is_list(AckSeqIds) ->
    gen_server:call(?SERVER, {tx_commit, Q, PubMsgIds, AckSeqIds}, infinity).

tx_commit_with_seqs(Q, PubMsgSeqIds, AckSeqIds)
  when is_list(PubMsgSeqIds) andalso is_list(AckSeqIds) ->
    gen_server:call(?SERVER, {tx_commit_with_seqs, Q, PubMsgSeqIds, AckSeqIds}, infinity).

tx_cancel(MsgIds) when is_list(MsgIds) ->
    gen_server:cast(?SERVER, {tx_cancel, MsgIds}).

requeue(Q, MsgSeqIds) when is_list(MsgSeqIds) ->
    gen_server:cast(?SERVER, {requeue, Q, MsgSeqIds}).

requeue_with_seqs(Q, MsgSeqSeqIds) when is_list(MsgSeqSeqIds) ->
    gen_server:cast(?SERVER, {requeue_with_seqs, Q, MsgSeqSeqIds}).

purge(Q) ->
    gen_server:call(?SERVER, {purge, Q}).

stop() ->
    gen_server:call(?SERVER, stop, infinity).

stop_and_obliterate() ->
    gen_server:call(?SERVER, stop_vaporise, infinity).

to_disk_only_mode() ->
    gen_server:call(?SERVER, to_disk_only_mode, infinity).

to_ram_disk_mode() ->
    gen_server:call(?SERVER, to_ram_disk_mode, infinity).

%% ---- GEN-SERVER INTERNAL API ----

init([FileSizeLimit, ReadFileHandlesLimit]) ->
    %% If the gen_server is part of a supervision tree and is ordered
    %% by its supervisor to terminate, terminate will be called with
    %% Reason=shutdown if the following conditions apply:
    %%     * the gen_server has been set to trap exit signals, and
    %%     * the shutdown strategy as defined in the supervisor's
    %%       child specification is an integer timeout value, not
    %%       brutal_kill.
    %% Otherwise, the gen_server will be immediately terminated.
    process_flag(trap_exit, true),
    Node = node(),
    ok = 
	case mnesia:change_table_copy_type(rabbit_disk_queue, Node, disc_only_copies) of
	    {atomic, ok} -> ok;
	    {aborted, {already_exists, rabbit_disk_queue, Node, disc_only_copies}} -> ok;
	    E -> E
	end,
    ok = filelib:ensure_dir(form_filename("nothing")),
    InitName = "0" ++ ?FILE_EXTENSION,
    {ok, MsgLocationDets} =
	dets:open_file(?MSG_LOC_NAME,
		       [{file, form_filename(atom_to_list(?MSG_LOC_NAME) ++
					     ?FILE_EXTENSION_DETS)},
			{min_no_slots, 1024*1024},
			%% man says this should be <= 32M. But it works...
			{max_no_slots, 1024*1024*1024},
			{type, set}
		       ]),

    %% it would be better to have this as private, but dets:from_ets/2
    %% seems to blow up if it is set private
    MsgLocationEts = ets:new(?MSG_LOC_NAME, [set, protected]),
    State =
	#dqstate { msg_location_dets       = MsgLocationDets,
		   msg_location_ets        = MsgLocationEts,
		   operation_mode          = disk_only,
		   file_summary            = ets:new(?FILE_SUMMARY_ETS_NAME,
						     [set, private]),
		   sequences               = ets:new(?SEQUENCE_ETS_NAME,
						     [set, private]),
		   current_file_num        = 0,
		   current_file_name       = InitName,
		   current_file_handle     = undefined,
		   current_offset          = 0,
		   file_size_limit         = FileSizeLimit,
		   read_file_handles       = {dict:new(), gb_trees:empty()},
		   read_file_handles_limit = ReadFileHandlesLimit
		  },
    {ok, State1 = #dqstate { current_file_name = CurrentName,
			     current_offset = Offset } } =
	load_from_disk(State),
    Path = form_filename(CurrentName),
    Exists = case file:read_file_info(Path) of
		 {error,enoent} -> false;
		 {ok, _} -> true
	     end,
    %% read is only needed so that we can seek
    {ok, FileHdl} = file:open(Path, [read, write, raw, binary, delayed_write]),
    ok = if Exists -> ok;
	    true -> %% new file, so preallocate
		 {ok, FileSizeLimit} = file:position(FileHdl, {bof, FileSizeLimit}),
		 file:truncate(FileHdl)
	 end,
    {ok, Offset} = file:position(FileHdl, {bof, Offset}),
    {ok, State1 #dqstate { current_file_handle = FileHdl }}.

handle_call({deliver, Q}, _From, State) ->
    {ok, Result, State1} = internal_deliver(Q, true, State),
    {reply, Result, State1};
handle_call({phantom_deliver, Q}, _From, State) ->
    {ok, Result, State1} = internal_deliver(Q, false, State),
    {reply, Result, State1};
handle_call({tx_commit, Q, PubMsgIds, AckSeqIds}, _From, State) ->
    PubMsgSeqIds = lists:zip(PubMsgIds, lists:duplicate(length(PubMsgIds), next)),
    {ok, State1} = internal_tx_commit(Q, PubMsgSeqIds, AckSeqIds, State),
    {reply, ok, State1};
handle_call({tx_commit_with_seqs, Q, PubSeqMsgIds, AckSeqIds}, _From, State) ->
    {ok, State1} = internal_tx_commit(Q, PubSeqMsgIds, AckSeqIds, State),
    {reply, ok, State1};
handle_call({purge, Q}, _From, State) ->
    {ok, Count, State1} = internal_purge(Q, State),
    {reply, Count, State1};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}; %% gen_server now calls terminate
handle_call(stop_vaporise, _From, State) ->
    State1 = #dqstate { file_summary = FileSummary,
		        sequences = Sequences } =
	shutdown(State), %% tidy up file handles early
    {atomic, ok} = mnesia:clear_table(rabbit_disk_queue),
    true = ets:delete(FileSummary),
    true = ets:delete(Sequences),
    lists:foreach(fun file:delete/1, filelib:wildcard(form_filename("*"))),
    {stop, normal, ok,
     State1 #dqstate { current_file_handle = undefined,
		       read_file_handles = {dict:new(), gb_trees:empty()}}};
    %% gen_server now calls terminate, which then calls shutdown
handle_call(to_disk_only_mode, _From, State = #dqstate { operation_mode = disk_only }) ->
    {reply, ok, State};
handle_call(to_disk_only_mode, _From, State = #dqstate { operation_mode = ram_disk,
							 msg_location_dets = MsgLocationDets,
							 msg_location_ets = MsgLocationEts }) ->
    {atomic, ok} = mnesia:change_table_copy_type(rabbit_disk_queue, node(), disc_only_copies),
    ok = dets:from_ets(MsgLocationDets, MsgLocationEts),
    true = ets:delete_all_objects(MsgLocationEts),
    {reply, ok, State #dqstate { operation_mode = disk_only }};
handle_call(to_ram_disk_mode, _From, State = #dqstate { operation_mode = ram_disk }) ->
    {reply, ok, State};
handle_call(to_ram_disk_mode, _From, State = #dqstate { operation_mode = disk_only,
							msg_location_dets = MsgLocationDets,
							msg_location_ets = MsgLocationEts }) ->
    {atomic, ok} = mnesia:change_table_copy_type(rabbit_disk_queue, node(), disc_copies),
    true = ets:from_dets(MsgLocationEts, MsgLocationDets),
    ok = dets:delete_all_objects(MsgLocationDets),
    {reply, ok, State #dqstate { operation_mode = ram_disk }}.

handle_cast({publish, Q, MsgId, MsgBody}, State) ->
    {ok, State1} = internal_publish(Q, MsgId, next, MsgBody, State),
    {noreply, State1};
handle_cast({publish_with_seq, Q, MsgId, SeqId, MsgBody}, State) ->
    {ok, State1} = internal_publish(Q, MsgId, SeqId, MsgBody, State),
    {noreply, State1};
handle_cast({ack, Q, MsgSeqIds}, State) ->
    {ok, State1} = internal_ack(Q, MsgSeqIds, State),
    {noreply, State1};
handle_cast({tx_publish, MsgId, MsgBody}, State) ->
    {ok, State1} = internal_tx_publish(MsgId, MsgBody, State),
    {noreply, State1};
handle_cast({tx_cancel, MsgIds}, State) ->
    {ok, State1} = internal_tx_cancel(MsgIds, State),
    {noreply, State1};
handle_cast({requeue, Q, MsgSeqIds}, State) ->
    MsgSeqSeqIds = lists:zip(MsgSeqIds, lists:duplicate(length(MsgSeqIds), next)),
    {ok, State1} = internal_requeue(Q, MsgSeqSeqIds, State),
    {noreply, State1};
handle_cast({requeue_with_seqs, Q, MsgSeqSeqIds}, State) ->
    {ok, State1} = internal_requeue(Q, MsgSeqSeqIds, State),
    {noreply, State1}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    shutdown(State).

shutdown(State = #dqstate { msg_location_dets = MsgLocationDets,
			    msg_location_ets = MsgLocationEts,
			    current_file_handle = FileHdl,
			    read_file_handles = {ReadHdls, _ReadHdlsAge}
			  }) ->
    %% deliberately ignoring return codes here
    dets:close(MsgLocationDets),
    file:delete(form_filename(atom_to_list(?MSG_LOC_NAME) ++
			      ?FILE_EXTENSION_DETS)),
    true = ets:delete_all_objects(MsgLocationEts),
    if FileHdl =:= undefined -> ok;
       true -> file:sync(FileHdl),
	       file:close(FileHdl)
    end,
    dict:fold(fun (_File, Hdl, _Acc) ->
		     file:close(Hdl)
	      end, ok, ReadHdls),
    State #dqstate { current_file_handle = undefined,
		     read_file_handles = {dict:new(), gb_trees:empty()}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---- UTILITY FUNCTIONS ----

form_filename(Name) ->
    filename:join(base_directory(), Name).

base_directory() ->
    filename:join(mnesia:system_info(directory), "rabbit_disk_queue/").

dets_ets_lookup(#dqstate { msg_location_dets = MsgLocationDets, operation_mode = disk_only },
		Key) ->
    dets:lookup(MsgLocationDets, Key);
dets_ets_lookup(#dqstate { msg_location_ets = MsgLocationEts, operation_mode = ram_disk },
	        Key) ->
    ets:lookup(MsgLocationEts, Key).

dets_ets_delete(#dqstate { msg_location_dets = MsgLocationDets, operation_mode = disk_only },
		Key) ->
    ok = dets:delete(MsgLocationDets, Key);
dets_ets_delete(#dqstate { msg_location_ets = MsgLocationEts, operation_mode = ram_disk },
	        Key) ->
    true = ets:delete(MsgLocationEts, Key),
    ok.

dets_ets_insert(#dqstate { msg_location_dets = MsgLocationDets, operation_mode = disk_only },
		Obj) ->
    ok = dets:insert(MsgLocationDets, Obj);
dets_ets_insert(#dqstate { msg_location_ets = MsgLocationEts, operation_mode = ram_disk },
		Obj) ->
    true = ets:insert(MsgLocationEts, Obj),
    ok.

dets_ets_insert_new(#dqstate { msg_location_dets = MsgLocationDets, operation_mode = disk_only },
		    Obj) ->
    true = dets:insert_new(MsgLocationDets, Obj);
dets_ets_insert_new(#dqstate { msg_location_ets = MsgLocationEts, operation_mode = ram_disk },
		    Obj) ->
    true = ets:insert_new(MsgLocationEts, Obj).

dets_ets_match_object(#dqstate { msg_location_dets = MsgLocationDets, operation_mode = disk_only },
		      Obj) ->
    dets:match_object(MsgLocationDets, Obj);
dets_ets_match_object(#dqstate { msg_location_ets = MsgLocationEts, operation_mode = ram_disk },
		      Obj) ->
    ets:match_object(MsgLocationEts, Obj).

%% ---- INTERNAL RAW FUNCTIONS ----

internal_deliver(Q, ReadMsg, State = #dqstate { sequences = Sequences }) ->
    case ets:lookup(Sequences, Q) of
	[] -> {ok, empty, State};
	[{Q, ReadSeqId, WriteSeqId}] ->
	    case mnesia:dirty_read(rabbit_disk_queue, {Q, ReadSeqId}) of
		[] -> {ok, empty, State};
		[Obj =
		 #dq_msg_loc {is_delivered = Delivered, msg_id = MsgId,
			      next_seq_id = ReadSeqId2}] ->
		    [{MsgId, _RefCount, File, Offset, TotalSize}] =
			dets_ets_lookup(State, MsgId),
		    true = ets:insert(Sequences, {Q, ReadSeqId2, WriteSeqId}),
		    ok =
			if Delivered -> ok;
			   true ->
				mnesia:dirty_write(rabbit_disk_queue,
						   Obj #dq_msg_loc {is_delivered = true})
			end,
		    if ReadMsg ->
			    {FileHdl, State1} = get_read_handle(File, State),
			    {ok, {MsgBody, BodySize}} =
				read_message_at_offset(FileHdl, Offset, TotalSize),
			    {ok, {MsgId, MsgBody, BodySize, Delivered, {MsgId, ReadSeqId}},
			     State1};
		       true -> {ok, {MsgId, Delivered, {MsgId, ReadSeqId}}, State}
		    end
	    end
    end.

get_read_handle(File, State =
	      #dqstate { read_file_handles = {ReadHdls, ReadHdlsAge},
			 read_file_handles_limit = ReadFileHandlesLimit }) ->
    Now = now(),
    {FileHdl, ReadHdls1, ReadHdlsAge1} =
	case dict:find(File, ReadHdls) of
	    error ->
		{ok, Hdl} = file:open(form_filename(File),
				      [read, raw, binary,
				       read_ahead]),
		case dict:size(ReadHdls) < ReadFileHandlesLimit of
		    true ->
			{Hdl, ReadHdls, ReadHdlsAge};
		    _False ->
			{Then, OldFile, ReadHdlsAge2} =
			    gb_trees:take_smallest(ReadHdlsAge),
			{ok, {OldHdl, Then}} =
			    dict:find(OldFile, ReadHdls),
			ok = file:close(OldHdl),
			{Hdl, dict:erase(OldFile, ReadHdls), ReadHdlsAge2}
		end;
	    {ok, {Hdl, Then}} ->
		{Hdl, ReadHdls, gb_trees:delete(Then, ReadHdlsAge)}
	end,
    ReadHdls3 = dict:store(File, {FileHdl, Now}, ReadHdls1),
    ReadHdlsAge3 = gb_trees:enter(Now, File, ReadHdlsAge1),
    {FileHdl, State #dqstate {read_file_handles = {ReadHdls3, ReadHdlsAge3}}}.

internal_ack(Q, MsgSeqIds, State) ->
    remove_messages(Q, MsgSeqIds, true, State).

%% Q is only needed if MnesiaDelete /= false
%% called from tx_cancel with MnesiaDelete = false
%% called from internal_tx_cancel with MnesiaDelete = txn
%% called from ack with MnesiaDelete = true
%% called from purge with MnesiaDelete = txn
remove_messages(Q, MsgSeqIds, MnesiaDelete,
		State = #dqstate { file_summary = FileSummary,
				   current_file_name = CurName
				 }) ->
    Files =
	lists:foldl(
	  fun ({MsgId, SeqId}, Files2) ->
		  [{MsgId, RefCount, File, Offset, TotalSize}] =
		      dets_ets_lookup(State, MsgId),
		  Files3 =
		      if 1 =:= RefCount ->
			      ok = dets_ets_delete(State, MsgId),
			      [{File, ValidTotalSize, ContiguousTop, Left, Right}] =
				  ets:lookup(FileSummary, File),
			      ContiguousTop1 = lists:min([ContiguousTop, Offset]),
			      true = ets:insert(FileSummary,
						{File, (ValidTotalSize - TotalSize
							- ?FILE_PACKING_ADJUSTMENT),
						 ContiguousTop1, Left, Right}),
			      if CurName =:= File -> Files2;
				 true -> sets:add_element(File, Files2)
			      end;
			 1 < RefCount ->
			      ok = dets_ets_insert(State, {MsgId, RefCount - 1,
							   File, Offset, TotalSize}),
			      Files2
		      end,
		  ok = if MnesiaDelete ->
			       mnesia:dirty_delete(rabbit_disk_queue, {Q, SeqId});
			  MnesiaDelete =:= txn ->
			       mnesia:delete(rabbit_disk_queue, {Q, SeqId}, write);
			  true -> ok
		       end,
		  Files3
	  end, sets:new(), MsgSeqIds),
    State2 = compact(Files, State),
    {ok, State2}.

internal_tx_publish(MsgId, MsgBody,
		    State = #dqstate { current_file_handle = CurHdl,
				       current_file_name = CurName,
				       current_offset = CurOffset,
				       file_summary = FileSummary
				      }) ->
    case dets_ets_lookup(State, MsgId) of
	[] ->
	    %% New message, lots to do
	    {ok, TotalSize} = append_message(CurHdl, MsgId, MsgBody),
	    true = dets_ets_insert_new(State, {MsgId, 1, CurName,
					       CurOffset, TotalSize}),
	    [{CurName, ValidTotalSize, ContiguousTop, Left, undefined}] =
		ets:lookup(FileSummary, CurName),
	    ValidTotalSize1 = ValidTotalSize + TotalSize +
		?FILE_PACKING_ADJUSTMENT,
	    ContiguousTop1 = if CurOffset =:= ContiguousTop ->
				     %% can't be any holes in this file
				     ValidTotalSize1;
				true -> ContiguousTop
			     end,
	    true = ets:insert(FileSummary, {CurName, ValidTotalSize1,
					    ContiguousTop1, Left, undefined}),
	    NextOffset = CurOffset + TotalSize + ?FILE_PACKING_ADJUSTMENT,
	    maybe_roll_to_new_file(NextOffset,
				   State #dqstate {current_offset = NextOffset});
	[{MsgId, RefCount, File, Offset, TotalSize}] ->
	    %% We already know about it, just update counter
	    ok = dets_ets_insert(State, {MsgId, RefCount + 1, File,
					 Offset, TotalSize}),
	    {ok, State}
    end.

adjust_last_msg_seq_id(_Q, ExpectedSeqId, next) ->
    ExpectedSeqId;
adjust_last_msg_seq_id(_Q, 0, SuppliedSeqId) ->
    SuppliedSeqId;
adjust_last_msg_seq_id(_Q, ExpectedSeqId, ExpectedSeqId) ->
    ExpectedSeqId;
adjust_last_msg_seq_id(Q, ExpectedSeqId, SuppliedSeqId) when SuppliedSeqId > ExpectedSeqId ->
    [Obj] = mnesia:dirty_read(rabbit_disk_queue, {Q, ExpectedSeqId - 1}),
    ok = mnesia:dirty_write(rabbit_disk_queue,
			    Obj #dq_msg_loc { next_seq_id = SuppliedSeqId }),
    SuppliedSeqId.

%% can call this with PubMsgSeqIds as zip(PubMsgIds, duplicate(N, next))
internal_tx_commit(Q, PubMsgSeqIds, AckSeqIds,
		   State = #dqstate { current_file_handle = CurHdl,
				      current_file_name = CurName,
				      sequences = Sequences
				     }) ->
    {PubList, PubAcc, ReadSeqId} =
	case PubMsgSeqIds of
	    [] -> {[], undefined, undefined};
	    [_|PubMsgSeqIdsTail] ->
		{InitReadSeqId, InitWriteSeqId} =
		    case ets:lookup(Sequences, Q) of
			[] -> {0,0};
			[{Q, ReadSeqId2, WriteSeqId2}] -> {ReadSeqId2, WriteSeqId2}
		    end,
		{ lists:zip(PubMsgSeqIds, (PubMsgSeqIdsTail ++ [{next, next}])),
		  InitWriteSeqId, InitReadSeqId}
	end,
    {atomic, {Sync, WriteSeqId, State2}} =
	mnesia:transaction(
	  fun() -> ok = mnesia:write_lock_table(rabbit_disk_queue),
		   %% must deal with publishes first, if we didn't
		   %% then we could end up acking a message before
		   %% it's been published, which is clearly
		   %% nonsense. I.e. in commit, do not do things in an
		   %% order which _could_not_ have happened.
		   {Sync2, WriteSeqId3} =
		       lists:foldl(
			 fun ({{MsgId, SeqId}, {_NextMsgId, NextSeqId}},
			       {Acc, ExpectedSeqId}) ->
				 [{MsgId, _RefCount, File, _Offset, _TotalSize}] =
				     dets_ets_lookup(State, MsgId),
				 SeqId2 = adjust_last_msg_seq_id(Q, ExpectedSeqId, SeqId),
				 NextSeqId2 = if NextSeqId =:= next -> SeqId2 + 1;
						 true -> NextSeqId
					      end,
				 true = NextSeqId2 > SeqId2,
				 ok = mnesia:write(rabbit_disk_queue,
						   #dq_msg_loc { queue_and_seq_id =
								 {Q, SeqId2},
								 msg_id = MsgId,
								 is_delivered = false,
								 next_seq_id = NextSeqId2
								},
						   write),
				 {Acc or (CurName =:= File), NextSeqId2}
			 end, {false, PubAcc}, PubList),

		   {ok, State3} = remove_messages(Q, AckSeqIds, txn, State),
		   {Sync2, WriteSeqId3, State3}
	  end),
    true = if PubList =:= [] -> true;
	      true -> ets:insert(Sequences, {Q, ReadSeqId, WriteSeqId})
	   end,
    ok = if Sync -> file:sync(CurHdl);
	    true -> ok
	 end,
    {ok, State2}.

%% SeqId can be 'next'
internal_publish(Q, MsgId, SeqId, MsgBody, State) ->
    {ok, State1 = #dqstate { sequences = Sequences }} =
	internal_tx_publish(MsgId, MsgBody, State),
    {ReadSeqId, WriteSeqId} =
	case ets:lookup(Sequences, Q) of
	    [] -> %% previously unseen queue
		{0, 0};
	    [{Q, ReadSeqId2, WriteSeqId2}] ->
		{ReadSeqId2, WriteSeqId2}
	end,
    WriteSeqId3 = adjust_last_msg_seq_id(Q, WriteSeqId, SeqId),
    WriteSeqId3Next = WriteSeqId3 + 1,
    true = ets:insert(Sequences, {Q, ReadSeqId, WriteSeqId3Next}),
    ok = mnesia:dirty_write(rabbit_disk_queue,
			    #dq_msg_loc { queue_and_seq_id = {Q, WriteSeqId3},
					  msg_id = MsgId,
					  next_seq_id = WriteSeqId3Next,
					  is_delivered = false}),
    {ok, State1}.

internal_tx_cancel(MsgIds, State) ->
    %% we don't need seq ids because we're not touching mnesia,
    %% because seqids were never assigned
    MsgSeqIds = lists:zip(MsgIds, lists:duplicate(length(MsgIds), undefined)),
    remove_messages(undefined, MsgSeqIds, false, State).

internal_requeue(_Q, [], State) ->
    {ok, State};
internal_requeue(Q, MsgSeqIds = [_|MsgSeqIdsTail],
		 State = #dqstate { sequences = Sequences }) ->
    %% We know that every seq_id in here is less than the ReadSeqId
    %% you'll get if you look up this queue in Sequences (i.e. they've
    %% already been delivered). We also know that the rows for these
    %% messages are still in rabbit_disk_queue (i.e. they've not been
    %% ack'd).

    %% Now, it would be nice if we could adjust the sequence ids in
    %% rabbit_disk_queue (mnesia) to create a contiguous block and
    %% then drop the ReadSeqId for the queue by the corresponding
    %% amount. However, this is not safe because there may be other
    %% sequence ids which have been sent out as part of deliveries
    %% which are not being requeued. As such, moving things about in
    %% rabbit_disk_queue _under_ the current ReadSeqId would result in
    %% such sequence ids referring to the wrong messages.

    %% Therefore, the only solution is to take these messages, and to
    %% reenqueue them at the top of the queue. Usefully, this only
    %% affects the Sequences and rabbit_disk_queue structures - there
    %% is no need to physically move the messages about on disk, so
    %% MsgLocation and FileSummary stay put (which makes further sense
    %% as they have no concept of sequence id anyway).

    %% the Q _must_ already exist
    [{Q, ReadSeqId, WriteSeqId}] = ets:lookup(Sequences, Q),
    MsgSeqIdsZipped = lists:zip(MsgSeqIds, MsgSeqIdsTail ++ [{next, next}]),
    {atomic, WriteSeqId2} =
	mnesia:transaction(
	  fun() ->
		  ok = mnesia:write_lock_table(rabbit_disk_queue),
		  lists:foldl(
		    fun ({{{MsgId, SeqIdOrig}, SeqIdTo},
			  {_NextMsgSeqId, NextSeqIdTo}},
			 ExpectedSeqIdTo) ->
			    SeqIdTo2 = adjust_last_msg_seq_id(Q, ExpectedSeqIdTo, SeqIdTo),
			    NextSeqIdTo2 = if NextSeqIdTo =:= next -> SeqIdTo2 + 1;
					      true -> NextSeqIdTo
					   end,
			    true = NextSeqIdTo2 > SeqIdTo2,
			    [Obj = #dq_msg_loc { is_delivered = true, msg_id = MsgId }] =
				mnesia:read(rabbit_disk_queue, {Q, SeqIdOrig}, write),
			    mnesia:write(rabbit_disk_queue,
					 Obj #dq_msg_loc { queue_and_seq_id = {Q, SeqIdTo2},
							   next_seq_id = NextSeqIdTo2
							 },
					 write),
			    mnesia:delete(rabbit_disk_queue, {Q, SeqIdOrig}, write),
			    NextSeqIdTo2
		    end, WriteSeqId, MsgSeqIdsZipped)
	  end),
    true = ets:insert(Sequences, {Q, ReadSeqId, WriteSeqId2}),
    {ok, State}.

internal_purge(Q, State = #dqstate { sequences = Sequences }) ->
    case ets:lookup(Sequences, Q) of
	[] -> {ok, 0, State};
	[{Q, ReadSeqId, WriteSeqId}] ->
	    {atomic, {ok, State2}} =
		mnesia:transaction(
		  fun() ->
			  ok = mnesia:write_lock_table(rabbit_disk_queue),
			  MsgSeqIds = lists:foldl(
			    fun (SeqId, Acc) ->
				    [#dq_msg_loc { is_delivered = false, msg_id = MsgId }] =
					mnesia:read(rabbit_disk_queue, {Q, SeqId}, write),
				    [{MsgId, SeqId} | Acc]
			    end, [], lists:seq(ReadSeqId, WriteSeqId - 1)),
			  remove_messages(Q, MsgSeqIds, txn, State)
		  end),
	    true = ets:insert(Sequences, {Q, WriteSeqId, WriteSeqId}),
	    {ok, WriteSeqId - ReadSeqId, State2}
    end.

%% ---- ROLLING OVER THE APPEND FILE ----

maybe_roll_to_new_file(Offset,
		       State = #dqstate { file_size_limit = FileSizeLimit,
					  current_file_name = CurName,
					  current_file_handle = CurHdl,
					  current_file_num = CurNum,
					  file_summary = FileSummary
					}
		      ) when Offset >= FileSizeLimit ->
    ok = file:sync(CurHdl),
    ok = file:close(CurHdl),
    NextNum = CurNum + 1,
    NextName = integer_to_list(NextNum) ++ ?FILE_EXTENSION,
    {ok, NextHdl} = file:open(form_filename(NextName),
			      [write, raw, binary, delayed_write]),
    {ok, FileSizeLimit} = file:position(NextHdl, {bof, FileSizeLimit}),
    ok = file:truncate(NextHdl),
    {ok, 0} = file:position(NextHdl, {bof, 0}),
    true = ets:update_element(FileSummary, CurName, {5, NextName}), %% 5 is Right
    true = ets:insert_new(FileSummary, {NextName, 0, 0, CurName, undefined}),
    State1 = State #dqstate { current_file_name = NextName,
			      current_file_handle = NextHdl,
			      current_file_num = NextNum,
			      current_offset = 0
			     },
    {ok, compact(sets:from_list([CurName]), State1)};
maybe_roll_to_new_file(_, State) ->
    {ok, State}.

%% ---- GARBAGE COLLECTION / COMPACTION / AGGREGATION ----

compact(FilesSet, State) ->
    %% smallest number, hence eldest, hence left-most, first
    Files = lists:sort(sets:to_list(FilesSet)),
    %% foldl reverses, so now youngest/right-most first
    RemainingFiles = lists:foldl(fun (File, Acc) ->
					 delete_empty_files(File, Acc, State)
				 end, [], Files),
    lists:foldl(fun combine_file/2, State, lists:reverse(RemainingFiles)).

combine_file(File, State = #dqstate { file_size_limit = FileSizeLimit,
				     file_summary = FileSummary,
				     current_file_name = CurName
				   }) ->
    %% the file we're looking at may no longer exist as it may have
    %% been deleted within the current GC run
    case ets:lookup(FileSummary, File) of
	[] -> State;
	[FileObj = {File, ValidData, _ContiguousTop, Left, Right}] ->
	    GoRight =
		fun() ->
			case Right of
			    undefined -> State;
			    _ when not(CurName =:= Right) ->
				[RightObj = {Right, RightValidData, 
					     _RightContiguousTop, File, RightRight}] =
				    ets:lookup(FileSummary, Right),
				RightSumData = ValidData + RightValidData,
				if FileSizeLimit >= RightSumData ->
					%% here, Right will be the source and so will be deleted,
					%%       File will be the destination
					State1 = combine_files(RightObj, FileObj,
							      State),
					%% this could fail if RightRight is undefined
					%% left is the 4th field
					ets:update_element(FileSummary,
							   RightRight, {4, File}),
					true = ets:insert(FileSummary, {File,
									RightSumData,
									RightSumData,
									Left,
									RightRight}),
					true = ets:delete(FileSummary, Right),
					State1;
				   true -> State
				end;
			    _ -> State
			end
		end,
	    case Left of
		undefined ->
		    GoRight();
		_ -> [LeftObj =
		      {Left, LeftValidData, _LeftContiguousTop, LeftLeft, File}] =
			 ets:lookup(FileSummary, Left),
		     LeftSumData = ValidData + LeftValidData,
		     if FileSizeLimit >= LeftSumData ->
			     %% here, File will be the source and so will be deleted,
			     %%       Left will be the destination
			     State1 = combine_files(FileObj, LeftObj, State),
			     %% this could fail if Right is undefined
			     %% left is the 4th field
			     ets:update_element(FileSummary, Right, {4, Left}),
			     true = ets:insert(FileSummary, {Left, LeftSumData,
							     LeftSumData,
							     LeftLeft, Right}),
			     true = ets:delete(FileSummary, File),
			     State1;
			true ->
			     GoRight()
		     end
	    end
    end.

sort_msg_locations_by_offset(Asc, List) ->
    Comp = if Asc  -> fun erlang:'<'/2;
	      true -> fun erlang:'>'/2
	   end,
    lists:sort(fun ({_, _, _, OffA, _}, {_, _, _, OffB, _}) ->
		       Comp(OffA, OffB)
	       end, List).

truncate_and_extend_file(FileHdl, Lowpoint, Highpoint) ->
    {ok, Lowpoint} = file:position(FileHdl, {bof, Lowpoint}),
    ok = file:truncate(FileHdl),
    {ok, Highpoint} = file:position(FileHdl, {bof, Highpoint}),
    ok = file:truncate(FileHdl),
    {ok, Lowpoint} = file:position(FileHdl, {bof, Lowpoint}),
    ok.

combine_files({Source, SourceValid, _SourceContiguousTop,
	      _SourceLeft, _SourceRight},
	     {Destination, DestinationValid, DestinationContiguousTop,
	      _DestinationLeft, _DestinationRight},
	     State1) ->
    State = close_file(Source, close_file(Destination, State1)),
    {ok, SourceHdl} =
	file:open(form_filename(Source),
		  [read, write, raw, binary, delayed_write, read_ahead]),
    {ok, DestinationHdl} =
	file:open(form_filename(Destination),
		  [read, write, raw, binary, delayed_write, read_ahead]),
    ExpectedSize = SourceValid + DestinationValid,
    %% if DestinationValid =:= DestinationContiguousTop then we don't need a tmp file
    %% if they're not equal, then we need to write out everything past the DestinationContiguousTop to a tmp file
    %%   then truncate, copy back in, and then copy over from Source
    %% otherwise we just truncate straight away and copy over from Source
    if DestinationContiguousTop =:= DestinationValid ->
	    ok = truncate_and_extend_file(DestinationHdl,
				       DestinationValid, ExpectedSize);
       true ->
	    Tmp = filename:rootname(Destination) ++ ?FILE_EXTENSION_TMP,
	    {ok, TmpHdl} =
		file:open(form_filename(Tmp),
			  [read, write, raw, binary, delayed_write, read_ahead]),
	    Worklist =
		lists:dropwhile(
		  fun ({_, _, _, Offset, _})
		      when Offset /= DestinationContiguousTop ->
			  %% it cannot be that Offset == DestinationContiguousTop
			  %% because if it was then DestinationContiguousTop would have been
			  %% extended by TotalSize
			  Offset < DestinationContiguousTop
			  %% Given expected access patterns, I suspect that the list should be
			  %% naturally sorted as we require, however, we need to enforce it anyway
		  end, sort_msg_locations_by_offset(true,
						dets_ets_match_object(State,
								      {'_', '_',
								       Destination,
								       '_', '_'}))),
	    TmpSize = DestinationValid - DestinationContiguousTop,
	    {TmpSize, BlockStart1, BlockEnd1} =
		lists:foldl(
		  fun ({MsgId, RefCount, _Destination, Offset, TotalSize},
		       {CurOffset, BlockStart, BlockEnd}) ->
			  %% CurOffset is in the TmpFile.
			  %% Offset, BlockStart and BlockEnd are in the DestinationFile (which is currently the source!)
			  Size = TotalSize + ?FILE_PACKING_ADJUSTMENT,
			  %% this message is going to end up back in
			  %% Destination, at DestinationContiguousTop
			  %% + CurOffset
			  FinalOffset = DestinationContiguousTop + CurOffset,
			  ok = dets_ets_insert(State, {MsgId, RefCount, Destination,
						       FinalOffset, TotalSize}),
			  NextOffset = CurOffset + Size,
			  if BlockStart =:= undefined ->
				  %% base case, called only for the
				  %% first list elem
				  {NextOffset, Offset, Offset + Size};
			     Offset =:= BlockEnd ->
				  %% extend the current block because
				  %% the next msg follows straight on
				  {NextOffset, BlockStart, BlockEnd + Size};
			     true ->
				  %% found a gap, so actually do the
				  %% work for the previous block
				  BSize = BlockEnd - BlockStart,
				  {ok, BlockStart} =
				      file:position(DestinationHdl,
						    {bof, BlockStart}),
				  {ok, BSize} = file:copy(DestinationHdl,
							  TmpHdl, BSize),
				  {NextOffset, Offset, Offset + Size}
			  end
		  end, {0, undefined, undefined}, Worklist),
	    %% do the last remaining block
	    BSize1 = BlockEnd1 - BlockStart1,
	    {ok, BlockStart1} = file:position(DestinationHdl, {bof, BlockStart1}),
	    {ok, BSize1} = file:copy(DestinationHdl, TmpHdl, BSize1),
	    %% so now Tmp contains everything we need to salvage from
	    %% Destination, and MsgLocationDets has been updated to
	    %% reflect compaction of Destination so truncate
	    %% Destination and copy from Tmp back to the end
	    {ok, 0} = file:position(TmpHdl, {bof, 0}),
	    ok = truncate_and_extend_file(DestinationHdl,
				       DestinationContiguousTop, ExpectedSize),
	    {ok, TmpSize} = file:copy(TmpHdl, DestinationHdl, TmpSize),
	    %% position in DestinationHdl should now be
	    %% DestinationValid
	    ok = file:sync(DestinationHdl),
	    ok = file:close(TmpHdl),
	    ok = file:delete(form_filename(Tmp))
    end,
    SourceWorkList =
	sort_msg_locations_by_offset(true,
				 dets_ets_match_object(State,
						       {'_', '_', Source,
							'_', '_'})),
    {ExpectedSize, BlockStart2, BlockEnd2} =
	lists:foldl(
	  fun ({MsgId, RefCount, _Source, Offset, TotalSize},
	       {CurOffset, BlockStart, BlockEnd}) ->
		  %% CurOffset is in the DestinationFile.
		  %% Offset, BlockStart and BlockEnd are in the SourceFile
		  Size = TotalSize + ?FILE_PACKING_ADJUSTMENT,
		  %% update MsgLocationDets to reflect change of file and offset
		  ok = dets_ets_insert(State, {MsgId, RefCount, Destination,
					       CurOffset, TotalSize}),
		  NextOffset = CurOffset + Size,
		  if BlockStart =:= undefined ->
			  %% base case, called only for the first list
			  %% elem
			  {NextOffset, Offset, Offset + Size};
		     Offset =:= BlockEnd ->
			  %% extend the current block because the next
			  %% msg follows straight on
			  {NextOffset, BlockStart, BlockEnd + Size};
		     true ->
			  %% found a gap, so actually do the work for
			  %% the previous block
			  BSize = BlockEnd - BlockStart,
			  {ok, BlockStart} =
				file:position(SourceHdl, {bof, BlockStart}),
			  {ok, BSize} =
			      file:copy(SourceHdl, DestinationHdl, BSize),
			  {NextOffset, Offset, Offset + Size}
		  end
	  end, {DestinationValid, undefined, undefined}, SourceWorkList),
    %% do the last remaining block
    BSize2 = BlockEnd2 - BlockStart2,
    {ok, BlockStart2} = file:position(SourceHdl, {bof, BlockStart2}),
    {ok, BSize2} = file:copy(SourceHdl, DestinationHdl, BSize2),
    %% tidy up
    ok = file:sync(DestinationHdl),
    ok = file:close(SourceHdl),
    ok = file:close(DestinationHdl),
    ok = file:delete(form_filename(Source)),
    State.

close_file(File, State = #dqstate { read_file_handles =
				   {ReadHdls, ReadHdlsAge} }) ->
    case dict:find(File, ReadHdls) of
	error ->
	    State;
	{ok, {Hdl, Then}} ->
	    ok = file:close(Hdl),
	    State #dqstate { read_file_handles =
			     { dict:erase(File, ReadHdls),
			       gb_trees:delete(Then, ReadHdlsAge) } }
    end.

delete_empty_files(File, Acc, #dqstate { file_summary = FileSummary }) ->
    [{File, ValidData, _ContiguousTop, Left, Right}] =
	ets:lookup(FileSummary, File),
    case ValidData of
	%% we should NEVER find the current file in here hence right
        %% should always be a file, not undefined
	0 -> case {Left, Right} of
		 {undefined, _} when not(is_atom(Right)) ->
		     %% the eldest file is empty. YAY!
		     %% left is the 4th field
		     true = ets:update_element(FileSummary, Right, {4, undefined});
		 {_, _} when not(is_atom(Right)) ->
		     %% left is the 4th field
		     true = ets:update_element(FileSummary, Right, {4, Left}),
		     %% right is the 5th field
		     true = ets:update_element(FileSummary, Left, {5, Right})
	     end,
	     true = ets:delete(FileSummary, File),
	     ok = file:delete(form_filename(File)),
	     Acc;
	_ -> [File|Acc]
    end.

%% ---- DISK RECOVERY ----

load_from_disk(State) ->
    %% sorted so that smallest number is first. which also means
    %% eldest file (left-most) first
    ok = case mnesia:add_table_index(rabbit_disk_queue, msg_id) of
	     {atomic, ok} -> ok;
	     {aborted,{already_exists,rabbit_disk_queue,_}} -> ok;
	     E -> E
	 end,
    {Files, TmpFiles} = get_disk_queue_files(),
    ok = recover_crashed_compactions(Files, TmpFiles),
    %% There should be no more tmp files now, so go ahead and load the
    %% whole lot
    State1 = load_messages(undefined, Files, State),
    %% Finally, check there is nothing in mnesia which we haven't
    %% loaded
    {atomic, true} = mnesia:transaction(
	     fun() ->
		     ok = mnesia:read_lock_table(rabbit_disk_queue),
		     mnesia:foldl(fun (#dq_msg_loc { msg_id = MsgId }, true) ->
					  true = 1 =:=
					      length(dets_ets_lookup(State1, MsgId))
				  end,
				  true, rabbit_disk_queue)
	     end),
    State2 = extract_sequence_numbers(State1),
    ok = case mnesia:del_table_index(rabbit_disk_queue, msg_id) of
	     {atomic, ok} -> ok;
	     %% hmm, something weird must be going on, but it's
	     %% probably not the end of the world
	     {aborted,{no_exists,rabbit_disk_queue,_}} -> ok;
	     E2 -> E2
	 end,
    {ok, State2}.

extract_sequence_numbers(State = #dqstate { sequences = Sequences }) ->
    {atomic, true} = mnesia:transaction(
      fun() ->
	      ok = mnesia:read_lock_table(rabbit_disk_queue),
	      mnesia:foldl(
		fun (#dq_msg_loc { queue_and_seq_id = {Q, SeqId} }, true) ->
			NextWrite = SeqId + 1,
			case ets:lookup(Sequences, Q) of
			    [] ->
				true = ets:insert_new(Sequences,
						      {Q, SeqId, NextWrite});
			    [Orig = {Q, Read, Write}] ->
				Repl = {Q, lists:min([Read, SeqId]),
					lists:max([Write, NextWrite])},
				if Orig /= Repl ->
					true = ets:insert(Sequences, Repl);
				   true -> true
				end
			end
		end, true, rabbit_disk_queue)
      end),
    remove_gaps_in_sequences(State),
    State.

remove_gaps_in_sequences(#dqstate { sequences = Sequences }) ->
    %% read the comments at internal_requeue.

    %% Because we are at startup, we know that no sequence ids have
    %% been issued (or at least, they were, but have been
    %% forgotten). Therefore, we can nicely shuffle up and not
    %% worry. Note that I'm choosing to shuffle up, but alternatively
    %% we could shuffle downwards. However, I think there's greater
    %% likelihood of gaps being at the bottom rather than the top of
    %% the queue, so shuffling up should be the better bet.
    {atomic, _} =
	mnesia:transaction(
	  fun() ->
		  ok = mnesia:write_lock_table(rabbit_disk_queue),
		  lists:foreach(
		    fun ({Q, ReadSeqId, WriteSeqId}) ->
			    Gap = shuffle_up(Q, ReadSeqId - 1, WriteSeqId - 1, 0),
			    true = ets:insert(Sequences, {Q, ReadSeqId + Gap, WriteSeqId})
		    end, ets:match_object(Sequences, '_'))
	  end).

shuffle_up(_Q, SeqId, SeqId, Gap) ->
    Gap;
shuffle_up(Q, BaseSeqId, SeqId, Gap) ->
    GapInc =
	case mnesia:read(rabbit_disk_queue, {Q, SeqId}, write) of
	    [] -> 1;
	    [Obj] ->
		if Gap =:= 0 -> ok;
		   true -> mnesia:write(rabbit_disk_queue,
					Obj #dq_msg_loc { queue_and_seq_id = {Q, SeqId + Gap },
							  next_seq_id = SeqId + Gap + 1
							},
					write),
			   mnesia:delete(rabbit_disk_queue, {Q, SeqId}, write)
		end,
		0
	end,
    shuffle_up(Q, BaseSeqId, SeqId - 1, Gap + GapInc).

load_messages(undefined, [], State = #dqstate { file_summary = FileSummary,
						current_file_name = CurName }) ->
    true = ets:insert_new(FileSummary, {CurName, 0, 0, undefined, undefined}),
    State;
load_messages(Left, [], State) ->
    Num = list_to_integer(filename:rootname(Left)),
    Offset = case dets_ets_match_object(State, {'_', '_', Left, '_', '_'}) of
		 [] -> 0;
		 L -> [{_MsgId, _RefCount, Left, MaxOffset, TotalSize}|_] =
			  sort_msg_locations_by_offset(false, L),
		      MaxOffset + TotalSize + ?FILE_PACKING_ADJUSTMENT
	     end,
    State #dqstate { current_file_num = Num, current_file_name = Left,
		     current_offset = Offset };
load_messages(Left, [File|Files],
	      State = #dqstate { file_summary = FileSummary }) ->
    %% [{MsgId, TotalSize, FileOffset}]
    {ok, Messages} = scan_file_for_valid_messages(form_filename(File)),
    {ValidMessagesRev, ValidTotalSize} = lists:foldl(
	fun ({MsgId, TotalSize, Offset}, {VMAcc, VTSAcc}) ->
		case length(mnesia:dirty_index_match_object
			    (rabbit_disk_queue,
			     #dq_msg_loc { msg_id = MsgId,
					   queue_and_seq_id = '_',
					   is_delivered = '_',
					   next_seq_id = '_'
					 },
			     msg_id)) of
		    0 -> {VMAcc, VTSAcc};
		    RefCount ->
			true = dets_ets_insert_new(State, {MsgId, RefCount, File,
							   Offset, TotalSize}),
			{[{MsgId, TotalSize, Offset}|VMAcc],
			 VTSAcc + TotalSize + ?FILE_PACKING_ADJUSTMENT
			}
		end
	end, {[], 0}, Messages),
    %% foldl reverses lists and find_contiguous_block_prefix needs
    %% elems in the same order as from scan_file_for_valid_messages
    {ContiguousTop, _} = find_contiguous_block_prefix(lists:reverse(ValidMessagesRev)),
    Right = case Files of
		[] -> undefined;
		[F|_] -> F
	    end,
    true = ets:insert_new(FileSummary, {File, ValidTotalSize, ContiguousTop, Left, Right}),
    load_messages(File, Files, State).

%% ---- DISK RECOVERY OF FAILED COMPACTION ----

recover_crashed_compactions(Files, TmpFiles) ->
    lists:foreach(fun (TmpFile) -> ok = recover_crashed_compactions1(Files, TmpFile) end,
		  TmpFiles),
    ok.

recover_crashed_compactions1(Files, TmpFile) ->
    GrabMsgId = fun ({MsgId, _TotalSize, _FileOffset}) -> MsgId end,
    NonTmpRelatedFile = filename:rootname(TmpFile) ++ ?FILE_EXTENSION,
    true = lists:member(NonTmpRelatedFile, Files),
    %% [{MsgId, TotalSize, FileOffset}]
    {ok, UncorruptedMessagesTmp} =
	scan_file_for_valid_messages(form_filename(TmpFile)),
    MsgIdsTmp = lists:map(GrabMsgId, UncorruptedMessagesTmp),
    %% all of these messages should appear in the mnesia table,
    %% otherwise they wouldn't have been copied out
    lists:foreach(fun (MsgId) ->
			  true = 0 < length(mnesia:dirty_index_match_object
					    (rabbit_disk_queue,
					     #dq_msg_loc { msg_id = MsgId,
							   queue_and_seq_id = '_',
							   is_delivered = '_',
							   next_seq_id = '_'
							 },
					     msg_id))
		  end, MsgIdsTmp),
    {ok, UncorruptedMessages} =
	scan_file_for_valid_messages(form_filename(NonTmpRelatedFile)),
    MsgIds = lists:map(GrabMsgId, UncorruptedMessages),
    %% 1) It's possible that everything in the tmp file is also in the main file
    %%    such that the main file is (prefix ++ tmpfile). This means that compaction
    %%    failed immediately prior to the final step of deleting the tmp file.
    %%    Plan: just delete the tmp file
    %% 2) It's possible that everything in the tmp file is also in the main file
    %%    but with holes throughout (or just somthing like main = (prefix ++ hole ++ tmpfile)).
    %%    This means that compaction wrote out the tmp file successfully and then failed.
    %%    Plan: just delete the tmp file and allow the compaction to eventually be triggered later
    %% 3) It's possible that everything in the tmp file is also in the main file
    %%    but such that the main file does not end with tmp file (and there are valid messages
    %%    in the suffix; main = (prefix ++ tmpfile[with extra holes?] ++ suffix)).
    %%    This means that compaction failed as we were writing out the tmp file.
    %%    Plan: just delete the tmp file and allow the compaction to eventually be triggered later
    %% 4) It's possible that there are messages in the tmp file which are not in the main file.
    %%    This means that writing out the tmp file succeeded, but then we failed as we
    %%    were copying them back over to the main file, after truncating the main file.
    %%    As the main file has already been truncated, it should consist only of valid messages
    %%    Plan: Truncate the main file back to before any of the files in the tmp file and copy
    %%    them over again
    case lists:all(fun (MsgId) -> lists:member(MsgId, MsgIds) end, MsgIdsTmp) of
	true -> %% we're in case 1, 2 or 3 above. Just delete the tmp file
	        %% note this also catches the case when the tmp file
	        %% is empty
	    ok = file:delete(TmpFile);
	_False ->
	    %% we're in case 4 above.
	    %% check that everything in the main file is a valid message in mnesia
	    lists:foreach(fun (MsgId) ->
				  true = 0 < length(mnesia:dirty_index_match_object
						    (rabbit_disk_queue,
						     #dq_msg_loc { msg_id = MsgId,
								   queue_and_seq_id = '_',
								   is_delivered = '_',
								   next_seq_id = '_'
								 },
						     msg_id))
			  end, MsgIds),
	    %% The main file should be contiguous
	    {Top, MsgIds} = find_contiguous_block_prefix(UncorruptedMessages),
	    %% we should have that none of the messages in the prefix
	    %% are in the tmp file
	    true = lists:all(fun (MsgId) -> not(lists:member(MsgId, MsgIdsTmp)) end,
			     MsgIds),

	    {ok, MainHdl} = file:open(form_filename(NonTmpRelatedFile),
				      [write, raw, binary, delayed_write]),
	    {ok, Top} = file:position(MainHdl, Top),
	    ok = file:truncate(MainHdl), %% wipe out any rubbish at the end of the file
	    %% there really could be rubbish at the end of the file -
	    %% we could have failed after the extending truncate.
	    %% Remember the head of the list will be the highest entry
	    %% in the file
	    [{_, TmpTopTotalSize, TmpTopOffset}|_] = UncorruptedMessagesTmp,
	    TmpSize = TmpTopOffset + TmpTopTotalSize + ?FILE_PACKING_ADJUSTMENT,
	    ExpectedAbsPos = Top + TmpSize,
	    {ok, ExpectedAbsPos} = file:position(MainHdl, {cur, TmpSize}),
	    %% and now extend the main file as big as necessary in a
	    %% single move if we run out of disk space, this truncate
	    %% could fail, but we still aren't risking losing data
	    ok = file:truncate(MainHdl),
	    {ok, TmpHdl} = file:open(form_filename(TmpFile),
				     [read, raw, binary, read_ahead]),
	    {ok, TmpSize} = file:copy(TmpHdl, MainHdl, TmpSize),
	    ok = file:close(MainHdl),
	    ok = file:close(TmpHdl),
	    ok = file:delete(TmpFile),

	    {ok, MainMessages} =
		scan_file_for_valid_messages(form_filename(NonTmpRelatedFile)),
	    MsgIdsMain = lists:map(GrabMsgId, MainMessages),
	    %% check that everything in MsgIds is in MsgIdsMain
	    true = lists:all(fun (MsgId) -> lists:member(MsgId, MsgIdsMain) end,
			     MsgIds),
	    %% check that everything in MsgIdsTmp is in MsgIdsMain
	    true = lists:all(fun (MsgId) -> lists:member(MsgId, MsgIdsMain) end,
			     MsgIdsTmp)
    end,
    ok.

%% this assumes that the messages are ordered such that the highest
%% address is at the head of the list. This matches what
%% scan_file_for_valid_messages produces
find_contiguous_block_prefix([]) -> {0, []};
find_contiguous_block_prefix([{MsgId, TotalSize, Offset}|Tail]) ->
    case find_contiguous_block_prefix(Tail, Offset, [MsgId]) of
	{ok, Acc} -> {Offset + TotalSize + ?FILE_PACKING_ADJUSTMENT,
		      lists:reverse(Acc)};
	Res -> Res
    end.
find_contiguous_block_prefix([], 0, Acc) ->
    {ok, Acc};
find_contiguous_block_prefix([], _N, _Acc) ->
    {0, []};
find_contiguous_block_prefix([{MsgId, TotalSize, Offset}|Tail],
			     ExpectedOffset, Acc)
  when ExpectedOffset =:= Offset + TotalSize + ?FILE_PACKING_ADJUSTMENT ->
    find_contiguous_block_prefix(Tail, Offset, [MsgId|Acc]);
find_contiguous_block_prefix(List, _ExpectedOffset, _Acc) ->
    find_contiguous_block_prefix(List).
    
file_name_sort(A, B) ->
    ANum = list_to_integer(filename:rootname(A)),
    BNum = list_to_integer(filename:rootname(B)),
    ANum < BNum.

get_disk_queue_files() ->
    DQFiles = filelib:wildcard("*" ++ ?FILE_EXTENSION, base_directory()),
    DQFilesSorted = lists:sort(fun file_name_sort/2, DQFiles),
    DQTFiles = filelib:wildcard("*" ++ ?FILE_EXTENSION_TMP, base_directory()),
    DQTFilesSorted = lists:sort(fun file_name_sort/2, DQTFiles),
    {DQFilesSorted, DQTFilesSorted}.

%% ---- RAW READING AND WRITING OF FILES ----

append_message(FileHdl, MsgId, MsgBody) when is_binary(MsgBody) ->
    BodySize = size(MsgBody),
    MsgIdBin = term_to_binary(MsgId),
    MsgIdBinSize = size(MsgIdBin),
    TotalSize = BodySize + MsgIdBinSize,
    case file:write(FileHdl, <<TotalSize:?INTEGER_SIZE_BITS,
			       MsgIdBinSize:?INTEGER_SIZE_BITS,
			       MsgIdBin:MsgIdBinSize/binary,
			       MsgBody:BodySize/binary,
			       ?WRITE_OK:?WRITE_OK_SIZE_BITS>>) of
	ok -> {ok, TotalSize};
	KO -> KO
    end.

read_message_at_offset(FileHdl, Offset, TotalSize) ->
    TotalSizeWriteOkBytes = TotalSize + 1,
    case file:position(FileHdl, {bof, Offset}) of
	{ok, Offset} ->
	    case file:read(FileHdl, TotalSize + ?FILE_PACKING_ADJUSTMENT) of
		{ok, <<TotalSize:?INTEGER_SIZE_BITS,
		       MsgIdBinSize:?INTEGER_SIZE_BITS,
		       Rest:TotalSizeWriteOkBytes/binary>>} ->
		    BodySize = TotalSize - MsgIdBinSize,
		    <<_MsgId:MsgIdBinSize/binary, MsgBody:BodySize/binary,
		      ?WRITE_OK:?WRITE_OK_SIZE_BITS>> = Rest,
		    {ok, {MsgBody, BodySize}};
		KO -> KO
	    end;
	KO -> KO
    end.

scan_file_for_valid_messages(File) ->
    {ok, Hdl} = file:open(File, [raw, binary, read]),
    Valid = scan_file_for_valid_messages(Hdl, 0, []),
    _ = file:close(Hdl), %% if something really bad's happened, the close could fail, but ignore
    Valid.

scan_file_for_valid_messages(FileHdl, Offset, Acc) ->
    case read_next_file_entry(FileHdl, Offset) of
	{ok, eof} -> {ok, Acc};
	{ok, {corrupted, NextOffset}} ->
	    scan_file_for_valid_messages(FileHdl, NextOffset, Acc);
	{ok, {ok, MsgId, TotalSize, NextOffset}} ->
	    scan_file_for_valid_messages(FileHdl, NextOffset,
					 [{MsgId, TotalSize, Offset}|Acc]);
	_KO -> {ok, Acc} %% bad message, but we may still have recovered some valid messages
    end.
	    

read_next_file_entry(FileHdl, Offset) ->
    TwoIntegers = 2 * ?INTEGER_SIZE_BYTES,
    case file:read(FileHdl, TwoIntegers) of
	{ok, <<TotalSize:?INTEGER_SIZE_BITS, MsgIdBinSize:?INTEGER_SIZE_BITS>>} ->
	    case {TotalSize =:= 0, MsgIdBinSize =:= 0} of
		{true, _} -> {ok, eof}; %% Nothing we can do other than stop
		{false, true} -> %% current message corrupted, try skipping past it
		    ExpectedAbsPos = Offset + ?FILE_PACKING_ADJUSTMENT + TotalSize,
		    case file:position(FileHdl, {cur, TotalSize + 1}) of
			{ok, ExpectedAbsPos} -> {ok, {corrupted, ExpectedAbsPos}};
			{ok, _SomeOtherPos} -> {ok, eof}; %% seek failed, so give up
			KO -> KO
		    end;
		{false, false} -> %% all good, let's continue
		    case file:read(FileHdl, MsgIdBinSize) of
			{ok, <<MsgId:MsgIdBinSize/binary>>} ->
			    ExpectedAbsPos = Offset + TwoIntegers + TotalSize,
			    case file:position(FileHdl,
					       {cur, TotalSize - MsgIdBinSize}) of
				{ok, ExpectedAbsPos} ->
				    NextOffset = Offset + TotalSize +
					?FILE_PACKING_ADJUSTMENT,
				    case file:read(FileHdl, 1) of
					{ok, <<?WRITE_OK:?WRITE_OK_SIZE_BITS>>} ->
					    {ok, {ok, binary_to_term(MsgId),
						  TotalSize, NextOffset}};
					{ok, _SomeOtherData} ->
					    {ok, {corrupted, NextOffset}};
					KO -> KO
				    end;
				{ok, _SomeOtherPos} -> {ok, eof}; %% seek failed, so give up
				KO -> KO
			    end;
			eof -> {ok, eof};
			KO -> KO
		    end
	    end;
	eof -> {ok, eof};
	KO -> KO
    end.
