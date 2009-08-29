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

-behaviour(gen_server2).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([handle_pre_hibernate/1]).

-export([publish/3, fetch/1, phantom_fetch/1, ack/2, tx_publish/1, tx_commit/3,
         tx_rollback/1, requeue/2, purge/1, delete_queue/1,
         delete_non_durable_queues/1, requeue_next_n/2, len/1, foldl/3,
         prefetch/1
        ]).

-export([filesync/0, cache_info/0]).

-export([stop/0, stop_and_obliterate/0, set_mode/1, to_disk_only_mode/0,
         to_ram_disk_mode/0]).

%%----------------------------------------------------------------------------

-include("rabbit.hrl").

-define(WRITE_OK_SIZE_BITS,      8).
-define(WRITE_OK_TRANSIENT,      255).
-define(WRITE_OK_PERSISTENT,     254).
-define(INTEGER_SIZE_BYTES,      8).
-define(INTEGER_SIZE_BITS,       (8 * ?INTEGER_SIZE_BYTES)).
-define(MSG_LOC_NAME,            rabbit_disk_queue_msg_location).
-define(FILE_SUMMARY_ETS_NAME,   rabbit_disk_queue_file_summary).
-define(SEQUENCE_ETS_NAME,       rabbit_disk_queue_sequences).
-define(CACHE_ETS_NAME,          rabbit_disk_queue_cache).
-define(FILE_EXTENSION,          ".rdq").
-define(FILE_EXTENSION_TMP,      ".rdt").
-define(FILE_EXTENSION_DETS,     ".dets").
-define(FILE_PACKING_ADJUSTMENT, (1 + (2* (?INTEGER_SIZE_BYTES)))).
-define(BATCH_SIZE,              10000).
-define(CACHE_MAX_SIZE,          10485760).
-define(WRITE_HANDLE_OPEN_MODE,  [append, raw, binary, delayed_write]).
-define(MAX_READ_FILE_HANDLES,   256).
-define(FILE_SIZE_LIMIT,         (256*1024*1024)).

-define(BINARY_MODE, [raw, binary]).
-define(READ_MODE,   [read, read_ahead]).
-define(WRITE_MODE,  [write, delayed_write]).

-define(SHUTDOWN_MESSAGE_KEY, shutdown_token).
-define(SHUTDOWN_MESSAGE,
        #dq_msg_loc { queue_and_seq_id = ?SHUTDOWN_MESSAGE_KEY,
                      msg_id = infinity_and_beyond,
                      is_delivered = never
                     }).

-define(MINIMUM_MEMORY_REPORT_TIME_INTERVAL, 10000). %% 10 seconds in millisecs
-define(SYNC_INTERVAL, 5). %% milliseconds
-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-define(SERVER, ?MODULE).

-record(dqstate,
        {msg_location_dets,       %% where are messages?
         msg_location_ets,        %% as above, but for ets version
         operation_mode,          %% ram_disk | disk_only
         file_summary,            %% what's in the files?
         sequences,               %% next read and write for each q
         current_file_num,        %% current file name as number
         current_file_name,       %% current file name
         current_file_handle,     %% current file handle
         current_offset,          %% current offset within current file
         current_dirty,           %% has the current file been written to
                                  %% since the last fsync?
         file_size_limit,         %% how big can our files get?
         read_file_handle_cache,  %% file handle cache for reading
         on_sync_txns,            %% list of commiters to run on sync (reversed)
         commit_timer_ref,        %% TRef for our interval timer
         last_sync_offset,        %% current_offset at the last time we sync'd
         message_cache,           %% ets message cache
         memory_report_timer_ref, %% TRef for the memory report timer
         wordsize,                %% bytes in a word on this platform
         mnesia_bytes_per_record, %% bytes per record in mnesia in ram_disk mode
         ets_bytes_per_record     %% bytes per record in msg_location_ets
        }).

-record(message_store_entry,
        {msg_id, ref_count, file, offset, total_size, is_persistent}).

%% The components:
%%
%% MsgLocation: this is a (d)ets table which contains:
%%              {MsgId, RefCount, File, Offset, TotalSize, IsPersistent}
%% FileSummary: this is an ets table which contains:
%%              {File, ValidTotalSize, ContiguousTop, Left, Right}
%% Sequences:   this is an ets table which contains:
%%              {Q, ReadSeqId, WriteSeqId}
%% rabbit_disk_queue: this is an mnesia table which contains:
%%              #dq_msg_loc { queue_and_seq_id = {Q, SeqId},
%%                            is_delivered = IsDelivered,
%%                            msg_id = MsgId
%%                          }
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
%% |   X   |    |   G   |         |   G   |
%% +-------+    +-------+         +-------+
%% |   D   |    |   X   |         |   F   |
%% +-------+    +-------+         +-------+
%% |   X   |    |   X   |         |   E   |
%% +-------+    +-------+         +-------+
%% |   C   |    |   F   |   ===>  |   D   |
%% +-------+    +-------+         +-------+
%% |   X   |    |   X   |         |   C   |
%% +-------+    +-------+         +-------+
%% |   B   |    |   X   |         |   B   |
%% +-------+    +-------+         +-------+
%% |   A   |    |   E   |         |   A   |
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

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(seq_id() :: non_neg_integer()).
-type(ack_tag() :: {msg_id(), seq_id()}).

-spec(start_link/0 :: () ->
              ({'ok', pid()} | 'ignore' | {'error', any()})).
-spec(publish/3 :: (queue_name(), message(), boolean()) -> 'ok').
-spec(fetch/1 :: (queue_name()) ->
             ('empty' |
              {message(), boolean(), ack_tag(), non_neg_integer()})).
-spec(phantom_fetch/1 :: (queue_name()) ->
             ('empty' |
              {msg_id(), boolean(), boolean(), ack_tag(), non_neg_integer()})).
-spec(prefetch/1 :: (queue_name()) -> 'ok').
-spec(ack/2 :: (queue_name(), [ack_tag()]) -> 'ok').
-spec(tx_publish/1 :: (message()) -> 'ok').
-spec(tx_commit/3 :: (queue_name(), [{msg_id(), boolean()}], [ack_tag()]) ->
             'ok').
-spec(tx_rollback/1 :: ([msg_id()]) -> 'ok').
-spec(requeue/2 :: (queue_name(), [{ack_tag(), boolean()}]) -> 'ok').
-spec(requeue_next_n/2 :: (queue_name(), non_neg_integer()) -> 'ok').
-spec(purge/1 :: (queue_name()) -> non_neg_integer()).
-spec(delete_queue/1 :: (queue_name()) -> 'ok').
-spec(delete_non_durable_queues/1 :: (set()) -> 'ok').
-spec(len/1 :: (queue_name()) -> non_neg_integer()).
-spec(foldl/3 :: (fun ((message(), ack_tag(), boolean(), A) -> A),
                  A, queue_name()) -> A).
-spec(stop/0 :: () -> 'ok').
-spec(stop_and_obliterate/0 :: () -> 'ok').
-spec(to_disk_only_mode/0 :: () -> 'ok').
-spec(to_ram_disk_mode/0 :: () -> 'ok').
-spec(filesync/0 :: () -> 'ok').
-spec(cache_info/0 :: () -> [{atom(), term()}]).
-spec(set_mode/1 :: ('oppressed' | 'liberated') -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE,
                           [?FILE_SIZE_LIMIT, ?MAX_READ_FILE_HANDLES], []).

publish(Q, Message = #basic_message {}, IsDelivered) ->
    gen_server2:cast(?SERVER, {publish, Q, Message, IsDelivered}).

fetch(Q) ->
    gen_server2:call(?SERVER, {fetch, Q}, infinity).

phantom_fetch(Q) ->
    gen_server2:call(?SERVER, {phantom_fetch, Q}, infinity).

prefetch(Q) ->
    gen_server2:pcast(?SERVER, -1, {prefetch, Q, self()}).

ack(Q, MsgSeqIds) when is_list(MsgSeqIds) ->
    gen_server2:cast(?SERVER, {ack, Q, MsgSeqIds}).

tx_publish(Message = #basic_message {}) ->
    gen_server2:cast(?SERVER, {tx_publish, Message}).

tx_commit(Q, PubMsgIds, AckSeqIds)
  when is_list(PubMsgIds) andalso is_list(AckSeqIds) ->
    gen_server2:call(?SERVER, {tx_commit, Q, PubMsgIds, AckSeqIds}, infinity).

tx_rollback(MsgIds) when is_list(MsgIds) ->
    gen_server2:cast(?SERVER, {tx_rollback, MsgIds}).

requeue(Q, MsgSeqIds) when is_list(MsgSeqIds) ->
    gen_server2:cast(?SERVER, {requeue, Q, MsgSeqIds}).

requeue_next_n(Q, N) when is_integer(N) ->
    gen_server2:cast(?SERVER, {requeue_next_n, Q, N}).

purge(Q) ->
    gen_server2:call(?SERVER, {purge, Q}, infinity).

delete_queue(Q) ->
    gen_server2:cast(?SERVER, {delete_queue, Q}).

delete_non_durable_queues(DurableQueues) ->
    gen_server2:call(?SERVER, {delete_non_durable_queues, DurableQueues},
                     infinity).

len(Q) ->
    gen_server2:call(?SERVER, {len, Q}, infinity).

foldl(Fun, Init, Acc) ->
    gen_server2:call(?SERVER, {foldl, Fun, Init, Acc}, infinity).

stop() ->
    gen_server2:call(?SERVER, stop, infinity).

stop_and_obliterate() ->
    gen_server2:call(?SERVER, stop_vaporise, infinity).

to_disk_only_mode() ->
    gen_server2:pcall(?SERVER, 9, to_disk_only_mode, infinity).

to_ram_disk_mode() ->
    gen_server2:pcall(?SERVER, 9, to_ram_disk_mode, infinity).

filesync() ->
    gen_server2:pcall(?SERVER, 9, filesync).

cache_info() ->
    gen_server2:call(?SERVER, cache_info, infinity).

set_mode(Mode) ->
    gen_server2:pcast(?SERVER, 10, {set_mode, Mode}).

%%----------------------------------------------------------------------------
%% gen_server behaviour
%%----------------------------------------------------------------------------

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
    ok = rabbit_memory_manager:register
           (self(), true, rabbit_disk_queue, set_mode, []),
    ok = filelib:ensure_dir(form_filename("nothing")),

    Node = node(),
    ok = case mnesia:change_table_copy_type(rabbit_disk_queue, Node,
                                            disc_copies) of
             {atomic, ok} -> ok;
             {aborted, {already_exists, rabbit_disk_queue, Node,
                        disc_copies}} -> ok;
             E -> E
         end,

    ok = detect_shutdown_state_and_adjust_delivered_flags(),

    file:delete(msg_location_dets_file()),

    {ok, MsgLocationDets} =
        dets:open_file(?MSG_LOC_NAME,
                       [{file, msg_location_dets_file()},
                        {min_no_slots, 1024*1024},
                        %% man says this should be <= 32M. But it works...
                        {max_no_slots, 30*1024*1024},
                        {type, set},
                        {keypos, 2}
                       ]),

    %% it would be better to have this as private, but dets:from_ets/2
    %% seems to blow up if it is set private - see bug21489
    MsgLocationEts = ets:new(?MSG_LOC_NAME, [set, protected, {keypos, 2}]),

    InitName = "0" ++ ?FILE_EXTENSION,
    HandleCache = rabbit_file_handle_cache:init(ReadFileHandlesLimit,
                                                ?BINARY_MODE ++ [read]),
    State =
        #dqstate { msg_location_dets       = MsgLocationDets,
                   msg_location_ets        = MsgLocationEts,
                   operation_mode          = ram_disk,
                   file_summary            = ets:new(?FILE_SUMMARY_ETS_NAME,
                                                     [set, private]),
                   sequences               = ets:new(?SEQUENCE_ETS_NAME,
                                                     [set, private]),
                   current_file_num        = 0,
                   current_file_name       = InitName,
                   current_file_handle     = undefined,
                   current_offset          = 0,
                   current_dirty           = false,
                   file_size_limit         = FileSizeLimit,
                   read_file_handle_cache  = HandleCache,
                   on_sync_txns            = [],
                   commit_timer_ref        = undefined,
                   last_sync_offset        = 0,
                   message_cache           = ets:new(?CACHE_ETS_NAME,
                                                     [set, private]),
                   memory_report_timer_ref = undefined,
                   wordsize                = erlang:system_info(wordsize),
                   mnesia_bytes_per_record = undefined,
                   ets_bytes_per_record    = undefined
                 },
    {ok, State1 = #dqstate { current_file_name = CurrentName,
                             current_offset = Offset } } =
        load_from_disk(State),
    %% read is only needed so that we can seek
    {ok, FileHdl} = open_file(CurrentName, ?WRITE_MODE ++ [read]),
    {ok, Offset} = file:position(FileHdl, Offset),
    State2 = State1 #dqstate { current_file_handle = FileHdl },
    %% by reporting a memory use of 0, we guarantee the manager will
    %% not oppress us. We have to start in ram_disk mode because we
    %% can't find values for mnesia_bytes_per_record or
    %% ets_bytes_per_record otherwise.
    ok = rabbit_memory_manager:report_memory(self(), 0, false),
    ok = report_memory(false, State2),
    {ok, start_memory_timer(State2), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({fetch, Q}, _From, State) ->
    {ok, Result, State1} =
        internal_fetch_body(Q, record_delivery, pop_queue, State),
    reply(Result, State1);
handle_call({phantom_fetch, Q}, _From, State) ->
    {ok, Result, State1} =
        internal_fetch_attributes(Q, record_delivery, pop_queue, State),
    reply(Result, State1);
handle_call({tx_commit, Q, PubMsgIds, AckSeqIds}, From, State) ->
    State1 =
        internal_tx_commit(Q, PubMsgIds, AckSeqIds, From, State),
    noreply(State1);
handle_call({purge, Q}, _From, State) ->
    {ok, Count, State1} = internal_purge(Q, State),
    reply(Count, State1);
handle_call(filesync, _From, State) ->
    reply(ok, sync_current_file_handle(State));
handle_call({len, Q}, _From, State = #dqstate { sequences = Sequences }) ->
    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    reply(WriteSeqId - ReadSeqId, State);
handle_call({foldl, Fun, Init, Q}, _From, State) ->
    {ok, Result, State1} = internal_foldl(Q, Fun, Init, State),
    reply(Result, State1);
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
     State1 #dqstate { current_file_handle = undefined }};
    %% gen_server now calls terminate, which then calls shutdown
handle_call(to_disk_only_mode, _From, State) ->
    reply(ok, to_disk_only_mode(State));
handle_call(to_ram_disk_mode, _From, State) ->
    reply(ok, to_ram_disk_mode(State));
handle_call({delete_non_durable_queues, DurableQueues}, _From, State) ->
    {ok, State1} = internal_delete_non_durable_queues(DurableQueues, State),
    reply(ok, State1);
handle_call(cache_info, _From, State = #dqstate { message_cache = Cache }) ->
    reply(ets:info(Cache), State).

handle_cast({publish, Q, Message, IsDelivered}, State) ->
    {ok, _MsgSeqId, State1} = internal_publish(Q, Message, IsDelivered, State),
    noreply(State1);
handle_cast({ack, Q, MsgSeqIds}, State) ->
    {ok, State1} = internal_ack(Q, MsgSeqIds, State),
    noreply(State1);
handle_cast({tx_publish, Message}, State) ->
    {ok, State1} = internal_tx_publish(Message, State),
    noreply(State1);
handle_cast({tx_rollback, MsgIds}, State) ->
    {ok, State1} = internal_tx_rollback(MsgIds, State),
    noreply(State1);
handle_cast({requeue, Q, MsgSeqIds}, State) ->
    {ok, State1} = internal_requeue(Q, MsgSeqIds, State),
    noreply(State1);
handle_cast({requeue_next_n, Q, N}, State) ->
    {ok, State1} = internal_requeue_next_n(Q, N, State),
    noreply(State1);
handle_cast({delete_queue, Q}, State) ->
    {ok, State1} = internal_delete_queue(Q, State),
    noreply(State1);
handle_cast({set_mode, Mode}, State) ->
    noreply((case Mode of
                 oppressed -> fun to_disk_only_mode/1;
                 liberated -> fun to_ram_disk_mode/1
             end)(State));
handle_cast({prefetch, Q, From}, State) ->
    {ok, Result, State1} =
        internal_fetch_body(Q, record_delivery, peek_queue, State),
    Cont = rabbit_misc:with_exit_handler(
             fun () -> false end,
             fun () ->
                     ok = rabbit_queue_prefetcher:publish(From, Result),
                     true
             end),
    State3 =
	case Cont of
	    true ->
		case internal_fetch_attributes(
                       Q, ignore_delivery, pop_queue, State1) of
		    {ok, empty, State2} -> State2;
		    {ok, _, State2} -> State2
		end;
	    false -> State1
	end,
    noreply(State3).

handle_info(report_memory, State) ->
    %% call noreply1/2, not noreply/1/2, as we don't want to restart the
    %% memory_report_timer_ref.
    %% By unsetting the timer, we force a report on the next normal message
    noreply1(State #dqstate { memory_report_timer_ref = undefined });
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};
handle_info(timeout, State) ->
    %% must have commit_timer set, so timeout was 0, and we're not hibernating
    noreply(sync_current_file_handle(State)).

handle_pre_hibernate(State) ->
    %% don't use noreply/1 or noreply1/1 as they'll restart the memory timer
    ok = report_memory(true, State),
    {hibernate, stop_memory_timer(State)}.

terminate(_Reason, State) ->
    shutdown(State).

shutdown(State = #dqstate { msg_location_dets = MsgLocationDets,
                            msg_location_ets = MsgLocationEts,
                            current_file_handle = FileHdl,
                            read_file_handle_cache = HC
                          }) ->
    %% deliberately ignoring return codes here
    State1 = stop_commit_timer(stop_memory_timer(State)),
    dets:close(MsgLocationDets),
    file:delete(msg_location_dets_file()),
    true = ets:delete_all_objects(MsgLocationEts),
    case FileHdl of
        undefined -> ok;
        _ -> sync_current_file_handle(State),
             file:close(FileHdl)
    end,
    store_safe_shutdown(),
    HC1 = rabbit_file_handle_cache:close_all(HC),
    State1 #dqstate { current_file_handle = undefined,
                      current_dirty = false,
                      read_file_handle_cache = HC1,
                      memory_report_timer_ref = undefined
                    }.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% memory management helper functions
%%----------------------------------------------------------------------------

stop_memory_timer(State = #dqstate { memory_report_timer_ref = undefined }) ->
    State;
stop_memory_timer(State = #dqstate { memory_report_timer_ref = TRef }) ->
    {ok, cancel} = timer:cancel(TRef),
    State #dqstate { memory_report_timer_ref = undefined }.

start_memory_timer(State = #dqstate { memory_report_timer_ref = undefined }) ->
    ok = report_memory(false, State),
    {ok, TRef} = timer:send_after(?MINIMUM_MEMORY_REPORT_TIME_INTERVAL,
                                  report_memory),
    State #dqstate { memory_report_timer_ref = TRef };
start_memory_timer(State) ->
    State.

report_memory(Hibernating, State) ->
    Bytes = memory_use(State),
    rabbit_memory_manager:report_memory(self(), trunc(2.5 * Bytes),
                                            Hibernating).

memory_use(#dqstate { operation_mode   = ram_disk,
                      file_summary     = FileSummary,
                      sequences        = Sequences,
                      msg_location_ets = MsgLocationEts,
                      message_cache    = Cache,
                      wordsize         = WordSize
                     }) ->
    WordSize * (mnesia:table_info(rabbit_disk_queue, memory) +
                lists:sum([ets:info(Table, memory)
                           || Table <- [MsgLocationEts, FileSummary, Cache,
                                        Sequences]]));
memory_use(#dqstate { operation_mode          = disk_only,
                      file_summary            = FileSummary,
                      sequences               = Sequences,
                      msg_location_dets       = MsgLocationDets,
                      message_cache           = Cache,
                      wordsize                = WordSize,
                      mnesia_bytes_per_record = MnesiaBytesPerRecord,
                      ets_bytes_per_record    = EtsBytesPerRecord }) ->
    (WordSize * (lists:sum([ets:info(Table, memory)
                            || Table <- [FileSummary, Cache, Sequences]]))) +
        rabbit_misc:ceil(
          mnesia:table_info(rabbit_disk_queue, size) * MnesiaBytesPerRecord) +
        rabbit_misc:ceil(
          dets:info(MsgLocationDets, size) * EtsBytesPerRecord).

to_disk_only_mode(State = #dqstate { operation_mode = disk_only }) ->
    State;
to_disk_only_mode(State = #dqstate { operation_mode    = ram_disk,
                                     msg_location_dets = MsgLocationDets,
                                     msg_location_ets  = MsgLocationEts,
                                     wordsize          = WordSize }) ->
    rabbit_log:info("Converting disk queue to disk only mode~n", []),
    MnesiaMemBytes = WordSize * mnesia:table_info(rabbit_disk_queue, memory),
    EtsMemBytes    = WordSize * ets:info(MsgLocationEts, memory),
    MnesiaSize     = lists:max([1, mnesia:table_info(rabbit_disk_queue, size)]),
    EtsSize        = lists:max([1, ets:info(MsgLocationEts, size)]),
    {atomic, ok} = mnesia:change_table_copy_type(rabbit_disk_queue, node(),
                                                 disc_only_copies),
    ok = dets:from_ets(MsgLocationDets, MsgLocationEts),
    true = ets:delete_all_objects(MsgLocationEts),
    garbage_collect(),
    State #dqstate { operation_mode          = disk_only,
                     mnesia_bytes_per_record = MnesiaMemBytes / MnesiaSize,
                     ets_bytes_per_record    = EtsMemBytes / EtsSize }.

to_ram_disk_mode(State = #dqstate { operation_mode = ram_disk }) ->
    State;
to_ram_disk_mode(State = #dqstate { operation_mode    = disk_only,
                                    msg_location_dets = MsgLocationDets,
                                    msg_location_ets  = MsgLocationEts }) ->
    rabbit_log:info("Converting disk queue to ram disk mode~n", []),
    {atomic, ok} = mnesia:change_table_copy_type(rabbit_disk_queue, node(),
                                                 disc_copies),
    true = ets:from_dets(MsgLocationEts, MsgLocationDets),
    ok = dets:delete_all_objects(MsgLocationDets),
    garbage_collect(),
    State #dqstate { operation_mode          = ram_disk,
                     mnesia_bytes_per_record = undefined,
                     ets_bytes_per_record    = undefined }.

%%----------------------------------------------------------------------------
%% message cache helper functions
%%----------------------------------------------------------------------------

remove_cache_entry(MsgId, #dqstate { message_cache = Cache }) ->
    true = ets:delete(Cache, MsgId),
    ok.

fetch_and_increment_cache(MsgId, #dqstate { message_cache = Cache }) ->
    case ets:lookup(Cache, MsgId) of
        [] ->
            not_found;
        [{MsgId, Message, _RefCount}] ->
            NewRefCount = ets:update_counter(Cache, MsgId, {3, 1}),
            {Message, NewRefCount}
    end.

decrement_cache(MsgId, #dqstate { message_cache = Cache }) ->
    true = try case ets:update_counter(Cache, MsgId, {3, -1}) of
                   N when N =< 0 -> true = ets:delete(Cache, MsgId);
                   _N -> true
               end
           catch error:badarg ->
                   %% MsgId is not in there because although it's been
                   %% delivered, it's never actually been read (think:
                   %% persistent message in mixed queue)
                   true
           end,
    ok.

insert_into_cache(Message = #basic_message { guid = MsgId },
                  #dqstate { message_cache = Cache }) ->
    case cache_is_full(Cache) of
        true -> ok;
        false -> true = ets:insert_new(Cache, {MsgId, Message, 1}),
                 ok
    end.

cache_is_full(Cache) ->
    ets:info(Cache, memory) > ?CACHE_MAX_SIZE.

%%----------------------------------------------------------------------------
%% dets/ets agnosticism
%%----------------------------------------------------------------------------

dets_ets_lookup(#dqstate { msg_location_dets = MsgLocationDets,
                           operation_mode = disk_only }, Key) ->
    dets:lookup(MsgLocationDets, Key);
dets_ets_lookup(#dqstate { msg_location_ets = MsgLocationEts,
                           operation_mode = ram_disk }, Key) ->
    ets:lookup(MsgLocationEts, Key).

dets_ets_delete(#dqstate { msg_location_dets = MsgLocationDets,
                           operation_mode = disk_only }, Key) ->
    ok = dets:delete(MsgLocationDets, Key);
dets_ets_delete(#dqstate { msg_location_ets = MsgLocationEts,
                           operation_mode = ram_disk }, Key) ->
    true = ets:delete(MsgLocationEts, Key),
    ok.

dets_ets_insert(#dqstate { msg_location_dets = MsgLocationDets,
                           operation_mode = disk_only }, Obj) ->
    ok = dets:insert(MsgLocationDets, Obj);
dets_ets_insert(#dqstate { msg_location_ets = MsgLocationEts,
                           operation_mode = ram_disk }, Obj) ->
    true = ets:insert(MsgLocationEts, Obj),
    ok.

dets_ets_insert_new(#dqstate { msg_location_dets = MsgLocationDets,
                               operation_mode = disk_only }, Obj) ->
    true = dets:insert_new(MsgLocationDets, Obj);
dets_ets_insert_new(#dqstate { msg_location_ets = MsgLocationEts,
                               operation_mode = ram_disk }, Obj) ->
    true = ets:insert_new(MsgLocationEts, Obj).

dets_ets_match_object(#dqstate { msg_location_dets = MsgLocationDets,
                                 operation_mode = disk_only }, Obj) ->
    dets:match_object(MsgLocationDets, Obj);
dets_ets_match_object(#dqstate { msg_location_ets = MsgLocationEts,
                                 operation_mode = ram_disk }, Obj) ->
    ets:match_object(MsgLocationEts, Obj).

%%----------------------------------------------------------------------------
%% general helper functions
%%----------------------------------------------------------------------------

noreply(NewState) ->
    noreply1(start_memory_timer(NewState)).

noreply1(NewState = #dqstate { on_sync_txns = [],
                               commit_timer_ref = undefined }) ->
    {noreply, NewState, hibernate};
noreply1(NewState = #dqstate { commit_timer_ref = undefined }) ->
    {noreply, start_commit_timer(NewState), 0};
noreply1(NewState = #dqstate { on_sync_txns = [] }) ->
    {noreply, stop_commit_timer(NewState), hibernate};
noreply1(NewState) ->
    {noreply, NewState, 0}.

reply(Reply, NewState) ->
    reply1(Reply, start_memory_timer(NewState)).

reply1(Reply, NewState = #dqstate { on_sync_txns = [],
                                    commit_timer_ref = undefined }) ->
    {reply, Reply, NewState, hibernate};
reply1(Reply, NewState = #dqstate { commit_timer_ref = undefined }) ->
    {reply, Reply, start_commit_timer(NewState), 0};
reply1(Reply, NewState = #dqstate { on_sync_txns = [] }) ->
    {reply, Reply, stop_commit_timer(NewState), hibernate};
reply1(Reply, NewState) ->
    {reply, Reply, NewState, 0}.

form_filename(Name) ->
    filename:join(base_directory(), Name).

base_directory() ->
    filename:join(rabbit_mnesia:dir(), "rabbit_disk_queue/").

msg_location_dets_file() ->
    form_filename(atom_to_list(?MSG_LOC_NAME) ++ ?FILE_EXTENSION_DETS).

open_file(File, Mode) -> file:open(form_filename(File), ?BINARY_MODE ++ Mode).

with_read_handle_at(File, Offset, Fun, State =
                    #dqstate { read_file_handle_cache = HC,
                               current_file_name = CurName,
                               current_dirty = IsDirty,
                               last_sync_offset = SyncOffset
                              }) ->
    State1 = if CurName =:= File andalso IsDirty andalso Offset >= SyncOffset ->
                     sync_current_file_handle(State);
                true -> State
             end,
    FilePath = form_filename(File),
    {Result, HC1} =
        rabbit_file_handle_cache:with_file_handle_at(FilePath, Offset, Fun, HC),
    {Result, State1 #dqstate { read_file_handle_cache = HC1 }}.

sync_current_file_handle(State = #dqstate { current_dirty = false,
                                            on_sync_txns = [] }) ->
    State;
sync_current_file_handle(State = #dqstate { current_file_handle = CurHdl,
                                            current_dirty = IsDirty,
                                            current_offset = CurOffset,
                                            on_sync_txns = Txns,
                                            last_sync_offset = SyncOffset
                                          }) ->
    SyncOffset1 = case IsDirty of
                      true -> ok = file:sync(CurHdl),
                              CurOffset;
                      false -> SyncOffset
                  end,
    State1 = lists:foldl(fun internal_do_tx_commit/2, State, lists:reverse(Txns)),
    State1 #dqstate { current_dirty = false, on_sync_txns = [],
                      last_sync_offset = SyncOffset1 }.

sequence_lookup(Sequences, Q) ->
    case ets:lookup(Sequences, Q) of
        [] ->
            {0, 0};
        [{Q, ReadSeqId, WriteSeqId}] ->
            {ReadSeqId, WriteSeqId}
    end.

start_commit_timer(State = #dqstate { commit_timer_ref = undefined }) ->
    {ok, TRef} = timer:apply_after(?SYNC_INTERVAL, ?MODULE, filesync, []),
    State #dqstate { commit_timer_ref = TRef }.

stop_commit_timer(State = #dqstate { commit_timer_ref = undefined }) ->
    State;
stop_commit_timer(State = #dqstate { commit_timer_ref = TRef }) ->
    {ok, cancel} = timer:cancel(TRef),
    State #dqstate { commit_timer_ref = undefined }.

msg_to_bin(Msg = #basic_message { content = Content }) ->
    ClearedContent = rabbit_binary_parser:clear_decoded_content(Content),
    term_to_binary(Msg #basic_message { content = ClearedContent }).

bin_to_msg(MsgBin) ->
    binary_to_term(MsgBin).

%%----------------------------------------------------------------------------
%% internal functions
%%----------------------------------------------------------------------------

internal_fetch_body(Q, MarkDelivered, Advance, State) ->
    case queue_head(Q, MarkDelivered, Advance, State) of
        E = {ok, empty, _} -> E;
        {ok, AckTag, IsDelivered, StoreEntry, Remaining, State1} ->
            {Message, State2} = read_stored_message(StoreEntry, State1),
            {ok, {Message, IsDelivered, AckTag, Remaining}, State2}
    end.

internal_fetch_attributes(Q, MarkDelivered, Advance, State) ->
    case queue_head(Q, MarkDelivered, Advance, State) of
        E = {ok, empty, _} -> E;
        {ok, AckTag, IsDelivered,
         #message_store_entry { msg_id = MsgId, is_persistent = IsPersistent },
         Remaining, State1} ->
            {ok, {MsgId, IsPersistent, IsDelivered, AckTag, Remaining}, State1}
    end.

queue_head(Q, MarkDelivered, Advance,
           State = #dqstate { sequences = Sequences }) ->
    case sequence_lookup(Sequences, Q) of
        {SeqId, SeqId} -> {ok, empty, State};
        {ReadSeqId, WriteSeqId} when WriteSeqId > ReadSeqId ->
            Remaining = WriteSeqId - ReadSeqId - 1,
            {AckTag, IsDelivered, StoreEntry} =
                update_message_attributes(Q, ReadSeqId, MarkDelivered, State),
            ok = maybe_advance(Advance, Sequences, Q, ReadSeqId, WriteSeqId),
            {ok, AckTag, IsDelivered, StoreEntry, Remaining, State}
    end.

maybe_advance(peek_queue, _, _, _, _) ->
    ok;
maybe_advance(pop_queue, Sequences, Q, ReadSeqId, WriteSeqId) ->
    true = ets:insert(Sequences, {Q, ReadSeqId + 1, WriteSeqId}),
    ok.

read_stored_message(#message_store_entry { msg_id = MsgId, ref_count = RefCount,
                                           file = File, offset = Offset,
                                           total_size = TotalSize }, State) ->
    case fetch_and_increment_cache(MsgId, State) of
        not_found ->
            {{ok, {MsgBody, _IsPersistent, _BodySize}}, State1} =
                with_read_handle_at(
                  File, Offset,
                  fun(Hdl) ->
                          Res = case read_message_from_disk(Hdl, TotalSize) of
                                    {ok, {_, _, _}} = Obj -> Obj;
                                    {ok, Rest} ->
                                        throw({error,
                                               {misread, [{old_state, State},
                                                          {file, File},
                                                          {offset, Offset},
                                                          {read, Rest}]}})
                                end,
                          {Offset + TotalSize + ?FILE_PACKING_ADJUSTMENT, Res}
                  end, State),
            Message = #basic_message {} = bin_to_msg(MsgBody),
            ok = if RefCount > 1 ->
                         insert_into_cache(Message, State1);
                    true -> ok
                            %% it's not in the cache and we only have
                            %% 1 queue with the message. So don't
                            %% bother putting it in the cache.
                 end,
            {Message, State1};
        {Message, _RefCount} ->
            {Message, State}
    end.

update_message_attributes(Q, SeqId, MarkDelivered, State) ->
    [Obj =
     #dq_msg_loc {is_delivered = IsDelivered, msg_id = MsgId}] =
        mnesia:dirty_read(rabbit_disk_queue, {Q, SeqId}),
    [StoreEntry = #message_store_entry { msg_id = MsgId }] =
        dets_ets_lookup(State, MsgId),
    ok = case {IsDelivered, MarkDelivered} of
             {true, _} -> ok;
             {false, ignore_delivery} -> ok;
             {false, record_delivery} ->
                 mnesia:dirty_write(rabbit_disk_queue,
                                    Obj #dq_msg_loc {is_delivered = true})
         end,
    {{MsgId, SeqId}, IsDelivered, StoreEntry}.

internal_foldl(Q, Fun, Init, State) ->
    State1 = #dqstate { sequences = Sequences } =
        sync_current_file_handle(State),
    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    internal_foldl(Q, WriteSeqId, Fun, State1, Init, ReadSeqId).

internal_foldl(_Q, SeqId, _Fun, State, Acc, SeqId) ->
    {ok, Acc, State};
internal_foldl(Q, WriteSeqId, Fun, State, Acc, ReadSeqId) ->
    {AckTag, IsDelivered, StoreEntry} =
        update_message_attributes(Q, ReadSeqId, ignore_delivery, State),
    {Message, State1} = read_stored_message(StoreEntry, State),
    Acc1 = Fun(Message, AckTag, IsDelivered, Acc),
    internal_foldl(Q, WriteSeqId, Fun, State1, Acc1, ReadSeqId + 1).

internal_ack(Q, MsgSeqIds, State) ->
    remove_messages(Q, MsgSeqIds, true, State).

%% Q is only needed if MnesiaDelete /= false
remove_messages(Q, MsgSeqIds, MnesiaDelete, State) ->
    Files =
        lists:foldl(
          fun ({MsgId, SeqId}, Files1) ->
                  Files2 = remove_message(MsgId, Files1, State),
                  ok = case MnesiaDelete of
                           true -> mnesia:dirty_delete(rabbit_disk_queue,
                                                       {Q, SeqId});
                           _ -> ok
                       end,
                  Files2
          end, sets:new(), MsgSeqIds),
    State1 = compact(Files, State),
    {ok, State1}.

remove_message(MsgId, Files,
               State = #dqstate { file_summary = FileSummary,
                                  current_file_name = CurName
                                }) ->
    [StoreEntry =
     #message_store_entry { msg_id = MsgId, ref_count = RefCount, file = File,
                            offset = Offset, total_size = TotalSize }] =
        dets_ets_lookup(State, MsgId),
    case RefCount of
        1 ->
            ok = dets_ets_delete(State, MsgId),
            ok = remove_cache_entry(MsgId, State),
            [{File, ValidTotalSize, ContiguousTop, Left, Right}] =
                ets:lookup(FileSummary, File),
            ContiguousTop1 = lists:min([ContiguousTop, Offset]),
            true =
                ets:insert(FileSummary,
                           {File,
                            (ValidTotalSize-TotalSize-?FILE_PACKING_ADJUSTMENT),
                            ContiguousTop1, Left, Right}),
            if CurName =:= File -> Files;
               true -> sets:add_element(File, Files)
            end;
        _ when 1 < RefCount ->
            ok = decrement_cache(MsgId, State),
            ok = dets_ets_insert(State, StoreEntry #message_store_entry
                                 { ref_count = RefCount - 1 }),
            Files
    end.

internal_tx_publish(Message = #basic_message { is_persistent = IsPersistent,
                                               guid = MsgId },
                    State = #dqstate { current_file_handle = CurHdl,
                                       current_file_name = CurName,
                                       current_offset = CurOffset,
                                       file_summary = FileSummary
                                      }) ->
    case dets_ets_lookup(State, MsgId) of
        [] ->
            %% New message, lots to do
            {ok, TotalSize} = append_message(CurHdl, MsgId, msg_to_bin(Message),
                                             IsPersistent),
            true = dets_ets_insert_new(
                     State, #message_store_entry
                     { msg_id = MsgId, ref_count = 1, file = CurName,
                       offset = CurOffset, total_size = TotalSize,
                       is_persistent = IsPersistent }),
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
            maybe_roll_to_new_file(
              NextOffset, State #dqstate {current_offset = NextOffset,
                                          current_dirty = true});
        [StoreEntry =
         #message_store_entry { msg_id = MsgId, ref_count = RefCount }] ->
            %% We already know about it, just update counter
            ok = dets_ets_insert(State, StoreEntry #message_store_entry
                                 { ref_count = RefCount + 1 }),
            {ok, State}
    end.

internal_tx_commit(Q, PubMsgIds, AckSeqIds, From,
                   State = #dqstate { current_file_name = CurFile,
                                      current_dirty = IsDirty,
                                      on_sync_txns = Txns,
                                      last_sync_offset = SyncOffset
                                    }) ->
    NeedsSync = IsDirty andalso
        lists:any(fun ({MsgId, _IsDelivered}) ->
                          [#message_store_entry { msg_id = MsgId, file = File,
                                                  offset = Offset }] =
                              dets_ets_lookup(State, MsgId),
                          File =:= CurFile andalso Offset >= SyncOffset
                  end, PubMsgIds),
    TxnDetails = {Q, PubMsgIds, AckSeqIds, From},
    case NeedsSync of
        true  -> Txns1 = [TxnDetails | Txns],
                 State #dqstate { on_sync_txns = Txns1 };
        false -> internal_do_tx_commit(TxnDetails, State)
    end.

internal_do_tx_commit({Q, PubMsgIds, AckSeqIds, From},
                      State = #dqstate { sequences = Sequences }) ->
    {InitReadSeqId, InitWriteSeqId} = sequence_lookup(Sequences, Q),
    WriteSeqId =
        rabbit_misc:execute_mnesia_transaction(
          fun() ->
                  ok = mnesia:write_lock_table(rabbit_disk_queue),
                  {ok, WriteSeqId1} =
                      lists:foldl(
                        fun ({MsgId, IsDelivered}, {ok, SeqId}) ->
                                {mnesia:write(
                                   rabbit_disk_queue,
                                   #dq_msg_loc { queue_and_seq_id = {Q, SeqId},
                                                 msg_id = MsgId,
                                                 is_delivered = IsDelivered
                                               }, write),
                                 SeqId + 1}
                        end, {ok, InitWriteSeqId}, PubMsgIds),
                  WriteSeqId1
          end),
    {ok, State1} = remove_messages(Q, AckSeqIds, true, State),
    true = case PubMsgIds of
               [] -> true;
               _  -> ets:insert(Sequences, {Q, InitReadSeqId, WriteSeqId})
           end,
    gen_server2:reply(From, ok),
    State1.

internal_publish(Q, Message = #basic_message { guid = MsgId },
                 IsDelivered, State) ->
    {ok, State1 = #dqstate { sequences = Sequences }} =
        internal_tx_publish(Message, State),
    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    ok = mnesia:dirty_write(rabbit_disk_queue,
                            #dq_msg_loc { queue_and_seq_id = {Q, WriteSeqId},
                                          msg_id = MsgId,
                                          is_delivered = IsDelivered}),
    true = ets:insert(Sequences, {Q, ReadSeqId, WriteSeqId + 1}),
    {ok, {MsgId, WriteSeqId}, State1}.

internal_tx_rollback(MsgIds, State) ->
    %% we don't need seq ids because we're not touching mnesia,
    %% because seqids were never assigned
    MsgSeqIds = lists:zip(MsgIds, lists:duplicate(length(MsgIds), undefined)),
    remove_messages(undefined, MsgSeqIds, false, State).

internal_requeue(_Q, [], State) ->
    {ok, State};
internal_requeue(Q, MsgSeqIds, State = #dqstate { sequences = Sequences }) ->
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

    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    {WriteSeqId1, Q, MsgIds} =
        rabbit_misc:execute_mnesia_transaction(
          fun() ->
                  ok = mnesia:write_lock_table(rabbit_disk_queue),
                  lists:foldl(fun requeue_message/2, {WriteSeqId, Q, []},
                              MsgSeqIds)
          end),
    true = ets:insert(Sequences, {Q, ReadSeqId, WriteSeqId1}),
    lists:foreach(fun (MsgId) -> decrement_cache(MsgId, State) end, MsgIds),
    {ok, State}.

requeue_message({{MsgId, SeqId}, IsDelivered}, {WriteSeqId, Q, Acc}) ->
    [Obj = #dq_msg_loc { is_delivered = true, msg_id = MsgId }] =
        mnesia:read(rabbit_disk_queue, {Q, SeqId}, write),
    ok = mnesia:write(rabbit_disk_queue,
                      Obj #dq_msg_loc {queue_and_seq_id = {Q, WriteSeqId},
                                       is_delivered = IsDelivered
                                      },
                      write),
    ok = mnesia:delete(rabbit_disk_queue, {Q, SeqId}, write),
    {WriteSeqId + 1, Q, [MsgId | Acc]}.

%% move the next N messages from the front of the queue to the back.
internal_requeue_next_n(Q, N, State = #dqstate { sequences = Sequences }) ->
    {ReadSeqId, WriteSeqId} = sequence_lookup(Sequences, Q),
    if N >= (WriteSeqId - ReadSeqId) -> {ok, State};
       true ->
            {ReadSeqIdN, WriteSeqIdN, MsgIds} =
                rabbit_misc:execute_mnesia_transaction(
                  fun() ->
                          ok = mnesia:write_lock_table(rabbit_disk_queue),
                          requeue_next_messages(Q, N, ReadSeqId, WriteSeqId, [])
                  end
                 ),
            true = ets:insert(Sequences, {Q, ReadSeqIdN, WriteSeqIdN}),
            lists:foreach(fun (MsgId) -> decrement_cache(MsgId, State) end, MsgIds),
            {ok, State}
    end.

requeue_next_messages(_Q, 0, ReadSeq, WriteSeq, Acc) ->
    {ReadSeq, WriteSeq, Acc};
requeue_next_messages(Q, N, ReadSeq, WriteSeq, Acc) ->
    [Obj = #dq_msg_loc { msg_id = MsgId }] =
        mnesia:read(rabbit_disk_queue, {Q, ReadSeq}, write),
    ok = mnesia:write(rabbit_disk_queue,
                      Obj #dq_msg_loc {queue_and_seq_id = {Q, WriteSeq}},
                      write),
    ok = mnesia:delete(rabbit_disk_queue, {Q, ReadSeq}, write),
    requeue_next_messages(Q, N - 1, ReadSeq + 1, WriteSeq + 1, [MsgId | Acc]).

internal_purge(Q, State = #dqstate { sequences = Sequences }) ->
    case sequence_lookup(Sequences, Q) of
        {SeqId, SeqId} -> {ok, 0, State};
        {ReadSeqId, WriteSeqId} ->
            {MsgSeqIds, WriteSeqId} =
                rabbit_misc:unfold(
                  fun (SeqId) when SeqId == WriteSeqId -> false;
                      (SeqId) ->
                          [#dq_msg_loc { msg_id = MsgId }] =
                              mnesia:dirty_read(rabbit_disk_queue, {Q, SeqId}),
                          {true, {MsgId, SeqId}, SeqId + 1}
                  end, ReadSeqId),
            true = ets:insert(Sequences, {Q, WriteSeqId, WriteSeqId}),
            {ok, State1} = remove_messages(Q, MsgSeqIds, true, State),
            {ok, WriteSeqId - ReadSeqId, State1}
    end.

internal_delete_queue(Q, State) ->
    State1 = sync_current_file_handle(State),
    {ok, _Count, State2 = #dqstate { sequences = Sequences }} =
        internal_purge(Q, State1), %% remove everything undelivered
    true = ets:delete(Sequences, Q),
    %% now remove everything already delivered
    Objs = mnesia:dirty_match_object(
             rabbit_disk_queue,
             #dq_msg_loc { queue_and_seq_id = {Q, '_'}, _ = '_' }),
    MsgSeqIds =
        lists:map(
          fun (#dq_msg_loc { queue_and_seq_id = {_Q, SeqId},
                             msg_id = MsgId }) ->
                  {MsgId, SeqId} end, Objs),
    remove_messages(Q, MsgSeqIds, true, State2).

internal_delete_non_durable_queues(
  DurableQueues, State = #dqstate { sequences = Sequences }) ->
    ets:foldl(
      fun ({Q, _Read, _Write}, {ok, State1}) ->
              case sets:is_element(Q, DurableQueues) of
                  true -> {ok, State1};
                  false -> internal_delete_queue(Q, State1)
              end
      end, {ok, State}, Sequences).

%%----------------------------------------------------------------------------
%% garbage collection / compaction / aggregation
%%----------------------------------------------------------------------------

maybe_roll_to_new_file(Offset,
                       State = #dqstate { file_size_limit = FileSizeLimit,
                                          current_file_name = CurName,
                                          current_file_handle = CurHdl,
                                          current_file_num = CurNum,
                                          file_summary = FileSummary
                                        }
                      ) when Offset >= FileSizeLimit ->
    State1 = sync_current_file_handle(State),
    ok = file:close(CurHdl),
    NextNum = CurNum + 1,
    NextName = integer_to_list(NextNum) ++ ?FILE_EXTENSION,
    {ok, NextHdl} = open_file(NextName, ?WRITE_MODE),
    true = ets:update_element(FileSummary, CurName, {5, NextName}),%% 5 is Right
    true = ets:insert_new(FileSummary, {NextName, 0, 0, CurName, undefined}),
    State2 = State1 #dqstate { current_file_name = NextName,
                               current_file_handle = NextHdl,
                               current_file_num = NextNum,
                               current_offset = 0,
                               last_sync_offset = 0
                              },
    {ok, compact(sets:from_list([CurName]), State2)};
maybe_roll_to_new_file(_, State) ->
    {ok, State}.

compact(FilesSet, State) ->
    %% smallest number, hence eldest, hence left-most, first
    Files = lists:sort(sets:to_list(FilesSet)),
    %% foldl reverses, so now youngest/right-most first
    RemainingFiles = lists:foldl(fun (File, Acc) ->
                                         delete_empty_files(File, Acc, State)
                                 end, [], Files),
    lists:foldl(fun combine_file/2, State, lists:reverse(RemainingFiles)).

%% At this stage, we simply know that the file has had msgs removed
%% from it. However, we don't know if we need to merge it left (which
%% is what we would prefer), or merge it right. If we merge left, then
%% this file is the source, and the left file is the destination. If
%% we merge right then this file is the destination and the right file
%% is the source.
combine_file(File, State = #dqstate { file_summary = FileSummary,
                                      current_file_name = CurName
                                    }) ->
    %% the file we're looking at may no longer exist as it may have
    %% been deleted within the current GC run
    case ets:lookup(FileSummary, File) of
        [] -> State;
        [FileObj = {File, _ValidData, _ContiguousTop, Left, Right}] ->
            GoRight =
                fun() ->
                        case Right of
                            undefined -> State;
                            _ when not (CurName == Right) ->
                                [RightObj] = ets:lookup(FileSummary, Right),
                                {_, State1} =
                                    adjust_meta_and_combine(FileObj, RightObj,
                                                            State),
                                State1;
                            _ -> State
                        end
                end,
            case Left of
                undefined ->
                    GoRight();
                _ -> [LeftObj] = ets:lookup(FileSummary, Left),
                     case adjust_meta_and_combine(LeftObj, FileObj, State) of
                         {true, State1} -> State1;
                         {false, State} -> GoRight()
                     end
            end
    end.

adjust_meta_and_combine(
  LeftObj = {LeftFile, LeftValidData, _LeftContigTop, LeftLeft, RightFile},
  RightObj = {RightFile, RightValidData, _RightContigTop, LeftFile, RightRight},
  State = #dqstate { file_size_limit = FileSizeLimit,
                     file_summary = FileSummary
                   }) ->
    TotalValidData = LeftValidData + RightValidData,
    if FileSizeLimit >= TotalValidData ->
            State1 = combine_files(RightObj, LeftObj, State),
            %% this could fail if RightRight is undefined
            %% left is the 4th field
            ets:update_element(FileSummary, RightRight, {4, LeftFile}),
            true = ets:insert(FileSummary, {LeftFile,
                                            TotalValidData, TotalValidData,
                                            LeftLeft,
                                            RightRight}),
            true = ets:delete(FileSummary, RightFile),
            {true, State1};
       true -> {false, State}
    end.

sort_msg_locations_by_offset(Dir, List) ->
    Comp = case Dir of
               asc  -> fun erlang:'<'/2;
               desc -> fun erlang:'>'/2
           end,
    lists:sort(fun (#message_store_entry { offset = OffA },
                    #message_store_entry { offset = OffB }) ->
                       Comp(OffA, OffB)
               end, List).

preallocate(Hdl, FileSizeLimit, FinalPos) ->
    {ok, FileSizeLimit} = file:position(Hdl, FileSizeLimit),
    ok = file:truncate(Hdl),
    {ok, FinalPos} = file:position(Hdl, FinalPos),
    ok.

truncate_and_extend_file(FileHdl, Lowpoint, Highpoint) ->
    {ok, Lowpoint} = file:position(FileHdl, Lowpoint),
    ok = file:truncate(FileHdl),
    ok = preallocate(FileHdl, Highpoint, Lowpoint).

combine_files({Source, SourceValid, _SourceContiguousTop,
              _SourceLeft, _SourceRight},
             {Destination, DestinationValid, DestinationContiguousTop,
              _DestinationLeft, _DestinationRight},
             State) ->
    State1 = close_file(Source, close_file(Destination, State)),
    {ok, SourceHdl} = open_file(Source, ?READ_MODE),
    {ok, DestinationHdl} = open_file(Destination, ?READ_MODE ++ ?WRITE_MODE),
    ExpectedSize = SourceValid + DestinationValid,
    %% if DestinationValid =:= DestinationContiguousTop then we don't
    %% need a tmp file
    %% if they're not equal, then we need to write out everything past
    %%   the DestinationContiguousTop to a tmp file then truncate,
    %%   copy back in, and then copy over from Source
    %% otherwise we just truncate straight away and copy over from Source
    if DestinationContiguousTop =:= DestinationValid ->
            ok = truncate_and_extend_file(DestinationHdl,
                                          DestinationValid, ExpectedSize);
       true ->
            Tmp = filename:rootname(Destination) ++ ?FILE_EXTENSION_TMP,
            {ok, TmpHdl} = open_file(Tmp, ?READ_MODE ++ ?WRITE_MODE),
            Worklist =
                lists:dropwhile(
                  fun (#message_store_entry { offset = Offset })
                      when Offset /= DestinationContiguousTop ->
                          %% it cannot be that Offset ==
                          %% DestinationContiguousTop because if it
                          %% was then DestinationContiguousTop would
                          %% have been extended by TotalSize
                          Offset < DestinationContiguousTop
                          %% Given expected access patterns, I suspect
                          %% that the list should be naturally sorted
                          %% as we require, however, we need to
                          %% enforce it anyway
                  end, sort_msg_locations_by_offset(
                         asc, dets_ets_match_object(
                                 State1, #message_store_entry
                                 { file = Destination, _ = '_' }))),
            ok = copy_messages(
                   Worklist, DestinationContiguousTop, DestinationValid,
                   DestinationHdl, TmpHdl, Destination, State1),
            TmpSize = DestinationValid - DestinationContiguousTop,
            %% so now Tmp contains everything we need to salvage from
            %% Destination, and MsgLocationDets has been updated to
            %% reflect compaction of Destination so truncate
            %% Destination and copy from Tmp back to the end
            {ok, 0} = file:position(TmpHdl, 0),
            ok = truncate_and_extend_file(
                   DestinationHdl, DestinationContiguousTop, ExpectedSize),
            {ok, TmpSize} = file:copy(TmpHdl, DestinationHdl, TmpSize),
            %% position in DestinationHdl should now be DestinationValid
            ok = file:sync(DestinationHdl),
            ok = file:close(TmpHdl),
            ok = file:delete(form_filename(Tmp))
    end,
    SourceWorkList =
        sort_msg_locations_by_offset(
          asc, dets_ets_match_object(State1, #message_store_entry
                                      { file = Source, _ = '_' })),
    ok = copy_messages(SourceWorkList, DestinationValid, ExpectedSize,
                       SourceHdl, DestinationHdl, Destination, State1),
    %% tidy up
    ok = file:sync(DestinationHdl),
    ok = file:close(SourceHdl),
    ok = file:close(DestinationHdl),
    ok = file:delete(form_filename(Source)),
    State1.

copy_messages(WorkList, InitOffset, FinalOffset, SourceHdl, DestinationHdl,
              Destination, State) ->
    {FinalOffset, BlockStart1, BlockEnd1} =
        lists:foldl(
          fun (StoreEntry = #message_store_entry { offset = Offset,
                                                   total_size = TotalSize },
               {CurOffset, BlockStart, BlockEnd}) ->
                  %% CurOffset is in the DestinationFile.
                  %% Offset, BlockStart and BlockEnd are in the SourceFile
                  Size = TotalSize + ?FILE_PACKING_ADJUSTMENT,
                  %% update MsgLocationDets to reflect change of file and offset
                  ok = dets_ets_insert(State, StoreEntry #message_store_entry
                                       { file = Destination,
                                         offset = CurOffset }),
                  NextOffset = CurOffset + Size,
                  if BlockStart =:= undefined ->
                          %% base case, called only for the first list elem
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
                              file:position(SourceHdl, BlockStart),
                          {ok, BSize} =
                              file:copy(SourceHdl, DestinationHdl, BSize),
                          {NextOffset, Offset, Offset + Size}
                  end
          end, {InitOffset, undefined, undefined}, WorkList),
    %% do the last remaining block
    BSize1 = BlockEnd1 - BlockStart1,
    {ok, BlockStart1} = file:position(SourceHdl, BlockStart1),
    {ok, BSize1} = file:copy(SourceHdl, DestinationHdl, BSize1),
    ok.

close_file(File, State = #dqstate { read_file_handle_cache = HC }) ->
    HC1 = rabbit_file_handle_cache:close_file(form_filename(File), HC),
    State #dqstate { read_file_handle_cache = HC1 }.

delete_empty_files(File, Acc, #dqstate { file_summary = FileSummary }) ->
    [{File, ValidData, _ContiguousTop, Left, Right}] =
        ets:lookup(FileSummary, File),
    case ValidData of
        %% we should NEVER find the current file in here hence right
        %% should always be a file, not undefined
        0 ->
            case {Left, Right} of
                {undefined, _} when not (is_atom(Right)) ->
                    %% the eldest file is empty. YAY!
                    %% left is the 4th field
                    true =
                        ets:update_element(FileSummary, Right, {4, undefined});
                {_, _} when not (is_atom(Right)) ->
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

%%----------------------------------------------------------------------------
%% recovery
%%----------------------------------------------------------------------------

store_safe_shutdown() ->
    ok = rabbit_misc:execute_mnesia_transaction(
           fun() ->
                   mnesia:write(rabbit_disk_queue,
                                ?SHUTDOWN_MESSAGE, write)
           end).

detect_shutdown_state_and_adjust_delivered_flags() ->
    MarkDelivered =
        rabbit_misc:execute_mnesia_transaction(
          fun() ->
                  case mnesia:read(rabbit_disk_queue,
                                   ?SHUTDOWN_MESSAGE_KEY, read) of
                      [?SHUTDOWN_MESSAGE] ->
                          mnesia:delete(rabbit_disk_queue,
                                        ?SHUTDOWN_MESSAGE_KEY, write),
                          false;
                      [] ->
                          true
                  end
          end),
    %% if we crash here, then on startup we'll not find the
    %% SHUTDOWN_MESSAGE so will mark everything delivered, which is
    %% the safe thing to do.
    case MarkDelivered of
        true -> mark_messages_delivered();
        false -> ok
    end.

mark_messages_delivered() ->
    mark_message_delivered('$start_of_table').

%% A single huge transaction is a bad idea because of memory
%% use. Equally, using dirty operations is a bad idea because you
%% shouldn't do writes when doing mnesia:dirty_next, because the
%% ordering can change. So we use transactions of bounded
%% size. However, even this does necessitate restarting between
%% transactions.
mark_message_delivered('$end_of_table') ->
    ok;
mark_message_delivered(_Key) ->
    mark_message_delivered(
      rabbit_misc:execute_mnesia_transaction(
        fun () ->
                ok = mnesia:write_lock_table(rabbit_disk_queue),
                mark_message_delivered(mnesia:first(rabbit_disk_queue),
                                       ?BATCH_SIZE)
        end)).

mark_message_delivered(Key, 0) ->
    Key;
mark_message_delivered(Key = '$end_of_table', _N) ->
    Key;
mark_message_delivered(Key, N) ->
    [Obj] = mnesia:read(rabbit_disk_queue, Key, write),
    M = case Obj #dq_msg_loc.is_delivered of
             true -> N;
             false ->
                ok = mnesia:write(rabbit_disk_queue,
                                  Obj #dq_msg_loc { is_delivered = true },
                                  write),
                N - 1
        end,
    mark_message_delivered(mnesia:next(rabbit_disk_queue, Key), M).

add_index() ->
    case mnesia:add_table_index(rabbit_disk_queue, msg_id) of
        {atomic, ok} -> ok;
        {aborted,{already_exists,rabbit_disk_queue,_}} -> ok;
        E -> E
    end.

del_index() ->
    case mnesia:del_table_index(rabbit_disk_queue, msg_id) of
        {atomic, ok} -> ok;
        %% hmm, something weird must be going on, but it's probably
        %% not the end of the world
        {aborted, {no_exists, rabbit_disk_queue,_}} -> ok;
        E1 -> E1
    end.

load_from_disk(State) ->
    %% sorted so that smallest number is first. which also means
    %% eldest file (left-most) first
    ok = add_index(),
    {Files, TmpFiles} = get_disk_queue_files(),
    ok = recover_crashed_compactions(Files, TmpFiles),
    %% There should be no more tmp files now, so go ahead and load the
    %% whole lot
    Files1 = case Files of
                 [] -> [State #dqstate.current_file_name];
                 _ -> Files
             end,
    State1 = load_messages(undefined, Files1, State),
    %% Finally, check there is nothing in mnesia which we haven't
    %% loaded
    Key = mnesia:dirty_first(rabbit_disk_queue),
    {ok, AlteredFiles} = prune_mnesia(State1, Key, sets:new(), [], 0),
    State2 = compact(AlteredFiles, State1),
    ok = extract_sequence_numbers(State2 #dqstate.sequences),
    ok = del_index(),
    {ok, State2}.

prune_mnesia_flush_batch(DeleteAcc) ->
    lists:foldl(fun (Key, ok) ->
                        mnesia:dirty_delete(rabbit_disk_queue, Key)
                end, ok, DeleteAcc).

prune_mnesia(_State, '$end_of_table', Files, _DeleteAcc, 0) ->
    {ok, Files};
prune_mnesia(_State, '$end_of_table', Files, DeleteAcc, _Len) ->
    ok = prune_mnesia_flush_batch(DeleteAcc),
    {ok, Files};
prune_mnesia(State, Key, Files, DeleteAcc, Len) ->
    [#dq_msg_loc { msg_id = MsgId, queue_and_seq_id = {Q, SeqId} }] =
        mnesia:dirty_read(rabbit_disk_queue, Key),
    {DeleteAcc1, Files1, Len1} =
        case dets_ets_lookup(State, MsgId) of
            [] ->
                %% msg hasn't been found on disk, delete it
                {[{Q, SeqId} | DeleteAcc], Files, Len + 1};
            [#message_store_entry { msg_id = MsgId, is_persistent = true }] ->
                %% msg is persistent, keep it
                {DeleteAcc, Files, Len};
            [#message_store_entry { msg_id = MsgId, is_persistent = false}] ->
                %% msg is not persistent, delete it
                Files2 = remove_message(MsgId, Files, State),
                {[{Q, SeqId} | DeleteAcc], Files2, Len + 1}
        end,
    {Key1, DeleteAcc2, Len2} =
        if
            Len1 >= ?BATCH_SIZE ->
                %% We have no way of knowing how flushing the batch
                %% will affect ordering of records within the table,
                %% so have no choice but to start again. Although this
                %% will make recovery slower for large queues, we
                %% guarantee we can start up in constant memory
                ok = prune_mnesia_flush_batch(DeleteAcc1),
                Key2 = mnesia:dirty_first(rabbit_disk_queue),
                {Key2, [], 0};
            true ->
                Key2 = mnesia:dirty_next(rabbit_disk_queue, Key),
                {Key2, DeleteAcc1, Len1}
        end,
    prune_mnesia(State, Key1, Files1, DeleteAcc2, Len2).

extract_sequence_numbers(Sequences) ->
    true =
        rabbit_misc:execute_mnesia_transaction(
          %% the ets manipulation within this transaction is
          %% idempotent, in particular we're only reading from mnesia,
          %% and combining what we read with what we find in
          %% ets. Should the transaction restart, the non-rolledback
          %% data in ets can still be successfully combined with what
          %% we find in mnesia
          fun() ->
                  ok = mnesia:read_lock_table(rabbit_disk_queue),
                  mnesia:foldl(
                    fun (#dq_msg_loc { queue_and_seq_id = {Q, SeqId} }, true) ->
                            NextWrite = SeqId + 1,
                            case ets:lookup(Sequences, Q) of
                                [] -> ets:insert_new(Sequences,
                                                     {Q, SeqId, NextWrite});
                                [Orig = {Q, Read, Write}] ->
                                    Repl = {Q, lists:min([Read, SeqId]),
                                            lists:max([Write, NextWrite])},
                                    case Orig == Repl of
                                        true -> true;
                                        false -> ets:insert(Sequences, Repl)
                                    end
                            end
                    end, true, rabbit_disk_queue)
          end),
    ok = remove_gaps_in_sequences(Sequences).

remove_gaps_in_sequences(Sequences) ->
    %% read the comments at internal_requeue.

    %% Because we are at startup, we know that no sequence ids have
    %% been issued (or at least, they were, but have been
    %% forgotten). Therefore, we can nicely shuffle up and not
    %% worry. Note that I'm choosing to shuffle up, but alternatively
    %% we could shuffle downwards. However, I think there's greater
    %% likelihood of gaps being at the bottom rather than the top of
    %% the queue, so shuffling up should be the better bet.
    QueueBoundaries =
        rabbit_misc:execute_mnesia_transaction(
          fun() ->
                  ok = mnesia:write_lock_table(rabbit_disk_queue),
                  lists:foldl(
                    fun ({Q, ReadSeqId, WriteSeqId}, Acc) ->
                            Gap = shuffle_up(Q, ReadSeqId-1, WriteSeqId-1, 0),
                            [{Q, ReadSeqId + Gap, WriteSeqId} | Acc]
                    end, [], ets:match_object(Sequences, '_'))
          end),
    true = lists:foldl(fun (Obj, true) -> ets:insert(Sequences, Obj) end,
                       true, QueueBoundaries),
    ok.

shuffle_up(_Q, SeqId, SeqId, Gap) ->
    Gap;
shuffle_up(Q, BaseSeqId, SeqId, Gap) ->
    GapInc =
        case mnesia:read(rabbit_disk_queue, {Q, SeqId}, write) of
            [] -> 1;
            [Obj] ->
                case Gap of
                    0 -> ok;
                    _ -> mnesia:write(rabbit_disk_queue,
                                      Obj #dq_msg_loc {
                                        queue_and_seq_id = {Q, SeqId + Gap }},
                                      write),
                         mnesia:delete(rabbit_disk_queue, {Q, SeqId}, write)
                end,
                0
        end,
    shuffle_up(Q, BaseSeqId, SeqId - 1, Gap + GapInc).

load_messages(Left, [], State) ->
    Num = list_to_integer(filename:rootname(Left)),
    Offset =
        case dets_ets_match_object(State, #message_store_entry
                                   { file = Left, _ = '_' }) of
            [] -> 0;
            L ->
                [ #message_store_entry {file = Left,
                                        offset = MaxOffset,
                                        total_size = TotalSize} | _ ] =
                    sort_msg_locations_by_offset(desc, L),
                MaxOffset + TotalSize + ?FILE_PACKING_ADJUSTMENT
             end,
    State #dqstate { current_file_num = Num, current_file_name = Left,
                     current_offset = Offset };
load_messages(Left, [File|Files],
              State = #dqstate { file_summary = FileSummary }) ->
    %% [{MsgId, TotalSize, FileOffset}]
    {ok, Messages} = scan_file_for_valid_messages(File),
    {ValidMessages, ValidTotalSize} = lists:foldl(
        fun (Obj = {MsgId, IsPersistent, TotalSize, Offset}, {VMAcc, VTSAcc}) ->
                case length(mnesia:dirty_index_match_object
                            (rabbit_disk_queue,
                             #dq_msg_loc { msg_id = MsgId, _ = '_' },
                             msg_id)) of
                    0 -> {VMAcc, VTSAcc};
                    RefCount ->
                        true = dets_ets_insert_new(
                                 State, #message_store_entry
                                 { msg_id = MsgId, ref_count = RefCount,
                                   file = File, offset = Offset,
                                   total_size = TotalSize,
                                   is_persistent = IsPersistent }),
                        {[Obj | VMAcc],
                         VTSAcc + TotalSize + ?FILE_PACKING_ADJUSTMENT
                        }
                end
        end, {[], 0}, Messages),
    %% foldl reverses lists, find_contiguous_block_prefix needs
    %% msgs eldest first, so, ValidMessages is the right way round
    {ContiguousTop, _} = find_contiguous_block_prefix(ValidMessages),
    Right = case Files of
                [] -> undefined;
                [F|_] -> F
            end,
    true = ets:insert_new(FileSummary,
                          {File, ValidTotalSize, ContiguousTop, Left, Right}),
    load_messages(File, Files, State).

recover_crashed_compactions(Files, TmpFiles) ->
    lists:foreach(fun (TmpFile) ->
                          ok = recover_crashed_compactions1(Files, TmpFile) end,
                  TmpFiles),
    ok.

verify_messages_in_mnesia(MsgIds) ->
    lists:foreach(
      fun (MsgId) ->
              true = 0 < length(mnesia:dirty_index_match_object(
                                  rabbit_disk_queue,
                                  #dq_msg_loc { msg_id = MsgId, _ = '_' },
                                  msg_id))
      end, MsgIds).

grab_msg_id({MsgId, _IsPersistent, _TotalSize, _FileOffset}) ->
    MsgId.

scan_file_for_valid_messages_msg_ids(File) ->
    {ok, Messages} = scan_file_for_valid_messages(File),
    {ok, Messages, lists:map(fun grab_msg_id/1, Messages)}.

recover_crashed_compactions1(Files, TmpFile) ->
    NonTmpRelatedFile = filename:rootname(TmpFile) ++ ?FILE_EXTENSION,
    true = lists:member(NonTmpRelatedFile, Files),
    %% [{MsgId, TotalSize, FileOffset}]
    {ok, UncorruptedMessagesTmp, MsgIdsTmp} =
        scan_file_for_valid_messages_msg_ids(TmpFile),
    %% all of these messages should appear in the mnesia table,
    %% otherwise they wouldn't have been copied out
    verify_messages_in_mnesia(MsgIdsTmp),
    {ok, UncorruptedMessages, MsgIds} =
        scan_file_for_valid_messages_msg_ids(NonTmpRelatedFile),
    %% 1) It's possible that everything in the tmp file is also in the
    %%    main file such that the main file is (prefix ++
    %%    tmpfile). This means that compaction failed immediately
    %%    prior to the final step of deleting the tmp file. Plan: just
    %%    delete the tmp file
    %% 2) It's possible that everything in the tmp file is also in the
    %%    main file but with holes throughout (or just somthing like
    %%    main = (prefix ++ hole ++ tmpfile)). This means that
    %%    compaction wrote out the tmp file successfully and then
    %%    failed. Plan: just delete the tmp file and allow the
    %%    compaction to eventually be triggered later
    %% 3) It's possible that everything in the tmp file is also in the
    %%    main file but such that the main file does not end with tmp
    %%    file (and there are valid messages in the suffix; main =
    %%    (prefix ++ tmpfile[with extra holes?] ++ suffix)). This
    %%    means that compaction failed as we were writing out the tmp
    %%    file. Plan: just delete the tmp file and allow the
    %%    compaction to eventually be triggered later
    %% 4) It's possible that there are messages in the tmp file which
    %%    are not in the main file. This means that writing out the
    %%    tmp file succeeded, but then we failed as we were copying
    %%    them back over to the main file, after truncating the main
    %%    file. As the main file has already been truncated, it should
    %%    consist only of valid messages. Plan: Truncate the main file
    %%    back to before any of the files in the tmp file and copy
    %%    them over again
    TmpPath = form_filename(TmpFile),
    case lists:all(fun (MsgId) -> lists:member(MsgId, MsgIds) end, MsgIdsTmp) of
        true -> %% we're in case 1, 2 or 3 above. Just delete the tmp file
                %% note this also catches the case when the tmp file
                %% is empty
            ok = file:delete(TmpPath);
        false ->
            %% we're in case 4 above. Check that everything in the
            %% main file is a valid message in mnesia
            verify_messages_in_mnesia(MsgIds),
            %% The main file should be contiguous
            {Top, MsgIds} = find_contiguous_block_prefix(
                              lists:reverse(UncorruptedMessages)),
            %% we should have that none of the messages in the prefix
            %% are in the tmp file
            true = lists:all(fun (MsgId) ->
                                     not (lists:member(MsgId, MsgIdsTmp))
                             end, MsgIds),
            %% must open with read flag, otherwise will stomp over contents
            {ok, MainHdl} = open_file(NonTmpRelatedFile, ?WRITE_MODE ++ [read]),
            {ok, Top} = file:position(MainHdl, Top),
            %% wipe out any rubbish at the end of the file
            ok = file:truncate(MainHdl),
            %% there really could be rubbish at the end of the file -
            %% we could have failed after the extending truncate.
            %% Remember the head of the list will be the highest entry
            %% in the file
            [{_, _, TmpTopTotalSize, TmpTopOffset}|_] = UncorruptedMessagesTmp,
            TmpSize = TmpTopOffset + TmpTopTotalSize + ?FILE_PACKING_ADJUSTMENT,
            ExpectedAbsPos = Top + TmpSize,
            {ok, ExpectedAbsPos} = file:position(MainHdl, {cur, TmpSize}),
            %% and now extend the main file as big as necessary in a
            %% single move if we run out of disk space, this truncate
            %% could fail, but we still aren't risking losing data
            ok = file:truncate(MainHdl),
            {ok, TmpHdl} = open_file(TmpFile, ?READ_MODE),
            {ok, TmpSize} = file:copy(TmpHdl, MainHdl, TmpSize),
            ok = file:sync(MainHdl),
            ok = file:close(MainHdl),
            ok = file:close(TmpHdl),
            ok = file:delete(TmpPath),

            {ok, _MainMessages, MsgIdsMain} =
                scan_file_for_valid_messages_msg_ids(NonTmpRelatedFile),
            %% check that everything in MsgIds is in MsgIdsMain
            true = lists:all(fun (MsgId) -> lists:member(MsgId, MsgIdsMain) end,
                             MsgIds),
            %% check that everything in MsgIdsTmp is in MsgIdsMain
            true = lists:all(fun (MsgId) -> lists:member(MsgId, MsgIdsMain) end,
                             MsgIdsTmp)
    end,
    ok.

%% Takes the list in *ascending* order (i.e. eldest message
%% first). This is the opposite of what scan_file_for_valid_messages
%% produces. The list of msgs that is produced is youngest first.
find_contiguous_block_prefix([]) -> {0, []};
find_contiguous_block_prefix(List) ->
    find_contiguous_block_prefix(List, 0, []).

find_contiguous_block_prefix([], ExpectedOffset, MsgIds) ->
    {ExpectedOffset, MsgIds};
find_contiguous_block_prefix([{MsgId, _IsPersistent, TotalSize, ExpectedOffset}
                             | Tail], ExpectedOffset, MsgIds) ->
    ExpectedOffset1 = ExpectedOffset + TotalSize + ?FILE_PACKING_ADJUSTMENT,
    find_contiguous_block_prefix(Tail, ExpectedOffset1, [MsgId | MsgIds]);
find_contiguous_block_prefix([_MsgAfterGap | _Tail], ExpectedOffset, MsgIds) ->
    {ExpectedOffset, MsgIds}.

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

%%----------------------------------------------------------------------------
%% raw reading and writing of files
%%----------------------------------------------------------------------------

append_message(FileHdl, MsgId, MsgBody, IsPersistent) when is_binary(MsgBody) ->
    BodySize = size(MsgBody),
    MsgIdBin = term_to_binary(MsgId),
    MsgIdBinSize = size(MsgIdBin),
    TotalSize = BodySize + MsgIdBinSize,
    StopByte = case IsPersistent of
                   true -> ?WRITE_OK_PERSISTENT;
                   false -> ?WRITE_OK_TRANSIENT
               end,
    case file:write(FileHdl, <<TotalSize:?INTEGER_SIZE_BITS,
                               MsgIdBinSize:?INTEGER_SIZE_BITS,
                               MsgIdBin:MsgIdBinSize/binary,
                               MsgBody:BodySize/binary,
                               StopByte:?WRITE_OK_SIZE_BITS>>) of
        ok -> {ok, TotalSize};
        KO -> KO
    end.

read_message_from_disk(FileHdl, TotalSize) ->
    TotalSizeWriteOkBytes = TotalSize + 1,
    case file:read(FileHdl, TotalSize + ?FILE_PACKING_ADJUSTMENT) of
        {ok, <<TotalSize:?INTEGER_SIZE_BITS,
               MsgIdBinSize:?INTEGER_SIZE_BITS,
               Rest:TotalSizeWriteOkBytes/binary>>} ->
            BodySize = TotalSize - MsgIdBinSize,
            <<_MsgId:MsgIdBinSize/binary, MsgBody:BodySize/binary,
             StopByte:?WRITE_OK_SIZE_BITS>> = Rest,
            Persistent = case StopByte of
                             ?WRITE_OK_TRANSIENT  -> false;
                             ?WRITE_OK_PERSISTENT -> true
                         end,
            {ok, {MsgBody, Persistent, BodySize}};
        KO -> KO
    end.

scan_file_for_valid_messages(File) ->
    case open_file(File, ?READ_MODE) of
        {ok, Hdl} ->
            Valid = scan_file_for_valid_messages(Hdl, 0, []),
            %% if something really bad's happened, the close could fail,
            %% but ignore
            file:close(Hdl),
            Valid;
        {error, enoent} -> {ok, []};
        {error, Reason} -> throw({error, {unable_to_scan_file, File, Reason}})
    end.

scan_file_for_valid_messages(FileHdl, Offset, Acc) ->
    case read_next_file_entry(FileHdl, Offset) of
        eof -> {ok, Acc};
        {corrupted, NextOffset} ->
            scan_file_for_valid_messages(FileHdl, NextOffset, Acc);
        {ok, {MsgId, IsPersistent, TotalSize, NextOffset}} ->
            scan_file_for_valid_messages(
              FileHdl, NextOffset,
              [{MsgId, IsPersistent, TotalSize, Offset} | Acc]);
        _KO ->
            %% bad message, but we may still have recovered some valid messages
            {ok, Acc}
    end.

read_next_file_entry(FileHdl, Offset) ->
    TwoIntegers = 2 * ?INTEGER_SIZE_BYTES,
    case file:read(FileHdl, TwoIntegers) of
        {ok,
         <<TotalSize:?INTEGER_SIZE_BITS, MsgIdBinSize:?INTEGER_SIZE_BITS>>} ->
            case {TotalSize, MsgIdBinSize} of
                {0, _} -> eof; %% Nothing we can do other than stop
                {_, 0} ->
                    %% current message corrupted, try skipping past it
                    ExpectedAbsPos =
                        Offset + ?FILE_PACKING_ADJUSTMENT + TotalSize,
                    case file:position(FileHdl, {cur, TotalSize + 1}) of
                        {ok, ExpectedAbsPos} -> {corrupted, ExpectedAbsPos};
                        {ok, _SomeOtherPos}  -> eof; %% seek failed, so give up
                        KO                   -> KO
                    end;
                {_, _} -> %% all good, let's continue
                    case file:read(FileHdl, MsgIdBinSize) of
                        {ok, <<MsgIdBin:MsgIdBinSize/binary>>} ->
                            ExpectedAbsPos = Offset + ?FILE_PACKING_ADJUSTMENT +
                                TotalSize - 1,
                            case file:position(
                                   FileHdl, {cur, TotalSize - MsgIdBinSize}) of
                                {ok, ExpectedAbsPos} ->
                                    NextOffset = ExpectedAbsPos + 1,
                                    case read_stop_byte(FileHdl) of
                                        {ok, Persistent} ->
                                            MsgId = binary_to_term(MsgIdBin),
                                            {ok, {MsgId, Persistent,
                                                  TotalSize, NextOffset}};
                                        corrupted ->
                                            {corrupted, NextOffset};
                                        KO -> KO
                                    end;
                                {ok, _SomeOtherPos} ->
                                    %% seek failed, so give up
                                    eof;
                                KO -> KO
                            end;
                        Other -> Other
                    end
            end;
        Other -> Other
    end.

read_stop_byte(FileHdl) ->
    case file:read(FileHdl, 1) of
        {ok, <<?WRITE_OK_TRANSIENT:?WRITE_OK_SIZE_BITS>>}  -> {ok, false};
        {ok, <<?WRITE_OK_PERSISTENT:?WRITE_OK_SIZE_BITS>>} -> {ok, true};
        {ok, _SomeOtherData}                               -> corrupted;
        KO                                                 -> KO
    end.
