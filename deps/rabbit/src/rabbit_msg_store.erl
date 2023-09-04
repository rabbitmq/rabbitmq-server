%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_msg_store).

-behaviour(gen_server2).

-export([start_link/5, successfully_recovered_state/1,
         client_init/3, client_terminate/1, client_delete_and_terminate/1,
         client_pre_hibernate/1, client_ref/1,
         write/4, write_flow/4, read/2, read_many/2, contains/2, remove/2]).

-export([compact_file/2, truncate_file/4, delete_file/2]). %% internal

-export([scan_file_for_valid_messages/1]). %% salvage tool

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, prioritise_call/4, prioritise_cast/3,
         prioritise_info/3, format_message_queue/2]).

%%----------------------------------------------------------------------------

-include_lib("rabbit_common/include/rabbit_msg_store.hrl").

%% We flush to disk when the write buffer gets above the max size,
%% or at an interval to make sure we don't keep the data in memory
%% too long. Confirms are sent after the data is flushed to disk.
-define(HANDLE_CACHE_BUFFER_SIZE, 1048576). %% 1MB.
-define(SYNC_INTERVAL,                200). %% Milliseconds.

-define(CLEAN_FILENAME, "clean.dot").
-define(FILE_SUMMARY_FILENAME, "file_summary.ets").

-define(BINARY_MODE,     [raw, binary]).
-define(READ_MODE,       [read]).
-define(WRITE_MODE,      [write]).

-define(FILE_EXTENSION,        ".rdq").
-define(FILE_EXTENSION_TMP,    ".rdt").

%% We keep track of flying messages for writes and removes. The idea is that
%% when a remove comes in before we could process the write, we skip the
%% write and send a publisher confirm immediately. We later skip the remove
%% as well since we didn't process the write.
%%
%% Flying events. They get added as things happen. The entry in the table
%% may get removed after the server write or after the server remove. When
%% the value is removed after a write, when the value is added again it
%% will be as if the value was never removed.
%%
%% So the possible values are only:
%% - 1: client write
%% - 3: client and server write
%% - 7: client and server wrote, client remove before entry could be deleted
%%
%% Values 1 and 7 indicate there is a message in flight: a write or a remove.

-define(FLYING_WRITE, 1).      %% Message in transit for writing.
-define(FLYING_WRITE_DONE, 2). %% Message written in the store.
-define(FLYING_REMOVE, 4).     %% Message removed from the store.
%% Useful states.
-define(FLYING_IS_WRITTEN, ?FLYING_WRITE + ?FLYING_WRITE_DONE). %% Write was handled.
-define(FLYING_IS_IGNORED, ?FLYING_WRITE + ?FLYING_REMOVE).     %% Remove before write was handled.
-define(FLYING_IS_REMOVED, ?FLYING_WRITE + ?FLYING_WRITE_DONE + ?FLYING_REMOVE). %% Remove.

%%----------------------------------------------------------------------------

-record(msstate,
        {
          %% store directory
          dir :: file:filename(),
          %% the module for index ops,
          %% rabbit_msg_store_ets_index by default
          index_module,
          %% where are messages?
          index_state,
          %% current file name as number
          current_file,
          %% current file handle since the last fsync?
          current_file_handle,
          %% file handle cache
          current_file_offset,
          %% TRef for our interval timer
          sync_timer_ref,
          %% files that had removes
          gc_candidates,
          %% timer ref for checking gc_candidates
          gc_check_timer,
          %% pid of our GC
          gc_pid,
          %% tid of the shared file handles table
          file_handles_ets,
          %% tid of the file summary table
          file_summary_ets,
          %% tid of current file cache table
          cur_file_cache_ets,
          %% tid of writes/removes in flight
          flying_ets,
          %% set of dying clients
          dying_clients,
          %% map of references of all registered clients
          %% to callbacks
          clients,
          %% boolean: did we recover state?
          successfully_recovered,
          %% how big are our files allowed to get?
          file_size_limit,
          %% client ref to synced messages mapping
          cref_to_msg_ids,
          %% See CREDIT_DISC_BOUND in rabbit.hrl
          credit_disc_bound
        }).

-record(client_msstate,
        { server,
          client_ref,
          reader,
          index_state,
          index_module,
          dir,
          file_handles_ets,
          cur_file_cache_ets,
          flying_ets,
          credit_disc_bound
        }).

-record(file_summary,
        {file, valid_total_size, unused1, unused2, file_size, locked, unused3}).

-record(gc_state,
        { dir,
          index_module,
          index_state,
          file_summary_ets,
          file_handles_ets,
          msg_store
        }).

-record(dying_client,
        { client_ref,
          file,
          offset
        }).

%%----------------------------------------------------------------------------

-export_type([gc_state/0, file_num/0]).

-type gc_state() :: #gc_state { dir              :: file:filename(),
                                index_module     :: atom(),
                                index_state      :: any(),
                                file_summary_ets :: ets:tid(),
                                file_handles_ets :: ets:tid(),
                                msg_store        :: server()
                              }.

-type server() :: pid() | atom().
-type client_ref() :: binary().
-type file_num() :: non_neg_integer().
-type client_msstate() :: #client_msstate {
                      server             :: server(),
                      client_ref         :: client_ref(),
                      reader             :: undefined | {non_neg_integer(), file:fd()},
                      index_state        :: any(),
                      index_module       :: atom(),
                      %% Stored as binary() as opposed to file:filename() to save memory.
                      dir                :: binary(),
                      file_handles_ets   :: ets:tid(),
                      cur_file_cache_ets :: ets:tid(),
                      flying_ets         :: ets:tid(),
                      credit_disc_bound  :: {pos_integer(), pos_integer()}}.
-type msg_ref_delta_gen(A) ::
        fun ((A) -> 'finished' |
                    {rabbit_types:msg_id(), non_neg_integer(), A}).
-type maybe_msg_id_fun() ::
        'undefined' | fun ((sets:set(), 'written' | 'ignored') -> any()).

%%----------------------------------------------------------------------------

%% Message store is responsible for storing messages
%% on disk and loading them back. The store handles both
%% persistent messages and transient ones.
%% The store is responsible for locating messages
%% on disk and maintaining an index.
%%
%% There are two message stores per vhost: one for transient
%% and one for persistent messages.
%%
%% Queue processes interact with the stores via clients.
%%
%% The components:
%%
%% Index: this is a mapping from MsgId to #msg_location{}.
%%        By default, it's in ETS, but other implementations can
%%        be used.
%% FileSummary: this maps File to #file_summary{} and is stored
%%              in ETS.
%%
%% The basic idea is that messages are appended to the current file up
%% until that file becomes too big (> file_size_limit). At that point,
%% the file is closed and a new file is created. Files are named
%% numerically ascending, thus the file with the lowest name is the
%% eldest file.
%%
%% We need to keep track of which messages are in which files (this is
%% the index) and how much useful data is in each file: this is the
%% purpose of the file summary table.
%%
%% As messages are removed from files, holes appear in these
%% files. The field ValidTotalSize contains the total amount of useful
%% data left in the file. This is needed for garbage collection.
%%
%% When we discover that a file is now empty, we delete it. When we
%% discover that a file has less valid data than removed data (or in
%% other words, the valid data accounts for less than half the total
%% size of the file), we start a garbage collection run concurrently,
%% which will compact the file. This keeps utilisation high.
%%
%% The discovery is done periodically on files that had messages
%% acked and removed. We delibirately do this lazily in order to
%% prevent doing GC on files which are soon to be emptied (and
%% hence deleted).
%%
%% Compaction is a two step process: first the file gets compacted;
%% then it gets truncated.
%%
%% Compaction is done concurrently regardless of there being readers
%% for this file. The compaction process carefully moves the data
%% at the end of the file to holes at the beginning of the file,
%% without overwriting anything. Index information is updated once
%% that data is fully moved and so the message data is always
%% available to readers, including those reading the data from
%% the end of the file.
%%
%% Truncation gets scheduled afterwards and only happens when
%% we know there are no readers using the data from the end of
%% the file. We do this by keeping track of what time readers
%% started operating and comparing it to the time at which
%% truncation was scheduled. It is therefore possible for the
%% file to be truncated when there are readers, but only when
%% those readers are reading from the start of the file.
%%
%% While there is no need to lock the files for compaction or
%% truncation, we still mark the file as soft-locked in the file
%% summary table in order to benefit from optimisations related
%% to fan-out scenarios and transient stores as there the behavior
%% will change depending on whether the file is getting
%% compacted or not.
%%
%% On non-clean startup, we scan the files we discover, dealing with
%% the possibilities of a crash having occurred during a compaction
%% (we keep duplicate messages near the start of the file rather
%% than the end), and rebuild the file summary and index ETS table.
%%
%%
%% Messages are reference-counted. When a message with the same msg id
%% is written several times we only store it once, and only remove it
%% from the store when it has been removed the same number of times.
%%
%% The reference counts do not persist. Therefore the initialisation
%% function must be provided with a generator that produces ref count
%% deltas for all recovered messages. This is only used on startup
%% when the shutdown was non-clean.
%%
%% Reads are always performed directly by clients without calling the
%% server. This is safe because multiple file handles can be used to
%% read files, and the compaction method ensures the data is always
%% available for reading by clients.
%%
%% When a message is removed, its reference count is decremented. Even
%% if the reference count becomes 0, its entry is not removed. This is
%% because in the event of the same message being sent to several
%% different queues, there is the possibility of one queue writing and
%% removing the message before other queues write it at all. Thus
%% accommodating 0-reference counts allows us to avoid unnecessary
%% writes here. Of course, there are complications: the file to which
%% the message has already been written could be locked pending
%% deletion or GC, which means we have to rewrite the message as the
%% original copy will now be lost. (Messages with 0 references get
%% deleted on GC as they're not considered valid data.)
%%
%% The current file to which messages are being written has a
%% write-back cache. This is written to immediately by clients and can
%% be read from by clients too. This means that there are only ever
%% writes made to the current file, thus eliminating delays due to
%% flushing write buffers in order to be able to safely read from the
%% current file. The one exception to this is that on start up, the
%% cache is not populated with msgs found in the current file, and
%% thus in this case only, reads may have to come from the file
%% itself. The effect of this is that even if the msg_store process is
%% heavily overloaded, clients can still write and read messages with
%% very low latency and not block at all.
%%
%% Clients of the msg_store are required to register before using the
%% msg_store. This provides them with the necessary client-side state
%% to allow them to directly access the various caches and files. When
%% they terminate, they should deregister. They can do this by calling
%% either client_terminate/1 or client_delete_and_terminate/1. The
%% differences are: (a) client_terminate is synchronous. As a result,
%% if the msg_store is badly overloaded and has lots of in-flight
%% writes and removes to process, this will take some time to
%% return. However, once it does return, you can be sure that all the
%% actions you've issued to the msg_store have been processed. (b) Not
%% only is client_delete_and_terminate/1 asynchronous, but it also
%% permits writes and subsequent removes from the current
%% (terminating) client which are still in flight to be safely
%% ignored. Thus from the point of view of the msg_store itself, and
%% all from the same client:
%%
%% (T) = termination; (WN) = write of msg N; (RN) = remove of msg N
%% --> W1, W2, W1, R1, T, W3, R2, W2, R1, R2, R3, W4 -->
%%
%% The client obviously sent T after all the other messages (up to
%% W4), but because the msg_store prioritises messages, the T can be
%% promoted and thus received early.
%%
%% Thus at the point of the msg_store receiving T, we have messages 1
%% and 2 with a refcount of 1. After T, W3 will be ignored because
%% it's an unknown message, as will R3, and W4. W2, R1 and R2 won't be
%% ignored because the messages that they refer to were already known
%% to the msg_store prior to T. However, it can be a little more
%% complex: after the first R2, the refcount of msg 2 is 0. At that
%% point, if a GC occurs or file deletion, msg 2 could vanish, which
%% would then mean that the subsequent W2 and R2 are then ignored.
%%
%% The use case then for client_delete_and_terminate/1 is if the
%% client wishes to remove everything it's written to the msg_store:
%% it issues removes for all messages it's written and not removed,
%% and then calls client_delete_and_terminate/1. At that point, any
%% in-flight writes (and subsequent removes) can be ignored, but
%% removes and writes for messages the msg_store already knows about
%% will continue to be processed normally (which will normally just
%% involve modifying the reference count, which is fast). Thus we save
%% disk bandwidth for writes which are going to be immediately removed
%% again by the the terminating client.
%%
%% We use a separate set to keep track of the dying clients in order
%% to keep that set, which is inspected on every write and remove, as
%% small as possible. Inspecting the set of all clients would degrade
%% performance with many healthy clients and few, if any, dying
%% clients, which is the typical case.
%% @todo Honestly if there wasn't gen_server2 we could just selective
%%       receive everything and drop the messages instead of doing
%%       all this.
%%
%% When the msg_store has a backlog (i.e. it has unprocessed messages
%% in its mailbox / gen_server priority queue), a further optimisation
%% opportunity arises: we can eliminate pairs of 'write' and 'remove'
%% from the same client for the same message. A typical occurrence of
%% these is when an empty durable queue delivers persistent messages
%% to ack'ing consumers. The queue will asynchronously ask the
%% msg_store to 'write' such messages, and when they are acknowledged
%% it will issue a 'remove'. That 'remove' may be issued before the
%% msg_store has processed the 'write'. There is then no point going
%% ahead with the processing of that 'write'.
%%
%% To detect this situation a 'flying_ets' table is shared between the
%% clients and the server. The table is keyed on the combination of
%% client (reference) and msg id, and the value represents an
%% integration of all the writes and removes currently "in flight" for
%% that message between the client and server - '+1' means all the
%% writes/removes add up to a single 'write', '-1' to a 'remove', and
%% '0' to nothing. (NB: the integration can never add up to more than
%% one 'write' or 'read' since clients must not write/remove a message
%% more than once without first removing/writing it).
%%
%% Maintaining this table poses two challenges: 1) both the clients
%% and the server access and update the table, which causes
%% concurrency issues, 2) we must ensure that entries do not stay in
%% the table forever, since that would constitute a memory leak. We
%% address the former by carefully modelling all operations as
%% sequences of atomic actions that produce valid results in all
%% possible interleavings. We address the latter by deleting table
%% entries whenever the server finds a 0-valued entry during the
%% processing of a write/remove. 0 is essentially equivalent to "no
%% entry". If, OTOH, the value is non-zero we know there is at least
%% one other 'write' or 'remove' in flight, so we get an opportunity
%% later to delete the table entry when processing these.
%%
%% There are two further complications. We need to ensure that 1)
%% eliminated writes still get confirmed, and 2) the write-back cache
%% doesn't grow unbounded. These are quite straightforward to
%% address. See the comments in the code.
%%
%% For notes on Clean Shutdown and startup, see documentation in
%% rabbit_variable_queue.

%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------

-spec start_link
        (binary(), atom(), file:filename(), [binary()] | 'undefined',
         {msg_ref_delta_gen(A), A}) -> rabbit_types:ok_pid_or_error().

start_link(VHost, Type, Dir, ClientRefs, StartupFunState) when is_atom(Type) ->
    gen_server2:start_link(?MODULE,
                           [VHost, Type, Dir, ClientRefs, StartupFunState],
                           [{timeout, infinity}]).

-spec successfully_recovered_state(server()) -> boolean().

successfully_recovered_state(Server) ->
    gen_server2:call(Server, successfully_recovered_state, infinity).

-spec client_init(server(), client_ref(), maybe_msg_id_fun()) -> client_msstate().

client_init(Server, Ref, MsgOnDiskFun) when is_pid(Server); is_atom(Server) ->
    {IState, IModule, Dir, FileHandlesEts, CurFileCacheEts, FlyingEts} =
        gen_server2:call(
          Server, {new_client_state, Ref, self(), MsgOnDiskFun},
          infinity),
    CreditDiscBound = rabbit_misc:get_env(rabbit, msg_store_credit_disc_bound,
                                          ?CREDIT_DISC_BOUND),
    #client_msstate { server             = Server,
                      client_ref         = Ref,
                      reader             = undefined,
                      index_state        = IState,
                      index_module       = IModule,
                      dir                = rabbit_file:filename_to_binary(Dir),
                      file_handles_ets   = FileHandlesEts,
                      cur_file_cache_ets = CurFileCacheEts,
                      flying_ets         = FlyingEts,
                      credit_disc_bound  = CreditDiscBound }.

-spec client_terminate(client_msstate()) -> 'ok'.

client_terminate(CState = #client_msstate { client_ref = Ref, reader = Reader }) ->
    ok = server_call(CState, {client_terminate, Ref}),
    reader_close(Reader).

-spec client_delete_and_terminate(client_msstate()) -> 'ok'.

client_delete_and_terminate(CState = #client_msstate { client_ref = Ref, reader = Reader }) ->
    ok = server_cast(CState, {client_dying, Ref}),
    ok = server_cast(CState, {client_delete, Ref}),
    reader_close(Reader).

-spec client_pre_hibernate(client_msstate()) -> client_msstate().

client_pre_hibernate(CState = #client_msstate{ reader = Reader }) ->
    reader_close(Reader),
    CState#client_msstate{ reader = undefined }.

-spec client_ref(client_msstate()) -> client_ref().

client_ref(#client_msstate { client_ref = Ref }) -> Ref.

-spec write_flow(any(), rabbit_types:msg_id(), msg(), client_msstate()) -> 'ok'.

write_flow(MsgRef, MsgId, Msg,
           CState = #client_msstate {
                       server = Server,
                       credit_disc_bound = CreditDiscBound }) ->
    %% Here we are tracking messages sent by the
    %% rabbit_amqqueue_process process via the
    %% rabbit_variable_queue. We are accessing the
    %% rabbit_amqqueue_process process dictionary.
    credit_flow:send(Server, CreditDiscBound),
    client_write(MsgRef, MsgId, Msg, flow, CState).

-spec write(any(), rabbit_types:msg_id(), msg(), client_msstate()) -> 'ok'.

%% This function is only used by tests.
write(MsgRef, MsgId, Msg, CState) -> client_write(MsgRef, MsgId, Msg, noflow, CState).

-spec read(rabbit_types:msg_id(), client_msstate()) ->
                     {rabbit_types:ok(msg()) | 'not_found', client_msstate()}.

read(MsgId,
     CState = #client_msstate { cur_file_cache_ets = CurFileCacheEts }) ->
    file_handle_cache_stats:update(msg_store_read),
    %% Check the cur file cache
    case ets:lookup(CurFileCacheEts, MsgId) of
        [] ->
            %% @todo It's probably a bug if we don't get a positive ref count.
            case index_lookup_positive_ref_count(MsgId, CState) of
                not_found   -> {not_found, CState};
                MsgLocation -> client_read3(MsgLocation, CState)
            end;
        [{MsgId, Msg, _CacheRefCount}] ->
            {{ok, Msg}, CState}
    end.

-spec read_many([rabbit_types:msg_id()], client_msstate())
    -> {#{rabbit_types:msg_id() => msg()}, client_msstate()}.

%% We disable read_many when the index module is not ETS for the time being.
%% We can introduce the new index module callback as a breaking change in 4.0.
read_many(_, CState = #client_msstate{ index_module = IndexMod })
        when IndexMod =/= rabbit_msg_store_ets_index ->
    {#{}, CState};
read_many(MsgIds, CState) ->
    file_handle_cache_stats:inc(msg_store_read, length(MsgIds)),
    %% We receive MsgIds in rouhgly the younger->older order so
    %% we can look for messages in the cache directly.
    read_many_cache(MsgIds, CState, #{}).

%% We read from the cache until we can't. Then we read from disk.
read_many_cache([MsgId|Tail], CState = #client_msstate{ cur_file_cache_ets = CurFileCacheEts }, Acc) ->
    case ets:lookup(CurFileCacheEts, MsgId) of
        [] ->
            read_many_disk([MsgId|Tail], CState, Acc);
        [{MsgId, Msg, _CacheRefCount}] ->
            read_many_cache(Tail, CState, Acc#{MsgId => Msg})
    end;
read_many_cache([], CState, Acc) ->
    {Acc, CState}.

%% We will read from disk one file at a time in no particular order.
read_many_disk([MsgId|Tail], CState, Acc) ->
    case index_lookup_positive_ref_count(MsgId, CState) of
        %% We ignore this message if it's not found and will try
        %% to read it individually later instead. We can't call
        %% the message store and expect good performance here.
        %% @todo Can this even happen? Isn't this a bug?
        not_found   -> read_many_disk(Tail, CState, Acc);
        MsgLocation -> read_many_file2([MsgId|Tail], CState, Acc, MsgLocation#msg_location.file)
    end;
read_many_disk([], CState, Acc) ->
    {Acc, CState}.

read_many_file2(MsgIds0, CState = #client_msstate{ dir              = Dir,
                                                   file_handles_ets = FileHandlesEts,
                                                   reader           = Reader0,
                                                   client_ref       = Ref }, Acc0, File) ->
    %% Mark file handle open.
    mark_handle_open(FileHandlesEts, File, Ref),
    %% Get index for all Msgids in File.
    %% It's possible that we get no results here if compaction
    %% was in progress. That's OK: we will try again with those
    %% MsgIds to get them from the new file.
    MsgLocations0 = index_select_from_file(MsgIds0, File, CState),
    case MsgLocations0 of
        [] ->
            read_many_file3(MsgIds0, CState, Acc0, File);
        _ ->
            %% At this point either the file is guaranteed to remain
            %% for us to read from it or we got zero locations.
            %%
            %% First we must order the locations so that we can consolidate
            %% the preads and improve read performance.
            MsgLocations = lists:keysort(#msg_location.offset, MsgLocations0),
            %% Then we can do the consolidation to get the pread LocNums.
            LocNums = consolidate_reads(MsgLocations, []),
            %% Read the data from the file.
            Reader = reader_open(Reader0, Dir, File),
            {ok, Msgs} = reader_pread(Reader, LocNums),
            %% Before we continue the read_many calls we must remove the
            %% MsgIds we have read from the list and add the messages to
            %% the Acc.
            {Acc, MsgIdsRead} = lists:foldl(
                                  fun(Msg, {Acc1, MsgIdsAcc}) ->
                                          MsgIdRead = mc:get_annotation(id, Msg),
                                          {Acc1#{MsgIdRead => Msg}, [MsgIdRead|MsgIdsAcc]}
                                  end, {Acc0, []}, Msgs),
            MsgIds = MsgIds0 -- MsgIdsRead,
            %% Unmark opened files and continue.
            read_many_file3(MsgIds, CState#client_msstate{ reader = Reader }, Acc, File)
    end.

index_select_from_file(MsgIds, File, #client_msstate { index_module = Index,
                                                       index_state  = State }) ->
    Index:select_from_file(MsgIds, File, State).

consolidate_reads([#msg_location{offset=NextOffset, total_size=NextSize}|Locs], [{Offset, Size}|Acc])
        when Offset + Size =:= NextOffset ->
    consolidate_reads(Locs, [{Offset, Size + NextSize}|Acc]);
consolidate_reads([#msg_location{offset=NextOffset, total_size=NextSize}|Locs], Acc) ->
    consolidate_reads(Locs, [{NextOffset, NextSize}|Acc]);
consolidate_reads([], Acc) ->
    lists:reverse(Acc).

%% Cleanup opened files and continue.
read_many_file3(MsgIds, CState = #client_msstate{ file_handles_ets = FileHandlesEts,
                                                  client_ref       = Ref }, Acc, File) ->
    mark_handle_closed(FileHandlesEts, File, Ref),
    read_many_disk(MsgIds, CState, Acc).

-spec contains(rabbit_types:msg_id(), client_msstate()) -> boolean().

contains(MsgId, CState) -> server_call(CState, {contains, MsgId}).

-spec remove([rabbit_types:msg_id()], client_msstate()) -> {'ok', [rabbit_types:msg_id()]}.

remove([],    _CState) -> ok;
remove(MsgIds, CState = #client_msstate { flying_ets = FlyingEts,
                                          client_ref = CRef }) ->
    %% If the entry was deleted we act as if it wasn't by using the right default.
    Res = [{MsgId, ets:update_counter(FlyingEts, {CRef, MsgRef}, ?FLYING_REMOVE, {'', ?FLYING_IS_WRITTEN})}
        || {MsgRef, MsgId} <- MsgIds],
    server_cast(CState, {remove, CRef, MsgIds}),
    {ok, [MsgId || {MsgId, ?FLYING_IS_IGNORED} <- Res]}.

%%----------------------------------------------------------------------------
%% Client-side-only helpers
%%----------------------------------------------------------------------------

server_call(#client_msstate { server = Server }, Msg) ->
    gen_server2:call(Server, Msg, infinity).

server_cast(#client_msstate { server = Server }, Msg) ->
    gen_server2:cast(Server, Msg).

client_write(MsgRef, MsgId, Msg, Flow,
             CState = #client_msstate { flying_ets         = FlyingEts,
                                        cur_file_cache_ets = CurFileCacheEts,
                                        client_ref         = CRef }) ->
    file_handle_cache_stats:update(msg_store_write),
    %% We are guaranteed that the insert will succeed.
    %% This is true even for queue crashes because CRef will change.
    true = ets:insert_new(FlyingEts, {{CRef, MsgRef}, ?FLYING_WRITE}),
    ok = update_msg_cache(CurFileCacheEts, MsgId, Msg),
    ok = server_cast(CState, {write, CRef, MsgRef, MsgId, Flow}).

%% We no longer check for whether the message's file is locked because we
%% rely on the fact that the file gets removed only when there are no
%% readers. So we mark the file as being opened and then check that
%% the message data is within this file and if that's the case then
%% we are guaranteed able to read from it until we release the file
%% handle. This is because the compaction has been reworked to copy
%% the data before changing the index information, therefore if the
%% index information points to the file we are expecting we are good.
%% And the file only gets deleted after all data was copied, index
%% was updated and file handles got closed.
client_read3(#msg_location { msg_id = MsgId, file = File },
             CState = #client_msstate { file_handles_ets = FileHandlesEts,
                                        client_ref       = Ref }) ->
    %% We immediately mark the handle open so that we don't get the
    %% file truncated while we are reading from it. The file may still
    %% be truncated past that point but that's OK because we do a second
    %% index lookup to ensure that we get the updated message location.
    mark_handle_open(FileHandlesEts, File, Ref),
    case index_lookup(MsgId, CState) of
        #msg_location { file = File, ref_count = RefCount } = MsgLocation when RefCount > 0 ->
            {Msg, CState1} = read_from_disk(MsgLocation, CState),
            mark_handle_closed(FileHandlesEts, File, Ref),
            {{ok, Msg}, CState1}
    end.

read_from_disk(#msg_location { msg_id = MsgId, file = File, offset = Offset,
                               total_size = TotalSize }, State = #client_msstate{ reader = Reader0, dir = Dir }) ->
    Reader = reader_open(Reader0, Dir, File),
    Msg = case reader_pread(Reader, [{Offset, TotalSize}]) of
        {ok, [Msg0]} ->
            Msg0;
        Other ->
            {error, {misread, [{old_state, State},
                               {file_num,  File},
                               {offset,    Offset},
                               {msg_id,    MsgId},
                               {read,      Other},
                               {proc_dict, get()}
                              ]}}
    end,
    {Msg, State#client_msstate{ reader = Reader }}.

%%----------------------------------------------------------------------------
%% Reader functions. A reader is a file num + fd tuple.
%%----------------------------------------------------------------------------

%% The reader tries to keep the FD open for subsequent reads.
%% When we attempt to read we first check if the opened file
%% is the one we want. If not we close/open the right one.
%%
%% The FD will be closed when the queue hibernates to save
%% resources.
reader_open(Reader, Dir, File) ->
    {ok, Fd} = case Reader of
        {File, Fd0} ->
            {ok, Fd0};
        {_AnotherFile, Fd0} ->
            ok = file:close(Fd0),
            file:open(form_filename(Dir, filenum_to_name(File)),
                      [binary, read, raw]);
        undefined ->
            file:open(form_filename(Dir, filenum_to_name(File)),
                      [binary, read, raw])
    end,
    {File, Fd}.

reader_pread({_, Fd}, LocNums) ->
    case file:pread(Fd, LocNums) of
        {ok, DataL} -> {ok, reader_pread_parse(DataL)};
        KO -> KO
    end.

reader_pread_parse([<<Size:64,
                      _MsgId:16/binary,
                      Rest0/bits>>|Tail]) ->
    BodyBinSize = Size - 16, %% Remove size of MsgId.
    <<MsgBodyBin:BodyBinSize/binary,
      255, %% OK marker.
      Rest/bits>> = Rest0,
    [binary_to_term(MsgBodyBin)|reader_pread_parse([Rest|Tail])];
reader_pread_parse([<<>>]) ->
    [];
reader_pread_parse([<<>>|Tail]) ->
    reader_pread_parse(Tail).

reader_close(Reader) ->
    case Reader of
        undefined -> ok;
        {_File, Fd} -> ok = file:close(Fd)
    end.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------


init([VHost, Type, BaseDir, ClientRefs, StartupFunState]) ->
    process_flag(trap_exit, true),
    pg:join({?MODULE, VHost, Type}, self()),

    Dir = filename:join(BaseDir, atom_to_list(Type)),
    Name = filename:join(filename:basename(BaseDir), atom_to_list(Type)),

    {ok, IndexModule} = application:get_env(rabbit, msg_store_index_module),
    rabbit_log:info("Message store ~tp: using ~tp to provide index", [Name, IndexModule]),

    AttemptFileSummaryRecovery =
        case ClientRefs of
            %% Transient.
            undefined -> ok = rabbit_file:recursive_delete([Dir]),
                         ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
                         false;
            %% Persistent.
            _         -> ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
                         true
        end,
    %% Always attempt to recover the file summary for persistent store.
    %% If the shutdown isn't clean we will delete all objects before
    %% we start recovering messages from the files on disk.
    {FileSummaryRecovered, FileSummaryEts} =
        recover_file_summary(AttemptFileSummaryRecovery, Dir),
    {CleanShutdown, IndexState, ClientRefs1} =
        recover_index_and_client_refs(IndexModule, FileSummaryRecovered,
                                      ClientRefs, Dir, Name),
    Clients = maps:from_list(
                [{CRef, {undefined, undefined}} ||
                    CRef <- ClientRefs1]),
    %% CleanShutdown => msg location index and file_summary both
    %% recovered correctly.
    true = case {FileSummaryRecovered, CleanShutdown} of
               {true, false} -> ets:delete_all_objects(FileSummaryEts);
               _             -> true
           end,
    %% CleanShutdown <=> msg location index and file_summary both
    %% recovered correctly.

    FileHandlesEts  = ets:new(rabbit_msg_store_shared_file_handles,
                              [ordered_set, public]),
    CurFileCacheEts = ets:new(rabbit_msg_store_cur_file, [set, public,
                              {read_concurrency, true},
                              {write_concurrency, true}]),
    FlyingEts       = ets:new(rabbit_msg_store_flying, [set, public,
                              {read_concurrency, true},
                              {write_concurrency, true}]),

    {ok, FileSizeLimit} = application:get_env(rabbit, msg_store_file_size_limit),

    {ok, GCPid} = rabbit_msg_store_gc:start_link(
                    #gc_state { dir              = Dir,
                                index_module     = IndexModule,
                                index_state      = IndexState,
                                file_summary_ets = FileSummaryEts,
                                file_handles_ets = FileHandlesEts,
                                msg_store        = self()
                              }),

    CreditDiscBound = rabbit_misc:get_env(rabbit, msg_store_credit_disc_bound,
                                          ?CREDIT_DISC_BOUND),

    State = #msstate { dir                    = Dir,
                       index_module           = IndexModule,
                       index_state            = IndexState,
                       current_file           = 0,
                       current_file_handle    = undefined,
                       current_file_offset    = 0,
                       sync_timer_ref         = undefined,
                       gc_candidates          = #{},
                       gc_check_timer         = undefined,
                       gc_pid                 = GCPid,
                       file_handles_ets       = FileHandlesEts,
                       file_summary_ets       = FileSummaryEts,
                       cur_file_cache_ets     = CurFileCacheEts,
                       flying_ets             = FlyingEts,
                       dying_clients          = #{},
                       clients                = Clients,
                       successfully_recovered = CleanShutdown,
                       file_size_limit        = FileSizeLimit,
                       cref_to_msg_ids        = #{},
                       credit_disc_bound      = CreditDiscBound
                     },
    %% If we didn't recover the msg location index then we need to
    %% rebuild it now.
    Cleanliness = case CleanShutdown of
                      true -> "clean";
                      false -> "unclean"
                  end,
    rabbit_log:debug("Rebuilding message location index after ~ts shutdown...",
                     [Cleanliness]),
    {CurOffset, State1 = #msstate { current_file = CurFile }} =
        build_index(CleanShutdown, StartupFunState, State),
    rabbit_log:debug("Finished rebuilding index", []),
    %% Open the most recent file.
    {ok, CurHdl} = writer_recover(Dir, CurFile, CurOffset),
    {ok, State1 #msstate { current_file_handle = CurHdl,
                           current_file_offset = CurOffset },
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of
        successfully_recovered_state                        -> 7;
        {new_client_state, _Ref, _Pid, _MODC}               -> 7;
        _                                                   -> 0
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {compacted_file, _File}       -> 8;
        {set_maximum_since_use, _Age} -> 8;
        {client_dying, _Pid}          -> 7;
        _                             -> 0
    end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of
        sync                                               -> 8;
        _                                                  -> 0
    end.

handle_call(successfully_recovered_state, _From, State) ->
    reply(State #msstate.successfully_recovered, State);

handle_call({new_client_state, CRef, CPid, MsgOnDiskFun}, _From,
            State = #msstate { dir                = Dir,
                               index_state        = IndexState,
                               index_module       = IndexModule,
                               file_handles_ets   = FileHandlesEts,
                               cur_file_cache_ets = CurFileCacheEts,
                               flying_ets         = FlyingEts,
                               clients            = Clients }) ->
    Clients1 = maps:put(CRef, {CPid, MsgOnDiskFun}, Clients),
    erlang:monitor(process, CPid),
    reply({IndexState, IndexModule, Dir, FileHandlesEts,
           CurFileCacheEts, FlyingEts},
          State #msstate { clients = Clients1 });

handle_call({client_terminate, CRef}, _From, State) ->
    reply(ok, clear_client(CRef, State));

handle_call({contains, MsgId}, From, State) ->
    State1 = contains_message(MsgId, From, State),
    noreply(State1).

handle_cast({client_dying, CRef},
            State = #msstate { dying_clients       = DyingClients,
                               current_file        = CurFile,
                               current_file_offset = CurOffset }) ->
    DyingClients1 = maps:put(CRef,
                             #dying_client{client_ref = CRef,
                                           file = CurFile,
                                           offset = CurOffset},
                             DyingClients),
    noreply(State #msstate { dying_clients = DyingClients1 });

handle_cast({client_delete, CRef},
            State = #msstate { clients = Clients }) ->
    State1 = State #msstate { clients = maps:remove(CRef, Clients) },
    noreply(clear_client(CRef, State1));

handle_cast({write, CRef, MsgRef, MsgId, Flow},
            State = #msstate { cur_file_cache_ets = CurFileCacheEts,
                               clients            = Clients,
                               credit_disc_bound  = CreditDiscBound }) ->
    case Flow of
        flow   -> {CPid, _} = maps:get(CRef, Clients),
                  %% We are going to process a message sent by the
                  %% rabbit_amqqueue_process. Now we are accessing the
                  %% msg_store process dictionary.
                  credit_flow:ack(CPid, CreditDiscBound);
        noflow -> ok
    end,
    true = 0 =< ets:update_counter(CurFileCacheEts, MsgId, {3, -1}),
    case flying_write({CRef, MsgRef}, State) of
        process ->
            [{MsgId, Msg, _PWC}] = ets:lookup(CurFileCacheEts, MsgId),
            noreply(write_message(MsgId, Msg, CRef, State));
        ignore ->
            %% A 'remove' has already been issued and eliminated the
            %% 'write'.
            %%
            %% If all writes get eliminated, cur_file_cache_ets could
            %% grow unbounded. To prevent that we delete the cache
            %% entry here, but only if the message isn't in the
            %% current file. That way reads of the message can
            %% continue to be done client side, from either the cache
            %% or the non-current files. If the message *is* in the
            %% current file then the cache entry will be removed by
            %% the normal logic for that in write_message/4 and
            %% maybe_roll_to_new_file/2.
            case index_lookup(MsgId, State) of
                [#msg_location { file = File }]
                  when File == State #msstate.current_file ->
                    ok;
                _ ->
                    true = ets:match_delete(CurFileCacheEts, {MsgId, '_', 0})
            end,
            noreply(State)
    end;

handle_cast({remove, CRef, MsgIds}, State) ->
    State1 =
        lists:foldl(
          fun ({MsgRef, MsgId}, State2) ->
                  case flying_remove({CRef, MsgRef}, State2) of
                      process -> remove_message(MsgId, CRef, State2);
                      ignore  -> State2
                  end
          end, State, MsgIds),
    noreply(State1);

handle_cast({compacted_file, File},
            State = #msstate { file_summary_ets = FileSummaryEts }) ->
    %% This can return false if the file gets deleted immediately
    %% after compaction ends, but before we can process this message.
    %% So this will become a no-op and we can ignore the return value.
    _ = ets:update_element(FileSummaryEts, File,
                           {#file_summary.locked, false}),
    noreply(State).

handle_info(sync, State) ->
    noreply(internal_sync(State));

handle_info(timeout, State) ->
    noreply(internal_sync(State));

handle_info({timeout, TimerRef, {maybe_gc, Candidates}}, State = #msstate{ gc_check_timer = TimerRef }) ->
    noreply(maybe_gc(Candidates, State));

%% @todo When a CQ crashes the message store does not remove
%%       the client information and clean up. This eventually
%%       leads to the queue running a full recovery on the next
%%       message store restart because the store will keep the
%%       crashed queue's ref in its persistent state and fail
%%       to find the corresponding ref during start.
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    %% similar to what happens in
    %% rabbit_amqqueue_process:handle_ch_down but with a relation of
    %% msg_store -> rabbit_amqqueue_process instead of
    %% rabbit_amqqueue_process -> rabbit_channel.
    credit_flow:peer_down(Pid),
    noreply(State);

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State}.

terminate(Reason, State = #msstate { index_state         = IndexState,
                                     index_module        = IndexModule,
                                     current_file_handle = CurHdl,
                                     gc_pid              = GCPid,
                                     file_handles_ets    = FileHandlesEts,
                                     file_summary_ets    = FileSummaryEts,
                                     cur_file_cache_ets  = CurFileCacheEts,
                                     flying_ets          = FlyingEts,
                                     clients             = Clients,
                                     dir                 = Dir }) ->
    {ExtraLog, ExtraLogArgs} = case Reason of
        normal -> {"", []};
        shutdown -> {"", []};
        {shutdown, _} -> {"", []};
        _ -> {" with reason ~0p", [Reason]}
    end,
    rabbit_log:info("Stopping message store for directory '~ts'" ++ ExtraLog, [Dir|ExtraLogArgs]),
    %% stop the gc first, otherwise it could be working and we pull
    %% out the ets tables from under it.
    ok = rabbit_msg_store_gc:stop(GCPid),
    State3 = case CurHdl of
                 undefined -> State;
                 _         -> State2 = internal_sync(State),
                              ok = writer_close(CurHdl),
                              State2
             end,
    case store_file_summary(FileSummaryEts, Dir) of
        ok           -> ok;
        {error, FSErr} ->
            rabbit_log:error("Unable to store file summary"
                             " for vhost message store for directory ~tp~n"
                             "Error: ~tp",
                             [Dir, FSErr])
    end,
    [true = ets:delete(T) || T <- [FileSummaryEts, FileHandlesEts,
                                   CurFileCacheEts, FlyingEts]],
    IndexModule:terminate(IndexState),
    case store_recovery_terms([{client_refs, maps:keys(Clients)},
                               {index_module, IndexModule}], Dir) of
        ok           ->
            rabbit_log:info("Message store for directory '~ts' is stopped", [Dir]),
            ok;
        {error, RTErr} ->
            rabbit_log:error("Unable to save message store recovery terms"
                             " for directory ~tp~nError: ~tp",
                             [Dir, RTErr])
    end,
    State3 #msstate { index_state         = undefined,
                      current_file_handle = undefined,
                      current_file_offset = 0 }.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%----------------------------------------------------------------------------
%% general helper functions
%%----------------------------------------------------------------------------

clear_client(CRef, State = #msstate { cref_to_msg_ids = CTM,
                                      dying_clients = DyingClients }) ->
    State #msstate { cref_to_msg_ids = maps:remove(CRef, CTM),
                     dying_clients = maps:remove(CRef, DyingClients) }.

noreply(State) ->
    {State1, Timeout} = next_state(State),
    {noreply, State1, Timeout}.

reply(Reply, State) ->
    {State1, Timeout} = next_state(State),
    {reply, Reply, State1, Timeout}.

next_state(State = #msstate { sync_timer_ref  = undefined,
                              cref_to_msg_ids = CTM }) ->
    case maps:size(CTM) of
        0 -> {State, hibernate};
        _ -> {start_sync_timer(State), 0}
    end;
next_state(State = #msstate { cref_to_msg_ids = CTM }) ->
    case maps:size(CTM) of
        0 -> {stop_sync_timer(State), hibernate};
        _ -> {State, 0}
    end.

start_sync_timer(State) ->
    rabbit_misc:ensure_timer(State, #msstate.sync_timer_ref,
                             ?SYNC_INTERVAL, sync).

stop_sync_timer(State) ->
    rabbit_misc:stop_timer(State, #msstate.sync_timer_ref).

internal_sync(State = #msstate { current_file_handle = CurHdl,
                                 clients             = Clients,
                                 cref_to_msg_ids     = CTM }) ->
    State1 = stop_sync_timer(State),
    ok = writer_flush(CurHdl),
    %% We confirm all pending messages because we know they are
    %% either on disk when we flush the current write file; or
    %% were removed by the queue already.
    maps:foreach(fun (CRef, MsgIds) ->
        case maps:get(CRef, Clients) of
            {_CPid, undefined   } -> ok;
            {_CPid, MsgOnDiskFun} -> MsgOnDiskFun(MsgIds, written)
        end
    end, CTM),
    State1#msstate{cref_to_msg_ids = #{}}.

flying_write(Key, #msstate { flying_ets = FlyingEts }) ->
    case ets:lookup(FlyingEts, Key) of
        [{_, ?FLYING_WRITE}] ->
            ets:update_counter(FlyingEts, Key, ?FLYING_WRITE_DONE),
            %% We only remove the object if it hasn't changed
            %% (a remove may be sent while we were processing the write).
            true = ets:delete_object(FlyingEts, {Key, ?FLYING_IS_WRITTEN}),
            process;
        [{_, ?FLYING_IS_IGNORED}] ->
            ignore
    end.

flying_remove(Key, #msstate { flying_ets = FlyingEts }) ->
    Res = case ets:lookup(FlyingEts, Key) of
        [{_, ?FLYING_IS_REMOVED}] ->
            process;
        [{_, ?FLYING_IS_IGNORED}] ->
            ignore
    end,
    %% We are done with this message, we can unconditionally delete the entry.
    true = ets:delete(FlyingEts, Key),
    Res.

%% The general idea is that when a client of a transient message store is dying,
%% we want to avoid writing messages that would end up being removed immediately
%% after.
write_action({true, not_found}, _MsgId, State) ->
    {ignore, undefined, State};
write_action({true, #msg_location { file = File }}, _MsgId, State) ->
    {ignore, File, State};
write_action({false, not_found}, _MsgId, State) ->
    {write, State};
%% @todo This clause is probably fairly costly when large fan-out is used
%%       with both producers and consumers more or less catched up.
%%       We probably should only increment the refcount for the current file?
%%       We know it's not locked, it has a valid total size, etc.
%%       We have to look up the index anyway so we know the file
%%       and can decide without querying file summary.
write_action({Mask, #msg_location { ref_count = 0, file = File,
                                    total_size = TotalSize }},
             MsgId, State = #msstate { current_file = CurrentFile,
                                       file_summary_ets = FileSummaryEts }) ->
    case {Mask, ets:lookup(FileSummaryEts, File)} of
        %% Never increase the ref_count for a file that is about to get deleted.
        {_, [#file_summary{valid_total_size = 0}]} when File =/= CurrentFile ->
            ok = index_delete(MsgId, State),
            {write, State};
        {false, [#file_summary { locked = true }]} ->
            ok = index_delete(MsgId, State),
            {write, State};
        {false_if_increment, [#file_summary { locked = true }]} ->
            %% The msg for MsgId is older than the client death
            %% message, but as it is being GC'd currently we'll have
            %% to write a new copy, which will then be younger, so
            %% ignore this write.
            {ignore, File, State};
        {_Mask, [#file_summary {}]} ->
            ok = index_update_ref_count(MsgId, 1, State),
            State1 = adjust_valid_total_size(File, TotalSize, State),
            {confirm, File, State1}
    end;
write_action({_Mask, #msg_location { ref_count = RefCount, file = File }},
             MsgId, State) ->
    ok = index_update_ref_count(MsgId, RefCount + 1, State),
    %% We already know about it, just update counter. Only update
    %% field otherwise bad interaction with concurrent GC
    {confirm, File, State}.

write_message(MsgId, Msg, CRef,
              State = #msstate { cur_file_cache_ets = CurFileCacheEts }) ->
    case write_action(should_mask_action(CRef, MsgId, State), MsgId, State) of
        {write, State1} ->
            write_message(MsgId, Msg,
                          record_pending_confirm(CRef, MsgId, State1));
        {ignore, CurFile, State1 = #msstate { current_file = CurFile }} ->
            State1;
        {ignore, _File, State1} ->
            true = ets:delete_object(CurFileCacheEts, {MsgId, Msg, 0}),
            State1;
        {confirm, CurFile, State1 = #msstate { current_file = CurFile }}->
            record_pending_confirm(CRef, MsgId, State1);
        {confirm, _File, State1} ->
            true = ets:delete_object(CurFileCacheEts, {MsgId, Msg, 0}),
            update_pending_confirms(
              fun (MsgOnDiskFun, CTM) ->
                      MsgOnDiskFun(sets:add_element(MsgId, sets:new([{version,2}])), written),
                      CTM
              end, CRef, State1)
    end.

remove_message(MsgId, CRef, State) ->
    case should_mask_action(CRef, MsgId, State) of
        {true, _Location} ->
            State;
        {false_if_increment, #msg_location { ref_count = 0 }} ->
            %% CRef has tried to both write and remove this msg whilst
            %% it's being GC'd.
            %%
            %% ASSERTION: [#file_summary { locked = true }] =
            %% ets:lookup(FileSummaryEts, File),
            State;
        {_Mask, #msg_location { ref_count = RefCount, file = File,
                                total_size = TotalSize }}
          when RefCount > 0 ->
            %% only update field, otherwise bad interaction with
            %% concurrent GC
            Dec = fun () -> index_update_ref_count(
                              MsgId, RefCount - 1, State) end,
            case RefCount of
                %% don't remove from cur_file_cache_ets here because
                %% there may be further writes in the mailbox for the
                %% same msg.
                1 -> ok = Dec(),
                     delete_file_if_empty(
                       File, gc_candidate(File,
                         adjust_valid_total_size(
                               File, -TotalSize, State)));
                _ -> ok = Dec(),
                     gc_candidate(File, State)
            end
    end.

%% We never want to GC the current file. We only want to GC
%% other files that have had removes. We will do the checks
%% for compaction on a timer so that we can GC individual
%% files even if we don't reach more than half invalid data,
%% otherwise we end up compacting less as the size of the
%% entire store increases.
gc_candidate(File, State = #msstate{ current_file = File }) ->
    State;
gc_candidate(File, State = #msstate{ gc_candidates  = Candidates,
                                     gc_check_timer = undefined }) ->
    TimerRef = erlang:start_timer(15000, self(), {maybe_gc, Candidates#{ File => true }}),
    State#msstate{ gc_candidates  = #{},
                   gc_check_timer = TimerRef };
gc_candidate(File, State = #msstate{ gc_candidates = Candidates }) ->
    State#msstate{ gc_candidates = Candidates#{ File => true }}.

write_message(MsgId, Msg,
              State0 = #msstate { current_file_handle = CurHdl,
                                  current_file        = CurFile,
                                  current_file_offset = CurOffset,
                                  file_summary_ets    = FileSummaryEts }) ->
    {MaybeFlush, TotalSize} = writer_append(CurHdl, MsgId, Msg),
    State = case MaybeFlush of
        flush -> internal_sync(State0);
        ok -> State0
    end,
    ok = index_insert(
           #msg_location { msg_id = MsgId, ref_count = 1, file = CurFile,
                           offset = CurOffset, total_size = TotalSize }, State),
    [_,_] = ets:update_counter(FileSummaryEts, CurFile,
                               [{#file_summary.valid_total_size, TotalSize},
                                {#file_summary.file_size,        TotalSize}]),
    maybe_roll_to_new_file(CurOffset + TotalSize,
                           State #msstate {
                             current_file_offset = CurOffset + TotalSize }).

contains_message(MsgId, From, State) ->
    MsgLocation = index_lookup_positive_ref_count(MsgId, State),
    gen_server2:reply(From, MsgLocation =/= not_found),
    State.

update_msg_cache(CacheEts, MsgId, Msg) ->
    case ets:insert_new(CacheEts, {MsgId, Msg, 1}) of
        true  ->
            ok;
        false ->
            %% Note: This is basically rabbit_misc:safe_ets_update_counter/5,
            %% but without the debug log that we don't want as the update is
            %% more likely to fail following recent reworkings.
            try
                ets:update_counter(CacheEts, MsgId, {3, +1}),
                ok
            catch error:badarg ->
                %% The entry must have been removed between
                %% insert_new and update_counter, most likely
                %% due to a file rollover or a confirm. Try again.
                update_msg_cache(CacheEts, MsgId, Msg)
            end
    end.

adjust_valid_total_size(File, Delta, State = #msstate {
                                       file_summary_ets = FileSummaryEts }) ->
    [_] = ets:update_counter(FileSummaryEts, File,
                             [{#file_summary.valid_total_size, Delta}]),
    State.

update_pending_confirms(Fun, CRef,
                        State = #msstate { clients         = Clients,
                                           cref_to_msg_ids = CTM }) ->
    case maps:get(CRef, Clients) of
        {_CPid, undefined   } -> State;
        {_CPid, MsgOnDiskFun} -> CTM1 = Fun(MsgOnDiskFun, CTM),
                                 State #msstate { cref_to_msg_ids = CTM1 }
    end.

record_pending_confirm(CRef, MsgId, State) ->
    update_pending_confirms(
      fun (_MsgOnDiskFun, CTM) ->
            NewMsgIds = case maps:find(CRef, CTM) of
                error        -> sets:add_element(MsgId, sets:new([{version,2}]));
                {ok, MsgIds} -> sets:add_element(MsgId, MsgIds)
            end,
            maps:put(CRef, NewMsgIds, CTM)
      end, CRef, State).

%% Detect whether the MsgId is older or younger than the client's death
%% msg (if there is one). If the msg is older than the client death
%% msg, and it has a 0 ref_count we must only alter the ref_count, not
%% rewrite the msg - rewriting it would make it younger than the death
%% msg and thus should be ignored. Note that this (correctly) returns
%% false when testing to remove the death msg itself.
should_mask_action(CRef, MsgId,
                   State = #msstate{dying_clients = DyingClients}) ->
    case {maps:find(CRef, DyingClients), index_lookup(MsgId, State)} of
        {error, Location} ->
            {false, Location};
        {{ok, _}, not_found} ->
            {true, not_found};
        {{ok, Client}, #msg_location { file = File, offset = Offset,
                                       ref_count = RefCount } = Location} ->
            #dying_client{file = DeathFile, offset = DeathOffset} = Client,
            {case {{DeathFile, DeathOffset} < {File, Offset}, RefCount} of
                 {true,  _} -> true;
                 {false, 0} -> false_if_increment;
                 {false, _} -> false
             end, Location}
    end.

%%----------------------------------------------------------------------------
%% file helper functions
%%----------------------------------------------------------------------------

-record(writer, {
    fd = file:fd(),
    %% We are only using this buffer from one pid therefore
    %% we will not acquire/release locks.
    buffer = prim_buffer:prim_buffer()
}).

-define(MAX_BUFFER_SIZE, 1048576). %% 1MB.

writer_open(Dir, Num) ->
    {ok, Fd} = file:open(form_filename(Dir, filenum_to_name(Num)),
                         [append, binary, raw]),
    {ok, #writer{fd = Fd, buffer = prim_buffer:new()}}.

writer_recover(Dir, Num, Offset) ->
    {ok, Fd} = file:open(form_filename(Dir, filenum_to_name(Num)),
                         [read, write, binary, raw]),
    {ok, Offset} = file:position(Fd, Offset),
    ok = file:truncate(Fd),
    {ok, #writer{fd = Fd, buffer = prim_buffer:new()}}.

writer_append(#writer{buffer = Buffer}, MsgId, MsgBody) ->
    MsgBodyBin = term_to_binary(MsgBody),
    MsgBodyBinSize = byte_size(MsgBodyBin),
    EntrySize = MsgBodyBinSize + 16, %% Size of MsgId + MsgBodyBin.
    %% We send an iovec to the buffer instead of building a binary.
    prim_buffer:write(Buffer, [
        <<EntrySize:64>>,
        MsgId,
        MsgBodyBin,
        <<255>> %% OK marker.
    ]),
    Res = case prim_buffer:size(Buffer) of
        Size when Size >= ?MAX_BUFFER_SIZE ->
            flush;
        _ ->
            ok
    end,
    {Res, EntrySize + 9}. %% EntrySize + size field + OK marker.

%% Note: the message store no longer fsyncs; it only flushes data
%% to disk. This is in line with classic queues v2 behavior.
writer_flush(#writer{fd = Fd, buffer = Buffer}) ->
    case prim_buffer:size(Buffer) of
        0 ->
            ok;
        Size ->
            file:write(Fd, prim_buffer:read_iovec(Buffer, Size))
    end.

writer_close(#writer{fd = Fd}) ->
    file:close(Fd).

open_file(File, Mode) ->
    file_handle_cache:open_with_absolute_path(
      File, ?BINARY_MODE ++ Mode,
      [{write_buffer, ?HANDLE_CACHE_BUFFER_SIZE},
       {read_buffer,  ?HANDLE_CACHE_BUFFER_SIZE}]).

mark_handle_open(FileHandlesEts, File, Ref) ->
    %% This is fine to fail (already exists). Note it could fail with
    %% the value being close, and not have it updated to open.
    ets:insert_new(FileHandlesEts, {{Ref, File}, erlang:monotonic_time()}),
    true.

mark_handle_closed(FileHandlesEts, File, Ref) ->
    ets:delete(FileHandlesEts, {Ref, File}).

form_filename(Dir, Name) -> filename:join(Dir, Name).

filenum_to_name(File) -> integer_to_list(File) ++ ?FILE_EXTENSION.

filename_to_num(FileName) -> list_to_integer(filename:rootname(FileName)).

list_sorted_filenames(Dir, Ext) ->
    lists:sort(fun (A, B) -> filename_to_num(A) < filename_to_num(B) end,
               filelib:wildcard("*" ++ Ext, Dir)).

%%----------------------------------------------------------------------------
%% index
%%----------------------------------------------------------------------------

index_lookup_positive_ref_count(Key, State) ->
    case index_lookup(Key, State) of
        not_found                       -> not_found;
        #msg_location { ref_count = 0 } -> not_found;
        #msg_location {} = MsgLocation  -> MsgLocation
    end.

index_update_ref_count(Key, RefCount, State) ->
    index_update_fields(Key, {#msg_location.ref_count, RefCount}, State).

index_lookup(Key, #gc_state { index_module = Index,
                              index_state  = State }) ->
    Index:lookup(Key, State);

index_lookup(Key, #client_msstate { index_module = Index,
                                    index_state  = State }) ->
    Index:lookup(Key, State);

index_lookup(Key, #msstate { index_module = Index, index_state = State }) ->
    Index:lookup(Key, State).

index_insert(Obj, #msstate { index_module = Index, index_state = State }) ->
    Index:insert(Obj, State).

index_update(Obj, #msstate { index_module = Index, index_state = State }) ->
    Index:update(Obj, State).

index_update_fields(Key, Updates, #msstate{ index_module = Index,
                                            index_state  = State }) ->
    Index:update_fields(Key, Updates, State);
index_update_fields(Key, Updates, #gc_state{ index_module = Index,
                                             index_state  = State }) ->
    Index:update_fields(Key, Updates, State).

index_delete(Key, #msstate { index_module = Index, index_state = State }) ->
    Index:delete(Key, State).

index_delete_object(Obj, #gc_state{ index_module = Index,
                                    index_state = State }) ->
     Index:delete_object(Obj, State).

index_clean_up_temporary_reference_count_entries(
        #msstate { index_module = Index,
                   index_state  = State }) ->
    Index:clean_up_temporary_reference_count_entries_without_file(State).

%%----------------------------------------------------------------------------
%% shutdown and recovery
%%----------------------------------------------------------------------------

recover_index_and_client_refs(IndexModule, _Recover, undefined, Dir, _Name) ->
    {false, IndexModule:new(Dir), []};
recover_index_and_client_refs(IndexModule, false, _ClientRefs, Dir, Name) ->
    rabbit_log:warning("Message store ~tp: rebuilding indices from scratch", [Name]),
    {false, IndexModule:new(Dir), []};
recover_index_and_client_refs(IndexModule, true, ClientRefs, Dir, Name) ->
    Fresh = fun (ErrorMsg, ErrorArgs) ->
                    rabbit_log:warning("Message store ~tp : " ++ ErrorMsg ++ "~n"
                                       "rebuilding indices from scratch",
                                       [Name | ErrorArgs]),
                    {false, IndexModule:new(Dir), []}
            end,
    case read_recovery_terms(Dir) of
        {false, Error} ->
            Fresh("failed to read recovery terms: ~tp", [Error]);
        {true, Terms} ->
            RecClientRefs  = proplists:get_value(client_refs, Terms, []),
            RecIndexModule = proplists:get_value(index_module, Terms),
            case (lists:sort(ClientRefs) =:= lists:sort(RecClientRefs)
                  andalso IndexModule =:= RecIndexModule) of
                true  -> case IndexModule:recover(Dir) of
                             {ok, IndexState1} ->
                                 {true, IndexState1, ClientRefs};
                             {error, Error} ->
                                 Fresh("failed to recover index: ~tp", [Error])
                         end;
                false -> Fresh("recovery terms differ from present", [])
            end
    end.

store_recovery_terms(Terms, Dir) ->
    rabbit_file:write_term_file(filename:join(Dir, ?CLEAN_FILENAME), Terms).

read_recovery_terms(Dir) ->
    Path = filename:join(Dir, ?CLEAN_FILENAME),
    case rabbit_file:read_term_file(Path) of
        {ok, Terms}    -> case file:delete(Path) of
                              ok             -> {true,  Terms};
                              {error, Error} -> {false, Error}
                          end;
        {error, Error} -> {false, Error}
    end.

store_file_summary(Tid, Dir) ->
    ets:tab2file(Tid, filename:join(Dir, ?FILE_SUMMARY_FILENAME),
                      [{extended_info, [object_count]}]).

recover_file_summary(false, _Dir) ->
    %% The only reason for this to be an *ordered*_set is so
    %% that build_index in fast recovery mode can easily identify
    %% the current file.
    {false, ets:new(rabbit_msg_store_file_summary,
                    [ordered_set, public, {keypos, #file_summary.file}])};
recover_file_summary(true, Dir) ->
    Path = filename:join(Dir, ?FILE_SUMMARY_FILENAME),
    case ets:file2tab(Path) of
        {ok, Tid}       -> ok = file:delete(Path),
                           {true, Tid};
        {error, _Error} -> recover_file_summary(false, Dir)
    end.

count_msg_refs(Gen, Seed, State) ->
    case Gen(Seed) of
        finished ->
            ok;
        {_MsgId, 0, Next} ->
            count_msg_refs(Gen, Next, State);
        {MsgId, Delta, Next} ->
            ok = case index_lookup(MsgId, State) of
                     not_found ->
                         index_insert(#msg_location { msg_id = MsgId,
                                                      file = undefined,
                                                      ref_count = Delta },
                                      State);
                     #msg_location { ref_count = RefCount } = StoreEntry ->
                         NewRefCount = RefCount + Delta,
                         case NewRefCount of
                             0 -> index_delete(MsgId, State);
                             _ -> index_update(StoreEntry #msg_location {
                                                 ref_count = NewRefCount },
                                               State)
                         end
                 end,
            count_msg_refs(Gen, Next, State)
    end.

scan_file_for_valid_messages(File) ->
    case open_file(File, ?READ_MODE) of
        {ok, Hdl}       -> Valid = rabbit_msg_file:scan(
                                     Hdl, filelib:file_size(File),
                                     fun scan_fun/2, []),
                           ok = file_handle_cache:close(Hdl),
                           Valid;
        {error, enoent} -> {ok, [], 0};
        {error, Reason} -> {error, {unable_to_scan_file,
                                    filename:basename(File),
                                    Reason}}
    end.

scan_file_for_valid_messages(Dir, FileName) ->
    scan_file_for_valid_messages(form_filename(Dir, FileName)).

scan_fun({MsgId, TotalSize, Offset, _Msg}, Acc) ->
    [{MsgId, TotalSize, Offset} | Acc].

build_index(true, _StartupFunState,
            State = #msstate { file_summary_ets = FileSummaryEts }) ->
    File = ets:last(FileSummaryEts),
    FileSize = ets:lookup_element(FileSummaryEts, File, #file_summary.file_size),
    {FileSize, State#msstate{ current_file = File }};
build_index(false, {MsgRefDeltaGen, MsgRefDeltaGenInit},
            State = #msstate { dir = Dir }) ->
    rabbit_log:debug("Rebuilding message refcount...", []),
    ok = count_msg_refs(MsgRefDeltaGen, MsgRefDeltaGenInit, State),
    rabbit_log:debug("Done rebuilding message refcount", []),
    {ok, Pid} = gatherer:start_link(),
    case [filename_to_num(FileName) ||
             FileName <- list_sorted_filenames(Dir, ?FILE_EXTENSION)] of
        []     -> rebuild_index(Pid, [State #msstate.current_file],
                                State);
        Files  -> {Offset, State1} = rebuild_index(Pid, Files, State),
                  {Offset, lists:foldl(fun delete_file_if_empty/2,
                                       State1, Files)}
    end.

build_index_worker(Gatherer, State = #msstate { dir = Dir },
                   File, Files) ->
    FileName = filenum_to_name(File),
    rabbit_log:debug("Rebuilding message location index from ~ts (~B file(s) remaining)",
                     [form_filename(Dir, FileName), length(Files)]),
    {ok, Messages0, FileSize} =
        scan_file_for_valid_messages(Dir, FileName),
    %% The scan gives us messages end-of-file first so we reverse the list
    %% in case a compaction had occurred before shutdown to not have to repeat it.
    Messages = lists:reverse(Messages0),
    {ValidMessages, ValidTotalSize} =
        lists:foldl(
          fun (Obj = {MsgId, TotalSize, Offset}, {VMAcc, VTSAcc}) ->
                  %% We only keep the first message in the file. Duplicates (due to compaction) get ignored.
                  case index_lookup(MsgId, State) of
                      #msg_location { file = undefined } = StoreEntry ->
                          ok = index_update(StoreEntry #msg_location {
                                              file = File, offset = Offset,
                                              total_size = TotalSize },
                                            State),
                          {[Obj | VMAcc], VTSAcc + TotalSize};
                      _ ->
                          {VMAcc, VTSAcc}
                  end
          end, {[], 0}, Messages),
    FileSize1 =
        case Files of
            %% if it's the last file, we'll truncate to remove any
            %% rubbish above the last valid message. This affects the
            %% file size.
            []    -> case ValidMessages of
                         [] -> 0;
                         _  -> {_MsgId, TotalSize, Offset} =
                                   hd(ValidMessages),
                               Offset + TotalSize
                     end;
            [_|_] -> FileSize
        end,
    ok = gatherer:in(Gatherer, #file_summary {
                                  file             = File,
                                  valid_total_size = ValidTotalSize,
                                  file_size        = FileSize1,
                                  locked           = false }),
    ok = gatherer:finish(Gatherer).

enqueue_build_index_workers(_Gatherer, [], _State) ->
    exit(normal);
enqueue_build_index_workers(Gatherer, [File|Files], State) ->
    ok = worker_pool:dispatch_sync(
           fun () ->
                   link(Gatherer),
                   ok = build_index_worker(Gatherer, State,
                                           File, Files),
                   unlink(Gatherer),
                   ok
           end),
    enqueue_build_index_workers(Gatherer, Files, State).

reduce_index(Gatherer, LastFile,
             State = #msstate { file_summary_ets = FileSummaryEts }) ->
    case gatherer:out(Gatherer) of
        empty ->
            ok = gatherer:stop(Gatherer),
            ok = index_clean_up_temporary_reference_count_entries(State),
            Offset = case ets:lookup(FileSummaryEts, LastFile) of
                         []                                       -> 0;
                         [#file_summary { file_size = FileSize }] -> FileSize
                     end,
            {Offset, State #msstate { current_file = LastFile }};
        {value, FileSummary} ->
            true = ets:insert_new(FileSummaryEts, FileSummary),
            reduce_index(Gatherer, LastFile, State)
    end.

rebuild_index(Gatherer, Files, State) ->
    lists:foreach(fun (_File) ->
                          ok = gatherer:fork(Gatherer)
                  end, Files),
    Pid = spawn(
            fun () ->
                    enqueue_build_index_workers(Gatherer,
                                                Files, State)
            end),
    erlang:monitor(process, Pid),
    reduce_index(Gatherer, lists:last(Files), State).

%%----------------------------------------------------------------------------
%% garbage collection / compaction / aggregation -- internal
%%----------------------------------------------------------------------------

maybe_roll_to_new_file(
  Offset,
  State = #msstate { dir                 = Dir,
                     current_file_handle = CurHdl,
                     current_file        = CurFile,
                     file_summary_ets    = FileSummaryEts,
                     cur_file_cache_ets  = CurFileCacheEts,
                     file_size_limit     = FileSizeLimit })
  when Offset >= FileSizeLimit ->
    State1 = internal_sync(State),
    ok = writer_close(CurHdl),
    NextFile = CurFile + 1,
    {ok, NextHdl} = writer_open(Dir, NextFile),
    true = ets:insert_new(FileSummaryEts, #file_summary {
                            file             = NextFile,
                            valid_total_size = 0,
                            file_size        = 0,
                            locked           = false }),
    %% We only delete messages from the cache that were written to disk
    %% in the previous file.
    true = ets:match_delete(CurFileCacheEts, {'_', '_', 0}),
    State1 #msstate { current_file_handle = NextHdl,
                      current_file        = NextFile,
                      current_file_offset = 0 };
maybe_roll_to_new_file(_, State) ->
    State.

%% We keep track of files that have seen removes and
%% check those periodically for compaction. We only
%% compact files that have less than half valid data.
maybe_gc(Candidates,
         State = #msstate { gc_pid                = GCPid,
                            file_summary_ets      = FileSummaryEts }) ->
    FilesToCompact = find_files_to_compact(maps:keys(Candidates), FileSummaryEts),
    lists:foreach(fun(File) ->
            %% We must lock the file here and not in the GC process to avoid
            %% some concurrency issues that can arise otherwise. The lock is
            %% a soft-lock mostly used to handle fan-out edge cases (such as
            %% writing (incrementing the reference of) a fan-out message when
            %% the file it already exists in is getting GC and the message
            %% reference is 0. The GC will remove the data in that case. The
            %% soft-lock prevents us from incrementing the reference of data
            %% soon to be removed.
            %% @todo Perhaps this soft-lock is worse than potentially writing
            %% the fan-out message multiple times and we can just avoid
            %% incrementing messages that have a reference count of 0.
            true = ets:update_element(FileSummaryEts, File,
                                      {#file_summary.locked, true}),
            ok = rabbit_msg_store_gc:compact(GCPid, File)
    end, FilesToCompact),
    State#msstate{ gc_check_timer = undefined }.

find_files_to_compact([], _) ->
    [];
find_files_to_compact([File|Tail], FileSummaryEts) ->
    case ets:lookup(FileSummaryEts, File) of
        [] ->
            %% File has already been deleted; no need to compact.
            find_files_to_compact(Tail, FileSummaryEts);
        [#file_summary{ valid_total_size = ValidSize,
                        file_size        = TotalSize,
                        locked           = IsLocked }] ->
            %% We compact only if at least half the file is empty.
            %% We do not compact if the file is locked (a compaction
            %% was already requested).
            case (ValidSize * 2 < TotalSize) andalso
                 (ValidSize > 0) andalso
                 not IsLocked of
                true -> [File|find_files_to_compact(Tail, FileSummaryEts)];
                false -> find_files_to_compact(Tail, FileSummaryEts)
            end
    end.

delete_file_if_empty(File, State = #msstate { current_file = File }) ->
    State;
delete_file_if_empty(File, State = #msstate {
                             gc_pid                = GCPid,
                             gc_candidates         = Candidates,
                             file_summary_ets      = FileSummaryEts }) ->
    [#file_summary { valid_total_size = ValidData }] =
        ets:lookup(FileSummaryEts, File),
    case ValidData of
        %% Don't delete the file_summary_ets entry for File here,
        %% delete when we have actually removed the file.
        %%
        %% We don't need to lock the file because we will never
        %% write to a file that has a valid_total_size of 0. Also
        %% note that locking would require us to delay the removal
        %% request while GC is happening.
        0 -> ok = rabbit_msg_store_gc:delete(GCPid, File),
             State#msstate{ gc_candidates = maps:remove(File, Candidates) };
        _ -> State
    end.

%%----------------------------------------------------------------------------
%% garbage collection / compaction / aggregation -- external
%%----------------------------------------------------------------------------

-spec compact_file(non_neg_integer(), gc_state()) -> ok.

%% Files can always be compacted even if there are readers because we are moving data
%% within the same file, updating the index after the copy, and only truncating when
%% no readers are left.
%%
%% The algorithm used is a naive defragmentation. This means that more holes are likely
%% to remain in the file compared to a more complex defragmentation algorithm. The goal
%% is to move messages at the end of the file closer to the start of the file.
%%
%% We first look for holes, then try to fill that hole with the message that's at the
%% end of the file, and repeat that process until we reach the end of the file.
%% In the worst case it is possible that this process will not free up any space. This
%% can happen when the holes are smaller than the last message in the file for example.
%% We do not try to look at messages that are not the last because we do not want to
%% accidentally write over messages that were moved earlier.

compact_file(File, State = #gc_state { file_summary_ets = FileSummaryEts,
                                       dir              = Dir,
                                       msg_store        = Server }) ->
    %% Get metadata about the file. Will be used to calculate
    %% how much data was reclaimed as a result of compaction.
    [#file_summary{file_size = FileSize}] = ets:lookup(FileSummaryEts, File),
    %% Open the file.
    FileName = filenum_to_name(File),
    {ok, Fd} = file:open(form_filename(Dir, FileName), [read, write, binary, raw]),
    %% Load the messages.
    Messages = load_and_vacuum_message_file(File, State),
    %% Compact the file.
    {ok, TruncateSize, IndexUpdates} = do_compact_file(Fd, 0, Messages, lists:reverse(Messages), []),
    %% Sync and close the file.
    ok = file:sync(Fd),
    ok = file:close(Fd),
    %% Update the index. We synced and closed the file so we know the data will
    %% be there for readers. Note that it's OK if we crash at any point before we
    %% update the index because the old data is still there until we truncate.
    lists:foreach(fun ({UpdateMsgId, UpdateOffset}) ->
        ok = index_update_fields(UpdateMsgId,
                                 [{#msg_location.offset, UpdateOffset}],
                                 State)
    end, IndexUpdates),
    %% We can truncate only if there are no files opened before this timestamp.
    ThresholdTimestamp = erlang:monotonic_time(),
    %% Log here even though we haven't yet truncated. We will log again
    %% after truncation. This is a debug message so it doesn't hurt to
    %% put out more details around what's happening.
    Reclaimed = FileSize - TruncateSize,
    rabbit_log:debug("Compacted segment file number ~tp; ~tp bytes can now be reclaimed",
                     [File, Reclaimed]),
    %% Tell the message store to update its state.
    gen_server2:cast(Server, {compacted_file, File}),
    %% Tell the GC process to truncate the file.
    %%
    %% We will truncate later when there are no readers. We want current
    %% readers to finish using this file. New readers will get the updated
    %% index data and so truncation is safe to do even if there are new
    %% readers. We do not need to lock the file for truncation.
    %%
    %% It's possible that after we compact we end up with a TruncateSize of 0.
    %% In that case we don't schedule a truncation and will let the message
    %% store ask the GC process to delete the file.
    case TruncateSize of
        0 -> ok;
        _ -> rabbit_msg_store_gc:truncate(self(), File, TruncateSize, ThresholdTimestamp)
    end,
    %% Force a GC because we had to read data into memory to move it within
    %% the file and we don't want to keep it around. The GC process will also
    %% hibernate but that may not happen immediately due to gen_server2 usage.
    %% @todo Make it not use gen_server2? Doesn't look useful.
    garbage_collect(),
    ok.

%% If the message at the end fits into the hole we have found, we copy it there.
%% We will do the ets:updates after the data is synced to disk.
do_compact_file(Fd, Offset, Start = [#msg_location{ offset = StartMsgOffset }|_],
        [#msg_location{ msg_id = EndMsgId, offset = EndMsgOffset, total_size = EndMsgSize }|EndTail],
        IndexAcc)
        when EndMsgSize =< StartMsgOffset - Offset ->
    %% Copy the message into the hole.
    {ok, Bytes} = file:pread(Fd, EndMsgOffset, EndMsgSize),
    ok = file:pwrite(Fd, Offset, Bytes),
    case StartMsgOffset of
        EndMsgOffset ->
            %% We moved the last message we could.
            {ok, Offset + EndMsgSize, [{EndMsgId, Offset}|IndexAcc]};
        _ ->
            %% We may be able to fit another message in the same hole.
            do_compact_file(Fd, Offset + EndMsgSize, Start, EndTail,
                [{EndMsgId, Offset}|IndexAcc])
    end;
%% If the message didn't fit into the hole and this was the last message
%% we could move (start and end have the same message), we stop there.
%% The truncate size is right after this last message.
do_compact_file(_, _, [#msg_location{ offset = MsgOffset, total_size = MsgSize }|_],
        [#msg_location{ offset = MsgOffset }|_], IndexAcc) ->
    {ok, MsgOffset + MsgSize, IndexAcc};
%% Regardless of where we are in the file, even if there is a small hole, if the last
%% message can't fit it, we skip to after the hole + the next message to look for
%% another hole.
do_compact_file(Fd, _, [#msg_location{ offset = StartMsgOffset, total_size = StartMsgSize }|StartTail],
        End, IndexAcc) ->
    do_compact_file(Fd, StartMsgOffset + StartMsgSize, StartTail, End, IndexAcc);
%% We have moved all messages in the file. Stop.
%% The offset is the truncate size of the file.
do_compact_file(_, Offset, _, [], IndexAcc) ->
    {ok, Offset, IndexAcc}.

-spec truncate_file(non_neg_integer(), non_neg_integer(), integer(), gc_state()) -> ok | defer.

truncate_file(File, Size, ThresholdTimestamp, #gc_state{ file_summary_ets = FileSummaryEts,
                                                         file_handles_ets = FileHandlesEts,
                                                         dir = Dir }) ->
    %% We must first check that the file still exists because the store GC process
    %% may have been asked to delete the file during compaction, before truncate
    %% gets queued up.
    case ets:lookup(FileSummaryEts, File) of
        [] ->
            ok; %% File is gone, we are done.
        _ ->
            case ets:select(FileHandlesEts, [{{{'_', File}, '$1'},
                    [{'=<', '$1', ThresholdTimestamp}], ['$$']}], 1) of
                {[_|_], _Cont} ->
                    rabbit_log:debug("Asked to truncate file ~p but it has active readers. Deferring.",
                                     [File]),
                    defer;
                _ ->
                    FileName = filenum_to_name(File),
                    {ok, Fd} = file:open(form_filename(Dir, FileName), [read, write, binary, raw]),
                    {ok, _} = file:position(Fd, Size),
                    ok = file:truncate(Fd),
                    ok = file:close(Fd),
                    true = ets:update_element(FileSummaryEts, File,
                                              {#file_summary.file_size, Size}),
                    rabbit_log:debug("Truncated file number ~tp; new size ~tp bytes", [File, Size]),
                    ok
            end
    end.

-spec delete_file(non_neg_integer(), gc_state()) -> ok | defer.

delete_file(File, State = #gc_state { file_summary_ets = FileSummaryEts,
                                      file_handles_ets = FileHandlesEts,
                                      dir              = Dir }) ->
    case ets:match_object(FileHandlesEts, {{'_', File}, '_'}, 1) of
        {[_|_], _Cont} ->
            rabbit_log:debug("Asked to delete file ~p but it has active readers. Deferring.",
                             [File]),
            defer;
        _ ->
            [#file_summary{ valid_total_size = 0,
                            file_size = FileSize }] = ets:lookup(FileSummaryEts, File),
            {[], 0} = scan_and_vacuum_message_file(File, State),
            ok = file:delete(form_filename(Dir, filenum_to_name(File))),
            true = ets:delete(FileSummaryEts, File),
            rabbit_log:debug("Deleted empty file number ~tp; reclaimed ~tp bytes", [File, FileSize]),
            ok
    end.

load_and_vacuum_message_file(File, State) ->
    Messages0 = index_select_all_from_file(File, State),
    %% Cleanup messages that have 0 ref_count.
    Messages = lists:foldl(fun
        (Entry = #msg_location{ ref_count = 0 }, Acc) ->
            ok = index_delete_object(Entry, State),
            Acc;
        (Entry, Acc) ->
            [Entry|Acc]
    end, [], Messages0),
    lists:keysort(#msg_location.offset, Messages).

index_select_all_from_file(File, #gc_state { index_module = Index,
                                             index_state  = State }) ->
    Index:select_all_from_file(File, State).

scan_and_vacuum_message_file(File, State = #gc_state { dir = Dir }) ->
    %% Messages here will be end-of-file at start-of-list
    {ok, Messages, _FileSize} =
        scan_file_for_valid_messages(Dir, filenum_to_name(File)),
    %% foldl will reverse so will end up with msgs in ascending offset order
    lists:foldl(
      fun ({MsgId, TotalSize, Offset}, Acc = {List, Size}) ->
              case index_lookup(MsgId, State) of
                  #msg_location { file = File, total_size = TotalSize,
                                  offset = Offset, ref_count = 0 } = Entry ->
                      ok = index_delete_object(Entry, State),
                      Acc;
                  #msg_location { file = File, total_size = TotalSize,
                                  offset = Offset } = Entry ->
                      {[ Entry | List ], TotalSize + Size};
                  _ ->
                      Acc
              end
      end, {[], 0}, Messages).
