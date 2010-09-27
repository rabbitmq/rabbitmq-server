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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_msg_store).

-behaviour(gen_server2).

-export([start_link/4, write/4, read/3, contains/2, remove/2, release/2,
         sync/3, client_init/2, client_terminate/2,
         client_delete_and_terminate/3, successfully_recovered_state/1,
         register_sync_callback/3]).

-export([sync/1, gc_done/4, set_maximum_since_use/2, gc/3]). %% internal

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, prioritise_call/3, prioritise_cast/2]).

%%----------------------------------------------------------------------------

-include("rabbit_msg_store.hrl").

-define(SYNC_INTERVAL,  5).   %% milliseconds
-define(CLEAN_FILENAME, "clean.dot").
-define(FILE_SUMMARY_FILENAME, "file_summary.ets").

-define(BINARY_MODE,     [raw, binary]).
-define(READ_MODE,       [read]).
-define(READ_AHEAD_MODE, [read_ahead | ?READ_MODE]).
-define(WRITE_MODE,      [write]).

-define(FILE_EXTENSION,        ".rdq").
-define(FILE_EXTENSION_TMP,    ".rdt").

-define(HANDLE_CACHE_BUFFER_SIZE, 1048576). %% 1MB

%%----------------------------------------------------------------------------

-record(msstate,
        { dir,                    %% store directory
          index_module,           %% the module for index ops
          index_state,            %% where are messages?
          current_file,           %% current file name as number
          current_file_handle,    %% current file handle since the last fsync?
          file_handle_cache,      %% file handle cache
          on_sync,                %% pending sync requests
          sync_timer_ref,         %% TRef for our interval timer
          sum_valid_data,         %% sum of valid data in all files
          sum_file_size,          %% sum of file sizes
          pending_gc_completion,  %% things to do once GC completes
          gc_active,              %% is the GC currently working?
          gc_pid,                 %% pid of our GC
          file_handles_ets,       %% tid of the shared file handles table
          file_summary_ets,       %% tid of the file summary table
          dedup_cache_ets,        %% tid of dedup cache table
          cur_file_cache_ets,     %% tid of current file cache table
          client_refs,            %% set of references of all registered clients
          successfully_recovered, %% boolean: did we recover state?
          file_size_limit,        %% how big are our files allowed to get?
          client_ondisk_callback, %% client ref to callback function mapping
          cref_to_guids           %% client ref to synced messages mapping
         }).

-record(client_msstate,
        { file_handle_cache,
          index_state,
          index_module,
          dir,
          gc_pid,
          file_handles_ets,
          file_summary_ets,
          dedup_cache_ets,
          cur_file_cache_ets,
          client_ref
         }).

-record(file_summary,
        {file, valid_total_size, left, right, file_size, locked, readers}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(server() :: pid() | atom()).
-type(file_num() :: non_neg_integer()).
-type(client_msstate() :: #client_msstate {
                      file_handle_cache  :: dict:dictionary(),
                      index_state        :: any(),
                      index_module       :: atom(),
                      dir                :: file:filename(),
                      gc_pid             :: pid(),
                      file_handles_ets   :: ets:tid(),
                      file_summary_ets   :: ets:tid(),
                      dedup_cache_ets    :: ets:tid(),
                      cur_file_cache_ets :: ets:tid(),
                      client_ref         :: rabbit_guid:guid()}).
-type(startup_fun_state() ::
        {(fun ((A) -> 'finished' | {rabbit_guid:guid(), non_neg_integer(), A})),
         A}).

-spec(start_link/4 ::
        (atom(), file:filename(), [binary()] | 'undefined',
         startup_fun_state()) -> rabbit_types:ok_pid_or_error()).
-spec(write/4 :: (server(), rabbit_guid:guid(),
                  msg(), client_msstate())
                 -> rabbit_types:ok(client_msstate())).
-spec(read/3 :: (server(), rabbit_guid:guid(), client_msstate()) ->
                     {rabbit_types:ok(msg()) | 'not_found', client_msstate()}).
-spec(contains/2 :: (server(), rabbit_guid:guid()) -> boolean()).
-spec(remove/2 :: (server(), [rabbit_guid:guid()]) -> 'ok').
-spec(release/2 :: (server(), [rabbit_guid:guid()]) -> 'ok').
-spec(sync/3 :: (server(), [rabbit_guid:guid()], fun (() -> any())) -> 'ok').
-spec(gc_done/4 :: (server(), non_neg_integer(), file_num(), file_num()) ->
                        'ok').
-spec(set_maximum_since_use/2 :: (server(), non_neg_integer()) -> 'ok').
-spec(client_init/2 :: (server(), rabbit_guid:guid()) -> client_msstate()).
-spec(client_terminate/2 :: (client_msstate(), server()) -> 'ok').
-spec(client_delete_and_terminate/3 ::
        (client_msstate(), server(), rabbit_guid:guid()) -> 'ok').
-spec(successfully_recovered_state/1 :: (server()) -> boolean()).

-spec(gc/3 :: (non_neg_integer(), non_neg_integer(),
               {ets:tid(), file:filename(), atom(), any()}) ->
                   'concurrent_readers' | non_neg_integer()).

-endif.

%%----------------------------------------------------------------------------

%% We run GC whenever (garbage / sum_file_size) > ?GARBAGE_FRACTION
%% It is not recommended to set this to < 0.5
-define(GARBAGE_FRACTION,      0.5).

%% The components:
%%
%% Index: this is a mapping from Guid to #msg_location{}:
%%        {Guid, RefCount, File, Offset, TotalSize}
%%        By default, it's in ets, but it's also pluggable.
%% FileSummary: this is an ets table which maps File to #file_summary{}:
%%        {File, ValidTotalSize, Left, Right, FileSize, Locked, Readers}
%%
%% The basic idea is that messages are appended to the current file up
%% until that file becomes too big (> file_size_limit). At that point,
%% the file is closed and a new file is created on the _right_ of the
%% old file which is used for new messages. Files are named
%% numerically ascending, thus the file with the lowest name is the
%% eldest file.
%%
%% We need to keep track of which messages are in which files (this is
%% the Index); how much useful data is in each file and which files
%% are on the left and right of each other. This is the purpose of the
%% FileSummary ets table.
%%
%% As messages are removed from files, holes appear in these
%% files. The field ValidTotalSize contains the total amount of useful
%% data left in the file. This is needed for garbage collection.
%%
%% When we discover that a file is now empty, we delete it. When we
%% discover that it can be combined with the useful data in either its
%% left or right neighbour, and overall, across all the files, we have
%% ((the amount of garbage) / (the sum of all file sizes)) >
%% ?GARBAGE_FRACTION, we start a garbage collection run concurrently,
%% which will compact the two files together. This keeps disk
%% utilisation high and aids performance. We deliberately do this
%% lazily in order to prevent doing GC on files which are soon to be
%% emptied (and hence deleted) soon.
%%
%% Given the compaction between two files, the left file (i.e. elder
%% file) is considered the ultimate destination for the good data in
%% the right file. If necessary, the good data in the left file which
%% is fragmented throughout the file is written out to a temporary
%% file, then read back in to form a contiguous chunk of good data at
%% the start of the left file. Thus the left file is garbage collected
%% and compacted. Then the good data from the right file is copied
%% onto the end of the left file. Index and FileSummary tables are
%% updated.
%%
%% On non-clean startup, we scan the files we discover, dealing with
%% the possibilites of a crash having occured during a compaction
%% (this consists of tidyup - the compaction is deliberately designed
%% such that data is duplicated on disk rather than risking it being
%% lost), and rebuild the FileSummary ets table and Index.
%%
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
%% good data at the bottom of files are not rewritten.
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
%%
%% Messages are reference-counted. When a message with the same guid
%% is written several times we only store it once, and only remove it
%% from the store when it has been removed the same number of times.
%%
%% The reference counts do not persist. Therefore the initialisation
%% function must be provided with a generator that produces ref count
%% deltas for all recovered messages. This is only used on startup
%% when the shutdown was non-clean.
%%
%% Read messages with a reference count greater than one are entered
%% into a message cache. The purpose of the cache is not especially
%% performance, though it can help there too, but prevention of memory
%% explosion. It ensures that as messages with a high reference count
%% are read from several processes they are read back as the same
%% binary object rather than multiples of identical binary
%% objects.
%%
%% Reads can be performed directly by clients without calling to the
%% server. This is safe because multiple file handles can be used to
%% read files. However, locking is used by the concurrent GC to make
%% sure that reads are not attempted from files which are in the
%% process of being garbage collected.
%%
%% The server automatically defers reads, removes and contains calls
%% that occur which refer to files which are currently being
%% GC'd. Contains calls are only deferred in order to ensure they do
%% not overtake removes.
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
%% For notes on Clean Shutdown and startup, see documentation in
%% variable_queue.

%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------

start_link(Server, Dir, ClientRefs, StartupFunState) ->
    gen_server2:start_link({local, Server}, ?MODULE,
                           [Server, Dir, ClientRefs, StartupFunState],
                           [{timeout, infinity}]).

write(Server, Guid, Msg,
      CState = #client_msstate { cur_file_cache_ets = CurFileCacheEts,
                                 client_ref         = CRef }) ->
    ok = update_msg_cache(CurFileCacheEts, Guid, Msg),
    {gen_server2:cast(Server, {write, CRef, Guid}), CState}.

read(Server, Guid,
     CState = #client_msstate { dedup_cache_ets    = DedupCacheEts,
                                cur_file_cache_ets = CurFileCacheEts }) ->
    %% 1. Check the dedup cache
    case fetch_and_increment_cache(DedupCacheEts, Guid) of
        not_found ->
            %% 2. Check the cur file cache
            case ets:lookup(CurFileCacheEts, Guid) of
                [] ->
                    Defer = fun() -> {gen_server2:call(
                                        Server, {read, Guid}, infinity),
                                      CState} end,
                    case index_lookup(Guid, CState) of
                        not_found   -> Defer();
                        MsgLocation -> client_read1(Server, MsgLocation, Defer,
                                                    CState)
                    end;
                [{Guid, Msg, _CacheRefCount}] ->
                    %% Although we've found it, we don't know the
                    %% refcount, so can't insert into dedup cache
                    {{ok, Msg}, CState}
            end;
        Msg ->
            {{ok, Msg}, CState}
    end.

contains(Server, Guid) -> gen_server2:call(Server, {contains, Guid}, infinity).
remove(_Server, [])    -> ok;
remove(Server, Guids)  -> gen_server2:cast(Server, {remove, Guids}).
release(_Server, [])   -> ok;
release(Server, Guids) -> gen_server2:cast(Server, {release, Guids}).
sync(Server, Guids, K) -> gen_server2:cast(Server, {sync, Guids, K}).
sync(Server)           -> gen_server2:cast(Server, sync). %% internal

gc_done(Server, Reclaimed, Source, Destination) ->
    gen_server2:cast(Server, {gc_done, Reclaimed, Source, Destination}).

set_maximum_since_use(Server, Age) ->
    gen_server2:cast(Server, {set_maximum_since_use, Age}).

client_init(Server, Ref) ->
    {IState, IModule, Dir, GCPid,
     FileHandlesEts, FileSummaryEts, DedupCacheEts, CurFileCacheEts} =
        gen_server2:call(Server, {new_client_state, Ref}, infinity),
    #client_msstate { file_handle_cache  = dict:new(),
                      index_state        = IState,
                      index_module       = IModule,
                      dir                = Dir,
                      gc_pid             = GCPid,
                      file_handles_ets   = FileHandlesEts,
                      file_summary_ets   = FileSummaryEts,
                      dedup_cache_ets    = DedupCacheEts,
                      cur_file_cache_ets = CurFileCacheEts,
                      client_ref         = Ref}.

client_terminate(CState, Server) ->
    close_all_handles(CState),
    ok = gen_server2:call(Server, {client_terminate, CState}, infinity).

client_delete_and_terminate(CState, Server, Ref) ->
    close_all_handles(CState),
    ok = gen_server2:cast(Server, {client_delete, Ref}).

successfully_recovered_state(Server) ->
    gen_server2:call(Server, successfully_recovered_state, infinity).

register_sync_callback(Server, ClientRef, Fun) ->
    gen_server2:call(Server, {register_sync_callback, ClientRef, Fun}, infinity).

%%----------------------------------------------------------------------------
%% Client-side-only helpers
%%----------------------------------------------------------------------------

client_read1(Server,
             #msg_location { guid = Guid, file = File } = MsgLocation,
             Defer,
             CState = #client_msstate { file_summary_ets = FileSummaryEts }) ->
    case ets:lookup(FileSummaryEts, File) of
        [] -> %% File has been GC'd and no longer exists. Go around again.
            read(Server, Guid, CState);
        [#file_summary { locked = Locked, right = Right }] ->
            client_read2(Server, Locked, Right, MsgLocation, Defer, CState)
    end.

client_read2(_Server, false, undefined, _MsgLocation, Defer, _CState) ->
    %% Although we've already checked both caches and not found the
    %% message there, the message is apparently in the
    %% current_file. We can only arrive here if we are trying to read
    %% a message which we have not written, which is very odd, so just
    %% defer.
    %%
    %% OR, on startup, the cur_file_cache is not populated with the
    %% contents of the current file, thus reads from the current file
    %% will end up here and will need to be deferred.
    Defer();
client_read2(_Server, true, _Right, _MsgLocation, Defer, _CState) ->
    %% Of course, in the mean time, the GC could have run and our msg
    %% is actually in a different file, unlocked. However, defering is
    %% the safest and simplest thing to do.
    Defer();
client_read2(Server, false, _Right,
             MsgLocation = #msg_location { guid = Guid, file = File },
             Defer,
             CState = #client_msstate { file_summary_ets = FileSummaryEts }) ->
    %% It's entirely possible that everything we're doing from here on
    %% is for the wrong file, or a non-existent file, as a GC may have
    %% finished.
    safe_ets_update_counter(
      FileSummaryEts, File, {#file_summary.readers, +1},
      fun (_) -> client_read3(Server, MsgLocation, Defer, CState) end,
      fun () -> read(Server, Guid, CState) end).

client_read3(Server, #msg_location { guid = Guid, file = File }, Defer,
             CState = #client_msstate { file_handles_ets = FileHandlesEts,
                                        file_summary_ets = FileSummaryEts,
                                        dedup_cache_ets  = DedupCacheEts,
                                        gc_pid           = GCPid }) ->
    Release =
        fun() -> ok = case ets:update_counter(FileSummaryEts, File,
                                              {#file_summary.readers, -1}) of
                          0 -> case ets:lookup(FileSummaryEts, File) of
                                   [#file_summary { locked = true }] ->
                                       rabbit_msg_store_gc:no_readers(
                                         GCPid, File);
                                   _ -> ok
                               end;
                          _ -> ok
                      end
        end,
    %% If a GC involving the file hasn't already started, it won't
    %% start now. Need to check again to see if we've been locked in
    %% the meantime, between lookup and update_counter (thus GC
    %% started before our +1. In fact, it could have finished by now
    %% too).
    case ets:lookup(FileSummaryEts, File) of
        [] -> %% GC has deleted our file, just go round again.
            read(Server, Guid, CState);
        [#file_summary { locked = true }] ->
            %% If we get a badarg here, then the GC has finished and
            %% deleted our file. Try going around again. Otherwise,
            %% just defer.
            %%
            %% badarg scenario: we lookup, msg_store locks, GC starts,
            %% GC ends, we +1 readers, msg_store ets:deletes (and
            %% unlocks the dest)
            try Release(),
                Defer()
            catch error:badarg -> read(Server, Guid, CState)
            end;
        [#file_summary { locked = false }] ->
            %% Ok, we're definitely safe to continue - a GC involving
            %% the file cannot start up now, and isn't running, so
            %% nothing will tell us from now on to close the handle if
            %% it's already open.
            %%
            %% Finally, we need to recheck that the msg is still at
            %% the same place - it's possible an entire GC ran between
            %% us doing the lookup and the +1 on the readers. (Same as
            %% badarg scenario above, but we don't have a missing file
            %% - we just have the /wrong/ file).
            case index_lookup(Guid, CState) of
                #msg_location { file = File } = MsgLocation ->
                    %% Still the same file.
                    mark_handle_open(FileHandlesEts, File),

                    CState1 = close_all_indicated(CState),
                    {Msg, CState2} = %% This will never be the current file
                        read_from_disk(MsgLocation, CState1, DedupCacheEts),
                    Release(), %% this MUST NOT fail with badarg
                    {{ok, Msg}, CState2};
                MsgLocation -> %% different file!
                    Release(), %% this MUST NOT fail with badarg
                    client_read1(Server, MsgLocation, Defer, CState)
            end
    end.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([Server, BaseDir, ClientRefs, StartupFunState]) ->
    process_flag(trap_exit, true),

    ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
                                             [self()]),

    Dir = filename:join(BaseDir, atom_to_list(Server)),

    {ok, IndexModule} = application:get_env(msg_store_index_module),
    rabbit_log:info("~w: using ~p to provide index~n", [Server, IndexModule]),

    AttemptFileSummaryRecovery =
        case ClientRefs of
            undefined -> ok = rabbit_misc:recursive_delete([Dir]),
                         ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
                         false;
            _         -> ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
                         recover_crashed_compactions(Dir)
        end,

    %% if we found crashed compactions we trust neither the
    %% file_summary nor the location index. Note the file_summary is
    %% left empty here if it can't be recovered.
    {FileSummaryRecovered, FileSummaryEts} =
        recover_file_summary(AttemptFileSummaryRecovery, Dir),

    {CleanShutdown, IndexState, ClientRefs1} =
        recover_index_and_client_refs(IndexModule, FileSummaryRecovered,
                                      ClientRefs, Dir, Server),
    %% CleanShutdown => msg location index and file_summary both
    %% recovered correctly.
    true = case {FileSummaryRecovered, CleanShutdown} of
               {true, false} -> ets:delete_all_objects(FileSummaryEts);
               _             -> true
           end,
    %% CleanShutdown <=> msg location index and file_summary both
    %% recovered correctly.

    DedupCacheEts   = ets:new(rabbit_msg_store_dedup_cache, [set, public]),
    FileHandlesEts  = ets:new(rabbit_msg_store_shared_file_handles,
                              [ordered_set, public]),
    CurFileCacheEts = ets:new(rabbit_msg_store_cur_file, [set, public]),

    {ok, FileSizeLimit} = application:get_env(msg_store_file_size_limit),

    State = #msstate { dir                    = Dir,
                       index_module           = IndexModule,
                       index_state            = IndexState,
                       current_file           = 0,
                       current_file_handle    = undefined,
                       file_handle_cache      = dict:new(),
                       on_sync                = [],
                       sync_timer_ref         = undefined,
                       sum_valid_data         = 0,
                       sum_file_size          = 0,
                       pending_gc_completion  = [],
                       gc_active              = false,
                       gc_pid                 = undefined,
                       file_handles_ets       = FileHandlesEts,
                       file_summary_ets       = FileSummaryEts,
                       dedup_cache_ets        = DedupCacheEts,
                       cur_file_cache_ets     = CurFileCacheEts,
                       client_refs            = ClientRefs1,
                       successfully_recovered = CleanShutdown,
                       file_size_limit        = FileSizeLimit,
                       client_ondisk_callback = dict:new(),
                       cref_to_guids          = dict:new()
                      },

    %% If we didn't recover the msg location index then we need to
    %% rebuild it now.
    {Offset, State1 = #msstate { current_file = CurFile }} =
        build_index(CleanShutdown, StartupFunState, State),

    %% read is only needed so that we can seek
    {ok, CurHdl} = open_file(Dir, filenum_to_name(CurFile),
                             [read | ?WRITE_MODE]),
    {ok, Offset} = file_handle_cache:position(CurHdl, Offset),
    ok = file_handle_cache:truncate(CurHdl),

    {ok, GCPid} = rabbit_msg_store_gc:start_link(Dir, IndexState, IndexModule,
                                                 FileSummaryEts),

    {ok, maybe_compact(
           State1 #msstate { current_file_handle = CurHdl, gc_pid = GCPid }),
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_call(Msg, _From, _State) ->
    case Msg of
        {new_client_state, _Ref}     -> 7;
        successfully_recovered_state -> 7;
        {read, _Guid}                -> 2;
        _                            -> 0
    end.

prioritise_cast(Msg, _State) ->
    case Msg of
        sync                                         -> 8;
        {gc_done, _Reclaimed, _Source, _Destination} -> 8;
        {set_maximum_since_use, _Age}                -> 8;
        _                                            -> 0
    end.

handle_call({read, Guid}, From, State) ->
    State1 = read_message(Guid, From, State),
    noreply(State1);

handle_call({contains, Guid}, From, State) ->
    State1 = contains_message(Guid, From, State),
    noreply(State1);

handle_call({new_client_state, CRef}, _From,
            State = #msstate { dir                = Dir,
                               index_state        = IndexState,
                               index_module       = IndexModule,
                               file_handles_ets   = FileHandlesEts,
                               file_summary_ets   = FileSummaryEts,
                               dedup_cache_ets    = DedupCacheEts,
                               cur_file_cache_ets = CurFileCacheEts,
                               client_refs        = ClientRefs,
                               gc_pid             = GCPid }) ->
    reply({IndexState, IndexModule, Dir, GCPid,
           FileHandlesEts, FileSummaryEts, DedupCacheEts, CurFileCacheEts},
          State #msstate { client_refs = sets:add_element(CRef, ClientRefs) });

handle_call(successfully_recovered_state, _From, State) ->
    reply(State #msstate.successfully_recovered, State);

handle_call({register_sync_callback, ClientRef, Fun}, _From,
            State = #msstate { client_ondisk_callback = CODC }) ->
    reply(ok, State #msstate { client_ondisk_callback =
                                   dict:store(ClientRef, Fun, CODC) });

handle_call({client_terminate, #client_msstate { client_ref = CRef }},
            _From,
            State = #msstate { client_ondisk_callback = CODC,
                               cref_to_guids          = CTG }) ->
    reply(ok, State #msstate { client_ondisk_callback = dict:erase(CRef, CODC),
                               cref_to_guids          = dict:erase(CRef, CTG) }).

handle_cast({write, CRef, Guid},
            State = #msstate { current_file_handle    = CurHdl,
                               current_file           = CurFile,
                               sum_valid_data         = SumValid,
                               sum_file_size          = SumFileSize,
                               file_summary_ets       = FileSummaryEts,
                               cur_file_cache_ets     = CurFileCacheEts,
                               client_ondisk_callback = CODC,
                               cref_to_guids          = CTG}) ->

    true = 0 =< ets:update_counter(CurFileCacheEts, Guid, {3, -1}),
    [{Guid, Msg, _CacheRefCount}] = ets:lookup(CurFileCacheEts, Guid),
    case index_lookup(Guid, State) of
        not_found ->
            %% New message, lots to do
            {ok, CurOffset} = file_handle_cache:current_virtual_offset(CurHdl),
            {ok, TotalSize} = rabbit_msg_file:append(CurHdl, Guid, Msg),
            ok = index_insert(#msg_location {
                                guid = Guid, ref_count = 1, file = CurFile,
                                offset = CurOffset, total_size = TotalSize },
                              State),
            [#file_summary { valid_total_size = ValidTotalSize,
                             right            = undefined,
                             locked           = false,
                             file_size        = FileSize }] =
                ets:lookup(FileSummaryEts, CurFile),
            ValidTotalSize1 = ValidTotalSize + TotalSize,
            true = ets:update_element(
                     FileSummaryEts, CurFile,
                     [{#file_summary.valid_total_size, ValidTotalSize1},
                      {#file_summary.file_size,        FileSize + TotalSize}]),
            NextOffset = CurOffset + TotalSize,
            noreply(
              maybe_roll_to_new_file(
                NextOffset, State #msstate {
                              sum_valid_data = SumValid + TotalSize,
                              sum_file_size  = SumFileSize + TotalSize,
                              cref_to_guids =
                                  case dict:find(CRef, CODC) of
                                      {ok, _} -> rabbit_misc:dict_cons(CRef, Guid, CTG);
                                      error   -> CTG
                                  end}));
        #msg_location { ref_count = RefCount } ->
            %% We already know about it, just update counter. Only
            %% update field otherwise bad interaction with concurrent GC
            ok = index_update_fields(Guid,
                                     {#msg_location.ref_count, RefCount + 1},
                                     State),
            noreply(State)
    end;

handle_cast({remove, Guids}, State) ->
    State1 = lists:foldl(
               fun (Guid, State2) -> remove_message(Guid, State2) end,
               State, Guids),
    noreply(maybe_compact(State1));

handle_cast({release, Guids}, State =
                #msstate { dedup_cache_ets = DedupCacheEts }) ->
    lists:foreach(
      fun (Guid) -> decrement_cache(DedupCacheEts, Guid) end, Guids),
    noreply(State);

handle_cast({sync, Guids, K},
            State = #msstate { current_file        = CurFile,
                               current_file_handle = CurHdl,
                               on_sync             = Syncs }) ->
    {ok, SyncOffset} = file_handle_cache:last_sync_offset(CurHdl),
    case lists:any(fun (Guid) ->
                           #msg_location { file = File, offset = Offset } =
                               index_lookup(Guid, State),
                           File =:= CurFile andalso Offset >= SyncOffset
                   end, Guids) of
        false -> K(),
                 noreply(State);
        true  -> noreply(State #msstate { on_sync = [K | Syncs] })
    end;

handle_cast(sync, State) ->
    noreply(internal_sync(State));

handle_cast({gc_done, Reclaimed, Src, Dst},
            State = #msstate { sum_file_size    = SumFileSize,
                               gc_active        = {Src, Dst},
                               file_handles_ets = FileHandlesEts,
                               file_summary_ets = FileSummaryEts }) ->
    %% GC done, so now ensure that any clients that have open fhs to
    %% those files close them before using them again. This has to be
    %% done here (given it's done in the msg_store, and not the gc),
    %% and not when starting up the GC, because if done when starting
    %% up the GC, the client could find the close, and close and
    %% reopen the fh, whilst the GC is waiting for readers to
    %% disappear, before it's actually done the GC.
    true = mark_handle_to_close(FileHandlesEts, Src),
    true = mark_handle_to_close(FileHandlesEts, Dst),
    %% we always move data left, so Src has gone and was on the
    %% right, so need to make dest = source.right.left, and also
    %% dest.right = source.right
    [#file_summary { left    = Dst,
                     right   = SrcRight,
                     locked  = true,
                     readers = 0 }] = ets:lookup(FileSummaryEts, Src),
    %% this could fail if SrcRight =:= undefined
    ets:update_element(FileSummaryEts, SrcRight, {#file_summary.left, Dst}),
    true = ets:update_element(FileSummaryEts, Dst,
                              [{#file_summary.locked, false},
                               {#file_summary.right,  SrcRight}]),
    true = ets:delete(FileSummaryEts, Src),
    noreply(
      maybe_compact(run_pending(
                      State #msstate { sum_file_size = SumFileSize - Reclaimed,
                                       gc_active     = false })));

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State);

handle_cast({client_delete, CRef},
            State = #msstate { client_refs = ClientRefs }) ->
    noreply(
      State #msstate { client_refs = sets:del_element(CRef, ClientRefs) }).

handle_info(timeout, State) ->
    noreply(internal_sync(State));

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State}.

terminate(_Reason, State = #msstate { index_state         = IndexState,
                                      index_module        = IndexModule,
                                      current_file_handle = CurHdl,
                                      gc_pid              = GCPid,
                                      file_handles_ets    = FileHandlesEts,
                                      file_summary_ets    = FileSummaryEts,
                                      dedup_cache_ets     = DedupCacheEts,
                                      cur_file_cache_ets  = CurFileCacheEts,
                                      client_refs         = ClientRefs,
                                      dir                 = Dir }) ->
    %% stop the gc first, otherwise it could be working and we pull
    %% out the ets tables from under it.
    ok = rabbit_msg_store_gc:stop(GCPid),
    State1 = case CurHdl of
                 undefined -> State;
                 _         -> State2 = internal_sync(State),
                              file_handle_cache:close(CurHdl),
                              State2
             end,
    State3 = close_all_handles(State1),
    store_file_summary(FileSummaryEts, Dir),
    [ets:delete(T) ||
        T <- [FileSummaryEts, DedupCacheEts, FileHandlesEts, CurFileCacheEts]],
    IndexModule:terminate(IndexState),
    store_recovery_terms([{client_refs, sets:to_list(ClientRefs)},
                          {index_module, IndexModule}], Dir),
    State3 #msstate { index_state         = undefined,
                      current_file_handle = undefined }.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% general helper functions
%%----------------------------------------------------------------------------

noreply(State) ->
    {State1, Timeout} = next_state(State),
    {noreply, State1, Timeout}.

reply(Reply, State) ->
    {State1, Timeout} = next_state(State),
    {reply, Reply, State1, Timeout}.

next_state(State = #msstate { sync_timer_ref = undefined,
                              on_sync = OS,
                              cref_to_guids = CTG }) ->
    case {OS, dict:size(CTG)} of
        {[], 0} -> {State, hibernate};
        _       -> {start_sync_timer(State), 0}
    end;
next_state(State = #msstate { on_sync = OS,
                              cref_to_guids = CTG }) ->
    case {OS, dict:size(CTG)} of
        {[], 0} -> {stop_sync_timer(State), hibernate};
        _       -> {State, 0}
    end.

start_sync_timer(State = #msstate { sync_timer_ref = undefined }) ->
    {ok, TRef} = timer:apply_after(?SYNC_INTERVAL, ?MODULE, sync, [self()]),
    State #msstate { sync_timer_ref = TRef }.

stop_sync_timer(State = #msstate { sync_timer_ref = undefined }) ->
    State;
stop_sync_timer(State = #msstate { sync_timer_ref = TRef }) ->
    {ok, cancel} = timer:cancel(TRef),
    State #msstate { sync_timer_ref = undefined }.

internal_sync(State = #msstate { current_file_handle = CurHdl,
                                 on_sync = Syncs,
                                 client_ondisk_callback = CODC,
                                 cref_to_guids = CTG }) ->
    State1 = stop_sync_timer(State),
    State2 = case Syncs of
                 [] -> State1;
                 _  -> ok = file_handle_cache:sync(CurHdl),
                       lists:foreach(fun (K) -> K() end, lists:reverse(Syncs)),
                       State1 #msstate { on_sync = [] }
             end,
    dict:map(fun(CRef, Guids) ->
                     case dict:find(CRef, CODC) of
                         {ok, Fun} -> Fun(Guids);
                         error     -> ok        %% shouldn't happen
                     end
             end, CTG),
    State2 #msstate { cref_to_guids = dict:new() }.


read_message(Guid, From,
             State = #msstate { dedup_cache_ets = DedupCacheEts }) ->
    case index_lookup(Guid, State) of
        not_found ->
            gen_server2:reply(From, not_found),
            State;
        MsgLocation ->
            case fetch_and_increment_cache(DedupCacheEts, Guid) of
                not_found -> read_message1(From, MsgLocation, State);
                Msg       -> gen_server2:reply(From, {ok, Msg}),
                             State
            end
    end.

read_message1(From, #msg_location { guid = Guid, ref_count = RefCount,
                                    file = File, offset = Offset } = MsgLoc,
              State = #msstate { current_file        = CurFile,
                                 current_file_handle = CurHdl,
                                 file_summary_ets    = FileSummaryEts,
                                 dedup_cache_ets     = DedupCacheEts,
                                 cur_file_cache_ets  = CurFileCacheEts }) ->
    case File =:= CurFile of
        true  -> {Msg, State1} =
                     %% can return [] if msg in file existed on startup
                     case ets:lookup(CurFileCacheEts, Guid) of
                         [] ->
                             {ok, RawOffSet} =
                                 file_handle_cache:current_raw_offset(CurHdl),
                             ok = case Offset >= RawOffSet of
                                      true  -> file_handle_cache:flush(CurHdl);
                                      false -> ok
                                  end,
                             read_from_disk(MsgLoc, State, DedupCacheEts);
                         [{Guid, Msg1, _CacheRefCount}] ->
                             ok = maybe_insert_into_cache(
                                    DedupCacheEts, RefCount, Guid, Msg1),
                             {Msg1, State}
                     end,
                 gen_server2:reply(From, {ok, Msg}),
                 State1;
        false -> [#file_summary { locked = Locked }] =
                     ets:lookup(FileSummaryEts, File),
                 case Locked of
                     true  -> add_to_pending_gc_completion({read, Guid, From},
                                                           State);
                     false -> {Msg, State1} =
                                  read_from_disk(MsgLoc, State, DedupCacheEts),
                              gen_server2:reply(From, {ok, Msg}),
                              State1
                 end
    end.

read_from_disk(#msg_location { guid = Guid, ref_count = RefCount,
                               file = File, offset = Offset,
                               total_size = TotalSize },
               State, DedupCacheEts) ->
    {Hdl, State1} = get_read_handle(File, State),
    {ok, Offset} = file_handle_cache:position(Hdl, Offset),
    {ok, {Guid, Msg}} =
        case rabbit_msg_file:read(Hdl, TotalSize) of
            {ok, {Guid, _}} = Obj ->
                Obj;
            Rest ->
                {error, {misread, [{old_state, State},
                                   {file_num,  File},
                                   {offset,    Offset},
                                   {guid,      Guid},
                                   {read,      Rest},
                                   {proc_dict, get()}
                                  ]}}
        end,
    ok = maybe_insert_into_cache(DedupCacheEts, RefCount, Guid, Msg),
    {Msg, State1}.

contains_message(Guid, From, State = #msstate { gc_active = GCActive }) ->
    case index_lookup(Guid, State) of
        not_found ->
            gen_server2:reply(From, false),
            State;
        #msg_location { file = File } ->
            case GCActive of
                {A, B} when File =:= A orelse File =:= B ->
                    add_to_pending_gc_completion(
                      {contains, Guid, From}, State);
                _ ->
                    gen_server2:reply(From, true),
                    State
            end
    end.

remove_message(Guid, State = #msstate { sum_valid_data   = SumValid,
                                        file_summary_ets = FileSummaryEts,
                                        dedup_cache_ets  = DedupCacheEts }) ->
    #msg_location { ref_count = RefCount, file = File,
                    total_size = TotalSize } = index_lookup(Guid, State),
    case RefCount of
        1 ->
            %% don't remove from CUR_FILE_CACHE_ETS_NAME here because
            %% there may be further writes in the mailbox for the same
            %% msg.
            ok = remove_cache_entry(DedupCacheEts, Guid),
            [#file_summary { valid_total_size = ValidTotalSize,
                             locked           = Locked }] =
                ets:lookup(FileSummaryEts, File),
            case Locked of
                true ->
                    add_to_pending_gc_completion({remove, Guid}, State);
                false ->
                    ok = index_delete(Guid, State),
                    ValidTotalSize1 = ValidTotalSize - TotalSize,
                    true =
                        ets:update_element(
                          FileSummaryEts, File,
                          [{#file_summary.valid_total_size, ValidTotalSize1}]),
                    State1 = delete_file_if_empty(File, State),
                    State1 #msstate { sum_valid_data = SumValid - TotalSize }
            end;
        _ when 1 < RefCount ->
            ok = decrement_cache(DedupCacheEts, Guid),
            %% only update field, otherwise bad interaction with concurrent GC
            ok = index_update_fields(Guid,
                                     {#msg_location.ref_count, RefCount - 1},
                                     State),
            State
    end.

add_to_pending_gc_completion(
  Op, State = #msstate { pending_gc_completion = Pending }) ->
    State #msstate { pending_gc_completion = [Op | Pending] }.

run_pending(State = #msstate { pending_gc_completion = [] }) ->
    State;
run_pending(State = #msstate { pending_gc_completion = Pending }) ->
    State1 = State #msstate { pending_gc_completion = [] },
    lists:foldl(fun run_pending/2, State1, lists:reverse(Pending)).

run_pending({read, Guid, From}, State) ->
    read_message(Guid, From, State);
run_pending({contains, Guid, From}, State) ->
    contains_message(Guid, From, State);
run_pending({remove, Guid}, State) ->
    remove_message(Guid, State).

safe_ets_update_counter(Tab, Key, UpdateOp, SuccessFun, FailThunk) ->
    try
        SuccessFun(ets:update_counter(Tab, Key, UpdateOp))
    catch error:badarg -> FailThunk()
    end.

safe_ets_update_counter_ok(Tab, Key, UpdateOp, FailThunk) ->
    safe_ets_update_counter(Tab, Key, UpdateOp, fun (_) -> ok end, FailThunk).

%%----------------------------------------------------------------------------
%% file helper functions
%%----------------------------------------------------------------------------

open_file(Dir, FileName, Mode) ->
    file_handle_cache:open(form_filename(Dir, FileName), ?BINARY_MODE ++ Mode,
                           [{write_buffer, ?HANDLE_CACHE_BUFFER_SIZE}]).

close_handle(Key, CState = #client_msstate { file_handle_cache = FHC }) ->
    CState #client_msstate { file_handle_cache = close_handle(Key, FHC) };

close_handle(Key, State = #msstate { file_handle_cache = FHC }) ->
    State #msstate { file_handle_cache = close_handle(Key, FHC) };

close_handle(Key, FHC) ->
    case dict:find(Key, FHC) of
        {ok, Hdl} -> ok = file_handle_cache:close(Hdl),
                     dict:erase(Key, FHC);
        error     -> FHC
    end.

mark_handle_open(FileHandlesEts, File) ->
    %% This is fine to fail (already exists)
    ets:insert_new(FileHandlesEts, {{self(), File}, open}),
    true.

mark_handle_to_close(FileHandlesEts, File) ->
    [ ets:update_element(FileHandlesEts, Key, {2, close})
      || {Key, open} <- ets:match_object(FileHandlesEts, {{'_', File}, open}) ],
    true.

close_all_indicated(#client_msstate { file_handles_ets = FileHandlesEts } =
                    CState) ->
    Objs = ets:match_object(FileHandlesEts, {{self(), '_'}, close}),
    lists:foldl(fun ({Key = {_Self, File}, close}, CStateM) ->
                        true = ets:delete(FileHandlesEts, Key),
                        close_handle(File, CStateM)
                end, CState, Objs).

close_all_handles(CState = #client_msstate { file_handles_ets = FileHandlesEts,
                                             file_handle_cache = FHC }) ->
    Self = self(),
    ok = dict:fold(fun (File, Hdl, ok) ->
                           true = ets:delete(FileHandlesEts, {Self, File}),
                           file_handle_cache:close(Hdl)
                   end, ok, FHC),
    CState #client_msstate { file_handle_cache = dict:new() };

close_all_handles(State = #msstate { file_handle_cache = FHC }) ->
    ok = dict:fold(fun (_Key, Hdl, ok) -> file_handle_cache:close(Hdl) end,
                   ok, FHC),
    State #msstate { file_handle_cache = dict:new() }.

get_read_handle(FileNum, CState = #client_msstate { file_handle_cache = FHC,
                                                    dir = Dir }) ->
    {Hdl, FHC2} = get_read_handle(FileNum, FHC, Dir),
    {Hdl, CState #client_msstate { file_handle_cache = FHC2 }};

get_read_handle(FileNum, State = #msstate { file_handle_cache = FHC,
                                            dir = Dir }) ->
    {Hdl, FHC2} = get_read_handle(FileNum, FHC, Dir),
    {Hdl, State #msstate { file_handle_cache = FHC2 }}.

get_read_handle(FileNum, FHC, Dir) ->
    case dict:find(FileNum, FHC) of
        {ok, Hdl} -> {Hdl, FHC};
        error     -> {ok, Hdl} = open_file(Dir, filenum_to_name(FileNum),
                                           ?READ_MODE),
                     {Hdl, dict:store(FileNum, Hdl, FHC)}
    end.

preallocate(Hdl, FileSizeLimit, FinalPos) ->
    {ok, FileSizeLimit} = file_handle_cache:position(Hdl, FileSizeLimit),
    ok = file_handle_cache:truncate(Hdl),
    {ok, FinalPos} = file_handle_cache:position(Hdl, FinalPos),
    ok.

truncate_and_extend_file(Hdl, Lowpoint, Highpoint) ->
    {ok, Lowpoint} = file_handle_cache:position(Hdl, Lowpoint),
    ok = file_handle_cache:truncate(Hdl),
    ok = preallocate(Hdl, Highpoint, Lowpoint).

form_filename(Dir, Name) -> filename:join(Dir, Name).

filenum_to_name(File) -> integer_to_list(File) ++ ?FILE_EXTENSION.

filename_to_num(FileName) -> list_to_integer(filename:rootname(FileName)).

list_sorted_file_names(Dir, Ext) ->
    lists:sort(fun (A, B) -> filename_to_num(A) < filename_to_num(B) end,
               filelib:wildcard("*" ++ Ext, Dir)).

%%----------------------------------------------------------------------------
%% message cache helper functions
%%----------------------------------------------------------------------------

maybe_insert_into_cache(DedupCacheEts, RefCount, Guid, Msg)
  when RefCount > 1 ->
    update_msg_cache(DedupCacheEts, Guid, Msg);
maybe_insert_into_cache(_DedupCacheEts, _RefCount, _Guid, _Msg) ->
    ok.

update_msg_cache(CacheEts, Guid, Msg) ->
    case ets:insert_new(CacheEts, {Guid, Msg, 1}) of
        true  -> ok;
        false -> safe_ets_update_counter_ok(
                   CacheEts, Guid, {3, +1},
                   fun () -> update_msg_cache(CacheEts, Guid, Msg) end)
    end.

remove_cache_entry(DedupCacheEts, Guid) ->
    true = ets:delete(DedupCacheEts, Guid),
    ok.

fetch_and_increment_cache(DedupCacheEts, Guid) ->
    case ets:lookup(DedupCacheEts, Guid) of
        [] ->
            not_found;
        [{_Guid, Msg, _RefCount}] ->
            safe_ets_update_counter_ok(
              DedupCacheEts, Guid, {3, +1},
              %% someone has deleted us in the meantime, insert us
              fun () -> ok = update_msg_cache(DedupCacheEts, Guid, Msg) end),
            Msg
    end.

decrement_cache(DedupCacheEts, Guid) ->
    true = safe_ets_update_counter(
             DedupCacheEts, Guid, {3, -1},
             fun (N) when N =< 0 -> true = ets:delete(DedupCacheEts, Guid);
                 (_N)            -> true
             end,
             %% Guid is not in there because although it's been
             %% delivered, it's never actually been read (think:
             %% persistent message held in RAM)
             fun () -> true end),
    ok.

%%----------------------------------------------------------------------------
%% index
%%----------------------------------------------------------------------------

index_lookup(Key, #client_msstate { index_module = Index,
                                    index_state  = State }) ->
    Index:lookup(Key, State);

index_lookup(Key, #msstate { index_module = Index, index_state = State }) ->
    Index:lookup(Key, State).

index_insert(Obj, #msstate { index_module = Index, index_state = State }) ->
    Index:insert(Obj, State).

index_update(Obj, #msstate { index_module = Index, index_state = State }) ->
    Index:update(Obj, State).

index_update_fields(Key, Updates, #msstate { index_module = Index,
                                             index_state  = State }) ->
    Index:update_fields(Key, Updates, State).

index_delete(Key, #msstate { index_module = Index, index_state = State }) ->
    Index:delete(Key, State).

index_delete_by_file(File, #msstate { index_module = Index,
                                      index_state  = State }) ->
    Index:delete_by_file(File, State).

%%----------------------------------------------------------------------------
%% shutdown and recovery
%%----------------------------------------------------------------------------

recover_index_and_client_refs(IndexModule, _Recover, undefined, Dir, _Server) ->
    {false, IndexModule:new(Dir), sets:new()};
recover_index_and_client_refs(IndexModule, false, _ClientRefs, Dir, Server) ->
    rabbit_log:warning("~w: rebuilding indices from scratch~n", [Server]),
    {false, IndexModule:new(Dir), sets:new()};
recover_index_and_client_refs(IndexModule, true, ClientRefs, Dir, Server) ->
    Fresh = fun (ErrorMsg, ErrorArgs) ->
                    rabbit_log:warning("~w: " ++ ErrorMsg ++ "~n"
                                       "rebuilding indices from scratch~n",
                                       [Server | ErrorArgs]),
                    {false, IndexModule:new(Dir), sets:new()}
            end,
    case read_recovery_terms(Dir) of
        {false, Error} ->
            Fresh("failed to read recovery terms: ~p", [Error]);
        {true, Terms} ->
            RecClientRefs  = proplists:get_value(client_refs, Terms, []),
            RecIndexModule = proplists:get_value(index_module, Terms),
            case (lists:sort(ClientRefs) =:= lists:sort(RecClientRefs)
                  andalso IndexModule =:= RecIndexModule) of
                true  -> case IndexModule:recover(Dir) of
                             {ok, IndexState1} ->
                                 {true, IndexState1,
                                  sets:from_list(ClientRefs)};
                             {error, Error} ->
                                 Fresh("failed to recover index: ~p", [Error])
                         end;
                false -> Fresh("recovery terms differ from present", [])
            end
    end.

store_recovery_terms(Terms, Dir) ->
    rabbit_misc:write_term_file(filename:join(Dir, ?CLEAN_FILENAME), Terms).

read_recovery_terms(Dir) ->
    Path = filename:join(Dir, ?CLEAN_FILENAME),
    case rabbit_misc:read_term_file(Path) of
        {ok, Terms}    -> case file:delete(Path) of
                              ok             -> {true,  Terms};
                              {error, Error} -> {false, Error}
                          end;
        {error, Error} -> {false, Error}
    end.

store_file_summary(Tid, Dir) ->
    ok = ets:tab2file(Tid, filename:join(Dir, ?FILE_SUMMARY_FILENAME),
                      [{extended_info, [object_count]}]).

recover_file_summary(false, _Dir) ->
    %% TODO: the only reason for this to be an *ordered*_set is so
    %% that a) maybe_compact can start a traversal from the eldest
    %% file, and b) build_index in fast recovery mode can easily
    %% identify the current file. It's awkward to have both that
    %% odering and the left/right pointers in the entries - replacing
    %% the former with some additional bit of state would be easy, but
    %% ditching the latter would be neater.
    {false, ets:new(rabbit_msg_store_file_summary,
                    [ordered_set, public, {keypos, #file_summary.file}])};
recover_file_summary(true, Dir) ->
    Path = filename:join(Dir, ?FILE_SUMMARY_FILENAME),
    case ets:file2tab(Path) of
        {ok, Tid}       -> file:delete(Path),
                          {true, Tid};
        {error, _Error} -> recover_file_summary(false, Dir)
    end.

count_msg_refs(Gen, Seed, State) ->
    case Gen(Seed) of
        finished ->
            ok;
        {_Guid, 0, Next} ->
            count_msg_refs(Gen, Next, State);
        {Guid, Delta, Next} ->
            ok = case index_lookup(Guid, State) of
                     not_found ->
                         index_insert(#msg_location { guid = Guid,
                                                      file = undefined,
                                                      ref_count = Delta },
                                      State);
                     #msg_location { ref_count = RefCount } = StoreEntry ->
                         NewRefCount = RefCount + Delta,
                         case NewRefCount of
                             0 -> index_delete(Guid, State);
                             _ -> index_update(StoreEntry #msg_location {
                                                 ref_count = NewRefCount },
                                               State)
                         end
                 end,
            count_msg_refs(Gen, Next, State)
    end.

recover_crashed_compactions(Dir) ->
    FileNames =    list_sorted_file_names(Dir, ?FILE_EXTENSION),
    TmpFileNames = list_sorted_file_names(Dir, ?FILE_EXTENSION_TMP),
    lists:foreach(
      fun (TmpFileName) ->
              NonTmpRelatedFileName =
                  filename:rootname(TmpFileName) ++ ?FILE_EXTENSION,
              true = lists:member(NonTmpRelatedFileName, FileNames),
              ok = recover_crashed_compaction(
                     Dir, TmpFileName, NonTmpRelatedFileName)
      end, TmpFileNames),
    TmpFileNames == [].

recover_crashed_compaction(Dir, TmpFileName, NonTmpRelatedFileName) ->
    %% Because a msg can legitimately appear multiple times in the
    %% same file, identifying the contents of the tmp file and where
    %% they came from is non-trivial. If we are recovering a crashed
    %% compaction then we will be rebuilding the index, which can cope
    %% with duplicates appearing. Thus the simplest and safest thing
    %% to do is to append the contents of the tmp file to its main
    %% file.
    {ok, TmpHdl}  = open_file(Dir, TmpFileName, ?READ_MODE),
    {ok, MainHdl} = open_file(Dir, NonTmpRelatedFileName,
                              ?READ_MODE ++ ?WRITE_MODE),
    {ok, _End} = file_handle_cache:position(MainHdl, eof),
    Size = filelib:file_size(form_filename(Dir, TmpFileName)),
    {ok, Size} = file_handle_cache:copy(TmpHdl, MainHdl, Size),
    ok = file_handle_cache:close(MainHdl),
    ok = file_handle_cache:delete(TmpHdl),
    ok.

scan_file_for_valid_messages(Dir, FileName) ->
    case open_file(Dir, FileName, ?READ_MODE) of
        {ok, Hdl}       -> Valid = rabbit_msg_file:scan(
                                     Hdl, filelib:file_size(
                                            form_filename(Dir, FileName))),
                           %% if something really bad has happened,
                           %% the close could fail, but ignore
                           file_handle_cache:close(Hdl),
                           Valid;
        {error, enoent} -> {ok, [], 0};
        {error, Reason} -> {error, {unable_to_scan_file, FileName, Reason}}
    end.

%% Takes the list in *ascending* order (i.e. eldest message
%% first). This is the opposite of what scan_file_for_valid_messages
%% produces. The list of msgs that is produced is youngest first.
drop_contiguous_block_prefix(L) -> drop_contiguous_block_prefix(L, 0).

drop_contiguous_block_prefix([], ExpectedOffset) ->
    {ExpectedOffset, []};
drop_contiguous_block_prefix([#msg_location { offset = ExpectedOffset,
                                              total_size = TotalSize } | Tail],
                             ExpectedOffset) ->
    ExpectedOffset1 = ExpectedOffset + TotalSize,
    drop_contiguous_block_prefix(Tail, ExpectedOffset1);
drop_contiguous_block_prefix(MsgsAfterGap, ExpectedOffset) ->
    {ExpectedOffset, MsgsAfterGap}.

build_index(true, _StartupFunState,
            State = #msstate { file_summary_ets = FileSummaryEts }) ->
    ets:foldl(
      fun (#file_summary { valid_total_size = ValidTotalSize,
                           file_size        = FileSize,
                           file             = File },
           {_Offset, State1 = #msstate { sum_valid_data = SumValid,
                                         sum_file_size  = SumFileSize }}) ->
              {FileSize, State1 #msstate {
                           sum_valid_data = SumValid + ValidTotalSize,
                           sum_file_size  = SumFileSize + FileSize,
                           current_file   = File }}
      end, {0, State}, FileSummaryEts);
build_index(false, {MsgRefDeltaGen, MsgRefDeltaGenInit},
            State = #msstate { dir = Dir }) ->
    ok = count_msg_refs(MsgRefDeltaGen, MsgRefDeltaGenInit, State),
    {ok, Pid} = gatherer:start_link(),
    case [filename_to_num(FileName) ||
             FileName <- list_sorted_file_names(Dir, ?FILE_EXTENSION)] of
        []     -> build_index(Pid, undefined, [State #msstate.current_file],
                              State);
        Files  -> {Offset, State1} = build_index(Pid, undefined, Files, State),
                  {Offset, lists:foldl(fun delete_file_if_empty/2,
                                       State1, Files)}
    end.

build_index(Gatherer, Left, [],
            State = #msstate { file_summary_ets = FileSummaryEts,
                               sum_valid_data   = SumValid,
                               sum_file_size    = SumFileSize }) ->
    case gatherer:out(Gatherer) of
        empty ->
            ok = gatherer:stop(Gatherer),
            ok = rabbit_misc:unlink_and_capture_exit(Gatherer),
            ok = index_delete_by_file(undefined, State),
            Offset = case ets:lookup(FileSummaryEts, Left) of
                         []                                       -> 0;
                         [#file_summary { file_size = FileSize }] -> FileSize
                     end,
            {Offset, State #msstate { current_file = Left }};
        {value, #file_summary { valid_total_size = ValidTotalSize,
                                file_size = FileSize } = FileSummary} ->
            true = ets:insert_new(FileSummaryEts, FileSummary),
            build_index(Gatherer, Left, [],
                        State #msstate {
                          sum_valid_data = SumValid + ValidTotalSize,
                          sum_file_size  = SumFileSize + FileSize })
    end;
build_index(Gatherer, Left, [File|Files], State) ->
    ok = gatherer:fork(Gatherer),
    ok = worker_pool:submit_async(
           fun () -> build_index_worker(Gatherer, State,
                                        Left, File, Files)
           end),
    build_index(Gatherer, File, Files, State).

build_index_worker(Gatherer, State = #msstate { dir = Dir },
                   Left, File, Files) ->
    {ok, Messages, FileSize} =
        scan_file_for_valid_messages(Dir, filenum_to_name(File)),
    {ValidMessages, ValidTotalSize} =
        lists:foldl(
          fun (Obj = {Guid, TotalSize, Offset}, {VMAcc, VTSAcc}) ->
                  case index_lookup(Guid, State) of
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
    {Right, FileSize1} =
        case Files of
            %% if it's the last file, we'll truncate to remove any
            %% rubbish above the last valid message. This affects the
            %% file size.
            []    -> {undefined, case ValidMessages of
                                     [] -> 0;
                                     _  -> {_Guid, TotalSize, Offset} =
                                               lists:last(ValidMessages),
                                           Offset + TotalSize
                                 end};
            [F|_] -> {F, FileSize}
        end,
    ok = gatherer:in(Gatherer, #file_summary {
                       file             = File,
                       valid_total_size = ValidTotalSize,
                       left             = Left,
                       right            = Right,
                       file_size        = FileSize1,
                       locked           = false,
                       readers          = 0 }),
    ok = gatherer:finish(Gatherer).

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
    ok = file_handle_cache:close(CurHdl),
    NextFile = CurFile + 1,
    {ok, NextHdl} = open_file(Dir, filenum_to_name(NextFile), ?WRITE_MODE),
    true = ets:insert_new(FileSummaryEts, #file_summary {
                            file             = NextFile,
                            valid_total_size = 0,
                            left             = CurFile,
                            right            = undefined,
                            file_size        = 0,
                            locked           = false,
                            readers          = 0 }),
    true = ets:update_element(FileSummaryEts, CurFile,
                              {#file_summary.right, NextFile}),
    true = ets:match_delete(CurFileCacheEts, {'_', '_', 0}),
    maybe_compact(State1 #msstate { current_file_handle = NextHdl,
                                    current_file        = NextFile });
maybe_roll_to_new_file(_, State) ->
    State.

maybe_compact(State = #msstate { sum_valid_data   = SumValid,
                                 sum_file_size    = SumFileSize,
                                 gc_active        = false,
                                 gc_pid           = GCPid,
                                 file_summary_ets = FileSummaryEts,
                                 file_size_limit  = FileSizeLimit })
  when (SumFileSize > 2 * FileSizeLimit andalso
        (SumFileSize - SumValid) / SumFileSize > ?GARBAGE_FRACTION) ->
    %% TODO: the algorithm here is sub-optimal - it may result in a
    %% complete traversal of FileSummaryEts.
    case ets:first(FileSummaryEts) of
        '$end_of_table' ->
            State;
        First ->
            case find_files_to_gc(FileSummaryEts, FileSizeLimit,
                                  ets:lookup(FileSummaryEts, First)) of
                not_found ->
                    State;
                {Src, Dst} ->
                    State1 = close_handle(Src, close_handle(Dst, State)),
                    true = ets:update_element(FileSummaryEts, Src,
                                              {#file_summary.locked, true}),
                    true = ets:update_element(FileSummaryEts, Dst,
                                              {#file_summary.locked, true}),
                    ok = rabbit_msg_store_gc:gc(GCPid, Src, Dst),
                    State1 #msstate { gc_active = {Src, Dst} }
            end
    end;
maybe_compact(State) ->
    State.

find_files_to_gc(FileSummaryEts, FileSizeLimit,
                 [#file_summary { file             = Dst,
                                  valid_total_size = DstValid,
                                  right            = Src }]) ->
    case Src of
        undefined ->
            not_found;
        _   ->
            [#file_summary { file             = Src,
                             valid_total_size = SrcValid,
                             left             = Dst,
                             right            = SrcRight }] = Next =
                ets:lookup(FileSummaryEts, Src),
            case SrcRight of
                undefined -> not_found;
                _         -> case DstValid + SrcValid =< FileSizeLimit of
                                 true  -> {Src, Dst};
                                 false -> find_files_to_gc(
                                            FileSummaryEts, FileSizeLimit, Next)
                             end
            end
    end.

delete_file_if_empty(File, State = #msstate { current_file = File }) ->
    State;
delete_file_if_empty(File, State = #msstate {
                             dir              = Dir,
                             sum_file_size    = SumFileSize,
                             file_handles_ets = FileHandlesEts,
                             file_summary_ets = FileSummaryEts }) ->
    [#file_summary { valid_total_size = ValidData,
                     left             = Left,
                     right            = Right,
                     file_size        = FileSize,
                     locked           = false }] =
        ets:lookup(FileSummaryEts, File),
    case ValidData of
        %% we should NEVER find the current file in here hence right
        %% should always be a file, not undefined
        0 -> case {Left, Right} of
                 {undefined, _} when Right =/= undefined ->
                     %% the eldest file is empty.
                     true = ets:update_element(
                              FileSummaryEts, Right,
                              {#file_summary.left, undefined});
                 {_, _} when Right =/= undefined ->
                     true = ets:update_element(FileSummaryEts, Right,
                                               {#file_summary.left, Left}),
                     true = ets:update_element(FileSummaryEts, Left,
                                               {#file_summary.right, Right})
             end,
             true = mark_handle_to_close(FileHandlesEts, File),
             true = ets:delete(FileSummaryEts, File),
             State1 = close_handle(File, State),
             ok = file:delete(form_filename(Dir, filenum_to_name(File))),
             State1 #msstate { sum_file_size = SumFileSize - FileSize };
        _ -> State
    end.

%%----------------------------------------------------------------------------
%% garbage collection / compaction / aggregation -- external
%%----------------------------------------------------------------------------

gc(SrcFile, DstFile, State = {FileSummaryEts, _Dir, _Index, _IndexState}) ->
    [SrcObj = #file_summary {
       readers          = SrcReaders,
       left             = DstFile,
       file_size        = SrcFileSize,
       locked           = true }] = ets:lookup(FileSummaryEts, SrcFile),
    [DstObj = #file_summary {
       readers          = DstReaders,
       right            = SrcFile,
       file_size        = DstFileSize,
       locked           = true }] = ets:lookup(FileSummaryEts, DstFile),

    case SrcReaders =:= 0 andalso DstReaders =:= 0 of
        true  -> TotalValidData = combine_files(SrcObj, DstObj, State),
                 %% don't update dest.right, because it could be
                 %% changing at the same time
                 true = ets:update_element(
                          FileSummaryEts, DstFile,
                          [{#file_summary.valid_total_size, TotalValidData},
                           {#file_summary.file_size,        TotalValidData}]),
                 SrcFileSize + DstFileSize - TotalValidData;
        false -> concurrent_readers
    end.

combine_files(#file_summary { file             = Source,
                              valid_total_size = SourceValid,
                              left             = Destination },
              #file_summary { file             = Destination,
                              valid_total_size = DestinationValid,
                              right            = Source },
              State = {_FileSummaryEts, Dir, _Index, _IndexState}) ->
    SourceName      = filenum_to_name(Source),
    DestinationName = filenum_to_name(Destination),
    {ok, SourceHdl}      = open_file(Dir, SourceName,
                                     ?READ_AHEAD_MODE),
    {ok, DestinationHdl} = open_file(Dir, DestinationName,
                                     ?READ_AHEAD_MODE ++ ?WRITE_MODE),
    ExpectedSize = SourceValid + DestinationValid,
    %% if DestinationValid =:= DestinationContiguousTop then we don't
    %% need a tmp file
    %% if they're not equal, then we need to write out everything past
    %%   the DestinationContiguousTop to a tmp file then truncate,
    %%   copy back in, and then copy over from Source
    %% otherwise we just truncate straight away and copy over from Source
    {DestinationWorkList, DestinationValid} =
        find_unremoved_messages_in_file(Destination, State),
    {DestinationContiguousTop, DestinationWorkListTail} =
        drop_contiguous_block_prefix(DestinationWorkList),
    case DestinationWorkListTail of
        [] -> ok = truncate_and_extend_file(
                     DestinationHdl, DestinationContiguousTop, ExpectedSize);
        _  -> Tmp = filename:rootname(DestinationName) ++ ?FILE_EXTENSION_TMP,
              {ok, TmpHdl} = open_file(Dir, Tmp, ?READ_AHEAD_MODE++?WRITE_MODE),
              ok = copy_messages(
                     DestinationWorkListTail, DestinationContiguousTop,
                     DestinationValid, DestinationHdl, TmpHdl, Destination,
                     State),
              TmpSize = DestinationValid - DestinationContiguousTop,
              %% so now Tmp contains everything we need to salvage
              %% from Destination, and index_state has been updated to
              %% reflect the compaction of Destination so truncate
              %% Destination and copy from Tmp back to the end
              {ok, 0} = file_handle_cache:position(TmpHdl, 0),
              ok = truncate_and_extend_file(
                     DestinationHdl, DestinationContiguousTop, ExpectedSize),
              {ok, TmpSize} =
                  file_handle_cache:copy(TmpHdl, DestinationHdl, TmpSize),
              %% position in DestinationHdl should now be DestinationValid
              ok = file_handle_cache:sync(DestinationHdl),
              ok = file_handle_cache:delete(TmpHdl)
    end,
    {SourceWorkList, SourceValid} =
        find_unremoved_messages_in_file(Source, State),
    ok = copy_messages(SourceWorkList, DestinationValid, ExpectedSize,
                       SourceHdl, DestinationHdl, Destination, State),
    %% tidy up
    ok = file_handle_cache:close(DestinationHdl),
    ok = file_handle_cache:delete(SourceHdl),
    ExpectedSize.

find_unremoved_messages_in_file(File,
                                {_FileSummaryEts, Dir, Index, IndexState}) ->
    %% Messages here will be end-of-file at start-of-list
    {ok, Messages, _FileSize} =
        scan_file_for_valid_messages(Dir, filenum_to_name(File)),
    %% foldl will reverse so will end up with msgs in ascending offset order
    lists:foldl(fun ({Guid, TotalSize, Offset}, Acc = {List, Size}) ->
                        case Index:lookup(Guid, IndexState) of
                            #msg_location { file = File, total_size = TotalSize,
                                            offset = Offset } = Entry ->
                                {[ Entry | List ], TotalSize + Size};
                            _ ->
                                Acc
                        end
                end, {[], 0}, Messages).

copy_messages(WorkList, InitOffset, FinalOffset, SourceHdl, DestinationHdl,
              Destination, {_FileSummaryEts, _Dir, Index, IndexState}) ->
    Copy = fun ({BlockStart, BlockEnd}) ->
                   BSize = BlockEnd - BlockStart,
                   {ok, BlockStart} =
                       file_handle_cache:position(SourceHdl, BlockStart),
                   {ok, BSize} =
                       file_handle_cache:copy(SourceHdl, DestinationHdl, BSize)
           end,
    case
        lists:foldl(
          fun (#msg_location { guid = Guid, offset = Offset,
                               total_size = TotalSize },
               {CurOffset, Block = {BlockStart, BlockEnd}}) ->
                  %% CurOffset is in the DestinationFile.
                  %% Offset, BlockStart and BlockEnd are in the SourceFile
                  %% update MsgLocation to reflect change of file and offset
                  ok = Index:update_fields(Guid,
                                           [{#msg_location.file, Destination},
                                            {#msg_location.offset, CurOffset}],
                                           IndexState),
                  {CurOffset + TotalSize,
                   case BlockEnd of
                       undefined ->
                           %% base case, called only for the first list elem
                           {Offset, Offset + TotalSize};
                       Offset ->
                           %% extend the current block because the
                           %% next msg follows straight on
                           {BlockStart, BlockEnd + TotalSize};
                       _ ->
                           %% found a gap, so actually do the work for
                           %% the previous block
                           Copy(Block),
                           {Offset, Offset + TotalSize}
                   end}
          end, {InitOffset, {undefined, undefined}}, WorkList) of
        {FinalOffset, Block} ->
            case WorkList of
                [] -> ok;
                _  -> Copy(Block), %% do the last remaining block
                      ok = file_handle_cache:sync(DestinationHdl)
            end;
        {FinalOffsetZ, _Block} ->
            {gc_error, [{expected, FinalOffset},
                        {got, FinalOffsetZ},
                        {destination, Destination}]}
    end.
