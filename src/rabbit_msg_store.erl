%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_msg_store).

-behaviour(gen_server2).

-export([start_link/4, start_global_store_link/4, successfully_recovered_state/1,
         client_init/4, client_terminate/1, client_delete_and_terminate/1,
         client_ref/1, close_all_indicated/1,
         write/3, write_flow/3, read/2, contains/2, remove/2]).

-export([set_maximum_since_use/2, has_readers/2, combine_files/3,
         delete_file/2]). %% internal

-export([transform_dir/3, force_recovery/2]). %% upgrade

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, prioritise_call/4, prioritise_cast/3,
         prioritise_info/3, format_message_queue/2]).

%%----------------------------------------------------------------------------

-include("rabbit_msg_store.hrl").

-define(SYNC_INTERVAL,  25).   %% milliseconds
-define(CLEAN_FILENAME, "clean.dot").
-define(FILE_SUMMARY_FILENAME, "file_summary.ets").
-define(TRANSFORM_TMP, "transform_tmp").

-define(BINARY_MODE,     [raw, binary]).
-define(READ_MODE,       [read]).
-define(READ_AHEAD_MODE, [read_ahead | ?READ_MODE]).
-define(WRITE_MODE,      [write]).

-define(FILE_EXTENSION,        ".rdq").
-define(FILE_EXTENSION_TMP,    ".rdt").

-define(HANDLE_CACHE_BUFFER_SIZE, 1048576). %% 1MB

 %% i.e. two pairs, so GC does not go idle when busy
-define(MAXIMUM_SIMULTANEOUS_GC_FILES, 4).

%%----------------------------------------------------------------------------

-record(msstate,
        {
          %% store directory
          dir,
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
          file_handle_cache,
          %% TRef for our interval timer
          sync_timer_ref,
          %% sum of valid data in all files
          sum_valid_data,
          %% sum of file sizes
          sum_file_size,
          %% things to do once GC completes
          pending_gc_completion,
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
          file_handle_cache,
          index_state,
          index_module,
          dir,
          gc_pid,
          file_handles_ets,
          file_summary_ets,
          cur_file_cache_ets,
          flying_ets,
          credit_disc_bound
        }).

-record(file_summary,
        {file, valid_total_size, left, right, file_size, locked, readers}).

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
                      file_handle_cache  :: map(),
                      index_state        :: any(),
                      index_module       :: atom(),
                      dir                :: file:filename(),
                      gc_pid             :: pid(),
                      file_handles_ets   :: ets:tid(),
                      file_summary_ets   :: ets:tid(),
                      cur_file_cache_ets :: ets:tid(),
                      flying_ets         :: ets:tid(),
                      credit_disc_bound  :: {pos_integer(), pos_integer()}}.
-type msg_ref_delta_gen(A) ::
        fun ((A) -> 'finished' |
                    {rabbit_types:msg_id(), non_neg_integer(), A}).
-type maybe_msg_id_fun() ::
        'undefined' | fun ((gb_sets:set(), 'written' | 'ignored') -> any()).
-type maybe_close_fds_fun() :: 'undefined' | fun (() -> 'ok').
-type deletion_thunk() :: fun (() -> boolean()).

%%----------------------------------------------------------------------------

%% We run GC whenever (garbage / sum_file_size) > ?GARBAGE_FRACTION
%% It is not recommended to set this to < 0.5
-define(GARBAGE_FRACTION,      0.5).

%% Message store is responsible for storing messages
%% on disk and loading them back. The store handles both
%% persistent messages and transient ones (when a node
%% is under RAM pressure and needs to page messages out
%% to disk). The store is responsible for locating messages
%% on disk and maintaining an index.
%%
%% There are two message stores per node: one for transient
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
%% the file is closed and a new file is created on the _right_ of the
%% old file which is used for new messages. Files are named
%% numerically ascending, thus the file with the lowest name is the
%% eldest file.
%%
%% We need to keep track of which messages are in which files (this is
%% the index); how much useful data is in each file and which files
%% are on the left and right of each other. This is the purpose of the
%% file summary ETS table.
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
%% emptied (and hence deleted).
%%
%% Given the compaction between two files, the left file (i.e. elder
%% file) is considered the ultimate destination for the good data in
%% the right file. If necessary, the good data in the left file which
%% is fragmented throughout the file is written out to a temporary
%% file, then read back in to form a contiguous chunk of good data at
%% the start of the left file. Thus the left file is garbage collected
%% and compacted. Then the good data from the right file is copied
%% onto the end of the left file. Index and file summary tables are
%% updated.
%%
%% On non-clean startup, we scan the files we discover, dealing with
%% the possibilities of a crash having occurred during a compaction
%% (this consists of tidyup - the compaction is deliberately designed
%% such that data is duplicated on disk rather than risking it being
%% lost), and rebuild the file summary and index ETS table.
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
%% case is that every time it is rewritten, it moves one position lower
%% in the file (for it to stay at the same position requires that
%% there are no holes beneath it, which means truncate would be used
%% and so it would not be rewritten at all). Thus this seems to
%% suggest the limit is the number of messages ahead of it in the
%% queue, though it's likely that that's pessimistic, given the
%% requirements for compaction/combination of files.
%%
%% The other property that we have is the bound on the lowest
%% utilisation, which should be 50% - worst case is that all files are
%% fractionally over half full and can't be combined (equivalent is
%% alternating full files and files with only one tiny message in
%% them).
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
%% When a message is removed, its reference count is decremented. Even
%% if the reference count becomes 0, its entry is not removed. This is
%% because in the event of the same message being sent to several
%% different queues, there is the possibility of one queue writing and
%% removing the message before other queues write it at all. Thus
%% accommodating 0-reference counts allows us to avoid unnecessary
%% writes here. Of course, there are complications: the file to which
%% the message has already been written could be locked pending
%% deletion or GC, which means we have to rewrite the message as the
%% original copy will now be lost.
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
%%
%% Client termination messages are stored in a separate ets index to
%% avoid filling primary message store index and message files with
%% client termination messages.
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
        (atom(), file:filename(), [binary()] | 'undefined',
         {msg_ref_delta_gen(A), A}) -> rabbit_types:ok_pid_or_error().

start_link(Type, Dir, ClientRefs, StartupFunState) when is_atom(Type) ->
    gen_server2:start_link(?MODULE,
                           [Type, Dir, ClientRefs, StartupFunState],
                           [{timeout, infinity}]).

start_global_store_link(Type, Dir, ClientRefs, StartupFunState) when is_atom(Type) ->
    gen_server2:start_link({local, Type}, ?MODULE,
                           [Type, Dir, ClientRefs, StartupFunState],
                           [{timeout, infinity}]).

-spec successfully_recovered_state(server()) -> boolean().

successfully_recovered_state(Server) ->
    gen_server2:call(Server, successfully_recovered_state, infinity).

-spec client_init(server(), client_ref(), maybe_msg_id_fun(),
                        maybe_close_fds_fun()) -> client_msstate().

client_init(Server, Ref, MsgOnDiskFun, CloseFDsFun) when is_pid(Server); is_atom(Server) ->
    {IState, IModule, Dir, GCPid,
     FileHandlesEts, FileSummaryEts, CurFileCacheEts, FlyingEts} =
        gen_server2:call(
          Server, {new_client_state, Ref, self(), MsgOnDiskFun, CloseFDsFun},
          infinity),
    CreditDiscBound = rabbit_misc:get_env(rabbit, msg_store_credit_disc_bound,
                                          ?CREDIT_DISC_BOUND),
    #client_msstate { server             = Server,
                      client_ref         = Ref,
                      file_handle_cache  = #{},
                      index_state        = IState,
                      index_module       = IModule,
                      dir                = Dir,
                      gc_pid             = GCPid,
                      file_handles_ets   = FileHandlesEts,
                      file_summary_ets   = FileSummaryEts,
                      cur_file_cache_ets = CurFileCacheEts,
                      flying_ets         = FlyingEts,
                      credit_disc_bound  = CreditDiscBound }.

-spec client_terminate(client_msstate()) -> 'ok'.

client_terminate(CState = #client_msstate { client_ref = Ref }) ->
    close_all_handles(CState),
    ok = server_call(CState, {client_terminate, Ref}).

-spec client_delete_and_terminate(client_msstate()) -> 'ok'.

client_delete_and_terminate(CState = #client_msstate { client_ref = Ref }) ->
    close_all_handles(CState),
    ok = server_cast(CState, {client_dying, Ref}),
    ok = server_cast(CState, {client_delete, Ref}).

-spec client_ref(client_msstate()) -> client_ref().

client_ref(#client_msstate { client_ref = Ref }) -> Ref.

-spec write_flow(rabbit_types:msg_id(), msg(), client_msstate()) -> 'ok'.

write_flow(MsgId, Msg,
           CState = #client_msstate {
                       server = Server,
                       credit_disc_bound = CreditDiscBound }) ->
    %% Here we are tracking messages sent by the
    %% rabbit_amqqueue_process process via the
    %% rabbit_variable_queue. We are accessing the
    %% rabbit_amqqueue_process process dictionary.
    credit_flow:send(Server, CreditDiscBound),
    client_write(MsgId, Msg, flow, CState).

-spec write(rabbit_types:msg_id(), msg(), client_msstate()) -> 'ok'.

write(MsgId, Msg, CState) -> client_write(MsgId, Msg, noflow, CState).

-spec read(rabbit_types:msg_id(), client_msstate()) ->
                     {rabbit_types:ok(msg()) | 'not_found', client_msstate()}.

read(MsgId,
     CState = #client_msstate { cur_file_cache_ets = CurFileCacheEts }) ->
    file_handle_cache_stats:update(msg_store_read),
    %% Check the cur file cache
    case ets:lookup(CurFileCacheEts, MsgId) of
        [] ->
            Defer = fun() -> {server_call(CState, {read, MsgId}), CState} end,
            case index_lookup_positive_ref_count(MsgId, CState) of
                not_found   -> Defer();
                MsgLocation -> client_read1(MsgLocation, Defer, CState)
            end;
        [{MsgId, Msg, _CacheRefCount}] ->
            {{ok, Msg}, CState}
    end.

-spec contains(rabbit_types:msg_id(), client_msstate()) -> boolean().

contains(MsgId, CState) -> server_call(CState, {contains, MsgId}).

-spec remove([rabbit_types:msg_id()], client_msstate()) -> 'ok'.

remove([],    _CState) -> ok;
remove(MsgIds, CState = #client_msstate { client_ref = CRef }) ->
    [client_update_flying(-1, MsgId, CState) || MsgId <- MsgIds],
    server_cast(CState, {remove, CRef, MsgIds}).

-spec set_maximum_since_use(server(), non_neg_integer()) -> 'ok'.

set_maximum_since_use(Server, Age) when is_pid(Server); is_atom(Server) ->
    gen_server2:cast(Server, {set_maximum_since_use, Age}).

%%----------------------------------------------------------------------------
%% Client-side-only helpers
%%----------------------------------------------------------------------------

server_call(#client_msstate { server = Server }, Msg) ->
    gen_server2:call(Server, Msg, infinity).

server_cast(#client_msstate { server = Server }, Msg) ->
    gen_server2:cast(Server, Msg).

client_write(MsgId, Msg, Flow,
             CState = #client_msstate { cur_file_cache_ets = CurFileCacheEts,
                                        client_ref         = CRef }) ->
    file_handle_cache_stats:update(msg_store_write),
    ok = client_update_flying(+1, MsgId, CState),
    ok = update_msg_cache(CurFileCacheEts, MsgId, Msg),
    ok = server_cast(CState, {write, CRef, MsgId, Flow}).

client_read1(#msg_location { msg_id = MsgId, file = File } = MsgLocation, Defer,
             CState = #client_msstate { file_summary_ets = FileSummaryEts }) ->
    case ets:lookup(FileSummaryEts, File) of
        [] -> %% File has been GC'd and no longer exists. Go around again.
            read(MsgId, CState);
        [#file_summary { locked = Locked, right = Right }] ->
            client_read2(Locked, Right, MsgLocation, Defer, CState)
    end.

client_read2(false, undefined, _MsgLocation, Defer, _CState) ->
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
client_read2(true, _Right, _MsgLocation, Defer, _CState) ->
    %% Of course, in the mean time, the GC could have run and our msg
    %% is actually in a different file, unlocked. However, deferring is
    %% the safest and simplest thing to do.
    Defer();
client_read2(false, _Right,
             MsgLocation = #msg_location { msg_id = MsgId, file = File },
             Defer,
             CState = #client_msstate { file_summary_ets = FileSummaryEts }) ->
    %% It's entirely possible that everything we're doing from here on
    %% is for the wrong file, or a non-existent file, as a GC may have
    %% finished.
    safe_ets_update_counter(
      FileSummaryEts, File, {#file_summary.readers, +1},
      fun (_) -> client_read3(MsgLocation, Defer, CState) end,
      fun () -> read(MsgId, CState) end).

client_read3(#msg_location { msg_id = MsgId, file = File }, Defer,
             CState = #client_msstate { file_handles_ets = FileHandlesEts,
                                        file_summary_ets = FileSummaryEts,
                                        gc_pid           = GCPid,
                                        client_ref       = Ref }) ->
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
            read(MsgId, CState);
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
            catch error:badarg -> read(MsgId, CState)
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
            case index_lookup(MsgId, CState) of
                #msg_location { file = File } = MsgLocation ->
                    %% Still the same file.
                    {ok, CState1} = close_all_indicated(CState),
                    %% We are now guaranteed that the mark_handle_open
                    %% call will either insert_new correctly, or will
                    %% fail, but find the value is open, not close.
                    mark_handle_open(FileHandlesEts, File, Ref),
                    %% Could the msg_store now mark the file to be
                    %% closed? No: marks for closing are issued only
                    %% when the msg_store has locked the file.
                    %% This will never be the current file
                    {Msg, CState2} = read_from_disk(MsgLocation, CState1),
                    Release(), %% this MUST NOT fail with badarg
                    {{ok, Msg}, CState2};
                #msg_location {} = MsgLocation -> %% different file!
                    Release(), %% this MUST NOT fail with badarg
                    client_read1(MsgLocation, Defer, CState);
                not_found -> %% it seems not to exist. Defer, just to be sure.
                    try Release() %% this can badarg, same as locked case, above
                    catch error:badarg -> ok
                    end,
                    Defer()
            end
    end.

client_update_flying(Diff, MsgId, #client_msstate { flying_ets = FlyingEts,
                                                    client_ref = CRef }) ->
    Key = {MsgId, CRef},
    case ets:insert_new(FlyingEts, {Key, Diff}) of
        true  -> ok;
        false -> try ets:update_counter(FlyingEts, Key, {2, Diff}) of
                     0    -> ok;
                     Diff -> ok;
                     Err  -> throw({bad_flying_ets_update, Diff, Err, Key})
                 catch error:badarg ->
                         %% this is guaranteed to succeed since the
                         %% server only removes and updates flying_ets
                         %% entries; it never inserts them
                         true = ets:insert_new(FlyingEts, {Key, Diff})
                 end,
                 ok
    end.

clear_client(CRef, State = #msstate { cref_to_msg_ids = CTM,
                                      dying_clients = DyingClients }) ->
    State #msstate { cref_to_msg_ids = maps:remove(CRef, CTM),
                     dying_clients = maps:remove(CRef, DyingClients) }.


%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------


init([Type, BaseDir, ClientRefs, StartupFunState]) ->
    process_flag(trap_exit, true),

    ok = file_handle_cache:register_callback(?MODULE, set_maximum_since_use,
                                             [self()]),

    Dir = filename:join(BaseDir, atom_to_list(Type)),
    Name = filename:join(filename:basename(BaseDir), atom_to_list(Type)),

    {ok, IndexModule} = application:get_env(rabbit, msg_store_index_module),
    rabbit_log:info("Message store ~tp: using ~p to provide index~n", [Name, IndexModule]),

    AttemptFileSummaryRecovery =
        case ClientRefs of
            undefined -> ok = rabbit_file:recursive_delete([Dir]),
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
                                      ClientRefs, Dir, Name),
    Clients = maps:from_list(
                [{CRef, {undefined, undefined, undefined}} ||
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
    CurFileCacheEts = ets:new(rabbit_msg_store_cur_file, [set, public]),
    FlyingEts       = ets:new(rabbit_msg_store_flying, [set, public]),

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
                       file_handle_cache      = #{},
                       sync_timer_ref         = undefined,
                       sum_valid_data         = 0,
                       sum_file_size          = 0,
                       pending_gc_completion  = maps:new(),
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
    {Offset, State1 = #msstate { current_file = CurFile }} =
        build_index(CleanShutdown, StartupFunState, State),
    %% read is only needed so that we can seek
    {ok, CurHdl} = open_file(Dir, filenum_to_name(CurFile),
                             [read | ?WRITE_MODE]),
    {ok, Offset} = file_handle_cache:position(CurHdl, Offset),
    ok = file_handle_cache:truncate(CurHdl),

    {ok, maybe_compact(State1 #msstate { current_file_handle = CurHdl }),
     hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of
        successfully_recovered_state                        -> 7;
        {new_client_state, _Ref, _Pid, _MODC, _CloseFDsFun} -> 7;
        {read, _MsgId}                                      -> 2;
        _                                                   -> 0
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {combine_files, _Source, _Destination, _Reclaimed} -> 8;
        {delete_file, _File, _Reclaimed}                   -> 8;
        {set_maximum_since_use, _Age}                      -> 8;
        {client_dying, _Pid}                               -> 7;
        _                                                  -> 0
    end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of
        sync                                               -> 8;
        _                                                  -> 0
    end.

handle_call(successfully_recovered_state, _From, State) ->
    reply(State #msstate.successfully_recovered, State);

handle_call({new_client_state, CRef, CPid, MsgOnDiskFun, CloseFDsFun}, _From,
            State = #msstate { dir                = Dir,
                               index_state        = IndexState,
                               index_module       = IndexModule,
                               file_handles_ets   = FileHandlesEts,
                               file_summary_ets   = FileSummaryEts,
                               cur_file_cache_ets = CurFileCacheEts,
                               flying_ets         = FlyingEts,
                               clients            = Clients,
                               gc_pid             = GCPid }) ->
    Clients1 = maps:put(CRef, {CPid, MsgOnDiskFun, CloseFDsFun}, Clients),
    erlang:monitor(process, CPid),
    reply({IndexState, IndexModule, Dir, GCPid, FileHandlesEts, FileSummaryEts,
           CurFileCacheEts, FlyingEts},
          State #msstate { clients = Clients1 });

handle_call({client_terminate, CRef}, _From, State) ->
    reply(ok, clear_client(CRef, State));

handle_call({read, MsgId}, From, State) ->
    State1 = read_message(MsgId, From, State),
    noreply(State1);

handle_call({contains, MsgId}, From, State) ->
    State1 = contains_message(MsgId, From, State),
    noreply(State1).

handle_cast({client_dying, CRef},
            State = #msstate { dying_clients       = DyingClients,
                               current_file_handle = CurHdl,
                               current_file        = CurFile }) ->
    {ok, CurOffset} = file_handle_cache:current_virtual_offset(CurHdl),
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

handle_cast({write, CRef, MsgId, Flow},
            State = #msstate { cur_file_cache_ets = CurFileCacheEts,
                               clients            = Clients,
                               credit_disc_bound  = CreditDiscBound }) ->
    case Flow of
        flow   -> {CPid, _, _} = maps:get(CRef, Clients),
                  %% We are going to process a message sent by the
                  %% rabbit_amqqueue_process. Now we are accessing the
                  %% msg_store process dictionary.
                  credit_flow:ack(CPid, CreditDiscBound);
        noflow -> ok
    end,
    true = 0 =< ets:update_counter(CurFileCacheEts, MsgId, {3, -1}),
    case update_flying(-1, MsgId, CRef, State) of
        process ->
            [{MsgId, Msg, _PWC}] = ets:lookup(CurFileCacheEts, MsgId),
            noreply(write_message(MsgId, Msg, CRef, State));
        ignore ->
            %% A 'remove' has already been issued and eliminated the
            %% 'write'.
            State1 = blind_confirm(CRef, gb_sets:singleton(MsgId),
                                   ignored, State),
            %% If all writes get eliminated, cur_file_cache_ets could
            %% grow unbounded. To prevent that we delete the cache
            %% entry here, but only if the message isn't in the
            %% current file. That way reads of the message can
            %% continue to be done client side, from either the cache
            %% or the non-current files. If the message *is* in the
            %% current file then the cache entry will be removed by
            %% the normal logic for that in write_message/4 and
            %% maybe_roll_to_new_file/2.
            case index_lookup(MsgId, State1) of
                [#msg_location { file = File }]
                  when File == State1 #msstate.current_file ->
                    ok;
                _ ->
                    true = ets:match_delete(CurFileCacheEts, {MsgId, '_', 0})
            end,
            noreply(State1)
    end;

handle_cast({remove, CRef, MsgIds}, State) ->
    {RemovedMsgIds, State1} =
        lists:foldl(
          fun (MsgId, {Removed, State2}) ->
                  case update_flying(+1, MsgId, CRef, State2) of
                      process -> {[MsgId | Removed],
                                  remove_message(MsgId, CRef, State2)};
                      ignore  -> {Removed, State2}
                  end
          end, {[], State}, MsgIds),
    noreply(maybe_compact(client_confirm(CRef, gb_sets:from_list(RemovedMsgIds),
                                         ignored, State1)));

handle_cast({combine_files, Source, Destination, Reclaimed},
            State = #msstate { sum_file_size    = SumFileSize,
                               file_handles_ets = FileHandlesEts,
                               file_summary_ets = FileSummaryEts,
                               clients          = Clients }) ->
    ok = cleanup_after_file_deletion(Source, State),
    %% see comment in cleanup_after_file_deletion, and client_read3
    true = mark_handle_to_close(Clients, FileHandlesEts, Destination, false),
    true = ets:update_element(FileSummaryEts, Destination,
                              {#file_summary.locked, false}),
    State1 = State #msstate { sum_file_size = SumFileSize - Reclaimed },
    noreply(maybe_compact(run_pending([Source, Destination], State1)));

handle_cast({delete_file, File, Reclaimed},
            State = #msstate { sum_file_size = SumFileSize }) ->
    ok = cleanup_after_file_deletion(File, State),
    State1 = State #msstate { sum_file_size = SumFileSize - Reclaimed },
    noreply(maybe_compact(run_pending([File], State1)));

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State).

handle_info(sync, State) ->
    noreply(internal_sync(State));

handle_info(timeout, State) ->
    noreply(internal_sync(State));

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    %% similar to what happens in
    %% rabbit_amqqueue_process:handle_ch_down but with a relation of
    %% msg_store -> rabbit_amqqueue_process instead of
    %% rabbit_amqqueue_process -> rabbit_channel.
    credit_flow:peer_down(Pid),
    noreply(State);

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State}.

terminate(_Reason, State = #msstate { index_state         = IndexState,
                                      index_module        = IndexModule,
                                      current_file_handle = CurHdl,
                                      gc_pid              = GCPid,
                                      file_handles_ets    = FileHandlesEts,
                                      file_summary_ets    = FileSummaryEts,
                                      cur_file_cache_ets  = CurFileCacheEts,
                                      flying_ets          = FlyingEts,
                                      clients             = Clients,
                                      dir                 = Dir }) ->
    rabbit_log:info("Stopping message store for directory '~s'", [Dir]),
    %% stop the gc first, otherwise it could be working and we pull
    %% out the ets tables from under it.
    ok = rabbit_msg_store_gc:stop(GCPid),
    State1 = case CurHdl of
                 undefined -> State;
                 _         -> State2 = internal_sync(State),
                              ok = file_handle_cache:close(CurHdl),
                              State2
             end,
    State3 = close_all_handles(State1),
    case store_file_summary(FileSummaryEts, Dir) of
        ok           -> ok;
        {error, FSErr} ->
            rabbit_log:error("Unable to store file summary"
                             " for vhost message store for directory ~p~n"
                             "Error: ~p~n",
                             [Dir, FSErr])
    end,
    [true = ets:delete(T) || T <- [FileSummaryEts, FileHandlesEts,
                                   CurFileCacheEts, FlyingEts]],
    IndexModule:terminate(IndexState),
    case store_recovery_terms([{client_refs, maps:keys(Clients)},
                               {index_module, IndexModule}], Dir) of
        ok           ->
            rabbit_log:info("Message store for directory '~s' is stopped", [Dir]),
            ok;
        {error, RTErr} ->
            rabbit_log:error("Unable to save message store recovery terms"
                             " for directory ~p~nError: ~p~n",
                             [Dir, RTErr])
    end,
    State3 #msstate { index_state         = undefined,
                      current_file_handle = undefined }.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%%----------------------------------------------------------------------------
%% general helper functions
%%----------------------------------------------------------------------------

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
                                 cref_to_msg_ids     = CTM }) ->
    State1 = stop_sync_timer(State),
    CGs = maps:fold(fun (CRef, MsgIds, NS) ->
                            case gb_sets:is_empty(MsgIds) of
                                true  -> NS;
                                false -> [{CRef, MsgIds} | NS]
                            end
                    end, [], CTM),
    ok = case CGs of
             [] -> ok;
             _  -> file_handle_cache:sync(CurHdl)
         end,
    lists:foldl(fun ({CRef, MsgIds}, StateN) ->
                        client_confirm(CRef, MsgIds, written, StateN)
                end, State1, CGs).

update_flying(Diff, MsgId, CRef, #msstate { flying_ets = FlyingEts }) ->
    Key = {MsgId, CRef},
    NDiff = -Diff,
    case ets:lookup(FlyingEts, Key) of
        []           -> ignore;
        [{_,  Diff}] -> ignore; %% [1]
        [{_, NDiff}] -> ets:update_counter(FlyingEts, Key, {2, Diff}),
                        true = ets:delete_object(FlyingEts, {Key, 0}),
                        process;
        [{_, 0}]     -> true = ets:delete_object(FlyingEts, {Key, 0}),
                        ignore;
        [{_, Err}]   -> throw({bad_flying_ets_record, Diff, Err, Key})
    end.
%% [1] We can get here, for example, in the following scenario: There
%% is a write followed by a remove in flight. The counter will be 0,
%% so on processing the write the server attempts to delete the
%% entry. If at that point the client injects another write it will
%% either insert a new entry, containing +1, or increment the existing
%% entry to +1, thus preventing its removal. Either way therefore when
%% the server processes the read, the counter will be +1.

write_action({true, not_found}, _MsgId, State) ->
    {ignore, undefined, State};
write_action({true, #msg_location { file = File }}, _MsgId, State) ->
    {ignore, File, State};
write_action({false, not_found}, _MsgId, State) ->
    {write, State};
write_action({Mask, #msg_location { ref_count = 0, file = File,
                                    total_size = TotalSize }},
             MsgId, State = #msstate { file_summary_ets = FileSummaryEts }) ->
    case {Mask, ets:lookup(FileSummaryEts, File)} of
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
                      MsgOnDiskFun(gb_sets:singleton(MsgId), written),
                      CTM
              end, CRef, State1)
    end.

remove_message(MsgId, CRef,
               State = #msstate { file_summary_ets = FileSummaryEts }) ->
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
                1 -> case ets:lookup(FileSummaryEts, File) of
                         [#file_summary { locked = true }] ->
                             add_to_pending_gc_completion(
                               {remove, MsgId, CRef}, File, State);
                         [#file_summary {}] ->
                             ok = Dec(),
                             delete_file_if_empty(
                               File, adjust_valid_total_size(
                                       File, -TotalSize, State))
                     end;
                _ -> ok = Dec(),
                     State
            end
    end.

write_message(MsgId, Msg,
              State = #msstate { current_file_handle = CurHdl,
                                 current_file        = CurFile,
                                 sum_valid_data      = SumValid,
                                 sum_file_size       = SumFileSize,
                                 file_summary_ets    = FileSummaryEts }) ->
    {ok, CurOffset} = file_handle_cache:current_virtual_offset(CurHdl),
    {ok, TotalSize} = rabbit_msg_file:append(CurHdl, MsgId, Msg),
    ok = index_insert(
           #msg_location { msg_id = MsgId, ref_count = 1, file = CurFile,
                           offset = CurOffset, total_size = TotalSize }, State),
    [#file_summary { right = undefined, locked = false }] =
        ets:lookup(FileSummaryEts, CurFile),
    [_,_] = ets:update_counter(FileSummaryEts, CurFile,
                               [{#file_summary.valid_total_size, TotalSize},
                                {#file_summary.file_size,        TotalSize}]),
    maybe_roll_to_new_file(CurOffset + TotalSize,
                           State #msstate {
                             sum_valid_data = SumValid    + TotalSize,
                             sum_file_size  = SumFileSize + TotalSize }).

read_message(MsgId, From, State) ->
    case index_lookup_positive_ref_count(MsgId, State) of
        not_found   -> gen_server2:reply(From, not_found),
                       State;
        MsgLocation -> read_message1(From, MsgLocation, State)
    end.

read_message1(From, #msg_location { msg_id = MsgId, file = File,
                                    offset = Offset } = MsgLoc,
              State = #msstate { current_file        = CurFile,
                                 current_file_handle = CurHdl,
                                 file_summary_ets    = FileSummaryEts,
                                 cur_file_cache_ets  = CurFileCacheEts }) ->
    case File =:= CurFile of
        true  -> {Msg, State1} =
                     %% can return [] if msg in file existed on startup
                     case ets:lookup(CurFileCacheEts, MsgId) of
                         [] ->
                             {ok, RawOffSet} =
                                 file_handle_cache:current_raw_offset(CurHdl),
                             ok = case Offset >= RawOffSet of
                                      true  -> file_handle_cache:flush(CurHdl);
                                      false -> ok
                                  end,
                             read_from_disk(MsgLoc, State);
                         [{MsgId, Msg1, _CacheRefCount}] ->
                             {Msg1, State}
                     end,
                 gen_server2:reply(From, {ok, Msg}),
                 State1;
        false -> [#file_summary { locked = Locked }] =
                     ets:lookup(FileSummaryEts, File),
                 case Locked of
                     true  -> add_to_pending_gc_completion({read, MsgId, From},
                                                           File, State);
                     false -> {Msg, State1} = read_from_disk(MsgLoc, State),
                              gen_server2:reply(From, {ok, Msg}),
                              State1
                 end
    end.

read_from_disk(#msg_location { msg_id = MsgId, file = File, offset = Offset,
                               total_size = TotalSize }, State) ->
    {Hdl, State1} = get_read_handle(File, State),
    {ok, Offset} = file_handle_cache:position(Hdl, Offset),
    {ok, {MsgId, Msg}} =
        case rabbit_msg_file:read(Hdl, TotalSize) of
            {ok, {MsgId, _}} = Obj ->
                Obj;
            Rest ->
                {error, {misread, [{old_state, State},
                                   {file_num,  File},
                                   {offset,    Offset},
                                   {msg_id,    MsgId},
                                   {read,      Rest},
                                   {proc_dict, get()}
                                  ]}}
        end,
    {Msg, State1}.

contains_message(MsgId, From,
                 State = #msstate { pending_gc_completion = Pending }) ->
    case index_lookup_positive_ref_count(MsgId, State) of
        not_found ->
            gen_server2:reply(From, false),
            State;
        #msg_location { file = File } ->
            case maps:is_key(File, Pending) of
                true  -> add_to_pending_gc_completion(
                           {contains, MsgId, From}, File, State);
                false -> gen_server2:reply(From, true),
                         State
            end
    end.

add_to_pending_gc_completion(
  Op, File, State = #msstate { pending_gc_completion = Pending }) ->
    State #msstate { pending_gc_completion =
                         rabbit_misc:maps_cons(File, Op, Pending) }.

run_pending(Files, State) ->
    lists:foldl(
      fun (File, State1 = #msstate { pending_gc_completion = Pending }) ->
              Pending1 = maps:remove(File, Pending),
              lists:foldl(
                fun run_pending_action/2,
                State1 #msstate { pending_gc_completion = Pending1 },
                lists:reverse(maps:get(File, Pending)))
      end, State, Files).

run_pending_action({read, MsgId, From}, State) ->
    read_message(MsgId, From, State);
run_pending_action({contains, MsgId, From}, State) ->
    contains_message(MsgId, From, State);
run_pending_action({remove, MsgId, CRef}, State) ->
    remove_message(MsgId, CRef, State).

safe_ets_update_counter(Tab, Key, UpdateOp, SuccessFun, FailThunk) ->
    try
        SuccessFun(ets:update_counter(Tab, Key, UpdateOp))
    catch error:badarg -> FailThunk()
    end.

update_msg_cache(CacheEts, MsgId, Msg) ->
    case ets:insert_new(CacheEts, {MsgId, Msg, 1}) of
        true  -> ok;
        false -> safe_ets_update_counter(
                   CacheEts, MsgId, {3, +1}, fun (_) -> ok end,
                   fun () -> update_msg_cache(CacheEts, MsgId, Msg) end)
    end.

adjust_valid_total_size(File, Delta, State = #msstate {
                                       sum_valid_data   = SumValid,
                                       file_summary_ets = FileSummaryEts }) ->
    [_] = ets:update_counter(FileSummaryEts, File,
                             [{#file_summary.valid_total_size, Delta}]),
    State #msstate { sum_valid_data = SumValid + Delta }.

maps_store(Key, Val, Dict) ->
    false = maps:is_key(Key, Dict),
    maps:put(Key, Val, Dict).

update_pending_confirms(Fun, CRef,
                        State = #msstate { clients         = Clients,
                                           cref_to_msg_ids = CTM }) ->
    case maps:get(CRef, Clients) of
        {_CPid, undefined,    _CloseFDsFun} -> State;
        {_CPid, MsgOnDiskFun, _CloseFDsFun} -> CTM1 = Fun(MsgOnDiskFun, CTM),
                                               State #msstate {
                                                 cref_to_msg_ids = CTM1 }
    end.

record_pending_confirm(CRef, MsgId, State) ->
    update_pending_confirms(
      fun (_MsgOnDiskFun, CTM) ->
            NewMsgIds = case maps:find(CRef, CTM) of
                error        -> gb_sets:singleton(MsgId);
                {ok, MsgIds} -> gb_sets:add(MsgId, MsgIds)
            end,
            maps:put(CRef, NewMsgIds, CTM)
      end, CRef, State).

client_confirm(CRef, MsgIds, ActionTaken, State) ->
    update_pending_confirms(
      fun (MsgOnDiskFun, CTM) ->
              case maps:find(CRef, CTM) of
                  {ok, Gs} -> MsgOnDiskFun(gb_sets:intersection(Gs, MsgIds),
                                           ActionTaken),
                              MsgIds1 = rabbit_misc:gb_sets_difference(
                                          Gs, MsgIds),
                              case gb_sets:is_empty(MsgIds1) of
                                  true  -> maps:remove(CRef, CTM);
                                  false -> maps:put(CRef, MsgIds1, CTM)
                              end;
                  error    -> CTM
              end
      end, CRef, State).

blind_confirm(CRef, MsgIds, ActionTaken, State) ->
    update_pending_confirms(
      fun (MsgOnDiskFun, CTM) -> MsgOnDiskFun(MsgIds, ActionTaken), CTM end,
      CRef, State).

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

open_file(Dir, FileName, Mode) ->
    file_handle_cache:open_with_absolute_path(
      form_filename(Dir, FileName), ?BINARY_MODE ++ Mode,
      [{write_buffer, ?HANDLE_CACHE_BUFFER_SIZE},
       {read_buffer,  ?HANDLE_CACHE_BUFFER_SIZE}]).

close_handle(Key, CState = #client_msstate { file_handle_cache = FHC }) ->
    CState #client_msstate { file_handle_cache = close_handle(Key, FHC) };

close_handle(Key, State = #msstate { file_handle_cache = FHC }) ->
    State #msstate { file_handle_cache = close_handle(Key, FHC) };

close_handle(Key, FHC) ->
    case maps:find(Key, FHC) of
        {ok, Hdl} -> ok = file_handle_cache:close(Hdl),
                     maps:remove(Key, FHC);
        error     -> FHC
    end.

mark_handle_open(FileHandlesEts, File, Ref) ->
    %% This is fine to fail (already exists). Note it could fail with
    %% the value being close, and not have it updated to open.
    ets:insert_new(FileHandlesEts, {{Ref, File}, open}),
    true.

%% See comment in client_read3 - only call this when the file is locked
mark_handle_to_close(ClientRefs, FileHandlesEts, File, Invoke) ->
    [ begin
          case (ets:update_element(FileHandlesEts, Key, {2, close})
                andalso Invoke) of
              true  -> case maps:get(Ref, ClientRefs) of
                           {_CPid, _MsgOnDiskFun, undefined} ->
                               ok;
                           {_CPid, _MsgOnDiskFun, CloseFDsFun} ->
                               ok = CloseFDsFun()
                       end;
              false -> ok
          end
      end || {{Ref, _File} = Key, open} <-
                 ets:match_object(FileHandlesEts, {{'_', File}, open}) ],
    true.

safe_file_delete_fun(File, Dir, FileHandlesEts) ->
    fun () -> safe_file_delete(File, Dir, FileHandlesEts) end.

safe_file_delete(File, Dir, FileHandlesEts) ->
    %% do not match on any value - it's the absence of the row that
    %% indicates the client has really closed the file.
    case ets:match_object(FileHandlesEts, {{'_', File}, '_'}, 1) of
        {[_|_], _Cont} -> false;
        _              -> ok = file:delete(
                                 form_filename(Dir, filenum_to_name(File))),
                          true
    end.

-spec close_all_indicated
        (client_msstate()) -> rabbit_types:ok(client_msstate()).

close_all_indicated(#client_msstate { file_handles_ets = FileHandlesEts,
                                      client_ref       = Ref } =
                        CState) ->
    Objs = ets:match_object(FileHandlesEts, {{Ref, '_'}, close}),
    {ok, lists:foldl(fun ({Key = {_Ref, File}, close}, CStateM) ->
                             true = ets:delete(FileHandlesEts, Key),
                             close_handle(File, CStateM)
                     end, CState, Objs)}.

close_all_handles(CState = #client_msstate { file_handles_ets  = FileHandlesEts,
                                             file_handle_cache = FHC,
                                             client_ref        = Ref }) ->
    ok = maps:fold(fun (File, Hdl, ok) ->
                           true = ets:delete(FileHandlesEts, {Ref, File}),
                           file_handle_cache:close(Hdl)
                   end, ok, FHC),
    CState #client_msstate { file_handle_cache = #{} };

close_all_handles(State = #msstate { file_handle_cache = FHC }) ->
    ok = maps:fold(fun (_Key, Hdl, ok) -> file_handle_cache:close(Hdl) end,
                   ok, FHC),
    State #msstate { file_handle_cache = #{} }.

get_read_handle(FileNum, CState = #client_msstate { file_handle_cache = FHC,
                                                    dir = Dir }) ->
    {Hdl, FHC2} = get_read_handle(FileNum, FHC, Dir),
    {Hdl, CState #client_msstate { file_handle_cache = FHC2 }};

get_read_handle(FileNum, State = #msstate { file_handle_cache = FHC,
                                            dir = Dir }) ->
    {Hdl, FHC2} = get_read_handle(FileNum, FHC, Dir),
    {Hdl, State #msstate { file_handle_cache = FHC2 }}.

get_read_handle(FileNum, FHC, Dir) ->
    case maps:find(FileNum, FHC) of
        {ok, Hdl} -> {Hdl, FHC};
        error     -> {ok, Hdl} = open_file(Dir, filenum_to_name(FileNum),
                                           ?READ_MODE),
                     {Hdl, maps:put(FileNum, Hdl, FHC)}
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
    rabbit_log:warning("Message store ~tp: rebuilding indices from scratch~n", [Name]),
    {false, IndexModule:new(Dir), []};
recover_index_and_client_refs(IndexModule, true, ClientRefs, Dir, Name) ->
    Fresh = fun (ErrorMsg, ErrorArgs) ->
                    rabbit_log:warning("Message store ~tp : " ++ ErrorMsg ++ "~n"
                                       "rebuilding indices from scratch~n",
                                       [Name | ErrorArgs]),
                    {false, IndexModule:new(Dir), []}
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
                                 {true, IndexState1, ClientRefs};
                             {error, Error} ->
                                 Fresh("failed to recover index: ~p", [Error])
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

recover_crashed_compactions(Dir) ->
    FileNames =    list_sorted_filenames(Dir, ?FILE_EXTENSION),
    TmpFileNames = list_sorted_filenames(Dir, ?FILE_EXTENSION_TMP),
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
                                            form_filename(Dir, FileName)),
                                     fun scan_fun/2, []),
                           ok = file_handle_cache:close(Hdl),
                           Valid;
        {error, enoent} -> {ok, [], 0};
        {error, Reason} -> {error, {unable_to_scan_file, FileName, Reason}}
    end.

scan_fun({MsgId, TotalSize, Offset, _Msg}, Acc) ->
    [{MsgId, TotalSize, Offset} | Acc].

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
             FileName <- list_sorted_filenames(Dir, ?FILE_EXTENSION)] of
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
            unlink(Gatherer),
            ok = gatherer:stop(Gatherer),
            ok = index_clean_up_temporary_reference_count_entries(State),
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
           fun () ->
                link(Gatherer),
                ok = build_index_worker(Gatherer, State,
                                   Left, File, Files),
                unlink(Gatherer),
                ok
           end),
    build_index(Gatherer, File, Files, State).

build_index_worker(Gatherer, State = #msstate { dir = Dir },
                   Left, File, Files) ->
    {ok, Messages, FileSize} =
        scan_file_for_valid_messages(Dir, filenum_to_name(File)),
    {ValidMessages, ValidTotalSize} =
        lists:foldl(
          fun (Obj = {MsgId, TotalSize, Offset}, {VMAcc, VTSAcc}) ->
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
    {Right, FileSize1} =
        case Files of
            %% if it's the last file, we'll truncate to remove any
            %% rubbish above the last valid message. This affects the
            %% file size.
            []    -> {undefined, case ValidMessages of
                                     [] -> 0;
                                     _  -> {_MsgId, TotalSize, Offset} =
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

maybe_compact(State = #msstate { sum_valid_data        = SumValid,
                                 sum_file_size         = SumFileSize,
                                 gc_pid                = GCPid,
                                 pending_gc_completion = Pending,
                                 file_summary_ets      = FileSummaryEts,
                                 file_size_limit       = FileSizeLimit })
  when SumFileSize > 2 * FileSizeLimit andalso
       (SumFileSize - SumValid) / SumFileSize > ?GARBAGE_FRACTION ->
    %% TODO: the algorithm here is sub-optimal - it may result in a
    %% complete traversal of FileSummaryEts.
    First = ets:first(FileSummaryEts),
    case First =:= '$end_of_table' orelse
        maps:size(Pending) >= ?MAXIMUM_SIMULTANEOUS_GC_FILES of
        true ->
            State;
        false ->
            case find_files_to_combine(FileSummaryEts, FileSizeLimit,
                                       ets:lookup(FileSummaryEts, First)) of
                not_found ->
                    State;
                {Src, Dst} ->
                    Pending1 = maps_store(Dst, [],
                                             maps_store(Src, [], Pending)),
                    State1 = close_handle(Src, close_handle(Dst, State)),
                    true = ets:update_element(FileSummaryEts, Src,
                                              {#file_summary.locked, true}),
                    true = ets:update_element(FileSummaryEts, Dst,
                                              {#file_summary.locked, true}),
                    ok = rabbit_msg_store_gc:combine(GCPid, Src, Dst),
                    State1 #msstate { pending_gc_completion = Pending1 }
            end
    end;
maybe_compact(State) ->
    State.

find_files_to_combine(FileSummaryEts, FileSizeLimit,
                      [#file_summary { file             = Dst,
                                       valid_total_size = DstValid,
                                       right            = Src,
                                       locked           = DstLocked }]) ->
    case Src of
        undefined ->
            not_found;
        _   ->
            [#file_summary { file             = Src,
                             valid_total_size = SrcValid,
                             left             = Dst,
                             right            = SrcRight,
                             locked           = SrcLocked }] = Next =
                ets:lookup(FileSummaryEts, Src),
            case SrcRight of
                undefined -> not_found;
                _         -> case (DstValid + SrcValid =< FileSizeLimit) andalso
                                 (DstValid > 0) andalso (SrcValid > 0) andalso
                                 not (DstLocked orelse SrcLocked) of
                                 true  -> {Src, Dst};
                                 false -> find_files_to_combine(
                                            FileSummaryEts, FileSizeLimit, Next)
                             end
            end
    end.

delete_file_if_empty(File, State = #msstate { current_file = File }) ->
    State;
delete_file_if_empty(File, State = #msstate {
                             gc_pid                = GCPid,
                             file_summary_ets      = FileSummaryEts,
                             pending_gc_completion = Pending }) ->
    [#file_summary { valid_total_size = ValidData,
                     locked           = false }] =
        ets:lookup(FileSummaryEts, File),
    case ValidData of
        %% don't delete the file_summary_ets entry for File here
        %% because we could have readers which need to be able to
        %% decrement the readers count.
        0 -> true = ets:update_element(FileSummaryEts, File,
                                       {#file_summary.locked, true}),
             ok = rabbit_msg_store_gc:delete(GCPid, File),
             Pending1 = maps_store(File, [], Pending),
             close_handle(File,
                          State #msstate { pending_gc_completion = Pending1 });
        _ -> State
    end.

cleanup_after_file_deletion(File,
                            #msstate { file_handles_ets = FileHandlesEts,
                                       file_summary_ets = FileSummaryEts,
                                       clients          = Clients }) ->
    %% Ensure that any clients that have open fhs to the file close
    %% them before using them again. This has to be done here (given
    %% it's done in the msg_store, and not the gc), and not when
    %% starting up the GC, because if done when starting up the GC,
    %% the client could find the close, and close and reopen the fh,
    %% whilst the GC is waiting for readers to disappear, before it's
    %% actually done the GC.
    true = mark_handle_to_close(Clients, FileHandlesEts, File, true),
    [#file_summary { left    = Left,
                     right   = Right,
                     locked  = true,
                     readers = 0 }] = ets:lookup(FileSummaryEts, File),
    %% We'll never delete the current file, so right is never undefined
    true = Right =/= undefined, %% ASSERTION
    true = ets:update_element(FileSummaryEts, Right,
                              {#file_summary.left, Left}),
    %% ensure the double linked list is maintained
    true = case Left of
               undefined -> true; %% File is the eldest file (left-most)
               _         -> ets:update_element(FileSummaryEts, Left,
                                               {#file_summary.right, Right})
           end,
    true = ets:delete(FileSummaryEts, File),
    ok.

%%----------------------------------------------------------------------------
%% garbage collection / compaction / aggregation -- external
%%----------------------------------------------------------------------------

-spec has_readers(non_neg_integer(), gc_state()) -> boolean().

has_readers(File, #gc_state { file_summary_ets = FileSummaryEts }) ->
    [#file_summary { locked = true, readers = Count }] =
        ets:lookup(FileSummaryEts, File),
    Count /= 0.

-spec combine_files(non_neg_integer(), non_neg_integer(), gc_state()) ->
                              deletion_thunk().

combine_files(Source, Destination,
              State = #gc_state { file_summary_ets = FileSummaryEts,
                                  file_handles_ets = FileHandlesEts,
                                  dir              = Dir,
                                  msg_store        = Server }) ->
    [#file_summary {
        readers          = 0,
        left             = Destination,
        valid_total_size = SourceValid,
        file_size        = SourceFileSize,
        locked           = true }] = ets:lookup(FileSummaryEts, Source),
    [#file_summary {
        readers          = 0,
        right            = Source,
        valid_total_size = DestinationValid,
        file_size        = DestinationFileSize,
        locked           = true }] = ets:lookup(FileSummaryEts, Destination),

    SourceName           = filenum_to_name(Source),
    DestinationName      = filenum_to_name(Destination),
    {ok, SourceHdl}      = open_file(Dir, SourceName,
                                     ?READ_AHEAD_MODE),
    {ok, DestinationHdl} = open_file(Dir, DestinationName,
                                     ?READ_AHEAD_MODE ++ ?WRITE_MODE),
    TotalValidData = SourceValid + DestinationValid,
    %% if DestinationValid =:= DestinationContiguousTop then we don't
    %% need a tmp file
    %% if they're not equal, then we need to write out everything past
    %%   the DestinationContiguousTop to a tmp file then truncate,
    %%   copy back in, and then copy over from Source
    %% otherwise we just truncate straight away and copy over from Source
    {DestinationWorkList, DestinationValid} =
        load_and_vacuum_message_file(Destination, State),
    {DestinationContiguousTop, DestinationWorkListTail} =
        drop_contiguous_block_prefix(DestinationWorkList),
    case DestinationWorkListTail of
        [] -> ok = truncate_and_extend_file(
                     DestinationHdl, DestinationContiguousTop, TotalValidData);
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
                     DestinationHdl, DestinationContiguousTop, TotalValidData),
              {ok, TmpSize} =
                  file_handle_cache:copy(TmpHdl, DestinationHdl, TmpSize),
              %% position in DestinationHdl should now be DestinationValid
              ok = file_handle_cache:sync(DestinationHdl),
              ok = file_handle_cache:delete(TmpHdl)
    end,
    {SourceWorkList, SourceValid} = load_and_vacuum_message_file(Source, State),
    ok = copy_messages(SourceWorkList, DestinationValid, TotalValidData,
                       SourceHdl, DestinationHdl, Destination, State),
    %% tidy up
    ok = file_handle_cache:close(DestinationHdl),
    ok = file_handle_cache:close(SourceHdl),

    %% don't update dest.right, because it could be changing at the
    %% same time
    true = ets:update_element(
             FileSummaryEts, Destination,
             [{#file_summary.valid_total_size, TotalValidData},
              {#file_summary.file_size,        TotalValidData}]),

    Reclaimed = SourceFileSize + DestinationFileSize - TotalValidData,
    gen_server2:cast(Server, {combine_files, Source, Destination, Reclaimed}),
    safe_file_delete_fun(Source, Dir, FileHandlesEts).

-spec delete_file(non_neg_integer(), gc_state()) -> deletion_thunk().

delete_file(File, State = #gc_state { file_summary_ets = FileSummaryEts,
                                      file_handles_ets = FileHandlesEts,
                                      dir              = Dir,
                                      msg_store        = Server }) ->
    [#file_summary { valid_total_size = 0,
                     locked           = true,
                     file_size        = FileSize,
                     readers          = 0 }] = ets:lookup(FileSummaryEts, File),
    {[], 0} = load_and_vacuum_message_file(File, State),
    gen_server2:cast(Server, {delete_file, File, FileSize}),
    safe_file_delete_fun(File, Dir, FileHandlesEts).

load_and_vacuum_message_file(File, State = #gc_state { dir = Dir }) ->
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

copy_messages(WorkList, InitOffset, FinalOffset, SourceHdl, DestinationHdl,
              Destination, State) ->
    Copy = fun ({BlockStart, BlockEnd}) ->
                   BSize = BlockEnd - BlockStart,
                   {ok, BlockStart} =
                       file_handle_cache:position(SourceHdl, BlockStart),
                   {ok, BSize} =
                       file_handle_cache:copy(SourceHdl, DestinationHdl, BSize)
           end,
    case
        lists:foldl(
          fun (#msg_location { msg_id = MsgId, offset = Offset,
                               total_size = TotalSize },
               {CurOffset, Block = {BlockStart, BlockEnd}}) ->
                  %% CurOffset is in the DestinationFile.
                  %% Offset, BlockStart and BlockEnd are in the SourceFile
                  %% update MsgLocation to reflect change of file and offset
                  ok = index_update_fields(MsgId,
                                           [{#msg_location.file, Destination},
                                            {#msg_location.offset, CurOffset}],
                                           State),
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

-spec force_recovery(file:filename(), server()) -> 'ok'.

force_recovery(BaseDir, Store) ->
    Dir = filename:join(BaseDir, atom_to_list(Store)),
    case file:delete(filename:join(Dir, ?CLEAN_FILENAME)) of
        ok              -> ok;
        {error, enoent} -> ok
    end,
    recover_crashed_compactions(BaseDir),
    ok.

foreach_file(D, Fun, Files) ->
    [ok = Fun(filename:join(D, File)) || File <- Files].

foreach_file(D1, D2, Fun, Files) ->
    [ok = Fun(filename:join(D1, File), filename:join(D2, File)) || File <- Files].

-spec transform_dir(file:filename(), server(),
        fun ((any()) -> (rabbit_types:ok_or_error2(msg(), any())))) -> 'ok'.

transform_dir(BaseDir, Store, TransformFun) ->
    Dir = filename:join(BaseDir, atom_to_list(Store)),
    TmpDir = filename:join(Dir, ?TRANSFORM_TMP),
    TransformFile = fun (A, B) -> transform_msg_file(A, B, TransformFun) end,
    CopyFile = fun (Src, Dst) -> {ok, _Bytes} = file:copy(Src, Dst), ok end,
    case filelib:is_dir(TmpDir) of
        true  -> throw({error, transform_failed_previously});
        false -> FileList = list_sorted_filenames(Dir, ?FILE_EXTENSION),
                 foreach_file(Dir, TmpDir, TransformFile,     FileList),
                 foreach_file(Dir,         fun file:delete/1, FileList),
                 foreach_file(TmpDir, Dir, CopyFile,          FileList),
                 foreach_file(TmpDir,      fun file:delete/1, FileList),
                 ok = file:del_dir(TmpDir)
    end.

transform_msg_file(FileOld, FileNew, TransformFun) ->
    ok = rabbit_file:ensure_parent_dirs_exist(FileNew),
    {ok, RefOld} = file_handle_cache:open_with_absolute_path(
                     FileOld, [raw, binary, read], []),
    {ok, RefNew} = file_handle_cache:open_with_absolute_path(
                     FileNew, [raw, binary, write],
                     [{write_buffer, ?HANDLE_CACHE_BUFFER_SIZE}]),
    {ok, _Acc, _IgnoreSize} =
        rabbit_msg_file:scan(
          RefOld, filelib:file_size(FileOld),
          fun({MsgId, _Size, _Offset, BinMsg}, ok) ->
                  {ok, MsgNew} = case binary_to_term(BinMsg) of
                                     <<>> -> {ok, <<>>};  %% dying client marker
                                     Msg  -> TransformFun(Msg)
                                 end,
                  {ok, _} = rabbit_msg_file:append(RefNew, MsgId, MsgNew),
                  ok
          end, ok),
    ok = file_handle_cache:close(RefOld),
    ok = file_handle_cache:close(RefNew),
    ok.
