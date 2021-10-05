%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_classic_queue_index_v2).

-export([erase/1, init/3, reset_state/1, recover/6,
         terminate/3, delete_and_terminate/1,
         publish/7, ack/2, read/3]).

%% Recovery. Unlike other functions in this module, these
%% apply to all queues all at once.
-export([start/2, stop/1]).

%% rabbit_queue_index/rabbit_variable_queue-specific functions.
%% Implementation details from the queue index leaking into the
%% queue implementation itself.
-export([pre_publish/7, flush_pre_publish_cache/2,
         sync/1, needs_sync/1, flush/1,
         bounds/1, next_segment_boundary/1]).

%% Shared with rabbit_classic_queue_store_v2.
-export([queue_dir/2]).

-define(QUEUE_NAME_STUB_FILE, ".queue_name").
-define(SEGMENT_EXTENSION, ".qi").

-define(MAGIC, 16#52435149). %% "RCQI"
-define(VERSION, 2).
-define(HEADER_SIZE, 64). %% bytes
-define(ENTRY_SIZE,  32). %% bytes

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/file.hrl").

%% Set to true to get an awful lot of debug logs.
-if(false).
-define(DEBUG(X,Y), logger:debug("~0p: " ++ X, [?FUNCTION_NAME|Y])).
-else.
-define(DEBUG(X,Y), _ = X, _ = Y, ok).
-endif.

-type entry() :: {rabbit_types:msg_id(),
                  rabbit_variable_queue:seq_id(),
                  rabbit_variable_queue:msg_location(),
                  rabbit_types:message_properties(),
                  boolean()}.

-record(qi, {
    %% Queue name (for the stub file).
    queue_name :: rabbit_amqqueue:name(),

    %% Queue index directory.
    dir :: file:filename(),

    %% Buffer of all write operations to be performed.
    %% When the buffer reaches a certain size, we reduce
    %% the buffer size by first checking if writing the
    %% acks gets us to a comfortable buffer
    %% size. If we do, write only the acks.
    %% If not, write everything. This allows the reader
    %% to continue reading from memory if it is fast
    %% enough to keep up with the producer.
    write_buffer = #{} :: #{rabbit_variable_queue:seq_id() => entry() | ack},

    %% The number of entries in the write buffer that
    %% refer to an update to the file, rather than a
    %% full entry. Used to determine if flushing only acks
    %% is enough to get the buffer to a comfortable size.
    write_buffer_updates = 0 :: non_neg_integer(),

    %% When using publisher confirms we flush index entries
    %% to disk regularly. This means we can no longer rely
    %% on entries being present in the write_buffer and
    %% have to read from disk. This cache is meant to avoid
    %% that by keeping the write_buffer entries in memory
    %% longer.
    %%
    %% When the write_buffer is flushed to disk fully, it
    %% replaces the cache entirely. When only acks are flushed,
    %% then the cache gets updated: old acks are removed and
    %% new acks are added to the cache.
    cache = #{} :: #{rabbit_variable_queue:seq_id() => entry() | ack},

    %% Messages waiting for publisher confirms. The
    %% publisher confirms will be sent when the message
    %% has been synced to disk (as an entry or an ack).
    %%
    %% The index does not call file:sync/1 by default.
    %% It only does when publisher confirms are used
    %% and there are outstanding unconfirmed messages.
    %% In that case the buffer is flushed to disk when
    %% the queue requests a sync (after a timeout).
    confirms = gb_sets:new() :: gb_sets:set(),

    %% Segments we currently know of along with the
    %% number of unacked messages remaining in the
    %% segment. We use this to determine whether a
    %% segment file can be deleted.
    segments = #{} :: #{non_neg_integer() => pos_integer()},

    %% File descriptors. We will keep up to 4 FDs
    %% at a time. See comments in reduce_fd_usage/2.
    fds = #{} :: #{non_neg_integer() => file:fd()},

    %% This fun must be called when messages that expect
    %% confirms have either an ack or their entry
    %% written to disk and file:sync/1 has been called.
    on_sync :: on_sync_fun()
}).

-type state() :: #qi{}.

%% Types copied from rabbit_queue_index.

-type on_sync_fun() :: fun ((gb_sets:set()) -> ok).
-type contains_predicate() :: fun ((rabbit_types:msg_id()) -> boolean()).
-type shutdown_terms() :: list() | 'non_clean_shutdown'.

%% ----

-spec erase(rabbit_amqqueue:name()) -> 'ok'.

erase(#resource{ virtual_host = VHost } = Name) ->
    ?DEBUG("~0p", [Name]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    erase_index_dir(Dir).

-spec init(rabbit_amqqueue:name(),
                 on_sync_fun(), on_sync_fun()) -> state().

%% We do not embed messages and as a result never need the OnSyncMsgFun.

init(#resource{ virtual_host = VHost } = Name, OnSyncFun, _OnSyncMsgFun) ->
    ?DEBUG("~0p ~0p", [Name, OnSyncFun]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    false = rabbit_file:is_file(Dir), %% is_file == is file or dir
    init1(Name, Dir, OnSyncFun).

init1(Name, Dir, OnSyncFun) ->
    ensure_queue_name_stub_file(Name, Dir),
    #qi{
        queue_name = Name,
        dir = Dir,
        on_sync = OnSyncFun
    }.

ensure_queue_name_stub_file(#resource{virtual_host = VHost, name = QName}, Dir) ->
    QueueNameFile = filename:join(Dir, ?QUEUE_NAME_STUB_FILE),
    ok = filelib:ensure_dir(QueueNameFile),
    ok = file:write_file(QueueNameFile, <<"VHOST: ", VHost/binary, "\n",
                                          "QUEUE: ", QName/binary, "\n",
                                          "INDEX: v2\n">>).

-spec reset_state(State) -> State when State::state().

reset_state(State = #qi{ queue_name     = Name,
                         dir            = Dir,
                         on_sync        = OnSyncFun }) ->
    ?DEBUG("~0p", [State]),
    delete_and_terminate(State),
    init1(Name, Dir, OnSyncFun).

-spec recover(rabbit_amqqueue:name(), shutdown_terms(), boolean(),
                    contains_predicate(),
                    on_sync_fun(), on_sync_fun()) ->
                        {'undefined' | non_neg_integer(),
                         'undefined' | non_neg_integer(), state()}.

-define(RECOVER_COUNT, 1).
-define(RECOVER_BYTES, 2).
-define(RECOVER_DROPPED_PERSISTENT_PER_VHOST, 3).
-define(RECOVER_DROPPED_PERSISTENT_PER_QUEUE, 4).
-define(RECOVER_DROPPED_PERSISTENT_OTHER, 5).
-define(RECOVER_DROPPED_TRANSIENT, 6).
-define(RECOVER_COUNTER_SIZE, 6).

recover(#resource{ virtual_host = VHost, name = QueueName } = Name, Terms,
        IsMsgStoreClean, ContainsCheckFun, OnSyncFun, OnSyncMsgFun) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [Name, Terms, IsMsgStoreClean,
                                       ContainsCheckFun, OnSyncFun, OnSyncMsgFun]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    State0 = init1(Name, Dir, OnSyncFun),
    %% We go over all segments if either the index or the
    %% message store has/had to recover. Otherwise we just
    %% take our state from Terms.
    IsIndexClean = Terms =/= non_clean_shutdown,
    case IsIndexClean andalso IsMsgStoreClean of
        true ->
            State = case proplists:get_value(mqi_state, Terms, undefined) of
                %% We are recovering a queue that was using the old index.
                undefined ->
                    recover_index_v1_clean(State0, Terms, IsMsgStoreClean,
                                           ContainsCheckFun, OnSyncFun, OnSyncMsgFun);
                {?VERSION, Segments} ->
                    State0#qi{ segments = Segments }
            end,
            %% The queue has stored the count/bytes values inside
            %% Terms so we don't need to provide them again.
            {undefined,
             undefined,
             State};
        false ->
            CountersRef = counters:new(?RECOVER_COUNTER_SIZE, []),
            State = recover_segments(State0, Terms, IsMsgStoreClean,
                                     ContainsCheckFun, OnSyncFun, OnSyncMsgFun,
                                     CountersRef),
            rabbit_log:warning("Queue ~s in vhost ~s dropped ~b/~b/~b persistent messages "
                               "and ~b transient messages during dirty recovery",
                               [QueueName, VHost,
                                counters:get(CountersRef, ?RECOVER_DROPPED_PERSISTENT_PER_VHOST),
                                counters:get(CountersRef, ?RECOVER_DROPPED_PERSISTENT_PER_QUEUE),
                                counters:get(CountersRef, ?RECOVER_DROPPED_PERSISTENT_OTHER),
                                counters:get(CountersRef, ?RECOVER_DROPPED_TRANSIENT)]),
            {counters:get(CountersRef, ?RECOVER_COUNT),
             counters:get(CountersRef, ?RECOVER_BYTES),
             State}
    end.

recover_segments(State0 = #qi { queue_name = Name, dir = Dir }, Terms, IsMsgStoreClean,
                 ContainsCheckFun, OnSyncFun, OnSyncMsgFun, CountersRef) ->
    SegmentFiles = rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir),
    State = case SegmentFiles of
        %% No segments found.
        [] ->
            State0;
        %% Count unackeds in the segments.
        _ ->
            Segments = lists:sort([
                list_to_integer(filename:basename(F, ?SEGMENT_EXTENSION))
            || F <- SegmentFiles]),
            %% We use a temporary store state to check that messages do exist.
            StoreState0 = rabbit_classic_queue_store_v2:init(Name, OnSyncMsgFun),
            {State1, StoreState} = recover_segments(State0, ContainsCheckFun, StoreState0, CountersRef, Segments),
            _ = rabbit_classic_queue_store_v2:terminate(StoreState),
            State1
    end,
    %% We always try to see if there are segment files from the old index as well.
    case rabbit_file:wildcard(".*\\.idx", Dir) of
        %% We are recovering a dirty queue that was using the old index.
        [_|_] ->
            recover_index_v1_dirty(State, Terms, IsMsgStoreClean,
                                   ContainsCheckFun, OnSyncFun, OnSyncMsgFun,
                                   CountersRef);
        %% Otherwise keep default values.
        [] ->
            State
    end.

recover_segments(State, _, StoreState, _, []) ->
    {State, StoreState};
recover_segments(State0, ContainsCheckFun, StoreState0, CountersRef, [Segment|Tail]) ->
    SegmentEntryCount = segment_entry_count(),
    SegmentFile = segment_file(Segment, State0),
    {ok, Fd} = file:open(SegmentFile, [read, read_ahead, write, raw, binary]),
    {ok, <<?MAGIC:32,?VERSION:8,
           _FromSeqId:64/unsigned,_ToSeqId:64/unsigned,
           _/bits>>} = file:read(Fd, ?HEADER_SIZE),
    %% Double check the file size before attempting to parse it.
    SegmentFileSize = ?HEADER_SIZE + SegmentEntryCount * ?ENTRY_SIZE,
    {ok, #file_info{size = SegmentFileSize}} = file:read_file_info(Fd),
    {Action, State, StoreState1} = recover_segment(State0, ContainsCheckFun, StoreState0, CountersRef, Fd,
                                                  Segment, 0, SegmentEntryCount,
                                                  SegmentEntryCount, []),
    ok = file:close(Fd),
    StoreState = case Action of
        keep ->
            StoreState1;
        delete ->
            file:delete(SegmentFile),
            rabbit_classic_queue_store_v2:delete_segments([Segment], StoreState1)
    end,
    recover_segments(State, ContainsCheckFun, StoreState, CountersRef, Tail).

recover_segment(State = #qi{ segments = Segments }, _, StoreState, _, Fd,
                Segment, ThisEntry, SegmentEntryCount,
                Unacked, LocBytes)
                when ThisEntry =:= SegmentEntryCount ->
    case Unacked of
        0 ->
            %% There are no more messages in this segment file.
            {delete, State, StoreState};
        _ ->
            %% We must ack some messages on disk.
            ok = file:pwrite(Fd, LocBytes),
            {keep, State#qi{ segments = Segments#{ Segment => Unacked }}, StoreState}
    end;
recover_segment(State, ContainsCheckFun, StoreState0, CountersRef, Fd,
                Segment, ThisEntry, SegmentEntryCount,
                Unacked, LocBytes0) ->
    case file:read(Fd, ?ENTRY_SIZE) of
        %% We found a non-ack persistent entry. Check that the corresponding
        %% message exists in the message store. If not, mark this message as
        %% acked.
        {ok, << 1:8,
                0:7, 1:1, %% Message is persistent.
                0:8, LocationBin:136/bits,
                Size:32/unsigned, _:64/unsigned >>} ->
            case LocationBin of
                %% Message is expected to be in the per-vhost store.
                << 1:8, Id:16/binary >> ->
                    case ContainsCheckFun(Id) of
                        %% Message is in the store.
                        true ->
                            counters:add(CountersRef, ?RECOVER_COUNT, 1),
                            counters:add(CountersRef, ?RECOVER_BYTES, Size),
                            recover_segment(State, ContainsCheckFun, StoreState0, CountersRef,
                                            Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                                            Unacked, LocBytes0);
                        %% Message is not in the store. Mark it as acked.
                        false ->
                            counters:add(CountersRef, ?RECOVER_DROPPED_PERSISTENT_PER_VHOST, 1),
                            LocBytes = [{?HEADER_SIZE + ThisEntry * ?ENTRY_SIZE, <<0>>}|LocBytes0],
                            recover_segment(State, ContainsCheckFun, StoreState0, CountersRef,
                                            Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                                            Unacked - 1, LocBytes)
                    end;
                %% Message is in the per-queue store.
                << 2:8,
                   StoreOffset:64/unsigned,
                   StoreSize:32/unsigned,
                   0:32 >> ->
                    Location = {rabbit_classic_queue_store_v2, StoreOffset, StoreSize},
                    case rabbit_classic_queue_store_v2:check_msg_on_disk(Segment * SegmentEntryCount + ThisEntry,
                                                                         Location, StoreState0) of
                        %% Message was found in store and is valid.
                        {ok, StoreState} ->
                            counters:add(CountersRef, ?RECOVER_COUNT, 1),
                            counters:add(CountersRef, ?RECOVER_BYTES, Size),
                            recover_segment(State, ContainsCheckFun, StoreState, CountersRef,
                                            Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                                            Unacked, LocBytes0);
                        %% Message was not found or checksum failed. Ack it.
                        {{error, _}, StoreState} ->
                            counters:add(CountersRef, ?RECOVER_DROPPED_PERSISTENT_PER_QUEUE, 1),
                            LocBytes = [{?HEADER_SIZE + ThisEntry * ?ENTRY_SIZE, <<0>>}|LocBytes0],
                            recover_segment(State, ContainsCheckFun, StoreState, CountersRef,
                                            Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                                            Unacked - 1, LocBytes)
                    end;
                %% Message was in memory or malformed. Ack it.
                _ ->
                    counters:add(CountersRef, ?RECOVER_DROPPED_PERSISTENT_OTHER, 1),
                    LocBytes = [{?HEADER_SIZE + ThisEntry * ?ENTRY_SIZE, <<0>>}|LocBytes0],
                    recover_segment(State, ContainsCheckFun, StoreState0, CountersRef,
                                    Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                                    Unacked - 1, LocBytes)
            end;
        %% We found a non-acked transient entry. We mark the message as acked
        %% without checking with the message store, because it already dropped
        %% this message via the queue walker functions.
        {ok, << 1:8, _:7, 0:1, _/bits >>} ->
            counters:add(CountersRef, ?RECOVER_DROPPED_TRANSIENT, 1),
            LocBytes = [{?HEADER_SIZE + ThisEntry * ?ENTRY_SIZE, <<0>>}|LocBytes0],
            recover_segment(State, ContainsCheckFun, StoreState0, CountersRef,
                            Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                            Unacked - 1, LocBytes);
        %% We found a non-entry or an acked entry. Consider it acked.
        {ok, << 0:8, _/bits >>} ->
            recover_segment(State, ContainsCheckFun, StoreState0, CountersRef,
                            Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                            Unacked - 1, LocBytes0)
    end.

recover_index_v1_clean(State0 = #qi{ queue_name = Name }, Terms, IsMsgStoreClean,
                       ContainsCheckFun, OnSyncFun, OnSyncMsgFun) ->
    #resource{virtual_host = VHost, name = QName} = Name,
    rabbit_log:info("Converting clean queue ~s on vhost ~s to the new index format", [QName, VHost]),
    {_, _, V1State} = rabbit_queue_index:recover(Name, Terms, IsMsgStoreClean,
                                                 ContainsCheckFun, OnSyncFun, OnSyncMsgFun),
    %% We will ignore the counter results because on clean shutdown
    %% we do not need to calculate the values again. This lets us
    %% share code with dirty recovery.
    DummyCountersRef = counters:new(2, []),
    State = recover_index_v1_common(State0, V1State, DummyCountersRef),
    rabbit_log:info("Queue ~s on vhost ~s converted ~b total messages to the new index format",
                    [QName, VHost, counters:get(DummyCountersRef, ?RECOVER_COUNT)]),
    State.

recover_index_v1_dirty(State0 = #qi{ queue_name = Name }, Terms, IsMsgStoreClean,
                       ContainsCheckFun, OnSyncFun, OnSyncMsgFun,
                       CountersRef) ->
    #resource{virtual_host = VHost, name = QName} = Name,
    rabbit_log:info("Converting dirty queue ~s on vhost ~s to the new index format", [QName, VHost]),
    %% We ignore the count and bytes returned here because we cannot trust
    %% rabbit_queue_index: it has a bug that may lead to more bytes being
    %% returned than it really has.
    {_, _, V1State} = rabbit_queue_index:recover(Name, Terms, IsMsgStoreClean,
                                                 ContainsCheckFun, OnSyncFun, OnSyncMsgFun),
    State = recover_index_v1_common(State0, V1State, CountersRef),
    rabbit_log:info("Queue ~s on vhost ~s converted ~b total messages to the new index format",
                    [QName, VHost, counters:get(CountersRef, ?RECOVER_COUNT)]),
    State.

%% At this point all messages are persistent because transient messages
%% were dropped during the old index recovery.
recover_index_v1_common(State0 = #qi{ queue_name = Name, dir = Dir },
                        V1State, CountersRef) ->
    %% Use a temporary per-queue store state to store embedded messages.
    StoreState0 = rabbit_classic_queue_store_v2:init(Name, fun(_, _) -> ok end),
    %% Go through the old index and publish messages to the new one.
    {LoSeqId, HiSeqId, _} = rabbit_queue_index:bounds(V1State),
    {State1, StoreState} = recover_index_v1_loop(State0, StoreState0, V1State, CountersRef, LoSeqId, HiSeqId),
    %% Terminate the message store client.
    _ = rabbit_classic_queue_store_v2:terminate(StoreState),
    %% Close the old index journal handle if any.
    JournalHdl = element(4, V1State),
    ok = case JournalHdl of
        undefined -> ok;
        _         -> file_handle_cache:close(JournalHdl)
    end,
    %% Delete the old index files.
    OldFiles = ["journal.jif"|rabbit_file:wildcard(".*\\.idx", Dir)],
    _ = [rabbit_file:delete(filename:join(Dir, F)) || F <- OldFiles],
    %% Ensure that everything in the new index is written to disk.
    State = flush(State1),
    %% Clean up all the garbage that we have surely been creating.
    garbage_collect(),
    State.

recover_index_v1_loop(State, StoreState, _, _, HiSeqId, HiSeqId) ->
    {State, StoreState};
recover_index_v1_loop(State0 = #qi{ queue_name = Name },
                      StoreState0, V1State0, CountersRef, LoSeqId, HiSeqId) ->
    UpSeqId = lists:min([rabbit_queue_index:next_segment_boundary(LoSeqId),
                         HiSeqId]),
    {Messages, V1State} = rabbit_queue_index:read(LoSeqId, UpSeqId, V1State0),
    %% We do a garbage collect immediately after the old index read
    %% because they may have created a lot of garbage.
    garbage_collect(),
    MessagesCount = length(Messages),
    counters:add(CountersRef, ?RECOVER_COUNT, MessagesCount),
    {State3, StoreState3} = lists:foldl(fun
        %% Move embedded messages to the per-queue store.
        ({Msg = #basic_message{id = MsgId}, SeqId, Props, IsPersistent, _IsDelivered},
         {State1, StoreState1}) ->
            MsgSize = Props#message_properties.size,
            {MsgLocation, StoreState2} = rabbit_classic_queue_store_v2:write(SeqId, MsgSize, Msg, StoreState1),
            counters:add(CountersRef, ?RECOVER_BYTES, MsgSize),
            State2 = publish(MsgId, SeqId, MsgLocation, Props, IsPersistent, infinity, State1),
            {State2, StoreState2};
        %% Keep messages in the per-vhost store where they are.
        ({MsgId, SeqId, Props, IsPersistent, _IsDelivered},
         {State1, StoreState1}) ->
            MsgSize = Props#message_properties.size,
            counters:add(CountersRef, ?RECOVER_BYTES, MsgSize),
            State2 = publish(MsgId, SeqId, rabbit_msg_store, Props, IsPersistent, infinity, State1),
            {State2, StoreState1}
    end, {State0, StoreState0}, Messages),
    %% Flush to disk to avoid keeping too much in memory between segments.
    State = flush(State3),
    StoreState = rabbit_classic_queue_store_v2:sync(StoreState3),
    %% We have written everything to disk. We can delete the old segment file
    %% to free up much needed space, to avoid doubling disk usage during the upgrade.
    rabbit_queue_index:delete_segment_file_for_seq_id(LoSeqId, V1State),
    %% Log some progress to keep the user aware of what's going on, as moving
    %% embedded messages can take quite some time.
    #resource{virtual_host = VHost, name = QName} = Name,
    rabbit_log:info("Queue ~s on vhost ~s converted ~b more messages to the new index format",
                    [QName, VHost, MessagesCount]),
    recover_index_v1_loop(State, StoreState, V1State, CountersRef, UpSeqId, HiSeqId).

-spec terminate(rabbit_types:vhost(), [any()], State) -> State when State::state().

terminate(VHost, Terms, State0 = #qi { dir = Dir,
                                       segments = Segments,
                                       fds = OpenFds }) ->
    ?DEBUG("~0p ~0p ~0p", [VHost, Terms, State0]),
    %% Flush the buffer.
    State = flush_buffer(State0, full),
    %% Fsync and close all FDs.
    _ = maps:map(fun(_, Fd) ->
        ok = file:sync(Fd),
        ok = file:close(Fd)
    end, OpenFds),
    file_handle_cache:release_reservation(),
    %% Write recovery terms for faster recovery.
    rabbit_recovery_terms:store(VHost, filename:basename(Dir),
                                [{mqi_state, {?VERSION, Segments}} | Terms]),
    State#qi{ segments = #{},
              fds = #{} }.

-spec delete_and_terminate(State) -> State when State::state().

delete_and_terminate(State = #qi { dir = Dir,
                                   fds = OpenFds }) ->
    ?DEBUG("~0p", [State]),
    %% Close all FDs.
    _ = maps:map(fun(_, Fd) ->
        ok = file:close(Fd)
    end, OpenFds),
    file_handle_cache:release_reservation(),
    %% Erase the data on disk.
    ok = erase_index_dir(Dir),
    State#qi{ segments = #{},
              fds = #{} }.

-spec publish(rabbit_types:msg_id(), rabbit_variable_queue:seq_id(),
              rabbit_variable_queue:msg_location(),
              rabbit_types:message_properties(), boolean(),
              non_neg_integer(), State) -> State when State::state().

%% Because we always persist to the msg_store, the Msg(Or)Id argument
%% here is always a binary, never a record.
publish(MsgId, SeqId, Location, Props, IsPersistent, TargetRamCount,
        State0 = #qi { write_buffer = WriteBuffer0,
                       segments = Segments }) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p ~0p", [MsgId, SeqId, Location, Props, IsPersistent, TargetRamCount, State0]),
    %% Add the entry to the write buffer.
    WriteBuffer = WriteBuffer0#{SeqId => {MsgId, SeqId, Location, Props, IsPersistent}},
    State1 = State0#qi{ write_buffer = WriteBuffer },
    %% When writing to a new segment we must prepare the file
    %% and update our segments state.
    SegmentEntryCount = segment_entry_count(),
    ThisSegment = SeqId div SegmentEntryCount,
    State2 = case maps:is_key(ThisSegment, Segments) of
        true -> State1;
        false -> new_segment_file(ThisSegment, State1)
    end,
    %% When publisher confirms have been requested for this
    %% message we mark the message as unconfirmed.
    State = maybe_mark_unconfirmed(MsgId, Props, State2),
    maybe_flush_buffer(State).

new_segment_file(Segment, State = #qi{ segments = Segments }) ->
    #qi{ fds = OpenFds } = reduce_fd_usage(Segment, State),
    false = maps:is_key(Segment, OpenFds), %% assert
    {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
    %% We must preallocate space for the file. We want the space
    %% to be allocated to not have to worry about errors when
    %% writing later on.
    SegmentEntryCount = segment_entry_count(),
    Size = ?HEADER_SIZE + SegmentEntryCount * ?ENTRY_SIZE,
    case file:allocate(Fd, 0, Size) of
        ok ->
            ok;
        %% On some platforms file:allocate is not supported (e.g. Windows).
        %% In that case we fill the file with zeroes manually.
        {error, enotsup} ->
            ok = file:write(Fd, <<0:Size/unit:8>>),
            {ok, 0} = file:position(Fd, bof),
            %% We do a full GC immediately after because we do not want
            %% to keep around the large binary we used to fill the file.
            _ = garbage_collect(),
            ok
    end,
    %% We then write the segment file header. It contains
    %% some useful info and some reserved bytes for future use.
    %% We currently do not make use of this information. It is
    %% only there for forward compatibility purposes (for example
    %% to support an index with mixed segment_entry_count() values).
    FromSeqId = Segment * SegmentEntryCount,
    ToSeqId = FromSeqId + SegmentEntryCount,
    ok = file:write(Fd, << ?MAGIC:32,
                           ?VERSION:8,
                           FromSeqId:64/unsigned,
                           ToSeqId:64/unsigned,
                           0:344 >>),
    %% Keep the file open.
    %% @todo Does segment file deleting work when not all entries get written? Probably not. Should keep an accurate count.
    State#qi{ segments = Segments#{Segment => SegmentEntryCount},
              fds = OpenFds#{Segment => Fd} }.

%% We try to keep the number of FDs open at 4 at a maximum.
%% Under normal circumstances we will end up with 1 or 2
%% open (roughly one for reading, one for writing, when
%% the consumer lags a little) so this is mostly to avoid
%% using too many FDs when the consumer lags a lot. We
%% limit at 4 because we try to keep up to 2 for reading
%% and 2 for writing.
reduce_fd_usage(SegmentToOpen, State = #qi{ fds = OpenFds })
        when map_size(OpenFds) < 4 ->
    %% The only case where we need to update reservations is
    %% when we are opening a segment that wasn't already open,
    %% and we are not closing another segment at the same time.
    case OpenFds of
        #{SegmentToOpen := _} ->
            State;
        _ ->
            file_handle_cache:set_reservation(map_size(OpenFds) + 1),
            State
    end;
reduce_fd_usage(SegmentToOpen, State = #qi{ fds = OpenFds0 }) ->
    case OpenFds0 of
        #{SegmentToOpen := _} ->
            State;
        _ ->
            %% We know we have 4 FDs open. Because we are typically reading
            %% or acking the oldest and writing to the newest, we will close
            %% the FDs in the middle here.
            OpenSegments = lists:sort(maps:keys(OpenFds0)),
            [_Left, MidLeft, MidRight, _Right] = OpenSegments,
            SegmentToClose = if
                %% We are opening a segment after the mid right FD. Close it.
                SegmentToOpen > MidRight ->
                    MidRight;
                %% We are opening a segment before the mid left FD. Close it.
                SegmentToOpen < MidLeft ->
                    MidLeft;
                %% Otherwise pick the middle segment close to where we are located.
                %% When we are located at equal distance we arbitrarily favor the
                %% right segment.
                MidRight - SegmentToOpen < SegmentToOpen - MidLeft ->
                    MidRight;
                true ->
                    MidLeft
            end,
            ?DEBUG("~0p ~0p ~0p", [SegmentToOpen, OpenSegments, SegmentToClose]),
            {Fd, OpenFds} = maps:take(SegmentToClose, OpenFds0),
            ok = file:close(Fd),
            State#qi{ fds = OpenFds }
    end.

maybe_mark_unconfirmed(MsgId, #message_properties{ needs_confirming = true },
        State = #qi { confirms = Confirms }) ->
    State#qi{ confirms = gb_sets:add_element(MsgId, Confirms) };
maybe_mark_unconfirmed(_, _, State) ->
    State.

%% @todo Perhaps make the two limits configurable. Also refine the default.
maybe_flush_buffer(State = #qi { write_buffer = WriteBuffer,
                                 write_buffer_updates = NumUpdates }) ->
    if
        %% When we have at least 100 entries, we always want to flush,
        %% in order to limit the memory usage and avoid losing too much
        %% data on crashes.
        (map_size(WriteBuffer) - NumUpdates) >= 100 ->
            flush_buffer(State, full);
        %% We may want to flush updates (acks) when
        %% too many have accumulated.
        NumUpdates >= 2000 ->
            flush_buffer(State, updates);
        %% Otherwise we do not flush this time.
        true ->
            State
    end.

flush_buffer(State0 = #qi { write_buffer = WriteBuffer0,
                            cache = Cache0 },
             FlushType) ->
    SegmentEntryCount = segment_entry_count(),
    %% First we prepare the writes sorted by segment.
    {Writes, WriteBuffer, AcksToCache} = maps:fold(fun
        (SeqId, ack, {WritesAcc, BufferAcc, AcksAcc}) when FlushType =:= updates ->
            {write_ack(SeqId, SegmentEntryCount, WritesAcc),
             BufferAcc,
             AcksAcc#{SeqId => ack}};
        (SeqId, ack, {WritesAcc, BufferAcc, AcksAcc}) ->
            {write_ack(SeqId, SegmentEntryCount, WritesAcc),
             BufferAcc,
             AcksAcc};
        (SeqId, Entry, {WritesAcc, BufferAcc, AcksAcc}) when FlushType =:= updates ->
            {WritesAcc,
             BufferAcc#{SeqId => Entry},
             AcksAcc};
        %% Otherwise we write the entire buffer.
        (SeqId, Entry, {WritesAcc, BufferAcc, AcksAcc}) ->
            {write_entry(SeqId, SegmentEntryCount, Entry, WritesAcc),
             BufferAcc,
             AcksAcc}
    end, {#{}, #{}, #{}}, WriteBuffer0),
    %% Then we do the writes for each segment.
    State = maps:fold(fun(Segment, LocBytes, FoldState0) ->
        FoldState1 = reduce_fd_usage(Segment, FoldState0),
        {Fd, FoldState} = get_fd_for_segment(Segment, FoldState1),
        ok = file:pwrite(Fd, LocBytes),
        file_handle_cache_stats:update(queue_index_write),
        FoldState
    end, State0, Writes),
    %% Update the cache. If we are flushing the entire write buffer,
    %% then that buffer becomes the new cache. Otherwise remove the
    %% old acks from the cache and add the new acks.
    Cache = case FlushType of
        full ->
            WriteBuffer0;
        updates ->
            Cache1 = maps:fold(fun
                (_, ack, CacheAcc) -> CacheAcc;
                (SeqId, Entry, CacheAcc) -> CacheAcc#{SeqId => Entry}
            end, #{}, Cache0),
            maps:merge(Cache1, AcksToCache)
    end,
    %% Finally we update the state.
    State#qi{ write_buffer = WriteBuffer,
              write_buffer_updates = 0,
              cache = Cache }.

write_ack(SeqId, SegmentEntryCount, WritesAcc) ->
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    LocBytesAcc = maps:get(Segment, WritesAcc, []),
    WritesAcc#{Segment => [{Offset, <<0>>}|LocBytesAcc]}.

write_entry(SeqId, SegmentEntryCount, Entry, WritesAcc) ->
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    LocBytesAcc = maps:get(Segment, WritesAcc, []),
    WritesAcc#{Segment => [{Offset, build_entry(Entry)}|LocBytesAcc]}.

build_entry({Id, _SeqId, Location, Props, IsPersistent}) ->
    Flags = case IsPersistent of
        true -> 1;
        false -> 0
    end,
    LocationBin = case Location of
        memory ->
            << 0:136 >>;
        rabbit_msg_store ->
            << 1:8,
               Id:16/binary             %% Message store ID.
               >>;
        {rabbit_classic_queue_store_v2, StoreOffset, StoreSize} ->
            << 2:8,
               StoreOffset:64/unsigned, %% Per-queue store offset.
               StoreSize:32/unsigned,   %% Per-queue store size.
               0:32 >>
    end,
    #message_properties{ expiry = Expiry0, size = Size } = Props,
    Expiry = case Expiry0 of
        undefined -> 0;
        _ -> Expiry0
    end,
    << 1:8,                   %% Status. 0 = no entry or entry acked, 1 = entry exists.
       Flags:8,               %% IsPersistent flag (least significant bit).
       0:8,                   %% Unused.
       LocationBin/binary,
       %% The shared message store uses 8 bytes for the size but that was never needed
       %% as the v1 index always used 4 bytes. So we are using 4 bytes here as well.
       Size:32/unsigned,      %% Message payload size.
       Expiry:64/unsigned >>. %% Expiration time.

get_fd_for_segment(Segment, State = #qi{ fds = OpenFds }) ->
    case OpenFds of
        #{ Segment := Fd } ->
            {Fd, State};
        _ ->
            {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            {Fd, State#qi{ fds = OpenFds#{ Segment => Fd }}}
    end.

%% When marking acks we need to update the file(s) on disk.
%% When a file has been fully acked we may also close its
%% open FD if any and delete it.

-spec ack([rabbit_variable_queue:seq_id()], State) -> State when State::state().

%% The rabbit_variable_queue module may call this function
%% with an empty list. Do nothing.
ack([], State) ->
    ?DEBUG("[] ~0p", [State]),
    {[], State};
ack(SeqIds, State0 = #qi{ write_buffer = WriteBuffer0,
                          write_buffer_updates = NumUpdates0,
                          cache = Cache0 }) ->
    ?DEBUG("~0p ~0p", [SeqIds, State0]),
    %% We start by updating the ack state information. We then
    %% use this information to delete segment files on disk that
    %% were fully acked.
    {Deletes, State1} = update_ack_state(SeqIds, State0),
    State = delete_segments(Deletes, State1),
    %% We add acks to the write buffer. We take special care not to add
    %% acks for segments that have been deleted. We do this using second
    %% step when there have been deletes to avoid having extra overhead
    %% in the normal case.
    {WriteBuffer1, NumUpdates1, Cache} = lists:foldl(fun ack_fold_fun/2,
                                                     {WriteBuffer0, NumUpdates0, Cache0},
                                                     SeqIds),
    {WriteBuffer, NumUpdates} = case Deletes of
        [] ->
            {WriteBuffer1, NumUpdates1};
        _ ->
            {WriteBuffer2, NumUpdates2, _, _} = maps:fold(
                fun ack_delete_fold_fun/3,
                {#{}, 0, Deletes, segment_entry_count()},
                WriteBuffer1),
            {WriteBuffer2, NumUpdates2}
    end,
    {Deletes, maybe_flush_buffer(State#qi{ write_buffer = WriteBuffer,
                                           write_buffer_updates = NumUpdates,
                                           cache = Cache })}.

update_ack_state(SeqIds, State = #qi{ segments = Segments0 }) ->
    SegmentEntryCount = segment_entry_count(),
    {Delete, Segments} = lists:foldl(fun(SeqId, {DeleteAcc, Segments1}) ->
        Segment = SeqId div SegmentEntryCount,
        case Segments1 of
            #{Segment := 1} ->
                %% We remove the segment information immediately.
                %% The file itself will be removed after we return.
                {[Segment|DeleteAcc], maps:remove(Segment, Segments1)};
            #{Segment := Unacked} ->
                {DeleteAcc, Segments1#{Segment => Unacked - 1}}
        end
    end, {[], Segments0}, SeqIds),
    {Delete, State#qi{ segments = Segments }}.

delete_segments([], State) ->
    State;
delete_segments([Segment|Tail], State) ->
    delete_segments(Tail, delete_segment(Segment, State)).

delete_segment(Segment, State0 = #qi{ fds = OpenFds0 }) ->
    %% We close the open fd if any.
    State = case maps:take(Segment, OpenFds0) of
        {Fd, OpenFds} ->
            ok = file:close(Fd),
            case map_size(OpenFds) of
                0 -> file_handle_cache:release_reservation();
                N -> file_handle_cache:set_reservation(N)
            end,
            State0#qi{ fds = OpenFds };
        error ->
            State0
    end,
    %% Then we can delete the segment file.
    ok = file:delete(segment_file(Segment, State)),
    State.

ack_fold_fun(SeqId, {Buffer, Updates, Cache}) ->
    case Buffer of
        %% Remove the write if there is one scheduled. In that case
        %% we will never write this entry to disk.
        #{SeqId := Write} when is_tuple(Write) ->
            {maps:remove(SeqId, Buffer), Updates, Cache};
        %% Otherwise ack an entry that is already on disk.
        %% Remove the entry from the cache if any since it is now covered by the buffer.
        _ when not is_map_key(SeqId, Buffer) ->
            {Buffer#{SeqId => ack}, Updates + 1, maps:remove(SeqId, Cache)}
    end.

%% Remove writes for all segments that have been deleted.
ack_delete_fold_fun(SeqId, Write, {Buffer, Updates, Deletes, SegmentEntryCount}) ->
    IsDeleted = lists:member(SeqId div SegmentEntryCount, Deletes),
    case IsDeleted of
        true when Write =:= ack ->
            {Buffer,
             Updates,
             Deletes, SegmentEntryCount};
        false ->
            {Buffer#{SeqId => Write},
             Updates + case is_atom(Write) of true -> 1; false -> 0 end,
             Deletes, SegmentEntryCount}
    end.

%% A better interface for read/3 would be to request a maximum
%% of N messages, rather than first call next_segment_boundary/3
%% and then read from S1 to S2. This function could then return
%% either N messages or less depending on the current state.

-spec read(rabbit_variable_queue:seq_id(),
           rabbit_variable_queue:seq_id(),
           State) ->
                     {[entry()], State}
                     when State::state().

%% From is inclusive, To is exclusive.

%% Nothing to read because the second argument is exclusive.
read(FromSeqId, ToSeqId, State)
        when FromSeqId =:= ToSeqId ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State]),
    {[], State};
%% Read the messages requested.
read(FromSeqId, ToSeqId, State0 = #qi{ write_buffer = WriteBuffer,
                                       cache = Cache }) ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State0]),
    %% We first try to read from the write buffer what we can,
    %% then from the cache, then we read the rest from disk.
    {Reads0, SeqIdsOnDisk} = read_from_buffers(FromSeqId, ToSeqId,
                                               WriteBuffer, Cache,
                                               [], []),
    {Reads1, State} = read_from_disk(SeqIdsOnDisk,
                                     State0,
                                     Reads0),
    %% The messages may not be in the correct order. Fix that.
    Reads = lists:keysort(2, Reads1),
    {Reads, State}.

read_from_buffers(ToSeqId, ToSeqId, _, _, SeqIdsOnDisk, Reads) ->
    %% We must do a lists:reverse here so that we are able
    %% to read multiple continuous messages from disk in one call.
    {Reads, lists:reverse(SeqIdsOnDisk)};
read_from_buffers(SeqId, ToSeqId, WriteBuffer, Cache, SeqIdsOnDisk, Reads) ->
    case WriteBuffer of
        #{SeqId := ack} ->
            read_from_buffers(SeqId + 1, ToSeqId, WriteBuffer, Cache, SeqIdsOnDisk, Reads);
        #{SeqId := Entry} when is_tuple(Entry) ->
            read_from_buffers(SeqId + 1, ToSeqId, WriteBuffer, Cache, SeqIdsOnDisk, [Entry|Reads]);
        _ ->
            %% Nothing was found in the write buffer, but we may have an entry in the cache.
            case Cache of
                #{SeqId := ack} ->
                    read_from_buffers(SeqId + 1, ToSeqId, WriteBuffer, Cache, SeqIdsOnDisk, Reads);
                #{SeqId := Entry} when is_tuple(Entry) ->
                    read_from_buffers(SeqId + 1, ToSeqId, WriteBuffer, Cache, SeqIdsOnDisk, [Entry|Reads]);
                _ ->
                    read_from_buffers(SeqId + 1, ToSeqId, WriteBuffer, Cache, [SeqId|SeqIdsOnDisk], Reads)
            end
    end.

%% We try to minimize the number of file:read calls when reading from
%% the disk. We find the number of continuous messages, read them all
%% at once, and then repeat the loop.

read_from_disk([], State, Acc) ->
    {Acc, State};
read_from_disk(SeqIdsToRead0, State0 = #qi{ write_buffer = WriteBuffer }, Acc0) ->
    FirstSeqId = hd(SeqIdsToRead0),
    %% We get the highest continuous seq_id() from the same segment file.
    %% If there are more continuous entries we will read them on the
    %% next loop.
    {LastSeqId, SeqIdsToRead} = highest_continuous_seq_id(SeqIdsToRead0,
                                                          next_segment_boundary(FirstSeqId)),
    ReadSize = (LastSeqId - FirstSeqId + 1) * ?ENTRY_SIZE,
    {Fd, OffsetForSeqId, State} = get_fd(FirstSeqId, State0),
    file_handle_cache_stats:update(queue_index_read),
    {ok, EntriesBin} = file:pread(Fd, OffsetForSeqId, ReadSize),
    %% We cons new entries into the Acc and only reverse it when we
    %% are completely done reading new entries.
    Acc = parse_entries(EntriesBin, FirstSeqId, WriteBuffer, Acc0),
    read_from_disk(SeqIdsToRead, State, Acc).

get_fd(SeqId, State0) ->
    SegmentEntryCount = segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    State1 = reduce_fd_usage(Segment, State0),
    {Fd, State} = get_fd_for_segment(Segment, State1),
    {Fd, Offset, State}.

%% When recovering from a dirty shutdown, we may end up reading entries that
%% have already been acked. We do not add them to the Acc in that case, and
%% as a result we may end up returning less messages than initially expected.

parse_entries(<<>>, _, _, Acc) ->
    Acc;
parse_entries(<< Status:8,
                 0:7, IsPersistent:1,
                 0:8,
                 LocationBin:136/bits,
                 Size:32/unsigned,
                 Expiry0:64/unsigned,
                 Rest/bits >>, SeqId, WriteBuffer, Acc) ->
    %% We skip entries that have already been acked. This may
    %% happen when we recover from a dirty shutdown or when
    %% some messages were requeued.
    case Status of
        1 ->
            %% We get the Id binary in two steps because we do not want
            %% to create a sub-binary and keep the larger binary around
            %% in memory.
            {Id, Location} = case LocationBin of
                << 0:136 >> ->
                    {undefined, memory};
                << 1:8, Id0:16/binary >> ->
                    {binary:copy(Id0), rabbit_msg_store};
                << 2:8, StoreOffset:64/unsigned, StoreSize:32/unsigned, 0:32 >> ->
                    {undefined, {rabbit_classic_queue_store_v2, StoreOffset, StoreSize}}
            end,
            Expiry = case Expiry0 of
                0 -> undefined;
                _ -> Expiry0
            end,
            Props = #message_properties{expiry = Expiry, size = Size},
            parse_entries(Rest, SeqId + 1, WriteBuffer,
                          [{Id, SeqId, Location, Props, IsPersistent =:= 1}|Acc]);
        0 -> %% No entry or acked entry.
            parse_entries(Rest, SeqId + 1, WriteBuffer, Acc)
    end.

%% ----
%%
%% Syncing and flushing to disk requested by the queue.
%% Note: the v2 no longer calls fsync, it only flushes.

-spec sync(State) -> State when State::state().

sync(State0 = #qi{ confirms = Confirms,
                   on_sync = OnSyncFun }) ->
    ?DEBUG("~0p", [State0]),
    State = flush_buffer(State0, full),
    OnSyncFun(Confirms),
    State#qi{ confirms = gb_sets:new() }.

-spec needs_sync(state()) -> 'false'.

needs_sync(State = #qi{ confirms = Confirms }) ->
    ?DEBUG("~0p", [State]),
    case gb_sets:is_empty(Confirms) of
        true -> false;
        false -> confirms
    end.

-spec flush(State) -> State when State::state().

flush(State) ->
    ?DEBUG("~0p", [State]),
    %% Flushing to disk is the same operation as sync
    %% except it is called before hibernating or when
    %% reducing memory use.
    sync(State).

%% ----
%%
%% Defer to rabbit_queue_index for recovery for the time being.
%% We can move the functions here when the old index is removed.

start(VHost, DurableQueueNames) ->
    ?DEBUG("~0p ~0p", [VHost, DurableQueueNames]),
    %% We replace the queue_index_walker function with our own.
    %% Everything else remains the same.
    {OrderedTerms, {_QueueIndexWalkerFun, FunState}} = rabbit_queue_index:start(VHost, DurableQueueNames),
    {OrderedTerms, {fun queue_index_walker/1, FunState}}.

queue_index_walker({start, DurableQueues}) when is_list(DurableQueues) ->
    ?DEBUG("~0p", [{start, DurableQueues}]),
    {ok, Gatherer} = gatherer:start_link(),
    [begin
         ok = gatherer:fork(Gatherer),
         ok = worker_pool:submit_async(
                fun () -> link(Gatherer),
                          try
                              queue_index_walker_reader(QueueName, Gatherer)
                          after
                              unlink(Gatherer)
                          end,
                          ok
                end)
     end || QueueName <- DurableQueues],
    queue_index_walker({next, Gatherer});
queue_index_walker({next, Gatherer}) when is_pid(Gatherer) ->
    ?DEBUG("~0p", [{next, Gatherer}]),
    case gatherer:out(Gatherer) of
        empty ->
            ok = gatherer:stop(Gatherer),
            finished;
        {value, {MsgId, Count}} ->
            {MsgId, Count, {next, Gatherer}}
    end.

queue_index_walker_reader(#resource{ virtual_host = VHost } = Name, Gatherer) ->
    ?DEBUG("~0p ~0p", [Name, Gatherer]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    SegmentFiles = rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir),
    _ = [queue_index_walker_segment(filename:join(Dir, F), Gatherer) || F <- SegmentFiles],
    %% When there are files belonging to the old index, we go through
    %% the old index walker function as well.
    case rabbit_file:wildcard(".*\\.idx", Dir) of
        [_|_] ->
            rabbit_queue_index:queue_index_walker_reader(Name, Gatherer);
        [] ->
            ok
    end,
    ok = gatherer:finish(Gatherer).

queue_index_walker_segment(F, Gatherer) ->
    ?DEBUG("~0p ~0p", [F, Gatherer]),
    {ok, Fd} = file:open(F, [read, read_ahead, raw, binary]),
    case file:read(Fd, ?HEADER_SIZE) of
        {ok, <<?MAGIC:32,?VERSION:8,
               FromSeqId:64/unsigned,ToSeqId:64/unsigned,
               _/bits>>} ->
            queue_index_walker_segment(Fd, Gatherer, 0, ToSeqId - FromSeqId);
        _ ->
            %% Invalid segment file. Skip.
            ok
    end,
    ok = file:close(Fd).

queue_index_walker_segment(_, _, N, N) ->
    %% We reached the end of the segment file.
    ok;
queue_index_walker_segment(Fd, Gatherer, N, Total) ->
    case file:read(Fd, ?ENTRY_SIZE) of
        %% We found a non-ack persistent entry. Gather it.
        {ok, <<1,_:7,1:1,_,1,Id:16/binary,_/bits>>} ->
            gatherer:sync_in(Gatherer, {Id, 1}),
            queue_index_walker_segment(Fd, Gatherer, N + 1, Total);
        %% We found an ack, a transient entry or a non-entry. Skip it.
        {ok, _} ->
            queue_index_walker_segment(Fd, Gatherer, N + 1, Total)
    end.

stop(VHost) ->
    ?DEBUG("~0p", [VHost]),
    rabbit_queue_index:stop(VHost).

%% ----
%%
%% These functions either call the normal functions or are no-ops.
%% They relate to specific optimizations of rabbit_queue_index and
%% rabbit_variable_queue.
%%
%% @todo The way pre_publish works is still fairly puzzling.
%%       When the old index gets removed we can just drop
%%       these functions.

pre_publish(MsgOrId, SeqId, Location, Props, IsPersistent, TargetRamCount, State) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p ~0p", [MsgOrId, SeqId, Location, Props, IsPersistent, TargetRamCount, State]),
    publish(MsgOrId, SeqId, Location, Props, IsPersistent, TargetRamCount, State).

%% @todo -spec flush_pre_publish_cache(???, State) -> State when State::state().

flush_pre_publish_cache(TargetRamCount, State) ->
    ?DEBUG("~0p ~0p", [TargetRamCount, State]),
    State.

%% See comment in rabbit_queue_index:bounds/1. We do not need to be
%% accurate about these values because they are simply used as lowest
%% and highest possible bounds. In fact we HAVE to be inaccurate for
%% the test suite to pass. This can probably be made more accurate
%% in the future.

-spec bounds(State) ->
                       {non_neg_integer(), non_neg_integer(), State}
                       when State::state().

bounds(State = #qi{ segments = Segments }) ->
    ?DEBUG("~0p", [State]),
    %% We must special case when we are empty to make tests happy.
    if
        Segments =:= #{} ->
            {0, 0, State};
        true ->
            SegmentEntryCount = segment_entry_count(),
            SortedSegments = lists:sort(maps:keys(Segments)),
            Oldest = hd(SortedSegments),
            Newest = hd(lists:reverse(SortedSegments)),
            {Oldest * SegmentEntryCount,
             (1 + Newest) * SegmentEntryCount,
             State}
    end.

%% The next_segment_boundary/1 function is used internally when
%% reading. It should not be called from rabbit_variable_queue.

-spec next_segment_boundary(SeqId) -> SeqId when SeqId::rabbit_variable_queue:seq_id().

next_segment_boundary(SeqId) ->
    ?DEBUG("~0p", [SeqId]),
    SegmentEntryCount = segment_entry_count(),
    (1 + (SeqId div SegmentEntryCount)) * SegmentEntryCount.

%% ----
%%
%% Internal.

segment_entry_count() ->
    %% A value lower than the max write_buffer size results in nothing needing
    %% to be written to disk as long as the consumer consumes as fast as the
    %% producer produces.
    application:get_env(rabbit, classic_queue_index_v2_segment_entry_count, 4096).

%% Note that store files will also be removed if there are any in this directory.
%% Currently the v2 per-queue store expects this function to remove its own files.

erase_index_dir(Dir) ->
    case rabbit_file:is_dir(Dir) of
        true  -> rabbit_file:recursive_delete([Dir]);
        false -> ok
    end.

queue_dir(VHostDir, QueueName) ->
    %% Queue directory is
    %% {node_database_dir}/msg_stores/vhosts/{vhost}/queues/{queue}
    QueueDir = queue_name_to_dir_name(QueueName),
    filename:join([VHostDir, "queues", QueueDir]).

queue_name_to_dir_name(#resource { kind = queue,
                                   virtual_host = VHost,
                                   name = QName }) ->
    <<Num:128>> = erlang:md5(<<"queue", VHost/binary, QName/binary>>),
    rabbit_misc:format("~.36B", [Num]).

segment_file(Segment, #qi{ dir = Dir }) ->
    filename:join(Dir, integer_to_list(Segment) ++ ?SEGMENT_EXTENSION).

highest_continuous_seq_id([SeqId|Tail], EndSeqId)
        when (1 + SeqId) =:= EndSeqId ->
    {SeqId, Tail};
highest_continuous_seq_id([SeqId1, SeqId2|Tail], EndSeqId)
        when (1 + SeqId1) =:= SeqId2 ->
    highest_continuous_seq_id([SeqId2|Tail], EndSeqId);
highest_continuous_seq_id([SeqId|Tail], _) ->
    {SeqId, Tail}.
