%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_modern_queue_index).

-export([erase/1, init/3, reset_state/1, recover/6,
         terminate/3, delete_and_terminate/1,
         publish/7, deliver/2, ack/2, read/3]).

%% Recovery. Unlike other functions in this module, these
%% apply to all queues all at once.
-export([start/2, stop/1]).

%% rabbit_queue_index/rabbit_variable_queue-specific functions.
%% Implementation details from the queue index leaking into the
%% queue implementation itself.
-export([pre_publish/7, flush_pre_publish_cache/2,
         sync/1, needs_sync/1, flush/1,
         bounds/1, next_segment_boundary/1]).

-define(QUEUE_NAME_STUB_FILE, ".queue_name").
-define(SEGMENT_EXTENSION, ".midx").

-define(MAGIC, 16#524D5149). %% "RMQI"
-define(VERSION, 1).
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

-type seq_id() :: non_neg_integer().
%% @todo Use a shared seq_id() type in all relevant modules.

-type entry() :: {rabbit_types:msg_id(), seq_id(), rabbit_types:message_properties(), boolean(), boolean()}.

-record(mqistate, {
    %% Queue name (for the stub file).
    queue_name :: rabbit_amqqueue:name(),

    %% Queue index directory.
    dir :: file:filename(),

    %% Buffer of all write operations to be performed.
    %% When the buffer reaches a certain size, we reduce
    %% the buffer size by first checking if writing the
    %% delivers and acks gets us to a comfortable buffer
    %% size. If we do, write only the delivers and acks.
    %% If not, write everything. This allows the reader
    %% to continue reading from memory if it is fast
    %% enough to keep up with the producer.
    write_buffer = #{} :: #{seq_id() => entry() | deliver | ack},

    %% The number of entries in the write buffer that
    %% refer to an update to the file, rather than a
    %% full entry. Updates include deliver and ack.
    %% Used to determine if flushing only delivers/acks
    %% is enough to get the buffer to a comfortable size.
    write_buffer_updates = 0 :: non_neg_integer(),

    %% Messages waiting for publisher confirms. The
    %% publisher confirms will be sent when the message
    %% has been synced to disk (as an entry or an ack).
    %%
    %% The index does not call file:sync/1 by default.
    %% It only does when publisher confirms are used
    %% and there are outstanding unconfirmed messages.
    %% In that case the buffer is flushed to disk when
    %% the queue requests a sync (after a timeout).
    confirms = #{} :: #{seq_id() => rabbit_types:msg_id()},

    %% Segments we currently know of along with the
    %% number of unacked messages remaining in the
    %% segment. We use this to determine whether a
    %% segment file can be deleted.
    segments = #{} :: #{non_neg_integer() => pos_integer()},

    %% File descriptors.
    %% @todo The current implementation does not limit the number of FDs open.
    %%       This is most likely not desirable... Perhaps we can have a limit
    %%       per queue index instead of relying on file_handle_cache.
    fds = #{} :: #{non_neg_integer() => file:fd()},

    %% This fun must be called when messages that expect
    %% confirms have either an ack or their entry
    %% written to disk and file:sync/1 has been called.
    on_sync :: on_sync_fun()
}).

-type mqistate() :: #mqistate{}.
-export_type([mqistate/0]).

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
                 on_sync_fun(), on_sync_fun()) -> mqistate().

%% We do not embed messages and as a result never need the OnSyncMsgFun.

init(#resource{ virtual_host = VHost } = Name, OnSyncFun, _OnSyncMsgFun) ->
    ?DEBUG("~0p ~0p", [Name, OnSyncFun]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    false = rabbit_file:is_file(Dir), %% is_file == is file or dir
    init1(Name, Dir, OnSyncFun).

init1(Name, Dir, OnSyncFun) ->
    ensure_queue_name_stub_file(Name, Dir),
    #mqistate{
        queue_name = Name,
        dir = Dir,
        on_sync = OnSyncFun
    }.

ensure_queue_name_stub_file(#resource{virtual_host = VHost, name = QName}, Dir) ->
    QueueNameFile = filename:join(Dir, ?QUEUE_NAME_STUB_FILE),
    ok = filelib:ensure_dir(QueueNameFile),
    ok = file:write_file(QueueNameFile, <<"VHOST: ", VHost/binary, "\n",
                                          "QUEUE: ", QName/binary, "\n">>).

-spec reset_state(State) -> State when State::mqistate().

reset_state(State = #mqistate{ queue_name     = Name,
                               dir            = Dir,
                               on_sync        = OnSyncFun }) ->
    ?DEBUG("~0p", [State]),
    delete_and_terminate(State),
    init1(Name, Dir, OnSyncFun).

-spec recover(rabbit_amqqueue:name(), shutdown_terms(), boolean(),
                    contains_predicate(),
                    on_sync_fun(), on_sync_fun()) ->
                        {'undefined' | non_neg_integer(),
                         'undefined' | non_neg_integer(), mqistate()}.

-define(RECOVER_COUNT, 1).
-define(RECOVER_BYTES, 2).
%% @todo Also count the number of entries dropped because the message wasn't in the store,
%%       and log something at the end. We could also count the number of bytes dropped.
%% @todo We may also want to log holes inside files.

recover(#resource{ virtual_host = VHost } = Name, Terms, IsMsgStoreClean,
        ContainsCheckFun, OnSyncFun, _OnSyncMsgFun) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p", [Name, Terms, IsMsgStoreClean, ContainsCheckFun, OnSyncFun]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    State0 = init1(Name, Dir, OnSyncFun),
    %% We go over all segments if either the index or the
    %% message store has/had to recover. Otherwise we just
    %% take our state from Terms.
    IsIndexClean = Terms =/= non_clean_shutdown,
    {Count, Bytes, State} = case IsIndexClean andalso IsMsgStoreClean of
        true ->
            Segments0 = proplists:get_value(mqi_state, Terms, #{}),
            State1 = State0#mqistate{ segments = Segments0 },
            %% The queue has stored the count/bytes values inside
            %% Terms so we don't need to provide them again.
            {undefined,
             undefined,
             State1};
        false ->
            CountersRef = counters:new(2, []),
            State1 = recover_segments(State0, ContainsCheckFun, CountersRef),
            {counters:get(CountersRef, ?RECOVER_COUNT),
             counters:get(CountersRef, ?RECOVER_BYTES),
             State1}
    end,
    %% We can now ensure the current segment file is in a good state and return.
    {Count, Bytes, State}.

recover_segments(State = #mqistate { dir = Dir }, ContainsCheckFun, CountersRef) ->
    SegmentFiles = rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir),
    case SegmentFiles of
        %% No segments found. Keep default values.
        [] ->
            State;
        %% Count unackeds in the segments.
        _ ->
            Segments = lists:sort([
                list_to_integer(filename:basename(F, ?SEGMENT_EXTENSION))
            || F <- SegmentFiles]),
            recover_segments(State, ContainsCheckFun, CountersRef, Segments)
    end.

recover_segments(State, _, _, []) ->
    State;
recover_segments(State0, ContainsCheckFun, CountersRef, [Segment|Tail]) ->
    %% @todo We may want to check that the file sizes are correct before attempting
    %%       to parse them, and to correct the file sizes.
    SegmentEntryCount = segment_entry_count(),
    SegmentFile = segment_file(Segment, State0),
    {ok, Fd} = file:open(SegmentFile, [read, read_ahead, write, raw, binary]),
    {ok, <<?MAGIC:32,?VERSION:8,
           _FromSeqId:64/unsigned,_ToSeqId:64/unsigned,
           _/bits>>} = file:read(Fd, ?HEADER_SIZE),
    {Action, State} = recover_segment(State0, ContainsCheckFun, CountersRef, Fd,
                                      Segment, 0, SegmentEntryCount,
                                      SegmentEntryCount, []),
    ok = file:close(Fd),
    ok = case Action of
        delete -> file:delete(SegmentFile);
        keep -> ok
    end,
    recover_segments(State, ContainsCheckFun, CountersRef, Tail).

recover_segment(State = #mqistate{ segments = Segments }, _, _, Fd,
                Segment, ThisEntry, SegmentEntryCount,
                Unacked, LocBytes)
                when ThisEntry =:= SegmentEntryCount ->
    case Unacked of
        0 ->
            %% There are no more messages in this segment file.
            {delete, State};
        _ ->
            %% We must update some messages on disk (ack or deliver).
            ok = file:pwrite(Fd, LocBytes),
            {keep, State#mqistate{ segments = Segments#{ Segment => Unacked }}}
    end;
recover_segment(State, ContainsCheckFun, CountersRef, Fd,
                Segment, ThisEntry, SegmentEntryCount,
                Unacked, LocBytes0) ->
    case file:read(Fd, ?ENTRY_SIZE) of
        %% We found a non-ack persistent entry. Check that the corresponding
        %% message exists in the message store. If not, mark this message as
        %% acked.
        {ok, <<1,IsDelivered:8,1,_:8,Id:16/binary,Size:32/unsigned,_/bits>>} ->
            case ContainsCheckFun(Id) of
                %% Message is in the store. We mark all those
                %% messages as delivered if they were not already
                %% (like the old index).
                true ->
                    LocBytes = case IsDelivered of
                        1 -> LocBytes0;
                        0 -> [{?HEADER_SIZE + ThisEntry * ?ENTRY_SIZE + 1, <<1>>}|LocBytes0]
                    end,
                    counters:add(CountersRef, ?RECOVER_COUNT, 1),
                    counters:add(CountersRef, ?RECOVER_BYTES, Size),
                    recover_segment(State, ContainsCheckFun, CountersRef,
                                    Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                                    Unacked, LocBytes);
                %% Message is not in the store. Mark it as acked.
                false ->
                    LocBytes = [{?HEADER_SIZE + ThisEntry * ?ENTRY_SIZE, <<0>>}|LocBytes0],
                    recover_segment(State, ContainsCheckFun, CountersRef,
                                    Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                                    Unacked - 1, LocBytes)
            end;
        %% We found a non-ack transient entry. We mark the message as acked
        %% without checking with the message store, because it already dropped
        %% this message via the queue walker functions.
        {ok, <<1,_,0,_/bits>>} ->
            LocBytes = [{?HEADER_SIZE + ThisEntry * ?ENTRY_SIZE, <<0>>}|LocBytes0],
            recover_segment(State, ContainsCheckFun, CountersRef,
                            Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                            Unacked - 1, LocBytes);
        %% We found a non-entry or an acked entry. Consider it acked.
        {ok, _} ->
            recover_segment(State, ContainsCheckFun, CountersRef,
                            Fd, Segment, ThisEntry + 1, SegmentEntryCount,
                            Unacked - 1, LocBytes0)
    end.

-spec terminate(rabbit_types:vhost(), [any()], State) -> State when State::mqistate().

terminate(VHost, Terms, State0 = #mqistate { dir = Dir,
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
    %% Write recovery terms for faster recovery.
    rabbit_recovery_terms:store(VHost, filename:basename(Dir),
                                [{mqi_state, Segments} | Terms]),
    State#mqistate{ segments = #{},
                    fds = #{} }.

-spec delete_and_terminate(State) -> State when State::mqistate().

delete_and_terminate(State = #mqistate { dir = Dir,
                                         fds = OpenFds }) ->
    ?DEBUG("~0p", [State]),
    %% Close all FDs.
    _ = maps:map(fun(_, Fd) ->
        ok = file:close(Fd)
    end, OpenFds),
    %% Erase the data on disk.
    ok = erase_index_dir(Dir),
    State#mqistate{ segments = #{},
                    fds = #{} }.

%% All messages must be published sequentially to the modern index.

-spec publish(rabbit_types:msg_id(), seq_id(),
                    rabbit_types:message_properties(), boolean(), boolean(),
                    non_neg_integer(), State) -> State when State::mqistate().

%% Because we always persist to the msg_store, the Msg(Or)Id argument
%% here is always a binary, never a record.
publish(MsgId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount,
        State0 = #mqistate { write_buffer = WriteBuffer0,
                             segments = Segments })
        when is_binary(MsgId)  ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [MsgId, SeqId, Props, IsPersistent, TargetRamCount, State0]),
    %% Add the entry to the write buffer.
    WriteBuffer = WriteBuffer0#{SeqId => {MsgId, SeqId, Props, IsPersistent, IsDelivered}},
    State1 = State0#mqistate{ write_buffer = WriteBuffer },
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
    State = maybe_mark_unconfirmed(MsgId, SeqId, Props, State2),
    maybe_flush_buffer(State).

new_segment_file(Segment, State = #mqistate{ segments = Segments,
                                             fds = OpenFds }) ->
    SegmentEntryCount = segment_entry_count(),
    false = maps:is_key(Segment, OpenFds), %% assert
    {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
    %% We preallocate space for the file when possible.
    %% @todo We NEED this to work. Can this fail on any supported platform?
    ok = file:allocate(Fd, 0, ?HEADER_SIZE + SegmentEntryCount * ?ENTRY_SIZE),
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
    State#mqistate{ segments = Segments#{Segment => SegmentEntryCount},
                    fds = OpenFds#{Segment => Fd} }.

maybe_mark_unconfirmed(MsgId, SeqId, #message_properties{ needs_confirming = true },
        State = #mqistate { confirms = Confirms }) ->
    State#mqistate{ confirms = Confirms#{SeqId => MsgId} };
maybe_mark_unconfirmed(_, _, _, State) ->
    State.

%% @todo Perhaps make the two limits configurable. Also refine the default.
maybe_flush_buffer(State = #mqistate { write_buffer = WriteBuffer,
                                       write_buffer_updates = NumUpdates }) ->
    if
        %% When we have at least 100 entries, we always want to flush,
        %% in order to limit the memory usage and avoid losing too much
        %% data on crashes.
        (map_size(WriteBuffer) - NumUpdates) >= 100 ->
            flush_buffer(State, full);
        %% We may want to flush updates (acks | delivers) when
        %% too many have accumulated.
        NumUpdates >= 2000 ->
            flush_buffer(State, updates);
        %% Otherwise we do not flush this time.
        true ->
            State
    end.

flush_buffer(State0 = #mqistate { write_buffer = WriteBuffer0 },
             FlushType) ->
    SegmentEntryCount = segment_entry_count(),
    %% First we prepare the writes sorted by segment.
    {Writes, WriteBuffer} = maps:fold(fun
        (SeqId, ack, {WritesAcc, BufferAcc}) ->
            {write_ack(SeqId, SegmentEntryCount, WritesAcc),
             BufferAcc};
        (SeqId, deliver, {WritesAcc, BufferAcc}) ->
            {write_deliver(SeqId, SegmentEntryCount, WritesAcc),
             BufferAcc};
        (SeqId, Entry, {WritesAcc, BufferAcc}) when FlushType =:= updates ->
            {WritesAcc,
             BufferAcc#{SeqId => Entry}};
        %% Otherwise we write the entire buffer.
        (SeqId, Entry, {WritesAcc, BufferAcc}) ->
            {write_entry(SeqId, SegmentEntryCount, Entry, WritesAcc),
             BufferAcc}
    end, {#{}, #{}}, WriteBuffer0),
    %% Then we do the writes for each segment.
    State = maps:fold(fun(Segment, LocBytes, FoldState0) ->
        {Fd, FoldState} = get_fd_for_segment(Segment, FoldState0),
        ok = file:pwrite(Fd, LocBytes),
        FoldState
    end, State0, Writes),
    %% Finally we update the state.
    State#mqistate{ write_buffer = WriteBuffer,
                    write_buffer_updates = 0 }.

write_ack(SeqId, SegmentEntryCount, WritesAcc) ->
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    LocBytesAcc = maps:get(Segment, WritesAcc, []),
    WritesAcc#{Segment => [{Offset, <<0>>}|LocBytesAcc]}.

write_deliver(SeqId, SegmentEntryCount, WritesAcc) ->
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    LocBytesAcc = maps:get(Segment, WritesAcc, []),
    WritesAcc#{Segment => [{Offset + 1, <<1>>}|LocBytesAcc]}.

write_entry(SeqId, SegmentEntryCount, Entry, WritesAcc) ->
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    LocBytesAcc = maps:get(Segment, WritesAcc, []),
    WritesAcc#{Segment => [{Offset, build_entry(Entry)}|LocBytesAcc]}.

build_entry({Id, _SeqId, Props, IsPersistent, IsDelivered}) ->
    IsDeliveredFlag = case IsDelivered of
        true -> 1;
        false -> 0
    end,
    Flags = case IsPersistent of
        true -> 1;
        false -> 0
    end,
    #message_properties{ expiry = Expiry0, size = Size } = Props,
    Expiry = case Expiry0 of
        undefined -> 0;
        _ -> Expiry0
    end,
    << 1:8,                   %% Status. 0 = no entry or entry acked, 1 = entry exists.
       IsDeliveredFlag:8,     %% Deliver.
       Flags:8,               %% IsPersistent flag (least significant bit).
       0:8,                   %% Reserved. Makes entries 32B in size to match page alignment on disk.
       Id:16/binary,          %% Message store ID.
       Size:32/unsigned,      %% Message payload size.
       Expiry:64/unsigned >>. %% Expiration time.

get_fd_for_segment(Segment, State = #mqistate{ fds = OpenFds }) ->
    case OpenFds of
        #{ Segment := Fd } ->
            {Fd, State};
        _ ->
            {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            {Fd, State#mqistate{ fds = OpenFds#{ Segment => Fd }}}
    end.

%% When marking delivers we may need to update the file(s) on disk.

-spec deliver([seq_id()], State) -> State when State::mqistate().

%% The rabbit_variable_queue module may call this function
%% with an empty list. Do nothing.
deliver([], State) ->
    ?DEBUG("[] ~0p", [State]),
    State;
deliver(SeqIds, State = #mqistate { write_buffer = WriteBuffer0,
                                    write_buffer_updates = NumUpdates0 }) ->
    ?DEBUG("~0p ~0p", [SeqIds, State]),
    %% Add delivers to the write buffer if necessary.
    {WriteBuffer, NumUpdates} = lists:foldl(fun
        (SeqId, {FoldBuffer, FoldUpdates}) ->
            case FoldBuffer of
                %% Update the IsDelivered flag in the entry if any.
                #{SeqId := {Id, SeqId, Props, IsPersistent, false}} ->
                    {FoldBuffer#{SeqId => {Id, SeqId, Props, IsPersistent, true}},
                     FoldUpdates};
                %% If any other write exists, do nothing.
                #{SeqId := _} ->
                    {FoldBuffer,
                     FoldUpdates};
                %% Otherwise mark for delivery and increase the updates count.
                _ ->
                    {FoldBuffer#{SeqId => deliver},
                     FoldUpdates + 1}
            end
    end, {WriteBuffer0, NumUpdates0}, SeqIds),
    maybe_flush_buffer(State#mqistate{ write_buffer = WriteBuffer,
                                       write_buffer_updates = NumUpdates }).

%% When marking acks we need to update the file(s) on disk.
%% When a file has been fully acked we may also close its
%% open FD if any and delete it.

-spec ack([seq_id()], State) -> State when State::mqistate().

%% The rabbit_variable_queue module may call this function
%% with an empty list. Do nothing.
ack([], State) ->
    ?DEBUG("[] ~0p", [State]),
    State;
ack(SeqIds, State0 = #mqistate{ write_buffer = WriteBuffer0,
                                write_buffer_updates = NumUpdates0 }) ->
    ?DEBUG("~0p ~0p", [SeqIds, State0]),
    %% We start by updating the ack state information. We then
    %% use this information to delete segment files on disk that
    %% were fully acked.
    {Deletes, State1} = update_ack_state(SeqIds, State0),
    State = delete_segments(Deletes, State1),
    %% We add acks to the write buffer. We take special care not to add
    %% acks for segments that have been deleted. We do this using a
    %% separate fold fun when there have been deletes to avoid having
    %% extra overhead in the normal case.
    {WriteBuffer, NumUpdates} = case Deletes of
        [] ->
            lists:foldl(fun ack_fold_fun/2,
                        {WriteBuffer0, NumUpdates0},
                        SeqIds);
        _ ->
            {WriteBuffer1, NumUpdates1, _, _} = lists:foldl(
                        fun ack_with_deletes_fold_fun/2,
                        {WriteBuffer0, NumUpdates0, Deletes, segment_entry_count()},
                        SeqIds),
            {WriteBuffer1, NumUpdates1}
    end,
    maybe_flush_buffer(State#mqistate{ write_buffer = WriteBuffer,
                                       write_buffer_updates = NumUpdates }).

update_ack_state(SeqIds, State = #mqistate{ segments = Segments0 }) ->
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
    {Delete, State#mqistate{ segments = Segments }}.

delete_segments([], State) ->
    State;
delete_segments([Segment|Tail], State) ->
    delete_segments(Tail, delete_segment(Segment, State)).

delete_segment(Segment, State0 = #mqistate{ fds = OpenFds0 }) ->
    %% We close the open fd if any.
    State = case maps:take(Segment, OpenFds0) of
        {Fd, OpenFds} ->
            ok = file:close(Fd),
            State0#mqistate{ fds = OpenFds };
        error ->
            State0
    end,
    %% Then we can delete the segment file.
    ok = file:delete(segment_file(Segment, State)),
    State.

ack_fold_fun(SeqId, {Buffer, Updates}) ->
    case Buffer of
        %% Remove the write if there is one scheduled. In that case
        %% we will never write this entry to disk.
        #{SeqId := Write} when is_tuple(Write) ->
            {maps:remove(SeqId, Buffer), Updates};
        %% Ack if an on-disk entry was marked for delivery.
        #{SeqId := deliver} ->
            {Buffer#{SeqId => ack}, Updates};
        %% Otherwise ack an entry that is already on disk.
        _ when not is_map_key(SeqId, Buffer) ->
            {Buffer#{SeqId => ack}, Updates + 1}
    end.

ack_with_deletes_fold_fun(SeqId, {Buffer, Updates, Deletes, SegmentEntryCount}) ->
    case Buffer of
        %% Remove the write if there is one scheduled. In that case
        %% we will never write this entry to disk.
        #{SeqId := Write} when is_tuple(Write) ->
            {maps:remove(SeqId, Buffer), Updates,
             Deletes, SegmentEntryCount};
        %% Ack if an on-disk entry was marked for delivery,
        %% or remove the deliver if the segment file has been deleted.
        #{SeqId := deliver} ->
            IsDeleted = lists:member(SeqId div SegmentEntryCount, Deletes),
            case IsDeleted of
                false ->
                    {Buffer#{SeqId => ack}, Updates,
                     Deletes, SegmentEntryCount};
                true ->
                    {maps:remove(SeqId, Buffer), Updates - 1,
                     Deletes, SegmentEntryCount}
            end;
        %% Otherwise ack an entry that is already on disk,
        %% unless the segment file has been deleted, in which
        %% case we do not do anything.
        _ when not is_map_key(SeqId, Buffer) ->
            IsDeleted = lists:member(SeqId div SegmentEntryCount, Deletes),
            case IsDeleted of
                false ->
                    {Buffer#{SeqId => ack}, Updates + 1,
                     Deletes, SegmentEntryCount};
                true ->
                    {Buffer, Updates,
                     Deletes, SegmentEntryCount}
            end
    end.

%% A better interface for read/3 would be to request a maximum
%% of N messages, rather than first call next_segment_boundary/3
%% and then read from S1 to S2. This function could then return
%% either N messages or less depending on the current state.

-spec read(seq_id(), seq_id(), State) ->
                     {[entry()], State}
                     when State::mqistate().

%% From is inclusive, To is exclusive.

%% Nothing to read because the second argument is exclusive.
read(FromSeqId, ToSeqId, State)
        when FromSeqId =:= ToSeqId ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State]),
    {[], State};
%% Read the messages requested.
read(FromSeqId, ToSeqId, State0 = #mqistate{ write_buffer = WriteBuffer }) ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State0]),
    %% We first try to read from the write buffer what we can,
    %% then we read the rest from disk.
    {Reads0, SeqIdsOnDisk} = read_from_buffer(FromSeqId, ToSeqId,
                                              WriteBuffer,
                                              [], []),
    {Reads1, State} = read_from_disk(SeqIdsOnDisk,
                                      State0,
                                      Reads0),
    %% The messages may not be in the correct order. Fix that.
    Reads = lists:keysort(2, Reads1),
    {Reads, State}.

read_from_buffer(ToSeqId, ToSeqId, _, SeqIdsOnDisk, Reads) ->
    %% We must do a lists:reverse here so that we are able
    %% to read multiple continuous messages from disk in one call.
    {Reads, lists:reverse(SeqIdsOnDisk)};
read_from_buffer(SeqId, ToSeqId, WriteBuffer, SeqIdsOnDisk, Reads) ->
    case WriteBuffer of
        #{SeqId := ack} ->
            read_from_buffer(SeqId + 1, ToSeqId, WriteBuffer, SeqIdsOnDisk, Reads);
        #{SeqId := Entry} when is_tuple(Entry) ->
            read_from_buffer(SeqId + 1, ToSeqId, WriteBuffer, SeqIdsOnDisk, [Entry|Reads]);
        _ ->
            read_from_buffer(SeqId + 1, ToSeqId, WriteBuffer, [SeqId|SeqIdsOnDisk], Reads)
    end.

%% We try to minimize the number of file:read calls when reading from
%% the disk. We find the number of continuous messages, read them all
%% at once, and then repeat the loop.

read_from_disk([], State, Acc) ->
    {Acc, State};
read_from_disk(SeqIdsToRead0, State0 = #mqistate{ write_buffer = WriteBuffer }, Acc0) ->
    FirstSeqId = hd(SeqIdsToRead0),
    %% We get the highest continuous seq_id() from the same segment file.
    %% If there are more continuous entries we will read them on the
    %% next loop.
    {LastSeqId, SeqIdsToRead} = highest_continuous_seq_id(SeqIdsToRead0,
                                                          next_segment_boundary(FirstSeqId)),
    %% @todo We should limit the read size to avoid creating 2MB binaries all of a sudden...
    ReadSize = (LastSeqId - FirstSeqId + 1) * ?ENTRY_SIZE,
    {Fd, OffsetForSeqId, State} = get_fd(FirstSeqId, State0),
    {ok, EntriesBin} = file:pread(Fd, OffsetForSeqId, ReadSize),
    %% We cons new entries into the Acc and only reverse it when we
    %% are completely done reading new entries.
    Acc = parse_entries(EntriesBin, FirstSeqId, WriteBuffer, Acc0),
    read_from_disk(SeqIdsToRead, State, Acc).

get_fd(SeqId, State = #mqistate{ fds = OpenFds }) ->
    SegmentEntryCount = segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    case OpenFds of
        #{ Segment := Fd } ->
            {Fd, Offset, State};
        _ ->
            {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            {Fd, Offset, State#mqistate{ fds = OpenFds#{ Segment => Fd }}}
    end.

%% When recovering from a dirty shutdown, we may end up reading entries that
%% have already been acked. We do not add them to the Acc in that case, and
%% as a result we may end up returning less messages than initially expected.

parse_entries(<<>>, _, _, Acc) ->
    Acc;
parse_entries(<< Status:8,
                 IsDelivered0:8,
                 _:7, IsPersistent:1,
                 _:8,
                 Id0:16/binary,
                 Size:32/unsigned,
                 Expiry0:64/unsigned,
                 Rest/bits >>, SeqId, WriteBuffer, Acc) ->
    %% We skip entries that have already been acked. This may only
    %% happen when we recover from a dirty shutdown.
    case Status of
        1 ->
            %% We get the Id binary in two steps because we do not want
            %% to create a sub-binary and keep the larger binary around
            %% in memory.
            Id = binary:copy(Id0),
            Expiry = case Expiry0 of
                0 -> undefined;
                _ -> Expiry0
            end,
            Props = #message_properties{expiry = Expiry, size = Size},
            %% We may have a deliver in the write buffer.
            IsDelivered = case IsDelivered0 of
                1 ->
                    true;
                0 ->
                    case WriteBuffer of
                        #{SeqId := deliver} -> true;
                        _ -> false
                    end
            end,
            parse_entries(Rest, SeqId + 1, WriteBuffer,
                          [{Id, SeqId, Props, IsPersistent =:= 1, IsDelivered}|Acc]);
        0 -> %% No entry or acked entry.
            %% @todo It would be good to keep track of how many "misses"
            %%       we have. We can use it to confirm the correct behavior
            %%       of the module in tests, as well as an occasionally
            %%       useful internal metric. Maybe use counters.
            parse_entries(Rest, SeqId + 1, WriteBuffer, Acc)
    end.

%% ----
%%
%% Syncing and flushing to disk requested by the queue.

-spec sync(State) -> State when State::mqistate().

sync(State0 = #mqistate{ confirms = Confirms,
                         fds = OpenFds,
                         on_sync = OnSyncFun }) ->
    ?DEBUG("~0p", [State0]),
    State = flush_buffer(State0, full),
    %% Call file:sync/1 on all open FDs. Some of them might not have
    %% writes but for the time being we don't discriminate.
    _ = maps:fold(fun(_, Fd, _) ->
        ok = file:sync(Fd)
    end, undefined, OpenFds),
    %% Notify syncs.
    Set = gb_sets:from_list(maps:values(Confirms)),
    OnSyncFun(Set),
    %% Reset confirms.
    State#mqistate{ confirms = #{} }.

-spec needs_sync(mqistate()) -> 'false'.

needs_sync(State = #mqistate{ confirms = Confirms }) ->
    ?DEBUG("~0p", [State]),
    case Confirms =:= #{} of
        true -> false;
        false -> confirms
    end.

-spec flush(State) -> State when State::mqistate().

flush(State) ->
    ?DEBUG("~0p", [State]),
    %% Flushing to disk is the same operation as sync
    %% except it is called before hibernating.
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
    {ok, Gatherer} = gatherer:start_link(),
    [begin
         ok = gatherer:fork(Gatherer),
         ok = worker_pool:submit_async(
                fun () -> link(Gatherer),
                          ok = queue_index_walker_reader(QueueName, Gatherer),
                          unlink(Gatherer),
                          ok
                end)
     end || QueueName <- DurableQueues],
    queue_index_walker({next, Gatherer});

queue_index_walker({next, Gatherer}) when is_pid(Gatherer) ->
    case gatherer:out(Gatherer) of
        empty ->
            ok = gatherer:stop(Gatherer),
            finished;
        {value, {MsgId, Count}} ->
            {MsgId, Count, {next, Gatherer}}
    end.

queue_index_walker_reader(#resource{ virtual_host = VHost } = Name, Gatherer) ->
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    SegmentFiles = rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir),
    _ = [queue_index_walker_segment(filename:join(Dir, F), Gatherer) || F <- SegmentFiles],
    ok = gatherer:finish(Gatherer).

queue_index_walker_segment(F, Gatherer) ->
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
        {ok, <<1,_,1,_,Id:16/binary,_/bits>>} ->
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
%% @todo The way pre_publish works is still fairly puzzling. It sounds
%%       like the only thing we should care about is IsDelivered possibly
%%       having changed compared to the previous publish call. When the
%%       old index gets removed we can just drop these functions.

pre_publish(MsgOrId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount, State) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [MsgOrId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount, State]),
    publish(MsgOrId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount, State).

%% @todo -spec flush_pre_publish_cache(???, State) -> State when State::mqistate().

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
                       when State::mqistate().

bounds(State = #mqistate{ segments = Segments }) ->
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

-spec next_segment_boundary(SeqId) -> SeqId when SeqId::seq_id().

next_segment_boundary(SeqId) ->
    ?DEBUG("~0p", [SeqId]),
    SegmentEntryCount = segment_entry_count(),
    (1 + (SeqId div SegmentEntryCount)) * SegmentEntryCount.

%% ----
%%
%% Internal.

segment_entry_count() ->
    %% @todo A value lower than the max write_buffer size results in nothing needing
    %%       to be written to disk as long as the consumer consumes as fast as the
    %%       producer produces. Accidental memory queue?
    %%
    %% @todo Probably put this in the state rather than read it all the time.
    %%       But we must change next_segment_boundary to allow that.
    SegmentEntryCount =
        application:get_env(rabbit, modern_queue_index_segment_entry_count, 65536),
    SegmentEntryCount.

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

segment_file(Segment, #mqistate{ dir = Dir }) ->
    filename:join(Dir, integer_to_list(Segment) ++ ?SEGMENT_EXTENSION).

highest_continuous_seq_id([SeqId|Tail], EndSeqId)
        when (1 + SeqId) =:= EndSeqId ->
    {SeqId, Tail};
highest_continuous_seq_id([SeqId1, SeqId2|Tail], EndSeqId)
        when (1 + SeqId1) =:= SeqId2 ->
    highest_continuous_seq_id([SeqId2|Tail], EndSeqId);
highest_continuous_seq_id([SeqId|Tail], _) ->
    {SeqId, Tail}.
