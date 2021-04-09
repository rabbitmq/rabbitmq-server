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

    %% seq_id() of the next message to be written
    %% in the index. Messages are written sequentially.
    %% Messages are always written to the index.
    write_marker = 0 :: seq_id(),

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

    %% seq_id() of the highest contiguous acked message.
    %% All messages before and including this seq_id()
    %% have been acked. We use this value both as an
    %% optimization and to know which segment files
    %% have been fully acked and can be deleted.
    %%
    %% In addition to the ack marker, messages in the
    %% index will also contain an ack byte flag that
    %% indicates whether that message was acked.
    ack_marker = undefined :: seq_id() | undefined,

    %% seq_id() of messages that have been acked
    %% and are higher than the ack_marker. Messages
    %% are only listed here when there are unacked
    %% messages before them.
    acks = [] :: [seq_id()],

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
    ensure_segment_file(new, #mqistate{
        queue_name = Name,
        dir = Dir,
        on_sync = OnSyncFun
    }).

ensure_queue_name_stub_file(#resource{virtual_host = VHost, name = QName}, Dir) ->
    QueueNameFile = filename:join(Dir, ?QUEUE_NAME_STUB_FILE),
    ok = filelib:ensure_dir(QueueNameFile),
    ok = file:write_file(QueueNameFile, <<"VHOST: ", VHost/binary, "\n",
                                          "QUEUE: ", QName/binary, "\n">>).

ensure_segment_file(Reason, State = #mqistate{ write_marker = WriteMarker,
                                               fds = OpenFds }) ->
    SegmentEntryCount = segment_entry_count(),
    Segment = WriteMarker div SegmentEntryCount,
    %% The file might already be opened during recovery. Don't open it again.
    Fd = case {Reason, OpenFds} of
        {recover, #{Segment := Fd0}} ->
            Fd0;
        _ ->
            false = maps:is_key(Segment, OpenFds), %% assert
            {ok, Fd0} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            Fd0
    end,
    %% When we are recovering from a shutdown we may need to check
    %% whether the file already has content. If it does, we do
    %% nothing. Otherwise, as well as in the normal case, we
    %% pre-allocate the file size and write in the file header.
    IsNewFile = case Reason of
        new ->
            true;
        recover ->
            {ok, #file_info{ size = Size }} = file:read_file_info(Fd),
            Size =:= 0
    end,
    case IsNewFile of
        true ->
            %% We preallocate space for the file when possible.
            %% We don't worry about failure here because an error
            %% is returned when the system doesn't support this feature.
            %% The index will simply be less efficient in that case.
            _ = file:allocate(Fd, 0, ?HEADER_SIZE + SegmentEntryCount * ?ENTRY_SIZE),
            %% We then write the segment file header. It contains
            %% some useful info and some reserved bytes for future use.
            %% We currently do not make use of this information. It is
            %% only there for forward compatibility purposes (for example
            %% to support an index with mixed segment_entry_count() values).
            FromSeqId = (WriteMarker div SegmentEntryCount) * SegmentEntryCount,
            ToSeqId = FromSeqId + SegmentEntryCount,
            ok = file:write(Fd, << ?MAGIC:32,
                                   ?VERSION:8,
                                   FromSeqId:64/unsigned,
                                   ToSeqId:64/unsigned,
                                   0:344 >>);
        false ->
            ok
    end,
    %% Keep the file open.
    State#mqistate{ fds = OpenFds#{Segment => Fd} }.

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
%% @todo We may also want to log holes inside files (not holes as in files missing, as
%%       those can happen in normal conditions, albeit rarely).

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
    {Count, Bytes, State2} = case IsIndexClean andalso IsMsgStoreClean of
        true ->
            {WriteMarker0, AckMarker0, Acks0} = proplists:get_value(mqi_state, Terms, {0, undefined, []}),
            %% The queue has stored the count/bytes values inside
            %% Terms so we don't need to provide them again.
            %% However I can see this being a problem if
            %% there's been corruption on the disk. But
            %% a better fix would be to rework the way
            %% rabbit_variable_queue works.
            {undefined,
             undefined,
             State0#mqistate{ write_marker = WriteMarker0,
                              ack_marker = AckMarker0,
                              acks = Acks0 }};
        false ->
            CountersRef = counters:new(2, []),
            State1 = recover_segments(State0, ContainsCheckFun, CountersRef),
            {counters:get(CountersRef, ?RECOVER_COUNT),
             counters:get(CountersRef, ?RECOVER_BYTES),
             State1}
    end,
    %% We double check the ack marker because when the message store has
    %% recovered we might have marked additional messages as acked as
    %% part of the recovery process.
    State = recover_maybe_advance_ack_marker(State2),
    %% We can now ensure the current segment file is in a good state and return.
    {Count, Bytes, ensure_segment_file(recover, State)}.

recover_segments(State0 = #mqistate { dir = Dir }, ContainsCheckFun, CountersRef) ->
    %% Figure out which files exist. Sort them in segment order.
    SegmentFiles = rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir),
    Segments = lists:sort([
        list_to_integer(filename:basename(F, ?SEGMENT_EXTENSION))
    || F <- SegmentFiles]),
    %% @todo We want to figure out if some files are missing (there are holes)
    %%       and if so, we want to create new fully acked files and add those
    %%       messages to the acks list.
    %%
    %% @todo We may want to check that the file sizes are correct before attempting
    %%       to parse them, and to correct the file sizes. Correcting file sizes
    %%       may create holes within segment files though if we only file:allocate.
    %%       Not sure what should be done about that one.
    case Segments of
        %% No segments found. Keep default values.
        [] ->
            State0;
        %% Look for markers and acks in the files we've found.
        [FirstSegment|Tail] ->
            {Fd, State} = get_fd_for_segment(FirstSegment, State0),
            SegmentEntryCount = segment_entry_count(),
            FirstSeqId = FirstSegment * SegmentEntryCount,
            recover_ack_marker(State, ContainsCheckFun, CountersRef,
                               Fd, FirstSegment, SegmentEntryCount,
                               FirstSeqId, Tail)
    end.

recover_ack_marker(State, ContainsCheckFun, CountersRef,
                   Fd, FdSegment, SegmentEntryCount, SeqId, Segments) ->
    case locate(SeqId, SegmentEntryCount) of
        %% The SeqId is in the current segment file.
        {FdSegment, Offset} ->
            case file:pread(Fd, Offset, 1) of
                %% We found an ack, continue.
                {ok, <<2>>} ->
                    recover_ack_marker(State, ContainsCheckFun, CountersRef,
                                       Fd, FdSegment, SegmentEntryCount,
                                       SeqId + 1, Segments);
                %% We found a non-ack entry! The previous SeqId is our ack_marker.
                %% We now need to figure out where the write_marker ends.
                {ok, <<1>>} ->
                    AckMarker = case SeqId of
                        0 -> undefined;
                        _ -> SeqId - 1
                    end,
                    recover_write_marker(State#mqistate{ ack_marker = AckMarker },
                                         ContainsCheckFun, CountersRef,
                                         Fd, FdSegment, SegmentEntryCount,
                                         SeqId, Segments, []);
                %% We found a non-entry. Everything was acked.
                %%
                %% @todo We may want to find out whether this is a hole in the file
                %%       or the real end of the segments. If this is a hole, we could
                %%       repair it automatically.
                {ok, <<0>>} ->
                    AckMarker = case SeqId of
                        0 -> undefined;
                        _ -> SeqId - 1
                    end,
                    State#mqistate{ write_marker = SeqId,
                                    ack_marker = AckMarker }
            end;
        %% The SeqId is in another segment file. The current file has been
        %% fully acked. Delete this file and open the next segment file if any.
        %% If there are no more files this means that everything was acked and
        %% that we can start again with default values.
        {_, _} ->
            EofState = delete_segment(FdSegment, State),
            case Segments of
                [] ->
                    EofState;
                [NextSegment|Tail] ->
                    {NextFd, NextState} = get_fd_for_segment(NextSegment, EofState),
                    NextSeqId = NextSegment * SegmentEntryCount, %% Might not be =:= SeqId if files are not contiguous.
                    recover_ack_marker(NextState, ContainsCheckFun, CountersRef,
                                       NextFd, NextSegment, SegmentEntryCount,
                                       NextSeqId, Tail)
            end
    end.

%% This function does two things: it recovers the write_marker,
%% and it ensures that non-acked entries found have a corresponding
%% message in the message store.

recover_write_marker(State, ContainsCheckFun, CountersRef,
                     Fd, FdSegment, SegmentEntryCount, SeqId, Segments, Acks) ->
    case locate(SeqId, SegmentEntryCount) of
        %% The SeqId is in the current segment file.
        {FdSegment, Offset} ->
            case file:pread(Fd, Offset, 24) of
                %% We found an ack, add it to the acks list.
                {ok, <<2,_/bits>>} ->
                    recover_write_marker(State, ContainsCheckFun, CountersRef,
                        Fd, FdSegment, SegmentEntryCount,
                        SeqId + 1, Segments, [SeqId|Acks]);
                %% We found a non-ack persistent entry. Check that the corresponding
                %% message exists in the message store. If not, mark this message as
                %% acked.
                {ok, <<1,IsDelivered:8,1,_:8,Id:16/binary,Size:32/unsigned>>} ->
                    NextAcks = case ContainsCheckFun(Id) of
                        %% Message is in the store. We mark all those
                        %% messages as delivered if they were not already
                        %% (like the old index).
                        true ->
                            case IsDelivered of
                                1 -> ok;
                                0 -> file:pwrite(Fd, Offset + 1, <<1>>)
                            end,
                            counters:add(CountersRef, ?RECOVER_COUNT, 1),
                            counters:add(CountersRef, ?RECOVER_BYTES, Size),
                            Acks;
                        %% Message is not in the store. Mark it as acked.
                        false ->
                            file:pwrite(Fd, Offset, <<2>>),
                            [SeqId|Acks]
                    end,
                    recover_write_marker(State, ContainsCheckFun, CountersRef,
                        Fd, FdSegment, SegmentEntryCount,
                        SeqId + 1, Segments, NextAcks);
                %% We found a non-ack transient entry. We mark the message as acked
                %% without checking with the message store, because it already dropped
                %% this message.
                {ok, <<1,_,0,_/bits>>} ->
                    file:pwrite(Fd, Offset, <<2>>),
                    recover_write_marker(State, ContainsCheckFun, CountersRef,
                        Fd, FdSegment, SegmentEntryCount,
                        SeqId + 1, Segments, [SeqId|Acks]);
                %% We found a non-entry. The SeqId is our write_marker.
                %%
                %% @todo We also want to find out whether this is a hole in the file
                %%       or the real end of the segments. If this is a hole, we could
                %%       repair it automatically.
                {ok, <<0,_/bits>>} when Segments =:= [] ->
                    State#mqistate{ write_marker = SeqId,
                                    acks = Acks }
            end;
        %% The SeqId is in another segment file.
        {_, _} ->
            case Segments of
                %% This was the last segment file. The SeqId is our
                %% write_marker.
                [] ->
                    State#mqistate{ write_marker = SeqId,
                                    acks = Acks };
                %% Continue reading.
                [NextSegment|Tail] ->
                    {NextFd, NextState} = get_fd_for_segment(NextSegment, State),
                    NextSeqId = NextSegment * SegmentEntryCount, %% Might not be =:= SeqId if files are not contiguous.
                    recover_write_marker(NextState, ContainsCheckFun, CountersRef,
                        NextFd, NextSegment, SegmentEntryCount,
                        NextSeqId, Tail, Acks)
            end
    end.

recover_maybe_advance_ack_marker(State = #mqistate{ ack_marker = AckMarker,
                                                    acks = Acks }) ->
    NextAckMarker = case AckMarker of
        undefined -> 0;
        _ -> AckMarker + 1
    end,
    case lists:member(NextAckMarker, Acks) of
        %% The ack_marker must be advanced because we found
        %% the next seq_id() in the acks list.
        true ->
            update_ack_marker(Acks, State);
        %% The ack_marker is correct.
        false ->
            State
    end.

-spec terminate(rabbit_types:vhost(), [any()], State) -> State when State::mqistate().

terminate(VHost, Terms, State0 = #mqistate { dir = Dir,
                                             write_marker = WriteMarker,
                                             ack_marker = AckMarker,
                                             acks = Acks,
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
    Term = {WriteMarker, AckMarker, Acks},
    rabbit_recovery_terms:store(VHost, filename:basename(Dir),
                                [{mqi_state, Term} | Terms]),
    State#mqistate{ fds = #{} }.

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
    State#mqistate{ fds = #{} }.

%% All messages must be published sequentially to the modern index.

-spec publish(rabbit_types:msg_id(), seq_id(),
                    rabbit_types:message_properties(), boolean(), boolean(),
                    non_neg_integer(), State) -> State when State::mqistate().

publish(MsgOrId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount,
        State = #mqistate { write_marker = WriteMarker })
        when SeqId < WriteMarker ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [MsgOrId, SeqId, Props, IsPersistent, TargetRamCount, State]),
    %% We have already written this message on disk. We do not need
    %% to write it again. We may deliver if necessary.
    %% @todo Maybe do this in pre_publish instead...
    case IsDelivered of
        true -> deliver([SeqId], State);
        false -> State
    end;
%% Because we always persist to the msg_store, the Msg(Or)Id argument
%% here is always a binary, never a record.
publish(MsgId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount,
        State0 = #mqistate { write_marker = WriteMarker,
                             write_buffer = WriteBuffer0 })
        when is_binary(MsgId)  ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [MsgId, SeqId, Props, IsPersistent, TargetRamCount, State0]),
    %% Add the entry to the write buffer.
    WriteBuffer = WriteBuffer0#{SeqId => {MsgId, SeqId, Props, IsPersistent, IsDelivered}},
    NextWriteMarker = WriteMarker + 1,
    State1 = State0#mqistate{ write_marker = NextWriteMarker,
                              write_buffer = WriteBuffer },
    %% When the write_marker ends up on a new segment we must
    %% prepare the new segment file even if we don't flush
    %% the buffer and write to that file yet.
    SegmentEntryCount = segment_entry_count(),
    ThisSegment = WriteMarker div SegmentEntryCount,
    NextSegment = NextWriteMarker div SegmentEntryCount,
    State2 = case ThisSegment =:= NextSegment of
        true -> State1;
        false -> ensure_segment_file(new, State1)
    end,
    %% When publisher confirms have been requested for this
    %% message we mark the message as unconfirmed.
    State = maybe_mark_unconfirmed(MsgId, SeqId, Props, State2),
    maybe_flush_buffer(State).

maybe_mark_unconfirmed(MsgId, SeqId, #message_properties{ needs_confirming = true },
        State = #mqistate { confirms = Confirms }) ->
    State#mqistate{ confirms = Confirms#{SeqId => MsgId} };
maybe_mark_unconfirmed(_, _, _, State) ->
    State.

%% @todo Perhaps make the two limits configurable.
maybe_flush_buffer(State = #mqistate { write_buffer = WriteBuffer })
        when map_size(WriteBuffer) < 2000 ->
    State;
maybe_flush_buffer(State = #mqistate { write_buffer_updates = NumUpdates }) ->
    FlushType = case NumUpdates >= 100 of
        true -> updates;
        false -> full
    end,
    flush_buffer(State, FlushType).

%% When there are less than 100 entries, we only write updates
%% (deliver | ack) and get the buffer back to a comfortable level.

flush_buffer(State0 = #mqistate { write_buffer = WriteBuffer0 },
             FlushType) ->
    SegmentEntryCount = segment_entry_count(),
    %% First we prepare the writes sorted by segment.
    {Writes, WriteBuffer} = maps:fold(fun
        (SeqId, ack, {WritesAcc, BufferAcc}) ->
            {acc_write(SeqId, SegmentEntryCount, <<2>>, +0, WritesAcc),
             BufferAcc};
        (SeqId, deliver, {WritesAcc, BufferAcc}) ->
            {acc_write(SeqId, SegmentEntryCount, <<1>>, +1, WritesAcc),
             BufferAcc};
        (SeqId, Entry, {WritesAcc, BufferAcc}) when FlushType =:= updates ->
            {WritesAcc,
             BufferAcc#{SeqId => Entry}};
        %% Otherwise we write the entire buffer.
        (SeqId, Entry, {WritesAcc, BufferAcc}) ->
            {acc_write(SeqId, SegmentEntryCount, build_entry(Entry), +0, WritesAcc),
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

acc_write(SeqId, SegmentEntryCount, Bytes, EntryOffset, WritesAcc) ->
    {Segment, Offset} = locate(SeqId, SegmentEntryCount),
    LocBytesAcc = maps:get(Segment, WritesAcc, []),
    WritesAcc#{Segment => [{Offset + EntryOffset, Bytes}|LocBytesAcc]}.

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
    << 1:8,                   %% Status. 0 = no entry, 1 = entry exists, 2 = entry acked
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

%% When marking acks we need to update the file(s) on disk
%% as well as update ack_marker and/or acks in the state.
%% When a file has been fully acked we may also close its
%% open FD if any and delete it.

-spec ack([seq_id()], State) -> State when State::mqistate().

%% The rabbit_variable_queue module may call this function
%% with an empty list. Do nothing.
ack([], State) ->
    ?DEBUG("[] ~0p", [State]),
    State;
ack(SeqIds0, State0 = #mqistate{ write_buffer = WriteBuffer0,
                                 write_buffer_updates = NumUpdates0,
                                 ack_marker = AckMarkerBefore }) ->
    ?DEBUG("~0p ~0p", [SeqIds0, State0]),
    %% We continue by updating the ack state information. We then
    %% use this information to determine if we can delete
    %% segment files on disk.
    State1 = update_ack_state(SeqIds0, State0),
    {HighestSegmentDeleted, State} = maybe_delete_segments(AckMarkerBefore, State1),
    %% When there were deleted files, we remove the seq_id()s that
    %% belong to deleted files from the acked list, as well as any
    %% write instructions in the buffer targeting those files.
    {SeqIds, WriteBuffer2, NumUpdates2} = case HighestSegmentDeleted of
        undefined ->
            {SeqIds0, WriteBuffer0, NumUpdates0};
        _ ->
            %% All seq_id()s below this belong to a segment that has been
            %% fully acked and is no longer tracked by the index.
            LowestSeqId = (1 + HighestSegmentDeleted) * segment_entry_count(),
            SeqIds1 = lists:filter(fun(FilterSeqId) ->
                FilterSeqId >= LowestSeqId
            end, SeqIds0),
            {WriteBuffer1, NumUpdates1} = maps:fold(fun
                (FoldSeqId, Write, {FoldBuffer, FoldUpdates}) when FoldSeqId < LowestSeqId ->
                    Num = case Write of
                        deliver -> 1;
                        ack -> 1;
                        _ -> 0
                    end,
                    {FoldBuffer, FoldUpdates - Num};
                (SeqId, Write, {FoldBuffer, FoldUpdates}) ->
                    {FoldBuffer#{SeqId => Write}, FoldUpdates}
            end, {#{}, NumUpdates0}, WriteBuffer0),
            {SeqIds1, WriteBuffer1, NumUpdates1}
    end,
    %% Finally we add any remaining acks to the write buffer.
    {WriteBuffer, NumUpdates} = lists:foldl(fun
        (SeqId, {FoldBuffer, FoldUpdates}) ->
            case FoldBuffer of
                %% Ack if the entry was already marked for delivery or acked.
                %% @todo Is this possible to have ack here? Shouldn't this be an error to ack twice?
                #{SeqId := Write} when Write =:= deliver; Write =:= ack ->
                    {FoldBuffer#{SeqId => ack},
                     FoldUpdates};
                %% Otherwise unconditionally ack. We replace the entry if any.
                _ ->
                    {FoldBuffer#{SeqId => ack},
                     FoldUpdates + 1}
            end
    end, {WriteBuffer2, NumUpdates2}, SeqIds),
    maybe_flush_buffer(State#mqistate{ write_buffer = WriteBuffer,
                                       write_buffer_updates = NumUpdates }).

update_ack_state(SeqIds, State = #mqistate{ ack_marker = undefined, acks = Acks0 }) ->
    Acks = lists:sort(SeqIds ++ Acks0),
    %% We must special case when there is no ack_marker in the state.
    %% The message with seq_id() 0 has not been acked before now.
    case Acks of
        [0|_] ->
            update_ack_marker(Acks, State);
        _ ->
            State#mqistate{ acks = Acks }
    end;
update_ack_state(SeqIds, State = #mqistate{ ack_marker = AckMarker, acks = Acks0 }) ->
    Acks = lists:sort(SeqIds ++ Acks0),
    case Acks of
        %% When SeqId is the message after the marker, we update the marker
        %% and potentially remove additional SeqIds from the acks list. The
        %% new ack marker becomes the largest continuous seq_id() found in
        %% Acks.
        [SeqId|_] when SeqId =:= AckMarker + 1 ->
            update_ack_marker(Acks, State);
        _ ->
            State#mqistate{ acks = Acks }
    end.

update_ack_marker(Acks, State) ->
    AckMarker = highest_continuous_seq_id(Acks),
    RemainingAcks = lists:dropwhile(
        fun(AckSeqId) -> AckSeqId =< AckMarker end,
        Acks),
    State#mqistate{ ack_marker = AckMarker,
                    acks = RemainingAcks }.

maybe_delete_segments(_, State = #mqistate{ ack_marker = undefined }) ->
    %% When the ack_marker is undefined, do nothing.
    {undefined, State};
maybe_delete_segments(AckMarkerBefore, State = #mqistate{ ack_marker = AckMarkerAfter }) ->
    SegmentEntryCount = segment_entry_count(),
    %% We add one because the ack_marker points to the highest
    %% continuous acked message, and we want to know which
    %% segment is after that, so that we immediately remove
    %% a fully-acked file when ack_marker points to the last
    %% entry in the file.
    SegmentBefore = case AckMarkerBefore of
        undefined -> 0;
        _ -> (1 + AckMarkerBefore) div SegmentEntryCount
    end,
    SegmentAfter = (1 + AckMarkerAfter) div SegmentEntryCount,
    if
        %% When the marker still points to the same segment,
        %% do nothing.
        SegmentBefore =:= SegmentAfter ->
            {undefined, State};
        %% When the marker points to a different segment,
        %% delete fully acked segment files.
        true ->
            delete_segments(SegmentBefore, SegmentAfter - 1, State)
    end.

delete_segments(Segment, Segment, State) ->
    {Segment, delete_segment(Segment, State)};
delete_segments(Segment, LastSegment, State) ->
    delete_segments(Segment + 1, LastSegment,
        delete_segment(Segment, State)).

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
%% Nothing to read because there is nothing at or before the ack marker.
read(FromSeqId, ToSeqId, State = #mqistate{ ack_marker = AckMarker })
        when ToSeqId =< AckMarker, AckMarker =/= undefined ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State]),
    {[], State};
%% Nothing to read because there is nothing at or past the write marker.
read(FromSeqId, ToSeqId, State = #mqistate{ write_marker = WriteMarker })
        when FromSeqId >= WriteMarker ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State]),
    {[], State};
%% We can only read from the message after the ack marker.
read(FromSeqId, ToSeqId, State = #mqistate{ ack_marker = AckMarker })
        when FromSeqId =< AckMarker, AckMarker =/= undefined ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State]),
    read(AckMarker + 1, ToSeqId, State);
%% We can only read up to the message before the write marker.
read(FromSeqId, ToSeqId, State = #mqistate{ write_marker = WriteMarker })
        when ToSeqId > WriteMarker ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State]),
    read(FromSeqId, WriteMarker, State);
%% Read the messages requested.
read(FromSeqId, ToSeqId, State = #mqistate{ acks = Acks }) ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State]),
    SeqIdsToRead = lists:seq(FromSeqId, ToSeqId - 1) -- Acks,
    read(SeqIdsToRead, State).

read(SeqIdsToRead, State0 = #mqistate{ write_buffer = WriteBuffer }) ->
    %% We first try to read from the write buffer what we can,
    %% then we read the rest from disk.
    {Reads0, SeqIdsOnDisk} = read_from_buffer(SeqIdsToRead,
                                              WriteBuffer,
                                              [], []),
    {Reads1, State} = read_from_disk(SeqIdsOnDisk,
                                      State0,
                                      Reads0),
    %% The messages may not be in the correct order. Fix that.
    Reads = lists:keysort(2, Reads1),
    {Reads, State}.

read_from_buffer([], _, SeqIdsOnDisk, Reads) ->
    %% We must do a lists:reverse here so that we are able
    %% to read multiple continuous messages from disk in one call.
    {Reads, lists:reverse(SeqIdsOnDisk)};
read_from_buffer([SeqId|Tail], WriteBuffer, SeqIdsOnDisk, Reads) ->
    case WriteBuffer of
        #{SeqId := ack} ->
            read_from_buffer(Tail, WriteBuffer, SeqIdsOnDisk, Reads);
        #{SeqId := Entry} when is_tuple(Entry) ->
            read_from_buffer(Tail, WriteBuffer, SeqIdsOnDisk, [Entry|Reads]);
        _ ->
            read_from_buffer(Tail, WriteBuffer, [SeqId|SeqIdsOnDisk], Reads)
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
    ReadSize = (LastSeqId - FirstSeqId + 1) * ?ENTRY_SIZE,
    {Fd, OffsetForSeqId, State} = get_fd(FirstSeqId, State0),
    {ok, EntriesBin} = file:pread(Fd, OffsetForSeqId, ReadSize),
    %% We cons new entries into the Acc and only reverse it when we
    %% are completely done reading new entries.
    Acc = parse_entries(EntriesBin, FirstSeqId, WriteBuffer, Acc0),
    read_from_disk(SeqIdsToRead, State, Acc).

get_fd(SeqId, State = #mqistate{ fds = OpenFds }) ->
    SegmentEntryCount = segment_entry_count(),
    {Segment, Offset} = locate(SeqId, SegmentEntryCount),
    case OpenFds of
        #{ Segment := Fd } ->
            {Fd, Offset, State};
        _ ->
            {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            {Fd, Offset, State#mqistate{ fds = OpenFds#{ Segment => Fd }}}
    end.

locate(SeqId, SegmentEntryCount) ->
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    {Segment, Offset}.

%% When recovering from a dirty shutdown, we may end up reading entries that
%% have already been acked. We do not add them to the Acc in that case, and
%% as a result we may end up returning less messages than initially expected.

parse_entries(<<>>, _, _, Acc) ->
    Acc;
parse_entries(<< Status:8,
                 IsDelivered0:8,
                 _:7, IsPersistent:1,
                 _:8,
                 Id0:128,
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
            Id = <<Id0:128>>,
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
        2 ->
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
    {ok, Fd} = file:open(F, [read, raw, binary]),
    case file:read(Fd, 21) of
        {ok, <<?MAGIC:32, ?VERSION:8,
               FromSeqId:64/unsigned,
               ToSeqId:64/unsigned>>} ->
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
    Offset = ?HEADER_SIZE + N * ?ENTRY_SIZE,
    case file:pread(Fd, Offset, 20) of
        %% We found an ack, skip the entry.
        {ok, <<2,_/bits>>} ->
            queue_index_walker_segment(Fd, Gatherer, N + 1, Total);
        %% We found a non-ack persistent entry. Gather it.
        {ok, <<1,_,1,_,Id:16/binary>>} ->
            gatherer:sync_in(Gatherer, {Id, 1}),
            queue_index_walker_segment(Fd, Gatherer, N + 1, Total);
        %% We found a non-ack transient entry. Skip it. It will
        %% be marked as acked later during the recover phase.
        %%
        %% @todo It might save us some cycles if we just did it here.
        {ok, <<1,_/bits>>} ->
            queue_index_walker_segment(Fd, Gatherer, N + 1, Total);
        %% We found a non-entry. This is the end of the segment file.
        %%
        %% @todo It might also be a hole, but I'm not sure it is
        %%       even possible to have holes. See comments in recover.
        {ok, <<0,_/bits>>} ->
            ok
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

%% We must special case when we are empty to make tests happy.
bounds(State = #mqistate{ write_marker = 0, ack_marker = undefined }) ->
    {0, 0, State};
bounds(State = #mqistate{ write_marker = WriteMarker,
                          ack_marker = AckMarker })
        when WriteMarker =:= AckMarker + 1 ->
    {0, 0, State};
bounds(State = #mqistate{ write_marker = Newest,
                          ack_marker = AckMarker }) ->
    ?DEBUG("~0p", [State]),
    Oldest = case AckMarker of
        undefined -> 0;
        _ -> AckMarker + 1
    end,
    {next_segment_boundary(Oldest) - segment_entry_count(),
     next_segment_boundary(Newest),
     State}.

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
    %% @todo Figure out what the best default would be.
    %%
    %% @todo A value lower than the max write_buffer size results in nothing needing
    %%       to be written to disk as long as the consumer consumes as fast as the
    %%       producer produces. Accidental memory queue?
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

highest_continuous_seq_id([SeqId1, SeqId2|Tail])
        when (1 + SeqId1) =:= SeqId2 ->
    highest_continuous_seq_id([SeqId2|Tail]);
highest_continuous_seq_id([SeqId|_]) ->
    SeqId.

highest_continuous_seq_id([SeqId|Tail], EndSeqId)
        when (1 + SeqId) =:= EndSeqId ->
    {SeqId, Tail};
highest_continuous_seq_id([SeqId1, SeqId2|Tail], EndSeqId)
        when (1 + SeqId1) =:= SeqId2 ->
    highest_continuous_seq_id([SeqId2|Tail], EndSeqId);
highest_continuous_seq_id([SeqId|Tail], _) ->
    {SeqId, Tail}.
