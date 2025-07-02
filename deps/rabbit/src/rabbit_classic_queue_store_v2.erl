%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% The classic queue store works as follow:
%%
%% When a message needs to be written to disk, it is appended to
%% its corresponding segment file. An offset is returned and that
%% offset will be added to the #msg_status (in lieu of msg_id)
%% and eventually written to disk by the index as well.
%%
%% The corresponding segment file is found by looking at the
%% SeqId, just like the index. There's a 1:1 abstract mapping
%% between index segment files and store segment files. (But
%% whether there are two files for each segment is another
%% question.)
%%
%% Messages are only ever written once to the store. Messages
%% are not reference counted, and are not shared between queues.
%%
%% Messages cannot be removed from the message store individually.
%% Instead, when the index deletes one of its segment files, it
%% tells the queue to also delete the corresponding segment files
%% in the store. This means that store segment files can grow
%% very large because their size is only limited by the number
%% of entries in the index.
%%
%% Messages are synced to disk only when the index tells us
%% they should be. What this effectively means is that the
%% ?STORE:sync function is called right before the ?INDEX:sync
%% function, and only if there are outstanding confirms in the
%% index.
%%
%% The old rabbit_msg_store has separate transient/persistent stores
%% to make recovery of data on disk quicker. We do not need to
%% do this here, because the recovery is only done via the index,
%% which already knows about persistent messages and does not
%% need to look into the store to discard them. Messages on disk
%% will be dropped at the same time as the index deletes the
%% corresponding segment file.

-module(rabbit_classic_queue_store_v2).

-export([init/1, terminate/1, info/1,
         write/4, sync/1, read/3, read_many/2, check_msg_on_disk/3,
         remove/2, delete_segments/2]).

-define(SEGMENT_EXTENSION, ".qs").

-define(MAGIC, 16#52435153). %% "RCQS"
-define(VERSION, 2).
-define(HEADER_SIZE,       64). %% bytes
-define(ENTRY_HEADER_SIZE,  8). %% bytes

-include_lib("rabbit_common/include/rabbit.hrl").

%% Set to true to get an awful lot of debug logs.
-if(false).
-define(DEBUG(X,Y), logger:debug("~0p: " ++ X, [?FUNCTION_NAME|Y])).
-else.
-define(DEBUG(X,Y), _ = X, _ = Y, ok).
-endif.

-type buffer() :: #{
    %% SeqId => {Offset, Size, Msg}
                    rabbit_variable_queue:seq_id() => {non_neg_integer(), non_neg_integer(), mc:state()}
}.

-record(qs, {
    %% Store directory - same as the queue index.
    %% Stored as binary() as opposed to file:filename() to save memory.
    dir :: binary(),

    %% We keep track of which segment is open
    %% and the current offset in the file. This offset
    %% is the position at which the next message will
    %% be written. The offset will be returned to be
    %% kept track of in the queue (or queue index) for
    %% later reads.
    write_segment = undefined :: undefined | non_neg_integer(),
    write_offset = ?HEADER_SIZE :: non_neg_integer(),

    %% We must keep the offset, expected size and message in order
    %% to write the message.
    write_buffer = #{} :: buffer(),
    write_buffer_size = 0 :: non_neg_integer(),

    %% We keep a cache of messages for faster reading
    %% for the cases where consumers can keep up with
    %% producers. The write_buffer becomes the cache
    %% when it is written to disk.
    cache = #{} :: buffer(),

    %% Similarly, we keep track of a single read fd.
    %% We cannot share this fd with the write fd because
    %% we are using file:pread/3 which leaves the file
    %% position undetermined.
    read_segment = undefined :: undefined | non_neg_integer(),
    read_fd = undefined :: undefined | file:fd()
}).

-type state() :: #qs{}.

-type msg_location() :: {?MODULE, non_neg_integer(), non_neg_integer()}.
-export_type([msg_location/0]).

-spec init(rabbit_amqqueue:name()) -> state().

init(#resource{ virtual_host = VHost } = Name) ->
    ?DEBUG("~0p", [Name]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = rabbit_classic_queue_index_v2:queue_dir(VHostDir, Name),
    DirBin = rabbit_file:filename_to_binary(Dir),
    #qs{dir = << DirBin/binary, "/" >>}.

-spec terminate(State) -> State when State::state().

terminate(State0 = #qs{ read_fd = ReadFd }) ->
    ?DEBUG("~0p", [State0]),
    State = flush_buffer(State0, fun(Fd) -> ok = file:sync(Fd) end),
    maybe_close_fd(ReadFd),
    State#qs{ write_segment = undefined,
              write_offset = ?HEADER_SIZE,
              cache = #{},
              read_segment = undefined,
              read_fd = undefined }.

maybe_close_fd(undefined) ->
    ok;
maybe_close_fd(Fd) ->
    ok = file:close(Fd).

-spec info(state()) -> [{atom(), non_neg_integer()}].

info(#qs{ write_buffer = WriteBuffer }) ->
    [{qs_buffer_size, map_size(WriteBuffer)}].

-spec write(rabbit_variable_queue:seq_id(), mc:state(),
            rabbit_types:message_properties(), State)
        -> {msg_location(), State} when State::state().

%% @todo I think we can disable the old message store at the same
%%       place where we create MsgId. If many queues receive the
%%       message, then we create an MsgId. If not, we don't. But
%%       we can only do this after removing support for v1.
write(SeqId, Msg, Props, State0 = #qs{ write_buffer = WriteBuffer0,
                                       write_buffer_size = WriteBufferSize }) ->
    ?DEBUG("~0p ~0p ~0p ~0p", [SeqId, Msg, Props, State0]),
    SegmentEntryCount = segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    Size = erlang:external_size(Msg),
    {Offset, State1} = get_write_offset(Segment, Size, State0),
    WriteBuffer = WriteBuffer0#{SeqId => {Offset, Size, Msg}},
    State = State1#qs{ write_buffer = WriteBuffer,
                       write_buffer_size = WriteBufferSize + Size },
    {{?MODULE, Offset, Size}, maybe_flush_buffer(State)}.

get_write_offset(Segment, Size, State = #qs{ write_segment = Segment,
                                             write_offset = Offset }) ->
    {Offset, State#qs{ write_offset = Offset + ?ENTRY_HEADER_SIZE + Size }};
get_write_offset(Segment, Size, State = #qs{ write_segment = WriteSegment })
        when Segment > WriteSegment ->
    {?HEADER_SIZE, State#qs{ write_segment = Segment,
                             write_offset = ?HEADER_SIZE + ?ENTRY_HEADER_SIZE + Size }};
%% The first time we write we have to figure out the write_offset by
%% looking at the segment file directly.
get_write_offset(Segment, Size, State = #qs{ write_segment = undefined }) ->
    Offset = case file:open(segment_file(Segment, State), [read, raw, binary]) of
        {ok, Fd} ->
            {ok, Offset0} = file:position(Fd, eof),
            ok = file:close(Fd),
            case Offset0 of
                0 -> ?HEADER_SIZE;
                _ -> Offset0
            end;
        {error, enoent} ->
            ?HEADER_SIZE
    end,
    {Offset, State#qs{ write_segment = Segment,
                       write_offset = Offset + ?ENTRY_HEADER_SIZE + Size }}.

-spec sync(State) -> State when State::state().

sync(State) ->
    ?DEBUG("~0p", [State]),
    flush_buffer(State, fun(_) -> ok end).

maybe_flush_buffer(State = #qs{ write_buffer_size = WriteBufferSize }) ->
    case WriteBufferSize >= max_cache_size() of
        true -> flush_buffer(State, fun(_) -> ok end);
        false -> State
    end.

flush_buffer(State = #qs{ write_buffer_size = 0 }, _) ->
    State;
flush_buffer(State0 = #qs{ write_buffer = WriteBuffer }, FsyncFun) ->
    CheckCRC32 = check_crc32(),
    SegmentEntryCount = segment_entry_count(),
    %% First we prepare the writes sorted by segment.
    WriteList = lists:sort(maps:to_list(WriteBuffer)),
    Writes = flush_buffer_build(WriteList, CheckCRC32, SegmentEntryCount),
    %% Then we do the writes for each segment.
    State = lists:foldl(fun({Segment, LocBytes}, FoldState) ->
        {ok, Fd} = rabbit_file:open_eventually(
            segment_file(Segment, FoldState),
            [read, write, raw, binary]),
        case file:position(Fd, eof) of
            {ok, 0} ->
                %% We write the file header if it does not exist.
                FromSeqId = Segment * SegmentEntryCount,
                ToSeqId = FromSeqId + SegmentEntryCount,
                ok = file:write(Fd,
                    << ?MAGIC:32,
                       ?VERSION:8,
                       FromSeqId:64/unsigned,
                       ToSeqId:64/unsigned,
                       0:344 >>);
             _ ->
                ok
        end,
        ok = file:pwrite(Fd, lists:sort(LocBytes)),
        FsyncFun(Fd),
        ok = file:close(Fd),
        FoldState
    end, State0, Writes),
    %% Finally we move the write_buffer to the cache.
    State#qs{ write_buffer = #{},
              write_buffer_size = 0,
              cache = WriteBuffer }.

flush_buffer_build(WriteBuffer = [{FirstSeqId, {Offset, _, _}}|_],
                   CheckCRC32, SegmentEntryCount) ->
    Segment = FirstSeqId div SegmentEntryCount,
    SegmentThreshold = (1 + Segment) * SegmentEntryCount,
    {Tail, LocBytes} = flush_buffer_build(WriteBuffer,
        CheckCRC32, SegmentThreshold, ?HEADER_SIZE, Offset, [], []),
    [{Segment, LocBytes}|flush_buffer_build(Tail, CheckCRC32, SegmentEntryCount)];
flush_buffer_build([], _, _) ->
    [].

flush_buffer_build(Tail = [{SeqId, _}|_], _, SegmentThreshold, _, WriteOffset, WriteAcc, Acc)
        when SeqId >= SegmentThreshold ->
    case WriteAcc of
        [] -> {Tail, Acc};
        _ -> {Tail, [{WriteOffset, lists:reverse(WriteAcc)}|Acc]}
    end;
flush_buffer_build([{_, Entry = {Offset, Size, _}}|Tail],
        CheckCRC32, SegmentThreshold, Offset, WriteOffset, WriteAcc, Acc) ->
    flush_buffer_build(Tail, CheckCRC32, SegmentThreshold,
        Offset + ?ENTRY_HEADER_SIZE + Size, WriteOffset,
        [build_data(Entry, CheckCRC32)|WriteAcc], Acc);
flush_buffer_build([{_, Entry = {Offset, Size, _}}|Tail],
        CheckCRC32, SegmentThreshold, _, _, [], Acc) ->
    flush_buffer_build(Tail, CheckCRC32, SegmentThreshold,
        Offset + ?ENTRY_HEADER_SIZE + Size, Offset, [build_data(Entry, CheckCRC32)], Acc);
flush_buffer_build([{_, Entry = {Offset, Size, _}}|Tail],
        CheckCRC32, SegmentThreshold, _, WriteOffset, WriteAcc, Acc) ->
    flush_buffer_build(Tail, CheckCRC32, SegmentThreshold,
        Offset + ?ENTRY_HEADER_SIZE + Size, Offset, [build_data(Entry, CheckCRC32)],
        [{WriteOffset, lists:reverse(WriteAcc)}|Acc]);
flush_buffer_build([], _, _, _, _, [], Acc) ->
    {[], Acc};
flush_buffer_build([], _, _, _, WriteOffset, WriteAcc, Acc) ->
    {[], [{WriteOffset, lists:reverse(WriteAcc)}|Acc]}.

build_data({_, Size, Msg}, CheckCRC32) ->
    MsgIovec = term_to_iovec(Msg),
    Padding = (Size - iolist_size(MsgIovec)) * 8,
    %% Calculate the CRC for the data if configured to do so.
    %% We will truncate the CRC to 16 bits to save on space. (Idea taken from postgres.)
    {UseCRC32, CRC32} = case CheckCRC32 of
        true -> {1, erlang:crc32(MsgIovec)};
        false -> {0, 0}
    end,
    [
        <<Size:32/unsigned, 0:7, UseCRC32:1, CRC32:16, 0:8>>,
        MsgIovec, <<0:Padding>>
    ].

-spec read(rabbit_variable_queue:seq_id(), msg_location(), State)
        -> {mc:state(), State} when State::state().

read(SeqId, DiskLocation, State = #qs{ write_buffer = WriteBuffer,
                                       cache = Cache }) ->
    ?DEBUG("~0p ~0p ~0p", [SeqId, DiskLocation, State]),
    case WriteBuffer of
        #{ SeqId := {_, _, Msg} } ->
            {Msg, State};
        _ ->
            case Cache of
                #{ SeqId := {_, _, Msg} } ->
                    {Msg, State};
                _ ->
                    read_from_disk(SeqId, DiskLocation, State)
            end
    end.

read_from_disk(SeqId, {?MODULE, Offset, Size}, State0) ->
    SegmentEntryCount = segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    {ok, Fd, State} = get_read_fd(Segment, State0),
    {ok, MsgBin0} = file:pread(Fd, Offset, ?ENTRY_HEADER_SIZE + Size),
    %% Assert the size to make sure we read the correct data.
    %% Check the CRC if configured to do so.
    <<Size:32/unsigned, _:7, UseCRC32:1, CRC32Expected:16/bits, _:8, MsgBin:Size/binary>> = MsgBin0,
    case UseCRC32 of
        0 ->
            ok;
        1 ->
            %% Always check the CRC32 if it was computed on write.
            CRC32 = erlang:crc32(MsgBin),
            %% We currently crash if the CRC32 is incorrect as we cannot recover automatically.
            try
                CRC32Expected = <<CRC32:16>>,
                ok
            catch C:E:S ->
                rabbit_log:error("Per-queue store CRC32 check failed in ~ts seq id ~b offset ~b size ~b",
                                 [segment_file(Segment, State), SeqId, Offset, Size]),
                erlang:raise(C, E, S)
            end
    end,
    Msg = binary_to_term(MsgBin),
    {Msg, State}.

-spec read_many([{rabbit_variable_queue:seq_id(), msg_location()}], State)
        -> {[mc:state()], State} when State::state().

read_many([], State) ->
    {[], State};
read_many(Reads0, State0 = #qs{ write_buffer = WriteBuffer,
                                cache        = Cache }) ->
    %% First we read what we can from memory.
    %% Because the Reads0 list is ordered we start from the end
    %% and stop when we get to a message that isn't in memory.
    case read_many_from_memory(lists:reverse(Reads0), WriteBuffer, Cache, []) of
        %% We've read everything, return.
        {Msgs, []} ->
            {Msgs, State0};
        %% Everything else will be on disk.
        {Msgs, Reads1} ->
            %% We prepare the pread locations sorted by segment.
            Reads2 = lists:reverse(Reads1),
            SegmentEntryCount = segment_entry_count(),
            [{FirstSeqId, _}|_] = Reads2,
            FirstSegment = FirstSeqId div SegmentEntryCount,
            SegmentThreshold = (1 + FirstSegment) * SegmentEntryCount,
            Segs = consolidate_reads(Reads2, SegmentThreshold,
                FirstSegment, SegmentEntryCount, [], #{}),
            %% We read from disk and convert multiple messages at a time.
            read_many_from_disk(Segs, Msgs, State0)
    end.

%% We only read from Map. If we don't find the entry in Map,
%% we replace Map with NextMap and continue. If we then don't
%% find an entry in NextMap, we are done.
read_many_from_memory(Reads = [{SeqId, _}|Tail], Map, NextMap, Acc) ->
    case Map of
        #{ SeqId := {_, _, Msg} } ->
            read_many_from_memory(Tail, Map, NextMap, [Msg|Acc]);
        _ when NextMap =:= #{} ->
            %% The Acc is in the order we want, no need to reverse.
            {Acc, Reads};
        _ ->
            read_many_from_memory(Reads, NextMap, #{}, Acc)
    end;
read_many_from_memory([], _, _, Acc) ->
    {Acc, []}.

consolidate_reads(Reads = [{SeqId, _}|_], SegmentThreshold,
        Segment, SegmentEntryCount, Acc, Segs)
        when SeqId >= SegmentThreshold ->
    %% We Segment + 1 because we expect reads to be contiguous.
    consolidate_reads(Reads, SegmentThreshold + SegmentEntryCount,
        Segment + 1, SegmentEntryCount, [], Segs#{Segment => lists:reverse(Acc)});
%% NextSize does not include the entry header size.
consolidate_reads([{_, {?MODULE, NextOffset, NextSize}}|Tail], SegmentThreshold,
        Segment, SegmentEntryCount, [{Offset, Size}|Acc], Segs)
        when Offset + Size =:= NextOffset ->
    consolidate_reads(Tail, SegmentThreshold, Segment, SegmentEntryCount,
        [{Offset, Size + NextSize + ?ENTRY_HEADER_SIZE}|Acc], Segs);
consolidate_reads([{_, {?MODULE, Offset, Size}}|Tail], SegmentThreshold,
        Segment, SegmentEntryCount, Acc, Segs) ->
    consolidate_reads(Tail, SegmentThreshold, Segment, SegmentEntryCount,
        [{Offset, Size + ?ENTRY_HEADER_SIZE}|Acc], Segs);
consolidate_reads([], _, _, _, [], Segs) ->
    Segs;
consolidate_reads([], _, Segment, _, Acc, Segs) ->
    %% We lists:reverse/1 because we need to preserve order.
    Segs#{Segment => lists:reverse(Acc)}.

read_many_from_disk(Segs, Msgs, State) ->
    %% We read from segments in reverse order because
    %% we need to control the order of returned messages.
    Keys = lists:reverse(lists:sort(maps:keys(Segs))),
    lists:foldl(fun(Segment, {Acc0, FoldState0}) ->
        {ok, Fd, FoldState} = get_read_fd(Segment, FoldState0),
        {ok, Bin} = file:pread(Fd, maps:get(Segment, Segs)),
        Acc = parse_many_from_disk(Bin, Segment, FoldState, Acc0),
        {Acc, FoldState}
    end, {Msgs, State}, Keys).

parse_many_from_disk([<<Size:32/unsigned, _:7, UseCRC32:1, CRC32Expected:16/bits,
                       _:8, MsgBin:Size/binary, R/bits>>|Tail], Segment, State, Acc) ->
    case UseCRC32 of
        0 ->
            ok;
        1 ->
            %% Always check the CRC32 if it was computed on write.
            CRC32 = erlang:crc32(MsgBin),
            %% We currently crash if the CRC32 is incorrect as we cannot recover automatically.
            try
                CRC32Expected = <<CRC32:16>>,
                ok
            catch C:E:S ->
                rabbit_log:error("Per-queue store CRC32 check failed in ~ts",
                                 [segment_file(Segment, State)]),
                erlang:raise(C, E, S)
            end
    end,
    Msg = binary_to_term(MsgBin),
    [Msg|parse_many_from_disk([R|Tail], Segment, State, Acc)];
parse_many_from_disk([<<>>], _, _, Acc) ->
    Acc;
parse_many_from_disk([<<>>|Tail], Segment, State, Acc) ->
    parse_many_from_disk(Tail, Segment, State, Acc).

get_read_fd(Segment, State = #qs{ read_segment = Segment,
                                  read_fd = Fd }) ->
    {ok, Fd, State};
get_read_fd(Segment, State = #qs{ read_fd = OldFd }) ->
    maybe_close_fd(OldFd),
    case file:open(segment_file(Segment, State), [read, raw, binary]) of
        {ok, Fd} ->
            case file:read(Fd, ?HEADER_SIZE) of
                {ok, <<?MAGIC:32,?VERSION:8,
                       _FromSeqId:64/unsigned,_ToSeqId:64/unsigned,
                       _/bits>>} ->
                    {ok, Fd, State#qs{ read_segment = Segment,
                                       read_fd = Fd }};
                eof ->
                    %% Something is wrong with the file. Close it
                    %% and let the caller decide what to do with it.
                    _ = file:close(Fd),
                    {{error, bad_header}, State#qs{ read_segment = undefined,
                                                    read_fd = undefined }}
            end;
        {error, enoent} ->
            {{error, no_file}, State#qs{ read_segment = undefined,
                                         read_fd = undefined }}
    end.

-spec check_msg_on_disk(rabbit_variable_queue:seq_id(), msg_location(), State)
        -> {ok | {error, any()}, State} when State::state().

check_msg_on_disk(SeqId, {?MODULE, Offset, Size}, State0) ->
    SegmentEntryCount = segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    case get_read_fd(Segment, State0) of
        {ok, Fd, State} ->
            case file:pread(Fd, Offset, ?ENTRY_HEADER_SIZE + Size) of
                {ok, MsgBin0} ->
                    %% Assert the size to make sure we read the correct data.
                    %% Check the CRC if configured to do so.
                    case MsgBin0 of
                        <<Size:32/unsigned, _:7, UseCRC32:1, CRC32Expected:16/bits, _:8, MsgBin:Size/binary>> ->
                            case UseCRC32 of
                                0 ->
                                    {ok, State};
                                1 ->
                                    CRC32 = erlang:crc32(MsgBin),
                                    case <<CRC32:16>> of
                                        CRC32Expected -> {ok, State};
                                        _ -> {{error, bad_crc}, State}
                                    end
                            end;
                        _ ->
                            {{error, bad_size}, State}
                    end;
                eof ->
                    {{error, eof}, State};
                Error ->
                    {Error, State}
            end;
        {Error, State} ->
            {Error, State}
    end.

-spec remove(rabbit_variable_queue:seq_id(), State) -> State when State::state().

%% We only remove the message from the write_buffer. We will remove
%% the message from the disk when we delete the segment file, and
%% from the cache on the next write.
remove(SeqId, State = #qs{ write_buffer = WriteBuffer0,
                           write_buffer_size = WriteBufferSize }) ->
    ?DEBUG("~0p ~0p", [SeqId, State]),
    case maps:take(SeqId, WriteBuffer0) of
        error ->
            State;
        {{_, MsgSize, _}, WriteBuffer} ->
            State#qs{ write_buffer = WriteBuffer,
                      write_buffer_size = WriteBufferSize - MsgSize }
    end.

-spec delete_segments([non_neg_integer()], State) -> State when State::state().

%% First we check if the write fd points to a segment
%% that must be deleted, and we close it if that's the case.
delete_segments([], State) ->
    ?DEBUG("[] ~0p", [State]),
    State;
delete_segments(Segments, State0 = #qs{ write_buffer = WriteBuffer0,
                                        write_buffer_size = WriteBufferSize0,
                                        read_segment = ReadSegment,
                                        read_fd = ReadFd }) ->
    ?DEBUG("~0p ~0p", [Segments, State0]),
    %% First we have to close fds for the segments, if any.
    %% 'undefined' is never in Segments so we don't
    %% need to special case it.
    CloseRead = lists:member(ReadSegment, Segments),
    State = if
        CloseRead ->
            ok = file:close(ReadFd),
            State0#qs{ read_segment = undefined,
                       read_fd = undefined };
        true ->
            State0
    end,
    %% Then we delete the files.
    _ = [
        case prim_file:delete(segment_file(Segment, State)) of
            ok -> ok;
            %% The file might not have been created. This is the case
            %% if all messages were sent to the per-vhost store for example.
            {error, enoent} -> ok
        end
    || Segment <- Segments],
    %% Finally, we remove any entries from the buffer that fall within
    %% the segments that were deleted.
    SegmentEntryCount = segment_entry_count(),
    {WriteBuffer, WriteBufferSize} = maps:fold(fun
        (SeqId, Value = {_, MsgSize, _}, {WriteBufferAcc, WriteBufferSize1}) ->
            case lists:member(SeqId div SegmentEntryCount, Segments) of
                true ->
                    {WriteBufferAcc, WriteBufferSize1 - MsgSize};
                false ->
                    {WriteBufferAcc#{SeqId => Value}, WriteBufferSize1}
            end
    end, {#{}, WriteBufferSize0}, WriteBuffer0),
    State#qs{ write_buffer = WriteBuffer,
              write_buffer_size = WriteBufferSize }.

%% ----
%%
%% Internal.

segment_entry_count() ->
    %% We use the same value as the index.
    persistent_term:get(classic_queue_index_v2_segment_entry_count, 4096).

max_cache_size() ->
    persistent_term:get(classic_queue_store_v2_max_cache_size, 512000).

check_crc32() ->
    persistent_term:get(classic_queue_store_v2_check_crc32, true).

%% Same implementation as rabbit_classic_queue_index_v2:segment_file/2,
%% but with a different state record.
segment_file(Segment, #qs{dir = Dir}) ->
    N = integer_to_binary(Segment),
    <<Dir/binary, N/binary, ?SEGMENT_EXTENSION>>.
