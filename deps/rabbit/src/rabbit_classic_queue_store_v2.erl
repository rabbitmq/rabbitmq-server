%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
%% index. As a result the store does not need to keep track of
%% confirms.
%%
%% The old rabbit_msg_store has separate transient/persistent stores
%% to make recovery of data on disk quicker. We do not need to
%% do this here, because the recovery is only done via the index,
%% which already knows about persistent messages and does not
%% need to look into the store to discard them. Messages on disk
%% will be dropped at the same time as the index deletes the
%% corresponding segment file.
%%
%% The file_handle_cache reservations are done by the v2 index
%% because they are handled at a pid level. Since we are using
%% up to 2 FDs in this module we make the index reserve 2 extra
%% FDs.

-module(rabbit_classic_queue_store_v2).

-export([init/2, terminate/1,
         write/4, sync/1, read/3, check_msg_on_disk/3,
         remove/2, delete_segments/2]).

-define(SEGMENT_EXTENSION, ".qs").

-define(MAGIC, 16#52435153). %% "RCQS"
-define(VERSION, 2).
-define(HEADER_SIZE,       64). %% bytes
-define(ENTRY_HEADER_SIZE,  8). %% bytes

-include_lib("rabbit_common/include/rabbit.hrl").

%% We need this to directly access the delayed file io pid.
-include_lib("kernel/include/file.hrl").

%% Set to true to get an awful lot of debug logs.
-if(false).
-define(DEBUG(X,Y), logger:debug("~0p: " ++ X, [?FUNCTION_NAME|Y])).
-else.
-define(DEBUG(X,Y), _ = X, _ = Y, ok).
-endif.

-record(qs, {
    %% Store directory - same as the queue index.
    dir :: file:filename(),

    %% We keep up to one write fd open at any time.
    %% Because queues are FIFO, writes are mostly sequential
    %% and they occur only once, we do not need to worry
    %% much about writing to multiple different segment
    %% files.
    %%
    %% We keep track of which segment is open, its fd,
    %% and the current offset in the file. This offset
    %% is the position at which the next message will
    %% be written, and it will be kept track in the
    %% queue (and potentially in the queue index) for
    %% a later read.
    write_segment = undefined :: undefined | non_neg_integer(),
    write_fd = undefined :: undefined | file:fd(),
    write_offset = ?HEADER_SIZE :: non_neg_integer(),

    %% We keep a cache of messages for faster reading
    %% for the cases where consumers can keep up with
    %% producers. Messages are written to the case as
    %% long as there is space available. Messages are
    %% not removed from the cache until the message
    %% is either acked or removed by the queue. This
    %% means that only a subset of messages may make
    %% it onto the store. We do not prune messages as
    %% that could lead to caching messages and pruning
    %% them before they can be read from the cache.
    %%
    %% The cache size is the size of the messages,
    %% it does not include the metadata. The real
    %% cache size will therefore potentially be larger
    %% than the configured maximum size.
    cache = #{} :: #{ rabbit_variable_queue:seq_id() => {non_neg_integer(), #basic_message{}}},
    cache_size = 0 :: non_neg_integer(),

    %% Similarly, we keep track of a single read fd.
    %% We cannot share this fd with the write fd because
    %% we are using file:pread/3 which leaves the file
    %% position undetermined.
    read_segment = undefined :: undefined | non_neg_integer(),
    read_fd = undefined :: undefined | file:fd(),

    %% Messages waiting for publisher confirms. The
    %% publisher confirms will be sent at regular
    %% intervals after the index has been flushed
    %% to disk.
    confirms = gb_sets:new() :: gb_sets:set(),
    on_sync :: on_sync_fun()
}).

-type state() :: #qs{}.

-type msg_location() :: {?MODULE, non_neg_integer(), non_neg_integer()}.
-export_type([msg_location/0]).

-type on_sync_fun() :: fun ((gb_sets:set(), 'written' | 'ignored') -> any()).

-spec init(rabbit_amqqueue:name(), on_sync_fun()) -> state().

init(#resource{ virtual_host = VHost } = Name, OnSyncFun) ->
    ?DEBUG("~0p ~0p", [Name, OnSyncFun]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = rabbit_classic_queue_index_v2:queue_dir(VHostDir, Name),
    #qs{
        dir = Dir,
        on_sync = OnSyncFun
    }.

-spec terminate(State) -> State when State::state().

terminate(State = #qs{ write_fd = WriteFd,
                       read_fd = ReadFd }) ->
    ?DEBUG("~0p", [State]),
    %% Fsync and close all FDs as needed.
    maybe_sync_close_fd(WriteFd),
    maybe_close_fd(ReadFd),
    State#qs{ write_segment = undefined,
              write_fd = undefined,
              write_offset = ?HEADER_SIZE,
              read_segment = undefined,
              read_fd = undefined }.

maybe_sync_close_fd(undefined) ->
    ok;
maybe_sync_close_fd(Fd) ->
    ok = file:sync(Fd),
    ok = file:close(Fd).

maybe_close_fd(undefined) ->
    ok;
maybe_close_fd(Fd) ->
    ok = file:close(Fd).

-spec write(rabbit_variable_queue:seq_id(), rabbit_types:basic_message(),
            rabbit_types:message_properties(), State)
        -> {msg_location(), State} when State::state().

write(SeqId, Msg=#basic_message{ id = MsgId }, Props, State0) ->
    ?DEBUG("~0p ~0p ~0p ~0p", [SeqId, Msg, Props, State0]),
    SegmentEntryCount = segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    %% We simply append to the related segment file.
    %% We will then return the offset and size. That
    %% combined with the SeqId is enough to find the
    %% message again.
    {Fd, Offset, State1} = get_write_fd(Segment, State0),
    %% @todo We could remove MsgId because we are not going to use it
    %%       after reading it back from disk. But we have to support
    %%       going back to v1 for the time being. When rolling back
    %%       to v1 is no longer possible, set `id = undefined` here.
    MsgIovec = term_to_iovec(Msg),
    Size = iolist_size(MsgIovec),
    %% Calculate the CRC for the data if configured to do so.
    %% We will truncate the CRC to 16 bits to save on space. (Idea taken from postgres.)
    {UseCRC32, CRC32} = case check_crc32() of
        true -> {1, erlang:crc32(MsgIovec)};
        false -> {0, 0}
    end,
    %% Append to the buffer. A short entry header is prepended:
    %% Size:32, Flags:8, CRC32:16, Unused:8.
    ok = file:write(Fd, [<<Size:32/unsigned, 0:7, UseCRC32:1, CRC32:16, 0:8>>, MsgIovec]),
    %% Maybe cache the message.
    State2 = maybe_cache(SeqId, Size, Msg, State1),
    %% When publisher confirms have been requested for this
    %% message we mark the message as unconfirmed.
    State = maybe_mark_unconfirmed(MsgId, Props, State2),
    %% Keep track of the offset we are at.
    {{?MODULE, Offset, Size}, State#qs{ write_offset = Offset + Size + ?ENTRY_HEADER_SIZE }}.

get_write_fd(Segment, State = #qs{ write_fd = Fd,
                                   write_segment = Segment,
                                   write_offset = Offset }) ->
    {Fd, Offset, State};
get_write_fd(Segment, State = #qs{ write_fd = OldFd }) ->
    maybe_close_fd(OldFd),
    {ok, Fd} = file:open(segment_file(Segment, State), [read, append, raw, binary, {delayed_write, 512000, 10000}]),
    {ok, Offset0} = file:position(Fd, eof),
    %% If we are at offset 0, write the file header.
    Offset = case Offset0 of
        0 ->
            SegmentEntryCount = segment_entry_count(),
            FromSeqId = Segment * SegmentEntryCount,
            ToSeqId = FromSeqId + SegmentEntryCount,
            ok = file:write(Fd, << ?MAGIC:32,
                                   ?VERSION:8,
                                   FromSeqId:64/unsigned,
                                   ToSeqId:64/unsigned,
                                   0:344 >>),
            ?HEADER_SIZE;
        _ ->
            Offset0
    end,
    {Fd, Offset, State#qs{ write_segment = Segment,
                           write_fd = Fd }}.

maybe_cache(SeqId, MsgSize, Msg, State = #qs{ cache = Cache,
                                              cache_size = CacheSize }) ->
    MaxCacheSize = max_cache_size(),
    if
        CacheSize + MsgSize > MaxCacheSize ->
            State;
        true ->
            State#qs{ cache = Cache#{ SeqId => {MsgSize, Msg} },
                      cache_size = CacheSize + MsgSize }
    end.

maybe_mark_unconfirmed(MsgId, #message_properties{ needs_confirming = true },
        State = #qs { confirms = Confirms }) ->
    State#qs{ confirms = gb_sets:add_element(MsgId, Confirms) };
maybe_mark_unconfirmed(_, _, State) ->
    State.

-spec sync(State) -> State when State::state().

sync(State = #qs{ confirms = Confirms,
                  on_sync = OnSyncFun }) ->
    ?DEBUG("~0p", [State]),
    flush_write_fd(State),
    case gb_sets:is_empty(Confirms) of
        true ->
            State;
        false ->
            OnSyncFun(Confirms, written),
            State#qs{ confirms = gb_sets:new() }
    end.

-spec read(rabbit_variable_queue:seq_id(), msg_location(), State)
        -> {rabbit_types:basic_message(), State} when State::state().

read(SeqId, DiskLocation, State = #qs{ cache = Cache }) ->
    ?DEBUG("~0p ~0p ~0p", [SeqId, DiskLocation, State]),
    case Cache of
        #{ SeqId := {_, Msg} } ->
            {Msg, State};
        _ ->
            read_from_disk(SeqId, DiskLocation, State)
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
            %% We only want to check the CRC32 if configured to do so.
            case check_crc32() of
                false ->
                    ok;
                true ->
                    CRC32 = erlang:crc32(MsgBin),
                    %% We currently crash if the CRC32 is incorrect as we cannot recover automatically.
                    try
                        CRC32Expected = <<CRC32:16>>,
                        ok
                    catch C:E:S ->
                        rabbit_log:error("Per-queue store CRC32 check failed in ~s seq id ~b offset ~b size ~b",
                                         [segment_file(Segment, State), SeqId, Offset, Size]),
                        erlang:raise(C, E, S)
                    end
            end
    end,
    Msg = binary_to_term(MsgBin),
    {Msg, State}.

get_read_fd(Segment, State = #qs{ read_segment = Segment,
                                  read_fd = Fd }) ->
    maybe_flush_write_fd(Segment, State),
    {ok, Fd, State};
get_read_fd(Segment, State = #qs{ read_fd = OldFd }) ->
    maybe_close_fd(OldFd),
    maybe_flush_write_fd(Segment, State),
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
                    file:close(Fd),
                    {{error, bad_header}, State#qs{ read_segment = undefined,
                                                    read_fd = undefined }}
            end;
        {error, enoent} ->
            {{error, no_file}, State}
    end.

maybe_flush_write_fd(Segment, State = #qs{ write_segment = Segment }) ->
    flush_write_fd(State);
maybe_flush_write_fd(_, _) ->
    ok.

flush_write_fd(#qs{ write_fd = undefined }) ->
    ok;
flush_write_fd(#qs{ write_fd = Fd }) ->
    %% We tell the pid handling delayed writes to flush to disk
    %% without issuing a separate command to the fd. We need
    %% to do this in order to read from a separate fd that
    %% points to the same file.
    #file_descriptor{
        module = raw_file_io_delayed, %% assert
        data = #{ pid := Pid }
    } = Fd,
    gen_statem:call(Pid, '$synchronous_flush').

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
                                    %% We only want to check the CRC32 if configured to do so.
                                    case check_crc32() of
                                        false ->
                                            {ok, State};
                                        true ->
                                            CRC32 = erlang:crc32(MsgBin),
                                            case <<CRC32:16>> of
                                                CRC32Expected -> {ok, State};
                                                _ -> {{error, bad_crc}, State}
                                            end
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

%% We only remove the message from the cache. We will remove
%% the message from the disk when we delete the segment file.
remove(SeqId, State = #qs{ cache = Cache0,
                           cache_size = CacheSize }) ->
    ?DEBUG("~0p ~0p", [SeqId, State]),
    case maps:take(SeqId, Cache0) of
        error ->
            State;
        {{MsgSize, _}, Cache} ->
            State#qs{ cache = Cache,
                      cache_size = CacheSize - MsgSize }
    end.

-spec delete_segments([non_neg_integer()], State) -> State when State::state().

%% First we check if the write fd points to a segment
%% that must be deleted, and we close it if that's the case.
delete_segments([], State) ->
    ?DEBUG("[] ~0p", [State]),
    State;
delete_segments(Segments, State0 = #qs{ write_segment = WriteSegment,
                                        write_fd = WriteFd,
                                        read_segment = ReadSegment,
                                        read_fd = ReadFd,
                                        cache = Cache0,
                                        cache_size = CacheSize0 }) ->
    ?DEBUG("~0p ~0p", [Segments, State0]),
    %% First we have to close fds for the segments, if any.
    %% 'undefined' is never in Segments so we don't
    %% need to special case it.
    CloseWrite = lists:member(WriteSegment, Segments),
    CloseRead = lists:member(ReadSegment, Segments),
    State = if
        CloseWrite andalso CloseRead ->
            ok = file:close(WriteFd),
            ok = file:close(ReadFd),
            State0#qs{ write_segment = undefined,
                       write_fd = undefined,
                       write_offset = ?HEADER_SIZE,
                       read_segment = undefined,
                       read_fd = undefined };
        CloseWrite ->
            ok = file:close(WriteFd),
            State0#qs{ write_segment = undefined,
                       write_fd = undefined,
                       write_offset = ?HEADER_SIZE };
        CloseRead ->
            ok = file:close(ReadFd),
            State0#qs{ read_segment = undefined,
                       read_fd = undefined };
        true ->
            State0
    end,
    %% Then we delete the files.
    _ = [
        case file:delete(segment_file(Segment, State)) of
            ok -> ok;
            %% The file might not have been created. This is the case
            %% if all messages were sent to the per-vhost store for example.
            {error, enoent} -> ok
        end
    || Segment <- Segments],
    %% Finally, we remove any entries from the cache that fall within
    %% the segments that were deleted. For simplicity's sake, we take
    %% the highest SeqId from these files and remove any SeqId lower
    %% than or equal to this SeqId from the cache.
    HighestSegment = lists:foldl(fun
        (S, SAcc) when S > SAcc -> S;
        (_, SAcc) -> SAcc
    end, -1, Segments),
    HighestSeqId = HighestSegment * segment_entry_count(),
    {Cache, CacheSize} = maps:fold(fun
        (SeqId, {MsgSize, _}, {CacheAcc, CacheSize1}) when SeqId =< HighestSeqId ->
            {CacheAcc, CacheSize1 - MsgSize};
        (SeqId, Value, {CacheAcc, CacheSize1}) ->
            {CacheAcc#{SeqId => Value}, CacheSize1}
    end, {#{}, CacheSize0}, Cache0),
    State#qs{ cache = Cache,
              cache_size = CacheSize }.

%% ----
%%
%% Internal.

segment_entry_count() ->
    %% We use the same value as the index.
    application:get_env(rabbit, classic_queue_index_v2_segment_entry_count, 4096).

max_cache_size() ->
    application:get_env(rabbit, classic_queue_store_v2_max_cache_size, 512000).

check_crc32() ->
    application:get_env(rabbit, classic_queue_store_v2_check_crc32, true).

%% Same implementation as rabbit_classic_queue_index_v2:segment_file/2,
%% but with a different state record.
segment_file(Segment, #qs{ dir = Dir }) ->
    filename:join(Dir, integer_to_list(Segment) ++ ?SEGMENT_EXTENSION).
