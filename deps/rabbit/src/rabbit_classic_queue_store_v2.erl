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

-module(rabbit_classic_queue_store_v2).

-export([init/2, terminate/1,
         write/4, sync/1, read/3,
         remove/2, delete_segments/2]).

%% @todo Maybe name the files .qi and .qs? (queue index and queue store?)
-define(SEGMENT_EXTENSION, ".mstr").

-include_lib("rabbit_common/include/rabbit.hrl").

%% Set to true to get an awful lot of debug logs.
-if(false).
-define(DEBUG(X,Y), logger:debug("~0p: " ++ X, [?FUNCTION_NAME|Y])).
-else.
-define(DEBUG(X,Y), _ = X, _ = Y, ok).
-endif.

%-type seq_id() :: non_neg_integer().
%% @todo Use a shared seq_id() type in all relevant modules.

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
    write_offset = 0 :: non_neg_integer(), %% @todo Perhaps have a file header.

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
    cache_table = undefined :: ets:tid(),
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
    on_sync :: on_sync_fun() %% @todo Rename to reflect the lack of file:sync.
}).

%% Types copied from rabbit_queue_index.
-type on_sync_fun() :: fun ((gb_sets:set()) -> ok).

%% @todo specs everywhere

init(#resource{ virtual_host = VHost } = Name, OnSyncFun) ->
    ?DEBUG("~0p ~0p", [Name, OnSyncFun]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = rabbit_classic_queue_index_v2:queue_dir(VHostDir, Name),
    CacheTable = ets:new(rabbit_classic_queue_store_v2_cache, [set, public]),
    #qs{
        dir = Dir,
        on_sync = OnSyncFun,
        cache_table = CacheTable
    }.

%% @todo Recover: do nothing? Just normal init?

terminate(State = #qs{ write_fd = WriteFd,
                       read_fd = ReadFd }) ->
    ?DEBUG("~0p", [State]),
    %% Fsync and close all FDs as needed.
    maybe_sync_close_fd(WriteFd),
    maybe_close_fd(ReadFd),
    State#qs{ write_segment = undefined,
              write_fd = undefined,
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

write(SeqId, MsgSize, Msg=#basic_message{ id = MsgId }, State0 = #qs{ confirms = Confirms }) ->
    ?DEBUG("~0p ~0p ~0p ~0p", [SeqId, MsgSize, Msg, State0]),
    SegmentEntryCount = 65536, %% @todo segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    %% We simply append to the related segment file.
    %% We will then return the offset and size. That
    %% combined with the SeqId is enough to find the
    %% message again.
    {Fd, Offset, State1} = get_write_fd(Segment, State0),
    MsgIovec = term_to_iovec(Msg),
    Size = iolist_size(MsgIovec),
    %% Append to the buffer.
    ok = file:write(Fd, MsgIovec),
    %% Maybe cache the message.
    State = maybe_cache(SeqId, MsgSize, Msg, State1),
    %% Keep track of the offset we are at.
    {{?MODULE, Offset, Size}, State#qs{ write_offset = Offset + Size,
                                        %% @todo Only add messages that need to be confirmed.
                                        confirms = gb_sets:add_element(MsgId, Confirms) }}.

get_write_fd(Segment, State = #qs{ write_fd = Fd,
                                   write_segment = Segment,
                                   write_offset = Offset }) ->
    {Fd, Offset, State};
get_write_fd(Segment, State = #qs{ write_fd = OldFd }) ->
    maybe_close_fd(OldFd),
    {ok, Fd} = file:open(segment_file(Segment, State), [read, append, raw, binary, {delayed_write, 512000, 10000}]),
    {ok, Offset} = file:position(Fd, cur),
    {Fd, Offset, State#qs{ write_segment = Segment,
                           write_fd = Fd,
                           %% We could as a tiny optimisation not write this offset
                           %% until after the write has been completed.
                           write_offset = Offset }}.

maybe_cache(SeqId, MsgSize, Msg, State = #qs{ cache_table = CacheTable,
                                              cache_size = CacheSize }) ->
    MaxCacheSize = 32000000, %% @todo Configurable.
    if
        CacheSize + MsgSize > MaxCacheSize ->
            State;
        true ->
            true = ets:insert_new(CacheTable, {SeqId, MsgSize, Msg}),
            State#qs{ cache_size = CacheSize + MsgSize }
    end.

sync(State = #qs{ confirms = Confirms,
                  on_sync = OnSyncFun }) ->
    ?DEBUG("~0p", [State]),
    OnSyncFun(Confirms, written),
    State#qs{ confirms = gb_sets:new() }.

read(SeqId, DiskLocation, State = #qs{ cache_table = CacheTable }) ->
    ?DEBUG("~0p ~0p ~0p", [SeqId, DiskLocation, State]),
    case ets:lookup(CacheTable, SeqId) of
        [{SeqId, _MsgSize, Msg}] ->
            {Msg, State};
        [] ->
            read_from_disk(SeqId, DiskLocation, State)
    end.

read_from_disk(SeqId, {?MODULE, Offset, Size}, State0) ->
    SegmentEntryCount = 65536, %% @todo segment_entry_count(),
    Segment = SeqId div SegmentEntryCount,
    {Fd, State} = get_read_fd(Segment, State0),
    %% @todo We can't use pread because the position is undefined after.
    %%       What to do? Have one fd for writing (writes should be
    %%       more or less sequential after all), and multiple for
    %%       reading? How many for reading? One? Reads should be
    %%       relatively sequential as well since we are fifo.
    %%       So have one read fd and one write fd then. We will
    %%       still use pread for reading.
    %%
    %% @todo Before trying to read we may need to flush the buffer to disk
    %%       otherwise we risk getting eof. Alternatively we could read
    %%       from the buffer directly? Though this can only happen if
    %%       the write buffer can get bigger than the cache I believe
    %%       (or more accurately, messages are in the write but not the cache).
    %%       A solution could be to use {delayed_write, Size, Delay}.
    {ok, MsgBin} = file:pread(Fd, Offset, Size),
    Msg = binary_to_term(MsgBin),
    {Msg, State}.

get_read_fd(Segment, State = #qs{ read_segment = Segment,
                                  read_fd = Fd }) ->
    {Fd, State};
get_read_fd(Segment, State = #qs{ read_fd = OldFd }) ->
    maybe_close_fd(OldFd),
    {ok, Fd} = file:open(segment_file(Segment, State), [read, raw, binary]),
    {Fd, State#qs{ read_segment = Segment,
                   read_fd = Fd }}.

%% We only remove the message from the cache. We will remove
%% the message from the disk when we delete the segment file.
%% @todo Maybe rename remove_from_cache? Since we don't really remove.
remove(SeqId, State = #qs{ cache_table = CacheTable,
                           cache_size = CacheSize }) ->
    ?DEBUG("~0p ~0p", [SeqId, State]),
    case ets:take(CacheTable, SeqId) of
        [] -> State;
        [{_, MsgSize, _}] -> State#qs{ cache_size = CacheSize - MsgSize }
    end.

%% First we check if the write fd points to a segment
%% that must be deleted, and we close it if that's the case.
delete_segments([], State) ->
    ?DEBUG("[] ~0p", [State]),
    State;
delete_segments(Segments, State0 = #qs{ write_segment = WriteSegment,
                                        write_fd = WriteFd,
                                        read_segment = ReadSegment,
                                        read_fd = ReadFd }) ->
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
                       read_segment = undefined,
                       read_fd = undefined };
        CloseWrite ->
            ok = file:close(WriteFd),
            State0#qs{ write_segment = undefined,
                       write_fd = undefined };
        CloseRead ->
            ok = file:close(ReadFd),
            State0#qs{ read_segment = undefined,
                       read_fd = undefined };
        true ->
            State0
    end,
    %% Then we delete the files.
    _ = [
        ok = file:delete(segment_file(Segment, State))
    || Segment <- Segments],
    State.

%% Same implementation as rabbit_classic_queue_index_v2:segment_file/2,
%% but with a different state record.
segment_file(Segment, #qs{ dir = Dir }) ->
    filename:join(Dir, integer_to_list(Segment) ++ ?SEGMENT_EXTENSION).
