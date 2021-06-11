%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_index).

-compile({inline, [segment_entry_count/0]}).

-export([erase/1, init/3, reset_state/1, recover/6,
         terminate/3, delete_and_terminate/1,
         pre_publish/7, flush_pre_publish_cache/2,
         publish/6, deliver/2, ack/2, sync/1, needs_sync/1, flush/1,
         read/3, next_segment_boundary/1, bounds/1, start/2, stop/1]).

-export([add_queue_ttl/0, avoid_zeroes/0, store_msg_size/0, store_msg/0]).
-export([scan_queue_segments/3, scan_queue_segments/4]).

%% Migrates from global to per-vhost message stores
-export([move_to_per_vhost_stores/1,
         update_recovery_term/2,
         read_global_recovery_terms/1,
         cleanup_global_recovery_terms/0]).

%% Used by rabbit_vhost to set the segment_entry_count.
-export([all_queue_directory_names/1]).

-define(CLEAN_FILENAME, "clean.dot").

%%----------------------------------------------------------------------------

%% The queue index is responsible for recording the order of messages
%% within a queue on disk. As such it contains records of messages
%% being published, delivered and acknowledged. The publish record
%% includes the sequence ID, message ID and a small quantity of
%% metadata about the message; the delivery and acknowledgement
%% records just contain the sequence ID. A publish record may also
%% contain the complete message if provided to publish/5; this allows
%% the message store to be avoided altogether for small messages. In
%% either case the publish record is stored in memory in the same
%% serialised format it will take on disk.
%%
%% Because of the fact that the queue can decide at any point to send
%% a queue entry to disk, you can not rely on publishes appearing in
%% order. The only thing you can rely on is a message being published,
%% then delivered, then ack'd.
%%
%% In order to be able to clean up ack'd messages, we write to segment
%% files. These files have a fixed number of entries: segment_entry_count()
%% publishes, delivers and acknowledgements. They are numbered, and so
%% it is known that the 0th segment contains messages 0 ->
%% segment_entry_count() - 1, the 1st segment contains messages
%% segment_entry_count() -> 2*segment_entry_count() - 1 and so on. As
%% such, in the segment files, we only refer to message sequence ids
%% by the LSBs as SeqId rem segment_entry_count(). This gives them a
%% fixed size.
%%
%% However, transient messages which are not sent to disk at any point
%% will cause gaps to appear in segment files. Therefore, we delete a
%% segment file whenever the number of publishes == number of acks
%% (note that although it is not fully enforced, it is assumed that a
%% message will never be ackd before it is delivered, thus this test
%% also implies == number of delivers). In practise, this does not
%% cause disk churn in the pathological case because of the journal
%% and caching (see below).
%%
%% Because of the fact that publishes, delivers and acks can occur all
%% over, we wish to avoid lots of seeking. Therefore we have a fixed
%% sized journal to which all actions are appended. When the number of
%% entries in this journal reaches max_journal_entries, the journal
%% entries are scattered out to their relevant files, and the journal
%% is truncated to zero size. Note that entries in the journal must
%% carry the full sequence id, thus the format of entries in the
%% journal is different to that in the segments.
%%
%% The journal is also kept fully in memory, pre-segmented: the state
%% contains a mapping from segment numbers to state-per-segment (this
%% state is held for all segments which have been "seen": thus a
%% segment which has been read but has no pending entries in the
%% journal is still held in this mapping. Also note that a map is
%% used for this mapping, not an array because with an array, you will
%% always have entries from 0). Actions are stored directly in this
%% state. Thus at the point of flushing the journal, firstly no
%% reading from disk is necessary, but secondly if the known number of
%% acks and publishes in a segment are equal, given the known state of
%% the segment file combined with the journal, no writing needs to be
%% done to the segment file either (in fact it is deleted if it exists
%% at all). This is safe given that the set of acks is a subset of the
%% set of publishes. When it is necessary to sync messages, it is
%% sufficient to fsync on the journal: when entries are distributed
%% from the journal to segment files, those segments appended to are
%% fsync'd prior to the journal being truncated.
%%
%% This module is also responsible for scanning the queue index files
%% and seeding the message store on start up.
%%
%% Note that in general, the representation of a message's state as
%% the tuple: {('no_pub'|{IsPersistent, Bin, MsgBin}),
%% ('del'|'no_del'), ('ack'|'no_ack')} is richer than strictly
%% necessary for most operations. However, for startup, and to ensure
%% the safe and correct combination of journal entries with entries
%% read from the segment on disk, this richer representation vastly
%% simplifies and clarifies the code.
%%
%% For notes on Clean Shutdown and startup, see documentation in
%% rabbit_variable_queue.
%%
%%----------------------------------------------------------------------------

%% ---- Journal details ----

-define(JOURNAL_FILENAME, "journal.jif").
-define(QUEUE_NAME_STUB_FILE, ".queue_name").

-define(PUB_PERSIST_JPREFIX, 2#00).
-define(PUB_TRANS_JPREFIX,   2#01).
-define(DEL_JPREFIX,         2#10).
-define(ACK_JPREFIX,         2#11).
-define(JPREFIX_BITS, 2).
-define(SEQ_BYTES, 8).
-define(SEQ_BITS, ((?SEQ_BYTES * 8) - ?JPREFIX_BITS)).

%% ---- Segment details ----

-define(SEGMENT_EXTENSION, ".idx").

%% TODO: The segment size would be configurable, but deriving all the
%% other values is quite hairy and quite possibly noticeably less
%% efficient, depending on how clever the compiler is when it comes to
%% binary generation/matching with constant vs variable lengths.

-define(REL_SEQ_BITS, 14).

%% seq only is binary 01 followed by 14 bits of rel seq id
%% (range: 0 - 16383)
-define(REL_SEQ_ONLY_PREFIX, 01).
-define(REL_SEQ_ONLY_PREFIX_BITS, 2).
-define(REL_SEQ_ONLY_RECORD_BYTES, 2).

%% publish record is binary 1 followed by a bit for is_persistent,
%% then 14 bits of rel seq id, 64 bits for message expiry, 32 bits of
%% size and then 128 bits of md5sum msg id.
-define(PUB_PREFIX, 1).
-define(PUB_PREFIX_BITS, 1).

-define(EXPIRY_BYTES, 8).
-define(EXPIRY_BITS, (?EXPIRY_BYTES * 8)).
-define(NO_EXPIRY, 0).

-define(MSG_ID_BYTES, 16). %% md5sum is 128 bit or 16 bytes
-define(MSG_ID_BITS, (?MSG_ID_BYTES * 8)).

%% This is the size of the message body content, for stats
-define(SIZE_BYTES, 4).
-define(SIZE_BITS, (?SIZE_BYTES * 8)).

%% This is the size of the message record embedded in the queue
%% index. If 0, the message can be found in the message store.
-define(EMBEDDED_SIZE_BYTES, 4).
-define(EMBEDDED_SIZE_BITS, (?EMBEDDED_SIZE_BYTES * 8)).

%% 16 bytes for md5sum + 8 for expiry
-define(PUB_RECORD_BODY_BYTES, (?MSG_ID_BYTES + ?EXPIRY_BYTES + ?SIZE_BYTES)).
%% + 4 for size
-define(PUB_RECORD_SIZE_BYTES, (?PUB_RECORD_BODY_BYTES + ?EMBEDDED_SIZE_BYTES)).

%% + 2 for seq, bits and prefix
-define(PUB_RECORD_PREFIX_BYTES, 2).

%% ---- misc ----

-define(PUB, {_, _, _}). %% {IsPersistent, Bin, MsgBin}

-define(READ_MODE, [binary, raw, read]).
-define(WRITE_MODE, [write | ?READ_MODE]).

%%----------------------------------------------------------------------------

-record(qistate, {
  %% queue directory where segment and journal files are stored
  dir,
  %% map of #segment records
  segments,
  %% journal file handle obtained from/used by file_handle_cache
  journal_handle,
  %% how many not yet flushed entries are there
  dirty_count,
  %% this many not yet flushed journal entries will force a flush
  max_journal_entries,
  %% callback function invoked when a message is "handled"
  %% by the index and potentially can be confirmed to the publisher
  on_sync,
  on_sync_msg,
  %% set of IDs of unconfirmed [to publishers] messages
  unconfirmed,
  unconfirmed_msg,
  %% optimisation
  pre_publish_cache,
  %% optimisation
  delivered_cache,
  %% queue name resource record
  queue_name}).

-record(segment, {
  %% segment ID (an integer)
  num,
  %% segment file path (see also ?SEGMENT_EXTENSION)
  path,
  %% index operation log entries in this segment
  journal_entries,
  entries_to_segment,
  %% counter of unacknowledged messages
  unacked
}).

-include_lib("rabbit_common/include/rabbit.hrl").

%%----------------------------------------------------------------------------

-rabbit_upgrade({add_queue_ttl,  local, []}).
-rabbit_upgrade({avoid_zeroes,   local, [add_queue_ttl]}).
-rabbit_upgrade({store_msg_size, local, [avoid_zeroes]}).
-rabbit_upgrade({store_msg,      local, [store_msg_size]}).

-type hdl() :: ('undefined' | any()).
-type segment() :: ('undefined' |
                    #segment { num                :: non_neg_integer(),
                               path               :: file:filename(),
                               journal_entries    :: array:array(),
                               entries_to_segment :: array:array(),
                               unacked            :: non_neg_integer()
                             }).
-type seq_id() :: integer().
-type seg_map() :: {map(), [segment()]}.
-type on_sync_fun() :: fun ((gb_sets:set()) -> ok).
-type qistate() :: #qistate { dir                 :: file:filename(),
                              segments            :: 'undefined' | seg_map(),
                              journal_handle      :: hdl(),
                              dirty_count         :: integer(),
                              max_journal_entries :: non_neg_integer(),
                              on_sync             :: on_sync_fun(),
                              on_sync_msg         :: on_sync_fun(),
                              unconfirmed         :: gb_sets:set(),
                              unconfirmed_msg     :: gb_sets:set(),
                              pre_publish_cache   :: list(),
                              delivered_cache     :: list()
                            }.
-type contains_predicate() :: fun ((rabbit_types:msg_id()) -> boolean()).
-type walker(A) :: fun ((A) -> 'finished' |
                               {rabbit_types:msg_id(), non_neg_integer(), A}).
-type shutdown_terms() :: [term()] | 'non_clean_shutdown'.

%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------

-spec erase(rabbit_amqqueue:name()) -> 'ok'.

erase(#resource{ virtual_host = VHost } = Name) ->
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    #qistate { dir = Dir } = blank_state(VHostDir, Name),
    erase_index_dir(Dir).

%% used during variable queue purge when there are no pending acks

-spec reset_state(qistate()) -> qistate().

reset_state(#qistate{ queue_name     = Name,
                      dir            = Dir,
                      on_sync        = OnSyncFun,
                      on_sync_msg    = OnSyncMsgFun,
                      journal_handle = JournalHdl }) ->
    ok = case JournalHdl of
             undefined -> ok;
             _         -> file_handle_cache:close(JournalHdl)
         end,
    ok = erase_index_dir(Dir),
    blank_state_name_dir_funs(Name, Dir, OnSyncFun, OnSyncMsgFun).

-spec init(rabbit_amqqueue:name(),
                 on_sync_fun(), on_sync_fun()) -> qistate().

init(#resource{ virtual_host = VHost } = Name, OnSyncFun, OnSyncMsgFun) ->
    #{segment_entry_count := SegmentEntryCount} = rabbit_vhost:read_config(VHost),
    put(segment_entry_count, SegmentEntryCount),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    State = #qistate { dir = Dir } = blank_state(VHostDir, Name),
    false = rabbit_file:is_file(Dir), %% is_file == is file or dir
    State#qistate{on_sync     = OnSyncFun,
                  on_sync_msg = OnSyncMsgFun}.

-spec recover(rabbit_amqqueue:name(), shutdown_terms(), boolean(),
                    contains_predicate(),
                    on_sync_fun(), on_sync_fun()) ->
                        {'undefined' | non_neg_integer(),
                         'undefined' | non_neg_integer(), qistate()}.

recover(#resource{ virtual_host = VHost } = Name, Terms, MsgStoreRecovered,
        ContainsCheckFun, OnSyncFun, OnSyncMsgFun) ->
    #{segment_entry_count := SegmentEntryCount} = rabbit_vhost:read_config(VHost),
    put(segment_entry_count, SegmentEntryCount),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    State = blank_state(VHostDir, Name),
    State1 = State #qistate{on_sync     = OnSyncFun,
                            on_sync_msg = OnSyncMsgFun},
    CleanShutdown = Terms /= non_clean_shutdown,
    case CleanShutdown andalso MsgStoreRecovered of
        true  -> case proplists:get_value(segments, Terms, non_clean_shutdown) of
                     non_clean_shutdown -> init_dirty(false, ContainsCheckFun, State1);
                     RecoveredCounts    -> init_clean(RecoveredCounts, State1)
                 end;
        false -> init_dirty(CleanShutdown, ContainsCheckFun, State1)
    end.

-spec terminate(rabbit_types:vhost(), [any()], qistate()) -> qistate().

terminate(VHost, Terms, State = #qistate { dir = Dir }) ->
    {SegmentCounts, State1} = terminate(State),
    rabbit_recovery_terms:store(VHost, filename:basename(Dir),
                                [{segments, SegmentCounts} | Terms]),
    State1.

-spec delete_and_terminate(qistate()) -> qistate().

delete_and_terminate(State) ->
    {_SegmentCounts, State1 = #qistate { dir = Dir }} = terminate(State),
    ok = rabbit_file:recursive_delete([Dir]),
    State1.

pre_publish(MsgOrId, SeqId, MsgProps, IsPersistent, IsDelivered, JournalSizeHint,
            State = #qistate{pre_publish_cache = PPC,
                             delivered_cache   = DC}) ->
    State1 = maybe_needs_confirming(MsgProps, MsgOrId, State),

    {Bin, MsgBin} = create_pub_record_body(MsgOrId, MsgProps),

    PPC1 =
        [[<<(case IsPersistent of
                true  -> ?PUB_PERSIST_JPREFIX;
                false -> ?PUB_TRANS_JPREFIX
            end):?JPREFIX_BITS,
           SeqId:?SEQ_BITS, Bin/binary,
           (size(MsgBin)):?EMBEDDED_SIZE_BITS>>, MsgBin] | PPC],

    DC1 =
        case IsDelivered of
            true ->
                [SeqId | DC];
            false ->
                DC
        end,

    State2 = add_to_journal(SeqId, {IsPersistent, Bin, MsgBin}, State1),
    maybe_flush_pre_publish_cache(
                     JournalSizeHint,
                     State2#qistate{pre_publish_cache = PPC1,
                                    delivered_cache   = DC1}).

%% pre_publish_cache is the entry with most elements when compared to
%% delivered_cache so we only check the former in the guard.
maybe_flush_pre_publish_cache(JournalSizeHint,
                              #qistate{pre_publish_cache = PPC} = State) ->
    case length(PPC) >= segment_entry_count() of
        true -> flush_pre_publish_cache(JournalSizeHint, State);
        false -> State
    end.

flush_pre_publish_cache(JournalSizeHint, State) ->
    State1 = flush_pre_publish_cache(State),
    State2 = flush_delivered_cache(State1),
    maybe_flush_journal(JournalSizeHint, State2).

flush_pre_publish_cache(#qistate{pre_publish_cache = []} = State) ->
    State;
flush_pre_publish_cache(State = #qistate{pre_publish_cache = PPC}) ->
    {JournalHdl, State1} = get_journal_handle(State),
    file_handle_cache_stats:update(queue_index_journal_write),
    ok = file_handle_cache:append(JournalHdl, lists:reverse(PPC)),
    State1#qistate{pre_publish_cache = []}.

flush_delivered_cache(#qistate{delivered_cache = []} = State) ->
    State;
flush_delivered_cache(State = #qistate{delivered_cache = DC}) ->
    State1 = deliver(lists:reverse(DC), State),
    State1#qistate{delivered_cache = []}.

-spec publish(rabbit_types:msg_id(), seq_id(),
                    rabbit_types:message_properties(), boolean(),
                    non_neg_integer(), qistate()) -> qistate().

publish(MsgOrId, SeqId, MsgProps, IsPersistent, JournalSizeHint, State) ->
    {JournalHdl, State1} =
        get_journal_handle(
          maybe_needs_confirming(MsgProps, MsgOrId, State)),
    file_handle_cache_stats:update(queue_index_journal_write),
    {Bin, MsgBin} = create_pub_record_body(MsgOrId, MsgProps),
    ok = file_handle_cache:append(
           JournalHdl, [<<(case IsPersistent of
                               true  -> ?PUB_PERSIST_JPREFIX;
                               false -> ?PUB_TRANS_JPREFIX
                           end):?JPREFIX_BITS,
                          SeqId:?SEQ_BITS, Bin/binary,
                          (size(MsgBin)):?EMBEDDED_SIZE_BITS>>, MsgBin]),
    maybe_flush_journal(
      JournalSizeHint,
      add_to_journal(SeqId, {IsPersistent, Bin, MsgBin}, State1)).

maybe_needs_confirming(MsgProps, MsgOrId,
        State = #qistate{unconfirmed     = UC,
                         unconfirmed_msg = UCM}) ->
    MsgId = case MsgOrId of
                #basic_message{id = Id} -> Id;
                Id when is_binary(Id)   -> Id
            end,
    ?MSG_ID_BYTES = size(MsgId),
    case {MsgProps#message_properties.needs_confirming, MsgOrId} of
      {true,  MsgId} -> UC1  = gb_sets:add_element(MsgId, UC),
                        State#qistate{unconfirmed     = UC1};
      {true,  _}     -> UCM1 = gb_sets:add_element(MsgId, UCM),
                        State#qistate{unconfirmed_msg = UCM1};
      {false, _}     -> State
    end.

-spec deliver([seq_id()], qistate()) -> qistate().

deliver(SeqIds, State) ->
    deliver_or_ack(del, SeqIds, State).

-spec ack([seq_id()], qistate()) -> qistate().

ack(SeqIds, State) ->
    deliver_or_ack(ack, SeqIds, State).

%% This is called when there are outstanding confirms or when the
%% queue is idle and the journal needs syncing (see needs_sync/1).

-spec sync(qistate()) -> qistate().

sync(State = #qistate { journal_handle = undefined }) ->
    State;
sync(State = #qistate { journal_handle = JournalHdl }) ->
    ok = file_handle_cache:sync(JournalHdl),
    notify_sync(State).

-spec needs_sync(qistate()) -> 'confirms' | 'other' | 'false'.

needs_sync(#qistate{journal_handle = undefined}) ->
    false;
needs_sync(#qistate{journal_handle  = JournalHdl,
                    unconfirmed     = UC,
                    unconfirmed_msg = UCM}) ->
    case gb_sets:is_empty(UC) andalso gb_sets:is_empty(UCM) of
        true  -> case file_handle_cache:needs_sync(JournalHdl) of
                     true  -> other;
                     false -> false
                 end;
        false -> confirms
    end.

-spec flush(qistate()) -> qistate().

flush(State = #qistate { dirty_count = 0 }) -> State;
flush(State)                                -> flush_journal(State).

-spec read(seq_id(), seq_id(), qistate()) ->
                     {[{rabbit_types:msg_id(), seq_id(),
                        rabbit_types:message_properties(),
                        boolean(), boolean()}], qistate()}.

read(StartEnd, StartEnd, State) ->
    {[], State};
read(Start, End, State = #qistate { segments = Segments,
                                    dir = Dir }) when Start =< End ->
    %% Start is inclusive, End is exclusive.
    LowerB = {StartSeg, _StartRelSeq} = seq_id_to_seg_and_rel_seq_id(Start),
    UpperB = {EndSeg,   _EndRelSeq}   = seq_id_to_seg_and_rel_seq_id(End - 1),
    {Messages, Segments1} =
        lists:foldr(fun (Seg, Acc) ->
                            read_bounded_segment(Seg, LowerB, UpperB, Acc, Dir)
                    end, {[], Segments}, lists:seq(StartSeg, EndSeg)),
    {Messages, State #qistate { segments = Segments1 }}.

-spec next_segment_boundary(seq_id()) -> seq_id().

next_segment_boundary(SeqId) ->
    {Seg, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    reconstruct_seq_id(Seg + 1, 0).

-spec bounds(qistate()) ->
                       {non_neg_integer(), non_neg_integer(), qistate()}.

bounds(State = #qistate { segments = Segments }) ->
    %% This is not particularly efficient, but only gets invoked on
    %% queue initialisation.
    SegNums = lists:sort(segment_nums(Segments)),
    %% Don't bother trying to figure out the lowest seq_id, merely the
    %% seq_id of the start of the lowest segment. That seq_id may not
    %% actually exist, but that's fine. The important thing is that
    %% the segment exists and the seq_id reported is on a segment
    %% boundary.
    %%
    %% We also don't really care about the max seq_id. Just start the
    %% next segment: it makes life much easier.
    %%
    %% SegNums is sorted, ascending.
    {LowSeqId, NextSeqId} =
        case SegNums of
            []         -> {0, 0};
            [MinSeg|_] -> {reconstruct_seq_id(MinSeg, 0),
                           reconstruct_seq_id(1 + lists:last(SegNums), 0)}
        end,
    {LowSeqId, NextSeqId, State}.

-spec start(rabbit_types:vhost(), [rabbit_amqqueue:name()]) -> {[[any()]], {walker(A), A}}.

start(VHost, DurableQueueNames) ->
    ok = rabbit_recovery_terms:start(VHost),
    {DurableTerms, DurableDirectories} =
        lists:foldl(
          fun(QName, {RecoveryTerms, ValidDirectories}) ->
                  DirName = queue_name_to_dir_name(QName),
                  RecoveryInfo = case rabbit_recovery_terms:read(VHost, DirName) of
                                     {error, _}  -> non_clean_shutdown;
                                     {ok, Terms} -> Terms
                                 end,
                  {[RecoveryInfo | RecoveryTerms],
                   sets:add_element(DirName, ValidDirectories)}
          end, {[], sets:new()}, DurableQueueNames),
    %% Any queue directory we've not been asked to recover is considered garbage
    rabbit_file:recursive_delete(
      [DirName ||
        DirName <- all_queue_directory_names(VHost),
        not sets:is_element(filename:basename(DirName), DurableDirectories)]),
    rabbit_recovery_terms:clear(VHost),

    %% The backing queue interface requires that the queue recovery terms
    %% which come back from start/1 are in the same order as DurableQueueNames
    OrderedTerms = lists:reverse(DurableTerms),
    {OrderedTerms, {fun queue_index_walker/1, {start, DurableQueueNames}}}.


stop(VHost) -> rabbit_recovery_terms:stop(VHost).

all_queue_directory_names(VHost) ->
    filelib:wildcard(filename:join([rabbit_vhost:msg_store_dir_path(VHost),
                                    "queues", "*"])).

all_queue_directory_names() ->
    filelib:wildcard(filename:join([rabbit_vhost:msg_store_dir_wildcard(),
                                    "queues", "*"])).

%%----------------------------------------------------------------------------
%% startup and shutdown
%%----------------------------------------------------------------------------

erase_index_dir(Dir) ->
    case rabbit_file:is_dir(Dir) of
        true  -> rabbit_file:recursive_delete([Dir]);
        false -> ok
    end.

blank_state(VHostDir, QueueName) ->
    Dir = queue_dir(VHostDir, QueueName),
    blank_state_name_dir_funs(QueueName,
                         Dir,
                         fun (_) -> ok end,
                         fun (_) -> ok end).

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

queue_name_to_dir_name_legacy(Name = #resource { kind = queue }) ->
    <<Num:128>> = erlang:md5(term_to_binary_compat:term_to_binary_1(Name)),
    rabbit_misc:format("~.36B", [Num]).

queues_base_dir() ->
    rabbit_mnesia:dir().

blank_state_name_dir_funs(Name, Dir, OnSyncFun, OnSyncMsgFun) ->
    {ok, MaxJournal} =
        application:get_env(rabbit, queue_index_max_journal_entries),
    #qistate { dir                 = Dir,
               segments            = segments_new(),
               journal_handle      = undefined,
               dirty_count         = 0,
               max_journal_entries = MaxJournal,
               on_sync             = OnSyncFun,
               on_sync_msg         = OnSyncMsgFun,
               unconfirmed         = gb_sets:new(),
               unconfirmed_msg     = gb_sets:new(),
               pre_publish_cache   = [],
               delivered_cache     = [],
               queue_name          = Name }.

init_clean(RecoveredCounts, State) ->
    %% Load the journal. Since this is a clean recovery this (almost)
    %% gets us back to where we were on shutdown.
    State1 = #qistate { dir = Dir, segments = Segments } = load_journal(State),
    %% The journal loading only creates records for segments touched
    %% by the journal, and the counts are based on the journal entries
    %% only. We need *complete* counts for *all* segments. By an
    %% amazing coincidence we stored that information on shutdown.
    Segments1 =
        lists:foldl(
          fun ({Seg, UnackedCount}, SegmentsN) ->
                  Segment = segment_find_or_new(Seg, Dir, SegmentsN),
                  segment_store(Segment #segment { unacked = UnackedCount },
                                SegmentsN)
          end, Segments, RecoveredCounts),
    %% the counts above include transient messages, which would be the
    %% wrong thing to return
    {undefined, undefined, State1 # qistate { segments = Segments1 }}.

init_dirty(CleanShutdown, ContainsCheckFun, State) ->
    %% Recover the journal completely. This will also load segments
    %% which have entries in the journal and remove duplicates. The
    %% counts will correctly reflect the combination of the segment
    %% and the journal.
    State1 = #qistate { dir = Dir, segments = Segments } =
        recover_journal(State),
    {Segments1, Count, Bytes, DirtyCount} =
        %% Load each segment in turn and filter out messages that are
        %% not in the msg_store, by adding acks to the journal. These
        %% acks only go to the RAM journal as it doesn't matter if we
        %% lose them. Also mark delivered if not clean shutdown. Also
        %% find the number of unacked messages. Also accumulate the
        %% dirty count here, so we can call maybe_flush_journal below
        %% and avoid unnecessary file system operations.
        lists:foldl(
          fun (Seg, {Segments2, CountAcc, BytesAcc, DirtyCount}) ->
                  {{Segment = #segment { unacked = UnackedCount }, Dirty},
                   UnackedBytes} =
                      recover_segment(ContainsCheckFun, CleanShutdown,
                                      segment_find_or_new(Seg, Dir, Segments2),
                                      State1#qistate.max_journal_entries),
                  {segment_store(Segment, Segments2),
                   CountAcc + UnackedCount,
                   BytesAcc + UnackedBytes, DirtyCount + Dirty}
          end, {Segments, 0, 0, 0}, all_segment_nums(State1)),
    State2 = maybe_flush_journal(State1 #qistate { segments = Segments1,
                                                   dirty_count = DirtyCount }),
    {Count, Bytes, State2}.

terminate(State = #qistate { journal_handle = JournalHdl,
                             segments = Segments }) ->
    ok = case JournalHdl of
             undefined -> ok;
             _         -> file_handle_cache:close(JournalHdl)
         end,
    SegmentCounts =
        segment_fold(
          fun (#segment { num = Seg, unacked = UnackedCount }, Acc) ->
                  [{Seg, UnackedCount} | Acc]
          end, [], Segments),
    {SegmentCounts, State #qistate { journal_handle = undefined,
                                     segments = undefined }}.

recover_segment(ContainsCheckFun, CleanShutdown,
                Segment = #segment { journal_entries = JEntries }, MaxJournal) ->
    {SegEntries, UnackedCount} = load_segment(false, Segment),
    {SegEntries1, UnackedCountDelta} =
        segment_plus_journal(SegEntries, JEntries),
    array:sparse_foldl(
      fun (RelSeq, {{IsPersistent, Bin, MsgBin}, Del, no_ack},
           {SegmentAndDirtyCount, Bytes}) ->
              {MsgOrId, MsgProps} = parse_pub_record_body(Bin, MsgBin),
              {recover_message(ContainsCheckFun(MsgOrId), CleanShutdown,
                               Del, RelSeq, SegmentAndDirtyCount, MaxJournal),
               Bytes + case IsPersistent of
                           true  -> MsgProps#message_properties.size;
                           false -> 0
                       end}
      end,
      {{Segment #segment { unacked = UnackedCount + UnackedCountDelta }, 0}, 0},
      SegEntries1).

recover_message( true,  true,   _Del, _RelSeq, SegmentAndDirtyCount, _MaxJournal) ->
    SegmentAndDirtyCount;
recover_message( true, false,    del, _RelSeq, SegmentAndDirtyCount, _MaxJournal) ->
    SegmentAndDirtyCount;
recover_message( true, false, no_del,  RelSeq, {Segment, _DirtyCount}, MaxJournal) ->
    %% force to flush the segment
    {add_to_journal(RelSeq, del, Segment), MaxJournal + 1}; 
recover_message(false,     _,    del,  RelSeq, {Segment, DirtyCount}, _MaxJournal) ->
    {add_to_journal(RelSeq, ack, Segment), DirtyCount + 1};
recover_message(false,     _, no_del,  RelSeq, {Segment, DirtyCount}, _MaxJournal) ->
    {add_to_journal(RelSeq, ack,
                    add_to_journal(RelSeq, del, Segment)),
     DirtyCount + 2}.

%%----------------------------------------------------------------------------
%% msg store startup delta function
%%----------------------------------------------------------------------------

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

queue_index_walker_reader(QueueName, Gatherer) ->
    ok = scan_queue_segments(
           fun (_SeqId, MsgId, _MsgProps, true, _IsDelivered, no_ack, ok)
                 when is_binary(MsgId) ->
                   gatherer:sync_in(Gatherer, {MsgId, 1});
               (_SeqId, _MsgId, _MsgProps, _IsPersistent, _IsDelivered,
                _IsAcked, Acc) ->
                   Acc
           end, ok, QueueName),
    ok = gatherer:finish(Gatherer).

scan_queue_segments(Fun, Acc, #resource{ virtual_host = VHost } = QueueName) ->
    %% Set the segment_entry_count for this worker process.
    #{segment_entry_count := SegmentEntryCount} = rabbit_vhost:read_config(VHost),
    put(segment_entry_count, SegmentEntryCount),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    scan_queue_segments(Fun, Acc, VHostDir, QueueName).

scan_queue_segments(Fun, Acc, VHostDir, QueueName) ->
    State = #qistate { segments = Segments, dir = Dir } =
        recover_journal(blank_state(VHostDir, QueueName)),
    Result = lists:foldr(
      fun (Seg, AccN) ->
              segment_entries_foldr(
                fun (RelSeq, {{MsgOrId, MsgProps, IsPersistent},
                              IsDelivered, IsAcked}, AccM) ->
                        Fun(reconstruct_seq_id(Seg, RelSeq), MsgOrId, MsgProps,
                            IsPersistent, IsDelivered, IsAcked, AccM)
                end, AccN, segment_find_or_new(Seg, Dir, Segments))
      end, Acc, all_segment_nums(State)),
    {_SegmentCounts, _State} = terminate(State),
    Result.

%%----------------------------------------------------------------------------
%% expiry/binary manipulation
%%----------------------------------------------------------------------------

create_pub_record_body(MsgOrId, #message_properties { expiry = Expiry,
                                                      size   = Size }) ->
    ExpiryBin = expiry_to_binary(Expiry),
    case MsgOrId of
        MsgId when is_binary(MsgId) ->
            {<<MsgId/binary, ExpiryBin/binary, Size:?SIZE_BITS>>, <<>>};
        #basic_message{id = MsgId} ->
            MsgBin = term_to_binary(MsgOrId),
            {<<MsgId/binary, ExpiryBin/binary, Size:?SIZE_BITS>>, MsgBin}
    end.

expiry_to_binary(undefined) -> <<?NO_EXPIRY:?EXPIRY_BITS>>;
expiry_to_binary(Expiry)    -> <<Expiry:?EXPIRY_BITS>>.

parse_pub_record_body(<<MsgIdNum:?MSG_ID_BITS, Expiry:?EXPIRY_BITS,
                        Size:?SIZE_BITS>>, MsgBin) ->
    %% work around for binary data fragmentation. See
    %% rabbit_msg_file:read_next/2
    <<MsgId:?MSG_ID_BYTES/binary>> = <<MsgIdNum:?MSG_ID_BITS>>,
    Props = #message_properties{expiry = case Expiry of
                                             ?NO_EXPIRY -> undefined;
                                             X          -> X
                                         end,
                                size   = Size},
    case MsgBin of
        <<>> -> {MsgId, Props};
        _    -> Msg = #basic_message{id = MsgId} = binary_to_term(MsgBin),
                {Msg, Props}
    end.

%%----------------------------------------------------------------------------
%% journal manipulation
%%----------------------------------------------------------------------------

add_to_journal(SeqId, Action, State = #qistate { dirty_count = DCount,
                                                 segments = Segments,
                                                 dir = Dir }) ->
    {Seg, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    Segment = segment_find_or_new(Seg, Dir, Segments),
    Segment1 = add_to_journal(RelSeq, Action, Segment),
    State #qistate { dirty_count = DCount + 1,
                     segments = segment_store(Segment1, Segments) };

add_to_journal(RelSeq, Action,
               Segment = #segment { journal_entries = JEntries,
                                    entries_to_segment = EToSeg,
                                    unacked = UnackedCount }) ->

    {Fun, Entry} = action_to_entry(RelSeq, Action, JEntries),

    {JEntries1, EToSeg1} =
        case Fun of
            set ->
                {array:set(RelSeq, Entry, JEntries),
                 array:set(RelSeq, entry_to_segment(RelSeq, Entry, []),
                           EToSeg)};
            reset ->
                {array:reset(RelSeq, JEntries),
                 array:reset(RelSeq, EToSeg)}
        end,

    Segment #segment {
      journal_entries = JEntries1,
      entries_to_segment = EToSeg1,
      unacked = UnackedCount + case Action of
                                   ?PUB -> +1;
                                   del  ->  0;
                                   ack  -> -1
                               end}.

action_to_entry(RelSeq, Action, JEntries) ->
    case array:get(RelSeq, JEntries) of
        undefined ->
            {set,
             case Action of
                 ?PUB -> {Action, no_del, no_ack};
                 del  -> {no_pub,    del, no_ack};
                 ack  -> {no_pub, no_del,    ack}
             end};
        ({Pub,    no_del, no_ack}) when Action == del ->
            {set, {Pub,    del, no_ack}};
        ({no_pub,    _del, no_ack}) when Action == ack ->
            {set, {no_pub, del,    ack}};
        ({?PUB,      _del, no_ack}) when Action == ack ->
            {reset, none}
    end.

maybe_flush_journal(State) ->
    maybe_flush_journal(infinity, State).

maybe_flush_journal(Hint, State = #qistate { dirty_count = DCount,
                                             max_journal_entries = MaxJournal })
  when DCount > MaxJournal orelse (Hint =/= infinity andalso DCount > Hint) ->
    flush_journal(State);
maybe_flush_journal(_Hint, State) ->
    State.

flush_journal(State = #qistate { segments = Segments }) ->
    Segments1 =
        segment_fold(
          fun (#segment { unacked = 0, path = Path }, SegmentsN) ->
                  case rabbit_file:is_file(Path) of
                      true  -> ok = rabbit_file:delete(Path);
                      false -> ok
                  end,
                  SegmentsN;
              (#segment {} = Segment, SegmentsN) ->
                  segment_store(append_journal_to_segment(Segment), SegmentsN)
          end, segments_new(), Segments),
    {JournalHdl, State1} =
        get_journal_handle(State #qistate { segments = Segments1 }),
    ok = file_handle_cache:clear(JournalHdl),
    notify_sync(State1 #qistate { dirty_count = 0 }).

append_journal_to_segment(#segment { journal_entries = JEntries,
                                     entries_to_segment = EToSeg,
                                     path = Path } = Segment) ->
    case array:sparse_size(JEntries) of
        0 -> Segment;
        _ ->
            file_handle_cache_stats:update(queue_index_write),

            {ok, Hdl} = file_handle_cache:open_with_absolute_path(
                          Path, ?WRITE_MODE,
                          [{write_buffer, infinity}]),
            %% the file_handle_cache also does a list reverse, so this
            %% might not be required here, but before we were doing a
            %% sparse_foldr, a lists:reverse/1 seems to be the correct
            %% thing to do for now.
            file_handle_cache:append(Hdl, lists:reverse(array:to_list(EToSeg))),
            ok = file_handle_cache:close(Hdl),
            Segment #segment { journal_entries    = array_new(),
                               entries_to_segment = array_new([]) }
    end.

get_journal_handle(State = #qistate { journal_handle = undefined,
                                      dir = Dir,
                                      queue_name = Name }) ->
    Path = filename:join(Dir, ?JOURNAL_FILENAME),
    ok = rabbit_file:ensure_dir(Path),
    ok = ensure_queue_name_stub_file(Dir, Name),
    {ok, Hdl} = file_handle_cache:open_with_absolute_path(
                  Path, ?WRITE_MODE, [{write_buffer, infinity}]),
    {Hdl, State #qistate { journal_handle = Hdl }};
get_journal_handle(State = #qistate { journal_handle = Hdl }) ->
    {Hdl, State}.

%% Loading Journal. This isn't idempotent and will mess up the counts
%% if you call it more than once on the same state. Assumes the counts
%% are 0 to start with.
load_journal(State = #qistate { dir = Dir }) ->
    Path = filename:join(Dir, ?JOURNAL_FILENAME),
    case rabbit_file:is_file(Path) of
        true  -> {JournalHdl, State1} = get_journal_handle(State),
                 Size = rabbit_file:file_size(Path),
                 {ok, 0} = file_handle_cache:position(JournalHdl, 0),
                 {ok, JournalBin} = file_handle_cache:read(JournalHdl, Size),
                 parse_journal_entries(JournalBin, State1);
        false -> State
    end.

%% ditto
recover_journal(State) ->
    State1 = #qistate { segments = Segments } = load_journal(State),
    Segments1 =
        segment_map(
          fun (Segment = #segment { journal_entries = JEntries,
                                    entries_to_segment = EToSeg,
                                    unacked = UnackedCountInJournal }) ->
                  %% We want to keep ack'd entries in so that we can
                  %% remove them if duplicates are in the journal. The
                  %% counts here are purely from the segment itself.
                  {SegEntries, UnackedCountInSeg} = load_segment(true, Segment),
                  {JEntries1, EToSeg1, UnackedCountDuplicates} =
                      journal_minus_segment(JEntries, EToSeg, SegEntries),
                  Segment #segment { journal_entries = JEntries1,
                                     entries_to_segment = EToSeg1,
                                     unacked = (UnackedCountInJournal +
                                                    UnackedCountInSeg -
                                                    UnackedCountDuplicates) }
          end, Segments),
    State1 #qistate { segments = Segments1 }.

parse_journal_entries(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>, State) ->
    parse_journal_entries(Rest, add_to_journal(SeqId, del, State));

parse_journal_entries(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>, State) ->
    parse_journal_entries(Rest, add_to_journal(SeqId, ack, State));
parse_journal_entries(<<0:?JPREFIX_BITS, 0:?SEQ_BITS,
                        0:?PUB_RECORD_SIZE_BYTES/unit:8, _/binary>>, State) ->
    %% Journal entry composed only of zeroes was probably
    %% produced during a dirty shutdown so stop reading
    State;
parse_journal_entries(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Bin:?PUB_RECORD_BODY_BYTES/binary,
                        MsgSize:?EMBEDDED_SIZE_BITS, MsgBin:MsgSize/binary,
                        Rest/binary>>, State) ->
    IsPersistent = case Prefix of
                       ?PUB_PERSIST_JPREFIX -> true;
                       ?PUB_TRANS_JPREFIX   -> false
                   end,
    parse_journal_entries(
      Rest, add_to_journal(SeqId, {IsPersistent, Bin, MsgBin}, State));
parse_journal_entries(_ErrOrEoF, State) ->
    State.

deliver_or_ack(_Kind, [], State) ->
    State;
deliver_or_ack(Kind, SeqIds, State) ->
    JPrefix = case Kind of ack -> ?ACK_JPREFIX; del -> ?DEL_JPREFIX end,
    {JournalHdl, State1} = get_journal_handle(State),
    file_handle_cache_stats:update(queue_index_journal_write),
    ok = file_handle_cache:append(
           JournalHdl,
           [<<JPrefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>> || SeqId <- SeqIds]),
    maybe_flush_journal(lists:foldl(fun (SeqId, StateN) ->
                                            add_to_journal(SeqId, Kind, StateN)
                                    end, State1, SeqIds)).

notify_sync(State = #qistate{unconfirmed     = UC,
                             unconfirmed_msg = UCM,
                             on_sync         = OnSyncFun,
                             on_sync_msg     = OnSyncMsgFun}) ->
    State1 = case gb_sets:is_empty(UC) of
                 true  -> State;
                 false -> OnSyncFun(UC),
                          State#qistate{unconfirmed = gb_sets:new()}
             end,
    case gb_sets:is_empty(UCM) of
        true  -> State1;
        false -> OnSyncMsgFun(UCM),
                 State1#qistate{unconfirmed_msg = gb_sets:new()}
    end.

%%----------------------------------------------------------------------------
%% segment manipulation
%%----------------------------------------------------------------------------

seq_id_to_seg_and_rel_seq_id(SeqId) ->
    SegmentEntryCount = segment_entry_count(),
    { SeqId div SegmentEntryCount, SeqId rem SegmentEntryCount }.

reconstruct_seq_id(Seg, RelSeq) ->
    (Seg * segment_entry_count()) + RelSeq.

all_segment_nums(#qistate { dir = Dir, segments = Segments }) ->
    lists:sort(
      sets:to_list(
        lists:foldl(
          fun (SegName, Set) ->
                  sets:add_element(
                    list_to_integer(
                      lists:takewhile(fun (C) -> $0 =< C andalso C =< $9 end,
                                      SegName)), Set)
          end, sets:from_list(segment_nums(Segments)),
          rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir)))).

segment_find_or_new(Seg, Dir, Segments) ->
    case segment_find(Seg, Segments) of
        {ok, Segment} -> Segment;
        error         -> SegName = integer_to_list(Seg)  ++ ?SEGMENT_EXTENSION,
                         Path = filename:join(Dir, SegName),
                         #segment { num                = Seg,
                                    path               = Path,
                                    journal_entries    = array_new(),
                                    entries_to_segment = array_new([]),
                                    unacked            = 0 }
    end.

segment_find(Seg, {_Segments, [Segment = #segment { num = Seg } |_]}) ->
    {ok, Segment}; %% 1 or (2, matches head)
segment_find(Seg, {_Segments, [_, Segment = #segment { num = Seg }]}) ->
    {ok, Segment}; %% 2, matches tail
segment_find(Seg, {Segments, _}) -> %% no match
    maps:find(Seg, Segments).

segment_store(Segment = #segment { num = Seg }, %% 1 or (2, matches head)
              {Segments, [#segment { num = Seg } | Tail]}) ->
    {Segments, [Segment | Tail]};
segment_store(Segment = #segment { num = Seg }, %% 2, matches tail
              {Segments, [SegmentA, #segment { num = Seg }]}) ->
    {Segments, [Segment, SegmentA]};
segment_store(Segment = #segment { num = Seg }, {Segments, []}) ->
    {maps:remove(Seg, Segments), [Segment]};
segment_store(Segment = #segment { num = Seg }, {Segments, [SegmentA]}) ->
    {maps:remove(Seg, Segments), [Segment, SegmentA]};
segment_store(Segment = #segment { num = Seg },
              {Segments, [SegmentA, SegmentB]}) ->
    {maps:put(SegmentB#segment.num, SegmentB, maps:remove(Seg, Segments)),
     [Segment, SegmentA]}.

segment_fold(Fun, Acc, {Segments, CachedSegments}) ->
    maps:fold(fun (_Seg, Segment, Acc1) -> Fun(Segment, Acc1) end,
              lists:foldl(Fun, Acc, CachedSegments), Segments).

segment_map(Fun, {Segments, CachedSegments}) ->
    {maps:map(fun (_Seg, Segment) -> Fun(Segment) end, Segments),
     lists:map(Fun, CachedSegments)}.

segment_nums({Segments, CachedSegments}) ->
    lists:map(fun (#segment { num = Num }) -> Num end, CachedSegments) ++
        maps:keys(Segments).

segments_new() ->
    {#{}, []}.

entry_to_segment(_RelSeq, {?PUB, del, ack}, Initial) ->
    Initial;
entry_to_segment(RelSeq, {Pub, Del, Ack}, Initial) ->
    %% NB: we are assembling the segment in reverse order here, so
    %% del/ack comes first.
    Buf1 = case {Del, Ack} of
               {no_del, no_ack} ->
                   Initial;
               _ ->
                   Binary = <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                              RelSeq:?REL_SEQ_BITS>>,
                   case {Del, Ack} of
                       {del, ack} -> [[Binary, Binary] | Initial];
                       _          -> [Binary | Initial]
                   end
           end,
    case Pub of
        no_pub ->
            Buf1;
        {IsPersistent, Bin, MsgBin} ->
            [[<<?PUB_PREFIX:?PUB_PREFIX_BITS,
                (bool_to_int(IsPersistent)):1,
                RelSeq:?REL_SEQ_BITS, Bin/binary,
                (size(MsgBin)):?EMBEDDED_SIZE_BITS>>, MsgBin] | Buf1]
    end.

read_bounded_segment(Seg, {StartSeg, StartRelSeq}, {EndSeg, EndRelSeq},
                     {Messages, Segments}, Dir) ->
    Segment = segment_find_or_new(Seg, Dir, Segments),
    {segment_entries_foldr(
       fun (RelSeq, {{MsgOrId, MsgProps, IsPersistent}, IsDelivered, no_ack},
            Acc)
             when (Seg > StartSeg orelse StartRelSeq =< RelSeq) andalso
                  (Seg < EndSeg   orelse EndRelSeq   >= RelSeq) ->
               [{MsgOrId, reconstruct_seq_id(StartSeg, RelSeq), MsgProps,
                 IsPersistent, IsDelivered == del} | Acc];
           (_RelSeq, _Value, Acc) ->
               Acc
       end, Messages, Segment),
     segment_store(Segment, Segments)}.

segment_entries_foldr(Fun, Init,
                      Segment = #segment { journal_entries = JEntries }) ->
    {SegEntries, _UnackedCount} = load_segment(false, Segment),
    {SegEntries1, _UnackedCountD} = segment_plus_journal(SegEntries, JEntries),
    array:sparse_foldr(
      fun (RelSeq, {{IsPersistent, Bin, MsgBin}, Del, Ack}, Acc) ->
              {MsgOrId, MsgProps} = parse_pub_record_body(Bin, MsgBin),
              Fun(RelSeq, {{MsgOrId, MsgProps, IsPersistent}, Del, Ack}, Acc)
      end, Init, SegEntries1).

%% Loading segments
%%
%% Does not do any combining with the journal at all.
load_segment(KeepAcked, #segment { path = Path }) ->
    Empty = {array_new(), 0},
    case rabbit_file:is_file(Path) of
        false -> Empty;
        true  -> Size = rabbit_file:file_size(Path),
                 file_handle_cache_stats:update(queue_index_read),
                 {ok, Hdl} = file_handle_cache:open_with_absolute_path(
                               Path, ?READ_MODE, []),
                 {ok, 0} = file_handle_cache:position(Hdl, bof),
                 {ok, SegBin} = file_handle_cache:read(Hdl, Size),
                 ok = file_handle_cache:close(Hdl),
                 Res = parse_segment_entries(SegBin, KeepAcked, Empty),
                 Res
    end.

parse_segment_entries(<<?PUB_PREFIX:?PUB_PREFIX_BITS,
                        IsPersistNum:1, RelSeq:?REL_SEQ_BITS, Rest/binary>>,
                      KeepAcked, Acc) ->
    parse_segment_publish_entry(
      Rest, 1 == IsPersistNum, RelSeq, KeepAcked, Acc);
parse_segment_entries(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                       RelSeq:?REL_SEQ_BITS, Rest/binary>>, KeepAcked, Acc) ->
    parse_segment_entries(
      Rest, KeepAcked, add_segment_relseq_entry(KeepAcked, RelSeq, Acc));
parse_segment_entries(<<>>, _KeepAcked, Acc) ->
    Acc.

parse_segment_publish_entry(<<Bin:?PUB_RECORD_BODY_BYTES/binary,
                              MsgSize:?EMBEDDED_SIZE_BITS,
                              MsgBin:MsgSize/binary, Rest/binary>>,
                            IsPersistent, RelSeq, KeepAcked,
                            {SegEntries, Unacked}) ->
    Obj = {{IsPersistent, Bin, MsgBin}, no_del, no_ack},
    SegEntries1 = array:set(RelSeq, Obj, SegEntries),
    parse_segment_entries(Rest, KeepAcked, {SegEntries1, Unacked + 1});
parse_segment_publish_entry(Rest, _IsPersistent, _RelSeq, KeepAcked, Acc) ->
    parse_segment_entries(Rest, KeepAcked, Acc).

add_segment_relseq_entry(KeepAcked, RelSeq, {SegEntries, Unacked}) ->
    case array:get(RelSeq, SegEntries) of
        {Pub, no_del, no_ack} ->
            {array:set(RelSeq, {Pub, del, no_ack}, SegEntries), Unacked};
        {Pub, del, no_ack} when KeepAcked ->
            {array:set(RelSeq, {Pub, del, ack},    SegEntries), Unacked - 1};
        {_Pub, del, no_ack} ->
            {array:reset(RelSeq,                   SegEntries), Unacked - 1}
    end.

array_new() ->
    array_new(undefined).

array_new(Default) ->
    array:new([{default, Default}, fixed, {size, segment_entry_count()}]).

segment_entry_count() ->
    get(segment_entry_count).

bool_to_int(true ) -> 1;
bool_to_int(false) -> 0.

%%----------------------------------------------------------------------------
%% journal & segment combination
%%----------------------------------------------------------------------------

%% Combine what we have just read from a segment file with what we're
%% holding for that segment in memory. There must be no duplicates.
segment_plus_journal(SegEntries, JEntries) ->
    array:sparse_foldl(
      fun (RelSeq, JObj, {SegEntriesOut, AdditionalUnacked}) ->
              SegEntry = array:get(RelSeq, SegEntriesOut),
              {Obj, AdditionalUnackedDelta} =
                  segment_plus_journal1(SegEntry, JObj),
              {case Obj of
                   undefined -> array:reset(RelSeq, SegEntriesOut);
                   _         -> array:set(RelSeq, Obj, SegEntriesOut)
               end,
               AdditionalUnacked + AdditionalUnackedDelta}
      end, {SegEntries, 0}, JEntries).

%% Here, the result is a tuple with the first element containing the
%% item which we may be adding to (for items only in the journal),
%% modifying in (bits in both), or, when returning 'undefined',
%% erasing from (ack in journal, not segment) the segment array. The
%% other element of the tuple is the delta for AdditionalUnacked.
segment_plus_journal1(undefined, {?PUB, no_del, no_ack} = Obj) ->
    {Obj, 1};
segment_plus_journal1(undefined, {?PUB, del, no_ack} = Obj) ->
    {Obj, 1};
segment_plus_journal1(undefined, {?PUB, del, ack}) ->
    {undefined, 0};

segment_plus_journal1({?PUB = Pub, no_del, no_ack}, {no_pub, del, no_ack}) ->
    {{Pub, del, no_ack}, 0};
segment_plus_journal1({?PUB, no_del, no_ack},       {no_pub, del, ack}) ->
    {undefined, -1};
segment_plus_journal1({?PUB, del, no_ack},          {no_pub, no_del, ack}) ->
    {undefined, -1}.

%% Remove from the journal entries for a segment, items that are
%% duplicates of entries found in the segment itself. Used on start up
%% to clean up the journal.
%%
%% We need to update the entries_to_segment since they are just a
%% cache of what's on the journal.
journal_minus_segment(JEntries, EToSeg, SegEntries) ->
    array:sparse_foldl(
      fun (RelSeq, JObj, {JEntriesOut, EToSegOut, UnackedRemoved}) ->
              SegEntry = array:get(RelSeq, SegEntries),
              {Obj, UnackedRemovedDelta} =
                  journal_minus_segment1(JObj, SegEntry),
              {JEntriesOut1, EToSegOut1} =
                  case Obj of
                      keep      ->
                          {JEntriesOut, EToSegOut};
                      undefined ->
                          {array:reset(RelSeq, JEntriesOut),
                           array:reset(RelSeq, EToSegOut)};
                      _         ->
                          {array:set(RelSeq, Obj, JEntriesOut),
                           array:set(RelSeq, entry_to_segment(RelSeq, Obj, []),
                                     EToSegOut)}
                  end,
               {JEntriesOut1, EToSegOut1, UnackedRemoved + UnackedRemovedDelta}
      end, {JEntries, EToSeg, 0}, JEntries).

%% Here, the result is a tuple with the first element containing the
%% item we are adding to or modifying in the (initially fresh) journal
%% array. If the item is 'undefined' we leave the journal array
%% alone. The other element of the tuple is the deltas for
%% UnackedRemoved.

%% Both the same. Must be at least the publish
journal_minus_segment1({?PUB, _Del, no_ack} = Obj, Obj) ->
    {undefined, 1};
journal_minus_segment1({?PUB, _Del, ack} = Obj,    Obj) ->
    {undefined, 0};

%% Just publish in journal
journal_minus_segment1({?PUB, no_del, no_ack},     undefined) ->
    {keep, 0};

%% Publish and deliver in journal
journal_minus_segment1({?PUB, del, no_ack},        undefined) ->
    {keep, 0};
journal_minus_segment1({?PUB = Pub, del, no_ack},  {Pub, no_del, no_ack}) ->
    {{no_pub, del, no_ack}, 1};

%% Publish, deliver and ack in journal
journal_minus_segment1({?PUB, del, ack},           undefined) ->
    {keep, 0};
journal_minus_segment1({?PUB = Pub, del, ack},     {Pub, no_del, no_ack}) ->
    {{no_pub, del, ack}, 1};
journal_minus_segment1({?PUB = Pub, del, ack},     {Pub, del, no_ack}) ->
    {{no_pub, no_del, ack}, 1};

%% Just deliver in journal
journal_minus_segment1({no_pub, del, no_ack},      {?PUB, no_del, no_ack}) ->
    {keep, 0};
journal_minus_segment1({no_pub, del, no_ack},      {?PUB, del, no_ack}) ->
    {undefined, 0};

%% Just ack in journal
journal_minus_segment1({no_pub, no_del, ack},      {?PUB, del, no_ack}) ->
    {keep, 0};
journal_minus_segment1({no_pub, no_del, ack},      {?PUB, del, ack}) ->
    {undefined, -1};

%% Deliver and ack in journal
journal_minus_segment1({no_pub, del, ack},         {?PUB, no_del, no_ack}) ->
    {keep, 0};
journal_minus_segment1({no_pub, del, ack},         {?PUB, del, no_ack}) ->
    {{no_pub, no_del, ack}, 0};
journal_minus_segment1({no_pub, del, ack},         {?PUB, del, ack}) ->
    {undefined, -1};

%% Missing segment. If flush_journal/1 is interrupted after deleting
%% the segment but before truncating the journal we can get these
%% cases: a delivery and an acknowledgement in the journal, or just an
%% acknowledgement in the journal, but with no segment. In both cases
%% we have really forgotten the message; so ignore what's in the
%% journal.
journal_minus_segment1({no_pub, no_del, ack},      undefined) ->
    {undefined, 0};
journal_minus_segment1({no_pub, del, ack},         undefined) ->
    {undefined, 0}.

%%----------------------------------------------------------------------------
%% upgrade
%%----------------------------------------------------------------------------

-spec add_queue_ttl() -> 'ok'.

add_queue_ttl() ->
    foreach_queue_index({fun add_queue_ttl_journal/1,
                         fun add_queue_ttl_segment/1}).

add_queue_ttl_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
add_queue_ttl_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
add_queue_ttl_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        MsgId:?MSG_ID_BYTES/binary, Rest/binary>>) ->
    {[<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, MsgId,
      expiry_to_binary(undefined)], Rest};
add_queue_ttl_journal(_) ->
    stop.

add_queue_ttl_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                        RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BYTES/binary,
                        Rest/binary>>) ->
    {[<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS>>,
      MsgId, expiry_to_binary(undefined)], Rest};
add_queue_ttl_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                        RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
add_queue_ttl_segment(_) ->
    stop.

avoid_zeroes() ->
    foreach_queue_index({none, fun avoid_zeroes_segment/1}).

avoid_zeroes_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS,  IsPersistentNum:1,
                       RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                       Expiry:?EXPIRY_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS>>, Rest};
avoid_zeroes_segment(<<0:?REL_SEQ_ONLY_PREFIX_BITS,
                       RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
avoid_zeroes_segment(_) ->
    stop.

%% At upgrade time we just define every message's size as 0 - that
%% will save us a load of faff with the message store, and means we
%% can actually use the clean recovery terms in VQ. It does mean we
%% don't count message bodies from before the migration, but we can
%% live with that.
store_msg_size() ->
    foreach_queue_index({fun store_msg_size_journal/1,
                         fun store_msg_size_segment/1}).

store_msg_size_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_size_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_size_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                         MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS,
                         Rest/binary>>) ->
    {<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS, MsgId:?MSG_ID_BITS,
       Expiry:?EXPIRY_BITS, 0:?SIZE_BITS>>, Rest};
store_msg_size_journal(_) ->
    stop.

store_msg_size_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                         RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                         Expiry:?EXPIRY_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, 0:?SIZE_BITS>>, Rest};
store_msg_size_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                        RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
store_msg_size_segment(_) ->
    stop.

store_msg() ->
    foreach_queue_index({fun store_msg_journal/1,
                         fun store_msg_segment/1}).

store_msg_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
                    Rest/binary>>) ->
    {<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS, MsgId:?MSG_ID_BITS,
       Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
       0:?EMBEDDED_SIZE_BITS>>, Rest};
store_msg_journal(_) ->
    stop.

store_msg_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                    RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                    Expiry:?EXPIRY_BITS, Size:?SIZE_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
       0:?EMBEDDED_SIZE_BITS>>, Rest};
store_msg_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                    RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
store_msg_segment(_) ->
    stop.



%%----------------------------------------------------------------------------
%% Migration functions
%%----------------------------------------------------------------------------

foreach_queue_index(Funs) ->
    QueueDirNames = all_queue_directory_names(),
    {ok, Gatherer} = gatherer:start_link(),
    [begin
         ok = gatherer:fork(Gatherer),
         ok = worker_pool:submit_async(
                fun () ->
                        transform_queue(QueueDirName, Gatherer, Funs)
                end)
     end || QueueDirName <- QueueDirNames],
    empty = gatherer:out(Gatherer),
    ok = gatherer:stop(Gatherer).

transform_queue(Dir, Gatherer, {JournalFun, SegmentFun}) ->
    ok = transform_file(filename:join(Dir, ?JOURNAL_FILENAME), JournalFun),
    [ok = transform_file(filename:join(Dir, Seg), SegmentFun)
     || Seg <- rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir)],
    ok = gatherer:finish(Gatherer).

transform_file(_Path, none) ->
    ok;
transform_file(Path, Fun) when is_function(Fun)->
    PathTmp = Path ++ ".upgrade",
    case rabbit_file:file_size(Path) of
        0    -> ok;
        Size -> {ok, PathTmpHdl} =
                    file_handle_cache:open_with_absolute_path(
                      PathTmp, ?WRITE_MODE,
                      [{write_buffer, infinity}]),

                {ok, PathHdl} = file_handle_cache:open_with_absolute_path(
                                  Path, ?READ_MODE, [{read_buffer, Size}]),
                {ok, Content} = file_handle_cache:read(PathHdl, Size),
                ok = file_handle_cache:close(PathHdl),

                ok = drive_transform_fun(Fun, PathTmpHdl, Content),

                ok = file_handle_cache:close(PathTmpHdl),
                ok = rabbit_file:rename(PathTmp, Path)
    end.

drive_transform_fun(Fun, Hdl, Contents) ->
    case Fun(Contents) of
        stop                -> ok;
        {Output, Contents1} -> ok = file_handle_cache:append(Hdl, Output),
                               drive_transform_fun(Fun, Hdl, Contents1)
    end.

move_to_per_vhost_stores(#resource{virtual_host = VHost} = QueueName) ->
    OldQueueDir = filename:join([queues_base_dir(), "queues",
                                 queue_name_to_dir_name_legacy(QueueName)]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    NewQueueDir = queue_dir(VHostDir, QueueName),
    rabbit_log_upgrade:info("About to migrate queue directory '~s' to '~s'",
                            [OldQueueDir, NewQueueDir]),
    case rabbit_file:is_dir(OldQueueDir) of
        true  ->
            ok = rabbit_file:ensure_dir(NewQueueDir),
            ok = rabbit_file:rename(OldQueueDir, NewQueueDir),
            ok = ensure_queue_name_stub_file(NewQueueDir, QueueName);
        false ->
            Msg  = "Queue index directory '~s' not found for ~s",
            Args = [OldQueueDir, rabbit_misc:rs(QueueName)],
            rabbit_log_upgrade:error(Msg, Args),
            rabbit_log:error(Msg, Args)
    end,
    ok.

ensure_queue_name_stub_file(Dir, #resource{virtual_host = VHost, name = QName}) ->
    QueueNameFile = filename:join(Dir, ?QUEUE_NAME_STUB_FILE),
    file:write_file(QueueNameFile, <<"VHOST: ", VHost/binary, "\n",
                                     "QUEUE: ", QName/binary, "\n">>).

read_global_recovery_terms(DurableQueueNames) ->
    ok = rabbit_recovery_terms:open_global_table(),

    DurableTerms =
        lists:foldl(
          fun(QName, RecoveryTerms) ->
                  DirName = queue_name_to_dir_name_legacy(QName),
                  RecoveryInfo = case rabbit_recovery_terms:read_global(DirName) of
                                     {error, _}  -> non_clean_shutdown;
                                     {ok, Terms} -> Terms
                                 end,
                  [RecoveryInfo | RecoveryTerms]
          end, [], DurableQueueNames),

    ok = rabbit_recovery_terms:close_global_table(),
    %% The backing queue interface requires that the queue recovery terms
    %% which come back from start/1 are in the same order as DurableQueueNames
    OrderedTerms = lists:reverse(DurableTerms),
    {OrderedTerms, {fun queue_index_walker/1, {start, DurableQueueNames}}}.

cleanup_global_recovery_terms() ->
    rabbit_file:recursive_delete([filename:join([queues_base_dir(), "queues"])]),
    rabbit_recovery_terms:delete_global_table(),
    ok.


update_recovery_term(#resource{virtual_host = VHost} = QueueName, Term) ->
    Key = queue_name_to_dir_name(QueueName),
    rabbit_recovery_terms:store(VHost, Key, Term).
