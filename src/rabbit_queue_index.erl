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

-module(rabbit_queue_index).

-export([init/4, terminate/2, delete_and_terminate/1, publish/4,
         deliver/2, ack/2, sync/2, flush/1, read/3,
         next_segment_boundary/1, bounds/1, recover/1]).

-define(CLEAN_FILENAME, "clean.dot").

%%----------------------------------------------------------------------------

%% The queue index is responsible for recording the order of messages
%% within a queue on disk.
%%
%% Because of the fact that the queue can decide at any point to send
%% a queue entry to disk, you can not rely on publishes appearing in
%% order. The only thing you can rely on is a message being published,
%% then delivered, then ack'd.
%%
%% In order to be able to clean up ack'd messages, we write to segment
%% files. These files have a fixed maximum size: ?SEGMENT_ENTRY_COUNT
%% publishes, delivers and acknowledgements. They are numbered, and so
%% it is known that the 0th segment contains messages 0 ->
%% ?SEGMENT_ENTRY_COUNT - 1, the 1st segment contains messages
%% ?SEGMENT_ENTRY_COUNT -> 2*?SEGMENT_ENTRY_COUNT - 1 and so on. As
%% such, in the segment files, we only refer to message sequence ids
%% by the LSBs as SeqId rem ?SEGMENT_ENTRY_COUNT. This gives them a
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
%% journal is still held in this mapping. Also note that a dict is
%% used for this mapping, not an array because with an array, you will
%% always have entries from 0). Actions are stored directly in this
%% state. Thus at the point of flushing the journal, firstly no
%% reading from disk is necessary, but secondly if the known number of
%% acks and publishes in a segment are equal, given the known state of
%% the segment file combined with the journal, no writing needs to be
%% done to the segment file either (in fact it is deleted if it exists
%% at all). This is safe given that the set of acks is a subset of the
%% set of publishes. When it's necessary to sync messages because of
%% transactions, it's only necessary to fsync on the journal: when
%% entries are distributed from the journal to segment files, those
%% segments appended to are fsync'd prior to the journal being
%% truncated.
%%
%% This module is also responsible for scanning the queue index files
%% and seeding the message store on start up.
%%
%% Note that in general, the representation of a message's state as
%% the tuple: {('no_pub'|{Guid, IsPersistent}), ('del'|'no_del'),
%% ('ack'|'no_ack')} is richer than strictly necessary for most
%% operations. However, for startup, and to ensure the safe and
%% correct combination of journal entries with entries read from the
%% segment on disk, this richer representation vastly simplifies and
%% clarifies the code.
%%
%% For notes on Clean Shutdown and startup, see documentation in
%% variable_queue.
%%
%%----------------------------------------------------------------------------

%% ---- Journal details ----

-define(JOURNAL_FILENAME, "journal.jif").

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
%% other values is quite hairy and quite possibly noticably less
%% efficient, depending on how clever the compiler is when it comes to
%% binary generation/matching with constant vs variable lengths.

-define(REL_SEQ_BITS, 14).
-define(SEGMENT_ENTRY_COUNT, 16384). %% trunc(math:pow(2,?REL_SEQ_BITS))).

%% seq only is binary 00 followed by 14 bits of rel seq id
%% (range: 0 - 16383)
-define(REL_SEQ_ONLY_PREFIX, 00).
-define(REL_SEQ_ONLY_PREFIX_BITS, 2).
-define(REL_SEQ_ONLY_ENTRY_LENGTH_BYTES, 2).

%% publish record is binary 1 followed by a bit for is_persistent,
%% then 14 bits of rel seq id, and 128 bits of md5sum msg id
-define(PUBLISH_PREFIX, 1).
-define(PUBLISH_PREFIX_BITS, 1).

-define(GUID_BYTES, 16). %% md5sum is 128 bit or 16 bytes
-define(GUID_BITS, (?GUID_BYTES * 8)).
%% 16 bytes for md5sum + 2 for seq, bits and prefix
-define(PUBLISH_RECORD_LENGTH_BYTES, ?GUID_BYTES + 2).

%% 1 publish, 1 deliver, 1 ack per msg
-define(SEGMENT_TOTAL_SIZE, ?SEGMENT_ENTRY_COUNT *
        (?PUBLISH_RECORD_LENGTH_BYTES +
         (2 * ?REL_SEQ_ONLY_ENTRY_LENGTH_BYTES))).

%% ---- misc ----

-define(PUB, {_, _}). %% {Guid, IsPersistent}

-define(READ_MODE, [binary, raw, read, {read_ahead, ?SEGMENT_TOTAL_SIZE}]).

%%----------------------------------------------------------------------------

-record(qistate, { dir, segments, journal_handle, dirty_count,
                   max_journal_entries }).

-record(segment, { num, path, journal_entries, unacked }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(hdl() :: ('undefined' | any())).
-type(segment() :: ('undefined' |
                    #segment { num             :: non_neg_integer(),
                               path            :: file:filename(),
                               journal_entries :: array(),
                               unacked         :: non_neg_integer()
                              })).
-type(seq_id() :: integer()).
-type(seg_dict() :: {dict:dictionary(), [segment()]}).
-type(qistate() :: #qistate { dir                 :: file:filename(),
                              segments            :: 'undefined' | seg_dict(),
                              journal_handle      :: hdl(),
                              dirty_count         :: integer(),
                              max_journal_entries :: non_neg_integer()
                             }).
-type(startup_fun_state() ::
        {(fun ((A) -> 'finished' | {rabbit_guid:guid(), non_neg_integer(), A})),
         A}).

-spec(init/4 :: (rabbit_amqqueue:name(), boolean(), boolean(),
                 fun ((rabbit_guid:guid()) -> boolean())) ->
             {'undefined' | non_neg_integer(), [any()], qistate()}).
-spec(terminate/2 :: ([any()], qistate()) -> qistate()).
-spec(delete_and_terminate/1 :: (qistate()) -> qistate()).
-spec(publish/4 :: (rabbit_guid:guid(), seq_id(), boolean(), qistate()) ->
                        qistate()).
-spec(deliver/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(ack/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(sync/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(flush/1 :: (qistate()) -> qistate()).
-spec(read/3 :: (seq_id(), seq_id(), qistate()) ->
                     {[{rabbit_guid:guid(), seq_id(), boolean(), boolean()}],
                      qistate()}).
-spec(next_segment_boundary/1 :: (seq_id()) -> seq_id()).
-spec(bounds/1 :: (qistate()) ->
             {non_neg_integer(), non_neg_integer(), qistate()}).
-spec(recover/1 ::
        ([rabbit_amqqueue:name()]) -> {[[any()]], startup_fun_state()}).

-endif.


%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------

init(Name, Recover, MsgStoreRecovered, ContainsCheckFun) ->
    State = #qistate { dir = Dir } = blank_state(Name, not Recover),
    Terms = case read_shutdown_terms(Dir) of
                {error, _}   -> [];
                {ok, Terms1} -> Terms1
            end,
    CleanShutdown = detect_clean_shutdown(Dir),
    {Count, State1} =
        case CleanShutdown andalso MsgStoreRecovered of
            true  -> RecoveredCounts = proplists:get_value(segments, Terms, []),
                     init_clean(RecoveredCounts, State);
            false -> init_dirty(CleanShutdown, ContainsCheckFun, State)
        end,
    {Count, Terms, State1}.

terminate(Terms, State) ->
    {SegmentCounts, State1 = #qistate { dir = Dir }} = terminate(State),
    store_clean_shutdown([{segments, SegmentCounts} | Terms], Dir),
    State1.

delete_and_terminate(State) ->
    {_SegmentCounts, State1 = #qistate { dir = Dir }} = terminate(State),
    ok = rabbit_misc:recursive_delete([Dir]),
    State1.

publish(Guid, SeqId, IsPersistent, State) when is_binary(Guid) ->
    ?GUID_BYTES = size(Guid),
    {JournalHdl, State1} = get_journal_handle(State),
    ok = file_handle_cache:append(
           JournalHdl, [<<(case IsPersistent of
                               true  -> ?PUB_PERSIST_JPREFIX;
                               false -> ?PUB_TRANS_JPREFIX
                           end):?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Guid]),
    maybe_flush_journal(add_to_journal(SeqId, {Guid, IsPersistent}, State1)).

deliver(SeqIds, State) ->
    deliver_or_ack(del, SeqIds, State).

ack(SeqIds, State) ->
    deliver_or_ack(ack, SeqIds, State).

sync([], State) ->
    State;
sync(_SeqIds, State = #qistate { journal_handle = undefined }) ->
    State;
sync(_SeqIds, State = #qistate { journal_handle = JournalHdl }) ->
    %% The SeqIds here contains the SeqId of every publish and ack in
    %% the transaction. Ideally we should go through these seqids and
    %% only sync the journal if the pubs or acks appear in the
    %% journal. However, this would be complex to do, and given that
    %% the variable queue publishes and acks to the qi, and then
    %% syncs, all in one operation, there is no possibility of the
    %% seqids not being in the journal, provided the transaction isn't
    %% emptied (handled above anyway).
    ok = file_handle_cache:sync(JournalHdl),
    State.

flush(State = #qistate { dirty_count = 0 }) -> State;
flush(State)                                -> flush_journal(State).

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

next_segment_boundary(SeqId) ->
    {Seg, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    reconstruct_seq_id(Seg + 1, 0).

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

recover(DurableQueues) ->
    DurableDict = dict:from_list([ {queue_name_to_dir_name(Queue), Queue} ||
                                     Queue <- DurableQueues ]),
    QueuesDir = queues_dir(),
    Directories = case file:list_dir(QueuesDir) of
                      {ok, Entries}   -> [ Entry || Entry <- Entries,
                                                    filelib:is_dir(
                                                      filename:join(
                                                        QueuesDir, Entry)) ];
                      {error, enoent} -> []
                  end,
    DurableDirectories = sets:from_list(dict:fetch_keys(DurableDict)),
    {DurableQueueNames, DurableTerms} =
        lists:foldl(
          fun (QueueDir, {DurableAcc, TermsAcc}) ->
                  case sets:is_element(QueueDir, DurableDirectories) of
                      true ->
                          TermsAcc1 =
                              case read_shutdown_terms(
                                     filename:join(QueuesDir, QueueDir)) of
                                  {error, _}  -> TermsAcc;
                                  {ok, Terms} -> [Terms | TermsAcc]
                              end,
                          {[dict:fetch(QueueDir, DurableDict) | DurableAcc],
                           TermsAcc1};
                      false ->
                          Dir = filename:join(queues_dir(), QueueDir),
                          ok = rabbit_misc:recursive_delete([Dir]),
                          {DurableAcc, TermsAcc}
                  end
          end, {[], []}, Directories),
    {DurableTerms, {fun queue_index_walker/1, {start, DurableQueueNames}}}.

%%----------------------------------------------------------------------------
%% startup and shutdown
%%----------------------------------------------------------------------------

blank_state(QueueName, EnsureFresh) ->
    StrName = queue_name_to_dir_name(QueueName),
    Dir = filename:join(queues_dir(), StrName),
    ok = case EnsureFresh of
             true  -> false = filelib:is_file(Dir), %% is_file == is file or dir
                      ok;
             false -> ok
         end,
    ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
    {ok, MaxJournal} =
        application:get_env(rabbit, queue_index_max_journal_entries),
    #qistate { dir                 = Dir,
               segments            = segments_new(),
               journal_handle      = undefined,
               dirty_count         = 0,
               max_journal_entries = MaxJournal }.

detect_clean_shutdown(Dir) ->
    case file:delete(filename:join(Dir, ?CLEAN_FILENAME)) of
        ok              -> true;
        {error, enoent} -> false
    end.

read_shutdown_terms(Dir) ->
    rabbit_misc:read_term_file(filename:join(Dir, ?CLEAN_FILENAME)).

store_clean_shutdown(Terms, Dir) ->
    rabbit_misc:write_term_file(filename:join(Dir, ?CLEAN_FILENAME), Terms).

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
    {undefined, State1 # qistate { segments = Segments1 }}.

init_dirty(CleanShutdown, ContainsCheckFun, State) ->
    %% Recover the journal completely. This will also load segments
    %% which have entries in the journal and remove duplicates. The
    %% counts will correctly reflect the combination of the segment
    %% and the journal.
    State1 = #qistate { dir = Dir, segments = Segments } =
        recover_journal(State),
    {Segments1, Count} =
        %% Load each segment in turn and filter out messages that are
        %% not in the msg_store, by adding acks to the journal. These
        %% acks only go to the RAM journal as it doesn't matter if we
        %% lose them. Also mark delivered if not clean shutdown. Also
        %% find the number of unacked messages.
        lists:foldl(
          fun (Seg, {Segments2, CountAcc}) ->
                  Segment = #segment { unacked = UnackedCount } =
                      recover_segment(ContainsCheckFun, CleanShutdown,
                                      segment_find_or_new(Seg, Dir, Segments2)),
                  {segment_store(Segment, Segments2), CountAcc + UnackedCount}
          end, {Segments, 0}, all_segment_nums(State1)),
    %% Unconditionally flush since the dirty_count doesn't get updated
    %% by the above foldl.
    State2 = flush_journal(State1 #qistate { segments = Segments1 }),
    {Count, State2}.

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
                Segment = #segment { journal_entries = JEntries }) ->
    {SegEntries, UnackedCount} = load_segment(false, Segment),
    {SegEntries1, UnackedCountDelta} =
        segment_plus_journal(SegEntries, JEntries),
    array:sparse_foldl(
      fun (RelSeq, {{Guid, _IsPersistent}, Del, no_ack}, Segment1) ->
              recover_message(ContainsCheckFun(Guid), CleanShutdown,
                              Del, RelSeq, Segment1)
      end,
      Segment #segment { unacked = UnackedCount + UnackedCountDelta },
      SegEntries1).

recover_message( true,  true,   _Del, _RelSeq, Segment) ->
    Segment;
recover_message( true, false,    del, _RelSeq, Segment) ->
    Segment;
recover_message( true, false, no_del,  RelSeq, Segment) ->
    add_to_journal(RelSeq, del, Segment);
recover_message(false,     _,    del,  RelSeq, Segment) ->
    add_to_journal(RelSeq, ack, Segment);
recover_message(false,     _, no_del,  RelSeq, Segment) ->
    add_to_journal(RelSeq, ack, add_to_journal(RelSeq, del, Segment)).

queue_name_to_dir_name(Name = #resource { kind = queue }) ->
    <<Num:128>> = erlang:md5(term_to_binary(Name)),
    lists:flatten(io_lib:format("~.36B", [Num])).

queues_dir() ->
    filename:join(rabbit_mnesia:dir(), "queues").

%%----------------------------------------------------------------------------
%% msg store startup delta function
%%----------------------------------------------------------------------------

queue_index_walker({start, DurableQueues}) when is_list(DurableQueues) ->
    {ok, Gatherer} = gatherer:start_link(),
    [begin
         ok = gatherer:fork(Gatherer),
         ok = worker_pool:submit_async(
                fun () -> queue_index_walker_reader(QueueName, Gatherer)
                end)
     end || QueueName <- DurableQueues],
    queue_index_walker({next, Gatherer});

queue_index_walker({next, Gatherer}) when is_pid(Gatherer) ->
    case gatherer:out(Gatherer) of
        empty ->
            ok = gatherer:stop(Gatherer),
            ok = rabbit_misc:unlink_and_capture_exit(Gatherer),
            finished;
        {value, {Guid, Count}} ->
            {Guid, Count, {next, Gatherer}}
    end.

queue_index_walker_reader(QueueName, Gatherer) ->
    State = #qistate { segments = Segments, dir = Dir } =
        recover_journal(blank_state(QueueName, false)),
    [ok = segment_entries_foldr(
            fun (_RelSeq, {{Guid, true}, _IsDelivered, no_ack}, ok) ->
                    gatherer:in(Gatherer, {Guid, 1});
                (_RelSeq, _Value, Acc) ->
                    Acc
            end, ok, segment_find_or_new(Seg, Dir, Segments)) ||
        Seg <- all_segment_nums(State)],
    {_SegmentCounts, _State} = terminate(State),
    ok = gatherer:finish(Gatherer).

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
                                    unacked = UnackedCount }) ->
    Segment1 = Segment #segment {
                 journal_entries = add_to_journal(RelSeq, Action, JEntries) },
    case Action of
        del  -> Segment1;
        ack  -> Segment1 #segment { unacked = UnackedCount - 1 };
        ?PUB -> Segment1 #segment { unacked = UnackedCount + 1 }
    end;

add_to_journal(RelSeq, Action, JEntries) ->
    Val = case array:get(RelSeq, JEntries) of
              undefined ->
                  case Action of
                      ?PUB -> {Action, no_del, no_ack};
                      del  -> {no_pub,    del, no_ack};
                      ack  -> {no_pub, no_del,    ack}
                  end;
              ({Pub, no_del, no_ack}) when Action == del ->
                  {Pub, del, no_ack};
              ({Pub,    Del, no_ack}) when Action == ack ->
                  {Pub, Del,    ack}
          end,
    array:set(RelSeq, Val, JEntries).

maybe_flush_journal(State = #qistate { dirty_count = DCount,
                                       max_journal_entries = MaxJournal })
  when DCount > MaxJournal ->
    flush_journal(State);
maybe_flush_journal(State) ->
    State.

flush_journal(State = #qistate { segments = Segments }) ->
    Segments1 =
        segment_fold(
          fun (#segment { unacked = 0, path = Path }, SegmentsN) ->
                  case filelib:is_file(Path) of
                      true  -> ok = file:delete(Path);
                      false -> ok
                  end,
                  SegmentsN;
              (#segment {} = Segment, SegmentsN) ->
                  segment_store(append_journal_to_segment(Segment), SegmentsN)
          end, segments_new(), Segments),
    {JournalHdl, State1} =
        get_journal_handle(State #qistate { segments = Segments1 }),
    ok = file_handle_cache:clear(JournalHdl),
    State1 #qistate { dirty_count = 0 }.

append_journal_to_segment(#segment { journal_entries = JEntries,
                                     path = Path } = Segment) ->
    case array:sparse_size(JEntries) of
        0 -> Segment;
        _ -> {ok, Hdl} = file_handle_cache:open(Path, [write | ?READ_MODE],
                                                [{write_buffer, infinity}]),
             array:sparse_foldl(fun write_entry_to_segment/3, Hdl, JEntries),
             ok = file_handle_cache:close(Hdl),
             Segment #segment { journal_entries = array_new() }
    end.

get_journal_handle(State = #qistate { journal_handle = undefined,
                                      dir = Dir }) ->
    Path = filename:join(Dir, ?JOURNAL_FILENAME),
    {ok, Hdl} = file_handle_cache:open(Path, [write | ?READ_MODE],
                                       [{write_buffer, infinity}]),
    {Hdl, State #qistate { journal_handle = Hdl }};
get_journal_handle(State = #qistate { journal_handle = Hdl }) ->
    {Hdl, State}.

%% Loading Journal. This isn't idempotent and will mess up the counts
%% if you call it more than once on the same state. Assumes the counts
%% are 0 to start with.
load_journal(State) ->
    {JournalHdl, State1} = get_journal_handle(State),
    {ok, 0} = file_handle_cache:position(JournalHdl, 0),
    load_journal_entries(State1).

%% ditto
recover_journal(State) ->
    State1 = #qistate { segments = Segments } = load_journal(State),
    Segments1 =
        segment_map(
          fun (Segment = #segment { journal_entries = JEntries,
                                    unacked = UnackedCountInJournal }) ->
                  %% We want to keep ack'd entries in so that we can
                  %% remove them if duplicates are in the journal. The
                  %% counts here are purely from the segment itself.
                  {SegEntries, UnackedCountInSeg} = load_segment(true, Segment),
                  {JEntries1, UnackedCountDuplicates} =
                      journal_minus_segment(JEntries, SegEntries),
                  Segment #segment { journal_entries = JEntries1,
                                     unacked = (UnackedCountInJournal +
                                                UnackedCountInSeg -
                                                UnackedCountDuplicates) }
          end, Segments),
    State1 #qistate { segments = Segments1 }.

load_journal_entries(State = #qistate { journal_handle = Hdl }) ->
    case file_handle_cache:read(Hdl, ?SEQ_BYTES) of
        {ok, <<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>>} ->
            case Prefix of
                ?DEL_JPREFIX ->
                    load_journal_entries(add_to_journal(SeqId, del, State));
                ?ACK_JPREFIX ->
                    load_journal_entries(add_to_journal(SeqId, ack, State));
                _ ->
                    case file_handle_cache:read(Hdl, ?GUID_BYTES) of
                        {ok, <<GuidNum:?GUID_BITS>>} ->
                            %% work around for binary data
                            %% fragmentation. See
                            %% rabbit_msg_file:read_next/2
                            <<Guid:?GUID_BYTES/binary>> =
                                <<GuidNum:?GUID_BITS>>,
                            Publish = {Guid, case Prefix of
                                                 ?PUB_PERSIST_JPREFIX -> true;
                                                 ?PUB_TRANS_JPREFIX   -> false
                                             end},
                            load_journal_entries(
                              add_to_journal(SeqId, Publish, State));
                        _ErrOrEoF -> %% err, we've lost at least a publish
                            State
                    end
            end;
        _ErrOrEoF -> State
    end.

deliver_or_ack(_Kind, [], State) ->
    State;
deliver_or_ack(Kind, SeqIds, State) ->
    JPrefix = case Kind of ack -> ?ACK_JPREFIX; del -> ?DEL_JPREFIX end,
    {JournalHdl, State1} = get_journal_handle(State),
    ok = file_handle_cache:append(
           JournalHdl,
           [<<JPrefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>> || SeqId <- SeqIds]),
    maybe_flush_journal(lists:foldl(fun (SeqId, StateN) ->
                                            add_to_journal(SeqId, Kind, StateN)
                                    end, State1, SeqIds)).

%%----------------------------------------------------------------------------
%% segment manipulation
%%----------------------------------------------------------------------------

seq_id_to_seg_and_rel_seq_id(SeqId) ->
    { SeqId div ?SEGMENT_ENTRY_COUNT, SeqId rem ?SEGMENT_ENTRY_COUNT }.

reconstruct_seq_id(Seg, RelSeq) ->
    (Seg * ?SEGMENT_ENTRY_COUNT) + RelSeq.

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
          filelib:wildcard("*" ++ ?SEGMENT_EXTENSION, Dir)))).

segment_find_or_new(Seg, Dir, Segments) ->
    case segment_find(Seg, Segments) of
        {ok, Segment} -> Segment;
        error         -> SegName = integer_to_list(Seg)  ++ ?SEGMENT_EXTENSION,
                         Path = filename:join(Dir, SegName),
                         #segment { num             = Seg,
                                    path            = Path,
                                    journal_entries = array_new(),
                                    unacked         = 0 }
    end.

segment_find(Seg, {_Segments, [Segment = #segment { num = Seg } |_]}) ->
    {ok, Segment}; %% 1 or (2, matches head)
segment_find(Seg, {_Segments, [_, Segment = #segment { num = Seg }]}) ->
    {ok, Segment}; %% 2, matches tail
segment_find(Seg, {Segments, _}) -> %% no match
    dict:find(Seg, Segments).

segment_store(Segment = #segment { num = Seg }, %% 1 or (2, matches head)
              {Segments, [#segment { num = Seg } | Tail]}) ->
    {Segments, [Segment | Tail]};
segment_store(Segment = #segment { num = Seg }, %% 2, matches tail
              {Segments, [SegmentA, #segment { num = Seg }]}) ->
    {Segments, [Segment, SegmentA]};
segment_store(Segment = #segment { num = Seg }, {Segments, []}) ->
    {dict:erase(Seg, Segments), [Segment]};
segment_store(Segment = #segment { num = Seg }, {Segments, [SegmentA]}) ->
    {dict:erase(Seg, Segments), [Segment, SegmentA]};
segment_store(Segment = #segment { num = Seg },
              {Segments, [SegmentA, SegmentB]}) ->
    {dict:store(SegmentB#segment.num, SegmentB, dict:erase(Seg, Segments)),
     [Segment, SegmentA]}.

segment_fold(Fun, Acc, {Segments, CachedSegments}) ->
    dict:fold(fun (_Seg, Segment, Acc1) -> Fun(Segment, Acc1) end,
              lists:foldl(Fun, Acc, CachedSegments), Segments).

segment_map(Fun, {Segments, CachedSegments}) ->
    {dict:map(fun (_Seg, Segment) -> Fun(Segment) end, Segments),
     lists:map(Fun, CachedSegments)}.

segment_nums({Segments, CachedSegments}) ->
    lists:map(fun (#segment { num = Num }) -> Num end, CachedSegments) ++
        dict:fetch_keys(Segments).

segments_new() ->
    {dict:new(), []}.

write_entry_to_segment(_RelSeq, {?PUB, del, ack}, Hdl) ->
    Hdl;
write_entry_to_segment(RelSeq, {Pub, Del, Ack}, Hdl) ->
    ok = case Pub of
             no_pub ->
                 ok;
             {Guid, IsPersistent} ->
                 file_handle_cache:append(
                   Hdl, [<<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
                          (bool_to_int(IsPersistent)):1,
                          RelSeq:?REL_SEQ_BITS>>, Guid])
         end,
    ok = case {Del, Ack} of
             {no_del, no_ack} ->
                 ok;
             _ ->
                 Binary = <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                           RelSeq:?REL_SEQ_BITS>>,
                 file_handle_cache:append(
                   Hdl, case {Del, Ack} of
                            {del, ack} -> [Binary, Binary];
                            _          -> Binary
                        end)
         end,
    Hdl.

read_bounded_segment(Seg, {StartSeg, StartRelSeq}, {EndSeg, EndRelSeq},
                     {Messages, Segments}, Dir) ->
    Segment = segment_find_or_new(Seg, Dir, Segments),
    {segment_entries_foldr(
       fun (RelSeq, {{Guid, IsPersistent}, IsDelivered, no_ack}, Acc)
             when (Seg > StartSeg orelse StartRelSeq =< RelSeq) andalso
                  (Seg < EndSeg   orelse EndRelSeq   >= RelSeq) ->
               [ {Guid, reconstruct_seq_id(StartSeg, RelSeq),
                  IsPersistent, IsDelivered == del} | Acc ];
           (_RelSeq, _Value, Acc) ->
               Acc
       end, Messages, Segment),
     segment_store(Segment, Segments)}.

segment_entries_foldr(Fun, Init,
                      Segment = #segment { journal_entries = JEntries }) ->
    {SegEntries, _UnackedCount} = load_segment(false, Segment),
    {SegEntries1, _UnackedCountD} = segment_plus_journal(SegEntries, JEntries),
    array:sparse_foldr(Fun, Init, SegEntries1).

%% Loading segments
%%
%% Does not do any combining with the journal at all.
load_segment(KeepAcked, #segment { path = Path }) ->
    case filelib:is_file(Path) of
        false -> {array_new(), 0};
        true  -> {ok, Hdl} = file_handle_cache:open(Path, ?READ_MODE, []),
                 {ok, 0} = file_handle_cache:position(Hdl, bof),
                 Res = load_segment_entries(KeepAcked, Hdl, array_new(), 0),
                 ok = file_handle_cache:close(Hdl),
                 Res
    end.

load_segment_entries(KeepAcked, Hdl, SegEntries, UnackedCount) ->
    case file_handle_cache:read(Hdl, ?REL_SEQ_ONLY_ENTRY_LENGTH_BYTES) of
        {ok, <<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
              IsPersistentNum:1, RelSeq:?REL_SEQ_BITS>>} ->
            %% because we specify /binary, and binaries are complete
            %% bytes, the size spec is in bytes, not bits.
            {ok, Guid} = file_handle_cache:read(Hdl, ?GUID_BYTES),
            Obj = {{Guid, 1 == IsPersistentNum}, no_del, no_ack},
            SegEntries1 = array:set(RelSeq, Obj, SegEntries),
            load_segment_entries(KeepAcked, Hdl, SegEntries1,
                                 UnackedCount + 1);
        {ok, <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
              RelSeq:?REL_SEQ_BITS>>} ->
            {UnackedCountDelta, SegEntries1} =
                case array:get(RelSeq, SegEntries) of
                    {Pub, no_del, no_ack} ->
                        { 0, array:set(RelSeq, {Pub, del, no_ack}, SegEntries)};
                    {Pub, del, no_ack} when KeepAcked ->
                        {-1, array:set(RelSeq, {Pub, del, ack}, SegEntries)};
                    {_Pub, del, no_ack} ->
                        {-1, array:reset(RelSeq, SegEntries)}
                end,
            load_segment_entries(KeepAcked, Hdl, SegEntries1,
                                 UnackedCount + UnackedCountDelta);
        _ErrOrEoF ->
            {SegEntries, UnackedCount}
    end.

array_new() ->
    array:new([{default, undefined}, fixed, {size, ?SEGMENT_ENTRY_COUNT}]).

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
journal_minus_segment(JEntries, SegEntries) ->
    array:sparse_foldl(
      fun (RelSeq, JObj, {JEntriesOut, UnackedRemoved}) ->
              SegEntry = array:get(RelSeq, SegEntries),
              {Obj, UnackedRemovedDelta} =
                  journal_minus_segment1(JObj, SegEntry),
              {case Obj of
                   keep      -> JEntriesOut;
                   undefined -> array:reset(RelSeq, JEntriesOut);
                   _         -> array:set(RelSeq, Obj, JEntriesOut)
               end,
               UnackedRemoved + UnackedRemovedDelta}
      end, {JEntries, 0}, JEntries).

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
    {undefined, -1}.
