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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_queue_index).

-export([init/1, terminate/1, terminate_and_erase/1, write_published/4,
         write_delivered/2, write_acks/2, sync_seq_ids/2, flush_journal/1,
         read_segment_entries/2, next_segment_boundary/1, segment_size/0,
         find_lowest_seq_id_seg_and_next_seq_id/1, start_msg_store/1]).

-define(CLEAN_FILENAME, "clean.dot").

%%----------------------------------------------------------------------------
%% ---- Journal details ----

-define(MAX_JOURNAL_ENTRY_COUNT, 262144).
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

-define(REL_SEQ_BITS, 14).
-define(REL_SEQ_BITS_BYTE_ALIGNED, (?REL_SEQ_BITS + 8 - (?REL_SEQ_BITS rem 8))).
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

-define(MSG_ID_BYTES, 16). %% md5sum is 128 bit or 16 bytes
-define(MSG_ID_BITS, (?MSG_ID_BYTES * 8)).
%% 16 bytes for md5sum + 2 for seq, bits and prefix
-define(PUBLISH_RECORD_LENGTH_BYTES, ?MSG_ID_BYTES + 2).

%% 1 publish, 1 deliver, 1 ack per msg
-define(SEGMENT_TOTAL_SIZE, ?SEGMENT_ENTRY_COUNT *
        (?PUBLISH_RECORD_LENGTH_BYTES +
         (2 * ?REL_SEQ_ONLY_ENTRY_LENGTH_BYTES))).

%%----------------------------------------------------------------------------

-record(qistate,
        { dir,
          segments,
          journal_handle,
          dirty_count
        }).

-record(segment,
        { pubs,
          acks,
          handle,
          journal_entries,
          path,
          num
        }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(hdl() :: ('undefined' | any())).
-type(segment() :: ('undefined' |
                    #segment { pubs            :: non_neg_integer(),
                               acks            :: non_neg_integer(),
                               handle          :: hdl(),
                               journal_entries :: array(),
                               path            :: file_path(),
                               num             :: non_neg_integer()
                             })).
-type(msg_id() :: binary()).
-type(seq_id() :: integer()).
-type(seg_dict() :: {dict(), [segment()]}).
-type(qistate() :: #qistate { dir             :: file_path(),
                              segments        :: seg_dict(),
                              journal_handle  :: hdl(),
                              dirty_count     :: integer()
                            }).

-spec(init/1 :: (queue_name()) -> {non_neg_integer(), qistate()}).
-spec(terminate/1 :: (qistate()) -> qistate()).
-spec(terminate_and_erase/1 :: (qistate()) -> qistate()).
-spec(write_published/4 :: (msg_id(), seq_id(), boolean(), qistate())
      -> qistate()).
-spec(write_delivered/2 :: (seq_id(), qistate()) -> qistate()).
-spec(write_acks/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(sync_seq_ids/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(flush_journal/1 :: (qistate()) -> qistate()).
-spec(read_segment_entries/2 :: (seq_id(), qistate()) ->
             {[{msg_id(), seq_id(), boolean(), boolean()}], qistate()}).
-spec(next_segment_boundary/1 :: (seq_id()) -> seq_id()).
-spec(segment_size/0 :: () -> non_neg_integer()).
-spec(find_lowest_seq_id_seg_and_next_seq_id/1 :: (qistate()) ->
             {non_neg_integer(), non_neg_integer(), qistate()}).
-spec(start_msg_store/1 :: ([amqqueue()]) -> 'ok').

-endif.


%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

init(Name) ->
    State = blank_state(Name),
    %% 1. Load the journal completely. This will also load segments
    %%    which have entries in the journal and remove duplicates.
    %%    The counts will correctly reflect the combination of the
    %%    segment and the journal.
    State1 = load_journal(State),
    %% 2. Flush the journal. This makes life easier for everyone, as
    %%    it means there won't be any publishes in the journal alone.
    State2 = #qistate { dir = Dir, segments = Segments,
                        dirty_count = DCount } = flush_journal(State1),
    %% 3. Load each segment in turn and filter out messages that are
    %%    not in the msg_store, by adding acks to the journal. These
    %%    acks only go to the RAM journal as it doesn't matter if we
    %%    lose them. Also mark delivered if not clean shutdown. Also
    %%    find the number of unacked messages.
    AllSegs = all_segment_nums(State2),
    CleanShutdown = detect_clean_shutdown(Dir),
    %% We know the journal is empty here, so we don't need to combine
    %% with the journal, and we don't need to worry about messages
    %% that have been acked.
    {Segments1, Count, DCount1} =
        lists:foldl(
          fun (Seg, {Segments2, CountAcc, DCountAcc}) ->
                  Segment = segment_find_or_new(Seg, Dir, Segments2),
                  {SegEntries, _PubCount, _AckCount, Segment1} =
                      load_segment(false, Segment),
                  {Segment2 = #segment { pubs = PubCount, acks = AckCount },
                   DCountAcc1} =
                      array:sparse_foldl(
                        fun (RelSeq, {{MsgId, _IsPersistent}, Del, no_ack},
                             {Segment3, DCountAcc2}) ->
                                InMsgStore = rabbit_msg_store:contains(MsgId),
                                case {InMsgStore, CleanShutdown} of
                                    {true, true} ->
                                        {Segment3, DCountAcc};
                                    {true, false} when Del == del ->
                                        {Segment3, DCountAcc};
                                    {true, false} ->
                                        {add_to_journal(RelSeq, del, Segment3),
                                         DCountAcc2 + 1};
                                    {false, _} when Del == del ->
                                        {add_to_journal(RelSeq, ack, Segment3),
                                         DCountAcc2 + 1};
                                    {false, _} ->
                                        {add_to_journal(
                                           RelSeq, ack,
                                           add_to_journal(
                                             RelSeq, del, Segment3)),
                                         DCountAcc2 + 2}
                                end
                        end, {Segment1, DCountAcc}, SegEntries),
                  {segment_store(Segment2, Segments2),
                   CountAcc + PubCount - AckCount, DCountAcc1}
          end, {Segments, 0, DCount}, AllSegs),
    {Count, State2 #qistate { segments = Segments1, dirty_count = DCount1 }}.

terminate(State) ->
    terminate(true, State).

terminate_and_erase(State) ->
    State1 = terminate(State),
    ok = delete_queue_directory(State1 #qistate.dir),
    State1.

write_published(MsgId, SeqId, IsPersistent, State)
  when is_binary(MsgId) ->
    ?MSG_ID_BYTES = size(MsgId),
    {JournalHdl, State1} = get_journal_handle(State),
    ok = file_handle_cache:append(JournalHdl,
                                  [<<(case IsPersistent of
                                          true  -> ?PUB_PERSIST_JPREFIX;
                                          false -> ?PUB_TRANS_JPREFIX
                                      end):?JPREFIX_BITS, SeqId:?SEQ_BITS>>,
                                   MsgId]),
    maybe_flush_journal(add_to_journal(SeqId, {MsgId, IsPersistent}, State1)).

write_delivered(SeqId, State) ->
    {JournalHdl, State1} = get_journal_handle(State),
    ok = file_handle_cache:append(JournalHdl,
                                  <<?DEL_JPREFIX:?JPREFIX_BITS,
                                   SeqId:?SEQ_BITS>>),
    maybe_flush_journal(add_to_journal(SeqId, del, State1)).

write_acks(SeqIds, State) ->
    {JournalHdl, State1} = get_journal_handle(State),
    ok = file_handle_cache:append(JournalHdl,
                                  [<<?ACK_JPREFIX:?JPREFIX_BITS,
                                    SeqId:?SEQ_BITS>> || SeqId <- SeqIds]),
    State2 = lists:foldl(fun (SeqId, StateN) ->
                                 add_to_journal(SeqId, ack, StateN)
                         end, State1, SeqIds),
    maybe_flush_journal(State2).

sync_seq_ids(_SeqIds, State = #qistate { journal_handle = undefined }) ->
    State;
sync_seq_ids(_SeqIds, State = #qistate { journal_handle = JournalHdl }) ->
    ok = file_handle_cache:sync(JournalHdl),
    State.

flush_journal(State = #qistate { dirty_count = 0 }) ->
    State;
flush_journal(State = #qistate { segments = Segments }) ->
    Segments1 =
        segment_fold(
          fun (_Seg, #segment { journal_entries = JEntries, pubs = PubCount,
                                acks = AckCount } = Segment, SegmentsN) ->
                  case PubCount > 0 andalso PubCount == AckCount of
                      true ->
                          ok = delete_segment(Segment),
                          SegmentsN;
                      false ->
                          Segment1 =
                              case array:sparse_size(JEntries) of
                                  0 -> Segment;
                                  _ -> {Hdl, Segment2} =
                                           get_segment_handle(Segment),
                                       array:sparse_foldl(
                                         fun write_entry_to_segment/3, Hdl,
                                         JEntries),
                                       ok = file_handle_cache:sync(Hdl),
                                       Segment2 #segment { journal_entries =
                                                           array_new() }
                              end,
                          segment_store(Segment1, SegmentsN)
                  end
          end, segments_new(), Segments),
    {JournalHdl, State1} =
        get_journal_handle(State #qistate { segments = Segments1 }),
    ok = file_handle_cache:clear(JournalHdl),
    State1 #qistate { dirty_count = 0 }.

read_segment_entries(InitSeqId, State = #qistate { segments = Segments,
                                                   dir = Dir }) ->
    {Seg, 0} = seq_id_to_seg_and_rel_seq_id(InitSeqId),
    Segment = segment_find_or_new(Seg, Dir, Segments),
    {SegEntries, _PubCount, _AckCount,
     Segment1 = #segment { journal_entries = JEntries }} =
        load_segment(false, Segment),
    SegEntries1 = journal_plus_segment(JEntries, SegEntries),
    %% deliberately sort the list desc, because foldl will reverse it
    {array:sparse_foldr(
       fun (RelSeq, {{MsgId, IsPersistent}, IsDelivered, no_ack}, Acc) ->
               [ {MsgId, reconstruct_seq_id(Seg, RelSeq),
                  IsPersistent, IsDelivered == del} | Acc ]
       end, [], SegEntries1),
     State #qistate { segments = segment_store(Segment1, Segments) }}.

next_segment_boundary(SeqId) ->
    {Seg, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    reconstruct_seq_id(Seg + 1, 0).

segment_size() ->
    ?SEGMENT_ENTRY_COUNT.

find_lowest_seq_id_seg_and_next_seq_id(State) ->
    SegNums = all_segment_nums(State),
    %% We don't want the lowest seq_id, merely the seq_id of the start
    %% of the lowest segment. That seq_id may not actually exist, but
    %% that's fine. The important thing is that the segment exists and
    %% the seq_id reported is on a segment boundary.

    %% We also don't really care about the max seq_id. Just start the
    %% next segment: it makes life much easier.

    %% SegNums is sorted, ascending.
    {LowSeqIdSeg, NextSeqId} =
        case SegNums of
            []         -> {0, 0};
            [MinSeg|_] -> {reconstruct_seq_id(MinSeg, 0),
                           reconstruct_seq_id(1 + lists:last(SegNums), 0)}
        end,
    {LowSeqIdSeg, NextSeqId, State}.

start_msg_store(DurableQueues) ->
    DurableDict =
        dict:from_list([ {queue_name_to_dir_name(Queue #amqqueue.name),
                          Queue #amqqueue.name} || Queue <- DurableQueues ]),
    QueuesDir = queues_dir(),
    Directories = case file:list_dir(QueuesDir) of
                      {ok, Entries} ->
                          [ Entry || Entry <- Entries,
                                     filelib:is_dir(
                                       filename:join(QueuesDir, Entry)) ];
                      {error, enoent} ->
                          []
                  end,
    DurableDirectories = sets:from_list(dict:fetch_keys(DurableDict)),
    {DurableQueueNames, TransientDirs} =
        lists:foldl(
          fun (QueueDir, {DurableAcc, TransientAcc}) ->
                  case sets:is_element(QueueDir, DurableDirectories) of
                      true ->
                          {[dict:fetch(QueueDir, DurableDict) | DurableAcc],
                           TransientAcc};
                      false ->
                          {DurableAcc, [QueueDir | TransientAcc]}
                  end
          end, {[], []}, Directories),
    MsgStoreDir = filename:join(rabbit_mnesia:dir(), "msg_store"),
    ok = rabbit:start_child(rabbit_msg_store, [MsgStoreDir,
                                               fun queue_index_walker/1,
                                               DurableQueueNames]),
    lists:foreach(fun (DirName) ->
                          Dir = filename:join(queues_dir(), DirName),
                          ok = delete_queue_directory(Dir)
                  end, TransientDirs),
    ok.

%%----------------------------------------------------------------------------
%% Msg Store Startup Delta Function
%%----------------------------------------------------------------------------

queue_index_walker([]) ->
    finished;
queue_index_walker([QueueName|QueueNames]) ->
    State = blank_state(QueueName),
    State1 = load_journal(State),
    SegNums = all_segment_nums(State1),
    queue_index_walker({SegNums, State1, QueueNames});

queue_index_walker({[], State, QueueNames}) ->
    _State = terminate(false, State),
    queue_index_walker(QueueNames);
queue_index_walker({[Seg | SegNums], State, QueueNames}) ->
    SeqId = reconstruct_seq_id(Seg, 0),
    {Messages, State1} = read_segment_entries(SeqId, State),
    queue_index_walker({Messages, State1, SegNums, QueueNames});

queue_index_walker({[], State, SegNums, QueueNames}) ->
    queue_index_walker({SegNums, State, QueueNames});
queue_index_walker({[{MsgId, _SeqId, IsPersistent, _IsDelivered} | Msgs],
                    State, SegNums, QueueNames}) ->
    case IsPersistent of
        true  -> {MsgId, 1, {Msgs, State, SegNums, QueueNames}};
        false -> queue_index_walker({Msgs, State, SegNums, QueueNames})
    end.

%%----------------------------------------------------------------------------
%% Minors
%%----------------------------------------------------------------------------

maybe_flush_journal(State = #qistate { dirty_count = DCount })
  when DCount > ?MAX_JOURNAL_ENTRY_COUNT ->
    flush_journal(State);
maybe_flush_journal(State) ->
    State.

all_segment_nums(#qistate { dir = Dir, segments = Segments }) ->
    sets:to_list(
      lists:foldl(
        fun (SegName, Set) ->
                sets:add_element(
                  list_to_integer(
                    lists:takewhile(fun(C) -> $0 =< C andalso C =< $9 end,
                                    SegName)), Set)
        end, sets:from_list(segment_fetch_keys(Segments)),
        filelib:wildcard("*" ++ ?SEGMENT_EXTENSION, Dir))).

blank_state(QueueName) ->
    StrName = queue_name_to_dir_name(QueueName),
    Dir = filename:join(queues_dir(), StrName),
    ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
    #qistate { dir            = Dir,
               segments       = segments_new(),
               journal_handle = undefined,
               dirty_count    = 0
             }.

array_new() ->
    array:new([{default, undefined}, fixed, {size, ?SEGMENT_ENTRY_COUNT}]).

seq_id_to_seg_and_rel_seq_id(SeqId) ->
    { SeqId div ?SEGMENT_ENTRY_COUNT, SeqId rem ?SEGMENT_ENTRY_COUNT }.

reconstruct_seq_id(Seg, RelSeq) ->
    (Seg * ?SEGMENT_ENTRY_COUNT) + RelSeq.

seg_num_to_path(Dir, Seg) ->
    SegName = integer_to_list(Seg),
    filename:join(Dir, SegName ++ ?SEGMENT_EXTENSION).

delete_segment(#segment { handle = undefined }) ->
    ok;
delete_segment(#segment { handle = Hdl }) ->
    ok = file_handle_cache:delete(Hdl).

detect_clean_shutdown(Dir) ->
    case file:delete(filename:join(Dir, ?CLEAN_FILENAME)) of
        ok              -> true;
        {error, enoent} -> false
    end.

store_clean_shutdown(Dir) ->
    {ok, Hdl} = file_handle_cache:open(filename:join(Dir, ?CLEAN_FILENAME),
                                       [write, raw, binary],
                                       [{write_buffer, unbuffered}]),
    ok = file_handle_cache:close(Hdl).

queue_name_to_dir_name(Name = #resource { kind = queue }) ->
    Bin = term_to_binary(Name),
    Size = 8*size(Bin),
    <<Num:Size>> = Bin,
    lists:flatten(io_lib:format("~.36B", [Num])).

queues_dir() ->
    filename:join(rabbit_mnesia:dir(), "queues").

delete_queue_directory(Dir) ->
    {ok, Entries} = file:list_dir(Dir),
    ok = lists:foldl(fun (Entry, ok) ->
                             file:delete(filename:join(Dir, Entry))
                     end, ok, Entries),
    ok = file:del_dir(Dir).

get_segment_handle(Segment = #segment { handle = undefined, path = Path }) ->
    {ok, Hdl} = file_handle_cache:open(Path,
                                       [binary, raw, read, write,
                                        {read_ahead, ?SEGMENT_TOTAL_SIZE}],
                                       [{write_buffer, infinity}]),
    {Hdl, Segment #segment { handle = Hdl }};
get_segment_handle(Segment = #segment { handle = Hdl }) ->
    {Hdl, Segment}.

segment_find(Seg, {_Segments, [Segment = #segment { num = Seg } |_]}) ->
    {ok, Segment}; %% 1 or (2, matches head)
segment_find(Seg, {_Segments, [_, Segment = #segment { num = Seg }]}) ->
    {ok, Segment}; %% 2, matches tail
segment_find(Seg, {Segments, _}) -> %% no match
    dict:find(Seg, Segments).

segment_new(Seg, Dir) ->
    #segment { pubs = 0,
               acks = 0,
               handle = undefined,
               journal_entries = array_new(),
               path = seg_num_to_path(Dir, Seg),
               num = Seg
             }.

segment_find_or_new(Seg, Dir, Segments) ->
    case segment_find(Seg, Segments) of
        error -> segment_new(Seg, Dir);
        {ok, Segment} -> Segment
    end.

segment_store(Segment = #segment { num = Seg }, %% 1 or (2, matches head)
              {Segments, [#segment { num = Seg } | Tail]}) ->
    {Segments, [Segment | Tail]};
segment_store(Segment = #segment { num = Seg }, %% 2, matches tail
              {Segments, [SegmentA, #segment { num = Seg }]}) ->
    {Segments, [SegmentA, Segment]};
segment_store(Segment = #segment { num = Seg }, {Segments, []}) ->
    {dict:erase(Seg, Segments), [Segment]};
segment_store(Segment = #segment { num = Seg }, {Segments, [SegmentA]}) ->
    {dict:erase(Seg, Segments), [Segment, SegmentA]};
segment_store(Segment = #segment { num = Seg },
              {Segments, [SegmentA, SegmentB]}) ->
    {dict:store(SegmentB#segment.num, SegmentB, dict:erase(Seg, Segments)),
     [Segment, SegmentA]}.

segment_fold(Fun, Acc, {Segments, []}) ->
    dict:fold(Fun, Acc, Segments);
segment_fold(Fun, Acc, {Segments, CachedSegments}) ->
    Acc1 = lists:foldl(fun (Segment = #segment { num = Num }, AccN) ->
                               Fun(Num, Segment, AccN)
                       end, Acc, CachedSegments),
    dict:fold(Fun, Acc1, Segments).

segment_map(Fun, {Segments, CachedSegments}) ->
    {dict:map(Fun, Segments),
     lists:map(fun (Segment = #segment { num = Num }) -> Fun(Num, Segment) end,
               CachedSegments)}.

segment_fetch_keys({Segments, CachedSegments}) ->
    lists:map(fun (Segment) -> Segment#segment.num end, CachedSegments) ++
        dict:fetch_keys(Segments).

segments_new() ->
    {dict:new(), []}.

get_journal_handle(State =
                   #qistate { journal_handle = undefined, dir = Dir }) ->
    Path = filename:join(Dir, ?JOURNAL_FILENAME),
    {ok, Hdl} = file_handle_cache:open(Path,
                                       [binary, raw, read, write,
                                        {read_ahead, ?SEGMENT_TOTAL_SIZE}],
                                       [{write_buffer, infinity}]),
    {Hdl, State #qistate { journal_handle = Hdl }};
get_journal_handle(State = #qistate { journal_handle = Hdl }) ->
    {Hdl, State}.

bool_to_int(true ) -> 1;
bool_to_int(false) -> 0.

write_entry_to_segment(_RelSeq, {{_MsgId, _IsPersistent}, del, ack}, Hdl) ->
    Hdl;
write_entry_to_segment(RelSeq, {Publish, Del, Ack}, Hdl) ->
    ok = case Publish of
             no_pub ->
                 ok;
             {MsgId, IsPersistent} ->
                 file_handle_cache:append(
                   Hdl, [<<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
                           (bool_to_int(IsPersistent)):1,
                           RelSeq:?REL_SEQ_BITS>>, MsgId])
         end,
    ok = case {Del, Ack} of
             {no_del, no_ack} -> ok;
             _ -> Binary = <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                             RelSeq:?REL_SEQ_BITS>>,
                  Data = case {Del, Ack} of
                             {del, ack} -> [Binary, Binary];
                             _          -> Binary
                         end,
                  file_handle_cache:append(Hdl, Data)
         end,
    Hdl.

terminate(StoreShutdown, State =
          #qistate { journal_handle = JournalHdl,
                     dir = Dir, segments = Segments }) ->
    ok = case JournalHdl of
             undefined -> ok;
             _         -> file_handle_cache:close(JournalHdl)
         end,
    ok = segment_fold(
           fun (_Seg, #segment { handle = undefined }, ok) ->
                   ok;
               (_Seg, #segment { handle = Hdl }, ok) ->
                   file_handle_cache:close(Hdl)
           end, ok, Segments),
    case StoreShutdown of
        true  -> store_clean_shutdown(Dir);
        false -> ok
    end,
    State #qistate { journal_handle = undefined, segments = segments_new() }.

%%----------------------------------------------------------------------------
%% Majors
%%----------------------------------------------------------------------------

%% Loading segments

%% Does not do any combining with the journal at all. The PubCount
%% that comes back is the number of publishes in the segment. The
%% number of unacked msgs is PubCount - AckCount. If KeepAcks is
%% false, then array:sparse_size(SegEntries) == PubCount -
%% AckCount. If KeepAcks is true, then array:sparse_size(SegEntries)
%% == PubCount.
load_segment(KeepAcks,
             Segment = #segment { path = Path, handle = SegHdl }) ->
    SegmentExists = case SegHdl of
                        undefined -> filelib:is_file(Path);
                        _         -> true
                    end,
    case SegmentExists of
        false ->
            {array_new(), 0, 0, Segment};
        true ->
            {Hdl, Segment1} = get_segment_handle(Segment),
            {ok, 0} = file_handle_cache:position(Hdl, bof),
            {SegEntries, PubCount, AckCount} =
                load_segment_entries(KeepAcks, Hdl, array_new(), 0, 0),
            {SegEntries, PubCount, AckCount, Segment1}
    end.

load_segment_entries(KeepAcks, Hdl, SegEntries, PubCount, AckCount) ->
    case file_handle_cache:read(Hdl, 1) of
        {ok, <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                MSB:(8-?REL_SEQ_ONLY_PREFIX_BITS)>>} ->
            {ok, LSB} = file_handle_cache:read(
                          Hdl, ?REL_SEQ_ONLY_ENTRY_LENGTH_BYTES - 1),
            <<RelSeq:?REL_SEQ_BITS_BYTE_ALIGNED>> = <<MSB, LSB/binary>>,
            {AckCount1, SegEntries1} =
                deliver_or_ack_msg(KeepAcks, RelSeq, AckCount, SegEntries),
            load_segment_entries(KeepAcks, Hdl, SegEntries1, PubCount,
                                 AckCount1);
        {ok, <<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
                IsPersistentNum:1, MSB:(7-?PUBLISH_PREFIX_BITS)>>} ->
            %% because we specify /binary, and binaries are complete
            %% bytes, the size spec is in bytes, not bits.
            {ok, <<LSB:1/binary, MsgId:?MSG_ID_BYTES/binary>>} =
                file_handle_cache:read(
                  Hdl, ?PUBLISH_RECORD_LENGTH_BYTES - 1),
            <<RelSeq:?REL_SEQ_BITS_BYTE_ALIGNED>> = <<MSB, LSB/binary>>,
            SegEntries1 =
                array:set(RelSeq,
                          {{MsgId, 1 == IsPersistentNum}, no_del, no_ack},
                          SegEntries),
            load_segment_entries(KeepAcks, Hdl, SegEntries1, PubCount + 1,
                                 AckCount);
        _ErrOrEoF ->
            {SegEntries, PubCount, AckCount}
    end.

deliver_or_ack_msg(KeepAcks, RelSeq, AckCount, SegEntries) ->
    case array:get(RelSeq, SegEntries) of
        {PubRecord, no_del, no_ack} ->
            {AckCount, array:set(RelSeq, {PubRecord, del, no_ack}, SegEntries)};
        {PubRecord, del, no_ack} when KeepAcks ->
            {AckCount + 1, array:set(RelSeq, {PubRecord, del, ack}, SegEntries)};
        {_PubRecord, del, no_ack} ->
            {AckCount + 1, array:reset(RelSeq, SegEntries)}
    end.

%% Loading Journal. This isn't idempotent and will mess up the counts
%% if you call it more than once on the same state. Assumes the counts
%% are 0 to start with.

load_journal(State) ->
    {JournalHdl, State1} = get_journal_handle(State),
    {ok, 0} = file_handle_cache:position(JournalHdl, 0),
    State2 = #qistate { segments = Segments } = load_journal_entries(State1),
    Segments1 =
        segment_map(
          fun (_Seg, Segment = #segment { journal_entries = JEntries,
                                          pubs = PubCountInJournal,
                                          acks = AckCountInJournal }) ->
                  %% We want to keep acks in so that we can remove
                  %% them if duplicates are in the journal. The counts
                  %% here are purely from the segment itself.
                  {SegEntries, PubCountInSeg, AckCountInSeg, Segment1} =
                      load_segment(true, Segment),
                  %% Removed counts here are the number of pubs and
                  %% acks that are duplicates - i.e. found in both the
                  %% segment and journal.
                  {JEntries1, PubsRemoved, AcksRemoved} =
                      journal_minus_segment(JEntries, SegEntries),
                  PubCount1 = PubCountInSeg + PubCountInJournal - PubsRemoved,
                  AckCount1 = AckCountInSeg + AckCountInJournal - AcksRemoved,
                  Segment1 #segment { journal_entries = JEntries1,
                                      pubs = PubCount1,
                                      acks = AckCount1 }
          end, Segments),
    State2 #qistate { segments = Segments1 }.

load_journal_entries(State = #qistate { journal_handle = Hdl }) ->
    case file_handle_cache:read(Hdl, ?SEQ_BYTES) of
        {ok, <<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>>} ->
            case Prefix of
                ?DEL_JPREFIX ->
                    load_journal_entries(add_to_journal(SeqId, del, State));
                ?ACK_JPREFIX ->
                    load_journal_entries(add_to_journal(SeqId, ack, State));
                _ ->
                    case file_handle_cache:read(Hdl, ?MSG_ID_BYTES) of
                        {ok, <<MsgIdNum:?MSG_ID_BITS>>} ->
                            %% work around for binary data
                            %% fragmentation. See
                            %% rabbit_msg_file:read_next/2
                            <<MsgId:?MSG_ID_BYTES/binary>> =
                                <<MsgIdNum:?MSG_ID_BITS>>,
                            Publish = {MsgId,
                                       case Prefix of
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

add_to_journal(SeqId, Action, State = #qistate { dirty_count = DCount,
                                                 segments = Segments,
                                                 dir = Dir }) ->
    {Seg, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    Segment = segment_find_or_new(Seg, Dir, Segments),
    Segment1 = add_to_journal(RelSeq, Action, Segment),
    State #qistate { dirty_count = DCount + 1,
                     segments = segment_store(Segment1, Segments) };

add_to_journal(RelSeq, Action, Segment =
               #segment { journal_entries = JEntries,
                          pubs = PubCount, acks = AckCount }) ->
    JEntries1 = add_to_journal(RelSeq, Action, JEntries),
    Segment1 = Segment #segment { journal_entries = JEntries1 },
    case Action of
        del                     -> Segment1;
        ack                     -> Segment1 #segment { acks = AckCount + 1 };
        {_MsgId, _IsPersistent} -> Segment1 #segment { pubs = PubCount + 1 }
    end;

%% This is a more relaxed version of deliver_or_ack_msg because we can
%% have dels or acks in the journal without the corresponding
%% pub. Also, always want to keep acks. Things must occur in the right
%% order though.
add_to_journal(RelSeq, Action, SegJArray) ->
    case array:get(RelSeq, SegJArray) of
        undefined ->
            array:set(RelSeq,
                      case Action of
                          {_Msg, _IsPersistent} -> {Action, no_del, no_ack};
                          del                   -> {no_pub,    del, no_ack};
                          ack                   -> {no_pub, no_del,    ack}
                      end, SegJArray);
        ({PubRecord, no_del, no_ack}) when Action == del ->
            array:set(RelSeq, {PubRecord, del, no_ack}, SegJArray);
        ({PubRecord,    Del, no_ack}) when Action == ack ->
            array:set(RelSeq, {PubRecord, Del,    ack}, SegJArray)
    end.

%% Combine what we have just read from a segment file with what we're
%% holding for that segment in memory. There must be no
%% duplicates. Used when providing segment entries to the variable
%% queue.
journal_plus_segment(JEntries, SegEntries) ->
    array:sparse_foldl(
      fun (RelSeq, JObj, SegEntriesOut) ->
              SegEntry = case array:get(RelSeq, SegEntriesOut) of
                             undefined -> not_found;
                             SObj = {_, _, _} -> SObj
                         end,
              journal_plus_segment(JObj, SegEntry, RelSeq, SegEntriesOut)
      end, SegEntries, JEntries).

%% Here, the Out is the Seg Array which we may be adding to (for
%% items only in the journal), modifying (bits in both), or erasing
%% from (ack in journal, not segment).
journal_plus_segment(Obj = {{_MsgId, _IsPersistent}, no_del, no_ack},
                     not_found,
                     RelSeq, Out) ->
    array:set(RelSeq, Obj, Out);
journal_plus_segment(Obj = {{_MsgId, _IsPersistent}, del, no_ack},
                     not_found,
                     RelSeq, Out) ->
    array:set(RelSeq, Obj, Out);
journal_plus_segment({{_MsgId, _IsPersistent}, del, ack},
                     not_found,
                     RelSeq, Out) ->
    array:reset(RelSeq, Out);

journal_plus_segment({no_pub, del, no_ack},
                     {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                     RelSeq, Out) ->
    array:set(RelSeq, {PubRecord, del, no_ack}, Out);

journal_plus_segment({no_pub, del, ack},
                     {{_MsgId, _IsPersistent}, no_del, no_ack},
                     RelSeq, Out) ->
    array:reset(RelSeq, Out);
journal_plus_segment({no_pub, no_del, ack},
                     {{_MsgId, _IsPersistent}, del, no_ack},
                     RelSeq, Out) ->
    array:reset(RelSeq, Out).

%% Remove from the journal entries for a segment, items that are
%% duplicates of entries found in the segment itself. Used on start up
%% to clean up the journal.
journal_minus_segment(JEntries, SegEntries) ->
    array:sparse_foldl(
      fun (RelSeq, JObj, {JEntriesOut, PubsRemoved, AcksRemoved}) ->
              SegEntry = case array:get(RelSeq, SegEntries) of
                             undefined -> not_found;
                             SObj = {_, _, _} -> SObj
                         end,
              journal_minus_segment(JObj, SegEntry, RelSeq, JEntriesOut,
                                    PubsRemoved, AcksRemoved)
      end, {array_new(), 0, 0}, JEntries).

%% Here, the Out is a fresh journal that we're filling with valid
%% entries. PubsRemoved and AcksRemoved only get increased when the a
%% publish or ack is in both the journal and the segment.

%% Both the same. Must be at least the publish
journal_minus_segment(Obj, Obj = {{_MsgId, _IsPersistent}, _Del, no_ack},
                      _RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {Out, PubsRemoved + 1, AcksRemoved};
journal_minus_segment(Obj, Obj = {{_MsgId, _IsPersistent}, _Del, ack},
                      _RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {Out, PubsRemoved + 1, AcksRemoved + 1};

%% Just publish in journal
journal_minus_segment(Obj = {{_MsgId, _IsPersistent}, no_del, no_ack},
                      not_found,
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, Obj, Out), PubsRemoved, AcksRemoved};

%% Just deliver in journal
journal_minus_segment(Obj = {no_pub, del, no_ack},
                      {{_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, Obj, Out), PubsRemoved, AcksRemoved};
journal_minus_segment({no_pub, del, no_ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      _RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {Out, PubsRemoved, AcksRemoved};

%% Just ack in journal
journal_minus_segment(Obj = {no_pub, no_del, ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, Obj, Out), PubsRemoved, AcksRemoved};
journal_minus_segment({no_pub, no_del, ack},
                      {{_MsgId, _IsPersistent}, del, ack},
                      _RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {Out, PubsRemoved, AcksRemoved};

%% Publish and deliver in journal
journal_minus_segment(Obj = {{_MsgId, _IsPersistent}, del, no_ack},
                      not_found,
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, Obj, Out), PubsRemoved, AcksRemoved};
journal_minus_segment({PubRecord, del, no_ack},
                      {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, {no_pub, del, no_ack}, Out),
     PubsRemoved + 1, AcksRemoved};

%% Deliver and ack in journal
journal_minus_segment(Obj = {no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, Obj, Out), PubsRemoved, AcksRemoved};
journal_minus_segment({no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, {no_pub, no_del, ack}, Out),
     PubsRemoved, AcksRemoved};
journal_minus_segment({no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, del, ack},
                      _RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {Out, PubsRemoved, AcksRemoved + 1};

%% Publish, deliver and ack in journal
journal_minus_segment({{_MsgId, _IsPersistent}, del, ack},
                      not_found,
                      _RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {Out, PubsRemoved, AcksRemoved};
journal_minus_segment({PubRecord, del, ack},
                      {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, {no_pub, del, ack}, Out),
     PubsRemoved + 1, AcksRemoved};
journal_minus_segment({PubRecord, del, ack},
                      {PubRecord = {_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, Out, PubsRemoved, AcksRemoved) ->
    {array:set(RelSeq, {no_pub, no_del, ack}, Out),
     PubsRemoved + 1, AcksRemoved}.
