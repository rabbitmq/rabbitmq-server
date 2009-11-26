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
         write_delivered/2, write_acks/2, sync_seq_ids/3, flush_journal/1,
         read_segment_entries/2, next_segment_boundary/1, segment_size/0,
         find_lowest_seq_id_seg_and_next_seq_id/1, start_msg_store/1]).

%%----------------------------------------------------------------------------
%% The queue disk index
%%
%% The queue disk index operates over a journal, and a number of
%% segment files. Each segment is the same size, both in max number of
%% entries, and max file size, owing to fixed sized records.
%%
%% Publishes are written directly to the segment files. The segment is
%% found by dividing the sequence id by the the max number of entries
%% per segment. Only the relative sequence within the segment is
%% recorded as the sequence id within a segment file (i.e. sequence id
%% modulo max number of entries per segment).  This is keeps entries
%% as small as possible. Publishes are only ever going to be received
%% in contiguous ascending order.
%%
%% Acks and deliveries are written to a bounded journal and are also
%% held in memory, each in a dict with the segment as the key. Again,
%% the records are fixed size: the entire sequence id is written and
%% is limited to a 63-bit unsigned integer. The remaining bit
%% indicates whether the journal entry is for a delivery or an
%% ack. When the journal gets too big, or flush_journal is called, the
%% journal is (possibly incrementally) flushed out to the segment
%% files. As acks and delivery notes can be received in any order
%% (this is not obvious for deliveries, but consider what happens when
%% eg msgs are *re*queued - you'll publish and then mark the msgs
%% delivered immediately, which may be out of order), this journal
%% reduces seeking, and batches writes to the segment files, keeping
%% performance high.
%%
%% On startup, the journal is read along with all the segment files,
%% and the journal is fully flushed out to the segment files. Care is
%% taken to ensure that no message can be delivered or ack'd twice.
%%
%%----------------------------------------------------------------------------

-define(CLEAN_FILENAME, "clean.dot").

-define(MAX_JOURNAL_ENTRY_COUNT, 32768).
-define(JOURNAL_FILENAME, "journal.jif").

-define(DEL_BIT, 0).
-define(ACK_BIT, 1).
-define(SEQ_BYTES, 8).
-define(SEQ_BITS, ((?SEQ_BYTES * 8) - 1)).
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
          seg_num_handles,
          journal_count,
          journal_ack_dict,
          journal_del_dict,
          seg_ack_counts,
          publish_handle,
          partial_segments
        }).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(hdl() :: ('undefined' | any())).
-type(msg_id() :: binary()).
-type(seq_id() :: integer()).
-type(hdl_and_count() :: ('undefined' |
                          {non_neg_integer(), hdl(), non_neg_integer()})).
-type(qistate() :: #qistate { dir               :: file_path(),
                              seg_num_handles   :: dict(),
                              journal_count     :: integer(),
                              journal_ack_dict  :: dict(),
                              journal_del_dict  :: dict(),
                              seg_ack_counts    :: dict(),
                              publish_handle    :: hdl_and_count(),
                              partial_segments  :: dict()
                            }).

-spec(init/1 :: (queue_name()) -> {non_neg_integer(), qistate()}).
-spec(terminate/1 :: (qistate()) -> qistate()).
-spec(terminate_and_erase/1 :: (qistate()) -> qistate()).
-spec(write_published/4 :: (msg_id(), seq_id(), boolean(), qistate())
      -> qistate()).
-spec(write_delivered/2 :: (seq_id(), qistate()) -> qistate()).
-spec(write_acks/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(sync_seq_ids/3 :: ([seq_id()], boolean(), qistate()) -> qistate()).
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
    {TotalMsgCount, State1} = read_and_prune_segments(State),
    scatter_journal(TotalMsgCount, State1).

terminate(State = #qistate { seg_num_handles = SegHdls }) ->
    case 0 == dict:size(SegHdls) of
        true  -> State;
        false -> State1 = #qistate { dir = Dir } = close_all_handles(State),
                 store_clean_shutdown(Dir),
                 State1 #qistate { publish_handle = undefined }
    end.

terminate_and_erase(State) ->
    State1 = terminate(State),
    ok = delete_queue_directory(State1 #qistate.dir),
    State1.

write_published(MsgId, SeqId, IsPersistent, State)
  when is_binary(MsgId) ->
    ?MSG_ID_BYTES = size(MsgId),
    {SegNum, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    {Hdl, State1} = get_pub_handle(SegNum, State),
    ok = file_handle_cache:append(Hdl,
                                  <<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
                                   (bool_to_int(IsPersistent)):1,
                                   RelSeq:?REL_SEQ_BITS, MsgId/binary>>),
    State1.

write_delivered(SeqId, State = #qistate { journal_del_dict = JDelDict }) ->
    {JDelDict1, State1} =
        write_to_journal([<<?DEL_BIT:1, SeqId:?SEQ_BITS>>],
                         [SeqId], JDelDict, State),
    maybe_flush(State1 #qistate { journal_del_dict = JDelDict1 }).

write_acks(SeqIds, State = #qistate { journal_ack_dict = JAckDict }) ->
    {JAckDict1, State1} =
        write_to_journal([<<?ACK_BIT:1, SeqId:?SEQ_BITS>> || SeqId <- SeqIds],
                         SeqIds, JAckDict, State),
    maybe_flush(State1 #qistate { journal_ack_dict = JAckDict1 }).

sync_seq_ids(SeqIds, SyncAckJournal, State) ->
    State1 = case SyncAckJournal of
                 true  -> {Hdl, State2} = get_journal_handle(State),
                          ok = file_handle_cache:sync(Hdl),
                          State2;
                 false -> State
             end,
    SegNumsSet =
        lists:foldl(
          fun (SeqId, Set) ->
                  {SegNum, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
                  sets:add_element(SegNum, Set)
          end, sets:new(), SeqIds),
    sets:fold(
      fun (SegNum, StateN) ->
              {Hdl1, StateM} = get_seg_handle(SegNum, StateN),
              ok = file_handle_cache:sync(Hdl1),
              StateM
      end, State1, SegNumsSet).

flush_journal(State = #qistate { journal_count = 0 }) ->
    State;
flush_journal(State = #qistate { journal_ack_dict = JAckDict,
                                 journal_del_dict = JDelDict,
                                 journal_count    = JCount }) ->
    SegNum = case dict:fetch_keys(JAckDict) of
                 []    -> hd(dict:fetch_keys(JDelDict));
                 [N|_] -> N
             end,
    Dels = seg_entries_from_dict(SegNum, JDelDict),
    Acks = seg_entries_from_dict(SegNum, JAckDict),
    State1 = append_dels_to_segment(SegNum, Dels, State),
    State2 = append_acks_to_segment(SegNum, Acks, State1),
    JCount1 = JCount - length(Dels) - length(Acks),
    State3 = State2 #qistate { journal_del_dict = dict:erase(SegNum, JDelDict),
                               journal_ack_dict = dict:erase(SegNum, JAckDict),
                               journal_count    = JCount1 },
    case JCount1 of
        0 -> {Hdl, State4} = get_journal_handle(State3),
             {ok, 0} = file_handle_cache:position(Hdl, bof),
             ok = file_handle_cache:truncate(Hdl),
             ok = file_handle_cache:sync(Hdl),
             State4;
        _ -> flush_journal(State3)
    end.

read_segment_entries(InitSeqId, State) ->
    {SegNum, 0} = seq_id_to_seg_and_rel_seq_id(InitSeqId),
    {SDict, _PubCount, _AckCount, _HighRelSeq, State1} =
        load_segment(SegNum, State),
    %% deliberately sort the list desc, because foldl will reverse it
    RelSeqs = rev_sort(dict:fetch_keys(SDict)),
    {lists:foldl(fun (RelSeq, Acc) ->
                         {MsgId, IsDelivered, IsPersistent} =
                             dict:fetch(RelSeq, SDict),
                         [ {MsgId, reconstruct_seq_id(SegNum, RelSeq),
                            IsPersistent, IsDelivered} | Acc]
                 end, [], RelSeqs),
     State1}.

next_segment_boundary(SeqId) ->
    {SegNum, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    reconstruct_seq_id(SegNum + 1, 0).

segment_size() ->
    ?SEGMENT_ENTRY_COUNT.

find_lowest_seq_id_seg_and_next_seq_id(State = #qistate { dir = Dir }) ->
    SegNums = all_segment_nums(Dir),
    %% We don't want the lowest seq_id, merely the seq_id of the start
    %% of the lowest segment. That seq_id may not actually exist, but
    %% that's fine. The important thing is that the segment exists and
    %% the seq_id reported is on a segment boundary.

    %% SegNums is sorted, ascending.
    LowSeqIdSeg =
        case SegNums of
            []            -> 0;
            [MinSegNum|_] -> reconstruct_seq_id(MinSegNum, 0)
        end,
    {NextSeqId, State1} =
        case SegNums of
            [] -> {0, State};
            _  -> MaxSegNum = lists:last(SegNums),
                  {_SDict, PubCount, _AckCount, HighRelSeq, State2} =
                      load_segment(MaxSegNum, State),
                  NextSeqId1 = reconstruct_seq_id(MaxSegNum, HighRelSeq),
                  NextSeqId2 = case PubCount of
                                   0 -> NextSeqId1;
                                   _ -> NextSeqId1 + 1
                               end,
                  {NextSeqId2, State2}
        end,
    {LowSeqIdSeg, NextSeqId, State1}.

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
%% Minor Helpers
%%----------------------------------------------------------------------------

maybe_flush(State = #qistate { journal_count = JCount })
  when JCount > ?MAX_JOURNAL_ENTRY_COUNT ->
    flush_journal(State);
maybe_flush(State) ->
    State.

write_to_journal(BinList, SeqIds, Dict,
                 State = #qistate { journal_count = JCount }) ->
    {Hdl, State1} = get_journal_handle(State),
    ok = file_handle_cache:append(Hdl, BinList),
    {Dict1, JCount1} =
        lists:foldl(
          fun (SeqId, {Dict2, JCount2}) ->
                  {add_seqid_to_dict(SeqId, Dict2), JCount2 + 1}
          end, {Dict, JCount}, SeqIds),
    {Dict1, State1 #qistate { journal_count = JCount1 }}.

queue_name_to_dir_name(Name = #resource { kind = queue }) ->
    Bin = term_to_binary(Name),
    Size = 8*size(Bin),
    <<Num:Size>> = Bin,
    lists:flatten(io_lib:format("~.36B", [Num])).

queues_dir() ->
    filename:join(rabbit_mnesia:dir(), "queues").

rev_sort(List) ->
    lists:sort(fun (A, B) -> B < A end, List).

get_journal_handle(State = #qistate { dir = Dir, seg_num_handles = SegHdls }) ->
    case dict:find(journal, SegHdls) of
        {ok, Hdl} ->
            {Hdl, State};
        error ->
            Path = filename:join(Dir, ?JOURNAL_FILENAME),
            Mode = [raw, binary, delayed_write, write, read, read_ahead],
            new_handle(journal, Path, Mode, State)
    end.

get_pub_handle(SegNum, State = #qistate { publish_handle = PubHandle }) ->
    {State1, PubHandle1 = {_SegNum, Hdl, _Count}} =
        get_counted_handle(SegNum, State, PubHandle),
    {Hdl, State1 #qistate { publish_handle = PubHandle1 }}.

get_counted_handle(SegNum, State, undefined) ->
    get_counted_handle(SegNum, State, {SegNum, undefined, 0});
get_counted_handle(SegNum, State = #qistate { partial_segments = Partials },
                   {SegNum, undefined, Count}) ->
    {Hdl, State1} = get_seg_handle(SegNum, State),
    {CountExtra, Partials1} =
        case dict:find(SegNum, Partials) of
            {ok, CountExtra1} -> {CountExtra1, dict:erase(SegNum, Partials)};
            error             -> {0, Partials}
        end,
    Count1 = Count + 1 + CountExtra,
    {State1 #qistate { partial_segments = Partials1 }, {SegNum, Hdl, Count1}};
get_counted_handle(SegNum, State, {SegNum, Hdl, Count})
  when Count < ?SEGMENT_ENTRY_COUNT ->
    {State, {SegNum, Hdl, Count + 1}};
get_counted_handle(SegNumA, State, {SegNumB, Hdl, ?SEGMENT_ENTRY_COUNT})
  when SegNumA == SegNumB + 1 ->
    ok = file_handle_cache:append_write_buffer(Hdl),
    get_counted_handle(SegNumA, State, undefined);
get_counted_handle(SegNumA, State = #qistate { partial_segments = Partials,
                                               seg_ack_counts = AckCounts },
                   {SegNumB, _Hdl, Count}) ->
    %% don't flush here because it's possible SegNumB has been deleted
    State1 =
        case dict:find(SegNumB, AckCounts) of
            {ok, Count} ->
                %% #acks == #pubs, and we're moving to different
                %% segment, so delete.
                delete_segment(SegNumB, State);
            _ ->
                State #qistate {
                  partial_segments = dict:store(SegNumB, Count, Partials) }
        end,
    get_counted_handle(SegNumA, State1, undefined).

get_seg_handle(SegNum, State = #qistate { dir = Dir, seg_num_handles = SegHdls }) ->
    case dict:find(SegNum, SegHdls) of
        {ok, Hdl} ->
            {Hdl, State};
        error ->
            new_handle(SegNum, seg_num_to_path(Dir, SegNum),
                       [binary, raw, read, write,
                        {delayed_write, ?SEGMENT_TOTAL_SIZE, 1000},
                        {read_ahead, ?SEGMENT_TOTAL_SIZE}],
                       State)
    end.

delete_segment(SegNum, State = #qistate { dir = Dir,
                                          seg_ack_counts = AckCounts,
                                          partial_segments = Partials }) ->
    State1 = close_handle(SegNum, State),
    ok = case file:delete(seg_num_to_path(Dir, SegNum)) of
             ok              -> ok;
             {error, enoent} -> ok
         end,
    State1 #qistate {seg_ack_counts = dict:erase(SegNum, AckCounts),
                     partial_segments = dict:erase(SegNum, Partials) }.

new_handle(Key, Path, Mode, State = #qistate { seg_num_handles = SegHdls }) ->
    {ok, Hdl} = file_handle_cache:open(Path, Mode, [{write_buffer, infinity}]),
    {Hdl, State #qistate { seg_num_handles = dict:store(Key, Hdl, SegHdls) }}.

close_handle(Key, State = #qistate { seg_num_handles = SegHdls }) ->
    case dict:find(Key, SegHdls) of
        {ok, Hdl} ->
            ok = file_handle_cache:close(Hdl),
            State #qistate { seg_num_handles = dict:erase(Key, SegHdls) };
        error ->
            State
    end.

close_all_handles(State = #qistate { seg_num_handles = SegHdls }) ->
    ok = dict:fold(fun (_Key, Hdl, ok) ->
                           file_handle_cache:close(Hdl)
                   end, ok, SegHdls),
    State #qistate { seg_num_handles = dict:new() }.

bool_to_int(true ) -> 1;
bool_to_int(false) -> 0.

seq_id_to_seg_and_rel_seq_id(SeqId) ->
    { SeqId div ?SEGMENT_ENTRY_COUNT, SeqId rem ?SEGMENT_ENTRY_COUNT }.

reconstruct_seq_id(SegNum, RelSeq) ->
    (SegNum * ?SEGMENT_ENTRY_COUNT) + RelSeq.

seg_num_to_path(Dir, SegNum) ->
    SegName = integer_to_list(SegNum),
    filename:join(Dir, SegName ++ ?SEGMENT_EXTENSION).

delete_queue_directory(Dir) ->
    {ok, Entries} = file:list_dir(Dir),
    ok = lists:foldl(fun (Entry, ok) ->
                             file:delete(filename:join(Dir, Entry))
                     end, ok, Entries),
    ok = file:del_dir(Dir).

add_seqid_to_dict(SeqId, Dict) ->
    {SegNum, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    add_seqid_to_dict(SegNum, RelSeq, Dict).

add_seqid_to_dict(SegNum, RelSeq, Dict) ->
    dict:update(SegNum, fun(Lst) -> [RelSeq|Lst] end, [RelSeq], Dict).

all_segment_nums(Dir) ->
    lists:sort(
      [list_to_integer(
         lists:takewhile(fun(C) -> $0 =< C andalso C =< $9 end, SegName))
       || SegName <- filelib:wildcard("*" ++ ?SEGMENT_EXTENSION, Dir)]).

blank_state(QueueName) ->
    StrName = queue_name_to_dir_name(QueueName),
    Dir = filename:join(queues_dir(), StrName),
    ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
    #qistate { dir              = Dir,
               seg_num_handles  = dict:new(),
               journal_count    = 0,
               journal_ack_dict = dict:new(),
               journal_del_dict = dict:new(),
               seg_ack_counts   = dict:new(),
               publish_handle   = undefined,
               partial_segments = dict:new()
             }.

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

seg_entries_from_dict(SegNum, Dict) ->
    case dict:find(SegNum, Dict) of
        {ok, Entries} -> Entries;
        error         -> []
    end.


%%----------------------------------------------------------------------------
%% Msg Store Startup Delta Function
%%----------------------------------------------------------------------------

queue_index_walker([]) ->
    finished;
queue_index_walker([QueueName|QueueNames]) ->
    State = blank_state(QueueName),
    {Hdl, State1} = get_journal_handle(State),
    {JAckDict, _JDelDict} = load_journal(Hdl, dict:new(), dict:new()),
    State2 = #qistate { dir = Dir } =
        close_handle(journal, State1 #qistate { journal_ack_dict = JAckDict }),
    SegNums = all_segment_nums(Dir),
    queue_index_walker({SegNums, State2, QueueNames});

queue_index_walker({[], State, QueueNames}) ->
    _State = terminate(State),
    queue_index_walker(QueueNames);
queue_index_walker({[SegNum | SegNums], State, QueueNames}) ->
    {SDict, _PubCount, _AckCount, _HighRelSeq, State1} =
        load_segment(SegNum, State),
    queue_index_walker({dict:to_list(SDict), State1, SegNums, QueueNames});

queue_index_walker({[], State, SegNums, QueueNames}) ->
    queue_index_walker({SegNums, State, QueueNames});
queue_index_walker({[{_RelSeq, {MsgId, _IsDelivered, IsPersistent}} | Msgs],
                    State, SegNums, QueueNames}) ->
    case IsPersistent of
        true  -> {MsgId, 1, {Msgs, State, SegNums, QueueNames}};
        false -> queue_index_walker({Msgs, State, SegNums, QueueNames})
    end.


%%----------------------------------------------------------------------------
%% Startup Functions
%%----------------------------------------------------------------------------

read_and_prune_segments(State = #qistate { dir = Dir }) ->
    SegNums = all_segment_nums(Dir),
    CleanShutdown = detect_clean_shutdown(Dir),
    {TotalMsgCount, State1} =
        lists:foldl(
          fun (SegNum, {TotalMsgCount1, StateN =
                        #qistate { publish_handle = PublishHandle,
                                   partial_segments = Partials }}) ->
                  {SDict, PubCount, AckCount, _HighRelSeq, StateM} =
                      load_segment(SegNum, StateN),
                  StateL = #qistate { seg_ack_counts = AckCounts } =
                      drop_and_deliver(SegNum, SDict, CleanShutdown, StateM),
                  %% ignore the effect of drop_and_deliver on
                  %% TotalMsgCount and AckCounts, as drop_and_deliver
                  %% will add to the journal dicts, which will then
                  %% effect TotalMsgCount when we scatter the journal
                  TotalMsgCount2 = TotalMsgCount1 + dict:size(SDict),
                  AckCounts1 = case AckCount of
                                   0 -> AckCounts;
                                   N -> dict:store(SegNum, N, AckCounts)
                               end,
                  %% In the following, whilst there may be several
                  %% partial segments, we only remember the last
                  %% one. All other partial segments get added into
                  %% the partial_segments dict
                  {PublishHandle1, Partials1} =
                      case PubCount of
                          ?SEGMENT_ENTRY_COUNT ->
                              {PublishHandle, Partials};
                          0 ->
                              {PublishHandle, Partials};
                          _ ->
                              {{SegNum, undefined, PubCount},
                               case PublishHandle of
                                   undefined ->
                                       Partials;
                                   {SegNumOld, undefined, PubCountOld} ->
                                       dict:store(SegNumOld, PubCountOld,
                                                  Partials)
                               end}
                      end,
                  {TotalMsgCount2,
                   StateL #qistate { seg_ack_counts = AckCounts1,
                                     publish_handle = PublishHandle1,
                                     partial_segments = Partials1 }}
          end, {0, State}, SegNums),
    {TotalMsgCount, State1}.

scatter_journal(TotalMsgCount, State = #qistate { dir = Dir }) ->
    {Hdl, State1 = #qistate { journal_del_dict = JDelDict,
                              journal_ack_dict = JAckDict }} =
        get_journal_handle(State),
    %% ADict and DDict may well contain duplicates. However, this is
    %% ok, because we use sets to eliminate dups before writing to
    %% segments
    {ADict, DDict} = load_journal(Hdl, JAckDict, JDelDict),
    State2 = close_handle(journal, State1),
    {TotalMsgCount1, ADict1, State3} =
        dict:fold(fun replay_journal_to_segment/3,
                  {TotalMsgCount, ADict,
                   %% supply empty dicts so that when
                   %% replay_journal_to_segment loads segments, it
                   %% gets all msgs, and ignores anything we've found
                   %% in the journal.
                   State2 #qistate { journal_del_dict = dict:new(),
                                     journal_ack_dict = dict:new() }}, DDict),
    %% replay for segments which only had acks, and no deliveries
    {TotalMsgCount2, State4} =
        dict:fold(fun replay_journal_acks_to_segment/3,
                  {TotalMsgCount1, State3}, ADict1),
    JournalPath = filename:join(Dir, ?JOURNAL_FILENAME),
    ok = file:delete(JournalPath),
    {TotalMsgCount2, State4}.

load_journal(Hdl, ADict, DDict) ->
    case file_handle_cache:read(Hdl, ?SEQ_BYTES) of
        {ok, <<?DEL_BIT:1, SeqId:?SEQ_BITS>>} ->
            load_journal(Hdl, ADict, add_seqid_to_dict(SeqId, DDict));
        {ok, <<?ACK_BIT:1, SeqId:?SEQ_BITS>>} ->
            load_journal(Hdl, add_seqid_to_dict(SeqId, ADict), DDict);
        _ErrOrEoF ->
            {ADict, DDict}
    end.

replay_journal_to_segment(_SegNum, [], {TotalMsgCount, ADict, State}) ->
    {TotalMsgCount, ADict, State};
replay_journal_to_segment(SegNum, Dels, {TotalMsgCount, ADict, State}) ->
    {SDict, _PubCount, _AckCount, _HighRelSeq, State1} =
        load_segment(SegNum, State),
    ValidDels = sets:to_list(
                  sets:filter(
                    fun (RelSeq) ->
                            case dict:find(RelSeq, SDict) of
                                {ok, {_MsgId, false, _IsPersistent}} -> true;
                                _                                    -> false
                            end
                    end, sets:from_list(Dels))),
    State2 = append_dels_to_segment(SegNum, ValidDels, State1),
    Acks = seg_entries_from_dict(SegNum, ADict),
    case Acks of
        [] -> {TotalMsgCount, ADict, State2};
        _  -> ADict1 = dict:erase(SegNum, ADict),
              {Count, State3} =
                  filter_acks_and_append_to_segment(SegNum, SDict,
                                                    Acks, State2),
              {TotalMsgCount - Count, ADict1, State3}
    end.

replay_journal_acks_to_segment(_SegNum, [], {TotalMsgCount, State}) ->
    {TotalMsgCount, State};
replay_journal_acks_to_segment(SegNum, Acks, {TotalMsgCount, State}) ->
    {SDict, _PubCount, _AckCount, _HighRelSeq, State1} =
        load_segment(SegNum, State),
    {Count, State2} =
        filter_acks_and_append_to_segment(SegNum, SDict, Acks, State1),
    {TotalMsgCount - Count, State2}.

filter_acks_and_append_to_segment(SegNum, SDict, Acks, State) ->
    ValidRelSeqIds = dict:fetch_keys(SDict),
    ValidAcks = sets:to_list(sets:intersection(sets:from_list(ValidRelSeqIds),
                                               sets:from_list(Acks))),
    {length(ValidAcks), append_acks_to_segment(SegNum, ValidAcks, State)}.

drop_and_deliver(SegNum, SDict, CleanShutdown,
                 State = #qistate { journal_del_dict = JDelDict,
                                    journal_ack_dict = JAckDict }) ->
    {JDelDict1, JAckDict1} =
        dict:fold(
          fun (RelSeq, {MsgId, IsDelivered, true}, {JDelDict2, JAckDict2}) ->
                  %% msg is persistent, keep only if the msg_store has it
                  case {IsDelivered, rabbit_msg_store:contains(MsgId)} of
                      {false, true} when not CleanShutdown ->
                          %% not delivered, but dirty shutdown => mark delivered
                          {add_seqid_to_dict(SegNum, RelSeq, JDelDict2),
                           JAckDict2};
                      {_, true} ->
                          {JDelDict2, JAckDict2};
                      {true, false} ->
                          {JDelDict2,
                           add_seqid_to_dict(SegNum, RelSeq, JAckDict2)};
                      {false, false} ->
                          {add_seqid_to_dict(SegNum, RelSeq, JDelDict2),
                           add_seqid_to_dict(SegNum, RelSeq, JAckDict2)}
                  end;
              (RelSeq, {_MsgId, false, false}, {JDelDict2, JAckDict2}) ->
                  %% not persistent and not delivered => deliver and ack it
                  {add_seqid_to_dict(SegNum, RelSeq, JDelDict2),
                   add_seqid_to_dict(SegNum, RelSeq, JAckDict2)};
              (RelSeq, {_MsgId, true, false}, {JDelDict2, JAckDict2}) ->
                  %% not persistent but delivered => ack it
                  {JDelDict2,
                   add_seqid_to_dict(SegNum, RelSeq, JAckDict2)}
          end, {JDelDict, JAckDict}, SDict),
    State #qistate { journal_del_dict = JDelDict1,
                     journal_ack_dict = JAckDict1 }.


%%----------------------------------------------------------------------------
%% Loading Segments
%%----------------------------------------------------------------------------

load_segment(SegNum, State = #qistate { seg_num_handles = SegHdls,
                                        dir = Dir }) ->
    SegmentExists = case dict:find(SegNum, SegHdls) of
                        {ok, _} -> true;
                        error   -> filelib:is_file(seg_num_to_path(Dir, SegNum))
                    end,
    case SegmentExists of
        false ->
            {dict:new(), 0, 0, 0, State};
        true ->
            {Hdl, State1 = #qistate { journal_del_dict = JDelDict,
                                      journal_ack_dict = JAckDict }} =
                get_seg_handle(SegNum, State),
            {ok, 0} = file_handle_cache:position(Hdl, bof),
            {SDict, PubCount, AckCount, HighRelSeq} =
                load_segment_entries(Hdl, dict:new(), 0, 0, 0),
            %% delete ack'd msgs first
            {SDict1, AckCount1} =
                lists:foldl(fun (RelSeq, {SDict2, AckCount2}) ->
                                    {dict:erase(RelSeq, SDict2), AckCount2 + 1}
                            end, {SDict, AckCount},
                            seg_entries_from_dict(SegNum, JAckDict)),
            %% ensure remaining msgs are delivered as necessary
            SDict3 =
                lists:foldl(
                  fun (RelSeq, SDict4) ->
                          case dict:find(RelSeq, SDict4) of
                              {ok, {MsgId, false, IsPersistent}} ->
                                  dict:store(RelSeq,
                                             {MsgId, true, IsPersistent},
                                             SDict4);
                              _ ->
                                  SDict4
                          end
                  end, SDict1, seg_entries_from_dict(SegNum, JDelDict)),
            {SDict3, PubCount, AckCount1, HighRelSeq, State1}
    end.

load_segment_entries(Hdl, SDict, PubCount, AckCount, HighRelSeq) ->
    case file_handle_cache:read(Hdl, 1) of
        {ok, <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                MSB:(8-?REL_SEQ_ONLY_PREFIX_BITS)>>} ->
            {ok, LSB} = file_handle_cache:read(
                          Hdl, ?REL_SEQ_ONLY_ENTRY_LENGTH_BYTES - 1),
            <<RelSeq:?REL_SEQ_BITS_BYTE_ALIGNED>> = <<MSB, LSB/binary>>,
            {SDict1, AckCount1} = deliver_or_ack_msg(SDict, AckCount, RelSeq),
            load_segment_entries(Hdl, SDict1, PubCount, AckCount1, HighRelSeq);
        {ok, <<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
                IsPersistentNum:1, MSB:(7-?PUBLISH_PREFIX_BITS)>>} ->
            %% because we specify /binary, and binaries are complete
            %% bytes, the size spec is in bytes, not bits.
            {ok, <<LSB:1/binary, MsgId:?MSG_ID_BYTES/binary>>} =
                file_handle_cache:read(
                  Hdl, ?PUBLISH_RECORD_LENGTH_BYTES - 1),
            <<RelSeq:?REL_SEQ_BITS_BYTE_ALIGNED>> = <<MSB, LSB/binary>>,
            HighRelSeq1 = lists:max([RelSeq, HighRelSeq]),
            load_segment_entries(
              Hdl, dict:store(RelSeq, {MsgId, false, 1 == IsPersistentNum},
                              SDict), PubCount + 1, AckCount, HighRelSeq1);
        _ErrOrEoF ->
            {SDict, PubCount, AckCount, HighRelSeq}
    end.

deliver_or_ack_msg(SDict, AckCount, RelSeq) ->
    case dict:find(RelSeq, SDict) of
        {ok, {MsgId, false, IsPersistent}} ->
            {dict:store(RelSeq, {MsgId, true, IsPersistent}, SDict), AckCount};
        {ok, {_MsgId, true, _IsPersistent}} ->
            {dict:erase(RelSeq, SDict), AckCount + 1}
    end.


%%----------------------------------------------------------------------------
%% Appending Acks or Dels to Segments
%%----------------------------------------------------------------------------

append_acks_to_segment(SegNum, Acks,
                       State = #qistate { seg_ack_counts = AckCounts,
                                          partial_segments = Partials }) ->
    AckCount = case dict:find(SegNum, AckCounts) of
                   {ok, AckCount1} -> AckCount1;
                   error           -> 0
               end,
    AckTarget = case dict:find(SegNum, Partials) of
                    {ok, PubCount} -> PubCount;
                    error          -> ?SEGMENT_ENTRY_COUNT
                end,
    AckCount2 = AckCount + length(Acks),
    append_acks_to_segment(SegNum, AckCount2, Acks, AckTarget, State).

append_acks_to_segment(SegNum, AckCount, _Acks, AckCount, State =
                       #qistate { publish_handle = PubHdl }) ->
    PubHdl1 = case PubHdl of
                  %% If we're adjusting the pubhdl here then there
                  %% will be no entry in partials, thus the target ack
                  %% count must be SEGMENT_ENTRY_COUNT
                  {SegNum, Hdl, AckCount = ?SEGMENT_ENTRY_COUNT}
                  when Hdl /= undefined ->
                      {SegNum + 1, undefined, 0};
                  _ ->
                      PubHdl
              end,
    delete_segment(SegNum, State #qistate { publish_handle = PubHdl1 });
append_acks_to_segment(_SegNum, _AckCount, [], _AckTarget, State) ->
    State;
append_acks_to_segment(SegNum, AckCount, Acks, AckTarget, State =
                       #qistate { seg_ack_counts = AckCounts })
  when AckCount < AckTarget ->
    {Hdl, State1} = append_to_segment(SegNum, Acks, State),
    ok = file_handle_cache:sync(Hdl),
    State1 #qistate { seg_ack_counts =
                      dict:store(SegNum, AckCount, AckCounts) }.

append_dels_to_segment(SegNum, Dels, State) ->
    {_Hdl, State1} = append_to_segment(SegNum, Dels, State),
    State1.

append_to_segment(SegNum, AcksOrDels, State) ->
    {Hdl, State1} = get_seg_handle(SegNum, State),
    ok = file_handle_cache:append(
           Hdl, [<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                  RelSeq:?REL_SEQ_BITS>> || RelSeq <- AcksOrDels ]),
    {Hdl, State1}.
