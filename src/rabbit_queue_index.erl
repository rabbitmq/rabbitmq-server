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

-export([init/1, write_published/4, write_delivered/2, write_acks/2,
         flush_journal/1, read_segment_entries/2, next_segment_boundary/1,
         segment_size/0, find_lowest_seq_id_seg_and_next_seq_id/1]).

%%----------------------------------------------------------------------------
%% The queue disk index
%%
%% The queue disk index operates over an ack journal, and a number of
%% segment files. Each segment is the same size, both in max number of
%% entries, and max file size, owing to fixed sized records.
%%
%% Publishes and delivery notes are written directly to the segment
%% files. The segment is found by dividing the sequence id by the the
%% max number of entries per segment. Only the relative sequence
%% within the segment is recorded as the sequence id within a segment
%% file (i.e. sequence id modulo max number of entries per segment).
%% This is keeps entries as small as possible. Publishes and
%% deliveries are only ever going to be received in contiguous
%% ascending order.
%%
%% Acks are written to a bounded journal and are also held in memory,
%% in a dict with the segment file as the key. Again, the records are
%% fixed size: the entire sequence id is written and is limited to a
%% 64-bit unsigned integer. When the journal gets too big, or
%% flush_journal is called, the journal is (possibly incrementally)
%% flushed out to the segment files. As acks can be received from any
%% delivered message in any order, this journal reduces seeking, and
%% batches writes to the segment files, keeping performance high. The
%% flush_journal/1 function returns a boolean indicating whether there
%% is more flushing work that can be done. This means that the process
%% can call this whenever it has an empty mailbox, only a small amount
%% of work is done, allowing the process to respond quickly to new
%% messages if they arrive, or to call flush_journal/1 several times
%% until the result indicates there is no more flushing to be done.
%%
%% On startup, the ack journal is read along with all the segment
%% files, and the ack journal is fully flushed out to the segment
%% files. Care is taken to ensure that no message can be ack'd twice.
%%
%%----------------------------------------------------------------------------

-define(MAX_ACK_JOURNAL_ENTRY_COUNT, 32768).
-define(ACK_JOURNAL_FILENAME, "ack_journal.jif").
-define(SEQ_BYTES, 8).
-define(SEQ_BITS, (?SEQ_BYTES * 8)).
-define(SEGMENT_EXTENSION, ".idx").

-define(REL_SEQ_BITS, 14).
-define(SEGMENT_ENTRIES_COUNT, 16384). %% trunc(math:pow(2,?REL_SEQ_BITS))).

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
-define(SEGMENT_TOTAL_SIZE, ?SEGMENT_ENTRIES_COUNT *
        (?PUBLISH_RECORD_LENGTH_BYTES +
         (2 * ?REL_SEQ_ONLY_ENTRY_LENGTH_BYTES))).

%%----------------------------------------------------------------------------

-record(qistate,
        { dir,
          cur_seg_num,
          cur_seg_hdl,
          journal_ack_count,
          journal_ack_dict,
          journal_handle,
          seg_ack_counts
        }).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(io_device() :: any()).
-type(msg_id() :: binary()).
-type(seq_id() :: integer()).
-type(file_path() :: any()).
-type(int_or_undef() :: integer() | 'undefined').
-type(io_dev_or_undef() :: io_device() | 'undefined').
-type(qistate() :: #qistate { dir               :: file_path(),
                              cur_seg_num       :: int_or_undef(),
                              cur_seg_hdl       :: io_dev_or_undef(),
                              journal_ack_count :: integer(),
                              journal_ack_dict  :: dict(),
                              journal_handle    :: io_device(),
                              seg_ack_counts    :: dict()
                            }).

-spec(init/1 :: (string()) -> {non_neg_integer(), qistate()}).
-spec(write_published/4 :: (msg_id(), seq_id(), boolean(), qistate())
      -> qistate()).
-spec(write_delivered/2 :: (seq_id(), qistate()) -> qistate()).
-spec(write_acks/2 :: ([seq_id()], qistate()) -> qistate()).
-spec(flush_journal/1 :: (qistate()) -> {boolean(), qistate()}).
-spec(read_segment_entries/2 :: (seq_id(), qistate()) ->
             {( [{msg_id(), seq_id(), boolean(), boolean()}]
              | 'not_found'), qistate()}).
-spec(next_segment_boundary/1 :: (seq_id()) -> seq_id()).
-spec(segment_size/0 :: () -> non_neg_integer()).
-spec(find_lowest_seq_id_seg_and_next_seq_id/1 :: (qistate()) ->
             {non_neg_integer(), non_neg_integer()}).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

init(Name) ->
    Dir = filename:join(queues_dir(), Name),
    ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
    {AckCounts, TotalMsgCount} = scatter_journal(Dir, find_ack_counts(Dir)),
    {ok, JournalHdl} = file:open(filename:join(Dir, ?ACK_JOURNAL_FILENAME),
                                 [raw, binary, delayed_write, write, read]),
    {TotalMsgCount, #qistate { dir = Dir,
                               cur_seg_num = undefined,
                               cur_seg_hdl = undefined,
                               journal_ack_count = 0,
                               journal_ack_dict = dict:new(),
                               journal_handle = JournalHdl,
                               seg_ack_counts = AckCounts
                             }}.

write_published(MsgId, SeqId, IsPersistent, State)
  when is_binary(MsgId) ->
    ?MSG_ID_BYTES = size(MsgId),
    {SegNum, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    {Hdl, State1} = get_file_handle_for_seg(SegNum, State),
    ok = file:write(Hdl,
                    <<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
                     (bool_to_int(IsPersistent)):1,
                     RelSeq:?REL_SEQ_BITS, MsgId/binary>>),
    State1.

write_delivered(SeqId, State) ->
    {SegNum, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    {Hdl, State1} = get_file_handle_for_seg(SegNum, State),
    ok = file:write(Hdl,
                    <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                     RelSeq:?REL_SEQ_BITS>>),
    State1.

write_acks(SeqIds, State = #qistate { journal_handle    = JournalHdl,
                                      journal_ack_dict  = JAckDict,
                                      journal_ack_count = JAckCount }) ->
    {JAckDict1, JAckCount1} =
        lists:foldl(
          fun (SeqId, {JAckDict2, JAckCount2}) ->
                  ok = file:write(JournalHdl, <<SeqId:?SEQ_BITS>>),
                  {add_ack_to_ack_dict(SeqId, JAckDict2), JAckCount2 + 1}
          end, {JAckDict, JAckCount}, SeqIds),
    State1 = State #qistate { journal_ack_dict = JAckDict1,
                              journal_ack_count = JAckCount1 },
    case JAckCount1 > ?MAX_ACK_JOURNAL_ENTRY_COUNT of
        true -> {_Cont, State2} = flush_journal(State1),
                State2;
        false -> State1
    end.

flush_journal(State = #qistate { journal_ack_count = 0 }) ->
    {false, State};
flush_journal(State = #qistate { journal_handle = JournalHdl,
                                 journal_ack_dict = JAckDict,
                                 journal_ack_count = JAckCount,
                                 seg_ack_counts = AckCounts,
                                 dir = Dir }) ->
    [SegNum|_] = dict:fetch_keys(JAckDict),
    Acks = dict:fetch(SegNum, JAckDict),
    SegPath = seg_num_to_path(Dir, SegNum),
    State1 = close_file_handle_for_seg(SegNum, State),
    AckCounts1 = append_acks_to_segment(SegPath, SegNum, AckCounts, Acks),
    JAckCount1 = JAckCount - length(Acks),
    State2 = State1 #qistate { journal_ack_dict = dict:erase(SegNum, JAckDict),
                               journal_ack_count = JAckCount1,
                               seg_ack_counts = AckCounts1 },
    if
        JAckCount1 == 0 ->
            {ok, 0} = file:position(JournalHdl, 0),
            ok = file:truncate(JournalHdl),
            {false, State2};
        JAckCount1 > ?MAX_ACK_JOURNAL_ENTRY_COUNT ->
            flush_journal(State2);
        true ->
            {true, State2}
    end.

read_segment_entries(InitSeqId, State =
                     #qistate { dir = Dir, journal_ack_dict = JAckDict }) ->
    {SegNum, 0} = seq_id_to_seg_and_rel_seq_id(InitSeqId),
    SegPath = seg_num_to_path(Dir, SegNum),
    {SDict, _AckCount, _HighRelSeq} = load_segment(SegNum, SegPath, JAckDict),
    %% deliberately sort the list desc, because foldl will reverse it
    RelSeqs = rev_sort(dict:fetch_keys(SDict)),
    {lists:foldl(fun (RelSeq, Acc) ->
                         {MsgId, IsDelivered, IsPersistent} =
                             dict:fetch(RelSeq, SDict),
                         [ {MsgId, reconstruct_seq_id(SegNum, RelSeq),
                            IsPersistent, IsDelivered} | Acc]
                 end, [], RelSeqs),
     State}.

next_segment_boundary(SeqId) ->
    {SegNum, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    reconstruct_seq_id(SegNum + 1, 0).

segment_size() ->
    ?SEGMENT_ENTRIES_COUNT.

find_lowest_seq_id_seg_and_next_seq_id(
  #qistate { dir = Dir, journal_ack_dict = JAckDict }) ->
    SegNumsPaths = all_segment_nums_paths(Dir),
    %% We don't want the lowest seq_id, merely the seq_id of the start
    %% of the lowest segment. That seq_id may not actually exist, but
    %% that's fine. The important thing is that the segment exists and
    %% the seq_id reported is on a segment boundary.
    LowSeqIdSeg =
        case SegNumsPaths of
            [] -> 0;
            _  -> {SegNum1, _SegPath1} = lists:min(SegNumsPaths),
                  reconstruct_seq_id(SegNum1, 0)
        end,
    HighestSeqId =
        case SegNumsPaths of
            [] -> 0;
            _  -> {SegNum2, SegPath2} = lists:max(SegNumsPaths),
                  {_SDict, _AckCount, HighRelSeq} =
                      load_segment(SegNum2, SegPath2, JAckDict),
                  1 + reconstruct_seq_id(SegNum2, HighRelSeq)
        end,
    {LowSeqIdSeg, HighestSeqId}.

%%----------------------------------------------------------------------------
%% Minor Helpers
%%----------------------------------------------------------------------------

queues_dir() ->
    filename:join(rabbit_mnesia:dir(), "queues").

rev_sort(List) ->
    lists:sort(fun (A, B) -> B < A end, List).

close_file_handle_for_seg(_SegNum,
                          State = #qistate { cur_seg_num = undefined }) ->
    State;
close_file_handle_for_seg(SegNum, State = #qistate { cur_seg_num = SegNum,
                                                     cur_seg_hdl = Hdl }) ->
    ok = file:sync(Hdl),
    ok = file:close(Hdl),
    State #qistate { cur_seg_num = undefined, cur_seg_hdl = undefined };
close_file_handle_for_seg(_SegNum, State) ->
    State.

get_file_handle_for_seg(SegNum, State = #qistate { cur_seg_num = SegNum,
                                                   cur_seg_hdl = Hdl }) ->
    {Hdl, State};
get_file_handle_for_seg(SegNum, State = #qistate { cur_seg_num = CurSegNum }) ->
    State1 = #qistate { dir = Dir } =
        close_file_handle_for_seg(CurSegNum, State),
    {ok, Hdl} = file:open(seg_num_to_path(Dir, SegNum),
                          [binary, raw, write, delayed_write, read]),
    {ok, _} = file:position(Hdl, {eof, 0}),
    {Hdl, State1 #qistate { cur_seg_num = SegNum, cur_seg_hdl = Hdl}}.

bool_to_int(true ) -> 1;
bool_to_int(false) -> 0.

seq_id_to_seg_and_rel_seq_id(SeqId) ->
    { SeqId div ?SEGMENT_ENTRIES_COUNT, SeqId rem ?SEGMENT_ENTRIES_COUNT }.

reconstruct_seq_id(SegNum, RelSeq) ->
    (SegNum * ?SEGMENT_ENTRIES_COUNT) + RelSeq.

seg_num_to_path(Dir, SegNum) ->
    SegName = integer_to_list(SegNum),
    filename:join(Dir, SegName ++ ?SEGMENT_EXTENSION).    


%%----------------------------------------------------------------------------
%% Startup Functions
%%----------------------------------------------------------------------------

all_segment_nums_paths(Dir) ->
    [{list_to_integer(
        lists:takewhile(fun(C) -> $0 =< C andalso C =< $9 end,
                        SegName)), filename:join(Dir, SegName)}
     || SegName <- filelib:wildcard("*" ++ ?SEGMENT_EXTENSION, Dir)].

find_ack_counts(Dir) ->
    SegNumsPaths = all_segment_nums_paths(Dir),
    lists:foldl(
      fun ({SegNum, SegPath}, {AccCount, AccDict}) ->
              {SDict, AckCount, _HighRelSeq} =
                  load_segment(SegNum, SegPath, dict:new()),
              {dict:size(SDict) + AccCount,
               case AckCount of
                   0 -> AccDict;
                   _ -> dict:store(SegNum, AckCount, AccDict)
               end}
      end, {0, dict:new()}, SegNumsPaths).

scatter_journal(Dir, {TotalMsgCount, AckCounts}) ->
    JournalPath = filename:join(Dir, ?ACK_JOURNAL_FILENAME),
    case file:open(JournalPath, [read, read_ahead, raw, binary]) of
        {error, enoent} -> AckCounts;
        {ok, Hdl} ->
            ADict = load_journal(Hdl, dict:new()),
            ok = file:close(Hdl),
            {AckCounts1, TotalMsgCount1, _Dir} =
                dict:fold(fun replay_journal_acks_to_segment/3,
                          {AckCounts, TotalMsgCount, Dir}, ADict),
            ok = file:delete(JournalPath),
            {AckCounts1, TotalMsgCount1}
    end.

load_journal(Hdl, ADict) ->
    case file:read(Hdl, ?SEQ_BYTES) of
        {ok, <<SeqId:?SEQ_BITS>>} ->
            load_journal(Hdl, add_ack_to_ack_dict(SeqId, ADict));
        _ErrOrEoF -> ADict
    end.

add_ack_to_ack_dict(SeqId, ADict) ->
    {SegNum, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    dict:update(SegNum, fun(Lst) -> [RelSeq|Lst] end, [RelSeq], ADict).

replay_journal_acks_to_segment(SegNum, Acks, {AckCounts, TotalMsgCount, Dir}) ->
    SegPath = seg_num_to_path(Dir, SegNum),
    {SDict, _AckCount, _HighRelSeq} = load_segment(SegNum, SegPath, dict:new()),
    ValidRelSeqIds = dict:fetch_keys(SDict),
    ValidAcks = sets:intersection(sets:from_list(ValidRelSeqIds),
                                  sets:from_list(Acks)),
    {append_acks_to_segment(SegPath, SegNum, AckCounts,
                            sets:to_list(ValidAcks)),
     TotalMsgCount - sets:size(ValidAcks), Dir}.


%%----------------------------------------------------------------------------
%% Loading Segments
%%----------------------------------------------------------------------------

load_segment(SegNum, SegPath, JAckDict) ->
    case file:open(SegPath, [raw, binary, read_ahead, read]) of
        {error, enoent} -> {dict:new(), 0, 0};
        {ok, Hdl} ->
            {SDict, AckCount, HighRelSeq} =
                load_segment_entries(SegNum, Hdl, {dict:new(), 0, 0}),
            ok = file:close(Hdl),
            RelSeqs = case dict:find(SegNum, JAckDict) of
                        {ok, RelSeqs1} -> RelSeqs1;
                        error -> []
                      end,
            {SDict1, AckCount1} =
                lists:foldl(fun (RelSeq, {SDict2, AckCount2}) ->
                                    {dict:erase(RelSeq, SDict2), AckCount2 + 1}
                            end, {SDict, AckCount}, RelSeqs),
            {SDict1, AckCount1, HighRelSeq}
    end.

load_segment_entries(SegNum, Hdl, {SDict, AckCount, HighRelSeq}) ->
    case file:read(Hdl, 1) of
        {ok, <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
               MSB/bitstring>>} ->
            {ok, LSB} = file:read(Hdl, ?REL_SEQ_ONLY_ENTRY_LENGTH_BYTES - 1),
            <<RelSeq:?REL_SEQ_BITS>> = <<MSB/bitstring, LSB/binary>>,
            {SDict1, AckCount1} = deliver_or_ack_msg(SDict, AckCount, RelSeq),
            load_segment_entries(SegNum, Hdl, {SDict1, AckCount1, HighRelSeq});
        {ok, <<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
               IsPersistentNum:1, MSB/bitstring>>} ->
            %% because we specify /binary, and binaries are complete
            %% bytes, the size spec is in bytes, not bits.
            {ok, <<LSB:1/binary, MsgId:?MSG_ID_BYTES/binary>>} =
                file:read(Hdl, ?PUBLISH_RECORD_LENGTH_BYTES - 1),
            <<RelSeq:?REL_SEQ_BITS>> = <<MSB/bitstring, LSB/binary>>,
            HighRelSeq1 = lists:max([RelSeq, HighRelSeq]),
            load_segment_entries(
              SegNum, Hdl, {dict:store(RelSeq, {MsgId, false,
                                                1 == IsPersistentNum},
                                       SDict), AckCount, HighRelSeq1});
        _ErrOrEoF -> {SDict, AckCount, HighRelSeq}
    end.

deliver_or_ack_msg(SDict, AckCount, RelSeq) ->
    case dict:find(RelSeq, SDict) of
        {ok, {MsgId, false, IsPersistent}} ->
            {dict:store(RelSeq, {MsgId, true, IsPersistent}, SDict), AckCount};
        {ok, {_MsgId, true, _IsPersistent}} ->
            {dict:erase(RelSeq, SDict), AckCount + 1}
    end.


%%----------------------------------------------------------------------------
%% Appending Acks to Segments
%%----------------------------------------------------------------------------

append_acks_to_segment(SegPath, SegNum, AckCounts, Acks) ->
    AckCount = case dict:find(SegNum, AckCounts) of
                   {ok, AckCount1} -> AckCount1;
                   error           -> 0
               end,
    case append_acks_to_segment(SegPath, AckCount, Acks) of
        0 -> AckCounts;
        ?SEGMENT_ENTRIES_COUNT -> dict:erase(SegNum, AckCounts);
        AckCount2 -> dict:store(SegNum, AckCount2, AckCounts)
    end.

append_acks_to_segment(SegPath, AckCount, Acks)
  when length(Acks) + AckCount == ?SEGMENT_ENTRIES_COUNT ->
    ok = case file:delete(SegPath) of
             ok -> ok;
             {error, enoent} -> ok
         end,
    ?SEGMENT_ENTRIES_COUNT;
append_acks_to_segment(SegPath, AckCount, Acks)
  when length(Acks) + AckCount < ?SEGMENT_ENTRIES_COUNT ->
    {ok, Hdl} = file:open(SegPath, [raw, binary, delayed_write, write, read]),
    {ok, _} = file:position(Hdl, {eof, 0}),
    AckCount1 =
        lists:foldl(
          fun (RelSeq, AckCount2) ->
                  ok = file:write(Hdl,
                                  <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                                   RelSeq:?REL_SEQ_BITS>>),
                  AckCount2 + 1
          end, AckCount, Acks),
    ok = file:sync(Hdl),
    ok = file:close(Hdl),
    AckCount1.
