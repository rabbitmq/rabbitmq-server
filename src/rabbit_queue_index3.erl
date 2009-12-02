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

-module(rabbit_queue_index3).

-export([init/1, terminate/1, terminate_and_erase/1, write_published/4,
         write_delivered/2, write_acks/2, sync_seq_ids/3, flush_journal/1,
         read_segment_entries/2, next_segment_boundary/1, segment_size/0,
         find_lowest_seq_id_seg_and_next_seq_id/1, start_msg_store/1]).

-define(CLEAN_FILENAME, "clean.dot").

%%----------------------------------------------------------------------------
%% ---- Journal details ----

-define(MAX_JOURNAL_ENTRY_COUNT, 32768).
-define(JOURNAL_FILENAME, "journal.jif").

-define(PUB_PERSIST_JPREFIX, 00).
-define(PUB_TRANS_JPREFIX,   01).
-define(DEL_JPREFIX,         10).
-define(ACK_JPREFIX,         11).
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

init(Name) ->
    State = blank_state(Name),
    %% 1. Load the journal completely. This will also load segments
    %%    which have entries in the journal and remove duplicates.
    %%    The counts will correctly reflect the combination of the
    %%    segment and the journal.
    State1 = load_journal(State),
    %% 2. Flush the journal. This makes life easier for everyone, as
    %%    it means there won't be any publishes in the journal alone.
    State2 = #qistate { dir = Dir } = flush_journal(State1),
    %% 3. Load each segment in turn and filter out messages that are
    %%    not in the msg_store, by adding acks to the journal. These
    %%    acks only go to the RAM journal as it doesn't matter if we
    %%    lose them. Also mark delivered if not clean shutdown.
    AllSegs = all_segment_nums(Dir),
    CleanShutdown = detect_clean_shutdown(Dir),
    %% We know the journal is empty here, so we don't need to combine
    %% with the journal, and we don't need to worry about messages
    %% that have been acked.
    State3 =
        lists:foldl(
          fun (Seg, StateN) ->
                  {SegDict, _PubCount, _AckCount, StateN1} =
                      load_segment(Seg, false, StateN),
                  dict:fold(
                    fun (RelSeq, {{MsgId, _IsPersistent}, Del, no_ack},
                         StateM) ->
                            SeqId = reconstruct_seq_id(Seg, RelSeq),
                            InMsgStore = rabbit_msg_store:contains(MsgId),
                            case {InMsgStore, CleanShutdown} of
                                {true, true} ->
                                    StateM;
                                {true, false} when Del == del ->
                                    StateM;
                                {true, false} ->
                                    add_to_journal(SeqId, del, StateM);
                                {false, _} when Del == del ->
                                    add_to_journal(SeqId, ack, StateM);
                                {false, _} ->
                                    add_to_journal(
                                      SeqId, ack,
                                      add_to_journal(SeqId, del, StateM))
                            end
                    end, StateN1, SegDict)
          end, State2, AllSegs),
    %% 4. Go through all segments and calculate the number of unacked
    %%    messages we have.
    Count = lists:foldl(
              fun (Seg, CountAcc) ->
                      #segment { pubs = PubCount, acks = AckCount } =
                          find_segment(Seg, State3),
                      CountAcc + PubCount - AckCount
              end, 0, AllSegs),
    {Count, State3}.

terminate(State = #qistate { segments = Segments, journal_handle = JournalHdl,
                             dir = Dir }) ->
    ok = case JournalHdl of
             undefined -> ok;
             _         -> file_handle_cache:close(JournalHdl)
         end,
    ok = dict:fold(
           fun (_Seg, #segment { handle = undefined }, ok) ->
                   ok;
               (_Seg, #segment { handle = Hdl }, ok) ->
                   file_handle_cache:close(Hdl)
           end, ok, Segments),
    store_clean_shutdown(Dir),
    State #qistate { journal_handle = undefined, segments = dict:new() }.

terminate_and_erase(State) ->
    State1 = terminate(State),
    ok = delete_queue_directory(State1 #qistate.dir),
    State1.

flush_journal(State = #qistate { dirty_count = 0 }) ->
    State;
flush_journal(State = #qistate { segments = Segments }) ->
    dict:fold(
      fun (_Seg, #segment { journal_entries = JEntries, pubs = PubCount,
                            acks = AckCount } = Segment, StateN) ->
              case dict:is_empty(JEntries) of
                  true -> store_segment(Segment, StateN);
                  false when AckCount == PubCount ->
                      ok = delete_segment(Segment);
                  false ->
                      {Hdl, Segment1} = get_segment_handle(Segment),
                      dict:fold(fun write_entry_to_segment/3,
                                Hdl, JEntries),
                      ok = file_handle_cache:sync(Hdl),
                      store_segment(
                        Segment1 #segment { journal_entries = dict:new() },
                        StateN)
              end
      end, State, Segments).

read_segment_entries(InitSeqId, State) ->
    {Seg, 0} = seq_id_to_seg_and_rel_seq_id(InitSeqId),
    {SegDict, _PubCount, _AckCount, State1} =
        load_segment(Seg, false, State),
    #segment { journal_entries = JEntries } = find_segment(Seg, State1),
    SegDict1 = journal_plus_segment(JEntries, SegDict),
    %% deliberately sort the list desc, because foldl will reverse it
    RelSeqs = rev_sort(dict:fetch_keys(SegDict1)),
    {lists:foldl(fun (RelSeq, Acc) ->
                         {{MsgId, IsPersistent}, IsDelivered, no_ack} =
                             dict:fetch(RelSeq, SegDict1),
                         [ {MsgId, reconstruct_seq_id(Seg, RelSeq),
                            IsPersistent, IsDelivered} | Acc ]
                 end, [], RelSeqs),
     State1}.

next_segment_boundary(SeqId) ->
    {Seg, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    reconstruct_seq_id(Seg + 1, 0).

segment_size() ->
    ?SEGMENT_ENTRY_COUNT.

find_lowest_seq_id_seg_and_next_seq_id(State = #qistate { dir = Dir }) ->
    SegNums = all_segment_nums(Dir),
    %% We don't want the lowest seq_id, merely the seq_id of the start
    %% of the lowest segment. That seq_id may not actually exist, but
    %% that's fine. The important thing is that the segment exists and
    %% the seq_id reported is on a segment boundary.

    %% We also don't really care about the max seq_id. Just start the
    %% next segment: it makes life much easier.

    %% SegNums is sorted, ascending.
    {LowSeqIdSeg, NextSeqId} =
        case SegNums of
            []            -> {0, 0};
            [MinSeg|_] -> {reconstruct_seq_id(MinSeg, 0),
                           reconstruct_seq_id(lists:last(SegNums), 0)}
        end,
    {LowSeqIdSeg, NextSeqId, State}.

%%----------------------------------------------------------------------------
%% Minors
%%----------------------------------------------------------------------------

all_segment_nums(Dir) ->
    lists:sort(
      [list_to_integer(
         lists:takewhile(fun(C) -> $0 =< C andalso C =< $9 end, SegName))
       || SegName <- filelib:wildcard("*" ++ ?SEGMENT_EXTENSION, Dir)]).

blank_state(QueueName) ->
    StrName = queue_name_to_dir_name(QueueName),
    Dir = filename:join(queues_dir(), StrName),
    ok = filelib:ensure_dir(filename:join(Dir, "nothing")),
    #qistate { dir            = Dir,
               segments       = dict:new(),
               journal_handle = undefined,
               dirty_count    = 0
             }.

rev_sort(List) ->
    lists:sort(fun (A, B) -> B < A end, List).

seq_id_to_seg_and_rel_seq_id(SeqId) ->
    { SeqId div ?SEGMENT_ENTRY_COUNT, SeqId rem ?SEGMENT_ENTRY_COUNT }.

reconstruct_seq_id(Seg, RelSeq) ->
    (Seg * ?SEGMENT_ENTRY_COUNT) + RelSeq.

seg_num_to_path(Dir, Seg) ->
    SegName = integer_to_list(Seg),
    filename:join(Dir, SegName ++ ?SEGMENT_EXTENSION).

delete_segment(#segment { handle = undefined }) ->
    ok;
delete_segment(#segment { handle = Hdl, path = Path }) ->
    ok = file_handle_cache:close(Hdl),
    ok = file:delete(Path),
    ok.

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

find_segment(Seg, #qistate { segments = Segments, dir = Dir }) ->
    case dict:find(Seg, Segments) of
        {ok, Segment = #segment{}} -> Segment;
        error -> #segment { pubs = 0,
                            acks = 0,
                            handle = undefined,
                            journal_entries = dict:new(),
                            path = seg_num_to_path(Dir, Seg),
                            num = Seg
                          }
    end.

store_segment(Segment = #segment { num = Seg },
              State = #qistate { segments = Segments }) ->
    State #qistate { segments = dict:store(Seg, Segment, Segments) }.

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

%%----------------------------------------------------------------------------
%% Majors
%%----------------------------------------------------------------------------

%% Loading segments

%% Does not do any combining with the journal at all. The PubCount
%% that comes back is the number of publishes in the segment. The
%% number of unacked msgs is PubCount - AckCount. If KeepAcks is
%% false, then dict:size(SegDict) == PubCount - AckCount. If KeepAcks
%% is true, then dict:size(SegDict) == PubCount.
load_segment(Seg, KeepAcks, State) ->
    Segment = #segment { path = Path, handle = SegHdl } =
        find_segment(Seg, State),
    SegmentExists = case SegHdl of
                        undefined -> filelib:is_file(Path);
                        _         -> true
                    end,
    case SegmentExists of
        false ->
            {dict:new(), 0, 0, State};
        true ->
            {Hdl, Segment1} = get_segment_handle(Segment),
            {ok, 0} = file_handle_cache:position(Hdl, bof),
            {SegDict, PubCount, AckCount} =
                load_segment_entries(KeepAcks, Hdl, dict:new(), 0, 0),
            {SegDict, PubCount, AckCount, store_segment(Segment1, State)}
    end.

load_segment_entries(KeepAcks, Hdl, SegDict, PubCount, AckCount) ->
    case file_handle_cache:read(Hdl, 1) of
        {ok, <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                MSB:(8-?REL_SEQ_ONLY_PREFIX_BITS)>>} ->
            {ok, LSB} = file_handle_cache:read(
                          Hdl, ?REL_SEQ_ONLY_ENTRY_LENGTH_BYTES - 1),
            <<RelSeq:?REL_SEQ_BITS_BYTE_ALIGNED>> = <<MSB, LSB/binary>>,
            {AckCount1, SegDict1} =
                deliver_or_ack_msg(KeepAcks, RelSeq, AckCount, SegDict),
            load_segment_entries(KeepAcks, Hdl, SegDict1, PubCount, AckCount1);
        {ok, <<?PUBLISH_PREFIX:?PUBLISH_PREFIX_BITS,
                IsPersistentNum:1, MSB:(7-?PUBLISH_PREFIX_BITS)>>} ->
            %% because we specify /binary, and binaries are complete
            %% bytes, the size spec is in bytes, not bits.
            {ok, <<LSB:1/binary, MsgId:?MSG_ID_BYTES/binary>>} =
                file_handle_cache:read(
                  Hdl, ?PUBLISH_RECORD_LENGTH_BYTES - 1),
            <<RelSeq:?REL_SEQ_BITS_BYTE_ALIGNED>> = <<MSB, LSB/binary>>,
            SegDict1 =
                dict:store(RelSeq,
                           {{MsgId, 1 == IsPersistentNum}, no_del, no_ack},
                           SegDict),
            load_segment_entries(KeepAcks, Hdl, SegDict1, PubCount+1, AckCount);
        _ErrOrEoF ->
            {SegDict, PubCount, AckCount}
    end.

deliver_or_ack_msg(KeepAcks, RelSeq, AckCount, SegDict) ->
    case dict:find(RelSeq, SegDict) of
        {ok, {PubRecord, no_del, no_ack}} ->
            {AckCount, dict:store(RelSeq, {PubRecord, del, no_ack}, SegDict)};
        {ok, {PubRecord, del, no_ack}} when KeepAcks ->
            {AckCount + 1, dict:store(RelSeq, {PubRecord, del, ack}, SegDict)};
        {ok, {_PubRecord, del, no_ack}} when KeepAcks ->
            {AckCount + 1, dict:erase(RelSeq, SegDict)}
    end.

%% Loading Journal. This isn't idempotent and will mess up the counts
%% if you call it more than once on the same state. Assumes the counts
%% are 0 to start with.

load_journal(State) ->
    {JournalHdl, State1} = get_journal_handle(State),
    {ok, 0} = file_handle_cache:position(JournalHdl, 0),
    State1 = #qistate { segments = Segments } = load_journal_entries(State),
    dict:fold(
      fun (Seg, #segment { journal_entries = JEntries,
                           pubs = PubCountInJournal,
                           acks = AckCountInJournal }, StateN) ->
              %% We want to keep acks in so that we can remove them if
              %% duplicates are in the journal. The counts here are
              %% purely from the segment itself.
              {SegDict, PubCount, AckCount, StateN1} =
                  load_segment(Seg, true, StateN),
              %% Removed counts here are the number of pubs and acks
              %% that are duplicates - i.e. found in both the segment
              %% and journal.
              {JEntries1, PubsRemoved, AcksRemoved} =
                  journal_minus_segment(JEntries, SegDict),
              {Segment1, StateN2} = find_segment(Seg, StateN1),
              PubCount1 = PubCount + PubCountInJournal - PubsRemoved,
              AckCount1 = AckCount + AckCountInJournal - AcksRemoved,
              store_segment(Segment1 #segment { journal_entries = JEntries1,
                                                pubs = PubCount1,
                                                acks = AckCount1 }, StateN2)
      end, State1, Segments).

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

add_to_journal(SeqId, Action, State = #qistate { dirty_count = DCount }) ->
    {Seg, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    Segment = #segment { journal_entries = SegJDict,
                         pubs = PubCount, acks = AckCount } =
        find_segment(Seg, State),
    SegJDict1 = add_to_journal(RelSeq, Action, SegJDict),
    Segment1 = Segment #segment { journal_entries = SegJDict1 },
    Segment2 =
        case Action of
            del                     -> Segment1;
            ack                     -> Segment1 #segment { acks = AckCount + 1 };
            {_MsgId, _IsPersistent} -> Segment1 #segment { pubs = PubCount + 1 }
        end,
    store_segment(Segment2, State #qistate { dirty_count = DCount + 1 });

%% This is a more relaxed version of deliver_or_ack_msg because we can
%% have dels or acks in the journal without the corresponding
%% pub. Also, always want to keep acks. Things must occur in the right
%% order though.
add_to_journal(RelSeq, Action, SegJDict) ->
    case dict:find(RelSeq, SegJDict) of
        {ok, {PubRecord, no_del, no_ack}} when Action == del ->
            dict:store(RelSeq, {PubRecord, del, no_ack}, SegJDict);
        {ok, {PubRecord, DelRecord, no_ack}} when Action == ack ->
            dict:store(RelSeq, {PubRecord, DelRecord, ack}, SegJDict);
        error when Action == del ->
            dict:store(RelSeq, {no_pub, del, no_ack}, SegJDict);
        error when Action == ack ->
            dict:store(RelSeq, {no_pub, no_del, ack}, SegJDict);
        error ->
            {_MsgId, _IsPersistent} = Action, %% ASSERTION
            dict:store(RelSeq, {Action, no_del, no_ack}, SegJDict)
    end.

%% Combine what we have just read from a segment file with what we're
%% holding for that segment in memory. There must be no
%% duplicates. Used when providing segment entries to the variable
%% queue.
journal_plus_segment(JEntries, SegDict) ->
    dict:fold(fun (RelSeq, JObj, SegDictOut) ->
                      SegEntry = case dict:find(RelSeq, SegDictOut) of
                                     error -> not_found;
                                     {ok, SObj = {_, _, _}} -> SObj
                                 end,
                      journal_plus_segment(JObj, SegEntry, RelSeq, SegDictOut)
              end, SegDict, JEntries).

%% Here, the OutDict is the SegDict which we may be adding to (for
%% items only in the journal), modifying (bits in both), or erasing
%% from (ack in journal, not segment).
journal_plus_segment(Obj = {{_MsgId, _IsPersistent}, no_del, no_ack},
                     not_found,
                     RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);
journal_plus_segment(Obj = {{_MsgId, _IsPersistent}, del, no_ack},
                     not_found,
                     RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);
journal_plus_segment({{_MsgId, _IsPersistent}, del, ack},
                     not_found,
                     RelSeq, OutDict) ->
    dict:erase(RelSeq, OutDict);

journal_plus_segment({no_pub, del, no_ack},
                     {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                     RelSeq, OutDict) ->
    dict:store(RelSeq, {PubRecord, del, no_ack}, OutDict);

journal_plus_segment({no_pub, del, ack},
                     {{_MsgId, _IsPersistent}, no_del, no_ack},
                     RelSeq, OutDict) ->
    dict:erase(RelSeq, OutDict);
journal_plus_segment({no_pub, no_del, ack},
                     {{_MsgId, _IsPersistent}, del, no_ack},
                     RelSeq, OutDict) ->
    dict:erase(RelSeq, OutDict).


%% Remove from the journal entries for a segment, items that are
%% duplicates of entries found in the segment itself. Used on start up
%% to clean up the journal.
journal_minus_segment(JEntries, SegDict) ->
    dict:fold(fun (RelSeq, JObj, {JEntriesOut, PubsRemoved, AcksRemoved}) ->
                      SegEntry = case dict:find(RelSeq, SegDict) of
                                     error -> not_found;
                                     {ok, SObj = {_, _, _}} -> SObj
                                 end,
                      journal_minus_segment(JObj, SegEntry, RelSeq, JEntriesOut,
                                            PubsRemoved, AcksRemoved)
              end, {dict:new(), 0, 0}, JEntries).

%% Here, the OutDict is a fresh journal that we're filling with valid
%% entries. PubsRemoved and AcksRemoved only get increased when the a
%% publish or ack is in both the journal and the segment.

%% Both the same. Must be at least the publish
journal_minus_segment(Obj, Obj = {{_MsgId, _IsPersistent}, _Del, no_ack},
                      _RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {OutDict, PubsRemoved + 1, AcksRemoved};
journal_minus_segment(Obj, Obj = {{_MsgId, _IsPersistent}, _Del, ack},
                      _RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {OutDict, PubsRemoved + 1, AcksRemoved + 1};

%% Just publish in journal
journal_minus_segment(Obj = {{_MsgId, _IsPersistent}, no_del, no_ack},
                      not_found,
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, Obj, OutDict), PubsRemoved, AcksRemoved};

%% Just deliver in journal
journal_minus_segment(Obj = {no_pub, del, no_ack},
                      {{_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, Obj, OutDict), PubsRemoved, AcksRemoved};
journal_minus_segment({no_pub, del, no_ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      _RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {OutDict, PubsRemoved, AcksRemoved};

%% Just ack in journal
journal_minus_segment(Obj = {no_pub, no_del, ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, Obj, OutDict), PubsRemoved, AcksRemoved};
journal_minus_segment({no_pub, no_del, ack},
                      {{_MsgId, _IsPersistent}, del, ack},
                      _RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {OutDict, PubsRemoved, AcksRemoved};

%% Publish and deliver in journal
journal_minus_segment(Obj = {{_MsgId, _IsPersistent}, del, no_ack},
                      not_found,
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, Obj, OutDict), PubsRemoved, AcksRemoved};
journal_minus_segment({PubRecord, del, no_ack},
                      {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, {no_pub, del, no_ack}, OutDict),
     PubsRemoved + 1, AcksRemoved};

%% Deliver and ack in journal
journal_minus_segment(Obj = {no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, Obj, OutDict), PubsRemoved, AcksRemoved};
journal_minus_segment({no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, {no_pub, no_del, ack}, OutDict),
     PubsRemoved, AcksRemoved};
journal_minus_segment({no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, del, ack},
                      _RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {OutDict, PubsRemoved, AcksRemoved + 1};

%% Publish, deliver and ack in journal
journal_minus_segment({{_MsgId, _IsPersistent}, del, ack},
                      not_found,
                      _RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {OutDict, PubsRemoved, AcksRemoved};
journal_minus_segment({PubRecord, del, ack},
                      {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, {no_pub, del, ack}, OutDict),
     PubsRemoved + 1, AcksRemoved};
journal_minus_segment({PubRecord, del, ack},
                      {PubRecord = {_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, OutDict, PubsRemoved, AcksRemoved) ->
    {dict:store(RelSeq, {no_pub, no_del, ack}, OutDict),
     PubsRemoved + 1, AcksRemoved}.
