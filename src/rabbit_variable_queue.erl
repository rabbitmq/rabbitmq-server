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

-module(rabbit_variable_queue).

-export([init/1, terminate/1, publish/2, publish_delivered/2,
         set_queue_ram_duration_target/2, remeasure_rates/1,
         ram_duration/1, fetch/1, ack/2, len/1, is_empty/1, purge/1, delete/1,
         requeue/2, tx_publish/2, tx_rollback/2, tx_commit/4,
         tx_commit_from_msg_store/4, tx_commit_from_vq/1, needs_sync/1,
         flush_journal/1, status/1]).

%%----------------------------------------------------------------------------

-record(vqstate,
        { q1,
          q2,
          delta,
          q3,
          q4,
          duration_target,
          target_ram_msg_count,
          ram_msg_count,
          ram_msg_count_prev,
          ram_index_count,
          index_state,
          next_seq_id,
          out_counter,
          in_counter,
          egress_rate,
          avg_egress_rate,
          ingress_rate,
          avg_ingress_rate,
          rate_timestamp,
          len,
          on_sync,
          msg_store_read_state
        }).

-include("rabbit.hrl").
-include("rabbit_queue.hrl").

-record(msg_status,
        { msg,
          msg_id,
          seq_id,
          is_persistent,
          is_delivered,
          msg_on_disk,
          index_on_disk
        }).

-define(RAM_INDEX_TARGET_RATIO, 32768).

%%----------------------------------------------------------------------------

%% WRONG - UPDATE ME!

%% Basic premise is that msgs move from q1 -> q2 -> delta -> q3 -> q4
%% but they can only do so in the right form. q1 and q4 only hold
%% alphas (msgs in ram), q2 and q3 only hold betas (msg on disk, index
%% in ram), and delta is just a count of the number of index entries
%% on disk at that stage (msg on disk, index on disk).
%%
%% When a msg arrives, we decide in which form it should be. It is
%% then added to the right-most appropriate queue, maintaining
%% order. Thus if the msg is to be an alpha, it will be added to q1,
%% unless all of q2, delta and q3 are empty, in which case it will go
%% to q4. If it is to be a beta, it will be added to q2 unless delta
%% is empty, in which case it will go to q3.
%%
%% The major invariant is that if the msg is to be a beta, q1 will be
%% empty, and if it is to be a delta then both q1 and q2 will be empty.
%%
%% When taking msgs out of the queue, if q4 is empty then we read
%% directly from q3, or delta, if q3 is empty. If q3 and delta are
%% empty then we have an invariant that q2 must be empty because q2
%% can only grow if delta is non empty.
%%
%% A further invariant is that if the queue is non empty, either q4 or
%% q3 contains at least one entry. I.e. we never allow delta to
%% contain all msgs in the queue.  Also, if q4 is non empty and delta
%% is non empty then q3 must be non empty.

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(msg_id()  :: binary()).
-type(seq_id()  :: non_neg_integer()).
-type(ack()     :: {'ack_index_and_store', msg_id(), seq_id()}
                 | 'ack_not_on_disk').
-type(vqstate() :: #vqstate {
               q1                    :: queue(),
               q2                    :: {non_neg_integer(), queue()},
               delta                 :: delta(),
               q3                    :: {non_neg_integer(), queue()},
               q4                    :: queue(),
               duration_target       :: non_neg_integer(),
               target_ram_msg_count  :: non_neg_integer(),
               ram_msg_count         :: non_neg_integer(),
               ram_msg_count_prev    :: non_neg_integer(),
               ram_index_count       :: non_neg_integer(),
               index_state           :: any(),
               next_seq_id           :: seq_id(),
               out_counter           :: non_neg_integer(),
               in_counter            :: non_neg_integer(),
               egress_rate           :: {{integer(), integer(), integer()}, non_neg_integer()},
               avg_egress_rate       :: float(),
               ingress_rate          :: {{integer(), integer(), integer()}, non_neg_integer()},
               avg_ingress_rate      :: float(),
               rate_timestamp        :: {integer(), integer(), integer()},
               len                   :: non_neg_integer(),
               on_sync               :: {[ack()], [msg_id()], [{pid(), any()}]},
               msg_store_read_state  :: any()
              }).

-spec(init/1 :: (queue_name()) -> vqstate()).
-spec(terminate/1 :: (vqstate()) -> vqstate()).
-spec(publish/2 :: (basic_message(), vqstate()) ->
             {seq_id(), vqstate()}).
-spec(publish_delivered/2 :: (basic_message(), vqstate()) ->
             {ack(), vqstate()}).
-spec(set_queue_ram_duration_target/2 ::
      (('undefined' | 'infinity' | number()), vqstate()) -> vqstate()).
-spec(remeasure_rates/1 :: (vqstate()) -> vqstate()).
-spec(ram_duration/1 :: (vqstate()) -> number()).
-spec(fetch/1 :: (vqstate()) ->
             {('empty'|{basic_message(), boolean(), ack(), non_neg_integer()}),
              vqstate()}).
-spec(ack/2 :: ([ack()], vqstate()) -> vqstate()).
-spec(len/1 :: (vqstate()) -> non_neg_integer()).
-spec(is_empty/1 :: (vqstate()) -> boolean()).
-spec(purge/1 :: (vqstate()) -> {non_neg_integer(), vqstate()}).
-spec(delete/1 :: (vqstate()) -> vqstate()).
-spec(requeue/2 :: ([{basic_message(), ack()}], vqstate()) -> vqstate()).
-spec(tx_publish/2 :: (basic_message(), vqstate()) -> vqstate()).
-spec(tx_rollback/2 :: ([msg_id()], vqstate()) -> vqstate()).
-spec(tx_commit/4 :: ([msg_id()], [ack()], {pid(), any()}, vqstate()) ->
             {boolean(), vqstate()}).
-spec(tx_commit_from_msg_store/4 ::
      ([msg_id()], [ack()], {pid(), any()}, vqstate()) -> vqstate()).
-spec(tx_commit_from_vq/1 :: (vqstate()) -> vqstate()).
-spec(needs_sync/1 :: (vqstate()) -> boolean()).
-spec(flush_journal/1 :: (vqstate()) -> vqstate()).
-spec(status/1 :: (vqstate()) -> [{atom(), any()}]).

-endif.

-define(BLANK_DELTA, #delta { start_seq_id = undefined,
                              count = 0,
                              end_seq_id = undefined }).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

init(QueueName) ->
    {DeltaCount, IndexState} =
        rabbit_queue_index:init(QueueName),
    {DeltaSeqId, NextSeqId, IndexState1} =
        rabbit_queue_index:find_lowest_seq_id_seg_and_next_seq_id(IndexState),
    Delta = case DeltaCount of
                0 -> ?BLANK_DELTA;
                _ -> #delta { start_seq_id = DeltaSeqId,
                              count = DeltaCount,
                              end_seq_id = NextSeqId }
            end,
    Now = now(),
    State =
        #vqstate { q1 = queue:new(), q2 = {0, queue:new()},
                   delta = Delta,
                   q3 = {0, queue:new()}, q4 = queue:new(),
                   duration_target = undefined,
                   target_ram_msg_count = undefined,
                   ram_msg_count = 0,
                   ram_msg_count_prev = 0,
                   ram_index_count = 0,
                   index_state = IndexState1,
                   next_seq_id = NextSeqId,
                   out_counter = 0,
                   in_counter = 0,
                   egress_rate = {Now, 0},
                   avg_egress_rate = 0,
                   ingress_rate = {Now, DeltaCount},
                   avg_ingress_rate = 0,
                   rate_timestamp = Now,
                   len = DeltaCount,
                   on_sync = {[], [], []},
                   msg_store_read_state = rabbit_msg_store:client_init()
                  },
    maybe_deltas_to_betas(State).

terminate(State = #vqstate { index_state = IndexState,
                             msg_store_read_state = MSCState }) ->
    rabbit_msg_store:client_terminate(MSCState),
    State #vqstate { index_state = rabbit_queue_index:terminate(IndexState) }.

publish(Msg, State) ->
    publish(Msg, false, false, State).

publish_delivered(Msg = #basic_message { guid = MsgId,
                                         is_persistent = IsPersistent },
                  State = #vqstate { len = 0, index_state = IndexState,
                                     next_seq_id = SeqId,
                                     out_counter = OutCount,
                                     in_counter = InCount}) ->
    State1 = State #vqstate { out_counter = OutCount + 1,
                              in_counter = InCount + 1 },
    MsgStatus = #msg_status {
      msg = Msg, msg_id = MsgId, seq_id = SeqId, is_persistent = IsPersistent,
      is_delivered = false, msg_on_disk = false, index_on_disk = false },
    MsgStatus1 = maybe_write_msg_to_disk(false, MsgStatus),
    case MsgStatus1 #msg_status.msg_on_disk of
        true ->
            {#msg_status { index_on_disk = true }, IndexState1} =
                maybe_write_index_to_disk(false, MsgStatus1, IndexState),
            {{ack_index_and_store, MsgId, SeqId},
             State1 #vqstate { index_state = IndexState1,
                               next_seq_id = SeqId + 1 }};
        false ->
            {ack_not_on_disk, State1}
    end.

set_queue_ram_duration_target(
  DurationTarget, State = #vqstate { avg_egress_rate = AvgEgressRate,
                                     avg_ingress_rate = AvgIngressRate,
                                     target_ram_msg_count = TargetRamMsgCount
                                   }) ->
    Rate = AvgEgressRate + AvgIngressRate,
    TargetRamMsgCount1 =
        case DurationTarget of
            infinity -> undefined;
            undefined -> undefined;
            _ -> trunc(DurationTarget * Rate) %% msgs = sec * msgs/sec
        end,
    State1 = State #vqstate { target_ram_msg_count = TargetRamMsgCount1,
                              duration_target = DurationTarget },
    case TargetRamMsgCount1 == undefined orelse
        TargetRamMsgCount1 >= TargetRamMsgCount of
        true  -> State1;
        false -> reduce_memory_use(State1)
    end.

remeasure_rates(State = #vqstate { egress_rate = Egress,
                                   ingress_rate = Ingress,
                                   rate_timestamp = Timestamp,
                                   in_counter = InCount,
                                   out_counter = OutCount,
                                   ram_msg_count = RamMsgCount,
                                   duration_target = DurationTarget }) ->
    Now = now(),
    {AvgEgressRate, Egress1} = update_rate(Now, Timestamp, OutCount, Egress),
    {AvgIngressRate, Ingress1} = update_rate(Now, Timestamp, InCount, Ingress),

    set_queue_ram_duration_target(
      DurationTarget,
      State #vqstate { egress_rate = Egress1,
                       avg_egress_rate = AvgEgressRate,
                       ingress_rate = Ingress1,
                       avg_ingress_rate = AvgIngressRate,
                       rate_timestamp = Now,
                       ram_msg_count_prev = RamMsgCount,
                       out_counter = 0, in_counter = 0 }).

ram_duration(#vqstate { avg_egress_rate = AvgEgressRate,
                        avg_ingress_rate = AvgIngressRate,
                        ram_msg_count = RamMsgCount,
                        ram_msg_count_prev = RamMsgCountPrev }) ->
    %% msgs / (msgs/sec) == sec
    case AvgEgressRate == 0 andalso AvgIngressRate == 0 of
        true  -> infinity;
        false -> (RamMsgCountPrev + RamMsgCount) / (2 * (AvgEgressRate + AvgIngressRate))
    end.

fetch(State =
      #vqstate { q4 = Q4, ram_msg_count = RamMsgCount, out_counter = OutCount,
                 index_state = IndexState, len = Len }) ->
    case queue:out(Q4) of
        {empty, _Q4} ->
            fetch_from_q3_or_delta(State);
        {{value, #msg_status {
            msg = Msg, msg_id = MsgId, seq_id = SeqId,
            is_persistent = IsPersistent, is_delivered = IsDelivered,
            msg_on_disk = MsgOnDisk, index_on_disk = IndexOnDisk }},
         Q4a} ->
            {IndexState1, IndexOnDisk1} =
                case IndexOnDisk of
                    true ->
                        IndexState2 =
                            case IsDelivered of
                                false -> rabbit_queue_index:write_delivered(
                                           SeqId, IndexState);
                                true -> IndexState
                            end,
                        case IsPersistent of
                            true -> {IndexState2, true};
                            false -> {rabbit_queue_index:write_acks(
                                        [SeqId], IndexState2), false}
                        end;
                    false ->
                        {IndexState, false}
                end,
            AckTag =
                case IndexOnDisk1 of
                    true  -> true = IsPersistent, %% ASSERTION
                             true = MsgOnDisk, %% ASSERTION
                             {ack_index_and_store, MsgId, SeqId};
                    false -> ok = case MsgOnDisk andalso not IsPersistent of
                                      true -> rabbit_msg_store:remove([MsgId]);
                                      false -> ok
                                  end,
                             ack_not_on_disk
                end,
            Len1 = Len - 1,
            {{Msg, IsDelivered, AckTag, Len1},
             State #vqstate { q4 = Q4a, out_counter = OutCount + 1,
                              ram_msg_count = RamMsgCount - 1,
                              index_state = IndexState1, len = Len1 }}
    end.

ack(AckTags, State = #vqstate { index_state = IndexState }) ->
    {MsgIds, SeqIds} =
        lists:foldl(
          fun (ack_not_on_disk, Acc) -> Acc;
              ({ack_index_and_store, MsgId, SeqId}, {MsgIds, SeqIds}) ->
                  {[MsgId | MsgIds], [SeqId | SeqIds]}
          end, {[], []}, AckTags),
    IndexState1 = case SeqIds of
                      [] -> IndexState;
                      _  -> rabbit_queue_index:write_acks(SeqIds, IndexState)
                  end,
    ok = case MsgIds of
             [] -> ok;
             _  -> rabbit_msg_store:remove(MsgIds)
         end,
    State #vqstate { index_state = IndexState1 }.

len(#vqstate { len = Len }) ->
    Len.

is_empty(State) ->
    0 == len(State).

purge(State = #vqstate { q4 = Q4, index_state = IndexState, len = Len }) ->
    {Q4Count, IndexState1} = remove_queue_entries(Q4, IndexState),
    {Len, State1} =
        purge1(Q4Count, State #vqstate { index_state = IndexState1,
                                         q4 = queue:new() }),
    {Len, State1 #vqstate { len = 0, ram_msg_count = 0, ram_index_count = 0 }}.

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.
delete(State) ->
    {_PurgeCount, State1 = #vqstate { index_state = IndexState }} = purge(State),
    IndexState1 =
        case rabbit_queue_index:find_lowest_seq_id_seg_and_next_seq_id(
               IndexState) of
            {N, N, IndexState2} ->
                IndexState2;
            {DeltaSeqId, NextSeqId, IndexState2} ->
                {_DeleteCount, IndexState3} =
                    delete1(NextSeqId, 0, DeltaSeqId, IndexState2),
                IndexState3
    end,
    IndexState4 = rabbit_queue_index:terminate_and_erase(IndexState1),
    State1 #vqstate { index_state = IndexState4 }.

%% [{Msg, AckTag}]
%% We guarantee that after fetch, only persistent msgs are left on
%% disk. This means that in a requeue, we set MsgOnDisk to true, thus
%% avoiding calls to msg_store:write for persistent msgs. It also
%% means that we don't need to worry about calling msg_store:remove
%% (as ack would do) because transient msgs won't be on disk anyway,
%% thus they won't need to be removed. However, we do call
%% msg_store:release so that the cache isn't held full of msgs which
%% are now at the tail of the queue.
requeue(MsgsWithAckTags, State) ->
    {SeqIds, MsgIds, State1 = #vqstate { index_state = IndexState }} =
        lists:foldl(
          fun ({Msg = #basic_message { guid = MsgId }, AckTag},
               {SeqIdsAcc, MsgIdsAcc, StateN}) ->
                  {SeqIdsAcc1, MsgIdsAcc1, MsgOnDisk} =
                      case AckTag of
                          ack_not_on_disk ->
                              {SeqIdsAcc, MsgIdsAcc, false};
                          {ack_index_and_store, MsgId, SeqId} ->
                              {[SeqId | SeqIdsAcc], [MsgId | MsgIdsAcc], true}
                      end,
                  {_SeqId, StateN1} = publish(Msg, true, MsgOnDisk, StateN),
                  {SeqIdsAcc1, MsgIdsAcc1, StateN1}
          end, {[], [], State}, MsgsWithAckTags),
    IndexState1 = case SeqIds of
                      [] -> IndexState;
                      _  -> rabbit_queue_index:write_acks(SeqIds, IndexState)
                  end,
    ok = case MsgIds of
             [] -> ok;
             _  -> rabbit_msg_store:release(MsgIds)
         end,
    State1 #vqstate { index_state = IndexState1 }.

tx_publish(Msg = #basic_message { is_persistent = true, guid = MsgId },
           State) ->
    MsgStatus = #msg_status {
      msg = Msg, msg_id = MsgId, seq_id = undefined, is_persistent = true,
      is_delivered = false, msg_on_disk = false, index_on_disk = false },
    #msg_status { msg_on_disk = true } =
        maybe_write_msg_to_disk(false, MsgStatus),
    State;
tx_publish(_Msg, State) ->
    State.

tx_rollback(Pubs, State) ->
    ok = case persistent_msg_ids(Pubs) of
             [] -> ok;
             PP -> rabbit_msg_store:remove(PP)
         end,
    State.

tx_commit(Pubs, AckTags, From, State) ->
    case persistent_msg_ids(Pubs) of
        [] ->
            {true, tx_commit_from_msg_store(Pubs, AckTags, From, State)};
        PersistentMsgIds ->
            Self = self(),
            ok = rabbit_msg_store:sync(
                   PersistentMsgIds,
                   fun () -> ok = rabbit_amqqueue:tx_commit_msg_store_callback(
                                    Self, Pubs, AckTags, From)
                   end),
            {false, State}
    end.

tx_commit_from_msg_store(Pubs, AckTags, From,
                         State = #vqstate { on_sync = {SAcks, SPubs, SFroms} }) ->
    DiskAcks =
        lists:filter(fun (AckTag) -> AckTag /= ack_not_on_disk end, AckTags),
    State #vqstate { on_sync = { [DiskAcks | SAcks],
                                 [Pubs | SPubs],
                                 [From | SFroms] }}.

tx_commit_from_vq(State = #vqstate { on_sync = {SAcks, SPubs, SFroms} }) ->
    State1 = ack(lists:flatten(SAcks), State),
    {PubSeqIds, State2 = #vqstate { index_state = IndexState }} =
        lists:foldl(
          fun (Msg = #basic_message { is_persistent = IsPersistent },
               {SeqIdsAcc, StateN}) ->
                  {SeqId, StateN1} = publish(Msg, false, IsPersistent, StateN),
                  SeqIdsAcc1 = case IsPersistent of
                                   true -> [SeqId | SeqIdsAcc];
                                   false -> SeqIdsAcc
                               end,
                  {SeqIdsAcc1, StateN1}
          end, {[], State1}, lists:flatten(lists:reverse(SPubs))),
    IndexState1 =
        rabbit_queue_index:sync_seq_ids(PubSeqIds, IndexState),
    [ gen_server2:reply(From, ok) || From <- lists:reverse(SFroms) ],
    State2 #vqstate { index_state = IndexState1, on_sync = {[], [], []} }.

needs_sync(#vqstate { on_sync = {_, _, []} }) ->
    false;
needs_sync(_) ->
    true.

flush_journal(State = #vqstate { index_state = IndexState }) ->
    State #vqstate { index_state =
                     rabbit_queue_index:flush_journal(IndexState) }.

status(#vqstate { q1 = Q1, q2 = {Q2Len, _Q2},
                  delta = Delta, q3 = {Q3Len, _Q3}, q4 = Q4,
                  len = Len, on_sync = {_, _, From},
                  target_ram_msg_count = TargetRamMsgCount,
                  ram_msg_count = RamMsgCount,
                  ram_index_count = RamIndexCount,
                  avg_egress_rate = AvgEgressRate,
                  avg_ingress_rate = AvgIngressRate,
                  next_seq_id = NextSeqId }) ->
    [ {q1, queue:len(Q1)},
      {q2, Q2Len},
      {delta, Delta},
      {q3, Q3Len},
      {q4, queue:len(Q4)},
      {len, Len},
      {outstanding_txns, length(From)},
      {target_ram_msg_count, TargetRamMsgCount},
      {ram_msg_count, RamMsgCount},
      {ram_index_count, RamIndexCount},
      {avg_egress_rate, AvgEgressRate},
      {avg_ingress_rate, AvgIngressRate},
      {next_seq_id, NextSeqId} ].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

update_rate(Now, Then, Count, {OThen, OCount}) ->
    %% form the avg over the current period and the previous
    Avg = 1000000 * ((Count + OCount) / timer:now_diff(Now, OThen)),
    {Avg, {Then, Count}}.

persistent_msg_ids(Pubs) ->
    [MsgId || Obj = #basic_message { guid = MsgId } <- Pubs,
              Obj #basic_message.is_persistent].

betas_from_segment_entries(List, SeqIdLimit) ->
    List1 = [#msg_status { msg           = undefined,
                           msg_id        = MsgId,
                           seq_id        = SeqId,
                           is_persistent = IsPersistent,
                           is_delivered  = IsDelivered,
                           msg_on_disk   = true,
                           index_on_disk = true
                         }
             || {MsgId, SeqId, IsPersistent, IsDelivered} <- List,
                SeqId < SeqIdLimit ],
    {length(List1), queue:from_list([{true, queue:from_list(List1)}])}.

join_betas({HeadLen, Head}, {TailLen, Tail}) ->
    {HeadLen + TailLen, join_betas1(Head, Tail)}.

join_betas1(Head, Tail) ->
    case {queue:out_r(Head), queue:out(Tail)} of
        {{empty, _Head}, _} ->
            Tail;
        {_, {empty, _Tail}} ->
            Head;
        {{{value, {IndexOnDisk, InnerQHead}}, Head1},
         {{value, {IndexOnDisk, InnerQTail}}, Tail1}} ->
             queue:join(
               queue:in({IndexOnDisk,
                         queue:join(InnerQHead, InnerQTail)}, Head1),
               Tail1);
        {_, _} -> queue:join(Head, Tail)
    end.

grab_beta(Gen, Q) ->
    case Gen(Q) of
        {empty, _Q} ->
            empty;
        {{value, {_IndexOnDisk, InnerQ}}, _Q} ->
            {{value, MsgStatus}, _InnerQ} = Gen(InnerQ),
            MsgStatus
    end.

read_index_segment(SeqId, IndexState) ->
    SeqId1 = SeqId + rabbit_queue_index:segment_size(),
    case rabbit_queue_index:read_segment_entries(SeqId, IndexState) of
        {[], IndexState1} -> read_index_segment(SeqId1, IndexState1);
        {List, IndexState1} -> {List, IndexState1, SeqId1}
    end.

ensure_binary_properties(Msg = #basic_message { content = Content }) ->
    Msg #basic_message {
      content = rabbit_binary_parser:clear_decoded_content(
                  rabbit_binary_generator:ensure_content_encoded(Content)) }.

%% the first arg is the older delta
combine_deltas(#delta { count = 0 }, #delta { count = 0 }) ->
    ?BLANK_DELTA;
combine_deltas(#delta { count = 0 }, #delta {       } = B) -> B;
combine_deltas(#delta {       } = A, #delta { count = 0 }) -> A;
combine_deltas(#delta { start_seq_id = SeqIdLow,  count = CountLow},
               #delta { start_seq_id = SeqIdHigh, count = CountHigh,
                        end_seq_id = SeqIdEnd }) ->
    true = SeqIdLow =< SeqIdHigh, %% ASSERTION
    Count = CountLow + CountHigh,
    true = Count =< SeqIdEnd - SeqIdLow, %% ASSERTION
    #delta { start_seq_id = SeqIdLow, count = Count, end_seq_id = SeqIdEnd }.

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

delete1(NextSeqId, Count, DeltaSeqId, IndexState)
  when DeltaSeqId >= NextSeqId ->
    {Count, IndexState};
delete1(NextSeqId, Count, DeltaSeqId, IndexState) ->
    Delta1SeqId = DeltaSeqId + rabbit_queue_index:segment_size(),
    case rabbit_queue_index:read_segment_entries(DeltaSeqId, IndexState) of
        {[], IndexState1} ->
            delete1(NextSeqId, Count, Delta1SeqId, IndexState1);
        {List, IndexState1} ->
            {QCount, Q} = betas_from_segment_entries(List, Delta1SeqId),
            {QCount, IndexState2} = remove_queue_entries(Q, IndexState1),
            delete1(NextSeqId, Count + QCount, Delta1SeqId, IndexState2)
    end.

purge1(Count, State = #vqstate { q3 = {Q3Len, Q3}, index_state = IndexState }) ->
    case 0 == Q3Len of
        true ->
            {Q1Count, IndexState1} =
                remove_queue_entries(State #vqstate.q1, IndexState),
            {Count + Q1Count, State #vqstate { q1 = queue:new(),
                                               index_state = IndexState1 }};
        false ->
            {Q3Count, IndexState1} = remove_queue_entries(Q3, IndexState),
            purge1(Count + Q3Count,
                   maybe_deltas_to_betas(
                     State #vqstate { index_state = IndexState1,
                                      q3 = {0, queue:new()} }))
    end.

remove_queue_entries(Q, IndexState) ->
    {Count, MsgIds, SeqIds, IndexState1} =
        lists:foldl(
          fun (#msg_status { msg_id = MsgId, seq_id = SeqId,
                             is_delivered = IsDelivered, msg_on_disk = MsgOnDisk,
                             index_on_disk = IndexOnDisk },
               {CountN, MsgIdsAcc, SeqIdsAcc, IndexStateN}) ->
                  MsgIdsAcc1 = case MsgOnDisk of
                                   true  -> [MsgId | MsgIdsAcc];
                                   false -> MsgIdsAcc
                               end,
                  SeqIdsAcc1 = case IndexOnDisk of
                                   true  -> [SeqId | SeqIdsAcc];
                                   false -> SeqIdsAcc
                               end,
                  IndexStateN1 = case IndexOnDisk andalso not IsDelivered of
                                     true -> rabbit_queue_index:write_delivered(
                                               SeqId, IndexStateN);
                                     false -> IndexStateN
                                 end,
                  {CountN + 1, MsgIdsAcc1, SeqIdsAcc1, IndexStateN1}
          %% we need to write the delivered records in order otherwise
          %% we upset the qi. So don't reverse.
          end, {0, [], [], IndexState}, queue:to_list(Q)),
    ok = case MsgIds of
             [] -> ok;
             _  -> rabbit_msg_store:remove(MsgIds)
         end,
    IndexState2 =
        case SeqIds of
            [] -> IndexState1;
            _  -> rabbit_queue_index:write_acks(SeqIds, IndexState1)
        end,
    {Count, IndexState2}.

fetch_from_q3_or_delta(State = #vqstate {
                         q1 = Q1, q2 = {Q2Len, _Q2}, delta = #delta { count = DeltaCount },
                         q3 = {Q3Len, Q3}, q4 = Q4, ram_msg_count = RamMsgCount,
                         ram_index_count = RamIndexCount,
                         msg_store_read_state = MSCState }) ->
    case queue:out(Q3) of
        {empty, _Q3} ->
            0 = DeltaCount, %% ASSERTION
            0 = Q2Len, %% ASSERTION
            0 = Q3Len, %% ASSERTION
            true = queue:is_empty(Q1), %% ASSERTION
            {empty, State};
        {{value, {IndexOnDisk, InnerQ}}, Q3a} ->
            {{value, MsgStatus = #msg_status {
                       msg = undefined, msg_id = MsgId,
                       is_persistent = IsPersistent
                      }}, InnerQ1} = queue:out(InnerQ),
            Q3LenB = Q3Len - 1,
            Q3b = {Q3LenB, case queue:is_empty(InnerQ1) of
                               true  -> Q3a;
                               false -> queue:in_r({IndexOnDisk, InnerQ1}, Q3a)
                           end},
            {{ok, Msg = #basic_message { is_persistent = IsPersistent,
                                         guid = MsgId }}, MSCState1} =
                rabbit_msg_store:read(MsgId, MSCState),
            Q4a = queue:in(MsgStatus #msg_status { msg = Msg }, Q4),
            RamIndexCount1 = case IndexOnDisk of
                                 true  -> RamIndexCount;
                                 false -> RamIndexCount - 1
                             end,
            true = RamIndexCount1 >= 0, %% ASSERTION
            State1 = State #vqstate { q3 = Q3b, q4 = Q4a,
                                      ram_msg_count = RamMsgCount + 1,
                                      ram_index_count = RamIndexCount1,
                                      msg_store_read_state = MSCState1 },
            State2 =
                case {0 == Q3LenB, 0 == DeltaCount} of
                    {true, true} ->
                        %% q3 is now empty, it wasn't before; delta is
                        %% still empty. So q2 must be empty, and q1
                        %% can now be joined onto q4
                        0 = Q2Len, %% ASSERTION
                        State1 #vqstate { q1 = queue:new(),
                                          q4 = queue:join(Q4a, Q1) };
                    {true, false} ->
                        maybe_deltas_to_betas(State1);
                    {false, _} ->
                        %% q3 still isn't empty, we've not touched
                        %% delta, so the invariants between q1, q2,
                        %% delta and q3 are maintained
                        State1
                end,
            fetch(State2)
    end.

reduce_memory_use(State = #vqstate { ram_msg_count = RamMsgCount,
                                     target_ram_msg_count = TargetRamMsgCount })
  when TargetRamMsgCount == undefined orelse TargetRamMsgCount >= RamMsgCount ->
    State;
reduce_memory_use(State =
                  #vqstate { target_ram_msg_count = TargetRamMsgCount }) ->
    State1 = maybe_push_q4_to_betas(maybe_push_q1_to_betas(State)),
    case TargetRamMsgCount of
        0 -> push_betas_to_deltas(State1);
        _ -> State1
    end.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

test_keep_msg_in_ram(SeqId, #vqstate { target_ram_msg_count = TargetRamMsgCount,
                                       ram_msg_count = RamMsgCount,
                                       q1 = Q1, q3 = {_Q3Len, Q3} }) ->
    case TargetRamMsgCount of
        undefined ->
            msg;
        0 ->
            case queue:out(Q3) of
                {empty, _Q3} ->
                    %% if TargetRamMsgCount == 0, we know we have no
                    %% alphas. If q3 is empty then delta must be empty
                    %% too, so create a beta, which should end up in
                    %% q3
                    index;
                {{value, {_IndexOnDisk, InnerQ}}, _Q3a} ->
                    {{value, #msg_status { seq_id = OldSeqId }}, _InnerQ} =
                        queue:out(InnerQ),
                    %% Don't look at the current delta as it may be
                    %% empty. If the SeqId is still within the current
                    %% segment, it'll be a beta, else it'll go into
                    %% delta
                    case SeqId >= rabbit_queue_index:next_segment_boundary(OldSeqId) of
                        true  -> neither;
                        false -> index
                    end
            end;
        _ when TargetRamMsgCount > RamMsgCount ->
            msg;
        _ ->
            case queue:is_empty(Q1) of
                true  -> index;
                %% Can push out elders (in q1) to disk. This may also
                %% result in the msg itself going to disk and q2/q3.
                false -> msg
            end
    end.

publish(Msg = #basic_message { is_persistent = IsPersistent, guid = MsgId },
        IsDelivered, MsgOnDisk, State =
        #vqstate { next_seq_id = SeqId, len = Len, in_counter = InCount }) ->
    MsgStatus = #msg_status {
      msg = Msg, msg_id = MsgId, seq_id = SeqId, is_persistent = IsPersistent,
      is_delivered = IsDelivered, msg_on_disk = MsgOnDisk,
      index_on_disk = false },
    {SeqId, publish(test_keep_msg_in_ram(SeqId, State), MsgStatus,
                    State #vqstate { next_seq_id = SeqId + 1, len = Len + 1,
                                     in_counter = InCount + 1 })}.

publish(msg, MsgStatus, State = #vqstate { index_state = IndexState,
                                           ram_msg_count = RamMsgCount }) ->
    MsgStatus1 = maybe_write_msg_to_disk(false, MsgStatus),
    {MsgStatus2, IndexState1} =
        maybe_write_index_to_disk(false, MsgStatus1, IndexState),
    State1 = State #vqstate { ram_msg_count = RamMsgCount + 1,
                              index_state = IndexState1 },
    store_alpha_entry(MsgStatus2, State1);

publish(index, MsgStatus, State =
        #vqstate { index_state = IndexState, q1 = Q1,
                   ram_index_count = RamIndexCount,
                   target_ram_msg_count = TargetRamMsgCount }) ->
    MsgStatus1 = #msg_status { msg_on_disk = true } =
        maybe_write_msg_to_disk(true, MsgStatus),
    ForceIndex = case TargetRamMsgCount of
                     undefined ->
                         false;
                     _ ->
                         RamIndexCount >= (?RAM_INDEX_TARGET_RATIO *
                                           TargetRamMsgCount)
                 end,
    {MsgStatus2, IndexState1} =
        maybe_write_index_to_disk(ForceIndex, MsgStatus1, IndexState),
    RamIndexCount1 = case MsgStatus2 #msg_status.index_on_disk of
                         true  -> RamIndexCount;
                         false -> RamIndexCount + 1
                     end,
    State1 = State #vqstate { index_state = IndexState1,
                              ram_index_count = RamIndexCount1 },
    true = queue:is_empty(Q1), %% ASSERTION
    store_beta_entry(MsgStatus2, State1);

publish(neither, MsgStatus = #msg_status { seq_id = SeqId }, State =
        #vqstate { index_state = IndexState, q1 = Q1, q2 = {Q2Len, _Q2},
                   delta = Delta }) ->
    MsgStatus1 = #msg_status { msg_on_disk = true } =
        maybe_write_msg_to_disk(true, MsgStatus),
    {#msg_status { index_on_disk = true }, IndexState1} =
        maybe_write_index_to_disk(true, MsgStatus1, IndexState),
    true = queue:is_empty(Q1) andalso 0 == Q2Len, %% ASSERTION
    %% delta may be empty, seq_id > next_segment_boundary from q3
    %% head, so we need to find where the segment boundary is before
    %% or equal to seq_id
    DeltaSeqId = rabbit_queue_index:next_segment_boundary(SeqId) -
        rabbit_queue_index:segment_size(),
    Delta1 = #delta { start_seq_id = DeltaSeqId, count = 1,
                      end_seq_id = SeqId + 1 },
    State #vqstate { index_state = IndexState1,
                     delta = combine_deltas(Delta, Delta1) }.

store_alpha_entry(MsgStatus, State =
                  #vqstate { q1 = Q1, q2 = {Q2Len, _Q2},
                             delta = #delta { count = DeltaCount },
                             q3 = {Q3Len, _Q3}, q4 = Q4 }) ->
    case 0 == Q2Len andalso 0 == DeltaCount andalso 0 == Q3Len of
        true  -> true = queue:is_empty(Q1), %% ASSERTION
                 State #vqstate { q4 = queue:in(MsgStatus, Q4) };
        false -> maybe_push_q1_to_betas(
                   State #vqstate { q1 = queue:in(MsgStatus, Q1) })
    end.

store_beta_entry(MsgStatus = #msg_status { msg_on_disk = true },
                 State = #vqstate { q2 = {Q2Len, Q2},
                                    delta = #delta { count = DeltaCount },
                                    q3 = {Q3Len, Q3} }) ->
    MsgStatus1 = MsgStatus #msg_status { msg = undefined },
    case DeltaCount == 0 of
        true  -> State #vqstate { q3 = {Q3Len + 1,
                                        store_beta_entry1(
                                          fun queue:out_r/1, fun queue:in/2,
                                          MsgStatus1, Q3)} };
        false -> State #vqstate { q2 = {Q2Len + 1,
                                        store_beta_entry1(
                                          fun queue:out_r/1, fun queue:in/2,
                                          MsgStatus1, Q2)} }
    end.

store_beta_entry1(Gen, Cons, MsgStatus =
                  #msg_status { index_on_disk = IndexOnDisk }, Q) ->
    case Gen(Q) of
        {{value, {IndexOnDisk, InnerQ}}, QTail} ->
            Cons({IndexOnDisk, Cons(MsgStatus, InnerQ)}, QTail);
        {_EmptyOrNotIndexOnDisk, _QTail} ->
            Cons({IndexOnDisk, Cons(MsgStatus, queue:new())}, Q)
    end.

maybe_write_msg_to_disk(_Force, MsgStatus =
                        #msg_status { msg_on_disk = true }) ->
    MsgStatus;
maybe_write_msg_to_disk(Force, MsgStatus = #msg_status {
                                 msg = Msg, msg_id = MsgId,
                                 is_persistent = IsPersistent })
  when Force orelse IsPersistent ->
    ok = rabbit_msg_store:write(MsgId, ensure_binary_properties(Msg)),
    MsgStatus #msg_status { msg_on_disk = true };
maybe_write_msg_to_disk(_Force, MsgStatus) ->
    MsgStatus.

maybe_write_index_to_disk(_Force, MsgStatus =
                          #msg_status { index_on_disk = true }, IndexState) ->
    true = MsgStatus #msg_status.msg_on_disk, %% ASSERTION
    {MsgStatus, IndexState};
maybe_write_index_to_disk(Force, MsgStatus = #msg_status {
                                   msg_id = MsgId, seq_id = SeqId,
                                   is_persistent = IsPersistent,
                                   is_delivered = IsDelivered }, IndexState)
  when Force orelse IsPersistent ->
    true = MsgStatus #msg_status.msg_on_disk, %% ASSERTION
    IndexState1 = rabbit_queue_index:write_published(
                    MsgId, SeqId, IsPersistent, IndexState),
    {MsgStatus #msg_status { index_on_disk = true },
     case IsDelivered of
         true  -> rabbit_queue_index:write_delivered(SeqId, IndexState1);
         false -> IndexState1
     end};
maybe_write_index_to_disk(_Force, MsgStatus, IndexState) ->
    {MsgStatus, IndexState}.

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------

maybe_deltas_to_betas(State = #vqstate { delta = #delta { count = 0 } }) ->
    State;
maybe_deltas_to_betas(
  State = #vqstate { index_state = IndexState,
                     q2 = Q2All, q3 = {Q3Len, _Q3} = Q3All,
                     target_ram_msg_count = TargetRamMsgCount,
                     delta = #delta { start_seq_id = DeltaSeqId,
                                      count = DeltaCount,
                                      end_seq_id = DeltaSeqIdEnd }}) ->
    case (0 < Q3Len) andalso (0 == TargetRamMsgCount) of
        true ->
            State;
        false ->
            %% either q3 is empty, in which case we load at least one
            %% segment, or TargetRamMsgCount > 0, meaning we should
            %% really be holding all the betas in memory.
            {List, IndexState1, Delta1SeqId} =
                read_index_segment(DeltaSeqId, IndexState),
            State1 = State #vqstate { index_state = IndexState1 },
            %% length(List) may be < segment_size because of acks. But
            %% it can't be []
            Q3bAll = {Q3bLen, _Q3b} =
                betas_from_segment_entries(List, DeltaSeqIdEnd),
            Q3a = join_betas(Q3All, Q3bAll),
            case DeltaCount - Q3bLen of
                0 ->
                    %% delta is now empty, but it wasn't before, so
                    %% can now join q2 onto q3
                    State1 #vqstate { delta = ?BLANK_DELTA,
                                      q2 = {0, queue:new()},
                                      q3 = join_betas(Q3a, Q2All) };
                N when N > 0 ->
                    State1 #vqstate {
                      q3 = Q3a,
                      delta = #delta { start_seq_id = Delta1SeqId,
                                       count = N,
                                       end_seq_id = DeltaSeqIdEnd } }
            end
    end.

maybe_push_q1_to_betas(State = #vqstate { q1 = Q1 }) ->
    maybe_push_alphas_to_betas(
      fun queue:out/1,
      fun (MsgStatus, Q1a, State1) ->
              %% these could legally go to q3 if delta and q2 are empty
              store_beta_entry(MsgStatus, State1 #vqstate { q1 = Q1a })
      end, Q1, State).

maybe_push_q4_to_betas(State = #vqstate { q4 = Q4 }) ->
    maybe_push_alphas_to_betas(
      fun queue:out_r/1,
      fun (MsgStatus, Q4a, State1 = #vqstate { q3 = {Q3Len, Q3} }) ->
              MsgStatus1 = MsgStatus #msg_status { msg = undefined },
              %% these must go to q3
              State1 #vqstate { q3 = {Q3Len + 1,
                                      store_beta_entry1(
                                        fun queue:out/1, fun queue:in_r/2,
                                        MsgStatus1, Q3)}, q4 = Q4a }
      end, Q4, State).

maybe_push_alphas_to_betas(_Generator, _Consumer, _Q, State =
                           #vqstate { ram_msg_count = RamMsgCount,
                                      target_ram_msg_count = TargetRamMsgCount })
  when TargetRamMsgCount == undefined orelse TargetRamMsgCount >= RamMsgCount ->
    State;
maybe_push_alphas_to_betas(
  Generator, Consumer, Q, State =
  #vqstate { ram_msg_count = RamMsgCount, ram_index_count = RamIndexCount,
             target_ram_msg_count = TargetRamMsgCount,
             index_state = IndexState }) ->
    case Generator(Q) of
        {empty, _Q} -> State;
        {{value, MsgStatus}, Qa} ->
            MsgStatus1 = maybe_write_msg_to_disk(true, MsgStatus),
            ForceIndex = case TargetRamMsgCount of
                             undefined ->
                                 false;
                             _ ->
                                 RamIndexCount >= (?RAM_INDEX_TARGET_RATIO *
                                                   TargetRamMsgCount)
                         end,
            {MsgStatus2, IndexState1} =
                maybe_write_index_to_disk(ForceIndex, MsgStatus1, IndexState),
            RamIndexCount1 = case MsgStatus2 #msg_status.index_on_disk of
                                 true  -> RamIndexCount;
                                 false -> RamIndexCount + 1
                             end,
            State1 = State #vqstate { ram_msg_count = RamMsgCount - 1,
                                      ram_index_count = RamIndexCount1,
                                      index_state = IndexState1 },
            maybe_push_alphas_to_betas(Generator, Consumer, Qa,
                                       Consumer(MsgStatus2, Qa, State1))
    end.

push_betas_to_deltas(State = #vqstate { q2 = {Q2Len, Q2}, delta = Delta,
                                        q3 = {Q3Len, Q3},
                                        ram_index_count = RamIndexCount,
                                        index_state = IndexState }) ->
    %% HighSeqId is high in the sense that it must be higher than the
    %% seq_id in Delta, but it's also the lowest of the betas that we
    %% transfer from q2 to delta.
    {HighSeqId, Q2Len, Q2a, RamIndexCount1, IndexState1} =
        push_betas_to_deltas(
          fun queue:out/1,
          fun (IndexOnDisk, InnerQ, Q) ->
                  join_betas1(queue:from_list([{IndexOnDisk, InnerQ}]), Q)
          end, undefined, Q2, RamIndexCount, IndexState),
    true = queue:is_empty(Q2a), %% ASSERTION
    EndSeqId = case queue:out_r(Q2) of
                   {empty, _Q2} ->
                       undefined;
                   {{value, {_IndexOnDisk, InnerQ}}, _Q2} ->
                       {{value, #msg_status { seq_id = EndSeqId1 }}, _InnerQ} =
                           queue:out_r(InnerQ),
                       EndSeqId1 + 1
               end,
    Delta1 = #delta { start_seq_id = Delta1SeqId } =
        combine_deltas(Delta, #delta { start_seq_id = HighSeqId,
                                       count = Q2Len,
                                       end_seq_id = EndSeqId }),
    State1 = State #vqstate { q2 = {0, Q2a}, delta = Delta1,
                              index_state = IndexState1,
                              ram_index_count = RamIndexCount1 },
    case queue:out(Q3) of
        {empty, _Q3} ->
            State1;
        {{value, {_IndexOnDisk1, InnerQ1}}, _Q3} ->
            {{value, #msg_status { seq_id = SeqId }}, _InnerQ1} =
                queue:out(InnerQ1),
            #msg_status { seq_id = SeqIdMax } =
                grab_beta(fun queue:out_r/1, Q3),
            Limit = rabbit_queue_index:next_segment_boundary(SeqId),
            %% ASSERTION
            true = Delta1SeqId == undefined orelse Delta1SeqId > SeqIdMax,
            case SeqIdMax < Limit of
                true -> %% already only holding LTE one segment indices in q3
                    State1;
                false ->
                    %% ASSERTION
                    %% This says that if Delta1SeqId /= undefined then
                    %% the gap from Limit to Delta1SeqId is an integer
                    %% multiple of segment_size
                    0 = case Delta1SeqId of
                            undefined -> 0;
                            _ -> (Delta1SeqId - Limit) rem
                                     rabbit_queue_index:segment_size()
                        end,
                    %% SeqIdMax is low in the sense that it must be
                    %% lower than the seq_id in delta1, in fact either
                    %% delta1 has undefined as its seq_id or there
                    %% does not exist a seq_id X s.t. X > SeqIdMax and
                    %% X < delta1's seq_id (would be +1 if it wasn't
                    %% for the possibility of gaps in the seq_ids).
                    %% But because we use queue:out_r, SeqIdMax is
                    %% actually also the highest seq_id of the betas we
                    %% transfer from q3 to deltas.
                    {SeqIdMax, Len2, Q3b, RamIndexCount2, IndexState2} =
                        push_betas_to_deltas(
                          fun queue:out_r/1,
                          fun (IndexOnDisk, InnerQ, Q) ->
                                  join_betas1(Q, queue:from_list(
                                                   [{IndexOnDisk, InnerQ}]))
                          end, Limit, Q3, RamIndexCount1, IndexState1),
                    Delta2 = combine_deltas(#delta { start_seq_id = Limit,
                                                     count = Len2,
                                                     end_seq_id = SeqIdMax+1 },
                                            Delta1),
                    State1 #vqstate { q3 = {Q3Len - Len2, Q3b}, delta = Delta2,
                                      index_state = IndexState2,
                                      ram_index_count = RamIndexCount2 }
            end
    end.

push_betas_to_deltas(
  Generator, Consumer, Limit, Q, RamIndexCount, IndexState) ->
    case Generator(Q) of
        {empty, Qa} -> {undefined, 0, Qa, RamIndexCount, IndexState};
        {{value, {IndexOnDisk, InnerQ}}, Qa} ->
            {{value, #msg_status { seq_id = SeqId }}, _Qb} = Generator(InnerQ),
            {Count, Qb, RamIndexCount1, IndexState1} =
                push_betas_to_deltas(
                  Generator, Consumer, Limit, IndexOnDisk, InnerQ, Qa, 0,
                  RamIndexCount, IndexState),
            {SeqId, Count, Qb, RamIndexCount1, IndexState1}
    end.

push_betas_to_deltas(
  Generator, Consumer, Limit, Q, Count, RamIndexCount, IndexState) ->
    case Generator(Q) of
        {empty, Qa} ->
            {Count, Qa, RamIndexCount, IndexState};
        {{value, {IndexOnDisk, InnerQ}}, Qa} ->
            push_betas_to_deltas(
              Generator, Consumer, Limit, IndexOnDisk, InnerQ, Qa, Count,
              RamIndexCount, IndexState)
    end.

push_betas_to_deltas(Generator, Consumer, Limit, IndexOnDisk, InnerQ, Q,
                     Count, RamIndexCount, IndexState) ->
    case Generator(InnerQ) of
        {empty, _InnerQ} ->
            push_betas_to_deltas(Generator, Consumer, Limit, Q, Count,
                                 RamIndexCount, IndexState);
        {{value, #msg_status { seq_id = SeqId }}, _InnerQ}
        when Limit /= undefined andalso SeqId < Limit ->
            {Count, Consumer(IndexOnDisk, InnerQ, Q), RamIndexCount,
             IndexState};
        {{value, MsgStatus}, InnerQa} ->
            {RamIndexCount1, IndexState1} =
                case IndexOnDisk of
                    true -> {RamIndexCount, IndexState};
                    false ->
                        {#msg_status { index_on_disk = true }, IndexState2} =
                            maybe_write_index_to_disk(true, MsgStatus,
                                                      IndexState),
                        {RamIndexCount - 1, IndexState2}
                end,
            push_betas_to_deltas(
              Generator, Consumer, Limit, IndexOnDisk, InnerQa, Q, Count + 1,
              RamIndexCount1, IndexState1)
    end.
