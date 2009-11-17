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
         full_flush_journal/1, status/1]).

%%----------------------------------------------------------------------------

-record(vqstate,
        { q1,
          q2,
          gamma,
          q3,
          q4,
          duration_target,
          target_ram_msg_count,
          ram_msg_count,
          queue,
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
          on_sync
        }).

-include("rabbit.hrl").
-include("rabbit_queue.hrl").

%%----------------------------------------------------------------------------

%% Basic premise is that msgs move from q1 -> q2 -> gamma -> q3 -> q4
%% but they can only do so in the right form. q1 and q4 only hold
%% alphas (msgs in ram), q2 and q3 only hold betas (msg on disk, index
%% in ram), and gamma is just a count of the number of index entries
%% on disk at that stage (msg on disk, index on disk).
%%
%% When a msg arrives, we decide in which form it should be. It is
%% then added to the right-most appropriate queue, maintaining
%% order. Thus if the msg is to be an alpha, it will be added to q1,
%% unless all of q2, gamma and q3 are empty, in which case it will go
%% to q4. If it is to be a beta, it will be added to q2 unless gamma
%% is empty, in which case it will go to q3.
%%
%% The major invariant is that if the msg is to be a beta, q1 will be
%% empty, and if it is to be a gamma then both q1 and q2 will be empty.
%%
%% When taking msgs out of the queue, if q4 is empty then we drain the
%% prefetcher. If that doesn't help then we read directly from q3, or
%% gamma, if q3 is empty. If q3 and gamma are empty then we have an
%% invariant that q2 must be empty because q2 can only grow if gamma
%% is non empty.
%%
%% A further invariant is that if the queue is non empty, either q4 or
%% q3 contains at least one entry. I.e. we never allow gamma to
%% contain all msgs in the queue.  Also, if q4 is non empty and gamma
%% is non empty then q3 must be non empty.

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(msg_id()  :: binary()).
-type(seq_id()  :: non_neg_integer()).
-type(ack()     :: {'ack_index_and_store', msg_id(), seq_id()}
                 | 'ack_not_on_disk').
-type(vqstate() :: #vqstate {
               q1                    :: queue(),
               q2                    :: queue(),
               gamma                 :: gamma(),
               q3                    :: queue(),
               q4                    :: queue(),
               duration_target       :: non_neg_integer(),
               target_ram_msg_count  :: non_neg_integer(),
               queue                 :: queue_name(),
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
               on_sync               :: {[ack()], [msg_id()], [{pid(), any()}]}
              }).

-spec(init/1 :: (queue_name()) -> vqstate()).
-spec(terminate/1 :: (vqstate()) -> vqstate()).
-spec(publish/2 :: (basic_message(), vqstate()) ->
             {seq_id(), vqstate()}).
-spec(publish_delivered/2 :: (basic_message(), vqstate()) ->
             {ack(), vqstate()}).
-spec(set_queue_ram_duration_target/2 ::
      (('undefined' | number()), vqstate()) -> vqstate()).
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
-spec(full_flush_journal/1 :: (vqstate()) -> vqstate()).
-spec(status/1 :: (vqstate()) -> [{atom(), any()}]).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

init(QueueName) ->
    {GammaCount, IndexState} =
        rabbit_queue_index:init(QueueName),
    {GammaSeqId, NextSeqId, IndexState1} =
        rabbit_queue_index:find_lowest_seq_id_seg_and_next_seq_id(IndexState),
    Gamma = case GammaCount of
                0 -> #gamma { seq_id = undefined, count = 0 };
                _ -> #gamma { seq_id = GammaSeqId, count = GammaCount }
            end,
    Now = now(),
    State =
        #vqstate { q1 = queue:new(), q2 = queue:new(),
                   gamma = Gamma,
                   q3 = queue:new(), q4 = queue:new(),
                   target_ram_msg_count = undefined,
                   duration_target = undefined,
                   ram_msg_count = 0,
                   queue = QueueName,
                   index_state = IndexState1,
                   next_seq_id = NextSeqId,
                   out_counter = 0,
                   in_counter = 0,
                   egress_rate = {Now, 0},
                   avg_egress_rate = 0,
                   ingress_rate = {Now, GammaCount},
                   avg_ingress_rate = 0,
                   rate_timestamp = Now,
                   len = GammaCount,
                   on_sync = {[], [], []}
                  },
    maybe_gammas_to_betas(State).

terminate(State = #vqstate { index_state = IndexState }) ->
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
    case maybe_write_msg_to_disk(false, false, Msg) of
        true ->
            {true, IndexState1} =
                maybe_write_index_to_disk(false, IsPersistent, MsgId, SeqId,
                                          true, IndexState),
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
    Rate = case 0 == AvgEgressRate of
               true -> AvgIngressRate;
               false -> AvgEgressRate
           end,
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
                       out_counter = 0, in_counter = 0 }).

ram_duration(#vqstate { avg_egress_rate = AvgEgressRate,
                        avg_ingress_rate = AvgIngressRate,
                        ram_msg_count = RamMsgCount }) ->
    %% msgs / (msgs/sec) == sec
    case AvgEgressRate == 0 of
        true  -> case AvgIngressRate == 0 of
                     true -> infinity;
                     false -> RamMsgCount / AvgIngressRate
                 end;
        false -> RamMsgCount / AvgEgressRate
    end.

fetch(State =
      #vqstate { q4 = Q4, ram_msg_count = RamMsgCount, out_counter = OutCount,
                 index_state = IndexState, len = Len }) ->
    case queue:out(Q4) of
        {empty, _Q4} ->
            fetch_from_q3_or_gamma(State);
        {{value,
          #alpha { msg = Msg = #basic_message { guid = MsgId,
                                                is_persistent = IsPersistent },
                   seq_id = SeqId, is_delivered = IsDelivered,
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
            _MsgOnDisk1 = IndexOnDisk1 =
                case IndexOnDisk1 of
                    true  -> true = IsPersistent, %% ASSERTION
                             true = MsgOnDisk; %% ASSERTION
                    false -> ok = case MsgOnDisk andalso not IsPersistent of
                                      true -> rabbit_msg_store:remove([MsgId]);
                                      false -> ok
                                  end,
                             false
                end,
            AckTag = case IndexOnDisk1 of
                         true  -> {ack_index_and_store, MsgId, SeqId};
                         false -> ack_not_on_disk
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
    {Len, State1 #vqstate { len = 0 }}.

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.
delete(State) ->
    {_PurgeCount, State1 = #vqstate { index_state = IndexState }} = purge(State),
    IndexState1 =
        case rabbit_queue_index:find_lowest_seq_id_seg_and_next_seq_id(
               IndexState) of
            {N, N, IndexState2} ->
                IndexState2;
            {GammaSeqId, NextSeqId, IndexState2} ->
                {_DeleteCount, IndexState3} =
                    delete1(NextSeqId, 0, GammaSeqId, IndexState2),
                IndexState3
    end,
    IndexState4 = rabbit_queue_index:terminate_and_erase(IndexState1),
    State1 #vqstate { index_state = IndexState4 }.

%% [{Msg, AckTag}]
%% We guarantee that after fetch, only persistent msgs are left on
%% disk. This means that in a requeue, we set
%% PersistentMsgsAlreadyOnDisk to true, thus avoiding calls to
%% msg_store:write for persistent msgs. It also means that we don't
%% need to worry about calling msg_store:remove (as ack would do)
%% because transient msgs won't be on disk anyway, thus they won't
%% need to be removed. However, we do call msg_store:release so that
%% the cache isn't held full of msgs which are now at the tail of the
%% queue.
requeue(MsgsWithAckTags, State) ->
    {SeqIds, MsgIds, State1 = #vqstate { index_state = IndexState }} =
        lists:foldl(
          fun ({Msg = #basic_message { guid = MsgId }, AckTag},
               {SeqIdsAcc, MsgIdsAcc, StateN}) ->
                  {_SeqId, StateN1} = publish(Msg, true, true, StateN),
                  {SeqIdsAcc1, MsgIdsAcc1} =
                      case AckTag of
                          ack_not_on_disk ->
                              {SeqIdsAcc, MsgIdsAcc};
                          {ack_index_and_store, MsgId, SeqId} ->
                              {[SeqId | SeqIdsAcc], [MsgId | MsgIdsAcc]}
                      end,
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

tx_publish(Msg = #basic_message { is_persistent = true }, State) ->
    true = maybe_write_msg_to_disk(true, false, Msg),
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
                  {SeqId, StateN1} = publish(Msg, false, true, StateN),
                  SeqIdsAcc1 = case IsPersistent of
                                   true -> [SeqId | SeqIdsAcc];
                                   false -> SeqIdsAcc
                               end,
                  {SeqIdsAcc1, StateN1}
          end, {[], State1}, lists:flatten(lists:reverse(SPubs))),
    IndexState1 =
        rabbit_queue_index:sync_seq_ids(PubSeqIds, [] /= SAcks, IndexState),
    [ gen_server2:reply(From, ok) || From <- lists:reverse(SFroms) ],
    State2 #vqstate { index_state = IndexState1, on_sync = {[], [], []} }.

needs_sync(#vqstate { on_sync = {_, _, []} }) ->
    false;
needs_sync(_) ->
    true.

full_flush_journal(State = #vqstate { index_state = IndexState }) ->
    State #vqstate { index_state =
                     rabbit_queue_index:full_flush_journal(IndexState) }.

status(#vqstate { q1 = Q1, q2 = Q2, gamma = Gamma, q3 = Q3, q4 = Q4,
                  len = Len, on_sync = {_, _, From},
                  target_ram_msg_count = TargetRamMsgCount,
                  ram_msg_count = RamMsgCount, 
                  avg_egress_rate = AvgEgressRate,
                  avg_ingress_rate = AvgIngressRate }) ->
    [ {q1, queue:len(Q1)},
      {q2, queue:len(Q2)},
      {gamma, Gamma},
      {q3, queue:len(Q3)},
      {q4, queue:len(Q4)},
      {len, Len},
      {outstanding_txns, length(From)},
      {target_ram_msg_count, TargetRamMsgCount},
      {ram_msg_count, RamMsgCount},
      {avg_egress_rate, AvgEgressRate},
      {avg_ingress_rate, AvgIngressRate} ].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

update_rate(Now, Then, Count, Rate = {OThen, OCount}) ->
    %% form the avg over the current periond and the previous
    Avg = 1000000 * ((Count + OCount) / timer:now_diff(Now, OThen)),
    Rate1 = case 0 == Count of
                true -> Rate; %% keep the last period with activity
                false -> {Then, Count}
            end,
    {Avg, Rate1}.

persistent_msg_ids(Pubs) ->
    [MsgId || Obj = #basic_message { guid = MsgId } <- Pubs,
              Obj #basic_message.is_persistent].

entry_salient_details(#alpha { msg = #basic_message { guid = MsgId },
                               seq_id = SeqId, is_delivered = IsDelivered,
                               msg_on_disk = MsgOnDisk,
                               index_on_disk = IndexOnDisk }) ->
    {MsgId, SeqId, IsDelivered, MsgOnDisk, IndexOnDisk};
entry_salient_details(#beta { msg_id = MsgId, seq_id = SeqId,
                              is_delivered = IsDelivered,
                              index_on_disk = IndexOnDisk }) ->
    {MsgId, SeqId, IsDelivered, true, IndexOnDisk}.

betas_from_segment_entries(List) ->
    queue:from_list([#beta { msg_id = MsgId, seq_id = SeqId,
                             is_persistent = IsPersistent,
                             is_delivered = IsDelivered,
                             index_on_disk = true }
                     || {MsgId, SeqId, IsPersistent, IsDelivered} <- List]).

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

%% the first arg is the older gamma            
combine_gammas(#gamma { count = 0 }, #gamma { count = 0 }) ->
    #gamma { seq_id = undefined, count = 0 };
combine_gammas(#gamma { count = 0 }, #gamma {       } = B) -> B;
combine_gammas(#gamma {       } = A, #gamma { count = 0 }) -> A;
combine_gammas(#gamma { seq_id = SeqIdLow,  count = CountLow },
               #gamma { seq_id = SeqIdHigh, count = CountHigh}) ->
    true = SeqIdLow =< SeqIdHigh, %% ASSERTION
    #gamma { seq_id = SeqIdLow, count = CountLow + CountHigh}.

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

delete1(NextSeqId, Count, GammaSeqId, IndexState)
  when GammaSeqId >= NextSeqId ->
    {Count, IndexState};
delete1(NextSeqId, Count, GammaSeqId, IndexState) ->
    Gamma1SeqId = GammaSeqId + rabbit_queue_index:segment_size(),
    case rabbit_queue_index:read_segment_entries(GammaSeqId, IndexState) of
        {[], IndexState1} ->
            delete1(NextSeqId, Count, Gamma1SeqId, IndexState1);
        {List, IndexState1} -> 
            Q = betas_from_segment_entries(List),
            {QCount, IndexState2} = remove_queue_entries(Q, IndexState1),
            delete1(NextSeqId, Count + QCount, Gamma1SeqId, IndexState2)
    end.

purge1(Count, State = #vqstate { q3 = Q3, index_state = IndexState }) ->
    case queue:is_empty(Q3) of
        true ->
            {Q1Count, IndexState1} =
                remove_queue_entries(State #vqstate.q1, IndexState),
            {Count + Q1Count, State #vqstate { q1 = queue:new(),
                                               index_state = IndexState1 }};
        false ->
            {Q3Count, IndexState1} = remove_queue_entries(Q3, IndexState),
            purge1(Count + Q3Count,
                   maybe_gammas_to_betas(
                     State #vqstate { index_state = IndexState1,
                                      q3 = queue:new() }))
    end.

remove_queue_entries(Q, IndexState) ->
    {Count, MsgIds, SeqIds, IndexState1} =
        lists:foldl(
          fun (Entry, {CountN, MsgIdsAcc, SeqIdsAcc, IndexStateN}) ->
                  {MsgId, SeqId, IsDelivered, MsgOnDisk, IndexOnDisk} =
                      entry_salient_details(Entry),
                  IndexStateN1 = case IndexOnDisk andalso not IsDelivered of
                                     true -> rabbit_queue_index:write_delivered(
                                               SeqId, IndexStateN);
                                     false -> IndexStateN
                                 end,
                  SeqIdsAcc1 = case IndexOnDisk of
                                   true  -> [SeqId | SeqIdsAcc];
                                   false -> SeqIdsAcc
                               end,
                  MsgIdsAcc1 = case MsgOnDisk of
                                   true  -> [MsgId | MsgIdsAcc];
                                   false -> MsgIdsAcc
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

fetch_from_q3_or_gamma(State = #vqstate {
                         q1 = Q1, q2 = Q2, gamma = #gamma { count = GammaCount },
                         q3 = Q3, q4 = Q4, ram_msg_count = RamMsgCount }) ->
    case queue:out(Q3) of
        {empty, _Q3} ->
            0 = GammaCount, %% ASSERTION
            true = queue:is_empty(Q2), %% ASSERTION
            true = queue:is_empty(Q1), %% ASSERTION
            {empty, State};
        {{value,
          #beta { msg_id = MsgId, seq_id = SeqId, is_delivered = IsDelivered,
                  is_persistent = IsPersistent, index_on_disk = IndexOnDisk }},
         Q3a} ->
            {ok, Msg = #basic_message { is_persistent = IsPersistent,
                                        guid = MsgId }} =
                rabbit_msg_store:read(MsgId),
            Q4a = queue:in(
                    #alpha { msg = Msg, seq_id = SeqId,
                             is_delivered = IsDelivered, msg_on_disk = true,
                             index_on_disk = IndexOnDisk }, Q4),
            State1 = State #vqstate { q3 = Q3a, q4 = Q4a,
                                      ram_msg_count = RamMsgCount + 1 },
            State2 =
                case {queue:is_empty(Q3a), 0 == GammaCount} of
                    {true, true} ->
                        %% q3 is now empty, it wasn't before; gamma is
                        %% still empty. So q2 must be empty, and q1
                        %% can now be joined onto q4
                        true = queue:is_empty(Q2), %% ASSERTION
                        State1 #vqstate { q1 = queue:new(),
                                          q4 = queue:join(Q4a, Q1) };
                    {true, false} ->
                        maybe_gammas_to_betas(State1);
                    {false, _} ->
                        %% q3 still isn't empty, we've not touched
                        %% gamma, so the invariants between q1, q2,
                        %% gamma and q3 are maintained
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
        0 -> push_betas_to_gammas(State1);
        _ -> State1
    end.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

test_keep_msg_in_ram(SeqId, #vqstate { target_ram_msg_count = TargetRamMsgCount,
                                       ram_msg_count = RamMsgCount,
                                       q1 = Q1, q3 = Q3 }) ->
    case TargetRamMsgCount of
        undefined ->
            msg;
        0 ->
            case queue:out(Q3) of
                {empty, _Q3} ->
                    %% if TargetRamMsgCount == 0, we know we have no
                    %% alphas. If q3 is empty then gamma must be empty
                    %% too, so create a beta, which should end up in
                    %% q3
                    index;
                {{value, #beta { seq_id = OldSeqId }}, _Q3a} ->
                    %% Don't look at the current gamma as it may be
                    %% empty. If the SeqId is still within the current
                    %% segment, it'll be a beta, else it'll go into
                    %% gamma
                    case SeqId >= rabbit_queue_index:next_segment_boundary(OldSeqId) of
                        true -> neither;
                        false -> index
                    end
            end;
        _ when TargetRamMsgCount > RamMsgCount ->
            msg;
        _ ->
            case queue:is_empty(Q1) of
                true -> index;
                false -> msg %% can push out elders to disk
            end
    end.

publish(Msg, IsDelivered, PersistentMsgsAlreadyOnDisk,
        State = #vqstate { next_seq_id = SeqId, len = Len,
                           in_counter = InCount }) ->
    {SeqId, publish(test_keep_msg_in_ram(SeqId, State), Msg, SeqId, IsDelivered,
                    PersistentMsgsAlreadyOnDisk,
                    State #vqstate { next_seq_id = SeqId + 1, len = Len + 1,
                                     in_counter = InCount + 1 })}.

publish(msg, Msg = #basic_message { guid = MsgId,
                                    is_persistent = IsPersistent },
        SeqId, IsDelivered, PersistentMsgsAlreadyOnDisk,
        State = #vqstate { index_state = IndexState,
                           ram_msg_count = RamMsgCount }) ->
    MsgOnDisk =
        maybe_write_msg_to_disk(false, PersistentMsgsAlreadyOnDisk, Msg),
    {IndexOnDisk, IndexState1} =
        maybe_write_index_to_disk(false, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    Entry = #alpha { msg = Msg, seq_id = SeqId, is_delivered = IsDelivered,
                     msg_on_disk = MsgOnDisk, index_on_disk = IndexOnDisk },
    State1 = State #vqstate { ram_msg_count = RamMsgCount + 1,
                              index_state = IndexState1 },
    store_alpha_entry(Entry, State1);

publish(index, Msg = #basic_message { guid = MsgId,
                                      is_persistent = IsPersistent },
        SeqId, IsDelivered, PersistentMsgsAlreadyOnDisk,
        State = #vqstate { index_state = IndexState, q1 = Q1 }) ->
    true = maybe_write_msg_to_disk(true, PersistentMsgsAlreadyOnDisk, Msg),
    {IndexOnDisk, IndexState1} =
        maybe_write_index_to_disk(false, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    Entry = #beta { msg_id = MsgId, seq_id = SeqId, is_delivered = IsDelivered,
                    is_persistent = IsPersistent, index_on_disk = IndexOnDisk },
    State1 = State #vqstate { index_state = IndexState1 },
    true = queue:is_empty(Q1), %% ASSERTION
    store_beta_entry(Entry, State1);

publish(neither, Msg = #basic_message { guid = MsgId,
                                        is_persistent = IsPersistent },
        SeqId, IsDelivered, PersistentMsgsAlreadyOnDisk,
        State = #vqstate { index_state = IndexState, q1 = Q1, q2 = Q2,
                           gamma = Gamma }) ->
    true = maybe_write_msg_to_disk(true, PersistentMsgsAlreadyOnDisk, Msg),
    {true, IndexState1} =
        maybe_write_index_to_disk(true, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    true = queue:is_empty(Q1) andalso queue:is_empty(Q2), %% ASSERTION
    %% gamma may be empty, seq_id > next_segment_boundary from q3
    %% head, so we need to find where the segment boundary is before
    %% or equal to seq_id
    GammaSeqId = rabbit_queue_index:next_segment_boundary(SeqId) -
        rabbit_queue_index:segment_size(),
    Gamma1 = #gamma { seq_id = GammaSeqId, count = 1 },
    State #vqstate { index_state = IndexState1,
                     gamma = combine_gammas(Gamma, Gamma1) }.

store_alpha_entry(Entry = #alpha {}, State =
                  #vqstate { q1 = Q1, q2 = Q2,
                             gamma = #gamma { count = GammaCount },
                             q3 = Q3, q4 = Q4 }) ->
    case queue:is_empty(Q2) andalso GammaCount == 0 andalso
        queue:is_empty(Q3) of
        true ->
            State #vqstate { q4 = queue:in(Entry, Q4) };
        false ->
            maybe_push_q1_to_betas(State #vqstate { q1 = queue:in(Entry, Q1) })
    end.

store_beta_entry(Entry = #beta {}, State =
                 #vqstate { q2 = Q2, gamma = #gamma { count = GammaCount },
                            q3 = Q3 }) ->
    case GammaCount == 0 of
        true  -> State #vqstate { q3 = queue:in(Entry, Q3) };
        false -> State #vqstate { q2 = queue:in(Entry, Q2) }
    end.

%% Bool  IsPersistent PersistentMsgsAlreadyOnDisk | WriteToDisk?
%% -----------------------------------------------+-------------
%% false false        false                       | false      1
%% false true         false                       | true       2
%% false false        true                        | false      3
%% false true         true                        | false      4
%% true  false        false                       | true       5
%% true  true         false                       | true       6
%% true  false        true                        | true       7
%% true  true         true                        | false      8

%% (Bool and not (IsPersistent and PersistentMsgsAlreadyOnDisk)) or  | 5 6 7
%% (IsPersistent and (not PersistentMsgsAlreadyOnDisk))              | 2 6
maybe_write_msg_to_disk(Bool, PersistentMsgsAlreadyOnDisk,
                        Msg = #basic_message { guid = MsgId,
                                               is_persistent = IsPersistent })
  when (Bool andalso not (IsPersistent andalso PersistentMsgsAlreadyOnDisk))
       orelse (IsPersistent andalso not PersistentMsgsAlreadyOnDisk) ->
    ok = rabbit_msg_store:write(MsgId, ensure_binary_properties(Msg)),
    true;
maybe_write_msg_to_disk(_Bool, true, #basic_message { is_persistent = true }) ->
    true;
maybe_write_msg_to_disk(_Bool, _PersistentMsgsAlreadyOnDisk, _Msg) ->
    false.

maybe_write_index_to_disk(Bool, IsPersistent, MsgId, SeqId, IsDelivered,
                          IndexState) when Bool orelse IsPersistent ->
    IndexState1 = rabbit_queue_index:write_published(
                    MsgId, SeqId, IsPersistent, IndexState),
    {true, case IsDelivered of
               true  -> rabbit_queue_index:write_delivered(SeqId, IndexState1);
               false -> IndexState1
           end};
maybe_write_index_to_disk(_Bool, _IsPersistent, _MsgId, _SeqId, _IsDelivered,
                          IndexState) ->
    {false, IndexState}.

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------

maybe_gammas_to_betas(State = #vqstate { gamma = #gamma { count = 0 } }) ->
    State;
maybe_gammas_to_betas(State =
                      #vqstate { index_state = IndexState, q2 = Q2, q3 = Q3,
                                 target_ram_msg_count = TargetRamMsgCount,
                                 gamma = #gamma { seq_id = GammaSeqId,
                                                  count = GammaCount }}) ->
    case (not queue:is_empty(Q3)) andalso 0 == TargetRamMsgCount of
        true ->
            State;
        false ->
            %% either q3 is empty, in which case we load at least one
            %% segment, or TargetRamMsgCount > 0, meaning we should
            %% really be holding all the betas in memory.
            {List, IndexState1, Gamma1SeqId} =
                read_index_segment(GammaSeqId, IndexState),
            State1 = State #vqstate { index_state = IndexState1 },
            %% length(List) may be < segment_size because of acks. But
            %% it can't be []
            Q3a = queue:join(Q3, betas_from_segment_entries(List)),
            case GammaCount - length(List) of
                0 ->
                    %% gamma is now empty, but it wasn't before, so
                    %% can now join q2 onto q3
                    State1 #vqstate { gamma = #gamma { seq_id = undefined,
                                                       count = 0 },
                                      q2 = queue:new(),
                                      q3 = queue:join(Q3a, Q2) };
                N when N > 0 ->
                    maybe_gammas_to_betas(
                      State1 #vqstate { q3 = Q3a, 
                                        gamma = #gamma { seq_id = Gamma1SeqId,
                                                         count = N } })
            end
    end.

maybe_push_q1_to_betas(State = #vqstate { q1 = Q1 }) ->
    maybe_push_alphas_to_betas(
      fun queue:out/1,
      fun (Beta, Q1a, State1) ->
              %% these could legally go to q3 if gamma and q2 are empty
              store_beta_entry(Beta, State1 #vqstate { q1 = Q1a })
      end, Q1, State).

maybe_push_q4_to_betas(State = #vqstate { q4 = Q4 }) ->
    maybe_push_alphas_to_betas(
      fun queue:out_r/1,
      fun (Beta, Q4a, State1 = #vqstate { q3 = Q3 }) ->
              %% these must go to q3
              State1 #vqstate { q3 = queue:in_r(Beta, Q3), q4 = Q4a }
      end, Q4, State).

maybe_push_alphas_to_betas(_Generator, _Consumer, _Q, State =
                           #vqstate { ram_msg_count = RamMsgCount,
                                      target_ram_msg_count = TargetRamMsgCount })
  when TargetRamMsgCount == undefined orelse TargetRamMsgCount >= RamMsgCount ->
    State;
maybe_push_alphas_to_betas(Generator, Consumer, Q, State =
                           #vqstate { ram_msg_count = RamMsgCount }) ->
    case Generator(Q) of
        {empty, _Q} -> State;
        {{value,
          #alpha { msg = Msg = #basic_message { guid = MsgId,
                                                is_persistent = IsPersistent },
                   seq_id = SeqId, is_delivered = IsDelivered,
                   index_on_disk = IndexOnDisk }},
         Qa} ->
            true = maybe_write_msg_to_disk(true, true, Msg),
            Beta = #beta { msg_id = MsgId, seq_id = SeqId,
                           is_persistent = IsPersistent,
                           is_delivered = IsDelivered,
                           index_on_disk = IndexOnDisk },
            State1 = State #vqstate { ram_msg_count = RamMsgCount - 1 },
            maybe_push_alphas_to_betas(Generator, Consumer, Qa,
                                       Consumer(Beta, Qa, State1))
    end.

push_betas_to_gammas(State = #vqstate { q2 = Q2, gamma = Gamma, q3 = Q3,
                                        index_state = IndexState }) ->
    %% HighSeqId is high in the sense that it must be higher than the
    %% seq_id in Gamma, but it's also the lowest of the betas that we
    %% transfer from q2 to gamma.
    {HighSeqId, Len1, Q2a, IndexState1} =
        push_betas_to_gammas(fun queue:out/1, undefined, Q2, IndexState),
    Gamma1 = #gamma { seq_id = Gamma1SeqId } =
        combine_gammas(Gamma, #gamma { seq_id = HighSeqId, count = Len1 }),
    State1 = State #vqstate { q2 = Q2a, gamma = Gamma1,
                              index_state = IndexState1 },
    case queue:out(Q3) of
        {empty, _Q3} -> State1;
        {{value, #beta { seq_id = SeqId }}, _Q3a} -> 
            {{value, #beta { seq_id = SeqIdMax }}, _Q3b} = queue:out_r(Q3),
            Limit = rabbit_queue_index:next_segment_boundary(SeqId),
            %% ASSERTION
            true = Gamma1SeqId == undefined orelse Gamma1SeqId > SeqIdMax,
            case SeqIdMax < Limit of
                true -> %% already only holding LTE one segment indices in q3
                    State1;
                false ->
                    %% ASSERTION (sadly large!)
                    %% This says that if Gamma1SeqId /= undefined then
                    %% the gap from Limit to Gamma1SeqId is an integer
                    %% multiple of segment_size
                    0 = case Gamma1SeqId of
                            undefined -> 0;
                            _ -> (Gamma1SeqId - Limit) rem
                                     rabbit_queue_index:segment_size()
                        end,
                    %% LowSeqId is low in the sense that it must be
                    %% lower than the seq_id in gamma1, in fact either
                    %% gamma1 has undefined as its seq_id or there
                    %% does not exist a seq_id X s.t. X > LowSeqId and
                    %% X < gamma1's seq_id (would be +1 if it wasn't
                    %% for the possibility of gaps in the seq_ids).
                    %% But because we use queue:out_r, LowSeqId is
                    %% actually also the highest seq_id of the betas we
                    %% transfer from q3 to gammas.
                    {LowSeqId, Len2, Q3b, IndexState2} =
                        push_betas_to_gammas(fun queue:out_r/1, Limit, Q3,
                                             IndexState1),
                    true = Gamma1SeqId > LowSeqId, %% ASSERTION
                    Gamma2 = combine_gammas(
                               #gamma { seq_id = Limit, count = Len2}, Gamma1),
                    State1 #vqstate { q3 = Q3b, gamma = Gamma2,
                                      index_state = IndexState2 }
            end
    end.

push_betas_to_gammas(Generator, Limit, Q, IndexState) ->
    case Generator(Q) of
        {empty, Qa} -> {undefined, 0, Qa, IndexState};
        {{value, #beta { seq_id = SeqId }}, _Qa} ->
            {Count, Qb, IndexState1} =
                push_betas_to_gammas(Generator, Limit, Q, 0, IndexState),
            {SeqId, Count, Qb, IndexState1}
    end.

push_betas_to_gammas(Generator, Limit, Q, Count, IndexState) ->
    case Generator(Q) of
        {empty, Qa} -> {Count, Qa, IndexState};
        {{value, #beta { seq_id = SeqId }}, _Qa}
        when Limit /= undefined andalso SeqId < Limit ->
            {Count, Q, IndexState};
        {{value, #beta { msg_id = MsgId, seq_id = SeqId,
                         is_persistent = IsPersistent,
                         is_delivered = IsDelivered,
                         index_on_disk = IndexOnDisk}}, Qa} ->
            IndexState1 =
                case IndexOnDisk of
                    true -> IndexState;
                    false ->
                        {true, IndexState2} =
                            maybe_write_index_to_disk(
                              true, IsPersistent, MsgId,
                              SeqId, IsDelivered, IndexState),
                        IndexState2
                end,
            push_betas_to_gammas(Generator, Limit, Qa, Count + 1, IndexState1)
    end.
