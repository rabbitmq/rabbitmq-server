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

-export([init/1, in/3, set_queue_ram_duration_target/2, remeasure_egress_rate/1,
         out/1]).

-record(vqstate,
        { q1,
          q2,
          gamma,
          q3,
          q4,
          target_ram_msg_count,
          ram_msg_count,
          queue,
          index_state,
          next_seq_id,
          out_counter,
          egress_rate,
          old_egress_rate,
          avg_egress_rate,
          egress_rate_timestamp,
          prefetcher
        }).

-include("rabbit.hrl").

init(QueueName) ->
    {NextSeqId, IndexState} = rabbit_queue_index:init(QueueName),
    #vqstate { q1 = queue:new(), q2 = queue:new(),
               gamma = {undefined, 0},
               q3 = queue:new(), q4 = queue:new(),
               target_ram_msg_count = undefined,
               ram_msg_count = 0,
               queue = QueueName,
               index_state = IndexState,
               next_seq_id = NextSeqId,
               out_counter = 0,
               egress_rate = 0,
               old_egress_rate = 0,
               avg_egress_rate = 0,
               egress_rate_timestamp = now(),
               prefetcher = undefined
             }.

in(Msg, IsDelivered, State) ->
    in(test_keep_msg_in_ram(State), Msg, IsDelivered, State).

in(msg_and_index, Msg = #basic_message { guid = MsgId,
                                         is_persistent = IsPersistent },
   IsDelivered, State = #vqstate { index_state = IndexState,
                                   next_seq_id = SeqId,
                                   ram_msg_count = RamMsgCount
                                 }) ->
    MsgOnDisk = maybe_write_msg_to_disk(false, Msg),
    {IndexOnDisk, IndexState1} =
        maybe_write_index_to_disk(false, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    Entry =
        {msg_and_index, Msg, SeqId, IsDelivered, MsgOnDisk, IndexOnDisk},
    State1 = State #vqstate { next_seq_id = SeqId + 1,
                              ram_msg_count = RamMsgCount + 1,
                              index_state = IndexState1 },
    store_alpha_entry(Entry, State1);

in(just_index, Msg = #basic_message { guid = MsgId,
                                      is_persistent = IsPersistent },
   IsDelivered, State = #vqstate { index_state = IndexState,
                                   next_seq_id = SeqId, q1 = Q1 }) ->
    true = maybe_write_msg_to_disk(true, Msg),
    {IndexOnDisk, IndexState1} =
        maybe_write_index_to_disk(false, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    Entry = {index, MsgId, SeqId, IsPersistent, IsDelivered, IndexOnDisk},
    State1 = State #vqstate { next_seq_id = SeqId + 1,
                              index_state = IndexState1 },
    true = queue:is_empty(Q1), %% ASSERTION
    store_beta_entry(Entry, State1);

in(neither, Msg = #basic_message { guid = MsgId,
                                   is_persistent = IsPersistent },
   IsDelivered, State = #vqstate { index_state = IndexState,
                                   next_seq_id = SeqId,
                                   q1 = Q1, q2 = Q2,
                                   gamma = {GammaSeqId, GammaCount} }) ->
    true = maybe_write_msg_to_disk(true, Msg),
    {true, IndexState1} =
        maybe_write_index_to_disk(true, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    true = queue:is_empty(Q1) andalso queue:is_empty(Q2), %% ASSERTION
    State #vqstate { next_seq_id = SeqId + 1,
                     index_state = IndexState1,
                     gamma = {GammaSeqId, GammaCount + 1} }.

set_queue_ram_duration_target(
  DurationTarget, State = #vqstate { avg_egress_rate = EgressRate,
                                     target_ram_msg_count = TargetRamMsgCount
                                   }) ->
    TargetRamMsgCount1 = trunc(DurationTarget * EgressRate), %% msgs = sec * msgs/sec
    State1 = State #vqstate { target_ram_msg_count = TargetRamMsgCount1 },
    if TargetRamMsgCount == TargetRamMsgCount1 ->
            State1;
       TargetRamMsgCount < TargetRamMsgCount1 ->
            maybe_start_prefetcher(State1);
       true ->
            reduce_memory_use(State1)
    end.

remeasure_egress_rate(State = #vqstate { egress_rate = OldEgressRate,
                                         egress_rate_timestamp = Timestamp,
                                         out_counter = OutCount }) ->
    Now = now(),
    EgressRate = OutCount / timer:now_diff(Now, Timestamp),
    AvgEgressRate = (EgressRate + OldEgressRate) / 2,
    State #vqstate { old_egress_rate = OldEgressRate,
                     egress_rate = EgressRate,
                     avg_egress_rate = AvgEgressRate,
                     egress_rate_timestamp = Now,
                     out_counter = 0 }.

out(State =
    #vqstate { q4 = Q4,
               out_counter = OutCount, prefetcher = Prefetcher,
               index_state = IndexState }) ->
    case queue:out(Q4) of
        {empty, _Q4} when Prefetcher == undefined ->
            out_from_q3(State);
        {empty, _Q4} ->
            Q4a =
                case rabbit_queue_prefetcher:drain_and_stop(Prefetcher) of
                    empty -> Q4;
                    Q4b -> Q4b
                end,
            out(State #vqstate { q4 = Q4a, prefetcher = undefined });
        {{value,
          {msg_and_index, Msg = #basic_message { guid = MsgId },
           SeqId, IsDelivered, MsgOnDisk, IndexOnDisk}}, Q4a} ->
            IndexState1 =
                case IndexOnDisk andalso not IsDelivered of
                    true ->
                        rabbit_queue_index:write_delivered(SeqId, IndexState);
                    false ->
                        IndexState
                end,
            AckTag = case {IndexOnDisk, MsgOnDisk} of
                         {true,  true } -> {ack_index_and_store, MsgId, SeqId};
                         {false, true } -> {ack_store, MsgId};
                         {false, false} -> not_on_disk
                     end,
            {{Msg, IsDelivered, AckTag},
             State #vqstate { q4 = Q4a, out_counter = OutCount + 1,
                              index_state = IndexState1 }}
    end.

out_from_q3(State = #vqstate { q2 = Q2, index_state = IndexState,
                               gamma = {GammaSeqId, GammaCount}, q3 = Q3,
                               q4 = Q4 }) ->
    case queue:out(Q3) of
        {empty, _Q3} ->
            case GammaCount of
                0 ->
                    undefined = GammaSeqId, %% ASSERTION
                    true = queue:is_empty(Q2), %% ASSERTION
                    {empty, State};
                _ ->
                    {List = [_|_], IndexState1} =
                        rabbit_queue_index:read_segment_entries(GammaSeqId,
                                                                IndexState),
                    State1 = State #vqstate { index_state = IndexState1 },
                    Q3a = queue:from_list(List),
                    State2 =
                        case GammaCount - length(List) of
                            0 -> 
                                State1 #vqstate { gamma = {undefined, 0},
                                                  q2 = queue:new(),
                                                  q3 = queue:join(Q3a, Q2) };
                            N when N > 0 ->
                                State1 #vqstate { gamma =
                                                  {rabbit_queue_index:segment_size() +
                                                   GammaSeqId, N},
                                                  q3 = Q3a }
                        end,
                    out_from_q3(State2)
            end;
        {{value, {index, MsgId, SeqId, IsPersistent, IsDelivered, IndexOnDisk}},
         Q3a} ->
            {ok, Msg = #basic_message { is_persistent = IsPersistent,
                                        guid = MsgId }} =
                rabbit_msg_store:read(MsgId),
            State1 = #vqstate { q1 = Q1, q4 = Q4a } =
                State #vqstate { q3 = Q3a,
                                 q4 = queue:in({msg_and_index, Msg, SeqId,
                                                IsDelivered, true, IndexOnDisk},
                                               Q4) },
            State2 = case queue:is_empty(Q3a) andalso 0 == GammaCount of
                         true ->
                             true = queue:is_empty(Q2), %% ASSERTION
                             State1 #vqstate { q1 = queue:new(),
                                               q4 = queue:join(Q4a, Q1) };
                         false ->
                             State1
                     end,
            out(State2)
    end.

maybe_start_prefetcher(State) ->
    %% TODO
    State.

reduce_memory_use(State = #vqstate { ram_msg_count = RamMsgCount,
                                     target_ram_msg_count = TargetRamMsgCount })
  when TargetRamMsgCount >= RamMsgCount ->
    State;
reduce_memory_use(State =
                  #vqstate { target_ram_msg_count = TargetRamMsgCount }) ->
    State1 = #vqstate { ram_msg_count = RamMsgCount } =
        maybe_push_q1_to_betas(State),
    State2 = case TargetRamMsgCount >= RamMsgCount of
                 true  -> State1;
                 false -> maybe_push_q4_to_betas(State)
             end,
    case TargetRamMsgCount of
        0 -> push_betas_to_gammas(State);
        _ -> State2
    end.

maybe_write_msg_to_disk(Bool, Msg = #basic_message {
                                guid = MsgId, is_persistent = IsPersistent })
  when Bool orelse IsPersistent ->
    ok = rabbit_msg_store:write(MsgId, ensure_binary_properties(Msg)),
    true;
maybe_write_msg_to_disk(_Bool, _Msg) ->
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

test_keep_msg_in_ram(#vqstate { target_ram_msg_count = TargetRamMsgCount,
                                ram_msg_count = RamMsgCount,
                                q1 = Q1 }) ->
    case TargetRamMsgCount of
        undefined -> msg_and_index;
        0         -> neither;
        _ when TargetRamMsgCount > RamMsgCount ->
                     msg_and_index;
        _         -> case queue:is_empty(Q1) of
                         true -> just_index;
                         false -> msg_and_index %% can push out elders to disk
                     end
    end.

ensure_binary_properties(Msg = #basic_message { content = Content }) ->
    Msg #basic_message {
      content = rabbit_binary_parser:clear_decoded_content(
                  rabbit_binary_generator:ensure_content_encoded(Content)) }.

store_alpha_entry(Entry, State = #vqstate { q1 = Q1, q2 = Q2,
                                            gamma = {_GammaSeqId, GammaCount},
                                            q3 = Q3, q4 = Q4 }) ->
    case queue:is_empty(Q1) andalso queue:is_empty(Q2) andalso 
        GammaCount == 0 andalso queue:is_empty(Q3) of
        true ->
            State #vqstate { q4 = queue:in(Entry, Q4) };
        false ->
            maybe_push_q1_to_betas(State #vqstate { q1 = queue:in(Entry, Q1) })
    end.

store_beta_entry(Entry, State =
                 #vqstate { q2 = Q2, gamma = {_GammaSeqId, GammaCount},
                            q3 = Q3 }) ->
    case queue:is_empty(Q2) andalso GammaCount == 0 of
        true  -> State #vqstate { q3 = queue:in(Entry, Q3) };
        false -> State #vqstate { q2 = queue:in(Entry, Q2) }
    end.

maybe_push_q1_to_betas(State =
                       #vqstate { ram_msg_count = RamMsgCount,
                                  target_ram_msg_count = TargetRamMsgCount
                                }) when TargetRamMsgCount >= RamMsgCount ->
    State;
maybe_push_q1_to_betas(State = #vqstate { ram_msg_count = RamMsgCount,
                                          q1 = Q1 }) ->
    case queue:out(Q1) of
        {empty, _Q1} -> State;
        {{value, {msg_and_index, Msg = #basic_message {
                                   guid = MsgId, is_persistent = IsPersistent },
                  SeqId, IsDelivered, MsgOnDisk, IndexOnDisk}}, Q1a} ->
            true = case MsgOnDisk of
                       true -> true;
                       false -> maybe_write_msg_to_disk(true, Msg)
                   end,
            maybe_push_q1_to_betas(
              store_beta_entry({index, MsgId, SeqId, IsPersistent, IsDelivered,
                                IndexOnDisk},
                               State #vqstate { ram_msg_count = RamMsgCount - 1,
                                                q1 = Q1a }))
    end.

maybe_push_q4_to_betas(State =
                       #vqstate { ram_msg_count = RamMsgCount,
                                  target_ram_msg_count = TargetRamMsgCount
                                }) when TargetRamMsgCount >= RamMsgCount ->
    State;
maybe_push_q4_to_betas(State = #vqstate { ram_msg_count = RamMsgCount,
                                          q4 = Q4, q3 = Q3 }) ->
    case queue:out_r(Q4) of
        {empty, _Q4} -> State;
        {{value, {msg_and_index, Msg = #basic_message {
                                   guid = MsgId, is_persistent = IsPersistent },
                  SeqId, IsDelivered, MsgOnDisk, IndexOnDisk}}, Q4a} ->
            true = case MsgOnDisk of
                       true -> true;
                       false -> maybe_write_msg_to_disk(true, Msg)
                   end,
            Q3a = queue:in_r({index, MsgId, SeqId, IsPersistent, IsDelivered,
                              IndexOnDisk}, Q3),
            maybe_push_q4_to_betas(
              State #vqstate { ram_msg_count = RamMsgCount - 1,
                               q3 = Q3a, q4 = Q4a })
    end.

push_betas_to_gammas(State = #vqstate { q2 = Q2, gamma = Gamma, q3 = Q3,
                                        index_state = IndexState }) ->
    %% HighSeqId is high in the sense that it must be higher than the
    %% seqid in Gamma, but it's also the lowest of the betas that we
    %% transfer from q2 to gamma.
    {HighSeqId, Len1, Q2a, IndexState1} =
        push_betas_to_gammas(fun queue:out/1, undefined, Q2, IndexState),
    Gamma1 = {Gamma1SeqId, _} = combine_gammas(Gamma, {HighSeqId, Len1}),
    State1 = State #vqstate { q2 = Q2a,
                              gamma = Gamma1,
                              index_state = IndexState1 },
    case queue:out(Q3) of
        {empty, _Q3} -> State1;
        {{value, {index, _MsgId, SeqId, _IsPersistent, _IsDelivered,
                  _IndexOnDisk}}, _Q3a} -> 
            Limit = rabbit_queue_index:next_segment_boundary(SeqId),
            case Limit == Gamma1SeqId of
                true -> %% already only holding the minimum, nothing to do
                    State1;
                false ->
                    %% ASSERTION
                    true = Gamma1SeqId == undefined orelse
                        Gamma1SeqId == Limit + rabbit_queue_index:segment_size(),
                    %% LowSeqId is low in the sense that it must be
                    %% lower than the seqid in Gamma1, in fact either
                    %% gamma1 has undefined as its seqid or its seqid
                    %% is LowSeqId + 1. But because we use
                    %% queue:out_r, LowSeqId is actually also the
                    %% highest seqid of the betas we transfer from q3
                    %% to gammas.
                    {LowSeqId, Len2, Q3b, IndexState2} =
                        push_betas_to_gammas(fun queue:out_r/1, Limit - 1, Q3,
                                             IndexState1),
                    Gamma1SeqId = LowSeqId + 1, %% ASSERTION
                    Gamma2 = combine_gammas({Limit, Len2}, Gamma1),
                    State1 #vqstate { q3 = Q3b, gamma = Gamma2,
                                      index_state = IndexState2 }
            end
    end.

push_betas_to_gammas(Generator, Limit, Q, IndexState) ->
    case Generator(Q) of
        {empty, Qa} -> {undefined, 0, Qa, IndexState};
        {{value, {index, _MsgId, SeqId, _IsPersistent, _IsDelivered,
                  _IndexOnDisk}}, _Qa} ->
            {Count, Qb, IndexState1} =
                push_betas_to_gammas(Generator, Limit, Q, 0, IndexState),
            {SeqId, Count, Qb, IndexState1}
    end.

push_betas_to_gammas(Generator, Limit, Q, Count, IndexState) ->
    case Generator(Q) of
        {empty, Qa} -> {Count, Qa, IndexState};
        {{value, {index, _MsgId, Limit, _IsPersistent, _IsDelivered,
                  _IndexOnDisk}}, _Qa} ->
            {Count, Q, IndexState};
        {{value, {index, MsgId, SeqId, IsPersistent, IsDelivered,
                  IndexOnDisk}}, Qa} ->
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
            
combine_gammas({_, 0}, {_, 0}) -> {undefined, 0};
combine_gammas({_, 0}, B     ) -> B;
combine_gammas(A     , {_, 0}) -> A;
combine_gammas({SeqIdLow, CountLow}, {SeqIdHigh, CountHigh}) ->
    SeqIdHigh = SeqIdLow + CountLow, %% ASSERTION
    {SeqIdLow, CountLow + CountHigh}.
