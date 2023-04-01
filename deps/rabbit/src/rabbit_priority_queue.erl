%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2015-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_priority_queue).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("amqqueue.hrl").

-behaviour(rabbit_backing_queue).

%% enabled unconditionally. Disabling priority queuing after
%% it has been enabled is dangerous.
-rabbit_boot_step({?MODULE,
                   [{description, "enable priority queue"},
                    {mfa,         {?MODULE, enable, []}},
                    {requires,    pre_boot},
                    {enables,     kernel_ready}]}).

-export([enable/0]).

-export([start/2, stop/1]).

-export([init/3, terminate/2, delete_and_terminate/2, delete_crashed/1,
         purge/1, purge_acks/1,
         publish/6, publish_delivered/5, discard/4, drain_confirmed/1,
         batch_publish/4, batch_publish_delivered/4,
         dropwhile/2, fetchwhile/4, fetch/2, drop/2, ack/2, requeue/2,
         ackfold/4, fold/3, len/1, is_empty/1, depth/1,
         set_ram_duration_target/2, ram_duration/1, needs_timeout/1, timeout/1,
         handle_pre_hibernate/1, resume/1, msg_rates/1,
         info/2, invoke/3, is_duplicate/2, set_queue_mode/2,
         set_queue_version/2,
         zip_msgs_and_acks/4]).

-record(state, {bq, bqss, max_priority}).
-record(passthrough, {bq, bqs}).

%% See 'note on suffixes' below
-define(passthrough1(F), State#passthrough{bqs = BQ:F}).
-define(passthrough2(F),
        {Res, BQS1} = BQ:F, {Res, State#passthrough{bqs = BQS1}}).
-define(passthrough3(F),
        {Res1, Res2, BQS1} = BQ:F, {Res1, Res2, State#passthrough{bqs = BQS1}}).

%% This module adds support for priority queues.
%%
%% Priority queues have one backing queue per priority. Backing queue functions
%% then produce a list of results for each BQ and fold over them, sorting
%% by priority.
%%
%%For queues that do not
%% have priorities enabled, the functions in this module delegate to
%% their "regular" backing queue module counterparts. See the `passthrough`
%% record and passthrough{1,2,3} macros.
%%
%% Delivery to consumers happens by first "running" the queue with
%% the highest priority until there are no more messages to deliver,
%% then the next one, and so on. This offers good prioritisation
%% but may result in lower priority messages not being delivered
%% when there's a high ingress rate of messages with higher priority.

enable() ->
    {ok, RealBQ} = application:get_env(rabbit, backing_queue_module),
    case RealBQ of
        ?MODULE -> ok;
        _       -> rabbit_log:info("Priority queues enabled, real BQ is ~ts",
                                   [RealBQ]),
                   application:set_env(
                     rabbitmq_priority_queue, backing_queue_module, RealBQ),
                   application:set_env(rabbit, backing_queue_module, ?MODULE)
    end.

%%----------------------------------------------------------------------------

start(VHost, QNames) ->
    BQ = bq(),
    %% TODO this expand-collapse dance is a bit ridiculous but it's what
    %% rabbit_amqqueue:recover/0 expects. We could probably simplify
    %% this if we rejigged recovery a bit.
    {DupNames, ExpNames} = expand_queues(QNames),
    case BQ:start(VHost, ExpNames) of
        {ok, ExpRecovery} ->
            {ok, collapse_recovery(QNames, DupNames, ExpRecovery)};
        Else ->
            Else
    end.

stop(VHost) ->
    BQ = bq(),
    BQ:stop(VHost).

%%----------------------------------------------------------------------------

mutate_name(P, Q) when ?is_amqqueue(Q) ->
    Res0 = #resource{name = QNameBin0} = amqqueue:get_name(Q),
    QNameBin1 = mutate_name_bin(P, QNameBin0),
    Res1 = Res0#resource{name = QNameBin1},
    amqqueue:set_name(Q, Res1).

mutate_name_bin(P, NameBin) ->
    <<NameBin/binary, 0, P:8>>.

expand_queues(QNames) ->
    Qs = rabbit_db_queue:get_many_durable(QNames),
    lists:unzip(lists:append([expand_queue(Q) || Q <- Qs])).

expand_queue(Q) ->
    #resource{name = QNameBin} = QName = amqqueue:get_name(Q),
    case priorities(Q) of
        none -> [{QName, QName}];
        Ps   -> [{QName, QName#resource{name = mutate_name_bin(P, QNameBin)}}
                   || P <- Ps]
    end.

collapse_recovery(QNames, DupNames, Recovery) ->
    NameToTerms = lists:foldl(fun({Name, RecTerm}, Dict) ->
                                      dict:append(Name, RecTerm, Dict)
                              end, dict:new(), lists:zip(DupNames, Recovery)),
    [dict:fetch(Name, NameToTerms) || Name <- QNames].

priorities(Q) when ?is_amqqueue(Q) ->
    Args = amqqueue:get_arguments(Q),
    Ints = [long, short, signedint, byte, unsignedbyte, unsignedshort, unsignedint],
    case rabbit_misc:table_lookup(Args, <<"x-max-priority">>) of
        {Type, RequestedMax} ->
            case lists:member(Type, Ints) of
                false -> none;
                true  ->
                    Max = min(RequestedMax, ?MAX_SUPPORTED_PRIORITY),
                    lists:reverse(lists:seq(0, Max))
            end;
        _                    -> none
    end.

%%----------------------------------------------------------------------------

init(Q, Recover, AsyncCallback) ->
    BQ = bq(),
    case priorities(Q) of
        none -> RealRecover = case Recover of
                                  [R] -> R; %% [0]
                                  R   -> R
                              end,
                #passthrough{bq  = BQ,
                             bqs = BQ:init(Q, RealRecover, AsyncCallback)};
        Ps   -> Init = fun (P, Term) ->
                               BQ:init(
                                 mutate_name(P, Q), Term,
                                 fun (M, F) -> AsyncCallback(M, {P, F}) end)
                       end,
                BQSs = case have_recovery_terms(Recover) of
                           false -> [{P, Init(P, Recover)} || P <- Ps];
                           _     -> PsTerms = lists:zip(Ps, Recover),
                                    [{P, Init(P, Term)} || {P, Term} <- PsTerms]
                       end,
                #state{bq   = BQ,
                       bqss = BQSs,
                       max_priority = hd(Ps)}
    end.
%% [0] collapse_recovery has the effect of making a list of recovery
%% terms in priority order, even for non priority queues. It's easier
%% to do that and "unwrap" in init/3 than to have collapse_recovery be
%% aware of non-priority queues.

have_recovery_terms(new)                -> false;
have_recovery_terms(non_clean_shutdown) -> false;
have_recovery_terms(_)                  -> true.

terminate(Reason, State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) -> BQ:terminate(Reason, BQSN) end, State);
terminate(Reason, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(terminate(Reason, BQS)).

delete_and_terminate(Reason, State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) ->
                     BQ:delete_and_terminate(Reason, BQSN)
             end, State);
delete_and_terminate(Reason, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(delete_and_terminate(Reason, BQS)).

delete_crashed(Q) ->
    BQ = bq(),
    case priorities(Q) of
        none -> BQ:delete_crashed(Q);
        Ps   -> [BQ:delete_crashed(mutate_name(P, Q)) || P <- Ps]
    end.

purge(State = #state{bq = BQ}) ->
    fold_add2(fun (_P, BQSN) -> BQ:purge(BQSN) end, State);
purge(State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(purge(BQS)).

purge_acks(State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) -> BQ:purge_acks(BQSN) end, State);
purge_acks(State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(purge_acks(BQS)).

publish(Msg, MsgProps, IsDelivered, ChPid, Flow, State = #state{bq = BQ}) ->
    pick1(fun (_P, BQSN) ->
                  BQ:publish(Msg, MsgProps, IsDelivered, ChPid, Flow, BQSN)
          end, Msg, State);
publish(Msg, MsgProps, IsDelivered, ChPid, Flow,
        State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(publish(Msg, MsgProps, IsDelivered, ChPid, Flow, BQS)).

batch_publish(Publishes, ChPid, Flow, State = #state{bq = BQ, bqss = [{MaxP, _} |_]}) ->
    PubMap = partition_publish_batch(Publishes, MaxP),
    lists:foldl(
      fun ({Priority, Pubs}, St) ->
              pick1(fun (_P, BQSN) ->
                            BQ:batch_publish(Pubs, ChPid, Flow, BQSN)
                    end, Priority, St)
      end, State, maps:to_list(PubMap));
batch_publish(Publishes, ChPid, Flow,
              State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(batch_publish(Publishes, ChPid, Flow, BQS)).

publish_delivered(Msg, MsgProps, ChPid, Flow, State = #state{bq = BQ}) ->
    pick2(fun (P, BQSN) ->
                  {AckTag, BQSN1} = BQ:publish_delivered(
                                      Msg, MsgProps, ChPid, Flow, BQSN),
                  {{P, AckTag}, BQSN1}
          end, Msg, State);
publish_delivered(Msg, MsgProps, ChPid, Flow,
                  State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(publish_delivered(Msg, MsgProps, ChPid, Flow, BQS)).

batch_publish_delivered(Publishes, ChPid, Flow, State = #state{bq = BQ, bqss = [{MaxP, _} |_]}) ->
    PubMap = partition_publish_delivered_batch(Publishes, MaxP),
    {PrioritiesAndAcks, State1} =
        lists:foldl(
          fun ({Priority, Pubs}, {PriosAndAcks, St}) ->
                  {PriosAndAcks1, St1} =
                      pick2(fun (P, BQSN) ->
                                    {AckTags, BQSN1} =
                                        BQ:batch_publish_delivered(
                                          Pubs, ChPid, Flow, BQSN),
                                    {priority_on_acktags(P, AckTags), BQSN1}
                            end, Priority, St),
                  {[PriosAndAcks1 | PriosAndAcks], St1}
          end, {[], State}, maps:to_list(PubMap)),
    {lists:reverse(PrioritiesAndAcks), State1};
batch_publish_delivered(Publishes, ChPid, Flow,
                        State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(batch_publish_delivered(Publishes, ChPid, Flow, BQS)).

discard(Msg, ChPid, Flow, State = #state{bq = BQ}) ->
    pick1(fun (_P, BQSN) ->
                  BQ:discard(Msg, ChPid, Flow, BQSN)
          end, Msg, State);
discard(Msg, ChPid, Flow, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(discard(Msg, ChPid, Flow, BQS)).

drain_confirmed(State = #state{bq = BQ}) ->
    fold_append2(fun (_P, BQSN) -> BQ:drain_confirmed(BQSN) end, State);
drain_confirmed(State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(drain_confirmed(BQS)).

dropwhile(Pred, State = #state{bq = BQ}) ->
    find2(fun (_P, BQSN) -> BQ:dropwhile(Pred, BQSN) end, undefined, State);
dropwhile(Pred, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(dropwhile(Pred, BQS)).

%% TODO this is a bit nasty. In the one place where fetchwhile/4 is
%% actually used the accumulator is a list of acktags, which of course
%% we need to mutate - so we do that although we are encoding an
%% assumption here.
fetchwhile(Pred, Fun, Acc, State = #state{bq = BQ}) ->
    findfold3(
      fun (P, BQSN, AccN) ->
              {Res, AccN1, BQSN1} = BQ:fetchwhile(Pred, Fun, AccN, BQSN),
              {Res, priority_on_acktags(P, AccN1), BQSN1}
      end, Acc, undefined, State);
fetchwhile(Pred, Fun, Acc, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough3(fetchwhile(Pred, Fun, Acc, BQS)).

fetch(AckRequired, State = #state{bq = BQ}) ->
    find2(
      fun (P, BQSN) ->
              case BQ:fetch(AckRequired, BQSN) of
                  {empty,            BQSN1} -> {empty, BQSN1};
                  {{Msg, Del, ATag}, BQSN1} -> {{Msg, Del, {P, ATag}}, BQSN1}
              end
      end, empty, State);
fetch(AckRequired, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(fetch(AckRequired, BQS)).

drop(AckRequired, State = #state{bq = BQ}) ->
    find2(fun (P, BQSN) ->
                  case BQ:drop(AckRequired, BQSN) of
                      {empty,           BQSN1} -> {empty, BQSN1};
                      {{MsgId, AckTag}, BQSN1} -> {{MsgId, {P, AckTag}}, BQSN1}
                  end
          end, empty, State);
drop(AckRequired, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(drop(AckRequired, BQS)).

ack(AckTags, State = #state{bq = BQ}) ->
    fold_by_acktags2(fun (AckTagsN, BQSN) ->
                             BQ:ack(AckTagsN, BQSN)
                     end, AckTags, State);
ack(AckTags, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(ack(AckTags, BQS)).

requeue(AckTags, State = #state{bq = BQ}) ->
    fold_by_acktags2(fun (AckTagsN, BQSN) ->
                             BQ:requeue(AckTagsN, BQSN)
                     end, AckTags, State);
requeue(AckTags, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(requeue(AckTags, BQS)).

%% Similar problem to fetchwhile/4
ackfold(MsgFun, Acc, State = #state{bq = BQ}, AckTags) ->
    AckTagsByPriority = partition_acktags(AckTags),
    fold2(
      fun (P, BQSN, AccN) ->
              case maps:find(P, AckTagsByPriority) of
                  {ok, ATagsN} -> {AccN1, BQSN1} =
                                      BQ:ackfold(MsgFun, AccN, BQSN, ATagsN),
                                  {priority_on_acktags(P, AccN1), BQSN1};
                  error        -> {AccN, BQSN}
              end
      end, Acc, State);
ackfold(MsgFun, Acc, State = #passthrough{bq = BQ, bqs = BQS}, AckTags) ->
    ?passthrough2(ackfold(MsgFun, Acc, BQS, AckTags)).

fold(Fun, Acc, State = #state{bq = BQ}) ->
    fold2(fun (_P, BQSN, AccN) -> BQ:fold(Fun, AccN, BQSN) end, Acc, State);
fold(Fun, Acc, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(fold(Fun, Acc, BQS)).

len(#state{bq = BQ, bqss = BQSs}) ->
    add0(fun (_P, BQSN) -> BQ:len(BQSN) end, BQSs);
len(#passthrough{bq = BQ, bqs = BQS}) ->
    BQ:len(BQS).

is_empty(#state{bq = BQ, bqss = BQSs}) ->
    all0(fun (_P, BQSN) -> BQ:is_empty(BQSN) end, BQSs);
is_empty(#passthrough{bq = BQ, bqs = BQS}) ->
    BQ:is_empty(BQS).

depth(#state{bq = BQ, bqss = BQSs}) ->
    add0(fun (_P, BQSN) -> BQ:depth(BQSN) end, BQSs);
depth(#passthrough{bq = BQ, bqs = BQS}) ->
    BQ:depth(BQS).

set_ram_duration_target(DurationTarget, State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) ->
                     BQ:set_ram_duration_target(DurationTarget, BQSN)
             end, State);
set_ram_duration_target(DurationTarget,
                        State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(set_ram_duration_target(DurationTarget, BQS)).

ram_duration(State = #state{bq = BQ}) ->
    fold_min2(fun (_P, BQSN) -> BQ:ram_duration(BQSN) end, State);
ram_duration(State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(ram_duration(BQS)).

needs_timeout(#state{bq = BQ, bqss = BQSs}) ->
    fold0(fun (_P, _BQSN, timed) -> timed;
              (_P, BQSN,  idle)  -> case BQ:needs_timeout(BQSN) of
                                        timed -> timed;
                                        _     -> idle
                                    end;
              (_P, BQSN,  false) -> BQ:needs_timeout(BQSN)
          end, false, BQSs);
needs_timeout(#passthrough{bq = BQ, bqs = BQS}) ->
    BQ:needs_timeout(BQS).

timeout(State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) -> BQ:timeout(BQSN) end, State);
timeout(State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(timeout(BQS)).

handle_pre_hibernate(State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) ->
                  BQ:handle_pre_hibernate(BQSN)
          end, State);
handle_pre_hibernate(State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(handle_pre_hibernate(BQS)).

resume(State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) -> BQ:resume(BQSN) end, State);
resume(State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(resume(BQS)).

msg_rates(#state{bq = BQ, bqss = BQSs}) ->
    fold0(fun(_P, BQSN, {InN, OutN}) ->
                  {In, Out} = BQ:msg_rates(BQSN),
                  {InN + In, OutN + Out}
          end, {0.0, 0.0}, BQSs);
msg_rates(#passthrough{bq = BQ, bqs = BQS}) ->
    BQ:msg_rates(BQS).

info(backing_queue_status, #state{bq = BQ, bqss = BQSs}) ->
    fold0(fun (P, BQSN, Acc) ->
                  combine_status(P, BQ:info(backing_queue_status, BQSN), Acc)
          end, nothing, BQSs);
info(head_message_timestamp, #state{bq = BQ, bqss = BQSs}) ->
    find_head_message_timestamp(BQ, BQSs, '');
info(online, _) ->
    '';
info(Item, #state{bq = BQ, bqss = BQSs}) ->
    fold0(fun (_P, BQSN, Acc) ->
                 add(Acc, BQ:info(Item, BQSN))
          end, 0, BQSs);
info(Item, #passthrough{bq = BQ, bqs = BQS}) ->
    BQ:info(Item, BQS).

add(Acc, Value) when is_number(Acc), is_number(Value) -> Acc + Value;
add(_, Value) -> Value.

invoke(Mod, {P, Fun}, State = #state{bq = BQ}) ->
    pick1(fun (_P, BQSN) -> BQ:invoke(Mod, Fun, BQSN) end, P, State);
invoke(Mod, Fun, State = #state{bq = BQ, max_priority = P}) ->
    pick1(fun (_P, BQSN) -> BQ:invoke(Mod, Fun, BQSN) end, P, State);
invoke(Mod, Fun, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(invoke(Mod, Fun, BQS)).

is_duplicate(Msg, State = #state{bq = BQ}) ->
    pick2(fun (_P, BQSN) -> BQ:is_duplicate(Msg, BQSN) end, Msg, State);
is_duplicate(Msg, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough2(is_duplicate(Msg, BQS)).

set_queue_mode(Mode, State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) -> BQ:set_queue_mode(Mode, BQSN) end, State);
set_queue_mode(Mode, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(set_queue_mode(Mode, BQS)).

set_queue_version(Version, State = #state{bq = BQ}) ->
    foreach1(fun (_P, BQSN) -> BQ:set_queue_version(Version, BQSN) end, State);
set_queue_version(Version, State = #passthrough{bq = BQ, bqs = BQS}) ->
    ?passthrough1(set_queue_version(Version, BQS)).

zip_msgs_and_acks(Msgs, AckTags, Accumulator, #state{bqss = [{MaxP, _} |_]}) ->
    MsgsByPriority = partition_publish_delivered_batch(Msgs, MaxP),
    lists:foldl(fun (Acks, MAs) ->
                        {P, _AckTag} = hd(Acks),
                        Pubs = maps:get(P, MsgsByPriority),
                        MAs0 = zip_msgs_and_acks(Pubs, Acks),
                        MAs ++ MAs0
                end, Accumulator, AckTags);
zip_msgs_and_acks(Msgs, AckTags, Accumulator,
                  #passthrough{bq = BQ, bqs = BQS}) ->
    BQ:zip_msgs_and_acks(Msgs, AckTags, Accumulator, BQS).

%%----------------------------------------------------------------------------

bq() ->
    {ok, RealBQ} = application:get_env(
                     rabbitmq_priority_queue, backing_queue_module),
    RealBQ.

%% Note on suffixes: Many utility functions here have suffixes telling
%% you the arity of the return type of the BQ function they are
%% designed to work with.
%%
%% 0 - BQ function returns a value and does not modify state
%% 1 - BQ function just returns a new state
%% 2 - BQ function returns a 2-tuple of {Result, NewState}
%% 3 - BQ function returns a 3-tuple of {Result1, Result2, NewState}

%% Fold over results
fold0(Fun,  Acc, [{P, BQSN} | Rest]) -> fold0(Fun, Fun(P, BQSN, Acc), Rest);
fold0(_Fun, Acc, [])                 -> Acc.

%% Do all BQs match?
all0(Pred, BQSs) -> fold0(fun (_P, _BQSN, false) -> false;
                              (P,  BQSN,  true)  -> Pred(P, BQSN)
                          end, true, BQSs).

%% Sum results
add0(Fun, BQSs) -> fold0(fun (P, BQSN, Acc) -> Acc + Fun(P, BQSN) end, 0, BQSs).

%% Apply for all states
foreach1(Fun, State = #state{bqss = BQSs}) ->
    a(State#state{bqss = foreach1(Fun, BQSs, [])}).
foreach1(Fun, [{Priority, BQSN} | Rest], BQSAcc) ->
    BQSN1 = Fun(Priority, BQSN),
    foreach1(Fun, Rest, [{Priority, BQSN1} | BQSAcc]);
foreach1(_Fun, [], BQSAcc) ->
    lists:reverse(BQSAcc).

%% For a given thing, just go to its BQ
pick1(Fun, Prioritisable, #state{bqss = BQSs} = State) ->
    {P, BQSN} = priority_bq(Prioritisable, BQSs),
    a(State#state{bqss = bq_store(P, Fun(P, BQSN), BQSs)}).

%% Fold over results
fold2(Fun, Acc, State = #state{bqss = BQSs}) ->
    {Res, BQSs1} = fold2(Fun, Acc, BQSs, []),
    {Res, a(State#state{bqss = BQSs1})}.

fold2(Fun, Acc, [{P, BQSN} | Rest], BQSAcc) ->
    {Acc1, BQSN1} = Fun(P, BQSN, Acc),
    fold2(Fun, Acc1, Rest, [{P, BQSN1} | BQSAcc]);
fold2(_Fun, Acc, [], BQSAcc) ->
    {Acc, lists:reverse(BQSAcc)}.

%% Fold over results assuming results are lists and we want to append them
fold_append2(Fun, State) ->
    fold2(fun (P, BQSN, Acc) ->
                  {Res, BQSN1} = Fun(P, BQSN),
                  {Res ++ Acc, BQSN1}
          end, [], State).

%% Fold over results assuming results are numbers and we want to sum them
fold_add2(Fun, State) ->
    fold2(fun (P, BQSN, Acc) ->
                  {Res, BQSN1} = Fun(P, BQSN),
                  {add_maybe_infinity(Res, Acc), BQSN1}
          end, 0, State).

%% Fold over results assuming results are numbers and we want the minimum
fold_min2(Fun, State) ->
    fold2(fun (P, BQSN, Acc) ->
                  {Res, BQSN1} = Fun(P, BQSN),
                  {erlang:min(Res, Acc), BQSN1}
          end, infinity, State).

%% Fold over results assuming results are lists and we want to append
%% them, and also that we have some AckTags we want to pass in to each
%% invocation.
fold_by_acktags2(Fun, AckTags, State) ->
    AckTagsByPriority = partition_acktags(AckTags),
    fold_append2(fun (P, BQSN) ->
                         case maps:find(P, AckTagsByPriority) of
                             {ok, AckTagsN} -> Fun(AckTagsN, BQSN);
                             error          -> {[], BQSN}
                         end
                 end, State).

%% For a given thing, just go to its BQ
pick2(Fun, Prioritisable, #state{bqss = BQSs} = State) ->
    {P, BQSN} = priority_bq(Prioritisable, BQSs),
    {Res, BQSN1} = Fun(P, BQSN),
    {Res, a(State#state{bqss = bq_store(P, BQSN1, BQSs)})}.

%% Run through BQs in priority order until one does not return
%% {NotFound, NewState} or we have gone through them all.
find2(Fun, NotFound, State = #state{bqss = BQSs}) ->
    {Res, BQSs1} = find2(Fun, NotFound, BQSs, []),
    {Res, a(State#state{bqss = BQSs1})}.
find2(Fun, NotFound, [{P, BQSN} | Rest], BQSAcc) ->
    case Fun(P, BQSN) of
        {NotFound, BQSN1} -> find2(Fun, NotFound, Rest, [{P, BQSN1} | BQSAcc]);
        {Res, BQSN1}      -> {Res, lists:reverse([{P, BQSN1} | BQSAcc]) ++ Rest}
    end;
find2(_Fun, NotFound, [], BQSAcc) ->
    {NotFound, lists:reverse(BQSAcc)}.

%% Run through BQs in priority order like find2 but also folding as we go.
findfold3(Fun, Acc, NotFound, State = #state{bqss = BQSs}) ->
    {Res, Acc1, BQSs1} = findfold3(Fun, Acc, NotFound, BQSs, []),
    {Res, Acc1, a(State#state{bqss = BQSs1})}.
findfold3(Fun, Acc, NotFound, [{P, BQSN} | Rest], BQSAcc) ->
    case Fun(P, BQSN, Acc) of
        {NotFound, Acc1, BQSN1} ->
            findfold3(Fun, Acc1, NotFound, Rest, [{P, BQSN1} | BQSAcc]);
        {Res, Acc1, BQSN1} ->
            {Res, Acc1, lists:reverse([{P, BQSN1} | BQSAcc]) ++ Rest}
    end;
findfold3(_Fun, Acc, NotFound, [], BQSAcc) ->
    {NotFound, Acc, lists:reverse(BQSAcc)}.

bq_fetch(P, []) -> exit({not_found, P});
bq_fetch(P, [{P,  BQSN} | _]) -> {P, BQSN};
bq_fetch(P, [{_, _BQSN} | T]) -> bq_fetch(P, T).

bq_store(P, BQS, BQSs) ->
    [{PN, case PN of
              P -> BQS;
              _ -> BQSN
          end} || {PN, BQSN} <- BQSs].

%%
a(State = #state{bqss = BQSs}) ->
    Ps = [P || {P, _} <- BQSs],
    case lists:reverse(lists:usort(Ps)) of
        Ps -> State;
        _  -> exit({bad_order, Ps})
    end.

%%----------------------------------------------------------------------------
partition_publish_batch(Publishes, MaxP) ->
    partition_publishes(
      Publishes, fun ({Msg, _, _}) -> Msg end, MaxP).

partition_publish_delivered_batch(Publishes, MaxP) ->
    partition_publishes(
      Publishes, fun ({Msg, _}) -> Msg end, MaxP).

partition_publishes(Publishes, ExtractMsg, MaxP) ->
    Partitioned =
        lists:foldl(fun (Pub, Dict) ->
                            Msg = ExtractMsg(Pub),
                            rabbit_misc:maps_cons(priority(Msg, MaxP), Pub, Dict)
                    end, maps:new(), Publishes),
    maps:map(fun (_P, RevPubs) ->
                 lists:reverse(RevPubs)
             end, Partitioned).


priority_bq(Priority, [{MaxP, _} | _] = BQSs) ->
    bq_fetch(priority(Priority, MaxP), BQSs).

%% Messages with a priority which is higher than the queue's maximum are treated
%% as if they were published with the maximum priority.
priority(undefined, _MaxP) ->
    0;
priority(Priority, MaxP) when is_integer(Priority), Priority =< MaxP ->
    Priority;
priority(Priority, MaxP) when is_integer(Priority), Priority > MaxP ->
    MaxP;
priority(#basic_message{content = Content}, MaxP) ->
    priority(rabbit_binary_parser:ensure_content_decoded(Content), MaxP);
priority(#content{properties = Props}, MaxP) ->
    #'P_basic'{priority = Priority0} = Props,
    priority(Priority0, MaxP).

add_maybe_infinity(infinity, _) -> infinity;
add_maybe_infinity(_, infinity) -> infinity;
add_maybe_infinity(A, B)        -> A + B.

partition_acktags(AckTags) -> partition_acktags(AckTags, maps:new()).

partition_acktags([], Partitioned) ->
    maps:map(fun (_P, RevAckTags) ->
                 lists:reverse(RevAckTags)
             end, Partitioned);
partition_acktags([{P, AckTag} | Rest], Partitioned) ->
    partition_acktags(Rest, rabbit_misc:maps_cons(P, AckTag, Partitioned)).

priority_on_acktags(P, AckTags) ->
    [case Tag of
         _ when is_integer(Tag) -> {P, Tag};
         _                      -> Tag
     end || Tag <- AckTags].

combine_status(P, New, nothing) ->
    [{priority_lengths, [{P, proplists:get_value(len, New)}]} | New];
combine_status(P, New, Old) ->
    Combined = [{K, cse(V, proplists:get_value(K, Old))} || {K, V} <- New],
    Lens = [{P, proplists:get_value(len, New)} |
            proplists:get_value(priority_lengths, Old)],
    [{priority_lengths, Lens} | Combined].

cse(infinity, _)            -> infinity;
cse(_, infinity)            -> infinity;
%% queue modes
cse(_, default)             -> default;
cse(default, _)             -> default;
cse(_, lazy)                -> lazy;
cse(lazy, _)                -> lazy;
%% numerical stats
cse(A, B) when is_number(A) -> A + B;
cse({delta, _, _, _, _}, _) -> {delta, todo, todo, todo, todo};
cse(_, _)                   -> undefined.

%% When asked about 'head_message_timestamp' fro this priority queue, we
%% walk all the backing queues, starting by the highest priority. Once a
%% backing queue having messages (ready or unacknowledged) is found, its
%% 'head_message_timestamp' is returned even if it is null.

find_head_message_timestamp(BQ, [{_, BQSN} | Rest], Timestamp) ->
    MsgCount = BQ:len(BQSN) + BQ:info(messages_unacknowledged_ram, BQSN),
    if
        MsgCount =/= 0 -> BQ:info(head_message_timestamp, BQSN);
        true           -> find_head_message_timestamp(BQ, Rest, Timestamp)
    end;
find_head_message_timestamp(_, [], Timestamp) ->
    Timestamp.

zip_msgs_and_acks(Pubs, AckTags) ->
    lists:zipwith(
      fun ({#basic_message{ id = Id }, _Props}, AckTag) ->
                  {Id, AckTag}
      end, Pubs, AckTags).
