-module(rabbit_fifo).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).

-include_lib("ra/include/ra.hrl").

-export([
         init/1,
         apply/4,
         state_enter/2,
         tick/2,
         overview/1,
         get_checked_out/4,
         %% aux
         init_aux/1,
         handle_aux/6,
         % queries
         query_messages_ready/1,
         query_messages_checked_out/1,
         query_processes/1,
         query_ra_indexes/1,
         query_consumer_count/1,
         usage/1,

         %% misc
         dehydrate_state/1
        ]).

-ifdef(TEST).
-export([
         metrics_handler/1
        ]).
-endif.

-type raw_msg() :: term().
%% The raw message. It is opaque to rabbit_fifo.

-type msg_in_id() :: non_neg_integer().
% a queue scoped monotonically incrementing integer used to enforce order
% in the unassigned messages map

-type msg_id() :: non_neg_integer().
%% A consumer-scoped monotonically incrementing integer included with a
%% {@link delivery/0.}. Used to settle deliveries using
%% {@link rabbit_fifo_client:settle/3.}

-type msg_seqno() :: non_neg_integer().
%% A sender process scoped monotonically incrementing integer included
%% in enqueue messages. Used to ensure ordering of messages send from the
%% same process

-type msg_header() :: #{delivery_count => non_neg_integer()}.
%% The message header map:
%% delivery_count: the number of unsuccessful delivery attempts.
%%                 A non-zero value indicates a previous attempt.

-type msg() :: {msg_header(), raw_msg()}.
%% message with a header map.

-type indexed_msg() :: {ra_index(), msg()}.

-type delivery_msg() :: {msg_id(), msg()}.
%% A tuple consisting of the message id and the headered message.

-type consumer_tag() :: binary().
%% An arbitrary binary tag used to distinguish between different consumers
%% set up by the same process. See: {@link rabbit_fifo_client:checkout/3.}

-type delivery() :: {delivery, consumer_tag(), [delivery_msg()]}.
%% Represents the delivery of one or more rabbit_fifo messages.

-type consumer_id() :: {consumer_tag(), pid()}.
%% The entity that receives messages. Uniquely identifies a consumer.

-type credit_mode() :: simple_prefetch | credited.
%% determines how credit is replenished

-type checkout_spec() :: {once | auto, Num :: non_neg_integer(),
                          credit_mode()} |
                         {dequeue, settled | unsettled} |
                         cancel.

-type protocol() ::
    {enqueue, Sender :: maybe(pid()), MsgSeq :: maybe(msg_seqno()),
     Msg :: raw_msg()} |
    {checkout, Spec :: checkout_spec(), Consumer :: consumer_id()} |
    {settle, MsgIds :: [msg_id()], Consumer :: consumer_id()} |
    {return, MsgIds :: [msg_id()], Consumer :: consumer_id()} |
    {discard, MsgIds :: [msg_id()], Consumer :: consumer_id()} |
    {credit,
     Credit :: non_neg_integer(),
     DeliveryCount :: non_neg_integer(),
     Drain :: boolean(),
     Consumer :: consumer_id()} |
    purge.

-type command() :: protocol() | ra_machine:builtin_command().
%% all the command types suppored by ra fifo

-type client_msg() :: delivery().
%% the messages `rabbit_fifo' can send to consumers.

-type applied_mfa() :: {module(), atom(), list()}.
% represents a partially applied module call

-define(SHADOW_COPY_INTERVAL, 4096 * 4).
-define(USE_AVG_HALF_LIFE, 10000.0).

-record(consumer,
        {checked_out = #{} :: #{msg_id() => {msg_in_id(), indexed_msg()}},
         next_msg_id = 0 :: msg_id(), % part of snapshot data
         %% max number of messages that can be sent
         %% decremented for each delivery
         credit = 0 : non_neg_integer(),
         %% total number of checked out messages - ever
         %% incremented for each delivery
         delivery_count = 0 :: non_neg_integer(),
         %% the mode of how credit is incremented
         %% simple_prefetch: credit is re-filled as deliveries are settled
         %% or returned.
         %% credited: credit can only be changed by receiving a consumer_credit
         %% command: `{consumer_credit, ReceiverDeliveryCount, Credit}'
         credit_mode = simple_prefetch :: credit_mode(), % part of snapshot data
         lifetime = once :: once | auto,
         suspected_down = false :: boolean()
        }).

-record(enqueuer,
        {next_seqno = 1 :: msg_seqno(),
         % out of order enqueues - sorted list
         pending = [] :: [{msg_seqno(), ra_index(), raw_msg()}],
         suspected_down = false :: boolean()
        }).

-record(state,
        {name :: atom(),
         shadow_copy_interval = ?SHADOW_COPY_INTERVAL :: non_neg_integer(),
         % unassigned messages
         messages = #{} :: #{msg_in_id() => indexed_msg()},
         % defines the lowest message in id available in the messages map
         % that isn't a return
         low_msg_num :: msg_in_id() | undefined,
         % defines the next message in id to be added to the messages map
         next_msg_num = 1 :: msg_in_id(),
         % list of returned msg_in_ids - when checking out it picks from
         % this list first before taking low_msg_num
         returns = queue:new() :: queue:queue(msg_in_id()),
         % a counter of enqueues - used to trigger shadow copy points
         enqueue_count = 0 :: non_neg_integer(),
         % a map containing all the live processes that have ever enqueued
         % a message to this queue as well as a cached value of the smallest
         % ra_index of all pending enqueues
         enqueuers = #{} :: #{pid() => #enqueuer{}},
         % master index of all enqueue raft indexes including pending
         % enqueues
         % rabbit_fifo_index can be slow when calculating the smallest
         % index when there are large gaps but should be faster than gb_trees
         % for normal appending operations as it's backed by a map
         ra_indexes = rabbit_fifo_index:empty() :: rabbit_fifo_index:state(),
         % consumers need to reflect consumer state at time of snapshot
         % needs to be part of snapshot
         consumers = #{} :: #{consumer_id() => #consumer{}},
         % consumers that require further service are queued here
         % needs to be part of snapshot
         service_queue = queue:new() :: queue:queue(consumer_id()),
         dead_letter_handler :: maybe(applied_mfa()),
         cancel_consumer_handler :: maybe(applied_mfa()),
         become_leader_handler :: maybe(applied_mfa()),
         metrics_handler :: maybe(applied_mfa()),
         %% This is a special field that is only used for snapshots
         %% It represents the number of queued messages at the time the
         %% dehydrated snapshot state was cached.
         %% As release_cursors are only emitted for raft indexes where all
         %% prior messages no longer contribute to the current state we can
         %% replace all message payloads at some index with a single integer
         %% to be decremented during `checkout_one' until it's 0 after which
         %% it instead takes messages from the `messages' map.
         %% This is done so that consumers are still served in a deterministic
         %% order on recovery.
         prefix_msg_count = 0 :: non_neg_integer()
        }).

-opaque state() :: #state{}.

-type config() :: #{name := atom(),
                    dead_letter_handler => applied_mfa(),
                    become_leader_handler => applied_mfa(),
                    cancel_consumer_handler => applied_mfa(),
                    metrics_handler => applied_mfa(),
                    shadow_copy_interval => non_neg_integer()}.

-export_type([protocol/0,
              delivery/0,
              command/0,
              consumer_tag/0,
              consumer_id/0,
              client_msg/0,
              msg/0,
              msg_id/0,
              msg_seqno/0,
              delivery_msg/0,
              state/0,
              config/0]).

-spec init(config()) -> {state(), ra_machine:effects()}.
init(#{name := Name} = Conf) ->
    update_state(Conf, #state{name = Name}).

update_state(Conf, State) ->
    DLH = maps:get(dead_letter_handler, Conf, undefined),
    CCH = maps:get(cancel_consumer_handler, Conf, undefined),
    BLH = maps:get(become_leader_handler, Conf, undefined),
    MH = maps:get(metrics_handler, Conf, undefined),
    SHI = maps:get(shadow_copy_interval, Conf, ?SHADOW_COPY_INTERVAL),
    State#state{dead_letter_handler = DLH,
                cancel_consumer_handler = CCH,
                become_leader_handler = BLH,
                metrics_handler = MH,
                shadow_copy_interval = SHI}.

% msg_ids are scoped per consumer
% ra_indexes holds all raft indexes for enqueues currently on queue
-spec apply(ra_machine:command_meta_data(), command(),
            ra_machine:effects(), state()) ->
    {state(), ra_machine:effects(), Reply :: term()}.
apply(#{index := RaftIdx}, {enqueue, From, Seq, RawMsg}, Effects0, State00) ->
    case maybe_enqueue(RaftIdx, From, Seq, RawMsg, Effects0, State00) of
        {ok, State0, Effects} ->
            State = append_to_master_index(RaftIdx, State0),
            checkout(State, Effects);
        {duplicate, State, Effects} ->
            {State, Effects, ok}
    end;
apply(#{index := RaftIdx}, {settle, MsgIds, ConsumerId}, Effects0,
      #state{consumers = Cons0} = State) ->
    case Cons0 of
        #{ConsumerId := Con0} ->
            % need to increment metrics before completing as any snapshot
            % states taken need to includ them
            complete_and_checkout(RaftIdx, MsgIds, ConsumerId,
                                  Con0, Effects0, State);
        _ ->
            {State, Effects0, ok}
    end;
apply(#{index := RaftIdx}, {discard, MsgIds, ConsumerId}, Effects0,
      #state{consumers = Cons0} = State0) ->
    case Cons0 of
        #{ConsumerId := Con0} ->
            {State, Effects, Res} = complete_and_checkout(RaftIdx, MsgIds,
                                                          ConsumerId, Con0,
                                                          Effects0, State0),
            Discarded = maps:with(MsgIds, Con0#consumer.checked_out),
            {State, dead_letter_effects(Discarded, State, Effects), Res};
        _ ->
            {State0, Effects0, ok}
    end;
apply(_, {return, MsgIds, ConsumerId}, Effects0,
      #state{consumers = Cons0} = State) ->
    case Cons0 of
        #{ConsumerId := Con0 = #consumer{checked_out = Checked0}} ->
            Checked = maps:without(MsgIds, Checked0),
            Returned = maps:with(MsgIds, Checked0),
            MsgNumMsgs = [M || M <- maps:values(Returned)],
            return(ConsumerId, MsgNumMsgs, Con0, Checked, Effects0, State);
        _ ->
            {State, Effects0, ok}
    end;
apply(_, {credit, NewCredit, RemoteDelCnt, Drain, ConsumerId}, Effects0,
      #state{consumers = Cons0,
             service_queue = ServiceQueue0} = State0) ->
    case Cons0 of
        #{ConsumerId := #consumer{delivery_count = DelCnt} = Con0} ->
            %% this can go below 0 when credit is reduced
            C = max(0, RemoteDelCnt + NewCredit - DelCnt),
            %% grant the credit
            Con1 = Con0#consumer{credit = C},
            ServiceQueue = maybe_queue_consumer(ConsumerId, Con1,
                                                ServiceQueue0),
            Cons = maps:put(ConsumerId, Con1, Cons0),
            {State1, Effects, ok} =
                checkout(State0#state{service_queue = ServiceQueue,
                                      consumers = Cons}, Effects0),
            Response = {send_credit_reply, maps:size(State1#state.messages)},
            %% by this point all checkouts for the updated credit value
            %% should be processed so we can evaluate the drain
            case Drain of
                false ->
                    %% just return the result of the checkout
                    {State1, Effects, Response};
                true ->
                    Con = #consumer{credit = PostCred} =
                        maps:get(ConsumerId, State1#state.consumers),
                    %% add the outstanding credit to the delivery count
                    DeliveryCount = Con#consumer.delivery_count + PostCred,
                    Consumers = maps:put(ConsumerId,
                                         Con#consumer{delivery_count = DeliveryCount,
                                                      credit = 0},
                                         State1#state.consumers),
                    Drained = Con#consumer.credit,
                    {CTag, _} = ConsumerId,
                    {State1#state{consumers = Consumers},
                     Effects,
                     %% returning a multi response with two client actions
                     %% for the channel to execute
                     {multi, [Response, {send_drained, [{CTag, Drained}]}]}}
            end;
        _ ->
            %% credit for unknown consumer - just ignore
            {State0, Effects0, ok}
    end;
apply(_, {checkout, {dequeue, _}, {_Tag, _Pid}}, Effects0,
      #state{messages = M,
             prefix_msg_count = 0} = State0) when map_size(M) == 0 ->
    %% TODO do we need metric visibility of empty get requests?
    {State0, Effects0, {dequeue, empty}};
apply(Meta, {checkout, {dequeue, settled}, ConsumerId},
      Effects0, State0) ->
    % TODO: this clause could probably be optimised
    State1 = update_consumer(ConsumerId, {once, 1, simple_prefetch}, State0),
    % turn send msg effect into reply
    {success, _, MsgId, Msg, State2} = checkout_one(State1),
    % immediately settle
    {State, Effects, _} = apply(Meta, {settle, [MsgId], ConsumerId},
                                Effects0, State2),
    {State, Effects, {dequeue, {MsgId, Msg}}};
apply(_, {checkout, {dequeue, unsettled}, {_Tag, Pid} = Consumer},
      Effects0, State0) ->
    State1 = update_consumer(Consumer, {once, 1, simple_prefetch}, State0),
    Effects1 = [{monitor, process, Pid} | Effects0],
    {State, Reply, Effects} = case checkout_one(State1) of
                                  {success, _, MsgId, Msg, S} ->
                                      {S, {MsgId, Msg}, Effects1};
                                  {inactive, S} ->
                                      {S, empty, [{aux, inactive} | Effects1]};
                                  S ->
                                      {S, empty, Effects1}
                              end,
    {State, Effects, {dequeue, Reply}};
apply(_, {checkout, cancel, ConsumerId}, Effects0, State0) ->
    {CancelEffects, State1} = cancel_consumer(ConsumerId, {Effects0, State0}),
    % TODO: here we should really demonitor the pid but _only_ if it has no
    % other consumers or enqueuers.
    checkout(State1, CancelEffects);
apply(_, {checkout, Spec, {_Tag, Pid} = ConsumerId}, Effects0, State0) ->
    State1 = update_consumer(ConsumerId, Spec, State0),
    {State, Effects, Res} = checkout(State1, Effects0),
    {State, [{monitor, process, Pid} | Effects], Res};
apply(#{index := RaftIdx}, purge, Effects0,
      #state{consumers = Cons0, ra_indexes = Indexes } = State0) ->
    Total = rabbit_fifo_index:size(Indexes),
    {State1, Effects1, _} =
        maps:fold(
          fun(ConsumerId, C = #consumer{checked_out = Checked0},
              {StateAcc0, EffectsAcc0, ok}) ->
                  MsgRaftIdxs = [RIdx || {_MsgInId, {RIdx, _}}
                                             <- maps:values(Checked0)],
                  complete(ConsumerId, MsgRaftIdxs, maps:size(Checked0), C,
                           #{}, EffectsAcc0, StateAcc0)
          end, {State0, Effects0, ok}, Cons0),
        {State, Effects, _} =
            update_smallest_raft_index(
              RaftIdx, Indexes,
              State1#state{ra_indexes = rabbit_fifo_index:empty(),
                           messages = #{},
                           returns = queue:new(),
                           low_msg_num = undefined}, Effects1),
    {State, [garbage_collection | Effects], {purge, Total}};
apply(_, {down, ConsumerPid, noconnection},
      Effects0, #state{consumers = Cons0,
                       enqueuers = Enqs0} = State0) ->
    Node = node(ConsumerPid),
    % mark all consumers and enqueuers as suspect
    % and monitor the node
    {Cons, State} = maps:fold(fun({_, P} = K, #consumer{checked_out = Checked0} = C,
                                  {Co, St0}) when node(P) =:= Node ->
                                      St = return_all(St0, Checked0),
                                      {maps:put(K, C#consumer{suspected_down = true,
                                                              checked_out = #{}},
                                                Co),
                                       St};
                                 (K, C, {Co, St}) ->
                                      {maps:put(K, C, Co), St}
                              end, {#{}, State0}, Cons0),
    Enqs = maps:map(fun(P, E) when node(P) =:= Node ->
                            E#enqueuer{suspected_down = true};
                       (_, E) -> E
                    end, Enqs0),
    Effects = case maps:size(Cons) of
                  0 ->
                      [{aux, inactive}, {monitor, node, Node} | Effects0];
                  _ ->
                      [{monitor, node, Node} | Effects0]
              end,
    {State#state{consumers = Cons, enqueuers = Enqs}, Effects, ok};
apply(_, {down, Pid, _Info}, Effects0,
      #state{consumers = Cons0,
             enqueuers = Enqs0} = State0) ->
    % Remove any enqueuer for the same pid and enqueue any pending messages
    % This should be ok as we won't see any more enqueues from this pid
    State1 = case maps:take(Pid, Enqs0) of
                 {#enqueuer{pending = Pend}, Enqs} ->
                     lists:foldl(fun ({_, RIdx, RawMsg}, S) ->
                                         enqueue(RIdx, RawMsg, S)
                                 end, State0#state{enqueuers = Enqs}, Pend);
                 error ->
                     State0
             end,
    % return checked out messages to main queue
    % Find the consumers for the down pid
    DownConsumers = maps:keys(
                      maps:filter(fun({_, P}, _) -> P =:= Pid end, Cons0)),
    {Effects1, State2} = lists:foldl(fun cancel_consumer/2, {Effects0, State1},
                                     DownConsumers),
    checkout(State2, Effects1);
apply(_, {nodeup, Node}, Effects0,
      #state{consumers = Cons0,
             enqueuers = Enqs0,
             service_queue = SQ0} = State0) ->
    %% A node we are monitoring has come back.
    %% If we have suspected any processes of being
    %% down we should now re-issue the monitors for them to detect if they're
    %% actually down or not
    Cons = maps:fold(fun({_, P}, #consumer{suspected_down = true}, Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) -> Acc
                     end, [], Cons0),
    Enqs = maps:fold(fun(P, #enqueuer{suspected_down = true}, Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) -> Acc
                     end, [], Enqs0),
    Monitors = [{monitor, process, P} || P <- Cons ++ Enqs],
    Enqs1 = maps:map(fun(P, E) when node(P) =:= Node ->
                             E#enqueuer{suspected_down = false};
                        (_, E) -> E
                     end, Enqs0),
    {Cons1, SQ, Effects} =
        maps:fold(fun({_, P} = ConsumerId, C, {CAcc, SQAcc, EAcc})
                        when node(P) =:= Node ->
                          update_or_remove_sub(
                            ConsumerId, C#consumer{suspected_down = false},
                            CAcc, SQAcc, EAcc);
                     (_, _, Acc) ->
                          Acc
                  end, {Cons0, SQ0, Effects0}, Cons0),
    % TODO: avoid list concat
    checkout(State0#state{consumers = Cons1, enqueuers = Enqs1,
                          service_queue = SQ}, Monitors ++ Effects);
apply(_, {nodedown, _Node}, Effects, State) ->
    {State, Effects, ok};
apply(_, {update_state, Conf}, Effects, State) ->
    {update_state(Conf, State), Effects, ok}.

-spec state_enter(ra_server:ra_state(), state()) -> ra_machine:effects().
state_enter(leader, #state{consumers = Custs,
                           enqueuers = Enqs,
                           name = Name,
                           become_leader_handler = BLH}) ->
    % return effects to monitor all current consumers and enqueuers
    ConMons = [{monitor, process, P} || {_, P} <- maps:keys(Custs)],
    EnqMons = [{monitor, process, P} || P <- maps:keys(Enqs)],
    Effects = ConMons ++ EnqMons,
    case BLH of
        undefined ->
            Effects;
        {Mod, Fun, Args} ->
            [{mod_call, Mod, Fun, Args ++ [Name]} | Effects]
    end;
state_enter(eol, #state{enqueuers = Enqs, consumers = Custs0}) ->
    Custs = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Custs0),
    [{send_msg, P, eol, ra_event} || P <- maps:keys(maps:merge(Enqs, Custs))];
state_enter(_, _) ->
    %% catch all as not handling all states
    [].


-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, #state{name = Name,
                 messages = Messages,
                 ra_indexes = Indexes,
                 metrics_handler = MH,
                 consumers = Cons} = State) ->
    Metrics = {Name,
               maps:size(Messages), % Ready
               num_checked_out(State), % checked out
               rabbit_fifo_index:size(Indexes), %% Total
               maps:size(Cons)}, % Consumers
    case MH of
        undefined ->
            [{aux, emit}];
        {Mod, Fun, Args} ->
            [{mod_call, Mod, Fun, Args ++ [Metrics]}, {aux, emit}]
    end.

-spec overview(state()) -> map().
overview(#state{consumers = Cons,
                enqueuers = Enqs,
                messages = Messages,
                ra_indexes = Indexes} = State) ->
    #{type => ?MODULE,
      num_consumers => maps:size(Cons),
      num_checked_out => num_checked_out(State),
      num_enqueuers => maps:size(Enqs),
      num_ready_messages => maps:size(Messages),
      num_messages => rabbit_fifo_index:size(Indexes)}.

-spec get_checked_out(consumer_id(), msg_id(), msg_id(), state()) ->
    [delivery_msg()].
get_checked_out(Cid, From, To, #state{consumers = Consumers}) ->
    case Consumers of
        #{Cid := #consumer{checked_out = Checked}} ->
            [{K, snd(snd(maps:get(K, Checked)))} || K <- lists:seq(From, To)];
        _ ->
            []
    end.

init_aux(Name) when is_atom(Name) ->
    %% TODO: catch specific exeption throw if table already exists
    ok = ra_machine_ets:create_table(rabbit_fifo_usage,
                                     [named_table, set, public,
                                      {write_concurrency, true}]),
    Now = erlang:monotonic_time(micro_seconds),
    {Name, {inactive, Now, 1, 1.0}}.

handle_aux(_, cast, Cmd, {Name, Use0}, Log, _) ->
    Use = case Cmd of
              _ when Cmd == active orelse Cmd == inactive ->
                  update_use(Use0, Cmd);
              emit ->
                  true = ets:insert(rabbit_fifo_usage,
                                    {Name, utilisation(Use0)}),
                  Use0
          end,
    {no_reply, {Name, Use}, Log}.

%%% Queries

query_messages_ready(#state{messages = M}) ->
    M.

query_messages_checked_out(#state{consumers = Consumers}) ->
    maps:fold(fun (_, #consumer{checked_out = C}, S) ->
                      maps:merge(S, maps:from_list(maps:values(C)))
              end, #{}, Consumers).

query_processes(#state{enqueuers = Enqs, consumers = Cons0}) ->
    Cons = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Cons0),
    maps:keys(maps:merge(Enqs, Cons)).


query_ra_indexes(#state{ra_indexes = RaIndexes}) ->
    RaIndexes.

query_consumer_count(#state{consumers = Consumers}) ->
    maps:size(Consumers).

%% other

-spec usage(atom()) -> float().
usage(Name) when is_atom(Name) ->
    case ets:lookup(rabbit_fifo_usage, Name) of
        [] -> 0.0;
        [{_, Use}] -> Use
    end.

%%% Internal

update_use({inactive, _, _, _} = CUInfo, inactive) ->
    CUInfo;
update_use({active, _, _} = CUInfo, active) ->
    CUInfo;
update_use({active, Since, Avg}, inactive) ->
    Now = erlang:monotonic_time(micro_seconds),
    {inactive, Now, Now - Since, Avg};
update_use({inactive, Since, Active, Avg},   active) ->
    Now = erlang:monotonic_time(micro_seconds),
    {active, Now, use_avg(Active, Now - Since, Avg)}.

utilisation({active, Since, Avg}) ->
    use_avg(erlang:monotonic_time(micro_seconds) - Since, 0, Avg);
utilisation({inactive, Since, Active, Avg}) ->
    use_avg(Active, erlang:monotonic_time(micro_seconds) - Since, Avg).

use_avg(0, 0, Avg) ->
    Avg;
use_avg(Active, Inactive, Avg) ->
    Time = Inactive + Active,
    moving_average(Time, ?USE_AVG_HALF_LIFE, Active / Time, Avg).

moving_average(_Time, _, Next, undefined) ->
    Next;
moving_average(Time, HalfLife, Next, Current) ->
    Weight = math:exp(Time * math:log(0.5) / HalfLife),
    Next * (1 - Weight) + Current * Weight.

num_checked_out(#state{consumers = Cons}) ->
    lists:foldl(fun (#consumer{checked_out = C}, Acc) ->
                        maps:size(C) + Acc
                end, 0, maps:values(Cons)).

cancel_consumer(ConsumerId,
                {Effects0, #state{consumers = C0, name = Name} = S0}) ->
    case maps:take(ConsumerId, C0) of
        {#consumer{checked_out = Checked0}, Cons} ->
            S = return_all(S0, Checked0),
            Effects = cancel_consumer_effects(ConsumerId, Name, S, Effects0),
            case maps:size(Cons) of
                0 ->
                    {[{aux, inactive} | Effects], S#state{consumers = Cons}};
                _ ->
                    {Effects, S#state{consumers = Cons}}
                end;
        error ->
            % already removed - do nothing
            {Effects0, S0}
    end.

incr_enqueue_count(#state{enqueue_count = C,
                          shadow_copy_interval = C} = State0) ->
    % time to stash a dehydrated state version
    State = State0#state{enqueue_count = 0},
    {State, dehydrate_state(State)};
incr_enqueue_count(#state{enqueue_count = C} = State) ->
    {State#state{enqueue_count = C + 1}, undefined}.

enqueue(RaftIdx, RawMsg, #state{messages = Messages,
                                low_msg_num = LowMsgNum,
                                next_msg_num = NextMsgNum} = State0) ->
    Msg = {RaftIdx, {#{}, RawMsg}}, % indexed message with header map
    State0#state{messages = Messages#{NextMsgNum => Msg},
                 % this is probably only done to record it when low_msg_num
                 % is undefined
                 low_msg_num = min(LowMsgNum, NextMsgNum),
                 next_msg_num = NextMsgNum + 1}.

append_to_master_index(RaftIdx,
                       #state{ra_indexes = Indexes0} = State0) ->
    {State, Shadow} = incr_enqueue_count(State0),
    Indexes = rabbit_fifo_index:append(RaftIdx, Shadow, Indexes0),
    State#state{ra_indexes = Indexes}.

enqueue_pending(From,
                #enqueuer{next_seqno = Next,
                          pending = [{Next, RaftIdx, RawMsg} | Pending]} = Enq0,
                State0) ->
            State = enqueue(RaftIdx, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = Next + 1, pending = Pending},
            enqueue_pending(From, Enq, State);
enqueue_pending(From, Enq, #state{enqueuers = Enqueuers0} = State) ->
    State#state{enqueuers = Enqueuers0#{From => Enq}}.

maybe_enqueue(RaftIdx, undefined, undefined, RawMsg, Effects,
              State0) ->
    % direct enqueue without tracking
    {ok, enqueue(RaftIdx, RawMsg, State0), Effects};
maybe_enqueue(RaftIdx, From, MsgSeqNo, RawMsg, Effects0,
              #state{enqueuers = Enqueuers0} = State0) ->
    case maps:get(From, Enqueuers0, undefined) of
        undefined ->
            State1 = State0#state{enqueuers = Enqueuers0#{From => #enqueuer{}}},
            {ok, State, Effects} = maybe_enqueue(RaftIdx, From, MsgSeqNo,
                                                 RawMsg, Effects0, State1),
            {ok, State, [{monitor, process, From} | Effects]};
        #enqueuer{next_seqno = MsgSeqNo} = Enq0 ->
            % it is the next expected seqno
            State1 = enqueue(RaftIdx, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = MsgSeqNo + 1},
            State = enqueue_pending(From, Enq, State1),
            {ok, State, Effects0};
        #enqueuer{next_seqno = Next,
                  pending = Pending0} = Enq0
          when MsgSeqNo > Next ->
            % out of order delivery
            Pending = [{MsgSeqNo, RaftIdx, RawMsg} | Pending0],
            Enq = Enq0#enqueuer{pending = lists:sort(Pending)},
            {ok, State0#state{enqueuers = Enqueuers0#{From => Enq}}, Effects0};
        #enqueuer{next_seqno = Next} when MsgSeqNo =< Next ->
            % duplicate delivery - remove the raft index from the ra_indexes
            % map as it was added earlier
            {duplicate, State0, Effects0}
    end.

snd(T) ->
    element(2, T).

return(ConsumerId, MsgNumMsgs, #consumer{lifetime = Life} = Con0, Checked,
       Effects0, #state{consumers = Cons0, service_queue = SQ0} = State0) ->
    Con = case Life of
              auto ->
                  Num = length(MsgNumMsgs),
                  Con0#consumer{checked_out = Checked,
                                credit = increase_credit(Con0, Num)};
              once ->
                  Con0#consumer{checked_out = Checked}
          end,
    {Cons, SQ, Effects} = update_or_remove_sub(ConsumerId, Con, Cons0,
                                               SQ0, Effects0),
    State1 = lists:foldl(fun('$prefix_msg',
                             #state{prefix_msg_count = MsgCount} = S0) ->
                                 S0#state{prefix_msg_count = MsgCount + 1};
                            ({MsgNum, Msg}, S0) ->
                                 return_one(MsgNum, Msg, S0)
                         end, State0, MsgNumMsgs),
    checkout(State1#state{consumers = Cons,
                          service_queue = SQ},
             Effects).

% used to processes messages that are finished
complete(ConsumerId, MsgRaftIdxs, NumDiscarded,
         Con0, Checked, Effects0,
         #state{consumers = Cons0, service_queue = SQ0,
                ra_indexes = Indexes0} = State0) ->
    %% credit_mode = simple_prefetch should automatically top-up credit
    %% as messages are simple_prefetch or otherwise returned
    Con = Con0#consumer{checked_out = Checked,
                        credit = increase_credit(Con0, NumDiscarded)},
    {Cons, SQ, Effects} = update_or_remove_sub(ConsumerId, Con, Cons0,
                                               SQ0, Effects0),
    Indexes = lists:foldl(fun rabbit_fifo_index:delete/2, Indexes0, MsgRaftIdxs),
    {State0#state{consumers = Cons,
                  ra_indexes = Indexes,
                  service_queue = SQ}, Effects, ok}.

increase_credit(#consumer{lifetime = once,
                          credit = Credit}, _) ->
    %% once consumers cannot increment credit
    Credit;
increase_credit(#consumer{lifetime = auto,
                          credit_mode = credited,
                          credit = Credit}, _) ->
    %% credit_mode: credit also doens't automatically increment credit
    Credit;
increase_credit(#consumer{credit = Current}, Credit) ->
    Current + Credit.

complete_and_checkout(IncomingRaftIdx, MsgIds, ConsumerId,
                      #consumer{checked_out = Checked0} = Con0,
                      Effects0, #state{ra_indexes = Indexes0} = State0) ->
    Checked = maps:without(MsgIds, Checked0),
    Discarded = maps:with(MsgIds, Checked0),
    MsgRaftIdxs = [RIdx || {_, {RIdx, _}} <- maps:values(Discarded)],
    %% need to pass the length of discarded as $prefix_msgs would be filtered
    %% by the above list comprehension
    {State1, Effects1, _} = complete(ConsumerId, MsgRaftIdxs,
                                     maps:size(Discarded),
                                     Con0, Checked, Effects0, State0),
    {State, Effects, _} = checkout(State1, Effects1),
    % settle metrics are incremented separately
    update_smallest_raft_index(IncomingRaftIdx, Indexes0, State, Effects).

dead_letter_effects(_Discarded,
                    #state{dead_letter_handler = undefined},
                    Effects) ->
    Effects;
dead_letter_effects(Discarded,
                    #state{dead_letter_handler = {Mod, Fun, Args}}, Effects) ->
    DeadLetters = maps:fold(fun(_, {_, {_, {_, Msg}}},
                                % MsgId, MsgIdID, RaftId, Header
                                Acc) -> [{rejected, Msg} | Acc]
                            end, [], Discarded),
    [{mod_call, Mod, Fun, Args ++ [DeadLetters]} | Effects].

cancel_consumer_effects(_, _, #state{cancel_consumer_handler = undefined},
                        Effects) ->
    Effects;
cancel_consumer_effects(Pid, Name,
                        #state{cancel_consumer_handler = {Mod, Fun, Args}},
                        Effects) ->
    [{mod_call, Mod, Fun, Args ++ [Pid, Name]} | Effects].

update_smallest_raft_index(IncomingRaftIdx, OldIndexes,
                           #state{ra_indexes = Indexes,
                                  messages = Messages} = State, Effects) ->
    case rabbit_fifo_index:size(Indexes) of
        0 when map_size(Messages) =:= 0 ->
            % there are no messages on queue anymore and no pending enqueues
            % we can forward release_cursor all the way until
            % the last received command
            {State, [{release_cursor, IncomingRaftIdx, State} | Effects], ok};
        _ ->
            NewSmallest = rabbit_fifo_index:smallest(Indexes),
            % Take the smallest raft index available in the index when starting
            % to process this command
            case {NewSmallest, rabbit_fifo_index:smallest(OldIndexes)} of
                {{Smallest, _}, {Smallest, _}} ->
                    % smallest has not changed, do not issue release cursor
                    % effects
                    {State, Effects, ok};
                {_, {Smallest, Shadow}} when Shadow =/= undefined ->
                    % ?INFO("RELEASE ~w ~w ~w~n", [IncomingRaftIdx, Smallest,
                    %                              Shadow]),
                    {State, [{release_cursor, Smallest, Shadow} | Effects], ok};
                 _ -> % smallest
                    % no shadow taken for this index,
                    % no release cursor increase
                    {State, Effects, ok}
            end
    end.

% TODO update message then update messages and returns in single operations
return_one(MsgNum, {RaftId, {Header0, RawMsg}},
           #state{messages = Messages,
                  returns = Returns} = State0) ->
    Header = maps:update_with(delivery_count,
                              fun (C) -> C+1 end,
                              1, Header0),
    Msg = {RaftId, {Header, RawMsg}},
    % this should not affect the release cursor in any way
    State0#state{messages = maps:put(MsgNum, Msg, Messages),
                 returns = queue:in(MsgNum, Returns)}.

return_all(State, Checked) ->
    maps:fold(fun (_, '$prefix_msg',
                   #state{prefix_msg_count = MsgCount} = S) ->
                      S#state{prefix_msg_count = MsgCount + 1};
                  (_, {MsgNum, Msg}, S) ->
                      return_one(MsgNum, Msg, S)
              end, State, Checked).

checkout(State, Effects) ->
    checkout0(checkout_one(State), Effects, #{}).

checkout0({success, ConsumerId, MsgId, Msg, State}, Effects, Acc0) ->
    DelMsg = {MsgId, Msg},
    Acc = maps:update_with(ConsumerId,
                           fun (M) -> [DelMsg | M] end,
                           [DelMsg], Acc0),
    checkout0(checkout_one(State), Effects, Acc);
checkout0({inactive, State}, Effects0, Acc) ->
    Effects = append_send_msg_effects(Effects0, Acc),
    {State, [{aux, inactive} | Effects], ok};
checkout0(State, Effects0, Acc) ->
    Effects = append_send_msg_effects(Effects0, Acc),
    {State, Effects, ok}.

append_send_msg_effects(Effects, AccMap) when map_size(AccMap) == 0 ->
    Effects;
append_send_msg_effects(Effects0, AccMap) ->
    Effects = maps:fold(fun (C, Msgs, Ef) ->
                                [send_msg_effect(C, lists:reverse(Msgs)) | Ef]
                        end, Effects0, AccMap),
    [{aux, active} | Effects].

next_checkout_message(#state{returns = Returns,
                             low_msg_num = Low0,
                             next_msg_num = NextMsgNum} = State) ->
    %% use peek rather than out there as the most likely case is an empty
    %% queue
    case queue:peek(Returns) of
        empty ->
            case Low0 of
                undefined ->
                    {undefined, State};
                _ ->
                    case Low0 + 1 of
                        NextMsgNum ->
                            %% the map will be empty after this item is removed
                            {Low0, State#state{low_msg_num = undefined}};
                        Low ->
                            {Low0, State#state{low_msg_num = Low}}
                    end
            end;
        {value, Next} ->
            {Next, State#state{returns = queue:drop(Returns)}}
    end.

take_next_msg(#state{prefix_msg_count = 0,
                     messages = Messages0} = State0) ->
    {NextMsgInId, State} = next_checkout_message(State0),
    %% messages are available
    case maps:take(NextMsgInId, Messages0) of
        {IdxMsg, Messages} ->
            {{NextMsgInId, IdxMsg}, State, Messages, 0};
        error ->
            error
    end;
take_next_msg(#state{prefix_msg_count = MsgCount,
                     messages = Messages} = State) ->
    %% there is still a prefix message count for the consumer
    %% "fake" a '$prefix_msg' message
    {'$prefix_msg', State, Messages, MsgCount - 1}.

send_msg_effect({CTag, CPid}, Msgs) ->
    {send_msg, CPid, {delivery, CTag, Msgs}, ra_event}.

checkout_one(#state{service_queue = SQ0,
                    messages = Messages0,
                    consumers = Cons0} = InitState) ->
    case queue:peek(SQ0) of
        {value, ConsumerId} ->
            case take_next_msg(InitState) of
                {ConsumerMsg, State0, Messages, PrefMsgC} ->
                    SQ1 = queue:drop(SQ0),
                    %% there are consumers waiting to be serviced
                    %% process consumer checkout
                    case maps:find(ConsumerId, Cons0) of
                        {ok, #consumer{credit = 0}} ->
                            %% no credit but was still on queue
                            %% can happen when draining
                            %% recurse without consumer on queue
                            checkout_one(InitState#state{service_queue = SQ1});
                        {ok, #consumer{suspected_down = true}} ->
                            checkout_one(InitState#state{service_queue = SQ1});
                        {ok, #consumer{checked_out = Checked0,
                                       next_msg_id = Next,
                                       credit = Credit,
                                       delivery_count = DelCnt} = Con0} ->
                            Checked = maps:put(Next, ConsumerMsg, Checked0),
                            Con = Con0#consumer{checked_out = Checked,
                                                next_msg_id = Next + 1,
                                                credit = Credit - 1,
                                                delivery_count = DelCnt + 1},
                            {Cons, SQ, []} = % we expect no effects
                                update_or_remove_sub(ConsumerId, Con,
                                                     Cons0, SQ1, []),
                            State = State0#state{service_queue = SQ,
                                                 messages = Messages,
                                                 prefix_msg_count = PrefMsgC,
                                                 consumers = Cons},
                            Msg = case ConsumerMsg of
                                      '$prefix_msg' -> '$prefix_msg';
                                      {_, {_, M}} -> M
                                  end,
                            {success, ConsumerId, Next, Msg, State};
                        error ->
                            %% consumer did not exist but was queued, recurse
                            checkout_one(InitState#state{service_queue = SQ1})
                    end;
                error ->
                    InitState
            end;
        empty ->
            case maps:size(Messages0) of
                0 -> InitState;
                _ -> {inactive, InitState}
            end
    end.


update_or_remove_sub(ConsumerId, #consumer{lifetime = auto,
                                           credit = 0} = Con,
                     Cons, ServiceQueue, Effects) ->
    {maps:put(ConsumerId, Con, Cons), ServiceQueue, Effects};
update_or_remove_sub(ConsumerId, #consumer{lifetime = auto} = Con,
                     Cons, ServiceQueue, Effects) ->
    {maps:put(ConsumerId, Con, Cons),
     uniq_queue_in(ConsumerId, ServiceQueue), Effects};
update_or_remove_sub(ConsumerId, #consumer{lifetime = once,
                                           checked_out = Checked,
                                           credit = 0} = Con,
                     Cons, ServiceQueue, Effects) ->
    case maps:size(Checked)  of
        0 ->
            % we're done with this consumer
            {maps:remove(ConsumerId, Cons), ServiceQueue,
             [{demonitor, process, ConsumerId} | Effects]};
        _ ->
            % there are unsettled items so need to keep around
            {maps:put(ConsumerId, Con, Cons), ServiceQueue, Effects}
    end;
update_or_remove_sub(ConsumerId, #consumer{lifetime = once} = Con,
                     Cons, ServiceQueue, Effects) ->
    {maps:put(ConsumerId, Con, Cons),
     uniq_queue_in(ConsumerId, ServiceQueue), Effects}.

uniq_queue_in(Key, Queue) ->
    % TODO: queue:member could surely be quite expensive, however the practical
    % number of unique consumers may not be large enough for it to matter
    case queue:member(Key, Queue) of
        true ->
            Queue;
        false ->
            queue:in(Key, Queue)
    end.


update_consumer(ConsumerId, {Life, Credit, Mode},
                #state{consumers = Cons0,
                       service_queue = ServiceQueue0} = State0) ->
    %% TODO: this logic may not be correct for updating a pre-existing consumer
    Init = #consumer{lifetime = Life, credit = Credit, credit_mode = Mode},
    Cons = maps:update_with(ConsumerId,
                             fun(S) ->
                                     %% remove any in-flight messages from
                                     %% the credit update
                                     N = maps:size(S#consumer.checked_out),
                                     C = max(0, Credit - N),
                                     S#consumer{lifetime = Life,
                                                credit = C}
                             end, Init, Cons0),
    ServiceQueue = maybe_queue_consumer(ConsumerId, maps:get(ConsumerId, Cons),
                                        ServiceQueue0),

    State0#state{consumers = Cons, service_queue = ServiceQueue}.

maybe_queue_consumer(ConsumerId, #consumer{credit = Credit},
                     ServiceQueue0) ->
    case Credit > 0 of
        true ->
            % consumerect needs service - check if already on service queue
            uniq_queue_in(ConsumerId, ServiceQueue0);
        false ->
            ServiceQueue0
    end.


%% creates a dehydrated version of the current state to be cached and
%% potentially used to for a snaphot at a later point
dehydrate_state(#state{messages = Messages0,
                       consumers = Consumers,
                       prefix_msg_count = MsgCount} = State) ->
    State#state{messages = #{},
                ra_indexes = rabbit_fifo_index:empty(),
                low_msg_num = undefined,
                consumers = maps:map(fun (_, C) ->
                                             C#consumer{checked_out = #{}}
                                     end, Consumers),
                returns = queue:new(),
                prefix_msg_count = maps:size(Messages0) + MsgCount}.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(ASSERT_EFF(EfxPat, Effects),
        ?ASSERT_EFF(EfxPat, true, Effects)).

-define(ASSERT_EFF(EfxPat, Guard, Effects),
    ?assert(lists:any(fun (EfxPat) when Guard -> true;
                          (_) -> false
                      end, Effects))).

-define(ASSERT_NO_EFF(EfxPat, Effects),
    ?assert(not lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

-define(assertNoEffect(EfxPat, Effects),
    ?assert(not lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

test_init(Name) ->
    init(#{name => Name,
           shadow_copy_interval => 0,
           metrics_handler => {?MODULE, metrics_handler, []}}).

metrics_handler(_) ->
    ok.

enq_enq_checkout_test() ->
    Cid = {<<"enq_enq_checkout_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {_State3, Effects, _} =
        apply(meta(3), {checkout, {once, 2, simple_prefetch}, Cid}, [], State2),
    ?ASSERT_EFF({monitor, _, _}, Effects),
    ?ASSERT_EFF({send_msg, _, {delivery, _, _}, _}, Effects),
    ok.

credit_enq_enq_checkout_settled_credit_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {State3, Effects, _} =
        apply(meta(3), {checkout, {auto, 1, credited}, Cid}, [], State2),
    ?ASSERT_EFF({monitor, _, _}, Effects),
    Deliveries = lists:filter(fun ({send_msg, _, {delivery, _, _}, _}) -> true;
                                  (_) -> false
                              end, Effects),
    ?assertEqual(1, length(Deliveries)),
    %% settle the delivery this should _not_ result in further messages being
    %% delivered
    {State4, SettledEffects} = settle(Cid, 4, 1, State3),
    ?assertEqual(false, lists:any(fun ({send_msg, _, {delivery, _, _}, _}) ->
                                          true;
                                      (_) -> false
                                  end, SettledEffects)),
    %% granting credit (3) should deliver the second msg if the receivers
    %% delivery count is (1)
    {State5, CreditEffects} = credit(Cid, 5, 1, 1, false, State4),
    % ?debugFmt("CreditEffects  ~p ~n~p", [CreditEffects, State4]),
    ?ASSERT_EFF({send_msg, _, {delivery, _, _}, _}, CreditEffects),
    {_State6, FinalEffects} = enq(6, 3, third, State5),
    ?assertEqual(false, lists:any(fun ({send_msg, _, {delivery, _, _}, _}) ->
                                          true;
                                      (_) -> false
                                  end, FinalEffects)),
    ok.

credit_with_drained_test() ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = test_init(test),
    %% checkout with a single credit
    {State1, _, _} =
        apply(meta(1), {checkout, {auto, 1, credited}, Cid}, [], State0),
    ?assertMatch(#state{consumers = #{Cid := #consumer{credit = 1,
                                                       delivery_count = 0}}},
                 State1),
    {State, _Effs, Result} =
         apply(meta(3), {credit, 0, 5, true, Cid}, [], State1),
    ?assertMatch(#state{consumers = #{Cid := #consumer{credit = 0,
                                                       delivery_count = 5}}},
                 State),
    ?assertEqual({multi, [{send_credit_reply, 0},
                          {send_drained, [{?FUNCTION_NAME, 5}]}]},
                           Result),
    ok.

credit_and_drain_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    %% checkout without any initial credit (like AMQP 1.0 would)
    {State3, CheckEffs, _} =
        apply(meta(3), {checkout, {auto, 0, credited}, Cid}, [], State2),

    ?ASSERT_NO_EFF({send_msg, _, {delivery, _, _}}, CheckEffs),
    {State4, Effects, {multi, [{send_credit_reply, 0},
                               {send_drained, [{?FUNCTION_NAME, 2}]}]}} =
     apply(meta(4), {credit, 4, 0, true, Cid}, [], State3),
    ?assertMatch(#state{consumers = #{Cid := #consumer{credit = 0,
                                                       delivery_count = 4}}},
                 State4),

    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}},
                                             {_, {_, second}}]}, _}, Effects),
    {_State5, EnqEffs} = enq(5, 2, third, State4),
    ?ASSERT_NO_EFF({send_msg, _, {delivery, _, _}}, EnqEffs),
    ok.



enq_enq_deq_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    % get returns a reply value
    {_State3, [{monitor, _, _}], {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, unsettled}, Cid}, [], State2),
    ok.

enq_enq_deq_deq_settle_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    % get returns a reply value
    {State3, [{monitor, _, _}], {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, unsettled}, Cid}, [], State2),
    {_State4, _Effects4, {dequeue, empty}} =
        apply(meta(4), {checkout, {dequeue, unsettled}, Cid}, [], State3),
    ok.

enq_enq_checkout_get_settled_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    % get returns a reply value
    {_State2, _Effects, {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, [], State1),
    ok.

checkout_get_empty_test() ->
    Cid = {?FUNCTION_NAME, self()},
    State = test_init(test),
    {_State2, [], {dequeue, empty}} =
        apply(meta(1), {checkout, {dequeue, unsettled}, Cid}, [], State),
    ok.

untracked_enq_deq_test() ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = test_init(test),
    {State1, _, _} = apply(meta(1), {enqueue, undefined, undefined, first}, [], State0),
    {_State2, _, {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, [], State1),
    ok.
release_cursor_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first,  test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {State3, _} = check(Cid, 3, 10, State2),
    % no release cursor effect at this point
    {State4, _} = settle(Cid, 4, 1, State3),
    {_Final, Effects1} = settle(Cid, 5, 0, State4),
    % empty queue forwards release cursor all the way
    ?ASSERT_EFF({release_cursor, 5, _}, Effects1),
    ok.

checkout_enq_settle_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, [{monitor, _, _}]} = check(Cid, 1, test_init(test)),
    {State2, Effects0} = enq(2, 1,  first, State1),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, ?FUNCTION_NAME,
                  [{0, {_, first}}]}, _},
                Effects0),
    {State3, [_Inactive]} = enq(3, 2, second, State2),
    {_, _Effects} = settle(Cid, 4, 0, State3),
    % the release cursor is the smallest raft index that does not
    % contribute to the state of the application
    % ?ASSERT_EFF({release_cursor, 2, _}, Effects),
    ok.

out_of_order_enqueue_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, [{monitor, _, _}]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(2, 1, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects2),
    % assert monitor was set up
    ?ASSERT_EFF({monitor, _, _}, Effects2),
    % enqueue seq num 3 and 4 before 2
    {State3, Effects3} = enq(3, 3, third, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, _}, _}, Effects3),
    {State4, Effects4} = enq(4, 4, fourth, State3),
    % assert no further deliveries where made
    ?assertNoEffect({send_msg, _, {delivery, _, _}, _}, Effects4),
    {_State5, Effects5} = enq(5, 2, second, State4),
    % assert two deliveries were now made
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, second}},
                                               {_, {_, third}},
                                               {_, {_, fourth}}]}, _},
                Effects5),
    ok.

out_of_order_first_enqueue_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = check_n(Cid, 5, 5, test_init(test)),
    {_State2, Effects2} = enq(2, 10, first, State1),
    ?ASSERT_EFF({monitor, process, _}, Effects2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}, _},
                    Effects2),
    ok.

duplicate_enqueue_test() ->
    Cid = {<<"duplicate_enqueue_test">>, self()},
    {State1, [{monitor, _, _}]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(2, 1, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects2),
    {_State3, Effects3} = enq(3, 1, first, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects3),
    ok.

return_non_existent_test() ->
    Cid = {<<"cid">>, self()},
    {State0, [_, _Inactive]} = enq(1, 1, second, test_init(test)),
    % return non-existent
    {_State2, [], _} = apply(meta(3), {return, [99], Cid}, [], State0),
    ok.

return_checked_out_test() ->
    Cid = {<<"cid">>, self()},
    {State0, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, [_Monitor, {aux, active},
              {send_msg, _, {delivery, _, [{MsgId, _}]}, _}]} =
        check(Cid, 2, State0),
    % return
    {_State2, [_, _], _} = apply(meta(3), {return, [MsgId], Cid}, [], State1),
    ok.

return_auto_checked_out_test() ->
    Cid = {<<"cid">>, self()},
    {State00, [_, _]} = enq(1, 1, first, test_init(test)),
    {State0, [_]} = enq(2, 2, second, State00),
    % it first active then inactive as the consumer took on but cannot take
    % any more
    {State1, [_Monitor, {aux, inactive}, {aux, active},
              {send_msg, _, {delivery, _, [{MsgId, _}]}, _} | _]} =
        check_auto(Cid, 2, State0),
    % return should include another delivery
    {_State2, Effects, _} = apply(meta(3), {return, [MsgId], Cid}, [], State1),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{_, {#{delivery_count := 1}, first}}]}, _},
                Effects),
    ok.


cancelled_checkout_out_test() ->
    Cid = {<<"cid">>, self()},
    {State00, [_, _]} = enq(1, 1, first, test_init(test)),
    {State0, [_]} = enq(2, 2, second, State00),
    {State1, _} = check_auto(Cid, 2, State0),
    % cancelled checkout should return all pending messages to queue
    {State2, _, _} = apply(meta(3), {checkout, cancel, Cid}, [], State1),

    {State3, _, {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, [], State2),
    {_State, _, {dequeue, {_, {_, second}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, [], State3),
    ok.

down_with_noproc_consumer_returns_unsettled_test() ->
    Cid = {<<"down_consumer_returns_unsettled_test">>, self()},
    {State0, [_, _]} = enq(1, 1, second, test_init(test)),
    {State1, [{monitor, process, Pid} | _]} = check(Cid, 2, State0),
    {State2, [_, _], _} = apply(meta(3), {down, Pid, noproc}, [], State1),
    {_State, Effects} = check(Cid, 4, State2),
    ?ASSERT_EFF({monitor, process, _}, Effects),
    ok.

down_with_noconnection_marks_suspect_and_node_is_monitored_test() ->
    Pid = spawn(fun() -> ok end),
    Cid = {<<"down_with_noconnect">>, Pid},
    Self = self(),
    Node = node(Pid),
    {State0, Effects0} = enq(1, 1, second, test_init(test)),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects0),
    {State1, Effects1} = check(Cid, 2, State0),
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects1),
    % monitor both enqueuer and consumer
    % because we received a noconnection we now need to monitor the node
    {State2a, _Effects2a, _} = apply(meta(3), {down, Pid, noconnection}, [], State1),
    {State2, Effects2, _} = apply(meta(3), {down, Self, noconnection}, [], State2a),
    ?ASSERT_EFF({monitor, node, _}, Effects2),
    ?assertNoEffect({demonitor, process, _}, Effects2),
    % when the node comes up we need to retry the process monitors for the
    % disconnected processes
    {_State3, Effects3, _} = apply(meta(3), {nodeup, Node}, [], State2),
    % try to re-monitor the suspect processes
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects3),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects3),
    ok.

down_with_noconnection_returns_unack_test() ->
    Pid = spawn(fun() -> ok end),
    Cid = {<<"down_with_noconnect">>, Pid},
    {State0, _} = enq(1, 1, second, test_init(test)),
    ?assertEqual(1, maps:size(State0#state.messages)),
    ?assertEqual(0, queue:len(State0#state.returns)),
    {State1, {_, _}} = deq(2, Cid, unsettled, State0),
    ?assertEqual(0, maps:size(State1#state.messages)),
    ?assertEqual(0, queue:len(State1#state.returns)),
    {State2a, _, _} = apply(meta(3), {down, Pid, noconnection}, [], State1),
    ?assertEqual(1, maps:size(State2a#state.messages)),
    ?assertEqual(1, queue:len(State2a#state.returns)),
    ok.

down_with_noproc_enqueuer_is_cleaned_up_test() ->
    State00 = test_init(test),
    Pid = spawn(fun() -> ok end),
    {State0, Effects0, _} = apply(meta(1), {enqueue, Pid, 1, first}, [], State00),
    ?ASSERT_EFF({monitor, process, _}, Effects0),
    {State1, _Effects1, _} = apply(meta(3), {down, Pid, noproc}, [], State0),
    % ensure there are no enqueuers
    ?assert(0 =:= maps:size(State1#state.enqueuers)),
    ok.

completed_consumer_yields_demonitor_effect_test() ->
    Cid = {<<"completed_consumer_yields_demonitor_effect_test">>, self()},
    {State0, [_, _]} = enq(1, 1, second, test_init(test)),
    {State1, [{monitor, process, _} |  _]} = check(Cid, 2, State0),
    {_, Effects} = settle(Cid, 3, 0, State1),
    ?ASSERT_EFF({demonitor, _, _}, Effects),
    % release cursor for empty queue
    ?ASSERT_EFF({release_cursor, 3, _}, Effects),
    ok.

discarded_message_without_dead_letter_handler_is_removed_test() ->
    Cid = {<<"completed_consumer_yields_demonitor_effect_test">>, self()},
    {State0, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, Effects1} = check_n(Cid, 2, 10, State0),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{0, {#{}, first}}]}, _},
                Effects1),
    {_State2, Effects2, _} = apply(meta(1), {discard, [0], Cid}, [], State1),
    ?assertNoEffect({send_msg, _,
                     {delivery, _, [{0, {#{}, first}}]}, _},
                    Effects2),
    ok.

discarded_message_with_dead_letter_handler_emits_mod_call_effect_test() ->
    Cid = {<<"completed_consumer_yields_demonitor_effect_test">>, self()},
    State00 = init(#{name => test,
                     dead_letter_handler =>
                     {somemod, somefun, [somearg]}}),
    {State0, [_, _]} = enq(1, 1, first, State00),
    {State1, Effects1} = check_n(Cid, 2, 10, State0),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{0, {#{}, first}}]}, _},
                Effects1),
    {_State2, Effects2, _} = apply(meta(1), {discard, [0], Cid}, [], State1),
    % assert mod call effect with appended reason and message
    ?ASSERT_EFF({mod_call, somemod, somefun, [somearg, [{rejected, first}]]},
                Effects2),
    ok.

tick_test() ->
    Cid = {<<"c">>, self()},
    Cid2 = {<<"c2">>, self()},
    {S0, _} = enq(1, 1, fst, test_init(test)),
    {S1, _} = enq(2, 2, snd, S0),
    {S2, {MsgId, _}} = deq(3, Cid, unsettled, S1),
    {S3, {_, _}} = deq(4, Cid2, unsettled, S2),
    {S4, _, _} = apply(meta(5), {return, [MsgId], Cid}, [], S3),

    [{mod_call, _, _, [{test, 1, 1, 2, 1}]}, {aux, emit}] = tick(1, S4),
    ok.

enq_deq_snapshot_recover_test() ->
    Tag = <<"release_cursor_snapshot_state_test">>,
    Cid = {Tag, self()},
    % OthPid = spawn(fun () -> ok end),
    % Oth = {<<"oth">>, OthPid},
    Commands = [
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {checkout, {dequeue, settled}, Cid},
                {enqueue, self(), 3, three},
                {enqueue, self(), 4, four},
                {checkout, {dequeue, settled}, Cid},
                {enqueue, self(), 5, five},
                {checkout, {dequeue, settled}, Cid}
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_settle_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    % OthPid = spawn(fun () -> ok end),
    % Oth = {<<"oth">>, OthPid},
    Commands = [
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {checkout, {dequeue, unsettled}, Cid},
                {settle, [0], Cid}
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_settle_snapshot_recover_2_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    OthPid = spawn(fun () -> ok end),
    Oth = {<<"oth">>, OthPid},
    Commands = [
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {checkout, {dequeue, unsettled}, Cid},
                {settle, [0], Cid},
                {enqueue, self(), 3, two},
                {checkout, {dequeue, unsettled}, Oth},
                {settle, [0], Oth}
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                {checkout, {auto, 2, simple_prefetch}, Cid},
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {enqueue, self(), 3, three},
                purge
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_return_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    % OthPid = spawn(fun () -> ok end),
    % Oth = {<<"oth">>, OthPid},
    Commands = [
                {enqueue, self(), 1, one}, %% to Cid
                {checkout, {auto, 1, simple_prefetch}, Cid},
                % {checkout, {auto, 1, simple_prefetch}, Oth},
                {return, [0], Cid}, %% should be re-delivered to Oth
                {enqueue, self(), 2, two}, %% Cid prefix_msg_count: 1
                % {enqueue, self(), 3, three}, %% Queued: prefetch_msg_count: 2?
                % {settle, [0], Oth},
                {settle, [1], Cid},
                {settle, [2], Cid}
                % purge
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_check_settle_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                {checkout, {auto, 2, simple_prefetch}, Cid},
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {settle, [1], Cid},
                {settle, [0], Cid},
                {enqueue, self(), 3, three},
                {settle, [2], Cid}
              ],
         % ?debugFmt("~w running commands ~w~n", [?FUNCTION_NAME, C]),
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_check_settle_snapshot_purge_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                {checkout, {auto, 2, simple_prefetch}, Cid},
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {settle, [1], Cid},
                {settle, [0], Cid},
                {enqueue, self(), 3, three},
                purge
              ],
         % ?debugFmt("~w running commands ~w~n", [?FUNCTION_NAME, C]),
    run_snapshot_test(?FUNCTION_NAME, Commands).

run_snapshot_test(Name, Commands) ->
    %% create every incremental permuation of the commands lists
    %% and run the snapshot tests against that
    [begin
         ?debugFmt("~w running command to ~w~n", [?FUNCTION_NAME, lists:last(C)]),
         run_snapshot_test0(Name, C)
     end || C <- prefixes(Commands, 1, [])].

run_snapshot_test0(Name, Commands) ->
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, Effects} = run_log(test_init(Name), Entries),

    [begin
         Filtered = lists:dropwhile(fun({X, _}) when X =< SnapIdx -> true;
                                       (_) -> false
                                    end, Entries),
         {S, _} = run_log(SnapState, Filtered),
         % assert log can be restored from any release cursor index
         % ?debugFmt("Name ~p Idx ~p S~p~nState~p~nSnapState ~p~nFiltered ~p~n",
         %           [Name, SnapIdx, S, State, SnapState, Filtered]),
         ?assertEqual(State, S)
     end || {release_cursor, SnapIdx, SnapState} <- Effects],
    ok.

prefixes(Source, N, Acc) when N > length(Source) ->
    lists:reverse(Acc);
prefixes(Source, N, Acc) ->
    {X, _} = lists:split(N, Source),
    prefixes(Source, N+1, [X | Acc]).

delivery_query_returns_deliveries_test() ->
    Tag = <<"release_cursor_snapshot_state_test">>,
    Cid = {Tag, self()},
    Commands = [
                {checkout, {auto, 5, simple_prefetch}, Cid},
                {enqueue, self(), 1, one},
                {enqueue, self(), 2, two},
                {enqueue, self(), 3, tre},
                {enqueue, self(), 4, for}
              ],
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, _Effects} = run_log(test_init(help), Entries),
    % 3 deliveries are returned
    [{0, {#{}, one}}] = get_checked_out(Cid, 0, 0, State),
    [_, _, _] = get_checked_out(Cid, 1, 3, State),
    ok.

pending_enqueue_is_enqueued_on_down_test() ->
    Cid = {<<"cid">>, self()},
    Pid = self(),
    {State0, _} = enq(1, 2, first, test_init(test)),
    {State1, _, _} = apply(meta(2), {down, Pid, noproc}, [], State0),
    {_State2, _, {dequeue, {0, {_, first}}}} =
        apply(meta(3), {checkout, {dequeue, settled}, Cid}, [], State1),
    ok.

duplicate_delivery_test() ->
    {State0, _} = enq(1, 1, first, test_init(test)),
    {#state{ra_indexes = RaIdxs,
            messages = Messages}, _} = enq(2, 1, first, State0),
    ?assertEqual(1, rabbit_fifo_index:size(RaIdxs)),
    ?assertEqual(1, maps:size(Messages)),
    ok.

state_enter_test() ->
    S0 = init(#{name => the_name,
                become_leader_handler => {m, f, [a]}}),
    [{mod_call, m, f, [a, the_name]}] = state_enter(leader, S0),
    ok.

leader_monitors_on_state_enter_test() ->
    Cid = {<<"cid">>, self()},
    {State0, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, _} = check_auto(Cid, 2, State0),
    Self = self(),
    %% as we have an enqueuer _and_ a consumer we chould
    %% get two monitor effects in total, even if they are for the same
    %% processs
    [{monitor, process, Self},
     {monitor, process, Self}] = state_enter(leader, State1),
    ok.


purge_test() ->
    Cid = {<<"purge_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _, {purge, 1}} = apply(meta(2), purge, [], State1),
    {State3, _} = enq(3, 2, second, State2),
    % get returns a reply value
    {_State4, [{monitor, _, _}], {dequeue, {0, {_, second}}}} =
        apply(meta(4), {checkout, {dequeue, unsettled}, Cid}, [], State3),
    ok.

purge_with_checkout_test() ->
    Cid = {<<"purge_test">>, self()},
    {State0, _} = check_auto(Cid, 1, test_init(?FUNCTION_NAME)),
    {State1, _} = enq(2, 1, first, State0),
    {State2, _} = enq(3, 2, second, State1),
    {State3, _, {purge, 2}} = apply(meta(2), purge, [], State2),
    #consumer{checked_out = Checked} = maps:get(Cid, State3#state.consumers),
    ?assertEqual(0, maps:size(Checked)),
    ok.

meta(Idx) ->
    #{index => Idx, term => 1}.

enq(Idx, MsgSeq, Msg, State) ->
    strip_reply(
        apply(meta(Idx), {enqueue, self(), MsgSeq, Msg}, [], State)).

deq(Idx, Cid, Settlement, State0) ->
    {State, _, {dequeue, Msg}} =
        apply(meta(Idx), {checkout, {dequeue,  Settlement}, Cid}, [], State0),
    {State, Msg}.

check_n(Cid, Idx, N, State) ->
    strip_reply(apply(meta(Idx),
                      {checkout, {auto, N, simple_prefetch}, Cid}, [], State)).

check(Cid, Idx, State) ->
    strip_reply(apply(meta(Idx),
                      {checkout, {once, 1, simple_prefetch}, Cid}, [], State)).

check_auto(Cid, Idx, State) ->
    strip_reply(apply(meta(Idx),
                      {checkout, {auto, 1, simple_prefetch}, Cid}, [], State)).

check(Cid, Idx, Num, State) ->
    strip_reply(apply(meta(Idx),
                      {checkout, {once, Num, simple_prefetch}, Cid}, [], State)).

settle(Cid, Idx, MsgId, State) ->
    strip_reply(apply(meta(Idx), {settle, [MsgId], Cid}, [], State)).

credit(Cid, Idx, Credit, DelCnt, Drain, State) ->
    strip_reply(apply(meta(Idx), {credit, Credit, DelCnt, Drain, Cid}, [], State)).

strip_reply({State, Effects, _Replu}) ->
    {State, Effects}.

run_log(InitState, Entries) ->
    lists:foldl(fun ({Idx, E}, {Acc0, Efx0}) ->
                        case apply(meta(Idx), E, Efx0, Acc0) of
                            {Acc, Efx, _} ->
                                {Acc, Efx}
                        end
                end, {InitState, []}, Entries).


%% AUX Tests

aux_test() ->
    _ = ra_machine_ets:start_link(),
    Aux0 = init_aux(aux_test),
    MacState = init(#{name => aux_test}),
    Log = undefined,
    {no_reply, Aux, undefined} = handle_aux(leader, cast, active, Aux0,
                                            Log, MacState),
    {no_reply, _Aux, undefined} = handle_aux(leader, cast, emit, Aux,
                                             Log, MacState),
    [X] = ets:lookup(rabbit_fifo_usage, aux_test),
    ?assert(X > 0.0),
    ok.


-endif.

