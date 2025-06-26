%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_quorum_queue).
-feature(maybe_expr, enable).

-behaviour(rabbit_queue_type).
-behaviour(rabbit_policy_validator).
-behaviour(rabbit_policy_merge_strategy).

-export([init/1,
         close/1,
         update/2,
         handle_event/3]).
-export([is_recoverable/1,
         recover/2,
         system_recover/1,
         stop/1,
         start_server/1,
         restart_server/1,
         stop_server/1,
         delete/4,
         delete_immediately/1]).
-export([state_info/1, info/2, stat/1, infos/1, infos/2]).
-export([settle/5, dequeue/5, consume/3, cancel/3]).
-export([credit_v1/5, credit/6]).
-export([purge/1]).
-export([deliver/3]).
-export([dead_letter_publish/5]).
-export([cluster_state/1, status/2]).
-export([update_consumer_handler/8, update_consumer/9]).
-export([cancel_consumer_handler/2, cancel_consumer/3]).
-export([become_leader/2, handle_tick/3, spawn_deleter/1]).
-export([rpc_delete_metrics/1,
         key_metrics_rpc/1]).
-export([format/2]).
-export([open_files/1]).
-export([peek/2, peek/3]).
-export([add_member/2,
         add_member/3,
         add_member/4,
         add_member/5]).
-export([delete_member/3, delete_member/2]).
-export([requeue/3]).
-export([policy_changed/1]).
-export([format_ra_event/3]).
-export([cleanup_data_dir/0]).
-export([shrink_all/1,
         grow/4,
         grow/5]).
-export([transfer_leadership/2, get_replicas/1, queue_length/1]).
-export([list_with_minimum_quorum/0,
         list_with_local_promotable/0,
         list_with_local_promotable_for_cli/0,
         filter_quorum_critical/3,
         all_replica_states/0]).
-export([capabilities/0]).
-export([repair_amqqueue_nodes/1,
         repair_amqqueue_nodes/2
         ]).
-export([reclaim_memory/2,
         wal_force_roll_over/1]).
-export([notify_decorators/1,
         notify_decorators/3,
         spawn_notify_decorators/3]).

-export([is_enabled/0,
         is_compatible/3,
         declare/2,
         is_stateful/0]).
-export([validate_policy/1, merge_policy_value/3]).

-export([force_shrink_member_to_current_member/2,
         force_vhost_queues_shrink_member_to_current_member/1,
         force_all_queues_shrink_member_to_current_member/0]).

-export([policy_apply_to_name/0,
         drain/1,
         revive/0,
         queue_vm_stats_sups/0,
         queue_vm_ets/0]).

-export([force_checkpoint/2, force_checkpoint_on_queue/1]).

%% for backwards compatibility
-export([file_handle_leader_reservation/1,
         file_handle_other_reservation/0,
         file_handle_release_reservation/0]).

-export([leader_health_check/2,
         run_leader_health_check/4]).

-ifdef(TEST).
-export([filter_promotable/2,
         ra_machine_config/1]).
-endif.

-import(rabbit_queue_type_util, [args_policy_lookup/3,
                                 qname_to_internal_name/1,
                                 erpc_call/5]).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-rabbit_boot_step(
   {rabbit_quorum_queue_type,
    [{description, "Quorum queue: queue type"},
     {mfa,      {rabbit_registry, register,
                    [queue, <<"quorum">>, ?MODULE]}},
     {cleanup,  {rabbit_registry, unregister,
                 [queue, <<"quorum">>]}},
     {requires, rabbit_registry}]}).

-type msg_id() :: non_neg_integer().
-type qmsg() :: {rabbit_types:r('queue'), pid(), msg_id(), boolean(),
                 mc:state()}.
-type membership() :: voter | non_voter | promotable.  %% see ra_membership() in Ra.
-type replica_states() :: #{atom() => replica_state()}.
-type replica_state() :: leader | follower | non_voter | promotable.

-define(RA_SYSTEM, quorum_queues).
-define(RA_WAL_NAME, ra_log_wal).

-define(DEFAULT_DELIVERY_LIMIT, 20).

-define(INFO(Str, Args),
        rabbit_log:info("[~s:~s/~b] " Str,
                        [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY | Args])).


-define(STATISTICS_KEYS,
        [policy,
         operator_policy,
         effective_policy_definition,
         consumers,
         memory,
         state,
         garbage_collection,
         leader,
         online,
         members,
         open_files,
         single_active_consumer_pid,
         single_active_consumer_tag,
         messages_ram,
         message_bytes_ram,
         messages_dlx,
         message_bytes_dlx
        ]).

-define(INFO_KEYS, [name, durable, auto_delete, arguments, pid, messages, messages_ready,
                    messages_unacknowledged, local_state, type] ++ ?STATISTICS_KEYS).

-define(RPC_TIMEOUT, 1000).
-define(START_CLUSTER_TIMEOUT, 5000).
-define(START_CLUSTER_RPC_TIMEOUT, 60_000). %% needs to be longer than START_CLUSTER_TIMEOUT
-define(TICK_INTERVAL, 5000). %% the ra server tick time
-define(DELETE_TIMEOUT, 5000).
-define(MEMBER_CHANGE_TIMEOUT, 20_000).
-define(SNAPSHOT_INTERVAL, 8192). %% the ra default is 4096
%% setting a low default here to allow quorum queues to better chose themselves
%% when to take a checkpoint
-define(MIN_CHECKPOINT_INTERVAL, 64).
-define(LEADER_HEALTH_CHECK_TIMEOUT, 5_000).
-define(GLOBAL_LEADER_HEALTH_CHECK_TIMEOUT, 60_000).

%%----------- QQ policies ---------------------------------------------------

-rabbit_boot_step(
   {?MODULE,
    [{description, "QQ target group size policies. "
      "target-group-size controls the targeted number of "
      "member nodes for the queue. If set, RabbitMQ will try to "
      "grow the queue members to the target size. "
      "See module rabbit_quorum_queue_periodic_membership_reconciliation."},
     {mfa, {rabbit_registry, register,
            [policy_validator, <<"target-group-size">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [operator_policy_validator, <<"target-group-size">>, ?MODULE]}},
     {mfa, {rabbit_registry, register,
            [policy_merge_strategy, <<"target-group-size">>, ?MODULE]}},
     {requires, [rabbit_registry]},
     {enables, recovery}]}).

validate_policy(Args) ->
    Count = proplists:get_value(<<"target-group-size">>, Args, none),
    case is_integer(Count) andalso Count > 0 of
        true -> ok;
        false -> {error, "~tp is not a valid qq target count value", [Count]}
    end.

merge_policy_value(<<"target-group-size">>, Val, OpVal) ->
    max(Val, OpVal).

%%----------- rabbit_queue_type ---------------------------------------------

-spec is_enabled() -> boolean().
is_enabled() -> true.

-spec is_compatible(boolean(), boolean(), boolean()) -> boolean().
is_compatible(_Durable = true,
              _Exclusive = false,
              _AutoDelete = false) ->
    true;
is_compatible(_, _, _) ->
    false.

-spec init(amqqueue:amqqueue()) -> {ok, rabbit_fifo_client:state()}.
init(Q) when ?is_amqqueue(Q) ->
    {ok, SoftLimit} = application:get_env(rabbit, quorum_commands_soft_limit),
    {Name, _} = MaybeLeader = amqqueue:get_pid(Q),
    Leader = case find_leader(Q) of
                 undefined ->
                     %% leader from queue record will have to suffice
                     MaybeLeader;
                 LikelyLeader ->
                     LikelyLeader
             end,
    Nodes = get_nodes(Q),
    %% Ensure the leader is listed first to increase likelihood of first
    %% server tried is the one we want
    Servers0 = [{Name, N} || N <- Nodes],
    Servers = [Leader | lists:delete(Leader, Servers0)],
    {ok, rabbit_fifo_client:init(Servers, SoftLimit)}.

-spec close(rabbit_fifo_client:state()) -> ok.
close(State) ->
    rabbit_fifo_client:close(State).

-spec update(amqqueue:amqqueue(), rabbit_fifo_client:state()) ->
    rabbit_fifo_client:state().
update(Q, State) when ?amqqueue_is_quorum(Q) ->
    %% QQ state maintains it's own updates
    State.

-spec handle_event(rabbit_amqqueue:name(),
                   {amqqueue:ra_server_id(), any()},
                   rabbit_fifo_client:state()) ->
    {ok, rabbit_fifo_client:state(), rabbit_queue_type:actions()} |
    {eol, rabbit_queue_type:actions()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
handle_event(QName, {From, Evt}, QState) ->
    rabbit_fifo_client:handle_ra_event(QName, From, Evt, QState).

-spec declare(amqqueue:amqqueue(), node()) ->
    {new | existing, amqqueue:amqqueue()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
declare(Q, _Node) when ?amqqueue_is_quorum(Q) ->
    maybe
        ok ?= rabbit_queue_type_util:check_auto_delete(Q),
        ok ?= rabbit_queue_type_util:check_exclusive(Q),
        ok ?= rabbit_queue_type_util:check_non_durable(Q),
        start_cluster(Q)
    end.

start_cluster(Q) ->
    QName = amqqueue:get_name(Q),
    Durable = amqqueue:is_durable(Q),
    AutoDelete = amqqueue:is_auto_delete(Q),
    Arguments = amqqueue:get_arguments(Q),
    Opts = amqqueue:get_options(Q),
    ActingUser = maps:get(user, Opts, ?UNKNOWN_USER),
    QuorumSize = get_default_quorum_initial_group_size(Arguments),
    RaName = case qname_to_internal_name(QName) of
                 {ok, A} ->
                     A;
                 {error, {too_long, N}} ->
                     rabbit_data_coercion:to_atom(ra:new_uid(N))
             end,
    {LeaderNode, FollowerNodes} =
        rabbit_queue_location:select_leader_and_followers(Q, QuorumSize),
    LeaderId = {RaName, LeaderNode},
    NewQ0 = amqqueue:set_pid(Q, LeaderId),
    NewQ1 = amqqueue:set_type_state(NewQ0,
                                    #{nodes => [LeaderNode | FollowerNodes]}),

    Versions = [V || {ok, V} <- erpc:multicall(FollowerNodes,
                                               rabbit_fifo, version, [],
                                               ?RPC_TIMEOUT)],
    MinVersion = lists:min([rabbit_fifo:version() | Versions]),

    rabbit_log:debug("Will start up to ~w replicas for quorum queue ~ts with "
                     "leader on node '~ts', initial machine version ~b",
                     [QuorumSize, rabbit_misc:rs(QName), LeaderNode, MinVersion]),
    case rabbit_amqqueue:internal_declare(NewQ1, false) of
        {created, NewQ} ->
            RaConfs = [make_ra_conf(NewQ, ServerId, voter, MinVersion)
                       || ServerId <- members(NewQ)],

            %% khepri projections on remote nodes are eventually consistent
            wait_for_projections(LeaderNode, QName),
            try erpc_call(LeaderNode, ra, start_cluster,
                          [?RA_SYSTEM, RaConfs, ?START_CLUSTER_TIMEOUT],
                          ?START_CLUSTER_RPC_TIMEOUT) of
                {ok, _, _} ->
                    %% ensure the latest config is evaluated properly
                    %% even when running the machine version from 0
                    %% as earlier versions may not understand all the config
                    %% keys
                    %% TODO: handle error - what should be done if the
                    %% config cannot be updated
                    ok = rabbit_fifo_client:update_machine_state(LeaderId,
                                                                 ra_machine_config(NewQ)),
                    notify_decorators(NewQ, startup),
                    rabbit_quorum_queue_periodic_membership_reconciliation:queue_created(NewQ),
                    rabbit_event:notify(queue_created,
                                        [{name, QName},
                                         {durable, Durable},
                                         {auto_delete, AutoDelete},
                                         {exclusive, false},
                                         {type, amqqueue:get_type(Q)},
                                         {arguments, Arguments},
                                         {user_who_performed_action,
                                          ActingUser}]),
                    {new, NewQ};
                {error, Error} ->
                    declare_queue_error(Error, NewQ, LeaderNode, ActingUser)
            catch
                error:Error ->
                    declare_queue_error(Error, NewQ, LeaderNode, ActingUser)
            end;
        {existing, _} = Ex ->
            Ex;
        {error, timeout} ->
            {protocol_error, internal_error,
             "Could not declare quorum ~ts on node '~ts' because the metadata "
             "store operation timed out",
             [rabbit_misc:rs(QName), node()]}
    end.

declare_queue_error(Error, Queue, Leader, ActingUser) ->
    _ = rabbit_amqqueue:internal_delete(Queue, ActingUser),
    {protocol_error, internal_error,
     "Cannot declare quorum ~ts on node '~ts' with leader on node '~ts': ~255p",
     [rabbit_misc:rs(amqqueue:get_name(Queue)), node(), Leader, Error]}.

ra_machine(Q) ->
    {module, rabbit_fifo, ra_machine_config(Q)}.

gather_policy_config(Q, IsQueueDeclaration) ->
    QName = amqqueue:get_name(Q),
    %% take the minimum value of the policy and the queue arg if present
    MaxLength = args_policy_lookup(<<"max-length">>, fun min/2, Q),
    OverflowBin = args_policy_lookup(<<"overflow">>, fun policy_has_precedence/2, Q),
    Overflow = overflow(OverflowBin, drop_head, QName),
    MaxBytes = args_policy_lookup(<<"max-length-bytes">>, fun min/2, Q),
    DeliveryLimit = case args_policy_lookup(<<"delivery-limit">>,
                                            fun resolve_delivery_limit/2, Q) of
                        undefined ->
                            case IsQueueDeclaration of
                                true ->
                                    rabbit_log:info(
                                              "~ts: delivery_limit not set, defaulting to ~b",
                                              [rabbit_misc:rs(QName), ?DEFAULT_DELIVERY_LIMIT]);
                                false ->
                                    ok
                            end,
                            ?DEFAULT_DELIVERY_LIMIT;
                        DL ->
                            DL
                    end,
    Expires = args_policy_lookup(<<"expires">>, fun min/2, Q),
    MsgTTL = args_policy_lookup(<<"message-ttl">>, fun min/2, Q),
    DeadLetterHandler = dead_letter_handler(Q, Overflow),
    #{dead_letter_handler => DeadLetterHandler,
      max_length => MaxLength,
      max_bytes => MaxBytes,
      delivery_limit => DeliveryLimit,
      overflow_strategy => Overflow,
      expires => Expires,
      msg_ttl => MsgTTL
     }.

ra_machine_config(Q) when ?is_amqqueue(Q) ->
    PolicyConfig = gather_policy_config(Q, true),
    QName = amqqueue:get_name(Q),
    {Name, _} = amqqueue:get_pid(Q),
    PolicyConfig#{
      name => Name,
      queue_resource => QName,
      become_leader_handler => {?MODULE, become_leader, [QName]},
      single_active_consumer_on => single_active_consumer_on(Q),
      created => erlang:system_time(millisecond)
     }.

resolve_delivery_limit(PolVal, ArgVal)
  when PolVal < 0 orelse ArgVal < 0 ->
    max(PolVal, ArgVal);
resolve_delivery_limit(PolVal, ArgVal) ->
    min(PolVal, ArgVal).

policy_has_precedence(Policy, _QueueArg) ->
    Policy.

queue_arg_has_precedence(_Policy, QueueArg) ->
    QueueArg.

single_active_consumer_on(Q) ->
    QArguments = amqqueue:get_arguments(Q),
    case rabbit_misc:table_lookup(QArguments, <<"x-single-active-consumer">>) of
        {bool, true} -> true;
        _            -> false
    end.

update_consumer_handler(QName, {ConsumerTag, ChPid}, Exclusive, AckRequired,
                        Prefetch, Active, ActivityStatus, Args) ->
    catch local_or_remote_handler(ChPid, rabbit_quorum_queue, update_consumer,
                                  [QName, ChPid, ConsumerTag, Exclusive,
                                   AckRequired, Prefetch, Active,
                                   ActivityStatus, Args]).

update_consumer(QName, ChPid, ConsumerTag, Exclusive, AckRequired, Prefetch,
                Active, ActivityStatus, Args) ->
    catch rabbit_core_metrics:consumer_updated(ChPid, ConsumerTag, Exclusive,
                                               AckRequired,
                                               QName, Prefetch, Active,
                                               ActivityStatus, Args).

cancel_consumer_handler(QName, {ConsumerTag, ChPid}) ->
    catch local_or_remote_handler(ChPid, rabbit_quorum_queue, cancel_consumer,
                                  [QName, ChPid, ConsumerTag]).

cancel_consumer(QName, ChPid, ConsumerTag) ->
    catch rabbit_core_metrics:consumer_deleted(ChPid, ConsumerTag, QName),
    emit_consumer_deleted(ChPid, ConsumerTag, QName, ?INTERNAL_USER).

local_or_remote_handler(ChPid, Module, Function, Args) ->
    Node = node(ChPid),
    case Node == node() of
        true ->
            erlang:apply(Module, Function, Args);
        false ->
            %% this could potentially block for a while if the node is
            %% in disconnected state or tcp buffers are full
            erpc:cast(Node, Module, Function, Args)
    end.

become_leader(_QName, _Name) ->
    %% noop now as we instead rely on the promt tick_timeout + repair to update
    %% the meta data store after a leader change
    ok.

become_leader0(QName, Name) ->
    Fun = fun (Q1) ->
                  amqqueue:set_state(
                    amqqueue:set_pid(Q1, {Name, node()}),
                    live)
          end,
    _ = rabbit_amqqueue:update(QName, Fun),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q0} when ?is_amqqueue(Q0) ->
            Nodes = get_nodes(Q0),
            _ = [_ = erpc_call(Node, ?MODULE, rpc_delete_metrics,
                               [QName], ?RPC_TIMEOUT)
                 || Node <- Nodes, Node =/= node()],
            ok;
        _ ->
            ok
    end.

-spec all_replica_states() -> {node(), #{atom() => atom()}}.
all_replica_states() ->
    Rows0 = ets:tab2list(ra_state),
    Rows = lists:filtermap(
                    fun
                        (T = {K, _, _}) ->
                            case whereis(K) of
                                undefined ->
                                    false;
                                P when is_pid(P) ->
                                    {true, to_replica_state(T)}
                            end;
                        (_T) ->
                            false
                    end, Rows0),
    {node(), maps:from_list(Rows)}.

to_replica_state({K, follower, promotable}) ->
    {K, promotable};
to_replica_state({K, follower, non_voter}) ->
    {K, non_voter};
to_replica_state({K, S, _}) ->
    %% voter or unknown
    {K, S}.

-spec list_with_minimum_quorum() -> [amqqueue:amqqueue()].
list_with_minimum_quorum() ->
    Queues = rabbit_amqqueue:list_local_quorum_queues(),
    ReplicaStates = get_replica_states(rabbit_nodes:list_running()),
    filter_quorum_critical(Queues, ReplicaStates, node()).

-spec list_with_local_promotable() -> [amqqueue:amqqueue()].
list_with_local_promotable() ->
    Queues = rabbit_amqqueue:list_local_quorum_queues(),
    #{node() := ReplicaStates} = get_replica_states([node()]),
    filter_promotable(Queues, ReplicaStates).

-spec list_with_local_promotable_for_cli() -> [#{binary() => any()}].
list_with_local_promotable_for_cli() ->
    Qs = list_with_local_promotable(),
    lists:map(fun amqqueue:to_printable/1, Qs).

-spec get_replica_states([node()]) -> #{node() => replica_states()}.
get_replica_states(Nodes) ->
    maps:from_list(
      rabbit_misc:append_rpc_all_nodes(Nodes, ?MODULE, all_replica_states, [])).

-spec filter_promotable([amqqueue:amqqueue()], replica_states()) ->
    [amqqueue:amqqueue()].
filter_promotable(Queues, ReplicaStates) ->
    lists:filter(fun (Q) ->
                    {RaName, _Node} = amqqueue:get_pid(Q),
                    State = maps:get(RaName, ReplicaStates),
                    State == promotable
                 end, Queues).

-spec filter_quorum_critical([amqqueue:amqqueue()], #{node() => replica_states()}, node()) ->
    [amqqueue:amqqueue()].
filter_quorum_critical(Queues, ReplicaStates, Self) ->
    lists:filter(fun (Q) ->
                    MemberNodes = get_nodes(Q),
                    {Name, _Node} = amqqueue:get_pid(Q),
                    AllUp = lists:filter(fun (N) ->
                                            case maps:get(N, ReplicaStates, undefined) of
                                                #{Name := State}
                                                  when State =:= follower orelse
                                                       State =:= leader orelse
                                                       (State =:= promotable andalso N =:= Self) orelse
                                                       (State =:= non_voter andalso N =:= Self) ->
                                                    true;
                                                _ -> false
                                            end
                                         end, MemberNodes),
                    MinQuorum = length(MemberNodes) div 2 + 1,
                    length(AllUp) =< MinQuorum
                 end, Queues).

capabilities() ->
    #{unsupported_policies => [%% Classic policies
                               <<"max-priority">>, <<"queue-mode">>,
                               <<"ha-mode">>, <<"ha-params">>,
                               <<"ha-sync-mode">>, <<"ha-promote-on-shutdown">>, <<"ha-promote-on-failure">>,
                               <<"queue-master-locator">>,
                               %% Stream policies
                               <<"max-age">>, <<"stream-max-segment-size-bytes">>, <<"initial-cluster-size">>],
      queue_arguments => [<<"x-dead-letter-exchange">>, <<"x-dead-letter-routing-key">>,
                          <<"x-dead-letter-strategy">>, <<"x-expires">>, <<"x-max-length">>,
                          <<"x-max-length-bytes">>, <<"x-max-in-memory-length">>,
                          <<"x-max-in-memory-bytes">>, <<"x-overflow">>,
                          <<"x-single-active-consumer">>, <<"x-queue-type">>,
                          <<"x-quorum-initial-group-size">>, <<"x-delivery-limit">>,
                          <<"x-message-ttl">>, <<"x-queue-leader-locator">>],
      consumer_arguments => [<<"x-priority">>],
      server_named => false,
      rebalance_module => ?MODULE,
      can_redeliver => true,
      is_replicable => true
     }.

rpc_delete_metrics(QName) ->
    ets:delete(queue_coarse_metrics, QName),
    ets:delete(queue_metrics, QName),
    ok.

spawn_deleter(QName) ->
    spawn(fun () ->
                  {ok, Q} = rabbit_amqqueue:lookup(QName),
                  delete(Q, false, false, <<"expired">>)
          end).

spawn_notify_decorators(QName, startup = Fun, Args) ->
    spawn(fun() ->
                  notify_decorators(QName, Fun, Args)
          end);
spawn_notify_decorators(QName, Fun, Args) ->
    %% run in ra process for now
    catch notify_decorators(QName, Fun, Args).

handle_tick(QName,
            #{config := #{name := Name} = Cfg,
              num_active_consumers := NumConsumers,
              num_checked_out := NumCheckedOut,
              num_ready_messages := NumReadyMsgs,
              num_messages := NumMessages,
              num_enqueuers := NumEnqueuers,
              enqueue_message_bytes := EnqueueBytes,
              checkout_message_bytes := CheckoutBytes,
              num_discarded := NumDiscarded,
              num_discard_checked_out  :=  NumDiscardedCheckedOut,
              discard_message_bytes := DiscardBytes,
              discard_checkout_message_bytes := DiscardCheckoutBytes,
              smallest_raft_index := _} = Overview,
            Nodes) ->
    %% this makes calls to remote processes so cannot be run inside the
    %% ra server
    spawn(
      fun() ->
              try
                  {ok, Q} = rabbit_amqqueue:lookup(QName),
                  ok = repair_leader_record(Q, Name),
                  Reductions = reductions(Name),
                  rabbit_core_metrics:queue_stats(QName, NumReadyMsgs,
                                                  NumCheckedOut, NumMessages,
                                                  Reductions),
                  Util = case NumConsumers of
                             0 -> 0;
                             _ -> rabbit_fifo:usage(Name)
                         end,

                  Keys = ?STATISTICS_KEYS -- [leader,
                                              consumers,
                                              messages_dlx,
                                              message_bytes_dlx,
                                              single_active_consumer_pid,
                                              single_active_consumer_tag
                                             ],
                  {SacTag, SacPid} = maps:get(single_active_consumer_id,
                                              Overview, {'', ''}),
                  Infos0 = maps:fold(
                             fun(num_ready_messages_high, V, Acc) ->
                                     [{messages_ready_high, V} | Acc];
                                (num_ready_messages_normal, V, Acc) ->
                                     [{messages_ready_normal, V} | Acc];
                                (num_ready_messages_return, V, Acc) ->
                                     [{messages_ready_returned, V} | Acc];
                                (_, _, Acc) ->
                                     Acc
                             end, info(Q, Keys), Overview),
                  MsgBytesDiscarded = DiscardBytes + DiscardCheckoutBytes,
                  MsgBytes = EnqueueBytes + CheckoutBytes + MsgBytesDiscarded,
                  Infos = [{consumers, NumConsumers},
                           {publishers, NumEnqueuers},
                           {consumer_capacity, Util},
                           {consumer_utilisation, Util},
                           {message_bytes_ready, EnqueueBytes},
                           {message_bytes_unacknowledged, CheckoutBytes},
                           {message_bytes, MsgBytes},
                           {message_bytes_persistent, MsgBytes},
                           {messages_persistent, NumMessages},
                           {messages_dlx, NumDiscarded + NumDiscardedCheckedOut},
                           {message_bytes_dlx, MsgBytesDiscarded},
                           {single_active_consumer_tag, SacTag},
                           {single_active_consumer_pid, SacPid},
                           {leader, node()},
                           {delivery_limit, case maps:get(delivery_limit, Cfg,
                                                          undefined) of
                                                undefined ->
                                                    unlimited;
                                                Limit ->
                                                    Limit
                                            end}
                           | Infos0],
                  rabbit_core_metrics:queue_stats(QName, Infos),
                  case repair_amqqueue_nodes(Q) of
                      ok ->
                          ok;
                      repaired ->
                          rabbit_log:debug("Repaired quorum queue ~ts amqqueue record",
                                           [rabbit_misc:rs(QName)])
                  end,
                  ExpectedNodes = rabbit_nodes:list_members(),
                  case Nodes -- ExpectedNodes of
                      [] ->
                          ok;
                      Stale when length(ExpectedNodes) > 0 ->
                          %% rabbit_nodes:list_members/0 returns [] when there
                          %% is an error so we need to handle that case
                          rabbit_log:debug("~ts: stale nodes detected in quorum "
                                           "queue state. Purging ~w",
                                           [rabbit_misc:rs(QName), Stale]),
                          %% pipeline purge command
                          ok = ra:pipeline_command(amqqueue:get_pid(Q),
                                                   rabbit_fifo:make_purge_nodes(Stale)),
                          ok;
                      _ ->
                          ok
                  end,
                  maybe_apply_policies(Q, Overview),
                  ok
              catch
                  _:Err ->
                      rabbit_log:debug("~ts: handle tick failed with ~p",
                                       [rabbit_misc:rs(QName), Err]),
                      ok
              end
      end);
handle_tick(QName, Config, _Nodes) ->
    rabbit_log:debug("~ts: handle tick received unexpected config format ~tp",
                     [rabbit_misc:rs(QName), Config]).

repair_leader_record(Q, Name) ->
    Node = node(),
    case amqqueue:get_pid(Q) of
        {_, Node} ->
            %% it's ok - we don't need to do anything
            ok;
        _ ->
            QName = amqqueue:get_name(Q),
            rabbit_log:debug("~ts: updating leader record to current node ~ts",
                             [rabbit_misc:rs(QName), Node]),
            ok = become_leader0(QName, Name),
            ok
    end,
    ok.

repair_amqqueue_nodes(VHost, QueueName) ->
    QName = #resource{virtual_host = VHost, name = QueueName, kind = queue},
    repair_amqqueue_nodes(QName).

-spec repair_amqqueue_nodes(rabbit_types:r('queue') | amqqueue:amqqueue()) ->
    ok | repaired.
repair_amqqueue_nodes(QName = #resource{}) ->
    {ok, Q0} = rabbit_amqqueue:lookup(QName),
    repair_amqqueue_nodes(Q0);
repair_amqqueue_nodes(Q0) ->
    QName = amqqueue:get_name(Q0),
    {Name, _} = amqqueue:get_pid(Q0),
    Members = ra_leaderboard:lookup_members(Name),
    RaNodes = [N || {_, N} <- Members],
    #{nodes := Nodes} = amqqueue:get_type_state(Q0),
    case lists:sort(RaNodes) =:= lists:sort(Nodes) of
        true ->
            %% up to date
            ok;
        false ->
            %% update amqqueue record
            Fun = fun (Q) ->
                          TS0 = amqqueue:get_type_state(Q),
                          TS = TS0#{nodes => RaNodes},
                          amqqueue:set_type_state(Q, TS)
                  end,
            _ = rabbit_amqqueue:update(QName, Fun),
            repaired
    end.

reductions(Name) ->
    try
        {reductions, R} = process_info(whereis(Name), reductions),
        R
    catch
        error:badarg ->
            0
    end.

is_recoverable(Q) when ?is_amqqueue(Q) and ?amqqueue_is_quorum(Q) ->
    Node = node(),
    Nodes = get_nodes(Q),
    lists:member(Node, Nodes).

system_recover(quorum_queues) ->
    case rabbit:is_booted() of
        true ->
            Queues = rabbit_amqqueue:list_local_quorum_queues(),
            ?INFO("recovering ~b queues", [length(Queues)]),
            {Recovered, Failed} = recover(<<>>, Queues),
            ?INFO("recovered ~b queues, "
                  "failed to recover ~b queues",
                  [length(Recovered), length(Failed)]),
            ok;
        false ->
            ?INFO("rabbit not booted, skipping queue recovery", []),
            ok
    end.

maybe_apply_policies(Q, #{config := CurrentConfig}) ->
    NewPolicyConfig = gather_policy_config(Q, false),

    RelevantKeys = maps:keys(NewPolicyConfig),
    CurrentPolicyConfig = maps:with(RelevantKeys, CurrentConfig),

    ShouldUpdate = NewPolicyConfig =/= CurrentPolicyConfig,
    case ShouldUpdate of
        true ->
            rabbit_log:debug("Re-applying policies to ~ts", [rabbit_misc:rs(amqqueue:get_name(Q))]),
            policy_changed(Q),
            ok;
        false -> ok
    end.

-spec recover(binary(), [amqqueue:amqqueue()]) ->
    {[amqqueue:amqqueue()], [amqqueue:amqqueue()]}.
recover(_Vhost, Queues) ->
    lists:foldl(
      fun (Q0, {R0, F0}) ->
         {Name, _} = amqqueue:get_pid(Q0),
         ServerId = {Name, node()},
         QName = amqqueue:get_name(Q0),
         MutConf = make_mutable_config(Q0),
         Res = case ra:restart_server(?RA_SYSTEM, ServerId, MutConf) of
                   ok ->
                       % queue was restarted, good
                       ok;
                   {error, Err1}
                     when Err1 == not_started orelse
                          Err1 == name_not_registered ->
                       rabbit_log:warning("Quorum queue recovery: configured member of ~ts was not found on this node. Starting member as a new one. "
                                          "Context: ~s",
                                          [rabbit_misc:rs(QName), Err1]),
                       % queue was never started on this node
                       % so needs to be started from scratch.
                       case start_server(make_ra_conf(Q0, ServerId)) of
                           ok -> ok;
                           Err2 ->
                               rabbit_log:warning("recover: quorum queue ~w could not"
                                                  " be started ~w", [Name, Err2]),
                               fail
                       end;
                   {error, {already_started, _}} ->
                       %% this is fine and can happen if a vhost crashes and performs
                       %% recovery whilst the ra application and servers are still
                       %% running
                       ok;
                   Err ->
                       %% catch all clause to avoid causing the vhost not to start
                       rabbit_log:warning("recover: quorum queue ~w could not be "
                                          "restarted ~w", [Name, Err]),
                       fail
               end,
         %% we have to ensure the quorum queue is
         %% present in the rabbit_queue table and not just in
         %% rabbit_durable_queue
         %% So many code paths are dependent on this.
         ok = rabbit_db_queue:set_dirty(Q0),
         Q = Q0,
         case Res of
             ok ->
                 {[Q | R0], F0};
             fail ->
                 {R0, [Q | F0]}
         end
      end, {[], []}, Queues).

-spec stop(rabbit_types:vhost()) -> ok.
stop(VHost) ->
    _ = [begin
             Pid = amqqueue:get_pid(Q),
             ra:stop_server(?RA_SYSTEM, Pid)
         end || Q <- find_quorum_queues(VHost)],
    ok.

-spec stop_server({atom(), node()}) -> ok | {error, term()}.
stop_server({_, _} = Ref) ->
    ra:stop_server(?RA_SYSTEM, Ref).

-spec start_server(map()) -> ok | {error, term()}.
start_server(Conf) when is_map(Conf) ->
    ra:start_server(?RA_SYSTEM, Conf).

-spec restart_server({atom(), node()}) -> ok | {error, term()}.
restart_server({_, _} = Ref) ->
    ra:restart_server(?RA_SYSTEM, Ref).

-spec delete(amqqueue:amqqueue(),
             boolean(), boolean(),
             rabbit_types:username()) ->
    {ok, QLen :: non_neg_integer()} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
delete(Q, true, _IfEmpty, _ActingUser) when ?amqqueue_is_quorum(Q) ->
    {protocol_error, not_implemented,
     "cannot delete ~ts. queue.delete operations with if-unused flag set are not supported by quorum queues",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
delete(Q, _IfUnused, true, _ActingUser) when ?amqqueue_is_quorum(Q) ->
    {protocol_error, not_implemented,
     "cannot delete ~ts. queue.delete operations with if-empty flag set are not supported by quorum queues",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
delete(Q, _IfUnused, _IfEmpty, ActingUser) when ?amqqueue_is_quorum(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    QNodes = get_nodes(Q),
    %% TODO Quorum queue needs to support consumer tracking for IfUnused
    Timeout = ?DELETE_TIMEOUT,
    {ok, ReadyMsgs, _} = stat(Q),
    Servers = [{Name, Node} || Node <- QNodes],
    case ra:delete_cluster(Servers, Timeout) of
        {ok, {_, LeaderNode} = Leader} ->
            MRef = erlang:monitor(process, Leader),
            receive
                {'DOWN', MRef, process, _, _} ->
                    %% leader is down,
                    %% force delete remaining members
                    ok = force_delete_queue(lists:delete(Leader, Servers)),
                    ok
            after Timeout ->
                    erlang:demonitor(MRef, [flush]),
                    ok = force_delete_queue(Servers)
            end,
            notify_decorators(QName, shutdown),
            case delete_queue_data(Q, ActingUser) of
                ok ->
                    _ = erpc_call(LeaderNode, rabbit_core_metrics, queue_deleted, [QName],
                                  ?RPC_TIMEOUT),
                    {ok, ReadyMsgs};
                {error, timeout} = Err ->
                    Err
            end;
        {error, {no_more_servers_to_try, Errs}} ->
            case lists:all(fun({{error, noproc}, _}) -> true;
                              (_) -> false
                           end, Errs) of
                true ->
                    %% If all ra nodes were already down, the delete
                    %% has succeed
                    ok;
                false ->
                    %% attempt forced deletion of all servers
                    rabbit_log:warning(
                      "Could not delete quorum '~ts', not enough nodes "
                       " online to reach a quorum: ~255p."
                       " Attempting force delete.",
                      [rabbit_misc:rs(QName), Errs]),
                    ok = force_delete_queue(Servers),
                    notify_decorators(QName, shutdown)
            end,
            case delete_queue_data(Q, ActingUser) of
                ok ->
                    {ok, ReadyMsgs};
                {error, timeout} = Err ->
                    Err
            end
    end.

force_delete_queue(Servers) ->
    [begin
         case catch(ra:force_delete_server(?RA_SYSTEM, S)) of
             ok -> ok;
             Err ->
                 rabbit_log:warning(
                   "Force delete of ~w failed with: ~w"
                   "This may require manual data clean up",
                   [S, Err]),
                 ok
         end
     end || S <- Servers],
    ok.

-spec delete_queue_data(Queue, ActingUser) -> Ret when
      Queue :: amqqueue:amqqueue(),
      ActingUser :: rabbit_types:username(),
      Ret :: ok | {error, timeout}.

delete_queue_data(Queue, ActingUser) ->
    rabbit_amqqueue:internal_delete(Queue, ActingUser).


delete_immediately(Queue) ->
    _ = rabbit_amqqueue:internal_delete(Queue, ?INTERNAL_USER),
    {ok, _} = ra:delete_cluster([amqqueue:get_pid(Queue)]),
    rabbit_core_metrics:queue_deleted(amqqueue:get_name(Queue)),
    ok.

settle(_QName, complete, CTag, MsgIds, QState) ->
    rabbit_fifo_client:settle(quorum_ctag(CTag), MsgIds, QState);
settle(_QName, requeue, CTag, MsgIds, QState) ->
    rabbit_fifo_client:return(quorum_ctag(CTag), MsgIds, QState);
settle(_QName, discard, CTag, MsgIds, QState) ->
    rabbit_fifo_client:discard(quorum_ctag(CTag), MsgIds, QState);
settle(_QName, {modify, DelFailed, Undel, Anns}, CTag, MsgIds, QState) ->
    rabbit_fifo_client:modify(quorum_ctag(CTag), MsgIds, DelFailed, Undel,
                              Anns, QState).

credit_v1(_QName, CTag, Credit, Drain, QState) ->
    rabbit_fifo_client:credit_v1(quorum_ctag(CTag), Credit, Drain, QState).

credit(_QName, CTag, DeliveryCount, Credit, Drain, QState) ->
    rabbit_fifo_client:credit(quorum_ctag(CTag), DeliveryCount, Credit, Drain, QState).

-spec dequeue(rabbit_amqqueue:name(), NoAck :: boolean(), pid(),
              rabbit_types:ctag(), rabbit_fifo_client:state()) ->
    {empty, rabbit_fifo_client:state()} |
    {ok, QLen :: non_neg_integer(), qmsg(), rabbit_fifo_client:state()} |
    {error, term()}.
dequeue(QName, NoAck, _LimiterPid, CTag0, QState0) ->
    CTag = quorum_ctag(CTag0),
    Settlement = case NoAck of
                     true ->
                         settled;
                     false ->
                         unsettled
                 end,
    rabbit_fifo_client:dequeue(QName, CTag, Settlement, QState0).

-spec consume(amqqueue:amqqueue(),
              rabbit_queue_type:consume_spec(),
              rabbit_fifo_client:state()) ->
    {ok, rabbit_fifo_client:state(), rabbit_queue_type:actions()} |
    {error, atom(), Format :: string(), FormatArgs :: [term()]}.
consume(Q, #{limiter_active := true}, _State)
  when ?amqqueue_is_quorum(Q) ->
    {error, not_implemented,
     "~ts does not support global qos",
     [rabbit_misc:rs(amqqueue:get_name(Q))]};
consume(Q, Spec, QState0) when ?amqqueue_is_quorum(Q) ->
    #{no_ack := NoAck,
      channel_pid := ChPid,
      mode := Mode,
      consumer_tag := ConsumerTag0,
      exclusive_consume := ExclusiveConsume,
      args := Args,
      ok_msg := OkMsg,
      acting_user := ActingUser} = Spec,
    %% TODO: validate consumer arguments
    %% currently quorum queues do not support any arguments
    QName = amqqueue:get_name(Q),
    maybe_send_reply(ChPid, OkMsg),
    ConsumerTag = quorum_ctag(ConsumerTag0),
    %% consumer info is used to describe the consumer properties
    AckRequired = not NoAck,
    Prefetch = case Mode of
                   {simple_prefetch, Declared} ->
                       Declared;
                   _ ->
                       0
               end,
    Priority = case rabbit_misc:table_lookup(Args, <<"x-priority">>) of
                   {_Key, Value} ->
                       Value;
                   _ ->
                       0
               end,
    ConsumerMeta = #{ack => AckRequired,
                     prefetch => Prefetch,
                     args => Args,
                     username => ActingUser,
                     priority => Priority},
    case rabbit_fifo_client:checkout(ConsumerTag, Mode, ConsumerMeta, QState0) of
        {ok, _Infos, QState} ->
            case single_active_consumer_on(Q) of
                true ->
                    %% get the leader from state
                    case rabbit_fifo_client:query_single_active_consumer(QState) of
                        {ok, SacResult} ->
                            ActivityStatus = case SacResult of
                                                 {value, {ConsumerTag, ChPid}} ->
                                                     single_active;
                                                 _ ->
                                                     waiting
                                             end,
                            rabbit_core_metrics:consumer_created(ChPid, ConsumerTag,
                                                                 ExclusiveConsume,
                                                                 AckRequired, QName,
                                                                 Prefetch,
                                                                 ActivityStatus == single_active,
                                                                 ActivityStatus, Args),
                            emit_consumer_created(ChPid, ConsumerTag,
                                                  ExclusiveConsume,
                                                  AckRequired, QName,
                                                  Prefetch, Args, none,
                                                  ActingUser),
                            {ok, QState};
                        Err ->
                            consume_error(Err, QName)
                    end;
                false ->
                    rabbit_core_metrics:consumer_created(ChPid, ConsumerTag,
                                                         ExclusiveConsume,
                                                         AckRequired, QName,
                                                         Prefetch, true,
                                                         up, Args),
                    emit_consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                          AckRequired, QName, Prefetch,
                                          Args, none, ActingUser),
                    {ok, QState}
            end;
        Err ->
            consume_error(Err, QName)
    end.

consume_error({error, Reason}, QName) ->
    {error, internal_error,
     "failed consuming from quorum ~ts: ~tp",
     [rabbit_misc:rs(QName), Reason]};
consume_error({timeout, RaServerId}, QName) ->
    {error, internal_error,
     "timed out consuming from quorum ~ts: ~tp",
     [rabbit_misc:rs(QName), RaServerId]}.

cancel(_Q, #{consumer_tag := ConsumerTag} = Spec, State) ->
    maybe_send_reply(self(), maps:get(ok_msg, Spec, undefined)),
    Reason = maps:get(reason, Spec, cancel),
    rabbit_fifo_client:cancel_checkout(quorum_ctag(ConsumerTag), Reason, State).

emit_consumer_created(ChPid, CTag, Exclusive, AckRequired, QName, PrefetchCount, Args, Ref, ActingUser) ->
    rabbit_event:notify(consumer_created,
                        [{consumer_tag,   CTag},
                         {exclusive,      Exclusive},
                         {ack_required,   AckRequired},
                         {channel,        ChPid},
                         {queue,          QName},
                         {prefetch_count, PrefetchCount},
                         {arguments,      Args},
                         {user_who_performed_action, ActingUser}],
                        Ref).

emit_consumer_deleted(ChPid, ConsumerTag, QName, ActingUser) ->
    rabbit_event:notify(consumer_deleted,
        [{consumer_tag, ConsumerTag},
            {channel, ChPid},
            {queue, QName},
            {user_who_performed_action, ActingUser}]).

deliver0(QName, undefined, Msg, QState0) ->
    case rabbit_fifo_client:enqueue(QName, Msg, QState0) of
        {ok, _, _} = Res -> Res;
        {reject_publish, State} ->
            {ok, State, []}
    end;
deliver0(QName, Correlation, Msg, QState0) ->
    rabbit_fifo_client:enqueue(QName, Correlation,
                               Msg, QState0).

deliver(QSs, Msg0, Options) ->
    Correlation = maps:get(correlation, Options, undefined),
    Msg = mc:prepare(store, Msg0),
    lists:foldl(
      fun({Q, stateless}, {Qs, Actions}) ->
              QRef = amqqueue:get_pid(Q),
              ok = rabbit_fifo_client:untracked_enqueue([QRef], Msg),
              {Qs, Actions};
         ({Q, S0}, {Qs, Actions}) ->
              QName = amqqueue:get_name(Q),
              case deliver0(QName, Correlation, Msg, S0) of
                  {reject_publish, S} ->
                      {[{Q, S} | Qs],
                       [{rejected, QName, [Correlation]} | Actions]};
                  {ok, S, As} ->
                      {[{Q, S} | Qs], As ++ Actions}
              end
      end, {[], []}, QSs).


state_info(S) ->
    #{pending_raft_commands => rabbit_fifo_client:pending_size(S),
      cached_segments => rabbit_fifo_client:num_cached_segments(S)}.

-spec infos(rabbit_types:r('queue')) -> rabbit_types:infos().
infos(QName) ->
    infos(QName, ?STATISTICS_KEYS).

infos(QName, Keys) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            info(Q, Keys);
        {error, not_found} ->
            []
    end.

info(Q, all_keys) ->
    info(Q, ?INFO_KEYS);
info(Q, Items) ->
    lists:foldr(fun(totals, Acc) ->
                        i_totals(Q) ++ Acc;
                   (type_specific, Acc) ->
                        format(Q, #{}) ++ Acc;
                   (Item, Acc) ->
                        [{Item, i(Item, Q)} | Acc]
                end, [], Items).

-spec stat(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer(), non_neg_integer()}.
stat(Q) when ?is_amqqueue(Q) ->
    %% same short default timeout as in rabbit_fifo_client:stat/1
    stat(Q, 250).

-spec stat(amqqueue:amqqueue(), non_neg_integer()) -> {'ok', non_neg_integer(), non_neg_integer()}.

stat(Q, Timeout) when ?is_amqqueue(Q) ->
    Leader = amqqueue:get_pid(Q),
    try
        case rabbit_fifo_client:stat(Leader, Timeout) of
          {ok, _, _} = Success -> Success;
          {error, _}           -> {ok, 0, 0};
          {timeout, _}         -> {ok, 0, 0}
        end
    catch
        _:_ ->
            %% Leader is not available, cluster might be in minority
            {ok, 0, 0}
    end.

-spec purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()}.
purge(Q) when ?is_amqqueue(Q) ->
    Server = amqqueue:get_pid(Q),
    rabbit_fifo_client:purge(Server).

requeue(ConsumerTag, MsgIds, QState) ->
    rabbit_fifo_client:return(quorum_ctag(ConsumerTag), MsgIds, QState).

cleanup_data_dir() ->
    Names = [begin
                 {Name, _} = amqqueue:get_pid(Q),
                 Name
             end
             || Q <- rabbit_amqqueue:list_by_type(?MODULE),
                lists:member(node(), get_nodes(Q))],
    Registered = ra_directory:list_registered(?RA_SYSTEM),
    Running = Names,
    _ = [maybe_delete_data_dir(UId) || {Name, UId} <- Registered,
                                       not lists:member(Name, Running)],
    ok.

maybe_delete_data_dir(UId) ->
    _ = ra_directory:unregister_name(?RA_SYSTEM, UId),
    Dir = ra_env:server_data_dir(?RA_SYSTEM, UId),
    {ok, Config} = ra_log:read_config(Dir),
    case maps:get(machine, Config) of
        {module, rabbit_fifo, _} ->
            ra_lib:recursive_delete(Dir);
        _ ->
            ok
    end.

policy_changed(Q) ->
    QPid = amqqueue:get_pid(Q),
    case rabbit_fifo_client:update_machine_state(QPid, ra_machine_config(Q)) of
        ok ->
            ok;
        Err ->
            FormattedQueueName = rabbit_misc:rs(amqqueue:get_name(Q)),
            rabbit_log:warning("~s: policy may not have been successfully applied. Error: ~p",
                               [FormattedQueueName, Err]),
            ok
    end.

-spec cluster_state(Name :: atom()) -> 'down' | 'recovering' | 'running'.

cluster_state(Name) ->
    case whereis(Name) of
        undefined -> down;
        _ ->
            case ets:lookup_element(ra_state, Name, 2, undefined) of
                recover ->
                    recovering;
                _ ->
                    running
            end
    end.

key_metrics_rpc(ServerId) ->
    Metrics = ra:key_metrics(ServerId),
    Metrics#{machine_version => rabbit_fifo:version()}.

-spec status(rabbit_types:vhost(), Name :: rabbit_misc:resource_name()) ->
    [[{binary(), term()}]] | {error, term()}.
status(Vhost, QueueName) ->
    %% Handle not found queues
    QName = #resource{virtual_host = Vhost, name = QueueName, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {RName, _} = amqqueue:get_pid(Q),
            Nodes = lists:sort(get_nodes(Q)),
            [begin
                 ServerId = {RName, N},
                 case erpc_call(N, ?MODULE, key_metrics_rpc, [ServerId], ?RPC_TIMEOUT) of
                     #{state := RaftState,
                       membership := Membership,
                       commit_index := Commit,
                       term := Term,
                       last_index := Last,
                       last_applied := LastApplied,
                       last_written_index := LastWritten,
                       snapshot_index := SnapIdx,
                       machine_version := MacVer} ->
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, RaftState},
                          {<<"Membership">>, Membership},
                          {<<"Last Log Index">>, Last},
                          {<<"Last Written">>, LastWritten},
                          {<<"Last Applied">>, LastApplied},
                          {<<"Commit Index">>, Commit},
                          {<<"Snapshot Index">>, SnapIdx},
                          {<<"Term">>, Term},
                          {<<"Machine Version">>, MacVer}
                         ];
                     {error, _} ->
                         %% try the old method
                         case get_sys_status(ServerId) of
                             {ok, Sys} ->
                                 {_, M} = lists:keyfind(ra_server_state, 1, Sys),
                                 {_, RaftState} = lists:keyfind(raft_state, 1, Sys),
                                 #{commit_index := Commit,
                                   machine_version := MacVer,
                                   current_term := Term,
                                   last_applied := LastApplied,
                                   log := #{last_index := Last,
                                            last_written_index_term := {LastWritten, _},
                                            snapshot_index := SnapIdx}} = M,
                                 [{<<"Node Name">>, N},
                                  {<<"Raft State">>, RaftState},
                                  {<<"Membership">>, voter},
                                  {<<"Last Log Index">>, Last},
                                  {<<"Last Written">>, LastWritten},
                                  {<<"Last Applied">>, LastApplied},
                                  {<<"Commit Index">>, Commit},
                                  {<<"Snapshot Index">>, SnapIdx},
                                  {<<"Term">>, Term},
                                  {<<"Machine Version">>, MacVer}
                                 ];
                             {error, Err} ->
                                 [{<<"Node Name">>, N},
                                  {<<"Raft State">>, Err},
                                  {<<"Membership">>, <<>>},
                                  {<<"LastLog Index">>, <<>>},
                                  {<<"Last Written">>, <<>>},
                                  {<<"Last Applied">>, <<>>},
                                  {<<"Commit Index">>, <<>>},
                                  {<<"Snapshot Index">>, <<>>},
                                  {<<"Term">>, <<>>},
                                  {<<"Machine Version">>, <<>>}
                                 ]
                         end
                 end
             end || N <- Nodes];
        {ok, _Q} ->
            {error, not_quorum_queue};
        {error, not_found} = E ->
            E
    end.

get_sys_status(Proc) ->
    try lists:nth(5, element(4, sys:get_status(Proc))) of
        Sys -> {ok, Sys}
    catch
        _:Err when is_tuple(Err) ->
            {error, element(1, Err)};
        _:_ ->
            {error, other}

    end.

add_member(VHost, Name, Node, Membership, Timeout)
  when is_binary(VHost) andalso
       is_binary(Name) andalso
       is_atom(Node) ->
    QName = #resource{virtual_host = VHost, name = Name, kind = queue},
    rabbit_log:debug("Asked to add a replica for queue ~ts on node ~ts",
                     [rabbit_misc:rs(QName), Node]),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            QNodes = get_nodes(Q),
            case lists:member(Node, rabbit_nodes:list_running()) of
                false ->
                    {error, node_not_running};
                true ->
                    case lists:member(Node, QNodes) of
                        true ->
                          %% idempotent by design
                          rabbit_log:debug("Quorum ~ts already has a replica on node ~ts",
                                           [rabbit_misc:rs(QName), Node]),
                          ok;
                        false ->
                            do_add_member(Q, Node, Membership, Timeout)
                    end
            end;
        {ok, _Q} ->
            {error, not_quorum_queue};
        {error, not_found} = E ->
                    E
    end.

add_member(VHost, Name, Node, Timeout) when is_binary(VHost) ->
    %% NOTE needed to pass mixed cluster tests.
    add_member(VHost, Name, Node, promotable, Timeout).

add_member(Q, Node) ->
    do_add_member(Q, Node, promotable, ?MEMBER_CHANGE_TIMEOUT).

add_member(Q, Node, Membership) ->
    do_add_member(Q, Node, Membership, ?MEMBER_CHANGE_TIMEOUT).


do_add_member(Q, Node, Membership, Timeout)
  when ?is_amqqueue(Q) andalso
       ?amqqueue_is_quorum(Q) andalso
       is_atom(Node) ->
    {RaName, _} = amqqueue:get_pid(Q),
    QName = amqqueue:get_name(Q),
    %% TODO parallel calls might crash this, or add a duplicate in quorum_nodes
    ServerId = {RaName, Node},
    Members = members(Q),

    MachineVersion = erpc_call(Node, rabbit_fifo, version, [], infinity),
    Conf = make_ra_conf(Q, ServerId, Membership, MachineVersion),
    case ra:start_server(?RA_SYSTEM, Conf) of
        ok ->
            ServerIdSpec  =
            case rabbit_feature_flags:is_enabled(quorum_queue_non_voters) of
                true ->
                    maps:with([id, uid, membership], Conf);
                false ->
                    maps:get(id, Conf)
            end,
            case ra:add_member(Members, ServerIdSpec, Timeout) of
                {ok, {RaIndex, RaTerm}, Leader} ->
                    Fun = fun(Q1) ->
                                  Q2 = update_type_state(
                                         Q1, fun(#{nodes := Nodes} = Ts) ->
                                                     Ts#{nodes => lists:usort([Node | Nodes])}
                                             end),
                                  amqqueue:set_pid(Q2, Leader)
                          end,
                    %% The `ra:member_add/3` call above returns before the
                    %% change is committed. This is ok for that addition but
                    %% any follow-up changes to the cluster might be rejected
                    %% with the `cluster_change_not_permitted` error.
                    %%
                    %% Instead of changing other places to wait or retry their
                    %% cluster membership change, we wait for the current add
                    %% to be applied using a conditional leader query before
                    %% proceeding and returning.
                    {ok, _, _} = ra:leader_query(
                                   Leader,
                                   {erlang, is_list, []},
                                   #{condition => {applied, {RaIndex, RaTerm}}}),
                    _ = rabbit_amqqueue:update(QName, Fun),
                    rabbit_log:info("Added a replica of quorum ~ts on node ~ts", [rabbit_misc:rs(QName), Node]),
                    ok;
                {timeout, _} ->
                    _ = ra:force_delete_server(?RA_SYSTEM, ServerId),
                    _ = ra:remove_member(Members, ServerId),
                    {error, timeout};
                E ->
                    _ = ra:force_delete_server(?RA_SYSTEM, ServerId),
                    E
            end;
        E ->
            rabbit_log:warning("Could not add a replica of quorum ~ts on node ~ts: ~p",
                               [rabbit_misc:rs(QName), Node, E]),
            E
    end.

delete_member(VHost, Name, Node) ->
    QName = #resource{virtual_host = VHost, name = Name, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            QNodes = get_nodes(Q),
            case lists:member(Node, QNodes) of
                false ->
                    %% idempotent by design
                    ok;
                true ->
                    delete_member(Q, Node)
            end;
        {ok, _Q} ->
            {error, not_quorum_queue};
        {error, not_found} = E ->
                    E
    end.


delete_member(Q, Node) when ?amqqueue_is_quorum(Q) ->
    QName = amqqueue:get_name(Q),
    {RaName, _} = amqqueue:get_pid(Q),
    ServerId = {RaName, Node},
    case members(Q) of
        [{_, Node}] ->

            %% deleting the last member is not allowed
            {error, last_node};
        Members ->
            case ra:remove_member(Members, ServerId, ?MEMBER_CHANGE_TIMEOUT) of
                Res when element(1, Res) == ok orelse
                         Res == {error, not_member} ->
                    %% if not a member we can still proceed with updating the
                    %% mnesia record and clean up server if still running
                    Fun = fun(Q1) ->
                                  update_type_state(
                                    Q1,
                                    fun(#{nodes := Nodes} = Ts) ->
                                            Ts#{nodes => lists:delete(Node, Nodes)}
                                    end)
                          end,
                    _ = rabbit_amqqueue:update(QName, Fun),
                    case ra:force_delete_server(?RA_SYSTEM, ServerId) of
                        ok ->
                            rabbit_log:info("Deleted a replica of quorum ~ts on node ~ts", [rabbit_misc:rs(QName), Node]),
                            ok;
                        {error, {badrpc, nodedown}} ->
                            ok;
                        {error, {badrpc, {'EXIT', {badarg, _}}}} ->
                            %% DETS/ETS tables can't be found, application isn't running
                            ok;
                        {error, _} = Err ->
                            Err;
                        Err ->
                            {error, Err}
                    end;
                {timeout, _} ->
                    {error, timeout};
                E ->
                    E
            end
    end.

-spec shrink_all(node()) ->
    [{rabbit_amqqueue:name(),
      {ok, pos_integer()} | {error, pos_integer(), term()}}].
shrink_all(Node) ->
    rabbit_log:info("Asked to remove all quorum queue replicas from node ~ts", [Node]),
    [begin
         QName = amqqueue:get_name(Q),
         rabbit_log:info("~ts: removing member (replica) on node ~w",
                         [rabbit_misc:rs(QName), Node]),
         Size = length(get_nodes(Q)),
         case delete_member(Q, Node) of
             ok ->
                 {QName, {ok, Size-1}};
             {error, cluster_change_not_permitted} ->
                 %% this could be timing related and due to a new leader just being
                 %% elected but it's noop command not been committed yet.
                 %% lets sleep and retry once
                 rabbit_log:info("~ts: failed to remove member (replica) on node ~w "
                                 "as cluster change is not permitted. "
                                 "retrying once in 500ms",
                                 [rabbit_misc:rs(QName), Node]),
                 timer:sleep(500),
                 case delete_member(Q, Node) of
                     ok ->
                         {QName, {ok, Size-1}};
                     {error, Err} ->
                         rabbit_log:warning("~ts: failed to remove member (replica) on node ~w, error: ~w",
                                            [rabbit_misc:rs(QName), Node, Err]),
                         {QName, {error, Size, Err}}
                 end;
             {error, Err} ->
                 rabbit_log:warning("~ts: failed to remove member (replica) on node ~w, error: ~w",
                                    [rabbit_misc:rs(QName), Node, Err]),
                 {QName, {error, Size, Err}}
         end
     end || Q <- rabbit_amqqueue:list(),
            amqqueue:get_type(Q) == ?MODULE,
            lists:member(Node, get_nodes(Q))].


grow(Node, VhostSpec, QueueSpec, Strategy) ->
    grow(Node, VhostSpec, QueueSpec, Strategy, promotable).

-spec grow(node(), binary(), binary(), all | even, membership()) ->
    [{rabbit_amqqueue:name(),
      {ok, pos_integer()} | {error, pos_integer(), term()}}].
grow(Node, VhostSpec, QueueSpec, Strategy, Membership) ->
    Running = rabbit_nodes:list_running(),
    [begin
         Size = length(get_nodes(Q)),
         QName = amqqueue:get_name(Q),
         rabbit_log:info("~ts: adding a new member (replica) on node ~w",
                         [rabbit_misc:rs(QName), Node]),
         case add_member(Q, Node, Membership) of
             ok ->
                 {QName, {ok, Size + 1}};
             {error, Err} ->
                 rabbit_log:warning(
                   "~ts: failed to add member (replica) on node ~w, error: ~w",
                   [rabbit_misc:rs(QName), Node, Err]),
                 {QName, {error, Size, Err}}
         end
     end
     || Q <- rabbit_amqqueue:list(),
        amqqueue:get_type(Q) == ?MODULE,
        %% don't add a member if there is already one on the node
        not lists:member(Node, get_nodes(Q)),
        %% node needs to be running
        lists:member(Node, Running),
        matches_strategy(Strategy, get_nodes(Q)),
        is_match(amqqueue:get_vhost(Q), VhostSpec) andalso
        is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec) ].

-spec transfer_leadership(amqqueue:amqqueue(), node()) -> {migrated, node()} | {not_migrated, atom()}.
transfer_leadership(Q, Destination) ->
    {RaName, _} = Pid = amqqueue:get_pid(Q),
    case ra:transfer_leadership(Pid, {RaName, Destination}) of
        ok ->
          case ra:members(Pid) of
            {_, _, {_, NewNode}} ->
              {migrated, NewNode};
            {timeout, _} ->
              {not_migrated, ra_members_timeout}
          end;
        already_leader ->
            {not_migrated, already_leader};
        {error, Reason} ->
            {not_migrated, Reason};
        {timeout, _} ->
            %% TODO should we retry once?
            {not_migrated, timeout}
    end.

queue_length(Q) ->
    Name = amqqueue:get_name(Q),
    case ets:lookup(ra_metrics, Name) of
        [] -> 0;
        [{_, _, SnapIdx, _, _, LastIdx, _}] ->
            LastIdx - SnapIdx
    end.

get_replicas(Q) ->
    get_nodes(Q).

get_resource_name(#resource{name  = Name}) ->
    Name.

matches_strategy(all, _) -> true;
matches_strategy(even, Members) ->
    length(Members) rem 2 == 0.

is_match(Subj, E) ->
   nomatch /= re:run(Subj, E).

-spec reclaim_memory(rabbit_types:vhost(), Name :: rabbit_misc:resource_name()) -> ok | {error, term()}.
reclaim_memory(Vhost, QueueName) ->
    QName = #resource{virtual_host = Vhost, name = QueueName, kind = queue},
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            ok = ra:pipeline_command(amqqueue:get_pid(Q),
                                     rabbit_fifo:make_garbage_collection());
        {ok, _Q} ->
            {error, not_quorum_queue};
        {error, not_found} = E ->
            E
    end.

-spec wal_force_roll_over(node()) -> ok.
 wal_force_roll_over(Node) ->
    ra_log_wal:force_roll_over({?RA_WAL_NAME, Node}).

%%----------------------------------------------------------------------------
dead_letter_handler(Q, Overflow) ->
    Exchange = args_policy_lookup(<<"dead-letter-exchange">>, fun queue_arg_has_precedence/2, Q),
    RoutingKey = args_policy_lookup(<<"dead-letter-routing-key">>, fun queue_arg_has_precedence/2, Q),
    Strategy = args_policy_lookup(<<"dead-letter-strategy">>, fun queue_arg_has_precedence/2, Q),
    QName = amqqueue:get_name(Q),
    dlh(Exchange, RoutingKey, Strategy, Overflow, QName).

dlh(undefined, undefined, undefined, _, _) ->
    undefined;
dlh(undefined, RoutingKey, undefined, _, QName) ->
    rabbit_log:warning("Disabling dead-lettering for ~ts despite configured dead-letter-routing-key '~ts' "
                       "because dead-letter-exchange is not configured.",
                       [rabbit_misc:rs(QName), RoutingKey]),
    undefined;
dlh(undefined, _, Strategy, _, QName) ->
    rabbit_log:warning("Disabling dead-lettering for ~ts despite configured dead-letter-strategy '~ts' "
                       "because dead-letter-exchange is not configured.",
                       [rabbit_misc:rs(QName), Strategy]),
    undefined;
dlh(_, _, <<"at-least-once">>, reject_publish, _) ->
    at_least_once;
dlh(Exchange, RoutingKey, <<"at-least-once">>, drop_head, QName) ->
    rabbit_log:warning("Falling back to dead-letter-strategy at-most-once for ~ts "
                       "because configured dead-letter-strategy at-least-once is incompatible with "
                       "effective overflow strategy drop-head. To enable dead-letter-strategy "
                       "at-least-once, set overflow strategy to reject-publish.",
                       [rabbit_misc:rs(QName)]),
    dlh_at_most_once(Exchange, RoutingKey, QName);
dlh(Exchange, RoutingKey, _, _, QName) ->
    dlh_at_most_once(Exchange, RoutingKey, QName).

dlh_at_most_once(Exchange, RoutingKey, QName) ->
    DLX = rabbit_misc:r(QName, exchange, Exchange),
    MFA = {?MODULE, dead_letter_publish, [DLX, RoutingKey, QName]},
    {at_most_once, MFA}.

dead_letter_publish(X, RK, QName, Reason, Msgs) ->
    case rabbit_exchange:lookup(X) of
        {ok, Exchange} ->
            lists:foreach(fun(Msg) ->
                                  rabbit_dead_letter:publish(Msg, Reason, Exchange, RK, QName)
                          end, Msgs),
            rabbit_global_counters:messages_dead_lettered(Reason, ?MODULE, at_most_once, length(Msgs));
        {error, not_found} ->
            %% Even though dead-letter-strategy is at_most_once,
            %% when configured dead-letter-exchange does not exist,
            %% we increment counter for dead-letter-strategy 'disabled' because
            %% 1. we know for certain that the message won't be delivered, and
            %% 2. that's in line with classic queue behaviour
            rabbit_global_counters:messages_dead_lettered(Reason, ?MODULE, disabled, length(Msgs))
    end.

find_quorum_queues(VHost) ->
    Node = node(),
    rabbit_db_queue:get_all_by_type_and_node(VHost, rabbit_quorum_queue, Node).

i_totals(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    case ets:lookup(queue_coarse_metrics, QName) of
        [{_, MR, MU, M, _}] ->
            [{messages_ready, MR},
             {messages_unacknowledged, MU},
             {messages, M}];
        [] ->
            [{messages_ready, 0},
             {messages_unacknowledged, 0},
             {messages, 0}]
    end.

i(name,        Q) when ?is_amqqueue(Q) -> amqqueue:get_name(Q);
i(durable,     Q) when ?is_amqqueue(Q) -> amqqueue:is_durable(Q);
i(auto_delete, Q) when ?is_amqqueue(Q) -> amqqueue:is_auto_delete(Q);
i(arguments,   Q) when ?is_amqqueue(Q) -> amqqueue:get_arguments(Q);
i(pid, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    whereis(Name);
i(messages, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    quorum_messages(QName);
i(messages_ready, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    ets:lookup_element(queue_coarse_metrics, QName, 2, 0);
i(messages_unacknowledged, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    ets:lookup_element(queue_coarse_metrics, QName, 3, 0);
i(policy, Q) ->
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(operator_policy, Q) ->
    case rabbit_policy:name_op(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(effective_policy_definition, Q) ->
    case rabbit_policy:effective_definition(Q) of
        undefined -> [];
        Def       -> Def
    end;
i(consumers, Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    Consumers = ets:lookup_element(queue_metrics, QName, 2, []),
    proplists:get_value(consumers, Consumers, 0);
i(memory, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    try
        {memory, M} = process_info(whereis(Name), memory),
        M
    catch
        error:badarg ->
            0
    end;
i(state, Q) when ?is_amqqueue(Q) ->
    {Name, Node} = amqqueue:get_pid(Q),
    %% Check against the leader or last known leader
    case erpc_call(Node, ?MODULE, cluster_state, [Name], ?RPC_TIMEOUT) of
        {error, _} ->
            down;
        State ->
            State
    end;
i(local_state, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    ets:lookup_element(ra_state, Name, 2, not_member);
i(garbage_collection, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    try
        rabbit_misc:get_gc_info(whereis(Name))
    catch
        error:badarg ->
            []
    end;
i(members, Q) when ?is_amqqueue(Q) ->
    get_nodes(Q);
i(online, Q) -> online(Q);
i(leader, Q) -> leader(Q);
i(open_files, Q) when ?is_amqqueue(Q) ->
    {Name, _} = amqqueue:get_pid(Q),
    Nodes = get_connected_nodes(Q),
    [Info || {ok, {_, _} = Info} <-
             erpc:multicall(Nodes, ?MODULE, open_files,
                            [Name], ?RPC_TIMEOUT)];
i(single_active_consumer_pid, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid, fun rabbit_fifo:query_single_active_consumer/1) of
        {ok, {_, {value, {_ConsumerTag, ChPid}}}, _} ->
            ChPid;
        {ok, _, _} ->
            '';
        {error, _} ->
            '';
        {timeout, _} ->
            ''
    end;
i(single_active_consumer_tag, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid,
                        fun rabbit_fifo:query_single_active_consumer/1) of
        {ok, {_, {value, {ConsumerTag, _ChPid}}}, _} ->
            ConsumerTag;
        {ok, _, _} ->
            '';
        {error, _} ->
            '';
        {timeout, _} ->
            ''
    end;
i(type, _) -> quorum;
i(messages_ram, Q) when ?is_amqqueue(Q) ->
    0;
i(message_bytes_ram, Q) when ?is_amqqueue(Q) ->
    0;
i(messages_dlx, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid,
                        fun rabbit_fifo:query_stat_dlx/1) of
        {ok, {_, {Num, _}}, _} ->
            Num;
        {error, _} ->
            0;
        {timeout, _} ->
            0
    end;
i(message_bytes_dlx, Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid,
                        fun rabbit_fifo:query_stat_dlx/1) of
        {ok, {_, {_, Bytes}}, _} ->
            Bytes;
        {error, _} ->
            0;
        {timeout, _} ->
            0
    end;
i(_K, _Q) -> ''.

open_files(Name) ->
    case whereis(Name) of
        undefined ->
            {node(), 0};
        _ ->
            case ra_counters:counters({Name, node()}, [open_segments]) of
                #{open_segments := Num} ->
                    {node(), Num};
                _ ->
                    {node(), 0}
            end
    end.

leader(Q) when ?is_amqqueue(Q) ->
    case find_leader(Q) of
        undefined ->
            '';
        {Name, LeaderNode} ->
            case is_process_alive(Name, LeaderNode) of
                true ->
                    LeaderNode;
                false ->
                    ''
            end
    end.

peek(Vhost, Queue, Pos) ->
    peek(Pos, rabbit_misc:r(Vhost, queue, Queue)).

peek(Pos, #resource{} = QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            peek(Pos, Q);
        Err ->
            Err
    end;
peek(Pos, Q) when ?is_amqqueue(Q) andalso ?amqqueue_is_quorum(Q) ->
    LeaderPid = amqqueue:get_pid(Q),
    case ra:aux_command(LeaderPid, {peek, Pos}) of
        {ok, {MsgHeader, Msg0}} ->
            Count = case MsgHeader of
                        #{delivery_count := C} -> C;
                       _ -> 0
                    end,
            Msg = mc:set_annotation(<<"x-delivery-count">>, Count, Msg0),
            XName = mc:exchange(Msg),
            RoutingKeys = mc:routing_keys(Msg),
            AmqpLegacyMsg = mc:prepare(read, mc:convert(mc_amqpl, Msg)),
            Content = mc:protocol_state(AmqpLegacyMsg),
            {ok, rabbit_basic:peek_fmt_message(XName, RoutingKeys, Content)};
        {error, Err} ->
            {error, Err};
        Err ->
            Err
    end;
peek(_Pos, Q) when ?is_amqqueue(Q) andalso ?amqqueue_is_classic(Q) ->
    {error, classic_queue_not_supported};
peek(_Pos, Q) when ?is_amqqueue(Q) ->
    {error, not_quorum_queue}.

online(Q) when ?is_amqqueue(Q) ->
    Nodes = get_connected_nodes(Q),
    {Name, _} = amqqueue:get_pid(Q),
    [node(Pid) || {ok, Pid} <-
                  erpc:multicall(Nodes, erlang, whereis,
                                 [Name], ?RPC_TIMEOUT),
                  is_pid(Pid)].

format(Q, Ctx) when ?is_amqqueue(Q) ->
    %% TODO: this should really just be voters
    Nodes = lists:sort(get_nodes(Q)),
    Running = case Ctx of
                  #{running_nodes := Running0} ->
                      Running0;
                  _ ->
                      %% WARN: slow
                      rabbit_nodes:list_running()
              end,
    Online = [N || N <- Nodes, lists:member(N, Running)],
    {_, LeaderNode} = amqqueue:get_pid(Q),
    State = case is_minority(Nodes, Online) of
                true when length(Online) == 0 ->
                    down;
                true ->
                    minority;
                false ->
                    case lists:member(LeaderNode, Online) of
                        true ->
                            running;
                        false ->
                            down
                    end
            end,
    [{type, rabbit_queue_type:short_alias_of(?MODULE)},
     {state, State},
     {node, LeaderNode},
     {members, Nodes},
     {leader, LeaderNode},
     {online, Online}].

-spec quorum_messages(rabbit_amqqueue:name()) -> non_neg_integer().

quorum_messages(QName) ->
    ets:lookup_element(queue_coarse_metrics, QName, 4, 0).

quorum_ctag(Int) when is_integer(Int) ->
    integer_to_binary(Int);
quorum_ctag(Other) ->
    Other.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

get_default_quorum_initial_group_size(Arguments) ->
    case rabbit_misc:table_lookup(Arguments, <<"x-quorum-initial-group-size">>) of
        undefined ->
            application:get_env(rabbit, quorum_cluster_size, 3);
        {_Type, Val} ->
            Val
    end.

%% member with the current leader first
members(Q) when ?amqqueue_is_quorum(Q) ->
    {RaName, LeaderNode} = amqqueue:get_pid(Q),
    Nodes = lists:delete(LeaderNode, get_nodes(Q)),
    [{RaName, N} || N <- [LeaderNode | Nodes]].

format_ra_event(ServerId, Evt, QRef) ->
    {'$gen_cast', {queue_event, QRef, {ServerId, Evt}}}.

make_ra_conf(Q, ServerId) ->
    make_ra_conf(Q, ServerId, voter, rabbit_fifo:version()).

make_ra_conf(Q, ServerId, Membership, MacVersion)
  when is_integer(MacVersion) ->
    TickTimeout = application:get_env(rabbit, quorum_tick_interval,
                                      ?TICK_INTERVAL),
    SnapshotInterval = application:get_env(rabbit, quorum_snapshot_interval,
                                           ?SNAPSHOT_INTERVAL),
    CheckpointInterval = application:get_env(rabbit,
                                             quorum_min_checkpoint_interval,
                                             ?MIN_CHECKPOINT_INTERVAL),
    make_ra_conf(Q, ServerId, TickTimeout,
                 SnapshotInterval, CheckpointInterval,
                 Membership, MacVersion).

make_ra_conf(Q, ServerId, TickTimeout,
             SnapshotInterval, CheckpointInterval,
             Membership, MacVersion) ->
    QName = amqqueue:get_name(Q),
    RaMachine = ra_machine(Q),
    [{ClusterName, _} | _] = Members = members(Q),
    UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
    FName = rabbit_misc:rs(QName),
    Formatter = {?MODULE, format_ra_event, [QName]},
    LogCfg = #{uid => UId,
               snapshot_interval => SnapshotInterval,
               min_checkpoint_interval => CheckpointInterval,
               max_checkpoints => 3},
    rabbit_misc:maps_put_truthy(membership, Membership,
                                #{cluster_name => ClusterName,
                                  id => ServerId,
                                  uid => UId,
                                  friendly_name => FName,
                                  metrics_key => QName,
                                  initial_members => Members,
                                  log_init_args => LogCfg,
                                  tick_timeout => TickTimeout,
                                  machine => RaMachine,
                                  initial_machine_version => MacVersion,
                                  ra_event_formatter => Formatter}).

make_mutable_config(Q) ->
    QName = amqqueue:get_name(Q),
    TickTimeout = application:get_env(rabbit, quorum_tick_interval,
                                      ?TICK_INTERVAL),
    Formatter = {?MODULE, format_ra_event, [QName]},
    #{tick_timeout => TickTimeout,
      ra_event_formatter => Formatter}.

get_nodes(Q) when ?is_amqqueue(Q) ->
    #{nodes := Nodes} = amqqueue:get_type_state(Q),
    Nodes.

get_connected_nodes(Q) when ?is_amqqueue(Q) ->
    ErlangNodes = [node() | nodes()],
    [N || N <- get_nodes(Q), lists:member(N, ErlangNodes)].

update_type_state(Q, Fun) when ?is_amqqueue(Q) ->
    Ts = amqqueue:get_type_state(Q),
    amqqueue:set_type_state(Q, Fun(Ts)).

overflow(undefined, Def, _QName) -> Def;
overflow(<<"reject-publish">>, _Def, _QName) -> reject_publish;
overflow(<<"drop-head">>, _Def, _QName) -> drop_head;
overflow(<<"reject-publish-dlx">> = V, Def, QName) ->
    rabbit_log:warning("Invalid overflow strategy ~tp for quorum queue: ~ts",
                       [V, rabbit_misc:rs(QName)]),
    Def.

-spec notify_decorators(amqqueue:amqqueue()) -> 'ok'.
notify_decorators(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    case ra:local_query(QPid, fun rabbit_fifo:query_notify_decorators_info/1) of
        {ok, {_, {MaxActivePriority, IsEmpty}}, _} ->
            notify_decorators(QName, consumer_state_changed,
                              [MaxActivePriority, IsEmpty]);
        _ -> ok
    end.

notify_decorators(QName, Event) ->
    notify_decorators(QName, Event, []).

notify_decorators(Q, F, A) when ?is_amqqueue(Q) ->
    Ds = amqqueue:get_decorators(Q),
    [ok = apply(M, F, [Q|A]) || M <- rabbit_queue_decorator:select(Ds)],
    ok;
notify_decorators(QName, F, A) ->
    %% Look up again in case policy and hence decorators have changed
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Ds = amqqueue:get_decorators(Q),
            [ok = apply(M, F, [Q|A]) || M <- rabbit_queue_decorator:select(Ds)],
            ok;
        {error, not_found} ->
            ok
    end.

is_stateful() -> true.

force_shrink_member_to_current_member(VHost, Name) ->
    Node = node(),
    QName = rabbit_misc:r(VHost, queue, Name),
    QNameFmt = rabbit_misc:rs(QName),
    rabbit_log:warning("Shrinking ~ts to a single node: ~ts", [QNameFmt, Node]),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} when ?is_amqqueue(Q) ->
            {RaName, _} = amqqueue:get_pid(Q),
            OtherNodes = lists:delete(Node, get_nodes(Q)),
            ok = ra_server_proc:force_shrink_members_to_current_member({RaName, Node}),
            Fun = fun (Q0) ->
                          TS0 = amqqueue:get_type_state(Q0),
                          TS = TS0#{nodes => [Node]},
                          amqqueue:set_type_state(Q, TS)
                  end,
            _ = rabbit_amqqueue:update(QName, Fun),
            _ = [ra:force_delete_server(?RA_SYSTEM, {RaName, N}) || N <- OtherNodes],
            rabbit_log:warning("Shrinking ~ts finished", [QNameFmt]);
        _ ->
            rabbit_log:warning("Shrinking failed, ~ts not found", [QNameFmt]),
            {error, not_found}
    end.

force_vhost_queues_shrink_member_to_current_member(VHost) when is_binary(VHost) ->
    rabbit_log:warning("Shrinking all quorum queues in vhost '~ts' to a single node: ~ts", [VHost, node()]),
    ListQQs = fun() -> rabbit_amqqueue:list(VHost) end,
    force_all_queues_shrink_member_to_current_member(ListQQs).

force_all_queues_shrink_member_to_current_member() ->
    rabbit_log:warning("Shrinking all quorum queues to a single node: ~ts", [node()]),
    ListQQs = fun() -> rabbit_amqqueue:list() end,
    force_all_queues_shrink_member_to_current_member(ListQQs).

force_all_queues_shrink_member_to_current_member(ListQQFun) when is_function(ListQQFun) ->
    Node = node(),
    _ = [begin
             QName = amqqueue:get_name(Q),
             {RaName, _} = amqqueue:get_pid(Q),
             OtherNodes = lists:delete(Node, get_nodes(Q)),
             rabbit_log:warning("Shrinking queue ~ts to a single node: ~ts", [rabbit_misc:rs(QName), Node]),
             ok = ra_server_proc:force_shrink_members_to_current_member({RaName, Node}),
             Fun = fun (QQ) ->
                           TS0 = amqqueue:get_type_state(QQ),
                           TS = TS0#{nodes => [Node]},
                           amqqueue:set_type_state(QQ, TS)
                   end,
             _ = rabbit_amqqueue:update(QName, Fun),
             _ = [ra:force_delete_server(?RA_SYSTEM, {RaName, N}) || N <- OtherNodes]
         end || Q <- ListQQFun(), amqqueue:get_type(Q) == ?MODULE],
    rabbit_log:warning("Shrinking finished"),
    ok.

force_checkpoint_on_queue(QName) ->
    QNameFmt = rabbit_misc:rs(QName),
    case rabbit_db_queue:get_durable(QName) of
        {ok, Q} when ?amqqueue_is_classic(Q) ->
            {error, classic_queue_not_supported};
        {ok, Q} when ?amqqueue_is_quorum(Q) ->
            {RaName, _} = amqqueue:get_pid(Q),
            rabbit_log:debug("Sending command to force ~ts to take a checkpoint", [QNameFmt]),
            Nodes = amqqueue:get_nodes(Q),
            _ = [ra:cast_aux_command({RaName, Node}, force_checkpoint)
                 || Node <- Nodes],
            ok;
        {ok, _Q} ->
            {error, not_quorum_queue};
        {error, _} = E ->
            E
    end.

force_checkpoint(VhostSpec, QueueSpec) ->
    [begin
         QName = amqqueue:get_name(Q),
         case force_checkpoint_on_queue(QName) of
             ok ->
                 {QName, {ok}};
             {error, Err} ->
                 rabbit_log:warning("~ts: failed to force checkpoint, error: ~w",
                                    [rabbit_misc:rs(QName), Err]),
                 {QName, {error, Err}}
         end
     end
     || Q <- rabbit_db_queue:get_all_durable_by_type(?MODULE),
        is_match(amqqueue:get_vhost(Q), VhostSpec)
        andalso is_match(get_resource_name(amqqueue:get_name(Q)), QueueSpec)].

is_minority(All, Up) ->
    MinQuorum = length(All) div 2 + 1,
    length(Up) < MinQuorum.

wait_for_projections(Node, QName) ->
    case rabbit_feature_flags:is_enabled(khepri_db) andalso
         Node =/= node() of
        true ->
            wait_for_projections(Node, QName, 256);
        false ->
            ok
    end.

wait_for_projections(Node, QName, 0) ->
    exit({wait_for_projections_timed_out, Node, QName});
wait_for_projections(Node, QName, N) ->
    case erpc_call(Node, rabbit_amqqueue, lookup, [QName], 100) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(100),
            wait_for_projections(Node, QName, N - 1)
    end.

find_leader(Q) when ?is_amqqueue(Q) ->
    %% the get_pid field in the queue record is updated async after a leader
    %% change, so is likely to be the more stale than the leaderboard
    {Name, _Node} = MaybeLeader = amqqueue:get_pid(Q),
    Leaders = case ra_leaderboard:lookup_leader(Name) of
                 undefined ->
                     %% leader from queue record will have to suffice
                     [MaybeLeader];
                 LikelyLeader ->
                     [LikelyLeader, MaybeLeader]
             end,
    Nodes = [node() | nodes()],
    case lists:search(fun ({_Nm, Nd}) ->
                              lists:member(Nd, Nodes)
                      end, Leaders) of
        {value, Leader} ->
            Leader;
        false ->
            undefined
    end.

is_process_alive(Name, Node) ->
    %% don't attempt rpc if node is not already connected
    %% as this function is used for metrics and stats and the additional
    %% latency isn't warranted
    erlang:is_pid(erpc_call(Node, erlang, whereis, [Name], ?RPC_TIMEOUT)).

%% backwards compat
file_handle_leader_reservation(_QName) ->
    ok.

file_handle_other_reservation() ->
    ok.

file_handle_release_reservation() ->
    ok.

leader_health_check(QueueNameOrRegEx, VHost) ->
    %% Set a process limit threshold to 20% of ErlangVM process limit, beyond which
    %% we cannot spawn any new processes for executing QQ leader health checks.
    ProcessLimitThreshold = round(0.2 * erlang:system_info(process_limit)),

    leader_health_check(QueueNameOrRegEx, VHost, ProcessLimitThreshold).

leader_health_check(QueueNameOrRegEx, VHost, ProcessLimitThreshold) ->
    Qs =
        case VHost of
            across_all_vhosts ->
                rabbit_db_queue:get_all_by_type(?MODULE);
            VHost when is_binary(VHost) ->
                rabbit_db_queue:get_all_by_type_and_vhost(?MODULE, VHost)
        end,
    check_process_limit_safety(length(Qs), ProcessLimitThreshold),
    ParentPID = self(),
    HealthCheckRef = make_ref(),
    HealthCheckPids =
        lists:flatten(
            [begin
                {resource, _VHostN, queue, QueueName} = QResource = amqqueue:get_name(Q),
                case re:run(QueueName, QueueNameOrRegEx, [{capture, none}]) of
                    match ->
                        {ClusterName, _} = rabbit_amqqueue:pid_of(Q),
                        _Pid = spawn(fun() -> run_leader_health_check(ClusterName, QResource, HealthCheckRef, ParentPID) end);
                    _ ->
                        []
                end
            end || Q <- Qs, amqqueue:get_type(Q) == ?MODULE]),
    Result = wait_for_leader_health_checks(HealthCheckRef, length(HealthCheckPids), []),
    _ = spawn(fun() -> maybe_log_leader_health_check_result(Result) end),
    Result.

run_leader_health_check(ClusterName, QResource, HealthCheckRef, From) ->
    Leader = ra_leaderboard:lookup_leader(ClusterName),

    %% Ignoring result here is required to clear a diayzer warning.
    _ =
        case ra_server_proc:ping(Leader, ?LEADER_HEALTH_CHECK_TIMEOUT) of
            {pong,leader} ->
                From ! {ok, HealthCheckRef, QResource};
            _ ->
                From ! {error, HealthCheckRef, QResource}
        end,
    ok.

wait_for_leader_health_checks(_Ref, 0, UnhealthyAcc) -> UnhealthyAcc;
wait_for_leader_health_checks(Ref, N, UnhealthyAcc) ->
    receive
        {ok, Ref, _QResource} ->
            wait_for_leader_health_checks(Ref, N - 1, UnhealthyAcc);
        {error, Ref, QResource} ->
            wait_for_leader_health_checks(Ref, N - 1, [amqqueue:to_printable(QResource, ?MODULE) | UnhealthyAcc])
    after
        ?GLOBAL_LEADER_HEALTH_CHECK_TIMEOUT ->
            UnhealthyAcc
    end.

check_process_limit_safety(QCount, ProcessLimitThreshold) ->
    case (erlang:system_info(process_count) + QCount) >= ProcessLimitThreshold of
        true ->
            rabbit_log:warning("Leader health check not permitted, process limit threshold will be exceeded."),
            throw({error, leader_health_check_process_limit_exceeded});
        false ->
            ok
    end.

maybe_log_leader_health_check_result([]) -> ok;
maybe_log_leader_health_check_result(Result) ->
    Qs = lists:map(fun(R) -> catch maps:get(<<"readable_name">>, R) end, Result),
    rabbit_log:warning("Leader health check result (unhealthy leaders detected): ~tp", [Qs]).

policy_apply_to_name() ->
    <<"quorum_queues">>.

-spec drain([node()]) -> ok.
drain(TransferCandidates) ->
    _ = transfer_leadership(TransferCandidates),
    _ = stop_local_quorum_queue_followers(),
    ok.

transfer_leadership([]) ->
    rabbit_log:warning("Skipping leadership transfer of quorum queues: no candidate "
                       "(online, not under maintenance) nodes to transfer to!");
transfer_leadership(_TransferCandidates) ->
    %% we only transfer leadership for QQs that have local leaders
    Queues = rabbit_amqqueue:list_local_leaders(),
    rabbit_log:info("Will transfer leadership of ~b quorum queues with current leader on this node",
                    [length(Queues)]),
    [begin
        Name = amqqueue:get_name(Q),
        rabbit_log:debug("Will trigger a leader election for local quorum queue ~ts",
                         [rabbit_misc:rs(Name)]),
        %% we trigger an election and exclude this node from the list of candidates
        %% by simply shutting its local QQ replica (Ra server)
        RaLeader = amqqueue:get_pid(Q),
        rabbit_log:debug("Will stop Ra server ~tp", [RaLeader]),
        case rabbit_quorum_queue:stop_server(RaLeader) of
            ok     ->
                rabbit_log:debug("Successfully stopped Ra server ~tp", [RaLeader]);
            {error, nodedown} ->
                rabbit_log:error("Failed to stop Ra server ~tp: target node was reported as down")
        end
     end || Q <- Queues],
    rabbit_log:info("Leadership transfer for quorum queues hosted on this node has been initiated").

%% TODO: I just copied it over, it looks like was always called inside maintenance so...
-spec stop_local_quorum_queue_followers() -> ok.
stop_local_quorum_queue_followers() ->
    Queues = rabbit_amqqueue:list_local_followers(),
    rabbit_log:info("Will stop local follower replicas of ~b quorum queues on this node",
                    [length(Queues)]),
    [begin
        Name = amqqueue:get_name(Q),
        rabbit_log:debug("Will stop a local follower replica of quorum queue ~ts",
                         [rabbit_misc:rs(Name)]),
        %% shut down Ra nodes so that they are not considered for leader election
        {RegisteredName, _LeaderNode} = amqqueue:get_pid(Q),
        RaNode = {RegisteredName, node()},
        rabbit_log:debug("Will stop Ra server ~tp", [RaNode]),
        case rabbit_quorum_queue:stop_server(RaNode) of
            ok     ->
                rabbit_log:debug("Successfully stopped Ra server ~tp", [RaNode]);
            {error, nodedown} ->
                rabbit_log:error("Failed to stop Ra server ~tp: target node was reported as down")
        end
     end || Q <- Queues],
    rabbit_log:info("Stopped all local replicas of quorum queues hosted on this node").

revive() ->
    revive_local_queue_members().

revive_local_queue_members() ->
    Queues = rabbit_amqqueue:list_local_followers(),
    %% NB: this function ignores the first argument so we can just pass the
    %% empty binary as the vhost name.
    {Recovered, Failed} = rabbit_quorum_queue:recover(<<>>, Queues),
    rabbit_log:debug("Successfully revived ~b quorum queue replicas",
                     [length(Recovered)]),
    case length(Failed) of
        0 ->
            ok;
        NumFailed ->
            rabbit_log:error("Failed to revive ~b quorum queue replicas",
                             [NumFailed])
    end,

    rabbit_log:info("Restart of local quorum queue replicas is complete"),
    ok.

queue_vm_stats_sups() ->
    {[quorum_queue_procs,
      quorum_queue_dlx_procs],
     [[ra_server_sup_sup],
      [rabbit_fifo_dlx_sup]]}.

queue_vm_ets() ->
    {[quorum_ets],
     [[ra_log_ets]]}.
