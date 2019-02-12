%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(queue_master_location_SUITE).

%% These tests use an ABC cluster with each node initialised with
%% a different number of queues. When a queue is declared, different
%% strategies can be applied to determine the queue's master node. Queue
%% location strategies can be applied in the following ways;
%%   1. As policy,
%%   2. As config (in rabbitmq.config),
%%   3. or as part of the queue's declare arguments.
%%
%% Currently supported strategies are;
%%   min-masters : The queue master node is calculated as the one with the
%%                 least bound queues in the cluster.
%%   client-local: The queue master node is the local node from which
%%                 the declaration is being carried out from
%%   random      : The queue master node is randomly selected.
%%

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(DEFAULT_VHOST_PATH, (<<"/">>)).
-define(POLICY, <<"^qm.location$">>).

all() ->
    [
      {group, cluster_size_3}
    ].

groups() ->
    [
      {cluster_size_3, [], [
          declare_args,
          declare_policy,
          declare_policy_nodes,
          declare_policy_all,
          declare_policy_exactly,
          declare_config,
          calculate_min_master,
          calculate_min_master_with_bindings,
          calculate_random,
          calculate_client_local
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 3} %% Replaced with a list of node names later.
      ]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    Nodenames = [
      list_to_atom(rabbit_misc:format("~s-~b", [Testcase, I]))
      || I <- lists:seq(1, ClusterSize)
    ],
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, Nodenames},
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%%
%% Queue 'declarations'
%%

declare_args(Config) ->
    setup_test_environment(Config),
    unset_location_config(Config),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    Args = [{<<"x-queue-master-locator">>, longstr, <<"min-masters">>}],
    declare(Config, QueueName, false, false, Args, none),
    verify_min_master(Config, Q).

declare_policy(Config) ->
    setup_test_environment(Config),
    unset_location_config(Config),
    set_location_policy(Config, ?POLICY, <<"min-masters">>),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    declare(Config, QueueName, false, false, _Args=[], none),
    verify_min_master(Config, Q).

declare_policy_nodes(Config) ->
    setup_test_environment(Config),
    unset_location_config(Config),
    % Note:
    % Node0 has 15 queues, Node1 has 8 and Node2 has 1
    Node0Name = rabbit_data_coercion:to_binary(
                  rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    Node1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    Node1Name =  rabbit_data_coercion:to_binary(Node1),
    Nodes = [Node1Name, Node0Name],
    Policy = [{<<"queue-master-locator">>, <<"min-masters">>},
              {<<"ha-mode">>, <<"nodes">>},
              {<<"ha-params">>, Nodes}],
    ok = rabbit_ct_broker_helpers:set_policy(Config, 0, ?POLICY,
                                             <<".*">>, <<"queues">>, Policy),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    declare(Config, QueueName, false, false, _Args=[], none),
    verify_min_master(Config, Q, Node1).

declare_policy_all(Config) ->
    setup_test_environment(Config),
    unset_location_config(Config),
    % Note:
    % Node0 has 15 queues, Node1 has 8 and Node2 has 1
    Policy = [{<<"queue-master-locator">>, <<"min-masters">>},
              {<<"ha-mode">>, <<"all">>}],
    ok = rabbit_ct_broker_helpers:set_policy(Config, 0, ?POLICY,
                                             <<".*">>, <<"queues">>, Policy),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    declare(Config, QueueName, false, false, _Args=[], none),
    verify_min_master(Config, Q).

declare_policy_exactly(Config) ->
    setup_test_environment(Config),
    unset_location_config(Config),
    Policy = [{<<"queue-master-locator">>, <<"min-masters">>},
              {<<"ha-mode">>, <<"exactly">>},
              {<<"ha-params">>, 2}],
    ok = rabbit_ct_broker_helpers:set_policy(Config, 0, ?POLICY,
                                             <<".*">>, <<"queues">>, Policy),
    QueueRes = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    declare(Config, QueueRes, false, false, _Args=[], none),

    Node0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_broker_helpers:control_action(sync_queue, Node0,
                                            [binary_to_list(Q)], [{"-p", "/"}]),
    wait_for_sync(Config, Node0, QueueRes, 1),

    {ok, Queue} = rabbit_ct_broker_helpers:rpc(Config, Node0,
                                               rabbit_amqqueue, lookup, [QueueRes]),
    {MNode0, [SNode], [SSNode]} = rabbit_ct_broker_helpers:rpc(Config, Node0,
                                                               rabbit_mirror_queue_misc,
                                                               actual_queue_nodes, [Queue]),
    ?assertEqual(SNode, SSNode),
    {ok, MNode1} = rabbit_ct_broker_helpers:rpc(Config, 0,
                                                rabbit_queue_master_location_misc,
                                                lookup_master, [Q, ?DEFAULT_VHOST_PATH]),
    ?assertEqual(MNode0, MNode1),
    Node2 = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
    ?assertEqual(MNode1, Node2).

declare_config(Config) ->
    setup_test_environment(Config),
    set_location_config(Config, <<"min-masters">>),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    declare(Config, QueueName, false, false, _Args=[], none),
    verify_min_master(Config, Q),
    unset_location_config(Config),
    ok.

%%
%% Test 'calculations'
%%

calculate_min_master(Config) ->
    setup_test_environment(Config),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    Args = [{<<"x-queue-master-locator">>, longstr, <<"min-masters">>}],
    declare(Config, QueueName, false, false, Args, none),
    verify_min_master(Config, Q),
    ok.

calculate_min_master_with_bindings(Config) ->
    setup_test_environment(Config),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test_bound">>),
    Args = [{<<"x-queue-master-locator">>, longstr, <<"min-masters">>}],
    declare(Config, QueueName, false, false, Args, none),
    verify_min_master(Config, Q),
    %% Add 20 bindings to this queue
    [ bind(Config, QueueName, integer_to_binary(N)) || N <- lists:seq(1, 20) ],

    QueueName1 = rabbit_misc:r(<<"/">>, queue, Q1 = <<"qm.test_unbound">>),
    declare(Config, QueueName1, false, false, Args, none),
    % Another queue should still be on the same node, bindings should
    % not account for min-masters counting
    verify_min_master(Config, Q1),
    ok.

calculate_random(Config) ->
    setup_test_environment(Config),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    Args = [{<<"x-queue-master-locator">>, longstr, <<"random">>}],
    declare(Config, QueueName, false, false, Args, none),
    verify_random(Config, Q),
    ok.

calculate_client_local(Config) ->
    setup_test_environment(Config),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q = <<"qm.test">>),
    Args = [{<<"x-queue-master-locator">>, longstr, <<"client-local">>}],
    declare(Config, QueueName, false, false, Args, none),
    verify_client_local(Config, Q),
    ok.

%%
%% Setup environment
%%

setup_test_environment(Config)  ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [distribute_queues(Config, Node) || Node <- Nodes],
    ok.

distribute_queues(Config, Node) ->
    ok  = rpc:call(Node, application, unset_env, [rabbit, queue_master_location]),
    Count = case rabbit_ct_broker_helpers:nodename_to_index(Config, Node) of
        0 -> 15;
        1 -> 8;
        2 -> 1
    end,

    Channel = rabbit_ct_client_helpers:open_channel(Config, Node),
    ok = declare_queues(Channel, declare_fun(), Count),
    ok = create_e2e_binding(Channel, [<< "ex_1" >>, << "ex_2" >>]),
    {ok, Channel}.

%%
%% Internal queue handling
%%

declare_queues(Channel, DeclareFun, 1) -> DeclareFun(Channel);
declare_queues(Channel, DeclareFun, N) ->
    DeclareFun(Channel),
    declare_queues(Channel, DeclareFun, N-1).

declare_exchange(Channel, Ex) ->
    #'exchange.declare_ok'{} =
        amqp_channel:call(Channel, #'exchange.declare'{exchange = Ex}),
    {ok, Ex}.

declare_binding(Channel, Binding) ->
    #'exchange.bind_ok'{} = amqp_channel:call(Channel, Binding),
    ok.

declare_fun() ->
    fun(Channel) ->
            #'queue.declare_ok'{} = amqp_channel:call(Channel, get_random_queue_declare()),
            ok
    end.

create_e2e_binding(Channel, ExNamesBin) ->
    [{ok, Ex1}, {ok, Ex2}] = [declare_exchange(Channel, Ex) || Ex <- ExNamesBin],
    Binding = #'exchange.bind'{source = Ex1, destination = Ex2},
    ok = declare_binding(Channel, Binding).

get_random_queue_declare() ->
    #'queue.declare'{passive     = false,
                     durable     = false,
                     exclusive   = true,
                     auto_delete = false,
                     nowait      = false,
                     arguments   = []}.

%%
%% Internal helper functions
%%

get_cluster() -> [node()|nodes()].

min_master_node(Config) ->
    hd(lists:reverse(
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename))).

set_location_config(Config, Strategy) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ok = rabbit_ct_broker_helpers:rpc(Config, Node,
                                       application, set_env,
                                       [rabbit, queue_master_locator, Strategy]) || Node <- Nodes],
    ok.

unset_location_config(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ok = rabbit_ct_broker_helpers:rpc(Config, Node,
                                       application, unset_env,
                                       [rabbit, queue_master_locator]) || Node <- Nodes],
    ok.

declare(Config, QueueName, Durable, AutoDelete, Args0, Owner) ->
    Args1 = [QueueName, Durable, AutoDelete, Args0, Owner, <<"acting-user">>],
    {new, Queue} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, declare, Args1),
    Queue.

bind(Config, QueueName, RoutingKey) ->
    ExchangeName = rabbit_misc:r(QueueName, exchange, <<"amq.direct">>),

    ok = rabbit_ct_broker_helpers:rpc(
        Config, 0, rabbit_binding, add,
        [#binding{source      = ExchangeName,
                  destination = QueueName,
                  key         = RoutingKey,
                  args        = []},
         <<"acting-user">>]).

verify_min_master(Config, Q, MinMasterNode) ->
    Rpc = rabbit_ct_broker_helpers:rpc(Config, 0,
                                       rabbit_queue_master_location_misc,
                                       lookup_master, [Q, ?DEFAULT_VHOST_PATH]),
    ?assertEqual({ok, MinMasterNode}, Rpc).

verify_min_master(Config, Q) ->
    MinMaster = min_master_node(Config),
    verify_min_master(Config, Q, MinMaster).

verify_random(Config, Q) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {ok, Master} = rabbit_ct_broker_helpers:rpc(Config, Node,
                                                rabbit_queue_master_location_misc,
                                                lookup_master, [Q, ?DEFAULT_VHOST_PATH]),
    ?assert(lists:member(Master, Nodes)).

verify_client_local(Config, Q) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Rpc = rabbit_ct_broker_helpers:rpc(Config, Node,
                                       rabbit_queue_master_location_misc,
                                       lookup_master, [Q, ?DEFAULT_VHOST_PATH]),
    ?assertEqual({ok, Node}, Rpc).

set_location_policy(Config, Name, Strategy) ->
    ok = rabbit_ct_broker_helpers:set_policy(Config, 0,
      Name, <<".*">>, <<"queues">>, [{<<"queue-master-locator">>, Strategy}]).

wait_for_sync(Config, Nodename, Q, ExpectedSSPidLen) ->
    wait_for_sync(Config, Nodename, Q, ExpectedSSPidLen, 600).

wait_for_sync(_, _, _, _, 0) ->
    throw(sync_timeout);
wait_for_sync(Config, Nodename, Q, ExpectedSSPidLen, N) ->
    case synced(Config, Nodename, Q, ExpectedSSPidLen) of
        true  -> ok;
        false -> timer:sleep(100),
                 wait_for_sync(Config, Nodename, Q, ExpectedSSPidLen, N-1)
    end.

synced(Config, Nodename, Q, ExpectedSSPidLen) ->
    Args = [<<"/">>, [name, synchronised_slave_pids]],
    Info = rabbit_ct_broker_helpers:rpc(Config, Nodename,
                                        rabbit_amqqueue, info_all, Args),
    [SSPids] = [Pids || [{name, Q1}, {synchronised_slave_pids, Pids}] <- Info, Q =:= Q1],
    length(SSPids) =:= ExpectedSSPidLen.
