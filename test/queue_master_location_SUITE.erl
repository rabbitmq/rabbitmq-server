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
%% Copyright (c) 2011-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(queue_master_location_SUITE).

%% These tests use an ABC cluster with each node initialised with
%% a different number of queues. When a queue is declared, different
%% strategies can be applied to determine the queue's master node. Queue
%% location strategies can be applied in the following ways;
%%   1. As policy,
%%   2. As config (in rabbitmq.config),
%%   3. or as part of the queue's declare arguements.
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
          declare_config,
          calculate_min_master,
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
    QueueName = rabbit_misc:r(<<"/">>, queue, Q= <<"qm.test">>),
    Args = [{<<"x-queue-master-locator">>, <<"min-masters">>}],
    declare(Config, QueueName, false, false, Args, none),
    verify_min_master(Config, Q).

declare_policy(Config) ->
    setup_test_environment(Config),
    unset_location_config(Config),
    set_location_policy(Config, ?POLICY, <<"min-masters">>),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q= <<"qm.test">>),
    declare(Config, QueueName, false, false, _Args=[], none),
    verify_min_master(Config, Q).

declare_config(Config) ->
    setup_test_environment(Config),
    set_location_config(Config, <<"min-masters">>),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q= <<"qm.test">>),
    declare(Config, QueueName, false, false, _Args=[], none),
    verify_min_master(Config, Q),
    unset_location_config(Config),
    ok.

%%
%% Test 'calculations'
%%

calculate_min_master(Config) ->
    setup_test_environment(Config),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q= <<"qm.test">>),
    Args = [{<<"x-queue-master-locator">>, <<"min-masters">>}],
    declare(Config, QueueName, false, false, Args, none),
    verify_min_master(Config, Q),
    ok.

calculate_random(Config) ->
    setup_test_environment(Config),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q= <<"qm.test">>),
    Args = [{<<"x-queue-master-locator">>, <<"random">>}],
    declare(Config, QueueName, false, false, Args, none),
    verify_random(Config, Q),
    ok.

calculate_client_local(Config) ->
    setup_test_environment(Config),
    QueueName = rabbit_misc:r(<<"/">>, queue, Q= <<"qm.test">>),
    Args = [{<<"x-queue-master-locator">>, <<"client-local">>}],
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
    [ok = rpc:call(Node, application, set_env,
                   [rabbit, queue_master_locator, Strategy]) || Node <- Nodes],
    ok.

unset_location_config(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ok = rpc:call(Node, application, unset_env,
                   [rabbit, queue_master_locator]) || Node <- Nodes],
    ok.

declare(Config, QueueName, Durable, AutoDelete, Args, Owner) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {new, Queue} = rpc:call(Node, rabbit_amqqueue, declare,
                            [QueueName, Durable, AutoDelete, Args, Owner]),
    Queue.

verify_min_master(Config, Q) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    MinMaster = min_master_node(Config),
    {ok, MinMaster} = rpc:call(Node, rabbit_queue_master_location_misc,
                               lookup_master, [Q, ?DEFAULT_VHOST_PATH]).

verify_random(Config, Q) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    {ok, Master} = rpc:call(Node, rabbit_queue_master_location_misc,
                            lookup_master, [Q, ?DEFAULT_VHOST_PATH]),
    true = lists:member(Master, Nodes).

verify_client_local(Config, Q) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, Node} = rpc:call(Node, rabbit_queue_master_location_misc,
                          lookup_master, [Q, ?DEFAULT_VHOST_PATH]).

set_location_policy(Config, Name, Strategy) ->
    ok = rabbit_ct_broker_helpers:set_policy(Config, 0,
      Name, <<".*">>, <<"queues">>, [{<<"queue-master-locator">>, Strategy}]).
