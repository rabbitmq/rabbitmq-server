%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(dynamic_qq_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(queue_utils, [wait_for_messages_ready/3,
                      ra_name/1]).

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
      {group, clustered}
    ].

groups() ->
    [
      {clustered, [], [
          {cluster_size_3, [], [
              vhost_deletion,
              quorum_unaffected_after_vhost_failure,
              forget_cluster_node,
              force_delete_if_no_consensus,
              takeover_on_failure,
              takeover_on_shutdown
            ]}
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

init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(cluster_size_5, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 5}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() andalso
         Testcase == quorum_unaffected_after_vhost_failure of
        true ->
            {skip, "test case not mixed versions compatible"};
        false ->
            rabbit_ct_helpers:testcase_started(Config, Testcase),
            ClusterSize = ?config(rmq_nodes_count, Config),
            TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
            Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
            Q = rabbit_data_coercion:to_binary(io_lib:format("~p_~tp", [Group, Testcase])),
            Config1 = rabbit_ct_helpers:set_config(Config, [
                                                            {rmq_nodename_suffix, Testcase},
                                                            {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
                                                            {queue_name, Q},
                                                            {queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                                          {<<"x-quorum-initial-group-size">>, long, 3}]}
                                                           ]),
            rabbit_ct_helpers:run_steps(
              Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps())
    end.

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
%% Vhost deletion needs to successfully tear down queues.
vhost_deletion(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QName = ?config(queue_name, Config),
    Args = ?config(queue_args, Config),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           arguments = Args,
                                           durable = true
                                          }),
    ok = rpc:call(Node, rabbit_vhost, delete, [<<"/">>, <<"acting-user">>]),
    ?assertMatch([],
                 rabbit_ct_broker_helpers:rabbitmqctl_list(
                   Config, 0, ["list_queues", "name"])),
    ok.

force_delete_if_no_consensus(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    Args = ?config(queue_args, Config),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           arguments = Args,
                                           durable = true
                                          }),
    rabbit_ct_client_helpers:close_channel(Ch),

    RaName = queue_utils:ra_name(QName),
    {ok, [{_, A}, {_, B}, {_, C}], _} = ra:members({RaName, Server}),

    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    rabbit_ct_client_helpers:publish(ACh, QName, 10),

    %% Delete a member on one node
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QName, B])),
    %% stop another node
    ok = rabbit_ct_broker_helpers:stop_node(Config, C),

    BCh = rabbit_ct_client_helpers:open_channel(Config, B),
    ?assertMatch(
       #'queue.declare_ok'{},
       amqp_channel:call(
         BCh, #'queue.declare'{queue   = QName,
                               arguments = Args,
                               durable = true,
                               passive = true})),
    BCh2 = rabbit_ct_client_helpers:open_channel(Config, B),
    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(BCh2, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_broker_helpers:restart_node(Config, C),
    ok.

takeover_on_failure(Config) ->
    takeover_on(Config, kill_node).

takeover_on_shutdown(Config) ->
    takeover_on(Config, stop_node).

takeover_on(Config, Fun) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    QName = ?config(queue_name, Config),
    Args = ?config(queue_args, Config),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                            arguments = Args,
                                            durable = true
                                           }),
    rabbit_ct_client_helpers:publish(Ch, QName, 10),

    RaName = queue_utils:ra_name(QName),
    {ok, [{_, A}, {_, B}, {_, C}], _} = ra:members({RaName, Server}),
    ok = rabbit_ct_broker_helpers:restart_node(Config, B),

    ok = rabbit_ct_broker_helpers:Fun(Config, C),
    ok = rabbit_ct_broker_helpers:Fun(Config, A),

    BCh = rabbit_ct_client_helpers:open_channel(Config, B),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          BCh, #'queue.declare'{queue   = QName,
                                arguments = Args,
                                durable = true}),
    ok = rabbit_ct_broker_helpers:start_node(Config, A),
    ACh2 = rabbit_ct_client_helpers:open_channel(Config, A),
    #'queue.declare_ok'{message_count = 10} =
        amqp_channel:call(
          ACh2, #'queue.declare'{queue   = QName,
                                 arguments = Args,
                                 durable = true}),
    ok.

quorum_unaffected_after_vhost_failure(Config) ->
    [A, B, _] = Servers0 = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Servers = lists:sort(Servers0),

    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    QName = ?config(queue_name, Config),
    Args = ?config(queue_args, Config),
    amqp_channel:call(ACh, #'queue.declare'{queue = QName,
                                            arguments = Args,
                                            durable = true
                                           }),
    ?awaitMatch(
       Servers,
       begin
           Info0 = rpc:call(A, rabbit_quorum_queue, infos,
                            [rabbit_misc:r(<<"/">>, queue, QName)]),
           lists:sort(proplists:get_value(online, Info0, []))
       end,
       60000),

    %% Crash vhost on both nodes
    {ok, SupA} = rabbit_ct_broker_helpers:rpc(Config, A, rabbit_vhost_sup_sup, get_vhost_sup, [<<"/">>]),
    exit(SupA, foo),
    {ok, SupB} = rabbit_ct_broker_helpers:rpc(Config, B, rabbit_vhost_sup_sup, get_vhost_sup, [<<"/">>]),
    exit(SupB, foo),

    ?awaitMatch(
       Servers,
       begin
           Info = rpc:call(A, rabbit_quorum_queue, infos,
                            [rabbit_misc:r(<<"/">>, queue, QName)]),
           lists:sort(proplists:get_value(online, Info, []))
       end,
       60000).

forget_cluster_node(Config) ->
    %% Tests that quorum queues shrink when forget_cluster_node
    %% operations are issues.
    quorum_queue_SUITE:check_quorum_queues_v4_compat(Config),

    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    QName = ?config(queue_name, Config),
    Args = ?config(queue_args, Config),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           arguments = Args,
                                           durable = true
                                          }),

    RaName = queue_utils:ra_name(QName),
    {ok, [{_, A}, {_, B}, {_, C}], _} = ra:members({RaName, Server}),
    Servers = [A, B, C],

    Name = ra_name(QName),

    rabbit_ct_client_helpers:publish(Ch, QName, 15),
    wait_for_messages_ready(Servers, Name, 15),
    rabbit_ct_client_helpers:close_channel(Ch),

    %% Restart one follower
    forget_cluster_node(Config, C, B),
    wait_for_messages_ready([C], Name, 15),
    forget_cluster_node(Config, C, A),
    wait_for_messages_ready([C], Name, 15),

    ok.

%%----------------------------------------------------------------------------
forget_cluster_node(Config, Node, NodeToRemove) ->
    ok = rabbit_control_helper:command(stop_app, NodeToRemove),
    rabbit_ct_broker_helpers:rabbitmqctl(
      Config, Node, ["forget_cluster_node", NodeToRemove]).
