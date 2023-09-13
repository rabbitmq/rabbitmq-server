%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.


-module(quorum_queue_member_reconciliation_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([nowarn_export_all, export_all]).


all() ->
    [
     {group, unclustered}
    ].

groups() ->
    [
     {unclustered, [],
      [
       {quorum_queue_3, [], [auto_grow, auto_grow_drained_node, auto_shrink]}
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000},
                                   {quorum_membership_reconciliation_enabled, true},
                                   {quorum_membership_reconciliation_auto_remove, true},
                                   {quorum_membership_reconciliation_interval, 5000},
                                   {quorum_membership_reconciliation_trigger_interval, 2000},
                                   {quorum_membership_reconciliation_target_group_size, 3}]}),
    rabbit_ct_helpers:run_setup_steps(Config1, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).
init_per_group(unclustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]);
init_per_group(Group, Config) ->
    ClusterSize = 3,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
    rabbit_ct_helpers:run_steps(Config1b,
                                [fun merge_app_env/1 ] ++
                                    rabbit_ct_broker_helpers:setup_steps()).

end_per_group(unclustered, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>},
                                            {alt_2_queue_name, <<Q/binary, "_alt_2">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

end_per_testcase(Testcase, Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    reset_nodes([Server1, Server2], Server0),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

reset_nodes([], _Leader) ->
    ok;
reset_nodes([Node| Nodes], Leader) ->
    ok = rabbit_control_helper:command(stop_app, Node),
    ok = rabbit_control_helper:command(forget_cluster_node, Leader, [atom_to_list(Node)]),
    ok = rabbit_control_helper:command(reset, Node),
    ok = rabbit_control_helper:command(start_app, Node),
    reset_nodes(Nodes, Leader).


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

auto_grow(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),

    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% There is only one node in the cluster at the moment
    {ok, Members, _} = ra:members({queue_utils:ra_name(QQ), Server0}),
    ?assertEqual(1, length(Members)),

    add_server_to_cluster(Server1, Server0),
    %% With 2 nodes in the cluster, target group size is not reached, so no
    %% new members should be available. We sleep a while so the periodic check
    %% runs
    timer:sleep(4000),
    {ok, Members, _} = ra:members({queue_utils:ra_name(QQ), Server0}),
    ?assertEqual(1, length(Members)),

    add_server_to_cluster(Server2, Server0),
    %% With 3 nodes in the cluster, target size is met so eventually it should
    %% be 3 members
    wait_until(fun() ->
                       {ok, M, _} = ra:members({queue_utils:ra_name(QQ), Server0}),
                       3 =:= length(M)
               end).

auto_grow_drained_node(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),

    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% There is only one node in the cluster at the moment
    {ok, Members, _} = ra:members({queue_utils:ra_name(QQ), Server0}),
    ?assertEqual(1, length(Members)),

    add_server_to_cluster(Server1, Server0),
    %% mark server1 as drained, which should mean the node is not a candiate
    %% for qq membership
    rabbit_ct_broker_helpers:mark_as_being_drained(Config, Server1),
    rabbit_ct_helpers:await_condition(
        fun () -> rabbit_ct_broker_helpers:is_being_drained_local_read(Config, Server1) end,
        10000),
    add_server_to_cluster(Server2, Server0),
    timer:sleep(5000),
    %% We have 3 nodes, but one is drained, so it will not be concidered.
    {ok, Members1, _} = ra:members({queue_utils:ra_name(QQ), Server0}),
    ?assertEqual(1, length(Members1)),

    rabbit_ct_broker_helpers:unmark_as_being_drained(Config, Server1),
    rabbit_ct_helpers:await_condition(
        fun () -> not rabbit_ct_broker_helpers:is_being_drained_local_read(Config, Server1) end,
        10000),
    %% We have 3 nodes, none is being drained, so we should grow membership to 3
    wait_until(fun() ->
                       {ok, M, _} = ra:members({queue_utils:ra_name(QQ), Server0}),
                       3 =:= length(M)
               end).


auto_shrink(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    add_server_to_cluster(Server1, Server0),
    add_server_to_cluster(Server2, Server0),

    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    wait_until(fun() ->
                       {ok, M, _} = ra:members({queue_utils:ra_name(QQ),
                                                Server0}),
                       3 =:= length(M)
               end),
    ok = rabbit_control_helper:command(stop_app, Server2),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_cluster, forget_member,
                                      [Server2, false]),
    %% with one node 'forgotten', eventually the membership will shrink to 2
    wait_until(fun() ->
                       {ok, M, _} = ra:members({queue_utils:ra_name(QQ),
                                                Server0}),
                       2 =:= length(M)
               end).



add_server_to_cluster(Server, Leader) ->
    ok = rabbit_control_helper:command(stop_app, Server),
    ok = rabbit_control_helper:command(join_cluster, Server, [atom_to_list(Leader)], []),
    rabbit_control_helper:command(start_app, Server).

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

wait_until(Condition) ->
    wait_until(Condition, 60).

wait_until(Condition, 0) ->
    ?assertEqual(true, Condition());
wait_until(Condition, N) ->
    case Condition() of
        true ->
            ok;
        _ ->
            timer:sleep(500),
            wait_until(Condition, N - 1)
    end.


delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].
