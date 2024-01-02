%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(cli_forget_cluster_node_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(clustering_utils, [
                           assert_cluster_status/2,
                           assert_clustered/1,
                           assert_not_clustered/1
                          ]).

all() ->
    [
      {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_3, [], [
                           forget_cluster_node_with_quorum_queues,
                           forget_cluster_node_with_last_quorum_member
                          ]}
    ].

suite() ->
    [
      %% If a testcase hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 5}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config, {rabbit, [
                          {mnesia_table_loading_retry_limit, 2},
                          {mnesia_table_loading_retry_timeout,1000}
                         ]}),
    rabbit_ct_helpers:run_setup_steps(Config1).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3},
                                          {rmq_nodes_clustered, true}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
        {keep_pid_file_on_exit, true}
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
%% Test cases
%% -------------------------------------------------------------------
forget_cluster_node_with_quorum_queues(Config) ->
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    assert_clustered([Rabbit, Hare, Bunny]),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    QQ1 = <<"quorum-queue-1">>,
    QQ2 = <<"quorum-queue-2">>,
    declare(Ch, QQ1, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    
    ?awaitMatch(Members when length(Members) == 3, get_quorum_members(Rabbit, QQ1), 30000),
    ?awaitMatch(Members when length(Members) == 3, get_quorum_members(Rabbit, QQ2), 30000),

    ?assertEqual(ok, rabbit_control_helper:command(stop_app, Bunny)),
    ?assertEqual(ok, forget_cluster_node(Rabbit, Bunny)),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Rabbit, Hare]},
                          [Rabbit, Hare]),
    ?awaitMatch(Members when length(Members) == 2, get_quorum_members(Rabbit, QQ1), 30000),
    ?awaitMatch(Members when length(Members) == 2, get_quorum_members(Rabbit, QQ2), 30000).

forget_cluster_node_with_last_quorum_member(Config) ->
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    assert_clustered([Rabbit, Hare, Bunny]),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Bunny),
    QQ1 = <<"quorum-queue-1">>,
    QQ2 = <<"quorum-queue-2">>,
    declare(Ch, QQ1, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                      {<<"x-quorum-initial-group-size">>, long, 1}]),
    declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                      {<<"x-quorum-initial-group-size">>, long, 1}]),
    
    ?awaitMatch(Members when length(Members) == 1, get_quorum_members(Rabbit, QQ1), 30000),
    ?awaitMatch(Members when length(Members) == 1, get_quorum_members(Rabbit, QQ2), 30000),

    ?assertEqual(ok, rabbit_control_helper:command(stop_app, Bunny)),
    ?assertMatch({error, 69, _}, forget_cluster_node(Rabbit, Bunny)),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Rabbit, Hare]},
                          [Rabbit, Hare]),
    ?awaitMatch(Members when length(Members) == 1, get_quorum_members(Rabbit, QQ1), 30000),
    ?awaitMatch(Members when length(Members) == 1, get_quorum_members(Rabbit, QQ2), 30000).

forget_cluster_node(Node, Removee) ->
    rabbit_control_helper:command(forget_cluster_node, Node, [atom_to_list(Removee)],
                                  []).

get_quorum_members(Server, Q) ->
    Info = rpc:call(Server, rabbit_quorum_queue, infos, [rabbit_misc:r(<<"/">>, queue, Q)]),
    proplists:get_value(members, Info).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).
