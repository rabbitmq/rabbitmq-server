%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(clustering_events_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(rabbit_ct_helpers, [eventually/3]).
-import(event_recorder,
        [assert_event_type/2,
         assert_event_prop/2]).

-compile(export_all).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  node_added_event,
                  node_deleted_event
                 ]}
    ].

%% -------------------------------------------------------------------
%% Per Suite
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

%%
%% Per Group
%%

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

%%
%% Per Test Case
%%
init_per_testcase(node_added_event = TestCase, Config) ->
    Config1 = configure_cluster_essentials(Config, TestCase, false),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
                                          rabbit_ct_broker_helpers:setup_steps() ++
                                              rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config2, TestCase);
init_per_testcase(node_deleted_event = TestCase, Config) ->
    Config1 = configure_cluster_essentials(Config, TestCase, true),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
                                          rabbit_ct_broker_helpers:setup_steps() ++
                                              rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config2, TestCase).

end_per_testcase(TestCase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
                                          rabbit_ct_client_helpers:teardown_steps() ++
                                              rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, TestCase).

%%
%% Helpers
%%
configure_cluster_essentials(Config, Group, Clustered) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 3},
        {rmq_nodes_clustered, Clustered}
    ]).

node_added_event(Config) ->
    [Server1, Server2, _Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ok = event_recorder:start(Config),
    join_cluster(Server2, Server1),
    E = event_recorder:get_events(Config),
    ok = event_recorder:stop(Config),
    ?assert(lists:any(fun(#event{type = node_added}) ->
                              true;
                         (_) ->
                              false
                      end, E)).

node_deleted_event(Config) ->
    [Server1, Server2, _Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ok = event_recorder:start(Config),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    ok = rabbit_control_helper:command(forget_cluster_node, Server1, [atom_to_list(Server2)],
                                       []),
    E = event_recorder:get_events(Config),
    ok = event_recorder:stop(Config),
    ?assert(lists:any(fun(#event{type = node_deleted}) ->
                              true;
                         (_) ->
                              false
                      end, E)).

join_cluster(Node, Cluster) ->
    ok = rabbit_control_helper:command(stop_app, Node),
    ok = rabbit_control_helper:command(join_cluster, Node, [atom_to_list(Cluster)], []),
    rabbit_control_helper:command(start_app, Node).
