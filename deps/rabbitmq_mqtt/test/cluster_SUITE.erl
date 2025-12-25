%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(cluster_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-import(util, [expect_publishes/3,
               connect/3,
               connect/4,
               await_exit/1]).

-import(rabbit_ct_broker_helpers,
        [setup_steps/0,
         teardown_steps/0,
         get_node_config/3,
         rabbitmqctl/3,
         rpc/4, rpc/5,
         stop_node/2
        ]).

-import(rabbit_ct_helpers,
        [eventually/3]).

-define(OPTS, [{connect_timeout, 1},
               {ack_timeout, 1}]).

all() ->
    [
     {group, v4},
     {group, v5}
    ].

groups() ->
    [
     {v4, [], cluster_size_5()},
     {v5, [], cluster_size_5()}
    ].
cluster_size_5() ->
    [
     connection_id_tracking,
     connection_id_tracking_on_nodedown
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      Config,
      {rabbit, [
                {collect_statistics, basic},
                {collect_statistics_interval, 100}
               ]}).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    rabbit_ct_helpers:set_config(
      Config, [{rmq_nodes_count, 5},
               {mqtt_version, Group},
               {start_rmq_with_plugins_disabled, true}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_clustered, true}
      ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
      Config1,
      [fun merge_app_env/1] ++
      setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config2, rabbitmq_mqtt),
    Config2.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

%% Note about running this testsuite in a mixed-versions cluster:
%% All even-numbered nodes will use the same code base when using a
%% secondary Umbrella. Odd-numbered nodes might use an incompatible code
%% base. When cluster-wide client ID tracking was introduced, it was not
%% put behind a feature flag because there was no need for one. Here, we
%% don't have a way to ensure that all nodes participate in client ID
%% tracking. However, those using the same code should. That's why we
%% limit our RPC calls to those nodes.
%%
%% That's also the reason why we use a 5-node cluster: with node 2 and
%% 4 which might not participate, it leaves nodes 1, 3 and 5: thus 3
%% nodes, the minimum to use Ra in proper conditions.

connection_id_tracking(Config) ->
    Id = <<"duplicate-id">>,
    C1 = connect(Id, Config, 0, ?OPTS),
    {ok, _, _} = emqtt:subscribe(C1, <<"TopicA">>, qos0),
    ok = emqtt:publish(C1, <<"TopicA">>, <<"Payload">>),
    ok = expect_publishes(C1, <<"TopicA">>, [<<"Payload">>]),

    %% there's one connection
    assert_connection_count(Config, 4, 1),

    %% connect to the same node (A or 0)
    process_flag(trap_exit, true),
    C2 = connect(Id, Config, 0, ?OPTS),
    await_exit(C1),
    assert_connection_count(Config, 4, 1),

    %% connect to a different node (C or 2)
    C3 = connect(Id, Config, 2, ?OPTS),
    await_exit(C2),
    assert_connection_count(Config, 4, 1),
    ok = emqtt:disconnect(C3).

connection_id_tracking_on_nodedown(Config) ->
    C = connect(<<"simpleClient">>, Config, ?OPTS),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>, qos0),
    ok = emqtt:publish(C, <<"TopicA">>, <<"Payload">>),
    ok = expect_publishes(C, <<"TopicA">>, [<<"Payload">>]),
    assert_connection_count(Config, 4, 1),
    process_flag(trap_exit, true),
    ok = stop_node(Config, 0),
    await_exit(C),
    ok = eventually(?_assertEqual([], util:all_connection_pids(Config)), 500, 4).

%%
%% Helpers
%%

assert_connection_count(_Config, 0, NumElements) ->
    ct:fail("failed to match connection count ~b", [NumElements]);
assert_connection_count(Config, Retries, NumElements) ->
    case util:all_connection_pids(Config) of
        Pids when length(Pids) =:= NumElements ->
            ok;
        Pids ->
            ct:pal("Waiting for ~b connections, got following connections: ~p",
                   [NumElements, Pids]),
            timer:sleep(500),
            assert_connection_count(Config, Retries-1, NumElements)
    end.
