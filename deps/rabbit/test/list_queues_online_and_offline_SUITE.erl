%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(list_queues_online_and_offline_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, list_queues_online_and_offline}
    ].

groups() ->
    [
      {list_queues_online_and_offline, [], [
          list_queues_online_and_offline %% Stop node B.
        ]}
    ].

group(_) ->
    [].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [
                                            {rmq_nodename_suffix, Group},
                                            {rmq_nodes_count, 2}
                                           ]),
    rabbit_ct_helpers:run_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                    rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Testcase
%% ---------------------------------------------------------------------------

list_queues_online_and_offline(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    %% Node B will be stopped
    BCh = rabbit_ct_client_helpers:open_channel(Config, B),
    #'queue.declare_ok'{} = amqp_channel:call(ACh, #'queue.declare'{queue = <<"q_a_1">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(ACh, #'queue.declare'{queue = <<"q_a_2">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(BCh, #'queue.declare'{queue = <<"q_b_1">>, durable = true}),
    #'queue.declare_ok'{} = amqp_channel:call(BCh, #'queue.declare'{queue = <<"q_b_2">>, durable = true}),

    rabbit_ct_broker_helpers:rabbitmqctl(Config, B, ["stop"]),

    rabbit_ct_helpers:await_condition(
      fun() ->
              [A] == rpc:call(A, rabbit_mnesia, cluster_nodes, [running])
      end, 60000),

    GotUp = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "--online", "name", "--no-table-headers"])),
    ExpectUp = [[<<"q_a_1">>], [<<"q_a_2">>]],
    ExpectUp = GotUp,

    GotDown = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "--offline", "name", "--no-table-headers"])),
    ExpectDown = [[<<"q_b_1">>], [<<"q_b_2">>]],
    ExpectDown = GotDown,

    GotAll = lists:sort(rabbit_ct_broker_helpers:rabbitmqctl_list(Config, A,
        ["list_queues", "name", "--no-table-headers"])),
    ExpectAll = ExpectUp ++ ExpectDown,
    ExpectAll = GotAll,

    ok.
