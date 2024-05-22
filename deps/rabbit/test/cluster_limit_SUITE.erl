%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.


-module(cluster_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([nowarn_export_all, export_all]).


all() ->
    [
     {group, clustered}
    ].

groups() ->
    [
     {clustered, [],
      [
       {size_2, [], [queue_limit]}
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000},
                                   {cluster_queue_limit, 3}]}),
    rabbit_ct_helpers:run_setup_steps(Config1, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).
init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(Group, Config) ->
    ClusterSize = case Group of
                      size_2 -> 2
                  end,
    IsMixed = rabbit_ct_helpers:is_mixed_versions(),
    case ClusterSize of
        2 when IsMixed ->
            {skip, "cluster size 2 isn't mixed versions compatible"};
        _ ->
            Config1 = rabbit_ct_helpers:set_config(Config,
                                                   [{rmq_nodes_count, ClusterSize},
                                                    {rmq_nodename_suffix, Group},
                                                    {tcp_ports_base}]),
            Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
            rabbit_ct_helpers:run_steps(Config1b,
                                        [fun merge_app_env/1 ] ++
                                            rabbit_ct_broker_helpers:setup_steps())
    end.

end_per_group(clustered, Config) ->
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
                                            {alt_2_queue_name, <<Q/binary, "_alt_2">>},
                                            {over_limit_queue_name, <<Q/binary, "_over_limit">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

queue_limit(Config) ->
    [Server0, Server1] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q1 = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q1, 0, 0},
                 declare(Ch, Q1)),

    Q2 = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q2, 0, 0},
                 declare(Ch, Q2)),

    Q3 = ?config(alt_2_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q3, 0, 0},
                 declare(Ch, Q3)),
    Q4 = ?config(over_limit_queue_name, Config),
    ExpectedError = list_to_binary(io_lib:format("PRECONDITION_FAILED - cannot declare queue '~s': queue limit in cluster (3) is reached", [Q4])),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(Ch, Q4)),

    %% Trying the second server, in the cluster, but no queues on it,
    %% but should still fail as the limit is cluster wide.
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(Ch2, Q4)),

    %Trying other types of queues
    ChQQ = rabbit_ct_client_helpers:open_channel(Config, Server0),
    ChStream = rabbit_ct_client_helpers:open_channel(Config, Server1),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(ChQQ, Q4, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(ChStream, Q4, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    ok.

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].
