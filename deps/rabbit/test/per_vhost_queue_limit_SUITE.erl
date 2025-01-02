%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(per_vhost_queue_limit_SUITE).

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-import(rabbit_ct_client_helpers, [open_unmanaged_connection/3,
                                   close_connection_and_channel/2]).

-define(AWAIT_TIMEOUT, 30000).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [], [
                  {cluster_size_1, [], cluster_size_1_tests()},
                  {cluster_size_2, [], cluster_size_2_tests()}
                 ]}
    ].

cluster_size_1_tests() ->
    [
     most_basic_single_node_queue_count,
     single_node_single_vhost_queue_count,
     single_node_multiple_vhosts_queue_count,
     single_node_single_vhost_limit,
     single_node_single_vhost_zero_limit,
     single_node_single_vhost_limit_with_durable_named_queue,
     single_node_single_vhost_zero_limit_with_durable_named_queue,
     single_node_single_vhost_limit_with_queue_ttl,
     single_node_single_vhost_limit_with_redeclaration
    ].

cluster_size_2_tests() ->
    [
     most_basic_cluster_queue_count,
     cluster_multiple_vhosts_queue_count,
     cluster_multiple_vhosts_limit,
     cluster_multiple_vhosts_zero_limit,
     cluster_multiple_vhosts_limit_with_durable_named_queue,
     cluster_multiple_vhosts_zero_limit_with_durable_named_queue,
     cluster_node_restart_queue_count
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 8}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(tests, Config) ->
    Config;
init_per_group(cluster_size_1 = Group, Config) ->
    init_per_multinode_group(Group, Config, 1);
init_per_group(cluster_size_2 = Group, Config) ->
    init_per_multinode_group(Group, Config, 2).

init_per_multinode_group(_Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                    rabbit_ct_client_helpers:setup_steps()).

end_per_group(tests, Config) ->
    Config;
end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(vhost_limit_after_node_renamed = Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(vhost_limit_after_node_renamed = Testcase, Config) ->
    Config1 = ?config(save_config, Config),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

most_basic_single_node_queue_count(Config) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),
    Conn     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_exclusive_queues(Ch, 10),
    ?awaitMatch(10, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),
    close_connection_and_channel(Conn, Ch),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

single_node_single_vhost_queue_count(Config) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),
    Conn     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_exclusive_queues(Ch, 10),
    ?awaitMatch(10, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),
    declare_durable_queues(Ch, 10),
    ?awaitMatch(20, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),
    delete_durable_queues(Ch, 10),
    ?awaitMatch(10, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),
    close_connection_and_channel(Conn, Ch),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

single_node_multiple_vhosts_queue_count(Config) ->
    VHost1 = <<"queue-limits1">>,
    VHost2 = <<"queue-limits2">>,
    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?awaitMatch(0, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost2), ?AWAIT_TIMEOUT),

    Conn1     = open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    Conn2     = open_unmanaged_connection(Config, 0, VHost2),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    declare_exclusive_queues(Ch1, 10),
    ?awaitMatch(10, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    declare_durable_queues(Ch1, 10),
    ?awaitMatch(20, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    delete_durable_queues(Ch1, 10),
    ?awaitMatch(10, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    declare_exclusive_queues(Ch2, 30),
    ?awaitMatch(30, count_queues_in(Config, VHost2), ?AWAIT_TIMEOUT),
    close_connection_and_channel(Conn1, Ch1),
    ?awaitMatch(0, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    close_connection_and_channel(Conn2, Ch2),
    ?awaitMatch(0, count_queues_in(Config, VHost2), ?AWAIT_TIMEOUT),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).

single_node_single_vhost_limit(Config) ->
    single_node_single_vhost_limit_with(Config, 5),
    single_node_single_vhost_limit_with(Config, 10).
single_node_single_vhost_zero_limit(Config) ->
    single_node_single_vhost_zero_limit_with(Config, #'queue.declare'{queue     = <<"">>,
                                                                      exclusive = true}).

single_node_single_vhost_limit_with_durable_named_queue(Config) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),

    set_vhost_queue_limit(Config, VHost, 3),
    Conn     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),

    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch, #'queue.declare'{queue     = <<"Q1">>,
                                                               exclusive = false,
                                                               durable   = true}),
    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch, #'queue.declare'{queue     = <<"Q2">>,
                                                               exclusive = false,
                                                               durable   = true}),
    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch, #'queue.declare'{queue     = <<"Q3">>,
                                                               exclusive = false,
                                                               durable   = true}),

    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch, #'queue.declare'{queue     = <<"Q4">>,
                                                    exclusive = false,
                                                    durable   = true})
     end),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

single_node_single_vhost_zero_limit_with_durable_named_queue(Config) ->
    single_node_single_vhost_zero_limit_with(Config, #'queue.declare'{queue     = <<"Q4">>,
                                                                      exclusive = false,
                                                                      durable   = true}).

single_node_single_vhost_limit_with(Config, WatermarkLimit) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),

    set_vhost_queue_limit(Config, VHost, 3),
    Conn     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),

    set_vhost_queue_limit(Config, VHost, WatermarkLimit),
    lists:foreach(fun (_) ->
                    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch, #'queue.declare'{queue     = <<"">>,
                                                               exclusive = true})
                  end, lists:seq(1, WatermarkLimit)),

    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch, #'queue.declare'{queue     = <<"">>,
                                                    exclusive = true})
     end),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

single_node_single_vhost_zero_limit_with(Config, QueueDeclare) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),

    Conn1     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),

    set_vhost_queue_limit(Config, VHost, 0),

    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch1, QueueDeclare)
     end),

    Conn2     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    %% lift the limit
    set_vhost_queue_limit(Config, VHost, -1),
    lists:foreach(fun (_) ->
                    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"">>,
                                                               exclusive = true})
                  end, lists:seq(1, 100)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).


single_node_single_vhost_limit_with_queue_ttl(Config) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),

    Conn1     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),

    set_vhost_queue_limit(Config, VHost, 3),

    lists:foreach(fun (_) ->
                    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"">>,
                                                                exclusive = true,
                                                                arguments = [{<<"x-expires">>, long, 2000}]})
                  end, lists:seq(1, 3)),


    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"">>,
                                                     exclusive = true})
     end),

    Conn2     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    %% wait for the queues to expire
    ?awaitMatch(
       [],
       rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list, []),
       ?AWAIT_TIMEOUT),

    #'queue.declare_ok'{queue = _} =
        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"">>,
                                                exclusive = true}),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).


single_node_single_vhost_limit_with_redeclaration(Config) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost), ?AWAIT_TIMEOUT),

    set_vhost_queue_limit(Config, VHost, 3),
    Conn1     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),

    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"Q1">>,
                                                                exclusive = false,
                                                                durable   = true}),
    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"Q2">>,
                                                                exclusive = false,
                                                                durable   = true}),
    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"Q3">>,
                                                                exclusive = false,
                                                                durable   = true}),

    %% can't declare a new queue...
    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"Q4">>,
                                                     exclusive = false,
                                                     durable   = true})
     end),


    Conn2     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    %% ...but re-declarations succeed
    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"Q1">>,
                                                                exclusive = false,
                                                                durable   = true}),
    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"Q2">>,
                                                                exclusive = false,
                                                                durable   = true}),
    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"Q3">>,
                                                                exclusive = false,
                                                                durable   = true}),

    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"Q4">>,
                                                     exclusive = false,
                                                     durable   = true})
     end),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).


most_basic_cluster_queue_count(Config) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),

    Conn1     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    declare_exclusive_queues(Ch1, 10),
    ?awaitMatch(10, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(10, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),

    Conn2     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),
    declare_exclusive_queues(Ch2, 15),
    ?awaitMatch(25, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(25, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),
    close_connection_and_channel(Conn1, Ch1),
    close_connection_and_channel(Conn2, Ch2),
    ?awaitMatch(0, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).

cluster_node_restart_queue_count(Config) ->
    VHost = <<"queue-limits">>,
    set_up_vhost(Config, VHost),
    ?awaitMatch(0, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),

    Conn1     = open_unmanaged_connection(Config, 0, VHost),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    declare_exclusive_queues(Ch1, 10),
    ?awaitMatch(10, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(10, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:restart_broker(Config, 0),
    ?awaitMatch(0, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),

    Conn2     = open_unmanaged_connection(Config, 1, VHost),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),
    declare_exclusive_queues(Ch2, 15),
    ?awaitMatch(15, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(15, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),

    declare_durable_queues(Ch2, 10),
    ?awaitMatch(25, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(25, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:restart_broker(Config, 1),

    ?awaitMatch(10, count_queues_in(Config, VHost, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(10, count_queues_in(Config, VHost, 1), ?AWAIT_TIMEOUT),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost).


cluster_multiple_vhosts_queue_count(Config) ->
    VHost1 = <<"queue-limits1">>,
    VHost2 = <<"queue-limits2">>,
    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?awaitMatch(0, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost2), ?AWAIT_TIMEOUT),

    Conn1     = open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    declare_exclusive_queues(Ch1, 10),
    ?awaitMatch(10, count_queues_in(Config, VHost1, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(10, count_queues_in(Config, VHost1, 1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0,  count_queues_in(Config, VHost2, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(0,  count_queues_in(Config, VHost2, 1), ?AWAIT_TIMEOUT),

    Conn2     = open_unmanaged_connection(Config, 0, VHost2),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),
    declare_exclusive_queues(Ch2, 15),
    ?awaitMatch(15, count_queues_in(Config, VHost2, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(15, count_queues_in(Config, VHost2, 1), ?AWAIT_TIMEOUT),
    close_connection_and_channel(Conn1, Ch1),
    close_connection_and_channel(Conn2, Ch2),
    ?awaitMatch(0, count_queues_in(Config, VHost1, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost1, 1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost2, 0), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost2, 1), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).

cluster_multiple_vhosts_limit(Config) ->
    cluster_multiple_vhosts_limit_with(Config, 10),
    cluster_multiple_vhosts_limit_with(Config, 20).

cluster_multiple_vhosts_limit_with(Config, WatermarkLimit) ->
    VHost1 = <<"queue-limits1">>,
    VHost2 = <<"queue-limits2">>,
    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),
    ?awaitMatch(0, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost2), ?AWAIT_TIMEOUT),

    set_vhost_queue_limit(Config, VHost1, 3),
    set_vhost_queue_limit(Config, VHost2, 3),

    Conn1     = open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    set_vhost_queue_limit(Config, VHost1, WatermarkLimit),

    lists:foreach(fun (_) ->
                    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"">>,
                                                               exclusive = true})
                  end, lists:seq(1, WatermarkLimit)),

    Conn2     = open_unmanaged_connection(Config, 1, VHost2),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),
    set_vhost_queue_limit(Config, VHost2, WatermarkLimit),

    lists:foreach(fun (_) ->
                    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"">>,
                                                               exclusive = true})
                  end, lists:seq(1, WatermarkLimit)),

    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"">>,
                                                    exclusive = true})
     end),
    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"">>,
                                                    exclusive = true})
     end),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).


cluster_multiple_vhosts_zero_limit(Config) ->
    cluster_multiple_vhosts_zero_limit_with(Config, #'queue.declare'{queue     = <<"">>,
                                                                      exclusive = true}).

cluster_multiple_vhosts_limit_with_durable_named_queue(Config) ->
    VHost1 = <<"queue-limits1">>,
    VHost2 = <<"queue-limits2">>,
    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),
    ?awaitMatch(0, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost2), ?AWAIT_TIMEOUT),

    set_vhost_queue_limit(Config, VHost1, 3),
    set_vhost_queue_limit(Config, VHost2, 3),

    Conn1     = open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),

    Conn2     = open_unmanaged_connection(Config, 1, VHost2),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    #'queue.declare_ok'{} =
                        amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"Q1">>,
                                                                exclusive = false,
                                                                durable   = true}),
    #'queue.declare_ok'{} =
                        amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"Q2">>,
                                                                exclusive = false,
                                                                durable   = true}),
    #'queue.declare_ok'{} =
                        amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"Q3">>,
                                                                exclusive = false,
                                                                durable   = true}),

    #'queue.declare_ok'{} =
                        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"Q1">>,
                                                                exclusive = false,
                                                                durable   = true}),
    #'queue.declare_ok'{} =
                        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"Q2">>,
                                                                exclusive = false,
                                                                durable   = true}),
    #'queue.declare_ok'{} =
                        amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"Q3">>,
                                                                exclusive = false,
                                                                durable   = true}),

    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch1, #'queue.declare'{queue     = <<"Q3">>,
                                                     exclusive = false,
                                                     durable   = true})
     end),
    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch2, #'queue.declare'{queue     = <<"Q3">>,
                                                     exclusive = false,
                                                     durable   = true})
     end),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).

cluster_multiple_vhosts_zero_limit_with_durable_named_queue(Config) ->
    cluster_multiple_vhosts_zero_limit_with(Config, #'queue.declare'{queue     = <<"Q4">>,
                                                                  exclusive = false,
                                                                  durable   = true}).

cluster_multiple_vhosts_zero_limit_with(Config, QueueDeclare) ->
    VHost1 = <<"queue-limits1">>,
    VHost2 = <<"queue-limits2">>,
    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),
    ?awaitMatch(0, count_queues_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_queues_in(Config, VHost2), ?AWAIT_TIMEOUT),

    Conn1     = open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    Conn2     = open_unmanaged_connection(Config, 1, VHost2),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    set_vhost_queue_limit(Config, VHost1, 0),
    set_vhost_queue_limit(Config, VHost2, 0),

    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch1, QueueDeclare)
     end),
    expect_shutdown_due_to_precondition_failed(
     fun () ->
             amqp_channel:call(Ch2, QueueDeclare)
     end),


    Conn3     = open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch3} = amqp_connection:open_channel(Conn3),
    Conn4     = open_unmanaged_connection(Config, 1, VHost2),
    {ok, Ch4} = amqp_connection:open_channel(Conn4),

    %% lift the limits
    set_vhost_queue_limit(Config, VHost1, -1),
    set_vhost_queue_limit(Config, VHost2, -1),
    lists:foreach(fun (_) ->
                    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch3, #'queue.declare'{queue     = <<"">>,
                                                               exclusive = true}),
                    #'queue.declare_ok'{queue = _} =
                        amqp_channel:call(Ch4, #'queue.declare'{queue     = <<"">>,
                                                               exclusive = true})
                  end, lists:seq(1, 400)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).



%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

set_up_vhost(Config, VHost) ->
    rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    set_vhost_queue_limit(Config, VHost, -1).

set_vhost_queue_limit(Config, VHost, Count) ->
    set_vhost_queue_limit(Config, 0, VHost, Count).

set_vhost_queue_limit(Config, NodeIndex, VHost, Count) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    rabbit_ct_broker_helpers:control_action(
      set_vhost_limits, Node,
      ["{\"max-queues\": " ++ integer_to_list(Count) ++ "}"],
      [{"-p", binary_to_list(VHost)}]).

count_queues_in(Config, VHost) ->
    count_queues_in(Config, VHost, 0).
count_queues_in(Config, VHost, NodeIndex) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_amqqueue,
                                 count, [VHost]).

declare_exclusive_queues(Ch, N) ->
    lists:foreach(fun (_) ->
                          amqp_channel:call(Ch,
                                            #'queue.declare'{queue     = <<"">>,
                                                             exclusive = true})
                  end,
                  lists:seq(1, N)).

declare_durable_queues(Ch, N) ->
    lists:foreach(fun (I) ->
                          amqp_channel:call(Ch,
                                            #'queue.declare'{queue     = durable_queue_name(I),
                                                             exclusive = false,
                                                             durable   = true})
                  end,
                  lists:seq(1, N)).

delete_durable_queues(Ch, N) ->
    lists:foreach(fun (I) ->
                          amqp_channel:call(Ch,
                                            #'queue.delete'{queue = durable_queue_name(I)})
                  end,
                  lists:seq(1, N)).

durable_queue_name(N) when is_integer(N) ->
    iolist_to_binary(io_lib:format("queue-limits-durable-~tp", [N])).

expect_shutdown_due_to_precondition_failed(Thunk) ->
    try
        Thunk(),
        ok
    catch _:{{shutdown, {server_initiated_close, 406, _}}, _} ->
        %% expected
        ok
    end.
