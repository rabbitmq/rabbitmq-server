%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(quorum_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, single_node},
      {group, clustered}
    ].

groups() ->
    [
     {single_node, [], all_tests()},
     {clustered, [], [
                      {cluster_size_2, [], all_tests()},
                      {cluster_size_3, [], [recover_from_single_failure,
                                            recover_from_multiple_failures,
                                            leadership_takeover,
                                            delete_declare,
                                            metrics_cleanup_on_leadership_takeover,
                                            metrics_cleanup_on_leader_crash]}
                     ]}
    ].

all_tests() ->
    [
     declare_args,
     declare_invalid_args,
     start_queue,
     stop_queue,
     restart_queue,
     restart_all_types,
     stop_start_rabbit_app,
     publish,
     publish_and_restart,
     consume,
     consume_from_empty_queue,
     consume_and_autoack,
     subscribe,
     subscribe_with_autoack,
     consume_and_ack,
     consume_and_multiple_ack,
     subscribe_and_ack,
     subscribe_and_multiple_ack,
     consume_and_requeue_nack,
     consume_and_requeue_multiple_nack,
     subscribe_and_requeue_nack,
     subscribe_and_requeue_multiple_nack,
     consume_and_nack,
     consume_and_multiple_nack,
     subscribe_and_nack,
     subscribe_and_multiple_nack,
     %% publisher_confirms,
     dead_letter_to_classic_queue,
     dead_letter_to_quorum_queue,
     dead_letter_from_classic_to_quorum_queue,
     cleanup_queue_state_on_channel_after_publish,
     cleanup_queue_state_on_channel_after_subscribe,
     basic_cancel,
     purge
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(single_node, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 1}]);
init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(cluster_size_2, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 2}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
                                          [fun merge_app_env/1 ] ++
                                          rabbit_ct_broker_helpers:setup_steps() ++
                                              rabbit_ct_client_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:rpc(
           Config2, 0, application, set_env,
           [rabbit, channel_queue_cleanup_interval, 100]),
    Config2.

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(Config, {rabbit, [{core_metrics_gc_interval, 100}]}).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

declare_args(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    LQ = <<"quorum-q">>,
    declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    assert_queue_type(Node, LQ, quorum),

    DQ = <<"classic-q">>,
    declare(Ch, DQ, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    assert_queue_type(Node, DQ, classic),

    DQ2 = <<"classic-q2">>,
    declare(Ch, DQ2),
    assert_queue_type(Node, DQ2, classic).

declare_invalid_args(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    LQ = <<"quorum-q">>,

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Node),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-expires">>, long, 2000}])),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Node),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-message-ttl">>, long, 2000}])),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Node),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-max-length">>, long, 2000}])),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Node),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-max-length-bytes">>, long, 2000}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Node),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-max-priority">>, long, 2000}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Node),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-overflow">>, longstr, <<"drop-head">>}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Node),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-queue-mode">>, longstr, <<"lazy">>}])).

start_queue(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    LQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Check that the application and one ra node are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Node, application, which_applications, []))),
    ?assertMatch([_], rpc:call(Node, supervisor, which_children, [ra_nodes_sup])),

    %% Test declare an existing queue
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Test declare an existing queue with different arguments
    ?assertExit(_, declare(Ch, LQ, [])),

    %% Check that the application and process are still up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Node, application, which_applications, []))),
    ?assertMatch([_], rpc:call(Node, supervisor, which_children, [ra_nodes_sup])).

stop_queue(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    LQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Check that the application and one ra node are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Node, application, which_applications, []))),
    ?assertMatch([_], rpc:call(Node, supervisor, which_children, [ra_nodes_sup])),

    %% Delete the quorum queue
    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch, #'queue.delete'{queue = LQ})),
    %% Check that the application and process are down
    wait_until(fun() ->
                       [] == rpc:call(Node, supervisor, which_children, [ra_nodes_sup])
               end),
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Node, application, which_applications, []))).

restart_queue(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    LQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node),

    %% Check that the application and one ra node are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Node, application, which_applications, []))),
    ?assertMatch([_], rpc:call(Node, supervisor, which_children, [ra_nodes_sup])).

restart_all_types(Config) ->
    %% Test the node restart with both types of queues (quorum and classic) to
    %% ensure there are no regressions
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ1 = <<"quorum-q1">>,
    ?assertEqual({'queue.declare_ok', QQ1, 0, 0},
                 declare(Ch, QQ1, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    QQ2 = <<"quorum-q2">>,
    ?assertEqual({'queue.declare_ok', QQ2, 0, 0},
                 declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    CQ1 = <<"classic-q1">>,
    ?assertEqual({'queue.declare_ok', CQ1, 0, 0}, declare(Ch, CQ1, [])),
    rabbit_ct_client_helpers:publish(Ch, CQ1, 1),
    CQ2 = <<"classic-q2">>,
    ?assertEqual({'queue.declare_ok', CQ2, 0, 0}, declare(Ch, CQ2, [])),
    rabbit_ct_client_helpers:publish(Ch, CQ2, 1),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node),

    %% Check that the application and two ra nodes are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Node, application, which_applications, []))),
    ?assertMatch([_,_], rpc:call(Node, supervisor, which_children, [ra_nodes_sup])),
    %% Check the classic queues restarted correctly
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Node),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ1, no_ack = false}),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ2, no_ack = false}).

stop_start_rabbit_app(Config) ->
    %% Test start/stop of rabbit app with both types of queues (quorum and
    %%  classic) to ensure there are no regressions
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ1 = <<"quorum-q1">>,
    ?assertEqual({'queue.declare_ok', QQ1, 0, 0},
                 declare(Ch, QQ1, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    QQ2 = <<"quorum-q2">>,
    ?assertEqual({'queue.declare_ok', QQ2, 0, 0},
                 declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    CQ1 = <<"classic-q1">>,
    ?assertEqual({'queue.declare_ok', CQ1, 0, 0}, declare(Ch, CQ1, [])),
    rabbit_ct_client_helpers:publish(Ch, CQ1, 1),
    CQ2 = <<"classic-q2">>,
    ?assertEqual({'queue.declare_ok', CQ2, 0, 0}, declare(Ch, CQ2, [])),
    rabbit_ct_client_helpers:publish(Ch, CQ2, 1),

    rabbit_control_helper:command(stop_app, Node),
    %% Check the ra application has stopped (thus its supervisor and queues)
    ?assertMatch(false, lists:keyfind(ra, 1,
                                      rpc:call(Node, application, which_applications, []))),

    rabbit_control_helper:command(start_app, Node),

    %% Check that the application and two ra nodes are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Node, application, which_applications, []))),
    ?assertMatch([_,_], rpc:call(Node, supervisor, which_children, [ra_nodes_sup])),
    %% Check the classic queues restarted correctly
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Node),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ1, no_ack = false}),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ2, no_ack = false}).

publish(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

publish_and_restart(Config) ->
    %% Test the node restart with both types of queues (quorum and classic) to
    %% ensure there are no regressions
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node),

    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    publish(rabbit_ct_client_helpers:open_channel(Config, Node), QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 2),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

consume(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

consume_and_autoack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    consume(Ch, QQ, true),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

consume_from_empty_queue(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    consume_empty(Ch, QQ, false).

subscribe(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, false),
    receive_basic_deliver(false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

subscribe_with_autoack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 2),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, true),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

consume_and_ack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

consume_and_multiple_ack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    _ = consume(Ch, QQ, false),
    _ = consume(Ch, QQ, false),
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 3),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                       multiple     = true}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

subscribe_and_ack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0)
    end.

subscribe_and_multiple_ack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, false),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 3),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                               multiple     = true}),
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0)
    end.

consume_and_requeue_nack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 2),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 2),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

consume_and_requeue_multiple_nack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    _ = consume(Ch, QQ, false),
    _ = consume(Ch, QQ, false),
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 3),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = true,
                                        requeue      = true}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

consume_and_nack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

consume_and_multiple_nack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    _ = consume(Ch, QQ, false),
    _ = consume(Ch, QQ, false),
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 3),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = true,
                                        requeue      = false}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

subscribe_and_requeue_multiple_nack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, false),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 3),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = true,
                                                requeue      = true}),
            receive_basic_deliver(true),
            receive_basic_deliver(true),
            receive
                {#'basic.deliver'{delivery_tag = DeliveryTag1,
                                  redelivered  = true}, _} ->
                    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
                    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 3),
                    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1,
                                                       multiple     = true}),
                    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
                    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0)
            end
    end.

subscribe_and_requeue_nack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = true}),
            receive
                {#'basic.deliver'{delivery_tag = DeliveryTag1,
                                  redelivered  = true}, _} ->
                    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
                    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
                    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1}),
                    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
                    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0)
            end
    end.

subscribe_and_nack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = false}),
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0)
    end.

subscribe_and_multiple_nack(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, false),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 3),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = true,
                                                requeue      = false}),
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0)
    end.
    
publisher_confirms(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    amqp_channel:wait_for_confirms(Ch),
    ct:pal("WAIT FOR CONFIRMS ~n", []).

dead_letter_to_classic_queue(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    CQ = <<"classic-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, CQ}
                                 ])),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0), 
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),
    _ = consume(Ch, CQ, false).

dead_letter_to_quorum_queue(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    QQ2 = <<"quorum-q2">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, QQ2}
                                 ])),
    ?assertEqual({'queue.declare_ok', QQ2, 0, 0},
                 declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_ready(Nodes, '%2F_quorum-q2', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q2', 0),
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_ready(Nodes, '%2F_quorum-q2', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q2', 0),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_ready(Nodes, '%2F_quorum-q2', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q2', 0),
    _ = consume(Ch, QQ2, false).

dead_letter_from_classic_to_quorum_queue(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    CQ = <<"classic-q">>,
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, QQ}
                                 ])),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, CQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),
    DeliveryTag = consume(Ch, CQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"0">>, <<"1">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    _ = consume(Ch, QQ, false).

cleanup_queue_state_on_channel_after_publish(Config) ->
    %% Declare/delete the queue in one channel and publish on a different one,
    %% to verify that the cleanup is propagated through channels
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Node),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch1, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch2, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    [NCh1, NCh2] = rpc:call(Node, rabbit_channel, list, []),
    %% Check the channel state contains the state for the quorum queue on channel 1 and 2
    wait_for_cleanup(Node, NCh1, 1),
    wait_for_cleanup(Node, NCh2, 1),
    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch1, #'queue.delete'{queue = QQ})),
    wait_until(fun() ->
                       [] == rpc:call(Node, supervisor, which_children, [ra_nodes_sup])
               end),
    %% Check that all queue states have been cleaned
    wait_for_cleanup(Node, NCh1, 0),
    wait_for_cleanup(Node, NCh2, 0).

cleanup_queue_state_on_channel_after_subscribe(Config) ->
    %% Declare/delete the queue and publish in one channel, while consuming on a
    %% different one to verify that the cleanup is propagated through channels
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Node),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch1, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch1, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch2, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
            amqp_channel:cast(Ch2, #'basic.ack'{delivery_tag = DeliveryTag,
                                                multiple     = true}),
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0)
    end,
    [NCh1, NCh2] = rpc:call(Node, rabbit_channel, list, []),
    %% Check the channel state contains the state for the quorum queue on channel 1 and 2
    wait_for_cleanup(Node, NCh1, 1),
    wait_for_cleanup(Node, NCh2, 1),
    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch1, #'queue.delete'{queue = QQ})),
    wait_until(fun() ->
                       [] == rpc:call(Node, supervisor, which_children, [ra_nodes_sup])
               end),
    %% Check that all queue states have been cleaned
    wait_for_cleanup(Node, NCh1, 0),
    wait_for_cleanup(Node, NCh2, 0).

recover_from_single_failure(Config) ->
    [Node, Node1, Node2] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node2),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready([Node, Node1], '%2F_quorum-q', 3),
    wait_for_messages_pending_ack([Node, Node1], '%2F_quorum-q', 0),

    ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

recover_from_multiple_failures(Config) ->
    [Node, Node1, Node2] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node2),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    wait_for_messages_ready([Node], '%2F_quorum-q', 3),
    wait_for_messages_pending_ack([Node], '%2F_quorum-q', 0),

    ok = rabbit_ct_broker_helpers:start_node(Config, Node1),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node2),

    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

leadership_takeover(Config) ->
    %% Kill nodes in succession forcing the takeover of leadership, and all messages that
    %% are in the queue.
    [Node, Node1, Node2] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node2),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    wait_for_messages_ready([Node], '%2F_quorum-q', 3),
    wait_for_messages_pending_ack([Node], '%2F_quorum-q', 0),

    ok = rabbit_ct_broker_helpers:start_node(Config, Node1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node),

    wait_for_messages_ready([Node2, Node], '%2F_quorum-q', 3),
    wait_for_messages_pending_ack([Node2, Node], '%2F_quorum-q', 0),

    ok = rabbit_ct_broker_helpers:start_node(Config, Node1),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 3),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

metrics_cleanup_on_leadership_takeover(Config) ->
    %% Queue core metrics should be deleted from a node once the leadership is transferred
    %% to another follower
    [Node, _, _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    wait_for_messages_ready([Node], '%2F_quorum-q', 3),
    wait_for_messages_pending_ack([Node], '%2F_quorum-q', 0),
    {ok, _, {_, Leader}} = ra:members({'%2F_quorum-q', Node}),
    QRes = rabbit_misc:r(<<"/">>, queue, QQ),
    wait_until(
      fun() ->
              case rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]) of
                  [{QRes, 3, 0, 3, _}] -> true;
                  _ -> false
              end
      end),
    force_leader_change(Leader, Nodes),
    [] = rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]),
    ok.

metrics_cleanup_on_leader_crash(Config) ->
    %% Queue core metrics should be deleted from a node once the leadership is transferred
    %% to another follower
    [Node, _, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    wait_for_messages_ready([Node], '%2F_quorum-q', 3),
    wait_for_messages_pending_ack([Node], '%2F_quorum-q', 0),
    {ok, _, {Name, Leader}} = ra:members({'%2F_quorum-q', Node}),
    QRes = rabbit_misc:r(<<"/">>, queue, QQ),
    wait_until(
      fun() ->
              case rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]) of
                  [{QRes, 3, 0, 3, _}] -> true;
                  _ -> false
              end
      end),
    Pid = rpc:call(Leader, erlang, whereis, [Name]),
    rpc:call(Leader, erlang, exit, [Pid, kill]),
    wait_until(
      fun() ->
              [] == rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes])
      end),
    ok.

delete_declare(Config) ->
    %% Delete cluster in ra is asynchronous, we have to ensure that we handle that in rmq
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch, #'queue.delete'{queue = QQ})),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Ensure that is a new queue and it's empty
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0).

basic_cancel(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{}, _} ->
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 0),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1),
            amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = <<"ctag">>}),
            wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
            wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0)
    end.

purge(Config) ->
    [Node | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 2),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    _DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 1),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 1), 
    {'queue.purge_ok', 2} = amqp_channel:call(Ch, #'queue.purge'{queue = QQ}),
    wait_for_messages_pending_ack(Nodes, '%2F_quorum-q', 0),
    wait_for_messages_ready(Nodes, '%2F_quorum-q', 0).

%%----------------------------------------------------------------------------

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           arguments = Args}).

assert_queue_type(Node, Q, Expected) ->
    Actual = get_queue_type(Node, Q),
    Expected = Actual.

get_queue_type(Node, Q) ->
    QNameRes = rabbit_misc:r(<<"/">>, queue, Q),
    {ok, AMQQueue} =
        rpc:call(Node, rabbit_amqqueue, lookup, [QNameRes]),
    AMQQueue#amqqueue.type.

wait_for_messages(Config, Stats) ->
    wait_for_messages(Config, lists:sort(Stats), 60).

wait_for_messages(Config, Stats, 0) ->
    ?assertEqual(Stats,
                 lists:sort(
                   filter_queues(Stats,
                                 rabbit_ct_broker_helpers:rabbitmqctl_list(
                                   Config, 0, ["list_queues", "name", "messages", "messages_ready",
                                               "messages_unacknowledged"]))));
wait_for_messages(Config, Stats, N) ->
    case lists:sort(
           filter_queues(Stats,
                         rabbit_ct_broker_helpers:rabbitmqctl_list(
                           Config, 0, ["list_queues", "name", "messages", "messages_ready",
                                       "messages_unacknowledged"]))) of
        Stats0 when Stats0 == Stats ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_messages(Config, Stats, N - 1)
    end.

filter_queues(Expected, Got) ->
    Keys = [K || [K, _, _, _] <- Expected],
    lists:filter(fun([K, _, _, _]) ->
                         lists:member(K, Keys)
                 end, Got).

publish(Ch, Queue) ->
    ok = amqp_channel:call(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = <<"msg">>}).

consume(Ch, Queue, NoAck) ->
    {GetOk, _} = Reply = amqp_channel:call(Ch, #'basic.get'{queue = Queue,
                                                            no_ack = NoAck}),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg">>}}, Reply),
    GetOk#'basic.get_ok'.delivery_tag.

consume_empty(Ch, Queue, NoAck) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Queue,
                                                    no_ack = NoAck})).

subscribe(Ch, Queue, NoAck) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end.

receive_basic_deliver(Redelivered) ->
    receive
        {#'basic.deliver'{redelivered = R}, _} when R == Redelivered ->
            ok
    end.

wait_for_cleanup(Node, Channel, Number) ->
    wait_for_cleanup(Node, Channel, Number, 60).

wait_for_cleanup(Node, Channel, Number, 0) ->
    ?assertEqual(Number, length(rpc:call(Node, rabbit_channel, list_queue_states, [Channel])));
wait_for_cleanup(Node, Channel, Number, N) ->
    case length(rpc:call(Node, rabbit_channel, list_queue_states, [Channel])) of
        Length when Number == Length ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_cleanup(Node, Channel, Number, N - 1)
    end.


wait_for_messages_ready(Nodes, QName, Ready) ->
    wait_for_messages(Nodes, QName, Ready, fun ra_fifo:query_messages_ready/1, 60).

wait_for_messages_pending_ack(Nodes, QName, Ready) ->
    wait_for_messages(Nodes, QName, Ready, fun ra_fifo:query_messages_checked_out/1, 60).

wait_for_messages(Nodes, QName, Number, Fun, 0) ->
    Msgs = dirty_query(Nodes, QName, Fun),
    Totals = lists:map(fun(M) -> maps:size(M) end, Msgs),
    ?assertEqual(Totals, [Number || _ <- lists:seq(1, length(Nodes))]);
wait_for_messages(Nodes, QName, Number, Fun, N) ->
    Msgs = dirty_query(Nodes, QName, Fun),
    case lists:all(fun(M) ->  maps:size(M) == Number end, Msgs) of
        true ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_messages(Nodes, QName, Number, Fun, N - 1)
    end.

dirty_query(Nodes, QName, Fun) ->
    lists:map(
      fun(N) ->
              {ok, {_, Msgs}, _} = rpc:call(N, ra, dirty_query, [{QName, N}, Fun]),
              Msgs
      end, Nodes).

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

force_leader_change(Leader, Nodes) ->
    [F1, _] = Nodes -- [Leader],
    ok = rpc:call(F1, ra, trigger_election, [{'%2F_quorum-q', F1}]),
    case ra:members({'%2F_quorum-q', Leader}) of
        {ok, _, {_, Leader}} ->
            %% Leader has been re-elected
            force_leader_change(Leader, Nodes);
        {ok, _, _} ->
            %% Leader has changed
            ok
    end.
