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
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          declare_args,
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
          subscribe_and_ack,
          consume_and_single_nack,
          subscribe_and_single_nack
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

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = 1,
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, ClusterSize},
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

    %% Check that the application and process are still up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Node, application, which_applications, []))),
    ?assertMatch([], rpc:call(Node, supervisor, which_children, [ra_nodes_sup])).

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
    %% Test the node restart with both types of queues (quorum and classic) to
    %% ensure there are no regressions
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>).

publish_and_restart(Config) ->
    %% Test the node restart with both types of queues (quorum and classic) to
    %% ensure there are no regressions
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q1">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node),

    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),
    publish(rabbit_ct_client_helpers:open_channel(Config, Node), QQ),
    wait_for_messages(Config, QQ, <<"2">>, <<"2">>, <<"0">>).

consume(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),
    consume(Ch, QQ, false),
    wait_for_messages(Config, QQ, <<"1">>, <<"0">>, <<"1">>),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>).

consume_and_autoack(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),
    consume(Ch, QQ, true),
    wait_for_messages(Config, QQ, <<"0">>, <<"0">>, <<"0">>),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, QQ, <<"0">>, <<"0">>, <<"0">>).

consume_from_empty_queue(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    consume_empty(Ch, QQ, false).

subscribe(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),
    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = _}, _} ->
            ok
    end,
    wait_for_messages(Config, QQ, <<"1">>, <<"0">>, <<"1">>),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>).

subscribe_with_autoack(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"2">>, <<"2">>, <<"0">>),
    subscribe(Ch, QQ, true),
    receive
        {#'basic.deliver'{delivery_tag = _}, _} ->
            ok
    end,
    receive
        {#'basic.deliver'{delivery_tag = _}, _} ->
            ok
    end,
    wait_for_messages(Config, QQ, <<"0">>, <<"0">>, <<"0">>),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, QQ, <<"0">>, <<"0">>, <<"0">>).

consume_and_ack(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),
    %% TODO we don't store consumer tag for basic.get!!! could it be fixed?
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages(Config, QQ, <<"1">>, <<"0">>, <<"1">>),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
    wait_for_messages(Config, QQ, <<"0">>, <<"0">>, <<"0">>).

subscribe_and_ack(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),
    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok
    end,
    wait_for_messages(Config, QQ, <<"1">>, <<"0">>, <<"1">>),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
    wait_for_messages(Config, QQ, <<"0">>, <<"0">>, <<"0">>).

consume_and_single_nack(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),
    %% TODO we don't store consumer tag for basic.get!!! could it be fixed?
    DeliveryTag = consume(Ch, QQ, false),
    wait_for_messages(Config, QQ, <<"1">>, <<"0">>, <<"1">>),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>).

subscribe_and_single_nack(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    QQ = <<"quorum-q">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>),
    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok
    end,
    wait_for_messages(Config, QQ, <<"1">>, <<"0">>, <<"1">>),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    wait_for_messages(Config, QQ, <<"1">>, <<"1">>, <<"0">>).

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

wait_for_messages(Config, Queue, Msgs, Ready, Unack) ->
    wait_for_messages(Config, Queue, Msgs, Ready, Unack, 60).

wait_for_messages(Config, Queue, Msgs, Ready, Unack, 0) ->
    ?assertEqual([[Queue, Msgs, Ready, Unack]],
                 rabbit_ct_broker_helpers:rabbitmqctl_list(
                   Config, 0, ["list_queues", "name", "messages", "messages_ready",
                               "messages_unacknowledged"]));
wait_for_messages(Config, Queue, Msgs, Ready, Unack, N) ->
    case rabbit_ct_broker_helpers:rabbitmqctl_list(
           Config, 0, ["list_queues", "name", "messages", "messages_ready",
                       "messages_unacknowledged"]) of
        [[Q, M, R, U]] when Q == Queue, M == Msgs, R == Ready, U == Unack ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_messages(Config, Queue, Msgs, Ready, Unack, N - 1)
    end.

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
