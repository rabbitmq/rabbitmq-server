%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(direct_exchange_routing_v2_SUITE).

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         remove_binding_unbind_queue/1,
         remove_binding_delete_queue/1,
         remove_binding_delete_queue_multiple/1,
         remove_binding_delete_exchange/1,
         recover_bindings/1,
         route_exchange_to_exchange/1,
         keep_binding_node_down_durable_queue/1,
         join_cluster/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

%% in file direct_exchange_routing_v2_SUITE_data/definition.json:
%% number of bindings
-define(NUM_BINDINGS_TO_DIRECT_ECHANGE, 670).
%% number of bindings where the source exchange is a direct exchange
%% and both source exchange and destination queue are durable
-define(NUM_BINDINGS_TO_DIRECT_ECHANGE_DURABLE, 420).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_2},
     {group, unclustered_cluster_size_2}
    ].

groups() ->
    [
     {cluster_size_1, [],
      [remove_binding_unbind_queue,
       remove_binding_delete_queue,
       remove_binding_delete_queue_multiple,
       remove_binding_delete_exchange,
       recover_bindings,
       route_exchange_to_exchange]},
     {cluster_size_2, [],
      [keep_binding_node_down_durable_queue]},
     {unclustered_cluster_size_2, [],
      [join_cluster]}
    ].

suite() -> [{timetrap, {minutes, 3}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group = cluster_size_1, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 1}]),
    start_broker(Group, Config);
init_per_group(Group = cluster_size_2, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 2}]),
    start_broker(Group, Config);
init_per_group(Group = cluster_size_3, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 3}]),
    start_broker(Group, Config);
init_per_group(Group = unclustered_cluster_size_2, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 2},
                                                    {rmq_nodes_clustered, false}]),
    start_broker(Group, Config).

start_broker(Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, {rmq_nodename_suffix, Group}),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    %% Test that all bindings got removed from the database.
    ?assertEqual([], list_bindings(Config)).

%%%===================================================================
%%% Test cases
%%%===================================================================

remove_binding_unbind_queue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q, false),
    bind_queue(Ch, Q, X, RKey),
    assert_bindings_list_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    unbind_queue(Ch, Q, X, RKey),
    assert_bindings_list_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    delete_queue(Ch, Q),
    ok.

remove_binding_delete_queue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q, false),
    bind_queue(Ch, Q, X, RKey),
    assert_bindings_list_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q),
    assert_bindings_list_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    ok.

remove_binding_delete_queue_multiple(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),

    X = <<"amq.direct">>,
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q1, false),
    bind_queue(Ch, Q1, X, RKey),
    bind_queue(Ch, Q1, <<"amq.fanout">>, RKey),
    declare_queue(Ch, Q2, true),
    bind_queue(Ch, Q2, X, RKey),

    %% Table rabbit_index_route stores only bindings
    %% where the source exchange is a direct exchange.
    ?assertEqual(3, count_bindings(Config)),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q1),
    assert_bindings_list_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q2),
    assert_bindings_list_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    ok.

remove_binding_delete_exchange(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    DirectX = <<"x1">>,
    FanoutX = <<"x2">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_exchange(Ch, DirectX, <<"direct">>),
    declare_exchange(Ch, FanoutX, <<"fanout">>),
    declare_queue(Ch, Q, false),
    bind_queue(Ch, Q, DirectX, RKey),
    bind_queue(Ch, Q, FanoutX, RKey),
    assert_bindings_list_non_empty(Config),
    publish(Ch, DirectX, RKey),
    assert_confirm(),

    %% Table rabbit_index_route stores only bindings
    %% where the source exchange is a direct exchange.
    delete_exchange(Ch, FanoutX),
    assert_bindings_list_non_empty(Config),

    delete_exchange(Ch, DirectX),
    assert_bindings_list_empty(Config),
    delete_queue(Ch, Q),
    ok.

keep_binding_node_down_durable_queue(Config) ->
    [_Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    {_Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),

    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    %% durable classic queue on Server2
    declare_queue(Ch2, Q, true),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_khepri, fence, [10000]),
    bind_queue(Ch1, Q, X, RKey),
    assert_bindings_list_non_empty(Config),
    publish(Ch1, X, RKey),
    assert_confirm(),

    rabbit_control_helper:command(stop_app, Server2),
    %% When the durable classic queue's host node is down,
    %% we expect that queue and its bindings still to exist
    %% (see https://github.com/rabbitmq/rabbitmq-server/pull/4563).
    assert_bindings_list_non_empty(Config),
    rabbit_control_helper:command(start_app, Server2),
    assert_bindings_list_non_empty(Config),
    delete_queue(Ch1, Q),
    ok.

recover_bindings(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Path = filename:join([?config(data_dir, Config), "definition.json"]),

    assert_bindings_list_empty(Config),
    rabbit_ct_broker_helpers:rabbitmqctl(Config, Server, ["import_definitions", Path], 10_000),
    ?assertEqual(?NUM_BINDINGS_TO_DIRECT_ECHANGE, count_bindings(Config)),
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    ?assertEqual(?NUM_BINDINGS_TO_DIRECT_ECHANGE_DURABLE, count_bindings(Config)),

    %% cleanup
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    delete_queue(Ch, <<"durable-q">>),
    ok.

%% Test that routing from a direct exchange to a fanout exchange works.
route_exchange_to_exchange(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    DirectX = <<"amq.direct">>,
    FanoutX = <<"amq.fanout">>,
    RKey = <<"k">>,
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,

    #'exchange.bind_ok'{} = amqp_channel:call(Ch, #'exchange.bind'{destination = FanoutX,
                                                                   source = DirectX,
                                                                   routing_key = RKey}),
    declare_queue(Ch, Q1, true),
    declare_queue(Ch, Q2, false),
    bind_queue(Ch, Q1, FanoutX, <<"ignored">>),
    bind_queue(Ch, Q2, FanoutX, <<"ignored">>),

    publish(Ch, DirectX, RKey),
    queue_utils:wait_for_messages(Config, [[Q1, <<"1">>, <<"1">>, <<"0">>]]),
    queue_utils:wait_for_messages(Config, [[Q2, <<"1">>, <<"1">>, <<"0">>]]),
    ?assertEqual(3, count_bindings(Config)),

    %% cleanup
    delete_queue(Ch, Q1),
    delete_queue(Ch, Q2),
    #'exchange.unbind_ok'{} = amqp_channel:call(Ch, #'exchange.unbind'{destination = FanoutX,
                                                                       source = DirectX,
                                                                       routing_key = RKey}),
    ok.

join_cluster(Config) ->
    [Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    {_Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Server2),
    DirectX = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch1, Q, true),
    bind_queue(Ch1, Q, DirectX, RKey),

    %% Server1 and Server2 are not clustered yet.
    %% Hence, every node has their own table (copy) and only Server2's table contains the binding.
    ?assertEqual(1, count_bindings(Config, Server2)),
    ?assertEqual(0, count_bindings(Config, Server1)),

    ok = rabbit_control_helper:command(stop_app, Server1),
    %% For the purpose of this test it shouldn't matter whether Server1 is reset. Both should work.
    case erlang:system_time() rem 2 of
        0 ->
            ok = rabbit_control_helper:command(reset, Server1);
        1 ->
            ok
    end,
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server2)], []),
    ok = rabbit_control_helper:command(start_app, Server1),

    %% After Server1 joined Server2, the table should be clustered.
    ?assertEqual(1, count_bindings(Config, Server1)),

    %% Publishing via Server2 via "direct exchange routing v2" should work.
    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),
    publish(Ch1, DirectX, RKey),
    assert_confirm(),

    %% Publishing via Server1 via "direct exchange routing v2" should work.
    {_Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Server1),
    amqp_channel:call(Ch2, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch2, self()),
    publish(Ch2, DirectX, RKey),
    assert_confirm(),
    delete_queue(Ch1, Q),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

declare_queue(Ch, QName, Durable) ->
    #'queue.declare_ok'{message_count = 0} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                                    durable = Durable
                                                                                   }).

delete_queue(Ch, QName) ->
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}).

bind_queue(Ch, Queue, Exchange, RoutingKey) ->
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue       = Queue,
                                                             exchange    = Exchange,
                                                             routing_key = RoutingKey}).

unbind_queue(Ch, Queue, Exchange, RoutingKey) ->
    #'queue.unbind_ok'{} = amqp_channel:call(Ch, #'queue.unbind'{queue       = Queue,
                                                                 exchange    = Exchange,
                                                                 routing_key = RoutingKey}).

declare_exchange(Ch, XName, Type) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = XName,
                                                                         type = Type}).

delete_exchange(Ch, XName) ->
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = XName}).

publish(Ch, Exchange, RoutingKey) ->
    ok = amqp_channel:call(Ch, #'basic.publish'{exchange = Exchange,
                                                routing_key = RoutingKey,
                                                mandatory = true},
                           #amqp_msg{}).

assert_confirm() ->
    receive
        #'basic.ack'{} ->
            ok
    after 30_000 ->
              throw(missing_confirm)
    end.

assert_return() ->
    receive
        {#'basic.return'{}, _} ->
            ok
    after 30_000 ->
              throw(missing_return)
    end.

assert_bindings_list_empty(Config) ->
    ?awaitMatch(0, count_bindings(Config), 3000).

assert_bindings_list_non_empty(Config) ->
    ?assertNotEqual(0, count_bindings(Config)).

count_bindings(Config) ->
    length(list_bindings(Config)).

count_bindings(Config, Server) ->
    length(list_bindings(Config, Server)).

list_bindings(Config) ->
    list_bindings(Config, 0).

list_bindings(Config, Server) ->
    rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_db_binding, get_all, []).
