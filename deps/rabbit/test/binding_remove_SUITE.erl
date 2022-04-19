-module(binding_remove_SUITE).

%% Mnesia table rabbit_index_route is an index table that stores bindings
%% by table key {SourceExchange, RoutingKey} where SourceExchange is of type
%% direct exchange.
%% This test suite tests that these bindings are cleaned up properly.

-export([all/0, groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

-export([unbind_queue/1,
         delete_queue/1,
         delete_queue_multiple/1,
         delete_exchange/1,
         node_down_transient_queue/1,
         node_down_durable_queue/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [], [unbind_queue,
                           delete_queue,
                           delete_queue_multiple,
                           delete_exchange]},
     {cluster_size_2, [], [node_down_transient_queue,
                           node_down_durable_queue]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config0) ->
    Size = case Group of
               cluster_size_1 -> 1;
               cluster_size_2 -> 2
           end,
    Config = rabbit_ct_helpers:set_config(Config0,
                                          [{rmq_nodes_count, Size},
                                           {rmq_nodename_suffix, Group}
                                          ]),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    %% Test that all bindings got removed from the database.
    ?assertEqual([0,0,0,0,0],
                 lists:map(fun(Table) ->
                                   table_size(Config, Table)
                           end, [rabbit_durable_route,
                                 rabbit_semi_durable_route,
                                 rabbit_route,
                                 rabbit_reverse_route,
                                 rabbit_index_route])
                ).

%%%===================================================================
%%% Test cases
%%%===================================================================

unbind_queue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q, false),
    bind_queue(Ch, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    unbind_queue(Ch, Q, X, RKey),
    assert_index_table_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    delete_queue(Ch, Q),
    ok.

delete_queue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q, false),
    bind_queue(Ch, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q),
    assert_index_table_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    ok.

delete_queue_multiple(Config) ->
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
    ?assertEqual(2, table_size(Config, rabbit_index_route)),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q1),
    assert_index_table_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q2),
    assert_index_table_empty(Config),
    publish(Ch, X, RKey),
    assert_return(),
    ok.

delete_exchange(Config) ->
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
    assert_index_table_non_empty(Config),
    publish(Ch, DirectX, RKey),
    assert_confirm(),

    %% Table rabbit_index_route stores only bindings
    %% where the source exchange is a direct exchange.
    delete_exchange(Ch, FanoutX),
    assert_index_table_non_empty(Config),

    delete_exchange(Ch, DirectX),
    assert_index_table_empty(Config),
    delete_queue(Ch, Q),
    ok.

node_down_transient_queue(Config) ->
    [_Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    {_Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),

    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),

    X = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    %% transient classic queue on Server2
    declare_queue(Ch2, Q, false),
    bind_queue(Ch1, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch1, X, RKey),
    assert_confirm(),

    rabbit_control_helper:command(stop_app, Server2),
    %% We expect no route to transient classic queue when its host node is down.
    assert_index_table_empty(Config),
    rabbit_control_helper:command(start_app, Server2),
    %% We expect route to NOT come back when transient queue's host node comes back.
    assert_index_table_empty(Config),
    delete_queue(Ch1, Q),
    ok.

node_down_durable_queue(Config) ->
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
    bind_queue(Ch1, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch1, X, RKey),
    assert_confirm(),

    rabbit_control_helper:command(stop_app, Server2),
    %% We expect no route to durable classic queue when its host node is down.
    assert_index_table_empty(Config),
    rabbit_control_helper:command(start_app, Server2),
    %% We expect route to come back when durable queue's host node comes back.
    assert_index_table_non_empty(Config),
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
    after 5000 ->
              throw(missing_confirm)
    end.

assert_return() ->
    receive
        {#'basic.return'{}, _} ->
            ok
    after 5000 ->
              throw(missing_return)
    end.

assert_index_table_empty(Config) ->
    ?assertEqual(0, table_size(Config, rabbit_index_route)).

assert_index_table_non_empty(Config) ->
    ?assertNotEqual(0, table_size(Config, rabbit_index_route)).

table_size(Config, Table) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ets, info, [Table, size], 5000).
