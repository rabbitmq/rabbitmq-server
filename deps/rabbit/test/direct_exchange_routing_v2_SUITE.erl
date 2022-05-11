-module(direct_exchange_routing_v2_SUITE).

%% Test suite for the feature flag direct_exchange_routing_v2

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(FEATURE_FLAG, direct_exchange_routing_v2).
-define(INDEX_TABLE_NAME, rabbit_index_route).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_2}
    ].

groups() ->
    [
     {cluster_size_1, [],
      [{start_feature_flag_enabled, [], [remove_binding_unbind_queue,
                                         remove_binding_delete_queue,
                                         remove_binding_delete_queue_multiple,
                                         remove_binding_delete_exchange]},
       {start_feature_flag_disabled, [], [enable_feature_flag]}
      ]},
     {cluster_size_2, [],
      [{start_feature_flag_enabled, [], [remove_binding_node_down_transient_queue,
                                         remove_binding_node_down_durable_queue
                                        ]},
       {start_feature_flag_disabled, [], [enable_feature_flag]}
      ]}
    ].

suite() ->
    [
     %% If a test hangs, no need to wait for 30 minutes.
     {timetrap, {minutes, 8}}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1});
init_per_group(cluster_size_2, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 2});

init_per_group(start_feature_flag_enabled = Group, Config0) ->
    Config = start_broker(Group, Config0),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FEATURE_FLAG) of
        ok ->
            Config;
        {skip, _} = Skip ->
            end_per_group(Group, Config),
            Skip
    end;
init_per_group(start_feature_flag_disabled = Group, Config0) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{forced_feature_flags_on_init, []}]}),
    Config = start_broker(Group, Config1),
    case rabbit_ct_broker_helpers:is_feature_flag_supported(Config, ?FEATURE_FLAG) of
        true ->
            Config;
        false ->
            end_per_group(Group, Config),
            {skip, io_lib:format("'~s' feature flag is unsupported", [?FEATURE_FLAG])}
    end.

start_broker(Group, Config0) ->
    Size = rabbit_ct_helpers:get_config(Config0, rmq_nodes_count),
    Config = rabbit_ct_helpers:set_config(Config0, {rmq_nodename_suffix,
                                                    io_lib:format("cluster_size_~b-~s", [Size, Group])}),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(Group, Config)
  when Group =:= start_feature_flag_enabled;
       Group =:= start_feature_flag_disabled ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_Group, Config) ->
    Config.

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
                                 ?INDEX_TABLE_NAME])
                ).

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
    assert_index_table_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    unbind_queue(Ch, Q, X, RKey),
    assert_index_table_empty(Config),
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
    assert_index_table_non_empty(Config),
    publish(Ch, X, RKey),
    assert_confirm(),

    delete_queue(Ch, Q),
    assert_index_table_empty(Config),
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
    ?assertEqual(2, table_size(Config, ?INDEX_TABLE_NAME)),
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

remove_binding_node_down_transient_queue(Config) ->
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

remove_binding_node_down_durable_queue(Config) ->
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

enable_feature_flag(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    DirectX = <<"amq.direct">>,
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    RKey = <<"k">>,

    declare_queue(Ch, Q1, true),
    bind_queue(Ch, Q1, DirectX, RKey),
    bind_queue(Ch, Q1, <<"amq.fanout">>, RKey),

    declare_queue(Ch, Q2, false),
    bind_queue(Ch, Q2, DirectX, RKey),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    %% Publishing via "direct exchange routing v1" works.
    publish(Ch, DirectX, RKey),
    assert_confirm(),

    %% Before the feature flag is enabled, there should not be an index table.
    Tids = rabbit_ct_broker_helpers:rpc_all(Config, ets, whereis, [?INDEX_TABLE_NAME]),
    ?assert(lists:all(fun(Tid) -> Tid =:= undefined end, Tids)),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FEATURE_FLAG),

    %% The feature flag migration should have created an index table with a ram copy on all nodes.
    ?assertEqual(lists:sort(Nodes),
                 lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, mnesia, table_info, [?INDEX_TABLE_NAME, ram_copies]))),
    %% The feature flag migration should have populated the index table with all bindings whose source exchange
    %% is a direct exchange.
    ?assertEqual([{rabbit_misc:r(<<"/">>, exchange, DirectX), RKey}],
                 rabbit_ct_broker_helpers:rpc(Config, 0, mnesia, dirty_all_keys, [?INDEX_TABLE_NAME])),
    ?assertEqual(2, rabbit_ct_broker_helpers:rpc(Config, 0, mnesia, table_info, [?INDEX_TABLE_NAME, size])),

    %% Publishing via "direct exchange routing v2" works.
    publish(Ch, DirectX, RKey),
    assert_confirm(),

    delete_queue(Ch, Q1),
    delete_queue(Ch, Q2),
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
    ?awaitMatch(0, table_size(Config, ?INDEX_TABLE_NAME), 3000).

assert_index_table_non_empty(Config) ->
    ?assertNotEqual(0, table_size(Config, ?INDEX_TABLE_NAME)).

table_size(Config, Table) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ets, info, [Table, size], 5000).
