%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

-module(direct_exchange_routing_v2_SUITE).

%% Test suite for the feature flag direct_exchange_routing_v2

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-define(FEATURE_FLAG, direct_exchange_routing_v2).
-define(INDEX_TABLE_NAME, rabbit_index_route).

%% in file direct_exchange_routing_v2_SUITE_data/definition.json:
%% number of bindings where the source exchange is a direct exchange
-define(NUM_BINDINGS_TO_DIRECT_ECHANGE, 620).
%% number of bindings where the source exchange is a direct exchange
%% and both source exchange and destination queue are durable
-define(NUM_BINDINGS_TO_DIRECT_ECHANGE_DURABLE, 220).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_2},
     {group, cluster_size_3},
     {group, unclustered_cluster_size_2}
    ].

groups() ->
    [
     {cluster_size_1, [],
      [{start_feature_flag_enabled, [], [remove_binding_unbind_queue,
                                         remove_binding_delete_queue,
                                         remove_binding_delete_queue_multiple,
                                         remove_binding_delete_exchange,
                                         recover_bindings,
                                         route_exchange_to_exchange,
                                         reset]},
       {start_feature_flag_disabled, [], [enable_feature_flag]}
      ]},
     {cluster_size_2, [],
      [{start_feature_flag_enabled, [], [
                                         remove_binding_node_down_transient_queue,
                                         keep_binding_node_down_durable_queue
                                        ]},
       {start_feature_flag_disabled, [], [enable_feature_flag_during_definition_import]}
      ]},
     {unclustered_cluster_size_2, [],
      [{start_feature_flag_enabled, [], [join_cluster]}
      ]},
     {cluster_size_3, [],
      [{start_feature_flag_disabled, [], [enable_feature_flag_during_binding_churn]}
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
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 3});
init_per_group(unclustered_cluster_size_2, Config0) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "This test group won't work in mixed mode with pre 3.11 releases"};
        false ->
            rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 2},
                                                   {rmq_nodes_clustered, false}])
    end;
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
            assert_no_index_table(Config),
            Config;
        false ->
            end_per_group(Group, Config),
            {skip, io_lib:format("'~ts' feature flag is unsupported", [?FEATURE_FLAG])}
    end.

start_broker(Group, Config0) ->
    Size = rabbit_ct_helpers:get_config(Config0, rmq_nodes_count),
    Clustered = rabbit_ct_helpers:get_config(Config0, rmq_nodes_clustered, true),
    Config = rabbit_ct_helpers:set_config(Config0, {rmq_nodename_suffix,
                                                    io_lib:format("cluster_size_~b-clustered_~tp-~ts",
                                                                  [Size, Clustered, Group])}),
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
    bind_queue(Ch1, Q, X, RKey),
    assert_index_table_non_empty(Config),
    publish(Ch1, X, RKey),
    assert_confirm(),

    rabbit_control_helper:command(stop_app, Server2),
    %% When the durable classic queue's host node is down,
    %% we expect that queue and its bindings still to exist
    %% (see https://github.com/rabbitmq/rabbitmq-server/pull/4563).
    assert_index_table_non_empty(Config),
    rabbit_control_helper:command(start_app, Server2),
    assert_index_table_non_empty(Config),
    delete_queue(Ch1, Q),
    ok.

recover_bindings(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Path = filename:join([?config(data_dir, Config), "definition.json"]),

    assert_index_table_empty(Config),
    rabbit_ct_broker_helpers:rabbitmqctl(Config, Server, ["import_definitions", Path], 10_000),
    ?assertEqual(?NUM_BINDINGS_TO_DIRECT_ECHANGE, table_size(Config, ?INDEX_TABLE_NAME)),
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    ?assertEqual(?NUM_BINDINGS_TO_DIRECT_ECHANGE_DURABLE, table_size(Config, ?INDEX_TABLE_NAME)),

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
    quorum_queue_utils:wait_for_messages(Config, [[Q1, <<"1">>, <<"1">>, <<"0">>]]),
    quorum_queue_utils:wait_for_messages(Config, [[Q2, <<"1">>, <<"1">>, <<"0">>]]),
    ?assertEqual(1, table_size(Config, ?INDEX_TABLE_NAME)),

    %% cleanup
    delete_queue(Ch, Q1),
    delete_queue(Ch, Q2),
    #'exchange.unbind_ok'{} = amqp_channel:call(Ch, #'exchange.unbind'{destination = FanoutX,
                                                                       source = DirectX,
                                                                       routing_key = RKey}),
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

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FEATURE_FLAG),

    %% The feature flag migration should have created an index table with a ram copy on all nodes.
    ?assertEqual(lists:sort(Nodes), index_table_ram_copies(Config, 0)),
    %% The feature flag migration should have populated the index table with all bindings whose source exchange
    %% is a direct exchange.
    ?assertEqual([{rabbit_misc:r(<<"/">>, exchange, DirectX), RKey}],
                 rabbit_ct_broker_helpers:rpc(Config, 0, mnesia, dirty_all_keys, [?INDEX_TABLE_NAME])),
    ?assertEqual(2, table_size(Config, ?INDEX_TABLE_NAME)),

    %% Publishing via "direct exchange routing v2" works.
    publish(Ch, DirectX, RKey),
    assert_confirm(),

    delete_queue(Ch, Q1),
    delete_queue(Ch, Q2),
    ok.

%% Test that enabling feature flag works when bindings are imported concurrently.
enable_feature_flag_during_definition_import(Config) ->
    Nodes = [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Path = filename:join([?config(data_dir, Config), "definition.json"]),

    {Pid, Ref} = spawn_monitor(
                   fun() ->
                           ct:pal("importing definitions..."),
                           rabbit_ct_broker_helpers:rabbitmqctl(
                             Config, Server1, ["import_definitions", Path]
                            ),
                           ct:pal("imported definitions")
                   end),

    timer:sleep(rand:uniform(400)),
    ct:pal("enabling feature flag..."),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FEATURE_FLAG),
    ct:pal("enabled feature flag"),

    receive {'DOWN', Ref, process, Pid, normal} ->
                ok
    after 10_000 ->
              ct:fail(timeout)
    end,

    ?assertEqual(lists:sort(Nodes), index_table_ram_copies(Config, 0)),
    ?assertEqual(?NUM_BINDINGS_TO_DIRECT_ECHANGE,
                 table_size(Config, ?INDEX_TABLE_NAME)),

    %% cleanup
    delete_queue(Ch, <<"durable-q">>),
    delete_queue(Ch, <<"transient-q">>),
    ok.

%% Test that enabling feature flag works when clients concurrently
%% create and delete bindings and send messages.
enable_feature_flag_during_binding_churn(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    {_Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),

    DirectX = <<"amq.direct">>,
    FanoutX = <<"amq.fanout">>,
    Q = <<"q">>,

    NumMessages = 500,
    BindingsDirectX = 1000,
    BindingsFanoutX = 10,

    %% setup
    declare_queue(Ch1, Q, true),
    lists:foreach(fun(N) ->
                          bind_queue(Ch1, Q, DirectX, integer_to_binary(N))
                  end, lists:seq(1, trunc(0.4 * BindingsDirectX))),
    lists:foreach(fun(N) ->
                          bind_queue(Ch1, Q, FanoutX, integer_to_binary(N))
                  end, lists:seq(1, BindingsFanoutX)),
    lists:foreach(fun(N) ->
                          bind_queue(Ch1, Q, DirectX, integer_to_binary(N))
                  end, lists:seq(trunc(0.4 * BindingsDirectX) + 1, trunc(0.8 * BindingsDirectX))),

    {_, Ref1} = spawn_monitor(
                  fun() ->
                          ct:pal("sending ~b messages...", [NumMessages]),
                          lists:foreach(
                            fun(_) ->
                                    publish(Ch1, DirectX, integer_to_binary(trunc(0.8 * BindingsDirectX))),
                                    timer:sleep(1)
                            end, lists:seq(1, NumMessages)),
                          ct:pal("sent ~b messages", [NumMessages])
                  end),
    {_, Ref2} = spawn_monitor(
                  fun() ->
                          ct:pal("creating bindings..."),
                          lists:foreach(
                            fun(N) ->
                                    bind_queue(Ch1, Q, DirectX, integer_to_binary(N)),
                                    timer:sleep(1)
                            end, lists:seq(trunc(0.8 * BindingsDirectX) + 1, BindingsDirectX)),
                          ct:pal("created bindings")
                  end),
    {_, Ref3} = spawn_monitor(
                  fun() ->
                          ct:pal("deleting bindings..."),
                          lists:foreach(
                            fun(N) ->
                                    unbind_queue(Ch2, Q, DirectX, integer_to_binary(N)),
                                    timer:sleep(1)
                            end, lists:seq(1, trunc(0.2 * BindingsDirectX))),
                          ct:pal("deleted bindings")
                  end),

    timer:sleep(rand:uniform(300)),
    ct:pal("enabling feature flag..."),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FEATURE_FLAG),
    ct:pal("enabled feature flag"),

    lists:foreach(
      fun(Ref) ->
              receive {'DOWN', Ref, process, _Pid, normal} ->
                          ok
              after 300_000 ->
                        ct:fail(timeout)
              end
      end, [Ref1, Ref2, Ref3]),

    NumMessagesBin = integer_to_binary(NumMessages),
    quorum_queue_utils:wait_for_messages(Config, [[Q, NumMessagesBin, NumMessagesBin, <<"0">>]]),

    ?assertEqual(lists:sort(Nodes), index_table_ram_copies(Config, 0)),

    ExpectedKeys = lists:map(
                     fun(N) ->
                             {rabbit_misc:r(<<"/">>, exchange, DirectX), integer_to_binary(N)}
                     end, lists:seq(trunc(0.2 * BindingsDirectX) + 1, BindingsDirectX)),
    ActualKeys = rabbit_ct_broker_helpers:rpc(Config, 0, mnesia, dirty_all_keys, [?INDEX_TABLE_NAME]),
    ?assertEqual(lists:sort(ExpectedKeys), lists:sort(ActualKeys)),

    %% cleanup
    delete_queue(Ch1, Q),
    ok.

reset(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    ?assertEqual([Server], index_table_ram_copies(Config, 0)),
    ok = rabbit_control_helper:command(stop_app, Server),
    ok = rabbit_control_helper:command(reset, Server),
    ok = rabbit_control_helper:command(start_app, Server),
    %% After reset, upon node boot, we expect that the table gets re-created.
    ?assertEqual([Server], index_table_ram_copies(Config, 0)).

join_cluster(Config) ->
    Servers0 = [Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Servers = lists:sort(Servers0),

    {_Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Server1),
    DirectX = <<"amq.direct">>,
    Q = <<"q">>,
    RKey = <<"k">>,

    declare_queue(Ch1, Q, true),
    bind_queue(Ch1, Q, DirectX, RKey),

    %% Server1 and Server2 are not clustered yet.
    %% Hence, every node has their own table (copy) and only Server1's table contains the binding.
    ?assertEqual([Server1], index_table_ram_copies(Config, Server1)),
    ?assertEqual([Server2], index_table_ram_copies(Config, Server2)),
    ?assertEqual(1, table_size(Config, ?INDEX_TABLE_NAME, Server1)),
    ?assertEqual(0, table_size(Config, ?INDEX_TABLE_NAME, Server2)),

    ok = rabbit_control_helper:command(stop_app, Server2),
    %% For the purpose of this test it shouldn't matter whether Server2 is reset. Both should work.
    case erlang:system_time() rem 2 of
        0 ->
            ok = rabbit_control_helper:command(reset, Server2);
        1 ->
            ok
    end,
    ok = rabbit_control_helper:command(join_cluster, Server2, [atom_to_list(Server1)], []),
    ok = rabbit_control_helper:command(start_app, Server2),

    %% After Server2 joined Server1, the table should be clustered.
    ?assertEqual(Servers, index_table_ram_copies(Config, Server2)),
    ?assertEqual(1, table_size(Config, ?INDEX_TABLE_NAME, Server2)),

    %% Publishing via Server1 via "direct exchange routing v2" should work.
    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),
    publish(Ch1, DirectX, RKey),
    assert_confirm(),

    %% Publishing via Server2 via "direct exchange routing v2" should work.
    {_Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Server2),
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

assert_no_index_table(Config) ->
    Tids = rabbit_ct_broker_helpers:rpc_all(Config, ets, whereis, [?INDEX_TABLE_NAME]),
    ?assert(lists:all(fun(Tid) -> Tid =:= undefined end, Tids)).

index_table_ram_copies(Config, Node) ->
    RamCopies = rabbit_ct_broker_helpers:rpc(Config, Node, mnesia, table_info,
                                             [?INDEX_TABLE_NAME, ram_copies]),
    lists:sort(RamCopies).

table_size(Config, Table) ->
    table_size(Config, Table, 0).

table_size(Config, Table, Server) ->
    %% Do not use
    %% mnesia:table_info(Table, size)
    %% as this returns 0 if the table doesn't exist.
    rabbit_ct_broker_helpers:rpc(Config, Server, ets, info, [Table, size], 5000).
