%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(bindings_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([nowarn_export_all, export_all]).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
     % {group, tests},
     {group, khepri_migration},
     {group, cluster}
    ].

groups() ->
    [
     % {tests, [], all_tests()},
     {khepri_migration, [], [
                             from_mnesia_to_khepri
                            ]},
     {cluster, [], all_tests()}
    ].

all_tests() ->
    [
     %% Queue bindings
     bind_and_unbind,
     bind_and_delete,
     bind_and_delete_source_exchange,
     list_bindings,
     list_for_source,
     list_for_destination,
     list_for_source_and_destination,
     list_explicit,
     info_all,
     list_with_multiple_vhosts,
     list_with_multiple_arguments,
     bind_to_unknown_queue,
     binding_args_direct_exchange,
     binding_args_fanout_exchange,

     %% Exchange bindings
     bind_and_unbind_direct_exchange,
     bind_and_unbind_fanout_exchange,
     bind_and_delete_exchange_source,
     bind_and_delete_exchange_destination,
     bind_to_unknown_exchange,
     transient_queue_on_node_down
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

% init_per_group(tests = Group, Config) ->
%     init_per_group_common(Group, Config, 1);
init_per_group(khepri_migration = Group, Config) ->
    case rabbit_ct_broker_helpers:configured_metadata_store(Config) of
        khepri ->
            {skip, "skip khepri migration test when khepri already configured"};
        mnesia ->
            init_per_group_common(Group, Config, 1)
    end;
init_per_group(cluster = Group, Config) ->
    init_per_group_common(Group, Config, 3).

init_per_group_common(Group, Config, Size) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, Size},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base, {skip_n_nodes, Size}}
                                            ]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Name = rabbit_data_coercion:to_binary(Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_exchange, [Name]),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Name},
                                            {alt_queue_name, <<Name/binary, "_alt">>},
                                            {exchange_name, Name}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_exchange,
                                 [?config(exchange_name, Config)]),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

bind_and_unbind(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),

    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    %% Let's bind to other exchange
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),

    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   QResource, Q, []),
    Bindings = lists:sort([DefaultBinding, DirectBinding]),

    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    #'queue.unbind_ok'{} = amqp_channel:call(Ch, #'queue.unbind'{exchange = <<"amq.direct">>,
                                                                 queue = Q,
                                                                 routing_key = Q}),

    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

bind_and_delete(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),

    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    %% Let's bind to other exchange
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),

    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   QResource, Q, []),
    Bindings = lists:sort([DefaultBinding, DirectBinding]),

    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

bind_and_delete_source_exchange(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    X = ?config(exchange_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = X}),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),

    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    %% Let's bind to other exchange
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = X,
                                                             queue = Q,
                                                             routing_key = Q}),

    XBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, X), QResource, Q, []),
    Bindings = lists:sort([DefaultBinding, XBinding]),

    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    ?assertMatch(#'exchange.delete_ok'{},
                 amqp_channel:call(Ch, #'exchange.delete'{exchange = X})),

    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

list_bindings(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),

    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    %% Let's bind to all other exchanges
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.fanout">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.headers">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.match">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.rabbitmq.trace">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),

    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   QResource, Q, []),
    FanoutBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.fanout">>),
                                   QResource, Q, []),
    HeadersBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.headers">>),
                                    QResource, Q, []),
    MatchBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.match">>),
                                  QResource, Q, []),
    TraceBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.rabbitmq.trace">>),
                                  QResource, Q, []),
    TopicBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
                                  QResource, Q, []),
    Bindings = lists:sort([DefaultBinding, DirectBinding, FanoutBinding, HeadersBinding,
                           MatchBinding, TraceBinding, TopicBinding]),

    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    ok.

list_for_source(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),

    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),

    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DirectBinding = binding_record(DirectExchange, QResource, Q, []),
    DirectABinding = binding_record(DirectExchange, QAltResource, QAlt, []),
    TopicBinding = binding_record(TopicExchange, QResource, Q, []),
    TopicABinding = binding_record(TopicExchange, QAltResource, QAlt, []),
    DirectBindings = lists:sort([DirectBinding, DirectABinding]),
    TopicBindings = lists:sort([TopicBinding, TopicABinding]),

    ?assertEqual(
       DirectBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [DirectExchange]))),
    ?assertEqual(
       TopicBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [TopicExchange]))).

list_with_multiple_vhosts(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost1),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost1),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost2),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost2),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost2),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch1, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch1, QAlt, [])),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch2, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch2, QAlt, [])),

    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),
    QAltResource1 = rabbit_misc:r(VHost1, queue, QAlt),
    QResource2 = rabbit_misc:r(VHost2, queue, Q),
    QAltResource2 = rabbit_misc:r(VHost2, queue, QAlt),

    %% Default vhost:
    %%    direct - queue
    %%    topic  - altqueue
    %% Vhost1:
    %%    direct - altqueue
    %% Vhost2:
    %%    topic  - queue
    %%    topic  - altqueue
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch1, #'queue.bind'{exchange = <<"amq.direct">>,
                                                              queue = QAlt,
                                                              routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch2, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch2, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DirectExchange1 = rabbit_misc:r(VHost1, exchange, <<"amq.direct">>),
    TopicExchange2 = rabbit_misc:r(VHost2, exchange, <<"amq.topic">>),
    DefaultExchange1 = rabbit_misc:r(VHost1, exchange, <<>>),
    DefaultExchange2 = rabbit_misc:r(VHost2, exchange, <<>>),

    %% Direct exchange on default vhost
    ?assertEqual(
       [binding_record(DirectExchange, QResource, Q, [])],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [DirectExchange]))),
    %% Direct exchange on vhost 1
    ?assertEqual(
       [binding_record(DirectExchange1, QAltResource1, QAlt, [])],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [DirectExchange1]))),
    ?assertEqual(
       lists:sort([binding_record(DefaultExchange1, QAltResource1, QAlt, []),
                   binding_record(DirectExchange1, QAltResource1, QAlt, [])]),
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QAltResource1]))),
    %% Topic exchange on default vhost
    ?assertEqual(
       [binding_record(TopicExchange, QAltResource, QAlt, [])],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [TopicExchange]))),
    %% Topic exchange on vhost 2
    ?assertEqual(
       lists:sort([binding_record(TopicExchange2, QAltResource2, QAlt, []),
                   binding_record(TopicExchange2, QResource2, Q, [])]),
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [TopicExchange2]))),
    ?assertEqual(
       lists:sort([binding_record(TopicExchange2, QAltResource2, QAlt, []),
                   binding_record(DefaultExchange2, QAltResource2, QAlt, [])]),
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QAltResource2]))).

list_with_multiple_arguments(Config) ->
    %% Bindings are made of source, destination, routing key and arguments.
    %% Arguments are difficult to use on khepri paths and also are not relevant to any
    %% existing query. Thus, internally the bindings in Khepri are indexed using
    %% source, destination and key. Each entry on Khepri contains a set of bindings.
    %% For the `rabbit_binding` API nothing has changed, let's test here listing outputs
    %% with multiple arguments for the same source, destination and routing key.
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),

    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    %% Let's bind with multiple arguments
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.headers">>,
                                                             queue = Q,
                                                             routing_key = Q,
                                                             arguments = [{<<"x-match">>, longstr, <<"all">>}]}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.headers">>,
                                                             queue = Q,
                                                             routing_key = Q,
                                                             arguments = [{<<"x-match">>, longstr, <<"any">>}]}),

    AllBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.headers">>),
                                QResource, Q, [{<<"x-match">>, longstr, <<"all">>}]),
    AnyBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.headers">>),
                                QResource, Q, [{<<"x-match">>, longstr, <<"any">>}]),
    Bindings = lists:sort([DefaultBinding, AllBinding, AnyBinding]),

    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    ok.

list_for_destination(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),

    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),

    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    DirectBinding = binding_record(DirectExchange, QResource, Q, []),
    DirectABinding = binding_record(DirectExchange, QAltResource, QAlt, []),
    TopicBinding = binding_record(TopicExchange, QResource, Q, []),
    TopicABinding = binding_record(TopicExchange, QAltResource, QAlt, []),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),
    DefaultABinding = binding_record(DefaultExchange, QAltResource, QAlt, []),

    Bindings = lists:sort([DefaultBinding, DirectBinding, TopicBinding]),
    AltBindings = lists:sort([DefaultABinding, DirectABinding, TopicABinding]),

    ?assertEqual(
       Bindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QResource]))),
    ?assertEqual(
       AltBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QAltResource]))).

list_for_source_and_destination(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),

    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),

    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    DirectBinding = binding_record(DirectExchange, QResource, Q, []),
    TopicBinding = binding_record(TopicExchange, QResource, Q, []),
    DefaultABinding = binding_record(DefaultExchange, QAltResource, QAlt, []),

    ?assertEqual(
       [DirectBinding],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding,
                                               list_for_source_and_destination,
                                               [DirectExchange, QResource]))),
    ?assertEqual(
       [TopicBinding],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding,
                                               list_for_source_and_destination,
                                               [TopicExchange, QResource]))),
    ?assertEqual(
       [DefaultABinding],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding,
                                               list_for_source_and_destination,
                                               [DefaultExchange, QAltResource]))).

 list_explicit(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    QResource = rabbit_misc:r(<<"/">>, queue, Q),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_explicit, [])),

    %% Let's bind to other exchanges
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.fanout">>,
                                                             queue = Q,
                                                             routing_key = Q}),

    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                     QResource, Q, []),
    FanoutBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.fanout">>),
                                     QResource, Q, []),
    Bindings = lists:sort([DirectBinding, FanoutBinding]),

    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_explicit, []))),

    ok.

info_all(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    Default = [{source_name,<<>>},
               {source_kind,exchange},
               {destination_name,<<"info_all">>},
               {destination_kind,queue},
               {routing_key,<<"info_all">>},
               {arguments,[]},
               {vhost,<<"/">>}],

    ?assertEqual([Default],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, info_all, [<<"/">>])),

    Direct = [{source_name,<<"amq.direct">>},
              {source_kind,exchange},
              {destination_name,<<"info_all">>},
              {destination_kind,queue},
              {routing_key,<<"info_all">>},
              {arguments,[]},
              {vhost,<<"/">>}],

    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),

    Infos = lists:sort([Default, Direct]),
    ?assertEqual(Infos,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, info_all, [<<"/">>]))),

    ok.

from_mnesia_to_khepri(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    AltQ = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', AltQ, 0, 0}, declare(Ch, AltQ, [], false)),

    %% Combine durable and transient queues and exchanges to test the migration of durable,
    %% semi-durable and transient bindings
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = AltQ,
                                                             routing_key = AltQ}),

    X = ?config(exchange_name, Config),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = X,
                                                                         durable = false}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = X,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = X,
                                                             queue = AltQ,
                                                             routing_key = AltQ}),


    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    AltQResource = rabbit_misc:r(<<"/">>, queue, AltQ),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),
    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   QResource, Q, []),
    AltDefaultBinding = binding_record(DefaultExchange, AltQResource, AltQ, []),
    AltDirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                      AltQResource, AltQ, []),
    XBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, X), QResource, Q, []),
    AltXBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, X),
                                 AltQResource, AltQ, []),
    Bindings = lists:sort([DefaultBinding, DirectBinding, AltDefaultBinding, AltDirectBinding,
                          XBinding, AltXBinding]),

    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    case rabbit_ct_broker_helpers:enable_feature_flag(Config, khepri_db) of
        ok ->
            rabbit_ct_helpers:await_condition(
              fun() ->
                      Bindings ==
                          lists:sort(
                            rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))
              end);
        Skip ->
            Skip
    end.

bind_to_unknown_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    %% Let's bind to exchange
    ?assertExit({{shutdown, {server_initiated_close,404, _}}, _},
                amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                    queue = Q,
                                                    routing_key = Q})),
    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

%% Test case for https://github.com/rabbitmq/rabbitmq-server/issues/14533
binding_args_direct_exchange(Config) ->
    binding_args(<<"amq.direct">>, Config).

binding_args_fanout_exchange(Config) ->
    binding_args(<<"amq.fanout">>, Config).

binding_args(Exchange, Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    %% Create two bindings that differ only in their binding arguments.
    RoutingKey = <<"some-key">>,
    BindingArgs1 = [{<<"app">>, longstr, <<"app-1">>}],
    BindingArgs2 = [{<<"app">>, longstr, <<"app-2">>}],
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Exchange,
                                                             routing_key = RoutingKey,
                                                             queue = Q,
                                                             arguments = BindingArgs1}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Exchange,
                                                             routing_key = RoutingKey,
                                                             queue = Q,
                                                             arguments = BindingArgs2}),
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{exchange = Exchange,
                                            routing_key = RoutingKey},
                           #amqp_msg{payload = <<"m1">>}),
    receive #'basic.ack'{} -> ok
    after 9000 -> ct:fail(confirm_timeout)
    end,

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q, no_ack = true})),

    %% If we delete the 1st binding, we expect RabbitMQ to still route via the 2nd binding.
    #'queue.unbind_ok'{} = amqp_channel:call(Ch, #'queue.unbind'{exchange = Exchange,
                                                                 routing_key = RoutingKey,
                                                                 queue = Q,
                                                                 arguments = BindingArgs1}),
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{exchange = Exchange,
                                            routing_key = RoutingKey},
                           #amqp_msg{payload = <<"m2">>}),
    receive #'basic.ack'{} -> ok
    after 9000 -> ct:fail(confirm_timeout)
    end,

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q, no_ack = true})).

bind_and_unbind_direct_exchange(Config) ->
    bind_and_unbind_exchange(<<"direct">>, Config).

bind_and_unbind_fanout_exchange(Config) ->
    bind_and_unbind_exchange(<<"fanout">>, Config).

bind_and_unbind_exchange(Type, Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    X = ?config(exchange_name, Config),
    Q = ?config(queue_name, Config),
    RoutingKey = <<"some key">>,
    SourceExchange = <<"amq.", Type/binary>>,

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = X,
                                                                         type = Type}),
    %% Let's bind to other exchange
    #'exchange.bind_ok'{} = amqp_channel:call(Ch, #'exchange.bind'{destination = X,
                                                                   source = SourceExchange,
                                                                   routing_key = RoutingKey}),

    Binding = binding_record(rabbit_misc:r(<<"/">>, exchange, SourceExchange),
                             rabbit_misc:r(<<"/">>, exchange, X),
                             RoutingKey, []),

    ?assertEqual([Binding],
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    %% Test that a message gets routed:
    %% exchange -> exchange -> queue
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = X,
                                                             routing_key = RoutingKey,
                                                             queue = Q}),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{exchange = SourceExchange,
                                            routing_key = RoutingKey},
                           #amqp_msg{payload = <<"m1">>}),
    receive #'basic.ack'{} -> ok
    after 9000 -> ct:fail(confirm_timeout)
    end,
    ?assertEqual(#'queue.delete_ok'{message_count = 1},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),

    #'exchange.unbind_ok'{} = amqp_channel:call(Ch,
                                                #'exchange.unbind'{destination = X,
                                                                   source = SourceExchange,
                                                                   routing_key = RoutingKey}),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

bind_to_unknown_exchange(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    X = ?config(exchange_name, Config),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    %% Let's bind to exchange
    ?assertExit({{shutdown, {server_initiated_close,404, _}}, _},
                amqp_channel:call(Ch, #'exchange.bind'{destination = X,
                                                       source = <<"amq.direct">>,
                                                       routing_key = <<"key">>})),
    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

bind_and_delete_exchange_destination(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    X = ?config(exchange_name, Config),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = X}),
    %% Let's bind to other exchange
    #'exchange.bind_ok'{} = amqp_channel:call(Ch, #'exchange.bind'{destination = X,
                                                                   source = <<"amq.direct">>,
                                                                   routing_key = <<"key">>}),

    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   rabbit_misc:r(<<"/">>, exchange, X),
                                   <<"key">>, []),

    ?assertEqual([DirectBinding],
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = X}),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

bind_and_delete_exchange_source(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    X = ?config(exchange_name, Config),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),

    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = X}),
    %% Let's bind to other exchange
    #'exchange.bind_ok'{} = amqp_channel:call(Ch, #'exchange.bind'{destination = <<"amq.direct">>,
                                                                   source = X,
                                                                   routing_key = <<"key">>}),

    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, X),
                                   rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   <<"key">>, []),

    ?assertEqual([DirectBinding],
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = X}),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

transient_queue_on_node_down(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [], false)),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),
    DefaultAltBinding = binding_record(DefaultExchange, QAltResource, QAlt, []),

    %% Binding to the default exchange, it's always present
    ?assertEqual(lists:sort([DefaultBinding, DefaultAltBinding]),
                 lists:sort(rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_binding, list, [<<"/">>]))),

    %% Let's bind to other exchange
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   QResource, Q, []),
    DirectAltBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                      QAltResource, QAlt, []),

    Bindings1 = lists:sort([DefaultBinding, DirectBinding, DefaultAltBinding, DirectAltBinding]),
    ?awaitMatch(Bindings1,
                lists:sort(
                  rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_binding, list, [<<"/">>])),
                30000),


    ?assertEqual(ok, rabbit_control_helper:command(stop_app, Server)),

    ?awaitMatch([DirectBinding],
                lists:sort(
                  rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_binding, list, [<<"/">>])),
                30000),
    ?awaitMatch([],
                rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_amqqueue, list, [<<"/">>]),
                30000),

    ?assertEqual(ok, rabbit_control_helper:command(start_app, Server)),

    Bindings2 = lists:sort([DefaultBinding, DirectBinding]),
    ?awaitMatch(Bindings2,
                lists:sort(
                  rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_binding, list, [<<"/">>])),
                30000),
    ?awaitMatch([_],
                rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_amqqueue, list, [<<"/">>]),
                30000),
    ok.

%% Internal

delete_queues() ->
    [{ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

delete_exchange(Name) ->
    ok = rabbit_exchange:ensure_deleted(
           rabbit_misc:r(<<"/">>, exchange, Name), false, <<"dummy">>).

declare(Ch, Q, Args) ->
    declare(Ch, Q, Args, true).

declare(Ch, Q, Args, Durable) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = Durable,
                                           auto_delete = false,
                                           arguments = Args}).

binding_record(Src, Dst, Key, Args) ->
    #binding{source = Src,
             destination = Dst,
             key = Key,
             args = Args}.
