%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(exchanges_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).
-compile(export_all).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
     {group, mnesia_store},
     {group, khepri_store},
     {group, khepri_migration}
    ].

groups() ->
    [
     {mnesia_store, [], all_tests()},
     {khepri_store, [], all_tests()},
     {khepri_migration, [], [
                             from_mnesia_to_khepri
                            ]}
    ].

all_tests() ->
    [
     direct_exchange,
     headers_exchange,
     topic_exchange,
     fanout_exchange,
     invalid_exchange
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mnesia_store = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia}]),
    init_per_group_common(Group, Config, 1);
init_per_group(khepri_store = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, khepri}]),
    init_per_group_common(Group, Config, 1);
init_per_group(khepri_migration = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia}]),
    init_per_group_common(Group, Config, 1).

init_per_group_common(Group, Config, Size) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, Size},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
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
direct_exchange(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    AltQ = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', AltQ, 0, 0}, declare(Ch, AltQ, [])),

    Direct = <<"amq.direct">>,
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Direct,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Direct,
                                                             queue = AltQ,
                                                             routing_key = AltQ}),
    publish(Ch, Direct, Q, <<"msg1">>),
    publish(Ch, Direct, <<"anyotherkey">>, <<"msg2">>),
    
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>],
                                                  [AltQ, <<"0">>, <<"0">>, <<"0">>]]),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = AltQ})),
    ok.

topic_exchange(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    Topic = <<"amq.topic">>,    
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Topic,
                                                             queue = Q,
                                                             routing_key = <<"this.*.rules">>}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Topic,
                                                             queue = Q,
                                                             routing_key = <<"*.for.*">>}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Topic,
                                                             queue = Q,
                                                             routing_key = <<"simply#carrots">>}),

    publish(Ch, Topic, <<"this.queue.rules">>, <<"msg1">>),
    publish(Ch, Topic, <<"this.exchange.rules">>, <<"msg2">>),
    publish(Ch, Topic, <<"another.queue.rules">>, <<"msg3">>),
    publish(Ch, Topic, <<"carrots.for.power">>, <<"msg4">>),
    publish(Ch, Topic, <<"simplycarrots">>, <<"msg5">>),
    publish(Ch, Topic, <<"*.queue.rules">>, <<"msg6">>),

    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"3">>, <<"3">>, <<"0">>]]),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg4">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),

    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Topic,
                                                             queue = Q,
                                                             routing_key = <<"#.noclue">>}),
    publish(Ch, Topic, <<"simplycarrots">>, <<"msg7">>),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"3">>, <<"0">>, <<"3">>]]),
    publish(Ch, Topic, <<"#.bla">>, <<"msg8">>),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"3">>, <<"0">>, <<"3">>]]),
    publish(Ch, Topic, <<"#.noclue">>, <<"msg9">>),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"4">>, <<"1">>, <<"3">>]]),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg9">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Topic,
                                                             queue = Q,
                                                             routing_key = <<"#">>}),
    publish(Ch, Topic, <<"simplycarrots">>, <<"msg10">>),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"5">>, <<"1">>, <<"4">>]]),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg10">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ok.

fanout_exchange(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    AltQ = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', AltQ, 0, 0}, declare(Ch, AltQ, [])),

    Fanout = <<"amq.fanout">>,
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Fanout,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Fanout,
                                                             queue = AltQ,
                                                             routing_key = AltQ}),
    publish(Ch, Fanout, Q, <<"msg1">>),
    publish(Ch, Fanout, <<"anyotherkey">>, <<"msg2">>),
    
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"2">>, <<"2">>, <<"0">>],
                                                  [AltQ, <<"2">>, <<"2">>, <<"0">>]]),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = AltQ})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = AltQ})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = AltQ})),
    ok.

headers_exchange(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    AltQ = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', AltQ, 0, 0}, declare(Ch, AltQ, [])),

    Headers = <<"amq.headers">>,
    #'queue.bind_ok'{} =
        amqp_channel:call(Ch,
                          #'queue.bind'{exchange = Headers,
                                        queue = Q,
                                        arguments = [{<<"x-match">>, longstr, <<"all">>},
                                                     {<<"foo">>, longstr, <<"bar">>},
                                                     {<<"fuu">>, longstr, <<"ber">>}]
                                       }),
    #'queue.bind_ok'{} =
        amqp_channel:call(Ch,
                          #'queue.bind'{exchange = Headers,
                                        queue = AltQ,
                                        arguments = [{<<"x-match">>, longstr, <<"any">>},
                                                     {<<"foo">>, longstr, <<"bar">>},
                                                     {<<"fuu">>, longstr, <<"ber">>}]
                                       }),

    publish(Ch, Headers, <<>>, <<"msg1">>, [{<<"foo">>, longstr, <<"bar">>},
                                            {<<"fuu">>, longstr, <<"ber">>}]),
    publish(Ch, Headers, <<>>, <<"msg2">>, [{<<"foo">>, longstr, <<"bar">>}]),
    publish(Ch, Headers, <<>>, <<"msg3">>),

    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>],
                                                  [AltQ, <<"2">>, <<"2">>, <<"0">>]]),

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q})),

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = AltQ})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = AltQ})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = AltQ})),
    ok.

invalid_exchange(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(Ch, #'queue.bind'{exchange = <<"invalid">>,
                                           queue = Q,
                                           routing_key = Q})).

from_mnesia_to_khepri(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    %% Test transient exchanges
    X = ?config(exchange_name, Config),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = X,
                                                                         durable = false}),
    
    %% Topic is the only exchange type that has its own mnesia/khepri tables.
    %% Let's test that the exchange works as expected after migration
    Topic = <<"amq.topic">>,
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = Topic,
                                                             queue = Q,
                                                             routing_key = <<"this.queue.rules">>}),
    
    Exchanges = lists:sort([rabbit_misc:r(<<"/">>, exchange, <<>>),
                            rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                            rabbit_misc:r(<<"/">>, exchange, <<"amq.fanout">>),
                            rabbit_misc:r(<<"/">>, exchange, <<"amq.headers">>),
                            rabbit_misc:r(<<"/">>, exchange, <<"amq.match">>),
                            rabbit_misc:r(<<"/">>, exchange, <<"amq.rabbitmq.trace">>),
                            rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
                            rabbit_misc:r(<<"/">>, exchange, X)]),
    ?assertEqual(
       Exchanges,
       lists:sort([X0#exchange.name ||
                      X0 <- rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list, [])])),
    
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, raft_based_metadata_store_phase1) of
        ok ->
            rabbit_ct_helpers:await_condition(
              fun() ->
                      RecoveredExchanges =
                          lists:sort([X0#exchange.name ||
                                         X0 <- rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list, [])]),
                      Exchanges == RecoveredExchanges
              end),
            publish(Ch, Topic, <<"this.queue.rules">>, <<"msg1">>),
            ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg1">>}},
                         amqp_channel:call(Ch, #'basic.get'{queue = Q})),
            ?assertMatch(#'basic.get_empty'{},
                         amqp_channel:call(Ch, #'basic.get'{queue = Q}));
        Skip ->
            Skip
    end.

%% Internal

delete_queues() ->
    [{ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

delete_exchange(Name) ->
    _ = rabbit_exchange:delete(rabbit_misc:r(<<"/">>, exchange, Name), false, <<"dummy">>).

declare(Ch, Q, Args) ->
    declare(Ch, Q, Args, true).

declare(Ch, Q, Args, Durable) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = Durable,
                                           auto_delete = false,
                                           arguments = Args}).

publish(Ch, X, RoutingKey, Msg) ->
    publish(Ch, X, RoutingKey, Msg, []).

publish(Ch, X, RoutingKey, Msg, Headers) ->
    ok = amqp_channel:cast(Ch, #'basic.publish'{exchange = X,
                                                routing_key = RoutingKey},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2,
                                                          headers = Headers},
                                     payload = Msg}).
