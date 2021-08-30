%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("src/emqttc_packet.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [],
       [connection
        , pubsub_shared_connection
        , pubsub_separate_connections
        , last_will_enabled
        , last_will_disabled
        , disconnect
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

-define(DEFAULT_TIMEOUT, 15000).


connection(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

pubsub_shared_connection(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            clean_sess = true,
            client_id = <<"web-mqtt-tests-pubsub">>,
            username  = <<"guest">>,
            password  = <<"guest">>})),

    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS),

    Dst = <<"/topic/test-web-mqtt">>,

    ok = raw_send(WS, ?SUBSCRIBE_PACKET(1, [{Dst, ?QOS_1}])),
    {ok, ?SUBACK_PACKET(_, _), _} = raw_recv(WS),

    Payload = <<"a\x00a">>,

    ok = raw_send(WS, ?PUBLISH_PACKET(?QOS_1, Dst, 2, Payload)),
    {ok, ?PUBLISH_PACKET(_, Dst, _, Payload), _} = raw_recv(WS),

    {close, _} = rfc6455_client:close(WS),
    ok.

pubsub_separate_connections(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),
    WS1 = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS1),
    ok = raw_send(WS1,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            clean_sess = true,
            client_id = <<"web-mqtt-tests-publisher">>,
            username  = <<"guest">>,
            password  = <<"guest">>})),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS1),

    WS2 = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS2),
    ok = raw_send(WS2,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            clean_sess = true,
            client_id = <<"web-mqtt-tests-consumer">>,
            username  = <<"guest">>,
            password  = <<"guest">>})),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS2),

    Dst = <<"/topic/test-web-mqtt">>,
    ok = raw_send(WS2, ?SUBSCRIBE_PACKET(1, [{Dst, ?QOS_1}])),
    {ok, ?SUBACK_PACKET(_, _), _} = raw_recv(WS2),

    Payload = <<"a\x00a">>,
    ok = raw_send(WS1, ?PUBLISH_PACKET(?QOS_1, Dst, 2, Payload)),
    {ok, ?PUBLISH_PACKET(_, Dst, _, Payload), _} = raw_recv(WS2),

    {close, _} = rfc6455_client:close(WS1),
    {close, _} = rfc6455_client:close(WS2),
    ok.

last_will_enabled(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),

    LastWillDst = <<"/topic/web-mqtt-tests-ws1-last-will">>,
    LastWillMsg = <<"a last will and testament message">>,

    WS1 = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS1),
    ok = raw_send(WS1,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            clean_sess = true,
            client_id = <<"web-mqtt-tests-last-will-ws1">>,
            will_flag  = true,
            will_qos   = ?QOS_1,
            will_topic = LastWillDst,
            will_msg   = LastWillMsg,
            username  = <<"guest">>,
            password  = <<"guest">>})),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS1),

    WS2 = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS2),
    ok = raw_send(WS2,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            clean_sess = true,
            client_id = <<"web-mqtt-tests-last-will-ws2">>,
            username  = <<"guest">>,
            password  = <<"guest">>})),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS2),

    ok = raw_send(WS2, ?SUBSCRIBE_PACKET(1, [{LastWillDst, ?QOS_1}])),
    {ok, ?SUBACK_PACKET(_, _), _} = raw_recv(WS2),

    {close, _} = rfc6455_client:close(WS1),
    ?assertMatch({ok, ?PUBLISH_PACKET(_, LastWillDst, _, LastWillMsg), _}, raw_recv(WS2, 5000)),

    {close, _} = rfc6455_client:close(WS2),
    ok.

last_will_disabled(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),

    LastWillDst = <<"/topic/web-mqtt-tests-ws1-last-will-disabled">>,
    LastWillMsg = <<"a last will and testament message">>,

    WS1 = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS1),
    ok = raw_send(WS1,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            clean_sess = true,
            client_id = <<"web-mqtt-tests-last-will-ws1-disabled">>,
            will_flag  = false,
            will_qos   = ?QOS_1,
            will_topic = LastWillDst,
            will_msg   = LastWillMsg,
            username  = <<"guest">>,
            password  = <<"guest">>})),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS1),

    WS2 = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS2),
    ok = raw_send(WS2,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            clean_sess = true,
            client_id = <<"web-mqtt-tests-last-will-ws2-disabled">>,
            username  = <<"guest">>,
            password  = <<"guest">>})),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS2),

    ok = raw_send(WS2, ?SUBSCRIBE_PACKET(1, [{LastWillDst, ?QOS_1}])),
    ?assertMatch({ok, ?SUBACK_PACKET(_, _), _}, raw_recv(WS2)),

    {close, _} = rfc6455_client:close(WS1),
    ?assertEqual({error, timeout}, raw_recv(WS2, 3000)),

    {close, _} = rfc6455_client:close(WS2),
    ok.

disconnect(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            clean_sess = true,
            client_id  = <<"web-mqtt-tests-disconnect">>,
            username   = <<"guest">>,
            password   = <<"guest">>})),

    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS),

    ok = raw_send(WS, ?PACKET(?DISCONNECT)),
    {close, {1000, _}} = rfc6455_client:recv(WS),

    ok.


raw_send(WS, Packet) ->
    Frame = emqttc_serialiser:serialise(Packet),
    rfc6455_client:send_binary(WS, Frame).

raw_recv(WS) ->
    raw_recv(WS, ?DEFAULT_TIMEOUT).

raw_recv(WS, Timeout) ->
    case rfc6455_client:recv(WS, Timeout) of
        {binary, P} ->
            emqttc_parser:parse(P, emqttc_parser:new());
        {error, timeout} ->
            {error, timeout}
    end.
