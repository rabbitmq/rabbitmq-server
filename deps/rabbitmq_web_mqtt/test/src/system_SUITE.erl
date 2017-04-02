%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("emqttc_packet.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [],
       [connection
        ,pubsub
        ,disconnect
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


connection(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

pubsub(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS,
        ?CONNECT_PACKET(#mqtt_packet_connect{
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


disconnect(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    PortStr = integer_to_list(Port),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS,
        ?CONNECT_PACKET(#mqtt_packet_connect{
            client_id = <<"web-mqtt-tests-disconnect">>,
            username = <<"guest">>,
            password = <<"guest">>})),

    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv(WS),

    ok = raw_send(WS, ?PACKET(?DISCONNECT)),
    {close, {1000, _}} = rfc6455_client:recv(WS),

    ok.


raw_send(WS, Packet) ->
    Frame = emqttc_serialiser:serialise(Packet),
    rfc6455_client:send_binary(WS, Frame).

raw_recv(WS) ->
    {binary, P} = rfc6455_client:recv(WS),
    emqttc_parser:parse(P, emqttc_parser:new()).
