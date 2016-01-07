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
%%   Copyright (c) 2012-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_web_mqtt_test).

-include_lib("eunit/include/eunit.hrl").
-include("emqttc_packet.hrl").

connection_test() ->
    WS = rfc6455_client:new("ws://127.0.0.1:15675/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.


raw_send(WS, Packet) ->
    Frame = emqttc_serialiser:serialise(Packet),
    rfc6455_client:send_binary(WS, Frame).

raw_recv(WS) ->
    {binary, P} = rfc6455_client:recv(WS),
    emqttc_parser:parse(P, emqttc_parser:new()).


pubsub_test() ->
    WS = rfc6455_client:new("ws://127.0.0.1:15675/ws", self()),
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


disconnect_test() ->
    WS = rfc6455_client:new("ws://127.0.0.1:15675/ws", self()),
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
