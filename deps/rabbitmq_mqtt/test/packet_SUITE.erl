%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.


-module(packet_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_mqtt_packet.hrl").

all() ->
    [
     {group, v3},
     {group, v4},
     {group, v5}
    ].

groups() ->
    [
     {v3, [parallel, shuffle], test_cases()},
     {v4, [parallel, shuffle], test_cases()},
     {v5, [parallel, shuffle], test_cases() ++ [publish_properties,
                                                puback_properties,
                                                disconnect_remaining_length_0,
                                                disconnect_properties]}
    ].

test_cases() ->
    [
     publish,
     puback
    ].

init_per_suite(Config) ->
    MaxPacketSize = 10_000,
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_UNAUTHENTICATED, MaxPacketSize),
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED, MaxPacketSize),
    Config.

end_per_suite(_) ->
    ok.

init_per_group(Group, Config) ->
    Vsn = case Group of
              v3 -> 3;
              v4 -> 4;
              v5 -> 5
          end,
    [{mqtt_vsn, Vsn} | Config].

end_per_group(_, Config) ->
    Config.

publish(Config) ->
    Packet = #mqtt_packet{
                fixed = #mqtt_packet_fixed{
                           type = ?PUBLISH,
                           dup = true,
                           qos = 1,
                           retain = true},
                variable = #mqtt_packet_publish{
                              packet_id = 16#ffff,
                              topic_name = <<"a/b">>},
                payload = <<"payload">>},
    serialise_parse(Packet, ?config(mqtt_vsn, Config)).

puback(Config) ->
    Packet = #mqtt_packet{
                fixed = #mqtt_packet_fixed{type = ?PUBACK},
                variable = #mqtt_packet_puback{packet_id = 16#ffff}
               },
    serialise_parse(Packet, ?config(mqtt_vsn, Config)).

publish_properties(Config) ->
    Packet = #mqtt_packet{
                fixed = #mqtt_packet_fixed{
                           type = ?PUBLISH,
                           dup = false,
                           qos = 0,
                           retain = false},
                variable = #mqtt_packet_publish{
                              packet_id = undefined,
                              topic_name = <<"mytopic">>,
                              props = #{'Payload-Format-Indicator' => 1,
                                        'Message-Expiry-Interval' => 16#ffffffff,
                                        'Topic-Alias' => 16#ffff,
                                        'Response-Topic' => <<"rabbit/うさぎ"/utf8>>,
                                        'Correlation-Data' => <<"some binary data">>,
                                        'User-Property' => [{<<"key1">>, <<"value1">>},
                                                            {<<"うさぎ"/utf8>>, <<"うさぎ"/utf8>>},
                                                            {<<"key3">>, <<"value3">>},
                                                            %% "The same name is allowed to appear more than once."
                                                            {<<"key1">>, <<"value1">>},
                                                            {<<"">>, <<"">>}
                                                           ],
                                        %% "The Subscription Identifier can have the value of 1 to 268,435,455."
                                        'Subscription-Identifier' => 268_435_455,
                                        'Content-Type' => <<"application/json">>}},
                payload = <<"">>},
    serialise_parse(Packet, ?config(mqtt_vsn, Config)).

puback_properties(Config) ->
    Packet = #mqtt_packet{
                fixed = #mqtt_packet_fixed{type = ?PUBACK},
                variable = #mqtt_packet_puback{
                              packet_id = 1,
                              reason_code = ?RC_QUOTA_EXCEEDED,
                              props = #{'Reason-String' => <<"Target queue rejected message">>,
                                        'User-Property' => [{<<"queue name">>, <<"queue 'q' in vhost '/'">>}]}}
               },
    serialise_parse(Packet, ?config(mqtt_vsn, Config)).

%% "The Reason Code and Property Length can be omitted if the Reason Code is 0x00
%% (Normal disconnecton) and there are no Properties. In this case the DISCONNECT
%% has a Remaining Length of 0."
disconnect_remaining_length_0(Config) ->
    Packet = #mqtt_packet{
                fixed = #mqtt_packet_fixed{type = ?DISCONNECT},
                variable = #mqtt_packet_disconnect{
                              reason_code = ?RC_NORMAL_DISCONNECTION,
                              props = #{}}
               },
    serialise_parse(Packet, ?config(mqtt_vsn, Config)).

disconnect_properties(Config) ->
    Packet = #mqtt_packet{
                fixed = #mqtt_packet_fixed{type = ?DISCONNECT},
                variable = #mqtt_packet_disconnect{
                              reason_code = ?RC_PACKET_TOO_LARGE,
                              props = #{'Reason-String' => <<"うさぎ can handle at most 2MB, but packet size was 3MB"/utf8>>,
                                        'User-Property' => [{<<"client subscriptions">>, <<"2">>},
                                                            {<<"network connection uptime">>, <<"87 minutes">>}]}}
               },
    serialise_parse(Packet, ?config(mqtt_vsn, Config)).

serialise_parse(Packet, Vsn) ->
    Binary = iolist_to_binary(rabbit_mqtt_packet:serialise(Packet, Vsn)),
    ?assertEqual({ok, Packet, <<>>, Vsn},
                 rabbit_mqtt_packet:parse(Binary, Vsn)).
