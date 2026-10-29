%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(packet_validation_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("rabbit_mqtt_packet.hrl").

all() ->
    [connect_reserved_flag, fixed_header_flags, publish_fixed_header_flags,
     connect_will_flags, connect_password_flag,
     utf8_strings, publish_utf8, topic_names_and_filters,
     subscription_payloads, subscription_identifier_zero].

init_per_suite(Config) ->
    persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_UNAUTHENTICATED, 65_536),
    persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED, ?MAX_PACKET_SIZE),
    persistent_term:put(?PERSISTENT_TERM_MAX_TOPIC_FILTERS_PER_PACKET, 512),
    persistent_term:put(?PERSISTENT_TERM_MAX_USER_PROPERTIES_PER_PACKET, 512),
    Config.

end_per_suite(_Config) ->
    ok.

connect_reserved_flag(_Config) ->
    [begin
         ?assertMatch({ok, _, <<>>, Vsn}, parse_connect(Vsn, 2, <<>>)),
         ?assertEqual({error, reserved_connect_flag}, parse_connect(Vsn, 3, <<>>)),
         Packet = connect_packet(Vsn, 3, <<>>),
         [begin
              <<First:Split/binary, Rest/binary>> = Packet,
              {more, Cont} = rabbit_mqtt_packet:parse(First, unauthenticated),
              ?assertEqual({error, reserved_connect_flag}, rabbit_mqtt_packet:parse(Rest, Cont))
          end || Split <- lists:seq(1, byte_size(Packet) - 1)]
     end || Vsn <- [4, 5]],
    ok.

fixed_header_flags(_Config) ->
    [begin
         ?assertMatch({ok, _, <<>>, Vsn}, rabbit_mqtt_packet:parse(<<16#C0, 0>>, Vsn)),
         [?assertThrow(invalid_fixed_header,
                       rabbit_mqtt_packet:parse(<<Type:4, Flags:4, 0>>, Vsn))
          || {Type, ValidFlags} <- [{?SUBSCRIBE, 2}, {?UNSUBSCRIBE, 2},
                                    {?PUBACK, 0}, {?PINGREQ, 0}, {?DISCONNECT, 0}],
             Flags <- lists:seq(0, 15), Flags =/= ValidFlags]
     end || Vsn <- [4, 5]],
    ?assertMatch({ok, _, <<>>, 3},
                 rabbit_mqtt_packet:parse(<<16#8B, 6, 0, 1, 0, 1, "a", 0>>, 3)),
    ?assertMatch({ok, _, <<>>, 3},
                 rabbit_mqtt_packet:parse(<<16#AB, 5, 0, 1, 0, 1, "a">>, 3)),
    ok.

publish_fixed_header_flags(_Config) ->
    [begin
         PacketId = case QoS of 0 -> <<>>; _ -> <<0, 1>> end,
         Packet = packet(16#30 bor (Dup bsl 3) bor (QoS bsl 1) bor Retain,
                         <<0, 1, "a", PacketId/binary, (properties(Vsn))/binary>>),
         case QoS =:= 3 orelse (QoS =:= 0 andalso Dup =:= 1) of
             true -> ?assertThrow(invalid_fixed_header, rabbit_mqtt_packet:parse(Packet, Vsn));
             false -> ?assertMatch({ok, _, <<>>, Vsn}, rabbit_mqtt_packet:parse(Packet, Vsn))
         end
     end || Vsn <- [4, 5], QoS <- lists:seq(0, 3), Dup <- [0, 1], Retain <- [0, 1]],
    ok.

connect_will_flags(_Config) ->
    [begin
         [assert_connect_error(connect_packet(Vsn, Flags, <<>>), invalid_will_flags)
          || Flags <- [16#0A, 16#12, 16#22, 16#1A, 16#1E, 16#3E]],
         Will = <<(properties(Vsn))/binary, 0, 1, "a", 0, 1, "b">>,
         [?assertMatch({ok, _, <<>>, Vsn}, parse_connect(Vsn, Flags, Will))
          || Flags <- [16#06, 16#0E, 16#26, 16#2E]]
     end || Vsn <- [4, 5]],
    ok.

connect_password_flag(_Config) ->
    Password = <<0, 2, 255, 0>>,
    assert_connect_error(connect_packet(4, 16#42, Password), password_without_username),
    ?assertMatch({ok, _, <<>>, 5}, parse_connect(5, 16#42, Password)),
    [?assertMatch({ok, _, <<>>, Vsn},
                  parse_connect(Vsn, 16#C2, <<0, 1, "u", Password/binary>>))
     || Vsn <- [4, 5]],
    ok.

utf8_strings(_Config) ->
    Invalid = invalid_utf8_strings(),
    [begin
         Props = properties(Vsn),
         Encoded = <<(byte_size(Bad)):16, Bad/binary>>,
         Body = <<0, 4, "MQTT", Vsn, 2, 0, 60, Props/binary, Encoded/binary>>,
         assert_connect_error(packet(16#10, Body), invalid_utf8),
         assert_connect_error(connect_packet(Vsn, 16#82, Encoded), invalid_utf8),
         assert_connect_error(connect_packet(Vsn, 6, <<Props/binary, Encoded/binary, 0, 0>>), invalid_utf8),
         ?assertThrow(invalid_utf8, rabbit_mqtt_packet:parse(packet(16#82, <<0, 1, Props/binary, Encoded/binary, 0>>), Vsn)),
         ?assertThrow(invalid_utf8, rabbit_mqtt_packet:parse(packet(16#A2, <<0, 1, Props/binary, Encoded/binary>>), Vsn))
     end || Vsn <- [4, 5], Bad <- Invalid],
    [begin
         Encoded = <<(byte_size(Bad)):16, Bad/binary>>,
         UserProps = [<<16#26, 0, 1, "k", Encoded/binary>>,
                      <<16#26, Encoded/binary, 0, 1, "v">>],
         [assert_connect_error(
            packet(16#10, <<0, 4, "MQTT", 5, 2, 0, 60,
                             (byte_size(Props)), Props/binary, 0, 4, "test">>), invalid_utf8)
          || Props <- [<<16#15, Encoded/binary>> | UserProps]],
         [assert_connect_error(
            connect_packet(5, 6, <<(byte_size(Props)), Props/binary, 0, 1, "a", 0, 0>>), invalid_utf8)
          || Props <- [<<16#03, Encoded/binary>>, <<16#08, Encoded/binary>> | UserProps]],
         [?assertThrow(invalid_utf8,
                       rabbit_mqtt_packet:parse(packet(Header, <<Prefix/binary, (byte_size(Props)),
                                                                Props/binary, Payload/binary>>), 5))
          || {Header, Prefix, Payload, Properties} <-
                 [{16#40, <<0, 1, 0>>, <<>>, [<<16#1F, Encoded/binary>> | UserProps]},
                  {16#E0, <<0>>, <<>>, [<<16#1C, Encoded/binary>>, <<16#1F, Encoded/binary>> | UserProps]},
                  {16#82, <<0, 1>>, <<0, 1, "a", 0>>, UserProps},
                  {16#A2, <<0, 1>>, <<0, 1, "a">>, UserProps}],
             Props <- Properties]
     end || Bad <- Invalid],
    Good = <<16#FEFF/utf8, 16#1F600/utf8>>,
    [?assertMatch({ok, #mqtt_packet{variable = #mqtt_packet_publish{topic_name = Good}}, <<>>, Vsn},
                  rabbit_mqtt_packet:parse(packet(16#30, <<(byte_size(Good)):16, Good/binary,
                                                          (properties(Vsn))/binary, 255, 0>>), Vsn))
     || Vsn <- [4, 5]],
    ok.

publish_utf8(_Config) ->
    [begin
         Encoded = <<(byte_size(Bad)):16, Bad/binary>>,
         [?assertThrow(invalid_utf8,
                       rabbit_mqtt_packet:parse(packet(16#30, <<Encoded/binary, (properties(Vsn))/binary>>), Vsn))
          || Vsn <- [4, 5]],
         [?assertThrow(invalid_utf8,
                       rabbit_mqtt_packet:parse(packet(16#30, <<0, 1, "a", (byte_size(Props)), Props/binary>>), 5))
          || Props <- [<<16#03, Encoded/binary>>,
                       <<16#08, Encoded/binary>>,
                       <<16#26, Encoded/binary, 0, 1, "v">>,
                       <<16#26, 0, 1, "k", Encoded/binary>>]]
     end || Bad <- invalid_utf8_strings()],
    ok.

invalid_utf8_strings() ->
    [<<0>>, <<255>>, <<16#C0, 16#80>>, <<16#ED, 16#A0, 16#80>>,
     <<16#F4, 16#90, 16#80, 16#80>>, <<16#E2, 16#82>>].

topic_names_and_filters(_Config) ->
    [begin
         Props = properties(Vsn),
         [?assertThrow(invalid_topic_name,
                       rabbit_mqtt_packet:parse(packet(16#32, <<(byte_size(Topic)):16, Topic/binary,
                                                                0, 1, Props/binary>>), Vsn))
          || Topic <- [<<"a/+">>, <<"a/#">>, <<"a+b">>]],
         [assert_connect_error(
            connect_packet(Vsn, 6, <<Props/binary, (byte_size(Topic)):16, Topic/binary, 0, 0>>),
            invalid_topic_name)
          || Topic <- [<<>>, <<"a/+">>, <<"a/#">>, <<"a+b">>]],
         [?assertMatch({ok, _, <<>>, Vsn},
                       parse_connect(Vsn, 6, <<Props/binary, (byte_size(Topic)):16, Topic/binary, 0, 0>>))
          || Topic <- [<<"/">>, <<"a/b">>, <<"$SYS/a">>]],
         [?assertException(error, {badmatch, _},
                           rabbit_mqtt_packet:parse(packet(Header, <<0, 1, Props/binary,
                                                                     (byte_size(Filter)):16, Filter/binary,
                                                                     Options/binary>>), Vsn))
          || Filter <- [<<>>, <<"a+">>, <<"a/#/b">>, <<"a#">>],
             {Header, Options} <- [{16#82, <<0>>}, {16#A2, <<>>}]],
         [?assertMatch({ok, _, <<>>, Vsn},
                       rabbit_mqtt_packet:parse(packet(16#82, <<0, 1, Props/binary,
                                                                (byte_size(Filter)):16, Filter/binary, 0>>), Vsn))
          || Filter <- [<<"/">>, <<"#">>, <<"+/#">>, <<"a//+">>, <<"$SYS/#">>]]
     end || Vsn <- [4, 5]],
    ?assertThrow(invalid_topic_name, rabbit_mqtt_packet:parse(<<16#32, 4, 0, 0, 0, 1>>, 4)),
    ?assertError({badmatch, false}, rabbit_mqtt_packet:parse(<<16#32, 5, 0, 0, 0, 1, 0>>, 5)),
    ?assertMatch({ok, _, <<>>, 5}, rabbit_mqtt_packet:parse(<<16#30, 6, 0, 0, 3, 16#23, 0, 1>>, 5)),
    ok.

subscription_payloads(_Config) ->
    [begin
         Props = properties(Vsn),
         [?assertError({badmatch, []},
                       rabbit_mqtt_packet:parse(packet(Header, <<0, 1, Props/binary>>), Vsn))
          || Header <- [16#82, 16#A2]],
         ?assertError(function_clause,
                      rabbit_mqtt_packet:parse(packet(16#82, <<0, 1, Props/binary, 0, 1, "a", 0, 0>>), Vsn)),
         ?assertError(function_clause,
                      rabbit_mqtt_packet:parse(packet(16#A2, <<0, 1, Props/binary, 0, 1, "a", 0>>), Vsn)),
         [?assertException(error, {badmatch, _},
                           rabbit_mqtt_packet:parse(packet(16#82, <<0, 1, Props/binary, 0, 1, "a", Opt>>), Vsn))
          || Opt <- [3, 16#30, 16#40, 16#80]]
     end || Vsn <- [4, 5]],
    ?assertMatch({ok, _, <<>>, 3}, rabbit_mqtt_packet:parse(<<16#82, 6, 0, 1, 0, 1, "a", 16#FC>>, 3)),
    ?assertError({badmatch, false}, rabbit_mqtt_packet:parse(<<16#82, 6, 0, 1, 0, 1, "a", 4>>, 4)),
    ?assertMatch({ok, _, <<>>, 5}, rabbit_mqtt_packet:parse(<<16#82, 7, 0, 1, 0, 0, 1, "a", 4>>, 5)),
    ok.

subscription_identifier_zero(_Config) ->
    ?assertError({badmatch, false},
                 rabbit_mqtt_packet:parse(<<16#82, 9, 0, 1, 2, 16#0B, 0, 0, 1, "a", 0>>, 5)),
    ?assertMatch({ok, _, <<>>, 5},
                 rabbit_mqtt_packet:parse(<<16#82, 9, 0, 1, 2, 16#0B, 1, 0, 1, "a", 0>>, 5)),
    ok.

parse_connect(Vsn, Flags, Payload) ->
    rabbit_mqtt_packet:parse(connect_packet(Vsn, Flags, Payload), unauthenticated).

assert_connect_error(Packet, Reason) ->
    ?assertEqual({error, Reason}, rabbit_mqtt_packet:parse(Packet, unauthenticated)),
    [begin
         <<First:Split/binary, Rest/binary>> = Packet,
         {more, Cont} = rabbit_mqtt_packet:parse(First, unauthenticated),
         ?assertEqual({error, Reason}, rabbit_mqtt_packet:parse(Rest, Cont))
     end || Split <- lists:seq(1, byte_size(Packet) - 1)],
    ok.

connect_packet(Vsn, Flags, Payload) ->
    Props = properties(Vsn),
    Body = <<0, 4, "MQTT", Vsn, Flags, 0, 60, Props/binary,
             0, 4, "test", Payload/binary>>,
    packet(16#10, Body).

packet(Header, Body) ->
    <<Header, (byte_size(Body)), Body/binary>>.

properties(4) -> <<>>;
properties(5) -> <<0>>.
