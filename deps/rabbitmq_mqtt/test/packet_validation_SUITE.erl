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
    [connect_reserved_flag, fixed_header_flags, connect_will_flags, connect_password_flag, utf8_strings].

init_per_suite(Config) ->
    persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_UNAUTHENTICATED, 65_536),
    persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED, ?MAX_PACKET_SIZE),
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
    Invalid = [<<0>>, <<255>>, <<16#C0, 16#80>>, <<16#ED, 16#A0, 16#80>>,
               <<16#F4, 16#90, 16#80, 16#80>>, <<16#E2, 16#82>>],
    [begin
         Props = properties(Vsn),
         Encoded = <<(byte_size(Bad)):16, Bad/binary>>,
         Body = <<0, 4, "MQTT", Vsn, 2, 0, 60, Props/binary, Encoded/binary>>,
         assert_connect_error(packet(16#10, Body), invalid_utf8),
         assert_connect_error(connect_packet(Vsn, 16#82, Encoded), invalid_utf8),
         assert_connect_error(connect_packet(Vsn, 6, <<Props/binary, Encoded/binary, 0, 0>>), invalid_utf8),
         ?assertThrow(invalid_utf8, rabbit_mqtt_packet:parse(packet(16#30, <<Encoded/binary, Props/binary>>), Vsn)),
         ?assertThrow(invalid_utf8, rabbit_mqtt_packet:parse(packet(16#82, <<0, 1, Props/binary, Encoded/binary, 0>>), Vsn)),
         ?assertThrow(invalid_utf8, rabbit_mqtt_packet:parse(packet(16#A2, <<0, 1, Props/binary, Encoded/binary>>), Vsn))
     end || Vsn <- [4, 5], Bad <- Invalid],
    [begin
         UserProps = <<16#26, 0, 1, "k", (byte_size(Bad)):16, Bad/binary>>,
         Body = <<0, 1, "a", (byte_size(UserProps)), UserProps/binary>>,
         ?assertThrow(invalid_utf8, rabbit_mqtt_packet:parse(packet(16#30, Body), 5))
     end || Bad <- Invalid],
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
          || Props <- [<<16#03, Encoded/binary>>, <<16#08, Encoded/binary>> | UserProps]]
     end || Bad <- Invalid],
    Good = <<16#FEFF/utf8, 16#1F600/utf8>>,
    [?assertMatch({ok, #mqtt_packet{variable = #mqtt_packet_publish{topic_name = Good}}, <<>>, Vsn},
                  rabbit_mqtt_packet:parse(packet(16#30, <<(byte_size(Good)):16, Good/binary,
                                                          (properties(Vsn))/binary, 255, 0>>), Vsn))
     || Vsn <- [4, 5]],
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
