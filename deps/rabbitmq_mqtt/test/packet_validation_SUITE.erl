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
    [connect_reserved_flag, fixed_header_flags].

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

parse_connect(Vsn, Flags, Payload) ->
    rabbit_mqtt_packet:parse(connect_packet(Vsn, Flags, Payload), unauthenticated).

connect_packet(Vsn, Flags, Payload) ->
    Props = properties(Vsn),
    Body = <<0, 4, "MQTT", Vsn, Flags, 0, 60, Props/binary,
             0, 4, "test", Payload/binary>>,
    packet(16#10, Body).

packet(Header, Body) ->
    <<Header, (byte_size(Body)), Body/binary>>.

properties(4) -> <<>>;
properties(5) -> <<0>>.
