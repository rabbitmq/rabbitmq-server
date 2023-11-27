%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

-module(packet_prop_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_mqtt_packet.hrl").

-import(rabbit_ct_proper_helpers, [run_proper/3]).

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
     {v5, [parallel, shuffle], test_cases() ++ [prop_publish_properties,
                                                prop_puback_properties,
                                                prop_disconnect]}
    ].

test_cases() ->
    [
     prop_publish,
     prop_puback
    ].

init_per_suite(Config) ->
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED, ?MAX_PACKET_SIZE),
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

%%%%%%%%%%%%%%%%%%
%%% Properties %%%
%%%%%%%%%%%%%%%%%%

prop_publish(Config) ->
    run_proper(fun publish_packet/0, Config).

prop_publish_properties(Config) ->
    run_proper(fun publish_with_properties_packet/0, Config).

prop_puback(Config) ->
    run_proper(fun puback_packet/0, Config).

prop_puback_properties(Config) ->
    run_proper(fun puback_with_properties_packet/0, Config).

prop_disconnect(Config) ->
    run_proper(fun disconnect_packet/0, Config).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%

run_proper(Generator, Config) ->
    run_proper(fun() -> ?FORALL(Packet,
                                Generator(),
                                symmetric(Packet, Config))
               end, [], 100).

symmetric(Packet, Config) ->
    Vsn = ?config(mqtt_vsn, Config),
    Binary = iolist_to_binary(rabbit_mqtt_packet:serialise(Packet, Vsn)),
    equals({ok, Packet, <<>>, Vsn},
           rabbit_mqtt_packet:parse(Binary, Vsn)).

%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%

publish_packet() ->
    ?LET(Qos, qos(),
         #mqtt_packet{
            fixed = #mqtt_packet_fixed{
                       type = ?PUBLISH,
                       dup = boolean(),
                       qos = Qos,
                       retain = boolean()},
            variable = #mqtt_packet_publish{
                          packet_id = packet_id(Qos),
                          topic_name = utf8_string()},
            payload = binary()}).

publish_with_properties_packet() ->
    ?LET(Packet = #mqtt_packet{variable = Publish},
         publish_packet(),
         Packet#mqtt_packet{variable = Publish#mqtt_packet_publish{props = publish_properties()}}).

puback_packet() ->
    #mqtt_packet{
       fixed = #mqtt_packet_fixed{type = ?PUBACK},
       variable = #mqtt_packet_puback{packet_id = packet_id()}
      }.

puback_with_properties_packet() ->
    ?LET(Packet = #mqtt_packet{variable = Puback},
         puback_packet(),
         Packet#mqtt_packet{variable = Puback#mqtt_packet_puback{reason_code = reason_code(),
                                                                 props = puback_properties()}}).

disconnect_packet() ->
    #mqtt_packet{
       fixed = #mqtt_packet_fixed{type = ?DISCONNECT},
       variable = #mqtt_packet_disconnect{
                     reason_code = reason_code(),
                     props = disconnect_properties()}}.

publish_properties() ->
    ?LET(L,
         list(elements([{'Payload-Format-Indicator', bit()},
                        {'Message-Expiry-Interval', four_byte_integer()},
                        {'Topic-Alias', two_byte_integer()},
                        {'Response-Topic', utf8_string()},
                        {'Correlation-Data', binary_data()},
                        user_property(),
                        {'Content-Type', utf8_string()}])),
         maps:from_list(L)).

puback_properties() ->
    ?LET(L,
         list(elements([{'Reason-String', utf8_string()},
                        user_property()
                       ])),
         maps:from_list(L)).

disconnect_properties() ->
    ?LET(L,
         list(elements([{'Session-Expiry-Interval', four_byte_integer()},
                        {'Reason-String', utf8_string()},
                        user_property()
                       ])),
         maps:from_list(L)).

user_property() ->
    {'User-Property',
     non_empty(list(frequency(
                      [{5, utf8_string_pair()},
                       %% "The same name is allowed to appear more than once." [v5 3.3.2.3.7]
                       {1, {<<"same name">>, utf8_string()}},
                       {1, {<<"same name">>, <<"same value">>}}
                      ])))}.

qos() ->
    range(0, 2).

packet_id() ->
    non_zero_two_byte_integer().

%% "The Packet Identifier field is only present in PUBLISH packets
%% where the QoS level is 1 or 2." [v5 3.3.2.2]
packet_id(0) ->
    undefined;
packet_id(Qos) when Qos =:= 1;
                    Qos =:= 2 ->
    packet_id().

two_byte_integer() ->
    integer(0, 16#ffff).

non_zero_two_byte_integer() ->
    integer(1, 16#ffff).

four_byte_integer() ->
    integer(0, 16#ffffffff).

%% v5 1.5.5
variable_byte_integer() ->
    integer(0, 268_435_455).

non_zero_variable_byte_integer() ->
    integer(1, 268_435_455).

%% "The length of Binary Data is limited to the range of 0 to 65,535 Bytes." [v5 1.5.6]
binary_data() ->
    binary_up_to(16#ffff).

binary_up_to(N) ->
    ?LET(X, integer(0, N), binary(X)).

%% v5 1.5.7
utf8_string_pair() ->
    {utf8_string(), utf8_string()}.

%% "Unless stated otherwise all UTF-8 encoded strings can have any length
%% in the range 0 to 65,535 bytes." v5 1.5.4
utf8_string() ->
    %% Defining an upper size other than 'inf' is too slow because the
    %% test ?SIZE is not taken into account anymore.
    MaxCodePointSize = 4,
    MaxCodePoints = 16#ffff div MaxCodePointSize,
    ?LET(Bin, utf8(inf, MaxCodePointSize),
         begin
             L0 = unicode:characters_to_list(Bin, utf8),
             L = lists:sublist(L0, MaxCodePoints),
             unicode:characters_to_binary(L, utf8, utf8)
         end).

%% "A Reason Code is a one byte unsigned value" [v5 2.4]
reason_code() ->
    %% Choose "Success" more often because the serialiser will omit some bytes.
    oneof([_Success = 0, byte()]).

bit() ->
    oneof([0, 1]).
