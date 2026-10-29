%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(packet_fragment_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rabbit_mqtt_packet.hrl").

-define(MAX_ENTRIES, 10).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [parallel],
      [
       fragmented_connect,
       fragmented_publish,
       fragment_carrying_next_packet,
       fragmentation_is_transparent,
       maximum_subscription_identifier_is_accepted,
       oversized_subscription_identifier_is_rejected,
       five_byte_subscription_identifier_is_rejected,
       topic_filters_up_to_limit_are_accepted,
       topic_filters_over_limit_are_rejected,
       subscription_topic_filter_does_not_reference_packet,
       fragmented_topic_filters_over_limit_are_rejected,
       user_properties_up_to_limit_are_accepted,
       user_properties_over_limit_are_rejected
      ]}
    ].

init_per_suite(Config) ->
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_UNAUTHENTICATED, 65_536),
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED, ?MAX_PACKET_SIZE),
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_TOPIC_FILTERS_PER_PACKET, ?MAX_ENTRIES),
    ok = persistent_term:put(?PERSISTENT_TERM_MAX_USER_PROPERTIES_PER_PACKET, ?MAX_ENTRIES),
    Config.

end_per_suite(_Config) ->
    ok.

%% A CONNECT packet parsed in one shot must parse identically when the same
%% bytes arrive one at a time.
fragmented_connect(_Config) ->
    Packet = connect_packet(64),
    ?assertEqual(parse_all(Packet, unauthenticated),
                 feed_all(Packet, unauthenticated, 1)).

%% Same for a PUBLISH body, which is parsed by a different code path.
fragmented_publish(_Config) ->
    Packet = publish_packet(300),
    ?assertEqual(parse_all(Packet, 4),
                 feed_all(Packet, 4, 1)).

%% A fragment that completes a packet may also carry the first bytes of the
%% next one. Those bytes must come back as Rest rather than being swallowed or
%% counted towards the completed packet.
fragment_carrying_next_packet(_Config) ->
    Connect = connect_packet(64),
    PingReq = pingreq_packet(),
    Stream = <<Connect/binary, PingReq/binary>>,
    %% A fragment size that does not divide the CONNECT packet size, so that
    %% one fragment necessarily straddles the boundary between the two packets.
    FragSize = 10,
    ?assertNotEqual(0, byte_size(Connect) rem FragSize),
    {ok, _Packet, Rest, _State} =
        feed(Stream, unauthenticated, FragSize),
    %% A non-empty Rest is what proves a fragment overshot the packet end:
    %% had every fragment landed on or before the boundary, Rest would be <<>>.
    ?assertEqual(PingReq, Rest),
    ?assertEqual(parse_all(Stream, unauthenticated),
                 feed_all(Stream, unauthenticated, FragSize)).

%% How the stream is split must never change what is parsed out of it.
fragmentation_is_transparent(_Config) ->
    Connect = connect_packet(64),
    Streams =
        [{connect, Connect},
         {connect_pingreq, <<Connect/binary, (pingreq_packet())/binary>>},
         {connect_publish, <<Connect/binary, (publish_packet(300))/binary>>},
         %% 20000 bytes needs a 3-byte remaining length, so this also covers
         %% a length field that is itself split across fragments.
         {many, <<Connect/binary,
                  (publish_packet(200))/binary,
                  (publish_packet(20000))/binary,
                  (pingreq_packet())/binary>>}],
    FragSizes = lists:seq(1, 24) ++ [32, 47, 64, 100, 512, 4096],
    [begin
         Expected = parse_all(Stream, unauthenticated),
         ?assertEqual(Expected, feed_all(Stream, unauthenticated, FragSize),
                      {Name, FragSize})
     end || {Name, Stream} <- Streams, FragSize <- FragSizes],
    ok.

maximum_subscription_identifier_is_accepted(_Config) ->
    {ok, Packet, <<>>, _} = rabbit_mqtt_packet:parse(subscribe_packet(<<16#FF, 16#FF, 16#FF, 16#7F>>), 5),
    #mqtt_packet{variable = #mqtt_packet_subscribe{props = Props}} = Packet,
    ?assertEqual(?VARIABLE_BYTE_INTEGER_MAX, maps:get('Subscription-Identifier', Props)).

%% 16#10000000
oversized_subscription_identifier_is_rejected(_Config) ->
    ?assertThrow(malformed_variable_byte_integer,
                 rabbit_mqtt_packet:parse(subscribe_packet(<<16#80, 16#80, 16#80, 16#80, 16#01>>), 5)).

%% 2^35 - 1, the largest value a 5-byte encoding can carry
five_byte_subscription_identifier_is_rejected(_Config) ->
    ?assertThrow(malformed_variable_byte_integer,
                 rabbit_mqtt_packet:parse(subscribe_packet(<<16#FF, 16#FF, 16#FF, 16#FF, 16#7F>>), 5)).

topic_filters_up_to_limit_are_accepted(_Config) ->
    [begin
         {ok, #mqtt_packet{variable = #mqtt_packet_subscribe{subscriptions = Subs}}, <<>>, _} =
         rabbit_mqtt_packet:parse(subscribe_packet(?MAX_ENTRIES, Vsn), Vsn),
         ?assertEqual(?MAX_ENTRIES, length(Subs)),
         {ok, #mqtt_packet{variable = #mqtt_packet_unsubscribe{topic_filters = Topics}}, <<>>, _} =
         rabbit_mqtt_packet:parse(unsubscribe_packet(?MAX_ENTRIES, Vsn), Vsn),
         ?assertEqual(?MAX_ENTRIES, length(Topics))
     end || Vsn <- [3, 4, 5]],
    ok.

subscription_topic_filter_does_not_reference_packet(_Config) ->
    Filter = binary:copy(<<"a">>, 100),
    Body = <<1:16, 100:16, Filter/binary, 0>>,
    Packet = <<?SUBSCRIBE:4, 2:4, (remaining_length(byte_size(Body)))/binary, Body/binary>>,
    {ok, #mqtt_packet{variable = #mqtt_packet_subscribe{
                                    subscriptions = [#mqtt_subscription{topic_filter = Parsed}]}},
     <<>>, _} = rabbit_mqtt_packet:parse(Packet, 4),
    ?assertEqual(Filter, Parsed),
    ?assertEqual(byte_size(Parsed), binary:referenced_byte_size(Parsed)).

topic_filters_over_limit_are_rejected(_Config) ->
    [begin
         ?assertEqual({error, {disconnect_reason_code, ?RC_QUOTA_EXCEEDED}},
                      rabbit_mqtt_packet:parse(subscribe_packet(?MAX_ENTRIES + 1, Vsn), Vsn)),
         ?assertEqual({error, {disconnect_reason_code, ?RC_QUOTA_EXCEEDED}},
                      rabbit_mqtt_packet:parse(unsubscribe_packet(?MAX_ENTRIES + 1, Vsn), Vsn))
     end || Vsn <- [3, 4, 5]],
    ok.

fragmented_topic_filters_over_limit_are_rejected(_Config) ->
    ?assertEqual({error, {disconnect_reason_code, ?RC_QUOTA_EXCEEDED}},
                 feed_until_error(subscribe_packet(?MAX_ENTRIES + 1, 5), 5, 1)).

user_properties_up_to_limit_are_accepted(_Config) ->
    [begin
         {ok, #mqtt_packet{variable = #mqtt_packet_publish{props = Props}}, <<>>, _} =
         rabbit_mqtt_packet:parse(publish_packet_with_user_properties(N), 5),
         ?assertEqual([{<<"k">>, integer_to_binary(I)} || I <- lists:seq(1, N)],
                      maps:get('User-Property', Props))
     end || N <- [1, ?MAX_ENTRIES]],
    ok.

user_properties_over_limit_are_rejected(_Config) ->
    ?assertEqual({error, {disconnect_reason_code, ?RC_QUOTA_EXCEEDED}},
                 rabbit_mqtt_packet:parse(publish_packet_with_user_properties(?MAX_ENTRIES + 1), 5)).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%

%% Parse a whole binary the way the readers do: whatever is left over after a
%% packet is parsed is fed straight back in.
parse_all(Bin, State) ->
    parse_all(Bin, State, []).

parse_all(<<>>, _State, Acc) ->
    lists:reverse(Acc);
parse_all(Bin, State, Acc) ->
    case rabbit_mqtt_packet:parse(Bin, State) of
        {ok, Packet, Rest, State1} ->
            parse_all(Rest, State1, [Packet | Acc]);
        {more, _State1} ->
            lists:reverse(Acc);
        {error, Reason} ->
            ct:fail({parse_error, Reason})
    end.

%% Same, but delivering the bytes FragSize at a time.
feed_all(Bin, State, FragSize) ->
    feed_all(Bin, State, FragSize, []).

feed_all(<<>>, _State, _FragSize, Acc) ->
    lists:reverse(Acc);
feed_all(Bin, State, FragSize, Acc) ->
    case feed(Bin, State, FragSize) of
        {more, _State1} ->
            lists:reverse(Acc);
        {ok, Packet, Rest, State1} ->
            feed_all(Rest, State1, FragSize, [Packet | Acc])
    end.

%% Feed fragments until the first packet is complete. Returns the leftover
%% bytes, which include everything not yet delivered.
feed(Bin, State, FragSize) ->
    N = min(FragSize, byte_size(Bin)),
    <<Frag:N/binary, Undelivered/binary>> = Bin,
    case rabbit_mqtt_packet:parse(Frag, State) of
        {more, State1} when Undelivered =:= <<>> ->
            {more, State1};
        {more, State1} ->
            feed(Undelivered, State1, FragSize);
        {ok, Packet, Rest, State1} ->
            {ok, Packet, <<Rest/binary, Undelivered/binary>>, State1};
        {error, Reason} ->
            ct:fail({parse_error, Reason})
    end.

feed_until_error(Bin, State, FragSize) ->
    N = min(FragSize, byte_size(Bin)),
    <<Frag:N/binary, Undelivered/binary>> = Bin,
    case rabbit_mqtt_packet:parse(Frag, State) of
        {more, State1} when Undelivered =/= <<>> ->
            feed_until_error(Undelivered, State1, FragSize);
        Other ->
            Other
    end.

%%%%%%%%%%%%%%%%
%%% Builders %%%
%%%%%%%%%%%%%%%%

connect_packet(PasswordSize) ->
    ProtoName = <<0, 4, "MQTT">>,
    KeepAlive = <<0, 60>>,
    ClientId = <<0, 1, "a">>,
    Username = <<0, 4, "user">>,
    Password = <<PasswordSize:16, 0:(PasswordSize * 8)>>,
    %% User name and password flags set, clean session.
    Flags = 2#11000010,
    Body = <<ProtoName/binary, 4, Flags, KeepAlive/binary,
             ClientId/binary, Username/binary, Password/binary>>,
    <<?CONNECT:4, 0:4, (remaining_length(byte_size(Body)))/binary, Body/binary>>.

publish_packet(PayloadSize) ->
    Topic = <<"a/b/c">>,
    Body = <<(byte_size(Topic)):16, Topic/binary,
             (binary:copy(<<"x">>, PayloadSize))/binary>>,
    <<?PUBLISH:4, 0:4, (remaining_length(byte_size(Body)))/binary, Body/binary>>.

subscribe_packet(SubscriptionId) ->
    Props = <<16#0B, SubscriptionId/binary>>,
    Body = <<1:16, (byte_size(Props)), Props/binary, 1:16, "a", 0>>,
    <<?SUBSCRIBE:4, 2:4, (byte_size(Body)), Body/binary>>.

subscribe_packet(NumFilters, Vsn) ->
    Filters = << <<1:16, "a", 0>> || _ <- lists:seq(1, NumFilters) >>,
    Body = <<1:16, (props(Vsn))/binary, Filters/binary>>,
    <<?SUBSCRIBE:4, 2:4, (remaining_length(byte_size(Body)))/binary, Body/binary>>.

unsubscribe_packet(NumFilters, Vsn) ->
    Filters = << <<1:16, "a">> || _ <- lists:seq(1, NumFilters) >>,
    Body = <<1:16, (props(Vsn))/binary, Filters/binary>>,
    <<?UNSUBSCRIBE:4, 2:4, (remaining_length(byte_size(Body)))/binary, Body/binary>>.

props(5) -> <<0>>;
props(_) -> <<>>.

publish_packet_with_user_properties(N) ->
    Topic = <<"a/b/c">>,
    Props = << <<16#26, 1:16, "k", (byte_size(V)):16, V/binary>>
               || I <- lists:seq(1, N), V <- [integer_to_binary(I)] >>,
    Body = <<(byte_size(Topic)):16, Topic/binary,
             (remaining_length(byte_size(Props)))/binary, Props/binary>>,
    <<?PUBLISH:4, 0:4, (remaining_length(byte_size(Body)))/binary, Body/binary>>.

pingreq_packet() ->
    <<?PINGREQ:4, 0:4, 0>>.

remaining_length(N) when N < 128 ->
    <<N>>;
remaining_length(N) ->
    <<(N rem 128 + 128), (remaining_length(N div 128))/binary>>.
