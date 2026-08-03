-module(rabbit_mqtt_packet_tests).

-include_lib("eunit/include/eunit.hrl").

-define(CONNECT, 1).
-define(PUBLISH, 3).

setup() ->
    persistent_term:put(mqtt_max_packet_size_unauthenticated, 65536),
    persistent_term:put(mqtt_max_packet_size_authenticated, 536870912),
    ok.

%% A CONNECT packet parsed in one shot must parse identically when the
%% same bytes arrive one TCP fragment at a time.
fragmented_connect_parses_test() ->
    setup(),
    Packet = connect_packet(64),
    {ok, Whole, <<>>, _} = rabbit_mqtt_packet:parse(Packet, unauthenticated),
    {ok, Frag, <<>>, _} = feed(Packet, unauthenticated),
    ?assertEqual(Whole, Frag).

%% Same for a PUBLISH body split across continuation calls.
fragmented_publish_parses_test() ->
    setup(),
    Packet = publish_packet(),
    {ok, Whole, <<>>, _} = rabbit_mqtt_packet:parse(Packet, 4),
    {ok, Frag, <<>>, _} = feed(Packet, 4),
    ?assertEqual(Whole, Frag).

%% When the final fragment also carries the next packet, the leftover bytes
%% must be returned as the Rest binary.
fragment_overshoot_returns_rest_test() ->
    setup(),
    Packet = connect_packet(64),
    PingReq = <<16#c0, 0>>,
    Stream = <<Packet/binary, PingReq/binary>>,
    {ok, _Whole, Rest0, _} = rabbit_mqtt_packet:parse(Stream, unauthenticated),
    ?assertEqual(PingReq, Rest0),
    {ok, _Frag, <<>>, S1} = feed(Stream, unauthenticated),
    %% the leftover bytes still parse as the next packet on the same connection
    {ok, _Ping, <<>>, _} = feed(PingReq, S1).

feed(Bin, State) -> feed(Bin, State, 0).
feed(<<>>, State, _) -> {ok, State};
feed(Bin, _State, N) when N > 100000 ->
    error({too_many_iterations, byte_size(Bin)});
feed(<<B, Rest/binary>>, State, N) ->
    case rabbit_mqtt_packet:parse(<<B>>, State) of
        {more, S1} -> feed(Rest, S1, N + 1);
        {ok, Packet, Rest1, S1} -> {ok, Packet, Rest1, S1};
        {error, E} -> error({parse_error, E})
    end.

connect_packet(PasswordSize) ->
    ProtocolName = <<0, 4, "MQTT">>,
    Version = 4,
    Flags = 2#11000010,
    KeepAlive = <<0, 60>>,
    ClientId = <<0, 1, "a">>,
    Username = <<0, 4, "user">>,
    Password = <<PasswordSize:16, 0:(PasswordSize*8)>>,
    VarHdr = <<ProtocolName/binary, Version, Flags, KeepAlive/binary>>,
    Payload = <<ClientId/binary, Username/binary, Password/binary>>,
    Body = <<VarHdr/binary, Payload/binary>>,
    RemLen = encode_remlen(byte_size(Body)),
    <<16#10, RemLen/binary, Body/binary>>.

publish_packet() ->
    Topic = <<"a/b/c">>,
    TopicLen = byte_size(Topic),
    Payload = binary:copy(<<"x">>, 300),
    Body = <<TopicLen:16, Topic/binary, Payload/binary>>,
    RemLen = encode_remlen(byte_size(Body)),
    <<16#30, RemLen/binary, Body/binary>>.

encode_remlen(N) when N < 128 -> <<N>>;
encode_remlen(N) ->
    Low = N rem 128 + 128,
    High = encode_remlen(N div 128),
    <<Low, High/binary>>.
