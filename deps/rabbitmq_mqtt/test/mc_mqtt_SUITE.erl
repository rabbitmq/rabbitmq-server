-module(mc_mqtt_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("rabbitmq_mqtt/include/rabbit_mqtt_packet.hrl").
-include_lib("rabbit/include/mc.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle],
      [roundtrip_amqp,
       roundtrip_amqp_response_topic,
       roundtrip_amqpl,
       roundtrip_amqpl_correlation,
       amqp_to_mqtt_body_sections,
       roundtrip_amqp_user_property,
       roundtrip_amqpl_user_property,
       roundtrip_amqp_content_type,
       amqp_to_mqtt_reply_to,
       amqp_to_mqtt_footer,
       mqtt_amqpl,
       mqtt_amqpl_alt,
       mqtt_amqp,
       mqtt_amqp_alt,
       amqp_mqtt,
       is_persistent
      ]}
    ].

roundtrip_amqp(_Config) ->
    Msg = #mqtt_msg{
             qos = 1,
             topic = <<"/my/topic">>,
             payload = <<"my payload">>,
             props = #{'Content-Type' => <<"text-plain">>,
                       'Correlation-Data' => <<0, 255, 0>>,
                       'User-Property' => [{<<"x-key-2">>, <<"val-2">>},
                                           {<<"x-key-3">>, <<"val-3">>},
                                           {<<"x-key-1">>, <<"val-1">>},
                                           {<<"key-2">>, <<"val-2">>},
                                           {<<"key-3">>, <<"val-3">>},
                                           {<<"key-1">>, <<"val-1">>}]}},
    Anns = #{?ANN_ROUTING_KEYS => [rabbit_mqtt_util:mqtt_to_amqp(Msg#mqtt_msg.topic)]},
    Mc0 = mc:init(mc_mqtt, Msg, Anns),

    BytesTopic = 9,
    BytesContentType = 10,
    BytesCorrelationData = 3,
    BytesUserProperty = 66,
    MetaDataSize = BytesTopic + BytesContentType + BytesCorrelationData + BytesUserProperty,
    PayloadSize = 10,
    ExpectedSize = {MetaDataSize, PayloadSize},
    ?assertEqual(ExpectedSize, mc:size(Mc0)),

    Env = #{},
    ?assertEqual(Msg, mc_mqtt:convert_to(mc_mqtt, Msg, Env)),
    ?assertEqual(not_implemented, mc_mqtt:convert_to(mc_stomp, Msg, Env)),
    ?assertEqual(Mc0, mc:convert(mc_mqtt, Mc0)),

    %% roundtrip
    Mc1 = mc:convert(mc_amqp, Mc0),
    Mc = mc:convert(mc_mqtt, Mc1),

    ?assertEqual({binary, <<0, 255, 0>>}, mc:correlation_id(Mc)),
    ?assertEqual(undefined, mc:timestamp(Mc)),
    ?assert(mc:is_persistent(Mc)),
    ?assertEqual(#{<<"key-1">> => <<"val-1">>,
                   <<"key-2">> => <<"val-2">>,
                   <<"key-3">> => <<"val-3">>},
                 mc:routing_headers(Mc, [])),
    ?assertEqual(#{<<"key-1">> => <<"val-1">>,
                   <<"key-2">> => <<"val-2">>,
                   <<"key-3">> => <<"val-3">>,
                   <<"x-key-1">> => <<"val-1">>,
                   <<"x-key-2">> => <<"val-2">>,
                   <<"x-key-3">> => <<"val-3">>},
                 mc:routing_headers(Mc, [x_headers])),
    ?assertEqual({utf8, <<"val-3">>}, mc:x_header(<<"x-key-3">>, Mc)),
    ?assertEqual(undefined, mc:x_header(<<"x-key-4">>, Mc)),

    #mqtt_msg{qos = Qos,
              topic = Topic,
              payload = Payload,
              props = Props
             } = mc:protocol_state(Mc),
    ?assertEqual(1, Qos),
    ?assertEqual(<<"/my/topic">>, Topic),
    ?assertEqual(<<"my payload">>, iolist_to_binary(Payload)),
    ?assertMatch(#{'Content-Type' := <<"text-plain">>,
                   'Correlation-Data' := <<0, 255, 0>>
                  }, Props),
    ExpectedUserProperty = maps:get('User-Property', Msg#mqtt_msg.props),
    %% We expect order to be maintained.
    ?assertMatch(#{'User-Property' := ExpectedUserProperty}, Props).

roundtrip_amqp_response_topic(_Config) ->
    Topic = <<"/rabbit/🐇"/utf8>>,
    Msg0 = mqtt_msg(),
    Key = mqtt_x,
    MqttExchanges = [<<"amq.topic">>,
                     <<"some-other-topic-exchange">>],
    [begin
         Env = #{Key => X},
         Msg = Msg0#mqtt_msg{props = #{'Response-Topic' => Topic}},
         ?assertMatch(#mqtt_msg{props = #{'Response-Topic' := Topic}},
                      roundtrip(mc_amqp, Msg, Env)),
         ok
     end || X <- MqttExchanges].

roundtrip_amqpl(_Config) ->
    Msg = #mqtt_msg{
             qos = 1,
             topic = <<"/my/topic">>,
             payload = <<"my payload">>,
             props = #{'Content-Type' => <<"text-plain">>,
                       'Correlation-Data' => <<"ABC-123">>,
                       'Response-Topic' => <<"my/response/topic">>,
                       'User-Property' => [{<<"x-key-2">>, <<"val-2">>},
                                           {<<"x-key-3">>, <<"val-3">>},
                                           {<<"x-key-1">>, <<"val-1">>},
                                           {<<"key-2">>, <<"val-2">>},
                                           {<<"key-3">>, <<"val-3">>},
                                           {<<"key-1">>, <<"val-1">>}]}},
    Anns = #{?ANN_ROUTING_KEYS => [rabbit_mqtt_util:mqtt_to_amqp(Msg#mqtt_msg.topic)]},
    Mc0 = mc:init(mc_mqtt, Msg, Anns),
    Mc1 = mc:convert(mc_amqpl, Mc0),
    Mc = mc:convert(mc_mqtt, Mc1),

    ?assertEqual({binary, <<"ABC-123">>}, mc:correlation_id(Mc)),
    ?assert(mc:is_persistent(Mc)),

    #mqtt_msg{qos = Qos,
              topic = Topic,
              payload = Payload,
              props = Props
             } = mc:protocol_state(Mc),
    ?assertEqual(1, Qos),
    ?assertEqual(<<"/my/topic">>, Topic),
    ?assertEqual(<<"my payload">>, iolist_to_binary(Payload)),
    ?assertMatch(#{'Content-Type' := <<"text-plain">>,
                   'Correlation-Data' := <<"ABC-123">>,
                   'Response-Topic' := <<"my/response/topic">>},
                 Props),
    UserProperty = maps:get('User-Property', Msg#mqtt_msg.props),
    %% We expect order to not be maintained since AMQP 0.9.1 sorts by key.
    ExpectedUserProperty = lists:keysort(1, UserProperty),
    ?assertMatch(#{'User-Property' := ExpectedUserProperty}, Props).

%% Non-UTF-8 Correlation Data should also be converted (via AMQP 0.9.1 header x-correlation-id).
roundtrip_amqpl_correlation(_Config) ->
    Msg0 = mqtt_msg(),
    Correlation = binary:copy(<<0>>, 1024),
    Msg = Msg0#mqtt_msg{
            props = #{'Correlation-Data' => Correlation}},
    ?assertMatch(#mqtt_msg{props = #{'Correlation-Data' := Correlation}},
                 roundtrip(mc_amqpl, Msg)).

amqp_to_mqtt_body_sections(_Config) ->
    %% An amqp-value section should get AMQP encoded.
    Body1 = [#'v1_0.amqp_value'{content = {list, [{uint, 3}]}}],
    #mqtt_msg{props = #{'Content-Type' := <<"message/vnd.rabbitmq.amqp">>},
              payload = Payload1} = amqp_to_mqtt(Body1),
    ?assertEqual(Body1, amqp10_framing:decode_bin(iolist_to_binary(Payload1))),

    %% amqp-sequence sections should get AMQP encoded.
    Body2 = [#'v1_0.amqp_sequence'{content = [true, false]},
             #'v1_0.amqp_sequence'{content = [{binary, <<0, 255>>}]}],
    #mqtt_msg{props = #{'Content-Type' := <<"message/vnd.rabbitmq.amqp">>},
              payload = Payload2} = amqp_to_mqtt(Body2),
    ?assertEqual(Body2, amqp10_framing:decode_bin(iolist_to_binary(Payload2))),

    %% Binary data of multiple data sections should get concatenated.
    Body3 = [#'v1_0.data'{content = <<0>>},
             #'v1_0.data'{content = <<11, 10>>},
             #'v1_0.data'{content = <<9>>}],
    #mqtt_msg{props = Props,
              payload = Payload3} = amqp_to_mqtt(Body3),
    ?assertEqual(0, maps:size(Props)),
    ?assertEqual(<<0, 11, 10, 9>>, iolist_to_binary(Payload3)).

%% When converting from MQTT 5.0 to AMQP 1.0, we expect to lose some User Property.
roundtrip_amqp_user_property(_Config) ->
    Msg0 = mqtt_msg(),
    Msg = Msg0#mqtt_msg{props = #{'User-Property' =>
                                  [{<<"x-dup"/utf8>>, <<"val-2">>},
                                   {<<"x-dup"/utf8>>, <<"val-3">>},
                                   {<<"dup">>, <<"val-4">>},
                                   {<<"dup">>, <<"val-5">>},
                                   {<<"x-key-no-ascii🐇"/utf8>>, <<"val-1">>}
                                  ]}},
    #mqtt_msg{props = Props} = roundtrip(mc_amqp, Msg),
    Lost = [%% AMQP 1.0 maps disallow duplicate keys.
            {<<"x-dup">>, <<"val-3">>},
            {<<"dup">>, <<"val-5">>},
            %% AMQP 1.0 annotations require keys to be symbols, i.e. ASCII
            {<<"x-key-no-ascii🐇"/utf8>>, <<"val-1">>}
           ],
    ExpectedUserProperty = maps:get('User-Property', Msg#mqtt_msg.props) -- Lost,
    ?assertMatch(#{'User-Property' := ExpectedUserProperty}, Props).

%% When converting from MQTT 5.0 to AMQP 0.9.1, we expect to lose any duplicates and
%% any User Property whose name is longer than 128 characters.
roundtrip_amqpl_user_property(_Config) ->
    Msg0 = mqtt_msg(),
    Msg = Msg0#mqtt_msg{
            props = #{'User-Property' => [{<<"key-2">>, <<"val-2">>},
                                          {<<"key-1">>, <<"val-1">>},
                                          {binary:copy(<<"k">>, 256), <<"val-2">>},
                                          {<<"key-1">>, <<"val-1">>}
                                         ]}},
    ?assertMatch(#mqtt_msg{props = #{'User-Property' := [{<<"key-1">>, <<"val-1">>},
                                                         {<<"key-2">>, <<"val-2">>}]}},
                 roundtrip(mc_amqpl, Msg)).

%% In MQTT 5.0 the Content Type is a UTF-8 encoded string.
%% In AMQP 1.0 the Content Type is a symbol.
%% We expect to lose the Content Type when converting from MQTT 5.0 to AMQP 1.0 if
%% the Content Type is not valid ASCII.
roundtrip_amqp_content_type(_Config) ->
    Msg0 = mqtt_msg(),
    Msg = Msg0#mqtt_msg{props = #{'Content-Type' => <<"no-ascii🐇"/utf8>>}},
    #mqtt_msg{props = Props} = roundtrip(mc_amqp, Msg),
    ?assertNot(maps:is_key('Content-Type', Props)).

amqp_to_mqtt_reply_to(_Config) ->
    Val = amqp_value({utf8, <<"hey">>}),
    Key = mqtt_x,
    Env = #{Key => <<"mqtt-topic-exchange">>},

    AmqpProps1 = #'v1_0.properties'{reply_to = {utf8, <<"/exchanges/mqtt-topic-exchange/my.routing.key">>}},
    #mqtt_msg{props = Props1} = amqp_to_mqtt([AmqpProps1, Val], Env),
    ?assertEqual({ok, <<"my/routing/key">>},
                 maps:find('Response-Topic', Props1)),

    AmqpProps2 = #'v1_0.properties'{reply_to = {utf8, <<"/exchanges/NON-mqtt-topic-exchange/my.routing.key">>}},
    #mqtt_msg{props = Props2} = amqp_to_mqtt([AmqpProps2, Val]),
    ?assertEqual(error,
                 maps:find('Response-Topic', Props2)),

    RoutingKey = <<"my.sp%$@cial.routing.key">>,
    %% The AMQP client must percent encode the AMQP reply_to address URI. We expect the
    %% AMQP -> MQTT conversion to percent decode because an MQTT response topic is not percent encoded.
    RoutingKeyQuoted = uri_string:quote(RoutingKey),
    AmqpProps3 = #'v1_0.properties'{reply_to = {utf8, <<"/exchanges/mqtt-topic-exchange/", RoutingKeyQuoted/binary>>}},
    #mqtt_msg{props = Props3} = amqp_to_mqtt([AmqpProps3, Val], Env),
    ?assertEqual({ok, <<"my/sp%$@cial/routing/key">>},
                 maps:find('Response-Topic', Props3)),

    %% If the AMQP client did not percent encode the AMQP reply_to address URI as required,
    %% then the reply_to should be ignored by the conversion.
    AmqpProps4 = #'v1_0.properties'{reply_to = {utf8, <<"/exchanges/mqtt-topic-exchange/", RoutingKey/binary>>}},
    #mqtt_msg{props = Props4} = amqp_to_mqtt([AmqpProps4, Val], Env),
    ?assertEqual(error,
                 maps:find('Response-Topic', Props4)).

amqp_to_mqtt_footer(_Config) ->
    Body = <<"hey">>,
    Footer = #'v1_0.footer'{content = [{{symbol, <<"x-key">>}, {utf8, <<"value">>}}]},
    %% We can translate, but lose the footer.
    #mqtt_msg{payload = Payload} = amqp_to_mqtt([#'v1_0.data'{content = Body}, Footer]),
    ?assertEqual(<<"hey">>, iolist_to_binary(Payload)).

mqtt_amqpl(_Config) ->
    Msg0 = mqtt_msg(),
    Msg = Msg0#mqtt_msg{qos = 1,
                        props = #{'Content-Type' => <<"text/plain">>,
                                  'User-Property' => [{<<"key-2">>, <<"val-2">>},
                                                      {<<"key-1">>, <<"val-1">>}],
                                  'Correlation-Data' => <<"banana">>,
                                  'Message-Expiry-Interval' => 1001,
                                  'Response-Topic' => <<"tmp/blah/responses">>
                                 }
                       },
    Anns = #{?ANN_ROUTING_KEYS => [rabbit_mqtt_util:mqtt_to_amqp(Msg#mqtt_msg.topic)]},
    Mc = mc:init(mc_mqtt, Msg, Anns),
    MsgL = mc:convert(mc_amqpl, Mc),

    #content{properties = #'P_basic'{headers = HL} = Props} =
        mc:protocol_state(MsgL),

    ?assertMatch(#'P_basic'{delivery_mode = 2,
                            correlation_id = <<"banana">>,
                            expiration = <<"1001000">>,
                            content_type = <<"text/plain">>}, Props),
    ?assertMatch({_, longstr, <<"val-2">>}, amqpl_header(<<"key-2">>, HL)),
    ?assertMatch({_, longstr, <<"val-1">>}, amqpl_header(<<"key-1">>, HL)),
    ?assertMatch({_, longstr, <<"tmp.blah.responses">>},
                 amqpl_header(<<"x-reply-to-topic">>, HL)),
    ok.

mqtt_amqpl_alt(_Config) ->
    InvalidUtf8 = <<14,23,97,23,144,149,12,108,140,66,151,2>>,
    Msg0 = mqtt_msg(),
    Msg = Msg0#mqtt_msg{qos = 0,
                        props = #{'Content-Type' => <<"no-ascii🐇"/utf8>>,
                                  % 'User-Property' => [{<<"key-2">>, <<"val-2">>},
                                  %                     {<<"key-1">>, <<"val-1">>}],
                                  'Correlation-Data' => InvalidUtf8
                                 }
                       },
    Anns = #{?ANN_ROUTING_KEYS => [rabbit_mqtt_util:mqtt_to_amqp(Msg#mqtt_msg.topic)]},
    Mc = mc:init(mc_mqtt, Msg, Anns),
    MsgL = mc:convert(mc_amqpl, Mc),

    #content{properties = #'P_basic'{headers = HL} = Props} =
        mc:protocol_state(MsgL),

    ?assertMatch(#'P_basic'{delivery_mode = 1,
                            correlation_id = undefined,
                            content_type = undefined}, Props),

    ?assertMatch({_, longstr, InvalidUtf8},
                 amqpl_header(<<"x-correlation-id">>, HL)),
    ok.

mqtt_amqp(_Config) ->
    Key = mqtt_x,
    Ex = <<"mqtt-topic-exchange">>,
    Env =  #{Key => <<"mqtt-topic-exchange">>},
    Mqtt0 = mqtt_msg(),
    Mqtt = Mqtt0#mqtt_msg{qos = 1,
                          props = #{'Content-Type' => <<"text/plain">>,
                                    'User-Property' =>
                                        [{<<"key-2">>, <<"val-2">>},
                                         {<<"key-1">>, <<"val-1">>},
                                         {<<"x-stream-filter">>, <<"apple">>}],
                                    'Correlation-Data' => <<"banana">>,
                                    'Message-Expiry-Interval' => 1001,
                                    'Response-Topic' => <<"tmp/blah/responses">>
                                   }
                         },
    Anns = #{?ANN_EXCHANGE => Ex,
             ?ANN_ROUTING_KEYS => [rabbit_mqtt_util:mqtt_to_amqp(Mqtt#mqtt_msg.topic)]},
    Mc = mc:init(mc_mqtt, Mqtt, Anns, Env),
    %% no target env
    Msg = mc:convert(mc_amqp, Mc),

    [H,
     #'v1_0.message_annotations'{content = MA},
     P,
     #'v1_0.application_properties'{content = AP},
     D] = amqp10_framing:decode_bin(iolist_to_binary(mc:protocol_state(Msg))),

    ?assertMatch(#'v1_0.header'{durable = true}, H),
    ?assertEqual({utf8, <<"apple">>}, amqp_map_get(symbol(<<"x-stream-filter">>), MA)),
    ?assertMatch(#'v1_0.properties'{content_type = {symbol, <<"text/plain">>},
                                    correlation_id = {binary, <<"banana">>}}, P),
    ?assertEqual({utf8, <<"val-1">>}, amqp_map_get(utf8(<<"key-1">>), AP)),
    ?assertEqual({utf8, <<"val-2">>}, amqp_map_get(utf8(<<"key-2">>), AP)),
    ?assertMatch(#'v1_0.data'{content = _}, D),
    ok.

mqtt_amqp_alt(_Config) ->
    Key = mqtt_x,
    Ex = <<"mqtt-topic-exchange">>,
    Env = #{Key => <<"mqtt-topic-exchange">>},
    CorrId = <<"urn:uuid:550e8400-e29b-41d4-a716-446655440000">>,
    Mqtt0 = mqtt_msg(),
    Mqtt = Mqtt0#mqtt_msg{qos = 0,
                          props = #{'Content-Type' => <<"text/plain">>,
                                    'User-Property' =>
                                        [{<<"key-2">>, <<"val-2">>},
                                         {<<"key-1">>, <<"val-1">>},
                                         {<<"x-stream-filter">>, <<"apple">>}],
                                    'Correlation-Data' => CorrId,
                                    'Message-Expiry-Interval' => 1001,
                                    'Response-Topic' => <<"tmp/blah/responses">>
                                   }
                         },
    Anns = #{?ANN_EXCHANGE => Ex,
             ?ANN_ROUTING_KEYS => [rabbit_mqtt_util:mqtt_to_amqp(Mqtt#mqtt_msg.topic)]},
    Mc = mc:init(mc_mqtt, Mqtt, Anns, Env),
    Msg = mc:convert(mc_amqp, Mc),

    [H,
     #'v1_0.message_annotations'{content = MA},
     P,
     #'v1_0.application_properties'{content = AP},
     D] = amqp10_framing:decode_bin(iolist_to_binary(mc:protocol_state(Msg))),

    ?assertMatch(#'v1_0.header'{durable = false}, H),
    ?assertEqual({utf8, <<"apple">>}, amqp_map_get(symbol(<<"x-stream-filter">>), MA)),
    ?assertMatch(#'v1_0.properties'{content_type = {symbol, <<"text/plain">>},
                                    correlation_id = {uuid, _}}, P),
    ?assertEqual({utf8, <<"val-1">>}, amqp_map_get(utf8(<<"key-1">>), AP)),
    ?assertEqual({utf8, <<"val-2">>}, amqp_map_get(utf8(<<"key-2">>), AP)),
    ?assertEqual(#'v1_0.data'{content = <<>>}, D),
    ok.

amqp_mqtt(_Config) ->
    Env = #{mqtt_x => <<"mqtt-topic-exchange">>},
    H = #'v1_0.header'{priority = {ubyte, 3},
                       ttl = {uint, 20000},
                       durable = true},
    MAC = [
           {{symbol, <<"x-stream-filter">>}, {utf8, <<"apple">>}},
           thead2('x-list', list, [utf8(<<"l">>)]),
           thead2('x-map', map, [{utf8(<<"k">>), utf8(<<"v">>)}])
          ],
    CorrIdOut = <<"urn:uuid:550e8400-e29b-41d4-a716-446655440000">>,
    {ok, CorrUUId} = mc_util:urn_string_to_uuid(CorrIdOut),
    M =  #'v1_0.message_annotations'{content = MAC},
    P = #'v1_0.properties'{content_type = {symbol, <<"text/plain">>},
                           correlation_id = {uuid, CorrUUId},
                           creation_time = {timestamp, 10000}
                          },
    AC = [
          thead(long, 5),
          thead(ulong, 5),
          thead(utf8, <<"a-string">>),
          thead(binary, <<"data">>),
          thead(symbol, <<"symbol">>),
          thead(ubyte, 255),
          thead(short, 2),
          thead(ushort, 3),
          thead(uint, 4),
          thead(int, 4),
          thead(double, 5.0),
          thead(float, 6.0),
          thead(timestamp, 7000),
          thead(byte, -128),
          {{utf8, <<"boolean1">>}, true},
          {{utf8, <<"boolean2">>}, false},
          {utf8(<<"null">>), null}
         ],
    A =  #'v1_0.application_properties'{content = AC},
    D =  #'v1_0.data'{content = <<"data">>},

    Anns = #{?ANN_EXCHANGE => <<"exch">>,
             ?ANN_ROUTING_KEYS => [<<"apple">>]},
    Payload = iolist_to_binary([amqp10_framing:encode_bin(Section) || Section <- [H, M, P, A, D]]),
    AMsg = mc:init(mc_amqp, Payload, Anns),
    Msg = mc:convert(mc_mqtt, AMsg, Env),
    ?assertMatch({uuid, CorrUUId}, mc:correlation_id(Msg)),
    Mqtt = mc:protocol_state(Msg),
    ?assertMatch(
       #mqtt_msg{
          qos = 1,
          props = #{'Content-Type' := <<"text/plain">>,
                    'User-Property' := [{<<"x-stream-filter">>,<<"apple">>},
                                        {<<"long">>,<<"5">>},
                                        {<<"ulong">>,<<"5">>},
                                        {<<"utf8">>,<<"a-string">>},
                                        {<<"symbol">>,<<"symbol">>},
                                        {<<"ubyte">>,<<"255">>},
                                        {<<"short">>,<<"2">>},
                                        {<<"ushort">>,<<"3">>},
                                        {<<"uint">>,<<"4">>},
                                        {<<"int">>,<<"4">>},
                                        {<<"double">>,
                                         <<"5.00000000000000000000e+00">>},
                                        {<<"float">>,
                                         <<"6.00000000000000000000e+00">>},
                                        {<<"timestamp">>,<<"7">>},
                                        {<<"byte">>,<<"-128">>},
                                        {<<"boolean1">>,<<"true">>},
                                        {<<"boolean2">>,<<"false">>},
                                        {<<"null">>,<<>>}],
                    'Correlation-Data' := CorrIdOut}},
       Mqtt).

is_persistent(_Config) ->
    Msg0 = #mqtt_msg{qos = 0,
                     topic = <<"my/topic">>,
                     payload = <<>>},
    Mc0 = mc:init(mc_mqtt, Msg0, #{}),
    ?assertNot(mc:is_persistent(Mc0)),

    Msg1 = #mqtt_msg{qos = 1,
                     topic = <<"my/topic">>,
                     payload = <<>>},
    Mc1 = mc:init(mc_mqtt, Msg1, #{}),
    ?assert(mc:is_persistent(Mc1)).

mqtt_msg() ->
    #mqtt_msg{qos = 0,
              topic = <<"my/topic">>,
              payload = <<>>}.

roundtrip(Mod, MqttMsg) ->
    roundtrip(Mod, MqttMsg, #{}).

roundtrip(Mod, MqttMsg, SrcEnv) ->
    Anns = #{?ANN_ROUTING_KEYS => [rabbit_mqtt_util:mqtt_to_amqp(MqttMsg#mqtt_msg.topic)]},
    Mc0 = mc:init(mc_mqtt, MqttMsg, Anns, SrcEnv),
    Mc1 = mc:convert(Mod, Mc0),
    Mc = mc:convert(mc_mqtt, Mc1),
    mc:protocol_state(Mc).

amqp_to_mqtt(Sections) ->
    amqp_to_mqtt(Sections, #{}).

amqp_to_mqtt(Sections, Env) ->
    Anns = #{?ANN_ROUTING_KEYS => [<<"apple">>]},
    Payload = iolist_to_binary([amqp10_framing:encode_bin(S) || S <- Sections]),
    Mc0 = mc:init(mc_amqp, Payload, Anns),
    Mc = mc:convert(mc_mqtt, Mc0, Env),
    mc:protocol_state(Mc).

amqp_value(Content) ->
    #'v1_0.amqp_value'{content = Content}.

amqpl_header(K, H) ->
    rabbit_basic:header(K, H).

amqp_map_get(_K, []) ->
    undefined;
amqp_map_get(K, Tuples) ->
    case lists:keyfind(K, 1, Tuples) of
        false ->
            undefined;
        {_, V}  ->
            V
    end.

symbol(X) ->
    {symbol, X}.

utf8(X) ->
    {utf8, X}.

thead(T, Value) ->
    {utf8(atom_to_binary(T)), {T, Value}}.

thead2(T, Value) ->
    {symbol(atom_to_binary(T)), {T, Value}}.

thead2(K, T, Value) ->
    {symbol(atom_to_binary(K)), {T, Value}}.
