-module(mc_mqtt_SUITE).

-compile([export_all, nowarn_export_all]).

% -include_lib("rabbit_common/include/rabbit_framing.hrl").
% -include_lib("rabbit_common/include/rabbit.hrl").
% -include_lib("amqp10_common/include/amqp10_framing.hrl").
% -include_lib("rabbit/include/mc.hrl").
-include_lib("rabbitmq_mqtt/include/rabbit_mqtt_packet.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, lossless},
     {group, lossy}
    ].

groups() ->
    [
     {lossless, [parallel],
      [amqp,
       amqpl,
       amqpl_correlation]
     },
     {lossy, [parallel],
      [amqp_user_property,
       amqpl_user_property,
       amqp_content_type
      ]
     }
    ].

amqp(_Config) ->
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
    Anns = #{routing_keys => [rabbit_mqtt_util:mqtt_to_amqp(Msg#mqtt_msg.topic)]},
    Mc0 = mc:init(mc_mqtt, Msg, Anns),

    BytesTopic = 9,
    BytesContentType = 10,
    BytesCorrelationData = 3,
    BytesUserProperty = 66,
    MetaDataSize = BytesTopic + BytesContentType + BytesCorrelationData + BytesUserProperty,
    PayloadSize = 10,
    ExpectedSize = {MetaDataSize, PayloadSize},
    ?assertEqual(ExpectedSize, mc:size(Mc0)),

    ?assertEqual(Msg, mc_mqtt:convert_to(mc_mqtt, Msg)),
    ?assertEqual(not_implemented, mc_mqtt:convert_to(mc_stomp, Msg)),
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

amqpl(_Config) ->
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
    Anns = #{routing_keys => [rabbit_mqtt_util:mqtt_to_amqp(Msg#mqtt_msg.topic)]},
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
amqpl_correlation(_Config) ->
    Msg0 = mqtt_msg(),
    Correlation = binary:copy(<<0>>, 1024),
    Msg = Msg0#mqtt_msg{
            props = #{'Correlation-Data' => Correlation}},
    ?assertMatch(#mqtt_msg{props = #{'Correlation-Data' := Correlation}},
                 roundtrip_amqp(mc_amqpl, Msg)).

%% When converting from MQTT 5.0 to AMQP 1.0, we expect to lose some User Property.
amqp_user_property(_Config) ->
    Msg0 = mqtt_msg(),
    Msg = Msg0#mqtt_msg{props = #{'User-Property' =>
                                  [{<<"x-dup"/utf8>>, <<"val-2">>},
                                   {<<"x-dup"/utf8>>, <<"val-3">>},
                                   {<<"dup">>, <<"val-4">>},
                                   {<<"dup">>, <<"val-5">>},
                                   {<<"x-key-no-ascii🐇"/utf8>>, <<"val-1">>}
                                  ]}},
    #mqtt_msg{props = Props} = roundtrip_amqp(mc_amqp, Msg),
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
amqpl_user_property(_Config) ->
    Msg0 = mqtt_msg(),
    Msg = Msg0#mqtt_msg{
            props = #{'User-Property' => [{<<"key-2">>, <<"val-2">>},
                                          {<<"key-1">>, <<"val-1">>},
                                          {binary:copy(<<"k">>, 129), <<"val-2">>},
                                          {<<"key-1">>, <<"val-1">>}
                                         ]}},
    ?assertMatch(#mqtt_msg{props = #{'User-Property' := [{<<"key-1">>, <<"val-1">>},
                                                         {<<"key-2">>, <<"val-2">>}]}},
                 roundtrip_amqp(mc_amqpl, Msg)).

%% In MQTT 5.0 the Content Type is a UTF-8 encoded string.
%% In AMQP 1.0 the Content Type is a symbol.
%% We expect to lose the Content Type when converting from MQTT 5.0 to AMQP 1.0 if
%% the Content Type is not valid ASCII.
amqp_content_type(_Config) ->
    Msg0 = mqtt_msg(),
    Msg = Msg0#mqtt_msg{props = #{'Content-Type' => <<"no-ascii🐇"/utf8>>}},
    #mqtt_msg{props = Props} = roundtrip_amqp(mc_amqp, Msg),
    ?assertNot(maps:is_key('Content-Type', Props)).

mqtt_msg() ->
    #mqtt_msg{qos = 0,
              topic = <<"my/topic">>,
              payload = <<>>}.

roundtrip_amqp(Mod, MqttMsg) ->
    Anns = #{routing_keys => [rabbit_mqtt_util:mqtt_to_amqp(MqttMsg#mqtt_msg.topic)]},
    Mc0 = mc:init(mc_mqtt, MqttMsg, Anns),
    Mc1 = mc:convert(Mod, Mc0),
    Mc = mc:convert(mc_mqtt, Mc1),
    mc:protocol_state(Mc).
