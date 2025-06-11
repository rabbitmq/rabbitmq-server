%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This test suite covers protocol interoperability publishing via MQTT 5.0,
%% receiving via AMQP 0.9.1, AMQP 1.0, STOMP 1.2, and Stream, and vice versa.
-module(protocol_interop_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_stomp/include/rabbit_stomp_frame.hrl").

-define(TIMEOUT, 30_000).

-import(util,
        [connect/2,
         connect/4]).
-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/1,
         eventually/3]).

all() ->
    [{group, cluster_size_1},
     {group, cluster_size_3}].

groups() ->
    [{cluster_size_1, [shuffle],
      [
       mqtt_amqpl_mqtt,
       amqpl_mqtt_gh_12707,
       mqtt_amqp_mqtt,
       amqp_mqtt_amqp,
       mqtt_stomp_mqtt,
       mqtt_stream
      ]},
     {cluster_size_3, [shuffle],
      [
       amqp_mqtt_qos0,
       amqp_mqtt_qos1
      ]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config0) ->
    Nodes = case Group of
                cluster_size_1 -> 1;
                cluster_size_3 -> 3
            end,
    Config1 = rabbit_ct_helpers:set_config(
                Config0,
                [{rmq_nodes_count, Nodes},
                 {mqtt_version, v5},
                 {start_rmq_with_plugins_disabled, true}
                ]),
    Config = rabbit_ct_helpers:run_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config, rabbitmq_mqtt),
    util:enable_plugin(Config, rabbitmq_stomp),
    util:enable_plugin(Config, rabbitmq_stream),
    Config.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Wait for exclusive or auto-delete queues being deleted.
    timer:sleep(800),
    rabbit_ct_broker_helpers:rpc(Config, ?MODULE, delete_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

mqtt_amqpl_mqtt(Config) ->
    Q = ClientId = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = Q,
                                                                   durable = true}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = Q,
                                                             exchange = <<"amq.topic">>,
                                                             routing_key = <<"my.topic">>}),
    C = connect(ClientId, Config),
    MqttResponseTopic = <<"response/topic">>,
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 999}, [{MqttResponseTopic, [{qos, 1}]}]),
    Correlation = <<"some correlation ID">>,
    RequestPayload = <<"my request">>,
    UserProperty = [{<<"rabbit🐇"/utf8>>, <<"carrot🥕"/utf8>>},
                    {<<"key">>, <<"val">>},
                    {<<"key">>, <<"val">>}],
    {ok, _} = emqtt:publish(C, <<"my/topic">>,
                            #{'Content-Type' => <<"text/plain">>,
                              'Correlation-Data' => Correlation,
                              'Response-Topic' => MqttResponseTopic,
                              'User-Property' => UserProperty},
                            RequestPayload, [{qos, 1}]),

    {#'basic.get_ok'{},
     #amqp_msg{payload = RequestPayload,
               props = #'P_basic'{content_type = <<"text/plain">>,
                                  correlation_id = Correlation,
                                  delivery_mode = 2,
                                  headers = Headers}}} = amqp_channel:call(Ch, #'basic.get'{queue = Q}),
    %% AMQP 0.9.1 expects unique headers sorted by key.
    [{<<"key">>, longstr, <<"val">>},
     {<<"rabbit🐇"/utf8>>, longstr, <<"carrot🥕"/utf8>>},
     {<<"x-reply-to-topic">>, longstr, AmqpResponseTopic}] = Headers,

    %% AMQP 0.9.1 to MQTT 5.0
    ReplyPayload = <<"{\"my\" : \"reply\"}">>,
    amqp_channel:call(Ch, #'basic.publish'{exchange = <<"amq.topic">>,
                                           routing_key = AmqpResponseTopic},
                      #amqp_msg{payload = ReplyPayload,
                                props = #'P_basic'{correlation_id = Correlation,
                                                   content_type = <<"application/json">>,
                                                   headers = Headers ++ [{<<"a">>, unsignedint, 4},
                                                                         {<<"b">>, bool, true},
                                                                         {"c", binary, <<0, 255, 0>>}]}}),

    receive {publish,
             #{client_pid := C,
               topic := MqttResponseTopic,
               payload := ReplyPayload,
               properties := #{'Content-Type' := <<"application/json">>,
                               'Correlation-Data' := Correlation,
                               'User-Property' := UserProperty1,
                               'Subscription-Identifier' := 999}}} ->
                ?assertEqual(
                   [{<<"a">>, <<"4">>},
                    {<<"b">>, <<"true">>},
                    {<<"key">>, <<"val">>},
                    {<<"rabbit🐇"/utf8>>, <<"carrot🥕"/utf8>>}],
                   lists:sort(UserProperty1))
    after ?TIMEOUT -> ct:fail("did not receive reply")
    end,

    %% Another message MQTT 5.0 to AMQP 0.9.1, this time with QoS 0
    ok = emqtt:publish(C, <<"my/topic">>, RequestPayload, [{qos, 0}]),
    eventually(
      ?_assertMatch(
         {#'basic.get_ok'{}, #amqp_msg{payload = RequestPayload,
                                       props = #'P_basic'{delivery_mode = 1}}},
         amqp_channel:call(Ch, #'basic.get'{queue = Q}))),

    ok = emqtt:disconnect(C).

amqpl_mqtt_gh_12707(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Topic = Payload = <<"gh_12707">>,
    C = connect(ClientId, Config),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch,
                      #'basic.publish'{exchange = <<"amq.topic">>,
                                       routing_key = Topic},
                      #amqp_msg{payload = Payload,
                                props = #'P_basic'{expiration = <<"12707">>,
                                                   headers = []}}),

    receive {publish,
             #{topic := MqttTopic,
               payload := MqttPayload}} ->
                ?assertEqual(Topic, MqttTopic),
                ?assertEqual(Payload, MqttPayload)
    after ?TIMEOUT ->
              ct:fail("did not receive a delivery")
    end,

    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = emqtt:disconnect(C).

mqtt_amqp_mqtt(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    ClientId = Container = atom_to_binary(?FUNCTION_NAME),
    OpnConf = #{address => Host,
                port => Port,
                container_id => Container,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection1} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session(Connection1),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session1, <<"pair">>),
    QName = <<"queue for AMQP 1.0 client">>,
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, <<"amq.topic">>, <<"topic.1">>, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session1, <<"test-receiver">>,
                       rabbitmq_amqp_address:queue(QName),
                       unsettled, configuration),

    %% MQTT 5.0 to AMQP 1.0
    C = connect(ClientId, Config),
    MqttResponseTopic = <<"response/topic/🥕"/utf8>>,
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 999},
                                   [{MqttResponseTopic, [{qos, 1}]}]),
    Correlation = <<"some correlation ID">>,
    ContentType = <<"text/plain">>,
    RequestPayload = <<"my request">>,
    UserProperty = [{<<"🐇"/utf8>>, <<"🥕"/utf8>>},
                    {<<"x-🐇"/utf8>>, <<"🥕"/utf8>>},
                    {<<"key">>, <<"val">>},
                    {<<"key">>, <<"val">>},
                    {<<"x-key">>, <<"val">>},
                    {<<"x-key">>, <<"val">>}],
    {ok, _} = emqtt:publish(C, <<"topic/1">>,
                            #{'Content-Type' => ContentType,
                              'Correlation-Data' => Correlation,
                              'Response-Topic' => MqttResponseTopic,
                              'User-Property' => UserProperty},
                            RequestPayload, [{qos, 1}]),

    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    ct:pal("Received AMQP 1.0 message:~n~p", [Msg1]),

    ?assert(amqp10_msg:header(durable, Msg1)),
    ?assert(amqp10_msg:header(first_acquirer, Msg1)),

    %% We expect to receive x-headers in message annotations.
    %% However, since annotation keys are symbols and symbols are only valid ASCII,
    %% we expect header
    %% {<<"x-🐇"/utf8>>, <<"🥕"/utf8>>}
    %% to be dropped.
    ?assertEqual(#{<<"x-key">> => <<"val">>,
                   <<"x-exchange">> => <<"amq.topic">>,
                   <<"x-routing-key">> => <<"topic.1">>},
                 amqp10_msg:message_annotations(Msg1)),
    %% In contrast, application property keys are of type string, and therefore UTF-8 encoded.
    ?assertEqual(#{<<"🐇"/utf8>> => <<"🥕"/utf8>>,
                   <<"key">> => <<"val">>},
                 amqp10_msg:application_properties(Msg1)),

    #{correlation_id := Correlation,
      content_type := ContentType,
      reply_to := ReplyToAddress} = amqp10_msg:properties(Msg1),
    ExpectedReplyToAddress = rabbitmq_amqp_address:exchange(
                               <<"amq.topic">>, <<"response.topic.🥕"/utf8>>),
    ?assertEqual(ExpectedReplyToAddress, ReplyToAddress),

    ?assertEqual(RequestPayload, amqp10_msg:body_bin(Msg1)),

    ok = amqp10_client:settle_msg(Receiver, Msg1, accepted),
    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:end_session(Session1),
    ok = amqp10_client:close_connection(Connection1),

    %% AMQP 1.0 to MQTT 5.0
    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    {ok, Session2} = amqp10_client:begin_session(Connection2),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session2, SenderLinkName, ReplyToAddress, unsettled),
    receive {amqp10_event, {link, Sender, credited}} -> ok
    after ?TIMEOUT -> ct:fail(credited_timeout)
    end,

    DTag = <<"my-dtag">>,
    ReplyPayload = <<"my response">>,
    Msg2a = amqp10_msg:new(DTag, #'v1_0.data'{content = ReplyPayload}),
    Msg2b = amqp10_msg:set_properties(
              #{correlation_id => Correlation,
                content_type => ContentType},
              Msg2a),
    %% Use the 2 byte AMQP boolean encoding, see AMQP §1.6.2
    True = {boolean, true},
    Msg2 = amqp10_msg:set_headers(#{durable => True}, Msg2b),
    ok = amqp10_client:send_msg(Sender, Msg2),
    receive {amqp10_disposition, {accepted, DTag}} -> ok
    after ?TIMEOUT -> ct:fail(settled_timeout)
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:end_session(Session2),
    ok = amqp10_client:close_connection(Connection2),

    receive {publish, MqttMsg} ->
                ct:pal("Received MQTT message:~n~p", [MqttMsg]),
                ?assertMatch(
                   #{client_pid := C,
                     qos := 1,
                     topic := MqttResponseTopic,
                     payload := ReplyPayload,
                     properties := #{'Content-Type' := ContentType,
                                     'Correlation-Data' := Correlation,
                                     'Subscription-Identifier' := 999}
                    },
                   MqttMsg)
    after ?TIMEOUT -> ct:fail("did not receive reply")
    end,
    ok = emqtt:disconnect(C).

amqp_mqtt_amqp(Config) ->
    Correlation = QName = ClientId = Container = atom_to_binary(?FUNCTION_NAME),

    C = connect(ClientId, Config),
    {ok, _, [1]} = emqtt:subscribe(C, <<"t/1">>, qos1),

    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => Host,
                port => Port,
                container_id => Container,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"pair">>),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, <<"amq.topic">>, <<"[.]">>, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, rabbitmq_amqp_address:queue(QName)),

    %% AMQP 1.0 to MQTT 5.0
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session,
                     <<"sender">>,
                     rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<"t.1">>)),
    receive {amqp10_event, {link, Sender, credited}} -> ok
    after ?TIMEOUT -> ct:fail(credited_timeout)
    end,
    RequestBody = <<"my request">>,

    Msg1 = amqp10_msg:set_headers(
             #{durable => true},
             amqp10_msg:set_properties(
               #{correlation_id => Correlation,
                 reply_to => rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<"[.]">>)},
               amqp10_msg:new(<<>>, RequestBody, true))),
    ok = amqp10_client:send_msg(Sender, Msg1),

    ResponseTopic = <<"[/]">>,
    receive {publish, MqttMsg} ->
                ct:pal("Received MQTT message:~n~p", [MqttMsg]),
                #{client_pid := C,
                  qos := 1,
                  topic := <<"t/1">>,
                  payload := RequestBody,
                  properties := Props = #{'Correlation-Data' := Correlation}
                 } = MqttMsg,
                case rabbit_ct_broker_helpers:is_feature_flag_enabled(
                       Config, 'rabbitmq_4.0.0') of
                    true ->
                        ?assertEqual({ok, ResponseTopic},
                                     maps:find('Response-Topic', Props));
                    false ->
                        ok
                end
    after ?TIMEOUT -> ct:fail("did not receive request")
    end,

    %% MQTT 5.0 to AMQP 1.0
    RespBody = <<"my response">>,
    {ok, _} = emqtt:publish(C, ResponseTopic,
                            #{'Correlation-Data' => Correlation},
                            RespBody, [{qos, 1}]),

    {ok, Msg2} = amqp10_client:get_msg(Receiver),
    ct:pal("Received AMQP 1.0 message:~n~p", [Msg2]),
    ?assertEqual(RespBody, amqp10_msg:body_bin(Msg2)),

    ok = emqtt:disconnect(C),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

%% Send messages with different AMQP body sections and
%% consume via MQTT 5.0 with a QoS 0 subscription.
amqp_mqtt_qos0(Config) ->
    %% We want to test that the old node can receive from an MQTT QoS 0 queue.
    amqp_mqtt(0, Config).

%% Send messages with different AMQP body sections and
%% consume via MQTT 5.0 with a QoS 1 subscription.
amqp_mqtt_qos1(Config) ->
    amqp_mqtt(1, Config).

amqp_mqtt(Qos, Config) ->
    ClientId = Container = atom_to_binary(?FUNCTION_NAME),

    %% Connect MQTT subscriber to the old node.
    C = connect(ClientId, Config, 1, []),
    {ok, _, [Qos]} = emqtt:subscribe(C, <<"my/topic">>, Qos),

    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => Host,
                port => Port,
                container_id => Container,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session,
                     <<"sender">>,
                     rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<"my.topic">>)),
    receive {amqp10_event, {link, Sender, credited}} -> ok
    after ?TIMEOUT -> ct:fail(credited_timeout)
    end,

    %% single amqp-value section
    Body1 = #'v1_0.amqp_value'{content = {binary, <<0, 255>>}},
    Body2 = #'v1_0.amqp_value'{content = false},
    %% single amqp-sequene section
    Body3 = [#'v1_0.amqp_sequence'{content = [{binary, <<0, 255>>}]}],
    %% multiple amqp-sequene sections
    Body4 = [#'v1_0.amqp_sequence'{content = [{long, -1}]},
             #'v1_0.amqp_sequence'{content = [true, {utf8, <<"🐇"/utf8>>}]}],
    %% single data section
    Body5 = [#'v1_0.data'{content = <<0, 255>>}],
    %% multiple data sections
    Body6 = [#'v1_0.data'{content = <<0, 1>>},
             #'v1_0.data'{content = <<2, 3>>}],

    [ok = amqp10_client:send_msg(Sender,
                                 amqp10_msg:set_headers(
                                   #{durable => true},
                                   amqp10_msg:new(<<>>, Body, true))) ||
     Body <- [Body1, Body2, Body3, Body4, Body5, Body6]],

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),

    receive {publish, MqttMsg1} ->
                #{client_pid := C,
                  qos := Qos,
                  topic := <<"my/topic">>,
                  payload := Payload1,
                  properties := Props
                 } = MqttMsg1,
                ?assertEqual([Body1], amqp10_framing:decode_bin(Payload1)),
                case rabbit_ct_broker_helpers:is_feature_flag_enabled(
                       Config, 'rabbitmq_4.0.0') of
                    true ->
                        ?assertEqual({ok, <<"message/vnd.rabbitmq.amqp">>},
                                     maps:find('Content-Type', Props));
                    false ->
                        ok
                end
    after ?TIMEOUT -> ct:fail({missing_publish, ?LINE})
    end,
    receive {publish, #{payload := Payload2}} ->
                ?assertEqual([Body2], amqp10_framing:decode_bin(Payload2))
    after ?TIMEOUT -> ct:fail({missing_publish, ?LINE})
    end,
    receive {publish, #{payload := Payload3}} ->
                ?assertEqual(Body3, amqp10_framing:decode_bin(Payload3))
    after ?TIMEOUT -> ct:fail({missing_publish, ?LINE})
    end,
    receive {publish, #{payload := Payload4}} ->
                ?assertEqual(Body4, amqp10_framing:decode_bin(Payload4))
    after ?TIMEOUT -> ct:fail({missing_publish, ?LINE})
    end,
    receive {publish, #{payload := Payload5}} ->
                ?assertEqual(<<0, 255>>, Payload5)
    after ?TIMEOUT -> ct:fail({missing_publish, ?LINE})
    end,
    receive {publish, #{payload := Payload6}} ->
                %% We expect that RabbitMQ concatenates the binaries of multiple data sections.
                ?assertEqual(<<0, 1, 2, 3>>, Payload6)
    after ?TIMEOUT -> ct:fail({missing_publish, ?LINE})
    end,

    ok = emqtt:disconnect(C).

mqtt_stomp_mqtt(Config) ->
    {ok, StompC0} = stomp_connect(Config),
    ok = stomp_send(StompC0, "SUBSCRIBE", [{"destination", "/topic/t.1"},
                                           {"receipt", "my-receipt"},
                                           {"id", "subscription-888"}]),
    {#stomp_frame{command = "RECEIPT",
                  headers = [{"receipt-id","my-receipt"}]}, StompC1} = stomp_recv(StompC0),

    %% MQTT 5.0 to STOMP 1.2
    C = connect(<<"my-mqtt-client">>, Config),
    MqttResponseTopic = <<"response/topic">>,
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 999},
                                   [{MqttResponseTopic, [{qos, 1}]}]),
    Correlation = <<"some correlation ID">>,
    ContentType = <<"application/json">>,
    RequestPayload = <<"{\"my\" : \"request\"}">>,
    UserProperty = [{<<"rabbit🐇"/utf8>>, <<"carrot🥕"/utf8>>},
                    {<<"x-rabbit🐇"/utf8>>, <<"carrot🥕"/utf8>>},
                    %% "If a client or a server receives repeated frame header entries,
                    %% only the first header entry SHOULD be used as the value of header
                    %% entry. " [STOMP 1.2]
                    {<<"key">>, <<"val1">>},
                    {<<"key">>, <<"val2">>},
                    {<<"x-key">>, <<"val1">>},
                    {<<"x-key">>, <<"val2">>}],
    {ok, _} = emqtt:publish(C, <<"t/1">>,
                            #{'Content-Type' => ContentType,
                              'Correlation-Data' => Correlation,
                              'Response-Topic' => MqttResponseTopic,
                              'User-Property' => UserProperty},
                            RequestPayload, [{qos, 1}]),

    {#stomp_frame{command = "MESSAGE",
                  headers = Headers0,
                  body_iolist = Body} = Msg1, StompC2} = stomp_recv(StompC1),
    ?assertEqual(RequestPayload, iolist_to_binary(Body)),
    Headers1 = maps:from_list(Headers0),
    Headers = maps:map(fun(_K, V) -> unicode:characters_to_binary(V) end, Headers1),
    ct:pal("Received STOMP 1.2 message:~n~p~n"
           "with headers map:~n~p", [Msg1, Headers]),
    ?assertMatch(
       #{"content-type" := ContentType,
         "correlation-id" := Correlation,
         "destination" := <<"/topic/t.1">>,
         %% With Native STOMP, this should be translated to
         %% reply-to: /topic/response.topic
         "x-reply-to-topic" := <<"response.topic">>,
         "subscription" := <<"subscription-888">>,
         "persistent" := <<"true">>,
         %% The STOMP spec mandates headers to be encoded as UTF-8, but unfortunately the RabbitMQ
         %% STOMP implementation (as of 3.13) does not adhere and therefore does not provide UTF-8 support.
         % "rabbit🐇" := <<"carrot🥕"/utf8>>,
         % "x-rabbit🐇" := <<"carrot🥕"/utf8>>,
         "key" := <<"val1">>,
         "x-key" := <<"val1">>
        },
       Headers),

    %% STOMP 1.2 to MQTT 5.0
    ok = stomp_send(StompC2, "SEND",
                    [{"destination", "/topic/response.topic"},
                     {"persistent", "true"},
                     {"content-type", "application/json"},
                     {"correlation-id", binary_to_list(Correlation)},
                     {"x-key", "val4"}],
                    ["{\"my\" : \"response\"}"]),
    ok = stomp_disconnect(StompC2),

    receive {publish, MqttMsg} ->
                ct:pal("Received MQTT message:~n~p", [MqttMsg]),
                #{client_pid := C,
                  qos := 1,
                  topic := MqttResponseTopic,
                  payload := <<"{\"my\" : \"response\"}">>,
                  properties := #{'Content-Type' := ContentType,
                                  'Correlation-Data' := Correlation,
                                  'User-Property' := UserProp}} = MqttMsg,
                ?assert(lists:member({<<"x-key">>, <<"val4">>}, UserProp))
    after ?TIMEOUT -> ct:fail("did not receive reply")
    end,

    ok = emqtt:disconnect(C).

%% The stream test case is one-way because an MQTT client can publish to a stream,
%% but not consume (directly) from a stream.
mqtt_stream(Config) ->
    Q = ClientId = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),

    %% Bind a stream to the MQTT topic exchange.
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = Q,
                                                   durable = true,
                                                   arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = Q,
                                                             exchange = <<"amq.topic">>,
                                                             routing_key = <<"my.topic">>}),

    %% MQTT 5.0 to Stream
    C = connect(ClientId, Config),
    ContentType = <<"text/plain">>,
    Correlation = <<"some correlation ID">>,
    Payload = <<"my payload">>,
    UserProperty = [{<<"rabbit🐇"/utf8>>, <<"carrot🥕"/utf8>>},
                    %% We expect that this message annotation will be dropped
                    %% since AMQP 1.0 annoations must be symbols, i.e encoded as ASCII.
                    {<<"x-rabbit🐇"/utf8>>, <<"carrot🥕"/utf8>>},
                    {<<"key">>, <<"val">>},
                    %% We expect that this application property will be dropped
                    %% since AMQP 1.0 application properties are maps and maps disallow duplicate keys.
                    {<<"key">>, <<"val">>},
                    {<<"x-key">>, <<"val">>},
                    %% We expect that this message annotation will be dropped
                    %% since AMQP 1.0 annoations are maps and maps disallow duplicate keys.
                    {<<"x-key">>, <<"val">>}],
    {ok, _} = emqtt:publish(C, <<"my/topic">>,
                            #{'Content-Type' => ContentType,
                              'Correlation-Data' => Correlation,
                              'Response-Topic' => <<"response/topic">>,
                              'User-Property' => UserProperty},
                            Payload, [{qos, 1}]),
    ok = emqtt:disconnect(C),

    %% There is no open source Erlang RabbitMQ Stream client.
    %% Therefore, we have to build the commands for the Stream protocol handshake manually.
    StreamPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stream),
    {ok, S} = gen_tcp:connect("localhost", StreamPort, [{active, false}, {mode, binary}]),

    C0 = rabbit_stream_core:init(0),
    PeerPropertiesFrame = rabbit_stream_core:frame({request, 1, {peer_properties, #{}}}),
    ok = gen_tcp:send(S, PeerPropertiesFrame),
    {{response, 1, {peer_properties, _, _}}, C1} = receive_stream_commands(S, C0),

    ok = gen_tcp:send(S, rabbit_stream_core:frame({request, 1, sasl_handshake})),
    {{response, _, {sasl_handshake, _, _}}, C2} = receive_stream_commands(S, C1),
    Username = <<"guest">>,
    Password = <<"guest">>,
    Null = 0,
    PlainSasl = <<Null:8, Username/binary, Null:8, Password/binary>>,
    ok = gen_tcp:send(S, rabbit_stream_core:frame({request, 2, {sasl_authenticate, <<"PLAIN">>, PlainSasl}})),
    {{response, 2, {sasl_authenticate, _}}, C3} = receive_stream_commands(S, C2),
    {{tune, DefaultFrameMax, _}, C4} = receive_stream_commands(S, C3),

    ok = gen_tcp:send(S, rabbit_stream_core:frame({response, 0, {tune, DefaultFrameMax, 0}})),
    ok = gen_tcp:send(S, rabbit_stream_core:frame({request, 3, {open, <<"/">>}})),
    {{response, 3, {open, _, _ConnectionProperties}}, C5} = receive_stream_commands(S, C4),

    SubscriptionId = 99,
    SubCmd = {request, 1, {subscribe, SubscriptionId, Q, 0, 10, #{}}},
    SubscribeFrame = rabbit_stream_core:frame(SubCmd),
    ok = gen_tcp:send(S, SubscribeFrame),
    {{response, 1, {subscribe, _}}, C6} = receive_stream_commands(S, C5),

    {{deliver, SubscriptionId, Chunk}, _C7} = receive_stream_commands(S, C6),
    <<5:4/unsigned,
      0:4/unsigned,
      0:8,
      1:16,
      1:32,
      _Timestamp:64,
      _Epoch:64,
      _COffset:64,
      _Crc:32,
      _DataLength:32,
      _TrailerLength:32,
      _ReservedBytes:32,
      0:1,
      BodySize:31/unsigned,
      Sections0:BodySize/binary>> = Chunk,
    Sections = amqp10_framing:decode_bin(Sections0),

    ct:pal("Stream client received AMQP 1.0 sections:~n~p", [Sections]),

    U = undefined,
    FakeTransfer = {'v1_0.transfer', U, U, U, U, U, U, U, U, U, U, U},
    Msg = amqp10_msg:from_amqp_records([FakeTransfer | Sections]),

    ?assert(amqp10_msg:header(durable, Msg)),
    ?assertEqual(#{<<"x-exchange">> => <<"amq.topic">>,
                   <<"x-routing-key">> => <<"my.topic">>,
                   <<"x-key">> => <<"val">>},
                 amqp10_msg:message_annotations(Msg)),
    ?assertEqual(
       #{correlation_id => Correlation,
         content_type => ContentType,
         %% We expect that reply_to contains a valid AMQP 1.0 address,
         %% and that the topic format got translated from MQTT to AMQP 0.9.1.
         reply_to => rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<"response.topic">>)},
       amqp10_msg:properties(Msg)),
    ?assertEqual(#{<<"rabbit🐇"/utf8>> => <<"carrot🥕"/utf8>>,
                   <<"key">> => <<"val">>},
                 amqp10_msg:application_properties(Msg)),
    ?assertEqual(Payload, amqp10_msg:body_bin(Msg)).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

delete_queues() ->
    [{ok, 0} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>) || Q <- rabbit_amqqueue:list()].

receive_stream_commands(Sock, C0) ->
    case rabbit_stream_core:next_command(C0) of
        empty ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, Data} ->
                    C1 = rabbit_stream_core:incoming_data(Data, C0),
                    case rabbit_stream_core:next_command(C1) of
                        empty ->
                            {ok, Data2} = gen_tcp:recv(Sock, 0, 5000),
                            rabbit_stream_core:next_command(
                                rabbit_stream_core:incoming_data(Data2, C1));
                        Res ->
                            Res
                    end;
                {error, Err} ->
                    ct:fail("error receiving stream data ~w", [Err])
            end;
        Res ->
            Res
    end.

%% -------------------------------------------------------------------
%% STOMP client BEGIN
%% -------------------------------------------------------------------
%% Below STOMP client is a simplified version of deps/rabbitmq_stomp/test/src/rabbit_stomp_client.erl
%% It would be better to use rabbit_stomp_client directly.

stomp_connect(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Sock} = gen_tcp:connect(localhost, Port, [{active, false}, binary]),
    Client0 = {Sock, []},
    stomp_send(Client0, "CONNECT", [{"accept-version", "1.2"}]),
    {#stomp_frame{command = "CONNECTED"}, Client1} = stomp_recv(Client0),
    {ok, Client1}.

stomp_disconnect(Client = {Sock, _}) ->
    stomp_send(Client, "DISCONNECT"),
    gen_tcp:close(Sock).

stomp_send(Client, Command) ->
    stomp_send(Client, Command, []).

stomp_send(Client, Command, Headers) ->
    stomp_send(Client, Command, Headers, []).

stomp_send({Sock, _}, Command, Headers, Body) ->
    Frame = rabbit_stomp_frame:serialize(
              #stomp_frame{command = list_to_binary(Command),
                           headers = Headers,
                           body_iolist = Body}),
    gen_tcp:send(Sock, Frame).

stomp_recv({_Sock, []} = Client) ->
    stomp_recv(Client, rabbit_stomp_frame:initial_state(), 0);
stomp_recv({Sock, [Frame | Frames]}) ->
    {Frame, {Sock, Frames}}.

stomp_recv(Client = {Sock, _}, FrameState, Length) ->
    {ok, Payload} = gen_tcp:recv(Sock, Length, 1000),
    stomp_parse(Payload, Client, FrameState, Length).

stomp_parse(Payload, Client = {Sock, FramesRev}, FrameState, Length) ->
    case rabbit_stomp_frame:parse(Payload, FrameState) of
        {ok, Frame, <<>>} ->
            stomp_recv({Sock, lists:reverse([Frame | FramesRev])});
        {ok, Frame, <<"\n">>} ->
            stomp_recv({Sock, lists:reverse([Frame | FramesRev])});
        {ok, Frame, Rest} ->
            stomp_parse(Rest, {Sock, [Frame | FramesRev]},
                        rabbit_stomp_frame:initial_state(), Length);
        {more, NewState} ->
            stomp_recv(Client, NewState, 0)
    end.

%% -------------------------------------------------------------------
%% STOMP client END
%% -------------------------------------------------------------------
