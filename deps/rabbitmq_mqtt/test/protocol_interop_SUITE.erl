%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This test suite covers protocol interoperability publishing via MQTT 5.0,
%% receiving via AMQP 0.9.1, AMQP 1.0, STOMP 1.2, and Stream, and vice versa.
-module(protocol_interop_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_stomp/include/rabbit_stomp_frame.hrl").

-import(util,
        [connect/2]).
-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/3]).

all() ->
    [{group, tests}].

groups() ->
    [{tests, [shuffle],
      [
       amqpl,
       amqp,
       stomp,
       stream
      ]
     }].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config0,
                {mqtt_version, v5}),
    Config = rabbit_ct_helpers:run_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, mqtt_v5),

    Plugins = [rabbitmq_amqp1_0,
               rabbitmq_stomp,
               rabbitmq_stream],
    [ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, Plugin) || Plugin <- Plugins],
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

amqpl(Config) ->
    Q = ClientId = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = Q,
                                                             exchange = <<"amq.topic">>,
                                                             routing_key = <<"my.topic">>}),
    %% MQTT 5.0 to AMQP 0.9.1
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
    after 1000 -> ct:fail("did not receive reply")
    end,

    ok = emqtt:disconnect(C).

amqp(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    ClientId = Container = atom_to_binary(?FUNCTION_NAME),
    OpnConf = #{address => Host,
                port => Port,
                container_id => Container,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection1} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session(Connection1),
    ReceiverLinkName = <<"test-receiver">>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session1, ReceiverLinkName, <<"/topic/topic.1">>, unsettled),

    %% MQTT 5.0 to AMQP 1.0
    C = connect(ClientId, Config),
    MqttResponseTopic = <<"response/topic">>,
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 999},
                                   [{MqttResponseTopic, [{qos, 1}]}]),
    Correlation = <<"some correlation ID">>,
    ContentType = <<"text/plain">>,
    RequestPayload = <<"my request">>,
    UserProperty = [{<<"rabbit🐇"/utf8>>, <<"carrot🥕"/utf8>>},
                    {<<"x-rabbit🐇"/utf8>>, <<"carrot🥕"/utf8>>},
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

    %% As of 3.13, AMQP 1.0 is proxied via AMQP 0.9.1 and therefore the conversion from
    %% mc_mqtt to mc_amqpl takes place. We therefore lose MQTT User Property and Response Topic
    %% which gets converted to AMQP 0.9.1 headers. In the future, Native AMQP 1.0 will convert
    %% from mc_mqtt to mc_amqp allowing us to do many more assertions here.
    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    ct:pal("Received AMQP 1.0 message:~n~p", [Msg1]),
    ?assertEqual([RequestPayload], amqp10_msg:body(Msg1)),
    ?assertMatch(#{correlation_id := Correlation,
                   content_type := ContentType}, amqp10_msg:properties(Msg1)),
    ?assert(amqp10_msg:header(durable, Msg1)),
    ?assert(amqp10_msg:header(first_acquirer, Msg1)),

    ok = amqp10_client:settle_msg(Receiver, Msg1, accepted),
    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:end_session(Session1),
    ok = amqp10_client:close_connection(Connection1),

    %% AMQP 1.0 to MQTT 5.0
    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    {ok, Session2} = amqp10_client:begin_session(Connection2),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     %% With Native AMQP 1.0, address should be read from received reply-to
                     Session2, SenderLinkName, <<"/topic/response.topic">>, unsettled),
    receive {amqp10_event, {link, Sender, credited}} -> ok
    after 1000 -> ct:fail(credited_timeout)
    end,

    DTag = <<"my-dtag">>,
    ReplyPayload = <<"my response">>,
    Msg2a = amqp10_msg:new(DTag, ReplyPayload),
    Msg2b = amqp10_msg:set_properties(
              #{correlation_id => Correlation,
                content_type => ContentType},
              Msg2a),
    Msg2 = amqp10_msg:set_headers(#{durable => true}, Msg2b),
    ok = amqp10_client:send_msg(Sender, Msg2),
    receive {amqp10_disposition, {accepted, DTag}} -> ok
    after 1000 -> ct:fail(settled_timeout)
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
                                     'Subscription-Identifier' := 999}},
                   MqttMsg)
    after 1000 -> ct:fail("did not receive reply")
    end,
    ok = emqtt:disconnect(C).

stomp(Config) ->
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
    after 1000 -> ct:fail("did not receive reply")
    end,

    ok = emqtt:disconnect(C).

stream(_Config) ->
    {skip, "TODO write test"}.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

delete_queues() ->
    [{ok, 0} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>) || Q <- rabbit_amqqueue:list()].

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
