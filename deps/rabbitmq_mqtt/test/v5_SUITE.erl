%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

%% This test suite covers MQTT 5.0 features.
-module(v5_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(util,
        [all_connection_pids/1,
         start_client/4,
         connect/2, connect/3, connect/4,
         assert_message_expiry_interval/2,
         non_clean_sess_opts/0,
         expect_publishes/3
        ]).
-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/1]).

-define(APP, rabbitmq_mqtt).
-define(QUEUE_TTL_KEY, <<"x-expires">>).

%% defined in MQTT v5 (not in v4 or v3)
-define(RC_SUCCESS, 16#00).
-define(RC_NO_SUBSCRIPTION_EXISTED, 16#11).
-define(RC_UNSPECIFIED_ERROR, 16#80).

all() ->
    [{group, mqtt},
     {group, web_mqtt}].

groups() ->
    [
     {mqtt, [],
      [{cluster_size_1, [shuffle], cluster_size_1_tests()},
       {cluster_size_3, [shuffle], cluster_size_3_tests()}
      ]},
     {web_mqtt, [],
      [{cluster_size_1, [shuffle], cluster_size_1_tests()},
       {cluster_size_3, [shuffle], cluster_size_3_tests()}
      ]}
    ].

cluster_size_1_tests() ->
    [
     client_set_max_packet_size_publish,
     client_set_max_packet_size_connack,
     client_set_max_packet_size_invalid,
     message_expiry_interval,
     message_expiry_interval_will_message,
     message_expiry_interval_retained_message,
     session_expiry_interval_classic_queue_disconnect_decrease,
     session_expiry_interval_quorum_queue_disconnect_decrease,
     session_expiry_interval_disconnect_zero_to_non_zero,
     session_expiry_interval_disconnect_non_zero_to_zero,
     session_expiry_interval_disconnect_infinity_to_zero,
     session_expiry_interval_disconnect_to_infinity,
     session_expiry_interval_reconnect_non_zero,
     session_expiry_interval_reconnect_zero,
     session_expiry_interval_reconnect_infinity_to_zero,
     client_publish_qos2,
     client_rejects_publish,
     will_qos2,
     client_receive_maximum_min,
     client_receive_maximum_large,
     unsubscribe_success,
     unsubscribe_topic_not_found,
     subscription_option_no_local,
     subscription_option_no_local_wildcards,
     subscription_option_retain_as_published,
     subscription_option_retain_as_published_wildcards,
     subscription_identifier,
     subscription_identifier_amqp091,
     subscription_identifier_at_most_once_dead_letter,
     at_most_once_dead_letter_detect_cycle,
     subscription_options_persisted,
     subscription_options_modify,
     subscription_options_modify_qos1,
     subscription_options_modify_qos0,
     session_upgrade_v3_v5_qos1,
     session_upgrade_v3_v5_qos0,
     compatibility_v3_v5,
     session_upgrade_v3_v5_unsubscribe,
     session_upgrade_v4_v5_no_queue_bind_permission,
     amqp091_cc_header
    ].

cluster_size_3_tests() ->
    [session_migrate_v3_v5,
     session_takeover_v3_v5
    ].

suite() ->
    [{timetrap, {minutes, 10}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mqtt, Config) ->
    rabbit_ct_helpers:set_config(Config, {websocket, false});
init_per_group(web_mqtt, Config) ->
    rabbit_ct_helpers:set_config(Config, {websocket, true});

init_per_group(Group, Config0) ->
    Nodes = case Group of
                cluster_size_1 -> 1;
                cluster_size_3 -> 3
            end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config0, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config0,
                [{mqtt_version, v5},
                 {rmq_nodes_count, Nodes},
                 {rmq_nodename_suffix, Suffix},
                 {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                                        tcp_port_mqtt_tls_extra]}]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit, [{classic_queue_default_version, 2},
                          {quorum_tick_interval, 200}]}),
    Config = rabbit_ct_helpers:run_steps(
               Config2,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    case Group of
        cluster_size_1 ->
            ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, mqtt_v5);
        cluster_size_3 ->
            ok
    end,
    Config.

end_per_group(G, Config)
  when G =:= cluster_size_1;
       G =:= cluster_size_3 ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(T, Config)
  when T =:= session_expiry_interval_disconnect_infinity_to_zero;
       T =:= session_expiry_interval_disconnect_to_infinity;
       T =:= session_expiry_interval_reconnect_infinity_to_zero ->
    Par = max_session_expiry_interval_secs,
    {ok, Default} = rpc(Config, application, get_env, [?APP, Par]),
    ok = rpc(Config, application, set_env, [?APP, Par, infinity]),
    Config1 = rabbit_ct_helpers:set_config(Config, {Par, Default}),
    init_per_testcase0(T, Config1);
init_per_testcase(T, Config) ->
    init_per_testcase0(T, Config).

init_per_testcase0(Testcase, Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ok = rabbit_ct_broker_helpers:enable_plugin(Config, N, rabbitmq_web_mqtt) || N <- Nodes],
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(T, Config)
  when T =:= session_expiry_interval_disconnect_infinity_to_zero;
       T =:= session_expiry_interval_disconnect_to_infinity;
       T =:= session_expiry_interval_reconnect_infinity_to_zero ->
    Par = max_session_expiry_interval_secs,
    Default = ?config(Par, Config),
    ok = rpc(Config, application, set_env, [?APP, Par, Default]),
    rabbit_ct_helpers:testcase_finished(Config, T);

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

client_set_max_packet_size_publish(Config) ->
    NumRejectedBefore = dead_letter_metric(messages_dead_lettered_rejected_total, Config),
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    MaxPacketSize = 500,
    C = connect(ClientId, Config, [{properties, #{'Maximum-Packet-Size' => MaxPacketSize}}]),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    PayloadTooLarge = binary:copy(<<"x">>, MaxPacketSize + 1),
    %% We expect the PUBLISH from client to server to succeed.
    ?assertMatch({ok, _}, emqtt:publish(C, Topic, PayloadTooLarge, [{qos, 1}])),
    %% We expect the server to drop the PUBLISH packet prior to sending to the client
    %% because the packet is larger than what the client is able to receive.
    assert_nothing_received(),
    NumRejected = dead_letter_metric(messages_dead_lettered_rejected_total, Config) - NumRejectedBefore,
    ?assertEqual(1, NumRejected),
    ok = emqtt:disconnect(C).

client_set_max_packet_size_connack(Config) ->
    {C, Connect} = start_client(?FUNCTION_NAME, Config, 0,
                                [{properties, #{'Maximum-Packet-Size' => 2}},
                                 {connect_timeout, 1}]),
    unlink(C),
    %% We expect the server to drop the CONNACK packet because it's larger than 2 bytes.
    ?assertEqual({error, connack_timeout}, Connect(C)).

%% "It is a Protocol Error to include the Receive Maximum
%% value more than once or for it to have the value 0."
client_set_max_packet_size_invalid(Config) ->
    {C, Connect} = start_client(?FUNCTION_NAME, Config, 0,
                                [{properties, #{'Maximum-Packet-Size' => 0}}]),
    unlink(C),
    ?assertMatch({error, _}, Connect(C)).

message_expiry_interval(Config) ->
    NumExpiredBefore = dead_letter_metric(messages_dead_lettered_expired_total, Config),
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    Pub = connect(<<"publisher">>, Config),
    Sub1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _, [1]} = emqtt:subscribe(Sub1, Topic, qos1),
    ok = emqtt:disconnect(Sub1),

    {ok, _} = emqtt:publish(Pub, Topic, #{'Message-Expiry-Interval' => 1}, <<"m1">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(Pub, Topic, #{}, <<"m2">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(Pub, Topic, #{'Message-Expiry-Interval' => 10}, <<"m3">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(Pub, Topic, #{'Message-Expiry-Interval' => 2}, <<"m4">>, [{qos, 1}]),
    timer:sleep(2001),
    Sub2 = connect(ClientId, Config, non_clean_sess_opts()),
    receive {publish, #{client_pid := Sub2,
                        topic := Topic,
                        payload := <<"m2">>,
                        properties := Props}}
              when map_size(Props) =:= 0 -> ok
    after 1000 -> ct:fail("did not receive m2")
    end,

    receive {publish, #{client_pid := Sub2,
                        topic := Topic,
                        payload := <<"m3">>,
                        %% "The PUBLISH packet sent to a Client by the Server MUST contain a Message
                        %% Expiry Interval set to the received value minus the time that the
                        %% Application Message has been waiting in the Server" [MQTT-3.3.2-6]
                        properties := #{'Message-Expiry-Interval' := MEI}}} ->
                assert_message_expiry_interval(10 - 2, MEI)
    after 100 -> ct:fail("did not receive m3")
    end,
    assert_nothing_received(),
    NumExpired = dead_letter_metric(messages_dead_lettered_expired_total, Config) - NumExpiredBefore,
    ?assertEqual(2, NumExpired),

    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(Sub2),
    Sub3 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(Sub3).

message_expiry_interval_will_message(Config) ->
    NumExpiredBefore = dead_letter_metric(messages_dead_lettered_expired_total, Config),
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    Opts = [{will_topic, Topic},
            {will_payload, <<"will payload">>},
            {will_qos, 1},
            {will_props, #{'Message-Expiry-Interval' => 1}}
           ],
    Pub = connect(<<"will-publisher">>, Config, Opts),
    timer:sleep(100),
    [ServerPublisherPid] = util:all_connection_pids(Config),

    Sub1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _, [1]} = emqtt:subscribe(Sub1, Topic, qos1),
    ok = emqtt:disconnect(Sub1),

    unlink(Pub),
    %% Trigger sending of will message.
    erlang:exit(ServerPublisherPid, test_will),
    %% Wait for will message to expire.
    timer:sleep(1100),
    NumExpired = dead_letter_metric(messages_dead_lettered_expired_total, Config) - NumExpiredBefore,
    ?assertEqual(1, NumExpired),

    Sub2 = connect(ClientId, Config, [{clean_start, true}]),
    assert_nothing_received(),
    ok = emqtt:disconnect(Sub2).

message_expiry_interval_retained_message(Config) ->
    Pub = connect(<<"publisher">>, Config),

    {ok, _} = emqtt:publish(Pub, <<"topic1">>, #{'Message-Expiry-Interval' => 100},
                            <<"m1.1">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(Pub, <<"topic2">>, #{'Message-Expiry-Interval' => 2},
                            <<"m2">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(Pub, <<"topic3">>, #{'Message-Expiry-Interval' => 100},
                            <<"m3.1">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(Pub, <<"topic4">>, #{'Message-Expiry-Interval' => 100},
                            <<"m4">>, [{retain, true}, {qos, 1}]),

    {ok, _} = emqtt:publish(Pub, <<"topic1">>, #{'Message-Expiry-Interval' => 2},
                            <<"m1.2">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(Pub, <<"topic2">>, #{'Message-Expiry-Interval' => 2},
                            <<>>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(Pub, <<"topic3">>, #{},
                            <<"m3.2">>, [{retain, true}, {qos, 1}]),
    timer:sleep(2001),
    %% Expectations:
    %% topic1 expired because 2 seconds elapsed
    %% topic2 is not retained because it got deleted
    %% topic3 is retained because its new message does not have an Expiry-Interval set
    %% topic4 is retained because 100 seconds have not elapsed
    Sub = connect(<<"subscriber">>, Config),
    {ok, _, [1,1,1,1]} = emqtt:subscribe(Sub, [{<<"topic1">>, qos1},
                                               {<<"topic2">>, qos1},
                                               {<<"topic3">>, qos1},
                                               {<<"topic4">>, qos1}]),
    receive {publish, #{client_pid := Sub,
                        retain := true,
                        topic := <<"topic3">>,
                        payload := <<"m3.2">>,
                        properties := Props}}
              when map_size(Props) =:= 0 -> ok
    after 100 -> ct:fail("did not topic3")
    end,

    receive {publish, #{client_pid := Sub,
                        retain := true,
                        topic := <<"topic4">>,
                        payload := <<"m4">>,
                        properties := #{'Message-Expiry-Interval' := MEI}}} ->
                assert_message_expiry_interval(100 - 2, MEI)
    after 100 -> ct:fail("did not receive topic4")
    end,
    assert_nothing_received(),

    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(Sub).

session_expiry_interval_classic_queue_disconnect_decrease(Config) ->
    ok = session_expiry_interval_disconnect_decrease(rabbit_classic_queue, Config).

session_expiry_interval_quorum_queue_disconnect_decrease(Config) ->
    ok = rpc(Config, application, set_env, [?APP, durable_queue_type, quorum]),
    ok = session_expiry_interval_disconnect_decrease(rabbit_quorum_queue, Config),
    ok = rpc(Config, application, unset_env, [?APP, durable_queue_type]).

session_expiry_interval_disconnect_decrease(QueueType, Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 100}}]),
    {ok, _, [1]} = emqtt:subscribe(C1, <<"t/1">>, qos1),

    [Q1] = rpc(Config, rabbit_amqqueue, list, []),
    ?assertEqual(QueueType,
                 amqqueue:get_type(Q1)),
    ?assertEqual({long, 100_000},
                 rabbit_misc:table_lookup(amqqueue:get_arguments(Q1), ?QUEUE_TTL_KEY)),

    %% DISCONNECT decreases Session Expiry Interval from 100 seconds to 1 second.
    ok = emqtt:disconnect(C1, _RcNormalDisconnection = 0, #{'Session-Expiry-Interval' => 1}),
    assert_queue_ttl(1, 1, Config),

    timer:sleep(1500),
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    %% Server should reply in CONNACK that it does not have session state for our client ID.
    ?assertEqual({session_present, 0},
                 proplists:lookup(session_present, emqtt:info(C2))),
    ok = emqtt:disconnect(C2).

session_expiry_interval_disconnect_zero_to_non_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 0}}]),
    {ok, _, [1]} = emqtt:subscribe(C1, <<"t/1">>, qos1),
    %% "If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol
    %% Error to set a non-zero Session Expiry Interval in the DISCONNECT packet sent by the Client.
    ok = emqtt:disconnect(C1, _RcNormalDisconnection = 0, #{'Session-Expiry-Interval' => 60}),
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    %% Due to the prior protocol error, we expect the requested session expiry interval of
    %% 60 seconds not to be applied. Therefore, the server should reply in CONNACK that
    %% it does not have session state for our client ID.
    ?assertEqual({session_present, 0},
                 proplists:lookup(session_present, emqtt:info(C2))),
    ok = emqtt:disconnect(C2).

session_expiry_interval_disconnect_non_zero_to_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 60}}]),
    {ok, _, [0, 1]} = emqtt:subscribe(C1, [{<<"t/0">>, qos0},
                                           {<<"t/1">>, qos1}]),
    ?assertEqual(2, rpc(Config, rabbit_amqqueue, count, [])),
    ok = emqtt:disconnect(C1, _RcNormalDisconnection = 0, #{'Session-Expiry-Interval' => 0}),
    eventually(?_assertEqual(0, rpc(Config, rabbit_amqqueue, count, []))),
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    ?assertEqual({session_present, 0},
                 proplists:lookup(session_present, emqtt:info(C2))),
    ok = emqtt:disconnect(C2).

session_expiry_interval_disconnect_infinity_to_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}}]),
    {ok, _, [1, 0]} = emqtt:subscribe(C1, [{<<"t/1">>, qos1},
                                           {<<"t/0">>, qos0}]),
    assert_no_queue_ttl(2, Config),

    ok = emqtt:disconnect(C1, _RcNormalDisconnection = 0, #{'Session-Expiry-Interval' => 0}),
    eventually(?_assertEqual(0, rpc(Config, rabbit_amqqueue, count, []))).

session_expiry_interval_disconnect_to_infinity(Config) ->
    ClientId = ?FUNCTION_NAME,
    %% Connect with a non-zero and non-infinity Session Expiry Interval.
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 1}}]),
    {ok, _, [0, 1]} = emqtt:subscribe(C1, [{<<"t/0">>, qos0},
                                           {<<"t/1">>, qos1}]),
    assert_queue_ttl(1, 2, Config),

    %% Disconnect with infinity should remove queue TTL from both queues.
    ok = emqtt:disconnect(C1, _RcNormalDisconnection = 0, #{'Session-Expiry-Interval' => 16#FFFFFFFF}),
    timer:sleep(100),
    assert_no_queue_ttl(2, Config),

    C2 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C2).

session_expiry_interval_reconnect_non_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 60}}]),
    {ok, _, [0, 1]} = emqtt:subscribe(C1, [{<<"t/0">>, qos0},
                                           {<<"t/1">>, qos1}]),
    assert_queue_ttl(60, 2, Config),
    ok = emqtt:disconnect(C1),

    C2 = connect(ClientId, Config, [{clean_start, false},
                                    {properties, #{'Session-Expiry-Interval' => 1}}]),
    ?assertEqual({session_present, 1},
                 proplists:lookup(session_present, emqtt:info(C2))),
    assert_queue_ttl(1, 2, Config),

    ok = emqtt:disconnect(C2),
    C3 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C3).

session_expiry_interval_reconnect_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 60}}]),
    {ok, _, [0, 1]} = emqtt:subscribe(C1, [{<<"t/0">>, qos0},
                                           {<<"t/1">>, qos1}]),
    assert_queue_ttl(60, 2, Config),
    ok = emqtt:disconnect(C1),

    C2 = connect(ClientId, Config, [{clean_start, false},
                                    {properties, #{'Session-Expiry-Interval' => 0}}]),
    ?assertEqual({session_present, 1},
                 proplists:lookup(session_present, emqtt:info(C2))),
    assert_queue_ttl(0, 2, Config),
    ok = emqtt:disconnect(C2).

session_expiry_interval_reconnect_infinity_to_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}}]),
    {ok, _, [0, 1]} = emqtt:subscribe(C1, [{<<"t/0">>, qos0},
                                           {<<"t/1">>, qos1}]),
    assert_no_queue_ttl(2, Config),
    ok = emqtt:disconnect(C1),

    C2 = connect(ClientId, Config, [{clean_start, false}]),
    ?assertEqual({session_present, 1},
                 proplists:lookup(session_present, emqtt:info(C2))),
    assert_queue_ttl(0, 2, Config),
    ok = emqtt:disconnect(C2).

client_publish_qos2(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    {C, Connect} = start_client(ClientId, Config, 0, []),
    ?assertMatch({ok, #{'Maximum-QoS' := 1}}, Connect(C)),
    ?assertEqual({error, {disconnected, _RcQosNotSupported = 155, #{}}},
                 emqtt:publish(C, Topic, <<"msg">>, qos2)).

client_rejects_publish(Config) ->
    NumRejectedBefore = dead_letter_metric(messages_dead_lettered_rejected_total, Config),
    Payload = Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config, [{auto_ack, false}]),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, Payload, qos1),
    receive {publish, #{payload := Payload,
                        packet_id := PacketId}} ->
                %% Negatively ack the PUBLISH.
                emqtt:puback(C, PacketId, ?RC_UNSPECIFIED_ERROR)
    after 1000 ->
              ct:fail("did not receive PUBLISH")
    end,
    %% Even though we nacked the PUBLISH, we expect the server to not re-send the same message:
    %% "If PUBACK [...] is received containing a Reason Code of 0x80 or greater the corresponding
    %% PUBLISH packet is treated as acknowledged, and MUST NOT be retransmitted" [MQTT-4.4.0-2].
    assert_nothing_received(),
    %% However, we expect RabbitMQ to dead letter negatively acknowledged messages.
    NumRejected = dead_letter_metric(messages_dead_lettered_rejected_total, Config) - NumRejectedBefore,
    ?assertEqual(1, NumRejected),
    ok = emqtt:disconnect(C).

will_qos2(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    Opts = [{will_topic, Topic},
            {will_payload, <<"msg">>},
            {will_qos, 2}],
    {C, Connect} = start_client(ClientId, Config, 0, Opts),
    unlink(C),
    ?assertEqual({error, {qos_not_supported, #{}}}, Connect(C)).

client_receive_maximum_min(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config, [{auto_ack, false},
                                   %% Minimum allowed Receive Maximum is 1.
                                   {properties, #{'Receive-Maximum' => 1}}]),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"m1">>, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"m2">>, qos1),
    %% Since client set Receive Maximum is 1, at most 1 QoS 1 message should be in
    %% flight from server to client at any given point in time.
    PacketId1 = receive {publish, #{payload := <<"m1">>,
                                    packet_id := Id}} ->
                            Id
                after 1000 ->
                          ct:fail("did not receive m1")
                end,
    assert_nothing_received(),
    %% Only when we ack the 1st message, we expect to receive the 2nd message.
    emqtt:puback(C, PacketId1),
    ok = expect_publishes(C, Topic, [<<"m2">>]),
    ok = emqtt:disconnect(C).

client_receive_maximum_large(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config, [{auto_ack, false},
                                   {properties, #{'Receive-Maximum' => 1_000}}]),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    %% We know that the configured mqtt.prefetch is 10.
    Prefetch = 10,
    Payloads = [integer_to_binary(N) || N <- lists:seq(1, Prefetch)],
    [{ok, _} = emqtt:publish(C, Topic, P, qos1) || P <- Payloads],
    {ok, _} = emqtt:publish(C, Topic, <<"I wait in the queue">>, qos1),
    ok = expect_publishes(C, Topic, Payloads),
    %% We expect the server to cap the number of in flight QoS 1 messages sent to the
    %% client to the configured mqtt.prefetch value even though the client set a larger
    %% Receive Maximum value.
    assert_nothing_received(),
    ok = emqtt:disconnect(C).

unsubscribe_success(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, [1]} = emqtt:subscribe(C, <<"topic/1">>, qos1),
    {ok, _, [0]} = emqtt:subscribe(C, <<"topic/0">>, qos0),
    ?assertMatch({ok, _, [?RC_SUCCESS, ?RC_SUCCESS]},
                 emqtt:unsubscribe(C, [<<"topic/1">>, <<"topic/0">>])),
    ok = emqtt:disconnect(C).

unsubscribe_topic_not_found(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, [1]} = emqtt:subscribe(C, <<"topic/1">>, qos1),
    ?assertMatch({ok, _, [?RC_SUCCESS, ?RC_NO_SUBSCRIPTION_EXISTED]},
                 emqtt:unsubscribe(C, [<<"topic/1">>, <<"topic/0">>])),
    ok = emqtt:disconnect(C).

subscription_option_no_local(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    Other = connect(<<"other">>, Config),
    {ok, _, [0, 0]} = emqtt:subscribe(C, [{<<"t/1">>, [{nl, true}]},
                                          {<<"t/2">>, [{nl, false}]}]),
    {ok, _, [0]} = emqtt:subscribe(Other, [{<<"t/1">>, [{nl, true}]}]),
    ok = emqtt:publish(C, <<"t/1">>, <<"m1">>),
    ok = emqtt:publish(C, <<"t/2">>, <<"m2">>),
    ok = expect_publishes(Other, <<"t/1">>, [<<"m1">>]),
    ok = expect_publishes(C, <<"t/2">>, [<<"m2">>]),
    %% We expect C to not receive m1.
    assert_nothing_received(),
    ok = emqtt:disconnect(C),
    ok = emqtt:disconnect(Other).

subscription_option_no_local_wildcards(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, [0, 0, 0, 0]} =
    emqtt:subscribe(C, [{<<"+/1">>, [{nl, true}]},
                        {<<"t/1/#">>, [{nl, true}]},
                        {<<"+/2">>, [{nl, true}]},
                        {<<"t/2/#">>, [{nl, false}]}]),
    %% Matches the first two subscriptions.
    %% All matching subscriptions have the No Local option set.
    %% Therefore, we should not receive m1.
    ok = emqtt:publish(C, <<"t/1">>, <<"m1">>),
    %% Matches the last two subscriptions.
    %% Not all matching subscriptions have the No Local option set.
    %% Therefore, we should receive m2.
    ok = emqtt:publish(C, <<"t/2">>, <<"m2">>),
    ok = expect_publishes(C, <<"t/2">>, [<<"m2">>]),
    assert_nothing_received(),
    ok = emqtt:disconnect(C).

subscription_option_retain_as_published(Config) ->
    C1 = connect(<<"c1">>, Config),
    C2 = connect(<<"c2">>, Config),
    {ok, _, [0, 0]} = emqtt:subscribe(C1, [{<<"t/1">>, [{rap, true}]},
                                           {<<"t/2">>, [{rap, false}]}]),
    {ok, _, [0]} = emqtt:subscribe(C2, [{<<"t/1">>, [{rap, true}]}]),
    ok = emqtt:publish(C1, <<"t/1">>, <<"m1">>, [{retain, true}]),
    ok = emqtt:publish(C1, <<"t/2">>, <<"m2">>, [{retain, true}]),
    receive {publish, #{client_pid := C1,
                        topic := <<"t/1">>,
                        payload := <<"m1">>,
                        retain := true}} -> ok
    after 1000 -> ct:fail("did not receive m1")
    end,
    receive {publish, #{client_pid := C1,
                        topic := <<"t/2">>,
                        payload := <<"m2">>,
                        retain := false}} -> ok
    after 1000 -> ct:fail("did not receive m2")
    end,
    receive {publish, #{client_pid := C2,
                        topic := <<"t/1">>,
                        payload := <<"m1">>,
                        retain := true}} -> ok
    after 1000 -> ct:fail("did not receive m1")
    end,
    ok = emqtt:publish(C1, <<"t/1">>, <<"">>, [{retain, true}]),
    ok = emqtt:publish(C1, <<"t/2">>, <<"">>, [{retain, true}]),
    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2).

subscription_option_retain_as_published_wildcards(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, [0, 0, 0, 0]} = emqtt:subscribe(C, [{<<"+/1">>, [{rap, false}]},
                                                {<<"t/1/#">>, [{rap, false}]},
                                                {<<"+/2">>, [{rap, false}]},
                                                {<<"t/2/#">>, [{rap, true}]}]),
    %% Matches the first two subscriptions.
    ok = emqtt:publish(C, <<"t/1">>, <<"m1">>, [{retain, true}]),
    %% Matches the last two subscriptions.
    ok = emqtt:publish(C, <<"t/2">>, <<"m2">>, [{retain, true}]),
    receive {publish, #{topic := <<"t/1">>,
                        payload := <<"m1">>,
                        %% No matching subscription has the
                        %% Retain As Published option set.
                        retain := false}} -> ok
    after 1000 -> ct:fail("did not receive m1")
    end,
    receive {publish, #{topic := <<"t/2">>,
                        payload := <<"m2">>,
                        %% (At least) one matching subscription has the
                        %% Retain As Published option set.
                        retain := true}} -> ok
    after 1000 -> ct:fail("did not receive m2")
    end,
    ok = emqtt:publish(C, <<"t/1">>, <<"">>, [{retain, true}]),
    ok = emqtt:publish(C, <<"t/2">>, <<"">>, [{retain, true}]),
    ok = emqtt:disconnect(C).

subscription_identifier(Config) ->
    C1 = connect(<<"c1">>, Config),
    C2 = connect(<<"c2">>, Config),
    {ok, _, [0, 0]} = emqtt:subscribe(C1, #{'Subscription-Identifier' => 1}, [{<<"t/1">>, []},
                                                                              {<<"+/1">>, []}]),
    {ok, _, [0]} = emqtt:subscribe(C2, #{'Subscription-Identifier' => 1}, [{<<"t/2">>, []}]),
    {ok, _, [0, 0]} = emqtt:subscribe(C2, #{'Subscription-Identifier' => 16#fffffff}, [{<<"+/2">>, []},
                                                                                       {<<"t/3">>, []}]),
    {ok, _, [0]} = emqtt:subscribe(C2, #{}, [{<<"t/2/#">>, []}]),
    ok = emqtt:publish(C1, <<"t/1">>, <<"m1">>),
    ok = emqtt:publish(C1, <<"t/2">>, <<"m2">>),
    ok = emqtt:publish(C1, <<"t/3">>, <<"m3">>),
    ok = emqtt:publish(C1, <<"t/2/xyz">>, <<"m4">>),
    receive {publish,
             #{client_pid := C1,
               topic := <<"t/1">>,
               payload := <<"m1">>,
               %% "It is possible that the Client made several subscriptions which match a publication
               %% and that it used the same identifier for more than one of them. In this case the
               %% PUBLISH packet will carry multiple identical Subscription Identifiers." [v5 3.3.4]
               properties := #{'Subscription-Identifier' := [1, 1]}}} -> ok
    after 1000 -> ct:fail("did not receive m1")
    end,
    receive {publish,
             #{client_pid := C2,
               topic := <<"t/2">>,
               payload := <<"m2">>,
               properties := #{'Subscription-Identifier' := Ids}}} ->
                %% "If the Server sends a single copy of the message it MUST include in the PUBLISH
                %% packet the Subscription Identifiers for all matching subscriptions which have a
                %% Subscription Identifiers, their order is not significant [MQTT-3.3.4-4]." [v5 3.3.4]
                ?assertEqual([1, 16#fffffff], lists:sort(Ids))
    after 1000 -> ct:fail("did not receive m2")
    end,
    receive {publish,
             #{client_pid := C2,
               topic := <<"t/3">>,
               payload := <<"m3">>,
               properties := #{'Subscription-Identifier' := 16#fffffff}}} -> ok
    after 1000 -> ct:fail("did not receive m3")
    end,
    receive {publish,
             #{client_pid := C2,
               topic := <<"t/2/xyz">>,
               payload := <<"m4">>,
               properties := Props}} ->
                ?assertNot(maps:is_key('Subscription-Identifier', Props))
    after 1000 -> ct:fail("did not receive m4")
    end,
    assert_nothing_received(),
    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2).

subscription_identifier_amqp091(Config) ->
    C1 = connect(<<"sub 1">>, Config),
    C2 = connect(<<"sub 2">>, Config),
    {ok, _, [1]} = emqtt:subscribe(C1, #{'Subscription-Identifier' => 1}, [{<<"a/+">>, [{qos, 1}]}]),
    {ok, _, [1]} = emqtt:subscribe(C2, #{'Subscription-Identifier' => 16#fffffff}, [{<<"a/b">>, [{qos, 1}]}]),
    Ch = rabbit_ct_client_helpers:open_channel(Config),

    %% Test routing to a single queue.
    amqp_channel:call(Ch, #'basic.publish'{exchange = <<"amq.topic">>,
                                           routing_key = <<"a.a">>},
                      #amqp_msg{payload = <<"m1">>}),
    receive {publish,
             #{client_pid := C1,
               topic := <<"a/a">>,
               payload := <<"m1">>,
               properties := #{'Subscription-Identifier' := 1}}} -> ok
    after 1000 -> ct:fail("did not receive message m1")
    end,

    %% Test routing to multiple queues.
    amqp_channel:call(Ch, #'basic.publish'{exchange = <<"amq.topic">>,
                                           routing_key = <<"a.b">>},
                      #amqp_msg{payload = <<"m2">>}),
    receive {publish,
             #{client_pid := C1,
               topic := <<"a/b">>,
               payload := <<"m2">>,
               properties := #{'Subscription-Identifier' := 1}}} -> ok
    after 1000 -> ct:fail("did not receive message m2")
    end,
    receive {publish,
             #{client_pid := C2,
               topic := <<"a/b">>,
               payload := <<"m2">>,
               properties := #{'Subscription-Identifier' := 16#fffffff}}} -> ok
    after 1000 -> ct:fail("did not receive message m2")
    end,

    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, 0).

subscription_identifier_at_most_once_dead_letter(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 1}, [{<<"dead letter/#">>, [{qos, 1}]}]),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QArgs = [{<<"x-dead-letter-exchange">>, longstr, <<"amq.topic">>},
             {<<"x-dead-letter-routing-key">>, longstr, <<"dead letter.a">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = <<"source queue">>,
                                                                   durable = true,
                                                                   exclusive = true,
                                                                   arguments = QArgs}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"source queue">>},
                      #amqp_msg{payload = <<"msg">>,
                                props = #'P_basic'{expiration = <<"0">>}}),
    receive {publish,
             #{client_pid := C,
               topic := <<"dead letter/a">>,
               payload := <<"msg">>,
               properties := #{'Subscription-Identifier' := 1}}} -> ok
    after 1000 -> ct:fail("did not receive msg")
    end,
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, 0).

at_most_once_dead_letter_detect_cycle(Config) ->
    NumExpiredBefore = dead_letter_metric(messages_dead_lettered_expired_total, Config, at_most_once),
    SubClientId = Payload = PolicyName = atom_to_binary(?FUNCTION_NAME),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, <<"mqtt-subscription-", SubClientId/binary, "qos1">>, <<"queues">>,
           %% Create dead letter cycle: qos1 queue -> topic exchange -> qos1 queue
           [{<<"dead-letter-exchange">>, <<"amq.topic">>},
            {<<"message-ttl">>, 1}]),
    Sub1 = connect(SubClientId, Config, non_clean_sess_opts()),
    {ok, _, [1]} = emqtt:subscribe(Sub1, #{'Subscription-Identifier' => 10}, [{<<"+/b">>, [{qos, 1}]}]),
    ok = emqtt:disconnect(Sub1),

    Pub = connect(<<"publisher">>, Config),
    {ok, _} = emqtt:publish(Pub, <<"a/b">>, Payload, qos1),
    ok = emqtt:disconnect(Pub),
    %% Given our subscribing client is disconnected, the message should be dead lettered after 1 ms.
    %% However, due to the dead letter cycle, we expect the message to be dropped.
    timer:sleep(5),
    Sub2 = connect(SubClientId, Config, [{clean_start, false}]),
    assert_nothing_received(),
    %% Double check that the message was indeed (exactly once) dead lettered.
    NumExpired = dead_letter_metric(messages_dead_lettered_expired_total,
                                    Config, at_most_once) - NumExpiredBefore,
    ?assertEqual(1, NumExpired),
    ok = emqtt:disconnect(Sub2),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName).

%% Tests that the session state in the server includes subscription options
%% and subscription identifiers and that this session state is persisted.
subscription_options_persisted(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _, [0, 1]} = emqtt:subscribe(C1, #{'Subscription-Identifier' => 99},
                                      [{<<"t1">>, [{nl, true}, {rap, false}, {qos, 0}]},
                                       {<<"t2">>, [{nl, false}, {rap, true}, {qos, 1}]}]),
    unlink(C1),
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    ok = emqtt:publish(C2, <<"t1">>, <<"m1">>),
    ok = emqtt:publish(C2, <<"t2">>, <<"m2">>, [{retain, true}]),
    receive {publish,
             #{client_pid := C2,
               payload := <<"m2">>,
               retain := true,
               qos := 0,
               properties := #{'Subscription-Identifier' := 99}}} -> ok
    after 1000 -> ct:fail("did not receive m2")
    end,
    assert_nothing_received(),
    ok = emqtt:publish(C2, <<"t2">>, <<"">>, [{retain, true}]),
    ok = emqtt:disconnect(C2).

%% "If a Server receives a SUBSCRIBE packet containing a Topic Filter that is identical to a Non‑shared
%% Subscription’s Topic Filter for the current Session, then it MUST replace that existing Subscription
%% with a new Subscription [MQTT-3.8.4-3]. The Topic Filter in the new Subscription will be identical
%% to that in the previous Subscription, although its Subscription Options could be different."
%%
%% "The Subscription Identifiers are part of the Session State in the Server and are returned to the
%% Client receiving a matching PUBLISH packet. They are removed from the Server’s Session State when the
%% Server receives an UNSUBSCRIBE packet, when the Server receives a SUBSCRIBE packet from the Client for
%% the same Topic Filter but with a different Subscription Identifier or with no Subscription Identifier,
%% or when the Server sends Session Present 0 in a CONNACK packet" [v5 3.8.4]
subscription_options_modify(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),

    {ok, _, [0]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 1}, Topic, [{nl, true}]),
    {ok, _} = emqtt:publish(C, Topic, <<"m1">>, qos1),
    assert_nothing_received(),

    %% modify No Local
    {ok, _, [0]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 1}, Topic, [{nl, false}]),
    {ok, _} = emqtt:publish(C, Topic, <<"m2">>, qos1),
    receive {publish, #{payload := <<"m2">>,
                        qos := 0 }} -> ok
    after 1000 -> ct:fail("did not receive m2")
    end,

    %% modify QoS
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 1}, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"m3">>, qos1),
    receive {publish, #{payload := <<"m3">>,
                        qos := 1,
                        properties := #{'Subscription-Identifier' := 1}}} -> ok
    after 1000 -> ct:fail("did not receive m3")
    end,

    %% modify Subscription Identifier
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 2}, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"m4">>, qos1),
    receive {publish, #{payload := <<"m4">>,
                        properties := #{'Subscription-Identifier' := 2}}} -> ok
    after 1000 -> ct:fail("did not receive m4")
    end,

    %% remove Subscription Identifier
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"m5">>, [{retain, true}, {qos, 1}]),
    receive {publish, #{payload := <<"m5">>,
                        retain := false,
                        properties := Props}} when map_size(Props) =:= 0 -> ok
    after 1000 -> ct:fail("did not receive m5")
    end,

    %% modify Retain As Published
    {ok, _, [1]} = emqtt:subscribe(C, Topic, [{rap, true}, {qos, 1}]),
    receive {publish, #{payload := <<"m5">>,
                        retain := true}} -> ok
    after 1000 -> ct:fail("did not receive retained m5")
    end,
    {ok, _} = emqtt:publish(C, Topic, <<"m6">>, [{retain, true}, {qos, 1}]),
    receive {publish, #{payload := <<"m6">>,
                        retain := true}} -> ok
    after 1000 -> ct:fail("did not receive m6")
    end,

    assert_nothing_received(),
    ok = emqtt:publish(C, Topic, <<"">>, [{retain, true}]),
    ok = emqtt:disconnect(C).

%% "If a Server receives a SUBSCRIBE packet containing a Topic Filter that is identical to a
%% Non‑shared Subscription’s Topic Filter for the current Session, then it MUST replace that
%% existing Subscription with a new Subscription [MQTT-3.8.4-3]. The Topic Filter in the new
%% Subscription will be identical to that in the previous Subscription, although its
%% Subscription Options could be different. [...] Applicaton Messages MUST NOT be lost due
%% to replacing the Subscription [MQTT-3.8.4-4]." [v5 3.8.4]
%%
%% This test ensures that messages are not lost when replacing a QoS 1 subscription.
subscription_options_modify_qos1(Config) ->
    subscription_options_modify_qos(1, Config).

%% This test ensures that messages are received at most once
%% when replacing a QoS 0 subscription.
subscription_options_modify_qos0(Config) ->
    subscription_options_modify_qos(0, Config).

subscription_options_modify_qos(Qos, Config) ->
    Topic = atom_to_binary(?FUNCTION_NAME),
    Pub = connect(<<"publisher">>, Config),
    Sub = connect(<<"subscriber">>, Config),
    {ok, _, [Qos]} = emqtt:subscribe(Sub, Topic, Qos),
    Sender = spawn_link(?MODULE, send, [self(), Pub, Topic, 0]),
    receive {publish, #{payload := <<"1">>,
                        properties := Props}} ->
                ?assertEqual(0, maps:size(Props))
    after 1000 -> ct:fail("did not receive 1")
    end,
    %% Replace subscription while another client is sending messages.
    {ok, _, [Qos]} = emqtt:subscribe(Sub, #{'Subscription-Identifier' => 1}, Topic, Qos),
    Sender ! stop,
    NumSent = receive {N, Sender} -> N
              after 1000 -> ct:fail("could not stop publisher")
              end,
    ct:pal("Publisher sent ~b messages", [NumSent]),
    LastExpectedPayload = integer_to_binary(NumSent),
    receive {publish, #{payload := LastExpectedPayload,
                        qos := Qos,
                        client_pid := Sub,
                        properties := #{'Subscription-Identifier' := 1}}} -> ok
    after 1000 -> ct:fail("did not receive ~s", [LastExpectedPayload])
    end,
    case Qos of
        0 ->
            assert_received_no_duplicates();
        1 ->
            ExpectedPayloads = [integer_to_binary(I) || I <- lists:seq(2, NumSent - 1)],
            ok = util:expect_publishes(Sub, Topic, ExpectedPayloads)
    end,
    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(Sub).

%% Tests that no message is lost when upgrading a session
%% with QoS 1 subscription from v3 to v5.
session_upgrade_v3_v5_qos1(Config) ->
    session_upgrade_v3_v5_qos(1, Config).

%% Tests that each message is received at most once
%% when upgrading a session with QoS 0 subscription from v3 to v5.
session_upgrade_v3_v5_qos0(Config) ->
    session_upgrade_v3_v5_qos(0, Config).

session_upgrade_v3_v5_qos(Qos, Config) ->
    ClientId = Topic = atom_to_binary(?FUNCTION_NAME),
    Pub = connect(<<"publisher">>, Config),
    Subv3 = connect(ClientId, Config, [{proto_ver, v3} | non_clean_sess_opts()]),
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(Subv3))),
    {ok, _, [Qos]} = emqtt:subscribe(Subv3, Topic, Qos),
    Sender = spawn_link(?MODULE, send, [self(), Pub, Topic, 0]),
    receive {publish, #{payload := <<"1">>,
                        client_pid := Subv3}} -> ok
    after 1000 -> ct:fail("did not receive 1")
    end,
    %% Upgrade session from v3 to v5 while another client is sending messages.
    ok = emqtt:disconnect(Subv3),
    Subv5 = connect(ClientId, Config, [{proto_ver, v5}, {clean_start, false}]),
    ?assertEqual(5, proplists:get_value(proto_ver, emqtt:info(Subv5))),
    Sender ! stop,
    NumSent = receive {N, Sender} -> N
              after 1000 -> ct:fail("could not stop publisher")
              end,
    ct:pal("Publisher sent ~b messages", [NumSent]),
    LastExpectedPayload = integer_to_binary(NumSent),
    receive {publish, #{payload := LastExpectedPayload,
                        qos := Qos,
                        client_pid := Subv5}} -> ok
    after 1000 -> ct:fail("did not receive ~s", [LastExpectedPayload])
    end,
    case Qos of
        0 ->
            assert_received_no_duplicates();
        1 ->
            ExpectedPayloads = [integer_to_binary(I) || I <- lists:seq(2, NumSent - 1)],
            ok = util:expect_publishes(Subv5, Topic, ExpectedPayloads)
    end,
    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(Subv5).

send(Parent, Client, Topic, NumSent) ->
    receive stop ->
                Parent ! {NumSent, self()}
    after 0 ->
              N = NumSent + 1,
              {ok, _} = emqtt:publish(Client, Topic, integer_to_binary(N), qos1),
              send(Parent, Client, Topic, N)
    end.

assert_received_no_duplicates() ->
    assert_received_no_duplicates0(#{}).

assert_received_no_duplicates0(Received) ->
    receive {publish, #{payload := P}} ->
                case maps:is_key(P, Received) of
                    true -> ct:fail("Received ~p twice", [P]);
                    false -> assert_received_no_duplicates0(maps:put(P, ok, Received))
                end
    after 500 ->
              %% Check that we received at least one message.
              ?assertNotEqual(0, maps:size(Received))
    end.

compatibility_v3_v5(Config) ->
    Cv3 = connect(<<"client v3">>, Config, [{proto_ver, v3}]),
    Cv5 = connect(<<"client v5">>, Config, [{proto_ver, v5}]),
    %% Sanity check that versions were set correctly.
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(Cv3))),
    ?assertEqual(5, proplists:get_value(proto_ver, emqtt:info(Cv5))),
    {ok, _, [1]} = emqtt:subscribe(Cv3, <<"v3/#">>, qos1),
    {ok, _, [1]} = emqtt:subscribe(Cv5, #{'Subscription-Identifier' => 99},
                                   [{<<"v5/#">>, [{rap, true}, {qos, 1}]}]),
    %% Send message in either direction.
    {ok, _} = emqtt:publish(Cv5, <<"v3">>, <<"from v5">>, qos1),
    {ok, _} = emqtt:publish(Cv3, <<"v5">>, <<"from v3">>, [{retain, true}, {qos, 1}]),
    ok = expect_publishes(Cv3, <<"v3">>, [<<"from v5">>]),
    receive {publish,
             #{client_pid := Cv5,
               topic := <<"v5">>,
               payload := <<"from v3">>,
               %% v5 features should work even when message comes from a v3 client.
               retain := true,
               properties := #{'Subscription-Identifier' := 99}}} -> ok
    after 1000 -> ct:fail("did not receive from v3")
    end,
    ok = emqtt:publish(Cv3, <<"v5">>, <<"">>, [{retain, true}]),
    ok = emqtt:disconnect(Cv3),
    ok = emqtt:disconnect(Cv5).

session_upgrade_v3_v5_unsubscribe(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(ClientId, Config, [{proto_ver, v3} | non_clean_sess_opts()]),
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(C1))),
    {ok, _, [0]} = emqtt:subscribe(C1, Topic),
    ok = emqtt:disconnect(C1),
    %% Upgrade the session from v3 to v5.
    C2 = connect(ClientId, Config, [{proto_ver, v5}, {clean_start, false}]),
    ?assertEqual(5, proplists:get_value(proto_ver, emqtt:info(C2))),
    ok = emqtt:publish(C2, Topic, <<"m1">>),
    ok = expect_publishes(C2, Topic, [<<"m1">>]),
    %% Unsubscribing in v5 should work.
    ?assertMatch({ok, _, [?RC_SUCCESS]}, emqtt:unsubscribe(C2, Topic)),
    ok = emqtt:publish(C2, Topic, <<"m2">>),
    assert_nothing_received(),
    ok = emqtt:disconnect(C2).

session_upgrade_v4_v5_no_queue_bind_permission(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(ClientId, Config, [{proto_ver, v4} | non_clean_sess_opts()]),
    ?assertEqual(4, proplists:get_value(proto_ver, emqtt:info(C1))),
    {ok, _, [0]} = emqtt:subscribe(C1, Topic),
    ok = emqtt:disconnect(C1),

    %% Revoking write access to queue will cause queue.bind to fail.
    rabbit_ct_broker_helpers:set_permissions(Config, <<"guest">>, <<"/">>, <<".*">>, <<"">>, <<".*">>),
    %% Upgrading the session from v4 to v5 should fail because it causes
    %% queue.bind and queue.unbind (to change the binding arguments).
    {C2, Connect} = start_client(ClientId, Config, 0, [{proto_ver, v5}, {clean_start, false}]),
    unlink(C2),
    ?assertEqual({error, {not_authorized, #{}}}, Connect(C2)),

    %% Cleanup
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, <<"/">>),
    C3 = connect(ClientId, Config),
    ok = emqtt:disconnect(C3).

amqp091_cc_header(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 1}, [{<<"#">>, [{qos, 1}]}]),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(
      Ch, #'basic.publish'{exchange = <<"amq.topic">>,
                           routing_key = <<"first.key">>},
      #amqp_msg{payload = <<"msg">>,
                props = #'P_basic'{
                           headers = [{<<"CC">>, array,
                                       [{longstr, <<"second.key">>}]}]}}),
    %% Even though both routing key and CC header match the topic filter,
    %% we expect to receive a single message (because only one message is sent)
    %% and a single subscription identifier (because we created only one subscription).
    receive {publish,
             #{topic := <<"first/key">>,
               payload := <<"msg">>,
               properties := #{'Subscription-Identifier' := 1}}} -> ok
    after 1000 -> ct:fail("did not receive msg")
    end,
    assert_nothing_received(),
    ok = emqtt:disconnect(C).

session_migrate_v3_v5(Config) ->
    session_switch_v3_v5(Config, true).

session_takeover_v3_v5(Config) ->
    session_switch_v3_v5(Config, false).

session_switch_v3_v5(Config, Disconnect) ->
    V5Enabled = rabbit_ct_broker_helpers:is_feature_flag_enabled(Config, mqtt_v5),
    [Server1, Server2, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    %% Connect to old node in mixed version cluster.
    C1 = connect(ClientId, Config, Server2, [{proto_ver, v3} | non_clean_sess_opts()]),
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(C1))),
    {ok, _, [1]} = emqtt:subscribe(C1, Topic, qos1),
    case Disconnect of
        true -> ok = emqtt:disconnect(C1);
        false -> unlink(C1)
    end,
    monitor(process, C1),

    case V5Enabled of
        true ->
            %% Upgrade session from v3 to v5.
            C2 = connect(ClientId, Config, Server1, [{proto_ver, v5} | non_clean_sess_opts()]),
            ?assertEqual(5, proplists:get_value(proto_ver, emqtt:info(C2))),
            %% Subscription created with old v3 client should work.
            {ok, _} = emqtt:publish(C2, Topic, <<"m1">>, qos1),
            receive {publish,
                     #{client_pid := C2,
                       payload := <<"m1">>,
                       qos := 1}} -> ok
            after 1000 -> ct:fail("did not receive from m1")
            end,
            %% Modifying subscription with v5 specific feature should work.
            {ok, _, [1]} = emqtt:subscribe(C2, Topic, [{nl, true}, {qos, 1}]),
            {ok, _} = emqtt:publish(C2, Topic, <<"m2">>, qos1),
            receive {publish, P} -> ct:fail("Unexpected local PUBLISH: ~p", [P])
            after 500 -> ok
            end,
            case Disconnect of
                true -> ok = emqtt:disconnect(C2);
                false -> unlink(C2)
            end;
        false ->
            ok
    end,

    %% If feature flag mqtt_v5 is
    %% * enabled: downgrade session from v5 to v3
    %% * disabled: in mixed version cluster connect to new node using v3
    C3 = connect(ClientId, Config, Server1, [{proto_ver, v3} | non_clean_sess_opts()]),
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(C3))),
    receive {'DOWN', _, _, C1, _} -> ok
    after 1000 -> ct:fail("C1 did not exit")
    end,
    case {Disconnect, V5Enabled} of
        {false, true} ->
            receive {disconnected, _RcSessionTakenOver = 142, #{}} -> ok
            after 1000 -> ct:fail("missing DISCONNECT packet for C2")
            end;
        _ ->
            ok
    end,
    %% We expect that v5 specific subscription feature does not apply
    %% anymore when downgrading the session.
    {ok, _} = emqtt:publish(C3, Topic, <<"m3">>, qos1),
    receive {publish,
             #{client_pid := C3,
               payload := <<"m3">>,
               qos := 1}} -> ok
    after 1000 -> ct:fail("did not receive m3 with QoS 1")
    end,
    %% Modifying the subscription once more with v3 client should work.
    {ok, _, [0]} = emqtt:subscribe(C3, Topic, qos0),
    {ok, _} = emqtt:publish(C3, Topic, <<"m4">>, qos1),
    receive {publish,
             #{client_pid := C3,
               payload := <<"m4">>,
               qos := 0}} -> ok
    after 1000 -> ct:fail("did not receive m3 with QoS 0")
    end,

    %% Unsubscribing in v3 should work.
    ?assertMatch({ok, _, _}, emqtt:unsubscribe(C3, Topic)),
    {ok, _} = emqtt:publish(C3, Topic, <<"m5">>, qos1),
    assert_nothing_received(),

    ok = emqtt:disconnect(C3),
    C4 = connect(ClientId, Config, Server1, [{proto_ver, v3}, {clean_start, true}]),
    ok = emqtt:disconnect(C4),
    eventually(?_assertEqual([], all_connection_pids(Config))).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

assert_no_queue_ttl(NumQs, Config) ->
    Qs = rpc(Config, rabbit_amqqueue, list, []),
    ?assertEqual(NumQs, length(Qs)),
    ?assertNot(lists:any(fun(Q) ->
                                 proplists:is_defined(?QUEUE_TTL_KEY, amqqueue:get_arguments(Q))
                         end, Qs)).

assert_queue_ttl(TTLSecs, NumQs, Config) ->
    Qs = rpc(Config, rabbit_amqqueue, list, []),
    ?assertEqual(NumQs, length(Qs)),
    ?assert(lists:all(fun(Q) ->
                              {long, timer:seconds(TTLSecs)} =:= rabbit_misc:table_lookup(
                                                                   amqqueue:get_arguments(Q), ?QUEUE_TTL_KEY)
                      end, Qs)).

dead_letter_metric(Metric, Config) ->
    dead_letter_metric(Metric, Config, disabled).

dead_letter_metric(Metric, Config, Strategy) ->
    Counters = rpc(Config, rabbit_global_counters, overview, []),
    Map = maps:get([{queue_type, rabbit_classic_queue}, {dead_letter_strategy, Strategy}], Counters),
    maps:get(Metric, Map).

assert_nothing_received() ->
    receive Unexpected -> ct:fail("Received unexpected message: ~p", [Unexpected])
    after 500 -> ok
    end.
