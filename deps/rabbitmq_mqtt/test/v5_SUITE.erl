%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

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
-define(RC_NORMAL_DISCONNECTION, 16#00).
-define(RC_DISCONNECT_WITH_WILL, 16#04).
-define(RC_NO_SUBSCRIPTION_EXISTED, 16#11).
-define(RC_UNSPECIFIED_ERROR, 16#80).
-define(RC_PROTOCOL_ERROR, 16#82).
-define(RC_SERVER_SHUTTING_DOWN, 16#8B).
-define(RC_SESSION_TAKEN_OVER, 16#8E).
-define(RC_TOPIC_ALIAS_INVALID, 16#94).

-define(TIMEOUT, 30_000).

all() ->
    [{group, mqtt}].

groups() ->
    [
     {mqtt, [],
      [{cluster_size_1, [shuffle], cluster_size_1_tests()},
       {cluster_size_3, [shuffle], cluster_size_3_tests()}
      ]}
    ].

cluster_size_1_tests() ->
    [
     client_set_max_packet_size_publish,
     client_set_max_packet_size_connack,
     client_set_max_packet_size_invalid,
     message_expiry,
     message_expiry_will_message,
     message_expiry_retained_message,
     session_expiry_classic_queue_disconnect_decrease,
     session_expiry_quorum_queue_disconnect_decrease,
     session_expiry_disconnect_zero_to_non_zero,
     session_expiry_disconnect_non_zero_to_zero,
     session_expiry_disconnect_infinity_to_zero,
     session_expiry_disconnect_to_infinity,
     session_expiry_reconnect_non_zero,
     session_expiry_reconnect_zero,
     session_expiry_reconnect_infinity_to_zero,
     zero_session_expiry_disconnect_autodeletes_qos0_queue,
     client_publish_qos2,
     client_rejects_publish,
     client_receive_maximum_min,
     client_receive_maximum_large,
     unsubscribe_success,
     unsubscribe_topic_not_found,
     subscription_option_no_local,
     subscription_option_no_local_wildcards,
     subscription_option_retain_as_published,
     subscription_option_retain_as_published_wildcards,
     subscription_option_retain_handling,
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
     session_upgrade_v3_v5_amqp091_pub,
     compatibility_v3_v5,
     session_upgrade_v3_v5_unsubscribe,
     session_upgrade_v4_v5_no_queue_bind_permission,
     amqp091_cc_header,
     publish_property_content_type,
     publish_property_payload_format_indicator,
     publish_property_response_topic_correlation_data,
     publish_property_user_property,
     disconnect_with_will,
     will_qos2,
     will_delay_greater_than_session_expiry,
     will_delay_less_than_session_expiry,
     will_delay_equals_session_expiry,
     will_delay_session_expiry_zero,
     will_delay_reconnect_no_will,
     will_delay_reconnect_with_will,
     will_delay_session_takeover,
     will_delay_message_expiry,
     will_delay_message_expiry_publish_properties,
     will_delay_properties,
     will_properties,
     retain_properties,
     topic_alias_client_to_server,
     topic_alias_server_to_client,
     topic_alias_bidirectional,
     topic_alias_invalid,
     topic_alias_unknown,
     topic_alias_disallowed,
     topic_alias_retained_message,
     topic_alias_disallowed_retained_message,
     extended_auth,
     headers_exchange,
     consistent_hash_exchange
    ].

cluster_size_3_tests() ->
    [session_migrate_v3_v5,
     session_takeover_v3_v5,
     will_delay_node_restart
    ].

suite() ->
    [{timetrap, {minutes, 10}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:set_config(Config0, {test_plugins, [rabbitmq_mqtt]}),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mqtt, Config) ->
    rabbit_ct_helpers:set_config(Config, {websocket, false});
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
                 {start_rmq_with_plugins_disabled, true}
                ]),
    Config = rabbit_ct_helpers:merge_app_env(
               Config1,
               {rabbit, [{quorum_tick_interval, 200}]}),
    Config2 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    [util:enable_plugin(Config2, Plugin) || Plugin <- ?config(test_plugins, Config2)],
    Config2.

end_per_group(G, Config)
  when G =:= cluster_size_1;
       G =:= cluster_size_3 ->
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(T, Config)
  when T =:= session_expiry_disconnect_infinity_to_zero;
       T =:= session_expiry_disconnect_to_infinity;
       T =:= session_expiry_reconnect_infinity_to_zero ->
    Par = max_session_expiry_interval_seconds,
    {ok, Default} = rpc(Config, application, get_env, [?APP, Par]),
    ok = rpc(Config, application, set_env, [?APP, Par, infinity]),
    Config1 = rabbit_ct_helpers:set_config(Config, {Par, Default}),
    init_per_testcase0(T, Config1);

init_per_testcase(T, Config)
    when T =:= zero_session_expiry_disconnect_autodeletes_qos0_queue ->
  rpc(Config, rabbit_registry, register, [queue, <<"qos0">>, rabbit_mqtt_qos0_queue]),
  init_per_testcase0(T, Config);

init_per_testcase(T, Config) ->
    init_per_testcase0(T, Config).

init_per_testcase0(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(T, Config)
  when T =:= session_expiry_disconnect_infinity_to_zero;
       T =:= session_expiry_disconnect_to_infinity;
       T =:= session_expiry_reconnect_infinity_to_zero ->
    Par = max_session_expiry_interval_seconds,
    Default = ?config(Par, Config),
    ok = rpc(Config, application, set_env, [?APP, Par, Default]),
    end_per_testcase0(T, Config);
end_per_testcase(T, Config)
    when T =:= zero_session_expiry_disconnect_autodeletes_qos0_queue ->
  ok = rpc(Config, rabbit_registry, unregister, [queue, <<"qos0">>]),
  init_per_testcase0(T, Config);

end_per_testcase(T, Config) ->
    end_per_testcase0(T, Config).

end_per_testcase0(Testcase, Config) ->
    %% Terminate all connections and wait for sessions to terminate before
    %% starting the next test case.
    _ = rabbit_ct_broker_helpers:rpc(
          Config, 0,
          rabbit_networking, close_all_connections, [<<"test finished">>]),
    _ = rabbit_ct_broker_helpers:rpc_all(
          Config,
          rabbit_mqtt, close_local_client_connections, [normal]),
    eventually(?_assertEqual(
                  [],
                  rpc(Config, rabbit_mqtt, local_connection_pids, []))),
    %% Assert that every testcase cleaned up their MQTT sessions.
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, []))),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

delete_queues() ->
    _ = [catch rabbit_amqqueue:delete(Q, false, false, <<"test finished">>)
         || Q <- rabbit_amqqueue:list()],
    ok.

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
    ok = emqtt:disconnect(C),
    ok.


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

message_expiry(Config) ->
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
    after ?TIMEOUT -> ct:fail("did not receive m2")
    end,

    receive {publish, #{client_pid := Sub2,
                        topic := Topic,
                        payload := <<"m3">>,
                        %% "The PUBLISH packet sent to a Client by the Server MUST contain a Message
                        %% Expiry Interval set to the received value minus the time that the
                        %% Application Message has been waiting in the Server" [MQTT-3.3.2-6]
                        properties := #{'Message-Expiry-Interval' := MEI}}} ->
                assert_message_expiry_interval(10 - 2, MEI)
    after ?TIMEOUT -> ct:fail("did not receive m3")
    end,
    assert_nothing_received(),
    NumExpired = dead_letter_metric(messages_dead_lettered_expired_total, Config) - NumExpiredBefore,
    ?assertEqual(2, NumExpired),

    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(Sub2),
    Sub3 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(Sub3).

message_expiry_will_message(Config) ->
    NumExpiredBefore = dead_letter_metric(messages_dead_lettered_expired_total, Config),
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    Opts = [{will_topic, Topic},
            {will_payload, <<"will payload">>},
            {will_qos, 1},
            {will_props, #{'Message-Expiry-Interval' => 1}}
           ],
    Pub = connect(<<"will-publisher">>, Config, Opts),
    Sub1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _, [1]} = emqtt:subscribe(Sub1, Topic, qos1),
    ok = emqtt:disconnect(Sub1),

    unlink(Pub),
    erlang:exit(Pub, trigger_will_message),
    %% Wait for will message to expire.
    timer:sleep(1100),
    NumExpired = dead_letter_metric(messages_dead_lettered_expired_total, Config) - NumExpiredBefore,
    ?assertEqual(1, NumExpired),

    Sub2 = connect(ClientId, Config, [{clean_start, true}]),
    assert_nothing_received(),
    ok = emqtt:disconnect(Sub2).

message_expiry_retained_message(Config) ->
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
    after ?TIMEOUT -> ct:fail("did not topic3")
    end,

    receive {publish, #{client_pid := Sub,
                        retain := true,
                        topic := <<"topic4">>,
                        payload := <<"m4">>,
                        properties := #{'Message-Expiry-Interval' := MEI}}} ->
                assert_message_expiry_interval(100 - 2, MEI)
    after ?TIMEOUT -> ct:fail("did not receive topic4")
    end,
    assert_nothing_received(),

    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(Sub).

session_expiry_classic_queue_disconnect_decrease(Config) ->
    ok = session_expiry_disconnect_decrease(rabbit_classic_queue, Config).

session_expiry_quorum_queue_disconnect_decrease(Config) ->
    ok = rpc(Config, application, set_env, [?APP, durable_queue_type, quorum]),
    ok = session_expiry_disconnect_decrease(rabbit_quorum_queue, Config),
    ok = rpc(Config, application, unset_env, [?APP, durable_queue_type]).

zero_session_expiry_disconnect_autodeletes_qos0_queue(Config) ->
    ClientId = ?FUNCTION_NAME,
    C = connect(ClientId, Config, [
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 0}}]),
    {ok, _, _} = emqtt:subscribe(C, <<"topic0">>, qos0),
    QsQos0 = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_mqtt_qos0_queue]),
    ?assertEqual(1, length(QsQos0)),

    ok = emqtt:disconnect(C),
    %% After terminating a clean session, we expect any session state to be cleaned up on the server.
    %% Give the node some time to clean up the MQTT QoS 0 queue.
    timer:sleep(200),
    L = rpc(Config, rabbit_amqqueue, list, []),
    ?assertEqual(0, length(L)).

session_expiry_disconnect_decrease(QueueType, Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 100}}]),
    {ok, _, [1]} = emqtt:subscribe(C1, <<"t/1">>, qos1),

    [Q1] = rpc(Config, rabbit_amqqueue, list, []),
    ?assertEqual(QueueType,
                 amqqueue:get_type(Q1)),
    ?assertEqual({long, 100_000},
                 rabbit_misc:table_lookup(amqqueue:get_arguments(Q1), ?QUEUE_TTL_KEY)),

    %% DISCONNECT decreases Session Expiry Interval from 100 seconds to 1 second.
    ok = emqtt:disconnect(C1, ?RC_NORMAL_DISCONNECTION, #{'Session-Expiry-Interval' => 1}),
    %% Wait a bit since DISCONNECT is async.
    timer:sleep(50),
    assert_queue_ttl(1, 1, Config),

    timer:sleep(1500),
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    %% Server should reply in CONNACK that it does not have session state for our client ID.
    ?assertEqual({session_present, 0},
                 proplists:lookup(session_present, emqtt:info(C2))),
    ok = emqtt:disconnect(C2).

session_expiry_disconnect_zero_to_non_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 0}}]),
    {ok, _, [1]} = emqtt:subscribe(C1, <<"t/1">>, qos1),
    %% "If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol
    %% Error to set a non-zero Session Expiry Interval in the DISCONNECT packet sent by the Client.
    ok = emqtt:disconnect(C1, ?RC_NORMAL_DISCONNECTION, #{'Session-Expiry-Interval' => 60}),
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    %% Due to the prior protocol error, we expect the requested session expiry interval of
    %% 60 seconds not to be applied. Therefore, the server should reply in CONNACK that
    %% it does not have session state for our client ID.
    ?assertEqual({session_present, 0},
                 proplists:lookup(session_present, emqtt:info(C2))),
    ok = emqtt:disconnect(C2).

session_expiry_disconnect_non_zero_to_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 60}}]),
    {ok, _, [0, 1]} = emqtt:subscribe(C1, [{<<"t/0">>, qos0},
                                           {<<"t/1">>, qos1}]),
    ?assertEqual(2, rpc(Config, rabbit_amqqueue, count, [])),
    ok = emqtt:disconnect(C1, ?RC_NORMAL_DISCONNECTION, #{'Session-Expiry-Interval' => 0}),
    eventually(?_assertEqual(0, rpc(Config, rabbit_amqqueue, count, []))),
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    ?assertEqual({session_present, 0},
                 proplists:lookup(session_present, emqtt:info(C2))),
    ok = emqtt:disconnect(C2).

session_expiry_disconnect_infinity_to_zero(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}}]),
    {ok, _, [1, 0]} = emqtt:subscribe(C1, [{<<"t/1">>, qos1},
                                           {<<"t/0">>, qos0}]),
    assert_no_queue_ttl(2, Config),

    ok = emqtt:disconnect(C1, ?RC_NORMAL_DISCONNECTION, #{'Session-Expiry-Interval' => 0}),
    eventually(?_assertEqual(0, rpc(Config, rabbit_amqqueue, count, []))).

session_expiry_disconnect_to_infinity(Config) ->
    ClientId = ?FUNCTION_NAME,
    %% Connect with a non-zero and non-infinity Session Expiry Interval.
    C1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 1}}]),
    {ok, _, [0, 1]} = emqtt:subscribe(C1, [{<<"t/0">>, qos0},
                                           {<<"t/1">>, qos1}]),
    assert_queue_ttl(1, 2, Config),

    %% Disconnect with infinity should remove queue TTL from both queues.
    ok = emqtt:disconnect(C1, ?RC_NORMAL_DISCONNECTION, #{'Session-Expiry-Interval' => 16#FFFFFFFF}),
    timer:sleep(100),
    assert_no_queue_ttl(2, Config),

    C2 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C2).

session_expiry_reconnect_non_zero(Config) ->
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

session_expiry_reconnect_zero(Config) ->
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

session_expiry_reconnect_infinity_to_zero(Config) ->
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
    unlink(C),
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
    after ?TIMEOUT ->
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
                after ?TIMEOUT ->
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
                        {<<"t/1/#">>, [{nl, false}]},
                        {<<"+/2">>, [{nl, true}]},
                        {<<"t/2/#">>, [{nl, true}]}]),
    %% Matches the first two subscriptions.
    %% Not all matching subscriptions have the No Local option set.
    %% Therefore, we should receive m1.
    ok = emqtt:publish(C, <<"t/1">>, <<"m1">>),
    ok = expect_publishes(C, <<"t/1">>, [<<"m1">>]),
    %% Matches the last two subscriptions.
    %% All matching subscriptions have the No Local option set.
    %% Therefore, we should not receive m2.
    ok = emqtt:publish(C, <<"t/2">>, <<"m2">>),
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
    after ?TIMEOUT -> ct:fail("did not receive m1")
    end,
    receive {publish, #{client_pid := C1,
                        topic := <<"t/2">>,
                        payload := <<"m2">>,
                        retain := false}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m2")
    end,
    receive {publish, #{client_pid := C2,
                        topic := <<"t/1">>,
                        payload := <<"m1">>,
                        retain := true}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m1")
    end,
    {ok, _} = emqtt:publish(C1, <<"t/1">>, <<>>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C1, <<"t/2">>, <<>>, [{retain, true}, {qos, 1}]),
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
    after ?TIMEOUT -> ct:fail("did not receive m1")
    end,
    receive {publish, #{topic := <<"t/2">>,
                        payload := <<"m2">>,
                        %% (At least) one matching subscription has the
                        %% Retain As Published option set.
                        retain := true}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m2")
    end,
    {ok, _} = emqtt:publish(C, <<"t/1">>, <<>>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C, <<"t/2">>, <<>>, [{retain, true}, {qos, 1}]),
    ok = emqtt:disconnect(C).

subscription_option_retain_handling(Config) ->
    ClientId = ?FUNCTION_NAME,
    C1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _} = emqtt:publish(C1, <<"t/1">>, <<"m1">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C1, <<"t/2">>, <<"m2">>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C1, <<"t/3">>, <<"m3">>, [{retain, true}, {qos, 1}]),
    {ok, _, [1, 1, 1]} = emqtt:subscribe(C1, [{<<"t/1">>, [{rh, 0}, {qos, 1}]},
                                              %% Subscription does not exist.
                                              {<<"t/2">>, [{rh, 1}, {qos, 1}]},
                                              {<<"t/3">>, [{rh, 2}, {qos, 1}]}]),
    ok = expect_publishes(C1, <<"t/1">>, [<<"m1">>]),
    ok = expect_publishes(C1, <<"t/2">>, [<<"m2">>]),
    assert_nothing_received(),

    {ok, _, [1, 1, 1]} = emqtt:subscribe(C1, [{<<"t/1">>, [{rh, 0}, {qos, 1}]},
                                              %% Subscription exists.
                                              {<<"t/2">>, [{rh, 1}, {qos, 1}]},
                                              {<<"t/3">>, [{rh, 2}, {qos, 1}]}]),
    ok = expect_publishes(C1, <<"t/1">>, [<<"m1">>]),
    assert_nothing_received(),

    {ok, _, [0, 0, 0]} = emqtt:subscribe(C1, [{<<"t/1">>, [{rh, 0}, {qos, 0}]},
                                              %% That specific subscription does not exist.
                                              {<<"t/2">>, [{rh, 1}, {qos, 0}]},
                                              {<<"t/3">>, [{rh, 2}, {qos, 0}]}]),
    ok = expect_publishes(C1, <<"t/1">>, [<<"m1">>]),
    ok = expect_publishes(C1, <<"t/2">>, [<<"m2">>]),
    assert_nothing_received(),

    ok = emqtt:disconnect(C1),
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    {ok, _, [0, 0, 0]} = emqtt:subscribe(C2, [{<<"t/1">>, [{rh, 0}, {qos, 0}]},
                                              %% Subscription exists.
                                              {<<"t/2">>, [{rh, 1}, {qos, 0}]},
                                              {<<"t/3">>, [{rh, 2}, {qos, 0}]}]),
    ok = expect_publishes(C2, <<"t/1">>, [<<"m1">>]),
    assert_nothing_received(),

    {ok, _} = emqtt:publish(C2, <<"t/1">>, <<>>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C2, <<"t/2">>, <<>>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(C2, <<"t/3">>, <<>>, [{retain, true}, {qos, 1}]),
    ok = emqtt:disconnect(C2).

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
    after ?TIMEOUT -> ct:fail("did not receive m1")
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
    after ?TIMEOUT -> ct:fail("did not receive m2")
    end,
    receive {publish,
             #{client_pid := C2,
               topic := <<"t/3">>,
               payload := <<"m3">>,
               properties := #{'Subscription-Identifier' := 16#fffffff}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m3")
    end,
    receive {publish,
             #{client_pid := C2,
               topic := <<"t/2/xyz">>,
               payload := <<"m4">>,
               properties := Props}} ->
                ?assertNot(maps:is_key('Subscription-Identifier', Props))
    after ?TIMEOUT -> ct:fail("did not receive m4")
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
    after ?TIMEOUT -> ct:fail("did not receive message m1")
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
    after ?TIMEOUT -> ct:fail("did not receive message m2")
    end,
    receive {publish,
             #{client_pid := C2,
               topic := <<"a/b">>,
               payload := <<"m2">>,
               properties := #{'Subscription-Identifier' := 16#fffffff}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive message m2")
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
    after ?TIMEOUT -> ct:fail("did not receive msg")
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
    timer:sleep(20),
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
    [util:enable_plugin(Config, Plugin) || Plugin <- ?config(test_plugins, Config)],
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    ok = emqtt:publish(C2, <<"t1">>, <<"m1">>),
    ok = emqtt:publish(C2, <<"t2">>, <<"m2">>, [{retain, true}]),
    receive {publish,
             #{client_pid := C2,
               payload := <<"m2">>,
               retain := true,
               qos := 0,
               properties := #{'Subscription-Identifier' := 99}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m2")
    end,
    assert_nothing_received(),
    {ok, _} = emqtt:publish(C2, <<"t2">>, <<>>, [{retain, true}, {qos, 1}]),
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
    after ?TIMEOUT -> ct:fail("did not receive m2")
    end,

    %% modify QoS
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 1}, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"m3">>, qos1),
    receive {publish, #{payload := <<"m3">>,
                        qos := 1,
                        properties := #{'Subscription-Identifier' := 1}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m3")
    end,

    %% modify Subscription Identifier
    {ok, _, [1]} = emqtt:subscribe(C, #{'Subscription-Identifier' => 2}, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"m4">>, qos1),
    receive {publish, #{payload := <<"m4">>,
                        properties := #{'Subscription-Identifier' := 2}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m4")
    end,

    %% remove Subscription Identifier
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"m5">>, [{retain, true}, {qos, 1}]),
    receive {publish, #{payload := <<"m5">>,
                        retain := false,
                        properties := Props}} when map_size(Props) =:= 0 -> ok
    after ?TIMEOUT -> ct:fail("did not receive m5")
    end,

    %% modify Retain As Published
    {ok, _, [1]} = emqtt:subscribe(C, Topic, [{rap, true}, {qos, 1}]),
    receive {publish, #{payload := <<"m5">>,
                        retain := true}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive retained m5")
    end,
    {ok, _} = emqtt:publish(C, Topic, <<"m6">>, [{retain, true}, {qos, 1}]),
    receive {publish, #{payload := <<"m6">>,
                        retain := true}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m6")
    end,

    assert_nothing_received(),
    {ok, _} = emqtt:publish(C, Topic, <<>>, [{retain, true}, {qos, 1}]),
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
    after ?TIMEOUT -> ct:fail("did not receive 1")
    end,
    %% Replace subscription while another client is sending messages.
    {ok, _, [Qos]} = emqtt:subscribe(Sub, #{'Subscription-Identifier' => 1}, Topic, Qos),
    Sender ! stop,
    NumSent = receive {N, Sender} -> N
              after ?TIMEOUT -> ct:fail("could not stop publisher")
              end,
    ct:pal("Publisher sent ~b messages", [NumSent]),
    LastExpectedPayload = integer_to_binary(NumSent),
    receive {publish, #{payload := LastExpectedPayload,
                        qos := Qos,
                        client_pid := Sub,
                        properties := #{'Subscription-Identifier' := 1}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive ~s", [LastExpectedPayload])
    end,
    case Qos of
        0 ->
            assert_received_no_duplicates();
        1 ->
            ExpectedPayloads = [integer_to_binary(I) || I <- lists:seq(2, NumSent - 1)],
            ok = expect_publishes(Sub, Topic, ExpectedPayloads)
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
    Subv3 = connect(ClientId, Config,
                    [{proto_ver, v3},
                     {auto_ack, false}] ++
                    non_clean_sess_opts()),
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(Subv3))),
    {ok, _, [Qos]} = emqtt:subscribe(Subv3, Topic, Qos),
    Sender = spawn_link(?MODULE, send, [self(), Pub, Topic, 0]),
    receive {publish, #{payload := <<"1">>,
                        client_pid := Subv3,
                        packet_id := PacketId}} ->
                case Qos of
                    0 -> ok;
                    1 -> emqtt:puback(Subv3, PacketId)
                end
    after ?TIMEOUT -> ct:fail("did not receive 1")
    end,
    %% Upgrade session from v3 to v5 while another client is sending messages.
    ok = emqtt:disconnect(Subv3),
    Subv5 = connect(ClientId, Config, [{proto_ver, v5},
                                       {clean_start, false},
                                       {auto_ack, true}]),
    ?assertEqual(5, proplists:get_value(proto_ver, emqtt:info(Subv5))),
    Sender ! stop,
    NumSent = receive {N, Sender} -> N
              after ?TIMEOUT -> ct:fail("could not stop publisher")
              end,
    ct:pal("Publisher sent ~b messages", [NumSent]),
    LastExpectedPayload = integer_to_binary(NumSent),
    receive {publish, #{payload := LastExpectedPayload,
                        qos := Qos,
                        client_pid := Subv5}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive ~s", [LastExpectedPayload])
    end,
    case Qos of
        0 ->
            assert_received_no_duplicates();
        1 ->
            ExpectedPayloads = [integer_to_binary(I) || I <- lists:seq(2, NumSent - 1)],
            ok = expect_publishes(Subv5, Topic, ExpectedPayloads)
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
    assert_received_no_duplicates0(#{}, 30000).

assert_received_no_duplicates0(Received, Timeout) ->
    receive {publish, #{payload := P}} ->
                case maps:is_key(P, Received) of
                    true -> ct:fail("Received ~p twice", [P]);
                    false -> assert_received_no_duplicates0(maps:put(P, ok, Received), 500)
                end
    after Timeout ->
              %% Check that we received at least one message.
              ?assertNotEqual(0, maps:size(Received))
    end.

session_upgrade_v3_v5_amqp091_pub(Config) ->
    Payload = ClientId = Topic = atom_to_binary(?FUNCTION_NAME),
    Subv3 = connect(ClientId, Config, [{proto_ver, v3} | non_clean_sess_opts()]),
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(Subv3))),
    {ok, _, [1]} = emqtt:subscribe(Subv3, Topic, 1),
    ok = emqtt:disconnect(Subv3),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch,
                      #'basic.publish'{exchange = <<"amq.topic">>,
                                       routing_key = Topic},
                      #amqp_msg{payload = Payload,
                                props = #'P_basic'{delivery_mode = 2}}),

    Subv5 = connect(ClientId, Config, [{proto_ver, v5}, {clean_start, false}]),
    ?assertEqual(5, proplists:get_value(proto_ver, emqtt:info(Subv5))),
    receive {publish, #{payload := Payload,
                        qos := 1,
                        client_pid := Subv5}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive message")
    end,
    ok = emqtt:disconnect(Subv5),
    ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, 0).

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
    after ?TIMEOUT -> ct:fail("did not receive from v3")
    end,
    {ok, _} = emqtt:publish(Cv3, <<"v5">>, <<>>, [{retain, true}, {qos, 1}]),
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
    after ?TIMEOUT -> ct:fail("did not receive msg")
    end,
    assert_nothing_received(),
    ok = emqtt:disconnect(C).

publish_property_content_type(Config) ->
    Topic = ClientId = Payload = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    %% "The Content Type MUST be a UTF-8 Encoded String" [v5 3.3.2.3.9]
    {ok, _} = emqtt:publish(C, Topic, #{'Content-Type' => <<"text/plain😎;charset=UTF-8"/utf8>>}, Payload, [{qos, 1}]),
    receive {publish, #{payload := Payload,
                        properties := #{'Content-Type' := <<"text/plain😎;charset=UTF-8"/utf8>>}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive message")
    end,
    ok = emqtt:disconnect(C).

publish_property_payload_format_indicator(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, #{'Payload-Format-Indicator' => 0}, <<"m1">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(C, Topic, #{'Payload-Format-Indicator' => 1}, <<"m2">>, [{qos, 1}]),
    receive {publish, #{payload := <<"m1">>,
                        properties := #{'Payload-Format-Indicator' := 0}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m1")
    end,
    receive {publish, #{payload := <<"m2">>,
                        properties := #{'Payload-Format-Indicator' := 1}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m2")
    end,
    ok = emqtt:disconnect(C).

publish_property_response_topic_correlation_data(Config) ->
    %% "The Response Topic MUST be a UTF-8 Encoded String" [v5 3.3.2.3.5]
    Requester = connect(<<"requester">>, Config),
    FrenchResponder = connect(<<"French responder">>, Config),
    ItalianResponder = connect(<<"Italian responder">>, Config),
    ResponseTopic = <<"🗣️/response/for/English/request"/utf8>>,
    {ok, _, [0]} = emqtt:subscribe(Requester, ResponseTopic),
    {ok, _, [0]} = emqtt:subscribe(FrenchResponder, <<"greet/French">>),
    {ok, _, [0]} = emqtt:subscribe(ItalianResponder, <<"greet/Italian">>),
    CorrelationFrench = <<"French">>,
    %% "the length of Binary Data is limited to the range of 0 to 65,535 Bytes" [v5 1.5.6]
    %% Let's also test with large correlation data.
    CorrelationItalian = <<"Italian", (binary:copy(<<"x">>, 65_500))/binary>>,
    ok = emqtt:publish(Requester, <<"greet/French">>,
                       #{'Response-Topic' => ResponseTopic,
                         'Correlation-Data' => CorrelationFrench},
                       <<"Harry">>, [{qos, 0}]),
    ok = emqtt:publish(Requester, <<"greet/Italian">>,
                       #{'Response-Topic' => ResponseTopic,
                         'Correlation-Data' => CorrelationItalian},
                       <<"Harry">>, [{qos, 0}]),
    receive {publish, #{client_pid := FrenchResponder,
                        payload := <<"Harry">>,
                        properties := #{'Response-Topic' := ResponseTopic,
                                        'Correlation-Data' := Corr0}}} ->
                ok = emqtt:publish(FrenchResponder, ResponseTopic,
                                   #{'Correlation-Data' => Corr0},
                                   <<"Bonjour Henri">>, [{qos, 0}])
    after ?TIMEOUT -> ct:fail("French responder did not receive request")
    end,
    receive {publish, #{client_pid := ItalianResponder,
                        payload := <<"Harry">>,
                        properties := #{'Response-Topic' := ResponseTopic,
                                        'Correlation-Data' := Corr1}}} ->
                ok = emqtt:publish(ItalianResponder, ResponseTopic,
                                   #{'Correlation-Data' => Corr1},
                                   <<"Buongiorno Enrico">>, [{qos, 0}])
    after ?TIMEOUT -> ct:fail("Italian responder did not receive request")
    end,
    receive {publish, #{client_pid := Requester,
                        properties := #{'Correlation-Data' := CorrelationItalian},
                        payload := Payload0
                       }} ->
                ?assertEqual(<<"Buongiorno Enrico">>, Payload0)
    after ?TIMEOUT -> ct:fail("did not receive Italian response")
    end,
    receive {publish, #{client_pid := Requester,
                        properties := #{'Correlation-Data' := CorrelationFrench},
                        payload := Payload1
                       }} ->
                ?assertEqual(<<"Bonjour Henri">>, Payload1)
    after ?TIMEOUT -> ct:fail("did not receive French response")
    end,
    [ok = emqtt:disconnect(C) || C <- [Requester, FrenchResponder, ItalianResponder]].

publish_property_user_property(Config) ->
    Payload = Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    %% Same keys and values are allowed. Order must be maintained.
    UserProperty = [{<<"k1">>, <<"v2">>},
                    {<<"k1">>, <<"v2">>},
                    {<<"k1">>, <<"v1">>},
                    {<<"k0">>, <<"v0">>},
                    %% "UTF-8 encoded strings can have any length in the range 0 to 65,535 bytes"
                    %% [v5 1.5.4]
                    {<<>>, <<>>},
                    {<<(binary:copy(<<"k">>, 65_000))/binary, "🐇"/utf8>>,
                     <<(binary:copy(<<"v">>, 65_000))/binary, "🐇"/utf8>>}],
    {ok, _} = emqtt:publish(C, Topic, #{'User-Property' => UserProperty}, Payload, [{qos, 1}]),
    receive {publish, #{payload := Payload,
                        properties := #{'User-Property' := UserProperty}}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive message")
    end,
    ok = emqtt:disconnect(C).

disconnect_with_will(Config) ->
    Topic = Payload = ClientId = atom_to_binary(?FUNCTION_NAME),
    Sub = connect(<<"subscriber">>, Config),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic),
    C = connect(ClientId, Config, [{will_topic, Topic},
                                   {will_payload, Payload}]),
    ok = emqtt:disconnect(C, ?RC_DISCONNECT_WITH_WILL),
    ok = expect_publishes(Sub, Topic, [Payload]),
    ok = emqtt:disconnect(Sub).

will_qos2(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    Opts = [{will_topic, Topic},
            {will_payload, <<"msg">>},
            {will_qos, 2}],
    {C, Connect} = start_client(ClientId, Config, 0, Opts),
    unlink(C),
    ?assertEqual({error, {qos_not_supported, #{}}}, Connect(C)).

will_delay_less_than_session_expiry(Config) ->
    will_delay(1, 5, ?FUNCTION_NAME, Config).

will_delay_equals_session_expiry(Config) ->
    will_delay(1, 1, ?FUNCTION_NAME, Config).

will_delay_greater_than_session_expiry(Config) ->
    will_delay(5, 1, ?FUNCTION_NAME, Config).

%% "The Server delays publishing the Client’s Will Message until the Will Delay
%% Interval has passed or the Session ends, whichever happens first." [v5 3.1.3.2.2]
will_delay(WillDelay, SessionExpiry, ClientId, Config)
  when WillDelay =:= 1 orelse
       SessionExpiry =:= 1->
    Topic = <<"a/b">>,
    Msg = <<"msg">>,
    Opts = [{properties, #{'Session-Expiry-Interval' => SessionExpiry}},
            {will_props, #{'Will-Delay-Interval' => WillDelay}},
            {will_topic, Topic},
            {will_payload, Msg}],
    C1 = connect(ClientId, Config, Opts),
    Sub = connect(<<"subscriber">>, Config),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic),
    unlink(C1),
    erlang:exit(C1, trigger_will_message),
    receive TooEarly -> ct:fail(TooEarly)
    after 800 -> ok
    end,
    receive {publish, #{payload := Msg}} -> ok;
            Unexpected -> ct:fail({unexpected_message, Unexpected})
    after ?TIMEOUT -> ct:fail(will_message_timeout)
    end,
    %% Cleanup
    C2 = connect(ClientId, Config),
    ok = emqtt:disconnect(C2),
    ok = emqtt:disconnect(Sub).

will_delay_session_expiry_zero(Config) ->
    Topic = <<"a/b">>,
    Msg = <<"msg">>,
    Opts = [{will_props, #{'Will-Delay-Interval' => 1}},
            {will_topic, Topic},
            {will_payload, Msg}],
    C = connect(?FUNCTION_NAME, Config, Opts),
    Sub = connect(<<"subscriber">>, Config),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic),
    unlink(C),
    erlang:exit(C, trigger_will_message),
    %% Since default Session Expiry Interval is 0, we expect Will Message immediately.
    receive {publish, #{payload := Msg}} -> ok
    after ?TIMEOUT -> ct:fail(will_message_timeout)
    end,
    ok = emqtt:disconnect(Sub).

will_delay_reconnect_no_will(Config) ->
    Topic = <<"my/topic">>,
    ClientId = Payload = atom_to_binary(?FUNCTION_NAME),

    Sub = connect(<<"sub">>, Config),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic),

    Opts = [{properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}},
            {will_props, #{'Will-Delay-Interval' => 1}},
            {will_topic, Topic},
            {will_payload, Payload}],
    C1 = connect(ClientId, Config, Opts),
    unlink(C1),
    erlang:exit(C1, trigger_will_message),
    %% Should not receive anything because Will Delay is 1 second.
    assert_nothing_received(200),
    %% Reconnect with same ClientId, this time without a Will Message.
    C2 = connect(ClientId, Config, [{clean_start, false}]),
    %% Should not receive anything because client reconnected within Will Delay Interval.
    assert_nothing_received(1100),
    ok = emqtt:disconnect(C2, ?RC_DISCONNECT_WITH_WILL),
    %% Should not receive anything because new client did not set Will Message.
    assert_nothing_received(1100),
    ok = emqtt:disconnect(Sub).

will_delay_reconnect_with_will(Config) ->
    Topic = <<"my/topic">>,
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Sub = connect(<<"sub">>, Config),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic),
    C1 = connect(ClientId, Config,
                 [{properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}},
                  {will_props, #{'Will-Delay-Interval' => 1}},
                  {will_topic, Topic},
                  {will_payload, <<"will-1">>}]),
    unlink(C1),
    erlang:exit(C1, trigger_will_message),
    %% Should not receive anything because Will Delay is 1 second.
    assert_nothing_received(300),
    %% Reconnect with same ClientId, again with a delayed will message.
    C2 = connect(ClientId, Config,
                 [{clean_start, false},
                  {properties, #{'Session-Expiry-Interval' => 16#FFFFFFFF}},
                  {will_props, #{'Will-Delay-Interval' => 1}},
                  {will_topic, Topic},
                  {will_payload, <<"will-2">>}]),
    ok = emqtt:disconnect(C2, ?RC_DISCONNECT_WITH_WILL),
    %% The second will message should be sent after 1 second.
    assert_nothing_received(700),
    ok = expect_publishes(Sub, Topic, [<<"will-2">>]),
    %% The first will message should not be sent.
    assert_nothing_received(),
    %% Cleanup
    C3 = connect(ClientId, Config),
    ok = emqtt:disconnect(C3),
    ok = emqtt:disconnect(Sub).

%% "If a Network Connection uses a Client Identifier of an existing Network Connection to the Server,
%% the Will Message for the exiting connection is sent unless the new connection specifies Clean
%% Start of 0 and the Will Delay is greater than zero." [v5 3.1.3.2.2]
will_delay_session_takeover(Config) ->
    Topic = <<"my/topic">>,
    Sub = connect(<<"sub">>, Config),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic),

    C1a = connect(<<"c1">>, Config,
                  [{properties, #{'Session-Expiry-Interval' => 120}},
                   {will_props, #{'Will-Delay-Interval' => 30}},
                   {will_topic, Topic},
                   {will_payload, <<"will-1a">>}]),
    C2a = connect(<<"c2">>, Config,
                  [{will_topic, Topic},
                   {will_payload, <<"will-2a">>}]),
    C3a = connect(<<"c3">>, Config,
                  [{will_topic, Topic},
                   {will_payload, <<"will-3a">>}]),
    C4a = connect(<<"c4">>, Config,
                  [{will_topic, Topic},
                   {will_payload, <<"will-4a">>}]),
    Clients = [C1a, C2a, C3a, C4a],
    [true = unlink(C) || C <- Clients],
    C1b = connect(<<"c1">>, Config,
                  [{clean_start, false},
                   {properties, #{'Session-Expiry-Interval' => 120}},
                   {will_props, #{'Will-Delay-Interval' => 30}},
                   {will_topic, Topic},
                   {will_payload, <<"will-1b">>}]),
    C2b = connect(<<"c2">>, Config,
                  [{clean_start, false},
                   {properties, #{'Session-Expiry-Interval' => 120}},
                   {will_props, #{'Will-Delay-Interval' => 30}},
                   {will_topic, Topic},
                   {will_payload, <<"will-2b">>}]),
    C3b = connect(<<"c3">>, Config,
                  [{clean_start, true},
                   {properties, #{'Session-Expiry-Interval' => 120}},
                   {will_props, #{'Will-Delay-Interval' => 30}},
                   {will_topic, Topic},
                   {will_payload, <<"will-3b">>}]),
    C4b = connect(<<"c4">>, Config,
                  [{clean_start, false},
                   {properties, #{'Session-Expiry-Interval' => 0}},
                   {will_topic, Topic},
                   {will_payload, <<"will-4b">>}]),
    [receive {disconnected, ?RC_SESSION_TAKEN_OVER, #{}} -> ok
     after ?TIMEOUT -> ct:fail("server did not disconnect us")
     end || _ <- Clients],

    receive {publish, #{client_pid := Sub,
                        payload := <<"will-3a">>}} -> ok
    after ?TIMEOUT -> ct:fail({missing_msg, ?LINE})
    end,
    receive {publish, #{client_pid := Sub,
                        payload := <<"will-4a">>}} -> ok
    after ?TIMEOUT -> ct:fail({missing_msg, ?LINE})
    end,
    assert_nothing_received(),

    [ok = emqtt:disconnect(C) || C <- [Sub, C1b, C2b, C3b, C4b]].

will_delay_message_expiry(Config) ->
    Q1 = <<"dead-letter-queue-1">>,
    Q2 = <<"dead-letter-queue-2">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = Q1,
                                     arguments = [{<<"x-dead-letter-exchange">>, longstr, <<"">>},
                                                  {<<"x-dead-letter-routing-key">>, longstr, Q2}]}),
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue = Q1,
                                             exchange = <<"amq.topic">>,
                                             routing_key = <<"my.topic">>}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = Q2}),
    C = connect(<<"my-client">>, Config,
                [{properties, #{'Session-Expiry-Interval' => 1}},
                 {will_props, #{'Will-Delay-Interval' => 1,
                                'Message-Expiry-Interval' => 1}},
                 {will_topic, <<"my/topic">>},
                 {will_payload, <<"msg">>}]),
    ok = emqtt:disconnect(C, ?RC_DISCONNECT_WITH_WILL),
    %% After 1 second, Will Message is published, i.e. dead lettered the 1st time,
    %% after 2 seconds, Will Message is dead lettered the 2nd time:
    %% mqtt-will-my-client -> dead-letter-queue-1 -> dead-letter-queue-2
    %% Wait for 2 more seconds to validate that the Will Message does not expire again in dead-letter-queue-2.
    timer:sleep(4000),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q2})),
    [#'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}) || Q <- [Q1, Q2]].

%% "The PUBLISH packet sent to a Client by the Server MUST contain a Message Expiry Interval
%% set to the received value minus the time that the message has been waiting in the Server."
%% [v5 MQTT-3.3.2-6]
%% Test that requirement for a Will Message with both Will Delay Interval and Message Expiry Interval.
will_delay_message_expiry_publish_properties(Config) ->
    Topic = <<"my/topic">>,
    ClientId = Payload = atom_to_binary(?FUNCTION_NAME),
    Sub1 = connect(ClientId, Config, [{properties, #{'Session-Expiry-Interval' => 60}}]),
    {ok, _, [1]} = emqtt:subscribe(Sub1, Topic, qos1),
    ok = emqtt:disconnect(Sub1),
    C = connect(<<"will">>, Config,
                [{properties, #{'Session-Expiry-Interval' => 2}},
                 {will_props, #{'Will-Delay-Interval' => 2,
                                'Message-Expiry-Interval' => 20}},
                 {will_topic, Topic},
                 {will_qos, 1},
                 {will_payload, Payload}]),
    ok = emqtt:disconnect(C, ?RC_DISCONNECT_WITH_WILL),
    %% After 2 seconds, Will Message is published.
    %% Wait for 2 more seconds to check the Message Expiry Interval sent to the client
    %% is adjusted correctly.
    timer:sleep(4000),
    Sub2 = connect(ClientId, Config, [{clean_start, false}]),
    receive {publish, #{client_pid := Sub2,
                        topic := Topic,
                        payload := Payload,
                        properties := #{'Message-Expiry-Interval' := MEI}}} ->
                %% Since the point in time the Message was published,
                %% it has been waiting in the Server for 2 seconds to be consumed.
                assert_message_expiry_interval(20 - 2, MEI);
            Other -> ct:fail("received unexpected message: ~p", [Other])
    after ?TIMEOUT -> ct:fail("did not receive Will Message")
    end,
    ok = emqtt:disconnect(Sub2).

%% Test all Will Properties (v5 3.1.3.2) that are forwarded unaltered by the server.
will_properties(Config) ->
    will_properties0(Config, 0).

%% Test all Will Properties (v5 3.1.3.2) that are forwarded unaltered by the server
%% when Will Delay Interval is set.
will_delay_properties(Config) ->
    will_properties0(Config, 1).

will_properties0(Config, WillDelayInterval) ->
    Topic = Payload = atom_to_binary(?FUNCTION_NAME),
    Sub = connect(<<"sub">>, Config),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic),
    UserProperty = [{<<"k1">>, <<"v2">>},
                    {<<>>, <<>>},
                    {<<"k1">>, <<"v1">>}],
    CorrelationData = binary:copy(<<"x">>, 65_000),
    C = connect(<<"will">>, Config,
                [{properties, #{'Session-Expiry-Interval' => 1}},
                 {will_props, #{'Will-Delay-Interval' => WillDelayInterval,
                                'User-Property' => UserProperty,
                                'Content-Type' => <<"text/plain😎;charset=UTF-8"/utf8>>,
                                'Payload-Format-Indicator' => 1,
                                'Response-Topic' => <<"response/topic">>,
                                'Correlation-Data' => CorrelationData}},
                 {will_topic, Topic},
                 {will_qos, 0},
                 {will_payload, Payload}]),
    ok = emqtt:disconnect(C, ?RC_DISCONNECT_WITH_WILL),
    if WillDelayInterval > 0 ->
           receive Unexpected -> ct:fail(Unexpected)
           after 700 -> ok
           end;
       WillDelayInterval =:= 0 ->
           ok
    end,
    receive {publish,
             #{client_pid := Sub,
               topic := Topic,
               payload := Payload,
               properties := #{'User-Property' := UserProperty,
                               'Content-Type' := <<"text/plain😎;charset=UTF-8"/utf8>>,
                               'Payload-Format-Indicator' := 1,
                               'Response-Topic' := <<"response/topic">>,
                               'Correlation-Data' := CorrelationData} = Props}}
              when map_size(Props) =:= 5 -> ok
    after ?TIMEOUT -> ct:fail("did not receive Will Message")
    end,
    ok = emqtt:disconnect(Sub).

%% "When an Application Message is transported by MQTT it contains payload data,
%% a Quality of Service (QoS), a collection of Properties, and a Topic Name" [v5 1.2]
%% Since a retained message is an Application Message, it must also include the Properties.
%% This test checks that the whole Application Message, especially Properties, are forwarded
%% to future subscribers.
retain_properties(Config) ->
    Props = #{'Content-Type' => <<"text/plain;charset=UTF-8">>,
              'User-Property' => [{<<"k1">>, <<"v2">>},
                                  {<<>>, <<>>},
                                  {<<"k1">>, <<"v1">>}],
              'Payload-Format-Indicator' => 1,
              'Response-Topic' => <<"response/topic">>,
              'Correlation-Data' => <<"some correlation data">>},
    %% Let's test both ways to retain messages:
    %% 1. a Will Message that is retained, and
    %% 2. a PUBLISH message that is retained
    Pub = connect(<<"publisher">>, Config,
                  [{will_retain, true},
                   {will_topic, <<"t/1">>},
                   {will_payload, <<"m1">>},
                   {will_qos, 1},
                   {will_props, Props}]),
    {ok, _} = emqtt:publish(Pub, <<"t/2">>, Props, <<"m2">>, [{retain, true}, {qos, 1}]),
    ok = emqtt:disconnect(Pub, ?RC_DISCONNECT_WITH_WILL),
    %% Both messages are now retained.
    Sub = connect(<<"subscriber">>, Config),
    {ok, _, [1, 1]} = emqtt:subscribe(Sub, [{<<"t/1">>, qos1},
                                            {<<"t/2">>, qos1}]),
    receive {publish,
             #{client_pid := Sub,
               topic := <<"t/1">>,
               payload := <<"m1">>,
               retain := true,
               qos := 1,
               properties := Props}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m1")
    end,
    receive {publish,
             #{client_pid := Sub,
               topic := <<"t/2">>,
               payload := <<"m2">>,
               retain := true,
               qos := 1,
               properties := Props}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m2")
    end,
    {ok, _} = emqtt:publish(Sub, <<"t/1">>, <<>>, [{retain, true}, {qos, 1}]),
    {ok, _} = emqtt:publish(Sub, <<"t/2">>, <<>>, [{retain, true}, {qos, 1}]),
    ok = emqtt:disconnect(Sub).

%% "In the case of a Server shutdown or failure, the Server MAY defer publication of Will Messages
%% until a subsequent restart. If this happens, there might be a delay between the time the Server
%% experienced failure and when the Will Message is published." [v5 3.1.2.5]
%%
%% This test ensures that if a server is drained, shut down, the delayed Will Message expires,
%% and the server restarts, the delayed Will Message will still be published.
%% Publishing delayed Will Messages for unclean server shutdowns is currently not supported.
will_delay_node_restart(Config) ->
    Topic = <<"my/topic">>,
    Payload = <<"my-will">>,

    Sub0a = connect(<<"sub0">>, Config, 0, [{properties, #{'Session-Expiry-Interval' => 900}}]),
    {ok, _, [0]} = emqtt:subscribe(Sub0a, Topic),
    Sub1 = connect(<<"sub1">>, Config, 1, []),
    {ok, _, [0]} = emqtt:subscribe(Sub1, Topic),
    %% In mixed version mode with Khepri, draining the node can take 30 seconds.
    WillDelaySecs = 40,
    C0a = connect(<<"will">>, Config, 0,
                  [{properties, #{'Session-Expiry-Interval' => 900}},
                   {will_props, #{'Will-Delay-Interval' => WillDelaySecs}},
                   {will_topic, Topic},
                   {will_qos, 0},
                   {will_payload, Payload}]),
    ClientsNode0 = [Sub0a, C0a],
    [unlink(C) || C <- ClientsNode0],
    T = erlang:monotonic_time(millisecond),
    ok = rabbit_ct_broker_helpers:drain_node(Config, 0),
    [receive {disconnected, ?RC_SERVER_SHUTTING_DOWN, #{}} -> ok
     after ?TIMEOUT -> ct:fail("server did not disconnect us")
     end || _ <- ClientsNode0],
    ok = rabbit_ct_broker_helpers:stop_node(Config, 0),
    ElapsedMs = erlang:monotonic_time(millisecond) - T,
    SleepMs = max(0, timer:seconds(WillDelaySecs) - ElapsedMs),
    ct:pal("Sleeping for ~b ms waiting for Will Message to expire while node 0 is down...", [SleepMs]),
    timer:sleep(SleepMs),
    assert_nothing_received(),
    ok = rabbit_ct_broker_helpers:start_node(Config, 0),
    [util:enable_plugin(Config, Plugin) || Plugin <- ?config(test_plugins, Config)],
    %% After node 0 restarts, we should receive the Will Message promptly on both nodes 0 and 1.
    receive {publish, #{client_pid := Sub1,
                        payload := Payload}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive Will Message on node 1")
    end,
    Sub0b = connect(<<"sub0">>, Config, 0, [{clean_start, false}]),
    receive {publish, #{client_pid := Sub0b,
                        payload := Payload}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive Will Message on node 0")
    end,

    ok = emqtt:disconnect(Sub0b),
    ok = emqtt:disconnect(Sub1),
    C0b = connect(<<"will">>, Config),
    ok = emqtt:disconnect(C0b).

session_migrate_v3_v5(Config) ->
    session_switch_v3_v5(Config, true).

session_takeover_v3_v5(Config) ->
    session_switch_v3_v5(Config, false).

session_switch_v3_v5(Config, Disconnect) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    %% Connect to old node in mixed version cluster.
    C1 = connect(ClientId, Config, 1, [{proto_ver, v3} | non_clean_sess_opts()]),
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(C1))),
    {ok, _, [1]} = emqtt:subscribe(C1, Topic, qos1),
    case Disconnect of
        true -> ok = emqtt:disconnect(C1);
        false -> unlink(C1)
    end,

    %% Upgrade session from v3 to v5 (on new node in mixed version cluster).
    C2 = connect(ClientId, Config, 0, [{proto_ver, v5} | non_clean_sess_opts()]),
    ?assertEqual(5, proplists:get_value(proto_ver, emqtt:info(C2))),
    %% Subscription created with old v3 client should work.
    {ok, _} = emqtt:publish(C2, Topic, <<"m1">>, qos1),
    receive {publish,
             #{client_pid := C2,
               payload := <<"m1">>,
               qos := 1}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive from m1")
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
    end,

    %% Downgrade session from v5 to v3.
    C3 = connect(ClientId, Config, 0, [{proto_ver, v3} | non_clean_sess_opts()]),
    ?assertEqual(3, proplists:get_value(proto_ver, emqtt:info(C3))),
    case Disconnect of
        true -> ok;
        false -> receive {disconnected, ?RC_SESSION_TAKEN_OVER, #{}} -> ok
                 after ?TIMEOUT -> ct:fail("missing DISCONNECT packet for C2")
                 end
    end,
    %% We expect that v5 specific subscription feature does not apply
    %% anymore when downgrading the session.
    {ok, _} = emqtt:publish(C3, Topic, <<"m3">>, qos1),
    receive {publish,
             #{client_pid := C3,
               payload := <<"m3">>,
               qos := 1}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m3 with QoS 1")
    end,
    %% Modifying the subscription once more with v3 client should work.
    {ok, _, [0]} = emqtt:subscribe(C3, Topic, qos0),
    {ok, _} = emqtt:publish(C3, Topic, <<"m4">>, qos1),
    receive {publish,
             #{client_pid := C3,
               payload := <<"m4">>,
               qos := 0}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m3 with QoS 0")
    end,

    %% Unsubscribing in v3 should work.
    ?assertMatch({ok, _, _}, emqtt:unsubscribe(C3, Topic)),
    {ok, _} = emqtt:publish(C3, Topic, <<"m5">>, qos1),
    assert_nothing_received(),

    ok = emqtt:disconnect(C3),
    C4 = connect(ClientId, Config, 0, [{proto_ver, v3}, {clean_start, true}]),
    ok = emqtt:disconnect(C4),
    eventually(?_assertEqual([], all_connection_pids(Config))).

topic_alias_client_to_server(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Topic1 = <<"t/1">>,
    Sub = connect(<<ClientId/binary, "_sub">>, Config),
    QoS1 = [{qos, 1}],
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic1, QoS1),

    Pub = connect(<<ClientId/binary, "_pub">>, Config),
    {ok, _} = emqtt:publish(Pub, Topic1, #{'Topic-Alias' => 5}, <<"m1">>, QoS1),
    {ok, _} = emqtt:publish(Pub, <<>>, #{'Topic-Alias' => 5}, <<"m2">>, QoS1),
    {ok, _} = emqtt:publish(Pub, <<>>, #{'Topic-Alias' => 5}, <<"m3">>, QoS1),
    {ok, _} = emqtt:publish(Pub, Topic1, #{'Topic-Alias' => 7}, <<"m4">>, QoS1),
    {ok, _} = emqtt:publish(Pub, <<>>, #{'Topic-Alias' => 7}, <<"m5">>, QoS1),
    ok = expect_publishes(Sub, Topic1, [<<"m1">>, <<"m2">>, <<"m3">>, <<"m4">>, <<"m5">>]),

    Topic2 = <<"t/2">>,
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic2, [{qos, 0}]),
    {ok, _} = emqtt:publish(Pub, Topic2, #{'Topic-Alias' => 2}, <<"m6">>, QoS1),
    {ok, _} = emqtt:publish(Pub, <<>>, #{'Topic-Alias' => 2}, <<"m7">>, QoS1),
    {ok, _} = emqtt:publish(Pub, <<>>, #{'Topic-Alias' => 7}, <<"m8">>, QoS1),
    ok = expect_publishes(Sub, Topic2, [<<"m6">>, <<"m7">>]),
    ok = expect_publishes(Sub, Topic1, [<<"m8">>]),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

topic_alias_server_to_client(Config) ->
    Key = mqtt_topic_alias_maximum,
    DefaultMax = rpc(Config, persistent_term, get, [Key]),
    %% The Topic Alias Maximum configured on the server:
    %% 1. defines the Topic Alias Maximum for messages from client to server, and
    %% 2. serves as an upper bound for the Topic Alias Maximum for messages from server to client
    TopicAliasMaximum = 2,
    ok = rpc(Config, persistent_term, put, [Key, TopicAliasMaximum]),
    ClientId = ?FUNCTION_NAME,
    {C1, Connect} = start_client(ClientId, Config, 0, [{properties, #{'Topic-Alias-Maximum' => 16#ffff}}]),
    %% Validate 2.
    ?assertMatch({ok, #{'Topic-Alias-Maximum' := TopicAliasMaximum}}, Connect(C1)),

    {ok, _, [1]} = emqtt:subscribe(C1, <<"#">>, qos1),
    {ok, _} = emqtt:publish(C1, <<"t/1">>, <<"m1">>, qos1),
    A1 = receive {publish, #{payload := <<"m1">>,
                             topic := <<"t/1">>,
                             properties := #{'Topic-Alias' := A1a}}} -> A1a
         after ?TIMEOUT -> ct:fail("Did not receive m1")
         end,

    %% We don't expect a Topic Alias when the Topic Name consists of a single byte.
    {ok, _} = emqtt:publish(C1, <<"t">>, <<"m2">>, qos1),
    receive {publish, #{payload := <<"m2">>,
                        topic := <<"t">>,
                        properties := Props1}}
              when map_size(Props1) =:= 0 -> ok
    after ?TIMEOUT -> ct:fail("Did not receive m2")
    end,

    {ok, _} = emqtt:publish(C1, <<"t/2">>, <<"m3">>, qos1),
    A2 = receive {publish, #{payload := <<"m3">>,
                             topic := <<"t/2">>,
                             properties := #{'Topic-Alias' := A2a}}} -> A2a
         after ?TIMEOUT -> ct:fail("Did not receive m3")
         end,
    ?assertEqual([1, 2], lists:sort([A1, A2])),

    {ok, _} = emqtt:publish(C1, <<"t/3">>, <<"m4">>, qos1),
    %% In the current server implementation, once the Topic Alias cache is full,
    %% existing aliases won't be replaced. So, we expect to get the Topic Name instead.
    receive {publish, #{payload := <<"m4">>,
                        topic := <<"t/3">>,
                        properties := Props2}}
              when map_size(Props2) =:= 0 -> ok
    after ?TIMEOUT -> ct:fail("Did not receive m4")
    end,

    %% Existing topic aliases should still be sent.
    ok = emqtt:publish(C1, <<"t/1">>, <<"m5">>, qos0),
    ok = emqtt:publish(C1, <<"t/2">>, <<"m6">>, qos0),
    receive {publish, #{payload := <<"m5">>,
                        topic := <<>>,
                        properties := #{'Topic-Alias' := A1b}}} ->
                ?assertEqual(A1, A1b)
    after ?TIMEOUT -> ct:fail("Did not receive m5")
    end,
    receive {publish, #{payload := <<"m6">>,
                        topic := <<>>,
                        properties := #{'Topic-Alias' := A2b}}} ->
                ?assertEqual(A2, A2b)
    after ?TIMEOUT -> ct:fail("Did not receive m6")
    end,

    ok = emqtt:disconnect(C1),
    ok = rpc(Config, persistent_term, put, [Key, DefaultMax]).

%% "The Topic Alias mappings used by the Client and Server are independent from each other.
%% Thus, when a Client sends a PUBLISH containing a Topic Alias value of 1 to a Server and
%% the Server sends a PUBLISH with a Topic Alias value of 1 to that Client they will in
%% general be referring to different Topics." [v5 3.3.2.3.4]
topic_alias_bidirectional(Config) ->
    C1 = connect(<<"client 1">>, Config, [{properties, #{'Topic-Alias-Maximum' => 1}}]),
    C2 = connect(<<"client 2">>, Config),
    Topic1 = <<"/a/a">>,
    Topic2 = <<"/b/b">>,
    {ok, _, [0]} = emqtt:subscribe(C1, Topic1),
    {ok, _, [0]} = emqtt:subscribe(C2, Topic2),
    ok = emqtt:publish(C1, Topic2, #{'Topic-Alias' => 1}, <<"m1">>, [{qos, 0}]),
    ok = emqtt:publish(C2, Topic1, <<"m2">>),
    ok = emqtt:publish(C1, <<>>, #{'Topic-Alias' => 1}, <<"m3">>, [{qos, 0}]),
    ok = emqtt:publish(C2, Topic1, <<"m4">>),
    ok = expect_publishes(C2, Topic2, [<<"m1">>, <<"m3">>]),
    receive {publish, #{client_pid := C1,
                        payload := <<"m2">>,
                        topic := Topic1,
                        properties := #{'Topic-Alias' := 1}}} -> ok
    after ?TIMEOUT -> ct:fail("Did not receive m2")
    end,
    receive {publish, #{client_pid := C1,
                        payload := <<"m4">>,
                        topic := <<>>,
                        properties := #{'Topic-Alias' := 1}}} -> ok
    after ?TIMEOUT -> ct:fail("Did not receive m4")
    end,
    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2).

topic_alias_invalid(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    {C1, Connect} = start_client(ClientId, Config, 0, []),
    ?assertMatch({ok, #{'Topic-Alias-Maximum' := 16}}, Connect(C1)),
    process_flag(trap_exit, true),
    ?assertMatch({error, {disconnected, ?RC_TOPIC_ALIAS_INVALID, _}},
                 emqtt:publish(C1, Topic, #{'Topic-Alias' => 17}, <<"msg">>, [{qos, 1}])),

    C2 = connect(ClientId, Config),
    ?assertMatch({error, {disconnected, ?RC_TOPIC_ALIAS_INVALID, _}},
                 emqtt:publish(C2, Topic, #{'Topic-Alias' => 0}, <<"msg">>, [{qos, 1}])).

topic_alias_unknown(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    unlink(C),
    ?assertMatch({error, {disconnected, ?RC_PROTOCOL_ERROR, _}},
                 emqtt:publish(C, <<>>, #{'Topic-Alias' => 1}, <<"msg">>, [{qos, 1}])).

%% A RabbitMQ operator should be able to disallow topic aliases.
topic_alias_disallowed(Config) ->
    Key = mqtt_topic_alias_maximum,
    DefaultMax = rpc(Config, persistent_term, get, [Key]),
    TopicAliasMaximum = 0,
    ok = rpc(Config, persistent_term, put, [Key, TopicAliasMaximum]),

    {C, Connect} = start_client(?FUNCTION_NAME, Config, 0, [{properties, #{'Topic-Alias-Maximum' => 10}}]),
    ?assertMatch({ok, #{'Topic-Alias-Maximum' := 0}}, Connect(C)),
    unlink(C),
    ?assertMatch({error, {disconnected, ?RC_TOPIC_ALIAS_INVALID, _}},
                 emqtt:publish(C, <<"t">>, #{'Topic-Alias' => 1}, <<"msg">>, [{qos, 1}])),

    ok = rpc(Config, persistent_term, put, [Key, DefaultMax]).

topic_alias_retained_message(Config) ->
    topic_alias_in_retained_message0(Config, 1, 10, #{'Topic-Alias' => 1}).

topic_alias_disallowed_retained_message(Config) ->
    topic_alias_in_retained_message0(Config, 0, 1, #{}).

topic_alias_in_retained_message0(Config, TopicAliasMax, TopicAlias, ExpectedProps) ->
    Payload = Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config, [{properties, #{'Topic-Alias-Maximum' => TopicAliasMax}}]),
    {ok, _} = emqtt:publish(C, Topic, #{'Topic-Alias' => TopicAlias}, Payload, [{retain, true}, {qos, 1}]),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, [{qos, 1}]),
    receive {publish, #{payload := Payload,
                        topic := Topic,
                        retain := true,
                        properties := Props}} ->
                ?assertEqual(ExpectedProps, Props)
    after ?TIMEOUT -> ct:fail("Did not receive retained message")
    end,
    ok = emqtt:disconnect(C).

extended_auth(Config) ->
    {C, Connect} = start_client(?FUNCTION_NAME, Config, 0,
                                [{properties, #{'Authentication-Method' => <<"OTP">>,
                                                'Authentication-Data' => <<"123456">>}}]),
    unlink(C),
    ?assertEqual({error, {bad_authentication_method, #{}}}, Connect(C)).

%% Binding a headers exchange to the MQTT topic exchange should support
%% routing based on (topic and) User Property in the PUBLISH packet.
headers_exchange(Config) ->
    HeadersX = <<"my-headers-exchange">>,
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Qs = [Q1, Q2],
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Ch, #'exchange.declare'{exchange = HeadersX,
                                                         type = <<"headers">>,
                                                         durable = true,
                                                         auto_delete = true}),
    #'exchange.bind_ok'{} = amqp_channel:call(
                              Ch, #'exchange.bind'{destination = HeadersX,
                                                   source = <<"amq.topic">>,
                                                   routing_key = <<"my.topic">>}),
    [#'queue.declare_ok'{} = amqp_channel:call(
                               Ch, #'queue.declare'{queue = Q,
                                                    durable = true}) || Q <- Qs],
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue = Q1,
                                             exchange = HeadersX,
                                             arguments = [{<<"x-match">>, longstr, <<"any">>},
                                                          {<<"k1">>, longstr, <<"v1">>},
                                                          {<<"k2">>, longstr, <<"v2">>}]
                                            }),
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue = Q2,
                                             exchange = HeadersX,
                                             arguments = [{<<"x-match">>, longstr, <<"all-with-x">>},
                                                          {<<"k1">>, longstr, <<"v1">>},
                                                          {<<"k2">>, longstr, <<"v2">>},
                                                          {<<"x-k3">>, longstr, <<"🐇"/utf8>>}]
                                            }),
    C = connect(?FUNCTION_NAME, Config),
    Topic = <<"my/topic">>,
    {ok, _} = emqtt:publish(
                C, Topic,
                #{'User-Property' => [{<<"k1">>, <<"v1">>},
                                      {<<"k2">>, <<"v2">>},
                                      {<<"x-k3">>, unicode:characters_to_binary("🐇")}
                                     ]},
                <<"m1">>, [{qos, 1}]),
    [?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m1">>}},
                  amqp_channel:call(Ch, #'basic.get'{queue = Q})) || Q <- Qs],

    ok = emqtt:publish(C, Topic, <<"m2">>),
    [?assertMatch(#'basic.get_empty'{},
                  amqp_channel:call(Ch, #'basic.get'{queue = Q})) || Q <- Qs],

    {ok, _} = emqtt:publish(
                C, Topic,
                #{'User-Property' => [{<<"k1">>, <<"nope">>}]},
                <<"m3">>, [{qos, 1}]),
    [?assertMatch(#'basic.get_empty'{},
                  amqp_channel:call(Ch, #'basic.get'{queue = Q})) || Q <- Qs],

    {ok, _} = emqtt:publish(
                C, Topic,
                #{'User-Property' => [{<<"k2">>, <<"v2">>}]},
                <<"m4">>, [{qos, 1}]),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m4">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q1})),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Q2})),

    ok = emqtt:disconnect(C),
    [#'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}) || Q <- Qs],
    ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, 0).

%% Binding a consistent hash exchange to the MQTT topic exchange should support
%% consistent routing based on Correlation-Data in the PUBLISH packet.
consistent_hash_exchange(Config) ->
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_consistent_hash_exchange),
    HashX = <<"my-consistent-hash-exchange">>,
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Qs = [Q1, Q2],
    Ch = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Ch, #'exchange.declare'{
                                        exchange = HashX,
                                        type = <<"x-consistent-hash">>,
                                        arguments = [{<<"hash-property">>, longstr, <<"correlation_id">>}],
                                        durable = true,
                                        auto_delete = true}),
    #'exchange.bind_ok'{} = amqp_channel:call(
                              Ch, #'exchange.bind'{destination = HashX,
                                                   source = <<"amq.topic">>,
                                                   routing_key = <<"a.*">>}),
    [#'queue.declare_ok'{} = amqp_channel:call(
                               Ch, #'queue.declare'{queue = Q,
                                                    durable = true}) || Q <- Qs],
    [#'queue.bind_ok'{} = amqp_channel:call(
                            Ch, #'queue.bind'{queue = Q,
                                              exchange = HashX,
                                              %% weight
                                              routing_key = <<"1">>}) || Q <- Qs],

    Rands = [integer_to_binary(rand:uniform(1000)) || _ <- lists:seq(1, 30)],
    UniqRands = lists:uniq(Rands),
    NumMsgs = 150,
    C = connect(?FUNCTION_NAME, Config),
    [begin
         N = integer_to_binary(rand:uniform(1_000_000)),
         Topic = <<"a/", N/binary>>,
         {ok, _} =  emqtt:publish(C, Topic,
                                  #{'Correlation-Data' => lists:nth(rand:uniform(length(UniqRands)), UniqRands)},
                                  N, [{qos, 1}])
     end || _ <- lists:seq(1, NumMsgs)],

    #'basic.consume_ok'{consumer_tag = Ctag1} = amqp_channel:subscribe(
                                                  Ch, #'basic.consume'{queue = Q1,
                                                                       no_ack = true}, self()),
    #'basic.consume_ok'{consumer_tag = Ctag2} = amqp_channel:subscribe(
                                                  Ch, #'basic.consume'{queue = Q2,
                                                                       no_ack = true}, self()),
    {N1, Corrs1} = receive_correlations(Ctag1, 0, sets:new([{version, 2}])),
    {N2, Corrs2} = receive_correlations(Ctag2, 0, sets:new([{version, 2}])),
    ct:pal("q1: ~b messages, ~b unique correlation-data", [N1, sets:size(Corrs1)]),
    ct:pal("q2: ~b messages, ~b unique correlation-data", [N2, sets:size(Corrs2)]),
    %% All messages should be routed.
    ?assertEqual(NumMsgs, N1 + N2),
    %% Each of the 2 queues should have received at least 1 message.
    ?assert(sets:size(Corrs1) > 0),
    ?assert(sets:size(Corrs2) > 0),
    %% Assert that the consistent hash exchange routed the given Correlation-Data consistently.
    %% The same Correlation-Data should never be present in both queues.
    Intersection = sets:intersection(Corrs1, Corrs2),
    ?assert(sets:is_empty(Intersection)),

    ok = emqtt:disconnect(C),
    [#'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}) || Q <- Qs],
    ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, 0).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

receive_correlations(Ctag, N, Set) ->
    receive {#'basic.deliver'{consumer_tag = Ctag},
             #amqp_msg{props = #'P_basic'{correlation_id = Corr}}} ->
                ?assert(is_binary(Corr)),
                receive_correlations(Ctag, N + 1, sets:add_element(Corr, Set))
    after 1000 ->
              {N, Set}
    end.

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
    assert_nothing_received(500).

assert_nothing_received(Timeout) ->
    receive Unexpected -> ct:fail("Received unexpected message: ~p", [Unexpected])
    after Timeout -> ok
    end.
