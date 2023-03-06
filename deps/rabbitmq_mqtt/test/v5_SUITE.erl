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

-import(util,
        [
         start_client/4,
         connect/2, connect/3, connect/4,
         assert_message_expiry_interval/2
        ]).

all() ->
    [{group, mqtt},
     {group, web_mqtt}].

groups() ->
    [
     {mqtt, [],
      [{cluster_size_1, [shuffle], cluster_size_1_tests()},
       {cluster_size_3, [shuffle], cluster_size_3_tests()}]},
     {web_mqtt, [],
      [{cluster_size_1, [shuffle], cluster_size_1_tests()},
       {cluster_size_3, [shuffle], cluster_size_3_tests()}]}
    ].

cluster_size_1_tests() ->
    [
     client_set_max_packet_size_publish,
     client_set_max_packet_size_connack,
     client_set_max_packet_size_invalid,
     message_expiry_interval,
     message_expiry_interval_will_message,
     message_expiry_interval_retained_message,
     client_publish_qos2,
     client_rejects_publish,
     will_qos2
    ].

cluster_size_3_tests() ->
    [
     satisfy_bazel
    ].

suite() ->
    [{timetrap, {minutes, 1}}].

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
                {rabbit, [{classic_queue_default_version, 2}]}),
    Config = rabbit_ct_helpers:run_steps(
               Config2,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    util:maybe_skip_v5(Config).

end_per_group(G, Config)
  when G =:= cluster_size_1;
       G =:= cluster_size_3 ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

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
    Sub1 = connect(ClientId, Config, [{clean_start, false}]),
    {ok, _, [1]} = emqtt:subscribe(Sub1, Topic, qos1),
    ok = emqtt:disconnect(Sub1),

    {ok, _} = emqtt:publish(Pub, Topic, #{'Message-Expiry-Interval' => 1}, <<"m1">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(Pub, Topic, #{}, <<"m2">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(Pub, Topic, #{'Message-Expiry-Interval' => 10}, <<"m3">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(Pub, Topic, #{'Message-Expiry-Interval' => 2}, <<"m4">>, [{qos, 1}]),
    timer:sleep(2001),
    Sub2 = connect(ClientId, Config, [{clean_start, false}]),
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

    Sub1 = connect(ClientId, Config, [{clean_start, false}]),
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

client_publish_qos2(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    {C, Connect} = start_client(ClientId, Config, 0, []),
    ?assertMatch({ok, #{'Maximum-QoS' := 1}}, Connect(C)),
    ?assertEqual({error, {disconnected, _RcQosNotSupported = 155, #{}}},
                 emqtt:publish(C, Topic, <<"msg">>, [{qos, 2}])).

client_rejects_publish(Config) ->
    NumRejectedBefore = dead_letter_metric(messages_dead_lettered_rejected_total, Config),
    Payload = Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config, [{auto_ack, false}]),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, Payload, [{qos, 1}]),
    receive {publish, #{payload := Payload,
                        packet_id := PacketId}} ->
                %% Negatively ack the PUBLISH.
                emqtt:puback(C, PacketId, _UnspecifiedError = 16#80)
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

satisfy_bazel(_Config) ->
    ok.

dead_letter_metric(Metric, Config) ->
    Counters = rabbit_ct_broker_helpers:rpc(Config, rabbit_global_counters, overview, []),
    Map = maps:get([{queue_type, rabbit_classic_queue}, {dead_letter_strategy, disabled}], Counters),
    maps:get(Metric, Map).

assert_nothing_received() ->
    receive Unexpected -> ct:fail("Received unexpected message: ~p", [Unexpected])
    after 500 -> ok
    end.
