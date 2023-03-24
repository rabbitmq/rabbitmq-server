%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

%% Test suite shared between rabbitmq_mqtt and rabbitmq_web_mqtt.
-module(shared_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_ct_broker_helpers,
        [rabbitmqctl_list/3,
         rpc/4,
         rpc/5,
         rpc_all/4,
         get_node_config/3,
         drain_node/2,
         revive_node/2,
         is_feature_flag_enabled/2
        ]).
-import(rabbit_ct_helpers,
        [eventually/3,
         eventually/1]).
-import(util,
        [all_connection_pids/1,
         get_global_counters/2, get_global_counters/3, get_global_counters/4,
         expect_publishes/3,
         connect/2, connect/3, connect/4,
         get_events/1, assert_event_type/2, assert_event_prop/2,
         await_exit/1, await_exit/2,
         publish_qos1_timeout/4]).
-import(rabbit_mgmt_test_util,
        [http_get/2,
         http_delete/3]).

all() ->
    [
     {group, mqtt}
     ,{group, web_mqtt}
    ].

groups() ->
    [
     {mqtt, [], subgroups()}
     ,{web_mqtt, [], subgroups()}
    ].

subgroups() ->
    [
     {cluster_size_1, [],
      [
       {global_counters, [],
        [
         global_counters_v3,
         global_counters_v4
        ]},
       {tests, [],
        [
         block_only_publisher
         ,many_qos1_messages
         ,subscription_ttl
         ,management_plugin_connection
         ,management_plugin_enable
         ,disconnect
         ,pubsub_shared_connection
         ,pubsub_separate_connections
         ,will_with_disconnect
         ,will_without_disconnect
         ,quorum_queue_rejects
         ,events
         ,internal_event_handler
         ,non_clean_sess_reconnect_qos1
         ,non_clean_sess_reconnect_qos0
         ,non_clean_sess_reconnect_qos0_and_qos1
         ,non_clean_sess_empty_client_id
         ,subscribe_same_topic_same_qos
         ,subscribe_same_topic_different_qos
         ,subscribe_multiple
         ,large_message_mqtt_to_mqtt
         ,large_message_amqp_to_mqtt
         ,keepalive
         ,keepalive_turned_off
         ,duplicate_client_id
         ,block
         ,amqp_to_mqtt_qos0
         ,clean_session_disconnect_client
         ,clean_session_node_restart
         ,clean_session_node_kill
         ,rabbit_status_connection_count
         ,trace
         ,max_packet_size_unauthenticated
         ,default_queue_type
        ]}
      ]},
     {cluster_size_3, [],
      [
       queue_down_qos1,
       consuming_classic_mirrored_queue_down,
       consuming_classic_queue_down,
       flow_classic_mirrored_queue,
       flow_quorum_queue,
       flow_stream,
       rabbit_mqtt_qos0_queue,
       cli_list_queues,
       maintenance,
       delete_create_queue,
       publish_to_all_queue_types_qos0,
       publish_to_all_queue_types_qos1
      ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

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

init_per_group(cluster_size_1, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 1}]);
init_per_group(cluster_size_3 = Group, Config) ->
    init_per_group0(Group,
                    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]));
init_per_group(Group, Config)
  when Group =:= global_counters orelse
       Group =:= tests ->
    init_per_group0(Group, Config).

init_per_group0(Group, Config0) ->
    Suffix = lists:flatten(io_lib:format("~s_websocket_~w", [Group, ?config(websocket, Config0)])),
    Config1 = rabbit_ct_helpers:set_config(
                Config0,
                [{rmq_nodename_suffix, Suffix},
                 {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                                        tcp_port_mqtt_tls_extra]}]),
    Config = rabbit_ct_helpers:merge_app_env(
               Config1,
               {rabbit, [{classic_queue_default_version, 2}]}),
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(G, Config)
  when G =:= mqtt;
       G =:= web_mqtt;
       G =:= cluster_size_1 ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(T, Config)
  when T =:= management_plugin_connection;
       T =:= management_plugin_enable ->
    ok = inets:start(),
    init_per_testcase0(T, Config);
init_per_testcase(Testcase, Config) ->
    init_per_testcase0(Testcase, Config).

init_per_testcase0(Testcase, Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ok = rabbit_ct_broker_helpers:enable_plugin(Config, N, rabbitmq_web_mqtt) || N <- Nodes],
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(T, Config)
  when T =:= management_plugin_connection;
       T =:= management_plugin_enable ->
    ok = inets:stop(),
    end_per_testcase0(T, Config);
end_per_testcase(Testcase, Config) ->
    end_per_testcase0(Testcase, Config).

end_per_testcase0(Testcase, Config) ->
    rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

disconnect(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    eventually(?_assertEqual(1, length(all_connection_pids(Config)))),
    process_flag(trap_exit, true),
    ok = emqtt:disconnect(C),
    await_exit(C, normal),
    eventually(?_assertEqual([], all_connection_pids(Config))),
    ok.

pubsub_shared_connection(Config) ->
    C = connect(?FUNCTION_NAME, Config),

    Topic = <<"/topic/test-mqtt">>,
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),

    Payload = <<"a\x00a">>,
    ?assertMatch({ok, #{packet_id := _,
                        reason_code := 0,
                        reason_code_name := success
                       }},
                 emqtt:publish(C, Topic, Payload, [{qos, 1}])),
    ok = expect_publishes(C, Topic, [Payload]),
    ok = emqtt:disconnect(C).

pubsub_separate_connections(Config) ->
    Pub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>, Config),
    Sub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_subscriber">>, Config),

    Topic = <<"/topic/test-mqtt">>,
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, qos1),

    Payload = <<"a\x00a">>,
    ?assertMatch({ok, #{packet_id := _,
                        reason_code := 0,
                        reason_code_name := success
                       }},
                 emqtt:publish(Pub, Topic, Payload, [{qos, 1}])),
    ok = expect_publishes(Sub, Topic, [Payload]),
    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(Sub).

will_with_disconnect(Config) ->
    LastWillTopic = <<"/topic/last-will">>,
    LastWillMsg = <<"last will message">>,
    Opts = [{will_topic, LastWillTopic},
            {will_payload, LastWillMsg},
            {will_qos, 1}],
    Pub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>, Config, Opts),
    Sub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_subscriber">>, Config),
    {ok, _, [1]} = emqtt:subscribe(Sub, LastWillTopic, qos1),

    %% Client sends DISCONNECT packet. Therefore, will message should not be sent.
    ok = emqtt:disconnect(Pub),
    ?assertEqual({publish_not_received, LastWillMsg},
                 expect_publishes(Sub, LastWillTopic, [LastWillMsg])),

    ok = emqtt:disconnect(Sub).

will_without_disconnect(Config) ->
    LastWillTopic = <<"/topic/last-will">>,
    LastWillMsg = <<"last will message">>,
    Opts = [{will_topic, LastWillTopic},
            {will_payload, LastWillMsg},
            {will_qos, 1}],
    Pub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>, Config, Opts),
    timer:sleep(100),
    [ServerPublisherPid] = all_connection_pids(Config),
    Sub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_subscriber">>, Config),
    {ok, _, [1]} = emqtt:subscribe(Sub, LastWillTopic, qos1),

    %% Client does not send DISCONNECT packet. Therefore, will message should be sent.
    unlink(Pub),
    erlang:exit(ServerPublisherPid, test_will),
    ?assertEqual(ok, expect_publishes(Sub, LastWillTopic, [LastWillMsg])),

    ok = emqtt:disconnect(Sub).

quorum_queue_rejects(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Name = atom_to_binary(?FUNCTION_NAME),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"qq-policy">>, Name, <<"queues">>, [{<<"max-length">>, 1},
                                                            {<<"overflow">>, <<"reject-publish">>}]),
    declare_queue(Ch, Name, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    bind(Ch, Name, Name),

    C = connect(Name, Config, [{retry_interval, 1}]),
    {ok, _} = emqtt:publish(C, Name, <<"m1">>, qos1),
    {ok, _} = emqtt:publish(C, Name, <<"m2">>, qos1),
    %% We expect m3 to be rejected and dropped.
    ?assertEqual(puback_timeout, util:publish_qos1_timeout(C, Name, <<"m3">>, 700)),

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Name, no_ack = true})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Name, no_ack = true})),
    %% m3 is re-sent by emqtt.
    ?awaitMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m3">>}},
                amqp_channel:call(Ch, #'basic.get'{queue = Name, no_ack = true}),
                2000, 200),

    ok = emqtt:disconnect(C),
    delete_queue(Ch, Name),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"qq-policy">>).

publish_to_all_queue_types_qos0(Config) ->
    publish_to_all_queue_types(Config, qos0).

publish_to_all_queue_types_qos1(Config) ->
    publish_to_all_queue_types(Config, qos1).

publish_to_all_queue_types(Config, QoS) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),

    CQ = <<"classic-queue">>,
    CMQ = <<"classic-mirrored-queue">>,
    QQ = <<"quorum-queue">>,
    SQ = <<"stream-queue">>,
    Topic = <<"mytopic">>,

    declare_queue(Ch, CQ, []),
    bind(Ch, CQ, Topic),

    ok = rabbit_ct_broker_helpers:set_ha_policy(Config, 0, CMQ, <<"all">>),
    declare_queue(Ch, CMQ, []),
    bind(Ch, CMQ, Topic),

    declare_queue(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    bind(Ch, QQ, Topic),

    declare_queue(Ch, SQ, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    bind(Ch, SQ, Topic),

    NumMsgs = 2000,
    C = connect(?FUNCTION_NAME, Config, [{retry_interval, 2}]),
    lists:foreach(fun(N) ->
                          case emqtt:publish(C, Topic, integer_to_binary(N), QoS) of
                              ok ->
                                  ok;
                              {ok, _} ->
                                  ok;
                              Other ->
                                  ct:fail("Failed to publish: ~p", [Other])
                          end
                  end, lists:seq(1, NumMsgs)),

    eventually(?_assert(
                  begin
                      L = rabbitmqctl_list(Config, 0, ["list_queues", "messages", "--no-table-headers"]),
                      length(L) =:= 4 andalso
                      lists:all(fun([Bin]) ->
                                        N = binary_to_integer(Bin),
                                        case QoS of
                                            qos0 ->
                                                N =:= NumMsgs;
                                            qos1 ->
                                                %% Allow for some duplicates when client resends
                                                %% a message that gets acked at roughly the same time.
                                                N >= NumMsgs andalso
                                                N < NumMsgs * 2
                                        end
                                end, L)
                  end), 2000, 10),

    delete_queue(Ch, [CQ, CMQ, QQ, SQ]),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, CMQ),
    ok = emqtt:disconnect(C),
    ?awaitMatch([],
                all_connection_pids(Config), 10_000, 1000).

flow_classic_mirrored_queue(Config) ->
    QueueName = <<"flow">>,
    ok = rabbit_ct_broker_helpers:set_ha_policy(Config, 0, QueueName, <<"all">>),
    flow(Config, {rabbit, credit_flow_default_credit, {2, 1}}, <<"classic">>),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, QueueName).

flow_quorum_queue(Config) ->
    flow(Config, {rabbit, quorum_commands_soft_limit, 1}, <<"quorum">>).

flow_stream(Config) ->
    flow(Config, {rabbit, stream_messages_soft_limit, 1}, <<"stream">>).

flow(Config, {App, Par, Val}, QueueType)
  when is_binary(QueueType) ->
    {ok, DefaultVal} = rpc(Config, application, get_env, [App, Par]),
    Result = rpc_all(Config, application, set_env, [App, Par, Val]),
    ?assert(lists:all(fun(R) -> R =:= ok end, Result)),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QueueName = Topic = atom_to_binary(?FUNCTION_NAME),
    declare_queue(Ch, QueueName, [{<<"x-queue-type">>, longstr, QueueType}]),
    bind(Ch, QueueName, Topic),

    NumMsgs = 1000,
    C = connect(?FUNCTION_NAME, Config, [{retry_interval, 600},
                                         {max_inflight, NumMsgs}]),
    TestPid = self(),
    lists:foreach(
      fun(N) ->
              %% Publish async all messages at once to trigger flow control
              ok = emqtt:publish_async(C, Topic, integer_to_binary(N), qos1,
                                       {fun(N0, {ok, #{reason_code_name := success}}) ->
                                                TestPid ! {self(), N0}
                                        end, [N]})
      end, lists:seq(1, NumMsgs)),
    ok = await_confirms_ordered(C, 1, NumMsgs),
    eventually(?_assertEqual(
                  [[integer_to_binary(NumMsgs)]],
                  rabbitmqctl_list(Config, 0, ["list_queues", "messages", "--no-table-headers"])
                 ), 1000, 10),

    delete_queue(Ch, QueueName),
    ok = emqtt:disconnect(C),
    ?awaitMatch([],
                all_connection_pids(Config), 10_000, 1000),
    Result = rpc_all(Config, application, set_env, [App, Par, DefaultVal]),
    ok.

events(Config) ->
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, event_recorder),
    Server = get_node_config(Config, 0, nodename),
    ok = gen_event:add_handler({rabbit_event, Server}, event_recorder, []),

    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),

    [E0, E1] = get_events(Server),
    assert_event_type(user_authentication_success, E0),
    assert_event_prop([{name, <<"guest">>},
                       {connection_type, network}],
                      E0),
    assert_event_type(connection_created, E1),
    [ConnectionPid] = all_connection_pids(Config),
    Proto = case ?config(websocket, Config) of
                true -> 'Web MQTT';
                false -> 'MQTT'
            end,
    ExpectedConnectionProps = [{protocol, {Proto, {3,1,1}}},
                               {node, Server},
                               {vhost, <<"/">>},
                               {user, <<"guest">>},
                               {pid, ConnectionPid}],
    assert_event_prop(ExpectedConnectionProps, E1),

    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>, qos0),

    QueueNameBin = <<"mqtt-subscription-", ClientId/binary, "qos0">>,
    QueueName = {resource, <<"/">>, queue, QueueNameBin},
    [E2, E3 | E4] = get_events(Server),
    QueueType = case is_feature_flag_enabled(Config, rabbit_mqtt_qos0_queue) of
                    true ->
                        ?assertEqual([], E4),
                        rabbit_mqtt_qos0_queue;
                    false ->
                        [ConsumerCreated] = E4,
                        assert_event_type(consumer_created, ConsumerCreated),
                        assert_event_prop([{queue, QueueName},
                                           {ack_required, false},
                                           {exclusive, false},
                                           {arguments, []}],
                                          ConsumerCreated),
                        classic
                end,
    assert_event_type(queue_created, E2),
    assert_event_prop([{name, QueueName},
                       {durable, true},
                       {auto_delete, false},
                       {exclusive, true},
                       {type, QueueType},
                       {arguments, []}],
                      E2),
    assert_event_type(binding_created, E3),
    assert_event_prop([{source_name, <<"amq.topic">>},
                       {source_kind, exchange},
                       {destination_name, QueueNameBin},
                       {destination_kind, queue},
                       {routing_key, <<"TopicA">>},
                       {arguments, []}],
                      E3),

    {ok, _, _} = emqtt:unsubscribe(C, <<"TopicA">>),

    [E5] = get_events(Server),
    assert_event_type(binding_deleted, E5),

    ok = emqtt:disconnect(C),

    [E6, E7 | E8] = get_events(Server),
    assert_event_type(connection_closed, E6),
    assert_event_prop(ExpectedConnectionProps, E6),
    case is_feature_flag_enabled(Config, rabbit_mqtt_qos0_queue) of
        true ->
            assert_event_type(queue_deleted, E7),
            assert_event_prop({name, QueueName}, E7);
        false ->
            assert_event_type(consumer_deleted, E7),
            assert_event_prop({queue, QueueName}, E7),
            [QueueDeleted] = E8,
            assert_event_type(queue_deleted, QueueDeleted),
            assert_event_prop({name, QueueName}, QueueDeleted)
    end,

    ok = gen_event:delete_handler({rabbit_event, Server}, event_recorder, []).

internal_event_handler(Config) ->
    Server = get_node_config(Config, 0, nodename),
    ok = gen_event:call({rabbit_event, Server}, rabbit_mqtt_internal_event_handler, ignored_request, 1000).

global_counters_v3(Config) ->
    global_counters(Config, v3).

global_counters_v4(Config) ->
    global_counters(Config, v4).

global_counters(Config, ProtoVer) ->
    C = connect(?FUNCTION_NAME, Config, [{proto_ver, ProtoVer}]),

    Topic0 = <<"test-topic0">>,
    Topic1 = <<"test-topic1">>,
    Topic2 = <<"test-topic2">>,
    {ok, _, [0]} = emqtt:subscribe(C, Topic0, qos0),
    {ok, _, [1]} = emqtt:subscribe(C, Topic1, qos1),
    {ok, _, [1]} = emqtt:subscribe(C, Topic2, qos1),

    ok = emqtt:publish(C, Topic0, <<"testm0">>, qos0),
    ok = emqtt:publish(C, Topic1, <<"testm1">>, qos0),
    {ok, _} = emqtt:publish(C, Topic2, <<"testm2">>, qos1),
    ok = emqtt:publish(C, <<"no/queue/bound">>, <<"msg-dropped">>, qos0),
    {ok, _} = emqtt:publish(C, <<"no/queue/bound">>, <<"msg-returned">>, qos1),

    ok = expect_publishes(C, Topic0, [<<"testm0">>]),
    ok = expect_publishes(C, Topic1, [<<"testm1">>]),
    ok = expect_publishes(C, Topic2, [<<"testm2">>]),

    ?assertEqual(#{publishers => 1,
                   consumers => 1,
                   messages_confirmed_total => 2,
                   messages_received_confirm_total => 2,
                   messages_received_total => 5,
                   messages_routed_total => 3,
                   messages_unroutable_dropped_total => 1,
                   messages_unroutable_returned_total => 1},
                 get_global_counters(Config, ProtoVer)),

    case is_feature_flag_enabled(Config, rabbit_mqtt_qos0_queue) of
        true ->
            ?assertEqual(#{messages_delivered_total => 2,
                           messages_acknowledged_total => 1,
                           messages_delivered_consume_auto_ack_total => 1,
                           messages_delivered_consume_manual_ack_total => 1,
                           messages_delivered_get_auto_ack_total => 0,
                           messages_delivered_get_manual_ack_total => 0,
                           messages_get_empty_total => 0,
                           messages_redelivered_total => 0},
                         get_global_counters(Config, ProtoVer, 0, [{queue_type, rabbit_classic_queue}])),
            ?assertEqual(#{messages_delivered_total => 1,
                           messages_acknowledged_total => 0,
                           messages_delivered_consume_auto_ack_total => 1,
                           messages_delivered_consume_manual_ack_total => 0,
                           messages_delivered_get_auto_ack_total => 0,
                           messages_delivered_get_manual_ack_total => 0,
                           messages_get_empty_total => 0,
                           messages_redelivered_total => 0},
                         get_global_counters(Config, ProtoVer, 0, [{queue_type, rabbit_mqtt_qos0_queue}]));
        false ->
            ?assertEqual(#{messages_delivered_total => 3,
                           messages_acknowledged_total => 1,
                           messages_delivered_consume_auto_ack_total => 2,
                           messages_delivered_consume_manual_ack_total => 1,
                           messages_delivered_get_auto_ack_total => 0,
                           messages_delivered_get_manual_ack_total => 0,
                           messages_get_empty_total => 0,
                           messages_redelivered_total => 0},
                         get_global_counters(Config, ProtoVer, 0, [{queue_type, rabbit_classic_queue}]))
    end,

    {ok, _, _} = emqtt:unsubscribe(C, Topic1),
    ?assertEqual(1, maps:get(consumers, get_global_counters(Config, ProtoVer))),

    ok = emqtt:disconnect(C),
    eventually(?_assertEqual(#{publishers => 0,
                               consumers => 0,
                               messages_confirmed_total => 2,
                               messages_received_confirm_total => 2,
                               messages_received_total => 5,
                               messages_routed_total => 3,
                               messages_unroutable_dropped_total => 1,
                               messages_unroutable_returned_total => 1},
                             get_global_counters(Config, ProtoVer))).

queue_down_qos1(Config) ->
    {Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),
    CQ = Topic = atom_to_binary(?FUNCTION_NAME),
    declare_queue(Ch1, CQ, []),
    bind(Ch1, CQ, Topic),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn1, Ch1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 1),

    C = connect(?FUNCTION_NAME, Config, [{retry_interval, 2}]),

    %% classic queue is down, therefore message is rejected
    ?assertEqual(puback_timeout, util:publish_qos1_timeout(C, Topic, <<"msg">>, 500)),

    ok = rabbit_ct_broker_helpers:start_node(Config, 1),
    %% classic queue is up, therefore message should arrive
    eventually(?_assertEqual([[<<"1">>]],
                             rabbitmqctl_list(Config, 1, ["list_queues", "messages", "--no-table-headers"])),
               500, 20),

    Ch0 = rabbit_ct_client_helpers:open_channel(Config, 0),
    delete_queue(Ch0, CQ),
    ok = emqtt:disconnect(C).

%% Even though classic mirrored queues are deprecated, we know that some users have set up
%% a policy to mirror MQTT queues. So, we need to support that use case in RabbitMQ 3.x
%% and failover consumption when the classic mirrored queue leader fails.
consuming_classic_mirrored_queue_down(Config) ->
    [Server1, Server2, _Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ClientId = Topic = PolicyName = atom_to_binary(?FUNCTION_NAME),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, Server1, PolicyName, <<".*">>, <<"queues">>,
           [{<<"ha-mode">>, <<"all">>},
            {<<"queue-master-locator">>, <<"client-local">>}]),

    %% Declare queue leader on Server1.
    C1 = connect(ClientId, Config, Server1, [{clean_start, false}]),
    {ok, _, _} = emqtt:subscribe(C1, Topic, qos1),
    ok = emqtt:disconnect(C1),

    %% Consume from Server2.
    C2 = connect(ClientId, Config, Server2, [{clean_start, false}]),

    %% Sanity check that consumption works.
    {ok, _} = emqtt:publish(C2, Topic, <<"m1">>, qos1),
    ok = expect_publishes(C2, Topic, [<<"m1">>]),

    %% Let's stop the queue leader node.
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),

    %% Consumption should continue to work.
    {ok, _} = emqtt:publish(C2, Topic, <<"m2">>, qos1),
    ok = expect_publishes(C2, Topic, [<<"m2">>]),

    %% Cleanup
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    ?assertMatch([_Q],
                 rpc(Config, Server1, rabbit_amqqueue, list, [])),
    C3 = connect(ClientId, Config, Server2, [{clean_start, true}]),
    ok = emqtt:disconnect(C3),
    ?assertEqual([],
                 rpc(Config, Server1, rabbit_amqqueue, list, [])),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, Server1, PolicyName).

%% Consuming classic queue on a different node goes down.
consuming_classic_queue_down(Config) ->
    [Server1, _Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ClientId = Topic = atom_to_binary(?FUNCTION_NAME),

    %% Declare classic queue on Server1.
    C1 = connect(ClientId, Config, [{clean_start, false}]),
    {ok, _, _} = emqtt:subscribe(C1, Topic, qos1),
    ok = emqtt:disconnect(C1),

    %% Consume from Server3.
    C2 = connect(ClientId, Config, Server3, [{clean_start, false}]),

    ProtoVer = v4,
    ?assertMatch(#{consumers := 1},
                 get_global_counters(Config, ProtoVer, Server3)),

    %% Let's stop the queue leader node.
    process_flag(trap_exit, true),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),

    %% When the dedicated MQTT connection (non-mirrored classic) queue goes down, it is reasonable
    %% that the server closes the MQTT connection because the MQTT client cannot consume anymore.
    eventually(?_assertMatch(#{consumers := 0},
                             get_global_counters(Config, ProtoVer, Server3)),
               1000, 5),
    await_exit(C2),

    %% Cleanup
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    C3 = connect(ClientId, Config, Server3, [{clean_start, true}]),
    ok = emqtt:disconnect(C3),
    ?assertEqual([],
                 rpc(Config, Server1, rabbit_amqqueue, list, [])),
    ok.

delete_create_queue(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    CQ1 = <<"classic-queue-1-delete-create">>,
    CQ2 = <<"classic-queue-2-delete-create">>,
    QQ = <<"quorum-queue-delete-create">>,
    Topic = atom_to_binary(?FUNCTION_NAME),

    DeclareQueues = fun() ->
                            declare_queue(Ch, CQ1, []),
                            bind(Ch, CQ1, Topic),
                            declare_queue(Ch, CQ2, []),
                            bind(Ch, CQ2, Topic),
                            declare_queue(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
                            bind(Ch, QQ, Topic)
                    end,
    DeclareQueues(),

    %% some large retry_interval to avoid re-sending
    C = connect(?FUNCTION_NAME, Config, [{retry_interval, 300}]),
    NumMsgs = 50,
    TestPid = self(),
    spawn(
      fun() ->
              lists:foreach(
                fun(N) ->
                        ok = emqtt:publish_async(C, Topic, integer_to_binary(N), qos1,
                                                 {fun(N0, {ok, #{reason_code_name := success}}) ->
                                                          TestPid ! {self(), N0}
                                                  end, [N]})
                end, lists:seq(1, NumMsgs))
      end),

    %% Delete queues while sending to them.
    %% We want to test the path where a queue is deleted while confirms are outstanding.
    timer:sleep(2),
    delete_queue(Ch, [CQ1, QQ]),
    %% Give queues some time to be fully deleted
    timer:sleep(2000),

    %% We expect confirms for all messages.
    %% Confirm here does not mean that messages made it ever to the deleted queues.
    %% It is valid for confirms to sporadically arrive out of order: This happens when the classic
    %% queue is being deleted while the remaining messages are routed and confirmed to the 2nd and 3rd queues
    %% before the monitor to the classic queue fires.
    ok = await_confirms_unordered(C, NumMsgs),

    %% Recreate the same queues.
    DeclareQueues(),

    %% Sending a message to each of them should work.
    {ok, _} = emqtt:publish(C, Topic, <<"m">>, qos1),
    eventually(?_assertEqual(lists:sort([[CQ1, <<"1">>],
                                         %% This queue should have all messages because we did not delete it.
                                         [CQ2, integer_to_binary(NumMsgs + 1)],
                                         [QQ, <<"1">>]]),
                             lists:sort(rabbitmqctl_list(Config, 0, ["list_queues", "name", "messages", "--no-table-headers"]))),
               1000, 10),

    delete_queue(Ch, [CQ1, CQ2, QQ]),
    ok = emqtt:disconnect(C).

subscription_ttl(Config) ->
    TTL = 1000,
    App = rabbitmq_mqtt,
    Par = ClientId = ?FUNCTION_NAME,
    {ok, DefaultVal} = rpc(Config, application, get_env, [App, Par]),
    ok = rpc(Config, application, set_env, [App, Par, TTL]),

    C = connect(ClientId, Config, [{clean_start, false}]),
    {ok, _, [0, 1]} = emqtt:subscribe(C, [{<<"topic0">>, qos0},
                                          {<<"topic1">>, qos1}]),
    ok = emqtt:disconnect(C),

    ?assertEqual(2, rpc(Config, rabbit_amqqueue, count, [])),
    timer:sleep(TTL + 100),
    ?assertEqual(0,  rpc(Config, rabbit_amqqueue, count, [])),

    ok = rpc(Config, application, set_env, [App, Par, DefaultVal]).

non_clean_sess_reconnect_qos1(Config) ->
    non_clean_sess_reconnect(Config, qos1).

non_clean_sess_reconnect_qos0(Config) ->
    non_clean_sess_reconnect(Config, qos0).

non_clean_sess_reconnect(Config, SubscriptionQoS) ->
    Pub = connect(<<"publisher">>, Config),
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    ProtoVer = v4,

    C1 = connect(ClientId, Config, [{clean_start, false}]),
    {ok, _, _} = emqtt:subscribe(C1, Topic, SubscriptionQoS),
    ?assertMatch(#{consumers := 1},
                 get_global_counters(Config, ProtoVer)),

    ok = emqtt:disconnect(C1),
    ?assertMatch(#{consumers := 0},
                 get_global_counters(Config, ProtoVer)),

    timer:sleep(20),
    ok = emqtt:publish(Pub, Topic, <<"msg-3-qos0">>, qos0),
    {ok, _} = emqtt:publish(Pub, Topic, <<"msg-4-qos1">>, qos1),

    C2 = connect(ClientId, Config, [{clean_start, false}]),
    ?assertMatch(#{consumers := 1},
                 get_global_counters(Config, ProtoVer)),

    ok = emqtt:publish(Pub, Topic, <<"msg-5-qos0">>, qos0),
    {ok, _} = emqtt:publish(Pub, Topic, <<"msg-6-qos1">>, qos1),

    %% shouldn't receive message after unsubscribe
    {ok, _, _} = emqtt:unsubscribe(C2, Topic),
    ?assertMatch(#{consumers := 0},
                 get_global_counters(Config, ProtoVer)),
    {ok, _} = emqtt:publish(Pub, Topic, <<"msg-7-qos0">>, qos1),

    %% "After the disconnection of a Session that had CleanSession set to 0, the Server MUST store
    %% further QoS 1 and QoS 2 messages that match any subscriptions that the client had at the
    %% time of disconnection as part of the Session state [MQTT-3.1.2-5].
    %% It MAY also store QoS 0 messages that meet the same criteria."
    %% Starting with RabbitMQ v3.12 we store QoS 0 messages as well.
    ok = expect_publishes(C2, Topic, [<<"msg-3-qos0">>, <<"msg-4-qos1">>,
                                      <<"msg-5-qos0">>, <<"msg-6-qos1">>]),
    {publish_not_received, <<"msg-7-qos0">>} = expect_publishes(C2, Topic, [<<"msg-7-qos0">>]),

    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(C2),
    %% connect with clean sess true to clean up
    C3 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C3).

non_clean_sess_reconnect_qos0_and_qos1(Config) ->
    Pub = connect(<<"publisher">>, Config),
    ProtoVer = v4,
    Topic0 = <<"t/0">>,
    Topic1 = <<"t/1">>,
    ClientId = ?FUNCTION_NAME,

    C1 = connect(ClientId, Config, [{clean_start, false}]),
    {ok, _, [1, 0]} = emqtt:subscribe(C1, [{Topic1, qos1}, {Topic0, qos0}]),
    ?assertMatch(#{consumers := 1},
                 get_global_counters(Config, ProtoVer)),

    ok = emqtt:disconnect(C1),
    ?assertMatch(#{consumers := 0},
                 get_global_counters(Config, ProtoVer)),

    {ok, _} = emqtt:publish(Pub, Topic0, <<"msg-0">>, qos1),
    {ok, _} = emqtt:publish(Pub, Topic1, <<"msg-1">>, qos1),

    C2 = connect(ClientId, Config, [{clean_start, false}]),
    ?assertMatch(#{consumers := 1},
                 get_global_counters(Config, ProtoVer)),

    ok = expect_publishes(C2, Topic0, [<<"msg-0">>]),
    ok = expect_publishes(C2, Topic1, [<<"msg-1">>]),

    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(C2),
    C3 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C3).

%% "If the Client supplies a zero-byte ClientId with CleanSession set to 0,
%% the Server MUST respond to the CONNECT Packet with a CONNACK return code 0x02
%% (Identifier rejected) and then close the Network Connection" [MQTT-3.1.3-8].
non_clean_sess_empty_client_id(Config) ->
    {C, Connect} = util:start_client(<<>>, Config, 0, [{clean_start, false}]),
    process_flag(trap_exit, true),
    ?assertMatch({error, {client_identifier_not_valid, _}},
                 Connect(C)),
    ok = await_exit(C).

subscribe_same_topic_same_qos(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    Topic = <<"a/b">>,

    {ok, _} = emqtt:publish(C, Topic, <<"retained">>, [{retain, true},
                                                       {qos, 1}]),
    %% Subscribe with QoS 0
    {ok, _, [0]} = emqtt:subscribe(C, Topic, qos0),
    {ok, _} = emqtt:publish(C, Topic, <<"msg1">>, qos1),
    %% Subscribe to same topic with same QoS
    {ok, _, [0]} = emqtt:subscribe(C, Topic, qos0),
    {ok, _} = emqtt:publish(C, Topic, <<"msg2">>, qos1),

    %% "Any existing retained messages matching the Topic Filter MUST be re-sent" [MQTT-3.8.4-3]
    ok = expect_publishes(C, Topic, [<<"retained">>, <<"msg1">>,
                                     <<"retained">>, <<"msg2">>
                                    ]),
    ok = emqtt:disconnect(C).

subscribe_same_topic_different_qos(Config) ->
    C = connect(?FUNCTION_NAME, Config, [{clean_start, false}]),
    Topic = <<"b/c">>,

    {ok, _} = emqtt:publish(C, Topic, <<"retained">>, [{retain, true},
                                                       {qos, 1}]),
    %% Subscribe with QoS 0
    {ok, _, [0]} = emqtt:subscribe(C, Topic, qos0),
    {ok, _} = emqtt:publish(C, Topic, <<"msg1">>, qos1),
    %% Subscribe to same topic with QoS 1
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"msg2">>, qos1),
    %% Subscribe to same topic with QoS 0 again
    {ok, _, [0]} = emqtt:subscribe(C, Topic, qos0),
    {ok, _} = emqtt:publish(C, Topic, <<"msg3">>, qos1),

    %% "Any existing retained messages matching the Topic Filter MUST be re-sent" [MQTT-3.8.4-3]
    ok = expect_publishes(C, Topic, [<<"retained">>, <<"msg1">>,
                                     <<"retained">>, <<"msg2">>,
                                     <<"retained">>, <<"msg3">>]),

    %% There should be exactly one consumer for each queue: qos0 and qos1
    Consumers = rpc(Config, rabbit_amqqueue, consumers_all, [<<"/">>]),
    ?assertEqual(2, length(Consumers)),

    ok = emqtt:disconnect(C),
    C1 = connect(?FUNCTION_NAME, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C1).

subscribe_multiple(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    %% Subscribe to multiple topics at once
    ?assertMatch({ok, _, [0, 1]},
                 emqtt:subscribe(C, [{<<"topic0">>, qos0},
                                     {<<"topic1">>, qos1}])),
    ok = emqtt:disconnect(C).

large_message_mqtt_to_mqtt(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    {ok, _, [1]} = emqtt:subscribe(C, {Topic, qos1}),

    Payload0 = binary:copy(<<"x">>, 8_000_000),
    Payload = <<Payload0/binary, "y">>,
    {ok, _} = emqtt:publish(C, Topic, Payload, qos1),
    ok = expect_publishes(C, Topic, [Payload]),
    ok = emqtt:disconnect(C).

large_message_amqp_to_mqtt(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    {ok, _, [1]} = emqtt:subscribe(C, {Topic, qos1}),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    Payload0 = binary:copy(<<"x">>, 8_000_000),
    Payload = <<Payload0/binary, "y">>,
    amqp_channel:call(Ch,
                      #'basic.publish'{exchange = <<"amq.topic">>,
                                       routing_key = Topic},
                      #amqp_msg{payload = Payload}),
    ok = expect_publishes(C, Topic, [Payload]),
    ok = emqtt:disconnect(C).

amqp_to_mqtt_qos0(Config) ->
    Topic = ClientId = Payload = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    {ok, _, [0]} = emqtt:subscribe(C, {Topic, qos0}),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch,
                      #'basic.publish'{exchange = <<"amq.topic">>,
                                       routing_key = Topic},
                      #amqp_msg{payload = Payload}),
    ok = expect_publishes(C, Topic, [Payload]),
    ok = emqtt:disconnect(C).

%% Packet identifier is a non zero two byte integer.
%% Test that the server wraps around the packet identifier.
many_qos1_messages(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config, 0, [{retry_interval, 600}]),
    {ok, _, [1]} = emqtt:subscribe(C, {Topic, qos1}),
    NumMsgs = 16#ffff + 100,
    Payloads = lists:map(fun integer_to_binary/1, lists:seq(1, NumMsgs)),
    lists:foreach(fun(P) ->
                          {ok, _} = emqtt:publish(C, Topic, P, qos1)
                  end, Payloads),
    expect_publishes(C, Topic, Payloads),
    ok = emqtt:disconnect(C).

%% This test is mostly interesting in mixed version mode where feature flag
%% rabbit_mqtt_qos0_queue is disabled and therefore a classic queue gets created.
rabbit_mqtt_qos0_queue(Config) ->
    Topic = atom_to_binary(?FUNCTION_NAME),

    %% Place MQTT subscriber process on new node in mixed version.
    Sub = connect(<<"subscriber">>, Config),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic, qos0),

    %% Place MQTT publisher process on old node in mixed version.
    Pub = connect(<<"publisher">>, Config, 1, []),

    Msg = <<"msg">>,
    ok = emqtt:publish(Pub, Topic, Msg, qos0),
    ok = expect_publishes(Sub, Topic, [Msg]),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

%% Test that MQTT connection can be listed and closed via the rabbitmq_management plugin.
management_plugin_connection(Config) ->
    KeepaliveSecs = 99,
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Node = atom_to_binary(get_node_config(Config, 0, nodename)),
    C = connect(ClientId, Config, [{keepalive, KeepaliveSecs}]),

    eventually(?_assertEqual(1, length(http_get(Config, "/connections"))), 1000, 10),
    [#{client_properties := #{client_id := ClientId},
       timeout := KeepaliveSecs,
       node := Node,
       name := ConnectionName}] = http_get(Config, "/connections"),

    process_flag(trap_exit, true),
    http_delete(Config,
                "/connections/" ++ binary_to_list(uri_string:quote((ConnectionName))),
                ?NO_CONTENT),
    await_exit(C),
    ?assertEqual([], http_get(Config, "/connections")),
    eventually(?_assertEqual([], all_connection_pids(Config)), 500, 3).

management_plugin_enable(Config) ->
    ?assertEqual(0, length(http_get(Config, "/connections"))),
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_management),
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_management_agent),

    %% If the (web) MQTT connection is established **before** the management plugin is enabled,
    %% the management plugin should still list the (web) MQTT connection.
    C = connect(?FUNCTION_NAME, Config),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_management_agent),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_management),
    eventually(?_assertEqual(1, length(http_get(Config, "/connections"))), 1000, 10),

    ok = emqtt:disconnect(C).

%% Test that queues of type rabbit_mqtt_qos0_queue can be listed via rabbitmqctl.
cli_list_queues(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, _} = emqtt:subscribe(C, <<"a/b/c">>, qos0),

    Qs = rabbit_ct_broker_helpers:rabbitmqctl_list(
           Config, 1,
           ["list_queues", "--no-table-headers",
            "type", "name", "state", "durable", "auto_delete",
            "arguments", "pid", "owner_pid", "messages", "exclusive_consumer_tag"
           ]),
    ExpectedQueueType = case is_feature_flag_enabled(Config, rabbit_mqtt_qos0_queue) of
                            true ->
                                <<"MQTT QoS 0">>;
                            false ->
                                <<"classic">>
                        end,
    ?assertMatch([[ExpectedQueueType, <<"mqtt-subscription-cli_list_queuesqos0">>,
                   <<"running">>, <<"true">>, <<"false">>,  <<"[]">>, _, _, <<"0">>, <<"">>]],
                 Qs),

    ?assertEqual([],
                 rabbit_ct_broker_helpers:rabbitmqctl_list(
                   Config, 1, ["list_queues", "--local", "--no-table-headers"])
                ),

    ok = emqtt:disconnect(C).

maintenance(Config) ->
    C0 = connect(<<"client-0">>, Config, 0, []),
    C1a = connect(<<"client-1a">>, Config, 1, []),
    C1b = connect(<<"client-1b">>, Config, 1, []),

    timer:sleep(500),

    ok = drain_node(Config, 2),
    ok = revive_node(Config, 2),
    timer:sleep(500),
    [?assert(erlang:is_process_alive(C)) || C <- [C0, C1a, C1b]],

    process_flag(trap_exit, true),
    ok = drain_node(Config, 1),
    [await_exit(Pid) || Pid <- [C1a, C1b]],
    ok = revive_node(Config, 1),
    ?assert(erlang:is_process_alive(C0)),

    ok = drain_node(Config, 0),
    await_exit(C0),
    ok = revive_node(Config, 0).

keepalive(Config) ->
    KeepaliveSecs = 1,
    KeepaliveMs = timer:seconds(KeepaliveSecs),
    ProtoVer = v4,
    WillTopic = <<"will/topic">>,
    WillPayload = <<"will-payload">>,
    C1 = connect(?FUNCTION_NAME, Config, [{keepalive, KeepaliveSecs},
                                          {proto_ver, ProtoVer},
                                          {will_topic, WillTopic},
                                          {will_payload, WillPayload},
                                          {will_retain, true},
                                          {will_qos, 0}]),
    ok = emqtt:publish(C1, <<"ignored">>, <<"msg">>),

    %% Connection should stay up when client sends PING requests.
    timer:sleep(KeepaliveMs),
    ?assertMatch(#{publishers := 1},
                 util:get_global_counters(Config, ProtoVer)),

    %% Mock the server socket to not have received any bytes.
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_net,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, getstat, 2, {ok, [{recv_oct, 999}]} ]),
    process_flag(trap_exit, true),

    %% We expect the server to respect the keepalive closing the connection.
    eventually(?_assertMatch(#{publishers := 0},
                             util:get_global_counters(Config, ProtoVer)),
               KeepaliveMs, 3 * KeepaliveSecs),
    await_exit(C1),

    true = rpc(Config, meck, validate, [Mod]),
    ok = rpc(Config, meck, unload, [Mod]),

    C2 = connect(<<"client2">>, Config),
    {ok, _, [0]} = emqtt:subscribe(C2, WillTopic),
    receive {publish, #{client_pid := C2,
                        dup := false,
                        qos := 0,
                        retain := true,
                        topic := WillTopic,
                        payload := WillPayload}} -> ok
    after 3000 -> ct:fail("missing will")
    end,
    ok = emqtt:disconnect(C2).

keepalive_turned_off(Config) ->
    %% "A Keep Alive value of zero (0) has the effect of turning off the keep alive mechanism."
    KeepaliveSecs = 0,
    C = connect(?FUNCTION_NAME, Config, [{keepalive, KeepaliveSecs}]),
    ok = emqtt:publish(C, <<"TopicB">>, <<"Payload">>),

    %% Mock the server socket to not have received any bytes.
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_net,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, getstat, 2, {ok, [{recv_oct, 999}]} ]),

    rabbit_ct_helpers:consistently(?_assert(erlang:is_process_alive(C))),

    true = rpc(Config, meck, validate, [Mod]),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = emqtt:disconnect(C).

duplicate_client_id(Config) ->
    DuplicateClientId = ?FUNCTION_NAME,
    C1 = connect(DuplicateClientId, Config),
    eventually(?_assertEqual(1, length(all_connection_pids(Config)))),

    process_flag(trap_exit, true),
    C2 = connect(DuplicateClientId, Config),
    await_exit(C1),
    timer:sleep(200),
    ?assertEqual(1, length(all_connection_pids(Config))),

    ok = emqtt:disconnect(C2).

block(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),

    {ok, _, _} = emqtt:subscribe(C, Topic),
    {ok, _} = emqtt:publish(C, Topic, <<"Not blocked yet">>, [{qos, 1}]),

    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0]),
    %% Let it block
    timer:sleep(100),

    %% Blocked, but still will publish when unblocked
    puback_timeout = publish_qos1_timeout(C, Topic, <<"Now blocked">>, 1000),
    puback_timeout = publish_qos1_timeout(C, Topic, <<"Still blocked">>, 1000),

    %% Unblock
    rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.4]),
    ok = expect_publishes(C, Topic, [<<"Not blocked yet">>,
                                     <<"Now blocked">>,
                                     <<"Still blocked">>]),
    ok = emqtt:disconnect(C).

block_only_publisher(Config) ->
    Topic = atom_to_binary(?FUNCTION_NAME),

    Opts = [{ack_timeout, 1}],
    Con = connect(<<"background-connection">>, Config, Opts),
    Sub = connect(<<"subscriber-connection">>, Config, Opts),
    Pub = connect(<<"publisher-connection">>, Config, Opts),
    PubSub = connect(<<"publisher-and-subscriber-connection">>, Config, Opts),

    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, qos1),
    {ok, _, [1]} = emqtt:subscribe(PubSub, Topic, qos1),
    {ok, _} = emqtt:publish(Pub, Topic, <<"from Pub">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(PubSub, Topic, <<"from PubSub">>, [{qos, 1}]),
    ok = expect_publishes(Sub, Topic, [<<"from Pub">>, <<"from PubSub">>]),
    ok = expect_publishes(PubSub, Topic, [<<"from Pub">>, <<"from PubSub">>]),

    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0]),
    %% Let it block
    timer:sleep(100),

    %% We expect that the publishing connections are blocked.
    [?assertEqual({error, ack_timeout}, emqtt:ping(Pid)) || Pid <- [Pub, PubSub]],
    %% We expect that the non-publishing connections are not blocked.
    [?assertEqual(pong, emqtt:ping(Pid)) || Pid <- [Con, Sub]],

    %% While the memory alarm is on, let's turn a non-publishing connection
    %% into a publishing connection.
    {ok, _} = emqtt:publish(Con, Topic, <<"from Con 1">>, [{qos, 1}]),
    %% The very first message still goes through.
    ok = expect_publishes(Sub, Topic, [<<"from Con 1">>]),
    %% But now the new publisher should be blocked as well.
    ?assertEqual({error, ack_timeout}, emqtt:ping(Con)),
    ?assertEqual(puback_timeout, publish_qos1_timeout(Con, Topic, <<"from Con 2">>, 500)),
    ?assertEqual(pong, emqtt:ping(Sub)),

    rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.4]),
    %% Let it unblock
    timer:sleep(100),

    %% All connections are unblocked.
    [?assertEqual(pong, emqtt:ping(Pid)) || Pid <- [Con, Sub, Pub, PubSub]],
    %% The publishing connections should be able to publish again.
    {ok, _} = emqtt:publish(Con, Topic, <<"from Con 3">>, [{qos, 1}]),
    ok = expect_publishes(Sub, Topic, [<<"from Con 2">>, <<"from Con 3">>]),
    ok = expect_publishes(PubSub, Topic, [<<"from Con 1">>, <<"from Con 2">>, <<"from Con 3">>]),

    [ok = emqtt:disconnect(Pid) || Pid <- [Con, Sub, Pub, PubSub]].

clean_session_disconnect_client(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, _} = emqtt:subscribe(C, <<"topic0">>, qos0),
    {ok, _, _} = emqtt:subscribe(C, <<"topic1">>, qos1),
    QsQos0 = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_mqtt_qos0_queue]),
    QsClassic = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_classic_queue]),
    case is_feature_flag_enabled(Config, rabbit_mqtt_qos0_queue) of
        true ->
            ?assertEqual(1, length(QsQos0)),
            ?assertEqual(1, length(QsClassic));
        false ->
            ?assertEqual(0, length(QsQos0)),
            ?assertEqual(2, length(QsClassic))
    end,

    ok = emqtt:disconnect(C),
    %% After terminating a clean session, we expect any session state to be cleaned up on the server.
    timer:sleep(200), %% Give some time to clean up exclusive classic queue.
    L = rpc(Config, rabbit_amqqueue, list, []),
    ?assertEqual(0, length(L)).

clean_session_node_restart(Config) ->
    clean_session_node_down(stop_node, Config).

clean_session_node_kill(Config) ->
    clean_session_node_down(kill_node, Config).

clean_session_node_down(NodeDown, Config) ->
    C = connect(?FUNCTION_NAME, Config),
    {ok, _, _} = emqtt:subscribe(C, <<"topic0">>, qos0),
    {ok, _, _} = emqtt:subscribe(C, <<"topic1">>, qos1),
    QsQos0 = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_mqtt_qos0_queue]),
    QsClassic = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_classic_queue]),
    case is_feature_flag_enabled(Config, rabbit_mqtt_qos0_queue) of
        true ->
            ?assertEqual(1, length(QsQos0)),
            ?assertEqual(1, length(QsClassic));
        false ->
            ?assertEqual(0, length(QsQos0)),
            ?assertEqual(2, length(QsClassic))
    end,
    Tables = [rabbit_durable_queue,
              rabbit_queue,
              rabbit_durable_route,
              rabbit_semi_durable_route,
              rabbit_route,
              rabbit_reverse_route,
              rabbit_topic_trie_node,
              rabbit_topic_trie_edge,
              rabbit_topic_trie_binding],
    [?assertNotEqual(0, rpc(Config, ets, info, [T, size])) || T <- Tables],

    unlink(C),
    ok = rabbit_ct_broker_helpers:NodeDown(Config, 0),
    ok = rabbit_ct_broker_helpers:start_node(Config, 0),

    %% After terminating a clean session by either node crash or graceful node shutdown, we
    %% expect any session state to be cleaned up on the server once the server finished booting.
    [?assertEqual(0, rpc(Config, ets, info, [T, size])) || T <- Tables].

rabbit_status_connection_count(Config) ->
    _Pid = rabbit_ct_client_helpers:open_connection(Config, 0),
    C = connect(?FUNCTION_NAME, Config),

    {ok, String} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["status"]),
    ?assertNotEqual(nomatch, string:find(String, "Connection count: 2")),

    ok = emqtt:disconnect(C).

trace(Config) ->
    Server = atom_to_binary(get_node_config(Config, 0, nodename)),
    Topic = Payload = TraceQ = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    declare_queue(Ch, TraceQ, []),
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue       = TraceQ,
                                             exchange    = <<"amq.rabbitmq.trace">>,
                                             routing_key = <<"#">>}),

    %% We expect traced messages for connections created before and connections
    %% created after tracing is enabled.
    Pub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>, Config),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_on"]),
    Sub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_subscriber">>, Config),

    {ok, _, [0]} = emqtt:subscribe(Sub, Topic, qos0),
    {ok, _} = emqtt:publish(Pub, Topic, Payload, qos1),
    ok = expect_publishes(Sub, Topic, [Payload]),

    {#'basic.get_ok'{routing_key = <<"publish.amq.topic">>},
     #amqp_msg{props = #'P_basic'{headers = PublishHeaders},
               payload = Payload}} =
    amqp_channel:call(Ch, #'basic.get'{queue = TraceQ, no_ack = false}),
    ?assertMatch(#{<<"exchange_name">> := <<"amq.topic">>,
                   <<"routing_keys">> := [Topic],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Server,
                   <<"vhost">> := <<"/">>,
                   <<"channel">> := 0,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"delivery_mode">> := 2,
                                         <<"headers">> := #{<<"x-mqtt-publish-qos">> := 1}},
                   <<"routed_queues">> := [<<"mqtt-subscription-trace_subscriberqos0">>]},
                 rabbit_misc:amqp_table(PublishHeaders)),

    {#'basic.get_ok'{routing_key = <<"deliver.mqtt-subscription-trace_subscriberqos0">>},
     #amqp_msg{props = #'P_basic'{headers = DeliverHeaders},
               payload = Payload}} =
    amqp_channel:call(Ch, #'basic.get'{queue = TraceQ, no_ack = false}),
    ?assertMatch(#{<<"exchange_name">> := <<"amq.topic">>,
                   <<"routing_keys">> := [Topic],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Server,
                   <<"vhost">> := <<"/">>,
                   <<"channel">> := 0,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"delivery_mode">> := 2,
                                         <<"headers">> := #{<<"x-mqtt-publish-qos">> := 1}},
                   <<"redelivered">> := 0},
                 rabbit_misc:amqp_table(DeliverHeaders)),

    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_off"]),
    {ok, _} = emqtt:publish(Pub, Topic, Payload, qos1),
    ok = expect_publishes(Sub, Topic, [Payload]),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = TraceQ, no_ack = false})),

    delete_queue(Ch, TraceQ),
    [ok = emqtt:disconnect(C) || C <- [Pub, Sub]].

max_packet_size_unauthenticated(Config) ->
    App = rabbitmq_mqtt,
    Par = ClientId = ?FUNCTION_NAME,
    Opts = [{will_topic, <<"will/topic">>}],

    {C1, Connect} = util:start_client(
                      ClientId, Config, 0,
                      [{will_payload, binary:copy(<<"a">>, 64_000)} | Opts]),
    ?assertMatch({ok, _}, Connect(C1)),
    ok = emqtt:disconnect(C1),

    MaxSize = 500,
    ok = rpc(Config, application, set_env, [App, Par, MaxSize]),

    {C2, Connect} = util:start_client(
                      ClientId, Config, 0,
                      [{will_payload, binary:copy(<<"b">>, MaxSize + 1)} | Opts]),
    true = unlink(C2),
    ?assertMatch({error, _}, Connect(C2)),

    {C3, Connect} = util:start_client(
                      ClientId, Config, 0,
                      [{will_payload, binary:copy(<<"c">>, round(MaxSize / 2))} | Opts]),
    ?assertMatch({ok, _}, Connect(C3)),
    ok = emqtt:disconnect(C3),

    ok = rpc(Config, application, unset_env, [App, Par]).

%% Test that the per vhost default queue type introduced in
%% https://github.com/rabbitmq/rabbitmq-server/pull/5305
%% does not apply to queues created for MQTT connections
%% because having millions of quorum queues is too expensive.
default_queue_type(Config) ->
    Server = get_node_config(Config, 0, nodename),
    QName = Vhost = ClientId = Topic = atom_to_binary(?FUNCTION_NAME),
    ok = erpc:call(Server, rabbit_vhost, add, [Vhost,
                                               #{default_queue_type => <<"quorum">>},
                                               <<"acting-user">>]),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, Vhost),

    ?assertEqual([], rpc(Config, rabbit_amqqueue, list, [])),
    %% Sanity check that the configured default queue type works with AMQP 0.9.1.
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Server, Vhost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    declare_queue(Ch, QName, []),
    QuorumQueues = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_quorum_queue]),
    ?assertEqual(1, length(QuorumQueues)),
    delete_queue(Ch, QName),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    %% Test that the configured default queue type does not apply to MQTT.
    Creds = [{username, <<Vhost/binary, ":guest">>},
             {password, <<"guest">>}],
    C1 = connect(ClientId, Config, [{clean_start, false} | Creds]),
    {ok, _, [1]} = emqtt:subscribe(C1, Topic, qos1),
    ClassicQueues = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_classic_queue]),
    ?assertEqual(1, length(ClassicQueues)),

    ok = emqtt:disconnect(C1),
    C2 = connect(ClientId, Config, [{clean_start, true} | Creds]),
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, Vhost).

%% -------------------------------------------------------------------
%% Internal helpers
%% -------------------------------------------------------------------

await_confirms_ordered(_, To, To) ->
    ok;
await_confirms_ordered(From, N, To) ->
    Expected = {From, N},
    receive
        Expected ->
            await_confirms_ordered(From, N + 1, To);
        Got ->
            ct:fail("Received unexpected message. Expected: ~p Got: ~p", [Expected, Got])
    after 10_000 ->
              ct:fail("Did not receive expected message: ~p", [Expected])
    end.

await_confirms_unordered(_, 0) ->
    ok;
await_confirms_unordered(From, Left) ->
    receive
        {From, _N} ->
            await_confirms_unordered(From, Left - 1);
        Other ->
            ct:fail("Received unexpected message: ~p", [Other])
    after 10_000 ->
              ct:fail("~b confirms are missing", [Left])
    end.

declare_queue(Ch, QueueName, Args)
  when is_pid(Ch), is_binary(QueueName), is_list(Args) ->
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QueueName,
                                     durable = true,
                                     arguments = Args}).

delete_queue(Ch, QueueNames)
  when is_pid(Ch), is_list(QueueNames) ->
    lists:foreach(
      fun(Q) ->
              delete_queue(Ch, Q)
      end, QueueNames);
delete_queue(Ch, QueueName)
  when is_pid(Ch), is_binary(QueueName) ->
    #'queue.delete_ok'{} = amqp_channel:call(
                             Ch, #'queue.delete'{
                                    queue = QueueName}).

bind(Ch, QueueName, Topic)
  when is_pid(Ch), is_binary(QueueName), is_binary(Topic) ->
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue       = QueueName,
                                             exchange    = <<"amq.topic">>,
                                             routing_key = Topic}).
