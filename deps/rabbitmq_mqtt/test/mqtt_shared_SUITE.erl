%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% This test suite contains test cases that are shared between (i.e. executed across):
%% 1. plugins rabbitmq_mqtt and rabbitmq_web_mqtt
%% 2. MQTT versions v3, v4 and v5
%%
%% In other words, this test suite should not contain any test case that is executed
%% only with a particular plugin or particular MQTT version.
%%
%% When adding a test case here the same function must be defined in web_mqtt_shared_SUITE.
-module(mqtt_shared_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_ct_broker_helpers,
        [rabbitmqctl_list/3,
         rabbitmqctl/3,
         rpc/4,
         rpc/5,
         rpc_all/4,
         get_node_config/3,
         drain_node/2,
         revive_node/2,
         await_metadata_store_consistent/2
        ]).
-import(rabbit_ct_helpers,
        [eventually/3,
         eventually/1]).
-import(util,
        [all_connection_pids/1,
         get_global_counters/1, get_global_counters/2,
         get_global_counters/3, get_global_counters/4,
         expect_publishes/3,
         connect/2, connect/3, connect/4,
         get_events/1, assert_event_type/2, assert_event_prop/2,
         await_exit/1, await_exit/2,
         publish_qos1_timeout/4,
         non_clean_sess_opts/0]).
-import(rabbit_mgmt_test_util,
        [http_get/2,
         http_delete/3]).

%% defined in MQTT v5 (not in v4 or v3)
-define(RC_SERVER_SHUTTING_DOWN, 16#8B).
-define(RC_KEEP_ALIVE_TIMEOUT, 16#8D).
-define(RC_SESSION_TAKEN_OVER, 16#8E).
-define(TIMEOUT, 30_000).

all() ->
    [{group, mqtt}].

%% The code being tested under v3 and v4 is almost identical.
%% To save time in CI, we therefore run only a very small subset of tests in v3.
groups() ->
    [
     {mqtt, [],
      [{cluster_size_1, [],
        [{v3, [], cluster_size_1_tests_v3()},
         {v4, [], cluster_size_1_tests()},
         {v5, [], cluster_size_1_tests()}]},
       {cluster_size_3, [],
        [{v4, [], cluster_size_3_tests()},
         {v5, [], cluster_size_3_tests()}]}
      ]}
    ].

cluster_size_1_tests_v3() ->
    [global_counters,
     events
    ].

cluster_size_1_tests() ->
    [
     global_counters %% must be the 1st test case
     ,message_size_metrics
     ,block_only_publisher
     ,many_qos1_messages
     ,session_expiry
     ,cli_close_all_connections
     ,cli_close_all_user_connections
     ,management_plugin_connection
     ,management_plugin_enable
     ,disconnect
     ,pubsub_shared_connection
     ,pubsub_separate_connections
     ,will_with_disconnect
     ,will_without_disconnect
     ,decode_basic_properties
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
     ,block
     ,amqp_to_mqtt_qos0
     ,clean_session_disconnect_client
     ,clean_session_node_restart
     ,clean_session_node_kill
     ,rabbit_status_connection_count
     ,trace
     ,trace_large_message
     ,max_packet_size_unauthenticated
     ,max_packet_size_authenticated
     ,default_queue_type
     ,message_interceptors
     ,utf8
     ,retained_message_conversion
     ,bind_exchange_to_exchange
     ,bind_exchange_to_exchange_single_message
     ,notify_consumer_classic_queue_deleted
     ,notify_consumer_quorum_queue_deleted
     ,notify_consumer_qos0_queue_deleted
    ].

cluster_size_3_tests() ->
    [
     pubsub,
     queue_down_qos1,
     consuming_classic_queue_down,
     flow_classic_queue,
     flow_quorum_queue,
     flow_stream,
     rabbit_mqtt_qos0_queue,
     rabbit_mqtt_qos0_queue_kill_node,
     cli_list_queues,
     delete_create_queue,
     session_reconnect,
     session_takeover,
     duplicate_client_id,
     publish_to_all_queue_types_qos0,
     publish_to_all_queue_types_qos1,
     maintenance
    ].

suite() ->
    [{timetrap, {minutes, 10}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config, {rabbit, [
                                  {quorum_tick_interval, 1000},
                                  {stream_tick_interval, 1000},
                                  {forced_feature_flags_on_init, [
                                                                  delete_ra_cluster_mqtt_node,
                                                                  mqtt_v5,
                                                                  rabbit_mqtt_qos0_queue,
                                                                  restart_streams,
                                                                  stream_sac_coordinator_unblock_group,
                                                                  stream_update_config_command,
                                                                  stream_filtering,
                                                                  message_containers,
                                                                  quorum_queue_non_voters
                                                                 ]},
                                  {start_rmq_with_plugins_disabled, true}
                                 ]}),
    rabbit_ct_helpers:run_setup_steps(Config1).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mqtt, Config0) ->
    rabbit_ct_helpers:set_config(Config0, {websocket, false});
init_per_group(Group, Config)
  when Group =:= v3;
       Group =:= v4;
       Group =:= v5 ->
    rabbit_ct_helpers:set_config(Config, {mqtt_version, Group});

init_per_group(Group, Config0) ->
    Nodes = case Group of
                cluster_size_1 -> 1;
                cluster_size_3 -> 3
            end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config0, "", "-"),
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{rmq_nodes_count, Nodes},
                {rmq_nodename_suffix, Suffix}]),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config1, rabbitmq_mqtt),
    Config1.

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
  when T =:= management_plugin_connection;
       T =:= management_plugin_enable ->
    inets:start(),
    init_per_testcase0(T, Config);
init_per_testcase(T, Config)
  when T =:= clean_session_disconnect_client;
       T =:= zero_session_expiry_interval_disconnect_client;
       T =:= clean_session_node_restart;
       T =:= clean_session_node_kill;
       T =:= notify_consumer_qos0_queue_deleted ->
    ok = rpc(Config, rabbit_registry, register, [queue, <<"qos0">>, rabbit_mqtt_qos0_queue]),
    init_per_testcase0(T, Config);
init_per_testcase(Testcase, Config) ->
    init_per_testcase0(Testcase, Config).

init_per_testcase0(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(T, Config)
  when T =:= management_plugin_connection;
       T =:= management_plugin_enable ->
    ok = inets:stop(),
    end_per_testcase0(T, Config);
end_per_testcase(T, Config)
  when T =:= clean_session_disconnect_client;
       T =:= zero_session_expiry_interval_disconnect_client;
       T =:= clean_session_node_restart;
       T =:= clean_session_node_kill;
       T =:= notify_consumer_qos0_queue_deleted ->
    ok = rpc(Config, rabbit_registry, unregister, [queue, <<"qos0">>]),
    end_per_testcase0(T, Config);
end_per_testcase(Testcase, Config) ->
    end_per_testcase0(Testcase, Config).

end_per_testcase0(Testcase, Config) ->
    rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),
    %% Assert that every testcase cleaned up their MQTT sessions.
    _ = rpc(Config, ?MODULE, delete_queues, []),
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, []))),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

delete_queues() ->
    [catch rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

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
    Sub = connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_subscriber">>, Config),
    {ok, _, [1]} = emqtt:subscribe(Sub, LastWillTopic, qos1),

    %% Client does not send DISCONNECT packet. Therefore, will message should be sent.
    unlink(Pub),
    erlang:exit(Pub, test_will),
    ?assertEqual(ok, expect_publishes(Sub, LastWillTopic, [LastWillMsg])),

    ok = emqtt:disconnect(Sub).

%% Test that an MQTT connection decodes the AMQP 0.9.1 'P_basic' properties.
%% see https://github.com/rabbitmq/rabbitmq-server/discussions/8252
decode_basic_properties(Config) ->
    set_durable_queue_type(Config),
    ClientId = Topic = Payload = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _, [1]} = emqtt:subscribe(C1, Topic, qos1),
    QuorumQueues = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_quorum_queue]),
    ?assertEqual(1, length(QuorumQueues)),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    amqp_channel:call(Ch, #'basic.publish'{exchange = <<"amq.topic">>,
                                           routing_key = Topic},
                      #amqp_msg{payload = Payload}),
    ok = expect_publishes(C1, Topic, [Payload]),
    ok = emqtt:disconnect(C1),
    C2 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C2),
    unset_durable_queue_type(Config),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

quorum_queue_rejects(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Name = atom_to_binary(?FUNCTION_NAME),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"qq-policy">>, Name, <<"queues">>, [{<<"max-length">>, 1},
                                                            {<<"overflow">>, <<"reject-publish">>}]),
    declare_queue(Ch, Name, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    bind(Ch, Name, Name),

    C = connect(Name, Config, [{retry_interval, 1}]),
    {ok, #{reason_code_name := success}} = emqtt:publish(C, Name, <<"m1">>, qos1),
    {ok, #{reason_code_name := success}} = emqtt:publish(C, Name, <<"m2">>, qos1),

    %% The queue will reject m3.
    V = ?config(mqtt_version, Config),
    if V =:= v3 orelse V =:= v4 ->
           %% v3 and v4 do not support NACKs. Therefore, the server should drop the message.
           ?assertEqual(puback_timeout, util:publish_qos1_timeout(C, Name, <<"m3">>, 700));
       V =:= v5 ->
           %% v5 supports NACKs. Therefore, the server should send us a NACK.
           ?assertMatch({ok, #{reason_code_name := implementation_specific_error}},
                        emqtt:publish(C, Name, <<"m3">>, qos1))
    end,

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m1">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Name})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = Name})),
    if V =:= v3 orelse V =:= v4 ->
           %% m3 is re-sent by emqtt since we didn't receive a PUBACK.
           ?awaitMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m3">>}},
                       amqp_channel:call(Ch, #'basic.get'{queue = Name}),
                       2000, 200);
       V =:= v5 ->
           %% m3 should not be re-sent by emqtt since we received a PUBACK.
           ?assertMatch(#'basic.get_empty'{},
                        amqp_channel:call(Ch, #'basic.get'{queue = Name}))
    end,

    ok = emqtt:disconnect(C),
    delete_queue(Ch, Name),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"qq-policy">>).

publish_to_all_queue_types_qos0(Config) ->
    publish_to_all_queue_types(Config, qos0).

publish_to_all_queue_types_qos1(Config) ->
    publish_to_all_queue_types(Config, qos1).

publish_to_all_queue_types(Config, QoS) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),

    CQ = <<"classic-queue">>,
    QQ = <<"quorum-queue">>,
    SQ = <<"stream-queue">>,
    Topic = <<"mytopic">>,

    declare_queue(Ch, CQ, []),
    bind(Ch, CQ, Topic),

    declare_queue(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    bind(Ch, QQ, Topic),

    declare_queue(Ch, SQ, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    bind(Ch, SQ, Topic),

    NumMsgs = 1000,
    C = connect(?FUNCTION_NAME, Config, [{max_inflight, 200},
                                         {retry_interval, 2}]),
    Self = self(),
    lists:foreach(
      fun(N) ->
              %% Publish async all messages at once to trigger flow control
              ok = emqtt:publish_async(C, Topic, integer_to_binary(N), QoS,
                                       {fun(N0, {ok, #{reason_code_name := success}}) ->
                                                Self ! {self(), N0};
                                           (N0, ok) ->
                                                Self ! {self(), N0}
                                        end, [N]})
      end, lists:seq(1, NumMsgs)),
    ok = await_confirms_ordered(C, 1, NumMsgs),
    eventually(?_assert(
                  begin
                      L = rabbitmqctl_list(Config, 0, ["list_queues", "messages", "--no-table-headers"]),
                      length(L) =:= 3 andalso
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
                  end), 1000, 20),

    delete_queue(Ch, [CQ, QQ, SQ]),
    ok = emqtt:disconnect(C),
    ?awaitMatch([],
                all_connection_pids(Config), 10_000, 1000),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

publish_to_all_non_deprecated_queue_types_qos0(Config) ->
    publish_to_all_non_deprecated_queue_types(Config, qos0).

publish_to_all_non_deprecated_queue_types_qos1(Config) ->
    publish_to_all_non_deprecated_queue_types(Config, qos1).

publish_to_all_non_deprecated_queue_types(Config, QoS) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),

    CQ = <<"classic-queue">>,
    QQ = <<"quorum-queue">>,
    SQ = <<"stream-queue">>,
    Topic = <<"mytopic">>,

    declare_queue(Ch, CQ, []),
    bind(Ch, CQ, Topic),

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
                      length(L) =:= 3 andalso
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

    delete_queue(Ch, [CQ, QQ, SQ]),
    ok = emqtt:disconnect(C),
    ?awaitMatch([],
                all_connection_pids(Config), 10_000, 1000),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

%% This test case does not require multiple nodes
%% but it is grouped together with flow test cases for other queue types
%% (and historically used to use a mirrored classic queue on multiple nodes)
flow_classic_queue(Config) ->
    %% New nodes lookup via persistent_term:get/1 (since 4.0.0)
    %% Old nodes lookup via application:get_env/2. (that is taken care of by flow/3)
    %% Therefore, we set both persistent_term and application.
    Key = credit_flow_default_credit,
    Val = {2, 1},
    DefaultVal = rabbit_ct_broker_helpers:rpc(Config, persistent_term, get, [Key]),
    Result = rpc_all(Config, persistent_term, put, [Key, Val]),
    ?assert(lists:all(fun(R) -> R =:= ok end, Result)),

    flow(Config, {rabbit, Key, Val}, <<"classic">>),

    ?assertEqual(Result, rpc_all(Config, persistent_term, put, [Key, DefaultVal])),
    ok.

flow_quorum_queue(Config) ->
    flow(Config, {rabbit, quorum_commands_soft_limit, 1}, <<"quorum">>).

flow_stream(Config) ->
    flow(Config, {rabbit, stream_messages_soft_limit, 1}, <<"stream">>).

flow(Config, {App, Par, Val}, QueueType)
  when is_binary(QueueType) ->
    {ok, DefaultVal} = rpc(Config, application, get_env, [App, Par]),
    Result = rpc_all(Config, application, set_env, [App, Par, Val]),
    ?assert(lists:all(fun(R) -> R =:= ok end, Result)),

    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
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
    ?assertEqual(Result,
                 rpc_all(Config, application, set_env, [App, Par, DefaultVal])),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

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
    ProtoName = case ?config(websocket, Config) of
                    true -> 'Web MQTT';
                    false -> 'MQTT'
                end,
    ProtoVer = case ?config(mqtt_version, Config) of
                   v3 -> {3,1,0};
                   v4 -> {3,1,1};
                   v5 -> {5,0}
               end,
    ExpectedConnectionProps = [{protocol, {ProtoName, ProtoVer}},
                               {node, Server},
                               {vhost, <<"/">>},
                               {user, <<"guest">>},
                               {client_properties, [{client_id, longstr, ClientId}]},
                               {pid, ConnectionPid}],
    assert_event_prop(ExpectedConnectionProps, E1),

    Qos = 0,
    MqttTopic = <<"my/topic">>,
    AmqpTopic = <<"my.topic">>,
    {ok, _, [Qos]} = emqtt:subscribe(C, MqttTopic, Qos),

    QueueNameBin = <<"mqtt-subscription-", ClientId/binary, "qos0">>,
    QueueName = {resource, <<"/">>, queue, QueueNameBin},
    [E2, E3] = get_events(Server),
    assert_event_type(queue_created, E2),
    assert_event_prop([{name, QueueName},
                       {durable, true},
                       {auto_delete, false},
                       {exclusive, true},
                       {type, rabbit_mqtt_qos0_queue},
                       {arguments, []}],
                      E2),
    assert_event_type(binding_created, E3),
    ExpectedBindingArgs = case ?config(mqtt_version, Config) of
                              v5 -> [{mqtt_subscription_opts, Qos, false, false, 0, undefined},
                                     {<<"x-binding-key">>, longstr, AmqpTopic}];
                              _ -> []
                          end,
    assert_event_prop([{source_name, <<"amq.topic">>},
                       {source_kind, exchange},
                       {destination_name, QueueNameBin},
                       {destination_kind, queue},
                       {routing_key, AmqpTopic},
                       {arguments, ExpectedBindingArgs}],
                      E3),

    {ok, _, _} = emqtt:unsubscribe(C, MqttTopic),

    [E4] = get_events(Server),
    assert_event_type(binding_deleted, E4),

    ok = emqtt:disconnect(C),

    [E5, E6] = get_events(Server),
    assert_event_type(connection_closed, E5),
    ?assertEqual(E1#event.props, E5#event.props,
                 "connection_closed event props should match connection_created event props. "
                 "See https://github.com/rabbitmq/rabbitmq-server/discussions/6331"),
    assert_event_type(queue_deleted, E6),
    assert_event_prop({name, QueueName}, E6),

    ok = gen_event:delete_handler({rabbit_event, Server}, event_recorder, []).

internal_event_handler(Config) ->
    Server = get_node_config(Config, 0, nodename),
    ok = gen_event:call({rabbit_event, Server}, rabbit_mqtt_internal_event_handler, ignored_request, 1000).

global_counters(Config) ->
    C = connect(?FUNCTION_NAME, Config),

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
    {ok, Pub} = emqtt:publish(C, <<"no/queue/bound">>, <<"msg-returned">>, qos1),
    case ?config(mqtt_version, Config) of
        v3 -> ok;
        v4 -> ok;
        v5 -> ?assertMatch(#{reason_code_name := no_matching_subscribers}, Pub)
    end,

    ok = expect_publishes(C, Topic0, [<<"testm0">>]),
    ok = expect_publishes(C, Topic1, [<<"testm1">>]),
    ok = expect_publishes(C, Topic2, [<<"testm2">>]),

    ProtoVer = ?config(mqtt_version, Config),
    ?assertEqual(#{publishers => 1,
                   consumers => 1,
                   messages_confirmed_total => 2,
                   messages_received_confirm_total => 2,
                   messages_received_total => 5,
                   messages_routed_total => 3,
                   messages_unroutable_dropped_total => 1,
                   messages_unroutable_returned_total => 1},
                 get_global_counters(Config, ProtoVer)),
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
                 get_global_counters(Config, ProtoVer, 0, [{queue_type, rabbit_mqtt_qos0_queue}])),

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

message_size_metrics(Config) ->
    Protocol = case ?config(mqtt_version, Config) of
                   v4 -> mqtt311;
                   v5 -> mqtt50
               end,
    BucketsBefore = rpc(Config, rabbit_msg_size_metrics, raw_buckets, [Protocol]),

    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    {ok, _, [0]} = emqtt:subscribe(C, Topic, qos0),
    Payload1B = <<255>>,
    Payload500B = binary:copy(Payload1B, 500),
    Payload5KB = binary:copy(Payload1B, 5_000),
    Payload2MB = binary:copy(Payload1B, 2_000_000),
    Payloads = [Payload2MB, Payload5KB, Payload500B, Payload1B, Payload500B],
    [ok = emqtt:publish(C, Topic, P, qos0) || P <- Payloads],
    ok = expect_publishes(C, Topic, Payloads),

    BucketsAfter = rpc(Config, rabbit_msg_size_metrics, raw_buckets, [Protocol]),
    ?assertEqual(
       [{100, 1},
        {1000, 2},
        {10_000, 1},
        {10_000_000, 1}],
       rabbit_msg_size_metrics:diff_raw_buckets(BucketsAfter, BucketsBefore)),

    ok = emqtt:disconnect(C).

pubsub(Config) ->
    Topic0 = <<"t/0">>,
    Topic1 = <<"t/1">>,
    C1 = connect(<<"c1">>, Config, 1, []),
    {ok, _, [1]} = emqtt:subscribe(C1, Topic1, qos1),
    C0 = connect(<<"c0">>, Config, 0, []),
    {ok, _, [1]} = emqtt:subscribe(C0, Topic0, qos1),

    {ok, _} = emqtt:publish(C0, Topic1, <<"m1">>, qos1),
    receive {publish, #{client_pid := C1,
                        qos := 1,
                        payload := <<"m1">>}} -> ok
    after ?TIMEOUT -> ct:fail("missing m1")
    end,

    ok = emqtt:publish(C0, Topic1, <<"m2">>, qos0),
    receive {publish, #{client_pid := C1,
                        qos := 0,
                        payload := <<"m2">>}} -> ok
    after ?TIMEOUT -> ct:fail("missing m2")
    end,

    {ok, _} = emqtt:publish(C1, Topic0, <<"m3">>, qos1),
    receive {publish, #{client_pid := C0,
                        qos := 1,
                        payload := <<"m3">>}} -> ok
    after ?TIMEOUT -> ct:fail("missing m3")
    end,

    ok = emqtt:publish(C1, Topic0, <<"m4">>, qos0),
    receive {publish, #{client_pid := C0,
                        qos := 0,
                        payload := <<"m4">>}} -> ok
    after ?TIMEOUT -> ct:fail("missing m4")
    end,

    ok = emqtt:disconnect(C0),
    ok = emqtt:disconnect(C1).

queue_down_qos1(Config) ->
    {Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),
    CQ = Topic = atom_to_binary(?FUNCTION_NAME),
    declare_queue(Ch1, CQ, []),
    bind(Ch1, CQ, Topic),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn1, Ch1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 1),
    C = connect(?FUNCTION_NAME, Config, [{retry_interval, 2}]),

    %% classic queue is down, therefore message is rejected
    V = ?config(mqtt_version, Config),
    if V =:= v3 orelse V =:= v4 ->
           ?assertEqual(puback_timeout, util:publish_qos1_timeout(C, Topic, <<"msg">>, 500)),
           ok = rabbit_ct_broker_helpers:start_node(Config, 1),
           %% Classic queue is up. Therefore, message should arrive.
           eventually(?_assertEqual([[<<"1">>]],
                                    rabbitmqctl_list(Config, 1, ["list_queues", "messages", "--no-table-headers"])),
                      500, 20);
       V =:= v5 ->
           ?assertMatch({ok, #{reason_code_name := implementation_specific_error}},
                        emqtt:publish(C, Topic, <<"msg">>, qos1)),
           ok = rabbit_ct_broker_helpers:start_node(Config, 1)
    end,

    {Conn, Ch0} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    delete_queue(Ch0, CQ),
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch0).

%% Consuming classic queue on a different node goes down.
consuming_classic_queue_down(Config) ->
    [Server1, _Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ClientId = Topic = atom_to_binary(?FUNCTION_NAME),

    %% Declare classic queue on Server1.
    C1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _, _} = emqtt:subscribe(C1, Topic, qos1),
    ok = emqtt:disconnect(C1),

    %% Consume from Server3.
    C2 = connect(ClientId, Config, Server3, non_clean_sess_opts()),

    ProtoVer = ?config(mqtt_version, Config),
    ?assertMatch(#{consumers := 1},
                 get_global_counters(Config, ProtoVer, Server3)),

    %% Let's stop the queue leader node.
    process_flag(trap_exit, true),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),

    %% When the dedicated MQTT connection queue goes down, it is reasonable
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
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
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
    %% TODO: wait longer for quorum queues in mixed mode as it can take longer
    %% for deletion to complete, delete timeout is 5s so we need to exceed that
    timer:sleep(6000),

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
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

session_expiry(Config) ->
    App = rabbitmq_mqtt,
    Par = max_session_expiry_interval_seconds,
    Seconds = 1,
    {ok, DefaultVal} = rpc(Config, application, get_env, [App, Par]),
    ok = rpc(Config, application, set_env, [App, Par, Seconds]),

    C = connect(?FUNCTION_NAME, Config, non_clean_sess_opts()),
    {ok, _, [0, 1]} = emqtt:subscribe(C, [{<<"topic0">>, qos0},
                                          {<<"topic1">>, qos1}]),
    ok = emqtt:disconnect(C),

    ?assertEqual(2, rpc(Config, rabbit_amqqueue, count, [])),
    timer:sleep(timer:seconds(Seconds) + 100),
    %% On a slow machine, this test might fail. Let's consider
    %% the expiry on a longer time window
    ?awaitMatch(0,  rpc(Config, rabbit_amqqueue, count, []), 15_000, 1000),

    ok = rpc(Config, application, set_env, [App, Par, DefaultVal]).

non_clean_sess_reconnect_qos1(Config) ->
    non_clean_sess_reconnect(Config, 1).

non_clean_sess_reconnect_qos0(Config) ->
    non_clean_sess_reconnect(Config, 0).

non_clean_sess_reconnect(Config, SubscriptionQoS) ->
    Pub = connect(<<"publisher">>, Config),
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),

    C1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _, [SubscriptionQoS]} = emqtt:subscribe(C1, Topic, SubscriptionQoS),
    ok = await_consumer_count(1, ClientId, SubscriptionQoS, Config),

    ok = emqtt:disconnect(C1),
    ok = await_consumer_count(0, ClientId, SubscriptionQoS, Config),

    ok = emqtt:publish(Pub, Topic, <<"msg-3-qos0">>, qos0),
    {ok, _} = emqtt:publish(Pub, Topic, <<"msg-4-qos1">>, qos1),

    C2 = connect(ClientId, Config, non_clean_sess_opts()),
    %% Server should reply in CONNACK that it has session state.
    ?assertEqual({session_present, 1},
                 proplists:lookup(session_present, emqtt:info(C2))),
    ok = await_consumer_count(1, ClientId, SubscriptionQoS, Config),

    ok = emqtt:publish(Pub, Topic, <<"msg-5-qos0">>, qos0),
    {ok, _} = emqtt:publish(Pub, Topic, <<"msg-6-qos1">>, qos1),

    %% shouldn't receive message after unsubscribe
    {ok, _, _} = emqtt:unsubscribe(C2, Topic),
    ?assertMatch(#{consumers := 0},
                 get_global_counters(Config)),
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
    Topic0 = <<"t/0">>,
    Topic1 = <<"t/1">>,
    ClientId = ?FUNCTION_NAME,

    C1 = connect(ClientId, Config, non_clean_sess_opts()),
    {ok, _, [1, 0]} = emqtt:subscribe(C1, [{Topic1, qos1},
                                           {Topic0, qos0}]),
    ok = await_consumer_count(1, ClientId, 0, Config),
    ok = await_consumer_count(1, ClientId, 1, Config),

    ok = emqtt:disconnect(C1),
    ok = await_consumer_count(0, ClientId, 0, Config),
    ok = await_consumer_count(0, ClientId, 1, Config),
    {ok, _} = emqtt:publish(Pub, Topic0, <<"msg-0">>, qos1),
    {ok, _} = emqtt:publish(Pub, Topic1, <<"msg-1">>, qos1),

    C2 = connect(ClientId, Config, non_clean_sess_opts()),
    ok = await_consumer_count(1, ClientId, 0, Config),
    ok = await_consumer_count(1, ClientId, 1, Config),
    ok = expect_publishes(C2, Topic0, [<<"msg-0">>]),
    ok = expect_publishes(C2, Topic1, [<<"msg-1">>]),

    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(C2),
    C3 = connect(ClientId, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C3).

non_clean_sess_empty_client_id(Config) ->
    {C, Connect} = util:start_client(<<>>, Config, 0, non_clean_sess_opts()),
    case ?config(mqtt_version, Config) of
        V when V =:= v3;
               V =:= v4 ->
            %% "If the Client supplies a zero-byte ClientId with CleanSession set to 0,
            %% the Server MUST respond to the CONNECT Packet with a CONNACK return code 0x02
            %% (Identifier rejected) and then close the Network Connection" [MQTT-3.1.3-8].
            process_flag(trap_exit, true),
            ?assertMatch({error, {client_identifier_not_valid, _}}, Connect(C)),
            ok = await_exit(C);
        v5 ->
            %% "If the Client connects using a zero length Client Identifier, the Server MUST respond with
            %% a CONNACK containing an Assigned Client Identifier. The Assigned Client Identifier MUST be
            %% a new Client Identifier not used by any other Session currently in the Server [MQTT-3.2.2-16]."
            {ok, #{'Assigned-Client-Identifier' := ClientId}} = Connect(C),
            {C2, Connect2} = util:start_client(<<>>, Config, 0, [{clean_start, true}]),
            {ok, #{'Assigned-Client-Identifier' := ClientId2}} = Connect2(C2),
            ?assertNotEqual(ClientId, ClientId2),
            ok = emqtt:disconnect(C),
            ok = emqtt:disconnect(C2)
    end.

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
    C = connect(?FUNCTION_NAME, Config, non_clean_sess_opts()),
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

    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Payload0 = binary:copy(<<"x">>, 8_000_000),
    Payload = <<Payload0/binary, "y">>,
    amqp_channel:call(Ch,
                      #'basic.publish'{exchange = <<"amq.topic">>,
                                       routing_key = Topic},
                      #amqp_msg{payload = Payload}),
    ok = expect_publishes(C, Topic, [Payload]),
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

amqp_to_mqtt_qos0(Config) ->
    Topic = ClientId = Payload = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    {ok, _, [0]} = emqtt:subscribe(C, {Topic, qos0}),

    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    amqp_channel:call(Ch,
                      #'basic.publish'{exchange = <<"amq.topic">>,
                                       routing_key = Topic},
                      #amqp_msg{payload = Payload}),
    ok = expect_publishes(C, Topic, [Payload]),
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

%% Packet identifier is a non zero two byte integer.
%% Test that the server wraps around the packet identifier.
many_qos1_messages(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    NumMsgs = 16#ffff + 100,
    C = connect(ClientId, Config, 0, [{retry_interval, 600},
                                      {max_inflight, NumMsgs div 8}]),
    {ok, _, [1]} = emqtt:subscribe(C, {Topic, qos1}),
    Payloads = lists:map(fun integer_to_binary/1, lists:seq(1, NumMsgs)),
    Self = self(),
    Target = lists:last(Payloads),
    lists:foreach(fun(P) ->
                          Cb = {fun(T, _) when T == Target ->
                                        Self ! proceed;
                                   (_, _) ->
                                        ok
                                end, [P]},
                          ok = emqtt:publish_async(C, Topic, P, qos1, Cb)
                  end, Payloads),
    receive
        proceed -> ok
    after 300_000 ->
              ct:fail("message to proceed never received")
    end,
    ok = expect_publishes(C, Topic, Payloads),
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

rabbit_mqtt_qos0_queue_kill_node(Config) ->
    Topic1 = <<"t/1">>,
    Topic2 = <<"t/2">>,
    Pub = connect(<<"publisher">>, Config, 2, []),

    SubscriberId = <<"subscriber">>,
    Sub0 = connect(SubscriberId, Config, 0, []),
    {ok, _, [0]} = emqtt:subscribe(Sub0, Topic1, qos0),
    ok = await_metadata_store_consistent(Config, 2),
    ok = emqtt:publish(Pub, Topic1, <<"m0">>, qos0),
    ok = expect_publishes(Sub0, Topic1, [<<"m0">>]),

    process_flag(trap_exit, true),
    ok = rabbit_ct_broker_helpers:kill_node(Config, 0),
    ok = await_exit(Sub0),
    %% Wait to run rabbit_amqqueue:on_node_down/1 on both live nodes.
    timer:sleep(500),
    %% Re-connect to a live node with same MQTT client ID.
    Sub1 = connect(SubscriberId, Config, 1, []),
    {ok, _, [0]} = emqtt:subscribe(Sub1, Topic2, qos0),
    ok = await_metadata_store_consistent(Config, 2),
    ok = emqtt:publish(Pub, Topic2, <<"m1">>, qos0),
    ok = expect_publishes(Sub1, Topic2, [<<"m1">>]),
    %% Since we started a new clean session, previous subscription should have been deleted.
    ok = emqtt:publish(Pub, Topic1, <<"m2">>, qos0),
    receive {publish, _} = Publish -> ct:fail({unexpected, Publish})
    after 300 -> ok
    end,

    ok = rabbit_ct_broker_helpers:start_node(Config, 0),
    ok = rabbit_ct_broker_helpers:kill_node(Config, 1),
    %% This time, do not wait.
    %% rabbit_amqqueue:on_node_down/1 may or may not have run.
    Sub2 = connect(SubscriberId, Config, 2, []),
    {ok, _, [0]} = emqtt:subscribe(Sub2, Topic2, qos0),
    ok = emqtt:publish(Pub, Topic2, <<"m3">>, qos0),
    ok = expect_publishes(Sub2, Topic2, [<<"m3">>]),

    ok = emqtt:disconnect(Sub2),
    ok = emqtt:disconnect(Pub),
    ok = rabbit_ct_broker_helpers:start_node(Config, 1),
    ?assertEqual([], rpc(Config, rabbit_db_binding, get_all, [])).

cli_close_all_connections(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    process_flag(trap_exit, true),
    {ok, String} = rabbit_ct_broker_helpers:rabbitmqctl(
                     Config, 0, ["close_all_connections", "bye"]),
    ?assertEqual(match, re:run(String, "Closing .* reason: bye", [{capture, none}])),
    ok = await_exit(C).

cli_close_all_user_connections(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    process_flag(trap_exit, true),
    {ok, String} = rabbit_ct_broker_helpers:rabbitmqctl(
                     Config, 0, ["close_all_user_connections","guest", "bye"]),
    ?assertEqual(match, re:run(String, "Closing .* reason: bye", [{capture, none}])),
    ok = await_exit(C).

%% Test that MQTT connection can be listed and closed via the rabbitmq_management plugin.
management_plugin_connection(Config) ->
    KeepaliveSecs = 99,
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Node = atom_to_binary(get_node_config(Config, 0, nodename)),

    C1 = connect(ClientId, Config, [{keepalive, KeepaliveSecs}]),
    FilterFun =
        fun(#{client_properties := #{client_id := CId}})
              when CId == ClientId -> true;
           (_) -> false
        end,
    %% Sometimes connections remain open from other testcases,
    %% let's match the one we're looking for
    eventually(
      ?_assertMatch(
         [_],
         lists:filter(FilterFun, http_get(Config, "/connections"))),
      1000, 10),
    [#{client_properties := #{client_id := ClientId},
       timeout := KeepaliveSecs,
       node := Node,
       name := ConnectionName}] =
        lists:filter(FilterFun, http_get(Config, "/connections")),
    process_flag(trap_exit, true),
    http_delete(Config,
                "/connections/" ++ binary_to_list(uri_string:quote(ConnectionName)),
                ?NO_CONTENT),
    await_exit(C1),
    eventually(
      ?_assertMatch(
         [],
         lists:filter(FilterFun, http_get(Config, "/connections"))),
      1000, 10),
    eventually(?_assertEqual([], all_connection_pids(Config)), 500, 3),

    C2 = connect(ClientId, Config, [{keepalive, KeepaliveSecs}]),
    eventually(
      ?_assertMatch(
         [_],
         lists:filter(FilterFun, http_get(Config, "/connections"))),
      1000, 10),
    http_delete(Config,
                "/connections/username/guest",
                ?NO_CONTENT),
    await_exit(C2),
    eventually(
      ?_assertMatch(
         [],
         lists:filter(FilterFun, http_get(Config, "/connections"))),
      1000, 10),
    eventually(?_assertEqual([], all_connection_pids(Config)), 500, 3).

management_plugin_enable(Config) ->
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_management),
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_management_agent),

    %% If the (web) MQTT connection is established **before** the management plugin is enabled,
    %% the management plugin should still list the (web) MQTT connection.
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_management_agent),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_management),
    FilterFun =
        fun(#{client_properties := #{client_id := CId}})
              when ClientId == CId -> true;
           (_) -> false
        end,
    %% Sometimes connections remain open from other testcases,
    %% let's match the one we're looking for
    eventually(
      ?_assertMatch(
         [_],
         lists:filter(FilterFun, http_get(Config, "/connections"))),
      1000, 10),

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
    ?assertMatch([[<<"MQTT QoS 0">>, <<"mqtt-subscription-cli_list_queuesqos0">>,
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
    ClientsNode1 = [C1a, C1b],

    timer:sleep(500),

    ok = drain_node(Config, 2),
    ok = revive_node(Config, 2),
    timer:sleep(500),
    [?assert(erlang:is_process_alive(C)) || C <- [C0, C1a, C1b]],

    process_flag(trap_exit, true),
    ok = drain_node(Config, 1),
    [await_exit(Pid) || Pid <- ClientsNode1],
    [assert_v5_disconnect_reason_code(Config, ?RC_SERVER_SHUTTING_DOWN) || _ <- ClientsNode1],
    ok = revive_node(Config, 1),
    ?assert(erlang:is_process_alive(C0)),

    ok = drain_node(Config, 0),
    await_exit(C0),
    assert_v5_disconnect_reason_code(Config, ?RC_SERVER_SHUTTING_DOWN),
    ok = revive_node(Config, 0).

keepalive(Config) ->
    KeepaliveSecs = 1,
    KeepaliveMs = timer:seconds(KeepaliveSecs),
    WillTopic = <<"will/topic">>,
    WillPayload = <<"will-payload">>,
    C1 = connect(?FUNCTION_NAME, Config, [{keepalive, KeepaliveSecs},
                                          {will_topic, WillTopic},
                                          {will_payload, WillPayload},
                                          {will_retain, true},
                                          {will_qos, 0}]),
    ok = emqtt:publish(C1, <<"ignored">>, <<"msg">>),

    %% Connection should stay up when client sends PING requests.
    timer:sleep(KeepaliveMs),
    ?assertMatch(#{publishers := 1},
                 util:get_global_counters(Config)),

    %% Mock the server socket to not have received any bytes.
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_net,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, getstat, 2, {ok, [{recv_oct, 999}]} ]),
    process_flag(trap_exit, true),

    %% We expect the server to respect the keepalive closing the connection.
    eventually(?_assertMatch(#{publishers := 0},
                             util:get_global_counters(Config)),
               KeepaliveMs, 4 * KeepaliveSecs),

    await_exit(C1),
    assert_v5_disconnect_reason_code(Config, ?RC_KEEP_ALIVE_TIMEOUT),
    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),

    C2 = connect(<<"client2">>, Config),
    {ok, _, [0]} = emqtt:subscribe(C2, WillTopic),
    receive {publish, #{client_pid := C2,
                        dup := false,
                        qos := 0,
                        retain := true,
                        topic := WillTopic,
                        payload := WillPayload}} -> ok
    after ?TIMEOUT -> ct:fail("missing will")
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

    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = emqtt:disconnect(C).

duplicate_client_id(Config) ->
    [Server1, Server2, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    %% Test session takeover by both new and old node in mixed version clusters.
    ClientId1 = <<"c1">>,
    ClientId2 = <<"c2">>,
    C1a = connect(ClientId1, Config, Server2, []),
    C2a = connect(ClientId2, Config, Server1, []),
    eventually(?_assertEqual(2, length(all_connection_pids(Config)))),
    process_flag(trap_exit, true),
    C1b = connect(ClientId1, Config, Server1, []),
    C2b = connect(ClientId2, Config, Server2, []),
    assert_v5_disconnect_reason_code(Config, ?RC_SESSION_TAKEN_OVER),
    assert_v5_disconnect_reason_code(Config, ?RC_SESSION_TAKEN_OVER),
    await_exit(C1a),
    await_exit(C2a),
    timer:sleep(200),
    ?assertEqual(2, length(all_connection_pids(Config))),
    ok = emqtt:disconnect(C1b),
    ok = emqtt:disconnect(C2b),
    eventually(?_assertEqual(0, length(all_connection_pids(Config)))).

session_reconnect(Config) ->
    session_switch(Config, true).

session_takeover(Config) ->
    session_switch(Config, false).

session_switch(Config, Disconnect) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    %% Connect to old node in mixed version cluster.
    C1 = connect(ClientId, Config, 1, non_clean_sess_opts()),
    {ok, _, [1]} = emqtt:subscribe(C1, Topic, qos1),
    case Disconnect of
        true -> ok = emqtt:disconnect(C1);
        false -> unlink(C1)
    end,
    %% Connect to new node in mixed version cluster.
    C2 = connect(ClientId, Config, 0, non_clean_sess_opts()),
    case Disconnect of
        true -> ok;
        false -> assert_v5_disconnect_reason_code(Config, ?RC_SESSION_TAKEN_OVER)
    end,
    %% New connection should be able to modify subscription.
    {ok, _, [0]} = emqtt:subscribe(C2, Topic, qos0),
    {ok, _} = emqtt:publish(C2, Topic, <<"m1">>, qos1),
    receive {publish, #{client_pid := C2,
                        payload := <<"m1">>,
                        qos := 0}} -> ok
    after ?TIMEOUT -> ct:fail("did not receive m1 with QoS 0")
    end,
    %% New connection should be able to unsubscribe.
    ?assertMatch({ok, _, _}, emqtt:unsubscribe(C2, Topic)),
    {ok, _} = emqtt:publish(C2, Topic, <<"m2">>, qos1),
    receive Unexpected -> ct:fail({unexpected, Unexpected})
    after 300 -> ok
    end,

    ok = emqtt:disconnect(C2),
    C3 = connect(ClientId, Config, 0, [{clean_start, true}]),
    ok = emqtt:disconnect(C3),
    eventually(?_assertEqual([], all_connection_pids(Config))).

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
    rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.6]),
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

    rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.6]),
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
    ?assertEqual(1, length(QsQos0)),
    ?assertEqual(1, length(QsClassic)),

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
    ?assertEqual(1, length(QsQos0)),
    ?assertEqual(1, length(QsClassic)),
    ?assertEqual(2, rpc(Config, rabbit_amqqueue, count, [])),

    unlink(C),
    ok = rabbit_ct_broker_helpers:NodeDown(Config, 0),
    ok = rabbit_ct_broker_helpers:start_node(Config, 0),

    %% After terminating a clean session by a node crash, we expect any session
    %% state to be cleaned up on the server once the server comes back up.
    ?assertEqual(0, rpc(Config, rabbit_amqqueue, count, [])).

rabbit_status_connection_count(Config) ->
    _Pid = rabbit_ct_client_helpers:open_connection(Config, 0),
    C = connect(?FUNCTION_NAME, Config),

    {ok, String} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["status"]),
    ?assertNotEqual(nomatch, string:find(String, "Connection count: 2")),

    ok = emqtt:disconnect(C).

trace(Config) ->
    Server = atom_to_binary(get_node_config(Config, 0, nodename)),
    Topic = Payload = TraceQ = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
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
    timer:sleep(10),

    {#'basic.get_ok'{routing_key = <<"publish.amq.topic">>},
     #amqp_msg{props = #'P_basic'{headers = PublishHeaders},
               payload = Payload}} =
    amqp_channel:call(Ch, #'basic.get'{queue = TraceQ}),
    ?assertMatch(#{<<"exchange_name">> := <<"amq.topic">>,
                   <<"routing_keys">> := [Topic],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Server,
                   <<"vhost">> := <<"/">>,
                   <<"channel">> := 0,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"delivery_mode">> := 2},
                   <<"routed_queues">> := [<<"mqtt-subscription-trace_subscriberqos0">>]},
                 rabbit_misc:amqp_table(PublishHeaders)),

    {#'basic.get_ok'{routing_key = <<"deliver.mqtt-subscription-trace_subscriberqos0">>},
     #amqp_msg{props = #'P_basic'{headers = DeliverHeaders},
               payload = Payload}} =
    amqp_channel:call(Ch, #'basic.get'{queue = TraceQ}),
    ?assertMatch(#{<<"exchange_name">> := <<"amq.topic">>,
                   <<"routing_keys">> := [Topic],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Server,
                   <<"vhost">> := <<"/">>,
                   <<"channel">> := 0,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"delivery_mode">> := 2},
                   <<"redelivered">> := 0},
                 rabbit_misc:amqp_table(DeliverHeaders)),

    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_off"]),
    {ok, _} = emqtt:publish(Pub, Topic, Payload, qos1),
    ok = expect_publishes(Sub, Topic, [Payload]),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = TraceQ})),

    delete_queue(Ch, TraceQ),
    [ok = emqtt:disconnect(C) || C <- [Pub, Sub]],
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

trace_large_message(Config) ->
    TraceQ = <<"trace-queue">>,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    declare_queue(Ch, TraceQ, []),
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue = TraceQ,
                                             exchange = <<"amq.rabbitmq.trace">>,
                                             routing_key = <<"deliver.*">>}),
    C = connect(<<"my-client">>, Config),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_on"]),
    {ok, _, [0]} = emqtt:subscribe(C, <<"/my/topic">>),
    Payload0 = binary:copy(<<"x">>, 1_000_000),
    Payload = <<Payload0/binary, "y">>,
    amqp_channel:call(Ch,
                      #'basic.publish'{exchange = <<"amq.topic">>,
                                       routing_key = <<".my.topic">>},
                      #amqp_msg{payload = Payload}),
    ok = expect_publishes(C, <<"/my/topic">>, [Payload]),
    timer:sleep(10),
    ?assertMatch(
       {#'basic.get_ok'{routing_key = <<"deliver.mqtt-subscription-my-clientqos0">>},
        #amqp_msg{payload = Payload}},
       amqp_channel:call(Ch, #'basic.get'{queue = TraceQ})
      ),

    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_off"]),
    delete_queue(Ch, TraceQ),
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

max_packet_size_unauthenticated(Config) ->
    ClientId = ?FUNCTION_NAME,
    Opts = [{will_topic, <<"will/topic">>}],

    {C1, Connect} = util:start_client(
                      ClientId, Config, 0,
                      [{will_payload, binary:copy(<<"a">>, 64_000)} | Opts]),
    ?assertMatch({ok, _}, Connect(C1)),
    ok = emqtt:disconnect(C1),

    Key = mqtt_max_packet_size_unauthenticated,
    OldMaxSize = rpc(Config, persistent_term, get, [Key]),
    MaxSize = 500,
    ok = rpc(Config, persistent_term, put, [Key, MaxSize]),

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

    ok = rpc(Config, persistent_term, put, [Key, OldMaxSize]).

max_packet_size_authenticated(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    Key = mqtt_max_packet_size_authenticated,
    OldMaxSize = rpc(Config, persistent_term, get, [Key]),
    MaxSize = 500,
    ok = rpc(Config, persistent_term, put, [Key, MaxSize]),

    {C, Connect} = util:start_client(ClientId, Config, 0, []),
    {ok, ConnAckProps} = Connect(C),
    process_flag(trap_exit, true),
    ok = emqtt:publish(C, Topic, binary:copy(<<"x">>, MaxSize + 1), qos0),
    await_exit(C),
    case ?config(mqtt_version, Config) of
        v3 -> ok;
        v4 -> ok;
        v5 -> ?assertMatch(#{'Maximum-Packet-Size' := MaxSize}, ConnAckProps),
              receive {disconnected, _ReasonCodePacketTooLarge = 149, _Props} -> ok
              after ?TIMEOUT -> ct:fail("missing DISCONNECT packet from server")
              end
    end,
    ok = rpc(Config, persistent_term, put, [Key, OldMaxSize]).

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
    C1 = connect(ClientId, Config, Creds ++ non_clean_sess_opts()),
    {ok, _, [1]} = emqtt:subscribe(C1, Topic, qos1),
    ClassicQueues = rpc(Config, rabbit_amqqueue, list_by_type, [rabbit_classic_queue]),
    ?assertEqual(1, length(ClassicQueues)),

    ok = emqtt:disconnect(C1),
    C2 = connect(ClientId, Config, [{clean_start, true} | Creds]),
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, Vhost).

message_interceptors(Config) ->
    ok = rpc(Config, persistent_term, put,
             [message_interceptors,
              [
               {rabbit_mqtt_msg_interceptor_client_id, #{}},
               {rabbit_msg_interceptor_timestamp, #{overwrite => false,
                                                    incoming => true,
                                                    outgoing => true}}
              ]]),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Payload = Topic = atom_to_binary(?FUNCTION_NAME),
    ClientId = <<"🆔"/utf8>>,
    CQName = <<"my classic queue">>,
    Stream = <<"my stream">>,
    declare_queue(Ch, CQName, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    declare_queue(Ch, Stream, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    bind(Ch, CQName, Topic),
    bind(Ch, Stream, Topic),
    C = connect(ClientId, Config),

    NowSecs = os:system_time(second),
    NowMillis = os:system_time(millisecond),
    {ok, _} = emqtt:publish(C, Topic, Payload, qos1),

    {#'basic.get_ok'{},
     #amqp_msg{payload = Payload,
               props = #'P_basic'{
                          timestamp = Secs,
                          headers = Headers
                         }}
    } = amqp_channel:call(Ch, #'basic.get'{queue = CQName}),

    {_, long, ReceivedTs} = lists:keyfind(<<"timestamp_in_ms">>, 1, Headers),
    ?assert(Secs < NowSecs + 9),
    ?assert(Secs > NowSecs - 9),
    ?assert(ReceivedTs < NowMillis + 9000),
    ?assert(ReceivedTs > NowMillis - 9000),
    {_, long, SentTs} = lists:keyfind(<<"x-opt-rabbitmq-sent-time">>, 1, Headers),
    ?assert(SentTs < NowMillis + 9000),
    ?assert(SentTs > NowMillis - 9000),

    ?assertEqual({<<"x-opt-mqtt-client-id">>, longstr, ClientId},
                 lists:keyfind(<<"x-opt-mqtt-client-id">>, 1, Headers)),

    #'basic.qos_ok'{}  = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 1}),
    CTag = <<"my ctag">>,
    #'basic.consume_ok'{} = amqp_channel:subscribe(
                              Ch,
                              #'basic.consume'{
                                 queue = Stream,
                                 consumer_tag = CTag,
                                 arguments = [{<<"x-stream-offset">>, longstr, <<"first">>}]},
                              self()),
    receive {#'basic.deliver'{consumer_tag = CTag},
             #amqp_msg{payload = Payload,
                       props = #'P_basic'{
                                  headers = [{<<"timestamp_in_ms">>, long, ReceivedTs} | XHeaders]
                                 }}} ->
                ?assertEqual({<<"x-opt-mqtt-client-id">>, longstr, ClientId},
                             lists:keyfind(<<"x-opt-mqtt-client-id">>, 1, XHeaders)),

                {_, long, SentTs1} = lists:keyfind(<<"x-opt-rabbitmq-sent-time">>, 1, XHeaders),
                ?assert(SentTs1 < NowMillis + 9000),
                ?assert(SentTs1 > NowMillis - 9000)
    after ?TIMEOUT -> ct:fail(missing_deliver)
    end,

    delete_queue(Ch, Stream),
    delete_queue(Ch, CQName),
    ok = rpc(Config, persistent_term, put, [message_interceptors, []]),
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

%% This test makes sure that a retained message that got written in 3.12 or earlier
%% can be consumed in 3.13 or later.
retained_message_conversion(Config) ->
    Topic = <<"a/b">>,
    Payload = <<"my retained msg">>,
    OldMqttMsgFormat = {mqtt_msg, _Retain = true, _QoS = 1, Topic, _Dup = false, _PktId = 1, Payload},
    RetainerPid = rpc(Config, rabbit_mqtt_retainer_sup, start_child_for_vhost, [<<"/">>]),
    {rabbit_mqtt_retainer, StoreState, _} = sys:get_state(RetainerPid),
    ok = rpc(Config, rabbit_mqtt_retained_msg_store, insert, [Topic, OldMqttMsgFormat, StoreState]),

    C = connect(?FUNCTION_NAME, Config),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    receive {publish, #{client_pid := C,
                        dup := false,
                        qos := 1,
                        retain := true,
                        topic := Topic,
                        payload := Payload}} -> ok
    after ?TIMEOUT -> ct:fail("missing retained message")
    end,
    ok = emqtt:publish(C, Topic, <<>>, [{retain, true}]),
    ok = emqtt:disconnect(C).

%% Test that the server can handle UTF-8 encoded strings.
utf8(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    % "The Topic Name MUST be present as the first field in the PUBLISH Packet Variable header.
    % It MUST be a UTF-8 encoded string [MQTT-3.3.2-1] as defined in section 1.5.3."
    Topic = <<"うさぎ"/utf8>>, %% Rabbit in Japanese
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _} = emqtt:publish(C, Topic, <<"msg">>, qos1),
    ok = expect_publishes(C, Topic, [<<"msg">>]),
    ok = emqtt:disconnect(C).

bind_exchange_to_exchange(Config) ->
    SourceX = <<"amq.topic">>,
    DestinationX = <<"destination">>,
    Q = <<"q">>,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DestinationX,
                                                                         durable = true,
                                                                         auto_delete = true}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = Q,
                                                                   durable = true}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = DestinationX,
                                                             queue = Q,
                                                             routing_key = <<"a.b">>}),
    #'exchange.bind_ok'{} = amqp_channel:call(Ch, #'exchange.bind'{destination = DestinationX,
                                                                   source = SourceX,
                                                                   routing_key = <<"*.b">>}),
    C = connect(?FUNCTION_NAME, Config),
    %% Message should be routed as follows: SourceX -> DestinationX -> Q
    {ok, _} = emqtt:publish(C, <<"a/b">>, <<"msg">>, qos1),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg">>}},
                             amqp_channel:call(Ch, #'basic.get'{queue = Q}))),
    #'queue.delete_ok'{message_count = 0} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}),
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

bind_exchange_to_exchange_single_message(Config) ->
    SourceX = <<"amq.topic">>,
    DestinationX = <<"destination">>,
    Q = <<"q">>,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DestinationX,
                                                                         durable = true,
                                                                         auto_delete = true}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = Q,
                                                                   durable = true}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = Q,
                                                             exchange = DestinationX,
                                                             routing_key = <<"a.b">>}),
    #'exchange.bind_ok'{} = amqp_channel:call(Ch, #'exchange.bind'{destination = DestinationX,
                                                                   source = SourceX,
                                                                   routing_key = <<"*.b">>}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = Q,
                                                             exchange = SourceX,
                                                             routing_key = <<"a.b">>}),
    C = connect(?FUNCTION_NAME, Config),
    %% Message should be routed as follows:
    %% SourceX -> DestinationX -> Q and
    %% SourceX -> Q
    {ok, _} = emqtt:publish(C, <<"a/b">>, <<"msg">>, qos1),
    %% However, since we publish only one time a single message and have a single destination queue,
    %% we expect only one copy of the message to end up in the destination queue.
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg">>}},
                             amqp_channel:call(Ch, #'basic.get'{queue = Q}))),
    timer:sleep(10),
    ?assertEqual(#'queue.delete_ok'{message_count = 0},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
    ok = emqtt:disconnect(C),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

notify_consumer_qos0_queue_deleted(Config) ->
    Topic = atom_to_binary(?FUNCTION_NAME),
    notify_consumer_queue_deleted(Config, Topic, <<"MQTT QoS 0">>, [{retry_interval, 1}], qos0).

notify_consumer_classic_queue_deleted(Config) ->
    Topic = atom_to_binary(?FUNCTION_NAME),
    notify_consumer_queue_deleted(Config, Topic, <<"classic">>, non_clean_sess_opts(), qos0).

notify_consumer_quorum_queue_deleted(Config) ->
    set_durable_queue_type(Config),
    Topic = atom_to_binary(?FUNCTION_NAME),
    notify_consumer_queue_deleted(Config, Topic, <<"quorum">>, non_clean_sess_opts(), qos1),
    unset_durable_queue_type(Config).

notify_consumer_queue_deleted(Config, Name = Topic, ExpectedType, ConnOpts, Qos) ->
    C = connect(Name, Config, ConnOpts),
    {ok, _, _} = emqtt:subscribe(C, Topic, Qos),
    {ok, #{reason_code_name := success}} = emqtt:publish(C, Name, <<"m1">>, qos1),
    {ok, #{reason_code_name := success}} = emqtt:publish(C, Name, <<"m2">>, qos1),
    ok = expect_publishes(C, Topic, [<<"m1">>, <<"m2">>]),

    [[QName, Type]] = rabbitmqctl_list(Config, 0, ["list_queues", "name", "type", "--no-table-headers"]),
    ?assertMatch(ExpectedType, Type),

    process_flag(trap_exit, true),
    {ok, _} = rabbitmqctl(Config, 0, ["delete_queue", QName]),

    await_exit(C).

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
    after ?TIMEOUT ->
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
    after ?TIMEOUT ->
              ct:fail("~b confirms are missing", [Left])
    end.

await_consumer_count(ConsumerCount, ClientId, QoS, Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    QueueName = rabbit_mqtt_util:queue_name_bin(
                  rabbit_data_coercion:to_binary(ClientId), QoS),
    eventually(
      ?_assertMatch(
         #'queue.declare_ok'{consumer_count = ConsumerCount},
         amqp_channel:call(Ch, #'queue.declare'{queue = QueueName,
                                                passive = true})), 500, 10),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

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

assert_v5_disconnect_reason_code(Config, ReasonCode) ->
    case ?config(mqtt_version, Config) of
        v3 -> ok;
        v4 -> ok;
        v5 -> receive {disconnected, ReasonCode, _Props} -> ok
              after ?TIMEOUT -> ct:fail("missing DISCONNECT packet from server")
              end
    end.

set_durable_queue_type(Config) ->
    ok = rpc(Config, application, set_env, [rabbitmq_mqtt, durable_queue_type, quorum]).

unset_durable_queue_type(Config) ->
    ok = rpc(Config, application, unset_env, [rabbitmq_mqtt, durable_queue_type]).
