%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(reader_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(rabbit_ct_broker_helpers, [rpc/4]).
-import(rabbit_ct_helpers, [eventually/3]).
-import(util, [all_connection_pids/1,
               publish_qos1_timeout/4,
               expect_publishes/3,
               connect/2, connect/3,
               await_exit/1,
               non_clean_sess_opts/0
              ]).

all() ->
    [
     {group, v4},
     {group, v5}
    ].

groups() ->
    [
     {v4, [shuffle], tests()},
     {v5, [shuffle], tests()}
    ].

tests() ->
    [
     block_connack_timeout,
     handle_invalid_packets,
     login_timeout,
     stats,
     quorum_clean_session_false,
     quorum_clean_session_true,
     classic_clean_session_true,
     classic_clean_session_false,
     event_authentication_failure,
     rabbit_mqtt_qos0_queue_overflow
    ].

suite() ->
    [{timetrap, {minutes, 3}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      Config, {rabbit, [{collect_statistics, basic},
                        {collect_statistics_interval, 100}
                       ]}).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodename_suffix, ?MODULE},
                 {start_rmq_with_plugins_disabled, true}
                ]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                [fun merge_app_env/1] ++
                    rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config2, rabbitmq_mqtt),
    Config2.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(Group, Config) ->
    rabbit_ct_helpers:set_config(Config, {mqtt_version, Group}).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

block_connack_timeout(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    Ports = rpc(Config, erlang, ports, []),

    DefaultWatermark = rpc(Config, vm_memory_monitor, get_vm_memory_high_watermark, []),
    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0]),
    %% Let connection block.
    timer:sleep(100),

    %% We can still connect via TCP, but CONNECT packet will not be processed on the server.
    {ok, Client} = emqtt:start_link([{host, "localhost"},
                                     {port, P},
                                     {clientid, atom_to_binary(?FUNCTION_NAME)},
                                     {proto_ver, ?config(mqtt_version, Config)},
                                     {connect_timeout, 1}]),
    unlink(Client),
    ClientMRef = monitor(process, Client),
    {error, connack_timeout} = emqtt:connect(Client),
    receive {'DOWN', ClientMRef, process, Client, connack_timeout} -> ok
    after 30_000 -> ct:fail("missing connack_timeout in client")
    end,

    MqttReader = rpc(Config, ?MODULE, mqtt_connection_pid, [Ports]),
    MqttReaderMRef = monitor(process, MqttReader),

    %% Unblock connection. CONNECT packet will be processed on the server.
    rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [DefaultWatermark]),

    receive {'DOWN', MqttReaderMRef, process, MqttReader, {shutdown, {socket_ends, einval}}} ->
                %% We expect that MQTT reader process exits (without crashing)
                %% because our client already disconnected.
                ok
    after 30_000 -> ct:fail("missing peername_not_known from server")
    end,
    %% Ensure that our client is not registered.
    ?assertEqual([], all_connection_pids(Config)),
    ok.

mqtt_connection_pid(ExistingPorts) ->
    NewPorts = erlang:ports() -- ExistingPorts,
    %% Server creates 1 new TCP port to handle our MQTT connection.
    [MqttConnectionPort] = lists:filter(fun(P) ->
                                                erlang:port_info(P, name) =:= {name, "tcp_inet"}
                                        end, NewPorts),
    {connected, MqttConnectionPid} = erlang:port_info(MqttConnectionPort, connected),
    MqttConnectionPid.

handle_invalid_packets(Config) ->
    N = rpc(Config, ets, info, [connection_metrics, size]),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, C} = gen_tcp:connect("localhost", P, []),
    Bin = <<"GET / HTTP/1.1\r\nHost: www.rabbitmq.com\r\nUser-Agent: curl/7.43.0\r\nAccept: */*">>,
    gen_tcp:send(C, Bin),
    gen_tcp:close(C),
    %% Wait for stats being emitted (every 100ms)
    timer:sleep(300),
    %% No new stats entries should be inserted as connection never got to initialize
    ?assertEqual(N, rpc(Config, ets, info, [connection_metrics, size])).

login_timeout(Config) ->
    App = rabbitmq_mqtt,
    Par = ?FUNCTION_NAME,
    ok = rpc(Config, application, set_env, [App, Par, 400]),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, Socket} = gen_tcp:connect("localhost", Port, [{active, false}]),
    ?assertEqual({error, closed}, gen_tcp:recv(Socket, 0, 500)),
    ok = rpc(Config, application, unset_env, [App, Par]).

stats(Config) ->
    C = connect(?FUNCTION_NAME, Config),
    %% Wait for stats being emitted (every 100ms)
    timer:sleep(300),
    %% Retrieve the connection Pid
    [Pid] = all_connection_pids(Config),
    [{pid, Pid}] = rpc(Config, rabbit_mqtt_reader, info, [Pid, [pid]]),
    %% Verify the content of the metrics, garbage_collection must be present
    [{Pid, Props}] = rpc(Config, ets, lookup, [connection_metrics, Pid]),
    true = proplists:is_defined(garbage_collection, Props),
    %% If the coarse entry is present, stats were successfully emitted
    [{Pid, _, _, _, _}] = rpc(Config, ets, lookup,
                              [connection_coarse_metrics, Pid]),
    ok = emqtt:disconnect(C).

get_durable_queue_type(Server, QNameBin) ->
    QName = rabbit_misc:r(<<"/">>, queue, QNameBin),
    {ok, Q} = rpc:call(Server, rabbit_amqqueue, lookup, [QName]),
    amqqueue:get_type(Q).

set_env(QueueType) ->
    application:set_env(rabbitmq_mqtt, durable_queue_type, QueueType).

get_env() ->
    rabbit_mqtt_util:env(durable_queue_type).

validate_durable_queue_type(Config, ClientName, Opts, ExpectedQueueType) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    C = connect(ClientName, Config, Opts),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicB">>, qos1),
    ok = emqtt:publish(C, <<"TopicB">>, <<"Payload">>),
    ok = expect_publishes(C, <<"TopicB">>, [<<"Payload">>]),
    {ok, _, _} = emqtt:unsubscribe(C, <<"TopicB">>),
    Prefix = <<"mqtt-subscription-">>,
    Suffix = <<"qos1">>,
    QNameBin = <<Prefix/binary, ClientName/binary, Suffix/binary>>,
    ?assertEqual(ExpectedQueueType, get_durable_queue_type(Server, QNameBin)),
    ok = emqtt:disconnect(C).

quorum_clean_session_false(Config) ->
    Default = rpc(Config, reader_SUITE, get_env, []),
    rpc(Config, reader_SUITE, set_env, [quorum]),
    validate_durable_queue_type(
      Config, <<"quorumCleanSessionFalse">>, non_clean_sess_opts(), rabbit_quorum_queue),
    rpc(Config, reader_SUITE, set_env, [Default]).

quorum_clean_session_true(Config) ->
    Default = rpc(Config, reader_SUITE, get_env, []),
    rpc(Config, reader_SUITE, set_env, [quorum]),
    %% Since we use a clean session and quorum queues cannot be auto-delete or exclusive,
    %% we expect a classic queue.
    validate_durable_queue_type(
      Config, <<"quorumCleanSessionTrue">>, [{clean_start, true}], rabbit_classic_queue),
    rpc(Config, reader_SUITE, set_env, [Default]).

classic_clean_session_true(Config) ->
    validate_durable_queue_type(
      Config, <<"classicCleanSessionTrue">>, [{clean_start, true}], rabbit_classic_queue).

classic_clean_session_false(Config) ->
    validate_durable_queue_type(
      Config, <<"classicCleanSessionFalse">>, non_clean_sess_opts(), rabbit_classic_queue).

event_authentication_failure(Config) ->
    {ok, C} = emqtt:start_link(
                [{username, <<"Trudy">>},
                 {password, <<"fake-password">>},
                 {host, "localhost"},
                 {port, rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt)},
                 {clientid, atom_to_binary(?FUNCTION_NAME)},
                 {proto_ver, ?config(mqtt_version, Config)}]),
    true = unlink(C),

    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, event_recorder),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = gen_event:add_handler({rabbit_event, Server}, event_recorder, []),

    ?assertMatch({error, _}, emqtt:connect(C)),

    [E | _] = util:get_events(Server, user_authentication_failure),
    util:assert_event_type(user_authentication_failure, E),
    util:assert_event_prop([{name, <<"Trudy">>},
                            {connection_type, network}],
                           E),

    ok = gen_event:delete_handler({rabbit_event, Server}, event_recorder, []).

%% Test that queue type rabbit_mqtt_qos0_queue drops QoS 0 messages when its
%% max length is reached.
rabbit_mqtt_qos0_queue_overflow(Config) ->
    ProtoVer = case ?config(mqtt_version, Config) of
                   v4 -> mqtt311;
                   v5 -> mqtt50
               end,
    QType = rabbit_mqtt_qos0_queue,

    #{
      [{protocol, ProtoVer}, {queue_type, QType}] :=
      #{messages_delivered_total := 0,
        messages_delivered_consume_auto_ack_total := 0},

      [{queue_type, QType}, {dead_letter_strategy, disabled}] :=
      #{messages_dead_lettered_maxlen_total := NumDeadLettered}
     } = rabbit_ct_broker_helpers:rpc(Config, rabbit_global_counters, overview, []),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Msg = binary:copy(<<"x">>, 4000),
    NumMsgs = 10_000,

    %% Provoke TCP back-pressure from client to server by using very small buffers.
    Opts = [{tcp_opts, [{recbuf, 256},
                        {buffer, 256}]}],
    Sub = connect(<<"subscriber">>, Config, Opts),
    {ok, _, [0]} = emqtt:subscribe(Sub, Topic, qos0),
    [ServerConnectionPid] = all_connection_pids(Config),

    %% Suspend the receiving client such that it stops reading from its socket
    %% causing TCP back-pressure to the server being applied.
    true = erlang:suspend_process(Sub),

    %% Let's overflow the receiving server MQTT connection process
    %% (i.e. the rabbit_mqtt_qos0_queue) by sending many large messages.
    Pub = connect(<<"publisher">>, Config),
    lists:foreach(fun(_) ->
                          ok = emqtt:publish(Pub, Topic, Msg, qos0)
                  end, lists:seq(1, NumMsgs)),

    %% Give the server some time to process (either send or drop) the messages.
    timer:sleep(2500),

    %% Let's resume the receiving client to receive any remaining messages that did
    %% not get dropped.
    true = erlang:resume_process(Sub),
    NumReceived = num_received(Topic, Msg, 0),

    {status, _, _, [_, _, _, _, Misc]} = sys:get_status(ServerConnectionPid),
    [State] = [S || {data, [{"State", S}]} <- Misc],
    #{proc_state := #{qos0_messages_dropped := NumDropped}} = State,

    ct:pal("NumReceived=~b NumDropped=~b", [NumReceived, NumDropped]),

    %% We expect that
    %% 1. all sent messages were either received or dropped
    ?assertEqual(NumMsgs, NumReceived + NumDropped),
    %% 2. at least one message was dropped (otherwise our whole test case did not
    %%    test what it was supposed to test: that messages are dropped due to the
    %%    server being overflowed with messages while the client receives too slowly)
    ?assert(NumDropped >= 1),
    %% 3. we received at least 200 messages because everything below the default
    %% of mailbox_soft_limit=200 should not be dropped
    ?assert(NumReceived >= 200),

    %% Assert that Prometheus metrics counted correctly.
    ExpectedNumDeadLettered = NumDeadLettered + NumDropped,
    ?assertMatch(
       #{
         [{protocol, ProtoVer}, {queue_type, QType}] :=
         #{messages_delivered_total := NumReceived,
           messages_delivered_consume_auto_ack_total := NumReceived},

         [{queue_type, QType}, {dead_letter_strategy, disabled}] :=
         #{messages_dead_lettered_maxlen_total := ExpectedNumDeadLettered}
        },
       rabbit_ct_broker_helpers:rpc(Config, rabbit_global_counters, overview, [])),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

num_received(Topic, Payload, N) ->
    receive
        {publish, #{topic := Topic,
                    payload := Payload}} ->
            num_received(Topic, Payload, N + 1)
    after 3000 ->
              N
    end.
