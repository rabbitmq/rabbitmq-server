%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(reader_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                block,
                                block_connack_timeout,
                                handle_invalid_frames,
                                login_timeout,
                                stats,
                                quorum_session_false,
                                quorum_session_true,
                                classic_session_true,
                                classic_session_false
      ]}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(Config,
                                    {rabbit, [
                                              {collect_statistics, basic},
                                              {collect_statistics_interval, 100}
                                             ]}).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                               tcp_port_mqtt_tls_extra]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      [ fun merge_app_env/1 ] ++
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

block(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {port, P},
                                {clientid, <<"simpleClient">>},
                                {proto_ver, v4}]),
    {ok, _Properties} = emqtt:connect(C),

    %% Only here to ensure the connection is really up
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>),
    ok = emqtt:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),
    {ok, _, _} = emqtt:unsubscribe(C, <<"TopicA">>),

    {ok, _, _} = emqtt:subscribe(C, <<"Topic1">>),
    {ok, _} = publish_qos1(C, <<"Topic1">>, <<"Not blocked yet">>),

    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.00000001]),
    % %% Let it block
    timer:sleep(100),

    %% Blocked, but still will publish
    puback_timeout = publish_qos1(C, <<"Topic1">>, <<"Now blocked">>),
    puback_timeout = publish_qos1(C, <<"Topic1">>, <<"Still blocked">>),

    %% Unblock
    rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.4]),
    expect_publishes(<<"Topic1">>, [<<"Not blocked yet">>,
                                    <<"Now blocked">>,
                                    <<"Still blocked">>]),
    ok = emqtt:disconnect(C).

block_connack_timeout(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    Ports0 = rpc(Config, erlang, ports, []),

    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.00000001]),
    %% Let connection block.
    timer:sleep(100),

    %% We can still connect via TCP, but CONNECT frame will not be processed on the server.
    {ok, Client} = emqtt:start_link([{host, "localhost"},
                                     {port, P},
                                     {clientid, <<"simpleClient">>},
                                     {proto_ver, v4},
                                     {connect_timeout, 1}]),
    unlink(Client),
    ClientMRef = monitor(process, Client),
    {error, connack_timeout} = emqtt:connect(Client),
    receive
        {'DOWN', ClientMRef, process, Client, connack_timeout} ->
            ok
    after 200 ->
              ct:fail("missing connack_timeout in client")
    end,

    Ports = rpc(Config, erlang, ports, []),
    %% Server creates 1 new port to handle our MQTT connection.
    [NewPort] = Ports -- Ports0,
    {connected, MqttReader} = rpc(Config, erlang, port_info, [NewPort, connected]),
    MqttReaderMRef = monitor(process, MqttReader),

    %% Unblock connection. CONNECT frame will be processed on the server.
    rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.4]),

    receive
        {'DOWN', MqttReaderMRef, process, MqttReader, {shutdown, {socket_ends, einval}}} ->
            %% We expect that MQTT reader process exits (without crashing)
            %% because our client already disconnected.
            ok
    after 2000 ->
              ct:fail("missing peername_not_known from server")
    end,
    %% Ensure that our client is not registered.
    [] = rpc(Config, rabbit_mqtt_collector, list, []),
    ok.

handle_invalid_frames(Config) ->
    N = rpc(Config, ets, info, [connection_metrics, size]),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, C} = gen_tcp:connect("localhost", P, []),
    Bin = <<"GET / HTTP/1.1\r\nHost: www.rabbitmq.com\r\nUser-Agent: curl/7.43.0\r\nAccept: */*">>,
    gen_tcp:send(C, Bin),
    gen_tcp:close(C),
    %% No new stats entries should be inserted as connection never got to initialize
    N = rpc(Config, ets, info, [connection_metrics, size]).

login_timeout(Config) ->
    rpc(Config, application, set_env, [rabbitmq_mqtt, login_timeout, 400]),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, C} = gen_tcp:connect("localhost", P, [{active, false}]),

    try
        {error, closed} = gen_tcp:recv(C, 0, 500)
    after
        rpc(Config, application, unset_env, [rabbitmq_mqtt, login_timeout])
    end.

stats(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    %% CMN = rpc(Config, ets, info, [connection_metrics, size]),
    %% CCMN = rpc(Config, ets, info, [connection_coarse_metrics, size]),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {port, P},
                                {clientid, <<"simpleClient">>},
                                {proto_ver, v4}]),
    {ok, _Properties} = emqtt:connect(C),
    %% Ensure that there are some stats
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>, qos0),
    ok = emqtt:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),
    {ok, _, _} = emqtt:unsubscribe(C, <<"TopicA">>),
    timer:sleep(1000), %% Wait for stats to be emitted, which it does every 100ms
    %% Retrieve the connection Pid
    [{_, Reader}] = rpc(Config, rabbit_mqtt_collector, list, []),
    [{_, Pid}] = rpc(Config, rabbit_mqtt_reader, info, [Reader, [connection]]),
    %% Verify the content of the metrics, garbage_collection must be present
    [{Pid, Props}] = rpc(Config, ets, lookup, [connection_metrics, Pid]),
    true = proplists:is_defined(garbage_collection, Props),
    %% If the coarse entry is present, stats were successfully emitted
    [{Pid, _, _, _, _}] = rpc(Config, ets, lookup,
                              [connection_coarse_metrics, Pid]),
    ok = emqtt:disconnect(C).

get_durable_queue_type(Server, Q0) ->
    QNameRes = rabbit_misc:r(<<"/">>, queue, Q0),
    {ok, Q1} = rpc:call(Server, rabbit_amqqueue, lookup, [QNameRes]),
    amqqueue:get_type(Q1).

set_env(QueueType) ->
    application:set_env(rabbitmq_mqtt, durable_queue_type, QueueType).

get_env() ->
    rabbit_mqtt_util:env(durable_queue_type).


validate_durable_queue_type(Config, ClientName, CleanSession, Expected) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {port, P},
                                {clean_start, CleanSession},
                                {clientid, ClientName},
                                {proto_ver, v4}]),
    {ok, _Properties} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicB">>, qos1),
    ok = emqtt:publish(C, <<"TopicB">>, <<"Payload">>),
    expect_publishes(<<"TopicB">>, [<<"Payload">>]),
    {ok, _, _} = emqtt:unsubscribe(C, <<"TopicB">>),
    Prefix = <<"mqtt-subscription-">>,
    Suffix = <<"qos1">>,
    Q= <<Prefix/binary, ClientName/binary, Suffix/binary>>,
    ?assertEqual(Expected,get_durable_queue_type(Server,Q)),
    timer:sleep(500),
    ok = emqtt:disconnect(C).

%% quorum queue test when enable
quorum_session_false(Config) ->
  %%  test if the quorum queue is enable after the setting
    Default = rpc(Config, reader_SUITE, get_env, []),
    rpc(Config, reader_SUITE, set_env, [quorum]),
    validate_durable_queue_type(Config, <<"qCleanSessionFalse">>, false, rabbit_quorum_queue),
    rpc(Config, reader_SUITE, set_env, [Default]).

quorum_session_true(Config) ->
  %%  in case clean session == true must be classic since quorum
  %% doesn't support auto-delete
    Default = rpc(Config, reader_SUITE, get_env, []),
    rpc(Config, reader_SUITE, set_env, [quorum]),
    validate_durable_queue_type(Config, <<"qCleanSessionTrue">>, true, rabbit_classic_queue),
    rpc(Config, reader_SUITE, set_env, [Default]).

classic_session_true(Config) ->
  %%  with default configuration the queue is classic
    validate_durable_queue_type(Config, <<"cCleanSessionTrue">>, true, rabbit_classic_queue).

classic_session_false(Config) ->
  %%  with default configuration the queue is classic
    validate_durable_queue_type(Config, <<"cCleanSessionFalse">>, false, rabbit_classic_queue).


<<<<<<< HEAD
expect_publishes(_Topic, []) -> ok;
expect_publishes(Topic, [Payload|Rest]) ->
=======
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, event_recorder),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = gen_event:add_handler({rabbit_event, Server}, event_recorder, []),

    ?assertMatch({error, _}, emqtt:connect(C)),

    [E] = util:get_events(Server),
    util:assert_event_type(user_authentication_failure, E),
    util:assert_event_prop([{name, <<"Trudy">>},
                            {connection_type, network}],
                           E),

    ok = gen_event:delete_handler({rabbit_event, Server}, event_recorder, []).

%% Test that queue type rabbit_mqtt_qos0_queue drops QoS 0 messages when its
%% max length is reached.
rabbit_mqtt_qos0_queue_overflow(Config) ->
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, rabbit_mqtt_qos0_queue),

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
    ct:pal("NumReceived=~b~nNumDropped=~b", [NumReceived, NumDropped]),

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

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

num_received(Topic, Payload, N) ->
>>>>>>> d670a7c50e (Make test rabbit_mqtt_qos0_queue_overflow less flaky)
    receive
        {publish, #{topic := Topic,
                    payload := Payload}} ->
            expect_publishes(Topic, Rest)
    after 5000 ->
              throw({publish_not_delivered, Payload})
    end.

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).

publish_qos1(Client, Topic, Payload) ->
    Mref = erlang:monitor(process, Client),
    ok = emqtt:publish_async(Client, Topic, #{}, Payload, [{qos, 1}], infinity,
                             {fun ?MODULE:sync_publish_result/3, [self(), Mref]}),
    receive
        {Mref, Reply} ->
            erlang:demonitor(Mref, [flush]),
            Reply;
        {'DOWN', Mref, process, Client, Reason} ->
            ct:fail("client is down: ~p", [Reason])
    after
        1000 ->
            erlang:demonitor(Mref, [flush]),
            puback_timeout
    end.

sync_publish_result(Caller, Mref, Result) ->
    erlang:send(Caller, {Mref, Result}).
