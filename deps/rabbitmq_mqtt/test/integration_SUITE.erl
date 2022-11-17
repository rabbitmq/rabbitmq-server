%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(integration_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(rabbit_ct_broker_helpers, [rabbitmqctl_list/3,
                                   rpc_all/4]).
-import(rabbit_ct_helpers, [eventually/3,
                            eventually/1]).
-import(util, [all_connection_pids/1,
               get_global_counters/2,
               get_global_counters/3,
               expect_publishes/2]).

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [],
      [
       %% separate RMQ so global counters start from 0
       {global_counters, [], [global_counters_v3, global_counters_v4]},
       {common_tests, [], common_tests()}
      ]},
     {cluster_size_3, [],
      [queue_down_qos1,
       consuming_classic_mirrored_queue_down,
       consuming_classic_queue_down] ++
      common_tests() ++
      [flow_classic_mirrored_queue,
       flow_quorum_queue,
       flow_stream]
     }
    ].

common_tests() ->
    [delete_create_queue
     ,quorum_queue_rejects
     ,publish_to_all_queue_types_qos0
     ,publish_to_all_queue_types_qos1
     ,events
     ,event_authentication_failure
     ,internal_event_handler
     ,non_clean_sess_disconnect
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

init_per_group(cluster_size_1, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 1}]);
init_per_group(cluster_size_3 = Group, Config) ->
    init_per_group0(Group,
                    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]));

init_per_group(Group, Config)
  when Group =:= global_counters orelse
       Group =:= common_tests ->
    init_per_group0(Group,Config).

init_per_group0(Group, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config0,
                [{rmq_nodename_suffix, Group},
                 {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                                        tcp_port_mqtt_tls_extra]}]),
    Config = rabbit_ct_helpers:run_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    Result = rpc_all(Config, application, set_env, [rabbit, classic_queue_default_version, 2]),
    ?assert(lists:all(fun(R) -> R =:= ok end, Result)),
    Config.

end_per_group(cluster_size_1, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

quorum_queue_rejects(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Name = atom_to_binary(?FUNCTION_NAME),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"qq-policy">>, Name, <<"queues">>, [{<<"max-length">>, 1},
                                                            {<<"overflow">>, <<"reject-publish">>}]),
    declare_queue(Ch, Name, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    bind(Ch, Name, Name),

    {C, _} = connect(Name, Config, [{retry_interval, 1}]),
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
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

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
    {C, _} = connect(?FUNCTION_NAME, Config, [{retry_interval, 2}]),
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
                all_connection_pids(Config), 10_000, 1000),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

flow_classic_mirrored_queue(Config) ->
    QueueName = <<"flow">>,
    ok = rabbit_ct_broker_helpers:set_ha_policy(Config, 0, QueueName, <<"all">>),
    flow(Config, [rabbit, credit_flow_default_credit, {2, 1}], <<"classic">>),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, QueueName).

flow_quorum_queue(Config) ->
    flow(Config, [rabbit, quorum_commands_soft_limit, 1], <<"quorum">>).

flow_stream(Config) ->
    flow(Config, [rabbit, stream_messages_soft_limit, 1], <<"stream">>).

flow(Config, Env, QueueType)
  when is_list(Env), is_binary(QueueType) ->
    Result = rpc_all(Config, application, set_env, Env),
    ?assert(lists:all(fun(R) -> R =:= ok end, Result)),

    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QueueName = Topic = atom_to_binary(?FUNCTION_NAME),
    declare_queue(Ch, QueueName, [{<<"x-queue-type">>, longstr, QueueType}]),
    bind(Ch, QueueName, Topic),

    NumMsgs = 1000,
    {C, _} = connect(?FUNCTION_NAME, Config, [{retry_interval, 600},
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
    ok = await_confirms(C, 1, NumMsgs),
    eventually(?_assertEqual(
                  [[integer_to_binary(NumMsgs)]],
                  rabbitmqctl_list(Config, 0, ["list_queues", "messages", "--no-table-headers"])
                 ), 1000, 10),

    delete_queue(Ch, QueueName),
    ok = emqtt:disconnect(C),
    ?awaitMatch([],
                all_connection_pids(Config), 10_000, 1000),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

events(Config) ->
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, event_recorder),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = gen_event:add_handler({rabbit_event, Server}, event_recorder, []),

    ClientId = atom_to_binary(?FUNCTION_NAME),
    {C, _} = connect(ClientId, Config),

    [E0, E1] = get_events(Server),
    assert_event_type(user_authentication_success, E0),
    assert_event_prop([{name, <<"guest">>},
                       {connection_type, network}],
                      E0),
    assert_event_type(connection_created, E1),
    [ConnectionPid] = all_connection_pids(Config),
    ExpectedConnectionProps = [{protocol, {'MQTT', "3.1.1"}},
                               {node, Server},
                               {vhost, <<"/">>},
                               {user, <<"guest">>},
                               {pid, ConnectionPid}],
    assert_event_prop(ExpectedConnectionProps, E1),

    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>, qos0),

    [E2, E3] = get_events(Server),
    assert_event_type(queue_created, E2),
    QueueNameBin = <<"mqtt-subscription-", ClientId/binary, "qos0">>,
    QueueName = {resource, <<"/">>, queue, QueueNameBin},
    assert_event_prop([{name, QueueName},
                       {durable, true},
                       {auto_delete, false},
                       {exclusive, true},
                       {type, rabbit_mqtt_qos0_queue},
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

    [E4] = get_events(Server),
    assert_event_type(binding_deleted, E4),

    ok = emqtt:disconnect(C),

    [E5, E6] = get_events(Server),
    assert_event_type(connection_closed, E5),
    assert_event_prop(ExpectedConnectionProps, E5),
    assert_event_type(queue_deleted, E6),
    assert_event_prop({name, QueueName}, E6),

    ok = gen_event:delete_handler({rabbit_event, Server}, event_recorder, []).

event_authentication_failure(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{username, <<"Trudy">>},
                                {password, <<"fake-password">>},
                                {host, "localhost"},
                                {port, P},
                                {clientid, ClientId},
                                {proto_ver, v4}]),
    true = unlink(C),

    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, event_recorder),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = gen_event:add_handler({rabbit_event, Server}, event_recorder, []),

    ?assertMatch({error, _}, emqtt:connect(C)),

    [E, _ConnectionClosedEvent] = get_events(Server),
    assert_event_type(user_authentication_failure, E),
    assert_event_prop([{name, <<"Trudy">>},
                       {connection_type, network}],
                      E),

    ok = gen_event:delete_handler({rabbit_event, Server}, event_recorder, []).

internal_event_handler(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = gen_event:call({rabbit_event, Server}, rabbit_mqtt_internal_event_handler, ignored_request, 1000).

global_counters_v3(Config) ->
    global_counters(Config, v3).

global_counters_v4(Config) ->
    global_counters(Config, v4).

global_counters(Config, ProtoVer) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {port, Port},
                                {proto_ver, ProtoVer},
                                {clientid, atom_to_binary(?FUNCTION_NAME)}]),
    {ok, _Properties} = emqtt:connect(C),

    Topic0 = <<"test-topic0">>,
    Topic1 = <<"test-topic1">>,
    {ok, _, [1]} = emqtt:subscribe(C, Topic0, qos1),
    {ok, _, [1]} = emqtt:subscribe(C, Topic1, qos1),
    {ok, _} = emqtt:publish(C, Topic0, <<"testm">>, qos1),
    {ok, _} = emqtt:publish(C, Topic1, <<"testm">>, qos1),

    ?assertEqual(#{publishers => 1,
                   consumers => 1,
                   messages_confirmed_total => 2,
                   messages_received_confirm_total => 2,
                   messages_received_total => 2,
                   messages_routed_total => 2,
                   messages_unroutable_dropped_total => 0,
                   messages_unroutable_returned_total => 0},
                 get_global_counters(Config, ProtoVer)),

    {ok, _, _} = emqtt:unsubscribe(C, Topic1),
    ?assertEqual(1, maps:get(consumers, get_global_counters(Config, ProtoVer))),

    ok = emqtt:disconnect(C),
    ?assertEqual(#{publishers => 0,
                   consumers => 0,
                   messages_confirmed_total => 2,
                   messages_received_confirm_total => 2,
                   messages_received_total => 2,
                   messages_routed_total => 2,
                   messages_unroutable_dropped_total => 0,
                   messages_unroutable_returned_total => 0},
                 get_global_counters(Config, ProtoVer)).

queue_down_qos1(Config) ->
    {Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 1),
    CQ = Topic = atom_to_binary(?FUNCTION_NAME),
    declare_queue(Ch1, CQ, []),
    bind(Ch1, CQ, Topic),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn1, Ch1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 1),

    {C, _} = connect(?FUNCTION_NAME, Config, [{retry_interval, 2}]),
    %% classic queue is down, therefore message is rejected
    ?assertEqual(puback_timeout, util:publish_qos1_timeout(C, Topic, <<"msg">>, 500)),

    ok = rabbit_ct_broker_helpers:start_node(Config, 1),
    %% classic queue is up, therefore message should arrive
    eventually(?_assertEqual([[<<"1">>]],
                             rabbitmqctl_list(Config, 1, ["list_queues", "messages", "--no-table-headers"])),
               500, 20),

    {Conn0, Ch0} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    delete_queue(Ch0, CQ),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn0, Ch0),
    ok = emqtt:disconnect(C).

%% Even though classic mirrored queues are deprecated, we know that some users have set up
%% a policy to mirror MQTT queues. So, we need to support that use case in RabbitMQ 3.x
%% and failover consumption when the classic mirrored queue leader fails.
consuming_classic_mirrored_queue_down(Config) ->
    [Server1, Server2, _Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Topic = PolicyName = atom_to_binary(?FUNCTION_NAME),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, Server1, PolicyName, <<".*">>, <<"queues">>,
           [{<<"ha-mode">>, <<"all">>},
            {<<"queue-master-locator">>, <<"client-local">>}]),

    %% Declare queue leader on Server1.
    {C1, _} = connect(?FUNCTION_NAME, Config, [{clean_start, false}]),
    {ok, _, _} = emqtt:subscribe(C1, Topic, qos1),
    ok = emqtt:disconnect(C1),

    %% Consume from Server2.
    Options = [{host, "localhost"},
               {port, rabbit_ct_broker_helpers:get_node_config(Config, Server2, tcp_port_mqtt)},
               {clientid, atom_to_binary(?FUNCTION_NAME)},
               {proto_ver, v4}],
    {ok, C2} = emqtt:start_link([{clean_start, false} | Options]),
    {ok, _} = emqtt:connect(C2),

    %% Sanity check that consumption works.
    {ok, _} = emqtt:publish(C2, Topic, <<"m1">>, qos1),
    ok = expect_publishes(Topic, [<<"m1">>]),

    %% Let's stop the queue leader node.
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),

    %% Consumption should continue to work.
    {ok, _} = emqtt:publish(C2, Topic, <<"m2">>, qos1),
    ok = expect_publishes(Topic, [<<"m2">>]),

    %% Cleanup
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    ?assertMatch([_Q],
                 rabbit_ct_broker_helpers:rpc(Config, Server1, rabbit_amqqueue, list, [])),
    %% "When a Client has determined that it has no further use for the session it should do a
    %% final connect with CleanSession set to 1 and then disconnect."
    {ok, C3} = emqtt:start_link([{clean_start, true} | Options]),
    {ok, _} = emqtt:connect(C3),
    ok = emqtt:disconnect(C3),
    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, Server1, rabbit_amqqueue, list, [])),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, Server1, PolicyName).

%% Consuming classic queue on a different node goes down.
consuming_classic_queue_down(Config) ->
    [Server1, Server2, _Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ClientId = Topic = atom_to_binary(?FUNCTION_NAME),

    %% Declare classic queue on Server1.
    {C1, _} = connect(?FUNCTION_NAME, Config, [{clean_start, false}]),
    {ok, _, _} = emqtt:subscribe(C1, Topic, qos1),
    ok = emqtt:disconnect(C1),

    ProtoVer = v4,
    %% Consume from Server2.
    Options = [{host, "localhost"},
               {port, rabbit_ct_broker_helpers:get_node_config(Config, Server2, tcp_port_mqtt)},
               {clientid, ClientId},
               {proto_ver, ProtoVer}],
    {ok, C2} = emqtt:start_link([{clean_start, false} | Options]),
    {ok, _} = emqtt:connect(C2),

    %%TODO uncomment below 2 lines once consumers counter works for clean_sess = false
    % ?assertMatch(#{consumers := 1},
    %              get_global_counters(Config, ProtoVer, Server2)),

    %% Let's stop the queue leader node.
    process_flag(trap_exit, true),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),

    %% When the dedicated MQTT connection (non-mirrored classic) queue goes down, it is reasonable
    %% that the server closes the MQTT connection because the MQTT client cannot consume anymore.
    eventually(?_assertMatch(#{consumers := 0},
                             get_global_counters(Config, ProtoVer, Server2))),
    receive
        {'EXIT', C2, _} ->
            ok
    after 3000 ->
              ct:fail("MQTT connection should have been closed")
    end,

    %% Cleanup
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    {ok, C3} = emqtt:start_link([{clean_start, true} | Options]),
    {ok, _} = emqtt:connect(C3),
    ok = emqtt:disconnect(C3),
    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, Server1, rabbit_amqqueue, list, [])),
    ok.

delete_create_queue(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
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
    {C, _} = connect(?FUNCTION_NAME, Config, [{retry_interval, 300}]),
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

    %% We expect all confirms in the right order (because emqtt publishes with increasing packet ID).
    %% Confirm here does not mean that messages made it ever to the deleted queues.
    ok = await_confirms(C, 1, NumMsgs),

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
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok = emqtt:disconnect(C).

non_clean_sess_disconnect(Config) ->
    {C1, _} = connect(?FUNCTION_NAME, Config, [{clean_start, false}]),
    Topic = <<"test-topic1">>,
    {ok, _, [1]} = emqtt:subscribe(C1, Topic, qos1),
    ok = emqtt:disconnect(C1),

    {C2, _} = connect(?FUNCTION_NAME, Config, [{clean_start, false}]),

    %% shouldn't receive message after unsubscribe
    {ok, _, _} = emqtt:unsubscribe(C2, Topic),
    Msg = <<"msg">>,
    {ok, _} = emqtt:publish(C2, Topic, Msg, qos1),
    {publish_not_received, Msg} = expect_publishes(Topic, [Msg]),

    %% connect with clean sess true to clean up
    {C3, _} = connect(?FUNCTION_NAME, Config, [{clean_start, true}]),
    ok = emqtt:disconnect(C3).

%% -------------------------------------------------------------------
%% Internal helpers
%% -------------------------------------------------------------------

await_confirms(_, To, To) ->
    ok;
await_confirms(From, N, To) ->
    Expected = {From, N},
    receive
        Expected ->
            await_confirms(From, N + 1, To);
        Got ->
            ct:fail("Received unexpected message. Expected: ~p Got: ~p", [Expected, Got])
    after 10_000 ->
              ct:fail("Did not receive expected message: ~p", [Expected])
    end.

connect(ClientId, Config) ->
    connect(ClientId, Config, []).

connect(ClientId, Config, AdditionalOpts) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    Options = [{host, "localhost"},
               {port, P},
               {clientid, rabbit_data_coercion:to_binary(ClientId)},
               {proto_ver, v4}
              ] ++ AdditionalOpts,
    {ok, C} = emqtt:start_link(Options),
    {ok, _Properties} = emqtt:connect(C),
    true = unlink(C),
    MRef = monitor(process, C),
    {C, MRef}.

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

get_events(Node) ->
    timer:sleep(100), %% events are sent and processed asynchronously
    Result = gen_event:call({rabbit_event, Node}, event_recorder, take_state),
    ?assert(is_list(Result)),
    Result.

assert_event_type(ExpectedType, #event{type = ActualType}) ->
    ?assertEqual(ExpectedType, ActualType).

assert_event_prop(ExpectedProp = {Key, _Value}, #event{props = Props}) ->
    ?assertEqual(ExpectedProp, lists:keyfind(Key, 1, Props));
assert_event_prop(ExpectedProps, Event)
  when is_list(ExpectedProps) ->
    lists:foreach(fun(P) ->
                          assert_event_prop(P, Event)
                  end, ExpectedProps).
