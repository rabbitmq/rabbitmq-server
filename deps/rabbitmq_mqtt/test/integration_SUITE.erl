%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(integration_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(rabbit_ct_broker_helpers, [rabbitmqctl_list/3]).
-import(util, [all_connection_pids/1,
               publish_qos1/4]).

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [], tests()},
     {cluster_size_3, [], tests()}
    ].

tests() ->
    [publish_to_all_queue_types_qos0
     ,publish_to_all_queue_types_qos1
     ,events
     ,event_authentication_failure
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

init_per_group(cluster_size_1 = Group, Config0) ->
    init_per_group0(Group,
                    rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 1}]));
init_per_group(cluster_size_3 = Group, Config0) ->
    init_per_group0(Group,
                    rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 3}])).

init_per_group0(Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{rmq_nodename_suffix, Group},
                {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                                       tcp_port_mqtt_tls_extra]}]),
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

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

publish_to_all_queue_types_qos0(Config) ->
    publish_to_all_queue_types(Config, qos0).

publish_to_all_queue_types_qos1(Config) ->
    publish_to_all_queue_types(Config, qos1).

publish_to_all_queue_types(Config, QoS) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

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

    {C, _} = connect(?FUNCTION_NAME, Config),
    lists:foreach(fun(_N) ->
                          case QoS of
                              qos0 ->
                                  ok = emqtt:publish(C, Topic, <<"hello">>);
                              qos1 ->
                                  {ok, _} = publish_qos1(C, Topic, <<"hello">>, 1000)
                          end
                  end, lists:seq(1, 2000)),
    Expected = lists:sort([[CQ,  <<"2000">>],
                           [CMQ, <<"2000">>],
                           [QQ,  <<"2000">>],
                           [SQ,  <<"2000">>]
                          ]),
    ?awaitMatch(Expected,
                lists:sort(rabbitmqctl_list(Config, 0, ["list_queues", "--no-table-headers",
                                                        "name", "messages_ready"])),
                20_000, 1000),

    delete_queue(Ch, [CQ, CMQ, QQ, SQ]),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, CMQ),
    ok = emqtt:disconnect(C),
    ?awaitMatch([], all_connection_pids(Config), 10_000, 1000).

%%TODO add test where target quorum queue rejects message

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
    assert_event_prop({protocol, {'MQTT', "3.1.1"}}, E1),

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

%% -------------------------------------------------------------------
%% Internal helpers
%% -------------------------------------------------------------------

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
