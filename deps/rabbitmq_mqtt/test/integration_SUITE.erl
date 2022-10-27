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
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [], [events,
                           event_authentication_failure]}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 1}]),
    init_per_group0(Config).

init_per_group0(Config0) ->
    Config = rabbit_ct_helpers:set_config(
               Config0,
               [{rmq_nodename_suffix, ?MODULE},
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

events(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                                {port, P},
                                {clientid, ClientId},
                                {proto_ver, v4}]),
    true = unlink(C),

    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, event_recorder),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = gen_event:add_handler({rabbit_event, Server}, event_recorder, []),

    {ok, _Properties} = emqtt:connect(C),

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
