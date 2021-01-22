%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(cluster_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                connection_id_tracking,
                                connection_id_tracking_on_nodedown,
                                connection_id_tracking_with_decommissioned_node
                               ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

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
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                               tcp_port_mqtt_tls_extra]},
        {rmq_nodes_clustered, true},
        {rmq_nodes_count, 5}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      [ fun merge_app_env/1 ] ++
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

%% Note about running this testsuite in a mixed-versions cluster:
%% All even-numbered nodes will use the same code base when using a
%% secondary Umbrella. Odd-numbered nodes might use an incompatible code
%% base. When cluster-wide client ID tracking was introduced, it was not
%% put behind a feature flag because there was no need for one. Here, we
%% don't have a way to ensure that all nodes participate in client ID
%% tracking. However, those using the same code should. That's why we
%% limit our RPC calls to those nodes.
%%
%% That's also the reason why we use a 5-node cluster: with node 2 and
%% 4 which might not participate, it leaves nodes 1, 3 and 5: thus 3
%% nodes, the minimum to use Ra in proper conditions.

connection_id_tracking(Config) ->
    ID = <<"duplicate-id">>,
    {ok, MRef1, C1} = connect_to_node(Config, 0, ID),
    emqttc:subscribe(C1, <<"TopicA">>, qos0),
    emqttc:publish(C1, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),

    %% there's one connection
    assert_connection_count(Config, 10, 2, 1),

    %% connect to the same node (A or 0)
    {ok, MRef2, _C2} = connect_to_node(Config, 0, ID),

    %% C1 is disconnected
    await_disconnection(MRef1),

    %% connect to a different node (C or 2)
    {ok, _, C3} = connect_to_node(Config, 2, ID),
    assert_connection_count(Config, 10, 2, 1),

    %% C2 is disconnected
    await_disconnection(MRef2),

    emqttc:disconnect(C3).

connection_id_tracking_on_nodedown(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, MRef, C} = connect_to_node(Config, 0, <<"simpleClient">>),
    emqttc:subscribe(C, <<"TopicA">>, qos0),
    emqttc:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),
    assert_connection_count(Config, 10, 2, 1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server),
    await_disconnection(MRef),
    assert_connection_count(Config, 10, 2, 0),
    ok.

connection_id_tracking_with_decommissioned_node(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, MRef, C} = connect_to_node(Config, 0, <<"simpleClient">>),
    emqttc:subscribe(C, <<"TopicA">>, qos0),
    emqttc:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),

    assert_connection_count(Config, 10, 2, 1),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["decommission_mqtt_node", Server]),
    await_disconnection(MRef),
    assert_connection_count(Config, 10, 2, 0),
    ok.

%%
%% Helpers
%%

assert_connection_count(_Config, 0,  _, _) ->
    ct:fail("failed to complete rabbit_mqtt_collector:list/0");
assert_connection_count(Config, Retries, NodeId, NumElements) ->
    List = rabbit_ct_broker_helpers:rpc(Config, NodeId, rabbit_mqtt_collector, list, []),
    case length(List) == NumElements of
        true ->
            ok;
        false ->
            timer:sleep(200),
            assert_connection_count(Config, Retries-1, NodeId, NumElements)
    end.



connect_to_node(Config, Node, ClientID) ->
  Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_mqtt),
  {ok, C} = connect(Port, ClientID),
  MRef = erlang:monitor(process, C),
  {ok, MRef, C}.

connect(Port, ClientID) ->
  {ok, C} = emqttc:start_link([{host, "localhost"},
                               {port, Port},
                               {client_id, ClientID},
                               {proto_ver, 3},
                               {logger, info},
                               {puback_timeout, 1}]),
  unlink(C),
  {ok, C}.

await_disconnection(Ref) ->
  receive
      {'DOWN', Ref, _, _, _} -> ok
      after 30000            -> exit(missing_down_message)
  end.

expect_publishes(_Topic, []) -> ok;
expect_publishes(Topic, [Payload|Rest]) ->
    receive
        {publish, Topic, Payload} -> expect_publishes(Topic, Rest)
        after 5000 ->
            throw({publish_not_delivered, Payload})
    end.
