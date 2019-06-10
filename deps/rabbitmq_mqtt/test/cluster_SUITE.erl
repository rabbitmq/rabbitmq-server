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
        {rmq_nodes_count, 3}
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

connection_id_tracking(Config) ->
    ID = <<"duplicate-id">>,
    {ok, MRef1, C1} = connect_to_node(Config, 0, ID),
    emqttc:subscribe(C1, <<"TopicA">>, qos0),
    emqttc:publish(C1, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),

    %% there's one connection
    [_] = rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_mqtt_collector, list, []),

    %% connect to the same node (A or 0)
    {ok, MRef2, C2} = connect_to_node(Config, 0, ID),

    %% C1 is disconnected
    await_disconnection(MRef1),

    %% connect to a different node (B or 1)
    {ok, _, C3} = connect_to_node(Config, 1, ID),
    [_] = rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_mqtt_collector, list, []),

    %% C2 is disconnected
    await_disconnection(MRef2),

    emqttc:disconnect(C3).

connection_id_tracking_on_nodedown(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, C} = emqttc:start_link([{host, "localhost"},
                                 {port, P},
                                 {client_id, <<"simpleClient">>},
                                 {proto_ver, 3},
                                 {logger, info},
                                 {puback_timeout, 1}]),
    unlink(C),
    MRef = erlang:monitor(process, C),
    emqttc:subscribe(C, <<"TopicA">>, qos0),
    emqttc:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_mqtt_collector, list, []),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server),
    receive
        {'DOWN', MRef, _, _, _} ->
            ok
    after
        30000 ->
            exit(missing_down_message)
    end,
    [] = rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_mqtt_collector, list, []).

connection_id_tracking_with_decommissioned_node(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, C} = emqttc:start_link([{host, "localhost"},
                                 {port, P},
                                 {client_id, <<"simpleClient">>},
                                 {proto_ver, 3},
                                 {logger, info},
                                 {puback_timeout, 1}]),
    unlink(C),
    MRef = erlang:monitor(process, C),
    emqttc:subscribe(C, <<"TopicA">>, qos0),
    emqttc:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),

    [_] = rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_mqtt_collector, list, []),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["decommission_mqtt_node", Server]),
    receive
        {'DOWN', MRef, _, _, _} ->
            ok
    after
        30000 ->
            exit(missing_down_message)
    end,
    [] = rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_mqtt_collector, list, []).

%%
%% Helpers
%%

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
