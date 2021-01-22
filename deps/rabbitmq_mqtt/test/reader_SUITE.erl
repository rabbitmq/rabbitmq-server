%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(reader_SUITE).
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
                                block,
                                handle_invalid_frames,
                                stats
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
    {ok, C} = emqttc:start_link([{host, "localhost"},
                                 {port, P},
                                 {client_id, <<"simpleClient">>},
                                 {proto_ver, 3},
                                 {logger, info},
                                 {puback_timeout, 1}]),
    %% Only here to ensure the connection is really up
    emqttc:subscribe(C, <<"TopicA">>, qos0),
    emqttc:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),
    emqttc:unsubscribe(C, [<<"TopicA">>]),

    emqttc:subscribe(C, <<"Topic1">>, qos0),

    %% Not blocked
    {ok, _} = emqttc:sync_publish(C, <<"Topic1">>, <<"Not blocked yet">>,
                                  [{qos, 1}]),

    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.00000001]),
    ok = rpc(Config, rabbit_alarm, set_alarm, [{{resource_limit, memory, node()}, []}]),

    %% Let it block
    timer:sleep(100),
    %% Blocked, but still will publish
    {error, ack_timeout} = emqttc:sync_publish(C, <<"Topic1">>, <<"Now blocked">>,
                                  [{qos, 1}]),

    %% Blocked
    {error, ack_timeout} = emqttc:sync_publish(C, <<"Topic1">>,
                                               <<"Blocked">>, [{qos, 1}]),

    rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.4]),
    rpc(Config, rabbit_alarm, clear_alarm, [{resource_limit, memory, node()}]),

    %% Let alarms clear
    timer:sleep(1000),

    expect_publishes(<<"Topic1">>, [<<"Not blocked yet">>,
                                    <<"Now blocked">>,
                                    <<"Blocked">>]),

    emqttc:disconnect(C).

handle_invalid_frames(Config) ->
    N = rpc(Config, ets, info, [connection_metrics, size]),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, C} = gen_tcp:connect("localhost", P, []),
    Bin = <<"GET / HTTP/1.1\r\nHost: www.rabbitmq.com\r\nUser-Agent: curl/7.43.0\r\nAccept: */*">>,
    gen_tcp:send(C, Bin),
    gen_tcp:close(C),
    %% No new stats entries should be inserted as connection never got to initialize
    N = rpc(Config, ets, info, [connection_metrics, size]).

stats(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    %% CMN = rpc(Config, ets, info, [connection_metrics, size]),
    %% CCMN = rpc(Config, ets, info, [connection_coarse_metrics, size]),
    {ok, C} = emqttc:start_link([{host, "localhost"},
                                 {port, P},
                                 {client_id, <<"simpleClient">>},
                                 {proto_ver, 3},
                                 {logger, info},
                                 {puback_timeout, 1}]),
    %% Ensure that there are some stats
    emqttc:subscribe(C, <<"TopicA">>, qos0),
    emqttc:publish(C, <<"TopicA">>, <<"Payload">>),
    expect_publishes(<<"TopicA">>, [<<"Payload">>]),
    emqttc:unsubscribe(C, [<<"TopicA">>]),
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
    emqttc:disconnect(C).

expect_publishes(_Topic, []) -> ok;
expect_publishes(Topic, [Payload|Rest]) ->
    receive
        {publish, Topic, Payload} -> expect_publishes(Topic, Rest)
        after 5000 ->
            throw({publish_not_delivered, Payload})
    end.

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).
