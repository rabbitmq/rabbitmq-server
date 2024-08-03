%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(prometheus_rabbitmq_shovel_collector_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("prometheus/include/prometheus_model.hrl").

-compile(export_all).

-define(DYN_RUNNING_METRIC(Gauge), #'MetricFamily'{name = <<"rabbitmq_shovel_dynamic">>,
                                                   help = "Current number of dynamic shovels.",type = 'GAUGE',
                                                   metric = [#'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                             value = <<"running">>}],
                                                                       gauge = #'Gauge'{value = Gauge},
                                                                       counter = undefined,summary = undefined,untyped = undefined,
                                                                       histogram = undefined,timestamp_ms = undefined}]}).

-define(STAT_RUNNING_METRIC(Gauge), #'MetricFamily'{name = <<"rabbitmq_shovel_static">>,
                                                    help = "Current number of static shovels.",type = 'GAUGE',
                                                    metric = [#'Metric'{label = [#'LabelPair'{name = <<"status">>,
                                                                                              value = <<"running">>}],
                                                                        gauge = #'Gauge'{value = Gauge},
                                                                        counter = undefined,summary = undefined,untyped = undefined,
                                                                        histogram = undefined,timestamp_ms = undefined}]}).

-define(EMPTY_DYN_METRIC, #'MetricFamily'{name = <<"rabbitmq_shovel_dynamic">>,
                                           help = "Current number of dynamic shovels.",type = 'GAUGE',
                                           metric = []}).

-define(EMPTY_STAT_METRIC, #'MetricFamily'{name = <<"rabbitmq_shovel_static">>,
                                           help = "Current number of static shovels.",type = 'GAUGE',
                                           metric = []}).


all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               dynamic,
                               static,
                               mix
                              ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {ignored_crashes, [
            "server_initiated_close,404",
            "writer,send_failed,closed"
        ]}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
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
%% Test cases
%% -------------------------------------------------------------------

dynamic(Config) ->
    create_dynamic_shovel(Config, <<"test">>),
    running = get_shovel_status(Config, <<"test">>),
    [?DYN_RUNNING_METRIC(1), ?EMPTY_STAT_METRIC] = get_metrics(Config),
    create_dynamic_shovel(Config, <<"test2">>),
    running = get_shovel_status(Config, <<"test2">>),
    [?DYN_RUNNING_METRIC(2), ?EMPTY_STAT_METRIC] = get_metrics(Config),
    clear_param(Config, <<"test">>),
    clear_param(Config, <<"test2">>),
    [?EMPTY_DYN_METRIC, ?EMPTY_STAT_METRIC] = get_metrics(Config),
    ok.

static(Config) ->
    create_static_shovel(Config, static_shovel),
    [?EMPTY_DYN_METRIC, ?STAT_RUNNING_METRIC(1)] = get_metrics(Config),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, clear_shovel,
                                      []),
    [?EMPTY_DYN_METRIC, ?EMPTY_STAT_METRIC] = get_metrics(Config),
    ok.


mix(Config) ->
    create_dynamic_shovel(Config, <<"test">>),
    running = get_shovel_status(Config, <<"test">>),
    create_static_shovel(Config, static_shovel),

    [?DYN_RUNNING_METRIC(1), ?STAT_RUNNING_METRIC(1)] = get_metrics(Config),

    clear_param(Config, <<"test">>),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, clear_shovel,
                                      []),
    [?EMPTY_DYN_METRIC, ?EMPTY_STAT_METRIC] = get_metrics(Config),
    ok.

%% -------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------

get_metrics(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbitmq_prometheus_collector_test_proxy, collect_mf,
                                 [default, prometheus_rabbitmq_shovel_collector]).

create_static_shovel(Config, Name) ->
    SourceQueue =  <<"source-queue">>,
    DestQueue =  <<"dest-queue">>,
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Shovel = [{Name,
               [{source,
                 [{protocol, amqp10},
                  {uris, [rabbit_misc:format("amqp://~ts:~b",
                                             [Hostname, Port])]},
                  {source_address, SourceQueue}]
                },
                {destination,
                 [{uris, [rabbit_misc:format("amqp://~ts:~b/%2f?heartbeat=5",
                                             [Hostname, Port])]},
                  {declarations,
                   [{'queue.declare', [{queue, DestQueue}, auto_delete]}]},
                  {publish_fields, [{exchange, <<>>},
                                    {routing_key, DestQueue}]},
                  {publish_properties, [{delivery_mode, 2},
                                        {content_type,  <<"shovelled">>}]},
                  {add_forward_headers, true},
                  {add_timestamp_header, true}]},
                {queue, <<>>},
                {ack_mode, no_ack}
               ]}],
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, setup_shovel,
                                      [Shovel, Name]).

setup_shovel(ShovelConfig, Name) ->
    _ = application:stop(rabbitmq_shovel),
    application:set_env(rabbitmq_shovel, shovels, ShovelConfig, infinity),
    ok = application:start(rabbitmq_shovel),
    await_shovel(Name, static).

clear_shovel() ->
    _ = application:stop(rabbitmq_shovel),
    application:unset_env(rabbitmq_shovel, shovels, infinity),
    ok = application:start(rabbitmq_shovel).

make_uri(Config, Node) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_amqp),
    list_to_binary(lists:flatten(io_lib:format("amqp://~ts:~b",
                                               [Hostname, Port]))).

create_dynamic_shovel(Config, Name) ->
    Node = 0,
    QueueNode = 0,
    Uri = make_uri(Config, QueueNode),
    Value = [{<<"src-queue">>,  <<"src">>},
             {<<"dest-queue">>, <<"dest">>}],
    ok = rabbit_ct_broker_helpers:rpc(
           Config,
           Node,
           rabbit_runtime_parameters,
           set, [
                 <<"/">>, <<"shovel">>, Name, [{<<"src-uri">>,  Uri},
                                               {<<"dest-uri">>, [Uri]} |
                                               Value], none]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, await_shovel,
                                      [Name, dynamic]).

await_shovel(Name, Type) ->
    Ret = await(fun() ->
                  Status = shovels_from_status(running, Type),
                  lists:member(Name, Status)
          end, 30_000),
    Ret.

shovels_from_status(ExpectedState, dynamic) ->
    S = rabbit_shovel_status:status(),
    [N || {{<<"/">>, N}, dynamic, {State, _}, _} <- S, State == ExpectedState];
shovels_from_status(ExpectedState, static) ->
    S = rabbit_shovel_status:status(),
    [N || {N, static, {State, _}, _} <- S, State == ExpectedState].

get_shovel_status(Config, Name) ->
    get_shovel_status(Config, 0, Name).

get_shovel_status(Config, Node, Name) ->
    S = rabbit_ct_broker_helpers:rpc(
          Config, Node, rabbit_shovel_status, lookup, [{<<"/">>, Name}]),
    case S of
        not_found ->
            not_found;
        _ ->
            {Status, Info} = proplists:get_value(info, S),
            proplists:get_value(blocked_status, Info, Status)
    end.

await(Pred) ->
    case Pred() of
        true  -> ok;
        false -> timer:sleep(100),
                 await(Pred)
    end.

await(_Pred, Timeout) when Timeout =< 0 ->
    error(await_timeout);
await(Pred, Timeout) ->
    case Pred() of
        true  -> ok;
        Other when Timeout =< 100 ->
            error({await_timeout, Other});
        _ -> timer:sleep(100),
             await(Pred, Timeout - 100)
    end.

clear_param(Config, Name) ->
    clear_param(Config, 0, Name).

clear_param(Config, Node, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, Node,
      rabbit_runtime_parameters, clear, [<<"/">>, <<"shovel">>, Name, <<"acting-user">>]).
