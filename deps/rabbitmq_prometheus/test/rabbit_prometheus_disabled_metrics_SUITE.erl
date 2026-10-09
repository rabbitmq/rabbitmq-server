%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_prometheus_disabled_metrics_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
        {group, default_settings},
        {group, disabled_endpoints},
        {group, disabled_metrics}
    ].

groups() ->
    [
        {default_settings, [], [
            per_object_endpoint_disabled_by_default_test
        ]},
        {disabled_endpoints, [], [
            disable_per_object_endpoint_test,
            disable_detailed_endpoint_test,
            disable_memory_breakdown_endpoint_test
        ]},
        {disabled_metrics, [], [
            disable_single_metric_test,
            disable_multiple_metrics_test,
            normalize_metric_name_test
        ]}
    ].

init_per_group(default_settings, Config) ->
    init_group(Config);
init_per_group(disabled_endpoints, Config) ->
    EndpointConfig = {rabbitmq_prometheus, [
        {disable_per_object_endpoint, true},
        {disable_detailed_endpoint, true},
        {disable_memory_breakdown_endpoint, true}
    ]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config, EndpointConfig),
    init_group(Config1);
init_per_group(disabled_metrics, Config) ->
    MetricsConfig = {rabbitmq_prometheus, [
        {disabled_metrics, [queue_messages, connection_incoming_bytes_total]}
    ]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config, MetricsConfig),
    init_group(Config1).

init_group(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    rabbit_ct_helpers:run_setup_steps(Config,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    inets:stop(),
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% Tests

disable_per_object_endpoint_test(Config) ->
    Port = rabbit_mgmt_test_util:config_port(Config, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~tp/metrics/per-object", [Port])),
    {ok, {{_, Code, _}, _, _}} = httpc:request(get, {URI, []}, ?HTTPC_OPTS, []),
    ?assertEqual(404, Code).

disable_detailed_endpoint_test(Config) ->
    Port = rabbit_mgmt_test_util:config_port(Config, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~tp/metrics/detailed", [Port])),
    {ok, {{_, Code, _}, _, _}} = httpc:request(get, {URI, []}, ?HTTPC_OPTS, []),
    ?assertEqual(404, Code).

disable_memory_breakdown_endpoint_test(Config) ->
    Port = rabbit_mgmt_test_util:config_port(Config, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~tp/metrics/memory-breakdown", [Port])),
    {ok, {{_, Code, _}, _, _}} = httpc:request(get, {URI, []}, ?HTTPC_OPTS, []),
    ?assertEqual(404, Code).

per_object_endpoint_disabled_by_default_test(Config) ->
    Port = rabbit_mgmt_test_util:config_port(Config, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~tp/metrics/per-object", [Port])),
    {ok, {{_, Code, _}, _, _}} = httpc:request(get, {URI, []}, ?HTTPC_OPTS, []),
    ?assertEqual(404, Code).

disable_single_metric_test(Config) ->
    {_, Body} = http_get(Config, [], 200),
    ?assertEqual(nomatch, re:run(Body, "^rabbitmq_queue_messages ", [{capture, none}, multiline])),
    ?assertEqual(match, re:run(Body, "^rabbitmq_connections ", [{capture, none}, multiline])).

disable_multiple_metrics_test(Config) ->
    {_, Body} = http_get(Config, [], 200),
    ?assertEqual(nomatch, re:run(Body, "^rabbitmq_queue_messages ", [{capture, none}, multiline])),
    ?assertEqual(nomatch, re:run(Body, "^rabbitmq_connection_incoming_bytes_total ", [{capture, none}, multiline])).

normalize_metric_name_test(_Config) ->
    ?assertEqual(queue_messages, rabbit_prometheus_util:normalize_metric_name(rabbitmq_queue_messages)),
    ?assertEqual(queue_messages, rabbit_prometheus_util:normalize_metric_name(rabbitmq_detailed_queue_messages)),
    ?assertEqual(vhost_status, rabbit_prometheus_util:normalize_metric_name(rabbitmq_cluster_vhost_status)),
    ?assertEqual(other_metric, rabbit_prometheus_util:normalize_metric_name(other_metric)).

%% Helpers

http_get(Config, ReqHeaders, CodeExp) ->
    Port = rabbit_mgmt_test_util:config_port(Config, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~tp/metrics", [Port])),
    {ok, {{_, CodeAct, _}, Headers, Body}} =
        httpc:request(get, {URI, ReqHeaders}, ?HTTPC_OPTS, []),
    ?assertMatch(CodeExp, CodeAct),
    {Headers, Body}.
