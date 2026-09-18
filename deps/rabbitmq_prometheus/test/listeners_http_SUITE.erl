%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(listeners_http_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [additional_listener_serves_the_api,
     removed_listener_stops_serving].

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0,
                [{rmq_nodename_suffix, ?MODULE},
                 {rmq_nodes_count, 1},
                 {rmq_extra_tcp_ports, [tcp_port_prometheus_extra]}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    _ = application:ensure_all_started(inets),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    reset_listeners(Config, undefined),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

additional_listener_serves_the_api(Config) ->
    Extra = extra_port(Config),
    reset_listeners(Config, [Extra]),
    ?assertMatch({ok, {{_, 200, _}, _, _}}, get_metrics(Config, Extra)),
    ?assertMatch({ok, {{_, 200, _}, _, _}}, get_metrics(Config, primary_port(Config))).

removed_listener_stops_serving(Config) ->
    Extra = extra_port(Config),
    reset_listeners(Config, [Extra]),
    ?assertMatch({ok, {{_, 200, _}, _, _}}, get_metrics(Config, Extra)),
    reset_listeners(Config, undefined),
    ?assertMatch({error, {failed_connect, _}}, get_metrics(Config, Extra)),
    ?assertMatch({ok, {{_, 200, _}, _, _}}, get_metrics(Config, primary_port(Config))).

reset_listeners(Config, undefined) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env, [rabbitmq_prometheus, tcp_listeners]),
    restart_app(Config);
reset_listeners(Config, Ports) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_prometheus, tcp_listeners, Ports]),
    restart_app(Config).

restart_app(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, stop, [rabbitmq_prometheus]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, start, [rabbitmq_prometheus]).

get_metrics(Config, Port) ->
    Host = rabbit_ct_helpers:get_config(Config, rmq_hostname),
    URL = lists:flatten(io_lib:format("http://~ts:~b/metrics", [Host, Port])),
    httpc:request(get, {URL, []}, [], []).

extra_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_prometheus_extra).

primary_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_prometheus).
