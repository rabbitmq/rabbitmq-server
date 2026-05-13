%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(web_mqtt_connection_limit_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").

-import(rabbit_web_mqtt_test_util,
        [connect/3]).

all() ->
    [node_connection_limit_cross_transport].

suite() ->
    [{timetrap, {minutes, 2}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodename_suffix, ?MODULE}]),
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
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% The `mqtt.max_connections` per-node limit must be enforced
%% across plain MQTT and MQTT-over-WebSockets listeners.
node_connection_limit_cross_transport(BaseConfig) ->
    Config = [{websocket, true} | BaseConfig],
    ok = rabbit_ct_broker_helpers:rpc(
           BaseConfig, 0, application, set_env,
           [rabbitmq_mqtt, max_connections, 2]),
    try
        C1 = connect(<<"plain-mqtt-client">>, BaseConfig, [{ack_timeout, 1}]),
        C2 = connect(<<"web-mqtt-client">>, Config, [{ack_timeout, 1}]),
        {ok, C3} = emqtt:start_link(
                     [{host, "localhost"},
                      {port, rabbit_ct_broker_helpers:get_node_config(
                               Config, 0, tcp_port_web_mqtt)},
                      {proto_ver, v5},
                      {clientid, <<"web-mqtt-client-2">>},
                      {ws_path, "/ws"},
                      {ack_timeout, 1}]),
        unlink(C3),
        ?assertMatch({error, {quota_exceeded, _}}, emqtt:ws_connect(C3)),
        ok = emqtt:disconnect(C1),
        ok = emqtt:disconnect(C2)
    after
        ok = rabbit_ct_broker_helpers:rpc(
               BaseConfig, 0, application, unset_env,
               [rabbitmq_mqtt, max_connections])
    end.
