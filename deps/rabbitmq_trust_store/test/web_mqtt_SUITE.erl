%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Covers the trust store's effect on a listener that configures its own
%% TLS options rather than reading `rabbit`'s `ssl_options` (see
%% `rabbit_networking:fix_ssl_options/1`). `rabbitmq_web_mqtt` stands in
%% for every listener in that position.
-module(web_mqtt_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% `rabbit_ct_broker_helpers` pre-assigns this port to
%% `tcp_port_web_mqtt_tls` but does not configure a listener on it.
-define(WEB_MQTT_TLS_PORT, 21010).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [], [
          trust_store_options_reach_the_web_mqtt_tls_listener
        ]}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [{rmq_nodename_suffix, ?MODULE}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1),
    {rmq_certsdir, CertsDir} = proplists:lookup(rmq_certsdir, Config2),
    Config3 = rabbit_ct_helpers:merge_app_env(
                Config2,
                {rabbitmq_web_mqtt,
                 [{ssl_config,
                   [{cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                    {certfile, filename:join([CertsDir, "server", "cert.pem"])},
                    {keyfile, filename:join([CertsDir, "server", "key.pem"])},
                    {verify, verify_peer},
                    {fail_if_no_peer_cert, true},
                    {port, ?WEB_MQTT_TLS_PORT}
                   ]}]}),
    rabbit_ct_helpers:run_setup_steps(Config3, rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

trust_store_options_reach_the_web_mqtt_tls_listener(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt_tls),
    SocketOpts = rabbit_ct_broker_helpers:rpc(Config, 0,
                                              ?MODULE, listener_socket_opts, [Port]),
    {VerifyFun, continue} = proplists:get_value(verify_fun, SocketOpts),
    {module, rabbit_trust_store} = erlang:fun_info(VerifyFun, module),
    ?assert(is_function(proplists:get_value(partial_chain, SocketOpts), 1)).

%% -------------------------------------------------------------------
%% Internal helpers
%% -------------------------------------------------------------------

listener_socket_opts(Port) ->
    Ref = rabbit_networking:ranch_ref([{port, Port}]),
    #{socket_opts := SocketOpts} = ranch:get_transport_options(Ref),
    SocketOpts.
