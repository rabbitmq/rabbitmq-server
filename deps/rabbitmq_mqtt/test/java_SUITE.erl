%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(java_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BASE_CONF_MQTT,
        {rabbitmq_mqtt, [
           {ssl_cert_login,   true},
           {allow_anonymous,  false},
           {sparkplug,        true},
           {tcp_listeners,    []},
           {ssl_listeners,    []}
           ]}).

all() ->
    [
      {group, v3},
      {group, v5}
    ].

groups() ->
    [
      {v3, [], [java_v3]},
      {v5, [], [java_v5]}
    ].

suite() ->
    [{timetrap, {seconds, 600}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

merge_app_env(Config) ->
    {ok, Ssl} = q(Config, [erlang_node_config, rabbit, ssl_options]),
    Ssl1 = lists:keyreplace(fail_if_no_peer_cert, 1, Ssl, {fail_if_no_peer_cert, false}),
    Config1 = rabbit_ct_helpers:merge_app_env(Config, {rabbit, [{ssl_options, Ssl1}]}),
    rabbit_ct_helpers:merge_app_env(Config1, ?BASE_CONF_MQTT).

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config0) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config0, "", "-"),
    Config = rabbit_ct_helpers:set_config(
               Config0, [{rmq_nodename_suffix, Suffix},
                         {rmq_certspwd, "bunnychow"},
                         {rmq_nodes_clustered, true},
                         {rmq_nodes_count, 3},
                         {mqtt_version, Group},
                         {start_rmq_with_plugins_disabled, true}
                        ]),
    Config1 = rabbit_ct_helpers:run_setup_steps(
                Config,
                [fun merge_app_env/1] ++
                    rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config1, rabbitmq_mqtt),
    Config1.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    CertFile = filename:join([CertsDir, "client", "cert.pem"]),
    {ok, CertBin} = file:read_file(CertFile),
    [{'Certificate', Cert, not_encrypted}] = public_key:pem_decode(CertBin),
    UserBin = rabbit_ct_broker_helpers:rpc(Config, 0,
                                           rabbit_ssl,
                                           peer_cert_auth_name,
                                           [Cert]),
    User = binary_to_list(UserBin),
    {ok,_} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["add_user", User, ""]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["set_permissions",  "-p", "/", User, ".*", ".*", ".*"]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0,
        ["set_topic_permissions",  "-p", "/", "guest", "amq.topic",
            % Write permission
            "test-topic|^test-retained-topic$|^{username}.{client_id}.a$|^sp[AB]v\\d+___\\d+",
            % Read permission
            "test-topic|^test-retained-topic$|^last-will$|^{username}.{client_id}.a$|^sp[AB]v\\d+___\\d+"]),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

java_v3(Config) ->
    run_test(Config, ["tests", "ssltests"]).

java_v5(Config) ->
    run_test(Config, ["v5tests", "v5ssltests"]).

run_test(Config, Target) ->
    CertsDir = rabbit_ct_helpers:get_config(Config, rmq_certsdir),
    MqttPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    MqttPort2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, tcp_port_mqtt),
    MqttPort3 = rabbit_ct_broker_helpers:get_node_config(Config, 2, tcp_port_mqtt),
    MqttSslPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt_tls),
    AmqpPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    os:putenv("SSL_CERTS_DIR", CertsDir),
    os:putenv("MQTT_SSL_PORT", erlang:integer_to_list(MqttSslPort)),
    os:putenv("MQTT_PORT", erlang:integer_to_list(MqttPort)),
    os:putenv("MQTT_PORT_2", erlang:integer_to_list(MqttPort2)),
    os:putenv("MQTT_PORT_3", erlang:integer_to_list(MqttPort3)),
    os:putenv("AMQP_PORT", erlang:integer_to_list(AmqpPort)),
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    MakeResult = rabbit_ct_helpers:make(Config, DataDir, Target),
    {ok, _} = MakeResult.

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).

q(P, [K | Rem]) ->
    case proplists:get_value(K, P) of
        undefined -> undefined;
        V -> q(V, Rem)
    end;
q(P, []) -> {ok, P}.

