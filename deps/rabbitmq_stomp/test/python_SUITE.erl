%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(python_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

%% Generated: one CT test per Python test method, grouped by class.
%% Regenerate: python3 test/generate_python_tests.py \
%%     test/python_SUITE_data/src test/python_SUITE_generated.hrl
-include("python_SUITE_generated.hrl").

all() ->
    [{group, tls},
     {group, implicit_connect},
     {group, main}].

groups() ->
    [{tls,              [], ?TLS_SUBGROUPS},
     {implicit_connect, [], ?IMPLICIT_CONNECT_SUBGROUPS},
     {main,             [], ?MAIN_SUBGROUPS}
     | ?CLASS_GROUPS].

init_per_suite(Config) ->
    {ok, _} = rabbit_ct_helpers:exec(
                ["pip", "install", "-r", requirements_path(Config),
                 "--target", deps_path(Config)]),
    Config.

end_per_suite(Config) ->
    ok = file:del_dir_r(deps_path(Config)),
    Config.

init_per_group(main, Config) ->
    Config1 = init_broker(Config),
    rabbit_ct_broker_helpers:rpc(
      Config1, 0,
      application, set_env, [rabbitmq_stomp, max_frame_size, 17 * 1024 * 1024]),
    Config1;
init_per_group(tls, Config) ->
    Config1 = init_broker(Config),
    ensure_ssl_auth_user(Config1),
    Config1;
init_per_group(implicit_connect, Config) ->
    init_broker(Config);
init_per_group(_, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= main;
                                  Group =:= tls;
                                  Group =:= implicit_connect ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(Test, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Test).

end_per_testcase(Test, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Test).

run_one_test(Config, PythonTestId) ->
    DataDir = ?config(data_dir, Config),
    SrcDir = filename:join(DataDir, "src"),
    setup_python_env(Config),
    {ok, _} = rabbit_ct_helpers:exec(
                ["python3", "-m", "unittest", "-v", PythonTestId],
                [{cd, SrcDir}]).

%%
%% Internal
%%

init_broker(Config) ->
    Config0 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE},
                                            {rmq_certspwd, "bunnychow"}]),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0,
                {rabbit,
                 [{permit_deprecated_features, #{transient_nonexcl_queues => true}}]}),
    rabbit_ct_helpers:run_setup_steps(
        Config1,
        rabbit_ct_broker_helpers:setup_steps()).

ensure_ssl_auth_user(Config) ->
    Host = net_adm:localhost(),
    User = "O=client,CN=" ++ Host,
    rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["add_user", User, "foo"]),
    rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["clear_password", User]),
    rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["set_permissions", User, ".*", ".*", ".*"]).

setup_python_env(Config) ->
    CertsDir = rabbit_ct_helpers:get_config(Config, rmq_certsdir),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    StompPortTls = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp_tls),
    AmqpPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    MgmtPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mgmt),
    NodeName = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    os:putenv("AMQP_PORT", integer_to_list(AmqpPort)),
    os:putenv("MGMT_PORT", integer_to_list(MgmtPort)),
    os:putenv("STOMP_PORT", integer_to_list(StompPort)),
    os:putenv("STOMP_PORT_TLS", integer_to_list(StompPortTls)),
    os:putenv("RABBITMQ_NODENAME", atom_to_list(NodeName)),
    os:putenv("SSL_CERTS_PATH", CertsDir),
    os:putenv("PYTHONPATH", python_path(Config)).

deps_path(Config) ->
    DataDir = ?config(data_dir, Config),
    filename:join([DataDir, "src", "deps"]).

requirements_path(Config) ->
    DataDir = ?config(data_dir, Config),
    filename:join([DataDir, "src", "requirements.txt"]).

python_path(Config) ->
    case os:getenv("PYTHONPATH") of
        false -> deps_path(Config);
        P -> deps_path(Config) ++ ":" ++ P
    end.
