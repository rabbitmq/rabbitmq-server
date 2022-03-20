%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(http_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-compile(export_all).

all() ->
    [{group, non_parallel_tests}].

groups() ->
    [{non_parallel_tests, [], [stream_management]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "suite is not mixed versions compatible"};
        _ ->
            rabbit_ct_helpers:log_environment(),
            Config1 =
                rabbit_ct_helpers:set_config(Config,
                                             [{rmq_nodename_suffix, ?MODULE}]),
            Config2 =
                rabbit_ct_helpers:set_config(Config1,
                                             {rabbitmq_ct_tls_verify,
                                              verify_none}),
            SetupStep =
                fun(StepConfig) ->
                   rabbit_ct_helpers:merge_app_env(StepConfig,
                                                   {rabbit,
                                                    [{collect_statistics_interval,
                                                      500}]})
                end,
            rabbit_ct_helpers:run_setup_steps(Config2,
                                              [SetupStep]
                                              ++ rabbit_ct_broker_helpers:setup_steps()
                                              ++ rabbit_ct_client_helpers:setup_steps())
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_client_helpers:teardown_steps()
                                         ++ rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

stream_management(Config) ->
    UserManagement = <<"user-management">>,
    UserMonitoring = <<"user-monitoring">>,
    Vhost1 = <<"vh1">>,
    Vhost2 = <<"vh2">>,
    rabbit_ct_broker_helpers:add_user(Config, UserManagement),
    rabbit_ct_broker_helpers:set_user_tags(Config,
                                           0,
                                           UserManagement,
                                           [management]),
    rabbit_ct_broker_helpers:add_user(Config, UserMonitoring),
    rabbit_ct_broker_helpers:set_user_tags(Config,
                                           0,
                                           UserMonitoring,
                                           [monitoring]),
    rabbit_ct_broker_helpers:add_vhost(Config, Vhost1),
    rabbit_ct_broker_helpers:add_vhost(Config, Vhost2),

    rabbit_ct_broker_helpers:set_full_permissions(Config, UserManagement,
                                                  Vhost1),
    rabbit_ct_broker_helpers:set_full_permissions(Config, UserMonitoring,
                                                  Vhost1),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>,
                                                  Vhost1),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>,
                                                  Vhost2),

    StreamPortNode = get_stream_port(Config),
    StreamPortTlsNode = get_stream_port_tls(Config),
    ManagementPortNode = get_management_port(Config),
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    MakeResult =
        rabbit_ct_helpers:make(Config, DataDir,
                               ["tests",
                                {"STREAM_PORT=~b", [StreamPortNode]},
                                {"STREAM_PORT_TLS=~b", [StreamPortTlsNode]},
                                {"MANAGEMENT_PORT=~b", [ManagementPortNode]}]),
    {ok, _} = MakeResult.

get_stream_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stream).

get_management_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mgmt).

get_stream_port_tls(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0,
                                             tcp_port_stream_tls).
