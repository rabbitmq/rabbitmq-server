%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(http_SUITE).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_mgmt_test_util, [
                                http_delete/3,
                                http_get/5,
                                http_put/4,
                                http_put/6
                               ]).

-compile(export_all).

all() ->
    [{group, non_parallel_tests}].

groups() ->
    [{non_parallel_tests, [], [
                               stream_management,
                               create_super_stream,
                               create_super_stream_requires_configure_permission,
                               create_super_stream_partition_limits,
                               stream_tracking_requires_vhost_access
                              ]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "suite is not mixed versions compatible"};
        _ ->
            inets:start(),
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

create_super_stream(Config) ->
    http_put(Config, "/stream/super-streams/%2F/carrots", #{partitions => 3,
                                                            'binding-keys' => "streamA"},
             ?BAD_REQUEST),
    http_put(Config, "/stream/super-streams/%2F/carrots", #{partitions => "this is not a partition"},
             ?BAD_REQUEST),
    http_put(Config, "/stream/super-streams/%2F/carrots", #{partitions => 3},
             {group, '2xx'}),
    http_put(Config, "/stream/super-streams/%2F/cucumber", #{'binding-keys' => "fresh-cucumber"},
             {group, '2xx'}),
    http_put(Config, "/stream/super-streams/%2F/aubergine",
             #{partitions => 3,
               arguments => #{'max-length-bytes' => 1000000,
                              'max-age' => <<"1h">>,
                              'stream-max-segment-size' => 500,
                              'initial-cluster-size' => 2,
                              'queue-leader-locator' => <<"client-local">>}},
             {group, '2xx'}),
    http_put(Config, "/stream/super-streams/%2F/watermelon",
             #{partitions => 3,
               arguments => #{'queue-leader-locator' => <<"remote">>}},
             ?BAD_REQUEST),
    ok.

create_super_stream_requires_configure_permission(Config) ->
    User = <<"no-configure">>,
    Pass = <<"no-configure">>,
    rabbit_ct_broker_helpers:add_user(Config, User, Pass),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, User, [management]),
    %% Grant write and read but no configure permission
    rabbit_ct_broker_helpers:set_permissions(Config, User, <<"/">>,
                                             <<"^$">>, <<".*">>, <<".*">>),
    http_put(Config, "/stream/super-streams/%2F/denied-stream",
             #{partitions => 3},
             User, Pass,
             ?NOT_AUTHORISED),
    http_put(Config, "/stream/super-streams/%2F/denied-stream-bk",
             #{'binding-keys' => "key1,key2"},
             User, Pass,
             ?NOT_AUTHORISED),
    %% Verify that the same request succeeds with full permissions
    http_put(Config, "/stream/super-streams/%2F/allowed-stream",
             #{partitions => 3},
             {group, '2xx'}),
    rabbit_ct_broker_helpers:delete_user(Config, User),
    ok.

create_super_stream_partition_limits(Config) ->
    http_put(Config, "/stream/super-streams/%2F/zero-parts",
             #{partitions => 0}, ?BAD_REQUEST),
    http_put(Config, "/stream/super-streams/%2F/neg-parts",
             #{partitions => -5}, ?BAD_REQUEST),
    http_put(Config, "/stream/super-streams/%2F/over-parts",
             #{partitions => 1001}, ?BAD_REQUEST),
    http_put(Config, "/stream/super-streams/%2F/huge-parts",
             #{partitions => 500000000}, ?BAD_REQUEST),
    http_put(Config, "/stream/super-streams/%2F/max-parts",
             #{partitions => 1000}, {group, '2xx'}),
    ok.

stream_tracking_requires_vhost_access(Config) ->
    Vhost = <<"tracking-vh">>,
    User = <<"tracking-user">>,
    rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, Vhost),
    StreamArgs = [{durable, true}, {arguments, [{'x-queue-type', 'stream'}]}],
    http_put(Config, "/queues/tracking-vh/test-stream", StreamArgs, {group, '2xx'}),
    rabbit_ct_broker_helpers:add_user(Config, User, User),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, User, [management]),
    http_get(Config, "/stream/tracking-vh/test-stream/tracking",
             User, User, ?NOT_AUTHORISED),
    %% Permissions granted, must succeed.
    rabbit_ct_broker_helpers:set_full_permissions(Config, User, Vhost),
    http_get(Config, "/stream/tracking-vh/test-stream/tracking",
             User, User, ?OK),
    http_delete(Config, "/queues/tracking-vh/test-stream", {group, '2xx'}),
    rabbit_ct_broker_helpers:delete_user(Config, User),
    rabbit_ct_broker_helpers:delete_vhost(Config, Vhost),
    ok.

get_stream_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stream).

get_management_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mgmt).

get_stream_port_tls(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0,
                                             tcp_port_stream_tls).
