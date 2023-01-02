%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(disk_monitor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, sequential_tests}
    ].

groups() ->
    [
      {sequential_tests, [], [
          disk_monitor,
          disk_monitor_enable
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

disk_monitor(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                      disk_monitor1, [Config]).

disk_monitor1(_Config) ->
    %% Issue: rabbitmq-server #91
    %% os module could be mocked using 'unstick', however it may have undesired
    %% side effects in following tests. Thus, we mock at rabbit_misc level
    ok = rabbit_sup:stop_child(rabbit_disk_monitor_sup),
    ok = rabbit_sup:start_delayed_restartable_child(rabbit_disk_monitor, [1000]),
    Value = rabbit_disk_monitor:get_disk_free(),
    true = is_integer(Value) andalso Value >= 0,
    ok.

disk_monitor_enable(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                      disk_monitor_enable1, [Config]).

disk_monitor_enable1(_Config) ->
    application:set_env(rabbit, disk_monitor_failure_retries, 20000),
    application:set_env(rabbit, disk_monitor_failure_retry_interval, 100),
    ok = rabbit_sup:stop_child(rabbit_disk_monitor_sup),
    ok = rabbit_sup:start_delayed_restartable_child(rabbit_disk_monitor, [250]),

    Value0 = rabbit_disk_monitor:get_disk_free(),
    true = (is_integer(Value0) andalso Value0 >= 0),
    ok = timer:sleep(500),
    Value1 = rabbit_disk_monitor:get_disk_free(),
    true = (is_integer(Value1) andalso Value1 >= 0),

    application:set_env(rabbit, disk_monitor_failure_retries, 10),
    application:set_env(rabbit, disk_monitor_failure_retry_interval, 120000),
    ok.
