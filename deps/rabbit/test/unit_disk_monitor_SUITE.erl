%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_disk_monitor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, sequential_tests}
    ].

groups() ->
    [
      {sequential_tests, [], [
          set_disk_free_limit_command
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

set_disk_free_limit_command(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_disk_free_limit_command1, [Config]).

set_disk_free_limit_command1(_Config) ->
    F = fun () ->
        DiskFree = rabbit_disk_monitor:get_disk_free(),
        DiskFree =/= unknown
    end,
    rabbit_ct_helpers:await_condition(F),

    %% Use an integer
    rabbit_disk_monitor:set_disk_free_limit({mem_relative, 1}),
    disk_free_limit_to_total_memory_ratio_is(1),

    %% Use a float
    rabbit_disk_monitor:set_disk_free_limit({mem_relative, 1.5}),
    disk_free_limit_to_total_memory_ratio_is(1.5),

    %% use an absolute value
    rabbit_disk_monitor:set_disk_free_limit("70MiB"),
    ?assertEqual(73400320, rabbit_disk_monitor:get_disk_free_limit()),

    rabbit_disk_monitor:set_disk_free_limit("50MB"),
    ?assertEqual(50 * 1000 * 1000, rabbit_disk_monitor:get_disk_free_limit()),
    passed.

disk_free_limit_to_total_memory_ratio_is(MemRatio) ->
    DiskFreeLimit = rabbit_disk_monitor:get_disk_free_limit(),
    ExpectedLimit = MemRatio * vm_memory_monitor:get_total_memory(),
    % Total memory is unstable, so checking order
    true = ExpectedLimit/DiskFreeLimit < 1.2,
    true = ExpectedLimit/DiskFreeLimit > 0.98.
