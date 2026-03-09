%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_disk_monitor_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile([nowarn_export_all, export_all]).

all() ->
    [
      {group, schema_tests},
      {group, resolve_data_dir_tests},
      {group, sequential_tests},
      {group, per_queue_type_alarm_tests}
    ].

groups() ->
    [
      {schema_tests, [], [
          duplicate_mount_path_is_allowed,
          incomplete_mount_entry_is_rejected
        ]},
      {resolve_data_dir_tests, [], [
          resolve_data_dir_single_result,
          resolve_data_dir_multiple_results_picks_most_specific
        ]},
      {sequential_tests, [], [
          set_disk_free_limit_command
        ]},
      {per_queue_type_alarm_tests, [], [
          per_queue_type_alarm_clears_when_limit_lowered
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

init_per_group(schema_tests, Config) ->
    Config;
init_per_group(resolve_data_dir_tests, Config) ->
    Config;
init_per_group(per_queue_type_alarm_tests = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    %% Configure a per-queue-type disk limit for the root mount and the classic
    %% queue type. The initial limit is tiny so that no alarm fires at boot.
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit,
                 [{disk_free_limits,
                   #{1 => #{name => <<"root">>,
                            mount => "/",
                            limit => "1",
                            queue_types => [<<"classic">>]}}}]}),
    rabbit_ct_helpers:run_steps(Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(schema_tests, _Config) ->
    ok;
end_per_group(resolve_data_dir_tests, _Config) ->
    ok;
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

duplicate_mount_path_is_allowed(_Config) ->
    SchemaFile = filename:join([code:priv_dir(rabbit), "schema", "rabbit.schema"]),
    Conf = [
        {["disk_free_limits", "1", "name"],        "streams"},
        {["disk_free_limits", "1", "mount"],       "/mnt/data"},
        {["disk_free_limits", "1", "limit"],       "2GB"},
        {["disk_free_limits", "1", "queue_types"], "stream"},
        {["disk_free_limits", "2", "name"],        "queues"},
        {["disk_free_limits", "2", "mount"],       "/mnt/data"},
        {["disk_free_limits", "2", "limit"],       "1GB"},
        {["disk_free_limits", "2", "queue_types"], "classic"}
    ],
    Generated = cuttlefish_unit:generate_config(file, SchemaFile, Conf),
    cuttlefish_unit:assert_valid_config(Generated).

incomplete_mount_entry_is_rejected(_Config) ->
    SchemaFile = filename:join([code:priv_dir(rabbit), "schema", "rabbit.schema"]),
    Conf = [
        {["disk_free_limits", "1", "name"],  "streams"},
        {["disk_free_limits", "1", "mount"], "/mnt/data"}
    ],
    Generated = cuttlefish_unit:generate_config(file, SchemaFile, Conf),
    cuttlefish_unit:assert_error_message(Generated, "Translation for 'rabbit.disk_free_limits' found invalid configuration: disk_free_limits.1 is missing required fields: [limit,queue_types]").

resolve_data_dir_single_result(_Config) ->
    ?assertEqual({ok, "/"}, rabbit_disk_monitor:resolve_data_dir([{"/", 1000, 500, 50}])).

resolve_data_dir_multiple_results_picks_most_specific(_Config) ->
    Infos = [{"/", 1000, 500, 50}, {"/var", 2000, 1000, 50}],
    ?assertEqual({ok, "/var"}, rabbit_disk_monitor:resolve_data_dir(Infos)).

set_disk_free_limit_command(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_disk_free_limit_command1, [Config]).

set_disk_free_limit_command1(_Config) ->
    F = fun () ->
        DiskFree = rabbit_disk_monitor:get_disk_free(),
        DiskFree =/= 'NaN'
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

%% Regression test: lowering a per-queue-type disk free limit must clear a
%% standing alarm. Previously the alarm remained set forever because
%% internal_update/1 recomputed the "before" alarmed set from the current
%% (already updated) limits instead of the alarms actually in effect.
per_queue_type_alarm_clears_when_limit_lowered(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    QTAlarm = {resource_limit, {disk, rabbit_classic_queue}, Node},
    MountName = <<"root">>,

    %% No per-queue-type disk alarm initially.
    ?assertNot(has_local_alarm(Config, QTAlarm)),

    %% Raise the limit far above the available space: the classic queue type
    %% disk alarm must be set.
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_disk_monitor, set_disk_free_limit,
           [MountName, 999_000_000_000_000]),
    ok = rabbit_ct_helpers:await_condition(
           fun() -> has_local_alarm(Config, QTAlarm) end),

    %% Lower the limit again: the alarm must clear.
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_disk_monitor, set_disk_free_limit,
           [MountName, 1]),
    ok = rabbit_ct_helpers:await_condition(
           fun() -> not has_local_alarm(Config, QTAlarm) end),
    ok.

has_local_alarm(Config, Alarm) ->
    Alarms = rabbit_ct_broker_helpers:rpc(
               Config, 0, rabbit_alarm, get_local_alarms, []),
    lists:keymember(Alarm, 1, Alarms).
