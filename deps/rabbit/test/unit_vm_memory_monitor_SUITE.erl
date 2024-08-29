%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_vm_memory_monitor_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, sequential_tests}
    ].

groups() ->
    [
      {sequential_tests, [], [
          parse_line_linux,
          set_vm_memory_high_watermark_relative1,
          set_vm_memory_high_watermark_relative2,
          set_vm_memory_high_watermark_absolute
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


parse_line_linux(_Config) ->
    lists:foreach(fun ({S, {K, V}}) ->
                          {K, V} = vm_memory_monitor:parse_line_linux(S)
                  end,
                  [{"MemTotal:      0 kB",        {'MemTotal', 0}},
                   {"MemTotal:      502968 kB  ", {'MemTotal', 515039232}},
                   {"MemFree:         178232 kB", {'MemFree',  182509568}},
                   {"MemTotal:         50296888", {'MemTotal', 50296888}},
                   {"MemTotal         502968 kB", {'MemTotal', 515039232}},
                   {"MemTotal     50296866   ",   {'MemTotal', 50296866}}]),
    ok.

set_vm_memory_high_watermark_relative1(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_and_verify_vm_memory_high_watermark_relative, [1.0]).

%% an alternative way of setting it via advanced.config, equivalent to the relative1 case above
set_vm_memory_high_watermark_relative2(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_and_verify_vm_memory_high_watermark_relative, [{relative, 1.0}]).

set_vm_memory_high_watermark_absolute(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      %% note: we cannot use 100M here because this function won't do any
      %% parsing of the argument
      ?MODULE, set_and_verify_vm_memory_high_watermark_absolute, [{absolute, 104857600}]).


set_and_verify_vm_memory_high_watermark_relative(MemLimitRatio) ->
    MemTotal = vm_memory_monitor:get_total_memory(),

    vm_memory_monitor:set_vm_memory_high_watermark(MemLimitRatio),
    MemLimit = vm_memory_monitor:get_memory_limit(),
    case MemLimit of
        MemTotal -> ok;
        _        -> MemTotalToMemLimitRatio = (MemLimit * 100) / (MemTotal * 100),
                    ct:fail(
                        "Expected memory high watermark to be ~tp (~tp), but it was ~tp (~.1f)",
                        [MemTotal, MemLimitRatio, MemLimit, MemTotalToMemLimitRatio]
                    )
    end.

set_and_verify_vm_memory_high_watermark_absolute(MemLimit0) ->
    MemTotal = vm_memory_monitor:get_total_memory(),
    Interpreted = vm_memory_monitor:interpret_limit(MemLimit0, MemTotal),

    vm_memory_monitor:set_vm_memory_high_watermark(MemLimit0),
    MemLimit = vm_memory_monitor:get_memory_limit(),
    case MemLimit of
        MemTotal    -> ok;
        Interpreted -> ok;
        _           ->
            ct:fail("Expected memory high watermark to be ~tp but it was ~tp", [Interpreted, MemLimit])
    end,
    vm_memory_monitor:set_vm_memory_high_watermark(0.6).
