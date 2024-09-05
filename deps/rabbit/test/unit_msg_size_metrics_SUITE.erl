%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_msg_size_metrics_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile(export_all).

all() ->
    [
      {group, tests}
    ].

groups() ->
    [
      {tests, [],
       [
        smoketest
       ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    _ = rabbit_msg_size_metrics:init([{protocol, proto1}]),
    Config.

end_per_suite(Config) ->
    _ = rabbit_msg_size_metrics:cleanup([{protocol, proto1}]),
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

smoketest(_Config) ->

    OverviewBefore = rabbit_msg_size_metrics:overview([{protocol, proto1}]),

    _ = rabbit_msg_size_metrics:update(proto1, 100),
    _ = rabbit_msg_size_metrics:update(proto1, 60_000),

    OverviewAfter = rabbit_msg_size_metrics:overview([{protocol, proto1}]),

    ?assertEqual(
       #{256 => 1,
         65536 => 1,
         sum => 60_100},
       rabbit_msg_size_metrics:changed_buckets(OverviewAfter, OverviewBefore)),

    ExpectedHistValues =
        #{64 => 0,
          256 => 1,
          1024 => 0,
          4096 => 0,
          16384 => 0,
          65536 => 1,
          262144 => 0,
          1048576 => 0,
          4194304 => 0,
          16777216 => 0,
          67108864 => 0,
          268435456 => 0,
          infinity => 0,
          sum => 60_100},

    ?assertEqual(ExpectedHistValues, OverviewAfter),
    ?assertEqual(
       #{[{protocol, proto1}] => #{message_size_bytes => ExpectedHistValues}},
       rabbit_msg_size_metrics:overview()),

    ?assertEqual(
       #{message_size_bytes =>
             #{type => histogram,
               values =>
                   [{[{protocol,proto1}],
                     [{64,0},
                      {256,1},
                      {1024,1},
                      {4096,1},
                      {16384,1},
                      {65536,2},
                      {262144,2},
                      {1048576,2},
                      {4194304,2},
                      {16777216,2},
                      {67108864,2},
                      {268435456,2},
                      {infinity,2}],
                     2,
                     60_100}],
               help =>
                   "Size of messages received from publishers"}},
       rabbit_msg_size_metrics:prometheus_format()),

    %% Value larger than largest limit
    _ = rabbit_msg_size_metrics:update(proto1, 1_000_000_000_000),
    OverviewInf = rabbit_msg_size_metrics:overview([{protocol, proto1}]),

    ?assertEqual(
       #{infinity => 1,
         sum => 1_000_000_000_000},
       rabbit_msg_size_metrics:changed_buckets(OverviewInf, OverviewAfter)),

    ok.
