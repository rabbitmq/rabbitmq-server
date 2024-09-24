%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_msg_size_metrics_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile([nowarn_export_all, export_all]).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [],
      [
       prometheus_format
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    ok = rabbit_msg_size_metrics:init(fake_protocol),
    Config.

end_per_suite(Config) ->
    ok = rabbit_msg_size_metrics:cleanup(fake_protocol),
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

prometheus_format(_Config) ->
    MsgSizes = [1, 100, 1_000_000_000, 99_000_000, 15_000, 15_000],
    [ok = rabbit_msg_size_metrics:observe(fake_protocol, MsgSize) || MsgSize <- MsgSizes],

    ?assertEqual(
       #{message_size_bytes =>
         #{type => histogram,
           help => "Size of messages received from publishers",
           values => [{
             [{protocol, fake_protocol}],
             [{100, 2},
              {1_000, 2},
              {10_000, 2},
              {100_000, 4},
              {1_000_000, 4},
              {10_000_000, 4},
              {50_000_000, 4},
              {100_000_000, 5},
              {infinity, 6}],
             length(MsgSizes),
             lists:sum(MsgSizes)}]}},
       rabbit_msg_size_metrics:prometheus_format()).
