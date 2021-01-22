%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_credit_flow_SUITE).

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
          credit_flow_settings
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


%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

credit_flow_settings(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, credit_flow_settings1, [Config]).

credit_flow_settings1(_Config) ->
    passed = test_proc(400, 200, {400, 200}),
    passed = test_proc(600, 300),
    passed.

test_proc(InitialCredit, MoreCreditAfter) ->
    test_proc(InitialCredit, MoreCreditAfter, {InitialCredit, MoreCreditAfter}).
test_proc(InitialCredit, MoreCreditAfter, Settings) ->
    Pid = spawn(?MODULE, dummy, [Settings]),
    Pid ! {credit, self()},
    {InitialCredit, MoreCreditAfter} =
        receive
            {credit, Val} -> Val
        end,
    passed.

dummy(Settings) ->
    credit_flow:send(self()),
    receive
        {credit, From} ->
            From ! {credit, Settings};
        _      ->
            dummy(Settings)
    end.
