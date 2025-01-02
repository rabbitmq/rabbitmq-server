%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_app_management_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          app_management
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 2}
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
%% Application management.
%% -------------------------------------------------------------------

app_management(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, app_management1, [Config]).

app_management1(_Config) ->
    wait_for_application(rabbit),
    %% Starting, stopping and diagnostics.  Note that we don't try
    %% 'report' when the rabbit app is stopped and that we enable
    %% tracing for the duration of this function.
    ok = rabbit_trace:start(<<"/">>),
    ok = rabbit:stop(),
    ok = rabbit:stop(),
    ok = no_exceptions(rabbit, status, []),
    ok = no_exceptions(rabbit, environment, []),
    ok = rabbit:start(),
    ok = rabbit:start(),
    ok = no_exceptions(rabbit, status, []),
    ok = no_exceptions(rabbit, environment, []),
    ok = rabbit_trace:stop(<<"/">>),
    passed.

no_exceptions(Mod, Fun, Args) ->
    try erlang:apply(Mod, Fun, Args) of _ -> ok
    catch Type:Ex -> {Type, Ex}
    end.

wait_for_application(Application) ->
    wait_for_application(Application, 5000).

wait_for_application(_, Time) when Time =< 0 ->
    {error, timeout};
wait_for_application(Application, Time) ->
    Interval = 100,
    case lists:keyfind(Application, 1, application:which_applications()) of
        false ->
            timer:sleep(Interval),
            wait_for_application(Application, Time - Interval);
        _ -> ok
    end.
