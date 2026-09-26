%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Without this hook a crash in `end_per_testcase`, `end_per_group` or
%% `end_per_suite` is only a warning and the run still passes.
-module(cth_fail_on_teardown_crash).
-moduledoc false.

-export([id/1, init/2, terminate/1]).
-export([post_end_per_testcase/4, post_end_per_group/4, post_end_per_suite/4]).

id(_Opts) ->
    ?MODULE.

init(_Id, _Opts) ->
    {ok, false}.

post_end_per_testcase(_TestcaseName, _Config, {failed, {_, end_per_testcase, _}} = Return, State) ->
    {{fail, Return}, State};
post_end_per_testcase(_TestcaseName, _Config, Return, State) ->
    {Return, State}.

%% A `{fail, _}` return from `end_per_group` or `end_per_suite` does not
%% change the `ct_run` exit status, so `terminate/1` fails the build instead.
post_end_per_group(_GroupName, _Config, {error, _} = Return, _State) ->
    {{fail, Return}, true};
post_end_per_group(_GroupName, _Config, Return, State) ->
    {Return, State}.

post_end_per_suite(_SuiteName, _Config, {error, _} = Return, _State) ->
    {{fail, Return}, true};
post_end_per_suite(_SuiteName, _Config, Return, State) ->
    {Return, State}.

%% Halting here keeps this run's own reports but skips the regeneration of
%% `logs/index.html` and `logs/all_runs.html`.
terminate(false) ->
    ok;
terminate(true) ->
    io:format(user,
              "~nlogs/index.html and logs/all_runs.html will not be "
              "updated with this run~n",
              []),
    halt(1).
