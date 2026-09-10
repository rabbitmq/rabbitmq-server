%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% A Common Test hook. A crash in `end_per_testcase'/`end_per_group'/
%% `end_per_suite' normally only shows up as a warning; the run is still
%% reported as a pass.
-module(cth_fail_on_teardown_crash).
-moduledoc false.

-export([id/1, init/2, terminate/1]).
-export([post_end_per_testcase/4, post_end_per_group/4, post_end_per_suite/4]).

id(_Opts) ->
    ?MODULE.

init(_Id, _Opts) ->
    {ok, false}.

%% A passing test case returns `ok' (or its `Config'); a crash in
%% `end_per_testcase' comes back as `{failed, {_, end_per_testcase, _}}'.
%% Returning `{fail, Return}' makes Common Test count the test case as
%% failed.
post_end_per_testcase(_TestcaseName, _Config, {failed, {_, end_per_testcase, _}} = Return, State) ->
    {{fail, Return}, State};
post_end_per_testcase(_TestcaseName, _Config, Return, State) ->
    {Return, State}.

%% `end_per_group'/`end_per_suite' are configuration functions, not test
%% cases: unlike above, returning `{fail, Reason}' here does not affect
%% `ct_run''s exit status. `terminate/1' is what fails the build for
%% these two; this just records whether either one crashed.
%%
%% `Return' is only `{error, _}' here if `end_per_group' itself crashed.
post_end_per_group(_GroupName, _Config, {error, _} = Return, _State) ->
    {{fail, Return}, true};
post_end_per_group(_GroupName, _Config, Return, State) ->
    {Return, State}.

post_end_per_suite(_SuiteName, _Config, {error, _} = Return, _State) ->
    {{fail, Return}, true};
post_end_per_suite(_SuiteName, _Config, Return, State) ->
    {Return, State}.

%% Runs once, after every suite's own `post_end_per_group'/
%% `post_end_per_suite' above, but before Common Test regenerates the
%% cross-run logs/index.html and logs/all_runs.html. Halting here leaves
%% this run's own reports intact, at the cost of those two files not
%% being updated with this run.
terminate(false) ->
    ok;
terminate(true) ->
    io:format(user,
              "~nlogs/index.html and logs/all_runs.html will not be "
              "updated with this run~n",
              []),
    halt(1).
