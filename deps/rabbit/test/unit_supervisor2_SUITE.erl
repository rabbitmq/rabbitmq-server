%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_supervisor2_SUITE).

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
          check_shutdown_stop,
          check_shutdown_ignored
        ]}
    ].

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

check_shutdown_stop(_Config) ->
    ok = check_shutdown(stop,    200, 200, 2000).

check_shutdown_ignored(_Config) ->
    ok = check_shutdown(ignored,   1,   2, 2000).

check_shutdown(SigStop, Iterations, ChildCount, SupTimeout) ->
    {ok, Sup} = supervisor2:start_link(dummy_supervisor2, [SupTimeout]),
    Res = lists:foldl(
            fun (I, ok) ->
                    TestSupPid = erlang:whereis(dummy_supervisor2),
                    ChildPids =
                        [begin
                             {ok, ChildPid} =
                                 supervisor2:start_child(TestSupPid, []),
                             ChildPid
                         end || _ <- lists:seq(1, ChildCount)],
                    MRef = erlang:monitor(process, TestSupPid),
                    [P ! SigStop || P <- ChildPids],
                    ok = supervisor2:terminate_child(Sup, test_sup),
                    {ok, _} = supervisor2:restart_child(Sup, test_sup),
                    receive
                        {'DOWN', MRef, process, TestSupPid, shutdown} ->
                            ok;
                        {'DOWN', MRef, process, TestSupPid, Reason} ->
                            {error, {I, Reason}}
                    end;
                (_, R) ->
                    R
            end, ok, lists:seq(1, Iterations)),
    unlink(Sup),
    MSupRef = erlang:monitor(process, Sup),
    exit(Sup, shutdown),
    receive
        {'DOWN', MSupRef, process, Sup, _Reason} ->
            ok
    end,
    Res.
