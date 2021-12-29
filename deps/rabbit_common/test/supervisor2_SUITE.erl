%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(supervisor2_SUITE).

-behaviour(supervisor2).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() -> [intrinsic, delayed_restart].

intrinsic(_Config) ->
    false = process_flag(trap_exit, true),
    Intensity = 5,
    Args = {one_for_one, intrinsic, Intensity},
    {passed, SupPid} = with_sup(Args, fun test_supervisor_intrinsic/1),
    receive
        {'EXIT', SupPid, shutdown} -> ok
    end,
    false = is_process_alive(SupPid).

delayed_restart(_Config) ->
    DelayInSeconds = 1,
    Intensity = 1,
    Args0 = {simple_one_for_one, {permanent, DelayInSeconds}, Intensity},
    F = fun(SupPid) ->
        {ok, _ChildPid} =
            supervisor2:start_child(SupPid, []),
        test_supervisor_delayed_restart(SupPid)
    end,
    {passed, _} = with_sup(Args0, F),

    Args1 = {one_for_one, {permanent, DelayInSeconds}, Intensity},
    {passed, _} = with_sup(Args1, fun test_supervisor_delayed_restart/1).

test_supervisor_intrinsic(SupPid) ->
    ok = ping_child(SupPid),

    ok = exit_child(SupPid, abnormal),
    ok = timer:sleep(100),
    ok = ping_child(SupPid),

    ok = exit_child(SupPid, {shutdown, restart}),
    ok = timer:sleep(100),
    ok = ping_child(SupPid),

    ok = exit_child(SupPid, shutdown),
    ok = timer:sleep(100),
    passed.

test_supervisor_delayed_restart(SupPid) ->
    ok = ping_child(SupPid),

    ok = exit_child(SupPid, abnormal),
    ok = timer:sleep(100),
    ok = ping_child(SupPid),

    ok = exit_child(SupPid, abnormal),
    ok = timer:sleep(100),
    timeout = ping_child(SupPid),

    ok = timer:sleep(1010),
    ok = ping_child(SupPid),
    passed.

with_sup({RestartStrategy, Restart, Intensity}, Fun) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, [RestartStrategy, Restart, Intensity]),
    Res = Fun(SupPid),
    true = unlink(SupPid),
    {Res, SupPid}.

init([RestartStrategy, Restart, Intensity]) ->
    SupFlags = #{
        strategy => RestartStrategy,
        intensity => Intensity,
        period => 1
    },
    ChildSpec = #{
        id => test,
        start => {?MODULE, start_child, []},
        restart => Restart,
        shutdown => 16#ffffffff,
        type => worker,
        modules => [?MODULE]
    },
    {ok, {SupFlags, [ChildSpec]}}.

start_child() ->
    {ok, proc_lib:spawn_link(fun run_child/0)}.

ping_child(SupPid) ->
    Ref = make_ref(),
    F = fun(ChildPid) ->
        ChildPid ! {ping, Ref, self()}
    end,
    with_child_pid(SupPid, F),
    receive
        {pong, Ref} -> ok
    after 1000 -> timeout
    end.

exit_child(SupPid, ExitType) ->
    F = fun(ChildPid) ->
        exit(ChildPid, ExitType)
    end,
    with_child_pid(SupPid, F),
    ok.

with_child_pid(SupPid, Fun) ->
    case supervisor2:which_children(SupPid) of
        [{_Id, undefined, worker, [?MODULE]}] -> ok;
        [{_Id, restarting, worker, [?MODULE]}] -> ok;
        [{_Id, ChildPid, worker, [?MODULE]}] -> Fun(ChildPid);
        [] -> ok
    end.

run_child() ->
    receive
        {ping, Ref, Pid} ->
            Pid ! {pong, Ref},
            run_child()
    end.
