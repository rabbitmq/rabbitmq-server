%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(sup_delayed_restart_SUITE).

-behaviour(supervisor2).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
      delayed_restart
    ].

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

delayed_restart(_Config) ->
    passed = with_sup(simple_one_for_one,
                      fun (SupPid) ->
                              {ok, _ChildPid} =
                                  supervisor2:start_child(SupPid, []),
                              test_supervisor_delayed_restart(SupPid)
                      end),
    passed = with_sup(one_for_one, fun test_supervisor_delayed_restart/1).

test_supervisor_delayed_restart(SupPid) ->
    ok = ping_child(SupPid),
    ok = exit_child(SupPid),
    timer:sleep(100),
    ok = ping_child(SupPid),
    ok = exit_child(SupPid),
    timer:sleep(100),
    timeout = ping_child(SupPid),
    timer:sleep(1010),
    ok = ping_child(SupPid),
    passed.

with_sup(RestartStrategy, Fun) ->
    {ok, SupPid} = supervisor2:start_link(?MODULE, [RestartStrategy]),
    Res = Fun(SupPid),
    unlink(SupPid),
    exit(SupPid, shutdown),
    Res.

init([RestartStrategy]) ->
    {ok, {{RestartStrategy, 1, 1},
          [{test, {?MODULE, start_child, []}, {permanent, 1},
            16#ffffffff, worker, [?MODULE]}]}}.

start_child() ->
    {ok, proc_lib:spawn_link(fun run_child/0)}.

ping_child(SupPid) ->
    Ref = make_ref(),
    with_child_pid(SupPid, fun(ChildPid) -> ChildPid ! {ping, Ref, self()} end),
    receive {pong, Ref} -> ok
    after 1000          -> timeout
    end.

exit_child(SupPid) ->
    with_child_pid(SupPid, fun(ChildPid) -> exit(ChildPid, abnormal) end),
    ok.

with_child_pid(SupPid, Fun) ->
    case supervisor2:which_children(SupPid) of
        [{_Id, undefined, worker, [?MODULE]}] -> ok;
        [{_Id, restarting, worker, [?MODULE]}] -> ok;
        [{_Id,  ChildPid, worker, [?MODULE]}] -> Fun(ChildPid);
        []                                     -> ok
    end.

run_child() ->
    receive {ping, Ref, Pid} -> Pid ! {pong, Ref},
                                run_child()
    end.
