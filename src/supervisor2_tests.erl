%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
%%

-module(supervisor2_tests).
-behaviour(supervisor2).

-export([test_all/0, start_link/0]).
-export([init/1]).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_RUNS, 2000).
-define(SLOW_TEST_RUNS, 45).
-define(CHILDREN, 100).

test_all() ->
    eunit:test(?MODULE, [verbose]).

simple_child_shutdown_without_deadlock_test_() ->
    lists:duplicate(?TEST_RUNS,
                    [{timeout, 60, fun test_clean_stop/0}]).

simple_child_shutdown_with_timeout_test_() ->
    lists:duplicate(?SLOW_TEST_RUNS,
                    [{timeout, 120, fun test_timeout/0}]).

test_clean_stop() ->
    test_it(stop).

test_timeout() ->
    test_it(ignored).

test_it(SigStop) ->
    {ok, Pid} = supervisor2:start_link(?MODULE, [?CHILDREN]),
    start_and_terminate_children(SigStop, Pid, ?CHILDREN),
    unlink(Pid),
    exit(Pid, shutdown).

start_link() ->
    Pid = spawn_link(fun () ->
                             process_flag(trap_exit, true),
                             receive stop -> ok end
                     end),
    {ok, Pid}.

init([N]) ->
    {ok, {{one_for_one, 0, 1},
          [{test_sup, {supervisor2, start_link,
                       [{local, ?MODULE}, ?MODULE, []]},
            transient, N * 100, supervisor, [?MODULE]}]}};
init([]) ->
    {ok, {{simple_one_for_one_terminate, 0, 1},
          [{test_worker, {?MODULE, start_link, []},
            temporary, 1000, worker, [?MODULE]}]}}.

start_and_terminate_children(SigStop, Sup, N) ->
    TestSupPid = whereis(?MODULE),
    ChildPids = [begin
                     {ok, ChildPid} = supervisor2:start_child(TestSupPid, []),
                     ChildPid
                 end || _ <- lists:seq(1, N)],
    erlang:monitor(process, TestSupPid),
    [P ! SigStop || P <- ChildPids],
    ?assertEqual(ok, supervisor2:terminate_child(Sup, test_sup)),
    ?assertMatch({ok,_}, supervisor2:restart_child(Sup, test_sup)),
    receive
        {'DOWN', _MRef, process, TestSupPid, Reason} ->
                ?assertMatch(shutdown, Reason)
    end.
