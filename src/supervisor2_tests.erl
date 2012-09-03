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

-export([test_all/0, start_sup/0, start_link/0, start_child/0]).
-export([init/1]).

-define(NUM_CHILDREN, 1000).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

test_all() ->
    eunit:test(supervisor2, [verbose]).

terminate_simple_children_without_deadlock_test_() ->
    lists:flatten(
      lists:duplicate(
        100,[{setup, fun init_supervisor/0,
              {with, [fun ensure_children_are_alive/1,
                      fun shutdown_and_verify_all_children_died/1]}},
             {setup, fun init_supervisor/0,
              {with, [fun shutdown_whilst_interleaving_exits_occur/1]}}])).

%%
%% Public (test facing) API
%%

start_sup() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

start_link() ->
    Pid = spawn_link(fun loop_infinity/0),
    {ok, Pid}.

start_child() ->
    {ok, Pid} = supervisor2:start_child(?MODULE, []),
    Pid.

%%
%% supervisor2 callbacks
%%

init([parent]) ->
    {ok, {{one_for_one, 0, 1},
      [{test_sup, {?MODULE, start_sup, []},
        transient, 5000, supervisor, [?MODULE]}]}};
init([]) ->
    {ok, {{simple_one_for_one_terminate, 0, 1},
          [{test_worker, {?MODULE, start_link, []},
            temporary, 1000, worker, []}]}}.

%%
%% Private API
%%

ensure_children_are_alive({_, ChildPids}) ->
    ?assertEqual(true,
         lists:all(fun erlang:is_process_alive/1, ChildPids)).

shutdown_and_verify_all_children_died({Parent, ChildPids}=State) ->
    ensure_children_are_alive(State),
    TestSup = erlang:whereis(?MODULE),
    ?assertEqual(true, erlang:is_process_alive(TestSup)),
    ?assertMatch(ok, supervisor2:terminate_child(Parent, test_sup)),
    ?assertMatch([], [P || P <- ChildPids,
               erlang:is_process_alive(P)]),
    ?assertEqual(false, erlang:is_process_alive(TestSup)).

shutdown_whilst_interleaving_exits_occur({Parent, ChildPids}=State) ->
    ensure_children_are_alive(State),
    TestPid = self(),
    Ref = erlang:make_ref(),
    spawn(fun() ->
          TestPid ! {Ref, supervisor2:terminate_child(Parent, test_sup)}
      end),
    [P ! stop || P <- ChildPids],
    receive {Ref, Res} ->
        ?assertEqual(ok, Res)
    end.

init_supervisor() ->
    {ok, Pid} = supervisor2:start_link(?MODULE, [parent]),
    {Pid, [start_child() || _ <- lists:seq(1, ?NUM_CHILDREN)]}.

loop_infinity() ->
    receive
        stop -> ok
    end.

