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

test_all() ->
    ok = check_shutdown(stop,    200, 200, 2000),
    ok = check_shutdown(ignored,   1,   2, 2000).

check_shutdown(SigStop, Iterations, ChildCount, SupTimeout) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, [SupTimeout]),
    Res = lists:foldl(
            fun (I, ok) ->
                    TestSupPid = erlang:whereis(?MODULE),
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
    exit(Sup, shutdown),
    Res.

start_link() ->
    Pid = spawn_link(fun () ->
                             process_flag(trap_exit, true),
                             receive stop -> ok end
                     end),
    {ok, Pid}.

init([Timeout]) ->
    {ok, {{one_for_one, 0, 1},
          [{test_sup, {supervisor2, start_link,
                       [{local, ?MODULE}, ?MODULE, []]},
            transient, Timeout, supervisor, [?MODULE]}]}};
init([]) ->
    {ok, {{simple_one_for_one_terminate, 0, 1},
          [{test_worker, {?MODULE, start_link, []},
            temporary, 1000, worker, [?MODULE]}]}}.
