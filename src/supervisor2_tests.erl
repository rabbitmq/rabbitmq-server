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
%% Copyright (c) 2011-2013 VMware, Inc.  All rights reserved.
%%

-module(supervisor2_tests).
-behaviour(supervisor2).

-include_lib("eunit/include/eunit.hrl").

-define(ASSERT, true).
-define(EUNIT_NOAUTO, true).

-export([test_all/0, start_link/0]).
-export([start_link_bad/0]).
-export([init/1]).

test_all() ->
    catch ets:new(?MODULE, [named_table, public]),
    %% ok = check_shutdown(stop,    200, 200, 2000),
    %% ok = check_shutdown(ignored,   1,   2, 2000),
    %% ok = check_logging(transient),
    ets:delete(?MODULE, bang),
    ok = check_logging({permanent, 1}).

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

check_logging(How) ->
    process_flag(trap_exit, true),
    {ok, Sup} = supervisor2:start_link(?MODULE, [bang, How]),
    io:format("super pid = ~p~n", [Sup]),
    MRef = erlang:monitor(process, Sup),
    [Pid] = supervisor2:find_child(Sup, test_try_again_sup),
    io:format("Pid == ~p~nChildren == ~p~n", [Pid, supervisor2:which_children(Sup)]),
    Pid ! {shutdown, bang},
    io:format("restart issued - awaiting sup death~n"),
    receive
        {'DOWN', MRef, process, Sup, Reason} ->
            io:format("stopped Sup == ~p~n", [Reason])
    end.

start_link() ->
    Pid = spawn_link(fun () ->
                             process_flag(trap_exit, true),
                             receive stop -> ok end
                     end),
    {ok, Pid}.

start_link_bad() ->
    Boom = ets:lookup(?MODULE, bang),
    case Boom of
        [{bang, true}] -> io:format("BOOM!~n"), exit(bang);
        _              -> ok
    end,
    io:format("no Boom - starting server~n"),
    Pid = spawn_link(fun () ->
                             process_flag(trap_exit, true),
                             receive
                                 {shutdown, Bang} ->
                                     ets:insert(?MODULE, [{bang, true}]),
                                     io:format("exiting...~n"),
                                     exit(Bang);
                                 shutdown ->
                                     io:format("exiting (shutdown)...~n"),
                                     exit(shutdown);
                                 Other ->
                                     io:format("odd signal: ~p~n", [Other]),
                                     exit(Other)
                             end
                     end),
    {ok, Pid}.

init([bang, How]) ->
    {ok, {{one_for_one, 3, 10},
          [{test_try_again_sup, {?MODULE, start_link_bad, []},
            How, 5000, worker, [?MODULE]}]}};

init([Timeout]) ->
    {ok, {{one_for_one, 0, 1},
          [{test_sup, {supervisor2, start_link,
                       [{local, ?MODULE}, ?MODULE, []]},
            transient, Timeout, supervisor, [?MODULE]}]}};
init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{test_worker, {?MODULE, start_link, []},
            temporary, 1000, worker, [?MODULE]}]}}.

