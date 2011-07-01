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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(mirrored_supervisor_tests).

-compile([export_all]).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-behaviour(gen_server).
-behaviour(mirrored_supervisor).

%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

all_tests() ->
    passed = test_migrate(),
    passed = test_migrate_twice(),
    passed = test_already_there(),
    passed = test_delete_restart(),
    passed.

test_migrate() ->
    with_sups(fun([A, _]) ->
                      mirrored_supervisor:start_child(a, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill(A),
                      Pid2 = pid_of(worker),
                      false = (Pid1 =:= Pid2)
              end, [a, b]).

test_migrate_twice() ->
    with_sups(fun([A, B]) ->
                      mirrored_supervisor:start_child(a, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill(A),
                      with_sups(fun([_]) ->
                                        kill(B),
                                        Pid2 = pid_of(worker),
                                        false = (Pid1 =:= Pid2)
                                end, [c])
              end, [a, b]).

test_already_there() ->
    with_sups(fun([_, _]) ->
                      S = childspec(worker),
                      {ok, Pid} = mirrored_supervisor:start_child(a, S),
                      {ok, Pid} = mirrored_supervisor:start_child(b, S)
              end, [a, b]).

test_delete_restart() ->
    with_sups(fun([_, _]) ->
                      S = childspec(worker),
                      {ok, Pid1} = mirrored_supervisor:start_child(a, S),
                      mirrored_supervisor:terminate_child(a, worker),
                      mirrored_supervisor:delete_child(a, worker),
                      {ok, Pid2} = mirrored_supervisor:start_child(b, S),
                      false = (Pid1 =:= Pid2),
                      mirrored_supervisor:restart_child(a, worker),
                      Pid3 = pid_of(worker),
                      false = (Pid2 =:= Pid3)
              end, [a, b]).


%% ---------------------------------------------------------------------------

with_sups(Fun, Sups) ->
    Pids = [begin {ok, Pid} = start_sup(Sup), Pid end || Sup <- Sups],
    Fun(Pids),
    [kill(Pid) || Pid <- Pids, is_process_alive(Pid)],
    passed.

start_sup(Name) ->
    start_sup(Name, group).

start_sup(Name, Group) ->
    {ok, Pid} = mirrored_supervisor:start_link({local, Name}, Group,
                                               ?MODULE, []),
    %% We are not a supervisor, when we kill the supervisor we do not
    %% want to die!
    unlink(Pid),
    {ok, Pid}.

childspec(Id) ->
    {Id, {?MODULE, start_gs, [Id]}, transient, 16#ffffffff, worker, [?MODULE]}.

start_gs(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, [], []).

pid_of(Id) ->
    {received, Pid, ping} = call(Id, ping),
    {ok, Pid}.

call(Id, Msg) -> call(Id, Msg, 100, 10).

call(Id, Msg, 0, _Decr) ->
    exit({timeout_waiting_for_server, Id, Msg});

call(Id, Msg, MaxDelay, Decr) ->
    try
        gen_server:call(Id, Msg, infinity)
    catch exit:_ -> timer:sleep(Decr),
                    call(Id, Msg, MaxDelay - Decr, Decr)
    end.

kill(Pid) ->
    exit(Pid, kill),
    timer:sleep(100).

%% ---------------------------------------------------------------------------
%% Dumb gen_server we can supervise
%% ---------------------------------------------------------------------------

%% Cheeky: this is used for both the gen_server and supervisor
%% behaviours. So our gen server has a weird-looking state. But we
%% don't care.
init(ChildSpecs) ->
    {ok, {{one_for_one, 3, 10}, ChildSpecs}}.

handle_call(Msg, _From, State) ->
    {reply, {received, self(), Msg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
