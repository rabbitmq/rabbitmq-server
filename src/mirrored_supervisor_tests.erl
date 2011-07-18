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
    passed = test_large_group(),
    passed = test_childspecs_at_init(),
    passed = test_anonymous_supervisors(),
    passed = test_no_migration_on_shutdown(),
    passed = test_start_idempotence(),
    passed.

%% Simplest test
test_migrate() ->
    with_sups(fun([A, _]) ->
                      mirrored_supervisor:start_child(a, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill(A),
                      Pid2 = pid_of(worker),
                      false = (Pid1 =:= Pid2)
              end, [a, b]).

%% Is migration transitive?
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

%% Can't start the same child twice
test_already_there() ->
    with_sups(fun([_, _]) ->
                      S = childspec(worker),
                      {ok, Pid} = mirrored_supervisor:start_child(a, S),
                      {ok, Pid} = mirrored_supervisor:start_child(b, S)
              end, [a, b]).

%% Deleting and restarting should work as per a normal supervisor
test_delete_restart() ->
    with_sups(fun([_, _]) ->
                      S = childspec(worker),
                      {ok, Pid1} = mirrored_supervisor:start_child(a, S),
                      ok = mirrored_supervisor:terminate_child(a, worker),
                      ok = mirrored_supervisor:delete_child(a, worker),
                      {ok, Pid2} = mirrored_supervisor:start_child(b, S),
                      false = (Pid1 =:= Pid2),
                      ok = mirrored_supervisor:terminate_child(b, worker),
                      {ok, Pid3} = mirrored_supervisor:restart_child(b, worker),
                      Pid3 = pid_of(worker),
                      false = (Pid2 =:= Pid3)
              end, [a, b]).

%% Not all the members of the group should actually do the failover
test_large_group() ->
    with_sups(fun([A, _, _, _]) ->
                      mirrored_supervisor:start_child(a, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill(A),
                      Pid2 = pid_of(worker),
                      false = (Pid1 =:= Pid2)
              end, [a, b, c, d]).

%% Do childspecs work when returned from init?
test_childspecs_at_init() ->
    S = childspec(worker),
    with_sups(fun([A, _]) ->
                      Pid1 = pid_of(worker),
                      kill(A),
                      Pid2 = pid_of(worker),
                      false = (Pid1 =:= Pid2)
              end, [{a, [S]}, {b, [S]}]).

test_anonymous_supervisors() ->
    with_sups(fun([A, _B]) ->
                      mirrored_supervisor:start_child(A, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill(A),
                      Pid2 = pid_of(worker),
                      false = (Pid1 =:= Pid2)
              end, [anon, anon]).

%% When a mirrored_supervisor terminates, we should not migrate, but
%% the whole supervisor group should shut down. To test this we set up
%% a situation where the gen_server will only fail if it's running
%% under the supervisor called 'evil'. It should not migrate to
%% 'good' and survive, rather the whole group should go away.
test_no_migration_on_shutdown() ->
    with_sups(fun([Evil, _]) ->
                      mirrored_supervisor:start_child(Evil, childspec(worker)),
                      try
                          call(worker, ping),
                          exit(worker_should_not_have_migrated)
                      catch exit:{timeout_waiting_for_server, _} ->
                              ok
                      end,
                      timer:sleep(1000)
              end, [evil, good]).

test_start_idempotence() ->
    with_sups(fun([A]) ->
                      mirrored_supervisor:start_child(a, childspec(worker)),
                      mirrored_supervisor:start_child(a, childspec(worker))
              end, [a]).

%% ---------------------------------------------------------------------------

with_sups(Fun, Sups) ->
    Pids = [begin {ok, Pid} = start_sup(Sup), Pid end || Sup <- Sups],
    Fun(Pids),
    [kill(Pid) || Pid <- Pids, is_process_alive(Pid)],
    passed.

start_sup(Spec) ->
    start_sup(Spec, group).

start_sup({Name, ChildSpecs}, Group) ->
    {ok, Pid} = start_sup0(Name, Group, ChildSpecs),
    %% We are not a supervisor, when we kill the supervisor we do not
    %% want to die!
    unlink(Pid),
    {ok, Pid};

start_sup(Name, Group) ->
    start_sup({Name, []}, Group).

start_sup0(anon, Group, ChildSpecs) ->
    mirrored_supervisor:start_link(Group, ?MODULE, {sup, ChildSpecs});

start_sup0(Name, Group, ChildSpecs) ->
    mirrored_supervisor:start_link({local, Name},
                                   Group, ?MODULE, {sup, ChildSpecs}).

childspec(Id) ->
    {Id, {?MODULE, start_gs, [Id]},
     transient, 16#ffffffff, worker, [?MODULE]}.

start_gs(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, server, []).

pid_of(Id) ->
    {received, Pid, ping} = call(Id, ping),
    Pid.

call(Id, Msg) -> call(Id, Msg, 100, 10).

call(Id, Msg, 0, _Decr) ->
    exit({timeout_waiting_for_server, {Id, Msg}});

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

init({sup, ChildSpecs}) ->
    {ok, {{one_for_one, 0, 1}, ChildSpecs}};

init(server) ->
    {ok, state}.

handle_call(Msg, _From, State) ->
    die_if_my_supervisor_is_evil(),
    {reply, {received, self(), Msg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

die_if_my_supervisor_is_evil() ->
    try lists:keyfind(self(), 2, mirrored_supervisor:which_children(evil)) of
        false -> ok;
        _     -> exit(doooom)
    catch
        exit:{noproc, _} -> ok
    end.
