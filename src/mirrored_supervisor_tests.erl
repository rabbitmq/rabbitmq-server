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

-module(mirrored_supervisor_tests).

-compile([export_all]).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-behaviour(gen_server).
-behaviour(mirrored_supervisor).

-define(MS,  mirrored_supervisor).

%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

all_tests() ->
    passed = test_migrate(),
    passed = test_migrate_twice(),
    passed = test_already_there(),
    passed = test_delete_restart(),
    passed = test_which_children(),
    passed = test_large_group(),
    passed = test_childspecs_at_init(),
    passed = test_anonymous_supervisors(),
    passed = test_no_migration_on_shutdown(),
    passed = test_start_idempotence(),
    passed = test_unsupported(),
    passed = test_ignore(),
    passed = test_startup_failure(),
    passed.

%% Simplest test
test_migrate() ->
    with_sups(fun([A, _]) ->
                      ?MS:start_child(a, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill_registered(A, Pid1),
                      Pid2 = pid_of(worker),
                      false = (Pid1 =:= Pid2)
              end, [a, b]).

%% Is migration transitive?
test_migrate_twice() ->
    with_sups(fun([A, B]) ->
                      ?MS:start_child(a, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill_registered(A, Pid1),
                      {ok, C} = start_sup(c),
                      Pid2 = pid_of(worker),
                      kill_registered(B, Pid2),
                      Pid3 = pid_of(worker),
                      false = (Pid1 =:= Pid3),
                      kill(C)
              end, [a, b]).

%% Can't start the same child twice
test_already_there() ->
    with_sups(fun([_, _]) ->
                      S = childspec(worker),
                      {ok, Pid}                       = ?MS:start_child(a, S),
                      {error, {already_started, Pid}} = ?MS:start_child(b, S)
              end, [a, b]).

%% Deleting and restarting should work as per a normal supervisor
test_delete_restart() ->
    with_sups(fun([_, _]) ->
                      S = childspec(worker),
                      {ok, Pid1} = ?MS:start_child(a, S),
                      {error, running} = ?MS:delete_child(a, worker),
                      ok = ?MS:terminate_child(a, worker),
                      ok = ?MS:delete_child(a, worker),
                      {ok, Pid2} = ?MS:start_child(b, S),
                      false = (Pid1 =:= Pid2),
                      ok = ?MS:terminate_child(b, worker),
                      {ok, Pid3} = ?MS:restart_child(b, worker),
                      Pid3 = pid_of(worker),
                      false = (Pid2 =:= Pid3),
                      %% Not the same supervisor as the worker is on
                      ok = ?MS:terminate_child(a, worker),
                      ok = ?MS:delete_child(a, worker),
                      {ok, Pid4} = ?MS:start_child(a, S),
                      false = (Pid3 =:= Pid4)
              end, [a, b]).

test_which_children() ->
    with_sups(
      fun([A, B] = Both) ->
              ?MS:start_child(A, childspec(worker)),
              assert_wc(Both, fun ([C]) -> true = is_pid(wc_pid(C)) end),
              ok = ?MS:terminate_child(a, worker),
              assert_wc(Both, fun ([C]) -> undefined = wc_pid(C) end),
              {ok, _} = ?MS:restart_child(a, worker),
              assert_wc(Both, fun ([C]) -> true = is_pid(wc_pid(C)) end),
              ?MS:start_child(B, childspec(worker2)),
              assert_wc(Both, fun (C) -> 2 = length(C) end)
      end, [a, b]).

assert_wc(Sups, Fun) ->
    [Fun(?MS:which_children(Sup)) || Sup <- Sups].

wc_pid(Child) ->
    {worker, Pid, worker, [mirrored_supervisor_tests]} = Child,
    Pid.

%% Not all the members of the group should actually do the failover
test_large_group() ->
    with_sups(fun([A, _, _, _]) ->
                      ?MS:start_child(a, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill_registered(A, Pid1),
                      Pid2 = pid_of(worker),
                      false = (Pid1 =:= Pid2)
              end, [a, b, c, d]).

%% Do childspecs work when returned from init?
test_childspecs_at_init() ->
    S = childspec(worker),
    with_sups(fun([A, _]) ->
                      Pid1 = pid_of(worker),
                      kill_registered(A, Pid1),
                      Pid2 = pid_of(worker),
                      false = (Pid1 =:= Pid2)
              end, [{a, [S]}, {b, [S]}]).

test_anonymous_supervisors() ->
    with_sups(fun([A, _B]) ->
                      ?MS:start_child(A, childspec(worker)),
                      Pid1 = pid_of(worker),
                      kill_registered(A, Pid1),
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
                      ?MS:start_child(Evil, childspec(worker)),
                      try
                          call(worker, ping, 1000, 100),
                          exit(worker_should_not_have_migrated)
                      catch exit:{timeout_waiting_for_server, _, _} ->
                              ok
                      end
              end, [evil, good]).

test_start_idempotence() ->
    with_sups(fun([_]) ->
                      CS = childspec(worker),
                      {ok, Pid}                       = ?MS:start_child(a, CS),
                      {error, {already_started, Pid}} = ?MS:start_child(a, CS),
                      ?MS:terminate_child(a, worker),
                      {error, already_present}        = ?MS:start_child(a, CS)
              end, [a]).

test_unsupported() ->
    try
        ?MS:start_link({global, foo}, get_group(group), ?MODULE,
                       {sup, one_for_one, []}),
        exit(no_global)
    catch error:badarg ->
            ok
    end,
    try
        ?MS:start_link({local, foo}, get_group(group), ?MODULE,
                       {sup, simple_one_for_one, []}),
        exit(no_sofo)
    catch error:badarg ->
            ok
    end,
    passed.

%% Just test we don't blow up
test_ignore() ->
    ?MS:start_link({local, foo}, get_group(group), ?MODULE,
                   {sup, fake_strategy_for_ignore, []}),
    passed.

test_startup_failure() ->
    [test_startup_failure(F) || F <- [want_error, want_exit]],
    passed.

test_startup_failure(Fail) ->
    process_flag(trap_exit, true),
    ?MS:start_link(get_group(group), ?MODULE,
                   {sup, one_for_one, [childspec(Fail)]}),
    receive
        {'EXIT', _, shutdown} ->
            ok
    after 1000 ->
            exit({did_not_exit, Fail})
    end,
    process_flag(trap_exit, false).

%% ---------------------------------------------------------------------------

with_sups(Fun, Sups) ->
    inc_group(),
    Pids = [begin {ok, Pid} = start_sup(Sup), Pid end || Sup <- Sups],
    Fun(Pids),
    [kill(Pid) || Pid <- Pids, is_process_alive(Pid)],
    timer:sleep(500),
    passed.

start_sup(Spec) ->
    start_sup(Spec, group).

start_sup({Name, ChildSpecs}, Group) ->
    {ok, Pid} = start_sup0(Name, get_group(Group), ChildSpecs),
    %% We are not a supervisor, when we kill the supervisor we do not
    %% want to die!
    unlink(Pid),
    {ok, Pid};

start_sup(Name, Group) ->
    start_sup({Name, []}, Group).

start_sup0(anon, Group, ChildSpecs) ->
    ?MS:start_link(Group, ?MODULE, {sup, one_for_one, ChildSpecs});

start_sup0(Name, Group, ChildSpecs) ->
    ?MS:start_link({local, Name}, Group, ?MODULE,
                   {sup, one_for_one, ChildSpecs}).

childspec(Id) ->
    {Id, {?MODULE, start_gs, [Id]}, transient, 16#ffffffff, worker, [?MODULE]}.

start_gs(want_error) ->
    {error, foo};

start_gs(want_exit) ->
    exit(foo);

start_gs(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, server, []).

pid_of(Id) ->
    {received, Pid, ping} = call(Id, ping),
    Pid.

inc_group() ->
    Count = case get(counter) of
                undefined -> 0;
                C         -> C
            end + 1,
    put(counter, Count).

get_group(Group) ->
    {Group, get(counter)}.

call(Id, Msg) -> call(Id, Msg, 10*1000, 100).

call(Id, Msg, 0, _Decr) ->
    exit({timeout_waiting_for_server, {Id, Msg}, erlang:get_stacktrace()});

call(Id, Msg, MaxDelay, Decr) ->
    try
        gen_server:call(Id, Msg, infinity)
    catch exit:_ -> timer:sleep(Decr),
                    call(Id, Msg, MaxDelay - Decr, Decr)
    end.

kill(Pid) -> kill(Pid, []).
kill(Pid, Wait) when is_pid(Wait) -> kill(Pid, [Wait]);
kill(Pid, Waits) ->
    erlang:monitor(process, Pid),
    [erlang:monitor(process, P) || P <- Waits],
    exit(Pid, bang),
    kill_wait(Pid),
    [kill_wait(P) || P <- Waits].

kill_registered(Pid, Child) ->
    {registered_name, Name} = erlang:process_info(Child, registered_name),
    kill(Pid, Child),
    false = (Child =:= whereis(Name)),
    ok.

kill_wait(Pid) ->
    receive
        {'DOWN', _Ref, process, Pid, _Reason} ->
            ok
    end.

%% ---------------------------------------------------------------------------
%% Dumb gen_server we can supervise
%% ---------------------------------------------------------------------------

init({sup, fake_strategy_for_ignore, _ChildSpecs}) ->
    ignore;

init({sup, Strategy, ChildSpecs}) ->
    {ok, {{Strategy, 0, 1}, ChildSpecs}};

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
    try lists:keysearch(self(), 2, ?MS:which_children(evil)) of
        false -> ok;
        _     -> exit(doooom)
    catch
        exit:{noproc, _} -> ok
    end.
