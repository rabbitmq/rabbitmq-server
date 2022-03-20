%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(mirrored_supervisor_SUITE).

-behaviour(mirrored_supervisor).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(MS,     mirrored_supervisor).
-define(SERVER, mirrored_supervisor_SUITE_gs).

all() ->
    [
      migrate,
      migrate_twice,
      already_there,
      delete_restart,
      which_children,
      large_group,
      childspecs_at_init,
      anonymous_supervisors,
      no_migration_on_shutdown,
      start_idempotence,
      unsupported,
      ignore,
      startup_failure
    ].

init_per_suite(Config) ->
    ok = application:set_env(mnesia, dir, ?config(priv_dir, Config)),
    ok = application:start(mnesia),
    lists:foreach(
      fun ({Tab, TabDef}) ->
              TabDef1 = proplists:delete(match, TabDef),
              case mnesia:create_table(Tab, TabDef1) of
                  {atomic, ok} ->
                      ok;
                  {aborted, Reason} ->
                      throw({error,
                          {table_creation_failed, Tab, TabDef1, Reason}})
              end
      end, mirrored_supervisor:table_definitions()),
    Config.

end_per_suite(Config) ->
    ok = application:stop(mnesia),
    Config.

%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

%% Simplest test
migrate(_Config) ->
    passed = with_sups(
      fun([A, _]) ->
              {ok, _} = ?MS:start_child(a, childspec(worker)),
              Pid1 = pid_of(worker),
              kill_registered(A, Pid1),
              Pid2 = pid_of(worker),
              false = (Pid1 =:= Pid2)
      end, [a, b]).

%% Is migration transitive?
migrate_twice(_Config) ->
    passed = with_sups(
      fun([A, B]) ->
              {ok, _} = ?MS:start_child(a, childspec(worker)),
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
already_there(_Config) ->
    passed = with_sups(
      fun([_, _]) ->
              S = childspec(worker),
              {ok, Pid}                       = ?MS:start_child(a, S),
              {error, {already_started, Pid}} = ?MS:start_child(b, S)
      end, [a, b]).

%% Deleting and restarting should work as per a normal supervisor
delete_restart(_Config) ->
    passed = with_sups(
      fun([_, _]) ->
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

which_children(_Config) ->
    passed = with_sups(
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
    {worker, Pid, worker, [?MODULE]} = Child,
    Pid.

%% Not all the members of the group should actually do the failover
large_group(_Config) ->
    passed = with_sups(
      fun([A, _, _, _]) ->
              {ok, _} = ?MS:start_child(a, childspec(worker)),
              Pid1 = pid_of(worker),
              kill_registered(A, Pid1),
              Pid2 = pid_of(worker),
              false = (Pid1 =:= Pid2)
      end, [a, b, c, d]).

%% Do childspecs work when returned from init?
childspecs_at_init(_Config) ->
    S = childspec(worker),
    passed = with_sups(
      fun([A, _]) ->
              Pid1 = pid_of(worker),
              kill_registered(A, Pid1),
              Pid2 = pid_of(worker),
              false = (Pid1 =:= Pid2)
      end, [{a, [S]}, {b, [S]}]).

anonymous_supervisors(_Config) ->
    passed = with_sups(
      fun([A, _B]) ->
              {ok, _} = ?MS:start_child(A, childspec(worker)),
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
no_migration_on_shutdown(_Config) ->
    passed = with_sups(
      fun([Evil, _]) ->
              {ok, _} = ?MS:start_child(Evil, childspec(worker)),
              try
                  call(worker, ping, 1000, 100),
                  exit(worker_should_not_have_migrated)
                  catch exit:{timeout_waiting_for_server, _, _} ->
                      ok
              end
      end, [evil, good]).

start_idempotence(_Config) ->
    passed = with_sups(
      fun([_]) ->
              CS = childspec(worker),
              {ok, Pid}                       = ?MS:start_child(a, CS),
              {error, {already_started, Pid}} = ?MS:start_child(a, CS),
              ?MS:terminate_child(a, worker),
              {error, already_present}        = ?MS:start_child(a, CS)
      end, [a]).

unsupported(_Config) ->
    try
        ?MS:start_link({global, foo}, get_group(group), fun tx_fun/1, ?MODULE,
                       {one_for_one, []}),
        exit(no_global)
    catch error:badarg ->
        ok
    end,
    try
        {ok, _} = ?MS:start_link({local, foo}, get_group(group),
          fun tx_fun/1, ?MODULE, {simple_one_for_one, []}),
        exit(no_sofo)
    catch error:badarg ->
        ok
    end.

%% Just test we don't blow up
ignore(_Config) ->
    ?MS:start_link({local, foo}, get_group(group), fun tx_fun/1, ?MODULE,
                   {fake_strategy_for_ignore, []}).

startup_failure(_Config) ->
    [test_startup_failure(F) || F <- [want_error, want_exit]].

test_startup_failure(Fail) ->
    process_flag(trap_exit, true),
    ?MS:start_link(get_group(group), fun tx_fun/1, ?MODULE,
                   {one_for_one, [childspec(Fail)]}),
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
    ?MS:start_link(Group, fun tx_fun/1, ?MODULE,
                   {one_for_one, ChildSpecs});

start_sup0(Name, Group, ChildSpecs) ->
    ?MS:start_link({local, Name}, Group, fun tx_fun/1, ?MODULE,
                   {one_for_one, ChildSpecs}).

childspec(Id) ->
    {Id,{?SERVER, start_link, [Id]}, transient, 16#ffffffff, worker, [?MODULE]}.

pid_of(Id) ->
    {received, Pid, ping} = call(Id, ping),
    Pid.

tx_fun(Fun) ->
    case mnesia:sync_transaction(Fun) of
        {atomic,  Result}         -> Result;
        {aborted, Reason}         -> throw({error, Reason})
    end.

inc_group() ->
    Count = case get(counter) of
                undefined -> 0;
                C         -> C
            end + 1,
    put(counter, Count).

get_group(Group) ->
    {Group, get(counter)}.

call(Id, Msg) -> call(Id, Msg, 10*1000, 100).

call(Id, Msg, MaxDelay, Decr) ->
    call(Id, Msg, MaxDelay, Decr, undefined).

call(Id, Msg, 0, _Decr, Stacktrace) ->
    exit({timeout_waiting_for_server, {Id, Msg}, Stacktrace});

call(Id, Msg, MaxDelay, Decr, _) ->
    try
        gen_server:call(Id, Msg, infinity)
    catch exit:_:Stacktrace -> timer:sleep(Decr),
                    call(Id, Msg, MaxDelay - Decr, Decr, Stacktrace)
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

init({fake_strategy_for_ignore, _ChildSpecs}) ->
    ignore;

init({Strategy, ChildSpecs}) ->
    {ok, {{Strategy, 0, 1}, ChildSpecs}}.
