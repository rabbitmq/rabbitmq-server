%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(mirrored_supervisor_SUITE).

-behaviour(mirrored_supervisor).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(MS,     mirrored_supervisor).
-define(SERVER, mirrored_supervisor_SUITE_gs).

all() ->
    [
     {group, broker_tests}
    ].

groups() ->
    [
     {broker_tests, [], [
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
                        ]}
    ].


init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{sup_prefix, Testcase}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).
%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

%% Simplest test
migrate(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, migrate1, [?config(sup_prefix, Config)]).

migrate1(Sup) ->
    with_sups(
      fun([A, _]) ->
              {ok, _} = ?MS:start_child(sup(Sup, 1), childspec(worker)),
              Pid1 = pid_of(worker),
              kill_registered(A, Pid1),
              Pid2 = pid_of(worker),
              false = (Pid1 =:= Pid2)
      end, [sup(Sup, 1), sup(Sup, 2)], Sup).

%% Is migration transitive?
migrate_twice(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, migrate_twice1, [?config(sup_prefix, Config)]).

migrate_twice1(Sup) ->
    with_sups(
      fun([A, B]) ->
              {ok, _} = ?MS:start_child(sup(Sup, 1), childspec(worker)),
              Pid1 = pid_of(worker),
              kill_registered(A, Pid1),
              {ok, C} = start_sup(sup(Sup, 3), Sup),
              Pid2 = pid_of(worker),
              kill_registered(B, Pid2),
              Pid3 = pid_of(worker),
              false = (Pid1 =:= Pid3),
              kill(C)
      end, [sup(Sup, 1), sup(Sup, 2)], Sup).

%% Can't start the same child twice
already_there(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, already_there1, [?config(sup_prefix, Config)]).

already_there1(Sup) ->
    with_sups(
      fun([_, _]) ->
              S = childspec(worker),
              {ok, Pid}                       = ?MS:start_child(sup(Sup, 1), S),
              {error, {already_started, Pid}} = ?MS:start_child(sup(Sup, 2), S)
      end, [sup(Sup, 1), sup(Sup, 2)], Sup).

%% Deleting and restarting should work as per a normal supervisor
delete_restart(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, delete_restart1, [?config(sup_prefix, Config)]).

delete_restart1(Sup) ->
    Sup1 = sup(Sup, 1),
    Sup2 = sup(Sup, 2),
    with_sups(
      fun([_, _]) ->
              S = childspec(worker),
              {ok, Pid1} = ?MS:start_child(Sup1, S),
              {error, running} = ?MS:delete_child(Sup1, id(worker)),
              ok = ?MS:terminate_child(Sup1, id(worker)),
              ok = ?MS:delete_child(Sup1, id(worker)),
              {ok, Pid2} = ?MS:start_child(Sup2, S),
              false = (Pid1 =:= Pid2),
              ok = ?MS:terminate_child(Sup2, id(worker)),
              {ok, Pid3} = ?MS:restart_child(Sup2, id(worker)),
              Pid3 = pid_of(worker),
              false = (Pid2 =:= Pid3),
              %% Not the same supervisor as the worker is on
              ok = ?MS:terminate_child(Sup1, id(worker)),
              ok = ?MS:delete_child(Sup1, id(worker)),
              {ok, Pid4} = ?MS:start_child(Sup1, S),
              false = (Pid3 =:= Pid4)
      end, [Sup1, Sup2], Sup).

which_children(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, which_children1, [?config(sup_prefix, Config)]).

which_children1(Sup) ->
    Sup1 = sup(Sup, 1),
    Sup2 = sup(Sup, 2),
    with_sups(
      fun([A, B] = Both) ->
              ?MS:start_child(A, childspec(worker)),
              assert_wc(Both, fun ([C]) -> true = is_pid(wc_pid(C)) end),
              ok = ?MS:terminate_child(Sup1, id(worker)),
              assert_wc(Both, fun ([C]) -> undefined = wc_pid(C) end),
              {ok, _} = ?MS:restart_child(Sup1, id(worker)),
              assert_wc(Both, fun ([C]) -> true = is_pid(wc_pid(C)) end),
              ?MS:start_child(B, childspec(worker2)),
              assert_wc(Both, fun (C) -> 2 = length(C) end)
      end, [Sup1, Sup2], Sup).

assert_wc(Sups, Fun) ->
    [Fun(?MS:which_children(Sup)) || Sup <- Sups].

wc_pid(Child) ->
    {_, Pid, worker, [?MODULE]} = Child,
    Pid.

%% Not all the members of the group should actually do the failover
large_group(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, large_group1, [?config(sup_prefix, Config)]).

large_group1(Sup) ->
    with_sups(
      fun([A, _, _, _]) ->
              {ok, _} = ?MS:start_child(sup(Sup, 1), childspec(worker)),
              Pid1 = pid_of(worker),
              kill_registered(A, Pid1),
              Pid2 = pid_of(worker),
              false = (Pid1 =:= Pid2)
      end, [sup(Sup, 1), sup(Sup, 2), sup(Sup, 3), sup(Sup, 4)], Sup).

%% Do childspecs work when returned from init?
childspecs_at_init(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, childspecs_at_init1, [?config(sup_prefix, Config)]).

childspecs_at_init1(Sup) ->
    S = childspec(worker),
    with_sups(
      fun([A, _]) ->
              Pid1 = pid_of(worker),
              kill_registered(A, Pid1),
              Pid2 = pid_of(worker),
              false = (Pid1 =:= Pid2)
      end, [{sup(Sup, 1), [S]}, {sup(Sup, 2), [S]}], Sup).

anonymous_supervisors(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, anonymous_supervisors1, [?config(sup_prefix, Config)]).

anonymous_supervisors1(Sup) ->
    with_sups(
      fun([A, _B]) ->
              {ok, _} = ?MS:start_child(A, childspec(worker)),
              Pid1 = pid_of(worker),
              kill_registered(A, Pid1),
              Pid2 = pid_of(worker),
              false = (Pid1 =:= Pid2)
      end, [sup(Sup, 1), sup(Sup, 2)], Sup).

%% When a mirrored_supervisor terminates, we should not migrate, but
%% the whole supervisor group should shut down. To test this we set up
%% a situation where the gen_server will only fail if it's running
%% under the supervisor called 'evil'. It should not migrate to
%% 'good' and survive, rather the whole group should go away.
no_migration_on_shutdown(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, no_migration_on_shutdown1, [?config(sup_prefix, Config)]).

no_migration_on_shutdown1(Sup) ->
    with_sups(
      fun([Evil, _]) ->
              {ok, _} = ?MS:start_child(Evil, childspec(worker3)),
              try
                  call(worker3, ping, 1000, 100),
                  exit(worker_should_not_have_migrated)
                  catch exit:{timeout_waiting_for_server, _, _} ->
                      ok
              end
      end, [evil, good], Sup).

start_idempotence(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, start_idempotence1, [?config(sup_prefix, Config)]).

start_idempotence1(Sup) ->
    Sup1 = sup(Sup, 1),
    with_sups(
      fun([_]) ->
              CS = childspec(worker2),
              {ok, Pid}                       = ?MS:start_child(Sup1, CS),
              {error, {already_started, Pid}} = ?MS:start_child(Sup1, CS),
              ?MS:terminate_child(Sup1, id(worker2)),
              {error, already_present}        = ?MS:start_child(Sup1, CS)
      end, [Sup1], Sup).

unsupported(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, unsupported1, [?config(sup_prefix, Config)]).

unsupported1(Sup) ->
    try
        ?MS:start_link({global, foo}, Sup, ?MODULE, {one_for_one, []}),
        exit(no_global)
    catch error:badarg ->
        ok
    end,
    try
        {ok, _} = ?MS:start_link({local, foo}, Sup, ?MODULE, {simple_one_for_one, []}),
        exit(no_sofo)
    catch error:badarg ->
        passed
    end.

%% Just test we don't blow up
ignore(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, ignore1, [?config(sup_prefix, Config)]).

ignore1(Sup) ->
    ?MS:start_link({local, foo}, Sup, ?MODULE, {fake_strategy_for_ignore, []}),
    passed.

startup_failure(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, startup_failure1, [?config(sup_prefix, Config)]).

startup_failure1(Sup) ->
    [test_startup_failure(F, Sup) || F <- [want_error, want_exit]],
    passed.

test_startup_failure(Fail, Group) ->
    process_flag(trap_exit, true),
    ?MS:start_link(Group, ?MODULE, {one_for_one, [childspec(Fail)]}),
    receive
        {'EXIT', _, shutdown} ->
            ok
    after 1000 ->
            exit({did_not_exit, Fail})
    end,
    process_flag(trap_exit, false),
    ok.

%% ---------------------------------------------------------------------------

with_sups(Fun, Sups, Group) ->
    Pids = [begin {ok, Pid} = start_sup(Sup, Group), Pid end || Sup <- Sups],
    Fun(Pids),
    [kill(Pid) || Pid <- Pids, is_process_alive(Pid)],
    timer:sleep(500),
    passed.

start_sup({Name, ChildSpecs}, Group) ->
    {ok, Pid} = start_sup0(Name, Group, ChildSpecs),
    %% We are not a supervisor, when we kill the supervisor we do not
    %% want to die!
    unlink(Pid),
    {ok, Pid};

start_sup(Name, Group) ->
    start_sup({Name, []}, Group).

start_sup0(anon, Group, ChildSpecs) ->
    ?MS:start_link(Group, ?MODULE,
                   {one_for_one, ChildSpecs});

start_sup0(Name, Group, ChildSpecs) ->
    ?MS:start_link({local, Name}, Group, ?MODULE,
                   {one_for_one, ChildSpecs}).

childspec(Id) ->
    {id(Id), {?SERVER, start_link, [Id]}, transient, 16#ffffffff, worker, [?MODULE]}.

id(Id) ->
    {[Id], Id}.

pid_of(Id) ->
    {received, Pid, ping} = call(Id, ping),
    Pid.

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

sup(Prefix, Number) ->
    rabbit_data_coercion:to_atom(lists:flatten(io_lib:format("~p~p", [Prefix, Number]))).
