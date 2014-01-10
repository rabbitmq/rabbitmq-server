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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_boot).

-export([boot_with/1, shutdown/1]).
-export([start/1, stop/1]).
-export([run_boot_steps/0]).
-export([boot_error/2, boot_error/4]).

-ifdef(use_specs).

-spec(boot_with/1      :: (fun(() -> 'ok')) -> 'ok').
-spec(shutdown/1       :: ([atom()]) -> 'ok').
-spec(start/1          :: ([atom()]) -> 'ok').
-spec(stop/1           :: ([atom()]) -> 'ok').
-spec(run_boot_steps/0 :: () -> 'ok').
-spec(boot_error/2     :: (term(), not_available | [tuple()]) -> no_return()).
-spec(boot_error/4     :: (term(), string(), [any()], not_available | [tuple()])
                          -> no_return()).

-endif.

-define(BOOT_FILE, "boot.info").

%%
%% When the broker is starting, we must run all the boot steps within the
%% rabbit:start/2 application callback, after rabbit_sup has started and
%% before any plugin applications begin to start. To achieve this, we process
%% the boot steps from all loaded applications and run them in dependent
%% order.
%%
%% When the broker is already running, we must run all the boot steps for
%% each application/plugin we're starting, plus any other (dependent) steps
%% that have not been run. To achieve this, we process the boot steps from
%% all loaded applications, but skip those that have already been run (i.e.,
%% steps that have been run once whilst or since the broker started).
%%
%% Tracking which boot steps have already been run is done via an ets table.
%% Because we frequently find ourselves needing to query this table without
%% the rabbit application running (e.g., during the initial boot sequence
%% and when we've "cold started" a node without any running applications),
%% this table is serialized to a file after each operation. When the node is
%% stopped cleanly, the file is deleted. When a node is in the process of
%% starting, the file is also removed and replaced (since we do not want to
%% track boot steps from a previous incarnation of the node).
%%
%% Cleanup steps...
%%

%%---------------------------------------------------------------------------
%% Public API

boot_with(StartFun) ->
    %% TODO: this should be done with monitors, not links, I think
    Marker = spawn_link(fun() -> receive stop -> ok end end),
    case catch register(rabbit_boot, Marker) of
        true -> try
                    case rabbit:is_running() of
                        true  -> ok;
                        false -> StartFun()
                    end
                catch
                    throw:{could_not_start, _App, _Reason}=Err ->
                        boot_error(Err, not_available);
                    _:Reason ->
                        boot_error(Reason, erlang:get_stacktrace())
                after
                    unlink(Marker),
                    Marker ! stop,
                    %% give the error loggers some time to catch up
                    timer:sleep(100)
                end;
        _    -> unlink(Marker),
                Marker ! stop
    end.

shutdown(Apps) ->
    try
        case whereis(rabbit_boot) of
            undefined -> ok;
            _         -> await_startup(Apps)
        end,
        %% TODO: put this back in somewhere sensible...
        %% rabbit_log:info("Stopping RabbitMQ~n"),
        ok = app_utils:stop_applications(Apps)
    after
        delete_boot_table()
    end.

start(Apps) ->
    try
        ensure_boot_table(),
        force_reload(Apps),
        StartupApps = app_utils:app_dependency_order(Apps, false),
        case whereis(?MODULE) of
            undefined -> run_boot_steps();
            _         -> ok
        end,
        ok = app_utils:start_applications(StartupApps,
                                          handle_app_error(could_not_start))
    after
        save_boot_table()
    end.

stop(Apps) ->
    ensure_boot_table(),
    TargetApps =
        sets:to_list(
          lists:foldl(
            fun(App, Set) ->
                    lists:foldl(fun sets:add_element/2, Set,
                                app_utils:isolated_dependencies(App) -- [rabbit])
            end, sets:new(), Apps)),
    try
        ok = app_utils:stop_applications(
               TargetApps, handle_app_error(error_during_shutdown))
    after
        try
            BootSteps = load_steps(),
            ToDelete = [Step || {App, _, _}=Step <- BootSteps,
                                lists:member(App, TargetApps)],
            [ets:delete(?MODULE, Step) || {_, Step, _} <- ToDelete],
            run_cleanup_steps(TargetApps)
        after
            save_boot_table()
        end,
        [begin
             {ok, Mods} = application:get_key(App, modules),
             [begin
                  code:soft_purge(Mod),
                  code:delete(Mod),
                  false = code:is_loaded(Mod)
              end || Mod <- Mods],
             application:unload(App)
         end || App <- TargetApps]
    end.

run_cleanup_steps(Apps) ->
    Completed = sets:new(),
    lists:foldl(
      fun({_, Name, _}=Step, Acc) ->
              case sets:is_element(Name, Completed) of
                  true  -> Acc;
                  false -> run_cleanup_step(Step),
                           sets:add_element(Name, Acc)
              end
      end,
      Completed,
      [Step || {App, _, _}=Step <- load_steps(), lists:member(App, Apps)]),
    ok.

run_boot_steps() ->
    Steps = load_steps(),
    [ok = run_boot_step(Step) || Step <- Steps],
    ok.

load_steps() ->
    StepAttrs = rabbit_misc:all_app_module_attributes(rabbit_boot_step),
    sort_boot_steps(
      lists:usort(
        [{Mod, {AppName, Steps}} || {AppName, Mod, Steps} <- StepAttrs])).

boot_error(Term={error, {timeout_waiting_for_tables, _}}, _Stacktrace) ->
    AllNodes = rabbit_mnesia:cluster_nodes(all),
    {Err, Nodes} =
        case AllNodes -- [node()] of
            [] -> {"Timeout contacting cluster nodes. Since RabbitMQ was"
                   " shut down forcefully~nit cannot determine which nodes"
                   " are timing out.~n", []};
            Ns -> {rabbit_misc:format(
                     "Timeout contacting cluster nodes: ~p.~n", [Ns]),
                   Ns}
        end,
    basic_boot_error(Term,
                     Err ++ rabbit_nodes:diagnostics(Nodes) ++ "~n~n", []);
boot_error(Reason, Stacktrace) ->
    Fmt = "Error description:~n   ~p~n~n" ++
        "Log files (may contain more information):~n   ~s~n   ~s~n~n",
    Args = [Reason, log_location(kernel), log_location(sasl)],
    boot_error(Reason, Fmt, Args, Stacktrace).

boot_error(Reason, Fmt, Args, not_available) ->
    basic_boot_error(Reason, Fmt, Args);
boot_error(Reason, Fmt, Args, Stacktrace) ->
    basic_boot_error(Reason, Fmt ++ "Stack trace:~n   ~p~n~n",
                     Args ++ [Stacktrace]).

%%---------------------------------------------------------------------------
%% Private API

force_reload(Apps) ->
    ok = app_utils:load_applications(Apps),
    ok = do_reload(Apps).

do_reload([App|Apps]) ->
    case application:get_key(App, modules) of
        {ok, Modules} -> reload_all(Modules);
        _             -> ok
    end,
    force_reload(Apps);
do_reload([]) ->
    ok.

reload_all(Modules) ->
    [begin
         case code:soft_purge(Mod) of
             true  -> load_mod(Mod);
             false -> io:format("unable to purge ~p~n", [Mod])
         end
     end || Mod <- Modules].

load_mod(Mod) ->
    case code:is_loaded(Mod) of
        {file, preloaded} -> code:load_file(Mod);
        {file, Path}      -> code:load_abs(Path);
        false             -> code:load_file(Mod)
    end.

await_startup(Apps) ->
    app_utils:wait_for_applications(Apps).

delete_boot_table() ->
    case filelib:is_file(boot_file()) of
        true  -> file:delete(boot_file());
        false -> ok
    end.

ensure_boot_table() ->
    case whereis(?MODULE) of
        undefined ->
            case filelib:is_file(boot_file()) of
                true  -> load_table();
                false -> clean_table()
            end;
        _Pid ->
            clean_table()
    end.

clean_table() ->
    delete_boot_table(),
    case ets:info(?MODULE) of
        undefined ->
            ets:new(?MODULE, [named_table, public, ordered_set]);
        _ ->
            ok
    end.

load_table() ->
    case ets:file2tab(boot_file(), [{verify, true}]) of
        {error, _} -> clean_table();
        {ok, _Tab} -> ok
    end.

save_boot_table() ->
    delete_boot_table(),
    case ets:info(?MODULE) of
        undefined -> ok;
        _         -> ets:tab2file(?MODULE, boot_file(),
                                  [{extended_info, [object_count]}]),
                     ets:delete(?MODULE)
    end.

boot_file() ->
    filename:join(rabbit_mnesia:dir(), ?BOOT_FILE).

handle_app_error(Term) ->
    fun(App, {bad_return, {_MFA, {'EXIT', {ExitReason, _}}}}) ->
            throw({Term, App, ExitReason});
       (App, Reason) ->
            throw({Term, App, Reason})
    end.

run_cleanup_step({_, _, Attributes}) ->
    run_step_name(Attributes, cleanup).

run_boot_step({_, StepName, Attributes}) ->
    case catch already_run(StepName) of
        false -> ok = run_step_name(Attributes, mfa),
                 mark_complete(StepName);
        true  -> ok
    end,
    ok.

run_step_name(Attributes, AttributeName) ->
    case [MFA || {Key, MFA} <- Attributes,
                 Key =:= AttributeName] of
        [] ->
            ok;
        MFAs ->
            [try
                 apply(M,F,A)
             of
                 ok              -> ok;
                 {error, Reason} -> boot_error(Reason, not_available)
             catch
                 _:Reason -> boot_error(Reason, erlang:get_stacktrace())
             end || {M,F,A} <- MFAs],
            ok
    end.

already_run(StepName) ->
    ets:member(?MODULE, StepName).

mark_complete(StepName) ->
    ets:insert(?MODULE, {StepName}).

basic_boot_error(Reason, Format, Args) ->
    io:format("~n~nBOOT FAILED~n===========~n~n" ++ Format, Args),
    rabbit_misc:local_info_msg(Format, Args),
    timer:sleep(1000),
    exit({?MODULE, failure_during_boot, Reason}).

%% TODO: move me to rabbit_misc
log_location(Type) ->
    case application:get_env(rabbit, case Type of
                                         kernel -> error_logger;
                                         sasl   -> sasl_error_logger
                                     end) of
        {ok, {file, File}} -> File;
        {ok, false}        -> undefined;
        {ok, tty}          -> tty;
        {ok, silent}       -> undefined;
        {ok, Bad}          -> throw({error, {cannot_log_to_file, Bad}});
        _                  -> undefined
    end.

vertices(_Module, {AppName, Steps}) ->
    [{StepName, {AppName, StepName, Atts}} || {StepName, Atts} <- Steps].

edges(_Module, {_AppName, Steps}) ->
    [case Key of
         requires -> {StepName, OtherStep};
         enables  -> {OtherStep, StepName}
     end || {StepName, Atts} <- Steps,
            {Key, OtherStep} <- Atts,
            Key =:= requires orelse Key =:= enables].

sort_boot_steps(UnsortedSteps) ->
    case rabbit_misc:build_acyclic_graph(fun vertices/2, fun edges/2,
                                         UnsortedSteps) of
        {ok, G} ->
            %% Use topological sort to find a consistent ordering (if
            %% there is one, otherwise fail).
            SortedSteps = lists:reverse(
                            [begin
                                 {StepName, Step} = digraph:vertex(G,
                                                                   StepName),
                                 Step
                             end || StepName <- digraph_utils:topsort(G)]),
            digraph:delete(G),
            %% Check that all mentioned {M,F,A} triples are exported.
            case [{StepName, {M,F,A}} ||
                     {_App, StepName, Attributes} <- SortedSteps,
                     {mfa, {M,F,A}}         <- Attributes,
                     not erlang:function_exported(M, F, length(A))] of
                []               -> SortedSteps;
                MissingFunctions -> basic_boot_error(
                                      {missing_functions, MissingFunctions},
                                      "Boot step functions not exported: ~p~n",
                                      [MissingFunctions])
            end;
        {error, {vertex, duplicate, StepName}} ->
            basic_boot_error({duplicate_boot_step, StepName},
                             "Duplicate boot step name: ~w~n", [StepName]);
        {error, {edge, Reason, From, To}} ->
            basic_boot_error(
              {invalid_boot_step_dependency, From, To},
              "Could not add boot step dependency of ~w on ~w:~n~s",
              [To, From,
               case Reason of
                   {bad_vertex, V} ->
                       io_lib:format("Boot step not registered: ~w~n", [V]);
                   {bad_edge, [First | Rest]} ->
                       [io_lib:format("Cyclic dependency: ~w", [First]),
                        [io_lib:format(" depends on ~w", [Next]) ||
                            Next <- Rest],
                        io_lib:format(" depends on ~w~n", [First])]
               end])
    end.


