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

-export([prepare_boot_table/0]).
-export([stop/1]).
-export([force_reload/1]).
-export([already_run/1, mark_complete/1]).

-ifdef(use_specs).

-spec(prepare_boot_table/0 :: () -> 'ok').
-spec(already_run/1        :: (atom()) -> boolean()).
-spec(mark_complete/1      :: (atom()) -> 'ok').
-spec(stop/1               :: ([atom()]) -> 'ok').
-spec(force_reload/1       :: ([atom()]) -> 'ok').

-endif.

%% When the broker is starting, we must run all the boot steps within the
%% rabbit:start/2 application callback, after rabbit_sup has started and
%% before any plugin applications begin to start. To achieve this, we process
%% the boot steps from all loaded applications.
%%
%% If the broker is already running however, we must run all boot steps for
%% each application/plugin we're starting, plus any other (dependent) steps.
%% To achieve this, we process boot steps as usual, but skip those that have
%% already run (i.e., whilst, or even since the broker started).
%%
%% Tracking which boot steps have run is done via a shared ets table, owned
%% by the "rabbit" process.

%%---------------------------------------------------------------------------
%% Public API

prepare_boot_table() ->
    ets:new(?MODULE, [named_table, public, ordered_set]).

stop(Apps) ->
    try
        ok = app_utils:stop_applications(
               Apps, rabbit:handle_app_error(error_during_shutdown))
    after
        BootSteps = rabbit:load_steps(boot),
        ToDelete = [Step || {App, _, _}=Step <- BootSteps,
                            lists:member(App, Apps)],
        [ets:delete(?MODULE, Step) || {_, Step, _} <- ToDelete],
        run_cleanup_steps(Apps),
        [begin
             {ok, Mods} = application:get_key(App, modules),
             [begin
                  code:soft_purge(Mod),
                  code:delete(Mod),
                  false = code:is_loaded(Mod)
              end || Mod <- Mods],
             application:unload(App)
         end || App <- Apps]
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
      [Step || {App, _, _}=Step <- rabbit:load_steps(cleanup),
               lists:member(App, Apps)]),
    ok.

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
             false -> ok
         end
     end || Mod <- Modules].

load_mod(Mod) ->
    case code:is_loaded(Mod) of
        {file, Path} when Path /= 'preloaded' -> code:load_abs(Path);
        _                                     -> code:load_file(Mod)
    end.

run_cleanup_step({_, StepName, Attributes}) ->
    rabbit:run_step(StepName, Attributes, cleanup).

already_run(StepName) ->
    ets:member(?MODULE, StepName).

mark_complete(StepName) ->
    ets:insert(?MODULE, {StepName}).

