%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_boot_steps).

-export([run_boot_steps/0, run_boot_steps/1, run_cleanup_steps/1]).
-export([find_steps/0, find_steps/1]).

run_boot_steps() ->
    run_boot_steps(loaded_applications()).

run_boot_steps(Apps) ->
    [begin
      _ = rabbit_log:info("Running boot step ~s defined by app ~s", [Step, App]),
      ok = run_step(Attrs, mfa)
    end || {App, Step, Attrs} <- find_steps(Apps)],
    ok.

run_cleanup_steps(Apps) ->
    [run_step(Attrs, cleanup) || {_, _, Attrs} <- find_steps(Apps)],
    ok.

loaded_applications() ->
    [App || {App, _, _} <- application:loaded_applications()].

find_steps() ->
    find_steps(loaded_applications()).

find_steps(Apps) ->
    All = sort_boot_steps(rabbit_misc:all_module_attributes(rabbit_boot_step)),
    [Step || {App, _, _} = Step <- All, lists:member(App, Apps)].

run_step(Attributes, AttributeName) ->
    [begin
        _ = rabbit_log:debug("Applying MFA: M = ~s, F = ~s, A = ~p",
                        [M, F, A]),
        case apply(M,F,A) of
            ok              -> ok;
            {error, Reason} -> exit({error, Reason})
        end
     end
      || {Key, {M,F,A}} <- Attributes,
          Key =:= AttributeName],
    ok.

vertices({AppName, _Module, Steps}) ->
    [{StepName, {AppName, StepName, Atts}} || {StepName, Atts} <- Steps].

edges({_AppName, _Module, Steps}) ->
    EnsureList = fun (L) when is_list(L) -> L;
                     (T)                 -> [T]
                 end,
    [case Key of
         requires -> {StepName, OtherStep};
         enables  -> {OtherStep, StepName}
     end || {StepName, Atts} <- Steps,
            {Key, OtherStepOrSteps} <- Atts,
            OtherStep <- EnsureList(OtherStepOrSteps),
            Key =:= requires orelse Key =:= enables].

sort_boot_steps(UnsortedSteps) ->
    case rabbit_misc:build_acyclic_graph(fun vertices/1, fun edges/1,
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
                     {mfa, {M,F,A}}               <- Attributes,
                     code:ensure_loaded(M) =/= {module, M} orelse
                     not erlang:function_exported(M, F, length(A))] of
                []         -> SortedSteps;
                MissingFns -> exit({boot_functions_not_exported, MissingFns})
            end;
        {error, {vertex, duplicate, StepName}} ->
            exit({duplicate_boot_step, StepName});
        {error, {edge, Reason, From, To}} ->
            exit({invalid_boot_step_dependency, From, To, Reason})
    end.
