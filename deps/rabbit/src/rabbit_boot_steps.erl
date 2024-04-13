%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_boot_steps).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-export([run_boot_steps/0, run_boot_steps/1, run_cleanup_steps/1]).
-export([find_steps/0, find_steps/1]).

run_boot_steps() ->
    run_boot_steps(loaded_applications()).

run_boot_steps(Apps) ->
    [begin
      rabbit_log:info("Running boot step ~ts defined by app ~ts", [Step, App]),
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
    T0 = erlang:monotonic_time(),
    AttrsPerApp = rabbit_misc:rabbitmq_related_module_attributes(rabbit_boot_step),
    T1 = erlang:monotonic_time(),
    ?LOG_DEBUG(
      "Boot steps: time to find boot steps: ~tp us",
      [erlang:convert_time_unit(T1 - T0, native, microsecond)],
      #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    All = sort_boot_steps(AttrsPerApp),
    [Step || {App, _, _} = Step <- All, lists:member(App, Apps)].

run_step(Attributes, AttributeName) ->
    [begin
        rabbit_log:debug("Applying MFA: M = ~ts, F = ~ts, A = ~tp",
                        [M, F, A]),
        case apply(M,F,A) of
            ok              ->
                rabbit_log:debug("Finished MFA: M = ~ts, F = ~ts, A = ~tp",
                                 [M, F, A]);
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
