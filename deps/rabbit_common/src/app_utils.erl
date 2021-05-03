%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(app_utils).

-export([load_applications/1,
         start_applications/1, start_applications/2, start_applications/3,
         stop_applications/1, stop_applications/2, app_dependency_order/2,
         app_dependencies/1]).

-type error_handler() :: fun((atom(), any()) -> 'ok' | no_return()).
-type restart_type() :: 'permanent' | 'transient' | 'temporary'.

-spec load_applications([atom()])                   -> 'ok'.
-spec start_applications([atom()])                  -> 'ok'.
-spec stop_applications([atom()])                   -> 'ok'.
-spec start_applications([atom()], error_handler()) -> 'ok'.
-spec start_applications([atom()], error_handler(), #{atom() => restart_type()}) -> 'ok'.
-spec stop_applications([atom()], error_handler())  -> 'ok'.
-spec app_dependency_order([atom()], boolean())     -> [digraph:vertex()].
-spec app_dependencies(atom())                      -> [atom()].
-spec failed_to_start_app(atom(), any())            -> no_return().
-spec failed_to_stop_app(atom(), any())             -> no_return().

%%---------------------------------------------------------------------------
%% Public API

load_applications(Apps) ->
    load_applications(queue:from_list(Apps), sets:new()),
    ok.

start_applications(Apps) ->
    start_applications(
      Apps, fun failed_to_start_app/2).

stop_applications(Apps) ->
    stop_applications(
      Apps, fun failed_to_stop_app/2).

failed_to_start_app(App, Reason) ->
    throw({error, {cannot_start_application, App, Reason}}).

failed_to_stop_app(App, Reason) ->
    throw({error, {cannot_stop_application, App, Reason}}).

start_applications(Apps, ErrorHandler) ->
    start_applications(Apps, ErrorHandler, #{}).

start_applications(Apps, ErrorHandler, RestartTypes) ->
    manage_applications(fun lists:foldl/3,
                        fun(App) -> ensure_all_started(App, RestartTypes) end,
                        fun application:stop/1,
                        already_started,
                        ErrorHandler,
                        Apps).

stop_applications(Apps, ErrorHandler) ->
    manage_applications(fun lists:foldr/3,
                        fun(App) ->
                            _ = rabbit_log:info("Stopping application '~s'", [App]),
                            application:stop(App)
                        end,
                        fun(App) -> ensure_all_started(App, #{}) end,
                        not_started,
                        ErrorHandler,
                        Apps).

app_dependency_order(RootApps, StripUnreachable) ->
    {ok, G} = rabbit_misc:build_acyclic_graph(
                fun ({App, _Deps}) -> [{App, App}] end,
                fun ({App,  Deps}) -> [{Dep, App} || Dep <- Deps] end,
                [{App, app_dependencies(App)} ||
                    {App, _Desc, _Vsn} <- application:loaded_applications()]),
    try
        case StripUnreachable of
            true -> digraph:del_vertices(G, digraph:vertices(G) --
                     digraph_utils:reachable(RootApps, G));
            false -> ok
        end,
        digraph_utils:topsort(G)
    after
        true = digraph:delete(G)
    end.

%%---------------------------------------------------------------------------
%% Private API

load_applications(Worklist, Loaded) ->
    case queue:out(Worklist) of
        {empty, _WorkList} ->
            ok;
        {{value, App}, Worklist1} ->
            case sets:is_element(App, Loaded) of
                true  -> load_applications(Worklist1, Loaded);
                false -> case application:load(App) of
                             ok                             -> ok;
                             {error, {already_loaded, App}} -> ok;
                             Error                          -> throw(Error)
                         end,
                         load_applications(
                           queue:join(Worklist1,
                                      queue:from_list(app_dependencies(App))),
                           sets:add_element(App, Loaded))
            end
    end.

app_dependencies(App) ->
    case application:get_key(App, applications) of
        undefined -> [];
        {ok, Lst} -> Lst
    end.

manage_applications(Iterate, Do, Undo, SkipError, ErrorHandler, Apps) ->
    Iterate(fun (App, Acc) ->
                    case Do(App) of
                        ok -> [App | Acc];
                        {ok, []} -> Acc;
                        {ok, [App]} -> [App | Acc];
                        {ok, StartedApps} -> StartedApps ++ Acc;
                        {error, {SkipError, _}} -> Acc;
                        {error, Reason} ->
                            lists:foreach(Undo, Acc),
                            ErrorHandler(App, Reason)
                    end
            end, [], Apps),
    ok.

%% Stops the Erlang VM when the rabbit application stops abnormally
%% i.e. message store reaches its restart limit
default_restart_type(rabbit) -> transient;
default_restart_type(_)      -> temporary.

%% Copyright Ericsson AB 1996-2016. All Rights Reserved.
%%
%% Code originally from Erlang/OTP source lib/kernel/src/application.erl
%% and modified to use RestartTypes map
%%
ensure_all_started(Application, RestartTypes) ->
    case ensure_all_started(Application, RestartTypes, []) of
        {ok, Started} ->
            {ok, lists:reverse(Started)};
        {error, Reason, Started} ->
            _ = [application:stop(App) || App <- Started],
        {error, Reason}
    end.

ensure_all_started(Application, RestartTypes, Started) ->
    RestartType = maps:get(Application, RestartTypes, default_restart_type(Application)),
    case application:start(Application, RestartType) of
        ok ->
            {ok, [Application | Started]};
        {error, {already_started, Application}} ->
            {ok, Started};
        {error, {not_started, Dependency}} ->
            case ensure_all_started(Dependency, RestartTypes, Started) of
                {ok, NewStarted} ->
                    ensure_all_started(Application, RestartTypes, NewStarted);
                Error ->
                    Error
            end;
        {error, Reason} ->
            {error, {Application, Reason}, Started}
    end.
