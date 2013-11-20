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
-module(app_utils).

-export([load_applications/1, start_applications/1, start_applications/2,
         stop_applications/1, stop_applications/2, app_dependency_order/2,
         running_applications/0, wait_for_applications/1, app_dependencies/1]).
-export([direct_dependencies/1]).

-ifdef(use_specs).

-type error_handler() :: fun((atom(), any()) -> 'ok').

-spec load_applications([atom()])                   -> 'ok'.
-spec start_applications([atom()])                  -> 'ok'.
-spec stop_applications([atom()])                   -> 'ok'.
-spec start_applications([atom()], error_handler()) -> 'ok'.
-spec stop_applications([atom()], error_handler())  -> 'ok'.
-spec wait_for_applications([atom()])               -> 'ok'.
-spec app_dependency_order([atom()], boolean())     -> [digraph:vertex()].
-spec app_dependencies(atom())                      -> [atom()].
-spec direct_dependencies(atom())                   -> [atom()].

-endif.

%%---------------------------------------------------------------------------
%% Public API

running_applications() ->
    [App || {App, _, _} <- application:which_applications()].

load_applications(Apps) ->
    load_applications(queue:from_list(Apps), sets:new()),
    ok.

start_applications(Apps) ->
    start_applications(
      Apps, fun (App, Reason) ->
                    throw({error, {cannot_start_application, App, Reason}})
            end).

stop_applications(Apps) ->
    stop_applications(
      Apps, fun (App, Reason) ->
                    throw({error, {cannot_stop_application, App, Reason}})
            end).

start_applications(Apps, ErrorHandler) ->
    manage_applications(fun lists:foldl/3,
                        fun start/1,
                        fun application:stop/1,
                        already_started,
                        ErrorHandler,
                        Apps).

stop_applications(Apps, ErrorHandler) ->
    manage_applications(fun lists:foldr/3,
                        fun application:stop/1,
                        fun application:start/1,
                        not_started,
                        ErrorHandler,
                        Apps).

wait_for_applications(Apps) ->
    [wait_for_application(App) || App <- Apps], ok.

direct_dependencies(Root) ->
    G = digraph:new(),
    Loaded = application:loaded_applications(),
    try
        [begin
             case digraph:vertex(G, App) of
                 false -> digraph:add_vertex(G, App, App);
                 _     -> ok = throw({graph_error, {vertex, duplicate, App}})
             end
         end || {App, _, _} <- Loaded],
        [digraph:add_edge(G, App, Dep) ||
                            {App, Deps} <- [{App, app_dependencies(App)} ||
                                                {App, _Desc, _Vsn} <- Loaded],
                            Dep <- Deps],
        Deps = lists:foldl(
                 fun(E, Acc) ->
                     {_, _InVertex, OutVertex, _Label} = digraph:edge(G, E),
                     case is_reachable(G, OutVertex, Root) of
                         [] -> sets:add_element(OutVertex, Acc);
                         _  -> Acc
                     end
                 end,
                 sets:from_list([Root]),
                 digraph:out_edges(G, Root)),
        sets:to_list(sets:del_element(rabbit, Deps))
    catch {graph_error, Reason} ->
        {error, Reason}
    after
        true = digraph:delete(G)
    end.

is_reachable(G, OutVertex, Root) ->
    digraph_utils:reaching_neighbours([OutVertex], G) -- [Root].

app_dependency_order(RootApps, StripUnreachable) ->
    {ok, G} = rabbit_misc:build_acyclic_graph(
                fun (App, _Deps) -> [{App, App}] end,
                fun (App,  Deps) -> [{Dep, App} || Dep <- Deps] end,
                [{App, app_dependencies(App)} ||
                    {App, _Desc, _Vsn} <- application:loaded_applications()]),
    try
        case StripUnreachable of
            true  -> digraph:del_vertices(
                       G, digraph:vertices(G) --
                           digraph_utils:reachable(RootApps, G));
            false -> ok
        end,
        digraph_utils:topsort(G)
    after
        true = digraph:delete(G)
    end.

%%---------------------------------------------------------------------------
%% Private API

start(rabbit) ->
    case application:start(rabbit) of
        ok  -> rabbit_boot:run_boot_steps(rabbit), ok;
        Err -> Err
    end;
start(App) ->
    rabbit_boot:run_boot_steps(App),
    application:start(App).

wait_for_application(Application) ->
    case lists:keymember(Application, 1, rabbit_misc:which_applications()) of
         true  -> ok;
         false -> timer:sleep(1000),
                  wait_for_application(Application)
    end.

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
                        {error, {SkipError, _}} -> Acc;
                        {error, Reason} ->
                            lists:foreach(Undo, Acc),
                            ErrorHandler(App, Reason)
                    end
            end, [], Apps),
    ok.

