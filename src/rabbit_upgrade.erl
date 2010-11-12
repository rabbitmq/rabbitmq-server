%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_upgrade).

-export([maybe_upgrade/0, read_version/0, write_version/0, desired_version/0]).

-include("rabbit.hrl").

-define(VERSION_FILENAME, "schema_version").
-define(LOCK_FILENAME, "schema_upgrade_lock").

%% -------------------------------------------------------------------

-ifdef(use_specs).

-spec(maybe_upgrade/0 :: () -> 'ok' | 'version_not_available').
-spec(read_version/0 ::
        () -> {'ok', [any()]} | rabbit_types:error(any())).
-spec(write_version/0 :: () -> 'ok').
-spec(desired_version/0 :: () -> [atom()]).

-endif.

%% -------------------------------------------------------------------

%% Try to upgrade the schema. If no information on the existing schema
%% could be found, do nothing. rabbit_mnesia:check_schema_integrity()
%% will catch the problem.
maybe_upgrade() ->
    case read_version() of
        {ok, CurrentHeads} ->
            G = load_graph(),
            case unknown_heads(CurrentHeads, G) of
                [] ->
                    case upgrades_to_apply(CurrentHeads, G) of
                        []       -> ok;
                        Upgrades -> apply_upgrades(Upgrades)
                    end;
                Unknown ->
                    exit({future_upgrades_found, Unknown})
            end,
            true = digraph:delete(G),
            ok;
        {error, enoent} ->
            version_not_available
    end.

read_version() ->
    case rabbit_misc:read_term_file(schema_filename()) of
        {ok, [Heads]}    -> {ok, Heads};
        {error, _} = Err -> Err
    end.

write_version() ->
    ok = rabbit_misc:write_term_file(schema_filename(), [desired_version()]),
    ok.

desired_version() ->
    G = load_graph(),
    Version = heads(G),
    true = digraph:delete(G),
    Version.

%% -------------------------------------------------------------------

load_graph() ->
    Upgrades = rabbit_misc:all_module_attributes(rabbit_upgrade),
    rabbit_misc:build_acyclic_graph(
      fun vertices/2, fun edges/2, fun graph_build_error/1, Upgrades).

vertices(Module, Steps) ->
    [{StepName, {Module, StepName}} || {StepName, _Reqs} <- Steps].

edges(_Module, Steps) ->
    [{Require, StepName} || {StepName, Requires} <- Steps, Require <- Requires].

graph_build_error({vertex, duplicate, StepName}) ->
    exit({duplicate_upgrade, StepName});
graph_build_error({edge, E, From, To}) ->
    exit({E, From, To}).

unknown_heads(Heads, G) ->
    [H || H <- Heads, digraph:vertex(G, H) =:= false].

upgrades_to_apply(Heads, G) ->
    %% Take all the vertices which can reach the known heads. That's
    %% everything we've already applied. Subtract that from all
    %% vertices: that's what we have to apply.
    Unsorted = sets:to_list(
                sets:subtract(
                  sets:from_list(digraph:vertices(G)),
                  sets:from_list(digraph_utils:reaching(Heads, G)))),
    %% Form a subgraph from that list and find a topological ordering
    %% so we can invoke them in order.
    [element(2, digraph:vertex(G, StepName)) ||
        StepName <- digraph_utils:topsort(digraph_utils:subgraph(G, Unsorted))].

heads(G) ->
    lists:sort([V || V <- digraph:vertices(G), digraph:out_degree(G, V) =:= 0]).

%% -------------------------------------------------------------------

apply_upgrades(Upgrades) ->
    LockFile = lock_filename(),
    case file:open(LockFile, [write, exclusive]) of
        {ok, Lock} ->
            ok = file:close(Lock),
            info("Upgrades: ~w to apply~n", [length(Upgrades)]),
            [apply_upgrade(Upgrade) || Upgrade <- Upgrades],
            info("Upgrades: All applied~n", []),
            ok = write_version(),
            ok = file:delete(LockFile);
        {error, eexist} ->
            exit(previous_upgrade_failed);
        {error, _} = Error ->
            exit(Error)
    end.

apply_upgrade({M, F}) ->
    info("Upgrades: Applying ~w:~w~n", [M, F]),
    ok = apply(M, F, []).

%% -------------------------------------------------------------------

dir() -> rabbit_mnesia:dir().

schema_filename() -> filename:join(dir(), ?VERSION_FILENAME).

lock_filename()   -> filename:join(dir(), ?LOCK_FILENAME).

%% NB: we cannot use rabbit_log here since it may not have been
%% started yet
info(Msg, Args) -> error_logger:info_msg(Msg, Args).
