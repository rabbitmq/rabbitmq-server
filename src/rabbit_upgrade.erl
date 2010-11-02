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

-export([maybe_upgrade/1, write_version/1]).

-include("rabbit.hrl").

-define(VERSION_FILENAME, "schema_version").
-define(LOCK_FILENAME, "schema_upgrade_lock").

%% -------------------------------------------------------------------

-ifdef(use_specs).

-spec(maybe_upgrade/1 :: (file:filename()) -> 'ok').
-spec(write_version/1 :: (file:filename()) -> 'ok').

-endif.

%% -------------------------------------------------------------------

%% Try to upgrade the schema. If no information on the existing schema could
%% be found, do nothing. rabbit_mnesia:check_schema_integrity() will catch the
%% problem.
maybe_upgrade(Dir) ->
    case rabbit_misc:read_term_file(schema_filename(Dir)) of
        {ok, [CurrentHeads]} ->
            G = load_graph(),
            case unknown_heads(CurrentHeads, G) of
                [] ->
                    case upgrades_to_apply(CurrentHeads, G) of
                        []       -> ok;
                        Upgrades -> apply_upgrades(Upgrades, Dir)
                    end;
                Unknown ->
                    [warn("Data store has had future upgrade ~w applied." ++
                              " Will not upgrade.~n", [U]) || U <- Unknown]
            end,
            true = digraph:delete(G),
            ok;
        {error, enoent} ->
            ok
    end.

write_version(Dir) ->
    G = load_graph(),
    ok = rabbit_misc:write_term_file(schema_filename(Dir), [heads(G)]),
    true = digraph:delete(G),
    ok.

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
    [element(2, digraph:vertex(G, StepName))
     || StepName <- digraph_utils:topsort(digraph_utils:subgraph(G, Unsorted))].

heads(G) ->
    [V || V <- digraph:vertices(G), digraph:out_degree(G, V) =:= 0].

%% -------------------------------------------------------------------

apply_upgrades(Upgrades, Dir) ->
    LockFile = lock_filename(Dir),
    case file:read_file_info(LockFile) of
        {error, enoent} ->
            info("Upgrades: ~w to apply~n", [length(Upgrades)]),
            {ok, Lock} = file:open(LockFile, write),
            ok = file:close(Lock),
            [apply_upgrade(Upgrade) || Upgrade <- Upgrades],
            info("Upgrades: All applied~n", []),
            ok = write_version(Dir),
            ok = file:delete(LockFile);
        {ok, _FI} ->
            exit(previous_upgrade_failed);
        {error, _} = Error ->
            exit(Error)
    end.

apply_upgrade({M, F}) ->
    info("Upgrades: Applying ~w:~w~n", [M, F]),
    ok = apply(M, F, []).

%% -------------------------------------------------------------------

schema_filename(Dir) ->
    filename:join(Dir, ?VERSION_FILENAME).

lock_filename(Dir) ->
    filename:join(Dir, ?LOCK_FILENAME).

%% NB: we cannot use rabbit_log here since it may not have been started yet
info(Msg, Args) ->
    error_logger:info_msg(Msg, Args).

warn(Msg, Args) ->
    error_logger:warning_msg(Msg, Args).
