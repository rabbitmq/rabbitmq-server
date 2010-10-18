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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_upgrade).

-export([maybe_upgrade/1, write_version/1]).
-export([mnesia_alter_column/3]).

-include("rabbit.hrl").

-define(VERSION_FILENAME, "schema_version").
-define(LOCK_FILENAME, "schema_upgrade_lock").

%% API ---------------------------------------------------------------

%% Try to upgrade the schema. If no information on the existing schema could
%% be found, do nothing. rabbit_mnesia:check_schema_integrity() will catch the
%% problem.
maybe_upgrade(Dir) ->
    case rabbit_misc:read_term_file(schema_filename(Dir)) of
        {ok, [CurrentHeads]} ->
            G = load_graph(),
            case check_unknown_heads(CurrentHeads, G) of
                ok ->
                    Upgrades = upgrades_to_apply(CurrentHeads, G),
                    case length(Upgrades) of
                        0 -> ok;
                        _ -> apply_upgrades(Upgrades, Dir)
                    end;
                _ ->
                    ok
            end,
            digraph:delete(G),
            ok;
        {error, enoent} ->
            ok
    end.

write_version(Dir) ->
    G = load_graph(),
    rabbit_misc:write_term_file(schema_filename(Dir), [heads(G)]),
    digraph:delete(G),
    ok.

%% -------------------------------------------------------------------

mnesia_alter_column(Table, Column, Fun) ->
    {atomic, _} =
        mnesia:transaction(
          fun() ->
                  [mnesia_alter_column_for_key(Table, Column, Key, Fun)
                   || Key <- mnesia:all_keys(Table)]
          end),
    ok.

mnesia_alter_column_for_key(Table, Column, Key, Fun) ->
    Vals = mnesia:read(Table, Key, write),
    ok = mnesia:delete(Table, Key, write),
    [mnesia_alter_column_for_value(Table, Column, Val, Fun) || Val <- Vals].

mnesia_alter_column_for_value(Table, Column, Val, Fun) ->
    Val2 = setelement(Column + 1, Val, Fun(element(Column + 1, Val))),
    mnesia:write(Table, Val2, write).

%% Graphs ------------------------------------------------------------

load_graph() ->
    G = digraph:new([acyclic]),
    Upgrades = rabbit:all_module_attributes(rabbit_upgrade),
    [case digraph:vertex(G, StepName) of
         false -> digraph:add_vertex(G, StepName, {Module, StepName});
         _     -> throw({duplicate_upgrade, StepName})
     end || {Module, StepName, _Requires} <- Upgrades],

    lists:foreach(fun ({_Module, Upgrade, Requires}) ->
                          add_edges(G, Requires, Upgrade)
                  end, Upgrades),
    G.

add_edges(G, Requires, Upgrade) ->
    Results = [digraph:add_edge(G, R, Upgrade) || R <- Requires],
    case [E || {error, _} = E <- Results] of
        [] -> ok;
        L  -> exit(L)
    end.

check_unknown_heads(Heads, G) ->
    case [H || H <- Heads, digraph:vertex(G, H) =:= false] of
        [] ->
            ok;
        Unknown ->
            [warn("Data store has had future upgrade ~w applied. Will not " ++
                      "upgrade.~n", [U]) || U <- Unknown],
            Unknown
    end.

upgrades_to_apply(Heads, G) ->
    Unsorted = sets:to_list(
                sets:subtract(
                  sets:from_list(digraph:vertices(G)),
                  sets:from_list(digraph_utils:reaching(Heads, G)))),
    [begin
         {StepName, Step} = digraph:vertex(G, StepName),
         Step
     end || StepName <- digraph_utils:topsort(
                          digraph_utils:subgraph(G, Unsorted))].

heads(G) ->
    [V || V <- digraph:vertices(G), digraph:out_degree(G, V) =:= 0].

%% Upgrades-----------------------------------------------------------

apply_upgrades(Upgrades, Dir) ->
    LockFile = lock_filename(Dir),
    case file:read_file_info(LockFile) of
        {error, enoent} ->
            info("Upgrades: ~w to apply~n", [length(Upgrades)]),
            {ok, Lock} = file:open(LockFile, write),
            ok = file:close(Lock),
            [apply_upgrade(Upgrade) || Upgrade <- Upgrades],
            info("Upgrades: All applied~n", []),
            write_version(Dir),
            ok = file:delete(LockFile);
        {ok, _FI} ->
            exit(previous_upgrade_failed);
        {error, _} = Error ->
            exit(Error)
    end.

apply_upgrade({M, F}) ->
    info("Upgrades: Applying ~w:~w~n", [M, F]),
    apply(M, F, []).

%% Utils -------------------------------------------------------------

schema_filename(Dir) ->
    filename:join(Dir, ?VERSION_FILENAME).

lock_filename(Dir) ->
    filename:join(Dir, ?LOCK_FILENAME).

%% NB: we cannot use rabbit_log here since it may not have been started yet
info(Msg, Args) ->
    error_logger:info_msg(Msg, Args).

warn(Msg, Args) ->
    error_logger:warning_msg(Msg, Args).
