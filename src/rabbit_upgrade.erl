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

-export([maybe_upgrade_mnesia/0, maybe_upgrade/1]).
-export([read_version/0, write_version/0, desired_version/0]).

-include("rabbit.hrl").

-define(VERSION_FILENAME, "schema_version").
-define(LOCK_FILENAME, "schema_upgrade_lock").

%% -------------------------------------------------------------------

-ifdef(use_specs).

-type(step() :: atom()).
-type(scope() :: 'mnesia' | 'local').
-type(version() :: [step()]).

-spec(maybe_upgrade_mnesia/0 :: () -> 'ok').
-spec(maybe_upgrade/1 :: ([scope()]) -> 'ok' | 'version_not_available').
-spec(read_version/0 :: () -> rabbit_types:ok_or_error2(version(), any())).
-spec(write_version/0 :: () -> 'ok').
-spec(desired_version/0 :: () -> version()).

-endif.

%% -------------------------------------------------------------------

maybe_upgrade_mnesia() ->
    rabbit:prepare(),
    case upgrades_required([mnesia]) of
        Upgrades = [_|_] ->
            DiscNodes = rabbit_mnesia:all_clustered_nodes(),
            Upgrader = upgrader(DiscNodes),
            case node() of
                Upgrader ->
                    primary_upgrade(Upgrades, DiscNodes);
                _ ->
                    non_primary_upgrade(Upgrader, DiscNodes)
            end;
        [] ->
            ok;
        version_not_available ->
            ok
    end.

upgrader(Nodes) ->
    [Upgrader|_] = lists:usort(Nodes),
    Upgrader.

primary_upgrade(Upgrades, DiscNodes) ->
    Others = DiscNodes -- [node()],
    %% TODO this should happen after backing up!
    rabbit_misc:ensure_ok(mnesia:start(),
                          cannot_start_mnesia),
    force_tables(),
    [{atomic, ok} = mnesia:del_table_copy(schema, Node) || Node <- Others],
    apply_upgrades(Upgrades),
    ok.

force_tables() ->
    [mnesia:force_load_table(T) || T <- rabbit_mnesia:table_names()].

non_primary_upgrade(Upgrader, DiscNodes) ->
    case node_running(Upgrader) of
        false ->
            Msg = "~n~n * Cluster upgrade needed. Please start node ~s "
                "first. * ~n~n~n",
            Args = [Upgrader],
            %% We don't throw or exit here since that gets thrown
            %% straight out into do_boot, generating an erl_crash.dump
            %% and displaying any error message in a confusing way.
            error_logger:error_msg(Msg, Args),
            io:format(Msg, Args),
            error_logger:logfile(close),
            halt(1);
        true ->
            rabbit_misc:ensure_ok(mnesia:delete_schema([node()]),
                                  cannot_delete_schema),
            ok = rabbit_mnesia:create_cluster_nodes_config(DiscNodes),
            ok
    end.

node_running(Node) ->
    case rpc:call(Node, application, which_applications, []) of
        {badrpc, _} -> false;
        Apps        -> lists:keysearch(rabbit, 1, Apps) =/= false
    end.

%% -------------------------------------------------------------------

maybe_upgrade(Scopes) ->
    case upgrades_required(Scopes) of
        version_not_available -> version_not_available;
        []                    -> ok;
        Upgrades              -> apply_upgrades(Upgrades)
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
    with_upgrade_graph(fun (G) -> heads(G) end).

%% -------------------------------------------------------------------

upgrades_required(Scopes) ->
    case read_version() of
        {ok, CurrentHeads} ->
            with_upgrade_graph(
              fun (G) ->
                      case unknown_heads(CurrentHeads, G) of
                          []      -> upgrades_to_apply(CurrentHeads, Scopes, G);
                          Unknown -> throw({error,
                                            {future_upgrades_found, Unknown}})
                      end
              end);
        {error, enoent} ->
            version_not_available
    end.

with_upgrade_graph(Fun) ->
    case rabbit_misc:build_acyclic_graph(
           fun vertices/2, fun edges/2,
           rabbit_misc:all_module_attributes(rabbit_upgrade)) of
        {ok, G} -> try
                       Fun(G)
                   after
                       true = digraph:delete(G)
                   end;
        {error, {vertex, duplicate, StepName}} ->
            throw({error, {duplicate_upgrade_step, StepName}});
        {error, {edge, {bad_vertex, StepName}, _From, _To}} ->
            throw({error, {dependency_on_unknown_upgrade_step, StepName}});
        {error, {edge, {bad_edge, StepNames}, _From, _To}} ->
            throw({error, {cycle_in_upgrade_steps, StepNames}})
    end.

vertices(Module, Steps) ->
    [{StepName, {Scope, {Module, StepName}}} ||
        {StepName, Scope, _Reqs} <- Steps].

edges(_Module, Steps) ->
    [{Require, StepName} || {StepName, _Scope, Requires} <- Steps,
                            Require <- Requires].

unknown_heads(Heads, G) ->
    [H || H <- Heads, digraph:vertex(G, H) =:= false].

upgrades_to_apply(Heads, Scopes, G) ->
    %% Take all the vertices which can reach the known heads. That's
    %% everything we've already applied. Subtract that from all
    %% vertices: that's what we have to apply.
    Unsorted = sets:to_list(
                sets:subtract(
                  sets:from_list(digraph:vertices(G)),
                  sets:from_list(digraph_utils:reaching(Heads, G)))),
    %% Form a subgraph from that list and find a topological ordering
    %% so we can invoke them in order.
    Sorted = [element(2, digraph:vertex(G, StepName)) ||
        StepName <- digraph_utils:topsort(digraph_utils:subgraph(G, Unsorted))],
    %% Only return the upgrades for the appropriate scopes
    [Upgrade || {Scope, Upgrade} <- Sorted, lists:member(Scope, Scopes)].

heads(G) ->
    lists:sort([V || V <- digraph:vertices(G), digraph:out_degree(G, V) =:= 0]).

%% -------------------------------------------------------------------

apply_upgrades(Upgrades) ->
    LockFile = lock_filename(dir()),
    case rabbit_misc:lock_file(LockFile) of
        ok ->
            BackupDir = dir() ++ "-upgrade-backup",
            info("Upgrades: ~w to apply~n", [length(Upgrades)]),
            case rabbit_mnesia:copy_db(BackupDir) of
                ok ->
                    %% We need to make the backup after creating the
                    %% lock file so that it protects us from trying to
                    %% overwrite the backup. Unfortunately this means
                    %% the lock file exists in the backup too, which
                    %% is not intuitive. Remove it.
                    ok = file:delete(lock_filename(BackupDir)),
                    info("Upgrades: Mnesia dir backed up to ~p~n", [BackupDir]),
                    [apply_upgrade(Upgrade) || Upgrade <- Upgrades],
                    info("Upgrades: All upgrades applied successfully~n", []),
                    ok = write_version(),
                    ok = rabbit_misc:recursive_delete([BackupDir]),
                    info("Upgrades: Mnesia backup removed~n", []),
                    ok = file:delete(LockFile);
                {error, E} ->
                    %% If we can't backup, the upgrade hasn't started
                    %% hence we don't need the lockfile since the real
                    %% mnesia dir is the good one.
                    ok = file:delete(LockFile),
                    throw({could_not_back_up_mnesia_dir, E})
            end;
        {error, eexist} ->
            throw({error, previous_upgrade_failed})
    end.

apply_upgrade({M, F}) ->
    info("Upgrades: Applying ~w:~w~n", [M, F]),
    ok = apply(M, F, []).

%% -------------------------------------------------------------------

dir() -> rabbit_mnesia:dir().

schema_filename() -> filename:join(dir(), ?VERSION_FILENAME).

lock_filename(Dir) -> filename:join(Dir, ?LOCK_FILENAME).

%% NB: we cannot use rabbit_log here since it may not have been
%% started yet
info(Msg, Args) -> error_logger:info_msg(Msg, Args).
