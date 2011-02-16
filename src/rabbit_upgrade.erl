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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_upgrade).

-export([maybe_upgrade_mnesia/0, maybe_upgrade/1]).
-export([read_version/0, write_version/0, desired_version/0,
         desired_version/1]).

-include("rabbit.hrl").

-define(VERSION_FILENAME, "schema_version").
-define(LOCK_FILENAME, "schema_upgrade_lock").
-define(SCOPES, [mnesia, local]).

%% -------------------------------------------------------------------

-ifdef(use_specs).

-type(step() :: atom()).
-type(version() :: [{scope(), [step()]}]).
-type(scope() :: 'mnesia' | 'local').

-spec(maybe_upgrade_mnesia/0 :: () -> 'ok').
-spec(maybe_upgrade/1 :: (scope()) -> 'ok' | 'version_not_available').
-spec(read_version/0 :: () -> rabbit_types:ok_or_error2(version(), any())).
-spec(write_version/0 :: () -> 'ok').
-spec(desired_version/0 :: () -> version()).
-spec(desired_version/1 :: (scope()) -> [step()]).

-endif.

%% -------------------------------------------------------------------

maybe_upgrade_mnesia() ->
    Nodes = rabbit_mnesia:all_clustered_nodes(),
    case upgrades_required(mnesia) of
        version_not_available ->
            case Nodes of
                [_] -> ok;
                _   -> die("Cluster upgrade needed but upgrading from "
                           "< 2.1.1.~n   Unfortunately you will need to "
                           "rebuild the cluster.", [])
            end;
        [] ->
            ok;
        Upgrades ->
            case upgrade_mode(Nodes) of
                primary   -> primary_upgrade(Upgrades, Nodes);
                secondary -> non_primary_upgrade(Nodes)
            end
    end.

upgrade_mode(Nodes) ->
    case nodes_running(Nodes) of
        [] ->
            case am_i_disc_node() of
                true  -> primary;
                false -> die("Cluster upgrade needed but this is a ram "
                             "node.~n   Please start any of the disc nodes "
                             "first.", [])
            end;
        [Another|_] ->
            ClusterVersion =
                case rpc:call(Another,
                              rabbit_upgrade, desired_version, [mnesia]) of
                    {badrpc, {'EXIT', {undef, _}}} -> unknown_old_version;
                    {badrpc, Reason}               -> {unknown, Reason};
                    V                              -> V
                end,
            case desired_version(mnesia) of
                ClusterVersion ->
                    %% The other node(s) have upgraded already, I am not the
                    %% upgrader
                    secondary;
                MyVersion ->
                    %% The other node(s) are running an unexpected version.
                    die("Cluster upgrade needed but other nodes are "
                        "running ~p~nand I want ~p",
                        [ClusterVersion, MyVersion])
            end
    end.

am_i_disc_node() ->
    %% The cluster config does not list all disc nodes, but it will list us
    %% if we're one.
    case rabbit_mnesia:read_cluster_nodes_config() of
        []        -> true;
        DiscNodes -> lists:member(node(), DiscNodes)
    end.

die(Msg, Args) ->
    %% We don't throw or exit here since that gets thrown
    %% straight out into do_boot, generating an erl_crash.dump
    %% and displaying any error message in a confusing way.
    error_logger:error_msg(Msg, Args),
    io:format("~n~n** " ++ Msg ++ " **~n~n~n", Args),
    error_logger:logfile(close),
    halt(1).

primary_upgrade(Upgrades, Nodes) ->
    Others = Nodes -- [node()],
    apply_upgrades(
      mnesia,
      Upgrades,
      fun () ->
              rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
              force_tables(),
              case Others of
                  [] -> ok;
                  _  -> info("mnesia upgrades: Breaking cluster~n", []),
                        [{atomic, ok} = mnesia:del_table_copy(schema, Node)
                         || Node <- Others]
              end
      end),
    ok.

force_tables() ->
    [mnesia:force_load_table(T) || T <- rabbit_mnesia:table_names()].

non_primary_upgrade(Nodes) ->
    rabbit_misc:ensure_ok(mnesia:delete_schema([node()]),
                          cannot_delete_schema),
    ok = rabbit_mnesia:create_cluster_nodes_config(Nodes),
    write_version(mnesia),
    ok.

nodes_running(Nodes) ->
    [N || N <- Nodes, node_running(N)].

node_running(Node) ->
    case rpc:call(Node, application, which_applications, []) of
        {badrpc, _} -> false;
        Apps        -> lists:keysearch(rabbit, 1, Apps) =/= false
    end.

%% -------------------------------------------------------------------

maybe_upgrade(Scope) ->
    case upgrades_required(Scope) of
        version_not_available -> version_not_available;
        []                    -> ok;
        Upgrades              -> apply_upgrades(Scope, Upgrades,
                                                fun() -> ok end)
    end.

read_version() ->
    case rabbit_misc:read_term_file(schema_filename()) of
        {ok, [V]}        -> case is_new_version(V) of
                                false -> {ok, convert_old_version(V)};
                                true  -> [{rabbit, RV}] = V,
                                         {ok, RV}
                            end;
        {error, _} = Err -> Err
    end.

read_version(Scope) ->
    case read_version() of
        {error, _} = E -> E;
        {ok, V}        -> {ok, orddict:fetch(Scope, V)}
    end.

write_version() ->
    ok = rabbit_misc:write_term_file(schema_filename(),
                                     [[{rabbit, desired_version()}]]),
    ok.

write_version(Scope) ->
    {ok, V0} = read_version(),
    V = orddict:store(Scope, desired_version(Scope), V0),
    ok = rabbit_misc:write_term_file(schema_filename(), [[{rabbit, V}]]),
    ok.

desired_version() ->
    lists:foldl(
      fun (Scope, Acc) ->
              orddict:store(Scope, desired_version(Scope), Acc)
      end,
      orddict:new(), ?SCOPES).

desired_version(Scope) ->
    with_upgrade_graph(fun (G) -> heads(G) end, Scope).

convert_old_version(Heads) ->
    Locals = [add_queue_ttl],
    V0 = orddict:new(),
    V1 = orddict:store(mnesia, Heads -- Locals, V0),
    orddict:store(local,
                  lists:filter(fun(H) -> lists:member(H, Locals) end, Heads),
                  V1).

%% -------------------------------------------------------------------

upgrades_required(Scope) ->
    case read_version(Scope) of
        {ok, CurrentHeads} ->
            with_upgrade_graph(
              fun (G) ->
                      case unknown_heads(CurrentHeads, G) of
                          []      -> upgrades_to_apply(CurrentHeads, G);
                          Unknown -> throw({error,
                                            {future_upgrades_found, Unknown}})
                      end
              end, Scope);
        {error, enoent} ->
            version_not_available
    end.

with_upgrade_graph(Fun, Scope) ->
    case rabbit_misc:build_acyclic_graph(
           fun (Module, Steps) -> vertices(Module, Steps, Scope) end,
           fun (Module, Steps) -> edges(Module, Steps, Scope) end,
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

vertices(Module, Steps, Scope0) ->
    [{StepName, {Module, StepName}} || {StepName, Scope1, _Reqs} <- Steps,
                                       Scope0 == Scope1].

edges(_Module, Steps, Scope0) ->
    [{Require, StepName} || {StepName, Scope1, Requires} <- Steps,
                            Require <- Requires,
                            Scope0 == Scope1].

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

apply_upgrades(Scope, Upgrades, Fun) ->
    LockFile = lock_filename(dir()),
    case rabbit_misc:lock_file(LockFile) of
        ok ->
            BackupDir = dir() ++ "-upgrade-backup",
            info("~s upgrades: ~w to apply~n", [Scope, length(Upgrades)]),
            case rabbit_mnesia:copy_db(BackupDir) of
                ok ->
                    %% We need to make the backup after creating the
                    %% lock file so that it protects us from trying to
                    %% overwrite the backup. Unfortunately this means
                    %% the lock file exists in the backup too, which
                    %% is not intuitive. Remove it.
                    ok = file:delete(lock_filename(BackupDir)),
                    info("~s upgrades: Mnesia dir backed up to ~p~n",
                         [Scope, BackupDir]),
                    Fun(),
                    [apply_upgrade(Scope, Upgrade) || Upgrade <- Upgrades],
                    info("~s upgrades: All upgrades applied successfully~n",
                         [Scope]),
                    ok = write_version(Scope),
                    ok = rabbit_misc:recursive_delete([BackupDir]),
                    info("~s upgrades: Mnesia backup removed~n", [Scope]),
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

apply_upgrade(Scope, {M, F}) ->
    info("~s upgrades: Applying ~w:~w~n", [Scope, M, F]),
    ok = apply(M, F, []).

%% -------------------------------------------------------------------

dir() -> rabbit_mnesia:dir().

schema_filename() -> filename:join(dir(), ?VERSION_FILENAME).

lock_filename(Dir) -> filename:join(Dir, ?LOCK_FILENAME).

%% NB: we cannot use rabbit_log here since it may not have been
%% started yet
info(Msg, Args) -> error_logger:info_msg(Msg, Args).

is_new_version(Version) ->
    is_list(Version) andalso
        length(Version) > 0 andalso
        lists:all(fun(Item) -> is_tuple(Item) andalso size(Item) == 2 end,
                  Version).
