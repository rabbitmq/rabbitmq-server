%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mnesia_rename).
-include_lib("rabbit_common/include/rabbit.hrl").

-export([rename/2]).
-export([maybe_finish/0, maybe_finish/1]).

-define(CONVERT_TABLES, [schema, rabbit_durable_queue]).

%% Supports renaming the nodes in the Mnesia database. In order to do
%% this, we take a backup of the database, traverse the backup
%% changing node names and pids as we go, then restore it.
%%
%% That's enough for a standalone node, for clusters the story is more
%% complex. We can take pairs of nodes From and To, but backing up and
%% restoring the database changes schema cookies, so if we just do
%% this on all nodes the cluster will refuse to re-form with
%% "Incompatible schema cookies.". Therefore we do something similar
%% to what we do for upgrades - the first node in the cluster to
%% restart becomes the authority, and other nodes wipe their own
%% Mnesia state and rejoin. They also need to tell Mnesia the old node
%% is not coming back.
%%
%% If we are renaming nodes one at a time then the running cluster
%% might not be aware that a rename has taken place, so after we wipe
%% and rejoin we then update any tables (in practice just
%% rabbit_durable_queue) which should be aware that we have changed.

%%----------------------------------------------------------------------------

-spec rename(node(), [{node(), node()}]) -> 'ok'.

rename(Node, NodeMapList) ->
    try
        %% Check everything is correct and figure out what we are
        %% changing from and to.
        {FromNode, ToNode, NodeMap} = prepare(Node, NodeMapList),

        %% We backup and restore Mnesia even if other nodes are
        %% running at the time, and defer the final decision about
        %% whether to use our mutated copy or rejoin the cluster until
        %% we restart. That means we might be mutating our copy of the
        %% database while the cluster is running. *Do not* contact the
        %% cluster while this is happening, we are likely to get
        %% confused.
        application:set_env(kernel, dist_auto_connect, never),

        %% Take a copy we can restore from if we abandon the
        %% rename. We don't restore from the "backup" since restoring
        %% that changes schema cookies and might stop us rejoining the
        %% cluster.
        ok = rabbit_mnesia:copy_db(mnesia_copy_dir()),

        %% And make the actual changes
        become(FromNode),
        take_backup(before_backup_name()),
        _ = convert_backup(NodeMap, before_backup_name(), after_backup_name()),
        ok = rabbit_file:write_term_file(rename_config_name(),
                                         [{FromNode, ToNode}]),
        _ = convert_config_files(NodeMap),
        become(ToNode),
        restore_backup(after_backup_name()),
        ok
    after
        stop_mnesia()
    end.

prepare(Node, NodeMapList) ->
    %% If we have a previous rename and haven't started since, give up.
    case rabbit_file:is_dir(dir()) of
        true  -> exit({rename_in_progress,
                       "Restart node under old name to roll back"});
        false -> ok = rabbit_file:ensure_dir(mnesia_copy_dir())
    end,

    %% Check we don't have two nodes mapped to the same node
    {FromNodes, ToNodes} = lists:unzip(NodeMapList),
    case length(FromNodes) - length(lists:usort(ToNodes)) of
        0 -> ok;
        _ -> exit({duplicate_node, ToNodes})
    end,

    %% Figure out which node we are before and after the change
    FromNode = case [From || {From, To} <- NodeMapList,
                             To =:= Node] of
                   [N] -> N;
                   []  -> Node
               end,
    NodeMap = dict:from_list(NodeMapList),
    ToNode = case dict:find(FromNode, NodeMap) of
                 {ok, N2} -> N2;
                 error    -> FromNode
             end,

    %% Check that we are in the cluster, all old nodes are in the
    %% cluster, and no new nodes are.
    Nodes = rabbit_nodes:all(),
    case {FromNodes -- Nodes, ToNodes -- (ToNodes -- Nodes),
          lists:member(Node, Nodes ++ ToNodes)} of
        {[], [], true}  -> ok;
        {[], [], false} -> exit({i_am_not_involved,        Node});
        {F,  [], _}     -> exit({nodes_not_in_cluster,     F});
        {_,  T,  _}     -> exit({nodes_already_in_cluster, T})
    end,
    {FromNode, ToNode, NodeMap}.

take_backup(Backup) ->
    start_mnesia(),
    %% We backup only local tables: in particular, this excludes the
    %% connection tracking tables which have no local replica.
    LocalTables = mnesia:system_info(local_tables),
    {ok, Name, _Nodes} = mnesia:activate_checkpoint([
        {max, LocalTables}
      ]),
    ok = mnesia:backup_checkpoint(Name, Backup),
    stop_mnesia().

restore_backup(Backup) ->
    ok = mnesia:install_fallback(Backup, [{scope, local}]),
    start_mnesia(),
    stop_mnesia(),
    rabbit_mnesia:force_load_next_boot().

-spec maybe_finish() -> ok.

maybe_finish() ->
    AllNodes = rabbit_nodes:all(),
    maybe_finish(AllNodes).

-spec maybe_finish([node()]) -> 'ok'.

maybe_finish(AllNodes) ->
    case rabbit_file:read_term_file(rename_config_name()) of
        {ok, [{FromNode, ToNode}]} -> finish(FromNode, ToNode, AllNodes);
        _                          -> ok
    end.

finish(FromNode, ToNode, AllNodes) ->
    case node() of
        ToNode ->
            case rabbit_nodes:filter_nodes_running_rabbitmq(AllNodes) of
                [] -> finish_primary(FromNode, ToNode);
                _  -> finish_secondary(FromNode, ToNode, AllNodes)
            end;
        FromNode ->
            rabbit_log:info(
              "Abandoning rename from ~ts to ~ts since we are still ~ts",
              [FromNode, ToNode, FromNode]),
            _ = [{ok, _} = file:copy(backup_of_conf(F), F) || F <- config_files()],
            ok = rabbit_file:recursive_delete([rabbit_mnesia:dir()]),
            ok = rabbit_file:recursive_copy(
                   mnesia_copy_dir(), rabbit_mnesia:dir()),
            delete_rename_files();
        _ ->
            %% Boot will almost certainly fail but we might as
            %% well just log this
            rabbit_log:info(
              "Rename attempted from ~ts to ~ts but we are ~ts - ignoring.",
              [FromNode, ToNode, node()])
    end.

finish_primary(FromNode, ToNode) ->
    rabbit_log:info("Restarting as primary after rename from ~ts to ~ts",
                    [FromNode, ToNode]),
    delete_rename_files(),
    ok.

finish_secondary(FromNode, ToNode, AllNodes) ->
    rabbit_log:info("Restarting as secondary after rename from ~ts to ~ts",
                    [FromNode, ToNode]),
    %% Renaming a node was partially handled by `rabbit_upgrade', the old
    %% upgrade mechanism used before we introduced feature flags. The
    %% following lines until `init_db_unchecked()' included were part of
    %% `rabbit_upgrade:secondary_upgrade(AllNodes)'.
    NodeType = node_type_legacy(),
    rabbit_misc:ensure_ok(mnesia:delete_schema([node()]),
                          cannot_delete_schema),
    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    ok = rabbit_mnesia:init_db_unchecked(AllNodes, NodeType),
    rename_in_running_mnesia(FromNode, ToNode),
    delete_rename_files(),
    ok.

node_type_legacy() ->
    %% This is pretty ugly but we can't start Mnesia and ask it (will
    %% hang), we can't look at the config file (may not include us
    %% even if we're a disc node).  We also can't use
    %% rabbit_mnesia:node_type/0 because that will give false
    %% positives on Rabbit up to 2.5.1.
    case filelib:is_regular(filename:join(rabbit_mnesia:dir(), "rabbit_durable_exchange.DCD")) of
        true  -> disc;
        false -> ram
    end.

dir()                -> rabbit_mnesia:dir() ++ "-rename".
before_backup_name() -> dir() ++ "/backup-before".
after_backup_name()  -> dir() ++ "/backup-after".
rename_config_name() -> dir() ++ "/pending.config".
mnesia_copy_dir()    -> dir() ++ "/mnesia-copy".

delete_rename_files() -> ok = rabbit_file:recursive_delete([dir()]).

start_mnesia() -> rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
                  rabbit_table:force_load(),
                  rabbit_table:wait_for_replicated(_Retry = false).
stop_mnesia()  -> stopped = mnesia:stop().

convert_backup(NodeMap, FromBackup, ToBackup) ->
    mnesia:traverse_backup(
      FromBackup, ToBackup,
      fun
          (Row, Acc) ->
              case lists:member(element(1, Row), ?CONVERT_TABLES) of
                  true  -> {[update_term(NodeMap, Row)], Acc};
                  false -> {[Row], Acc}
              end
      end, switched).

config_files() ->
    [rabbit_node_monitor:running_nodes_filename(),
     rabbit_node_monitor:cluster_status_filename()].

backup_of_conf(Path) ->
    filename:join([dir(), filename:basename(Path)]).

convert_config_files(NodeMap) ->
    [convert_config_file(NodeMap, Path) || Path <- config_files()].

convert_config_file(NodeMap, Path) ->
    {ok, Term} = rabbit_file:read_term_file(Path),
    {ok, _} = file:copy(Path, backup_of_conf(Path)),
    ok = rabbit_file:write_term_file(Path, update_term(NodeMap, Term)).

lookup_node(OldNode, NodeMap) ->
    case dict:find(OldNode, NodeMap) of
        {ok, NewNode} -> NewNode;
        error         -> OldNode
    end.

mini_map(FromNode, ToNode) -> dict:from_list([{FromNode, ToNode}]).

update_term(NodeMap, L) when is_list(L) ->
    [update_term(NodeMap, I) || I <- L];
update_term(NodeMap, T) when is_tuple(T) ->
    list_to_tuple(update_term(NodeMap, tuple_to_list(T)));
update_term(NodeMap, Node) when is_atom(Node) ->
    lookup_node(Node, NodeMap);
update_term(NodeMap, Pid) when is_pid(Pid) ->
    rabbit_misc:pid_change_node(Pid, lookup_node(node(Pid), NodeMap));
update_term(_NodeMap, Term) ->
    Term.

rename_in_running_mnesia(FromNode, ToNode) ->
    All = rabbit_nodes:all(),
    Running = rabbit_nodes:all_running(),
    case {lists:member(FromNode, Running), lists:member(ToNode, All)} of
        {false, true}  -> ok;
        {true,  _}     -> exit({old_node_running,        FromNode});
        {_,     false} -> exit({new_node_not_in_cluster, ToNode})
    end,
    {atomic, ok} = mnesia:del_table_copy(schema, FromNode),
    Map = mini_map(FromNode, ToNode),
    {atomic, _} = transform_table(rabbit_durable_queue, Map),
    ok.

transform_table(Table, Map) ->
    mnesia:sync_transaction(
      fun () ->
              _ = mnesia:lock({table, Table}, write),
              transform_table(Table, Map, mnesia:first(Table))
      end).

transform_table(_Table, _Map, '$end_of_table') ->
    ok;
transform_table(Table, Map, Key) ->
    [Term] = mnesia:read(Table, Key, write),
    ok = mnesia:write(Table, update_term(Map, Term), write),
    transform_table(Table, Map, mnesia:next(Table, Key)).

become(BecomeNode) ->
    error_logger:tty(false),
    case net_adm:ping(BecomeNode) of
        pong -> exit({node_running, BecomeNode});
        pang -> ok = net_kernel:stop(),
                io:format("  * Impersonating node: ~ts...", [BecomeNode]),
                {ok, _} = start_distribution(BecomeNode),
                io:format(" done~n", []),
                Dir = mnesia:system_info(directory),
                io:format("  * Mnesia directory  : ~ts~n", [Dir])
    end.

start_distribution(Name) ->
    rabbit_nodes:ensure_epmd(),
    NameType = rabbit_nodes_common:name_type(Name),
    net_kernel:start([Name, NameType]).
