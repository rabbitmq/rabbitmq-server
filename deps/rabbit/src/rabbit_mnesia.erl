%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mnesia).

-export([%% Main interface
         init/0,
         join_cluster/2,
         reset/0,
         force_reset/0,
         update_cluster_nodes/1,
         change_cluster_node_type/1,
         forget_cluster_node/2,
         force_load_next_boot/0,

         %% Various queries to get the status of the db
         status/0,
         is_running/0,
         is_clustered/0,
         on_running_node/1,
         is_process_alive/1,
         is_registered_process_alive/1,
         cluster_nodes/1,
         node_type/0,
         is_virgin_node/0,
         dir/0,
         cluster_status_from_mnesia/0,

         %% Operations on the db and utils, mainly used in `rabbit_mnesia_rename' and `rabbit'
         init_db_unchecked/2,
         copy_db/1,
         check_cluster_consistency/0,
         ensure_mnesia_dir/0,

         %% Hooks used in `rabbit_node_monitor'
         on_node_up/1,
         on_node_down/1,

         %% Helpers for diagnostics commands
         schema_info/1
        ]).

%% Mnesia queries
-export([
         table_filter/3,
         dirty_read_all/1,
         dirty_read/1,
         execute_mnesia_tx_with_tail/1,
         execute_mnesia_transaction/1,
         execute_mnesia_transaction/2
        ]).

%% Used internally in rpc calls
-export([node_info/0, remove_node_if_mnesia_running/1]).

-deprecated({on_running_node, 1,
             "Use rabbit_process:on_running_node/1 instead"}).
-deprecated({is_process_alive, 1,
             "Use rabbit_process:is_process_alive/1 instead"}).
-deprecated({is_registered_process_alive, 1,
             "Use rabbit_process:is_registered_process_alive/1 instead"}).

-ifdef(TEST).
-compile(export_all).
-export([init_with_lock/3]).
-endif.

%%----------------------------------------------------------------------------

-export_type([cluster_status/0]).

-type cluster_status() :: {[node()], [node()], [node()]}.

%%----------------------------------------------------------------------------
%% Main interface
%%----------------------------------------------------------------------------

-spec init() -> 'ok'.

init() ->
    ensure_mnesia_running(),
    ensure_mnesia_dir(),
    case is_virgin_node() of
        true  ->
            rabbit_log:info("Node database directory at ~ts is empty. "
                            "Assuming we need to join an existing cluster or initialise from scratch...",
                            [dir()]),
            rabbit_peer_discovery:log_configured_backend(),
            rabbit_peer_discovery:maybe_init(),
            init_with_lock();
        false ->
            NodeType = node_type(),
            init_db_and_upgrade(cluster_nodes(all), NodeType,
                                NodeType =:= ram, _Retry = true),
            rabbit_peer_discovery:maybe_init(),
            rabbit_peer_discovery:maybe_register()
    end,
    %% We intuitively expect the global name server to be synced when
    %% Mnesia is up. In fact that's not guaranteed to be the case -
    %% let's make it so.
    ok = rabbit_node_monitor:global_sync(),
    ok.

init_with_lock() ->
    {Retries, Timeout} = rabbit_peer_discovery:locking_retry_timeout(),
    init_with_lock(Retries, Timeout, fun run_peer_discovery/0).

init_with_lock(0, _, RunPeerDiscovery) ->
    case rabbit_peer_discovery:lock_acquisition_failure_mode() of
        ignore ->
            rabbit_log:warning("Could not acquire a peer discovery lock, out of retries", []),
            RunPeerDiscovery(),
            rabbit_peer_discovery:maybe_register();
        fail ->
            exit(cannot_acquire_startup_lock)
    end;
init_with_lock(Retries, Timeout, RunPeerDiscovery) ->
    LockResult = rabbit_peer_discovery:lock(),
    rabbit_log:debug("rabbit_peer_discovery:lock returned ~tp", [LockResult]),
    case LockResult of
        not_supported ->
            RunPeerDiscovery(),
            rabbit_peer_discovery:maybe_register();
        {ok, Data} ->
            try
                RunPeerDiscovery(),
                rabbit_peer_discovery:maybe_register()
            after
                rabbit_peer_discovery:unlock(Data)
            end;
        {error, _Reason} ->
            timer:sleep(Timeout),
            init_with_lock(Retries - 1, Timeout, RunPeerDiscovery)
    end.

-spec run_peer_discovery() -> ok | {[node()], rabbit_db_cluster:node_type()}.
run_peer_discovery() ->
    {RetriesLeft, DelayInterval} = rabbit_peer_discovery:discovery_retries(),
    run_peer_discovery_with_retries(RetriesLeft, DelayInterval).

-spec run_peer_discovery_with_retries(non_neg_integer(), non_neg_integer()) -> ok | {[node()], rabbit_db_cluster:node_type()}.
run_peer_discovery_with_retries(0, _DelayInterval) ->
    ok;
run_peer_discovery_with_retries(RetriesLeft, DelayInterval) ->
    FindBadNodeNames = fun
        (Name, BadNames) when is_atom(Name) -> BadNames;
        (Name, BadNames)                    -> [Name | BadNames]
    end,
    {DiscoveredNodes0, NodeType} =
        case rabbit_peer_discovery:discover_cluster_nodes() of
            {error, Reason} ->
                RetriesLeft1 = RetriesLeft - 1,
                rabbit_log:error("Peer discovery returned an error: ~tp. Will retry after a delay of ~b ms, ~b retries left...",
                                [Reason, DelayInterval, RetriesLeft1]),
                timer:sleep(DelayInterval),
                run_peer_discovery_with_retries(RetriesLeft1, DelayInterval);
            {ok, {Nodes, Type} = Config}
              when is_list(Nodes) andalso (Type == disc orelse Type == disk orelse Type == ram) ->
                case lists:foldr(FindBadNodeNames, [], Nodes) of
                    []       -> Config;
                    BadNames -> e({invalid_cluster_node_names, BadNames})
                end;
            {ok, {_, BadType}} when BadType /= disc andalso BadType /= ram ->
                e({invalid_cluster_node_type, BadType});
            {ok, _} ->
                e(invalid_cluster_nodes_conf)
        end,
    DiscoveredNodes = lists:usort(DiscoveredNodes0),
    rabbit_log:info("All discovered existing cluster peers: ~ts",
                    [rabbit_peer_discovery:format_discovered_nodes(DiscoveredNodes)]),
    Peers = nodes_excl_me(DiscoveredNodes),
    case Peers of
        [] ->
            rabbit_log:info("Discovered no peer nodes to cluster with. "
                            "Some discovery backends can filter nodes out based on a readiness criteria. "
                            "Enabling debug logging might help troubleshoot."),
            init_db_and_upgrade([node()], disc, false, _Retry = true);
        _  ->
            rabbit_log:info("Peer nodes we can cluster with: ~ts",
                [rabbit_peer_discovery:format_discovered_nodes(Peers)]),
            join_discovered_peers(Peers, NodeType)
    end.

%% Attempts to join discovered,
%% reachable and compatible (in terms of Mnesia internal protocol version and such)
%% cluster peers in order.
join_discovered_peers(TryNodes, NodeType) ->
    {RetriesLeft, DelayInterval} = rabbit_peer_discovery:discovery_retries(),
    join_discovered_peers_with_retries(TryNodes, NodeType, RetriesLeft, DelayInterval).

join_discovered_peers_with_retries(TryNodes, _NodeType, 0, _DelayInterval) ->
    rabbit_log:info(
              "Could not successfully contact any node of: ~ts (as in Erlang distribution). "
               "Starting as a blank standalone node...",
                [string:join(lists:map(fun atom_to_list/1, TryNodes), ",")]),
            init_db_and_upgrade([node()], disc, false, _Retry = true);
join_discovered_peers_with_retries(TryNodes, NodeType, RetriesLeft, DelayInterval) ->
    case find_reachable_peer_to_cluster_with(nodes_excl_me(TryNodes)) of
        {ok, Node} ->
            rabbit_log:info("Node '~ts' selected for auto-clustering", [Node]),
            {ok, {_, DiscNodes, _}} = discover_cluster0(Node),
            init_db_and_upgrade(DiscNodes, NodeType, true, _Retry = true),
            rabbit_node_monitor:notify_joined_cluster();
        none ->
            RetriesLeft1 = RetriesLeft - 1,
            rabbit_log:info("Trying to join discovered peers failed. Will retry after a delay of ~b ms, ~b retries left...",
                            [DelayInterval, RetriesLeft1]),
            timer:sleep(DelayInterval),
            join_discovered_peers_with_retries(TryNodes, NodeType, RetriesLeft1, DelayInterval)
    end.

%% Make the node join a cluster. The node will be reset automatically
%% before we actually cluster it. The nodes provided will be used to
%% find out about the nodes in the cluster.
%%
%% This function will fail if:
%%
%%   * The node is currently the only disc node of its cluster
%%   * We can't connect to any of the nodes provided
%%   * The node is currently already clustered with the cluster of the nodes
%%     provided
%%
%% Note that we make no attempt to verify that the nodes provided are
%% all in the same cluster, we simply pick the first online node and
%% we cluster to its cluster.

-spec join_cluster(node(), rabbit_db_cluster:node_type())
                        -> ok | {ok, already_member} | {error, {inconsistent_cluster, string()}}.

join_cluster(DiscoveryNode, NodeType) ->
    ensure_mnesia_not_running(),
    ensure_mnesia_dir(),
    case is_only_clustered_disc_node() of
        true  -> e(clustering_only_disc_node);
        false -> ok
    end,
    {ClusterNodes, _, _} = discover_cluster([DiscoveryNode]),
    case me_in_nodes(ClusterNodes) of
        false ->
            case check_cluster_consistency(DiscoveryNode, false) of
                {ok, _} ->
                    %% reset the node. this simplifies things and it
                    %% will be needed in this case - we're joining a new
                    %% cluster with new nodes which are not in synch
                    %% with the current node. It also lifts the burden
                    %% of resetting the node from the user.
                    reset_gracefully(),

                    %% Join the cluster
                    rabbit_log:info("Clustering with ~tp as ~tp node",
                                    [ClusterNodes, NodeType]),
                    ok = init_db_with_mnesia(ClusterNodes, NodeType,
                                             true, true, _Retry = true),
                    rabbit_node_monitor:notify_joined_cluster(),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        true ->
            %% DiscoveryNode thinks that we are part of a cluster, but
            %% do we think so ourselves?
            case are_we_clustered_with(DiscoveryNode) of
                true ->
                    rabbit_log:info("Asked to join a cluster but already a member of it: ~tp", [ClusterNodes]),
                    {ok, already_member};
                false ->
                    Msg = format_inconsistent_cluster_message(DiscoveryNode, node()),
                    rabbit_log:error(Msg),
                    {error, {inconsistent_cluster, Msg}}
            end
    end.

%% return node to its virgin state, where it is not member of any
%% cluster, has no cluster configuration, no local database, and no
%% persisted messages

-spec reset() -> 'ok'.

reset() ->
    ensure_mnesia_not_running(),
    rabbit_log:info("Resetting Rabbit", []),
    reset_gracefully().

-spec force_reset() -> 'ok'.

force_reset() ->
    ensure_mnesia_not_running(),
    rabbit_log:info("Resetting Rabbit forcefully", []),
    wipe().

reset_gracefully() ->
    AllNodes = cluster_nodes(all),
    %% Reconnecting so that we will get an up to date nodes.  We don't
    %% need to check for consistency because we are resetting.
    %% Force=true here so that reset still works when clustered with a
    %% node which is down.
    init_db_with_mnesia(AllNodes, node_type(), false, false, _Retry = false),
    case is_only_clustered_disc_node() of
        true  -> e(resetting_only_disc_node);
        false -> ok
    end,
    leave_cluster(),
    rabbit_misc:ensure_ok(mnesia:delete_schema([node()]), cannot_delete_schema),
    wipe().

wipe() ->
    %% We need to make sure that we don't end up in a distributed
    %% Erlang system with nodes while not being in an Mnesia cluster
    %% with them. We don't handle that well.
    [erlang:disconnect_node(N) || N <- cluster_nodes(all)],
    %% remove persisted messages and any other garbage we find
    ok = rabbit_file:recursive_delete(filelib:wildcard(dir() ++ "/*")),
    ok = rabbit_node_monitor:reset_cluster_status(),
    ok.

-spec change_cluster_node_type(rabbit_db_cluster:node_type()) -> 'ok'.

change_cluster_node_type(Type) ->
    ensure_mnesia_not_running(),
    ensure_mnesia_dir(),
    case is_clustered() of
        false -> e(not_clustered);
        true  -> ok
    end,
    {_, _, RunningNodes} = discover_cluster(cluster_nodes(all)),
    %% We might still be marked as running by a remote node since the
    %% information of us going down might not have propagated yet.
    Node = case RunningNodes -- [node()] of
               []        -> e(no_online_cluster_nodes);
               [Node0|_] -> Node0
           end,
    ok = reset(),
    ok = join_cluster(Node, Type).

-spec update_cluster_nodes(node()) -> 'ok'.

update_cluster_nodes(DiscoveryNode) ->
    ensure_mnesia_not_running(),
    ensure_mnesia_dir(),
    Status = {AllNodes, _, _} = discover_cluster([DiscoveryNode]),
    case me_in_nodes(AllNodes) of
        true ->
            %% As in `check_consistency/0', we can safely delete the
            %% schema here, since it'll be replicated from the other
            %% nodes
            _ = mnesia:delete_schema([node()]),
            rabbit_node_monitor:write_cluster_status(Status),
            rabbit_log:info("Updating cluster nodes from ~tp",
                            [DiscoveryNode]),
            init_db_with_mnesia(AllNodes, node_type(), true, true, _Retry = false);
        false ->
            e(inconsistent_cluster)
    end,
    ok.

%% We proceed like this: try to remove the node locally. If the node
%% is offline, we remove the node if:
%%   * This node is a disc node
%%   * All other nodes are offline
%%   * This node was, at the best of our knowledge (see comment below)
%%     the last or second to last after the node we're removing to go
%%     down

-spec forget_cluster_node(node(), boolean()) -> 'ok'.

forget_cluster_node(Node, RemoveWhenOffline) ->
    forget_cluster_node(Node, RemoveWhenOffline, true).

forget_cluster_node(Node, RemoveWhenOffline, EmitNodeDeletedEvent) ->
    case lists:member(Node, cluster_nodes(all)) of
        true  -> ok;
        false -> e(not_a_cluster_node)
    end,
    case {RemoveWhenOffline, is_running()} of
        {true,  false} -> remove_node_offline_node(Node);
        {true,   true} -> e(online_node_offline_flag);
        {false, false} -> e(offline_node_no_offline_flag);
        {false,  true} -> rabbit_log:info(
                            "Removing node ~tp from cluster", [Node]),
                          case remove_node_if_mnesia_running(Node) of
                              ok when EmitNodeDeletedEvent ->
                                  rabbit_event:notify(node_deleted, [{node, Node}]),
                                  ok;
                              ok               -> ok;
                              {error, _} = Err -> throw(Err)
                          end
    end.

remove_node_offline_node(Node) ->
    %% Here `mnesia:system_info(running_db_nodes)' will RPC, but that's what we
    %% want - we need to know the running nodes *now*.  If the current node is a
    %% RAM node it will return bogus results, but we don't care since we only do
    %% this operation from disc nodes.
    case {mnesia:system_info(running_db_nodes) -- [Node], node_type()} of
        {[], disc} ->
            start_mnesia(),
            try
                %% What we want to do here is replace the last node to
                %% go down with the current node.  The way we do this
                %% is by force loading the table, and making sure that
                %% they are loaded.
                rabbit_table:force_load(),
                rabbit_table:wait_for_replicated(_Retry = false),
                %% We skip the 'node_deleted' event because the
                %% application is stopped and thus, rabbit_event is not
                %% enabled.
                forget_cluster_node(Node, false, false),
                force_load_next_boot()
            after
                stop_mnesia()
            end;
        {_, _} ->
            e(removing_node_from_offline_node)
    end.

%%----------------------------------------------------------------------------
%% Queries
%%----------------------------------------------------------------------------

-spec status() -> [{'nodes', [{rabbit_db_cluster:node_type(), [node()]}]} |
                         {'running_nodes', [node()]} |
                         {'partitions', [{node(), [node()]}]}].

status() ->
    IfNonEmpty = fun (_,       []) -> [];
                     (Type, Nodes) -> [{Type, Nodes}]
                 end,
    [{nodes, (IfNonEmpty(disc, cluster_nodes(disc)) ++
                  IfNonEmpty(ram, cluster_nodes(ram)))}] ++
        case is_running() of
            true  -> RunningNodes = cluster_nodes(running),
                     [{running_nodes, RunningNodes},
                      {cluster_name,  rabbit_nodes:cluster_name()},
                      {partitions,    mnesia_partitions(RunningNodes)}];
            false -> []
        end.

mnesia_partitions(Nodes) ->
    Replies = rabbit_node_monitor:partitions(Nodes),
    [Reply || Reply = {_, R} <- Replies, R =/= []].

is_running() -> mnesia:system_info(is_running) =:= yes.

-spec is_clustered() -> boolean().

is_clustered() -> AllNodes = cluster_nodes(all),
                  AllNodes =/= [] andalso AllNodes =/= [node()].

-spec on_running_node(pid()) -> boolean().

on_running_node(Pid) -> lists:member(node(Pid), cluster_nodes(running)).

%% This requires the process be in the same running cluster as us
%% (i.e. not partitioned or some random node).
%%
%% See also rabbit_misc:is_process_alive/1 which does not.

-spec is_process_alive(pid() | {atom(), node()}) -> boolean().

is_process_alive(Pid) when is_pid(Pid) ->
    on_running_node(Pid) andalso
        rpc:call(node(Pid), erlang, is_process_alive, [Pid]) =:= true;
is_process_alive({Name, Node}) ->
    lists:member(Node, cluster_nodes(running)) andalso
        rpc:call(Node, rabbit_mnesia, is_registered_process_alive, [Name]) =:= true.

-spec is_registered_process_alive(atom()) -> boolean().

is_registered_process_alive(Name) ->
    is_pid(whereis(Name)).

-spec cluster_nodes('all' | 'disc' | 'ram' | 'running') -> [node()];
                   ('status') -> {[node()], [node()], [node()]}.

cluster_nodes(WhichNodes) -> cluster_status(WhichNodes).

%% This function is the actual source of information, since it gets
%% the data from mnesia. Obviously it'll work only when mnesia is
%% running.

-spec cluster_status_from_mnesia() -> rabbit_types:ok_or_error2(
                                              cluster_status(), any()).

cluster_status_from_mnesia() ->
    case is_running() of
        false ->
            {error, mnesia_not_running};
        true ->
            %% If the tables are not present, it means that
            %% `init_db/3' hasn't been run yet. In other words, either
            %% we are a virgin node or a restarted RAM node. In both
            %% cases we're not interested in what mnesia has to say.
            NodeType = case mnesia:system_info(use_dir) of
                           true  -> disc;
                           false -> ram
                       end,
            case rabbit_table:is_present() of
                true  -> AllNodes = mnesia:system_info(db_nodes),
                         DiscCopies = mnesia:table_info(schema, disc_copies),
                         DiscNodes = case NodeType of
                                         disc -> nodes_incl_me(DiscCopies);
                                         ram  -> DiscCopies
                                     end,
                         %% `mnesia:system_info(running_db_nodes)' is safe since
                         %% we know that mnesia is running
                         RunningNodes = mnesia:system_info(running_db_nodes),
                         {ok, {AllNodes, DiscNodes, RunningNodes}};
                false -> {error, tables_not_present}
            end
    end.

cluster_status(WhichNodes) ->
    {AllNodes, DiscNodes, RunningNodes} = Nodes =
        case cluster_status_from_mnesia() of
            {ok, Nodes0} ->
                Nodes0;
            {error, _Reason} ->
                {AllNodes0, DiscNodes0, RunningNodes0} =
                    rabbit_node_monitor:read_cluster_status(),
                %% The cluster status file records the status when the node is
                %% online, but we know for sure that the node is offline now, so
                %% we can remove it from the list of running nodes.
                {AllNodes0, DiscNodes0, nodes_excl_me(RunningNodes0)}
        end,
    case WhichNodes of
        status  -> Nodes;
        all     -> AllNodes;
        disc    -> DiscNodes;
        ram     -> AllNodes -- DiscNodes;
        running -> RunningNodes
    end.

node_info() ->
    {rabbit_misc:otp_release(), rabbit_misc:version(),
     mnesia:system_info(protocol_version),
     cluster_status_from_mnesia()}.

-spec node_type() -> rabbit_db_cluster:node_type().

node_type() ->
    {_AllNodes, DiscNodes, _RunningNodes} =
        rabbit_node_monitor:read_cluster_status(),
    case DiscNodes =:= [] orelse me_in_nodes(DiscNodes) of
        true  -> disc;
        false -> ram
    end.

-spec dir() -> file:filename().

dir() -> mnesia:system_info(directory).

%%----------------------------------------------------------------------------
%% Operations on the db
%%----------------------------------------------------------------------------

%% Adds the provided nodes to the mnesia cluster, creating a new
%% schema if there is the need to and catching up if there are other
%% nodes in the cluster already. It also updates the cluster status
%% file.
init_db(ClusterNodes, NodeType, CheckOtherNodes) ->
    NodeIsVirgin = is_virgin_node(),
    rabbit_log:debug("Does data directory looks like that of a blank (uninitialised) node? ~tp", [NodeIsVirgin]),
    Nodes = change_extra_db_nodes(ClusterNodes, CheckOtherNodes),
    %% Note that we use `system_info' here and not the cluster status
    %% since when we start rabbit for the first time the cluster
    %% status will say we are a disc node but the tables won't be
    %% present yet.
    WasDiscNode = mnesia:system_info(use_dir),
    case {Nodes, WasDiscNode, NodeType} of
        {[], _, ram} ->
            %% Standalone ram node, we don't want that
            throw({error, cannot_create_standalone_ram_node});
        {[], false, disc} ->
            %% RAM -> disc, starting from scratch
            ok = create_schema(),
            ensure_feature_flags_are_in_sync(Nodes, NodeIsVirgin),
            ok;
        {[], true, disc} ->
            %% First disc node up
            _ = maybe_force_load(),
            ensure_feature_flags_are_in_sync(Nodes, NodeIsVirgin),
            ok;
        {[_ | _], _, _} ->
            %% Subsequent node in cluster, catch up
            _ = maybe_force_load(),
            %% We want to synchronize feature flags first before we wait for
            %% tables (which is needed to ensure the local view of the tables
            %% matches the rest of the cluster). The reason is that some
            %% feature flags may add or remove tables. In this case the list
            %% of tables returned by `rabbit_table:definitions()' usually
            %% depends on the state of feature flags but this state is local.
            %%
            %% For instance, a feature flag may remove a table (so it's gone
            %% from the cluster). If we were to wait for that table locally
            %% before synchronizing feature flags, we would wait forever;
            %% indeed the feature flag being disabled before sync,
            %% `rabbit_table:definitions()' would return the old table.
            ensure_feature_flags_are_in_sync(Nodes, NodeIsVirgin),
            ok = rabbit_table:wait_for_replicated(_Retry = true),
            ok = rabbit_table:ensure_local_copies(NodeType)
    end,
    ensure_schema_integrity(),
    rabbit_node_monitor:update_cluster_status(),
    ok.

-spec init_db_unchecked([node()], rabbit_db_cluster:node_type()) -> 'ok'.

init_db_unchecked(ClusterNodes, NodeType) ->
    init_db(ClusterNodes, NodeType, false).

init_db_and_upgrade(ClusterNodes, NodeType, CheckOtherNodes, Retry) ->
    ok = init_db(ClusterNodes, NodeType, CheckOtherNodes),
    ok = record_node_type(NodeType),
    rabbit_table:wait_for_replicated(Retry),
    ok.

record_node_type(NodeType) ->
    %% We record the node type in the Mnesia directory. Not that we care about
    %% the content of this file, but we need to create a file in the Mnesia
    %% directory fo `is_virgin_node()' to work even with ram nodes.
    MnesiaDir = dir(),
    Filename = filename:join(MnesiaDir, "node-type.txt"),
    rabbit_file:write_term_file(Filename, [NodeType]).

init_db_with_mnesia(ClusterNodes, NodeType,
                    CheckOtherNodes, CheckConsistency, Retry) ->
    start_mnesia(CheckConsistency),
    try
        init_db_and_upgrade(ClusterNodes, NodeType, CheckOtherNodes, Retry)
    after
        stop_mnesia()
    end.

-spec ensure_mnesia_dir() -> 'ok'.

ensure_mnesia_dir() ->
    MnesiaDir = dir() ++ "/",
    case filelib:ensure_dir(MnesiaDir) of
        {error, Reason} ->
            throw({error, {cannot_create_mnesia_dir, MnesiaDir, Reason}});
        ok ->
            ok
    end.

ensure_mnesia_running() ->
    case mnesia:system_info(is_running) of
        yes ->
            ok;
        starting ->
            wait_for(mnesia_running),
            ensure_mnesia_running();
        Reason when Reason =:= no; Reason =:= stopping ->
            throw({error, mnesia_not_running})
    end.

ensure_mnesia_not_running() ->
    case mnesia:system_info(is_running) of
        no ->
            ok;
        stopping ->
            wait_for(mnesia_not_running),
            ensure_mnesia_not_running();
        Reason when Reason =:= yes; Reason =:= starting ->
            throw({error, mnesia_unexpectedly_running})
    end.

ensure_feature_flags_are_in_sync(Nodes, NodeIsVirgin) ->
    Ret = rabbit_feature_flags:sync_feature_flags_with_cluster(
            Nodes, NodeIsVirgin),
    case Ret of
        ok              -> ok;
        {error, Reason} -> throw({error, {incompatible_feature_flags, Reason}})
    end.

ensure_schema_integrity() ->
    case rabbit_table:check_schema_integrity(_Retry = true) of
        ok ->
            ok;
        {error, Reason} ->
            throw({error, {schema_integrity_check_failed, Reason}})
    end.

-spec copy_db(file:filename()) ->  rabbit_types:ok_or_error(any()).

copy_db(Destination) ->
    ok = ensure_mnesia_not_running(),
    rabbit_file:recursive_copy(dir(), Destination).

force_load_filename() ->
    filename:join(dir(), "force_load").

-spec force_load_next_boot() -> 'ok'.

force_load_next_boot() ->
    rabbit_file:write_file(force_load_filename(), <<"">>).

maybe_force_load() ->
    case rabbit_file:is_file(force_load_filename()) of
        true  -> rabbit_table:force_load(),
                 rabbit_file:delete(force_load_filename());
        false -> ok
    end.

%% This does not guarantee us much, but it avoids some situations that
%% will definitely end up badly

-spec check_cluster_consistency() -> 'ok'.

check_cluster_consistency() ->
    %% We want to find 0 or 1 consistent nodes.
    case lists:foldl(
           fun (Node,  {error, _})    -> check_cluster_consistency(Node, true);
               (_Node, {ok, Status})  -> {ok, Status}
           end, {error, not_found}, nodes_excl_me(cluster_nodes(all)))
    of
        {ok, Status = {RemoteAllNodes, _, _}} ->
            case ordsets:is_subset(ordsets:from_list(cluster_nodes(all)),
                                   ordsets:from_list(RemoteAllNodes)) of
                true  ->
                    ok;
                false ->
                    %% We delete the schema here since we think we are
                    %% clustered with nodes that are no longer in the
                    %% cluster and there is no other way to remove
                    %% them from our schema. On the other hand, we are
                    %% sure that there is another online node that we
                    %% can use to sync the tables with. There is a
                    %% race here: if between this check and the
                    %% `init_db' invocation the cluster gets
                    %% disbanded, we're left with a node with no
                    %% mnesia data that will try to connect to offline
                    %% nodes.
                    _ = mnesia:delete_schema([node()]),
                    ok
            end,
            rabbit_node_monitor:write_cluster_status(Status);
        {error, not_found} ->
            ok;
        {error, _} = E ->
            throw(E)
    end.

check_cluster_consistency(Node, CheckNodesConsistency) ->
    case remote_node_info(Node) of
        {badrpc, _Reason} ->
            {error, not_found};
        {_OTP, Rabbit, DelegateModuleHash, _Status} when is_binary(DelegateModuleHash) ->
            %% when a delegate module .beam file hash is present
            %% in the tuple, we are dealing with an old version
            rabbit_version:version_error("Rabbit", rabbit_misc:version(), Rabbit);
        {_OTP, _Rabbit, _Protocol, {error, _}} ->
            {error, not_found};
        {OTP, Rabbit, Protocol, {ok, Status}} when CheckNodesConsistency ->
            case check_consistency(Node, OTP, Rabbit, Protocol, Status) of
                {error, _} = E -> E;
                {ok, Res}      -> {ok, Res}
            end;
        {OTP, Rabbit, Protocol, {ok, Status}} ->
            case check_consistency(Node, OTP, Rabbit, Protocol) of
                {error, _} = E -> E;
                ok             -> {ok, Status}
            end
    end.

remote_node_info(Node) ->
    case rpc:call(Node, rabbit_mnesia, node_info, []) of
        {badrpc, _} = Error   -> Error;
        %% RabbitMQ prior to 3.6.2
        {OTP, Rabbit, Status} -> {OTP, Rabbit, unsupported, Status};
        %% RabbitMQ 3.6.2 or later
        {OTP, Rabbit, Protocol, Status} -> {OTP, Rabbit, Protocol, Status}
    end.


%%--------------------------------------------------------------------
%% Hooks for `rabbit_node_monitor'
%%--------------------------------------------------------------------

-spec on_node_up(node()) -> 'ok'.

on_node_up(Node) ->
    case running_disc_nodes() of
        [Node] -> rabbit_log:info("cluster contains disc nodes again~n");
        _      -> ok
    end.

-spec on_node_down(node()) -> 'ok'.

on_node_down(_Node) ->
    case running_disc_nodes() of
        [] -> rabbit_log:info("only running disc node went down~n");
        _  -> ok
    end.

running_disc_nodes() ->
    {_AllNodes, DiscNodes, RunningNodes} = cluster_status(status),
    ordsets:to_list(ordsets:intersection(ordsets:from_list(DiscNodes),
                                         ordsets:from_list(RunningNodes))).

%%--------------------------------------------------------------------
%% Helpers for diagnostics commands
%%--------------------------------------------------------------------

schema_info(Items) ->
    Tables = mnesia:system_info(tables),
    [info(Table, Items) || Table <- Tables].

info(Table, Items) ->
    All = [{name, Table} | mnesia:table_info(Table, all)],
    [{Item, proplists:get_value(Item, All)} || Item <- Items].

%%--------------------------------------------------------------------
%% Queries
%%--------------------------------------------------------------------

-spec table_filter
        (fun ((A) -> boolean()), fun ((A, boolean()) -> 'ok'), atom()) -> [A].
%% Apply a pre-post-commit function to all entries in a table that
%% satisfy a predicate, and return those entries.
%%
%% We ignore entries that have been modified or removed.
table_filter(Pred, PrePostCommitFun, TableName) ->
    lists:foldl(
      fun (E, Acc) ->
              case execute_mnesia_transaction(
                     fun () -> mnesia:match_object(TableName, E, read) =/= []
                                   andalso Pred(E) end,
                     fun (false, _Tx) -> false;
                         (true,   Tx) -> PrePostCommitFun(E, Tx), true
                     end) of
                  false -> Acc;
                  true  -> [E | Acc]
              end
      end, [], dirty_read_all(TableName)).

-spec dirty_read_all(atom()) -> [any()].
dirty_read_all(TableName) ->
    mnesia:dirty_select(TableName, [{'$1',[],['$1']}]).

-spec dirty_read({atom(), any()}) ->
          rabbit_types:ok_or_error2(any(), 'not_found').
%% Normally we'd call mnesia:dirty_read/1 here, but that is quite
%% expensive due to general mnesia overheads (figuring out table types
%% and locations, etc). We get away with bypassing these because we
%% know that the tables we are looking at here
%% - are not the schema table
%% - have a local ram copy
%% - do not have any indices
dirty_read({Table, Key}) ->
    case ets:lookup(Table, Key) of
        [Result] -> {ok, Result};
        []       -> {error, not_found}
    end.

-spec execute_mnesia_tx_with_tail
        (rabbit_misc:thunk(fun ((boolean()) -> B))) -> B | (fun ((boolean()) -> B)).
%% Like execute_mnesia_transaction/2, but TxFun is expected to return a
%% TailFun which gets called (only) immediately after the tx commit
execute_mnesia_tx_with_tail(TxFun) ->
    case mnesia:is_transaction() of
        true  -> execute_mnesia_transaction(TxFun);
        false -> TailFun = execute_mnesia_transaction(TxFun),
                 TailFun()
    end.

-spec execute_mnesia_transaction(rabbit_misc:thunk(A)) -> A.
execute_mnesia_transaction(TxFun) ->
    %% Making this a sync_transaction allows us to use dirty_read
    %% elsewhere and get a consistent result even when that read
    %% executes on a different node.
    case worker_pool:submit(
           fun () ->
                   case mnesia:is_transaction() of
                       false -> DiskLogBefore = mnesia_dumper:get_log_writes(),
                                Res = mnesia:sync_transaction(TxFun),
                                DiskLogAfter  = mnesia_dumper:get_log_writes(),
                                case DiskLogAfter == DiskLogBefore of
                                    true  -> file_handle_cache_stats:update(
                                              mnesia_ram_tx),
                                             Res;
                                    false -> file_handle_cache_stats:update(
                                              mnesia_disk_tx),
                                             {sync, Res}
                                end;
                       true  -> mnesia:sync_transaction(TxFun)
                   end
           end, single) of
        {sync, {atomic,  Result}} -> mnesia_sync:sync(), Result;
        {sync, {aborted, Reason}} -> throw({error, Reason});
        {atomic,  Result}         -> Result;
        {aborted, Reason}         -> throw({error, Reason})
    end.

-spec execute_mnesia_transaction(rabbit_misc:thunk(A), fun ((A, boolean()) -> B)) -> B.
%% Like execute_mnesia_transaction/1 with additional Pre- and Post-
%% commit function
execute_mnesia_transaction(TxFun, PrePostCommitFun) ->
    case mnesia:is_transaction() of
        true  -> throw(unexpected_transaction);
        false -> ok
    end,
    PrePostCommitFun(execute_mnesia_transaction(
                       fun () ->
                               Result = TxFun(),
                               PrePostCommitFun(Result, true),
                               Result
                       end), false).

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

discover_cluster(Nodes) ->
    case lists:foldl(fun (_,    {ok, Res}) -> {ok, Res};
                         (Node, _)         -> discover_cluster0(Node)
                     end, {error, no_nodes_provided}, Nodes) of
        {ok, Res}        -> Res;
        {error, E}       -> throw({error, E});
        {badrpc, Reason} -> throw({badrpc_multi, Reason, Nodes})
    end.

discover_cluster0(Node) when Node == node() ->
    {error, cannot_cluster_node_with_itself};
discover_cluster0(Node) ->
    rpc:call(Node, rabbit_mnesia, cluster_status_from_mnesia, []).

%% We only care about disc nodes since ram nodes are supposed to catch
%% up only
create_schema() ->
    stop_mnesia(),
    rabbit_log:debug("Will bootstrap a schema database..."),
    rabbit_misc:ensure_ok(mnesia:create_schema([node()]), cannot_create_schema),
    rabbit_log:debug("Bootstraped a schema database successfully"),
    start_mnesia(),

    rabbit_log:debug("Will create schema database tables"),
    ok = rabbit_table:create(),
    rabbit_log:debug("Created schema database tables successfully"),
    rabbit_log:debug("Will check schema database integrity..."),
    ensure_schema_integrity(),
    rabbit_log:debug("Schema database schema integrity check passed"),
    ok.

remove_node_if_mnesia_running(Node) ->
    case is_running() of
        false ->
            {error, mnesia_not_running};
        true ->
            %% Deleting the the schema copy of the node will result in
            %% the node being removed from the cluster, with that
            %% change being propagated to all nodes
            case mnesia:del_table_copy(schema, Node) of
                {atomic, ok} ->
                    rabbit_amqqueue:forget_all_durable(Node),
                    rabbit_node_monitor:notify_left_cluster(Node),
                    ok;
                {aborted, Reason} ->
                    {error, {failed_to_remove_node, Node, Reason}}
            end
    end.

leave_cluster() ->
    case nodes_excl_me(cluster_nodes(all)) of
        []       -> ok;
        AllNodes -> case lists:any(fun leave_cluster/1, AllNodes) of
                        true  -> ok;
                        false -> e(no_running_cluster_nodes)
                    end
    end.

leave_cluster(Node) ->
    case rpc:call(Node,
                  rabbit_mnesia, remove_node_if_mnesia_running, [node()]) of
        ok                          -> true;
        {error, mnesia_not_running} -> false;
        {error, Reason}             -> throw({error, Reason});
        {badrpc, nodedown}          -> false
    end.

wait_for(Condition) ->
    rabbit_log:info("Waiting for ~tp...", [Condition]),
    timer:sleep(1000).

start_mnesia(CheckConsistency) ->
    case CheckConsistency of
        true  -> check_cluster_consistency();
        false -> ok
    end,
    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    ensure_mnesia_running().

start_mnesia() ->
    start_mnesia(true).

stop_mnesia() ->
    stopped = mnesia:stop(),
    ensure_mnesia_not_running().

change_extra_db_nodes(ClusterNodes0, CheckOtherNodes) ->
    ClusterNodes = nodes_excl_me(ClusterNodes0),
    case {mnesia:change_config(extra_db_nodes, ClusterNodes), ClusterNodes} of
        {{ok, []}, [_|_]} when CheckOtherNodes ->
            throw({error, {failed_to_cluster_with, ClusterNodes,
                           "Mnesia could not connect to any nodes."}});
        {{ok, Nodes}, _} ->
            Nodes
    end.

check_consistency(Node, OTP, Rabbit, ProtocolVersion) ->
    rabbit_misc:sequence_error(
      [check_mnesia_or_otp_consistency(Node, ProtocolVersion, OTP),
       check_rabbit_consistency(Node, Rabbit)]).

check_consistency(Node, OTP, Rabbit, ProtocolVersion, Status) ->
    rabbit_misc:sequence_error(
      [check_mnesia_or_otp_consistency(Node, ProtocolVersion, OTP),
       check_rabbit_consistency(Node, Rabbit),
       check_nodes_consistency(Node, Status)]).

check_nodes_consistency(Node, RemoteStatus = {RemoteAllNodes, _, _}) ->
    case me_in_nodes(RemoteAllNodes) of
        true ->
            {ok, RemoteStatus};
        false ->
            {error, {inconsistent_cluster,
                     format_inconsistent_cluster_message(node(), Node)}}
    end.

check_mnesia_or_otp_consistency(_Node, unsupported, OTP) ->
    rabbit_version:check_otp_consistency(OTP);
check_mnesia_or_otp_consistency(Node, ProtocolVersion, _) ->
    check_mnesia_consistency(Node, ProtocolVersion).

check_mnesia_consistency(Node, ProtocolVersion) ->
    % If mnesia is running we will just check protocol version
    % If it's not running, we don't want it to join cluster until all checks pass
    % so we start it without `dir` env variable to prevent
    % joining cluster and/or corrupting data
    with_running_or_clean_mnesia(fun() ->
        case negotiate_protocol([Node]) of
            [Node] -> ok;
            []     ->
                LocalVersion = mnesia:system_info(protocol_version),
                {error, {inconsistent_cluster,
                         rabbit_misc:format("Mnesia protocol negotiation failed."
                                            " Local version: ~tp."
                                            " Remote version ~tp",
                                            [LocalVersion, ProtocolVersion])}}
        end
    end).

negotiate_protocol([Node]) ->
    mnesia_monitor:negotiate_protocol([Node]).

with_running_or_clean_mnesia(Fun) ->
    IsMnesiaRunning = case mnesia:system_info(is_running) of
        yes      -> true;
        no       -> false;
        stopping ->
            ensure_mnesia_not_running(),
            false;
        starting ->
            ensure_mnesia_running(),
            true
    end,
    case IsMnesiaRunning of
        true  -> Fun();
        false ->
            SavedMnesiaDir = dir(),
            application:unset_env(mnesia, dir),
            SchemaLoc = application:get_env(mnesia, schema_location, opt_disc),
            application:set_env(mnesia, schema_location, ram),
            _ = mnesia:start(),
            Result = Fun(),
            _ = application:stop(mnesia),
            application:set_env(mnesia, dir, SavedMnesiaDir),
            application:set_env(mnesia, schema_location, SchemaLoc),
            Result
    end.

check_rabbit_consistency(RemoteNode, RemoteVersion) ->
    rabbit_misc:sequence_error(
      [rabbit_version:check_version_consistency(
         rabbit_misc:version(), RemoteVersion, "Rabbit",
         fun rabbit_misc:version_minor_equivalent/2),
       rabbit_feature_flags:check_node_compatibility(RemoteNode)]).

%% This is fairly tricky.  We want to know if the node is in the state
%% that a `reset' would leave it in.  We cannot simply check if the
%% mnesia tables aren't there because restarted RAM nodes won't have
%% tables while still being non-virgin.  What we do instead is to
%% check if the mnesia directory is non existent or empty, with the
%% exception of certain files and directories, which can be there very early
%% on node boot.
is_virgin_node() ->
    case rabbit_file:list_dir(dir()) of
        {error, enoent} ->
            true;
        {ok, []} ->
            true;
        {ok, List0} ->
            IgnoredFiles0 =
            [rabbit_node_monitor:cluster_status_filename(),
             rabbit_node_monitor:running_nodes_filename(),
             rabbit_node_monitor:coordination_filename(),
             rabbit_node_monitor:stream_filename(),
             rabbit_node_monitor:default_quorum_filename(),
             rabbit_node_monitor:quorum_filename(),
             rabbit_feature_flags:enabled_feature_flags_list_file()],
            IgnoredFiles = [filename:basename(File) || File <- IgnoredFiles0],
            rabbit_log:debug("Files and directories found in node's data directory: ~ts, of them to be ignored: ~ts",
                            [string:join(lists:usort(List0), ", "), string:join(lists:usort(IgnoredFiles), ", ")]),
            List = List0 -- IgnoredFiles,
            rabbit_log:debug("Files and directories found in node's data directory sans ignored ones: ~ts", [string:join(lists:usort(List), ", ")]),
            List =:= []
    end.

find_reachable_peer_to_cluster_with([]) ->
    none;
find_reachable_peer_to_cluster_with([Node | Nodes]) ->
    Fail = fun (Fmt, Args) ->
                   rabbit_log:warning(
                     "Could not auto-cluster with node ~ts: " ++ Fmt, [Node | Args]),
                   find_reachable_peer_to_cluster_with(Nodes)
           end,
    case remote_node_info(Node) of
        {badrpc, _} = Reason ->
            Fail("~tp", [Reason]);
        %% old delegate hash check
        {_OTP, RMQ, Hash, _} when is_binary(Hash) ->
            Fail("version ~ts", [RMQ]);
        {_OTP, _RMQ, _Protocol, {error, _} = E} ->
            Fail("~tp", [E]);
        {OTP, RMQ, Protocol, _} ->
            case check_consistency(Node, OTP, RMQ, Protocol) of
                {error, _} -> Fail("versions ~tp",
                                   [{OTP, RMQ}]);
                ok         -> {ok, Node}
            end
    end.

is_only_clustered_disc_node() ->
    node_type() =:= disc andalso is_clustered() andalso
        cluster_nodes(disc) =:= [node()].

are_we_clustered_with(Node) ->
    lists:member(Node, mnesia_lib:all_nodes()).

me_in_nodes(Nodes) -> lists:member(node(), Nodes).

nodes_incl_me(Nodes) -> lists:usort([node()|Nodes]).

nodes_excl_me(Nodes) -> Nodes -- [node()].

-spec e(any()) -> no_return().

e(Tag) -> throw({error, {Tag, error_description(Tag)}}).

error_description({invalid_cluster_node_names, BadNames}) ->
    "In the 'cluster_nodes' configuration key, the following node names "
        "are invalid: " ++ lists:flatten(io_lib:format("~tp", [BadNames]));
error_description({invalid_cluster_node_type, BadType}) ->
    "In the 'cluster_nodes' configuration key, the node type is invalid "
        "(expected 'disc' or 'ram'): " ++
        lists:flatten(io_lib:format("~tp", [BadType]));
error_description(invalid_cluster_nodes_conf) ->
    "The 'cluster_nodes' configuration key is invalid, it must be of the "
        "form {[Nodes], Type}, where Nodes is a list of node names and "
        "Type is either 'disc' or 'ram'";
error_description(clustering_only_disc_node) ->
    "You cannot cluster a node if it is the only disc node in its existing "
        " cluster. If new nodes joined while this node was offline, use "
        "'update_cluster_nodes' to add them manually.";
error_description(resetting_only_disc_node) ->
    "You cannot reset a node when it is the only disc node in a cluster. "
        "Please convert another node of the cluster to a disc node first.";
error_description(not_clustered) ->
    "Non-clustered nodes can only be disc nodes.";
error_description(no_online_cluster_nodes) ->
    "Could not find any online cluster nodes. If the cluster has changed, "
        "you can use the 'update_cluster_nodes' command.";
error_description(inconsistent_cluster) ->
    "The nodes provided do not have this node as part of the cluster.";
error_description(not_a_cluster_node) ->
    "The node selected is not in the cluster.";
error_description(online_node_offline_flag) ->
    "You set the --offline flag, which is used to remove nodes remotely from "
        "offline nodes, but this node is online.";
error_description(offline_node_no_offline_flag) ->
    "You are trying to remove a node from an offline node. That is dangerous, "
        "but can be done with the --offline flag. Please consult the manual "
        "for rabbitmqctl for more information.";
error_description(removing_node_from_offline_node) ->
    "To remove a node remotely from an offline node, the node you are removing "
        "from must be a disc node and all the other nodes must be offline.";
error_description(no_running_cluster_nodes) ->
    "You cannot leave a cluster if no online nodes are present.".

format_inconsistent_cluster_message(Thinker, Dissident) ->
    rabbit_misc:format("Node ~tp thinks it's clustered "
                       "with node ~tp, but ~tp disagrees",
                       [Thinker, Dissident, Dissident]).
