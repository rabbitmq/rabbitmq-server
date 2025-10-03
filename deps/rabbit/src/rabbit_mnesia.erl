%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mnesia).

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([%% Main interface
         init/0,
         can_join_cluster/1,
         join_cluster/2,
         reset/0,
         force_reset/0,
         change_cluster_node_type/1,
         forget_cluster_node/2,
         force_load_next_boot/0,

         %% Various queries to get the status of the db
         %% %% FIXME: Comment below not true anymore.
         %%
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

         %% Operations on the db and utils, mainly used in `rabbit' and Mnesia-era modules
         %% (some of which may now be gone)
         init_db_unchecked/2,
         copy_db/1,
         check_mnesia_consistency/1,
         check_cluster_consistency/0,
         ensure_mnesia_dir/0,
         ensure_mnesia_running/0,
         ensure_node_type_is_permitted/1,

         %% Hooks used in `rabbit_node_monitor'
         on_node_up/1,
         on_node_down/1,

         %% Helpers for diagnostics commands
         schema_info/1,

         start_mnesia/1,
         stop_mnesia/0,

         reset_gracefully/0,

         e/1
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

%% Used internally in `rabbit_db_cluster'.
-export([members/0, leave_then_rediscover_cluster/1]).

%% Used internally in `rabbit_khepri'.
-export([mnesia_and_msg_store_files/0]).

-export([check_reset_gracefully/0]).

-deprecated({on_running_node, 1,
             "Use rabbit_process:on_running_node/1 instead"}).
-deprecated({is_process_alive, 1,
             "Use rabbit_process:is_process_alive/1 instead"}).
-deprecated({is_registered_process_alive, 1,
             "Use rabbit_process:is_registered_process_alive/1 instead"}).

-ifdef(TEST).
-compile(export_all).
-endif.

%%----------------------------------------------------------------------------

-export_type([cluster_status/0]).

-type cluster_status() :: {[node()], [node()], [node()]}.

%%----------------------------------------------------------------------------
%% Main interface
%%----------------------------------------------------------------------------

-rabbit_deprecated_feature(
   {ram_node_type,
    #{deprecation_phase => permitted_by_default,
      doc_url => "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#removal-of-ram-nodes"
     }}).

-spec init() -> 'ok'.

init() ->
    ensure_mnesia_running(),
    ensure_mnesia_dir(),
    %% Peer discovery may have been a no-op if it decided that all other nodes
    %% should join this one. Therefore, we need to look at if this node is
    %% still virgin and finish our init of Mnesia accordingly. In particular,
    %% this second part creates all our Mnesia tables.
    case is_virgin_node() of
        true ->
            init_db_and_upgrade([node()], disc, true, _Retry = true);
        false ->
            NodeType = node_type(),
            case is_node_type_permitted(NodeType) of
                false ->
                    ?LOG_INFO(
                      "RAM nodes are deprecated and not permitted. This "
                      "node will be converted to a disc node."),
                    init_db_and_upgrade(cluster_nodes(all), disc,
                                        true, _Retry = true);
                true ->
                    init_db_and_upgrade(cluster_nodes(all), NodeType,
                                        NodeType =:= ram, _Retry = true)
            end
    end,
    %% We intuitively expect the global name server to be synced when
    %% Mnesia is up. In fact that's not guaranteed to be the case -
    %% let's make it so.
    ok = rabbit_node_monitor:global_sync(),
    ok.

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

-spec can_join_cluster(node())
                      -> {ok, [node()]} | {ok, already_member} | {error, {inconsistent_cluster, string()} | {error, {erpc, noconnection}}}.

can_join_cluster(DiscoveryNode) ->
    ensure_mnesia_dir(),
    case is_only_clustered_disc_node() of
        true  -> e(clustering_only_disc_node);
        false -> ok
    end,
    {ClusterNodes, _, _} = discover_cluster([DiscoveryNode]),
    case rabbit_nodes:me_in_nodes(ClusterNodes) of
        false ->
            case check_cluster_consistency(DiscoveryNode, false) of
                {ok, _} -> {ok, ClusterNodes};
                Error   -> Error
            end;
        true ->
            %% DiscoveryNode thinks that we are part of a cluster, but
            %% do we think so ourselves?
            case are_we_clustered_with(DiscoveryNode) of
                true ->
                    ?LOG_INFO("Asked to join a cluster but already a member of it: ~tp", [ClusterNodes]),
                    {ok, already_member};
                false ->
                    Msg = format_inconsistent_cluster_message(DiscoveryNode, node()),
                    {error, {inconsistent_cluster, Msg}}
            end
    end.

-spec join_cluster
([node()], rabbit_db_cluster:node_type()) ->
    ok | {error, any()};
(node(), rabbit_db_cluster:node_type()) ->
    ok | {ok, already_member} | {error, {inconsistent_cluster, string()}}.

join_cluster(ClusterNodes, NodeType) when is_list(ClusterNodes) ->
    %% Join the cluster.
    NodeType1 = case is_node_type_permitted(NodeType) of
                    false -> disc;
                    true  -> NodeType
                end,
    ?LOG_INFO("Clustering with ~tp as ~tp node",
                    [ClusterNodes, NodeType1]),
    ok = init_db_with_mnesia(ClusterNodes, NodeType1,
                             true, true, _Retry = true),
    rabbit_node_monitor:notify_joined_cluster(),
    ok;
join_cluster(DiscoveryNode, NodeType) when is_atom(DiscoveryNode) ->
    %% Code to remain compatible with `change_cluster_node_type/1' and older
    %% CLI.
    ensure_mnesia_not_running(),
    case can_join_cluster(DiscoveryNode) of
        {ok, ClusterNodes} when is_list(ClusterNodes) ->
            ok = reset_gracefully(),
            ok = join_cluster(ClusterNodes, NodeType),
            ok;
        {ok, already_member} ->
            {ok, already_member};
        Error ->
            Error
    end.

%% return node to its virgin state, where it is not member of any
%% cluster, has no cluster configuration, no local database, and no
%% persisted messages

-spec reset() -> 'ok'.

reset() ->
    ensure_mnesia_not_running(),
    reset_gracefully().

-spec force_reset() -> 'ok'.

force_reset() ->
    ensure_mnesia_not_running(),
    ?LOG_INFO("Resetting Rabbit forcefully", []),
    wipe().

reset_gracefully() ->
    AllNodes = cluster_nodes(all),
    %% Reconnecting so that we will get an up to date nodes.  We don't
    %% need to check for consistency because we are resetting.
    %% Force=true here so that reset still works when clustered with a
    %% node which is down.
    init_db_with_mnesia(AllNodes, node_type(), false, false, _Retry = false),
    check_reset_gracefully(),
    leave_cluster(),
    rabbit_misc:ensure_ok(mnesia:delete_schema([node()]), cannot_delete_schema),
    wipe().

check_reset_gracefully() ->
    case is_only_clustered_disc_node() of
        true  -> e(resetting_only_disc_node);
        false -> ok
    end.

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
    ensure_node_type_is_permitted(Type),
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

%% We proceed like this: try to remove the node locally. If the node
%% is offline, we remove the node if:
%%   * This node is a disc node
%%   * All other nodes are offline
%%   * This node was, at the best of our knowledge (see comment below)
%%     the last or second to last after the node we're removing to go
%%     down

-spec forget_cluster_node(node(), boolean()) -> 'ok'.

forget_cluster_node(Node, RemoveWhenOffline) ->
    case lists:member(Node, cluster_nodes(all)) of
        true  -> ok;
        false -> e(not_a_cluster_node)
    end,
    case {RemoveWhenOffline, is_running()} of
        {true,  false} -> remove_node_offline_node(Node);
        {true,   true} -> e(online_node_offline_flag);
        {false, false} -> e(offline_node_no_offline_flag);
        {false,  true} -> ?LOG_INFO(
                            "Removing node ~tp from cluster", [Node]),
                          case remove_node_if_mnesia_running(Node) of
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
                forget_cluster_node(Node, false),
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
            case rabbit_khepri:get_feature_state() of
                enabled ->
                    %% To keep this API compatible with older remote nodes who
                    %% don't know about Khepri, we take the cluster status
                    %% from `rabbit_khepri' and reformat the return value to
                    %% ressemble the node from this module.
                    %%
                    %% Both nodes won't be compatible, but let's leave that
                    %% decision to the Feature flags subsystem.
                    case rabbit_khepri:cluster_status_from_khepri() of
                        {ok, {All, Running}} ->
                            {ok, {All, All, Running}};
                        {error, _} = Error ->
                            Error
                    end;
                _ ->
                    {error, mnesia_not_running}
            end;
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
                                         disc -> rabbit_nodes:nodes_incl_me(DiscCopies);
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
                {AllNodes0, DiscNodes0, rabbit_nodes:nodes_excl_me(RunningNodes0)}
        end,
    case WhichNodes of
        status  -> Nodes;
        all     -> AllNodes;
        disc    -> DiscNodes;
        ram     -> AllNodes -- DiscNodes;
        running -> RunningNodes
    end.

members() ->
    case is_running() andalso rabbit_table:is_present() of
        true ->
            %% If Mnesia is running locally and some tables exist, we can know
            %% the database was initialized and we can query the list of
            %% members.
            mnesia:system_info(db_nodes);
        false ->
            try
                %% When Mnesia is not running, we fall back to reading the
                %% cluster status files stored on disk, if they exist.
                {Members, _, _} = rabbit_node_monitor:read_cluster_status(),
                Members
            catch
                throw:{error, _Reason}:_Stacktrace ->
                    %% If we couldn't read those files, we consider that only
                    %% this node is part of the "cluster".
                    [node()]
            end
    end.

node_info() ->
    %% Once Khepri is enabled, the Mnesia protocol is irrelevant obviously.
    %%
    %% That said, older remote nodes who don't known about Khepri will request
    %% this information anyway as part of calling `node_info/0'. Here, we
    %% simply return `unsupported' as the Mnesia protocol. Older versions of
    %% RabbitMQ will skip the protocol negotiation and use other ways.
    %%
    %% The goal is mostly to let older nodes which check Mnesia before feature
    %% flags to reach the feature flags check. This one will correctly
    %% indicate that they are incompatible. That's why we return `unsupported'
    %% here, even if we could return the actual Mnesia protocol.
    MnesiaProtocol = case rabbit_khepri:get_feature_state() of
                         enabled -> unsupported;
                         _       -> mnesia:system_info(protocol_version)
                     end,
    {rabbit_misc:otp_release(), rabbit_misc:version(),
     MnesiaProtocol,
     cluster_status_from_mnesia()}.

-spec node_type() -> rabbit_db_cluster:node_type().

node_type() ->
    {_AllNodes, DiscNodes, _RunningNodes} =
        rabbit_node_monitor:read_cluster_status(),
    case DiscNodes =:= [] orelse rabbit_nodes:me_in_nodes(DiscNodes) of
        true  -> disc;
        false -> ram
    end.

is_node_type_permitted(ram) ->
    rabbit_deprecated_features:is_permitted(ram_node_type);
is_node_type_permitted(_NodeType) ->
    true.

ensure_node_type_is_permitted(NodeType) ->
    case is_node_type_permitted(NodeType) of
        true ->
            ok;
        false ->
            Warning = rabbit_deprecated_features:get_warning(ram_node_type),
            throw({error, Warning})
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
    ensure_node_type_is_permitted(NodeType),

    NodeIsVirgin = is_virgin_node(),
    ?LOG_DEBUG("Does data directory looks like that of a blank (uninitialised) node? ~tp", [NodeIsVirgin]),
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
            ok;
        {[], true, disc} ->
            %% First disc node up
            _ = maybe_force_load(),
            ok;
        {[_ | _], _, _} ->
            %% Subsequent node in cluster, catch up
            _ = maybe_force_load(),
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
           end, {error, not_found}, rabbit_nodes:nodes_excl_me(cluster_nodes(all)))
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
        {_OTP, _Rabbit, _Protocol, {error, _}} ->
            {error, not_found};
        {_OTP, _Rabbit, _Protocol, {ok, Status}} when CheckNodesConsistency ->
            case rabbit_db_cluster:check_compatibility(Node) of
                ok ->
                    case check_nodes_consistency(Node, Status) of
                        ok    -> {ok, Status};
                        Error -> Error
                    end;
                Error ->
                    Error
            end;
        {_OTP, _Rabbit, _Protocol, {ok, Status}} ->
            {ok, Status}
    end.

remote_node_info(Node) ->
    case rpc:call(Node, rabbit_mnesia, node_info, []) of
        {badrpc, _} = Error   -> Error;
        %% RabbitMQ 3.6.2 or later
        {OTP, Rabbit, Protocol, Status} -> {OTP, Rabbit, Protocol, Status}
    end.


%%--------------------------------------------------------------------
%% Hooks for `rabbit_node_monitor'
%%--------------------------------------------------------------------

-spec on_node_up(node()) -> 'ok'.

on_node_up(Node) ->
    case running_disc_nodes() of
        [Node] -> ?LOG_INFO("cluster contains disc nodes again~n");
        _      -> ok
    end.

-spec on_node_down(node()) -> 'ok'.

on_node_down(_Node) ->
    case running_disc_nodes() of
        [] -> ?LOG_INFO("only running disc node went down~n");
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
                                    true  -> Res;
                                    false -> {sync, Res}
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
    %% Assert we are not supposed to use Khepri.
    false = rabbit_khepri:is_enabled(),

    stop_mnesia(),
    ?LOG_DEBUG("Will bootstrap a schema database..."),
    rabbit_misc:ensure_ok(mnesia:create_schema([node()]), cannot_create_schema),
    ?LOG_DEBUG("Bootstraped a schema database successfully"),
    start_mnesia(),

    ?LOG_DEBUG("Will create schema database tables"),
    ok = rabbit_table:create(),
    ?LOG_DEBUG("Created schema database tables successfully"),
    ?LOG_DEBUG("Will check schema database integrity..."),
    ensure_schema_integrity(),
    ?LOG_DEBUG("Schema database schema integrity check passed"),
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
                    rabbit_node_monitor:notify_left_cluster(Node),
                    rabbit_amqqueue:forget_all(Node),
                    ok;
                {aborted, Reason} ->
                    {error, {failed_to_remove_node, Node, Reason}}
            end
    end.

leave_then_rediscover_cluster(DiscoveryNode) ->
    {ClusterNodes, _, _} = discover_cluster([DiscoveryNode]),
    leave_cluster(rabbit_nodes:nodes_excl_me(ClusterNodes)).

leave_cluster() ->
    leave_cluster(rabbit_nodes:nodes_excl_me(cluster_nodes(all))).
leave_cluster([]) ->
    ok;
leave_cluster(Nodes) when is_list(Nodes)  ->
    case lists:any(fun leave_cluster/1, Nodes) of
        true  -> ok;
        false -> e(no_running_cluster_nodes)
    end;
leave_cluster(Node) ->
    case rpc:call(Node,
                  rabbit_mnesia, remove_node_if_mnesia_running, [node()]) of
        ok                          -> true;
        {error, mnesia_not_running} -> false;
        {error, Reason}             -> throw({error, Reason});
        {badrpc, nodedown}          -> false
    end.

wait_for(Condition) ->
    ?LOG_INFO("Waiting for ~tp...", [Condition]),
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
    ClusterNodes = rabbit_nodes:nodes_excl_me(ClusterNodes0),
    case {mnesia:change_config(extra_db_nodes, ClusterNodes), ClusterNodes} of
        {{ok, []}, [_|_]} when CheckOtherNodes ->
            throw({error, {failed_to_cluster_with, ClusterNodes,
                           "Mnesia could not connect to any nodes."}});
        {{ok, Nodes}, _} ->
            Nodes
    end.

check_nodes_consistency(Node, {RemoteAllNodes, _, _}) ->
    case rabbit_nodes:me_in_nodes(RemoteAllNodes) of
        true ->
            ok;
        false ->
            {error, {inconsistent_cluster,
                     format_inconsistent_cluster_message(node(), Node)}}
    end.

check_mnesia_consistency(Node) ->
    case remote_node_info(Node) of
        {badrpc, _} = Reason ->
            {error, Reason};
        {_OTP, _RMQ, _ProtocolVersion, {error, _} = Error} ->
            Error;
        {_OTP, _RMQ, ProtocolVersion, _Status} ->
            check_mnesia_consistency(Node, ProtocolVersion)
    end.

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

%% This is fairly tricky.  We want to know if the node is in the state
%% that a `reset' would leave it in.  We cannot simply check if the
%% mnesia tables aren't there because restarted RAM nodes won't have
%% tables while still being non-virgin.  What we do instead is to
%% check if the mnesia directory is non existent or empty, with the
%% exception of certain files and directories, which can be there very early
%% on node boot.
is_virgin_node() ->
    mnesia_and_msg_store_files() =:= [].

mnesia_and_msg_store_files() ->
    case rabbit_file:list_dir(dir()) of
        {error, enoent} ->
            [];
        {ok, []} ->
            [];
        {ok, List0} ->
            IgnoredFiles0 =
            [rabbit_node_monitor:cluster_status_filename(),
             rabbit_node_monitor:running_nodes_filename(),
             rabbit_node_monitor:coordination_filename(),
             rabbit_node_monitor:stream_filename(),
             rabbit_node_monitor:default_quorum_filename(),
             rabbit_node_monitor:classic_filename(),
             rabbit_node_monitor:quorum_filename(),
             rabbit_feature_flags:enabled_feature_flags_list_file(),
             rabbit_khepri:dir(),
             rabbit_plugins:user_provided_plugins_data_dir()],
            IgnoredFiles = [filename:basename(File) || File <- IgnoredFiles0],
            ?LOG_DEBUG("Files and directories found in node's data directory: ~ts, of them to be ignored: ~ts",
                            [string:join(lists:usort(List0), ", "), string:join(lists:usort(IgnoredFiles), ", ")]),
            List = List0 -- IgnoredFiles,
            ?LOG_DEBUG("Files and directories found in node's data directory sans ignored ones: ~ts", [string:join(lists:usort(List), ", ")]),
            List
    end.

is_only_clustered_disc_node() ->
    node_type() =:= disc andalso is_clustered() andalso
        cluster_nodes(disc) =:= [node()].

are_we_clustered_with(Node) ->
    lists:member(Node, mnesia_lib:all_nodes()).

-spec e(any()) -> no_return().

e(Tag) -> throw({error, {Tag, error_description(Tag)}}).

error_description(clustering_only_disc_node) ->
    "You cannot cluster a node if it is the only disc node in its existing "
        " cluster.";
error_description(resetting_only_disc_node) ->
    "You cannot reset a node when it is the only disc node in a cluster. "
        "Please convert another node of the cluster to a disc node first.";
error_description(not_clustered) ->
    "Non-clustered nodes can only be disc nodes.";
error_description(no_online_cluster_nodes) ->
    "Could not find any online cluster nodes.";
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
    rabbit_misc:format("Mnesia: node ~tp thinks it's clustered "
                       "with node ~tp, but ~tp disagrees",
                       [Thinker, Dissident, Dissident]).
