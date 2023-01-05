%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_cluster).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([ensure_feature_flags_are_in_sync/2,
         join/2,
         forget_member/2]).
-export([change_node_type/1]).
-export([is_clustered/0,
         members/0,
         disc_members/0,
         node_type/0,
         check_compatibility/1,
         check_consistency/0,
         cli_cluster_status/0]).

%% These two functions are not supported by Khepri and probably
%% shouldn't be part of this API in the future, but currently
%% they're needed here so they can fail when invoked using Khepri.
-export([rename/2,
         update_cluster_nodes/1]).

-type node_type() :: disc_node_type() | ram_node_type().
-type disc_node_type() :: disc.
-type ram_node_type() :: ram.

-export_type([node_type/0, disc_node_type/0, ram_node_type/0]).

-define(
   IS_NODE_TYPE(NodeType),
   ((NodeType) =:= disc orelse (NodeType) =:= ram)).

%% -------------------------------------------------------------------
%% Cluster formation.
%% -------------------------------------------------------------------

ensure_feature_flags_are_in_sync(Nodes, NodeIsVirgin) ->
    Ret = rabbit_feature_flags:sync_feature_flags_with_cluster(
            Nodes, NodeIsVirgin),
    case Ret of
        ok              -> ok;
        {error, Reason} -> throw({error, {incompatible_feature_flags, Reason}})
    end.

-spec can_join(RemoteNode) -> Ret when
      RemoteNode :: node(),
      Ret :: Ok | Error,
      Ok :: {ok, [node()]} | {ok, already_member},
      Error :: {error, {inconsistent_cluster, string()}}.

can_join(RemoteNode) ->
    ?LOG_INFO(
       "DB: checking if `~ts` can join cluster using remote node `~ts`",
       [node(), RemoteNode],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    case rabbit_feature_flags:check_node_compatibility(RemoteNode) of
        ok ->
            case rabbit_khepri:is_enabled(RemoteNode) of
                true  -> can_join_using_khepri(RemoteNode);
                false -> can_join_using_mnesia(RemoteNode)
            end;
        Error ->
            Error
    end.

can_join_using_mnesia(RemoteNode) ->
    rabbit_mnesia:can_join_cluster(RemoteNode).

can_join_using_khepri(RemoteNode) ->
    rabbit_khepri:can_join_cluster(RemoteNode).

-spec join(RemoteNode, NodeType) -> Ret when
      RemoteNode :: node(),
      NodeType :: rabbit_db_cluster:node_type(),
      Ret :: Ok | Error,
      Ok :: ok | {ok, already_member},
      Error :: {error, {inconsistent_cluster, string()}}.
%% @doc Adds this node to a cluster using `RemoteNode' to reach it.

join(RemoteNode, NodeType)
  when is_atom(RemoteNode) andalso ?IS_NODE_TYPE(NodeType) ->
    case can_join(RemoteNode) of
        {ok, ClusterNodes} when is_list(ClusterNodes) ->
            ok = rabbit_db:reset(),

            ?LOG_INFO(
               "DB: joining cluster using remote nodes:~n~tp", [ClusterNodes],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            Ret = case rabbit_khepri:is_enabled(RemoteNode) of
                      true  -> join_using_khepri(ClusterNodes, NodeType);
                      false -> join_using_mnesia(ClusterNodes, NodeType)
                  end,
            case Ret of
                ok ->
                    rabbit_feature_flags:copy_feature_states_after_reset(
                      RemoteNode),
                    rabbit_node_monitor:notify_joined_cluster(),
                    ok;
                {error, _} = Error ->
                    Error
            end;
        {ok, already_member} ->
            {ok, already_member};
        {error, _} = Error ->
            Error
    end.

join_using_mnesia(ClusterNodes, NodeType) when is_list(ClusterNodes) ->
    ok = rabbit_mnesia:reset_gracefully(),
    rabbit_mnesia:join_cluster(ClusterNodes, NodeType).

join_using_khepri(ClusterNodes, disc) ->
    rabbit_khepri:add_member(node(), ClusterNodes);
join_using_khepri(_ClusterNodes, ram = NodeType) ->
    {error, {node_type_unsupported, khepri, NodeType}}.

-spec forget_member(Node, RemoveWhenOffline) -> ok when
      Node :: node(),
      RemoveWhenOffline :: boolean().
%% @doc Removes `Node' from the cluster.

forget_member(Node, RemoveWhenOffline) ->
    case rabbit:is_running(Node) of
        false ->
            ?LOG_DEBUG(
               "DB: removing cluster member `~ts`", [Node],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            case rabbit_khepri:is_enabled() of
                true  -> forget_member_using_khepri(Node, RemoveWhenOffline);
                false -> forget_member_using_mnesia(Node, RemoveWhenOffline)
            end;
        true ->
            {error, {failed_to_remove_node, Node, rabbit_still_running}}
    end.

forget_member_using_mnesia(Node, RemoveWhenOffline) ->
    rabbit_mnesia:forget_cluster_node(Node, RemoveWhenOffline).

forget_member_using_khepri(_Node, true) ->
    ?LOG_WARNING(
       "Remove node with --offline flag is not supported by Khepri. "
       "Skipping...",
       #{domain => ?RMQLOG_DOMAIN_DB}),
    {error, not_supported};
forget_member_using_khepri(Node, false = _RemoveWhenOffline) ->
    rabbit_khepri:leave_cluster(Node).

%% -------------------------------------------------------------------
%% Cluster update.
%% -------------------------------------------------------------------

-spec change_node_type(NodeType) -> ok when
      NodeType :: rabbit_db_cluster:node_type().
%% @doc Changes the node type to `NodeType'.
%%
%% Node types may not all be valid with all databases.

change_node_type(NodeType) ->
    rabbit_mnesia:ensure_node_type_is_permitted(NodeType),
    case rabbit_khepri:is_enabled() of
        true  -> ok;
        false -> change_node_type_using_mnesia(NodeType)
    end.

change_node_type_using_mnesia(NodeType) ->
    rabbit_mnesia:change_cluster_node_type(NodeType).

%% -------------------------------------------------------------------
%% Cluster status.
%% -------------------------------------------------------------------

-spec is_clustered() -> IsClustered when
      IsClustered :: boolean().
%% @doc Indicates if this node is clustered with other nodes or not.

is_clustered() ->
    Members = members(),
    Members =/= [] andalso Members =/= [node()].

-spec members() -> Members when
      Members :: [node()].
%% @doc Returns the list of cluster members.

members() ->
    case rabbit_khepri:get_feature_state() of
        enabled -> members_using_khepri();
        _       -> members_using_mnesia()
    end.

members_using_mnesia() ->
    rabbit_mnesia:members().

members_using_khepri() ->
    case rabbit_khepri:locally_known_nodes() of
        []      -> [node()];
        Members -> Members
    end.

-spec disc_members() -> Members when
      Members :: [node()].
%% @private

disc_members() ->
    case rabbit_khepri:get_feature_state() of
        enabled -> members_using_khepri();
        _       -> disc_members_using_mnesia()
    end.

disc_members_using_mnesia() ->
    rabbit_mnesia:cluster_nodes(disc).

-spec node_type() -> NodeType when
      NodeType :: rabbit_db_cluster:node_type().
%% @doc Returns the type of this node, `disc' or `ram'.
%%
%% Node types may not all be relevant with all databases.

node_type() ->
    case rabbit_khepri:get_feature_state() of
        enabled -> node_type_using_khepri();
        _       -> node_type_using_mnesia()
    end.

node_type_using_mnesia() ->
    rabbit_mnesia:node_type().

node_type_using_khepri() ->
    disc.

-spec check_compatibility(RemoteNode) -> ok | {error, Reason} when
      RemoteNode :: node(),
      Reason :: any().
%% @doc Ensures the given remote node is compatible with the node calling this
%% function.

check_compatibility(RemoteNode) ->
    case rabbit_feature_flags:check_node_compatibility(RemoteNode) of
        ok ->
            case rabbit_khepri:get_feature_state() of
                enabled -> ok;
                _       -> check_compatibility_using_mnesia(RemoteNode)
            end;
        Error ->
            Error
    end.

check_compatibility_using_mnesia(RemoteNode) ->
    rabbit_mnesia:check_mnesia_consistency(RemoteNode).

-spec check_consistency() -> ok.
%% @doc Ensures the cluster is consistent.

check_consistency() ->
    case rabbit_khepri:get_feature_state() of
        enabled -> check_consistency_using_khepri();
        _       -> check_consistency_using_mnesia()
    end.

check_consistency_using_mnesia() ->
    rabbit_mnesia:check_cluster_consistency().

check_consistency_using_khepri() ->
    rabbit_khepri:check_cluster_consistency().

-spec cli_cluster_status() -> ClusterStatus when
      ClusterStatus :: [{nodes, [{rabbit_db_cluster:node_type(), [node()]}]} |
                        {running_nodes, [node()]} |
                        {partitions, [{node(), [node()]}]}].
%% @doc Returns information from the cluster for the `cluster_status' CLI
%% command.

cli_cluster_status() ->
    case rabbit_khepri:is_enabled() of
        true  -> cli_cluster_status_using_khepri();
        false -> cli_cluster_status_using_mnesia()
    end.

cli_cluster_status_using_mnesia() ->
    rabbit_mnesia:status().

cli_cluster_status_using_khepri() ->
    rabbit_khepri:cli_cluster_status().

rename(Node, NodeMapList) ->
    case rabbit_khepri:is_enabled() of
        true  -> {error, not_supported};
        false -> rabbit_mnesia_rename:rename(Node, NodeMapList)
    end.

update_cluster_nodes(DiscoveryNode) ->
    case rabbit_khepri:is_enabled() of
        true  -> {error, not_supported};
        false -> rabbit_mnesia:update_cluster_nodes(DiscoveryNode)
    end.
