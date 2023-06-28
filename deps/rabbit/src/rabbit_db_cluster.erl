%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_cluster).

-include_lib("kernel/include/logger.hrl").
%% -include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([ensure_feature_flags_are_in_sync/2,
         join/2,
         forget_member/2,
         current_status/0,
         current_or_last_status/0]).
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

%% Just for inspection while Mnesia and Khepri are both enabled
%% rabbitmqctl eval 'rabbit_db_cluster:global_cluster_status().' --formatter=pretty_table
-export([global_cluster_status/0]).

-type node_type() :: disc_node_type() | ram_node_type().
-type disc_node_type() :: disc.
-type ram_node_type() :: ram.
-type cluster_status() :: {[node()], [node()], [node()]}.

-export_type([node_type/0,
              disc_node_type/0,
              ram_node_type/0,
              cluster_status/0]).

%% Because it uses rabbit_khepri:cluster_status_from_khepri/0,
%% that works on a too restrictive `sys:get_statys/1` spec
-dialyzer({no_match, global_cluster_status/0}).

-define(
   IS_NODE_TYPE(NodeType),
   ((NodeType) =:= disc orelse (NodeType) =:= ram)).

%% -------------------------------------------------------------------
%% Cluster formation.
%% -------------------------------------------------------------------

ensure_feature_flags_are_in_sync(Nodes, NodeIsVirgin) when is_list(Nodes) ->
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
            LocalKhepriUse = rabbit_khepri:use_khepri(),
            RemoteKhepriUse = rabbit_khepri:use_khepri(RemoteNode),
            case LocalKhepriUse orelse RemoteKhepriUse of
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
            rabbit_db:reset(),

            ?LOG_INFO(
               "DB: joining cluster using remote nodes:~n~tp", [ClusterNodes],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            Ret = rabbit_db:run(
                    #{mnesia =>
                      fun() -> join_using_mnesia(ClusterNodes, NodeType) end,
                      khepri =>
                      fun() -> join_using_khepri(ClusterNodes, NodeType) end
                     }),
            case Ret of
                {ok, Status} ->
                    rabbit_node_monitor:write_cluster_status(Status),
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
    rabbit_mnesia:join_cluster(ClusterNodes, NodeType).

join_using_khepri(_ClusterNodes, ram) ->
    ?LOG_WARNING(
       "Join node with --ram flag is not supported by Khepri. Skipping...",
       #{domain => ?RMQLOG_DOMAIN_DB}),
    {error, not_supported};
join_using_khepri([RemoteNode | _], _NodeType) ->
    case rabbit_khepri:add_member(node(), RemoteNode) of
        ok ->
            Members = rabbit_khepri:members(),
            {ok, {Members, Members, Members}};
        {error, _} = Error ->
            Error
    end.

-spec forget_member(Node, RemoveWhenOffline) -> ok when
      Node :: node(),
      RemoveWhenOffline :: boolean().
%% @doc Removes `Node' from the cluster.

forget_member(Node, RemoveWhenOffline) ->
    ?LOG_DEBUG(
      "DB: removing cluster member `~ts`", [Node],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_db:run(
      #{mnesia => fun() -> forget_member_using_mnesia(Node, RemoveWhenOffline) end,
        khepri => fun() -> forget_member_using_khepri(Node, RemoveWhenOffline) end
       }).

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

-spec current_status() -> Ret when
      Ret :: {ok, Status} | {error, Reason},
      Status :: rabbit_db_cluster:cluster_status(),
      Reason :: any().

current_status() ->
    rabbit_db:run(
      #{mnesia => fun() -> current_status_using_mnesia() end,
        khepri => fun() -> current_status_using_khepri() end
       }).

current_status_using_mnesia() ->
    rabbit_mnesia:cluster_status_from_mnesia().

current_status_using_khepri() ->
    rabbit_khepri:cluster_status_from_khepri().

-spec current_or_last_status() -> Status when
      Status :: rabbit_db_cluster:cluster_status().

current_or_last_status() ->
    case current_status() of
        {ok, Status} ->
            Status;
        {error, _} ->
            {AllNodes, DiscNodes, RunningNodes0} =
            rabbit_node_monitor:read_cluster_status(),

            %% The cluster status file records the status when the node is
            %% online, but we know for sure that the node is offline now, so
            %% we can remove it from the list of running nodes.
            RunningNodes = rabbit_nodes:nodes_excl_me(RunningNodes0),
            {AllNodes, DiscNodes, RunningNodes}
    end.

%% -------------------------------------------------------------------
%% Cluster update.
%% -------------------------------------------------------------------

-spec change_node_type(NodeType) -> ok when
      NodeType :: rabbit_db_cluster:node_type().
%% @doc Changes the node type to `NodeType'.
%%
%% Node types may not all be valid with all databases.

change_node_type(NodeType) ->
    rabbit_db:run(
      #{mnesia => fun() -> change_node_type_using_mnesia(NodeType) end,
        khepri => fun() -> change_node_type_using_khepri(NodeType) end
       }).

change_node_type_using_mnesia(NodeType) ->
    rabbit_mnesia:change_cluster_node_type(NodeType).

change_node_type_using_khepri(disc) ->
    rabbit_mnesia:change_cluster_node_type(disc);
change_node_type_using_khepri(ram) ->
    ?LOG_WARNING(
       "Change cluster node type to ram is not supported by Khepri. "
       "Only disc nodes are allowed. Skipping...",
       #{domain => ?RMQLOG_DOMAIN_DB}),
    {error, not_supported}.

%% -------------------------------------------------------------------
%% Cluster status.
%% -------------------------------------------------------------------

-spec is_clustered() -> IsClustered when
      IsClustered :: boolean().
%% @doc Indicates if this node is clustered with other nodes or not.

is_clustered() ->
    rabbit_db:run(
      #{mnesia => fun() -> is_clustered_using_mnesia() end,
        khepri => fun() -> is_clustered_using_khepri() end
       }).

is_clustered_using_mnesia() ->
    rabbit_mnesia:is_clustered().

is_clustered_using_khepri() ->
    rabbit_khepri:is_clustered().

-spec members() -> Members when
      Members :: [node()].
%% @doc Returns the list of cluster members.

members() ->
    Members = rabbit_db:run(
                #{mnesia => fun() -> members_using_mnesia() end,
                  khepri => fun() -> members_using_khepri() end
                 }),
    %% ?assert(lists:member(node(), Members)),
    Members.

members_using_mnesia() ->
    case rabbit_mnesia:is_running() andalso rabbit_table:is_present() of
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

members_using_khepri() ->
    case rabbit_khepri:nodes() of
        []      ->
            case rabbit_khepri:locally_known_nodes() of
                [] -> [node()];
                Members -> Members
            end;
        Members -> Members
    end.

-spec disc_members() -> Members when
      Members :: [node()].
%% @private

disc_members() ->
    rabbit_db:run(
      #{mnesia => fun() -> disc_members_using_mnesia() end,
        khepri => fun() -> members_using_khepri() end
       }).

disc_members_using_mnesia() ->
    rabbit_mnesia:cluster_nodes(disc).

-spec node_type() -> NodeType when
      NodeType :: rabbit_db_cluster:node_type().
%% @doc Returns the type of this node, `disc' or `ram'.
%%
%% Node types may not all be relevant with all databases.

node_type() ->
    rabbit_db:run(
      #{mnesia => fun() -> node_type_using_mnesia() end,
        khepri => fun() -> node_type_using_khepri() end
       }).

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
            rabbit_db:run(
              #{mnesia =>
                fun() -> check_compatibility_using_mnesia(RemoteNode) end,
                khepri =>
                fun() -> ok end
               });
        Error ->
            Error
    end.

check_compatibility_using_mnesia(RemoteNode) ->
    rabbit_mnesia:check_mnesia_consistency(RemoteNode).

-spec check_consistency() -> ok.
%% @doc Ensures the cluster is consistent.

check_consistency() ->
    rabbit_db:run(
      #{mnesia => fun() -> check_consistency_using_mnesia() end,
        khepri => fun() -> check_consistency_using_khepri() end
       }).

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
    rabbit_db:run(
      #{mnesia => fun() -> cli_cluster_status_using_mnesia() end,
        khepri => fun() -> cli_cluster_status_using_khepri() end
       }).

cli_cluster_status_using_mnesia() ->
    rabbit_mnesia:status().

cli_cluster_status_using_khepri() ->
    rabbit_khepri:cli_cluster_status().

global_cluster_status() ->
    {Status, All, Running} =
        case rabbit_mnesia:cluster_status_from_mnesia() of
            {ok, {A, _D, R}} ->
                {to_string(running), A, R};
            {error, Reason} ->
                {to_string(Reason), [], []}
        end,
    {StatusK, AllK, RunningK} =
        case rabbit_khepri:cluster_status_from_khepri() of
            {ok, {AK, RK}} ->
                {to_string(running), AK, RK};
            {error, ReasonK} ->
                {to_string(ReasonK), [], []}
        end,
    [[{<<"Metadata store">>, <<"mnesia">>},
      {<<"Status">>, Status},
      {<<"Nodes">>, to_string(All)},
      {<<"Running nodes">>, to_string(Running)}],
     [{<<"Metadata store">>, <<"khepri">>},
      {<<"Status">>, StatusK},
      {<<"Nodes">>, to_string(AllK)},
      {<<"Running nodes">>, to_string(RunningK)}]].

to_string(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
to_string(AtomList) ->
    string:join([atom_to_list(A) || A <- lists:sort(AtomList)], ", ").

rename(Node, NodeMapList) ->
    rabbit_db:run(
      #{mnesia => fun() -> rabbit_mnesia_rename:rename(Node, NodeMapList) end,
        khepri => fun() -> {error, not_supported} end
       }).

update_cluster_nodes(DiscoveryNode) ->
    rabbit_db:run(
      #{mnesia => fun() -> rabbit_mnesia:update_cluster_nodes(DiscoveryNode) end,
        khepri => fun() -> {error, not_supported} end
       }).
