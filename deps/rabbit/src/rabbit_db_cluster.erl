%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2025 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% TODO
%%
%% Opérations sur le cluster :
%%
%% En commun :
%% 1. On stoppe "rabbit" sur le nœud à ajouter/retirer
%% 2. On lock le registre des feature flags
%%
%% Join :
%% * reset du nœud (cf ci-dessous)
%% * copie des feature flags (+ reset registre)
%% * Mnesia :
%%     * init avec checks
%%   Khepri :
%%     * setup
%%     * préparation
%%     * join
%% * reset registre feature flags (utile ?)
%% * notify_joined_cluster
%%
%% Reset/forget member, depuis un autre nœud que celui retiré :
%% * liste des membres
%% * Mnesia :
%%     * online :
%%     * offline :
%%   Khepri :
%%     * online :
%%     * offline :
%% * amqqueue:forget_all()
%% * quorum queue shrink_all
%% * stream queue delete_all_replicas
%% * stream coordinator forget_node
%% * notify_left_cluster
%% * suppression des fichiers
%% * reset registre feature flags
%% * cluster status
%% * déconnexion du nœud (?)
%%
%% En commun :
%% 1. On unlock le registre des feature flags
%% 2. On redémarre "rabbit"
%%
%% À exécuter depuis quel nœud ?
%% [ ] depuis le nœud qu’ajoute/retire
%% [x] depuis le cluster qui est modifié
%% (doutes : risqué si le nœud à ajouter fait partie d’un autre cluster avant
%% son reset)
%%
%% JOIN (depuis le nœud qu’on ajoute)
%%
%% * est-ce que le nœud fait partie du cluster cible ?
%% * sinon :
%%
%% * stoppe rabbit
%%
%% * reset (cf ci-dessous)
%%
%% * lock registre feature flags
%% * can join ?
%% * copy feature flags + reset registre
%% * (Mnesia/Khepri specific)
%% * notify_joined_cluster
%% * unlock registre feature flags
%%
%% * start rabbit
%%
%% RESET
%%
%% * stoppe rabbit
%%
%% * lock registre feature flags
%% * en cluster ? oui -> forget member locked (cf ci-dessous)
%% * suppression des fichiers
%% * reset registre feature flags
%% * cluster status
%% * unlock registre feature flags
%% * déco des anciens collègues de cluster
%%
%% * start rabbit
%%
%% FORGET MEMBER
%%
%% * stoppe rabbit sur nœud distant
%% * lock registre feature flags
%%
%% * (Mnesia/Khepri specific)
%% * amqqueue:forget_all()
%% * quorum queue shrink_all
%% * stream queue delete_all_replicas
%% * stream coordinator forget_node
%% * notify_left_cluster
%%
%% * unlock registre feature flags
%% * start rabbit sur nœud distant
%% * déco de l’ancien membre
%%
%% (on ne redémarre pas rabbit ; est-ce qu’on arrête la VM ?)

-module(rabbit_db_cluster).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([ensure_feature_flags_are_in_sync/2,
         join/2,
         forget_member/2]).
-export([change_node_type/1]).
-export([is_clustered/0,
         members/0,
         consistent_members/0,
         disc_members/0,
         node_type/0,
         check_compatibility/1,
         check_consistency/0,
         cli_cluster_status/0]).

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
      Error :: {error, {inconsistent_cluster, string()} |
                       {error, {erpc, noconnection}}}.

can_join(RemoteNode) ->
    ?LOG_INFO(
       "DB: checking if `~ts` can join cluster using remote node `~ts`",
       [node(), RemoteNode],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    case rabbit_feature_flags:check_node_compatibility(RemoteNode, true) of
        ok ->
            case rabbit_khepri:is_enabled(RemoteNode) of
                true  -> can_join_using_khepri(RemoteNode);
                false -> can_join_using_mnesia(RemoteNode)
            end;
        Error ->
            Error
    end.

can_join_using_mnesia(RemoteNode) ->
    case rabbit_khepri:is_enabled() of
        true  -> rabbit_node_monitor:prepare_cluster_status_files();
        false -> ok
    end,
    rabbit_mnesia:can_join_cluster(RemoteNode).

can_join_using_khepri(RemoteNode) ->
    rabbit_khepri:can_join_cluster(RemoteNode).

-spec join(RemoteNode, NodeType) -> Ret when
      RemoteNode :: node(),
      NodeType :: node_type(),
      Ret :: Ok | Error,
      Ok :: ok | {ok, already_member},
      Error :: {error, {inconsistent_cluster, string()} |
                       {error, {erpc, noconnection}}}.
%% @doc Adds this node to a cluster using `RemoteNode' to reach it.

join(ThisNode, _NodeType) when ThisNode =:= node() ->
    {error, cannot_cluster_node_with_itself};
join(RemoteNode, NodeType)
  when is_atom(RemoteNode) andalso ?IS_NODE_TYPE(NodeType) ->
    case can_join(RemoteNode) of
        {ok, ClusterNodes} when is_list(ClusterNodes) ->
            %% RabbitMQ and Mnesia must be stopped to modify the cluster. In
            %% particular, we stop Mnesia regardless of the remotely selected
            %% database because we might change it during the join.
            RestartMnesia = rabbit_mnesia:is_running(),
            RestartFFCtl = rabbit_ff_controller:is_running(),
            RestartRaSystems = rabbit_ra_systems:are_running(),
            RestartRabbit = rabbit:is_running(),
            case RestartRabbit of
                true ->
                    rabbit:stop();
                false ->
                    %% The Ra systems were started before we initialize the
                    %% database (because Khepri depends on one of them).
                    %% Therefore, there are files in the data directory. They
                    %% will go away with the reset and we will need to restart
                    %% Ra systems afterwards.
                    case RestartRaSystems of
                        true  -> ok = rabbit_ra_systems:ensure_stopped();
                        false -> ok
                    end,

                    case RestartFFCtl of
                        true ->
                            ok = rabbit_ff_controller:wait_for_task_and_stop();
                        false ->
                            ok
                    end,
                    case RestartMnesia of
                        true  -> rabbit_mnesia:stop_mnesia();
                        false -> ok
                    end
            end,

            %% We acquire the feature flags registry reload lock because
            %% between the time we reset the registry (as part of
            %% `rabbit_db:reset/0' and the states copy from the remote node,
            %% there could be a concurrent reload of the registry (for instance
            %% because of peer discovery on another node) with the
            %% default/empty states.
            %%
            %% To make this work, the lock is also acquired from
            %% `rabbit_ff_registry_wrapper'.
            rabbit_ff_registry_factory:acquire_state_change_lock(),
            try
                ok = rabbit_db:reset(),
                rabbit_feature_flags:copy_feature_states_after_reset(
                  RemoteNode)
            catch
                Class:Reason:Stacktrace ->
                    ?LOG_ERROR(
                       "DB: failed to reset node before adding it to a "
                       "cluster: ~p:~p:~p", [Class, Reason, Stacktrace],
                       #{domain => ?RMQLOG_DOMAIN_DB}),
                    erlang:raise(Class, Reason, Stacktrace)
            after
                rabbit_ff_registry_factory:release_state_change_lock()
            end,

            %% After the regular reset, we also reset Mnesia specifically if
            %% it is meant to be used. That's because we may switch back from
            %% Khepri to Mnesia. To be safe, remove possibly stale files from
            %% a previous instance where Mnesia was used.
            case rabbit_khepri:is_enabled(RemoteNode) of
                true  -> ok;
                false -> ok = rabbit_mnesia:reset_gracefully()
            end,

            ok = rabbit_node_monitor:notify_left_cluster(node()),

            %% Now that the files are all gone after the reset above, restart
            %% the Ra systems. They will recreate their folder in the process.
            case RestartRabbit of
                true ->
                    ok;
                false ->
                    case RestartRaSystems of
                        true ->
                            ok = rabbit_ra_systems:ensure_started(),
                            ok = rabbit_khepri:setup();
                        false ->
                            ok
                    end
            end,

            ?LOG_INFO(
               "DB: joining cluster using remote nodes:~n~tp", [ClusterNodes],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            Ret = case rabbit_khepri:is_enabled(RemoteNode) of
                      true  -> join_using_khepri(ClusterNodes, NodeType);
                      false -> join_using_mnesia(ClusterNodes, NodeType)
                  end,

            case Ret of
                ok ->
                    ok;
                {error, _} ->
                    %% We reset feature flags states again and make sure the
                    %% recorded states on disk are deleted.
                    rabbit_feature_flags:reset()
            end,

            %% Restart RabbitMQ afterwards, if it was running before the join.
            %% Likewise for the Feature flags controller and Mnesia (if we
            %% still need it).
            case RestartRabbit of
                true ->
                    rabbit:start();
                false ->
                    case RestartFFCtl of
                        true ->
                            ok = rabbit_sup:start_child(rabbit_ff_controller);
                        false ->
                            ok
                    end,
                    NeedMnesia = not rabbit_khepri:is_enabled(),
                    case RestartMnesia andalso NeedMnesia of
                        true  -> rabbit_mnesia:start_mnesia(false);
                        false -> ok
                    end
            end,

            case Ret of
                ok ->
                    rabbit_node_monitor:notify_joined_cluster(),
                    ok;
                {error, _} = Error ->
                    Error
            end;
        {ok, already_member} ->
            {ok, already_member};
        {error, {inconsistent_cluster, _Msg}} = Error ->
            case rabbit_khepri:is_enabled() of
                true  ->
                    Error;
                false ->
                    %% rabbit_mnesia:can_join_cluster/1 notice
                    %% inconsistent_cluster, as RemoteNode thinks this node is
                    %% already in the cluster. Attempt to leave the RemoteNode
                    %% cluster, the discovery cluster, and simply retry the
                    %% operation.
                    ?LOG_INFO(
                       "Mnesia: node ~tp thinks it's clustered "
                       "with node ~tp, but ~tp disagrees. ~tp will ask "
                       "to leave the cluster and try again.",
                       [RemoteNode, node(), node(), node()]),
                    ok = rabbit_mnesia:leave_then_rediscover_cluster(
                           RemoteNode),
                    join(RemoteNode, NodeType)
            end;
        {error, _} = Error ->
            Error
    end.

join_using_mnesia(ClusterNodes, NodeType) when is_list(ClusterNodes) ->
    rabbit_mnesia:join_cluster(ClusterNodes, NodeType).

join_using_khepri(ClusterNodes, disc) ->
    rabbit_khepri:add_member(node(), ClusterNodes);
join_using_khepri(_ClusterNodes, ram = NodeType) ->
    {error, {node_type_unsupported, khepri, NodeType}}.

-spec forget_member(Node, RemoveWhenOffline) -> ok when
      Node :: node(),
      RemoveWhenOffline :: boolean().
%% @doc Removes `Node' from the cluster.

forget_member(Node, RemoveWhenOffline) when is_atom(Node) ->
    case rabbit:is_running(Node) of
        false ->
            {ok, InitialState} = lock_cluster_changes(Node),
            try
                forget_member_locked(Node, RemoveWhenOffline)
            after
                unlock_cluster_changes(InitialState)
            end;
        true ->
            {error, {failed_to_remove_node, Node, rabbit_still_running}}
    end.

forget_member_locked(Node, RemoveWhenOffline)
  when is_atom(Node) andalso Node =/= node() ->
    ?LOG_DEBUG(
       "DB: removing cluster member `~ts`", [Node],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    ?assertNot(rabbit:is_running(Node)),
    Ret = case rabbit_khepri:is_enabled() of
              true  -> forget_member_using_khepri(Node, RemoveWhenOffline);
              false -> forget_member_using_mnesia(Node, RemoveWhenOffline)
          end,
    case Ret of
        ok -> post_forget_member_locked(Node, RemoveWhenOffline);
        _  -> ok
    end,
    Ret;
forget_member_locked(Node, RemoveWhenOffline)
  when is_atom(Node) andalso Node =:= node() ->
    OtherNodes = members() -- [Node],
    forget_member_locked_remotely(OtherNodes, Node, RemoveWhenOffline).

forget_member_locked_remotely([OtherNode | Rest], Node, RemoveWhenOffline) ->
    try
        ?LOG_DEBUG(
           "DB: removing cluster member `~ts` (this node); doing it from "
           "remote node `~s`",
           [Node, OtherNode],
           #{domain => ?RMQLOG_DOMAIN_DB}),
        Ret = erpc:call(
                OtherNode,
                ?MODULE, forget_member_locked, [Node, RemoveWhenOffline]),
        case Ret of
            ok  ->
                ok;
            Error ->
                ?LOG_DEBUG(
                   "DB: failed to remove cluster member `~ts` from node "
                   "`~s`: ~0p",
                   [Node, OtherNode, Error],
                   #{domain => ?RMQLOG_DOMAIN_DB}),
                forget_member_locked_remotely(Rest, Node, RemoveWhenOffline)
        end
    catch
        _:Reason ->
            ?LOG_DEBUG(
               "DB: failed to remove cluster member `~ts` from node `~s`: ~0p",
               [Node, OtherNode, Reason],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            forget_member_locked_remotely(Rest, Node, RemoveWhenOffline)
    end;
forget_member_locked_remotely([], _Node, _RemoveWhenOffline) ->
    ok.

forget_member_using_mnesia(Node, RemoveWhenOffline) ->
    rabbit_mnesia:forget_cluster_node(Node, RemoveWhenOffline).

forget_member_using_khepri(_Node, true) ->
    ?LOG_WARNING(
       "Remove node with --offline flag is not supported by Khepri. "
       "Skipping...",
       #{domain => ?RMQLOG_DOMAIN_DB}),
    {error, not_supported};
forget_member_using_khepri(Node, false = _RemoveWhenOffline) ->
    rabbit_khepri:remove_member(Node).

post_forget_member_locked(Node, false = _RemoveWhenOffline) ->
    ?LOG_DEBUG(
       "DB: removing node `~s` from various Ra clusters", [Node],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    _ = rabbit_amqqueue:forget_all(Node),
    _ = rabbit_quorum_queue:shrink_all(Node),
    _ = rabbit_stream_queue:delete_all_replicas(Node),
    _ = rabbit_stream_coordinator:forget_node(Node),
    rabbit_node_monitor:notify_left_cluster(Node),
    ok;
post_forget_member_locked(_Node, true = _RemoveWhenOffline) ->
    ok.

lock_cluster_changes(ChangingNode) ->
    RabbitWasRunning = stop_rabbit_if_running(ChangingNode),
    InitialState = #{changing_node => ChangingNode,
                     rabbit_was_running => RabbitWasRunning},

    %% We acquire the feature flags registry reload lock because between
    %% the time we reset the registry (as part of `rabbit_db:reset/0' and
    %% the states copy from the remote node, there could be a concurrent
    %% reload of the registry (for instance because of peer discovery on
    %% another node) with the default/empty states.
    %%
    %% To make this work, the lock is also acquired from
    %% `rabbit_ff_registry_wrapper'.
    ?LOG_DEBUG(
       "DB: lock feature flags registry to avoid concurrent changes to the "
       "cluster from a feature flag callback",
       #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_ff_registry_factory:acquire_state_change_lock(),
    {ok, InitialState}.

stop_rabbit_if_running(ThisNode) when ThisNode =:= node() ->
    RabbitWasRunning = rabbit:is_running(),
    case RabbitWasRunning of
        true ->
            ?LOG_DEBUG(
               "DB: stop \"rabbit\" on this node (~ts) before making changes "
               "to the cluster",
               [ThisNode],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            ok = rabbit:stop();
        false ->
            ?LOG_DEBUG(
               "DB: \"rabbit\" already stopped on this node (~ts), ready for "
               "changes to the cluster",
               [ThisNode],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            ok
    end,
    RabbitWasRunning;
stop_rabbit_if_running(RemoteNode) when is_atom(RemoteNode) ->
    try
        RabbitWasRunning = erpc:call(RemoteNode, rabbit, is_running, []),
        case RabbitWasRunning of
            true ->
                ?LOG_DEBUG(
                   "DB: stop \"rabbit\" on node `~ts` before making changes "
                   "to the cluster",
                   [RemoteNode],
                   #{domain => ?RMQLOG_DOMAIN_DB}),
                ok = erpc:call(RemoteNode, rabbit, stop, []);
            false ->
                ?LOG_DEBUG(
                   "DB: \"rabbit\" already stopped on node `~ts`, ready for "
                   "changes to the cluster",
                   [RemoteNode],
                   #{domain => ?RMQLOG_DOMAIN_DB}),
                ok
        end,
        RabbitWasRunning
    catch
        error:{erpc, noconnection} ->
            ?LOG_DEBUG(
               "DB: node `~ts` unreachable, considering that \"rabbit\" is "
               "stopped on it, ready for changes to the cluster",
               [RemoteNode],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            false
    end.

unlock_cluster_changes(
  #{changing_node := ChangingNode,
    rabbit_was_running := RabbitWasRunning}) ->
    rabbit_ff_registry_factory:release_state_change_lock(),
    start_rabbit_if_was_running(ChangingNode, RabbitWasRunning),
    ok.

start_rabbit_if_was_running(ChangingNode, false = _RabbitWasRunning) ->
    ?LOG_DEBUG(
       "DB: leaving \"rabbit\" stopped on node `~ts` after changes to the "
       "cluster",
       [ChangingNode],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    ok;
start_rabbit_if_was_running(ThisNode, true = _RabbitWasRunning)
  when ThisNode =:= node() ->
    ?LOG_DEBUG(
       "DB: restart \"rabbit\" on this node (~ts) after changes to the "
       "cluster",
       [ThisNode],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit:start();
start_rabbit_if_was_running(RemoteNode, true = _RabbitWasRunning) ->
    ?LOG_DEBUG(
       "DB: restart \"rabbit\" on node `~ts` after changes to the cluster",
       [RemoteNode],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    erpc:call(RemoteNode, rabbit, start, []).

%% -------------------------------------------------------------------
%% Cluster update.
%% -------------------------------------------------------------------

-spec change_node_type(NodeType) -> ok when
      NodeType :: node_type().
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
    %% This function returns the empty list when it encounters an error
    %% trying to query khepri for it's members. As this function does not
    %% return ok | error this is the only way for callers to detect this.
    %% rabbit_mnesia:members/0 however _will_ still return at least the
    %% current node making it impossible to detect the situation where
    %% the current cluster members may not be correct. It is unlikely we
    %% ever reach that as the mnesia cluster file probably always exists.
    %% For khepri however it is a lot more likely to encounter an error
    %% so we need to allow callers to be more defensive in this case.
    rabbit_khepri:locally_known_nodes().

-spec consistent_members() -> Members when
      Members :: [node()].
%% @doc Returns the list of cluster members.

consistent_members() ->
    case rabbit_khepri:get_feature_state() of
        enabled -> consistent_members_using_khepri();
        _       -> members_using_mnesia()
    end.

consistent_members_using_khepri() ->
    rabbit_khepri:nodes().

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
      NodeType :: node_type().
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
      ClusterStatus :: [{nodes, [{node_type(), [node()]}]} |
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
