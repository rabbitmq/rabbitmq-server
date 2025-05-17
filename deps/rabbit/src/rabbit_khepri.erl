%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2025 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% @doc Khepri database uses wrapper.
%%
%% This module has three purposes:
%%
%% <ol>
%% <li>It provides a wrapper API on top of the regular Khepri API. The main
%% goal of this wrapper is to make sure the correct store identifier is being
%% used.</li>
%% <li>It is responsible for managing the Khepri database and clustering.</li>
%% <li>It provides functions to help with the transition from Mnesia to
%% Khepri.</li>
%% </ol>
%%
%% == Khepri API wrapper ==
%%
%% Most Khepri regular functions are wrapped by this module, but not all of
%% them. The reason is that the missing functions were not used so far. Feel
%% free to add another wrapper when the need arises.
%%
%% See <a href="https://rabbitmq.github.io/khepri/">Khepri's documentation</a>
%% to learn how to use its API.
%%
%%
%% == Transition from Mnesia to Khepri ==
%%
%% Until Mnesia code is removed, RabbitMQ should support both databases and
%% allow to migrate data from Mnesia to Khepri at runtime. The `khepri_db'
%% feature flag, its associated callback functions and the
%% `khepri_mnesia_migration' application take care of the one-time migration.
%%
%% To make database reads and writes work before, during and after the
%% migration, one can use the following functions:
%% <ul>
%% <li>{@link is_enabled/0}, {@link is_enabled/1}</li>
%% <li>{@link get_feature_state/0}, {@link get_feature_state/1}</li>
%% <li>{@link handle_fallback/1}</li>
%% </ul>
%%
%% {@link is_enabled/0} and {@link is_enabled/1} query the state of the
%% `khepri_db' feature flag state and return `true' if Khepri is the active
%% database or `false' if Mnesia is the active one. Furthermore, it will block
%% during the migration.
%%
%% {@link get_feature_state/0} and {@link get_feature_state/1} query the same
%% feature flag state. However, they do not block during the migration and
%% return `enabled' if Khepri is active, `disabled' if Mnesia is active, or
%% `state_changing' if RabbitMQ is between these two states.
%%
%% Finally {@link handle_fallback/1}, is a helper that takes two anonymous
%% functions: one for Mnesia and one for Khepri. If Khepri is already enabled,
%% its associated anonymous function is executed. Otherwise, the Mnesia one is
%% executed. If the migration runs concurrently, whether it started before or
%% during the execution of the Mnesia-specific anonymous function, {@link
%% handle_fallback/1} will watch for "no exists" table exceptions from Mnesia
%% and will retry the Mnesia functino or run the Khepri function accordingly.
%% The Mnesia function must be idempotent because it can be executed multiple
%% times.
%%
%% Which function to use then?
%%
%% If you want to read from or write to one or more Mnesia tables or the
%% Khepri store, you should use {@link handle_fallback/1}:
%% <pre>
%% rabbit_khepri:handle_fallback(
%%   #{mnesia => fun() -> do_something_with_mnesia_tables() end,
%%     khepri => fun() -> do_something_with_khepri_store() end).
%% </pre>
%%
%% However, if you call into Mnesia but that doesn't involve reading or
%% writing to tables (e.g. querying the cluster status), you need to use
%% {@link is_enabled/0} or {@link get_feature_state/0}, depending on whether
%% you want to block or not. Most of the time, you want the call to block to
%% not have to deal with the intermediate state. For example:
%% <pre>
%% case rabbit_khepri:is_enabled() of
%%     true  -> do_something_with_khepri();
%%     false -> do_something_with_mnesia()
%% end.
%% </pre>

-module(rabbit_khepri).

-feature(maybe_expr, enable).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("include/rabbit_khepri.hrl").

%% Initialisation.
-export([setup/0,
         setup/1,
         init/1,
         reset/0,

         dir/0,
         get_ra_cluster_name/0,
         get_store_id/0,
         root_path/0,

         info/0]).

%% Clustering.
-export([can_join_cluster/1,
         add_member/2,
         do_join/1, %% Internal RPC from this module.
         remove_member/1,

         members/0,
         locally_known_members/0,
         nodes/0,
         locally_known_nodes/0,

         check_cluster_consistency/0,
         node_info/0, %% Internal RPC from this module.
         cluster_status_from_khepri/0,
         transfer_leadership/1]).

%% CLI command support.
-export([force_shrink_member_to_current_member/0,
         status/0,
         cli_cluster_status/0]).

%% "Proxy" functions to Khepri query/update API.
-export([is_empty/0,

         get/1, get/2,
         adv_get/1, adv_get/2,
         get_many/1, get_many/2,
         adv_get_many/1, adv_get_many/2,
         exists/1, exists/2,
         count/1, count/2,
         fold/3, fold/4,
         foreach/2, foreach/3,
         map/2, map/3,
         filter/2, filter/3,

         put/2, put/3,
         adv_put/2, adv_put/3,
         create/2, create/3,
         adv_create/2, adv_create/3,
         update/2, update/3,
         adv_update/2, adv_update/3,

         delete/1, delete/2,
         adv_delete/1, adv_delete/2,
         clear_payload/1, clear_payload/2,

         transaction/1, transaction/2, transaction/3,

         fence/1,

         handle_async_ret/1,

         delete_or_fail/1]).

%% Used during migration to join the standalone Khepri nodes and form the
%% equivalent cluster
-export([khepri_db_migration_enable/1,
         khepri_db_migration_post_enable/1,
         is_enabled/0, is_enabled/1,
         get_feature_state/0, get_feature_state/1,
         handle_fallback/1]).

-ifdef(TEST).
-export([register_projections/0,
         force_metadata_store/1,
         clear_forced_metadata_store/0]).
-endif.

-type timeout_error() :: khepri:error(timeout).
%% Commands like 'put'/'delete' etc. might time out in Khepri. It might take
%% the leader longer to apply the command and reply to the caller than the
%% configured timeout. This error is easy to reproduce - a cluster which is
%% only running a minority of nodes will consistently return `{error, timeout}`
%% for commands until the cluster majority can be re-established. Commands
%% returning `{error, timeout}` are a likely (but not certain) indicator that
%% the node which submitted the command is running in a minority.

-export_type([timeout_error/0]).

-compile({no_auto_import, [get/1, get/2, nodes/0]}).

-define(RA_SYSTEM, coordination).
-define(RA_CLUSTER_NAME, rabbitmq_metadata).
-define(RA_FRIENDLY_NAME, "RabbitMQ metadata store").
-define(STORE_ID, ?RA_CLUSTER_NAME).
-define(MIGRATION_ID, <<"rabbitmq_metadata">>).

%% By default we should try to reply from the cluster member that makes a
%% request to change the store. Projections are immediately consistent on the
%% node that issues the reply effect and eventually consistent everywhere else.
%% There isn't a performance penalty for replying from the local node and if
%% the local node isn't a part of the cluster, the reply will come from the
%% leader instead.
-define(DEFAULT_COMMAND_OPTIONS, #{reply_from => local}).

%% Mnesia tables to migrate and cleanup.
%%
%% This table order is important. For instance, user permissions depend on
%% both vhosts and users to exist in the metadata store.
%%
%% Channel and connection tracking are core features with difference: tables
%% cannot be predeclared as they include the node name

-rabbit_mnesia_tables_to_khepri_db(
   [
    {rabbit_vhost, rabbit_db_vhost_m2k_converter},
    {rabbit_user, rabbit_db_user_m2k_converter},
    {rabbit_user_permission, rabbit_db_user_m2k_converter},
    {rabbit_topic_permission, rabbit_db_user_m2k_converter},
    {rabbit_runtime_parameters, rabbit_db_rtparams_m2k_converter},
    {rabbit_queue, rabbit_db_queue_m2k_converter},
    {rabbit_exchange, rabbit_db_exchange_m2k_converter},
    {rabbit_exchange_serial, rabbit_db_exchange_m2k_converter},
    {rabbit_route, rabbit_db_binding_m2k_converter},
    {rabbit_node_maintenance_states, rabbit_db_maintenance_m2k_converter},
    {mirrored_sup_childspec, rabbit_db_msup_m2k_converter},

    rabbit_durable_queue,
    rabbit_durable_exchange,
    rabbit_durable_route,
    rabbit_semi_durable_route,
    rabbit_reverse_route,
    rabbit_index_route
   ]).

%% -------------------------------------------------------------------
%% Khepri integration initialisation.
%% -------------------------------------------------------------------

-spec setup() -> ok | no_return().
%% @doc Starts the local Khepri store.
%%
%% @see setup/1.

setup() ->
    setup(rabbit_prelaunch:get_context()).

-spec setup(Context) -> ok | no_return() when
      Context :: map().
%% @doc Starts the local Khepri store.
%%
%% Before starting the Khepri store, it ensures that the underlying Ra system
%% we want to use is also running.
%%
%% This function is idempotent whether the Khepri store is started for the
%% first time or it is restarted.
%%
%% This function blocks until a leader is elected.
%%
%% The Khepri application must be running.
%%
%% If it fails to start the Khepri store or if it reaches a timeout waiting for
%% a leader, this function exits.

setup(_Context) ->
    ?LOG_DEBUG("Starting Khepri-based " ?RA_FRIENDLY_NAME),
    ok = ensure_ra_system_started(),
    Timeout = application:get_env(rabbit, khepri_default_timeout, 30000),
    ok = application:set_env(
           [{khepri, [{default_timeout, Timeout},
                      {default_store_id, ?STORE_ID},
                      {default_ra_system, ?RA_SYSTEM}]}],
           [{persistent, true}]),
    RaServerConfig = #{cluster_name => ?RA_CLUSTER_NAME,
                       metrics_labels => #{ra_system => ?RA_SYSTEM, module => ?MODULE},
                       friendly_name => ?RA_FRIENDLY_NAME},
    case khepri:start(?RA_SYSTEM, RaServerConfig) of
        {ok, ?STORE_ID} ->
            RetryTimeout = retry_timeout(),
            case khepri_cluster:wait_for_leader(?STORE_ID, RetryTimeout) of
                ok ->
                    ?LOG_DEBUG(
                       "Khepri-based " ?RA_FRIENDLY_NAME " ready",
                       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                    ok;
                {error, timeout} ->
                    exit(timeout_waiting_for_leader);
                {error, _} = Error ->
                    exit(Error)
            end;
        {error, _} = Error ->
            exit(Error)
    end.

ensure_ra_system_started() ->
    {ok, _} = application:ensure_all_started(khepri),
    ok = rabbit_ra_systems:ensure_ra_system_started(?RA_SYSTEM).

retry_timeout() ->
    case application:get_env(rabbit, khepri_leader_wait_retry_timeout) of
        {ok, T} when is_integer(T) andalso T >= 0 -> T;
        undefined                                 -> 300_000
    end.

-spec init(IsVirgin) -> Ret when
      IsVirgin :: boolean(),
      Ret :: ok | timeout_error().
%% @doc Ensures the store has caught up with the cluster.
%%
%% In addition to making sure the local Khepri store is on the same page as the
%% leader, it initialises the Khepri projections if this node is virgin.
%%
%% Finally, it requests the deletion of transient queues on this node.

init(IsVirgin) ->
    case members() of
        [] ->
            timer:sleep(1000),
            init(IsVirgin);
        Members ->
            ?LOG_NOTICE(
               "Found the following metadata store members: ~p", [Members],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            maybe
                ok ?= await_replication(),
                ?LOG_DEBUG(
                   "local Khepri-based " ?RA_FRIENDLY_NAME " member is caught "
                   "up to the Raft cluster leader", [],
                   #{domain => ?RMQLOG_DOMAIN_DB}),
                ok ?= case IsVirgin of
                          true  -> register_projections();
                          false -> ok
                      end,
                %% Delete transient queues on init.
                %% Note that we also do this in the
                %% `rabbit_amqqueue:on_node_down/1' callback. We must try this
                %% deletion during init because the cluster may have been in a
                %% minority when this node went down. We wait for a majority
                %% while registering projections above though so this deletion
                %% is likely to succeed.
                rabbit_amqqueue:delete_transient_queues_on_node(node())
            end
    end.

await_replication() ->
    Timeout = retry_timeout(),
    ?LOG_DEBUG(
       "Khepri-based " ?RA_FRIENDLY_NAME " waiting to catch up on replication "
       "to the Raft cluster leader. Waiting for ~tb ms",
       [Timeout],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    fence(Timeout).

-spec reset() -> ok | no_return().
%% @doc Reset and stops the local Khepri store.
%%
%% This function first ensures that the local Khepri store is running.
%%
%% Then it resets the store. This includes removing it from its cluster if
%% any, and deleting all tree nodes.
%%
%% Finally, it stops the store and deteles files on disk.
%%
%% The Khepri application is left running.
%%
%% RabbitMQ must be stopped on this Erlang node. This functions throws an
%% exception if it is called while RabbitMQ is still running.
%%
%% @private

reset() ->
    case rabbit:is_running() of
        false ->
            %% Rabbit should be stopped, but Khepri needs to be running.
            %% Restart it.
            ok = setup(),
            ok = khepri_cluster:reset(?RA_CLUSTER_NAME),
            ok = khepri:stop(?RA_CLUSTER_NAME),

            _ = file:delete(rabbit_guid:filename()),
            ok;
        true ->
            throw({error, rabbitmq_unexpectedly_running})
    end.

-spec dir() -> Dir when
      Dir :: file:filename_all().
%% @doc Returns the Khepri store directory.
%%
%% This corresponds to the underlying Ra system's directory.

dir() ->
    DataDir = rabbit_mnesia:dir(),
    StoreDir = filename:join(DataDir, atom_to_list(?STORE_ID)),
    StoreDir.

-spec get_ra_cluster_name() -> RaClusterName when
      RaClusterName :: ra:cluster_name().
%% @doc Returns the Ra cluster name.

get_ra_cluster_name() ->
    ?RA_CLUSTER_NAME.

-spec get_store_id() -> StoreId when
      StoreId :: khepri:store_id().
%% @doc Returns the Khepri store identifier.

get_store_id() ->
    ?STORE_ID.

-spec root_path() -> RootPath when
      RootPath :: khepri_path:path().
%% @doc Returns the path where RabbitMQ stores every metadata.
%%
%% This path must be prepended to all paths used by RabbitMQ subsystems.

root_path() ->
    ?RABBITMQ_KHEPRI_ROOT_PATH.

info() ->
    ok = setup(),
    khepri:info(?STORE_ID).

%% -------------------------------------------------------------------
%% Clustering.
%% -------------------------------------------------------------------

-spec can_join_cluster(DiscoveryNode) -> Ret when
      DiscoveryNode :: node(),
      Ret :: {ok, ClusterNodes} | {error, any()},
      ClusterNodes :: [node()].
%% @doc Indicates if this node can join `DiscoveryNode' cluster.
%%
%% At the level of Khepri, it is always possible to join a remote cluster for
%% now. Therefore this function only queries the list of members in
%% `DiscoveryNode' cluster and returns it.
%%
%% @returns an `ok' tuple with the list of members in `DiscoveryNode' cluster,
%% or an error tuple.
%%
%% @private

can_join_cluster(DiscoveryNode) when is_atom(DiscoveryNode) ->
    ThisNode = node(),
    try
        ClusterNodes0 = erpc:call(
                          DiscoveryNode,
                          ?MODULE, locally_known_nodes, []),
        ClusterNodes1 = ClusterNodes0 -- [ThisNode],
        {ok, ClusterNodes1}
    catch
        _:Reason ->
            {error, Reason}
    end.

-spec add_member(JoiningNode, JoinedNode | JoinedCluster) -> Ret when
      JoiningNode :: node(),
      JoinedNode :: node(),
      JoinedCluster :: [node()],
      Ret :: ok | {error, any()}.
%% @doc Adds `JoiningNode' to `JoinedNode''s cluster.
%%
%% If a list of nodes is passed as `JoinedCluster', this function will pick
%% this node if it is part of the list and the Khepri store is running, or the
%% first node in the list that runs the Khepri store.
%%
%% The actual join code runs on the node that wants to join a cluster.
%% Therefore, if `JoiningNode' is this node, the code runs locally. Otherwise,
%% this function does an RPC call to execute the remote function.
%%
%% @private

add_member(JoiningNode, JoinedNode)
  when JoiningNode =:= node() andalso is_atom(JoinedNode) ->
    Ret = do_join(JoinedNode),
    post_add_member(JoiningNode, JoinedNode, Ret);
add_member(JoiningNode, JoinedNode) when is_atom(JoinedNode) ->
    Ret = rabbit_misc:rpc_call(
            JoiningNode, ?MODULE, do_join, [JoinedNode]),
    post_add_member(JoiningNode, JoinedNode, Ret);
add_member(JoiningNode, [_ | _] = Cluster) ->
    case pick_node_in_cluster(Cluster) of
        {ok, JoinedNode} ->
            ?LOG_INFO(
               "Khepri clustering: Attempt to add node ~p to cluster ~0p "
               "through node ~p",
               [JoiningNode, Cluster, JoinedNode],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            %% Recurse with a single node taken in the `Cluster' list.
            add_member(JoiningNode, JoinedNode);
        {error, _} = Error ->
            Error
    end.

pick_node_in_cluster([_ | _] = Cluster) ->
    RunningNodes = lists:filter(
                     fun(Node) ->
                             try
                                 erpc:call(
                                   Node,
                                   khepri_cluster, is_store_running,
                                   [?STORE_ID])
                             catch
                                 _:_ ->
                                     false
                             end
                     end, Cluster),
    case RunningNodes of
        [_ | _] ->
            ThisNode = node(),
            SelectedNode = case lists:member(ThisNode, RunningNodes) of
                               true  -> ThisNode;
                               false -> hd(RunningNodes)
                           end,
            {ok, SelectedNode};
        [] ->
            {error, {no_nodes_to_cluster_with, Cluster}}
    end.

-spec do_join(RemoteNode) -> Ret when
      RemoteNode :: node(),
      Ret :: ok | {error, any()}.
%% @doc Adds this node to `RemoteNode''s cluster.
%%
%% Before adding this node to the remote node's cluster, this function call
%% {@link setup/0} to ensure the Khepri store is running.
%%
%% It also pings the remote node to make sure it is reachable.
%%
%% If RabbitMQ is still running on the Erlang node, it will put it in
%% maintenance before proceeding. It will resume RabbitMQ after the join (or if
%% the join fails).
%%
%% @private

do_join(RemoteNode) when RemoteNode =/= node() ->
    ThisNode = node(),

    ?LOG_DEBUG(
       "Khepri clustering: Trying to add this node (~p) to cluster \"~s\" "
       "through node ~p",
       [ThisNode, ?RA_CLUSTER_NAME, RemoteNode],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),

    %% Ensure the local Khepri store is running before we can reset it. It
    %% could be stopped if RabbitMQ is not running for instance.
    ok = setup(),

    %% Ensure the remote node is reachable before we add it.
    case net_adm:ping(RemoteNode) of
        pong ->
            %% We verify the cluster membership before adding `ThisNode' to
            %% `RemoteNode''s cluster. We do it mostly to keep the same
            %% behavior as what we do with Mnesia. Otherwise, the interest is
            %% limited given the check and the actual join are not atomic.

            ?LOG_DEBUG(
               "Adding this node (~p) to Khepri cluster \"~s\" through "
               "node ~p",
               [ThisNode, ?RA_CLUSTER_NAME, RemoteNode],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),

            %% If the remote node to add is running RabbitMQ, we need to put it
            %% in maintenance mode at least. We remember that state to revive
            %% the node only if it was fully running before this code.
            IsRunning = rabbit:is_running(ThisNode),
            AlreadyBeingDrained =
            rabbit_maintenance:is_being_drained_consistent_read(ThisNode),
            NeedToRevive = IsRunning andalso not AlreadyBeingDrained,
            maybe_drain_node(IsRunning),

            %% Joining a cluster includes a reset of the local Khepri store.
            Ret = khepri_cluster:join(?RA_CLUSTER_NAME, RemoteNode),

            %% Revive the remote node if it was running and not under
            %% maintenance before we changed the cluster membership.
            maybe_revive_node(NeedToRevive),

            Ret;
        pang ->
            {error, {node_unreachable, RemoteNode}}
    end.

maybe_drain_node(true) ->
    ok = rabbit_maintenance:drain();
maybe_drain_node(false) ->
    ok.

maybe_revive_node(true) ->
    ok = rabbit_maintenance:revive();
maybe_revive_node(false) ->
    ok.

post_add_member(JoiningNode, JoinedNode, ok) ->
    ?LOG_INFO(
       "Khepri clustering: Node ~p successfully added to cluster \"~s\" "
       "through node ~p",
       [JoiningNode, ?RA_CLUSTER_NAME, JoinedNode],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    ok;
post_add_member(JoiningNode, JoinedNode, Error) ->
    ?LOG_INFO(
       "Khepri clustering: Failed to add node ~p to cluster \"~s\" "
       "through ~p: ~p",
       [JoiningNode, ?RA_CLUSTER_NAME, JoinedNode, Error],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    Error.

-spec remove_member(NodeToRemove) -> ok when
      NodeToRemove :: node().
%% @doc Removes `NodeToRemove' from its cluster.
%%
%% This function runs on the node calling it.
%%
%% If `NodeToRemove' is reachable, this function calls the regular {@link
%% khepri_cluster:reset/1} on `NodeToRemove'. If it is unreachable, this
%% function call Ra on this node to remove the remote member.
%%
%% @private

remove_member(Node) ->
    retry_khepri_op(fun() -> do_remove_member(Node) end, 60).

-spec do_remove_member(NodeToRemove) -> Ret when
      NodeToRemove :: node(),
      Ret :: ok | {error, any()}.
%% @private

do_remove_member(NodeToRemove) when NodeToRemove =/= node() ->
    ?LOG_DEBUG(
       "Trying to remove node ~s from Khepri cluster \"~s\" on node ~s",
       [NodeToRemove, ?RA_CLUSTER_NAME, node()],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),

    %% Check if the node is part of the cluster. We query the local Ra server
    %% only, in case the cluster can't elect a leader right now.
    CurrentNodes = locally_known_nodes(),
    case lists:member(NodeToRemove, CurrentNodes) of
        true ->
            %% Ensure the remote node is reachable before we remove it.
            case net_adm:ping(NodeToRemove) of
                pong ->
                    remove_reachable_member(NodeToRemove);
                pang ->
                    remove_down_member(NodeToRemove)
            end;
        false ->
            ?LOG_INFO(
               "Asked to remove node ~s from Khepri cluster \"~s\" but not "
               "member of it: ~p",
               [NodeToRemove, ?RA_CLUSTER_NAME, lists:sort(CurrentNodes)],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            rabbit_mnesia:e(not_a_cluster_node)
    end.

remove_reachable_member(NodeToRemove) ->
    ?LOG_DEBUG(
       "Removing remote node ~s from Khepri cluster \"~s\"",
       [NodeToRemove, ?RA_CLUSTER_NAME],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),

    %% We need the Khepri store to run on the node to remove, to be
    %% able to reset it.
    ok = rabbit_misc:rpc_call(
           NodeToRemove, ?MODULE, setup, []),

    Ret = rabbit_misc:rpc_call(
            NodeToRemove, khepri_cluster, reset, [?RA_CLUSTER_NAME]),
    case Ret of
        ok ->
            rabbit_amqqueue:forget_all_durable(NodeToRemove),
            ?LOG_DEBUG(
               "Node ~s removed from Khepri cluster \"~s\"",
               [NodeToRemove, ?RA_CLUSTER_NAME],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, _} = Error ->
            ?LOG_ERROR(
               "Failed to remove remote node ~s from Khepri "
               "cluster \"~s\": ~p",
               [NodeToRemove, ?RA_CLUSTER_NAME, Error],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            Error
    end.

remove_down_member(NodeToRemove) ->
    ServerRef = khepri_cluster:node_to_member(?STORE_ID, node()),
    ServerId = khepri_cluster:node_to_member(?STORE_ID, NodeToRemove),
    Timeout = khepri_app:get_default_timeout(),
    Ret = ra:remove_member(ServerRef, ServerId, Timeout),
    case Ret of
        {ok, _, _} ->
            rabbit_amqqueue:forget_all_durable(NodeToRemove),
            ?LOG_DEBUG(
               "Node ~s removed from Khepri cluster \"~s\"",
               [NodeToRemove, ?RA_CLUSTER_NAME],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, Reason} = Error ->
            ?LOG_ERROR(
               "Failed to remove remote down node ~s from Khepri "
               "cluster \"~s\": ~p",
               [NodeToRemove, ?RA_CLUSTER_NAME, Reason],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            Error;
        {timeout, _LeaderId} ->
            ?LOG_ERROR(
               "Failed to remove remote down node ~s from Khepri "
               "cluster \"~s\" due to timeout",
               [NodeToRemove, ?RA_CLUSTER_NAME],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            {error, timeout}
    end.

retry_khepri_op(Fun, 0) ->
    Fun();
retry_khepri_op(Fun, N) ->
    case Fun() of
        {error, {no_more_servers_to_try, Reasons}} = Err ->
            case lists:member({error,cluster_change_not_permitted}, Reasons) of
                true ->
                    timer:sleep(1000),
                    retry_khepri_op(Fun, N - 1);
                false ->
                    Err
            end;
        {error, cluster_change_not_permitted} ->
            timer:sleep(1000),
            retry_khepri_op(Fun, N - 1);
        Any ->
            Any
    end.

-spec members() -> Members when
      Members :: [ra:server_id()].
%% @doc Returns the list of Ra server identifiers that are part of the
%% cluster.
%%
%% The membership is as it is known to the Ra leader in the cluster.
%%
%% The returned list is empty if there was an error.

members() ->
    case khepri_cluster:members(?RA_CLUSTER_NAME) of
        {ok, Members}    -> Members;
        {error, _Reason} -> []
    end.

-spec locally_known_members() -> Members when
      Members :: [ra:server_id()].
%% @doc Returns the list of Ra server identifiers that are part of the
%% cluster.
%%
%% The membership is as it is known to the local Ra server and may be
%% inconsistent compared to the "official" membership as seen by the Ra
%% leader.
%%
%% The returned list is empty if there was an error.

locally_known_members() ->
    case khepri_cluster:members(?RA_CLUSTER_NAME, #{favor => low_latency}) of
        {ok, Members}    -> Members;
        {error, _Reason} -> []
    end.

-spec nodes() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of Erlang nodes that are part of the cluster.
%%
%% The membership is as it is known to the Ra leader in the cluster.
%%
%% The returned list is empty if there was an error.

nodes() ->
    case khepri_cluster:nodes(?RA_CLUSTER_NAME) of
        {ok, Nodes}      -> Nodes;
        {error, _Reason} -> []
    end.

-spec locally_known_nodes() -> Nodes when
      Nodes :: [node()].
%% @doc Returns the list of Erlang node that are part of the cluster.
%%
%% The membership is as it is known to the local Ra server and may be
%% inconsistent compared to the "official" membership as seen by the Ra
%% leader.
%%
%% The returned list is empty if there was an error.

locally_known_nodes() ->
    case khepri_cluster:nodes(?RA_CLUSTER_NAME, #{favor => low_latency}) of
        {ok, Nodes}      -> Nodes;
        {error, _Reason} -> []
    end.

-spec check_cluster_consistency() -> Ret when
      Ret :: ok | {error, any()}.
%% @doc Performs various checks to validate that this node is healthy at the
%% metadata store level.
%%
%% @private

check_cluster_consistency() ->
    %% We want to find 0 or 1 consistent nodes.
    ReachableNodes = rabbit_nodes:list_reachable(),
    case lists:foldl(
           fun (Node,  {error, _})    -> check_cluster_consistency(Node, true);
               (_Node, {ok, Status})  -> {ok, Status}
           end, {error, not_found}, nodes_excl_me(ReachableNodes))
    of
        {ok, {RemoteAllNodes, _Running}} ->
            case ordsets:is_subset(ordsets:from_list(ReachableNodes),
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
                    %% TODO delete schema in khepri ???
                    ok
            end;
        {error, not_found} ->
            ok;
        {error, _} = E ->
            E
    end.

-spec check_cluster_consistency(Node, CheckNodesConsistency) -> Ret when
      Node :: node(),
      CheckNodesConsistency :: boolean(),
      Ret :: {ok, Status} | {error, any()},
      Status :: {All, Running},
      All :: [node()],
      Running :: [node()].
%% @private

check_cluster_consistency(Node, CheckNodesConsistency) ->
    case (catch remote_node_info(Node)) of
        {badrpc, _Reason} ->
            {error, not_found};
        {'EXIT', {badarg, _Reason}} ->
            {error, not_found};
        {_OTP, _Rabbit, {error, _Reason}} ->
            {error, not_found};
        {_OTP, _Rabbit, {ok, Status}} when CheckNodesConsistency ->
            case rabbit_db_cluster:check_compatibility(Node) of
                ok ->
                    case check_nodes_consistency(Node, Status) of
                        ok    -> {ok, Status};
                        Error -> Error
                    end;
                Error ->
                    Error
            end;
        {_OTP, _Rabbit, {ok, Status}} ->
            {ok, Status}
    end.

-spec remote_node_info(Node) -> Info when
      Node :: node(),
      Info :: {OtpVersion, RabbitMQVersion, ClusterStatus},
      OtpVersion :: string(),
      RabbitMQVersion :: string(),
      ClusterStatus :: {ok, {All, Running}} | {error, any()},
      All :: [node()],
      Running :: [node()].
%% @private

remote_node_info(Node) ->
    rpc:call(Node, ?MODULE, node_info, []).

-spec node_info() -> Info when
      Info :: {OtpVersion, RabbitMQVersion, ClusterStatus},
      OtpVersion :: string(),
      RabbitMQVersion :: string(),
      ClusterStatus :: {ok, {All, Running}} | {error, khepri_not_running},
      All :: [node()],
      Running :: [node()].
%% @private

node_info() ->
    {rabbit_misc:otp_release(),
     rabbit_misc:version(),
     cluster_status_from_khepri()}.

check_nodes_consistency(Node, {RemoteAllNodes, _RemoteRunningNodes}) ->
    case me_in_nodes(RemoteAllNodes) of
        true ->
            ok;
        false ->
            {error, {inconsistent_cluster,
                     format_inconsistent_cluster_message(node(), Node)}}
    end.

format_inconsistent_cluster_message(Thinker, Dissident) ->
    rabbit_misc:format("Khepri: node ~tp thinks it's clustered "
                       "with node ~tp, but ~tp disagrees",
                       [Thinker, Dissident, Dissident]).

nodes_excl_me(Nodes) -> Nodes -- [node()].
me_in_nodes(Nodes) -> lists:member(node(), Nodes).

-spec cluster_status_from_khepri() -> ClusterStatus when
      ClusterStatus :: {ok, {All, Running}} | {error, khepri_not_running},
      All :: [node()],
      Running :: [node()].
%% @private

cluster_status_from_khepri() ->
    try
        _ = get_ra_key_metrics(node()),
        All = locally_known_nodes(),
        Running = lists:filter(
                    fun(N) ->
                            rabbit_nodes:is_running(N)
                    end, All),
        {ok, {All, Running}}
    catch
        _:_ ->
            {error, khepri_not_running}
    end.

-spec transfer_leadership(Candidates) -> Ret when
      Candidates :: [node()],
      Ret :: {ok, Result} | {error, any()},
      Result :: node() | undefined.
%% @private

transfer_leadership([]) ->
    ?LOG_WARNING(
       "Skipping leadership transfer of metadata store: no candidate "
       "(online, not under maintenance) nodes to transfer to!",
       #{domain => ?RMQLOG_DOMAIN_DB}),
    {error, no_candidates};
transfer_leadership(TransferCandidates) ->
    case get_feature_state() of
        enabled ->
            do_transfer_leadership(TransferCandidates);
        _ ->
            ?LOG_INFO(
               "Skipping leadership transfer of metadata store: Khepri "
               "is not enabled",
               #{domain => ?RMQLOG_DOMAIN_DB}),
            {error, khepri_not_enabled}
    end.

do_transfer_leadership([]) ->
    ?LOG_WARNING(
       "Khepri clustering: failed to transfer leadership, no more "
       "candidates available",
       #{domain => ?RMQLOG_DOMAIN_DB}),
    {error, not_migrated};
do_transfer_leadership([Destination | TransferCandidates]) ->
    ?LOG_INFO(
       "Khepri clustering: transferring leadership to node ~p",
       [Destination],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    case ra_leaderboard:lookup_leader(?STORE_ID) of
        {Name, Node} = Id when Node == node() ->
            Timeout = khepri_app:get_default_timeout(),
            case ra:transfer_leadership(Id, {Name, Destination}) of
                ok ->
                    case ra:members(Id, Timeout) of
                        {_, _, {_, NewNode}} ->
                            ?LOG_INFO(
                               "Khepri clustering: successfully "
                               "transferred leadership to node ~p",
                               [Destination],
                               #{domain => ?RMQLOG_DOMAIN_DB}),
                            {ok, NewNode};
                        {timeout, _} ->
                            ?LOG_WARNING(
                               "Khepri clustering: maybe failed to transfer "
                               "leadership to node ~p, members query has "
                               "timed out",
                               [Destination],
                               #{domain => ?RMQLOG_DOMAIN_DB}),
                            {error, not_migrated}
                    end;
                already_leader ->
                    ?LOG_INFO(
                       "Khepri clustering: successfully transferred "
                       "leadership to node ~p, already the leader",
                       [Destination],
                       #{domain => ?RMQLOG_DOMAIN_DB}),
                    {ok, Destination};
                {error, Reason} ->
                    ?LOG_WARNING(
                       "Khepri clustering: failed to transfer leadership "
                       "to node ~p with the following error ~p",
                       [Destination, Reason],
                       #{domain => ?RMQLOG_DOMAIN_DB}),
                    do_transfer_leadership(TransferCandidates);
                {timeout, _} ->
                    ?LOG_WARNING(
                       "Khepri clustering: failed to transfer leadership "
                       "to node ~p with a timeout",
                       [Destination],
                       #{domain => ?RMQLOG_DOMAIN_DB}),
                    do_transfer_leadership(TransferCandidates)
            end;
        {_, Node} ->
            ?LOG_INFO(
               "Khepri clustering: skipping leadership transfer, leader is "
               "already on node ~p",
               [Node],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            {ok, Node};
        undefined ->
            ?LOG_INFO(
               "Khepri clustering: skipping leadership transfer, leader "
               "not elected",
               #{domain => ?RMQLOG_DOMAIN_DB}),
            {ok, undefined}
    end.

%% -------------------------------------------------------------------
%% CLI command support functions.
%% -------------------------------------------------------------------

-spec force_shrink_member_to_current_member() -> ok.
%% @doc Shrinks the local Khepri store to be alone in its cluster.d
%%
%% The difference with a reset is that it does not lose its data.
%%
%% This is only used by the CLI's `force_standalone_khepri_boot' command.
%%
%% @private

force_shrink_member_to_current_member() ->
    ok = ra_server_proc:force_shrink_members_to_current_member(
           {?RA_CLUSTER_NAME, node()}).

-spec status() -> Status when
      Status :: [Metrics],
      Metrics :: [{Key, Value}],
      Key :: binary(),
      Value :: any().
%% @private

status() ->
    Nodes = rabbit_nodes:all_running(),
    [try
         Metrics = get_ra_key_metrics(N),
         #{state := RaftState,
           membership := Membership,
           commit_index := Commit,
           term := Term,
           last_index := Last,
           last_applied := LastApplied,
           last_written_index := LastWritten,
           snapshot_index := SnapIdx,
           machine_version := MacVer} = Metrics,
         [{<<"Node Name">>, N},
          {<<"Raft State">>, RaftState},
          {<<"Membership">>, Membership},
          {<<"Last Log Index">>, Last},
          {<<"Last Written">>, LastWritten},
          {<<"Last Applied">>, LastApplied},
          {<<"Commit Index">>, Commit},
          {<<"Snapshot Index">>, SnapIdx},
          {<<"Term">>, Term},
          {<<"Machine Version">>, MacVer}
         ]
     catch
         _:Error ->
             [{<<"Node Name">>, N},
              {<<"Raft State">>, Error},
              {<<"Membership">>, <<>>},
              {<<"Last Log Index">>, <<>>},
              {<<"Last Written">>, <<>>},
              {<<"Last Applied">>, <<>>},
              {<<"Commit Index">>, <<>>},
              {<<"Snapshot Index">>, <<>>},
              {<<"Term">>, <<>>},
              {<<"Machine Version">>, <<>>}
             ]
     end || N <- Nodes].

-spec get_ra_key_metrics(Node) -> Metrics when
      Node :: node(),
      Metrics :: map().
%% @private

get_ra_key_metrics(Node) ->
    ServerId = {?RA_CLUSTER_NAME, Node},
    Metrics0 = ra:key_metrics(ServerId),
    MacVer = try
                 erpc:call(Node, khepri_machine, version, [])
             catch
                 _:{exception, undef, [{khepri_machine, version, _, _} | _]} ->
                     0
             end,
    Metrics1 = Metrics0#{machine_version => MacVer},
    Metrics1.

-spec cli_cluster_status() -> Status when
      Status :: [{nodes, [{disc, [node()]}]} |
                 {running_nodes, [node()]} |
                 {cluster_name, binary()} |
                 {partitions, []}].
%% @private

cli_cluster_status() ->
    case rabbit:is_running() of
        true ->
            Nodes = locally_known_nodes(),
            [{nodes, [{disc, Nodes}]},
             {running_nodes, [N || N <- Nodes, rabbit_nodes:is_running(N)]},
             {cluster_name, rabbit_nodes:cluster_name()},
             {partitions, []}];
        false ->
            []
    end.

%% -------------------------------------------------------------------
%% "Proxy" functions to Khepri API.
%% -------------------------------------------------------------------

%% They just add the store ID to every calls.
%%
%% The only exceptions are get() and match() which both call khepri:get()
%% behind the scene with different options.
%%
%% They are some additional functions too, because they are useful in
%% RabbitMQ. They might be moved to Khepri in the future.

is_empty() ->
    khepri:is_empty(?STORE_ID).

get(PathPattern) ->
    khepri:get(?STORE_ID, PathPattern).

get(PathPattern, Options) ->
    khepri:get(?STORE_ID, PathPattern, Options).

adv_get(PathPattern) ->
    khepri_adv:get(?STORE_ID, PathPattern).

adv_get(PathPattern, Options) ->
    khepri_adv:get(?STORE_ID, PathPattern, Options).

get_many(PathPattern) ->
    khepri:get_many(?STORE_ID, PathPattern).

get_many(PathPattern, Options) ->
    khepri:get_many(?STORE_ID, PathPattern, Options).

adv_get_many(PathPattern) ->
    khepri_adv:get_many(?STORE_ID, PathPattern).

adv_get_many(PathPattern, Options) ->
    khepri_adv:get_many(?STORE_ID, PathPattern, Options).

exists(PathPattern) ->
    khepri:exists(?STORE_ID, PathPattern).

exists(PathPattern, Options) ->
    khepri:exists(?STORE_ID, PathPattern, Options).

%% `count/{1,2}' sets the `favor => low_latency' option.

count(PathPattern) ->
    count(PathPattern, #{}).

count(PathPattern, Options) ->
    Options1 = Options#{favor => low_latency},
    khepri:count(?STORE_ID, PathPattern, Options1).

fold(PathPattern, Pred, Acc) ->
    khepri:fold(?STORE_ID, PathPattern, Pred, Acc).

fold(PathPattern, Pred, Acc, Options) ->
    khepri:fold(?STORE_ID, PathPattern, Pred, Acc, Options).

foreach(PathPattern, Pred) ->
    khepri:foreach(?STORE_ID, PathPattern, Pred).

foreach(PathPattern, Pred, Options) ->
    khepri:foreach(?STORE_ID, PathPattern, Pred, Options).

map(PathPattern, Pred) ->
    khepri:map(?STORE_ID, PathPattern, Pred).

map(PathPattern, Pred, Options) ->
    khepri:map(?STORE_ID, PathPattern, Pred, Options).

filter(PathPattern, Pred) ->
    khepri:filter(?STORE_ID, PathPattern, Pred).

filter(PathPattern, Pred, Options) ->
    khepri:filter(?STORE_ID, PathPattern, Pred, Options).

put(PathPattern, Data) ->
    put(PathPattern, Data, #{}).

put(PathPattern, Data, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri:put(?STORE_ID, PathPattern, Data, Options1).

adv_put(PathPattern, Data) ->
    adv_put(PathPattern, Data, #{}).

adv_put(PathPattern, Data, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri_adv:put(?STORE_ID, PathPattern, Data, Options1).

create(PathPattern, Data) ->
    create(PathPattern, Data, #{}).

create(PathPattern, Data, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri:create(?STORE_ID, PathPattern, Data, Options1).

adv_create(PathPattern, Data) ->
    adv_create(PathPattern, Data, #{}).

adv_create(PathPattern, Data, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri_adv:create(?STORE_ID, PathPattern, Data, Options1).

update(PathPattern, Data) ->
    update(PathPattern, Data, #{}).

update(PathPattern, Data, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri:update(?STORE_ID, PathPattern, Data, Options1).

adv_update(PathPattern, Data) ->
    adv_update(PathPattern, Data, #{}).

adv_update(PathPattern, Data, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri_adv:update(?STORE_ID, PathPattern, Data, Options1).

%% `delete/{1,2}' calls `khepri:delete_many/2.

delete(PathPattern) ->
    delete(PathPattern, #{}).

delete(PathPattern, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri:delete_many(?STORE_ID, PathPattern, Options1).

adv_delete(PathPattern) ->
    adv_delete(PathPattern, #{}).

adv_delete(PathPattern, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri_adv:delete_many(?STORE_ID, PathPattern, Options1).

clear_payload(PathPattern) ->
    clear_payload(PathPattern, #{}).

clear_payload(PathPattern, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    khepri:clear_payload(?STORE_ID, PathPattern, Options1).

transaction(Fun) ->
    transaction(Fun, auto, #{}).

transaction(Fun, ReadWrite) ->
    transaction(Fun, ReadWrite, #{}).

transaction(Fun, ReadWrite, Options) ->
    Options1 = maps:merge(?DEFAULT_COMMAND_OPTIONS, Options),
    case khepri:transaction(?STORE_ID, Fun, ReadWrite, Options1) of
        ok              -> ok; %% Async transaction.
        {ok, Result}    -> Result;
        {error, Reason} -> throw({error, Reason})
    end.

fence(Timeout) ->
    khepri:fence(?STORE_ID, Timeout).

handle_async_ret(RaEvent) ->
    khepri:handle_async_ret(?STORE_ID, RaEvent).

%% `delete_or_fail/1' is not a proxy to a Khepri function.

delete_or_fail(Path) ->
    %% `Path' must not be a pattern.
    case khepri_adv:delete(?STORE_ID, Path, ?DEFAULT_COMMAND_OPTIONS) of
        {ok, #{Path := NodeProps}} ->
            case maps:size(NodeProps) of
                0 -> {error, {node_not_found, #{}}};
                _ -> ok
            end;
        {ok, #{} = NodePropsMap} when NodePropsMap =:= #{} ->
            {error, {node_not_found, #{}}};
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% Projections setup.
%% -------------------------------------------------------------------

register_projections() ->
    RegFuns = [fun register_rabbit_exchange_projection/0,
               fun register_rabbit_queue_projection/0,
               fun register_rabbit_vhost_projection/0,
               fun register_rabbit_users_projection/0,
               fun register_rabbit_global_runtime_parameters_projection/0,
               fun register_rabbit_per_vhost_runtime_parameters_projection/0,
               fun register_rabbit_user_permissions_projection/0,
               fun register_rabbit_bindings_projection/0,
               fun register_rabbit_index_route_projection/0,
               fun register_rabbit_topic_graph_projection/0],
    rabbit_misc:for_each_while_ok(
      fun(RegisterFun) ->
              case RegisterFun() of
                  ok ->
                      ok;
                  %% Before Khepri v0.13.0, `khepri:register_projection/1,2,3`
                  %% would return `{error, exists}` for projections which
                  %% already exist.
                  {error, exists} ->
                      ok;
                  %% In v0.13.0+, Khepri returns a `?khepri_error(..)` instead.
                  {error, {khepri, projection_already_exists, _Info}} ->
                      ok;
                  {error, _} = Error ->
                      Error
              end
      end, RegFuns).

register_rabbit_exchange_projection() ->
    Name = rabbit_khepri_exchange,
    PathPattern = rabbit_db_exchange:khepri_exchange_path(
                    _VHost = ?KHEPRI_WILDCARD_STAR,
                    _Name = ?KHEPRI_WILDCARD_STAR),
    KeyPos = #exchange.name,
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_queue_projection() ->
    Name = rabbit_khepri_queue,
    PathPattern = rabbit_db_queue:khepri_queue_path(
                    _VHost = ?KHEPRI_WILDCARD_STAR,
                    _Name = ?KHEPRI_WILDCARD_STAR),
    KeyPos = 2, %% #amqqueue.name
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_vhost_projection() ->
    Name = rabbit_khepri_vhost,
    PathPattern = rabbit_db_vhost:khepri_vhost_path(
                    _VHost = ?KHEPRI_WILDCARD_STAR),
    KeyPos = 2, %% #vhost.virtual_host
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_users_projection() ->
    Name = rabbit_khepri_user,
    PathPattern = rabbit_db_user:khepri_user_path(
                    _UserName = ?KHEPRI_WILDCARD_STAR),
    KeyPos = 2, %% #internal_user.username
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_global_runtime_parameters_projection() ->
    Name = rabbit_khepri_global_rtparam,
    PathPattern = rabbit_db_rtparams:khepri_global_rp_path(
                    _Key = ?KHEPRI_WILDCARD_STAR_STAR),
    KeyPos = #runtime_parameters.key,
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_per_vhost_runtime_parameters_projection() ->
    Name = rabbit_khepri_per_vhost_rtparam,
    PathPattern = rabbit_db_rtparams:khepri_vhost_rp_path(
                    _VHost = ?KHEPRI_WILDCARD_STAR_STAR,
                    _Component = ?KHEPRI_WILDCARD_STAR_STAR,
                    _Name = ?KHEPRI_WILDCARD_STAR_STAR),
    KeyPos = #runtime_parameters.key,
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_user_permissions_projection() ->
    Name = rabbit_khepri_user_permission,
    PathPattern = rabbit_db_user:khepri_user_permission_path(
                    _UserName = ?KHEPRI_WILDCARD_STAR,
                    _VHost = ?KHEPRI_WILDCARD_STAR),
    KeyPos = #user_permission.user_vhost,
    register_simple_projection(Name, PathPattern, KeyPos).

register_simple_projection(Name, PathPattern, KeyPos) ->
    Options = #{keypos => KeyPos},
    Projection = khepri_projection:new(Name, copy, Options),
    khepri:register_projection(?STORE_ID, PathPattern, Projection).

register_rabbit_bindings_projection() ->
    MapFun = fun(_Path, Binding) ->
                     #route{binding = Binding}
             end,
    ProjectionFun = projection_fun_for_sets(MapFun),
    Options = #{keypos => #route.binding},
    Projection = khepri_projection:new(
                   rabbit_khepri_binding, ProjectionFun, Options),
    PathPattern = rabbit_db_binding:khepri_route_path(
                    _VHost = ?KHEPRI_WILDCARD_STAR,
                    _ExchangeName = ?KHEPRI_WILDCARD_STAR,
                    _Kind = ?KHEPRI_WILDCARD_STAR,
                    _DstName = ?KHEPRI_WILDCARD_STAR,
                    _RoutingKey = ?KHEPRI_WILDCARD_STAR),
    khepri:register_projection(?STORE_ID, PathPattern, Projection).

register_rabbit_index_route_projection() ->
    MapFun = fun(Path, _) ->
                     {
                      VHost,
                      ExchangeName,
                      Kind,
                      DstName,
                      RoutingKey
                     } = rabbit_db_binding:khepri_route_path_to_args(Path),
                     Exchange = rabbit_misc:r(VHost, exchange, ExchangeName),
                     Destination = rabbit_misc:r(VHost, Kind, DstName),
                     SourceKey = {Exchange, RoutingKey},
                     #index_route{source_key = SourceKey,
                                  destination = Destination}
             end,
    ProjectionFun = projection_fun_for_sets(MapFun),
    Options = #{type => bag, keypos => #index_route.source_key},
    Projection = khepri_projection:new(
                   rabbit_khepri_index_route, ProjectionFun, Options),
    DirectOrFanout = #if_data_matches{
                        pattern = #exchange{type = '$1', _ = '_'},
                        conditions = [{'andalso',
                                       {'=/=', '$1', headers},
                                       {'=/=', '$1', topic}}]},
    PathPattern = rabbit_db_binding:khepri_route_path(
                    _VHost = ?KHEPRI_WILDCARD_STAR,
                    _Exchange = DirectOrFanout,
                    _Kind = ?KHEPRI_WILDCARD_STAR,
                    _DstName = ?KHEPRI_WILDCARD_STAR,
                    _RoutingKey = ?KHEPRI_WILDCARD_STAR),
    khepri:register_projection(?STORE_ID, PathPattern, Projection).

%% Routing information is stored in the Khepri store as a `set'.
%% In order to turn these bindings into records in an ETS `bag', we use a
%% `khepri_projection:extended_projection_fun()' to determine the changes
%% `khepri_projection' should apply to the ETS table using set algebra.
projection_fun_for_sets(MapFun) ->
    fun
        (Table, Path, #{data := OldPayload}, #{data := NewPayload}) ->
            Deletions = sets:subtract(OldPayload, NewPayload),
            Creations = sets:subtract(NewPayload, OldPayload),
            sets:fold(
              fun(Element, _Acc) ->
                      ets:delete_object(Table, MapFun(Path, Element))
              end, [], Deletions),
            ets:insert(Table, [MapFun(Path, Element) ||
                               Element <- sets:to_list(Creations)]);
        (Table, Path, _OldProps, #{data := NewPayload}) ->
            ets:insert(Table, [MapFun(Path, Element) ||
                               Element <- sets:to_list(NewPayload)]);

        (Table, Path, #{data := OldPayload}, _NewProps) ->
            sets:fold(
              fun(Element, _Acc) ->
                      ets:delete_object(Table, MapFun(Path, Element))
              end, [], OldPayload);
        (_Table, _Path, _OldProps, _NewProps) ->
            ok
    end.

register_rabbit_topic_graph_projection() ->
    Name = rabbit_khepri_topic_trie,
    %% This projection calls some external functions which are disallowed by
    %% Horus because they interact with global or random state. We explicitly
    %% allow them here for performance reasons.
    ShouldProcessFun =
    fun (rabbit_db_topic_exchange, split_topic_key_binary, 1, _From) ->
            %% This function uses `persistent_term' to store a lazily compiled
            %% binary pattern.
            false;
        (erlang, make_ref, 0, _From) ->
            %% Randomness is discouraged in Ra effects since the effects are
            %% executed separately by each cluster member. We'll use a random
            %% value for trie node IDs but these IDs will live as long as the
            %% projection table and do not need to be stable or reproducible
            %% across restarts or across Erlang nodes.
            false;
        (ets, _F, _A, _From) ->
            false;
        (M, F, A, From) ->
            khepri_tx_adv:should_process_function(M, F, A, From)
    end,
    Options = #{keypos => #topic_trie_edge.trie_edge,
                standalone_fun_options =>
                #{should_process_function => ShouldProcessFun}},
    ProjectionFun =
    fun(Table, Path, OldProps, NewProps) ->
        {
         VHost,
         ExchangeName,
         _Kind,
         _DstName,
         RoutingKey
        } = rabbit_db_binding:khepri_route_path_to_args(Path),
        Exchange = rabbit_misc:r(VHost, exchange, ExchangeName),
        Words = rabbit_db_topic_exchange:split_topic_key_binary(RoutingKey),
        case {OldProps, NewProps} of
            {#{data := OldBindings}, #{data := NewBindings}} ->
                ToInsert = sets:subtract(NewBindings, OldBindings),
                ToDelete = sets:subtract(OldBindings, NewBindings),
                follow_down_update(
                  Table, Exchange, Words,
                  fun(ExistingBindings) ->
                          sets:union(
                            sets:subtract(ExistingBindings, ToDelete),
                            ToInsert)
                  end);
            {_, #{data := NewBindings}} ->
                follow_down_update(
                  Table, Exchange, Words,
                  fun(ExistingBindings) ->
                          sets:union(ExistingBindings, NewBindings)
                  end);
            {#{data := OldBindings}, _} ->
                follow_down_update(
                  Table, Exchange, Words,
                  fun(ExistingBindings) ->
                          sets:subtract(ExistingBindings, OldBindings)
                  end);
            {_, _} ->
                ok
        end
    end,
    Projection = khepri_projection:new(Name, ProjectionFun, Options),
    PathPattern = rabbit_db_binding:khepri_route_path(
                    _VHost = ?KHEPRI_WILDCARD_STAR,
                    _Exchange = #if_data_matches{
                                   pattern = #exchange{type = topic, _ = '_'}},
                    _Kind = ?KHEPRI_WILDCARD_STAR,
                    _DstName = ?KHEPRI_WILDCARD_STAR,
                    _RoutingKey = ?KHEPRI_WILDCARD_STAR),
    khepri:register_projection(?STORE_ID, PathPattern, Projection).

-spec follow_down_update(Table, Exchange, Words, UpdateFn) -> Ret when
      Table :: ets:tid(),
      Exchange :: rabbit_types:exchange_name(),
      Words :: [binary()],
      BindingsSet :: sets:set(rabbit_types:binding()),
      UpdateFn :: fun((BindingsSet) -> BindingsSet),
      Ret :: ok.

follow_down_update(Table, Exchange, Words, UpdateFn) ->
    follow_down_update(Table, Exchange, root, Words, UpdateFn),
    ok.

-spec follow_down_update(Table, Exchange, NodeId, Words, UpdateFn) -> Ret when
      Table :: ets:tid(),
      Exchange :: rabbit_types:exchange_name(),
      NodeId :: root | rabbit_guid:guid(),
      Words :: [binary()],
      BindingsSet :: sets:set(rabbit_types:binding()),
      UpdateFn :: fun((BindingsSet) -> BindingsSet),
      Ret :: keep | delete.

follow_down_update(Table, Exchange, FromNodeId, [To | Rest], UpdateFn) ->
    TrieEdge = #trie_edge{exchange_name = Exchange,
                          node_id       = FromNodeId,
                          word          = To},
    ToNodeId = case ets:lookup(Table, TrieEdge) of
                   [#topic_trie_edge{node_id = ExistingId}] ->
                       ExistingId;
                   [] ->
                       %% The Khepri topic graph table uses references for node
                       %% IDs instead of `rabbit_guid:gen/0' used by mnesia.
                       %% This is possible because the topic graph table is
                       %% never persisted to disk. References take up slightly
                       %% less memory and are very cheap to produce compared to
                       %% `rabbit_guid' (which requires the `rabbit_guid'
                       %% genserver to be online).
                       NewNodeId = make_ref(),
                       NewEdge = #topic_trie_edge{trie_edge = TrieEdge,
                                                  node_id = NewNodeId},
                       %% Create the intermediary node.
                       ets:insert(Table, NewEdge),
                       NewNodeId
               end,
    case follow_down_update(Table, Exchange, ToNodeId, Rest, UpdateFn) of
        delete ->
            OutEdgePattern = #topic_trie_edge{trie_edge =
                                              TrieEdge#trie_edge{word = '_'},
                                              node_id = '_'},
            case ets:match(Table, OutEdgePattern, 1) of
                '$end_of_table' ->
                    ets:delete(Table, TrieEdge),
                    delete;
                {_Match, _Continuation} ->
                    keep
            end;
        keep ->
            keep
    end;
follow_down_update(Table, Exchange, LeafNodeId, [], UpdateFn) ->
    TrieEdge = #trie_edge{exchange_name = Exchange,
                          node_id       = LeafNodeId,
                          word          = bindings},
    Bindings = case ets:lookup(Table, TrieEdge) of
                   [#topic_trie_edge{node_id =
                                     {bindings, ExistingBindings}}] ->
                       ExistingBindings;
                   [] ->
                       sets:new([{version, 2}])
               end,
    NewBindings = UpdateFn(Bindings),
    case sets:is_empty(NewBindings) of
        true ->
            %% If the bindings have been deleted, delete the trie edge and
            %% any edges that no longer lead to any bindings or other edges.
            ets:delete(Table, TrieEdge),
            delete;
        false ->
            ToNodeId = {bindings, NewBindings},
            Edge = #topic_trie_edge{trie_edge = TrieEdge, node_id = ToNodeId},
            ets:insert(Table, Edge),
            keep
    end.

%% -------------------------------------------------------------------
%% Mnesia->Khepri migration code.
%% -------------------------------------------------------------------

-spec is_enabled() -> IsEnabled when
      IsEnabled :: boolean().
%% @doc Returns true if Khepri is enabled, false otherwise.
%%
%% This function will block while the feature flag is being enabled and Mnesia
%% tables are migrated.

is_enabled() ->
    is_enabled__internal(blocking).

-spec is_enabled(Node) -> IsEnabled when
      Node :: node(),
      IsEnabled :: boolean().
%% @doc Returns true if Khepri is enabled on node `Node', false otherwise.
%%
%% This function will block while the feature flag is being enabled and Mnesia
%% tables are migrated.

is_enabled(Node) ->
    try
        erpc:call(Node, ?MODULE, ?FUNCTION_NAME, [])
    catch
        error:{exception, undef, [{?MODULE, ?FUNCTION_NAME, _, _} | _]} ->
            false
    end.

-spec get_feature_state() -> State when
      State :: enabled | state_changing | disabled.
%% @doc Returns the current state of the Khepri use.
%%
%% This function will not block while the feature flag is being enabled and
%% Mnesia tables are migrated. It is your responsibility to handle the
%% intermediate state.

get_feature_state() ->
    Ret = is_enabled__internal(non_blocking),
    case Ret of
        true           -> enabled;
        false          -> disabled;
        state_changing -> Ret
    end.

-spec get_feature_state(Node) -> State when
      Node :: node(),
      State :: enabled | state_changing | disabled.
%% @doc Returns the current state of the Khepri use on node `Node'.
%%
%% This function will not block while the feature flag is being enabled and
%% Mnesia tables are migrated. It is your responsibility to handle the
%% intermediate state.

get_feature_state(Node) ->
    try
        erpc:call(Node, ?MODULE, ?FUNCTION_NAME, [])
    catch
        error:{exception, undef, [{?MODULE, ?FUNCTION_NAME, _, _} | _]} ->
            disabled
    end.

%% @private

khepri_db_migration_enable(#{feature_name := FeatureName}) ->
    Members = locally_known_members(),
    case length(Members) < 2 of
        true ->
            maybe
                ok ?= sync_cluster_membership_from_mnesia(FeatureName),
                ?LOG_INFO(
                   "Feature flag `~s`: unregistering legacy projections",
                   [FeatureName],
                   #{domain => ?RMQLOG_DOMAIN_DB}),
                ok ?= unregister_legacy_projections(),
                ?LOG_INFO(
                   "Feature flag `~s`: registering projections",
                   [FeatureName],
                   #{domain => ?RMQLOG_DOMAIN_DB}),
                ok ?= register_projections(),
                migrate_mnesia_tables(FeatureName)
            end;
        false ->
            ?LOG_INFO(
               "Feature flag `~s`: node ~0p already clustered (feature flag "
               "enabled as part of clustering?); "
               "skipping Mnesia->Khepri migration",
               [node()],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            ok
    end.

%% @private

khepri_db_migration_post_enable(
  #{feature_name := FeatureName, enabled := true}) ->
    ?LOG_DEBUG(
       "Feature flag `~s`: cleaning up after finished migration",
       [FeatureName],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    _ = mnesia_to_khepri:cleanup_after_table_copy(?STORE_ID, ?MIGRATION_ID),

    rabbit_mnesia:stop_mnesia(),

    %% We delete all Mnesia-related files in the data directory. This is in
    %% case this node joins a Mnesia-based cluster: it will be reset and switch
    %% back from Khepri to Mnesia. If there were Mnesia files left, Mnesia
    %% would restart with stale/incorrect data.
    MsgStoreDir = filename:dirname(rabbit_vhost:msg_store_dir_base()),
    DataDir = rabbit:data_dir(),
    MnesiaAndMsgStoreFiles = rabbit_mnesia:mnesia_and_msg_store_files(),
    MnesiaFiles0 = MnesiaAndMsgStoreFiles -- [filename:basename(MsgStoreDir)],
    MnesiaFiles = [filename:join(DataDir, File) || File <- MnesiaFiles0],
    NodeMonitorFiles = [rabbit_node_monitor:cluster_status_filename(),
                        rabbit_node_monitor:running_nodes_filename()],
    _ = rabbit_file:recursive_delete(MnesiaFiles ++ NodeMonitorFiles),

    ok;
khepri_db_migration_post_enable(
  #{feature_name := FeatureName, enabled := false}) ->
    ?LOG_DEBUG(
       "Feature flag `~s`: cleaning up after aborted migration",
       [FeatureName],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    _ = mnesia_to_khepri:rollback_table_copy(?STORE_ID, ?MIGRATION_ID),
    ok.

-spec sync_cluster_membership_from_mnesia(FeatureName) -> Ret when
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: ok | {error, Reason},
      Reason :: any().
%% @doc Initialises the Khepri cluster based on the Mnesia cluster.
%%
%% It uses the `khepri_mnesia_migration' application to synchronize membership
%% between both cluster.
%%
%% This function is called as part of the `enable' callback of the `khepri_db'
%% feature flag.

sync_cluster_membership_from_mnesia(FeatureName) ->
    %Lock = {{FeatureName, ?FUNCTION_NAME}, self()},
    %global:set_lock(Lock),
    try
        %% We use a global lock because `rabbit_khepri:setup()' on one node
        %% can't run concurrently with the membership sync on another node:
        %% the reset which is part of a join might conflict with the start in
        %% `rabbit_khepri:setup()'.
        sync_cluster_membership_from_mnesia_locked(FeatureName)
    after
        %global:del_lock(Lock)
        ok
    end.

sync_cluster_membership_from_mnesia_locked(FeatureName) ->
    rabbit_mnesia:ensure_mnesia_running(),

    try
        ?LOG_INFO(
           "Feature flag `~s`: syncing cluster membership",
           [FeatureName],
           #{domain => ?RMQLOG_DOMAIN_DB}),
        Ret = mnesia_to_khepri:sync_cluster_membership(?STORE_ID),
        ?LOG_INFO(
           "Feature flag `~s`: cluster membership synchronized; "
           "members are: ~1p",
           [FeatureName, lists:sort(nodes())],
           #{domain => ?RMQLOG_DOMAIN_DB}),
        Ret
    catch
        error:{khepri_mnesia_migration_ex, _, _} = Error ->
            ?LOG_ERROR(
               "Feature flag `~s`: failed to sync membership: ~p",
               [FeatureName, Error],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            {error, Error}
    end.

migrate_mnesia_tables(FeatureName) ->
    LoadedPlugins = load_disabled_plugins(),
    Migrations = discover_mnesia_tables_to_migrate(),
    Ret = do_migrate_mnesia_tables(FeatureName, Migrations),
    unload_disabled_plugins(LoadedPlugins),
    Ret.

load_disabled_plugins() ->
    #{plugins_path := PluginsPath} = rabbit_prelaunch:get_context(),
    %% We need to call the application master in a short-lived process, just in
    %% case it can't answer. This can happen if `rabbit` is stopped
    %% concurrently. In this case, the application master is busy trying to
    %% stop `rabbit`. However, `rabbit` is waiting for any feature flag
    %% operations to finish before it stops.
    %%
    %% By using this short-lived process and killing it after some time, we
    %% prevent a deadlock with the application master.
    Parent = self(),
    Loader = spawn_link(
               fun() ->
                       Plugins = [P#plugin.name
                                  || P <- rabbit_plugins:list(PluginsPath)],
                       Plugins1 = lists:map(
                                    fun(Plugin) ->
                                            case application:load(Plugin) of
                                                ok -> {Plugin, true};
                                                _  -> {Plugin, false}
                                            end
                                    end, Plugins),
                       Parent ! {plugins_loading, Plugins1},
                       erlang:unlink(Parent)
               end),
    receive
        {plugins_loading, Plugins} ->
            Plugins
    after 60_000 ->
              erlang:unlink(Loader),
              throw(
                {failed_to_discover_mnesia_tables_to_migrate,
                 plugins_loading_timeout})
    end.

unload_disabled_plugins(Plugins) ->
    %% See `load_disabled_plugins/0' for the reason why we use a short-lived
    %% process here.
    Parent = self(),
    Unloader = spawn_link(
                 fun() ->
                         lists:foreach(
                           fun
                               ({Plugin, true})   -> _ = application:unload(Plugin);
                               ({_Plugin, false}) -> ok
                           end, Plugins),
                         Parent ! plugins_unloading
                 end),
    receive
        plugins_unloading ->
            ok
    after 30_000 ->
              erlang:unlink(Unloader),
              throw(
                {failed_to_discover_mnesia_tables_to_migrate,
                 plugins_unloading_timeout})
    end.

discover_mnesia_tables_to_migrate() ->
    Apps = rabbit_misc:rabbitmq_related_apps(),
    AttrsPerApp = rabbit_misc:module_attributes_from_apps(
                    rabbit_mnesia_tables_to_khepri_db, Apps),
    discover_mnesia_tables_to_migrate1(AttrsPerApp, #{}).

discover_mnesia_tables_to_migrate1(
  [{App, _Module, Migrations} | Rest],
  MigrationsPerApp)
  when is_list(Migrations) ->
    Migrations0 = maps:get(App, MigrationsPerApp, []),
    Migrations1 = Migrations0 ++ Migrations,
    MigrationsPerApp1 = MigrationsPerApp#{App => Migrations1},
    discover_mnesia_tables_to_migrate1(Rest, MigrationsPerApp1);
discover_mnesia_tables_to_migrate1([], MigrationsPerApp) ->
    %% We list the applications involved and make sure `rabbit' is handled
    %% first.
    Apps = lists:sort(
             fun
                 (rabbit, _) -> true;
                 (_, rabbit) -> false;
                 (A, B)      -> A =< B
             end,
             maps:keys(MigrationsPerApp)),
    lists:foldl(
      fun(App, Acc) ->
              Acc ++ maps:get(App, MigrationsPerApp)
      end, [], Apps).

do_migrate_mnesia_tables(FeatureName, Migrations) ->
    Tables = lists:map(
               fun
                   ({Table, _Mod}) when is_atom(Table) -> Table;
                   (Table) when is_atom(Table)         -> Table
               end,
               Migrations),
    ?LOG_NOTICE(
       "Feature flags: `~ts`: starting migration of ~b tables from Mnesia "
       "to Khepri; expect decrease in performance and increase in memory "
       "footprint",
       [FeatureName, length(Migrations)],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_table:wait(Tables, _Retry = true),
    Ret = mnesia_to_khepri:copy_tables(
            ?STORE_ID, ?MIGRATION_ID, Tables,
            {rabbit_db_m2k_converter, Migrations}),
    case Ret of
        ok ->
            ?LOG_NOTICE(
               "Feature flags: `~ts`: migration from Mnesia to Khepri "
               "finished",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            ok;
        {error, _} = Error ->
            ?LOG_ERROR(
               "Feature flags: `~ts`: failed to migrate Mnesia tables to "
               "Khepri:~n  ~p",
               [FeatureName, Error],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            {error, {migration_failure, Error}}
    end.

-spec unregister_legacy_projections() -> Ret when
      Ret :: ok | timeout_error().
%% @doc Unregisters any projections which were registered in RabbitMQ 3.13.x
%% versions.
%%
%% In 3.13.x until 3.13.8 we mistakenly registered these projections even if
%% Khepri was not enabled. This function is used by the `khepri_db' enable
%% callback to remove those projections before we register the ones necessary
%% for 4.0.x.
%%
%% @private

unregister_legacy_projections() ->
    %% Note that we don't use `all' since `khepri_mnesia_migration' also
    %% creates a projection table which we don't want to unregister. Instead
    %% we list all of the legacy projection names:
    LegacyNames = [
        rabbit_khepri_exchange,
        rabbit_khepri_queue,
        rabbit_khepri_vhost,
        rabbit_khepri_users,
        rabbit_khepri_global_rtparams,
        rabbit_khepri_per_vhost_rtparams,
        rabbit_khepri_user_permissions,
        rabbit_khepri_bindings,
        rabbit_khepri_index_route,
        rabbit_khepri_topic_trie
    ],
    khepri:unregister_projections(?STORE_ID, LegacyNames).

-spec handle_fallback(Funs) -> Ret when
      Funs :: #{mnesia := Fun, khepri := Fun | Ret},
      Fun :: fun(() -> Ret),
      Ret :: any().
%% @doc Runs the function corresponding to the used database engine.
%%
%% If the `khepri_db' feature flag is already enabled, it executes the `Fun'
%% corresponding to Khepri directly and returns its value.
%%
%% Otherwise, it tries `Fun' corresponding to Mnesia first. It relies on the
%% "no table" exception from Mnesia to check the state of the feature flag
%% again and possibly switch th Khepri's `Fun'.
%%
%% Mnesia's `Fun' may be executed several times. Therefore, it must be
%% idempotent.
%%
%% Because this relies on the "no exists" table exception, the Mnesia function
%% must read from and/or write to Mnesia tables for this to work. If your
%% function does not access Mnesia tables, please use {@link is_enabled/0}
%% instead.
%%
%% @returns the return value of `Fun'.

handle_fallback(#{mnesia := MnesiaFun, khepri := KhepriFunOrRet})
  when is_function(MnesiaFun, 0) ->
    case get_feature_state() of
        enabled when is_function(KhepriFunOrRet, 0) ->
            KhepriFunOrRet();
        enabled ->
            KhepriFunOrRet;
        _ ->
            mnesia_to_khepri:handle_fallback(
              ?STORE_ID, ?MIGRATION_ID, MnesiaFun, KhepriFunOrRet)
    end.

-ifdef(TEST).
-define(FORCED_MDS_KEY, {?MODULE, forced_metadata_store}).

force_metadata_store(Backend) ->
    persistent_term:put(?FORCED_MDS_KEY, Backend).

get_forced_metadata_store() ->
    persistent_term:get(?FORCED_MDS_KEY, undefined).

clear_forced_metadata_store() ->
    _ = persistent_term:erase(?FORCED_MDS_KEY),
    ok.

is_enabled__internal(Blocking) ->
    case get_forced_metadata_store() of
        khepri ->
            ?assert(
               rabbit_feature_flags:is_enabled(khepri_db, non_blocking)),
            true;
        mnesia ->
            ?assertNot(
               rabbit_feature_flags:is_enabled(khepri_db, non_blocking)),
            false;
        undefined ->
            rabbit_feature_flags:is_enabled(khepri_db, Blocking)
    end.
-else.
is_enabled__internal(Blocking) ->
    rabbit_feature_flags:is_enabled(khepri_db, Blocking).
-endif.
