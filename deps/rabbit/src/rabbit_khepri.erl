%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_khepri).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([setup/0,
         setup/1,
         can_join_cluster/1,
         add_member/2,
         remove_member/1,
         members/0,
         locally_known_members/0,
         nodes/0,
         locally_known_nodes/0,
         get_store_id/0,
         transfer_leadership/1,

         is_empty/0,
         create/2,
         adv_create/2,
         update/2,
         cas/3,
         fold/3,
         filter/2,

         get/1,
         get/2,
         adv_get/1,
         match/1,
         match/2,
         exists/1,
         list/1,
         list_child_nodes/1,
         count_children/1,

         put/2, put/3,
         adv_put/2,
         clear_payload/1,
         delete/1,
         delete_or_fail/1,

         transaction/1,
         transaction/2,
         transaction/3,

         clear_store/0,

         dir/0,
         info/0,
         is_enabled/0,
         is_enabled/1,
         nodes_if_khepri_enabled/0,
         use_khepri/0, use_khepri/1,

         status/0]).
%% Flag used during migration
-export([is_ready/0,
         set_ready/0]).
%% Used during migration to join the standalone Khepri nodes and form the
%% equivalent cluster
-export([init_cluster/0]).
-export([do_join/1]).
%% To add the current node to an existing cluster
-export([leave_cluster/1]).
-export([is_clustered/0]).
-export([check_cluster_consistency/0,
         check_cluster_consistency/2,
         node_info/0]).
-export([reset/0,
         force_reset/0]).
-export([cluster_status_from_khepri/0,
         cli_cluster_status/0]).

%% Path functions
-export([if_has_data/1,
         if_has_data_wildcard/0]).

-export([force_shrink_member_to_current_member/0]).

-ifdef(TEST).
-export([force_metadata_store/1,
         clear_forced_metadata_store/0]).
-endif.

-compile({no_auto_import, [get/1, get/2, nodes/0]}).

%% `sys:get_status/1''s spec only allows `sys:name()' but can work on any
%% `erlang:send_destination()' including a `ra:server_id()'.
-dialyzer({nowarn_function, get_sys_status/1}).
-dialyzer({no_match, [status/0, cluster_status_from_khepri/0]}).

-define(RA_SYSTEM, coordination).
-define(RA_CLUSTER_NAME, metadata_store).
-define(RA_FRIENDLY_NAME, "RabbitMQ metadata store").
-define(STORE_ID, ?RA_CLUSTER_NAME).
-define(MDSTORE_SARTUP_LOCK, {?MODULE, self()}).
-define(PT_KEY, ?MODULE).

%% -------------------------------------------------------------------
%% API wrapping Khepri.
%% -------------------------------------------------------------------

-spec setup() -> ok | no_return().

setup() ->
    setup(rabbit_prelaunch:get_context()).

-spec setup(map()) -> ok | no_return().

setup(_) ->
    ?LOG_DEBUG("Starting Khepri-based " ?RA_FRIENDLY_NAME),
    ok = ensure_ra_system_started(),
    Timeout = application:get_env(rabbit, khepri_default_timeout, 30000),
    ok = application:set_env(
           khepri, default_timeout, Timeout, [{persistent, true}]),
    RaServerConfig = #{cluster_name => ?RA_CLUSTER_NAME,
                       friendly_name => ?RA_FRIENDLY_NAME},
    case khepri:start(?RA_SYSTEM, RaServerConfig) of
        {ok, ?STORE_ID} ->
            wait_for_leader(),
            register_projections(),
            ?LOG_DEBUG(
               "Khepri-based " ?RA_FRIENDLY_NAME " ready",
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, _} = Error ->
            exit(Error)
    end.

wait_for_leader() ->
    wait_for_leader(retry_timeout(), retry_limit()).

retry_timeout() ->
    case application:get_env(rabbit, khepri_leader_wait_retry_timeout) of
        {ok, T}   -> T;
        undefined -> 30000
    end.

retry_limit() ->
    case application:get_env(rabbit, khepri_leader_wait_retry_limit) of
        {ok, T}   -> T;
        undefined -> 10
    end.

wait_for_leader(_Timeout, 0) ->
    exit(timeout_waiting_for_leader);
wait_for_leader(Timeout, Retries) ->
    rabbit_log:info("Waiting for Khepri leader for ~tp ms, ~tp retries left",
                    [Timeout, Retries - 1]),
    Options = #{timeout => Timeout,
                favor => compromise},
    case khepri:exists(?STORE_ID, [], Options) of
        Exists when is_boolean(Exists) ->
            rabbit_log:info("Khepri leader elected"),
            ok;
        {error, {timeout, _ServerId}} ->
            wait_for_leader(Timeout, Retries -1);
        {error, Reason} ->
            throw(Reason)
    end.

can_join_cluster(DiscoveryNode) when is_atom(DiscoveryNode) ->
    ThisNode = node(),
    try
        ClusterNodes = erpc:call(
                         DiscoveryNode,
                         rabbit_khepri, locally_known_nodes, []),
        case lists:member(ThisNode, ClusterNodes) of
            false ->
                {ok, ClusterNodes};
            true ->
                {ok, already_member}
        end
    catch
        _:Reason ->
            {error, Reason}
    end.

add_member(JoiningNode, JoinedNode)
  when JoiningNode =:= node() andalso is_atom(JoinedNode) ->
    Ret = do_join(JoinedNode),
    post_add_member(JoiningNode, JoinedNode, Ret);
add_member(JoiningNode, JoinedNode) when is_atom(JoinedNode) ->
    Ret = rabbit_misc:rpc_call(
            JoiningNode, rabbit_khepri, do_join, [JoinedNode]),
    post_add_member(JoiningNode, JoinedNode, Ret);
add_member(JoiningNode, [_ | _] = Cluster) ->
    case lists:member(JoiningNode, Cluster) of
        false ->
            JoinedNode = pick_node_in_cluster(Cluster),
            ?LOG_INFO(
               "Khepri clustering: Attempt to add node ~p to cluster ~0p "
               "through node ~p",
               [JoiningNode, Cluster, JoinedNode],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            %% Recurse with a single node taken in the `Cluster' list.
            add_member(JoiningNode, JoinedNode);
        true ->
            ?LOG_DEBUG(
               "Khepri clustering: Node ~p is already a member of cluster ~p",
               [JoiningNode, Cluster]),
            {ok, already_member}
    end.

pick_node_in_cluster([_ | _] = Cluster) when is_list(Cluster) ->
    ThisNode = node(),
    case lists:member(ThisNode, Cluster) of
        true  -> ThisNode;
        false -> hd(Cluster)
    end.

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
    khepri:info(?RA_CLUSTER_NAME),

    %% Ensure the remote node is reachable before we add it.
    pong = net_adm:ping(RemoteNode),

    %% We verify the cluster membership before adding `ThisNode' to
    %% `RemoteNode''s cluster. We do it mostly to keep the same behavior as
    %% what we do with Mnesia. Otherwise, the interest is limited given the
    %% check and the actual join are not atomic.

    ClusteredNodes = rabbit_misc:rpc_call(
                       RemoteNode, rabbit_khepri, locally_known_nodes, []),
    case lists:member(ThisNode, ClusteredNodes) of
        false ->
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
        true ->
            ?LOG_DEBUG(
               "This node (~p) is already part of the Khepri cluster \"~s\" "
               "like node ~p",
               [ThisNode, ?RA_CLUSTER_NAME, RemoteNode],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            {ok, already_member}
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
post_add_member(
  JoiningNode, _JoinedNode, {error, {already_member, Cluster}}) ->
    ?LOG_INFO(
       "Khepri clustering: Asked to add node ~p to cluster \"~s\" "
       "but already a member of it: ~p",
       [JoiningNode, ?RA_CLUSTER_NAME, lists:sort(Cluster)],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    {ok, already_member};
post_add_member(
  JoiningNode, JoinedNode,
  {badrpc, {'EXIT', {undef, [{rabbit_khepri, do_join, _, _}]}}} = Error) ->
    ?LOG_INFO(
       "Khepri clustering: Can't add node ~p to cluster \"~s\"; "
       "Khepri unavailable on node ~p: ~p",
       [JoiningNode, ?RA_CLUSTER_NAME, JoinedNode, Error],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    %% TODO: Should we return an error and let the caller decide?
    ok;
post_add_member(JoiningNode, JoinedNode, Error) ->
    ?LOG_INFO(
       "Khepri clustering: Failed to add node ~p to cluster \"~s\" "
       "through ~p: ~p",
       [JoiningNode, ?RA_CLUSTER_NAME, JoinedNode, Error],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    Error.

remove_member(NodeToRemove) when NodeToRemove =/= node() ->
    ?LOG_DEBUG(
       "Trying to remove node ~s from Khepri cluster \"~s\" on node ~s",
       [NodeToRemove, ?RA_CLUSTER_NAME, node()],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),

    %% Check if the node is part of the cluster. We query the local Ra server
    %% only, in case the cluster can't elect a leader right now.
    CurrentNodes = locally_known_nodes(),
    case CurrentNodes of
        [] -> rabbit_mnesia:e(not_a_cluster_node);
        _  -> ok
    end,
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
            ?LOG_DEBUG(
               "Node ~s removed from Khepri cluster \"~s\"",
               [NodeToRemove, ?RA_CLUSTER_NAME],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        Error ->
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
    Ret = ra:remove_member(ServerRef, ServerId),
    case Ret of
        {ok, _, _} ->
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
        {timeout, _} = Reason ->
            ?LOG_ERROR(
               "Failed to remove remote down node ~s from Khepri "
               "cluster \"~s\": ~p",
               [NodeToRemove, ?RA_CLUSTER_NAME, Reason],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            {error, Reason}
    end.

reset() ->
    %% Rabbit should be stopped, but Khepri needs to be running. Restart it.
    ok = setup(),
    ok = khepri_cluster:reset(?RA_CLUSTER_NAME),
    ok = khepri:stop(?RA_CLUSTER_NAME).

force_reset() ->
    DataDir = maps:get(data_dir, ra_system:fetch(coordination)),
    ok = rabbit_file:recursive_delete(filelib:wildcard(DataDir ++ "/*")).

force_shrink_member_to_current_member() ->
    ok = ra_server_proc:force_shrink_members_to_current_member(
           {?RA_CLUSTER_NAME, node()}).

ensure_ra_system_started() ->
    {ok, _} = application:ensure_all_started(khepri),
    ok = rabbit_ra_systems:ensure_ra_system_started(?RA_SYSTEM).

members() ->
    khepri_cluster:members(?RA_CLUSTER_NAME).

locally_known_members() ->
    khepri_cluster:locally_known_members(?RA_CLUSTER_NAME).

nodes() ->
    khepri_cluster:nodes(?RA_CLUSTER_NAME).

locally_known_nodes() ->
    khepri_cluster:locally_known_nodes(?RA_CLUSTER_NAME).

get_store_id() ->
    ?STORE_ID.

dir() ->
    filename:join(rabbit_mnesia:dir(), atom_to_list(?STORE_ID)).

-spec transfer_leadership([node()]) -> {ok, in_progress | undefined | node()} | {error, any()}.
transfer_leadership([Destination | _] = _TransferCandidates) ->
    case ra_leaderboard:lookup_leader(?STORE_ID) of
        {Name, Node} = Id when Node == node() ->
            case ra:transfer_leadership(Id, {Name, Destination}) of
                ok ->
                    case ra:members(Id) of
                        {_, _, {_, NewNode}} ->
                            {ok, NewNode};
                        {timeout, _} ->
                            {error, not_migrated}
                    end;
                already_leader ->
                    {ok, Destination};
                {error, _} = Error ->
                    Error;
                {timeout, _} ->
                    {error, timeout}
            end;
        {_, Node} ->
            {ok, Node};
        undefined ->
            {ok, undefined}
    end.

status() ->
    Nodes = rabbit_nodes:all_running(),
    [begin
         case get_sys_status({metadata_store, N}) of
             {ok, Sys} ->
                 {_, M} = lists:keyfind(ra_server_state, 1, Sys),
                 {_, RaftState} = lists:keyfind(raft_state, 1, Sys),
                 #{commit_index := Commit,
                   machine_version := MacVer,
                   current_term := Term,
                   log := #{last_index := Last,
                            snapshot_index := SnapIdx}} = M,
                 [{<<"Node Name">>, N},
                  {<<"Raft State">>, RaftState},
                  {<<"Log Index">>, Last},
                  {<<"Commit Index">>, Commit},
                  {<<"Snapshot Index">>, SnapIdx},
                  {<<"Term">>, Term},
                  {<<"Machine Version">>, MacVer}
                 ];
             {error, Err} ->
                 [{<<"Node Name">>, N},
                  {<<"Raft State">>, Err},
                  {<<"Log Index">>, <<>>},
                  {<<"Commit Index">>, <<>>},
                  {<<"Snapshot Index">>, <<>>},
                  {<<"Term">>, <<>>},
                  {<<"Machine Version">>, <<>>}
                 ]
         end
     end || N <- Nodes].


get_sys_status(Proc) ->
    try lists:nth(5, element(4, sys:get_status(Proc))) of
        Sys -> {ok, Sys}
    catch
        _:Err when is_tuple(Err) ->
            {error, element(1, Err)};
        _:_ ->
            {error, other}

    end.

cli_cluster_status() ->
    case rabbit:is_running() of
        true ->
            Nodes = locally_known_nodes(),
            [{nodes, [{disc, Nodes}]},
             {running_nodes, [N || N <- Nodes, rabbit_nodes:is_running(N)]},
             {cluster_name, rabbit_nodes:cluster_name()}];
        false ->
            []
    end.

%% For when Khepri is enabled
init_cluster() ->
    %% Ensure the local Khepri store is running before we can join it. It
    %% could be stopped if RabbitMQ is not running for instance.
    rabbit_log:debug("Khepri clustering: starting Mnesia..."),
    IsRunning = rabbit_mnesia:is_running(),
    try
        rabbit_mnesia:start_mnesia(false),
        rabbit_log:debug("Khepri clustering: starting Khepri..."),
        ok = setup(),
        khepri:info(?RA_CLUSTER_NAME),
        rabbit_log:debug("Khepri clustering: starting khepri_mnesia_migration..."),
        _ = application:ensure_all_started(khepri_mnesia_migration),
        rabbit_log:debug("Khepri clustering: syncing cluster membership"),
        mnesia_to_khepri:sync_cluster_membership(?STORE_ID)
    catch
        error:{khepri_mnesia_migration_ex, _, _} = Reason ->
            {error, Reason}
    after
        case IsRunning of
            true -> ok;
            false -> rabbit_mnesia:stop_mnesia()
        end
    end.

leave_cluster(Node) ->
    retry_khepri_op(fun() -> remove_member(Node) end, 60).

-spec is_clustered() -> boolean().

is_clustered() -> AllNodes = locally_known_nodes(),
                  AllNodes =/= [] andalso AllNodes =/= [node()].

check_cluster_consistency() ->
    %% We want to find 0 or 1 consistent nodes.
    case lists:foldl(
           fun (Node,  {error, _})    -> check_cluster_consistency(Node, true);
               (_Node, {ok, Status})  -> {ok, Status}
           end, {error, not_found}, nodes_excl_me(nodes()))
    of
        {ok, {RemoteAllNodes, _DiscNodes, _Running}} ->
            case ordsets:is_subset(ordsets:from_list(nodes()),
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

nodes_excl_me(Nodes) -> Nodes -- [node()].

check_cluster_consistency(Node, CheckNodesConsistency) ->
    case (catch remote_node_info(Node)) of
        {badrpc, _Reason} ->
            {error, not_found};
        {'EXIT', {badarg, _Reason}} ->
            {error, not_found};
        {_OTP, _Rabbit, _Protocol, {error, _Reason}} ->
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
            case rabbit_db_cluster:check_compatibility(Node) of
                ok    -> {ok, Status};
                Error -> Error
            end
    end.

remote_node_info(Node) ->
    rpc:call(Node, ?MODULE, node_info, []).

check_nodes_consistency(
  Node, {RemoteAllNodes, _DiscNodes, _RemoteRunningNodes}) ->
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

me_in_nodes(Nodes) -> lists:member(node(), Nodes).

node_info() ->
    {rabbit_misc:otp_release(), rabbit_misc:version(),
     "",
     cluster_status_from_khepri()}.

cluster_status_from_khepri() ->
    case get_sys_status({metadata_store, node()}) of
        {ok, _} ->
            All = locally_known_nodes(),
            Running = lists:filter(fun(N) -> rabbit_nodes:is_running(N) end, All),
            {ok, {All, All, Running}};
        _ ->
            {error, khepri_not_running}
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

is_empty() -> khepri:is_empty(?STORE_ID).

create(Path, Data) -> khepri:create(?STORE_ID, Path, Data).
adv_create(Path, Data) -> khepri_adv:create(?STORE_ID, Path, Data).
update(Path, Data) -> khepri:update(?STORE_ID, Path, Data).
cas(Path, Pattern, Data) ->
    khepri:compare_and_swap(?STORE_ID, Path, Pattern, Data).

fold(Path, Pred, Acc) -> khepri:fold(?STORE_ID, Path, Pred, Acc).

filter(Path, Pred) -> khepri:filter(?STORE_ID, Path, Pred).

get(Path) ->
    khepri:get(?STORE_ID, Path, #{favor => low_latency}).

get(Path, Options) ->
    khepri:get(?STORE_ID, Path, Options).

adv_get(Path) ->
    khepri_adv:get(?STORE_ID, Path, #{favor => low_latency}).

match(Path) ->
    match(Path, #{}).

match(Path, Options) -> khepri:get_many(?STORE_ID, Path, Options).

exists(Path) -> khepri:exists(?STORE_ID, Path, #{favor => low_latency}).

list(Path) -> khepri:get_many(?STORE_ID, Path ++ [?KHEPRI_WILDCARD_STAR]).

list_child_nodes(Path) ->
    Options = #{props_to_return => [child_names]},
    case khepri_adv:get_many(?STORE_ID, Path, Options) of
        {ok, Result} ->
            case maps:values(Result) of
                [#{child_names := ChildNames}] ->
                    {ok, ChildNames};
                [] ->
                    []
            end;
        Error ->
            Error
    end.

count_children(Path) ->
    Options = #{props_to_return => [child_list_length]},
    case khepri_adv:get_many(?STORE_ID, Path, Options) of
        {ok, Map} ->
            lists:sum([L || #{child_list_length := L} <- maps:values(Map)]);
        _ ->
            0
    end.

clear_payload(Path) -> khepri:clear_payload(?STORE_ID, Path).

delete(Path) -> khepri:delete_many(?STORE_ID, Path).

delete_or_fail(Path) ->
    case khepri_adv:delete(?STORE_ID, Path) of
        {ok, Result} ->
            case maps:size(Result) of
                0 -> {error, {node_not_found, #{}}};
                _ -> ok
            end;
        Error ->
            Error
    end.

put(PathPattern, Data) ->
    khepri:put(
      ?STORE_ID, PathPattern, Data).

put(PathPattern, Data, Extra) ->
    khepri:put(
      ?STORE_ID, PathPattern, Data, Extra).

adv_put(PathPattern, Data) ->
    khepri_adv:put(
      ?STORE_ID, PathPattern, Data).

transaction(Fun) ->
    transaction(Fun, auto, #{}).

transaction(Fun, ReadWrite) ->
    transaction(Fun, ReadWrite, #{}).

transaction(Fun, ReadWrite, Options) ->
    case khepri:transaction(?STORE_ID, Fun, ReadWrite, Options) of
        {ok, Result} -> Result;
        {error, Reason} -> throw({error, Reason})
    end.

clear_store() ->
    khepri:delete_many(?STORE_ID, "*").

info() ->
    ok = setup(),
    khepri:info(?STORE_ID).

%% -------------------------------------------------------------------
%% Raft-based metadata store (phase 1).
%% -------------------------------------------------------------------

is_enabled() ->
    rabbit_feature_flags:is_enabled(raft_based_metadata_store_phase1).

is_enabled(Blocking) ->
    rabbit_feature_flags:is_enabled(
      raft_based_metadata_store_phase1, Blocking) =:= true.

nodes_if_khepri_enabled() ->
    case rabbit_feature_flags:is_enabled(
           raft_based_metadata_store_phase1, non_blocking) of
        true  -> [Node || {_, Node} <- members()];
        false -> [];
        state_changing -> [Node || {_, Node} <- members()]
    end.

is_ready() ->
    case get([?MODULE, migration, ready]) of
        {ok, #{data := true}} ->
            true;
        _ ->
            false
    end.

set_ready() ->
    khepri:put(
      ?STORE_ID, [?MODULE, migration, ready], true).

%% -------------------------------------------------------------------
%% if_has_data_wildcard().
%% -------------------------------------------------------------------

-spec if_has_data_wildcard() -> Condition when
      Condition :: khepri_condition:condition().

if_has_data_wildcard() ->
    if_has_data([?KHEPRI_WILDCARD_STAR_STAR]).

%% -------------------------------------------------------------------
%% if_has_data().
%% -------------------------------------------------------------------

-spec if_has_data(Conditions) -> Condition when
      Conditions :: [Condition],
      Condition :: khepri_condition:condition().

if_has_data(Conditions) ->
    #if_all{conditions = Conditions ++ [#if_has_data{has_data = true}]}.

%% -------------------------------------------------------------------
%% Testing
%% -------------------------------------------------------------------

-ifdef(TEST).
-define(FORCED_MDS_KEY, {?MODULE, forced_metadata_store}).

force_metadata_store(Backend) ->
    persistent_term:put(?FORCED_MDS_KEY, Backend).

get_forced_metadata_store() ->
    persistent_term:get(?FORCED_MDS_KEY, undefined).

clear_forced_metadata_store() ->
    _ = persistent_term:erase(?FORCED_MDS_KEY),
    ok.
-endif.

-ifdef(TEST).
use_khepri() ->
    Ret = case get_forced_metadata_store() of
        khepri ->
            %% We use ?MODULE:is_enabled() to make sure the call goes through
            %% the possibly mocked module.
            ?assert(?MODULE:is_enabled(non_blocking)),
            true;
        mnesia ->
            ?assertNot(?MODULE:is_enabled(non_blocking)),
            false;
        undefined ->
            ?MODULE:is_enabled(non_blocking)
    end,
    %rabbit_log:notice("~s: ~p [TEST]", [?FUNCTION_NAME, Ret]),
    Ret.
-else.
use_khepri() ->
    Ret = is_enabled(non_blocking),
    %rabbit_log:notice("~s: ~p", [?FUNCTION_NAME, Ret]),
    Ret.
-endif.

use_khepri(Node) ->
    try
        erpc:call(Node, ?MODULE, use_khepri, [])
    catch
        error:{exception, undef, [{?MODULE, use_khepri, _, _} | _]} ->
            false
    end.

register_projections() ->
    RegisterFuns = [fun register_rabbit_exchange_projection/0,
                    fun register_rabbit_queue_projection/0,
                    fun register_rabbit_vhost_projection/0,
                    fun register_rabbit_users_projection/0,
                    fun register_rabbit_user_permissions_projection/0,
                    fun register_rabbit_bindings_projection/0,
                    fun register_rabbit_index_route_projection/0,
                    fun register_rabbit_topic_graph_projection/0],
    [case RegisterFun() of
         ok              -> ok;
         {error, exists} -> ok;
         {error, Error}  -> throw(Error)
     end || RegisterFun <- RegisterFuns],
    ok.

register_rabbit_exchange_projection() ->
    Name = rabbit_khepri_exchange,
    PathPattern = [rabbit_db_exchange,
                   exchanges,
                   _VHost = ?KHEPRI_WILDCARD_STAR,
                   _Name = ?KHEPRI_WILDCARD_STAR],
    KeyPos = #exchange.name,
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_queue_projection() ->
    Name = rabbit_khepri_queue,
    PathPattern = [rabbit_db_queue,
                   queues,
                   _VHost = ?KHEPRI_WILDCARD_STAR,
                   _Name = ?KHEPRI_WILDCARD_STAR],
    KeyPos = 2, %% #amqqueue.name
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_vhost_projection() ->
    Name = rabbit_khepri_vhost,
    PathPattern = [rabbit_db_vhost, _VHost = ?KHEPRI_WILDCARD_STAR],
    KeyPos = 2, %% #vhost.virtual_host
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_users_projection() ->
    Name = rabbit_khepri_users,
    PathPattern = [rabbit_db_user,
                   users,
                   _UserName = ?KHEPRI_WILDCARD_STAR],
    KeyPos = 2, %% #internal_user.username
    register_simple_projection(Name, PathPattern, KeyPos).

register_rabbit_user_permissions_projection() ->
    Name = rabbit_khepri_user_permissions,
    PathPattern = [rabbit_db_user,
                   users,
                   _UserName = ?KHEPRI_WILDCARD_STAR,
                   user_permissions,
                   _VHost = ?KHEPRI_WILDCARD_STAR],
    KeyPos = #user_permission.user_vhost,
    register_simple_projection(Name, PathPattern, KeyPos).

register_simple_projection(Name, PathPattern, KeyPos) ->
    Options = #{keypos => KeyPos},
    Projection = khepri_projection:new(Name, copy, Options),
    khepri:register_projection(?RA_CLUSTER_NAME, PathPattern, Projection).

register_rabbit_bindings_projection() ->
    MapFun = fun(_Path, Binding) ->
                     #route{binding = Binding}
             end,
    ProjectionFun = projection_fun_for_sets(MapFun),
    Options = #{keypos => #route.binding},
    Projection = khepri_projection:new(
                   rabbit_khepri_bindings, ProjectionFun, Options),
    PathPattern = [rabbit_db_binding,
                   routes,
                   _VHost = ?KHEPRI_WILDCARD_STAR,
                   _ExchangeName = ?KHEPRI_WILDCARD_STAR,
                   _Kind = ?KHEPRI_WILDCARD_STAR,
                   _DstName = ?KHEPRI_WILDCARD_STAR,
                   _RoutingKey = ?KHEPRI_WILDCARD_STAR],
    khepri:register_projection(?RA_CLUSTER_NAME, PathPattern, Projection).

register_rabbit_index_route_projection() ->
    MapFun = fun(Path, _) ->
                     [rabbit_db_binding, routes, VHost, ExchangeName, Kind, DstName,
                      RoutingKey] = Path,
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
    DirectOrFanout = #if_data_matches{pattern = #{type => '$1'},
                                      conditions = [{'andalso',
                                                     {'=/=', '$1', headers},
                                                     {'=/=', '$1', topic}}]},
    PathPattern = [rabbit_db_binding,
                   routes,
                   _VHost = ?KHEPRI_WILDCARD_STAR,
                   _Exchange = DirectOrFanout,
                   _Kind = ?KHEPRI_WILDCARD_STAR,
                   _DstName = ?KHEPRI_WILDCARD_STAR,
                   _RoutingKey = ?KHEPRI_WILDCARD_STAR],
    khepri:register_projection(?RA_CLUSTER_NAME, PathPattern, Projection).

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
        (rabbit_guid, gen, 0, _From) ->
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
        [rabbit_db_binding, routes,
         VHost, ExchangeName, _Kind, _DstName, RoutingKey] = Path,
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
    PathPattern = [rabbit_db_binding,
                   routes,
                   _VHost = ?KHEPRI_WILDCARD_STAR,
                   _Exchange = #if_data_matches{pattern = #{type => topic}},
                   _Kind = ?KHEPRI_WILDCARD_STAR,
                   _DstName = ?KHEPRI_WILDCARD_STAR,
                   _RoutingKey = ?KHEPRI_WILDCARD_STAR],
    khepri:register_projection(?RA_CLUSTER_NAME, PathPattern, Projection).

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
                       %% Create the intermediary node.
                       NewNodeId = rabbit_guid:gen(),
                       NewEdge = #topic_trie_edge{trie_edge = TrieEdge,
                                                  node_id = NewNodeId},
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
        {no_more_servers_to_try, Reasons} = Err ->
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
