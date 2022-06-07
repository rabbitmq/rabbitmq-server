%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_khepri).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-export([setup/0,
         setup/1,
         add_member/2,
         remove_member/1,
         members/0,
         locally_known_members/0,
         nodes/0,
         locally_known_nodes/0,
         get_store_id/0,

         create/2,
         update/2,
         cas/3,

         get/1,
         get_data/1,
         match/1,
         match_and_get_data/1,
         tx_match_and_get_data/1,
         exists/1,
         find/2,
         list/1,
         list_child_nodes/1,
         list_child_data/1,

         put/2, put/3,
         clear_payload/1,
         delete/1,
         delete_or_fail/1,

         transaction/1,
         transaction/2,

         clear_store/0,

         dir/0,
         info/0,
         is_enabled/0,
         is_enabled/1,
         nodes_if_khepri_enabled/0,
         try_mnesia_or_khepri/2]).
-export([do_join/1]).

-ifdef(TEST).
-export([force_metadata_store/1,
         clear_forced_metadata_store/0]).
-endif.

-compile({no_auto_import, [get/1, get/2, nodes/0]}).

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
    RaServerConfig = #{cluster_name => ?RA_CLUSTER_NAME,
                       friendly_name => ?RA_FRIENDLY_NAME},
    case khepri:start(?RA_SYSTEM, RaServerConfig) of
        {ok, ?STORE_ID} ->
            ?LOG_DEBUG(
               "Khepri-based " ?RA_FRIENDLY_NAME " ready",
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, _} = Error ->
            exit(Error)
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
            ok
    end.

pick_node_in_cluster(Cluster) when is_list(Cluster) ->
    ?assertNotEqual([], Cluster),
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

    %% We don't verify the cluster membership before adding this node to the
    %% remote cluster because such a check would not be atomic: the membership
    %% could well change between the check and the actual join.

    ?LOG_DEBUG(
       "Adding this node (~p) to Khepri cluster \"~s\" through "
       "node ~p",
       [ThisNode, ?RA_CLUSTER_NAME, RemoteNode],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),

    %% Ensure the remote node is reachable before we add it.
    pong = net_adm:ping(RemoteNode),

    %% If the remote node to add is running RabbitMQ, we need to put
    %% it in maintenance mode at least. We remember that state to
    %% revive the node only if it was fully running before this code.
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

    Ret.

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
    case lists:member(NodeToRemove, CurrentNodes) of
        true ->
            %% Ensure the remote node is reachable before we remove it.
            pong = net_adm:ping(NodeToRemove),

            ?LOG_DEBUG(
               "Removing remote node ~s from Khepri cluster \"~s\"",
               [NodeToRemove, ?RA_CLUSTER_NAME],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok = ensure_ra_system_started(),
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
            end;
        false ->
            ?LOG_INFO(
               "Asked to remove node ~s from Khepri cluster \"~s\" but not "
               "member of it: ~p",
               [NodeToRemove, ?RA_CLUSTER_NAME, lists:sort(CurrentNodes)],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok
    end.

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

create(Path, Data) -> khepri:create(?STORE_ID, Path, Data).
update(Path, Data) -> khepri:update(?STORE_ID, Path, Data).
cas(Path, Pattern, Data) ->
    khepri:compare_and_swap(?STORE_ID, Path, Pattern, Data).

get(Path) ->
    case khepri:get(?STORE_ID, Path, #{expect_specific_node => true}) of
        {ok, Result} ->
            [PropsAndData] = maps:values(Result),
            {ok, PropsAndData};
        Error ->
            Error
    end.

get_data(Path) ->
    case get(Path) of
        {ok, #{data := Data}} -> {ok, Data};
        {ok, Result}          -> {error, {no_data, Result}};
        Error                 -> Error
    end.

match(Path) -> khepri:get(?STORE_ID, Path).

match_and_get_data(Path) ->
    Ret = match(Path),
    keep_data_only_in_result(Ret).

tx_match_and_get_data(Path) ->
    Ret = khepri_tx:get(Path),
    keep_data_only_in_result(Ret).

exists(Path) -> khepri:exists(?STORE_ID, Path).
find(Path, Condition) -> khepri:find(?STORE_ID, Path, Condition).

list(Path) -> khepri:list(?STORE_ID, Path).

list_child_nodes(Path) ->
    Options = #{expect_specific_node => true,
                include_child_names => true},
    case khepri:get(?STORE_ID, Path, Options) of
        {ok, Result} ->
            [#{child_names := ChildNames}] = maps:values(Result),
            {ok, ChildNames};
        Error ->
            Error
    end.

list_child_data(Path) ->
    Ret = list(Path),
    keep_data_only_in_result(Ret).

keep_data_only(Result) ->
    maps:fold(
      fun
          (Path, #{data := Data}, Acc) -> Acc#{Path => Data};
          (_, _, Acc)                  -> Acc
      end, #{}, Result).

keep_data_only_in_result({ok, Result}) ->
    Result1 = keep_data_only(Result),
    {ok, Result1};
keep_data_only_in_result(Error) ->
    Error.

clear_payload(Path) -> khepri:clear_payload(?STORE_ID, Path).
delete(Path) -> khepri:delete(?STORE_ID, Path).

delete_or_fail(Path) ->
    case khepri:delete(?STORE_ID, Path) of
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

transaction(Fun) ->
    transaction(Fun, auto).

transaction(Fun, ReadWrite) ->
    case khepri:transaction(?STORE_ID, Fun, ReadWrite) of
        {atomic, Result} -> Result;
        {aborted, Reason} -> throw({error, Reason})
    end.

clear_store() ->
    khepri:clear_store(?STORE_ID).

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
    case is_enabled(non_blocking) of
        true  -> nodes();
        false -> []
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
-endif.

try_mnesia_or_khepri(MnesiaFun, KhepriFun) ->
    case use_khepri() of
        true ->
            KhepriFun();
        false ->
            try
                MnesiaFun()
            catch
                Class:{Type, {no_exists, Table}} = Reason:Stacktrace
                  when Type =:= aborted orelse Type =:= error ->
                    case is_mnesia_table_covered_by_feature_flag(Table) of
                        true ->
                            %% We wait for the feature flag(s) to be enabled
                            %% or disabled (this is a blocking call) and
                            %% retry.
                            ?LOG_DEBUG(
                               "Mnesia function failed because table ~s "
                               "is gone or read-only; checking if the new "
                               "metadata store was enabled in parallel and "
                               "retry",
                               [Table]),
                            _ = is_enabled(),
                            try_mnesia_or_khepri(MnesiaFun, KhepriFun);
                        false ->
                            erlang:raise(Class, Reason, Stacktrace)
                    end
            end
    end.

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

is_mnesia_table_covered_by_feature_flag(rabbit_vhost)            -> true;
is_mnesia_table_covered_by_feature_flag(rabbit_user)             -> true;
is_mnesia_table_covered_by_feature_flag(rabbit_user_permission)  -> true;
is_mnesia_table_covered_by_feature_flag(rabbit_topic_permission) -> true;
is_mnesia_table_covered_by_feature_flag(_)                       -> false.
