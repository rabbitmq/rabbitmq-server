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
         join_cluster/1,
         add_member/1,
         remove_member/1,
         members/0,
         locally_known_members/0,
         nodes/0,
         locally_known_nodes/0,
         get_store_id/0,

         create/2,
         insert/2,
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
-export([priv_reset/0]).

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
    case khepri:start(?RA_SYSTEM, ?RA_CLUSTER_NAME, ?RA_FRIENDLY_NAME) of
        {ok, ?STORE_ID} ->
            ?LOG_DEBUG(
               "Khepri-based " ?RA_FRIENDLY_NAME " ready",
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, _} = Error ->
            exit(Error)
    end.

%% TODO: Move this logic to Khepri itself, it could be useful to any user,
%% right?
-define(wait(Code), wait_for_leader(fun() -> Code end, 20000)).

wait_for_leader(Fun, Timeout) ->
    T0 = erlang:monotonic_time(),
    case Fun() of
        {ok, _} = Ret->
            Ret;
        {error, ra_leader_unknown} when Timeout >= 0 ->
            ?LOG_INFO(
               "Waiting for " ?RA_FRIENDLY_NAME " leader to be elected "
               "for ~b milliseconds before retrying Khepri call",
               [Timeout],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            timer:sleep(500),
            T1 = erlang:monotonic_time(),
            TDiff = erlang:convert_time_unit(T1 - T0, native, millisecond),
            TimeLeft = Timeout - TDiff,
            wait_for_leader(Fun, TimeLeft);
        {error, ra_leader_unknown} = Error ->
            ?LOG_ERROR(
               "Khepri call imeout while waiting for " ?RA_FRIENDLY_NAME " "
               "leader to be elected",
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            Error;
        Error ->
            Error
    end.

join_cluster(RemoteNode) when RemoteNode =/= node() ->
    ThisNode = node(),
    ?LOG_INFO(
      "Khepri clustering: Attempt to add node ~p to Khepri cluster through "
      "node ~p",
      [ThisNode, RemoteNode],
      #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    Ret = rabbit_misc:rpc_call(
            RemoteNode, rabbit_khepri, add_member, [ThisNode]),
    case Ret of
        ok ->
            ?LOG_INFO(
              "Khepri clustering: Node ~p successfully added to Khepri "
              "cluster through node ~p",
              [ThisNode, RemoteNode],
              #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        Error ->
            ?LOG_INFO(
              "Khepri clustering: Failed to add node ~p to Khepri cluster "
              "through ~p: ~p",
              [ThisNode, RemoteNode, Error],
              #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            Error
    end.

add_member(NewNode) when NewNode =/= node() ->
    ?LOG_DEBUG(
       "Trying to add node ~s to Khepri cluster \"~s\" from node ~s",
       [NewNode, ?RA_CLUSTER_NAME, node()],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),

    %% Check if the node is already part of the cluster. We query the local Ra
    %% server only, in case the cluster can't elect a leader right now.
    CurrentNodes = locally_known_nodes(),
    case lists:member(NewNode, CurrentNodes) of
        false ->
            %% Ensure the remote node is reachable before we add it.
            pong = net_adm:ping(NewNode),

            ?LOG_DEBUG(
               "Resetting Khepri on remote node ~s",
               [NewNode],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            %% If the remote node to add is running RabbitMQ, we need to put
            %% it in maintenance mode at least. We remember that state to
            %% revive the node only if it was fully running before this code.
            RemoteIsRunning = rabbit:is_running(NewNode),
            RemoteAlreadyBeingDrained =
            rabbit_maintenance:is_being_drained_consistent_read(NewNode),
            NeedToReviveRemote =
            RemoteIsRunning andalso RemoteAlreadyBeingDrained,
            case RemoteIsRunning of
                true ->
                    ok = rabbit_misc:rpc_call(
                           NewNode, rabbit_maintenance, drain, []);
                false ->
                    ok
            end,
            Ret1 = rabbit_misc:rpc_call(
                     NewNode, rabbit_khepri, priv_reset, []),
            case Ret1 of
                ok ->
                    ?LOG_DEBUG(
                       "Adding remote node ~s to Khepri cluster \"~s\"",
                       [NewNode, ?RA_CLUSTER_NAME],
                       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                    ok = ensure_ra_system_started(),
                    Ret2 = khepri:add_member(
                             ?RA_SYSTEM, ?RA_CLUSTER_NAME, ?RA_FRIENDLY_NAME,
                             NewNode),
                    %% Revive the remote node if it was running and not under
                    %% maintenance before we changed the cluster membership.
                    case NeedToReviveRemote of
                        true ->
                            ok = rabbit_misc:rpc_call(
                                   NewNode, rabbit_maintenance, revive, []);
                        false ->
                            ok
                    end,
                    case Ret2 of
                        ok ->
                            ?LOG_DEBUG(
                               "Node ~s added to Khepri cluster \"~s\"",
                               [NewNode, ?RA_CLUSTER_NAME],
                               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                            ok;
                        {error, _} = Error ->
                            ?LOG_ERROR(
                               "Failed to add remote node ~s to Khepri "
                               "cluster \"~s\": ~p",
                               [NewNode, ?RA_CLUSTER_NAME, Error],
                               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                            Error
                    end;
                Error ->
                    ?LOG_ERROR(
                       "Failed to reset Khepri on remote node ~s: ~p",
                       [NewNode, Error],
                       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                    Error
            end;
        true ->
            ?LOG_INFO(
               "Asked to add node ~s to Khepri cluster \"~s\" but already a "
               "member of it: ~p",
               [NewNode, ?RA_CLUSTER_NAME, lists:sort(CurrentNodes)],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok
    end.

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
            Ret = khepri:remove_member(?RA_CLUSTER_NAME, NodeToRemove),
            %% FIXME: Stop the Ra server? Apparently it's still running after
            %% calling this function and answers queries (like the list of
            %% locally known members).
            case Ret of
                ok ->
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
            end;
        false ->
            ?LOG_INFO(
               "Asked to remove node ~s from Khepri cluster \"~s\" but not "
               "member of it: ~p",
               [NodeToRemove, ?RA_CLUSTER_NAME, lists:sort(CurrentNodes)],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok
    end.

priv_reset() ->
    %% To reset the content of Khepri, we need RabbitMQ to be under
    %% maintenance or stopped. The reason is we can't let RabbitMQ do any
    %% reads from or writes to Khepri because the content will be garbage.
    %%
    %% Stopping RabbitMQ would be done by the `stop_app' CLI command.
    %%
    %% If RabbitMQ is running, the node is put under maintenance by the
    %% `add_member/1' function above.
    ?assert(
       not rabbit:is_running() orelse
       rabbit_maintenance:is_being_drained_consistent_read(node())),
    ok = ensure_ra_system_started(),
    ok = khepri:reset(?RA_SYSTEM, ?RA_CLUSTER_NAME).

ensure_ra_system_started() ->
    {ok, _} = application:ensure_all_started(khepri),
    ok = rabbit_ra_systems:ensure_ra_system_started(?RA_SYSTEM).

members() ->
    khepri:members(?RA_CLUSTER_NAME).

locally_known_members() ->
    khepri:locally_known_members(?RA_CLUSTER_NAME).

nodes() ->
    khepri:nodes(?RA_CLUSTER_NAME).

locally_known_nodes() ->
    khepri:locally_known_nodes(?RA_CLUSTER_NAME).

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

create(Path, Data) -> ?wait(khepri:create(?STORE_ID, Path, Data)).
insert(Path, Data) -> ?wait(khepri:insert(?STORE_ID, Path, Data)).
update(Path, Data) -> ?wait(khepri:update(?STORE_ID, Path, Data)).
cas(Path, Pattern, Data) ->
    ?wait(khepri:compare_and_swap(?STORE_ID, Path, Pattern, Data)).

get(Path) ->
    case ?wait(khepri:get(?STORE_ID, Path, #{expect_specific_node => true})) of
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

match(Path) -> ?wait(khepri:get(?STORE_ID, Path)).

match_and_get_data(Path) ->
    Ret = match(Path),
    keep_data_only_in_result(Ret).

tx_match_and_get_data(Path) ->
    Ret = khepri_tx:get(Path),
    keep_data_only_in_result(Ret).

exists(Path) -> ?wait(khepri:exists(?STORE_ID, Path)).
find(Path, Condition) -> ?wait(khepri:find(?STORE_ID, Path, Condition)).

list(Path) -> ?wait(khepri:list(?STORE_ID, Path)).

list_child_nodes(Path) ->
    Options = #{expect_specific_node => true,
                include_child_names => true},
    case ?wait(khepri:get(?STORE_ID, Path, Options)) of
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

clear_payload(Path) -> ?wait(khepri:clear_payload(?STORE_ID, Path)).
delete(Path) -> ?wait(khepri:delete(?STORE_ID, Path)).

delete_or_fail(Path) ->
    case khepri_machine:delete(?STORE_ID, Path) of
        {ok, Result} ->
            case maps:size(Result) of
                0 -> {error, {node_not_found, #{}}};
                _ -> ok
            end;
        Error ->
            Error
    end.

put(PathPattern, Data) ->
    khepri_machine:put(?STORE_ID, PathPattern, ?DATA_PAYLOAD(Data)).

put(PathPattern, Data, Extra) ->
    khepri_machine:put(?STORE_ID, PathPattern, ?DATA_PAYLOAD(Data), Extra).

transaction(Fun) ->
    transaction(Fun, auto).

transaction(Fun, ReadWrite) ->
    ?wait(case khepri:transaction(?STORE_ID, Fun, ReadWrite) of
              {atomic, Result} -> Result;
              {aborted, Reason} -> throw({error, Reason})
          end).

clear_store() ->
    ?wait(khepri:clear_store(?STORE_ID)).

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
