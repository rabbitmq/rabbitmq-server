%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_khepri).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup/0,
         setup/1,
         add_member/1,
         members/0,
         locally_known_members/0,
         nodes/0,
         locally_known_nodes/0,
         get_store_id/0,
         machine_insert/3,
         insert/2,
         insert/3,
         match/1,
         match_with_props/1,
         get/1,
         get_with_props/1,
         exists/1,
         list/1,
         list_with_props/1,
         list_matching/2,
         list_matching_with_props/2,
         delete/1,
         dir/0,
         info/0,
         is_enabled/0,
         is_enabled/1,
         try_mnesia_or_khepri/2]).
-export([priv_reset/0]).

-compile({no_auto_import, [get/2]}).

-define(RA_SYSTEM, metadata_store). %% FIXME: Also hard-coded in rabbit.erl.
-define(RA_CLUSTER_NAME, ?RA_SYSTEM).
-define(RA_FRIENDLY_NAME, "RabbitMQ metadata store").
-define(STORE_NAME, ?RA_SYSTEM).
-define(MDSTORE_SARTUP_LOCK, {?MODULE, self()}).
-define(PT_KEY, ?MODULE).

%% -------------------------------------------------------------------
%% API wrapping Khepri.
%% -------------------------------------------------------------------

setup() ->
    setup(rabbit_prelaunch:get_context()).

setup(_) ->
    ok = ensure_ra_system_started(?RA_SYSTEM),
    case khepri:start(?RA_SYSTEM, ?RA_CLUSTER_NAME, ?RA_FRIENDLY_NAME) of
        {ok, ?STORE_NAME} ->
            ?LOG_DEBUG(
               "Khepri-based metadata store ready",
               [],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, _} = Error ->
            exit(Error)
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
            Ret1 = rpc:call(NewNode, rabbit_khepri, priv_reset, []),
            case Ret1 of
                ok ->
                    ?LOG_DEBUG(
                       "Adding remote node ~s to Khepri cluster \"~s\"",
                       [NewNode, ?RA_CLUSTER_NAME],
                       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                    ok = ensure_ra_system_started(?RA_SYSTEM),
                    Ret2 = khepri:add_member(
                             ?RA_SYSTEM, ?RA_CLUSTER_NAME, ?RA_FRIENDLY_NAME,
                             NewNode),
                    case Ret2 of
                        ok ->
                            ?LOG_DEBUG(
                               "Node ~s added to Khepri cluster \"~s\"",
                               [NewNode, ?RA_CLUSTER_NAME],
                               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                            ok = rpc:call(NewNode, rabbit, start, []);
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

priv_reset() ->
    ok = rabbit:stop(),
    {ok, _} = application:ensure_all_started(khepri),
    ok = ensure_ra_system_started(?RA_SYSTEM),
    ok = khepri:reset(?RA_SYSTEM, ?RA_CLUSTER_NAME).

ensure_ra_system_started(RaSystem) ->
    Default = ra_system:default_config(),
    MDStoreDir = filename:join(
                   [rabbit_mnesia:dir(), "metadata_store", node()]),
    RaSystemConfig = Default#{name => RaSystem,
                              data_dir => MDStoreDir,
                              wal_data_dir => MDStoreDir,
                              wal_max_size_bytes => 1024 * 1024,
                              names => ra_system:derive_names(RaSystem)},
    ?LOG_DEBUG(
       "Starting Ra system for Khepri with configuration:~n~p",
       [RaSystemConfig],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    case ra_system:start(RaSystemConfig) of
        {ok, _} ->
            ?LOG_DEBUG(
               "Ra system for Khepri ready",
               [],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, {already_started, _}} ->
            ?LOG_DEBUG(
               "Ra system for Khepri ready",
               [],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        Error ->
            ?LOG_ERROR(
               "Failed to start Ra system for Khepri: ~p",
               [Error],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            throw(Error)
    end.

members() ->
    khepri:members(?RA_CLUSTER_NAME).

locally_known_members() ->
    khepri:locally_known_members(?RA_CLUSTER_NAME).

nodes() ->
    khepri:nodes(?RA_CLUSTER_NAME).

locally_known_nodes() ->
    khepri:locally_known_nodes(?RA_CLUSTER_NAME).

get_store_id() ->
    ?STORE_NAME.

dir() ->
    filename:join(rabbit_mnesia:dir(), atom_to_list(?STORE_NAME)).

machine_insert(PathPattern, Data, Extra) ->
    khepri_machine:insert(?STORE_NAME, PathPattern, Data, Extra).

insert(Path, Data) ->
    khepri:insert(?STORE_NAME, Path, Data).

insert(Path, Data, Conditions) ->
    khepri:insert(?STORE_NAME, Path, Data, Conditions).

get(Path) ->
    khepri:get(?STORE_NAME, Path).

get_with_props(Path) ->
    khepri:get_with_props(?STORE_NAME, Path).

match(Path) ->
    khepri:match(?STORE_NAME, Path).

match_with_props(Path) ->
    khepri:match_with_props(?STORE_NAME, Path).

exists(Path) ->
    case match(Path) of
        {ok, #{Path := _}} -> true;
        _                  -> false
    end.

list(Path) ->
    khepri:list(?STORE_NAME, Path).

list_matching(Path, Pattern) ->
    khepri:list_matching(?STORE_NAME, Path, Pattern).

list_matching_with_props(Path, Pattern) ->
    khepri:list_matching_with_props(?STORE_NAME, Path, Pattern).

list_with_props(Path) ->
    khepri:list_with_props(?STORE_NAME, Path).

delete(Path) ->
    khepri:delete(?STORE_NAME, Path).

info() ->
    ok = setup(),
    khepri:info(?STORE_NAME).

%% -------------------------------------------------------------------
%% Raft-based metadata store (phase 1).
%% -------------------------------------------------------------------

is_enabled() ->
    rabbit_feature_flags:is_enabled(raft_based_metadata_store_phase1).

is_enabled(Blocking) ->
    rabbit_feature_flags:is_enabled(
      raft_based_metadata_store_phase1, Blocking) =:= true.

try_mnesia_or_khepri(MnesiaFun, KhepriFun) ->
    case rabbit_khepri:is_enabled(non_blocking) of
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
                            _ = rabbit_khepri:is_enabled(),
                            try_mnesia_or_khepri(MnesiaFun, KhepriFun);
                        false ->
                            erlang:raise(Class, Reason, Stacktrace)
                    end
            end
    end.

is_mnesia_table_covered_by_feature_flag(rabbit_vhost)            -> true;
is_mnesia_table_covered_by_feature_flag(rabbit_user)             -> true;
is_mnesia_table_covered_by_feature_flag(rabbit_user_permission)  -> true;
is_mnesia_table_covered_by_feature_flag(rabbit_topic_permission) -> true;
is_mnesia_table_covered_by_feature_flag(_)                       -> false.
