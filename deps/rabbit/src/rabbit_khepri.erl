%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_khepri).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1,
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
         i/0]).

-compile({no_auto_import, [get/2]}).

-define(RA_SYSTEM, metadata_store). %% FIXME: Also hard-coded in rabbit.erl.
-define(STORE_NAME, ?RA_SYSTEM).
-define(MDSTORE_SARTUP_LOCK, {?MODULE, self()}).
-define(PT_KEY, ?MODULE).

setup(_) ->
    ClusterName = ?RA_SYSTEM,
    FriendlyName = "RabbitMQ metadata store",

    %% FIXME: We want to get rid of Mnesia cluster here :)
    Nodes = rabbit_mnesia:cluster_nodes(all),

    case khepri:new(?RA_SYSTEM, ClusterName, FriendlyName, Nodes) of
        {ok, ?STORE_NAME} ->
            ?LOG_DEBUG(
               "Khepri-based metadata store ready",
               [],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, _} = Error ->
            exit(Error)
    end.

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

i() ->
    khepri:i(?STORE_NAME).
