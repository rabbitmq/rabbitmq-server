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
         insert/2,
         insert/3,
         get/1,
         match/1,
         list/1,
         delete/1,
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

insert(Path, Data) ->
    khepri:insert(?STORE_NAME, Path, Data).

insert(Path, Data, Conditions) ->
    khepri:insert(?STORE_NAME, Path, Data, Conditions).

get(Path) ->
    khepri:get(?STORE_NAME, Path).

match(Path) ->
    khepri:match(?STORE_NAME, Path).

list(Path) ->
    khepri:list(?STORE_NAME, Path).

delete(Path) ->
    khepri:delete(?STORE_NAME, Path).

i() ->
    khepri:i(?STORE_NAME).
