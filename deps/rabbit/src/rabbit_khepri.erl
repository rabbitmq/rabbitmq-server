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
         get_store/0,
         insert/2,
         insert/3,
         get/1,
         list/1,
         delete/1,
         i/0]).

-compile({no_auto_import, [get/2]}).

-define(RA_SYSTEM, metadata_store). %% FIXME: Also hard-coded in rabbit.erl.
-define(MDSTORE_SARTUP_LOCK, {?MODULE, self()}).
-define(PT_KEY, ?MODULE).

setup(_) ->
    ClusterName = ?RA_SYSTEM,
    FriendlyName = "RabbitMQ metadata store",

    %% FIXME: We want to get rid of Mnesia cluster here :)
    Nodes = rabbit_mnesia:cluster_nodes(all),

    case khepri:new(?RA_SYSTEM, ClusterName, FriendlyName, Nodes) of
        {error, _} = Error ->
            exit(Error);
        Store ->
            ?LOG_DEBUG(
               "Khepri-based metadata store ready",
               [],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            persistent_term:put(?PT_KEY, Store)
    end.

get_store() ->
    persistent_term:get(?PT_KEY).

insert(Path, Object) ->
    Store = get_store(),
    khepri:insert(Store, Path, Object).

insert(Path, Object, Condition) ->
    Store = get_store(),
    khepri:insert(Store, Path, Object, Condition).

get(Path) ->
    Store = get_store(),
    khepri:get(Store, Path).

list(Path) ->
    Store = get_store(),
    khepri:list(Store, Path).

delete(Path) ->
    Store = get_store(),
    khepri:delete(Store, Path).

i() ->
    Store = get_store(),
    khepri:i(Store).
