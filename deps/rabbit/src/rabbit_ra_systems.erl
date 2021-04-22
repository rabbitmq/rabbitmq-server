%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ra_systems).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup/0,
         setup/1,
         all_ra_systems/0,
         ensure_ra_system_started/1]).

-type ra_system_name() :: atom().

-define(COORD_WAL_MAX_SIZE_B, 64_000_000).

-spec setup() -> ok | no_return().

setup() ->
    setup(rabbit_prelaunch:get_context()).

-spec setup(Context :: map()) -> ok | no_return().

setup(_) ->
    ?LOG_DEBUG("Starting Ra systems"),
    lists:foreach(fun ensure_ra_system_started/1, all_ra_systems()),
    ?LOG_DEBUG("Ra systems started"),
    ok.

-spec all_ra_systems() -> [ra_system_name()].

all_ra_systems() ->
    [quorum_queues,
     coordination].

-spec ensure_ra_system_started(ra_system_name()) -> ok | no_return().

ensure_ra_system_started(RaSystem) ->
    RaSystemConfig = get_config(RaSystem),
    ?LOG_DEBUG(
       "Starting Ra system called \"~s\" with configuration:~n~p",
       [RaSystem, RaSystemConfig],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    case ra_system:start(RaSystemConfig) of
        {ok, _} ->
            ?LOG_DEBUG(
               "Ra system \"~s\" ready",
               [RaSystem],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, {already_started, _}} ->
            ?LOG_DEBUG(
               "Ra system \"~s\" ready",
               [RaSystem],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        Error ->
            ?LOG_ERROR(
               "Failed to start Ra system \"~s\": ~p",
               [RaSystem, Error],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            throw(Error)
    end.

-spec get_config(ra_system_name()) -> ra_system:config().

get_config(quorum_queues = RaSystem) ->
    DefaultConfig = get_default_config(),
    DefaultConfig#{name => RaSystem}; % names => ra_system:derive_names(quorum)
get_config(coordination = RaSystem) ->
    DefaultConfig = get_default_config(),
    CoordDataDir = filename:join(
                     [rabbit_mnesia:dir(), "coordination", node()]),
    DefaultConfig#{name => RaSystem,
                   data_dir => CoordDataDir,
                   wal_data_dir => CoordDataDir,
                   wal_max_size_bytes => ?COORD_WAL_MAX_SIZE_B,
                   names => ra_system:derive_names(RaSystem)}.

-spec get_default_config() -> ra_system:config().

get_default_config() ->
    ra_system:default_config().
