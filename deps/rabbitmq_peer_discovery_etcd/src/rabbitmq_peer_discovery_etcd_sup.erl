%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbitmq_peer_discovery_etcd_sup).

-behaviour(supervisor).

-export([init/1, start_link/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_peer_discovery_etcd.hrl").

%%
%% API
%%

init([]) ->
    Flags = #{strategy => one_for_one, intensity => 10, period => 1},
    Fun0 = fun() -> {ok, {Flags, []}} end,
    Fun1 = fun() -> {ok, {Flags, []}} end,
    Fun2 = fun(_) ->
            %% we stop the previously started client and "re-attach" it. MK.
            rabbitmq_peer_discovery_etcd_v3_client:stop(),
            Formation = application:get_env(rabbit, cluster_formation, []),
            Opts = maps:from_list(proplists:get_value(peer_discovery_etcd, Formation, [])),
            EtcdClientFSM = #{
                id => rabbitmq_peer_discovery_etcd_v3_client,
                start => {rabbitmq_peer_discovery_etcd_v3_client, start_link, [Opts]},
                restart => permanent,
                shutdown => ?SUPERVISOR_WAIT,
                type => worker,
                modules => [rabbitmq_peer_discovery_etcd_v3_client]
            },
            Specs = [
                EtcdClientFSM
            ],
            {ok, {Flags, Specs}}
           end,
    rabbit_peer_discovery_util:maybe_backend_configured(?BACKEND_CONFIG_KEY, Fun0, Fun1, Fun2).

start_link() ->
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        {ok, Pid}                       -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, _} = Err                -> Err
    end.
