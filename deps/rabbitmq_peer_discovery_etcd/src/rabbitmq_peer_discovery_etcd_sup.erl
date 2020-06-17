%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   Copyright (c) 2007-2020 VMware, Inc. or its affiliates. All rights reserved.
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
