%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbitmq_peer_discovery_k8s_sup).

-behaviour(supervisor).

-export([init/1, start_link/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_peer_discovery_k8s.hrl").

%%
%% API
%%

init([]) ->
    Flags = #{strategy => one_for_one, intensity => 1, period => 1},
    Fun0 = fun() -> {ok, {Flags, []}} end,
    Fun1 = fun() -> {ok, {Flags, []}} end,
    Fun2 = fun(_) ->
                   Specs = [#{id       => rabbitmq_peer_discovery_k8s_node_monitor,
                              start    => {rabbitmq_peer_discovery_k8s_node_monitor, start_link, []},
                              restart  => permanent,
                              shutdown => ?SUPERVISOR_WAIT,
                              type     => worker,
                              modules  => [rabbitmq_peer_discovery_k8s]
                             }],
                   {ok, {Flags, Specs}}
           end,
    rabbit_peer_discovery_util:maybe_backend_configured(?BACKEND_CONFIG_KEY, Fun0, Fun1, Fun2).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
