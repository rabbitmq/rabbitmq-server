%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This gen_server receives node monitoring events from net_kernel
%% and forwards them to the Kubernetes API.

-module(rabbitmq_peer_discovery_k8s_node_monitor).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%
%% API
%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    _ = net_kernel:monitor_nodes(true, []),
    {ok, #{}}.

handle_call(_Msg, _From, State) ->
    {reply, not_understood, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, Node}, State) ->
    Details = io_lib:format("Node ~s is up ", [Node]),
    rabbit_peer_discovery_k8s:send_event("Normal", "NodeUp", Details),
    {noreply, State};
handle_info({nodedown, Node}, State) ->
    Details = io_lib:format("Node ~s is down or disconnected ", [Node]),
    rabbit_peer_discovery_k8s:send_event("Warning", "NodeDown", Details),
    {noreply, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
