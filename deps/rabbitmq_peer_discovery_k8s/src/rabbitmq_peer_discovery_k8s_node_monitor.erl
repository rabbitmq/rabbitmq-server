%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Console.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
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
