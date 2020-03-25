%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(INITIAL_CREDITS, 50000).
-define(CREDITS_REQUIRED_FOR_UNBLOCKING, 12500).
-define(FRAME_MAX, 131072).
-define(HEARTBEAT, 600).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Listeners} = application:get_env(rabbitmq_stream, tcp_listeners),
    NumTcpAcceptors = application:get_env(rabbitmq_stream, num_tcp_acceptors, 10),
    {ok, SocketOpts} = application:get_env(rabbitmq_stream, tcp_listen_options),
    Nodes = rabbit_mnesia:cluster_nodes(all),
    OsirisConf = #{nodes => Nodes},

    ServerConfiguration = #{
        initial_credits => application:get_env(rabbitmq_stream, initial_credits, ?INITIAL_CREDITS),
        credits_required_for_unblocking => application:get_env(rabbitmq_stream, credits_required_for_unblocking, ?CREDITS_REQUIRED_FOR_UNBLOCKING),
        frame_max => application:get_env(rabbit_stream, frame_max, ?FRAME_MAX),
        heartbeat => application:get_env(rabbit_stream, heartbeat, ?HEARTBEAT)
    },

    StreamManager = #{id => rabbit_stream_manager,
        type => worker,
        start => {rabbit_stream_manager, start_link, [OsirisConf]}},

    {ok, {{one_for_all, 10, 10},
            [StreamManager] ++
            listener_specs(fun tcp_listener_spec/1,
                [SocketOpts, ServerConfiguration, NumTcpAcceptors], Listeners)}}.

listener_specs(Fun, Args, Listeners) ->
    [Fun([Address | Args]) ||
        Listener <- Listeners,
        Address <- rabbit_networking:tcp_listener_addresses(Listener)].

tcp_listener_spec([Address, SocketOpts, Configuration, NumAcceptors]) ->
    rabbit_networking:tcp_listener_spec(
        rabbit_stream_listener_sup, Address, SocketOpts,
        ranch_tcp, rabbit_stream_protocol, Configuration,
        stream, NumAcceptors, "Stream TCP listener").

