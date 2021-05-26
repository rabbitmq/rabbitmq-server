%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include("rabbit_stream.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Listeners} = application:get_env(rabbitmq_stream, tcp_listeners),
    NumTcpAcceptors =
        application:get_env(rabbitmq_stream, num_tcp_acceptors, 10),
    SocketOpts =
        application:get_env(rabbitmq_stream, tcp_listen_options, []),

    {ok, SslListeners0} =
        application:get_env(rabbitmq_stream, ssl_listeners),
    SslSocketOpts =
        application:get_env(rabbitmq_stream, ssl_listen_options, []),
    {SslOpts, NumSslAcceptors, SslListeners} =
        case SslListeners0 of
            [] ->
                {none, 0, []};
            _ ->
                {rabbit_networking:ensure_ssl(),
                 application:get_env(rabbitmq_stream, num_ssl_acceptors, 10),
                 case rabbit_networking:poodle_check('STREAM') of
                     ok ->
                         SslListeners0;
                     danger ->
                         []
                 end}
        end,

    Nodes = rabbit_mnesia:cluster_nodes(all),
    OsirisConf = #{nodes => Nodes},

    ServerConfiguration =
        #{initial_credits =>
              application:get_env(rabbitmq_stream, initial_credits,
                                  ?DEFAULT_INITIAL_CREDITS),
          credits_required_for_unblocking =>
              application:get_env(rabbitmq_stream,
                                  credits_required_for_unblocking,
                                  ?DEFAULT_CREDITS_REQUIRED_FOR_UNBLOCKING),
          frame_max =>
              application:get_env(rabbit_stream, frame_max, ?DEFAULT_FRAME_MAX),
          heartbeat =>
              application:get_env(rabbit_stream, heartbeat,
                                  ?DEFAULT_HEARTBEAT)},

    StreamManager =
        #{id => rabbit_stream_manager,
          type => worker,
          start => {rabbit_stream_manager, start_link, [OsirisConf]}},

    MetricsGc =
        #{id => rabbit_stream_metrics_gc_sup,
          type => worker,
          start => {rabbit_stream_metrics_gc, start_link, []}},

    {ok,
     {{one_for_all, 10, 10},
      [StreamManager, MetricsGc]
      ++ listener_specs(fun tcp_listener_spec/1,
                        [SocketOpts, ServerConfiguration, NumTcpAcceptors],
                        Listeners)
      ++ listener_specs(fun ssl_listener_spec/1,
                        [SslSocketOpts,
                         SslOpts,
                         ServerConfiguration,
                         NumSslAcceptors],
                        SslListeners)}}.

listener_specs(Fun, Args, Listeners) ->
    [Fun([Address | Args])
     || Listener <- Listeners,
        Address <- rabbit_networking:tcp_listener_addresses(Listener)].

tcp_listener_spec([Address,
                   SocketOpts,
                   Configuration,
                   NumAcceptors]) ->
    rabbit_networking:tcp_listener_spec(rabbit_stream_listener_sup,
                                        Address,
                                        SocketOpts,
                                        ranch_tcp,
                                        rabbit_stream_connection_sup,
                                        Configuration#{transport => tcp},
                                        stream,
                                        NumAcceptors,
                                        "Stream TCP listener").

ssl_listener_spec([Address,
                   SocketOpts,
                   SslOpts,
                   Configuration,
                   NumAcceptors]) ->
    rabbit_networking:tcp_listener_spec(rabbit_stream_listener_sup,
                                        Address,
                                        SocketOpts ++ SslOpts,
                                        ranch_ssl,
                                        rabbit_stream_connection_sup,
                                        Configuration#{transport => ssl},
                                        'stream/ssl',
                                        NumAcceptors,
                                        "Stream TLS listener").
