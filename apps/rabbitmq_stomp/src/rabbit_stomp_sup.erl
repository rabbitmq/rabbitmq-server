%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp_sup).
-behaviour(supervisor).

-export([start_link/2, init/1, stop_listeners/0]).

-define(TCP_PROTOCOL, 'stomp').
-define(TLS_PROTOCOL, 'stomp/ssl').

start_link(Listeners, Configuration) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE,
                          [Listeners, Configuration]).

init([{Listeners, SslListeners0}, Configuration]) ->
    NumTcpAcceptors = application:get_env(rabbitmq_stomp, num_tcp_acceptors, 10),
    ConcurrentConnsSups = application:get_env(rabbitmq_stomp, num_conns_sups, 1),
    {ok, SocketOpts} = application:get_env(rabbitmq_stomp, tcp_listen_options),
    {SslOpts, NumSslAcceptors, SslListeners}
        = case SslListeners0 of
              [] -> {none, 0, []};
              _  -> {rabbit_networking:ensure_ssl(),
                     application:get_env(rabbitmq_stomp, num_ssl_acceptors, 10),
                     case rabbit_networking:poodle_check('STOMP') of
                         ok     -> SslListeners0;
                         danger -> []
                     end}
          end,
    Flags = #{
        strategy => one_for_all,
        period => 10,
        intensity => 10
    },
    {ok, {Flags,
           listener_specs(fun tcp_listener_spec/1,
                          [SocketOpts, Configuration, NumTcpAcceptors, ConcurrentConnsSups],
                          Listeners) ++
           listener_specs(fun ssl_listener_spec/1,
                          [SocketOpts, SslOpts, Configuration, NumSslAcceptors, ConcurrentConnsSups],
                          SslListeners)}}.

stop_listeners() ->
    rabbit_networking:stop_ranch_listener_of_protocol(?TCP_PROTOCOL),
    rabbit_networking:stop_ranch_listener_of_protocol(?TLS_PROTOCOL),
    ok.

%%
%% Implementation
%%

listener_specs(Fun, Args, Listeners) ->
    [Fun([Address | Args]) ||
        Listener <- Listeners,
        Address  <- rabbit_networking:tcp_listener_addresses(Listener)].

tcp_listener_spec([Address, SocketOpts, Configuration, NumAcceptors, ConcurrentConnsSups]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_stomp_listener_sup, Address, SocketOpts,
      transport(?TCP_PROTOCOL), rabbit_stomp_client_sup, Configuration,
      stomp, NumAcceptors, ConcurrentConnsSups, "STOMP TCP listener").

ssl_listener_spec([Address, SocketOpts, SslOpts, Configuration, NumAcceptors, ConcurrentConnsSups]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_stomp_listener_sup, Address, SocketOpts ++ SslOpts,
      transport(?TLS_PROTOCOL), rabbit_stomp_client_sup, Configuration,
      'stomp/ssl', NumAcceptors, ConcurrentConnsSups, "STOMP TLS listener").

transport(Protocol) ->
    case Protocol of
        ?TCP_PROTOCOL -> ranch_tcp;
        ?TLS_PROTOCOL -> ranch_ssl
    end.
