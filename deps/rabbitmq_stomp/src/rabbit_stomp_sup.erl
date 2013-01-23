%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_stomp_sup).
-behaviour(supervisor).

-export([start_link/2, init/1]).

-export([start_client/2, start_ssl_client/3]).

start_link(Listeners, Configuration) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE,
                          [Listeners, Configuration]).

init([{Listeners, SslListeners}, Configuration]) ->
    {ok, SocketOpts} = application:get_env(rabbitmq_stomp, tcp_listen_options),

    SslOpts = case SslListeners of
                  [] -> none;
                  _  -> rabbit_networking:ensure_ssl()
              end,

    {ok, {{one_for_all, 10, 10},
          [{rabbit_stomp_client_sup_sup,
            {rabbit_client_sup, start_link,
             [{local, rabbit_stomp_client_sup_sup},
              {rabbit_stomp_client_sup, start_link,[]}]},
            transient, infinity, supervisor, [rabbit_client_sup]} |
           listener_specs(fun tcp_listener_spec/1,
                          [SocketOpts, Configuration], Listeners) ++
           listener_specs(fun ssl_listener_spec/1,
                          [SocketOpts, SslOpts, Configuration], SslListeners)]}}.

listener_specs(Fun, Args, Listeners) ->
    [Fun([Address | Args]) ||
        Listener <- Listeners,
        Address  <- rabbit_networking:tcp_listener_addresses(Listener)].

tcp_listener_spec([Address, SocketOpts, Configuration]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_stomp_listener_sup, Address, SocketOpts,
      stomp, "STOMP TCP Listener",
      {?MODULE, start_client, [Configuration]}).

ssl_listener_spec([Address, SocketOpts, SslOpts, Configuration]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_stomp_listener_sup, Address, SocketOpts,
      'stomp/ssl', "STOMP SSL Listener",
      {?MODULE, start_ssl_client, [Configuration, SslOpts]}).

start_client(Configuration, Sock, SockTransform) ->
    {ok, _Child, Reader} = supervisor:start_child(rabbit_stomp_client_sup_sup,
                                                  [Configuration]),
    ok = rabbit_net:controlling_process(Sock, Reader),
    Reader ! {go, Sock, SockTransform},

    %% see comment in rabbit_networking:start_client/2
    gen_event:which_handlers(error_logger),

    Reader.

start_client(Configuration, Sock) ->
    start_client(Configuration, Sock, fun (S) -> {ok, S} end).

start_ssl_client(Configuration, SslOpts, Sock) ->
    Transform = rabbit_networking:ssl_transform_fun(SslOpts),
    start_client(Configuration, Sock, Transform).
