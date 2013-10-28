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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_sup).
-behaviour(supervisor2).

-define(MAX_WAIT, 16#ffffffff).

-export([start_link/2, init/1]).

-export([start_client/1, start_ssl_client/2]).

start_link(Listeners, []) ->
    {ok, SupPid} = supervisor:start_link({local, ?MODULE}, ?MODULE,
                                         [Listeners]),
    {ok, _Collector} =
        supervisor2:start_child(
          SupPid,
          {collector, {rabbit_mqtt_collector, start_link, []},
          transient, ?MAX_WAIT, worker, [rabbit_mqtt_collector]}),
    {ok, SupPid}.

init([{Listeners, SslListeners}]) ->
    {ok, SocketOpts} = application:get_env(rabbitmq_mqtt, tcp_listen_options),
    SslOpts = case SslListeners of
                  [] -> none;
                  _  -> rabbit_networking:ensure_ssl()
              end,
    {ok, {{one_for_all, 10, 10},
          [{rabbit_mqtt_client_sup,
            {rabbit_client_sup, start_link_worker,
             [{local, rabbit_mqtt_client_sup},
              {rabbit_mqtt_reader, start_link, []}]},
            transient, infinity, supervisor, [rabbit_client_sup]} |
           listener_specs(fun tcp_listener_spec/1,
                          [SocketOpts], Listeners) ++
           listener_specs(fun ssl_listener_spec/1,
                          [SocketOpts, SslOpts], SslListeners)]}}.

listener_specs(Fun, Args, Listeners) ->
    [Fun([Address | Args]) ||
        Listener <- Listeners,
        Address  <- rabbit_networking:tcp_listener_addresses(Listener)].

tcp_listener_spec([Address, SocketOpts]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_mqtt_listener_sup, Address, SocketOpts,
      mqtt, "MQTT TCP Listener",
      {?MODULE, start_client, []}).

ssl_listener_spec([Address, SocketOpts, SslOpts]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_stomp_listener_sup, Address, SocketOpts,
      'mqtt/ssl', "MQTT SSL Listener",
      {?MODULE, start_ssl_client, [SslOpts]}).

start_client(Sock, SockTransform) ->
    {ok, Reader} = supervisor:start_child(rabbit_mqtt_client_sup, []),
    ok = rabbit_net:controlling_process(Sock, Reader),
    ok = gen_server2:call(Reader, {go, Sock, SockTransform}, infinity),

    %% see comment in rabbit_networking:start_client/2
    gen_event:which_handlers(error_logger),
    Reader.

start_client(Sock) ->
    start_client(Sock, fun (S) -> {ok, S} end).

start_ssl_client(SslOpts, Sock) ->
    Transform = rabbit_networking:ssl_transform_fun(SslOpts),
    start_client(Sock, Transform).

