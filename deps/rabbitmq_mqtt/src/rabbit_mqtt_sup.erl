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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_sup).
-behaviour(supervisor2).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([start_link/2, init/1]).

start_link(Listeners, []) ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, [Listeners]).

init([{Listeners, SslListeners0}]) ->
    NumTcpAcceptors = application:get_env(rabbitmq_mqtt, num_tcp_acceptors, 10),
    {ok, SocketOpts} = application:get_env(rabbitmq_mqtt, tcp_listen_options),
    {SslOpts, NumSslAcceptors, SslListeners}
        = case SslListeners0 of
              [] -> {none, 0, []};
              _  -> {rabbit_networking:ensure_ssl(),
                     application:get_env(rabbitmq_mqtt, num_ssl_acceptors, 10),
                     case rabbit_networking:poodle_check('MQTT') of
                         ok     -> SslListeners0;
                         danger -> []
                     end}
          end,
    {ok, {{one_for_all, 10, 10},
          [{rabbit_mqtt_retainer_sup,
            {rabbit_mqtt_retainer_sup, start_link, [{local, rabbit_mqtt_retainer_sup}]},
             transient, ?SUPERVISOR_WAIT, supervisor, [rabbit_mqtt_retainer_sup]} |
           listener_specs(fun tcp_listener_spec/1,
                          [SocketOpts, NumTcpAcceptors], Listeners) ++
           listener_specs(fun ssl_listener_spec/1,
                          [SocketOpts, SslOpts, NumSslAcceptors], SslListeners)]}}.

listener_specs(Fun, Args, Listeners) ->
    [Fun([Address | Args]) ||
        Listener <- Listeners,
        Address  <- rabbit_networking:tcp_listener_addresses(Listener)].

tcp_listener_spec([Address, SocketOpts, NumAcceptors]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_mqtt_listener_sup, Address, SocketOpts,
      transport(mqtt), rabbit_mqtt_connection_sup, [],
      mqtt, NumAcceptors, "MQTT TCP listener").

ssl_listener_spec([Address, SocketOpts, SslOpts, NumAcceptors]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_mqtt_listener_sup, Address, SocketOpts ++ SslOpts,
      transport('mqtt/ssl'), rabbit_mqtt_connection_sup, [],
      'mqtt/ssl', NumAcceptors, "MQTT TLS listener").

transport(Protocol) ->
    case Protocol of
        mqtt       -> ranch_tcp;
        'mqtt/ssl' -> ranch_ssl
    end.
