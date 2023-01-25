%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_sup).
-behaviour(supervisor).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_mqtt.hrl").

-export([start_link/2, init/1, stop_listeners/0]).

-define(TCP_PROTOCOL, 'mqtt').
-define(TLS_PROTOCOL, 'mqtt/ssl').

start_link(Listeners, []) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Listeners]).

init([{Listeners, SslListeners0}]) ->
    NumTcpAcceptors = application:get_env(?APP_NAME, num_tcp_acceptors, 10),
    ConcurrentConnsSups = application:get_env(?APP_NAME, num_conns_sups, 1),
    {ok, SocketOpts} = application:get_env(?APP_NAME, tcp_listen_options),
    {SslOpts, NumSslAcceptors, SslListeners}
        = case SslListeners0 of
              [] -> {none, 0, []};
              _  -> {rabbit_networking:ensure_ssl(),
                     application:get_env(?APP_NAME, num_ssl_acceptors, 10),
                     case rabbit_networking:poodle_check('MQTT') of
                         ok     -> SslListeners0;
                         danger -> []
                     end}
          end,
    %% Use separate process group scope per RabbitMQ node. This achieves a local-only
    %% process group which requires less memory with millions of connections.
    PgScope = list_to_atom(io_lib:format("~s_~s", [?PG_SCOPE, node()])),
    persistent_term:put(?PG_SCOPE, PgScope),
    {ok,
     {#{strategy => one_for_all,
        intensity => 10,
        period => 10},
      [
       #{id => PgScope,
         start => {pg, start_link, [PgScope]},
         restart => transient,
         shutdown => ?WORKER_WAIT,
         type => worker,
         modules => [pg]
        },
       #{
         id => rabbit_mqtt_retainer_sup,
         start => {rabbit_mqtt_retainer_sup, start_link,
                   [{local, rabbit_mqtt_retainer_sup}]},
         restart => transient,
         shutdown => ?SUPERVISOR_WAIT,
         type => supervisor,
         modules => [rabbit_mqtt_retainer_sup]
        }
       | listener_specs(
           fun tcp_listener_spec/1,
           [SocketOpts, NumTcpAcceptors, ConcurrentConnsSups],
           Listeners
          ) ++
       listener_specs(
         fun ssl_listener_spec/1,
         [SocketOpts, SslOpts, NumSslAcceptors, ConcurrentConnsSups],
         SslListeners
        )
      ]}}.

-spec stop_listeners() -> ok.
stop_listeners() ->
    _ = rabbit_networking:stop_ranch_listener_of_protocol(?TCP_PROTOCOL),
    _ = rabbit_networking:stop_ranch_listener_of_protocol(?TLS_PROTOCOL),
    ok.

%%
%% Implementation
%%

listener_specs(Fun, Args, Listeners) ->
    [
        Fun([Address | Args])
     || Listener <- Listeners,
        Address <- rabbit_networking:tcp_listener_addresses(Listener)
    ].

tcp_listener_spec([Address, SocketOpts, NumAcceptors, ConcurrentConnsSups]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_mqtt_listener_sup,
      Address,
      SocketOpts,
      transport(?TCP_PROTOCOL),
      rabbit_mqtt_reader,
      [],
      mqtt,
      NumAcceptors,
      ConcurrentConnsSups,
      worker,
      "MQTT TCP listener"
     ).

ssl_listener_spec([Address, SocketOpts, SslOpts, NumAcceptors, ConcurrentConnsSups]) ->
    rabbit_networking:tcp_listener_spec(
      rabbit_mqtt_listener_sup,
      Address,
      SocketOpts ++ SslOpts,
      transport(?TLS_PROTOCOL),
      rabbit_mqtt_reader,
      [],
      'mqtt/ssl',
      NumAcceptors,
      ConcurrentConnsSups,
      worker,
      "MQTT TLS listener"
     ).

transport(?TCP_PROTOCOL) ->
    ranch_tcp;
transport(?TLS_PROTOCOL) ->
    ranch_ssl.
