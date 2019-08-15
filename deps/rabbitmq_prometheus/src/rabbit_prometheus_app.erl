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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_prometheus_app).

-behaviour(application).
-export([start/2, stop/1]).

-behaviour(supervisor).
-export([init/1]).

-define(TCP_CONTEXT, rabbitmq_prometheus_tcp).
-define(TLS_CONTEXT, rabbitmq_prometheus_tls).
-define(DEFAULT_PORT, 15692).

start(_Type, _StartArgs) ->
    %% TCP listener uses prometheus.tcp.*.
    %% TLS listener uses prometheus.ssl.*
    start_configured_listener(),
    supervisor:start_link({local,?MODULE},?MODULE,[]).

stop(_State) ->
    unregister_all_contexts(),
    ok.

init(_) ->
    {ok, {{one_for_one, 3, 10}, []}}.

-spec start_configured_listener() -> ok.
start_configured_listener() ->
    Listeners = case {has_configured_tcp_listener(),
                      has_configured_tls_listener()} of
                    {false, false} ->
                        %% nothing is configured
                        [get_tcp_listener()];
                    {false, true} ->
                        [get_tls_listener()];
                    {true, false} ->
                        [get_tcp_listener()];
                    {true, true} ->
                        [get_tcp_listener(),
                         get_tls_listener()]
                end,
    [ start_listener(Listener) || Listener <- Listeners ].

has_configured_tcp_listener() ->
    has_configured_listener(tcp_config).

has_configured_tls_listener() ->
    has_configured_listener(ssl_config).

has_configured_listener(Key) ->
    case application:get_env(rabbitmq_prometheus, Key, undefined) of
        undefined -> false;
        _         -> true
    end.

get_tls_listener() ->
    {ok, Listener0} = application:get_env(rabbitmq_prometheus, ssl_config),
    [{ssl, true} | Listener0].

get_tcp_listener() ->
    application:get_env(rabbitmq_prometheus, tcp_config, []).

start_listener(Listener) ->
    {Type, ContextName, Protocol} = case is_tls(Listener) of
        true  -> {tls, ?TLS_CONTEXT, 'https/prometheus'};
        false -> {tcp, ?TCP_CONTEXT, 'http/prometheus'}
    end,
    {ok, _} = register_context(ContextName, Protocol, Listener),
    log_startup(Type, Listener).

register_context(ContextName, Protocol, Listener0) ->
    M0 = maps:from_list(Listener0),
    %% include default port if it's not provided in the config
    %% as Cowboy won't start if the port is missing
    M1 = maps:merge(#{port => ?DEFAULT_PORT,
                      protocol => Protocol}, M0),
    rabbit_web_dispatch:register_context_handler(
      ContextName, maps:to_list(M1), "",
      rabbit_prometheus_dispatcher:build_dispatcher(),
      "RabbitMQ Prometheus").

unregister_all_contexts() ->
    rabbit_web_dispatch:unregister_context(?TCP_CONTEXT),
    rabbit_web_dispatch:unregister_context(?TLS_CONTEXT).

log_startup(tcp, Listener) ->
    rabbit_log:info("Prometheus metrics: HTTP (non-TLS) listener started on port ~w", [port(Listener)]);
log_startup(tls, Listener) ->
    rabbit_log:info("Prometheus metrics: HTTPS listener started on port ~w", [port(Listener)]).


port(Listener) ->
    proplists:get_value(port, Listener, ?DEFAULT_PORT).

is_tls(Listener) ->
    case proplists:get_value(ssl, Listener) of
        undefined -> false;
        false     -> false;
        _         -> true
    end.
