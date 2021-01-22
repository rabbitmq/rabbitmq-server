%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_prometheus_app).

-behaviour(application).
-export([start/2, stop/1]).

-behaviour(supervisor).
-export([init/1]).

-define(TCP_CONTEXT, rabbitmq_prometheus_tcp).
-define(TLS_CONTEXT, rabbitmq_prometheus_tls).
-define(DEFAULT_PORT, 15692).
-define(DEFAULT_TLS_PORT, 15691).

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
    Listeners0 = case {has_configured_tcp_listener(),
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
    Listeners1 = maybe_disable_sendfile(Listeners0),
    [start_listener(Listener) || Listener <- Listeners1].

maybe_disable_sendfile(Listeners) ->
    DisableSendfile = #{sendfile => false},
    F = fun(L0) ->
                CowboyOptsL0 = proplists:get_value(cowboy_opts, L0, []),
                CowboyOptsM0 = maps:from_list(CowboyOptsL0),
                CowboyOptsM1 = maps:merge(DisableSendfile, CowboyOptsM0),
                CowboyOptsL1 = maps:to_list(CowboyOptsM1),
                L1 = lists:keydelete(cowboy_opts, 1, L0),
                [{cowboy_opts, CowboyOptsL1}|L1]
        end,
    lists:map(F, Listeners).

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
     case proplists:get_value(cowboy_opts, Listener0) of
        undefined ->
             [{ssl, true}, {ssl_opts, Listener0}];
        CowboyOpts ->
            Listener1 = lists:keydelete(cowboy_opts, 1, Listener0),
            [{ssl, true}, {ssl_opts, Listener1}, {cowboy_opts, CowboyOpts}]
     end.

get_tcp_listener() ->
    application:get_env(rabbitmq_prometheus, tcp_config, []).

start_listener(Listener0) ->
    {Type, ContextName, Protocol} = case is_tls(Listener0) of
        true  -> {tls, ?TLS_CONTEXT, 'https/prometheus'};
        false -> {tcp, ?TCP_CONTEXT, 'http/prometheus'}
    end,
    {ok, Listener1} = ensure_port_and_protocol(Type, Protocol, Listener0),
    {ok, _} = register_context(ContextName, Listener1),
    log_startup(Type, Listener1).

register_context(ContextName, Listener) ->
    Dispatcher = rabbit_prometheus_dispatcher:build_dispatcher(),
    rabbit_web_dispatch:register_context_handler(
      ContextName, Listener, "",
      Dispatcher, "RabbitMQ Prometheus").

unregister_all_contexts() ->
    rabbit_web_dispatch:unregister_context(?TCP_CONTEXT),
    rabbit_web_dispatch:unregister_context(?TLS_CONTEXT).

ensure_port_and_protocol(tls, Protocol, Listener) ->
    do_ensure_port_and_protocol(?DEFAULT_TLS_PORT, Protocol, Listener);
ensure_port_and_protocol(tcp, Protocol, Listener) ->
    do_ensure_port_and_protocol(?DEFAULT_PORT, Protocol, Listener).

do_ensure_port_and_protocol(Port, Protocol, Listener) ->
    %% include default port if it's not provided in the config
    %% as Cowboy won't start if the port is missing
    M0 = maps:from_list(Listener),
    M1 = maps:merge(#{port => Port, protocol => Protocol}, M0),
    {ok, maps:to_list(M1)}.

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
