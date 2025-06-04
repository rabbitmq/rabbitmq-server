%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
    TCPListenerConf = get_env(tcp_config, []),
    TLSListenerConf0 = get_env(ssl_config, []),
    TLSListenerConf =
        case proplists:get_value(ssl_opts, TLSListenerConf0, undefined) of
            undefined ->
                TLSListenerConf0;
            Opts0 ->
                Opts = rabbit_ssl:wrap_password_opt(Opts0),
                Tmp = proplists:delete(ssl_opts, TLSListenerConf0),
                [{ssl_opts, Opts} | Tmp]
        end,

    case {TCPListenerConf, TLSListenerConf} of
        %% nothing is configured
        {[], []}  -> start_default_tcp_listener();
        %% TLS only
        {[], Val} -> start_configured_tls_listener(Val);
        %% plain TCP only
        {Val, []} -> start_configured_tcp_listener(Val);
        %% both
        {Val0, Val1} ->
            start_configured_tcp_listener(Val0),
            start_configured_tls_listener(Val1)
    end,
    ok.

start_default_tcp_listener() ->
    start_configured_tcp_listener([{port, ?DEFAULT_PORT}]).

start_configured_tcp_listener(Conf) ->
    case Conf of
        [] -> ok;
        TCPCon ->
            TCPListener = maybe_disable_sendfile(TCPCon),
            start_listener(TCPListener)
    end.

start_configured_tls_listener(Conf) ->
    case Conf of
        [] -> ok;
        TLSConf ->
            TLSListener0 = [{ssl, true} | TLSConf],
            TLSListener1 = maybe_disable_sendfile(TLSListener0),
            TLSListener2 = rabbit_ssl:wrap_password_opt(TLSListener1),
            start_listener(TLSListener2)
    end.

maybe_disable_sendfile(Listener) ->
    DisableSendfile = #{sendfile => false},
    CowboyOptsL0 = proplists:get_value(cowboy_opts, Listener, []),
    CowboyOptsM0 = maps:from_list(CowboyOptsL0),
    CowboyOptsM1 = maps:merge(DisableSendfile, CowboyOptsM0),
    CowboyOptsL1 = maps:to_list(CowboyOptsM1),
    L1 = lists:keydelete(cowboy_opts, 1, Listener),
    [{cowboy_opts, CowboyOptsL1} | L1].

get_env(Key, Default) ->
    rabbit_misc:get_env(rabbitmq_prometheus, Key, Default).

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
    %% Include default port if it's not provided in the config
    %% as Cowboy won't start if the port is missing.
    %% Protocol is displayed in mgmt UI and CLI output.
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
