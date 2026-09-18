%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_prometheus_app).

-include_lib("kernel/include/logger.hrl").


-behaviour(application).
-export([start/2, stop/1]).

-behaviour(supervisor).
-export([init/1]).

-ifdef(TEST).
-export([get_listeners_config/0, listeners_with_contexts/0]).
-endif.

-define(TCP_CONTEXT, rabbitmq_prometheus_tcp).
-define(TLS_CONTEXT, rabbitmq_prometheus_tls).
-define(CONTEXTS_KEY, active_listener_contexts).
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
    Listeners = listeners_with_contexts(),
    ok = application:set_env(rabbitmq_prometheus, ?CONTEXTS_KEY,
                             [Context || {Context, _} <- Listeners]),
    start_listeners(Listeners, []).

%% A later listener's registration failure must not leave an earlier
%% context registered with nothing owning it; contexts already started in
%% this call are unregistered before the failure is re-raised.
start_listeners([], _Started) ->
    ok;
start_listeners([{Context, Listener} | Rest], Started) ->
    try start_listener(Context, Listener) of
        ok -> start_listeners(Rest, [Context | Started])
    catch
        Class:Reason:Stacktrace ->
            _ = [rabbit_web_dispatch:unregister_context(C) || C <- Started],
            erlang:raise(Class, Reason, Stacktrace)
    end.

-spec listeners_with_contexts() -> [{atom(), [{atom(), any()}]}].
listeners_with_contexts() ->
    {Named, _} = lists:mapfoldl(
        fun(Listener, Seen) ->
            Base = case is_tls(Listener) of
                       true  -> ?TLS_CONTEXT;
                       false -> ?TCP_CONTEXT
                   end,
            N = maps:get(Base, Seen, 0),
            {{context_name(Base, N), Listener}, Seen#{Base => N + 1}}
        end, #{}, get_listeners_config()),
    Named.

%% Numbered rather than named after the port, because two listeners can share
%% one port on different interfaces.
-spec context_name(atom(), non_neg_integer()) -> atom().
context_name(Base, 0) ->
    Base;
context_name(Base, N) ->
    list_to_atom(atom_to_list(Base) ++ "_" ++ integer_to_list(N)).

get_listeners_config() ->
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
    Primary = case {TCPListenerConf, TLSListenerConf} of
        %% nothing is configured
        {[], []}     -> [tcp_listener([{port, ?DEFAULT_PORT}])];
        %% TLS only
        {[], Val}    -> [tls_listener(Val)];
        %% plain TCP only
        {Val, []}    -> [tcp_listener(Val)];
        %% both
        {Val0, Val1} -> [tcp_listener(Val0), tls_listener(Val1)]
    end,
    Primary ++ extra_listeners(TCPListenerConf, TLSListenerConf).

extra_listeners(TCPListenerConf, TLSListenerConf) ->
    [tcp_listener(with_address(Address, TCPListenerConf))
     || Address <- get_env(tcp_listeners, [])] ++
    [tls_listener(with_address(Address, TLSListenerConf))
     || Address <- get_env(ssl_listeners, [])].

with_address(Port, Config) when is_integer(Port) ->
    lists:keystore(port, 1, lists:keydelete(ip, 1, Config), {port, Port});
with_address({IP, Port}, Config) ->
    lists:keystore(ip, 1, with_address(Port, Config), {ip, IP}).

tcp_listener(Conf) ->
    maybe_disable_sendfile(Conf).

tls_listener(Conf) ->
    rabbit_ssl:wrap_password_opt(maybe_disable_sendfile([{ssl, true} | Conf])).

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

start_listener(ContextName, Listener0) ->
    {Type, Protocol} = case is_tls(Listener0) of
        true  -> {tls, 'https/prometheus'};
        false -> {tcp, 'http/prometheus'}
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
    Contexts = application:get_env(rabbitmq_prometheus, ?CONTEXTS_KEY,
                                   [?TCP_CONTEXT, ?TLS_CONTEXT]),
    _ = [rabbit_web_dispatch:unregister_context(Context) || Context <- Contexts],
    ok.

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
    ?LOG_INFO("Prometheus metrics: HTTP (non-TLS) listener started on port ~w", [port(Listener)]);
log_startup(tls, Listener) ->
    ?LOG_INFO("Prometheus metrics: HTTPS listener started on port ~w", [port(Listener)]).


port(Listener) ->
    proplists:get_value(port, Listener, ?DEFAULT_PORT).

is_tls(Listener) ->
    case proplists:get_value(ssl, Listener) of
        undefined -> false;
        false     -> false;
        _         -> true
    end.
