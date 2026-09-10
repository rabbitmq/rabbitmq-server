%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_app).

-behaviour(application).
-export([start/2, stop/1, reset_dispatcher/1]).

-ifdef(TEST).
-export([get_listeners_config/0, listeners_with_contexts/0]).
-endif.

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("kernel/include/logger.hrl").

-define(TCP_CONTEXT, rabbitmq_management_tcp).
-define(TLS_CONTEXT, rabbitmq_management_tls).
-define(CONTEXTS_KEY, active_listener_contexts).
-define(DEFAULT_PORT, 15672).
-define(DEFAULT_TLS_PORT, 15671).

-rabbit_boot_step({rabbit_management_load_definitions,
                   [{description, "Imports definition file at management.load_definitions"},
                    {mfa,         {rabbit_mgmt_load_definitions, boot, []}}]}).

-rabbit_feature_flag(
   {detailed_queues_endpoint,
    #{desc          => "Add a detailed queues HTTP API endpoint. Reduce number of metrics in the default endpoint.",
      stability     => required,
      depends_on    => [feature_flags_v2]
     }}).

start(_Type, _StartArgs) ->
    case rabbit_mgmt_agent_config:is_metrics_collector_enabled() of
        true ->
            start();
        false ->
            ?LOG_WARNING("Metrics collection disabled in management agent, "
                               "management only interface started", []),
            start()
    end.

stop(_State) ->
    unregister_all_contexts(),
    ok.

%% At the point at which this is invoked we have both newly enabled
%% apps and about-to-disable apps running (so that
%% rabbit_mgmt_reset_handler can look at all of them to find
%% extensions). Therefore we have to explicitly exclude
%% about-to-disable apps from our new dispatcher.
reset_dispatcher(IgnoreApps) ->
    unregister_all_contexts(),
    start_configured_listeners(IgnoreApps, false).

-spec start_configured_listeners([atom()], boolean()) -> ok.
start_configured_listeners(IgnoreApps, NeedLogStartup) ->
    Listeners = listeners_with_contexts(),
    ok = application:set_env(rabbitmq_management, ?CONTEXTS_KEY,
                             [Context || {Context, _} <- Listeners]),
    [start_listener(Context, Listener, IgnoreApps, NeedLogStartup)
      || {Context, Listener} <- Listeners],
    ok.

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
    Listeners = case {has_configured_legacy_listener(),
          has_configured_tcp_listener(),
          has_configured_tls_listener()} of
        {false, false, false} ->
            %% nothing is configured
            [get_tcp_listener()];
        {false, false, true} ->
            [get_tls_listener()];
        {false, true, false} ->
            [get_tcp_listener()];
        {false, true, true} ->
            [get_tcp_listener(),
             get_tls_listener()];
        {true,  false, false} ->
            [get_legacy_listener()];
        {true,  false, true} ->
            [get_legacy_listener(),
             get_tls_listener()];
        {true,  true, false}  ->
            %% This combination makes some sense:
            %% legacy listener can be used to set up TLS :/
            [get_legacy_listener(),
             get_tcp_listener()];
        {true,  true, true}  ->
            %% what is happening?
            ?LOG_WARNING("Management plugin: TCP, TLS and a legacy (management.listener.*) listener are all configured. "
                               "Only two listeners at a time are supported. "
                               "Ignoring the legacy listener"),
            [get_tcp_listener(),
             get_tls_listener()]
    end,
    maybe_disable_sendfile(Listeners ++ extra_listeners()).

extra_listeners() ->
    TcpConfig = application:get_env(rabbitmq_management, tcp_config, []),
    [tcp_listener(with_address(Address, TcpConfig))
     || Address <- application:get_env(rabbitmq_management, tcp_listeners, [])] ++
    [extra_tls_listener(Address)
     || Address <- application:get_env(rabbitmq_management, ssl_listeners, [])].

extra_tls_listener(Address) ->
    case application:get_env(rabbitmq_management, ssl_config) of
        {ok, SslConfig} -> tls_listener(with_address(Address, SslConfig));
        undefined       -> [{ssl, true} | with_address(Address, [])]
    end.

with_address(Port, Config) when is_integer(Port) ->
    lists:keystore(port, 1, lists:keydelete(ip, 1, Config), {port, Port});
with_address({IP, Port}, Config) ->
    lists:keystore(ip, 1, with_address(Port, Config), {ip, IP}).

maybe_disable_sendfile(Listeners) ->
    DisableSendfile = [{sendfile, false}],
    F = fun(L0) ->
                CowboyOptsL0 = proplists:get_value(cowboy_opts, L0, []),
                CowboyOptsL1 = rabbit_misc:plmerge(DisableSendfile, CowboyOptsL0),
                L1 = lists:keydelete(cowboy_opts, 1, L0),
                [{cowboy_opts, CowboyOptsL1}|L1]
        end,
    lists:map(F, Listeners).

has_configured_legacy_listener() ->
    has_configured_listener(listener).

has_configured_tcp_listener() ->
    has_configured_listener(tcp_config).

has_configured_tls_listener() ->
    has_configured_listener(ssl_config).

has_configured_listener(Key) ->
    case application:get_env(rabbitmq_management, Key, undefined) of
        undefined -> false;
        _         -> true
    end.

get_legacy_listener() ->
    {ok, Listener0} = application:get_env(rabbitmq_management, listener),
    {ok, Listener1} = ensure_port(tcp, Listener0),
    Listener1.

get_tls_listener() ->
    {ok, Listener0} = application:get_env(rabbitmq_management, ssl_config),
    tls_listener(Listener0).

tls_listener(Listener0) ->
    {ok, Listener1} = ensure_port(tls, Listener0),
    Listener2 = rabbit_ssl:wrap_password_opt(Listener1),
    Address = [{port, proplists:get_value(port, Listener1)} |
               [{ip, IP} || {ip, IP} <- Listener1]],
    case proplists:get_value(cowboy_opts, Listener0) of
        undefined ->
            Address ++ [{ssl, true},
                        {ssl_opts, Listener2}];
        CowboyOpts ->
            Address ++ [{ssl, true},
                        {ssl_opts, lists:keydelete(cowboy_opts, 1, Listener2)},
                        {cowboy_opts, CowboyOpts}]
    end.

get_tcp_listener() ->
    tcp_listener(application:get_env(rabbitmq_management, tcp_config, [])).

tcp_listener(Listener0) ->
    {ok, Listener1} = ensure_port(tcp, Listener0),
    Listener1.

start_listener(ContextName, Listener, IgnoreApps, NeedLogStartup) ->
    Type = case is_tls(Listener) of
        true  -> tls;
        false -> tcp
    end,
    {ok, _} = register_context(ContextName, Listener, IgnoreApps),
    case NeedLogStartup of
        true  -> log_startup(Type, Listener);
        false -> ok
    end,
    ok.

register_context(ContextName, Listener, IgnoreApps) ->
    Dispatcher = rabbit_mgmt_dispatcher:build_dispatcher(IgnoreApps),
    rabbit_web_dispatch:register_context_handler(
      ContextName, Listener, "",
      Dispatcher, "RabbitMQ Management").

unregister_all_contexts() ->
    Contexts = application:get_env(rabbitmq_management, ?CONTEXTS_KEY,
                                   [?TCP_CONTEXT, ?TLS_CONTEXT]),
    _ = [rabbit_web_dispatch:unregister_context(Context) || Context <- Contexts],
    ok.

ensure_port(tls, Listener) ->
    do_ensure_port(?DEFAULT_TLS_PORT, Listener);
ensure_port(tcp, Listener) ->
    do_ensure_port(?DEFAULT_PORT, Listener).

do_ensure_port(Port, Listener) ->
    %% include default port if it's not provided in the config
    %% as Cowboy won't start if the port is missing
    {ok, rabbit_misc:plmerge([{port, Port}], Listener)}.

log_startup(tcp, Listener) ->
    ?LOG_INFO("Management plugin: HTTP (non-TLS) listener started on port ~w", [port(Listener)]);
log_startup(tls, Listener) ->
    ?LOG_INFO("Management plugin: HTTPS listener started on port ~w", [port(Listener)]).


port(Listener) ->
    proplists:get_value(port, Listener, ?DEFAULT_PORT).

is_tls(Listener) ->
    case proplists:get_value(ssl, Listener) of
        undefined -> false;
        false     -> false;
        _         -> true
    end.

start() ->
    %% Modern TCP listener uses management.tcp.*.
    %% Legacy TCP (or TLS) listener uses management.listener.*.
    %% Modern TLS listener uses management.ssl.*
    start_configured_listeners([], true),
    rabbit_mgmt_sup_sup:start_link().
