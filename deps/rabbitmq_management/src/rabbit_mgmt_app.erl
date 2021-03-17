%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_app).

-behaviour(application).
-export([start/2, stop/1, reset_dispatcher/1]).

-ifdef(TEST).
-export([get_listeners_config/0]).
-endif.

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(TCP_CONTEXT, rabbitmq_management_tcp).
-define(TLS_CONTEXT, rabbitmq_management_tls).
-define(DEFAULT_PORT, 15672).
-define(DEFAULT_TLS_PORT, 15671).

-rabbit_boot_step({rabbit_management_load_definitions,
                   [{description, "Imports definition file at management.load_definitions"},
                    {mfa,         {rabbit_mgmt_load_definitions, boot, []}}]}).

start(_Type, _StartArgs) ->
    case application:get_env(rabbitmq_management_agent, disable_metrics_collector, false) of
        false ->
            start();
        true ->
            _ = rabbit_log:warning("Metrics collection disabled in management agent, "
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
    [start_listener(Listener, IgnoreApps, NeedLogStartup)
      || Listener <- get_listeners_config()],
    ok.

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
            _ = rabbit_log:warning("Management plugin: TCP, TLS and a legacy (management.listener.*) listener are all configured. "
                               "Only two listeners at a time are supported. "
                               "Ignoring the legacy listener"),
            [get_tcp_listener(),
             get_tls_listener()]
    end,
    maybe_disable_sendfile(Listeners).

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
    {ok, Listener1} = ensure_port(tls, Listener0),
    Port = proplists:get_value(port, Listener1),
     case proplists:get_value(cowboy_opts, Listener0) of
        undefined ->
             [
                 {port, Port},
                 {ssl, true},
                 {ssl_opts, Listener0}
             ];
        CowboyOpts ->
            Listener1 = lists:keydelete(cowboy_opts, 1, Listener0),
            [
                {port, Port},
                {ssl, true},
                {ssl_opts, Listener1},
                {cowboy_opts, CowboyOpts}
            ]
     end.

get_tcp_listener() ->
    Listener0 = application:get_env(rabbitmq_management, tcp_config, []),
    {ok, Listener1} = ensure_port(tcp, Listener0),
    Listener1.

start_listener(Listener, IgnoreApps, NeedLogStartup) ->
    {Type, ContextName} = case is_tls(Listener) of
        true  -> {tls, ?TLS_CONTEXT};
        false -> {tcp, ?TCP_CONTEXT}
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
    rabbit_web_dispatch:unregister_context(?TCP_CONTEXT),
    rabbit_web_dispatch:unregister_context(?TLS_CONTEXT).

ensure_port(tls, Listener) ->
    do_ensure_port(?DEFAULT_TLS_PORT, Listener);
ensure_port(tcp, Listener) ->
    do_ensure_port(?DEFAULT_PORT, Listener).

do_ensure_port(Port, Listener) ->
    %% include default port if it's not provided in the config
    %% as Cowboy won't start if the port is missing
    M0 = maps:from_list(Listener),
    M1 = maps:merge(#{port => Port}, M0),
    {ok, maps:to_list(M1)}.

log_startup(tcp, Listener) ->
    _ = rabbit_log:info("Management plugin: HTTP (non-TLS) listener started on port ~w", [port(Listener)]);
log_startup(tls, Listener) ->
    _ = rabbit_log:info("Management plugin: HTTPS listener started on port ~w", [port(Listener)]).


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
