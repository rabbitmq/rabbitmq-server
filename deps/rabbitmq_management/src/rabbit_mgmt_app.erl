%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_app).

-behaviour(application).
-export([start/2, stop/1, reset_dispatcher/1]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(CONTEXT, rabbit_mgmt).
-define(DEFAULT_PORT, 15672).

start(_Type, _StartArgs) ->
    %% Modern TCP listener uses management.tcp.*.
    %% Legacy TCP (or TLS) listener uses management.listener.*.
    %% Modern TLS listener uses management.ssl.*
    case {has_configured_legacy_listener(),
          has_configured_tcp_listener(),
          has_configured_tls_listener()} of
        {false, false, false} ->
            %% nothing is configured
            start_tcp_listener();
        {false, false, true} ->
            maybe_start_tls_listener();
        {false, true, false} ->
            maybe_start_tcp_listener();
        {false, true, true} ->
            maybe_start_tcp_listener(),
            maybe_start_tls_listener();
        {true,  false, false} ->
            maybe_start_legacy_listener();
        {true,  false, true} ->
            maybe_start_legacy_listener(),
            maybe_start_tls_listener();
        {true,  true, false}  ->
            %% This combination makes some sense:
            %% legacy listener can be used to set up TLS :/
            maybe_start_legacy_listener(),
            maybe_start_tcp_listener();
        {true,  true, true}  ->
            %% what is happening?
            maybe_start_tcp_listener(),
            maybe_start_legacy_listener(),
            maybe_start_tls_listener()
    end,

    rabbit_mgmt_sup_sup:start_link().

stop(_State) ->
    unregister_context(),
    ok.

has_configured_legacy_listener() ->
    has_configured_listener(listener).

has_configured_tcp_listener() ->
    has_configured_listener(tcp_config).

has_configured_tls_listener() ->
    has_configured_listener(ssl_config).

has_configured_listener(Key) ->
    case rabbit_misc:get_env(rabbitmq_management, Key, undefined) of
        undefined -> false;
        _         -> true
    end.

maybe_start_legacy_listener() ->
    case rabbit_misc:get_env(rabbitmq_management, listener, undefined) of
        undefined -> ok;
        Listener  ->
            {ok, _} = register_context(Listener, []),
            Type = case is_tls(Listener) of
                       true  -> tls;
                       false -> tcp
                   end,
            log_startup(Type, Listener),
            ok
    end.

maybe_start_tls_listener() ->
    case rabbit_misc:get_env(rabbitmq_management, ssl_config, undefined) of
        undefined  -> ok;
        Listener0  ->
            Listener = [{ssl, true} | Listener0],
            {ok, _}  = register_context(Listener, []),
            log_startup(tls, Listener),
            ok
    end.

maybe_start_tcp_listener() ->
    case rabbit_misc:get_env(rabbitmq_management, tcp_config, undefined) of
        undefined -> ok;
        Listener  ->
            {ok, _} = register_context(Listener, []),
            log_startup(tcp, Listener),
            ok
    end.

start_tcp_listener() ->
    Listener = rabbit_misc:get_env(rabbitmq_management, tcp_config, []),
    {ok, _} = register_context(Listener, []),
    log_startup(tcp, Listener),
    ok.

%% At the point at which this is invoked we have both newly enabled
%% apps and about-to-disable apps running (so that
%% rabbit_mgmt_reset_handler can look at all of them to find
%% extensions). Therefore we have to explicitly exclude
%% about-to-disable apps from our new dispatcher.
reset_dispatcher(IgnoreApps) ->
    unregister_context(),
    {ok, Listener} = application:get_env(rabbitmq_management, listener),
    register_context(Listener, IgnoreApps).

register_context(Listener0, IgnoreApps) ->
    M0 = maps:from_list(Listener0),
    %% include default port if it's not provided in the config
    %% as Cowboy won't start if the port is missing
    M1 = maps:merge(#{port => ?DEFAULT_PORT}, M0),
    rabbit_web_dispatch:register_context_handler(
      ?CONTEXT, maps:to_list(M1), "",
      rabbit_mgmt_dispatcher:build_dispatcher(IgnoreApps),
      "RabbitMQ Management").

unregister_context() ->
    rabbit_web_dispatch:unregister_context(?CONTEXT).

log_startup(tcp, Listener) ->
    rabbit_log:info("Management plugin: HTTP (non-TLS) listener started on port ~w", [port(Listener)]);
log_startup(tls, Listener) ->
    rabbit_log:info("Management plugin: HTTPS listener started on port ~w", [port(Listener)]).


port(Listener) ->
    proplists:get_value(port, Listener, ?DEFAULT_PORT).

is_tls(Listener) ->
    case proplists:get_value(ssl, Listener) of
        undefined -> false;
        false     -> false;
        _         -> true
    end.
