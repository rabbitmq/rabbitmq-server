%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
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

start(_Type, _StartArgs) ->
    %% Modern TCP listener uses management.tcp.*.
    %% Legacy TCP (or TLS) listener uses management.listener.*.
    %% Modern TLS listener uses management.ssl.*
    start_configured_listener([], true),
    rabbit_mgmt_sup_sup:start_link().

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
    start_configured_listener(IgnoreApps, false).

-spec start_configured_listener([atom()], boolean()) -> ok.
start_configured_listener(IgnoreApps, NeedLogStartup) ->
    [ start_listener(Listener, IgnoreApps, NeedLogStartup)
      || Listener <- get_listeners_config() ].

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
            rabbit_log:warning("Management plugin: TCP, TLS and a legacy (management.listener.*) listener are all configured. "
                               "Only two listeners at a time are supported. "
                               "Ignoring the legacy listener"),
            [get_tcp_listener(),
             get_tls_listener()]
    end,
    maybe_disable_sendfile(Listeners).

maybe_disable_sendfile(Listeners) ->
    DisableSendfile = #{sendfile => false},
    lists:map(fun(Listener) ->
        CowboyOpts0 = maps:from_list(proplists:get_value(cowboy_opts, Listener, [])),

        [{cowboy_opts, maps:to_list(maps:merge(DisableSendfile, CowboyOpts0))} | lists:keydelete(cowboy_opts, 1, Listener)]
    end,
    Listeners).

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
    {ok, Listener} = application:get_env(rabbitmq_management, listener),
    Listener.

get_tls_listener() ->
    {ok, Listener0} = application:get_env(rabbitmq_management, ssl_config),
    [{ssl, true} | Listener0].

get_tcp_listener() ->
    application:get_env(rabbitmq_management, tcp_config, []).

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

register_context(ContextName, Listener0, IgnoreApps) ->
    M0 = maps:from_list(Listener0),
    %% include default port if it's not provided in the config
    %% as Cowboy won't start if the port is missing
    M1 = maps:merge(#{port => ?DEFAULT_PORT}, M0),
    rabbit_web_dispatch:register_context_handler(
      ContextName, maps:to_list(M1), "",
      rabbit_mgmt_dispatcher:build_dispatcher(IgnoreApps),
      "RabbitMQ Management").

unregister_all_contexts() ->
    rabbit_web_dispatch:unregister_context(?TCP_CONTEXT),
    rabbit_web_dispatch:unregister_context(?TLS_CONTEXT).

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
