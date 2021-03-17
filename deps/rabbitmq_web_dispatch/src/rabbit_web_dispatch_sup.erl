%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_dispatch_sup).

-behaviour(supervisor).

-define(SUP, ?MODULE).

%% External exports
-export([start_link/0, ensure_listener/1, stop_listener/1]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

ensure_listener(Listener) ->
    case proplists:get_value(port, Listener) of
        undefined ->
            {error, {no_port_given, Listener}};
        _ ->
            {Transport, TransportOpts, ProtoOpts} = preprocess_config(Listener),
            ProtoOptsMap = maps:from_list(ProtoOpts),
            StreamHandlers = stream_handlers_config(ProtoOpts),
            _ = rabbit_log:debug("Starting HTTP[S] listener with transport ~s, options ~p and protocol options ~p, stream handlers ~p",
                             [Transport, TransportOpts, ProtoOptsMap, StreamHandlers]),
            CowboyOptsMap =
                maps:merge(#{env =>
                                #{rabbit_listener => Listener},
                             middlewares =>
                                [rabbit_cowboy_middleware, cowboy_router, cowboy_handler],
                             stream_handlers => StreamHandlers},
                           ProtoOptsMap),
            Child = ranch:child_spec(rabbit_networking:ranch_ref(Listener),
                Transport, TransportOpts,
                cowboy_clear, CowboyOptsMap),
            case supervisor:start_child(?SUP, Child) of
                {ok,                      _}  -> new;
                {error, {already_started, _}} -> existing;
                {error, {E, _}}               -> check_error(Listener, E)
            end
    end.

stop_listener(Listener) ->
    Name = rabbit_networking:ranch_ref(Listener),
    ok = supervisor:terminate_child(?SUP, {ranch_listener_sup, Name}),
    ok = supervisor:delete_child(?SUP, {ranch_listener_sup, Name}).

%% @spec init([[instance()]]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    Registry = {rabbit_web_dispatch_registry,
                {rabbit_web_dispatch_registry, start_link, []},
                transient, 5000, worker, dynamic},
    Log = {rabbit_mgmt_access_logger, {gen_event, start_link,
            [{local, webmachine_log_event}]},
           permanent, 5000, worker, [dynamic]},
    {ok, {{one_for_one, 10, 10}, [Registry, Log]}}.

%%
%% Implementation
%%

preprocess_config(Options) ->
    case proplists:get_value(ssl, Options) of
        true -> _ = rabbit_networking:ensure_ssl(),
                case rabbit_networking:poodle_check('HTTP') of
                    ok     -> case proplists:get_value(ssl_opts, Options) of
                                  undefined -> auto_ssl(Options);
                                  _         -> fix_ssl(Options)
                              end;
                    danger -> {ranch_tcp, transport_config(Options), protocol_config(Options)}
                end;
        _    -> {ranch_tcp, transport_config(Options), protocol_config(Options)}
    end.

auto_ssl(Options) ->
    {ok, ServerOpts} = application:get_env(rabbit, ssl_options),
    Remove = [verify, fail_if_no_peer_cert],
    SSLOpts = [{K, V} || {K, V} <- ServerOpts,
                         not lists:member(K, Remove)],
    fix_ssl([{ssl_opts, SSLOpts} | Options]).

fix_ssl(Options) ->
    SSLOpts = proplists:get_value(ssl_opts, Options),
    {ranch_ssl,
        transport_config(Options ++ rabbit_networking:fix_ssl_options(SSLOpts)),
        protocol_config(Options)}.

transport_config(Options0) ->
    Options = proplists:delete(protocol,
        proplists:delete(ssl,
        proplists:delete(ssl_opts,
            proplists:delete(cowboy_opts,
                Options0)))),
    case proplists:get_value(ip, Options) of
        undefined ->
            Options;
        IP when is_tuple(IP) ->
            Options;
        IP when is_list(IP) ->
            {ok, ParsedIP} = inet_parse:address(IP),
            [{ip, ParsedIP}|proplists:delete(ip, Options)]
    end.

protocol_config(Options) ->
    proplists:get_value(cowboy_opts, Options, []).

stream_handlers_config(Options) ->
    case lists:keyfind(compress, 1, Options) of
        {compress, false} -> [rabbit_cowboy_stream_h, cowboy_stream_h];
        %% Compress by default. Since 2.0 the compress option in cowboys
        %% has been replaced by the cowboy_compress_h handler
        %% Compress is not applied if data < 300 bytes
        _ -> [rabbit_cowboy_stream_h, cowboy_compress_h, cowboy_stream_h]
    end.

check_error(Listener, Error) ->
    Ignore = proplists:get_value(ignore_in_use, Listener, false),
    case {Error, Ignore} of
        {eaddrinuse, true} -> ignore;
        _                  -> exit({could_not_start_listener, Listener, Error})
    end.
