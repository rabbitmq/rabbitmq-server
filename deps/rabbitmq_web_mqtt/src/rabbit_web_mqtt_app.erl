%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_web_mqtt_app).

-include_lib("kernel/include/logger.hrl").


-behaviour(application).
-export([
    start/2,
    prep_stop/1,
    stop/1,
    list_connections/0,
    emit_connection_info_all/4,
    emit_connection_info_local/3
]).

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.org/pipermail/erlang-questions/2010-April/050508.html
-behaviour(supervisor).
-export([init/1]).

-import(rabbit_misc, [pget/2]).

-define(TCP_PROTOCOL, 'http/web-mqtt').
-define(TLS_PROTOCOL, 'https/web-mqtt').

%%
%% API
%%

-spec start(_, _) -> {ok, pid()}.
start(_Type, _StartArgs) ->
    mqtt_init(),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec prep_stop(term()) -> term().
prep_stop(State) ->
    State.

-spec stop(_) -> ok.
stop(_State) ->
    rabbit_networking:stop_ranch_listeners_of_protocol(?TCP_PROTOCOL),
    rabbit_networking:stop_ranch_listeners_of_protocol(?TLS_PROTOCOL),
    ok.

init([]) -> {ok, {{one_for_one, 1, 5}, []}}.

-spec list_connections() -> [pid()].
list_connections() ->
    PlainPids = rabbit_networking:list_local_connections_of_protocol(?TCP_PROTOCOL),
    TLSPids   = rabbit_networking:list_local_connections_of_protocol(?TLS_PROTOCOL),
    PlainPids ++ TLSPids.

-spec emit_connection_info_all([node()], rabbit_types:info_keys(), reference(), pid()) -> term().
emit_connection_info_all(Nodes, Items, Ref, AggregatorPid) ->
    Pids = [spawn_link(Node, ?MODULE, emit_connection_info_local,
                       [Items, Ref, AggregatorPid])
            || Node <- Nodes],

    rabbit_control_misc:await_emitters_termination(Pids).

-spec emit_connection_info_local(rabbit_types:info_keys(), reference(), pid()) -> ok.
emit_connection_info_local(Items, Ref, AggregatorPid) ->
    LocalPids = list_connections(),
    emit_connection_info(Items, Ref, AggregatorPid, LocalPids).

emit_connection_info(Items, Ref, AggregatorPid, Pids) ->
    rabbit_control_misc:emitting_map_with_exit_handler(
      AggregatorPid, Ref,
      fun(Pid) ->
              rabbit_web_mqtt_handler:info(Pid, Items)
      end, Pids).
%%
%% Implementation
%%

mqtt_init() ->
    CowboyOpts0  = maps:from_list(get_env(cowboy_opts, [])),
    CowboyWsOpts = maps:from_list(get_env(cowboy_ws_opts, [])),
    TcpConfig = get_env(tcp_config, []),
    SslConfig = get_env(ssl_config, []),
    Routes = cowboy_router:compile([{'_', [
        {get_env(ws_path, "/ws"), rabbit_web_mqtt_handler, [{ws_opts, CowboyWsOpts}]}
    ]}]),
    CowboyOpts = CowboyOpts0#{
                 env => #{dispatch => Routes},
                 proxy_header => get_env(proxy_protocol, false),
                 stream_handlers => [rabbit_web_mqtt_stream_handler, cowboy_stream_h]
                },
    start_tcp_listener(TcpConfig, CowboyOpts),
    start_tls_listener(SslConfig, CowboyOpts).

start_tcp_listener([], _) -> ok;
start_tcp_listener(TCPConf0, CowboyOpts) ->
    TCPConf = get_tcp_conf(TCPConf0),
    _ = [start_tcp_listener_on(Bound, CowboyOpts)
         || Bound <- rabbit_networking:listener_per_ip_address(TCPConf)],
    listener_started(?TCP_PROTOCOL, TCPConf).

-spec start_tcp_listener_on([{atom(), any()}], map()) -> ok.
start_tcp_listener_on(TCPConf, CowboyOpts) ->
    Port = rabbit_misc:pget(port, TCPConf),
    RanchRef = rabbit_networking:ranch_ref(TCPConf),
    RanchTransportOpts =
    #{
      socket_opts => TCPConf,
      max_connections => get_max_connections(),
      num_acceptors => get_env(num_tcp_acceptors, 10),
      num_conns_sups => get_env(num_conns_sup, 1)
     },
    case cowboy:start_clear(RanchRef, RanchTransportOpts, CowboyOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, ErrTCP} ->
            ?LOG_ERROR(
              "Failed to start a WebSocket (HTTP) listener. Error: ~p, listener settings: ~p",
              [ErrTCP, TCPConf]),
            throw(ErrTCP)
    end,
    ?LOG_INFO("rabbit_web_mqtt: listening for HTTP connections on ~s:~w",
                    [binding_address(TCPConf), Port]).


start_tls_listener([], _) -> ok;
start_tls_listener(TLSConf0, CowboyOpts0) ->
    _ = rabbit_networking:ensure_ssl(),
    TLSConf = rabbit_networking:fix_ssl_options(get_tls_conf(TLSConf0)),
    _ = [start_tls_listener_on(Bound, CowboyOpts0)
         || Bound <- rabbit_networking:listener_per_ip_address(TLSConf)],
    listener_started(?TLS_PROTOCOL, TLSConf).

-spec start_tls_listener_on([{atom(), any()}], map()) -> ok.
start_tls_listener_on(TLSConf, CowboyOpts0) ->
    TLSPort = rabbit_misc:pget(port, TLSConf),
    RanchRef = rabbit_networking:ranch_ref(TLSConf),
    RanchTransportOpts =
    #{
      socket_opts => TLSConf,
      max_connections => get_max_connections(),
      num_acceptors => get_env(num_ssl_acceptors, 10),
      num_conns_sups => get_env(num_conns_sup, 1)
     },
    CowboyOpts = CowboyOpts0#{
        %% Enable HTTP/2 Websocket if not explicitly disabled.
        enable_connect_protocol => maps:get(enable_connect_protocol, CowboyOpts0, true)
    },
    case cowboy:start_tls(RanchRef, RanchTransportOpts, CowboyOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, ErrTLS} ->
            ?LOG_ERROR(
              "Failed to start a TLS WebSocket (HTTPS) listener. Error: ~p, listener settings: ~p",
              [ErrTLS, TLSConf]),
            throw(ErrTLS)
    end,
    ?LOG_INFO("rabbit_web_mqtt: listening for HTTPS connections on ~s:~w",
                    [binding_address(TLSConf), TLSPort]).

listener_started(Protocol, Listener) ->
    Port = rabbit_misc:pget(port, Listener),
    _ = [rabbit_networking:tcp_listener_started(Protocol, Listener, IPAddress, Port)
         || IPAddress <- rabbit_networking:listener_ip_addresses(Listener)],
    ok.

-spec binding_address([{atom(), any()}]) -> string().
binding_address(Listener) ->
    rabbit_misc:ntoa(rabbit_misc:pget(ip, Listener)).

get_tcp_conf(TCPConf0) ->
    TCPConf1 = case proplists:get_value(port, TCPConf0) of
                   undefined -> [{port, 15675}|TCPConf0];
                   _ -> TCPConf0
               end,
    get_ip_port(TCPConf1).

get_tls_conf(TLSConf0) ->
    TLSConf1 = case proplists:get_value(port, TLSConf0) of
                   undefined -> [{port, 15675}|proplists:delete(port, TLSConf0)];
                   _ -> TLSConf0
               end,
    get_ip_port(TLSConf1).

get_ip_port(Conf0) ->
    Ip = normalize_ip(proplists:get_value(ip, Conf0)),
    lists:keyreplace(ip, 1, Conf0, {ip, Ip}).

normalize_ip(IpStr) when is_list(IpStr) ->
    {ok, Ip} = inet:parse_address(IpStr),
    Ip;
normalize_ip(Ip) ->
    Ip.

get_max_connections() ->
  get_env(max_connections, infinity).

get_env(Key, Default) ->
    rabbit_misc:get_env(rabbitmq_web_mqtt, Key, Default).
