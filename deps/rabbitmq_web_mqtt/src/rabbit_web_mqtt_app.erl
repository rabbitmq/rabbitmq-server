%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_web_mqtt_app).

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
    %% @todo Do the rabbit_web equivalent.
    _ = rabbit_networking:stop_ranch_listener_of_protocol(?TCP_PROTOCOL),
    _ = rabbit_networking:stop_ranch_listener_of_protocol(?TLS_PROTOCOL),
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
    CowboyWsOpts0 = maps:from_list(get_env(cowboy_ws_opts, [])),
    CowboyWsOpts = CowboyWsOpts0#{compress => true},
    TcpConfig = get_env(tcp_config, []),
%    SslConfig = get_env(ssl_config, []),
    Routes = cowboy_router:compile([{'_', [
        rabbit_web:ws_route(get_env(ws_path, "/ws")),
        {"/web-mqtt-examples/[...]", cowboy_static, {priv_dir, rabbitmq_web_mqtt_examples, "", []}}
    ]}]),
    CowboyOpts0  = maps:from_list(get_env(cowboy_opts, [])),
    CowboyOpts = CowboyOpts0#{
        env => #{dispatch => Routes},
        proxy_header => get_env(proxy_protocol, false)
    },
    rabbit_web:start_listeners(rabbit_web_mqtt, #{
        tcp => [listener_config(TcpConfig)],
        tcp_opts => #{
            socket_opts => TcpConfig,
            max_connections => get_max_connections(),
            num_acceptors => get_env(num_tcp_acceptors, 10),
            num_conns_sups => get_env(num_conns_sup, 1)
        },
        websocket => #{
            protocols => [<<"mqtt">>],
            handler => rabbit_web_mqtt_handler,
            opts => CowboyWsOpts
        }
    }, CowboyOpts).

listener_config(Config) ->
    Port = proplists:get_value(port, Config, 15675),
    case proplists:get_value(ip, Config) of
        undefined ->
            Port;
        IpStr ->
            {normalize_ip(IpStr), Port}
    end.


%    start_tcp_listener(TcpConfig, CowboyOpts),
%    start_tls_listener(SslConfig, CowboyOpts).

start_tcp_listener([], _) -> ok;
start_tcp_listener(TCPConf0, CowboyOpts) ->
    {TCPConf, IpStr, Port} = get_tcp_conf(TCPConf0),
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
            rabbit_log:error(
              "Failed to start a WebSocket (HTTP) listener. Error: ~p, listener settings: ~p",
              [ErrTCP, TCPConf]),
            throw(ErrTCP)
    end,
    listener_started(?TCP_PROTOCOL, TCPConf),
    rabbit_log:info("rabbit_web_mqtt: listening for HTTP connections on ~s:~w",
                    [IpStr, Port]).


start_tls_listener([], _) -> ok;
start_tls_listener(TLSConf0, CowboyOpts) ->
    _ = rabbit_networking:ensure_ssl(),
    {TLSConf, TLSIpStr, TLSPort} = get_tls_conf(TLSConf0),
    RanchRef = rabbit_networking:ranch_ref(TLSConf),
    RanchTransportOpts =
    #{
      socket_opts => TLSConf,
      max_connections => get_max_connections(),
      num_acceptors => get_env(num_ssl_acceptors, 10),
      num_conns_sups => get_env(num_conns_sup, 1)
     },
    case cowboy:start_tls(RanchRef, RanchTransportOpts, CowboyOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, ErrTLS} ->
            rabbit_log:error(
              "Failed to start a TLS WebSocket (HTTPS) listener. Error: ~p, listener settings: ~p",
              [ErrTLS, TLSConf]),
            throw(ErrTLS)
    end,
    listener_started(?TLS_PROTOCOL, TLSConf),
    rabbit_log:info("rabbit_web_mqtt: listening for HTTPS connections on ~s:~w",
                    [TLSIpStr, TLSPort]).

listener_started(Protocol, Listener) ->
    Port = rabbit_misc:pget(port, Listener),
    _ = case rabbit_misc:pget(ip, Listener) of
            undefined ->
                [rabbit_networking:tcp_listener_started(Protocol, Listener,
                                                        IPAddress, Port)
                 || {IPAddress, _Port, _Family}
                        <- rabbit_networking:tcp_listener_addresses(Port)];
            IP when is_tuple(IP) ->
                rabbit_networking:tcp_listener_started(Protocol, Listener,
                                                       IP, Port);
            IP when is_list(IP) ->
                {ok, ParsedIP} = inet_parse:address(IP),
                rabbit_networking:tcp_listener_started(Protocol, Listener,
                                                       ParsedIP, Port)
        end,
    ok.

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
    IpStr = proplists:get_value(ip, Conf0),
    Ip = normalize_ip(IpStr),
    Conf1 = lists:keyreplace(ip, 1, Conf0, {ip, Ip}),
    Port = proplists:get_value(port, Conf1),
    {Conf1, IpStr, Port}.

normalize_ip(IpStr) when is_list(IpStr) ->
    {ok, Ip} = inet:parse_address(IpStr),
    Ip;
normalize_ip(Ip) ->
    Ip.

get_max_connections() ->
  get_env(max_connections, infinity).

get_env(Key, Default) ->
    rabbit_misc:get_env(rabbitmq_web_mqtt, Key, Default).
