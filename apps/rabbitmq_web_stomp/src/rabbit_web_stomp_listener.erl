%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_stomp_listener).

-export([
  init/0,
  stop/1,
  list_connections/0,
  close_all_client_connections/1
]).

%% for testing purposes
-export([get_binding_address/1, get_tcp_port/1, get_tcp_conf/2]).

-include_lib("rabbitmq_stomp/include/rabbit_stomp.hrl").

-import(rabbit_misc, [pget/2]).

-define(TCP_PROTOCOL, 'http/web-stomp').
-define(TLS_PROTOCOL, 'https/web-stomp').

%%
%% API
%%

-spec init() -> ok.
init() ->
    WsFrame = get_env(ws_frame, text),
    CowboyOpts0 = maps:from_list(get_env(cowboy_opts, [])),
    CowboyOpts = CowboyOpts0#{proxy_header => get_env(proxy_protocol, false),
                              stream_handlers => [rabbit_web_stomp_stream_handler, cowboy_stream_h]},
    CowboyWsOpts = maps:from_list(get_env(cowboy_ws_opts, [])),

    VhostRoutes = [
        {get_env(ws_path, "/ws"), rabbit_web_stomp_handler, [{type, WsFrame}, {ws_opts, CowboyWsOpts}]}
    ],
    Routes = cowboy_router:compile([{'_',  VhostRoutes}]), % any vhost

    case get_env(tcp_config, []) of
        []       -> ok;
        TCPConf0 -> start_tcp_listener(TCPConf0, CowboyOpts, Routes)
    end,
    case get_env(ssl_config, []) of
        []       -> ok;
        TLSConf0 -> start_tls_listener(TLSConf0, CowboyOpts, Routes)
    end,
    ok.

stop(State) ->
    rabbit_networking:stop_ranch_listener_of_protocol(?TCP_PROTOCOL),
    rabbit_networking:stop_ranch_listener_of_protocol(?TLS_PROTOCOL),
    State.

-spec list_connections() -> [pid()].
list_connections() ->
    PlainPids = connection_pids_of_protocol(?TCP_PROTOCOL),
    TLSPids   = connection_pids_of_protocol(?TLS_PROTOCOL),
    
    PlainPids ++ TLSPids.

-spec close_all_client_connections(string()) -> {'ok', non_neg_integer()}.
close_all_client_connections(Reason) ->
    Connections = list_connections(),
    [rabbit_web_stomp_handler:close_connection(Pid, Reason) || Pid <- Connections],
    {ok, length(Connections)}.
    

%%
%% Implementation
%%

connection_pids_of_protocol(Protocol) ->
    case rabbit_networking:ranch_ref_of_protocol(Protocol) of
        undefined   -> [];
        AcceptorRef ->
            lists:map(fun cowboy_ws_connection_pid/1, ranch:procs(AcceptorRef, connections))
    end.

-spec cowboy_ws_connection_pid(pid()) -> pid().
cowboy_ws_connection_pid(RanchConnPid) ->
    Children = supervisor:which_children(RanchConnPid),
    {cowboy_clear, Pid, _, _} = lists:keyfind(cowboy_clear, 1, Children),
    Pid.

start_tcp_listener(TCPConf0, CowboyOpts0, Routes) ->
  NumTcpAcceptors = case application:get_env(rabbitmq_web_stomp, num_tcp_acceptors) of
      undefined -> get_env(num_acceptors, 10);
      {ok, NumTcp}  -> NumTcp
  end,
  Port = get_tcp_port(application:get_all_env(rabbitmq_web_stomp)),
  TCPConf = get_tcp_conf(TCPConf0, Port),
  RanchTransportOpts = #{
    socket_opts     => TCPConf,
    connection_type => supervisor,
    max_connections => get_max_connections(),
    num_acceptors   => NumTcpAcceptors,
    num_conns_sups => 1
  },
  CowboyOpts = CowboyOpts0#{env => #{dispatch => Routes},
                            middlewares => [cowboy_router,
                                            rabbit_web_stomp_middleware,
                                            cowboy_handler]},
  case ranch:start_listener(rabbit_networking:ranch_ref(TCPConf),
                            ranch_tcp,
                            RanchTransportOpts,
                            rabbit_web_stomp_connection_sup,
                            CowboyOpts) of
      {ok, _}                       -> ok;
      {error, {already_started, _}} -> ok;
      {error, ErrTCP}                  ->
          rabbit_log_connection:error(
              "Failed to start a WebSocket (HTTP) listener. Error: ~p,"
              " listener settings: ~p",
              [ErrTCP, TCPConf]),
          throw(ErrTCP)
  end,
  listener_started(?TCP_PROTOCOL, TCPConf),
  rabbit_log_connection:info(
      "rabbit_web_stomp: listening for HTTP connections on ~s:~w",
      [get_binding_address(TCPConf), Port]).


start_tls_listener(TLSConf0, CowboyOpts0, Routes) ->
  rabbit_networking:ensure_ssl(),
  NumSslAcceptors = case application:get_env(rabbitmq_web_stomp, num_ssl_acceptors) of
      undefined     -> get_env(num_acceptors, 10);
      {ok, NumSsl}  -> NumSsl
  end,
  TLSPort = proplists:get_value(port, TLSConf0),
  TLSConf = maybe_parse_ip(TLSConf0),
  RanchTransportOpts = #{
    socket_opts     => TLSConf,
    connection_type => supervisor,
    max_connections => get_max_connections(),
    num_acceptors   => NumSslAcceptors,
    num_conns_sups => 1
  },
  CowboyOpts = CowboyOpts0#{env => #{dispatch => Routes},
                            middlewares => [cowboy_router,
                                            rabbit_web_stomp_middleware,
                                            cowboy_handler]},
  case ranch:start_listener(rabbit_networking:ranch_ref(TLSConf),
                            ranch_ssl,
                            RanchTransportOpts,
                            rabbit_web_stomp_connection_sup,
                            CowboyOpts) of
      {ok, _}                       -> ok;
      {error, {already_started, _}} -> ok;
      {error, ErrTLS}                  ->
          rabbit_log_connection:error(
              "Failed to start a TLS WebSocket (HTTPS) listener. Error: ~p,"
              " listener settings: ~p",
              [ErrTLS, TLSConf]),
          throw(ErrTLS)
  end,
  listener_started(?TLS_PROTOCOL, TLSConf),
  rabbit_log_connection:info(
      "rabbit_web_stomp: listening for HTTPS connections on ~s:~w",
      [get_binding_address(TLSConf), TLSPort]).

listener_started(Protocol, Listener) ->
    Port = rabbit_misc:pget(port, Listener),
    [rabbit_networking:tcp_listener_started(Protocol, Listener,
                                            IPAddress, Port)
     || {IPAddress, _Port, _Family}
        <- rabbit_networking:tcp_listener_addresses(Port)],
    ok.

get_env(Key, Default) ->
    rabbit_misc:get_env(rabbitmq_web_stomp, Key, Default).

get_tcp_port(Configuration) ->
    %% The 'tcp_config' option may include the port, and we already have
    %% a 'port' option. We prioritize the 'port' option  in 'tcp_config' (if any)
    %% over the one found at the root of the env proplist.
    TcpConfiguration = proplists:get_value(tcp_config, Configuration, []),
    case proplists:get_value(port, TcpConfiguration) of
        undefined ->
            proplists:get_value(port, Configuration, 15674);
        Port ->
            Port
    end.

get_tcp_conf(TcpConfiguration, Port0) ->
    Port = [{port, Port0} | proplists:delete(port, TcpConfiguration)],
    maybe_parse_ip(Port).

maybe_parse_ip(Configuration) ->
    case proplists:get_value(ip, Configuration) of
        undefined ->
            Configuration;
        IP when is_tuple(IP) ->
            Configuration;
        IP when is_list(IP) ->
            {ok, ParsedIP} = inet_parse:address(IP),
            [{ip, ParsedIP} | proplists:delete(ip, Configuration)]
    end.

get_binding_address(Configuration) ->
    case proplists:get_value(ip, Configuration) of
        undefined ->
            "0.0.0.0";
        IP when is_tuple(IP) ->
            inet:ntoa(IP);
        IP when is_list(IP) ->
            IP
    end.

get_max_connections() ->
  rabbit_misc:get_env(rabbitmq_web_stomp, max_connections, infinity).
