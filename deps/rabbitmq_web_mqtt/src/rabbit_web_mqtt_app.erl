%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_mqtt_app).

-behaviour(application).
-export([
    start/2,
    prep_stop/1,
    stop/1,
    list_connections/0,
    close_all_client_connections/1
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
    rabbit_networking:stop_ranch_listener_of_protocol(?TCP_PROTOCOL),
    rabbit_networking:stop_ranch_listener_of_protocol(?TLS_PROTOCOL),
    ok.

init([]) -> {ok, {{one_for_one, 1, 5}, []}}.

-spec list_connections() -> [pid()].
list_connections() ->
    PlainPids = connection_pids_of_protocol(?TCP_PROTOCOL),
    TLSPids   = connection_pids_of_protocol(?TLS_PROTOCOL),

    PlainPids ++ TLSPids.

-spec close_all_client_connections(string()) -> {'ok', non_neg_integer()}.
close_all_client_connections(Reason) ->
    Connections = list_connections(),
    [rabbit_web_mqtt_handler:close_connection(Pid, Reason) || Pid <- Connections],
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

mqtt_init() ->
  CowboyOpts0  = maps:from_list(get_env(cowboy_opts, [])),
  CowboyWsOpts = maps:from_list(get_env(cowboy_ws_opts, [])),

  Routes = cowboy_router:compile([{'_', [
      {get_env(ws_path, "/ws"), rabbit_web_mqtt_handler, [{ws_opts, CowboyWsOpts}]}
  ]}]),
  CowboyOpts = CowboyOpts0#{env          => #{dispatch => Routes},
                            middlewares  => [cowboy_router, rabbit_web_mqtt_middleware, cowboy_handler],
                            proxy_header => get_env(proxy_protocol, false),
                            stream_handlers => [rabbit_web_mqtt_stream_handler, cowboy_stream_h]},
  case get_env(tcp_config, []) of
      []       -> ok;
      TCPConf0 -> start_tcp_listener(TCPConf0, CowboyOpts)
  end,
  case get_env(ssl_config, []) of
      []       -> ok;
      TLSConf0 -> start_tls_listener(TLSConf0, CowboyOpts)
  end,
  ok.

start_tcp_listener(TCPConf0, CowboyOpts) ->
  {TCPConf, IpStr, Port} = get_tcp_conf(TCPConf0),
  RanchTransportOpts = #{
    socket_opts => TCPConf,
    connection_type => supervisor,
    max_connections => get_max_connections(),
    num_acceptors => get_env(num_tcp_acceptors, 10),
    num_conns_sups => get_env(num_conns_sup, 1)
  },
  case ranch:start_listener(rabbit_networking:ranch_ref(TCPConf),
                            ranch_tcp,
                            RanchTransportOpts,
                            rabbit_web_mqtt_connection_sup,
                            CowboyOpts) of
      {ok, _}                       -> ok;
      {error, {already_started, _}} -> ok;
      {error, ErrTCP}               ->
          rabbit_log_connection:error(
              "Failed to start a WebSocket (HTTP) listener. Error: ~p,"
              " listener settings: ~p",
              [ErrTCP, TCPConf]),
          throw(ErrTCP)
  end,
  listener_started(?TCP_PROTOCOL, TCPConf),
  rabbit_log:info("rabbit_web_mqtt: listening for HTTP connections on ~s:~w",
                  [IpStr, Port]).

start_tls_listener(TLSConf0, CowboyOpts) ->
  rabbit_networking:ensure_ssl(),
  {TLSConf, TLSIpStr, TLSPort} = get_tls_conf(TLSConf0),
  RanchTransportOpts = #{
    socket_opts => TLSConf,
    connection_type => supervisor,
    max_connections => get_max_connections(),
    num_acceptors => get_env(num_ssl_acceptors, 10),
    num_conns_sups => get_env(num_conns_sup, 1)
  },
  case ranch:start_listener(rabbit_networking:ranch_ref(TLSConf),
                            ranch_ssl,
                            RanchTransportOpts,
                            rabbit_web_mqtt_connection_sup,
                            CowboyOpts) of
      {ok, _}                       -> ok;
      {error, {already_started, _}} -> ok;
      {error, ErrTLS}               ->
          rabbit_log_connection:error(
              "Failed to start a TLS WebSocket (HTTPS) listener. Error: ~p,"
              " listener settings: ~p",
              [ErrTLS, TLSConf]),
          throw(ErrTLS)
  end,
  listener_started(?TLS_PROTOCOL, TLSConf),
  rabbit_log:info("rabbit_web_mqtt: listening for HTTPS connections on ~s:~w",
                  [TLSIpStr, TLSPort]).

listener_started(Protocol, Listener) ->
    Port = rabbit_misc:pget(port, Listener),
    [rabbit_networking:tcp_listener_started(Protocol, Listener,
                                            IPAddress, Port)
     || {IPAddress, _Port, _Family}
        <- rabbit_networking:tcp_listener_addresses(Port)],
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
