%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ws_sockjs).

-export([init/0]).

%% for testing purposes
-export([get_binding_address/1, get_tcp_port/1, get_tcp_conf/2]).

-include_lib("rabbitmq_stomp/include/rabbit_stomp.hrl").


%% --------------------------------------------------------------------------

-spec init() -> ok.
init() ->
    Port = get_tcp_port(application:get_all_env(rabbitmq_web_stomp)),

    TcpConf = get_tcp_conf(get_env(tcp_config, []), Port),

    WsFrame = get_env(ws_frame, text),
    CowboyOpts = get_env(cowboy_opts, []),

    SockjsOpts = get_env(sockjs_opts, []) ++ [{logger, fun logger/3}],

    SockjsState = sockjs_handler:init_state(
                    <<"/stomp">>, fun service_stomp/3, {}, SockjsOpts),
    VhostRoutes = [
        {"/stomp/[...]", sockjs_cowboy_handler, SockjsState},
        {"/ws", rabbit_ws_handler, [{type, WsFrame}]}
    ],
    Routes = cowboy_router:compile([{'_',  VhostRoutes}]), % any vhost
    NumTcpAcceptors = case application:get_env(rabbitmq_web_stomp, num_tcp_acceptors) of
        undefined -> get_env(num_acceptors, 10);
        {ok, NumTcp}  -> NumTcp
    end,
    case cowboy:start_http(http, NumTcpAcceptors,
                           TcpConf,
                           [{env, [{dispatch, Routes}]}|CowboyOpts]) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        {error, Err}                  ->
            rabbit_log:error("Failed to start an HTTP listener for SockJS. Error: ~p, listener settings: ~p~n", [Err, TcpConf]),
            throw(Err)
    end,
    listener_started('http/web-stomp', TcpConf),
    rabbit_log:info("rabbit_web_stomp: listening for HTTP connections on ~s:~w~n",
                    [get_binding_address(TcpConf), Port]),
    case get_env(ssl_config, []) of
        [] ->
            ok;
        TLSConf0 ->
            rabbit_networking:ensure_ssl(),
            TLSPort = proplists:get_value(port, TLSConf0),
            TLSConf = maybe_parse_ip(TLSConf0),
            NumSslAcceptors = case application:get_env(rabbitmq_web_stomp, num_ssl_acceptors) of
                undefined     -> get_env(num_acceptors, 1);
                {ok, NumSsl}  -> NumSsl
            end,
            {ok, _} = cowboy:start_https(https, NumSslAcceptors,
                                         TLSConf,
                                         [{env, [{dispatch, Routes}]} | CowboyOpts]),
            listener_started('https/web-stomp', TLSConf),
            rabbit_log:info("rabbit_web_stomp: listening for HTTPS connections on ~s:~w~n",
                            [get_binding_address(TLSConf), TLSPort])
    end,
    ok.

listener_started(Protocol, Listener) ->
    Port = rabbit_misc:pget(port, Listener),
    [rabbit_networking:tcp_listener_started(Protocol, Listener,
                                            IPAddress, Port)
     || {IPAddress, _Port, _Family}
        <- rabbit_networking:tcp_listener_addresses(Port)],
    ok.

get_env(Key, Default) ->
    case application:get_env(rabbitmq_web_stomp, Key) of
        undefined -> Default;
        {ok, V}   -> V
    end.


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

%% Don't print sockjs logs
logger(_Service, Req, _Type) ->
    Req.

%% --------------------------------------------------------------------------

service_stomp(Conn, init, _State) ->
    {ok, _Sup, Pid} = rabbit_ws_sup:start_client({Conn, no_heartbeat}),
    {ok, Pid};

service_stomp(_Conn, {recv, Data}, Pid) ->
    rabbit_ws_client:sockjs_msg(Pid, Data),
    {ok, Pid};

service_stomp(_Conn, closed, Pid) ->
    rabbit_ws_client:sockjs_closed(Pid),
    ok.
