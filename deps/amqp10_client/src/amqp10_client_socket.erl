%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(amqp10_client_socket).

-feature(maybe_expr, enable).

-export([connect/3,
         set_active_once/1,
         send/2,
         close/1]).

-type socket() :: {tcp, gen_tcp:socket()} |
                  {ssl, ssl:sslsocket()} |
                  {ws, pid(), gun:stream_ref()}.

-export_type([socket/0]).

-define(TCP_OPTS, [binary,
                   {packet, 0},
                   {active, false},
                   {nodelay, true}]).

-spec connect(inet:hostname() | inet:ip_address(),
              inet:port_number(),
              amqp10_client_connection:connection_config()) ->
    {ok, socket()} | {error, any()}.
connect(Host, Port, #{ws_path := Path} = Opts) ->
    GunOpts = maps:merge(#{tcp_opts => [{nodelay, true}]},
                         maps:get(ws_opts, Opts, #{})),
    maybe
        {ok, _Started} ?= application:ensure_all_started(gun),
        {ok, Pid} ?= gun:open(Host, Port, GunOpts),
        MRef = monitor(process, Pid),
        {ok, _HttpVsn} ?= gun:await_up(Pid, MRef),
        {ok, StreamRef} ?= ws_upgrade(Pid, Path),
        {ok, {ws, Pid, StreamRef}}
    end;
connect(Host, Port, #{tls_opts := {secure_port, Opts0}}) ->
    Opts = rabbit_ssl_options:fix_client(Opts0),
    case ssl:connect(Host, Port, ?TCP_OPTS ++ Opts) of
        {ok, S} ->
            {ok, {ssl, S}};
        Err ->
            Err
    end;
connect(Host, Port, _) ->
    case gen_tcp:connect(Host, Port, ?TCP_OPTS) of
        {ok, S} ->
            {ok, {tcp, S}};
        Err ->
            Err
    end.

ws_upgrade(Pid, Path) ->
    StreamRef = gun:ws_upgrade(Pid,
                               Path,
                               [{<<"cache-control">>, <<"no-cache">>}],
                               #{protocols => [{<<"amqp">>, gun_ws_h}]}),
    receive
        {gun_upgrade, Pid, StreamRef, [<<"websocket">>], _Headers} ->
            {ok, StreamRef};
        {gun_response, Pid, _, _, Status, Headers} ->
            {error, {ws_upgrade, Status, Headers}};
        {gun_error, Pid, StreamRef, Reason} ->
            {error, {ws_upgrade, Reason}}
    after 5000 ->
              {error, {ws_upgrade, timeout}}
    end.

-spec set_active_once(socket()) -> ok.
set_active_once({tcp, Sock}) ->
    ok = inet:setopts(Sock, [{active, once}]);
set_active_once({ssl, Sock}) ->
    ok = ssl:setopts(Sock, [{active, once}]);
set_active_once({ws, _Pid, _Ref}) ->
    %% Gun also has an active-like mode via the flow option and gun:update_flow.
    %% It will even make Gun stop reading from the socket if flow is zero.
    %% If needed, we can make use of it in future.
    ok.

-spec send(socket(), iodata()) ->
    ok | {error, any()}.
send({tcp, Socket}, Data) ->
    gen_tcp:send(Socket, Data);
send({ssl, Socket}, Data) ->
    ssl:send(Socket, Data);
send({ws, Pid, Ref}, Data) ->
    gun:ws_send(Pid, Ref, {binary, Data}).

-spec close(socket()) ->
    ok | {error, any()}.
close({tcp, Socket}) ->
    gen_tcp:close(Socket);
close({ssl, Socket}) ->
    ssl:close(Socket);
close({ws, Pid, _Ref}) ->
    gun:shutdown(Pid).
