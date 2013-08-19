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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_net).
-include("rabbit.hrl").

-export([is_ssl/1, ssl_info/1, controlling_process/2, getstat/2,
         recv/1, sync_recv/2, async_recv/3, port_command/2, getopts/2,
         setopts/2, send/2, close/1, fast_close/1, sockname/1, peername/1,
         peercert/1, connection_string/2, socket_ends/2]).

%%---------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([socket/0]).

-type(stat_option() ::
        'recv_cnt' | 'recv_max' | 'recv_avg' | 'recv_oct' | 'recv_dvi' |
        'send_cnt' | 'send_max' | 'send_avg' | 'send_oct' | 'send_pend').
-type(ok_val_or_error(A) :: rabbit_types:ok_or_error2(A, any())).
-type(ok_or_any_error() :: rabbit_types:ok_or_error(any())).
-type(socket() :: port() | #ssl_socket{}).
-type(opts() :: [{atom(), any()} |
                 {raw, non_neg_integer(), non_neg_integer(), binary()}]).
-type(host_or_ip() :: binary() | inet:ip_address()).
-spec(is_ssl/1 :: (socket()) -> boolean()).
-spec(ssl_info/1 :: (socket())
                    -> 'nossl' | ok_val_or_error(
                                   {atom(), {atom(), atom(), atom()}})).
-spec(controlling_process/2 :: (socket(), pid()) -> ok_or_any_error()).
-spec(getstat/2 ::
        (socket(), [stat_option()])
        -> ok_val_or_error([{stat_option(), integer()}])).
-spec(recv/1 :: (socket()) ->
                     {'data', [char()] | binary()} | 'closed' |
                     rabbit_types:error(any()) | {'other', any()}).
-spec(sync_recv/2 :: (socket(), integer()) -> rabbit_types:ok(binary()) |
                                              rabbit_types:error(any())).
-spec(async_recv/3 ::
        (socket(), integer(), timeout()) -> rabbit_types:ok(any())).
-spec(port_command/2 :: (socket(), iolist()) -> 'true').
-spec(getopts/2 :: (socket(), [atom() | {raw,
                                         non_neg_integer(),
                                         non_neg_integer(),
                                         non_neg_integer() | binary()}])
                   -> ok_val_or_error(opts())).
-spec(setopts/2 :: (socket(), opts()) -> ok_or_any_error()).
-spec(send/2 :: (socket(), binary() | iolist()) -> ok_or_any_error()).
-spec(close/1 :: (socket()) -> ok_or_any_error()).
-spec(fast_close/1 :: (socket()) -> ok_or_any_error()).
-spec(sockname/1 ::
        (socket())
        -> ok_val_or_error({inet:ip_address(), rabbit_networking:ip_port()})).
-spec(peername/1 ::
        (socket())
        -> ok_val_or_error({inet:ip_address(), rabbit_networking:ip_port()})).
-spec(peercert/1 ::
        (socket())
        -> 'nossl' | ok_val_or_error(rabbit_ssl:certificate())).
-spec(connection_string/2 ::
        (socket(), 'inbound' | 'outbound') -> ok_val_or_error(string())).
-spec(socket_ends/2 ::
        (socket(), 'inbound' | 'outbound')
        -> ok_val_or_error({host_or_ip(), rabbit_networking:ip_port(),
                            host_or_ip(), rabbit_networking:ip_port()})).

-endif.

%%---------------------------------------------------------------------------

-define(SSL_CLOSE_TIMEOUT, 5000).

-define(IS_SSL(Sock), is_record(Sock, ssl_socket)).

is_ssl(Sock) -> ?IS_SSL(Sock).

ssl_info(Sock) when ?IS_SSL(Sock) ->
    ssl:connection_info(Sock#ssl_socket.ssl);
ssl_info(_Sock) ->
    nossl.

controlling_process(Sock, Pid) when ?IS_SSL(Sock) ->
    ssl:controlling_process(Sock#ssl_socket.ssl, Pid);
controlling_process(Sock, Pid) when is_port(Sock) ->
    gen_tcp:controlling_process(Sock, Pid).

getstat(Sock, Stats) when ?IS_SSL(Sock) ->
    inet:getstat(Sock#ssl_socket.tcp, Stats);
getstat(Sock, Stats) when is_port(Sock) ->
    inet:getstat(Sock, Stats).

recv(Sock) when ?IS_SSL(Sock) ->
    recv(Sock#ssl_socket.ssl, {ssl, ssl_closed, ssl_error});
recv(Sock) when is_port(Sock) ->
    recv(Sock, {tcp, tcp_closed, tcp_error}).

recv(S, {DataTag, ClosedTag, ErrorTag}) ->
    receive
        {DataTag, S, Data}    -> {data, Data};
        {ClosedTag, S}        -> closed;
        {ErrorTag, S, Reason} -> {error, Reason};
        Other                 -> {other, Other}
    end.

sync_recv(Sock, Length) when ?IS_SSL(Sock) ->
    ssl:recv(Sock#ssl_socket.ssl, Length);
sync_recv(Sock, Length) ->
    gen_tcp:recv(Sock, Length).

async_recv(Sock, Length, Timeout) when ?IS_SSL(Sock) ->
    Pid = self(),
    Ref = make_ref(),

    spawn(fun () -> Pid ! {inet_async, Sock, Ref,
                           ssl:recv(Sock#ssl_socket.ssl, Length, Timeout)}
          end),

    {ok, Ref};
async_recv(Sock, Length, infinity) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, -1);
async_recv(Sock, Length, Timeout) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, Timeout).

port_command(Sock, Data) when ?IS_SSL(Sock) ->
    case ssl:send(Sock#ssl_socket.ssl, Data) of
        ok              -> self() ! {inet_reply, Sock, ok},
                           true;
        {error, Reason} -> erlang:error(Reason)
    end;
port_command(Sock, Data) when is_port(Sock) ->
    erlang:port_command(Sock, Data).

getopts(Sock, Options) when ?IS_SSL(Sock) ->
    ssl:getopts(Sock#ssl_socket.ssl, Options);
getopts(Sock, Options) when is_port(Sock) ->
    inet:getopts(Sock, Options).

setopts(Sock, Options) when ?IS_SSL(Sock) ->
    ssl:setopts(Sock#ssl_socket.ssl, Options);
setopts(Sock, Options) when is_port(Sock) ->
    inet:setopts(Sock, Options).

send(Sock, Data) when ?IS_SSL(Sock) -> ssl:send(Sock#ssl_socket.ssl, Data);
send(Sock, Data) when is_port(Sock) -> gen_tcp:send(Sock, Data).

close(Sock)      when ?IS_SSL(Sock) -> ssl:close(Sock#ssl_socket.ssl);
close(Sock)      when is_port(Sock) -> gen_tcp:close(Sock).

fast_close(Sock) when ?IS_SSL(Sock) ->
    %% We cannot simply port_close the underlying tcp socket since the
    %% TLS protocol is quite insistent that a proper closing handshake
    %% should take place (see RFC 5245 s7.2.1). So we call ssl:close
    %% instead, but that can block for a very long time, e.g. when
    %% there is lots of pending output and there is tcp backpressure,
    %% or the ssl_connection process has entered the the
    %% workaround_transport_delivery_problems function during
    %% termination, which, inexplicably, does a gen_tcp:recv(Socket,
    %% 0), which may never return if the client doesn't send a FIN or
    %% that gets swallowed by the network. Since there is no timeout
    %% variant of ssl:close, we construct our own.
    {Pid, MRef} = spawn_monitor(fun () -> ssl:close(Sock#ssl_socket.ssl) end),
    erlang:send_after(?SSL_CLOSE_TIMEOUT, self(), {Pid, ssl_close_timeout}),
    receive
        {Pid, ssl_close_timeout} ->
            erlang:demonitor(MRef, [flush]),
            exit(Pid, kill);
        {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end,
    catch port_close(Sock#ssl_socket.tcp),
    ok;
fast_close(Sock) when is_port(Sock) ->
    catch port_close(Sock), ok.

sockname(Sock)   when ?IS_SSL(Sock) -> ssl:sockname(Sock#ssl_socket.ssl);
sockname(Sock)   when is_port(Sock) -> inet:sockname(Sock).

peername(Sock)   when ?IS_SSL(Sock) -> ssl:peername(Sock#ssl_socket.ssl);
peername(Sock)   when is_port(Sock) -> inet:peername(Sock).

peercert(Sock)   when ?IS_SSL(Sock) -> ssl:peercert(Sock#ssl_socket.ssl);
peercert(Sock)   when is_port(Sock) -> nossl.

connection_string(Sock, Direction) ->
    case socket_ends(Sock, Direction) of
        {ok, {FromAddress, FromPort, ToAddress, ToPort}} ->
            {ok, rabbit_misc:format(
                   "~s:~p -> ~s:~p",
                   [maybe_ntoab(FromAddress), FromPort,
                    maybe_ntoab(ToAddress),   ToPort])};
        Error ->
            Error
    end.

socket_ends(Sock, Direction) ->
    {From, To} = sock_funs(Direction),
    case {From(Sock), To(Sock)} of
        {{ok, {FromAddress, FromPort}}, {ok, {ToAddress, ToPort}}} ->
            {ok, {rdns(FromAddress), FromPort,
                  rdns(ToAddress),   ToPort}};
        {{error, _Reason} = Error, _} ->
            Error;
        {_, {error, _Reason} = Error} ->
            Error
    end.

maybe_ntoab(Addr) when is_tuple(Addr) -> rabbit_misc:ntoab(Addr);
maybe_ntoab(Host)                     -> Host.

rdns(Addr) ->
    {ok, Lookup} = application:get_env(rabbit, reverse_dns_lookups),
    case Lookup of
        true -> list_to_binary(rabbit_networking:tcp_host(Addr));
        _    -> Addr
    end.

sock_funs(inbound)  -> {fun peername/1, fun sockname/1};
sock_funs(outbound) -> {fun sockname/1, fun peername/1}.
