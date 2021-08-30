%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_net).
-include("rabbit.hrl").

-include_lib("kernel/include/inet.hrl").

-export([is_ssl/1, ssl_info/1, controlling_process/2, getstat/2,
    recv/1, sync_recv/2, async_recv/3, port_command/2, getopts/2,
    setopts/2, send/2, close/1, fast_close/1, sockname/1, peername/1,
    peercert/1, connection_string/2, socket_ends/2, is_loopback/1,
    tcp_host/1, unwrap_socket/1, maybe_get_proxy_socket/1,
    hostname/0, getifaddrs/0, proxy_ssl_info/2]).

%%---------------------------------------------------------------------------

-export_type([socket/0, ip_port/0, hostname/0]).

-type stat_option() ::
        'recv_cnt' | 'recv_max' | 'recv_avg' | 'recv_oct' | 'recv_dvi' |
        'send_cnt' | 'send_max' | 'send_avg' | 'send_oct' | 'send_pend'.
-type ok_val_or_error(A) :: rabbit_types:ok_or_error2(A, any()).
-type ok_or_any_error() :: rabbit_types:ok_or_error(any()).
-type socket() :: port() | ssl:sslsocket().
-type opts() :: [{atom(), any()} |
                 {raw, non_neg_integer(), non_neg_integer(), binary()}].
-type hostname() :: inet:hostname().
-type ip_port() :: inet:port_number().
% -type host_or_ip() :: binary() | inet:ip_address().
-spec is_ssl(socket()) -> boolean().
-spec ssl_info(socket()) -> 'nossl' | ok_val_or_error([{atom(), any()}]).
-spec proxy_ssl_info(socket(), ranch_proxy:proxy_socket()) -> 'nossl' | ok_val_or_error([{atom(), any()}]).
-spec controlling_process(socket(), pid()) -> ok_or_any_error().
-spec getstat(socket(), [stat_option()]) ->
          ok_val_or_error([{stat_option(), integer()}]).
-spec recv(socket()) ->
          {'data', [char()] | binary()} |
          'closed' |
          rabbit_types:error(any()) |
          {'other', any()}.
-spec sync_recv(socket(), integer()) ->
          rabbit_types:ok(binary()) |
          rabbit_types:error(any()).
-spec async_recv(socket(), integer(), timeout()) ->
          rabbit_types:ok(any()).
-spec port_command(socket(), iolist()) -> 'true'.
-spec getopts
        (socket(),
         [atom() |
          {raw, non_neg_integer(), non_neg_integer(),
           non_neg_integer() | binary()}]) ->
            ok_val_or_error(opts()).
-spec setopts(socket(), opts()) -> ok_or_any_error().
-spec send(socket(), binary() | iolist()) -> ok_or_any_error().
-spec close(socket()) -> ok_or_any_error().
-spec fast_close(socket()) -> ok_or_any_error().
-spec sockname(socket()) ->
          ok_val_or_error({inet:ip_address(), ip_port()}).
-spec peername(socket()) ->
          ok_val_or_error({inet:ip_address(), ip_port()}).
-spec peercert(socket()) ->
          'nossl' | ok_val_or_error(rabbit_ssl:certificate()).
-spec connection_string(socket(), 'inbound' | 'outbound') ->
          ok_val_or_error(string()).
% -spec socket_ends(socket() | ranch_proxy:proxy_socket() | ranch_proxy_ssl:ssl_socket(),
%                   'inbound' | 'outbound') ->
%           ok_val_or_error({host_or_ip(), ip_port(),
%                            host_or_ip(), ip_port()}).
-spec is_loopback(socket() | inet:ip_address()) -> boolean().
% -spec unwrap_socket(socket() | ranch_proxy:proxy_socket() | ranch_proxy_ssl:ssl_socket()) -> socket().

-dialyzer({nowarn_function, [socket_ends/2, unwrap_socket/1]}).

%%---------------------------------------------------------------------------

-define(SSL_CLOSE_TIMEOUT, 5000).

-define(IS_SSL(Sock), is_tuple(Sock)
    andalso (tuple_size(Sock) =:= 3)
    andalso (element(1, Sock) =:= sslsocket)).

is_ssl(Sock) -> ?IS_SSL(Sock).

%% Seems hackish. Is hackish. But the structure is stable and
%% kept this way for backward compatibility reasons. We need
%% it for two reasons: there are no ssl:getstat(Sock) function,
%% and no ssl:close(Timeout) function. Both of them are being
%% worked on as we speak.
ssl_get_socket(Sock) ->
    element(2, element(2, Sock)).

ssl_info(Sock) when ?IS_SSL(Sock) ->
    ssl:connection_information(Sock);
ssl_info(_Sock) ->
    nossl.

proxy_ssl_info(Sock, {rabbit_proxy_socket, _, ProxyInfo}) ->
    case ProxyInfo of
        #{ssl := #{version := Version, cipher := Cipher}} ->
            Proto = case Version of
                        <<"SSL3">> -> 'ssl3';
                        <<"TLSv1">> -> 'tlsv1';
                        <<"TLSv1.1">> -> 'tlsv1.1';
                        <<"TLSv1.2">> -> 'tlsv1.2';
                        <<"TLSv1.3">> -> 'tlsv1.3';
                        _ -> nossl
                    end,
            CipherSuite = case ssl:str_to_suite(binary_to_list(Cipher)) of
                              #{} = CS -> CS;
                              _ -> ssl_info(Sock)
                          end,
            case {Proto, CipherSuite} of
                {nossl, _} -> ssl_info(Sock);
                {_, nossl} -> ssl_info(Sock);
                _ -> {ok, [{protocol, Proto}, {selected_cipher_suite, CipherSuite}]}
            end;
        _ ->
            ssl_info(Sock)
    end;
proxy_ssl_info(Sock, _) ->
    ssl_info(Sock).


controlling_process(Sock, Pid) when ?IS_SSL(Sock) ->
    ssl:controlling_process(Sock, Pid);
controlling_process(Sock, Pid) when is_port(Sock) ->
    gen_tcp:controlling_process(Sock, Pid).

getstat(Sock, Stats) when ?IS_SSL(Sock) ->
    inet:getstat(ssl_get_socket(Sock), Stats);
getstat(Sock, Stats) when is_port(Sock) ->
    inet:getstat(Sock, Stats);
%% Used by Proxy protocol support in plugins
getstat({rabbit_proxy_socket, Sock, _}, Stats) when ?IS_SSL(Sock) ->
    inet:getstat(ssl_get_socket(Sock), Stats);
getstat({rabbit_proxy_socket, Sock, _}, Stats) when is_port(Sock) ->
    inet:getstat(Sock, Stats).

recv(Sock) when ?IS_SSL(Sock) ->
    recv(Sock, {ssl, ssl_closed, ssl_error});
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
    ssl:recv(Sock, Length);
sync_recv(Sock, Length) ->
    gen_tcp:recv(Sock, Length).

async_recv(Sock, Length, Timeout) when ?IS_SSL(Sock) ->
    Pid = self(),
    Ref = make_ref(),

    spawn(fun () -> Pid ! {inet_async, Sock, Ref,
                           ssl:recv(Sock, Length, Timeout)}
          end),

    {ok, Ref};
async_recv(Sock, Length, infinity) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, -1);
async_recv(Sock, Length, Timeout) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, Timeout).

port_command(Sock, Data) when ?IS_SSL(Sock) ->
    case ssl:send(Sock, Data) of
        ok              -> self() ! {inet_reply, Sock, ok},
                           true;
        {error, Reason} -> erlang:error(Reason)
    end;
port_command(Sock, Data) when is_port(Sock) ->
    erlang:port_command(Sock, Data).

getopts(Sock, Options) when ?IS_SSL(Sock) ->
    ssl:getopts(Sock, Options);
getopts(Sock, Options) when is_port(Sock) ->
    inet:getopts(Sock, Options).

setopts(Sock, Options) when ?IS_SSL(Sock) ->
    ssl:setopts(Sock, Options);
setopts(Sock, Options) when is_port(Sock) ->
    inet:setopts(Sock, Options).

send(Sock, Data) when ?IS_SSL(Sock) -> ssl:send(Sock, Data);
send(Sock, Data) when is_port(Sock) -> gen_tcp:send(Sock, Data).

close(Sock)      when ?IS_SSL(Sock) -> ssl:close(Sock);
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
    {Pid, MRef} = spawn_monitor(fun () -> ssl:close(Sock) end),
    erlang:send_after(?SSL_CLOSE_TIMEOUT, self(), {Pid, ssl_close_timeout}),
    receive
        {Pid, ssl_close_timeout} ->
            erlang:demonitor(MRef, [flush]),
            exit(Pid, kill);
        {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end,
    catch port_close(ssl_get_socket(Sock)),
    ok;
fast_close(Sock) when is_port(Sock) ->
    catch port_close(Sock), ok.

sockname(Sock)   when ?IS_SSL(Sock) -> ssl:sockname(Sock);
sockname(Sock)   when is_port(Sock) -> inet:sockname(Sock).

peername(Sock)   when ?IS_SSL(Sock) -> ssl:peername(Sock);
peername(Sock)   when is_port(Sock) -> inet:peername(Sock).

peercert(Sock)   when ?IS_SSL(Sock) -> ssl:peercert(Sock);
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

socket_ends(Sock, Direction) when ?IS_SSL(Sock);
    is_port(Sock) ->
    {From, To} = sock_funs(Direction),
    case {From(Sock), To(Sock)} of
        {{ok, {FromAddress, FromPort}}, {ok, {ToAddress, ToPort}}} ->
            {ok, {rdns(FromAddress), FromPort,
                rdns(ToAddress),   ToPort}};
        {{error, _Reason} = Error, _} ->
            Error;
        {_, {error, _Reason} = Error} ->
            Error
    end;
socket_ends({rabbit_proxy_socket, _, ProxyInfo}, _) ->
    #{
      src_address := FromAddress,
      src_port := FromPort,
      dest_address := ToAddress,
      dest_port := ToPort
     } = ProxyInfo,
    {ok, {rdns(FromAddress), FromPort,
          rdns(ToAddress),   ToPort}}.

maybe_ntoab(Addr) when is_tuple(Addr) -> rabbit_misc:ntoab(Addr);
maybe_ntoab(Host)                     -> Host.

tcp_host({0,0,0,0}) ->
    hostname();

tcp_host({0,0,0,0,0,0,0,0}) ->
    hostname();

tcp_host(IPAddress) ->
    case inet:gethostbyaddr(IPAddress) of
        {ok, #hostent{h_name = Name}} -> Name;
        {error, _Reason} -> rabbit_misc:ntoa(IPAddress)
    end.

hostname() ->
    {ok, Hostname} = inet:gethostname(),
    case inet:gethostbyname(Hostname) of
        {ok,    #hostent{h_name = Name}} -> Name;
        {error, _Reason}                 -> Hostname
    end.

format_nic_attribute({Key, undefined}) ->
    {Key, undefined};
format_nic_attribute({Key = flags, List}) when is_list(List) ->
    Val = string:join(lists:map(fun rabbit_data_coercion:to_list/1, List), ", "),
    {Key, rabbit_data_coercion:to_binary(Val)};
format_nic_attribute({Key, Tuple}) when is_tuple(Tuple) and (Key =:= addr orelse
                                                             Key =:= broadaddr orelse
                                                             Key =:= netmask orelse
                                                             Key =:= dstaddr) ->
    Val = inet_parse:ntoa(Tuple),
    {Key, rabbit_data_coercion:to_binary(Val)};
format_nic_attribute({Key = hwaddr, List}) when is_list(List) ->
    %% [140, 133, 144, 28, 241, 121] => 8C:85:90:1C:F1:79
    Val = string:join(lists:map(fun(N) -> integer_to_list(N, 16) end, List), ":"),
    {Key, rabbit_data_coercion:to_binary(Val)}.

getifaddrs() ->
    {ok, AddrList} = inet:getifaddrs(),
    Addrs0 = maps:from_list(AddrList),
    maps:map(fun (_Key, Proplist) ->
                lists:map(fun format_nic_attribute/1, Proplist)
             end, Addrs0).

rdns(Addr) ->
    case application:get_env(rabbit, reverse_dns_lookups) of
        {ok, true} -> list_to_binary(tcp_host(Addr));
        _          -> Addr
    end.

sock_funs(inbound)  -> {fun peername/1, fun sockname/1};
sock_funs(outbound) -> {fun sockname/1, fun peername/1}.

is_loopback(Sock) when is_port(Sock) ; ?IS_SSL(Sock) ->
    case sockname(Sock) of
        {ok, {Addr, _Port}} -> is_loopback(Addr);
        {error, _}          -> false
    end;
%% We could parse the results of inet:getifaddrs() instead. But that
%% would be more complex and less maybe Windows-compatible...
is_loopback({127,_,_,_})             -> true;
is_loopback({0,0,0,0,0,0,0,1})       -> true;
is_loopback({0,0,0,0,0,65535,AB,CD}) -> is_loopback(ipv4(AB, CD));
is_loopback(_)                       -> false.

ipv4(AB, CD) -> {AB bsr 8, AB band 255, CD bsr 8, CD band 255}.

unwrap_socket({rabbit_proxy_socket, Sock, _}) ->
    Sock;
unwrap_socket(Sock) ->
    Sock.

maybe_get_proxy_socket(Sock={rabbit_proxy_socket, _, _}) ->
    Sock;
maybe_get_proxy_socket(_Sock) ->
    undefined.
