%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_net).

-include_lib("kernel/include/inet.hrl").
-include_lib("kernel/include/net_address.hrl").

-export([is_ssl/1, ssl_info/1, controlling_process/2, getstat/2,
    recv/1, sync_recv/2, async_recv/3, getopts/2,
    setopts/2, send/2, close/1, fast_close/1, sockname/1, peername/1,
    peercert/1, connection_string/2, socket_ends/2, is_loopback/1,
    tcp_host/1, unwrap_socket/1, maybe_get_proxy_socket/1,
    hostname/0, getifaddrs/0, proxy_ssl_info/2,
    dist_info/0]).

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
-type proxy_socket() :: {'rabbit_proxy_socket', ranch_transport:socket(), ranch_proxy_header:proxy_info()}.
% -type host_or_ip() :: binary() | inet:ip_address().
-spec is_ssl(socket()) -> boolean().
-spec ssl_info(socket()) -> 'nossl' | ok_val_or_error([{atom(), any()}]).
-spec proxy_ssl_info(socket(), proxy_socket() | 'undefined') -> 'nossl' | ok_val_or_error([{atom(), any()}]).
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
-spec getopts
        (socket(),
         [atom() |
          {raw, non_neg_integer(), non_neg_integer(),
           non_neg_integer() | binary()}]) ->
            ok_val_or_error(opts()).
-spec setopts(socket(), opts()) -> ok_or_any_error().
-spec send(socket(), iodata()) -> ok_or_any_error().
-spec close(socket()) -> ok_or_any_error().
-spec fast_close(socket()) -> ok_or_any_error().
-spec sockname(socket()) ->
          ok_val_or_error({inet:ip_address(), ip_port()}).
-spec peername(socket()) ->
          ok_val_or_error({inet:ip_address(), ip_port()}).
-spec peercert(socket()) ->
          'nossl' | ok_val_or_error(rabbit_cert_info:certificate()).
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
    andalso (element(1, Sock) =:= sslsocket)).

is_ssl(Sock) -> ?IS_SSL(Sock).

ssl_info(Sock) when ?IS_SSL(Sock) ->
    ssl:connection_information(Sock);
ssl_info(_Sock) ->
    nossl.

proxy_ssl_info(Sock, {rabbit_proxy_socket, _, ProxyInfo}) ->
    ConnInfo = ranch_proxy_header:to_connection_info(ProxyInfo),
    case lists:keymember(protocol, 1, ConnInfo) andalso
         lists:keymember(selected_cipher_suite, 1, ConnInfo) of
        true ->
            {ok, ConnInfo};
        false ->
            ssl_info(Sock)
    end;
proxy_ssl_info(Sock, _) ->
    ssl_info(Sock).


controlling_process(Sock, Pid) when ?IS_SSL(Sock) ->
    ssl:controlling_process(Sock, Pid);
controlling_process(Sock, Pid) when is_port(Sock) ->
    gen_tcp:controlling_process(Sock, Pid).

getstat(Sock, Stats) when ?IS_SSL(Sock) ->
    ssl:getstat(Sock, Stats);
getstat(Sock, Stats) when is_port(Sock) ->
    inet:getstat(Sock, Stats);
%% Used by Proxy protocol support in plugins
getstat({rabbit_proxy_socket, Sock, _}, Stats) when ?IS_SSL(Sock) ->
    ssl:getstat(Sock, Stats);
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
    _ = ssl:close(Sock, ?SSL_CLOSE_TIMEOUT),
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
                   "~ts:~tp -> ~ts:~tp",
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
socket_ends({rabbit_proxy_socket, Sock, ProxyInfo}, Direction) ->
    case ProxyInfo of
        %% LOCAL header: we take the IP/ports from the socket.
        #{command := local} ->
            socket_ends(Sock, Direction);
        %% PROXY header: use the IP/ports from the proxy header.
        #{
          src_address := FromAddress,
          src_port := FromPort,
          dest_address := ToAddress,
          dest_port := ToPort
         } ->
            {ok, {rdns(FromAddress), FromPort,
                  rdns(ToAddress),   ToPort}}
    end.

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

%% deps/prometheus/src/collectors/vm/prometheus_vm_dist_collector.erl
%% https://github.com/deadtrickster/prometheus.erl/blob/v4.8.2/src/collectors/vm/prometheus_vm_dist_collector.erl#L386-L450
dist_info() ->
    {ok, NodesInfo} = net_kernel:nodes_info(),
    TcpInetPorts = [P || P <- erlang:ports(),
                         erlang:port_info(P, name) =:= {name, "tcp_inet"}],
    [dist_info(NodeInfo, TcpInetPorts) || NodeInfo <- NodesInfo].

dist_info({Node, Info}, TcpInetPorts) ->
    DistPid = proplists:get_value(owner, Info),
    case proplists:get_value(address, Info, #net_address{}) of
        #net_address{address=undefined} ->
            %% No stats available
            {Node, DistPid, []};
        #net_address{address=PeerAddr} ->
            dist_info(Node, TcpInetPorts, DistPid, PeerAddr)
    end.

dist_info(Node, TcpInetPorts, DistPid, PeerAddrArg) ->
    case [P || P <- TcpInetPorts, inet:peername(P) =:= {ok, PeerAddrArg}] of
        %% Note: sometimes the port closes before we get here and thus can't get stats
        %% See rabbitmq/rabbitmq-server#5490
        [] ->
            {Node, DistPid, []};
        [DistPort] ->
            S = dist_port_stats(DistPort),
            {Node, DistPid, S};
        %% And sometimes there multiple ports, most likely right after a peer node
        %% restart.
        DistPorts when is_list(DistPorts) ->
            ConnectedPorts = lists:filter(fun(Port) ->
                                            case erlang:port_info(Port, connected) of
                                                {connected, AssocPid} ->
                                                    erlang:is_process_alive(AssocPid);
                                                undefined ->
                                                    false
                                            end
                                          end, DistPorts),
            case ConnectedPorts of
                [] ->
                    {Node, DistPid, []};
                CPs when is_list(CPs) ->
                    S = dist_port_stats(hd(CPs)),
                    {Node, DistPid, S}
            end
    end.

dist_port_stats(DistPort) ->
    case {socket_ends(DistPort, inbound), getstat(DistPort, [recv_oct, send_oct])} of
        {{ok, {PeerAddr, PeerPort, SockAddr, SockPort}}, {ok, Stats}} ->
            PeerAddrBin = rabbit_data_coercion:to_binary(maybe_ntoab(PeerAddr)),
            SockAddrBin = rabbit_data_coercion:to_binary(maybe_ntoab(SockAddr)),
            [{peer_addr, PeerAddrBin},
                {peer_port, PeerPort},
                {sock_addr, SockAddrBin},
                {sock_port, SockPort},
                {recv_bytes, rabbit_misc:pget(recv_oct, Stats)},
                {send_bytes, rabbit_misc:pget(send_oct, Stats)}];
        _ ->
            []
    end.
