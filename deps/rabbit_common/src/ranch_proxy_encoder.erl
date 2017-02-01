%% Based off https://github.com/haproxy/haproxy/blob/ebec7ecb157c79a9da9283ff38636e4e0ee1fac3/doc/proxy-protocol.txt
%% note: Erlang's bit syntax is big-endian by default. So is this protocol
-module(ranch_proxy_encoder).
-export([v1_encode/4, v2_encode/2, v2_encode/5]).
-include("ranch_proxy.hrl").

-type opts() :: [{negotiated_protocol, binary()}
                |{protocol, sslv3 | tlsv1 | 'tlsv1.1' | 'tlsv1.2'}
                |{sni_hostname, iodata()}
                |{verify, verify_peer | verify_none}
                ].
-export_type([opts/0]).

%%%%%%%%%%%%%%%%%%
%%% PUBLIC API %%%
%%%%%%%%%%%%%%%%%%
%%% Proxy v1
-spec v1_encode(proxy, inet, {inet:ip4_address(), inet:port_number()}, {inet:ip4_address(), inet:port_number()}) -> binary()
    ;          (proxy, inet6, {inet:ip6_address(), inet:port_number()}, {inet:ip6_address(), inet:port_number()}) -> binary().
v1_encode(proxy, Proto, {SrcIp, SrcPort}, {DstIp, DstPort}) ->
    TCP = case Proto of
        inet -> <<"TCP4">>;
        inet6 -> <<"TCP6">>
    end,
    BinSrcIp = list_to_binary(inet:ntoa(SrcIp)),
    BinDstIp = list_to_binary(inet:ntoa(DstIp)),
    BinSrcPort = list_to_binary(integer_to_list(SrcPort)),
    BinDstPort = list_to_binary(integer_to_list(DstPort)),
    <<"PROXY ", TCP/binary, " ", BinSrcIp/binary, " ", BinDstIp/binary, " ",
      BinSrcPort/binary, " ", BinDstPort/binary, "\r\n">>.

%%% Proxy v2

%% supports connection-oriented IP stuff only, no DGRAM nor unix sockets yet.
-spec v2_encode(local, undefined) -> binary().
v2_encode(local, undefined) ->
    Cmd = command(local),
    Proto = protocol(undefined),
    AddrLen = addr_len(undefined),
    %% Header
    <<?HEADER, ?VSN:4, Cmd:4, Proto/binary, AddrLen:16>>.

-spec v2_encode(proxy, inet, {inet:ip4_address(), inet:port_number()}, {inet:ip4_address(), inet:port_number()}, opts()) -> binary()
    ;          (proxy, inet6, {inet:ip6_address(), inet:port_number()}, {inet:ip6_address(), inet:port_number()}, opts()) -> binary().
v2_encode(Command, Protocol, Src, Dst, Opt) ->
    Cmd = command(Command),
    Proto = protocol(Protocol),
    AddrLen = addr_len(Protocol),
    Addresses = addr(Protocol, Src, Dst),
    AdditionalBytes = more(Opt),
    HeaderLen = AddrLen + byte_size(AdditionalBytes),
    %% Header
    <<?HEADER, ?VSN:4, Cmd:4, Proto/binary, HeaderLen:16,
    %% Body
      Addresses:AddrLen/binary, AdditionalBytes/binary>>.

%%%%%%%%%%%%%%%
%%% PRIVATE %%%
%%%%%%%%%%%%%%%
command(local) -> 16#00;
command(proxy) -> 16#11.

protocol(undefined) -> <<?AF_UNSPEC:4, ?UNSPEC:4>>;
protocol(inet) ->      <<?AF_INET:4,   ?STREAM:4>>;
protocol(inet6) ->     <<?AF_INET6:4,  ?STREAM:4>>.
%protocol(unix) ->      <<?AF_UNIX:4,   ?STREAM:4>>.

addr_len(undefined) -> 0;
addr_len(inet) -> 12;
addr_len(inet6) -> 36.
%addr_len(unix) -> 216.

addr(inet, {{SA,SB,SC,SD}, SP}, {{DA,DB,DC,DD}, DP}) ->
      <<SA:8, SB:8, SC:8, SD:8,
        DA:8, DB:8, DC:8, DD:8,
        SP:16, DP:16>>;
addr(inet6, {{SA,SB,SC,SD,SE,SF,SG,SH}, SP}, {{DA,DB,DC,DD,DE,DF,DG,DH}, DP}) ->
      <<SA:16, SB:16, SC:16, SD:16, SE:16, SF:16, SG:16, SH:16,
        DA:16, DB:16, DC:16, DD:16, DE:16, DF:16, DG:16, DH:16,
        SP:16, DP:16>>.

more(List) ->
    iolist_to_binary([check(alpn, List), check(ssl, List)]).

check(alpn, List) ->
    case lists:keyfind(negotiated_protocol, 1, List) of
        {negotiated_protocol, Proto} ->
            <<?PP2_TYPE_ALPN:8, (byte_size(Proto)):16, Proto/binary>>;
        false ->
            <<>>
    end;
check(ssl, List) ->
    case lists:keyfind(protocol, 1, List) of
        {_, Val} -> ssl_record(Val, List);
        _ -> <<>>
    end.

ssl_record(Proto, List) ->
    ClientCert = case lists:keyfind(verify, 1, List) of
        {verify, verify_peer} -> 1; % otherwise the conn would have failed
        {verify, verify_none} -> 0;
        false -> 0
    end,
    ClientSSL = 1, % otherwise the conn would have failed
    BitField = <<0:5,            % UNASSIGNED
                 ClientCert:1,   % PP2_CLIENT_CERT_SESS
                 ClientCert:1,   % PP2_CLIENT_CERT_CONN
                 ClientSSL:1>>,  % PP2_CLIENT_SSL
    Verify = <<(bnot ClientCert):32>>,
    VsnStr = case Proto of
        ssl3 -> <<"SSL 3.0">>;
        tlsv1 -> <<"TLS 1.0">>;
        'tlsv1.1' -> <<"TLS 1.1">>;
        'tlsv1.2' -> <<"TLS 1.2">>
    end,
    Vsn = <<?PP2_SUBTYPE_SSL_VERSION:8, (byte_size(VsnStr)):16, VsnStr/binary>>,
    CN = case lists:keyfind(sni_hostname, 1, List) of
        {_, Name} ->
            CNStr = iolist_to_binary(Name),
            <<?PP2_SUBTYPE_SSL_CN:8, (byte_size(CNStr)):16, CNStr/binary>>;
        _ ->
            <<>>
    end,
    <<?PP2_TYPE_SSL:8, (1+4+byte_size(Vsn)+byte_size(CN)):16,
      BitField/binary, Verify/binary, Vsn/binary, CN/binary>>.
