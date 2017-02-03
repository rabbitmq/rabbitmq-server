%% Copyright (c) 2012, Heroku Inc.
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are
%% met:
%%
%% * Redistributions of source code must retain the above copyright
%%   notice, this list of conditions and the following disclaimer.
%%
%% * Redistributions in binary form must reproduce the above copyright
%%   notice, this list of conditions and the following disclaimer in the
%%   documentation and/or other materials provided with the distribution.
%%
%% * The names of its contributors may not be used to endorse or promote
%%   products derived from this software without specific prior written
%%   permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
%% "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
%% LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
%% A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
%% OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%% SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
%% LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
%% DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
%% THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%% (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
%% OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-module(ranch_proxy_ssl).
-behaviour(ranch_transport).

-record(ssl_socket, { upgraded = false :: boolean(),
                      proxy_socket :: ranch_proxy_protocol:proxy_socket(),
                      sslopts :: ranch_ssl:opts()
                    }).

-export([name/0,
         secure/0,
         messages/0,
         listen/1,
         accept/2,
         accept_ack/2,
         connect/3,
         connect/4,
         recv/3,
         send/2,
         sendfile/2,
         sendfile/4,
         sendfile/5,
         setopts/2,
         controlling_process/2,
         peername/1,
         proxyname/1,
         sockname/1,
         shutdown/2,
         close/1,
         opts_from_socket/2,
         bearer_port/1,
         listen_port/1,
         match_port/1,
         connection_info/1,
         connection_info/2,
         ssl_connection_information/1,
         ssl_connection_information/2
        ]).

% Record manipulation
-export([get_csocket/1]).

-type proxy_opts() :: ranch_proxy_protocol:proxy_opts().
-type proxy_socket() :: ranch_proxy_protocol:proxy_socket().
-type proxy_protocol_info() :: ranch_proxy_protocol:proxy_protocol_info().
-opaque ssl_socket() :: #ssl_socket{}.

-define(TRANSPORT, ranch_ssl).

-export_type([ssl_socket/0]).

%% Record manipulation API
-spec get_csocket(ssl_socket()) -> port().
get_csocket(#ssl_socket{proxy_socket=ProxySocket}) ->
    ranch_proxy_protocol:get_csocket(ProxySocket).

-spec name() -> atom().
name() -> proxy_protocol_ssl.

-spec secure() -> boolean().
secure() -> true.

-spec messages() -> tuple().
messages() -> ranch_ssl:messages().

-spec listen(ranch_ssl:opts()) -> {ok, ssl_socket()} | {error, atom()}.
listen(Opts) ->
    {SslOpts, SocketOpts} = filter_ssl_opts(Opts, [], []),
    case ranch_proxy:listen(SocketOpts) of
        {ok, ProxySocket} ->
            {ok, #ssl_socket{proxy_socket = ProxySocket,
                             upgraded     = false,
                             sslopts      = SslOpts}};
        {error, Error} ->
            {error, Error}
    end.

-spec accept(proxy_socket(), timeout())
            -> {ok, ssl_socket()} | {error, closed | timeout | not_proxy_protocol |
                                     closed_on_ssl_accept |
                                     {timeout, proxy_handshake} | atom()}.
accept(#ssl_socket{proxy_socket = ProxySocket,
                   sslopts      = Opts} = ProxySslSocket, Timeout) ->
    case ranch_proxy:accept(ProxySocket, Timeout) of
        {ok, ProxySocket1} ->
            CSocket = ranch_proxy_protocol:get_csocket(ProxySocket1),
            SSLOpts = application:get_env(ranch_proxy_protocol, ssl_accept_opts, []),
            case ssl:ssl_accept(CSocket, SSLOpts++Opts, Timeout) of
                {ok, SslSocket} ->
                    ProxySocket2 = ranch_proxy_protocol:set_csocket(ProxySocket1,
                                                                    SslSocket),
                    {ok, ProxySslSocket#ssl_socket{proxy_socket = ProxySocket2,
                                                   upgraded     = true}};
                {error, closed} ->
                    {error, closed_on_ssl_accept};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.

-spec accept_ack(ssl_socket(), timeout()) -> ok.
accept_ack(#ssl_socket{proxy_socket = ProxySocket,
                       upgraded = false}, Timeout) ->
    ranch_proxy_protocol:accept_ack(?TRANSPORT, ProxySocket, Timeout);
accept_ack(_, _) ->
    ok.

-spec connect(inet:ip_address() | inet:hostname(),
              inet:port_number(), any())
             -> {ok, ssl_socket()} | {error, atom()}.
connect(Host, Port, Opts) when is_integer(Port) ->
    connect(Host, Port, Opts, []).

-spec connect(inet:ip_address() | inet:hostname(),
              inet:port_number(), any(), proxy_opts())
             -> {ok, ssl_socket()} | {error, atom()}.
connect(Host, Port, Opts, ProxyOpts) when is_integer(Port) ->
    % Before connecting remove the SSL specific options.
    % @todo extend to support them all
    {SslOpts, SocketOpts} = filter_ssl_opts(Opts, [], []),
    case ranch_proxy:connect(Host, Port, SocketOpts, ProxyOpts) of
        {ok, ProxySocket} ->
            % The proxy protocol header has been sent. The connection is now
            % ready to be upgraded. Ranch_ssl doesn't support upgrading a socket
            % so it is done here
            upgrade_to_ssl(ProxySocket, SslOpts);
        {error, Error} ->
            {error, Error}
    end.

-spec recv(ssl_socket(), non_neg_integer(), timeout())
          -> {ok, any()} | {error, closed | atom()}.
recv(#ssl_socket{proxy_socket = ProxySocket}, Length, Timeout) ->
    ranch_proxy_protocol:recv(?TRANSPORT, ProxySocket, Length, Timeout).

-spec send(ssl_socket(), iodata()) -> ok | {error, atom()}.
send(#ssl_socket{proxy_socket = ProxySocket}, Packet) ->
    ranch_proxy_protocol:send(?TRANSPORT, ProxySocket, Packet).

-spec sendfile(ssl_socket(), file:name_all())
              -> {ok, non_neg_integer()} | {error, atom()}.
sendfile(SslSocket, Filename) ->
    sendfile(SslSocket, Filename, 0, 0, []).

-spec sendfile(ssl_socket(), file:name_all() | file:fd(), non_neg_integer(),
               non_neg_integer())
              -> {ok, non_neg_integer()} | {error, atom()}.
sendfile(SslSocket, File, Offset, Bytes) ->
    sendfile(SslSocket, File, Offset, Bytes, []).

-spec sendfile(ssl_socket(), file:name_all() | file:fd(), non_neg_integer(),
               non_neg_integer(), [{chunk_size, non_neg_integer()}])
              -> {ok, non_neg_integer()} | {error, atom()}.
sendfile(#ssl_socket{proxy_socket = ProxySocket}, Filename, Offset, Bytes, Opts) ->
    ranch_proxy_protocol:sendfile(?TRANSPORT, ProxySocket, Filename, Offset, Bytes, Opts).

-spec setopts(ssl_socket(), list()) -> ok | {error, atom()}.
setopts(#ssl_socket{proxy_socket = ProxySocket}, Opts) ->
    ranch_proxy_protocol:setopts(?TRANSPORT, ProxySocket, Opts).

-spec controlling_process(ssl_socket(), pid())
                         -> ok | {error, closed | not_owner | atom()}.
controlling_process(#ssl_socket{proxy_socket = ProxySocket,
                                upgraded = false}, Pid) ->
    ranch_proxy:controlling_process(ProxySocket, Pid);
controlling_process(#ssl_socket{proxy_socket = ProxySocket}, Pid) ->
    ranch_proxy_protocol:controlling_process(?TRANSPORT, ProxySocket, Pid).

-spec peername(ssl_socket())
              -> {ok, {inet:ip_address(), inet:port_number()}} | {error, atom()}.
peername(#ssl_socket{proxy_socket = ProxySocket}) ->
    ranch_proxy_protocol:peername(?TRANSPORT, ProxySocket).

-spec proxyname(ssl_socket()) ->
                       {ok, proxy_protocol_info()}.
proxyname(#ssl_socket{proxy_socket = ProxySocket}) ->
    ranch_proxy_protocol:proxyname(?TRANSPORT, ProxySocket).

-spec sockname(ssl_socket())
              -> {ok, {inet:ip_address(), inet:port_number()}} | {error, atom()}.
sockname(#ssl_socket{proxy_socket = ProxySocket,
                                upgraded = false}) ->
    ranch_proxy:sockname(ProxySocket);
sockname(#ssl_socket{proxy_socket = ProxySocket}) ->
    ranch_proxy_protocol:sockname(?TRANSPORT, ProxySocket).

-spec shutdown(ssl_socket(), read|write|read_write)
              -> ok | {error, atom()}.
shutdown(#ssl_socket{proxy_socket = ProxySocket}, How) ->
    ranch_proxy_protocol:shutdown(?TRANSPORT, ProxySocket, How).

-spec close(ssl_socket()) -> ok.
close(#ssl_socket{proxy_socket = ProxySocket}) ->
    ranch_proxy_protocol:close(?TRANSPORT, ProxySocket).

-spec bearer_port(ssl_socket()) -> port().
bearer_port(#ssl_socket{proxy_socket=ProxySocket}) ->
    ranch_proxy_protocol:bearer_port(?TRANSPORT, ProxySocket).

-spec listen_port(ssl_socket()) -> port().
listen_port(#ssl_socket{proxy_socket=ProxySocket}) ->
    ranch_proxy_protocol:listen_port(?TRANSPORT, ProxySocket).

match_port(#ssl_socket{proxy_socket=ProxySocket}) ->
    ranch_proxy_protocol:match_port(?TRANSPORT, ProxySocket).

-spec connection_info(ssl_socket()) -> {ok, list()}.
connection_info(#ssl_socket{proxy_socket=ProxySocket}) ->
    ranch_proxy_protocol:connection_info(ProxySocket).

-spec connection_info(ssl_socket(), [protocol | cipher_suite | sni_hostname]) -> {ok, list()}.
connection_info(#ssl_socket{proxy_socket=ProxySocket}, Items) ->
    ranch_proxy_protocol:connection_info(ProxySocket, Items).

%% @doc Expose SSL-only options regarding the current live SSL connection for
%% introspection purposes.
%% This callback does not make sense for others protocols of this library,
%% since non-SSL connections should have none of this information within them.
%%
%% It is therefore distinct from `connection_info/1-2' whose purpose is
%% to extract PROXY-level information, whereas this is a tunnel to
%% `ssl:connection_information/1-2', which contains SSL-level protocol data.
-spec ssl_connection_information(ssl_socket()) -> {ok, list()}.
ssl_connection_information(Socket = #ssl_socket{upgraded=true}) ->
    Bearer = bearer_port(Socket),
    ssl:connection_information(Bearer).

-spec ssl_connection_information(ssl_socket(), [protocol | cipher_suite | sni_hostname]) -> {ok, list()}.
ssl_connection_information(Socket = #ssl_socket{upgraded=true}, Items) ->
    Bearer = bearer_port(Socket),
    ssl:connection_information(Bearer, Items).

-spec opts_from_socket(atom(), ssl_socket()) ->
                              ranch_proxy_protocol:proxy_opts().
opts_from_socket(Transport, Socket) ->
    ranch_proxy_protocol:opts_from_socket(Transport, Socket).

% Internal
upgrade_to_ssl(ProxySocket, Opts) ->
    CSocket = ranch_proxy_protocol:get_csocket(ProxySocket),
    case ssl:connect(CSocket, Opts, 1000) of
        {ok, SecureSocket} ->
            ProxySocket1 = ranch_proxy_protocol:set_csocket(ProxySocket,
                                                            SecureSocket),
            {ok, #ssl_socket{proxy_socket = ProxySocket1,
                             sslopts = Opts,
                             upgraded = true}};
        {error, Error} ->
            {error, Error}
    end.

filter_ssl_opts([], SslOpts, SocketOpts) ->
    {SslOpts, SocketOpts};
filter_ssl_opts([{Key, _}=SslOpt|Rest], SslOpts, SocketOpts) when
      Key == alpn_advertised_protocols;
      Key == alpn_preferred_protocols;
      Key == beast_mitigation;
      Key == cacertfile;
      Key == cacerts;
      Key == cert;
      Key == certfile;
      Key == ciphers;
      Key == client;
      Key == client_preferred_next_protocols;
      Key == client_preferred_next_protocols;
      Key == client_renegotiation;
      Key == crl_cache;
      Key == crl_check;
      Key == depth;
      Key == dh;
      Key == dhfile;
      Key == fail_if_no_peer_cert;
      Key == fallback;
      Key == hibernate_after;
      Key == honor_cipher_order;
      Key == key;
      Key == keyfile;
      Key == log_alert;
      Key == next_protocols_advertised;
      Key == padding_check;
      Key == partial_chain;
      Key == password;
      Key == psk_identity;
      Key == reuse_session;
      Key == reuse_sessions;
      Key == secure_renegotiate;
      Key == server_name_indication;
      Key == signature_algs;
      Key == sni_fun;
      Key == sni_hosts;
      Key == srp_identity;
      Key == ssl_imp;
      Key == user_lookup_fun;
      Key == verify;
      Key == verify_fun;
      Key == versions ->
    filter_ssl_opts(Rest, [SslOpt|SslOpts], SocketOpts);
filter_ssl_opts([SocketOpt|Rest], SslOpts, SocketOpts) ->
    filter_ssl_opts(Rest, SslOpts, [SocketOpt|SocketOpts]).
