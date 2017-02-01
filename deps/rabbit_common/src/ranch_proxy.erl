-module(ranch_proxy).
-behaviour(ranch_transport).

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
         connection_info/2
        ]).

-type proxy_opts() :: ranch_proxy_protocol:proxy_opts().
-type proxy_socket() :: ranch_proxy_protocol:proxy_socket().
-type proxy_protocol_info() :: ranch_proxy_protocol:proxy_protocol_info().

-define(TRANSPORT, ranch_tcp).

-spec name() -> atom().
name() -> proxy_protocol_tcp.

-spec secure() -> boolean().
secure() -> false.

-spec messages() -> tuple().
messages() -> ?TRANSPORT:messages().

-spec match_port(proxy_socket()) -> port().
match_port(ProxySocket) ->
    ranch_proxy_protocol:match_port(?TRANSPORT, ProxySocket).

-spec listen(ranch_tcp:opts()) -> {ok, proxy_socket()} | {error, atom()}.
listen(Opts) ->
    ranch_proxy_protocol:listen(?TRANSPORT, Opts).

-spec accept(proxy_socket(), timeout())
            -> {ok, proxy_socket()} | {error, closed | timeout | not_proxy_protocol |
                                       {timeout, proxy_handshake} | atom()}.
accept(ProxySocket, Timeout) ->
    ranch_proxy_protocol:accept(?TRANSPORT, ProxySocket, Timeout).

-spec accept_ack(proxy_socket(), timeout()) -> ok.
accept_ack(ProxySocket, Timeout) ->
    ranch_proxy_protocol:accept_ack(?TRANSPORT, ProxySocket, Timeout).

-spec connect(inet:ip_address() | inet:hostname(),
              inet:port_number(), any())
             -> {ok, proxy_socket()} | {error, atom()}.
connect(Host, Port, Opts) when is_integer(Port) ->
    connect(Host, Port, Opts, []).

-spec connect(inet:ip_address() | inet:hostname(),
              inet:port_number(), any(), proxy_opts())
             -> {ok, proxy_socket()} | {error, atom()}.
connect(Host, Port, Opts, ProxyOpts) when is_integer(Port) ->
    ranch_proxy_protocol:connect(?TRANSPORT, Host, Port, Opts, ProxyOpts).

-spec recv(proxy_socket(), non_neg_integer(), timeout())
          -> {ok, any()} | {error, closed | atom()}.
recv(ProxySocket, Length, Timeout) ->
    ranch_proxy_protocol:recv(?TRANSPORT, ProxySocket, Length, Timeout).

-spec send(proxy_socket(), iodata()) -> ok | {error, atom()}.
send(ProxySocket, Packet) ->
    ranch_proxy_protocol:send(?TRANSPORT, ProxySocket, Packet).

-spec sendfile(proxy_socket(), file:name_all())
              -> {ok, non_neg_integer()} | {error, atom()}.
sendfile(Socket, Filename) ->
    sendfile(Socket, Filename, 0, 0, []).

-spec sendfile(proxy_socket(), file:name_all() | file:fd(), non_neg_integer(),
               non_neg_integer())
              -> {ok, non_neg_integer()} | {error, atom()}.
sendfile(Socket, File, Offset, Bytes) ->
    sendfile(Socket, File, Offset, Bytes, []).

-spec sendfile(proxy_socket(), file:name_all() | file:fd(), non_neg_integer(),
               non_neg_integer(), [{chunk_size, non_neg_integer()}])
              -> {ok, non_neg_integer()} | {error, atom()}.
sendfile(ProxySocket, Filename, Offset, Bytes, Opts) ->
    ranch_proxy_protocol:sendfile(?TRANSPORT, ProxySocket, Filename, Offset, Bytes, Opts).

-spec setopts(proxy_socket(), list()) -> ok | {error, atom()}.
setopts(ProxySocket, Opts) ->
    ranch_proxy_protocol:setopts(?TRANSPORT, ProxySocket, Opts).

-spec controlling_process(proxy_socket(), pid())
                         -> ok | {error, closed | not_owner | atom()}.
controlling_process(ProxySocket, Pid) ->
    ranch_proxy_protocol:controlling_process(?TRANSPORT, ProxySocket, Pid).

-spec peername(proxy_socket())
              -> {ok, {inet:ip_address(), inet:port_number()}} | {error, atom()}.
peername(ProxySocket) ->
    ranch_proxy_protocol:peername(?TRANSPORT, ProxySocket).

-spec proxyname(proxy_socket()) ->
                       {ok, proxy_protocol_info()}.
proxyname(ProxySocket) ->
    ranch_proxy_protocol:proxyname(?TRANSPORT, ProxySocket).

-spec connection_info(proxy_socket()) -> {ok, list()}.
connection_info(ProxySocket) ->
    ranch_proxy_protocol:connection_info(ProxySocket).

-spec connection_info(proxy_socket(), [protocol | cipher_suite | sni_hostname]) -> {ok, list()}.
connection_info(ProxySocket, Items) ->
    ranch_proxy_protocol:connection_info(ProxySocket, Items).

-spec sockname(proxy_socket())
              -> {ok, {inet:ip_address(), inet:port_number()}} | {error, atom()}.
sockname(ProxySocket) ->
    ranch_proxy_protocol:sockname(?TRANSPORT, ProxySocket).

-spec shutdown(proxy_socket(), read|write|read_write)
              -> ok | {error, atom()}.
shutdown(ProxySocket, How) ->
    ranch_proxy_protocol:shutdown(?TRANSPORT, ProxySocket, How).

-spec close(proxy_socket()) -> ok.
close(ProxySocket) ->
    ranch_proxy_protocol:close(?TRANSPORT, ProxySocket).

-spec bearer_port(proxy_socket()) -> port().
bearer_port(ProxySocket) ->
    ranch_proxy_protocol:bearer_port(?TRANSPORT, ProxySocket).

-spec listen_port(proxy_socket()) -> port().
listen_port(ProxySocket) ->
    ranch_proxy_protocol:listen_port(?TRANSPORT, ProxySocket).

-spec opts_from_socket(atom(), proxy_socket()) ->
                              ranch_proxy_protocol:proxy_opts().
opts_from_socket(Transport, Socket) ->
    ranch_proxy_protocol:opts_from_socket(Transport, Socket).
