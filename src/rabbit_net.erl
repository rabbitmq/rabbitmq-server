%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_net).
-include("rabbit.hrl").

-export([async_recv/3, close/1, controlling_process/2,
        getstat/2, peername/1, peercert/1, port_command/2,
        send/2, sockname/1]).

%%---------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([socket/0]).

-type(stat_option() ::
	'recv_cnt' | 'recv_max' | 'recv_avg' | 'recv_oct' | 'recv_dvi' |
	'send_cnt' | 'send_max' | 'send_avg' | 'send_oct' | 'send_pend').
-type(error() :: rabbit_types:error(any())).
-type(socket() :: rabbit_networking:ip_port() | rabbit_types:ssl_socket()).

-spec(async_recv/3 ::
        (socket(), integer(), timeout()) -> rabbit_types:ok(any())).
-spec(close/1 :: (socket()) -> rabbit_types:ok_or_error(any())).
-spec(controlling_process/2 ::
        (socket(), pid()) -> rabbit_types:ok_or_error(any())).
-spec(port_command/2 :: (socket(), iolist()) -> 'true').
-spec(send/2 ::
        (socket(), binary() | iolist()) -> rabbit_types:ok_or_error(any())).
-spec(peername/1 ::
        (socket())
        -> rabbit_types:ok({inet:ip_address(), rabbit_networking:ip_port()}) |
           error()).
-spec(peercert/1 ::
        (rabbit_types:ssl_socket()) -> rabbit_types:ok(any()) | error()).
        %% any() should be x509_certificate()
-spec(sockname/1 ::
        (socket())
        -> rabbit_types:ok({inet:ip_address(), rabbit_networking:ip_port()}) |
           error()).
-spec(getstat/2 ::
        (socket(), [stat_option()])
        -> rabbit_types:ok([{stat_option(), integer()}]) | error()).

-endif.

%%---------------------------------------------------------------------------


async_recv(Sock, Length, Timeout) when is_record(Sock, ssl_socket) ->
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

close(Sock) when is_record(Sock, ssl_socket) ->
    ssl:close(Sock#ssl_socket.ssl);

close(Sock) when is_port(Sock) ->
    gen_tcp:close(Sock).


controlling_process(Sock, Pid) when is_record(Sock, ssl_socket) ->
    ssl:controlling_process(Sock#ssl_socket.ssl, Pid);

controlling_process(Sock, Pid) when is_port(Sock) ->
    gen_tcp:controlling_process(Sock, Pid).


getstat(Sock, Stats) when is_record(Sock, ssl_socket) ->
    inet:getstat(Sock#ssl_socket.tcp, Stats);

getstat(Sock, Stats) when is_port(Sock) ->
    inet:getstat(Sock, Stats).


peername(Sock) when is_record(Sock, ssl_socket) ->
    ssl:peername(Sock#ssl_socket.ssl);

peername(Sock) when is_port(Sock) ->
    inet:peername(Sock).


peercert(Sock) when is_record(Sock, ssl_socket) ->
    public_key:pkix_decode_cert(ssl:peercert(Sock#ssl_socket.ssl), plain);
peercert(_) ->
    nossl.


port_command(Sock, Data) when is_record(Sock, ssl_socket) ->
    case ssl:send(Sock#ssl_socket.ssl, Data) of
        ok ->
            self() ! {inet_reply, Sock, ok},
            true;
        {error, Reason} ->
            erlang:error(Reason)
    end;

port_command(Sock, Data) when is_port(Sock) ->
    erlang:port_command(Sock, Data).

send(Sock, Data) when is_record(Sock, ssl_socket) ->
    ssl:send(Sock#ssl_socket.ssl, Data);

send(Sock, Data) when is_port(Sock) ->
    gen_tcp:send(Sock, Data).


sockname(Sock) when is_record(Sock, ssl_socket) ->
    ssl:sockname(Sock#ssl_socket.ssl);

sockname(Sock) when is_port(Sock) ->
    inet:sockname(Sock).
