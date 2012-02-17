-module(rabbit_ws_sockjs_net).

-export([sockname/1, peername/1, is_ssl/1, send/2, port_command/2]).

sockname(Conn) ->
    {ok, {{1,2,3,4}, 1}}.

peername(Conn) ->
    {ok, {{1,2,3,4}, 1}}.

is_ssl(Conn) ->
    false.

send({_, Conn}, Data) ->
    Data1 = iolist_to_binary(Data),
    sockjs:send(Data1, Conn),
    ok.

port_command(Conn, Data)->
    send(Conn, Data).

% ssl_info,ssl_peercert
