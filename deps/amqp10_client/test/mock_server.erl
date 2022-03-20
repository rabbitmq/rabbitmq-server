%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(mock_server).

%% API functions
-export([start/1,
         set_steps/2,
         stop/1,
         run/1,
         amqp_step/1,
         send_amqp_header_step/1,
         recv_amqp_header_step/1
        ]).

-include_lib("src/amqp10_client.hrl").

start(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}]),
    Pid = spawn(?MODULE, run, [LSock]),
    {LSock, Pid}.

set_steps({_Sock, Pid}, Steps) ->
    Pid ! {set_steps, Steps},
    ok.

stop({S, P}) ->
    P ! close,
    gen_tcp:close(S),
    exit(P, stop).

run(Listener) ->
    receive
        {set_steps, Steps} ->
            {ok, Sock} = gen_tcp:accept(Listener),
            lists:foreach(fun(S) -> S(Sock) end, Steps),
            receive
                close -> ok
            end
    end.


send(Socket, Ch, Records) ->
    Encoded = [amqp10_framing:encode_bin(R) || R <- Records],
    Frame = amqp10_binary_generator:build_frame(Ch, Encoded),
    ok = gen_tcp:send(Socket, Frame).

recv(Sock) ->
    {ok, <<Length:32/unsigned, 2:8/unsigned,
           _/unsigned, Ch:16/unsigned>>} = gen_tcp:recv(Sock, 8),
    {ok, Data} = gen_tcp:recv(Sock, Length - 8),
    {PerfDesc, Payload} = amqp10_binary_parser:parse(Data),
    Perf = amqp10_framing:decode(PerfDesc),
    {Ch, Perf, Payload}.

amqp_step(Fun) ->
    fun (Sock) ->
            Recv = recv(Sock),
            ct:pal("AMQP Step receieved ~p~n", [Recv]),
            case Fun(Recv) of
                {_Ch, []} -> ok;
                {Ch, {multi, Records}} ->
                    [begin
                         ct:pal("AMQP multi Step send ~p~n", [R]),
                         send(Sock, Ch, R)
                     end || R <- Records];
                {Ch, Records} ->
                    ct:pal("AMQP Step send ~p~n", [Records]),
                    send(Sock, Ch, Records)
            end
    end.


send_amqp_header_step(Sock) ->
    ct:pal("Sending AMQP protocol header"),
    ok = gen_tcp:send(Sock, ?AMQP_PROTOCOL_HEADER).

recv_amqp_header_step(Sock) ->
    ct:pal("Receiving AMQP protocol header"),
    {ok, R} = gen_tcp:recv(Sock, 8),
    ct:pal("handshake Step receieved ~p~n", [R]).
