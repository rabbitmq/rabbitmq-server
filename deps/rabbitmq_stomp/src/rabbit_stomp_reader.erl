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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_stomp_reader).

-export([start_link/1]).
-export([init/1]).
-export([conserve_memory/2]).

-include("rabbit_stomp_frame.hrl").

-record(reader_state, {socket, parse_state, processor, state, iterations,
                       conserve_memory}).

start_link(ProcessorPid) ->
        {ok, proc_lib:spawn_link(?MODULE, init, [ProcessorPid])}.

init(ProcessorPid) ->
    receive
        {go, Sock} ->
            {ok, {PeerAddress, PeerPort}} = rabbit_net:peername(Sock),
            PeerAddressS = inet_parse:ntoa(PeerAddress),
            error_logger:info_msg("starting STOMP connection ~p from ~s:~p~n",
                                  [self(), PeerAddressS, PeerPort]),

            ParseState = rabbit_stomp_frame:initial_state(),
            try
                mainloop(
                  control_throttle(
                    register_memory_alarm(
                      #reader_state{socket          = Sock,
                                    parse_state     = ParseState,
                                    processor       = ProcessorPid,
                                    state           = running,
                                    iterations      = 0,
                                    conserve_memory = false})), 0)
            after
                rabbit_stomp_processor:flush_and_die(ProcessorPid),
                error_logger:info_msg("ending STOMP connection ~p from ~s:~p~n",
                                      [self(), PeerAddressS, PeerPort])
            end
    end.

mainloop(State = #reader_state{socket = Sock}, ByteCount) ->
    run_socket(State, ByteCount),
    receive
        {inet_async, Sock, _Ref, {ok, Data}} ->
            process_received_bytes(Data, State);
        {inet_async, Sock, _Ref, {error, closed}} ->
            error_logger:info_msg("Socket ~p closed by client~n", [Sock]),
            ok;
        {inet_async, Sock, _Ref, {error, Reason}} ->
            error_logger:error_msg("Socket ~p closed abruptly with "
                                   "error code ~p~n",
                                   [Sock, Reason]),
            ok;
        {conserve_memory, Conserve} ->
            mainloop(
              control_throttle(
                State#reader_state{conserve_memory = Conserve}), ByteCount);
        {bump_credit, Msg} ->
            credit_flow:handle_bump_msg(Msg),
            mainloop(control_throttle(State), ByteCount)
    end.

process_received_bytes([], State) ->
    mainloop(State, 0);
process_received_bytes(Bytes,
                       State = #reader_state{
                         processor   = Processor,
                         parse_state = ParseState,
                         state       = S}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1, Length} ->
            mainloop(State#reader_state{parse_state = ParseState1}, Length);
        {ok, Frame, Rest} ->
            rabbit_stomp_processor:process_frame(Processor, Frame),
            PS = rabbit_stomp_frame:initial_state(),
            process_received_bytes(Rest,
                                   control_throttle(
                                     State#reader_state{
                                       parse_state = PS,
                                       state       = next_state(S, Frame)}))
    end.

conserve_memory(Pid, Conserve) ->
    Pid ! {conserve_memory, Conserve},
    ok.

register_memory_alarm(State) ->
    rabbit_alarm:register(self(), {?MODULE, conserve_memory, []}), State.

control_throttle(State = #reader_state{state            = CS,
                                       conserve_memory  = Mem}) ->
    case {CS, Mem orelse credit_flow:blocked()} of
        {running,   true} -> State#reader_state{state = blocking};
        {blocking, false} -> State#reader_state{state = running};
        {blocked,  false} -> State#reader_state{state = running};
        {_,            _} -> State
    end.

next_state(blocking, #stomp_frame{command = "SEND"}) ->
    blocked;
next_state(S, _) ->
    S.

run_socket(#reader_state{state = blocked}, _ByteCount) ->
    ok;
run_socket(#reader_state{socket = Sock}, ByteCount) ->
    rabbit_net:async_recv(Sock, ByteCount, infinity),
    ok.
