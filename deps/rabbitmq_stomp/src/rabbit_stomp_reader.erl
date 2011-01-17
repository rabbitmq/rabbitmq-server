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
-export([init/1, mainloop/1]).

-include("rabbit_stomp_frame.hrl").

-record(reader_state, {socket, parse_state, processor}).

start_link(ProcessorPid) ->
        {ok, proc_lib:spawn_link(?MODULE, init, [ProcessorPid])}.

init(ProcessorPid) ->
    receive
        {go, Sock} ->
            ok = inet:setopts(Sock, [{active, once}]),

            {ok, {PeerAddress, PeerPort}} = inet:peername(Sock),
            PeerAddressS = inet_parse:ntoa(PeerAddress),
            error_logger:info_msg("starting STOMP connection ~p from ~s:~p~n",
                                  [self(), PeerAddressS, PeerPort]),
            ParseState = rabbit_stomp_frame:initial_state(),
            try
                ?MODULE:mainloop(#reader_state{socket        = Sock,
                                               parse_state   = ParseState,
                                               processor     = ProcessorPid})
            after
                error_logger:info_msg("ending STOMP connection ~p from ~s:~p~n",
                                      [self(), PeerAddressS, PeerPort])
            end
    end.

mainloop(State) ->
    receive
        {tcp, Sock, Bytes} ->
            inet:setopts(Sock, [{active, once}]),
            process_received_bytes(Bytes, State);
        {tcp_closed, _Sock} ->
            ok
    end.

process_received_bytes([], State) ->
    ?MODULE:mainloop(State);
process_received_bytes(Bytes,
                       State = #reader_state{
                         processor   = Processor,
                         parse_state = ParseState}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            ?MODULE:mainloop(State#reader_state{parse_state = ParseState1});
        {ok, Frame, Rest} ->
            rabbit_stomp_processor:process_frame(Processor, Frame),
            PS = rabbit_stomp_frame:initial_state(),
            process_received_bytes(Rest, State#reader_state{parse_state = PS})
    end.
