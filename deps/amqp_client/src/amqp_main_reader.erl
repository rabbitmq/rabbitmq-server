%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_main_reader).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/4]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {sock,
                connection,
                channels_manager,
                astate,
                message = none %% none | {Type, Channel, Length}
               }).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Sock, Connection, ChMgr, AState) ->
    gen_server:start_link(?MODULE, [Sock, Connection, ChMgr, AState], []).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init([Sock, Connection, ChMgr, AState]) ->
    {ok, _Ref} = rabbit_net:async_recv(Sock, 7, infinity),
    {ok, #state{sock = Sock, connection = Connection,
                channels_manager = ChMgr, astate = AState}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

handle_call(Call, From, State) ->
    {stop, {unexpected_call, Call, From}, State}.

handle_cast(Cast, State) ->
    {stop, {unexpected_cast, Cast}, State}.

handle_info({inet_async, _, _, _} = InetAsync, State) ->
    handle_inet_async(InetAsync, State).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

handle_inet_async({inet_async, Sock, _, Msg},
                  State = #state{sock = Sock, message = CurMessage}) ->
    {Type, Number, Length} = case CurMessage of {T, N, L} -> {T, N, L};
                                                none      -> {none, none, none}
                             end,
    case Msg of
        {ok, <<Payload:Length/binary, ?FRAME_END>>} ->
            State1 = process_frame(Type, Number, Payload, State),
            {ok, _Ref} = rabbit_net:async_recv(Sock, 7, infinity),
            {noreply, State1#state{message = none}};
        {ok, <<NewType:8, NewChannel:16, NewLength:32>>} ->
            {ok, _Ref} = rabbit_net:async_recv(Sock, NewLength + 1, infinity),
            {noreply, State#state{message={NewType, NewChannel, NewLength}}};
        {error, closed} ->
            State#state.connection ! socket_closed,
            {noreply, State};
        {error, Reason} ->
            State#state.connection ! {socket_error, Reason},
            {stop, {socket_error, Reason}, State}
    end.

process_frame(Type, ChNumber, Payload, State = #state{connection = Connection}) ->
    case rabbit_command_assembler:analyze_frame(Type, Payload, ?PROTOCOL) of
        heartbeat when ChNumber /= 0 ->
            amqp_gen_connection:server_misbehaved(
                Connection,
                #amqp_error{name        = command_invalid,
                            explanation = "heartbeat on non-zero channel"}),
            State;
        %% Match heartbeats but don't do anything with them
        heartbeat ->
            State;
        AnalyzedFrame ->
            pass_frame(ChNumber, AnalyzedFrame, State)
    end.

pass_frame(0, Frame, State = #state{connection = Conn, astate = AState}) ->
    State#state{astate = amqp_channels_manager:process_channel_frame(
                           Frame, 0, Conn, AState)};
pass_frame(Number, Frame, State = #state{channels_manager = ChMgr}) ->
    amqp_channels_manager:pass_frame(ChMgr, Number, Frame),
    State.
