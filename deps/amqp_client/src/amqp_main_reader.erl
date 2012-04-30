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
    case next(7, #state{sock = Sock, connection = Connection,
                        channels_manager = ChMgr, astate = AState}) of
        {noreply, State}       -> {ok, State};
        {stop, Reason, _State} -> {stop, Reason}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(Call, From, State) ->
    {stop, {unexpected_call, Call, From}, State}.

handle_cast(Cast, State) ->
    {stop, {unexpected_cast, Cast}, State}.

handle_info({inet_async, Sock, _, {ok, <<Type:8, Channel:16, Length:32>>}},
            State = #state{sock = Sock, message = none}) ->
    next(Length + 1, State#state{message = {Type, Channel, Length}});
handle_info({inet_async, Sock, _, {ok, Data}},
            State = #state{sock = Sock, message = {Type, Channel, L}}) ->
    <<Payload:L/binary, ?FRAME_END>> = Data,
    next(7, process_frame(Type, Channel, Payload, State#state{message = none}));
handle_info({inet_async, Sock, _, {error, Reason}},
            State = #state{sock = Sock}) ->
    handle_error(Reason, State).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

process_frame(Type, ChNumber, Payload,
              State = #state{connection       = Connection,
                             channels_manager = ChMgr,
                             astate           = AState}) ->
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
        AnalyzedFrame when ChNumber /= 0 ->
            amqp_channels_manager:pass_frame(ChMgr, ChNumber, AnalyzedFrame),
            State;
        AnalyzedFrame ->
            State#state{astate = amqp_channels_manager:process_channel_frame(
                                   AnalyzedFrame, 0, Connection, AState)}
    end.

next(Length, State = #state{sock = Sock}) ->
     case rabbit_net:async_recv(Sock, Length, infinity) of
         {ok, _}         -> {noreply, State};
         {error, Reason} -> handle_error(Reason, State)
     end.

handle_error(closed, State = #state{connection = Conn}) ->
    Conn ! socket_closed,
    {noreply, State};
handle_error(Reason, State = #state{connection = Conn}) ->
    Conn ! {socket_error, Reason},
    {stop, {socket_error, Reason}, State}.
