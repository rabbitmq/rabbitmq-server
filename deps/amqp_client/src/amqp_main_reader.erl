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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

%% @private
-module(amqp_main_reader).

-include("amqp_client_internal.hrl").

-behaviour(gen_server).

-export([start_link/5]).
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

start_link(Sock, Connection, ChMgr, AState, ConnName) ->
    gen_server:start_link(
      ?MODULE, [Sock, Connection, ConnName, ChMgr, AState], []).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init([Sock, Connection, ConnName, ChMgr, AState]) ->
    ?store_proc_name(ConnName),
    State = #state{sock             = Sock,
                   connection       = Connection,
                   channels_manager = ChMgr,
                   astate           = AState,
                   message          = none},
    case rabbit_net:async_recv(Sock, 0, infinity) of
        {ok, _}         -> {ok, State};
        {error, Reason} -> {stop, Reason, _} = handle_error(Reason, State),
                           {stop, Reason}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(Call, From, State) ->
    {stop, {unexpected_call, Call, From}, State}.

handle_cast(Cast, State) ->
    {stop, {unexpected_cast, Cast}, State}.

handle_info({inet_async, Sock, _, {ok, Data}},
            State = #state {sock = Sock}) ->
    %% Latency hiding: Request next packet first, then process data
    case rabbit_net:async_recv(Sock, 0, infinity) of
         {ok, _}         -> handle_data(Data, State);
         {error, Reason} -> handle_error(Reason, State)
    end;
handle_info({inet_async, Sock, _, {error, Reason}},
            State = #state{sock = Sock}) ->
    handle_error(Reason, State).

handle_data(<<Type:8, Channel:16, Length:32, Payload:Length/binary, ?FRAME_END,
              More/binary>>,
            #state{message = none} = State) when
      Type =:= ?FRAME_METHOD; Type =:= ?FRAME_HEADER;
      Type =:= ?FRAME_BODY;   Type =:= ?FRAME_HEARTBEAT ->
    %% Optimisation for the direct match
    handle_data(
      More, process_frame(Type, Channel, Payload, State#state{message = none}));
handle_data(<<Type:8, Channel:16, Length:32, Data/binary>>,
            #state{message = none} = State) when
      Type =:= ?FRAME_METHOD; Type =:= ?FRAME_HEADER;
      Type =:= ?FRAME_BODY;   Type =:= ?FRAME_HEARTBEAT ->
    {noreply, State#state{message = {Type, Channel, Length, Data}}};
handle_data(<<"AMQP", A, B, C>>, #state{sock = Sock, message = none} = State) ->
    {ok, <<D>>} = rabbit_net:sync_recv(Sock, 1),
    handle_error({refused, {A, B, C, D}}, State);
handle_data(<<Malformed:7/binary, _Rest/binary>>,
            #state{message = none} = State) ->
    handle_error({malformed_header, Malformed}, State);
handle_data(<<Data/binary>>, #state{message = none} = State) ->
    {noreply, State#state{message = {expecting_header, Data}}};
handle_data(Data, #state{message = {Type, Channel, L, OldData}} = State) ->
    case <<OldData/binary, Data/binary>> of
        <<Payload:L/binary, ?FRAME_END, More/binary>> ->
            handle_data(More,
                        process_frame(Type, Channel, Payload,
                                            State#state{message = none}));
        NotEnough ->
            %% Read in more data from the socket
            {noreply, State#state{message = {Type, Channel, L, NotEnough}}}
    end;
handle_data(Data,
            #state{message = {expecting_header, Old}} = State) ->
    handle_data(<<Old/binary, Data/binary>>, State#state{message = none});
handle_data(<<>>, State) ->
    {noreply, State}.

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

handle_error(closed, State = #state{connection = Conn}) ->
    Conn ! socket_closed,
    {noreply, State};
handle_error({refused, Version},  State = #state{connection = Conn}) ->
    Conn ! {refused, Version},
    {noreply, State};
handle_error({malformed_header, Version},  State = #state{connection = Conn}) ->
    Conn ! {malformed_header, Version},
    {noreply, State};
handle_error(Reason, State = #state{connection = Conn}) ->
    Conn ! {socket_error, Reason},
    {stop, {socket_error, Reason}, State}.
