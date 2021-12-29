%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_main_reader).

-include("amqp_client_internal.hrl").

-behaviour(gen_server).

-export([start_link/5, post_init/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {sock,
                timer,
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

post_init(Reader) ->
    try
      gen_server:call(Reader, post_init)
    catch
      exit:{timeout, Timeout} ->
        {error, {timeout, Timeout}}
    end.

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
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% We need to use a call because we are not controlling the socket yet.
handle_call(post_init, _From, State = #state{sock = Sock}) ->
    case rabbit_net:setopts(Sock, [{active, once}]) of
        ok              -> {reply, ok, set_timeout(State)};
        {error, Reason} -> handle_error(Reason, State)
    end;
handle_call(Call, From, State) ->
    {stop, {unexpected_call, Call, From}, State}.

handle_cast(Cast, State) ->
    {stop, {unexpected_cast, Cast}, State}.

handle_info({Tag, Sock, Data}, State = #state{sock = Sock})
            when Tag =:= tcp; Tag =:= ssl ->
    %% Latency hiding: Request next packet first, then process data
    case rabbit_net:setopts(Sock, [{active, once}]) of
         ok              -> handle_data(Data, set_timeout(State));
         {error, Reason} -> handle_error(Reason, State)
    end;
handle_info({Tag, Sock}, State = #state{sock = Sock})
            when Tag =:= tcp_closed; Tag =:= ssl_closed ->
    handle_error(closed, State);
handle_info({Tag, Sock, Reason}, State = #state{sock = Sock})
            when Tag =:= tcp_error; Tag =:= ssl_error ->
    handle_error(Reason, State);
handle_info({timeout, _TimerRef, idle_timeout}, State) ->
    handle_error(timeout, State).

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

set_timeout(State0) ->
	State = cancel_timeout(State0),
	TimerRef = case amqp_util:call_timeout() of
		infinity -> undefined;
		Timeout -> erlang:start_timer(Timeout, self(), idle_timeout)
	end,
	State#state{timer=TimerRef}.

cancel_timeout(State=#state{timer=TimerRef}) ->
	ok = case TimerRef of
		undefined -> ok;
		_ -> erlang:cancel_timer(TimerRef, [{async, true}, {info, false}])
	end,
	State#state{timer=undefined}.

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
