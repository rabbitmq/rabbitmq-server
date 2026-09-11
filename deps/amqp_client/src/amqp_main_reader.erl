%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% @private
-module(amqp_main_reader).

-include("amqp_client_internal.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

-export([start_link/5, post_init/1, set_frame_max/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {sock,
                timer,
                connection,
                channels_manager,
                astate,
                frame_max = ?HANDSHAKE_FRAME_MAX,
                message = none %% none | {expecting_header, Buf} | {Type, Channel, Length, Buf}
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

%% Synchronous: the caller sends connection.tune_ok right after, and the
%% new limit must be in effect before the peer can send a post-negotiation
%% frame. A dead reader means the connection is already failing, so the
%% error is left for the caller's own socket error handling.
set_frame_max(Reader, FrameMax) ->
    try
        gen_server:call(Reader, {set_frame_max, FrameMax},
                        amqp_util:call_timeout())
    catch
        exit:{Reason, _} -> {error, Reason}
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
%% A frame header buffered under the handshake ceiling was never checked
%% against the negotiated limit, so it is checked here.
handle_call({set_frame_max, FrameMax}, _From,
            State0 = #state{message = {_Type, _Channel, Length, _Buf}})
  when Length > FrameMax ->
    State = State0#state{frame_max = FrameMax},
    {stop, Reason, State1} = handle_error({frame_too_large, Length, FrameMax},
                                          State),
    {stop, Reason, ok, State1};
handle_call({set_frame_max, FrameMax}, _From, State) ->
    {reply, ok, State#state{frame_max = FrameMax}};
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

%% Length is the payload size, frame_max the size of the whole frame. The
%% payload is compared to frame_max rather than to frame_max minus
%% ?EMPTY_FRAME_SIZE so that the client tolerates the same 8-byte overshoot
%% the server does (see ?FRAME_SIZE_FUDGE in rabbit_reader).
handle_data(<<Type:8, _Channel:16, Length:32, _/binary>>,
            #state{message = none, frame_max = FrameMax} = State)
  when (Type =:= ?FRAME_METHOD orelse Type =:= ?FRAME_HEADER orelse
        Type =:= ?FRAME_BODY orelse Type =:= ?FRAME_HEARTBEAT) andalso
       Length > FrameMax ->
    handle_error({frame_too_large, Length, FrameMax}, State);
handle_data(<<Type:8, Channel:16, Length:32, Payload:Length/binary, ?FRAME_END,
              More/binary>>,
            #state{message = none} = State) when
      Type =:= ?FRAME_METHOD; Type =:= ?FRAME_HEADER;
      Type =:= ?FRAME_BODY;   Type =:= ?FRAME_HEARTBEAT ->
    %% Optimisation for the direct match
    handle_data(
      More, process_frame(Type, Channel, Payload, State#state{message = none}));
handle_data(<<Type:8, _Channel:16, Length:32, _:Length/binary, EndMarker,
              _/binary>>,
            #state{message = none} = State) when
      Type =:= ?FRAME_METHOD; Type =:= ?FRAME_HEADER;
      Type =:= ?FRAME_BODY;   Type =:= ?FRAME_HEARTBEAT ->
    handle_error({invalid_frame_end_marker, EndMarker}, State);
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
        <<_:L/binary, EndMarker, _/binary>> ->
            handle_error({invalid_frame_end_marker, EndMarker}, State);
        NotEnough ->
            %% Read in more data from the socket
            {noreply, State#state{message = {Type, Channel, L, NotEnough}}}
    end;
handle_data(Data,
            #state{message = {expecting_header, Old}} = State) ->
    handle_data(<<Old/binary, Data/binary>>, State#state{message = none}).

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
    case rabbit_command_assembler:analyze_frame(Type, Payload) of
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
handle_error({frame_too_large, Length, FrameMax} = Reason,
             State = #state{connection = Conn}) ->
    ?LOG_WARNING("AMQP 0-9-1 client connection ~tp: peer sent a frame with a "
                 "payload of ~tp bytes, exceeding the frame_max limit of "
                 "~tp bytes; closing the connection",
                 [Conn, Length, FrameMax]),
    handle_socket_error(Reason, State);
handle_error({invalid_frame_end_marker, EndMarker} = Reason,
             State = #state{connection = Conn}) ->
    ?LOG_WARNING("AMQP 0-9-1 client connection ~tp: peer sent an invalid "
                 "frame end marker ~tp; closing the connection",
                 [Conn, EndMarker]),
    handle_socket_error(Reason, State);
handle_error(Reason, State) ->
    handle_socket_error(Reason, State).

handle_socket_error(Reason, State = #state{connection = Conn}) ->
    Conn ! {socket_error, Reason},
    %% The connection stops as a consequence; a second, abnormal exit
    %% reason here would only add redundant crash and supervisor reports.
    {stop, {shutdown, {socket_error, Reason}}, State}.
