-module(amqp10_client_session).

-behaviour(gen_fsm).

-include("amqp10_client.hrl").
-include("rabbit_amqp1_0_framing.hrl").

%% Public API.
-export(['begin'/1,
         'end'/1
        ]).

%% Private API.
-export([start_link/2,
         socket_ready/2,
         frame/2
        ]).

%% gen_fsm callbacks.
-export([init/1,
         expecting_socket/2,
         expecting_begin_frame/2,
         begun/2,
         expecting_end_frame/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define(MAX_SESSION_WINDOW_SIZE, 65535).
-define(DEFAULT_MAX_HANDLE, 16#ffffffff).

-type session_frame() :: #'v1_0.begin'{} |
                         #'v1_0.end'{} |
                         any(). %TODO: constrain type

-record(state,
        {channel :: pos_integer(),
         remote_channel :: pos_integer() | undefined,
         next_outgoing_id = 0 :: non_neg_integer(),
         reader :: pid(),
         socket :: gen_tcp:socket() | undefined
        }).

%% -------------------------------------------------------------------
%% Public API.
%% -------------------------------------------------------------------

-spec 'begin'(pid()) -> supervisor:startchild_ret().

'begin'(Connection) ->
    %% The connection process is responsible for allocating a channel
    %% number and contact the sessions supervisor to start a new session
    %% process.
    case amqp10_client_connection:new_session(Connection) of
        {ok, Session} ->
            {ok, Session};
        Error ->
            Error
    end.

-spec 'end'(pid()) -> ok.

'end'(Pid) ->
    gen_fsm:send_event(Pid, 'end').

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

start_link(Channel, Reader) ->
    gen_fsm:start_link(?MODULE, [Channel, Reader], []).

-spec socket_ready(pid(), gen_tcp:socket()) -> ok.

socket_ready(Pid, Socket) ->
    gen_fsm:send_event(Pid, {socket_ready, Socket}).

-spec frame(pid(), session_frame()) -> ok.

frame(Pid, Frame) ->
    error_logger:info_msg("Frame for session ~p: ~p~n", [Pid, Frame]),
    gen_fsm:send_event(Pid, Frame).

%% -------------------------------------------------------------------
%% gen_fsm callbacks.
%% -------------------------------------------------------------------

init([Channel, Reader]) ->
    amqp10_client_frame_reader:register_session(Reader, self(), Channel),
    State = #state{channel = Channel, reader = Reader},
    {ok, expecting_socket, State}.

expecting_socket({socket_ready, Socket}, State) ->
    State1 = State#state{socket = Socket},
    case send_begin(State1) of
        ok    -> {next_state, expecting_begin_frame, State1};
        Error -> {stop, Error, State1}
    end.

expecting_begin_frame(#'v1_0.begin'{remote_channel = {ushort, RemoteChannel}},
                      State) ->
    error_logger:info_msg("-- SESSION BEGUN (~b <-> ~b) --~n",
                          [State#state.channel, RemoteChannel]),
    State1 = State#state{remote_channel = RemoteChannel},
    {next_state, begun, State1}.

begun('end', State) ->
    %% We send the first end frame and wait for the reply.
    case send_end(State) of
        ok              -> {next_state, expecting_end_frame, State};
        {error, closed} -> {stop, normal, State};
        Error           -> {stop, Error, State}
    end;
begun(#'v1_0.end'{}, State) ->
    %% We receive the first end frame, reply and terminate.
    _ = send_end(State),
    {stop, normal, State};
begun(_Frame, State) ->
    {next_state, begun, State}.

expecting_end_frame(#'v1_0.end'{}, State) ->
    {stop, normal, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(Reason, _StateName, #state{channel = Channel,
                                     remote_channel = RemoteChannel,
                                     reader = Reader}) ->
    case Reason of
        normal -> amqp10_client_frame_reader:unregister_session(
                    Reader, self(), Channel, RemoteChannel);
        _      -> ok
    end,
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

send_begin(#state{channel = Channel,
                  socket = Socket,
                  next_outgoing_id = NextOutId}) ->
    Begin = #'v1_0.begin'{
               next_outgoing_id = {uint, NextOutId},
               incoming_window = {uint, ?MAX_SESSION_WINDOW_SIZE},
               outgoing_window = {uint, ?MAX_SESSION_WINDOW_SIZE}
              },
    Encoded = rabbit_amqp1_0_framing:encode_bin(Begin),
    Frame = rabbit_amqp1_0_binary_generator:build_frame(Channel, Encoded),
    gen_tcp:send(Socket, Frame).

send_end(#state{channel = Channel, socket = Socket}) ->
    End = #'v1_0.end'{},
    Encoded = rabbit_amqp1_0_framing:encode_bin(End),
    Frame = rabbit_amqp1_0_binary_generator:build_frame(Channel, Encoded),
    gen_tcp:send(Socket, Frame).
