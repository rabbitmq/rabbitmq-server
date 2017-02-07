-module(amqp10_client_frame_reader).

-behaviour(gen_fsm).

-include("amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Private API.
-export([start_link/2,
         set_connection/2,
         connection_closing/1,
         register_session/3,
         unregister_session/4]).

%% gen_fsm callbacks.
-export([init/1,
         expecting_connection_pid/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define(RABBIT_TCP_OPTS, [binary,
                          {packet, 0},
                          {active, false},
                          {nodelay, true}]).

-type frame_type() ::  amqp | sasl.

-record(frame_state,
        {data_offset :: 2..255,
         type :: frame_type(),
         channel :: non_neg_integer(),
         frame_length :: pos_integer()}).

-record(state,
        {connection_sup :: pid(),
         socket :: gen_tcp:socket() | undefined,
         buffer = <<>> :: binary(),
         frame_state :: #frame_state{} | undefined,
         connection :: pid() | undefined,
         connection_config = #{} :: amqp10_client_connection:connection_config(),
         outgoing_channels = #{},
         incoming_channels = #{}}).

%% -------------------------------------------------------------------
%% Private API.
%% -------------------------------------------------------------------

%% @private

-spec start_link(pid(), amqp10_client_connection:connection_config()) ->
    {ok, pid()} | ignore | {error, any()}.

start_link(Sup, Config) ->
    gen_fsm:start_link(?MODULE, [Sup, Config], []).

%% @private
%% @doc
%% Passes the connection process PID to the reader process.
%%
%% This function is called when a connection supervision tree is
%% started.

-spec set_connection(pid(), pid()) -> ok.

set_connection(Reader, Connection) ->
    gen_fsm:send_event(Reader, {set_connection, Connection}).

connection_closing(Reader) ->
    gen_fsm:send_all_state_event(Reader, connection_closing).

register_session(Reader, Session, OutgoingChannel) ->
    gen_fsm:send_all_state_event(
      Reader, {register_session, Session, OutgoingChannel}).

unregister_session(Reader, Session, OutgoingChannel, IncomingChannel) ->
    gen_fsm:send_all_state_event(
      Reader, {unregister_session, Session, OutgoingChannel, IncomingChannel}).

%% -------------------------------------------------------------------
%% gen_fsm callbacks.
%% -------------------------------------------------------------------

init([Sup, #{address := Host, port := Port} = ConnConfig]) ->
    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Socket} ->
            State = #state{connection_sup = Sup, socket = Socket,
                           connection_config = ConnConfig},
            {ok, expecting_connection_pid, State};
        {error, Reason} ->
            {stop, Reason}
    end.

expecting_connection_pid({set_connection, ConnectionPid},
                         #state{socket = Socket} = State) ->
    ok = amqp10_client_connection:socket_ready(ConnectionPid, Socket),
    set_active_once(State),
    State1 = State#state{connection = ConnectionPid},
    {next_state, expecting_frame_header, State1}.

handle_event({register_session, Session, OutgoingChannel},
             StateName,
             #state{socket = Socket,
                    outgoing_channels = OutgoingChannels} = State) ->
    ok = amqp10_client_session:socket_ready(Session, Socket),
    OutgoingChannels1 = OutgoingChannels#{OutgoingChannel => Session},
    State1 = State#state{outgoing_channels = OutgoingChannels1},
    {next_state, StateName, State1};
handle_event({unregister_session, _Session, OutgoingChannel, IncomingChannel},
             StateName,
             #state{outgoing_channels = OutgoingChannels,
                    incoming_channels = IncomingChannels} = State) ->
    OutgoingChannels1 = maps:remove(OutgoingChannel, OutgoingChannels),
    IncomingChannels1 = maps:remove(IncomingChannel, IncomingChannels),
    State1 = State#state{outgoing_channels = OutgoingChannels1,
                         incoming_channels = IncomingChannels1},
    {next_state, StateName, State1};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info({tcp, _, Packet}, StateName, #state{buffer = Buffer} = State) ->
    Data = <<Buffer/binary, Packet/binary>>,
    case handle_input(StateName, Data, State) of
        {ok, NextState, Remaining, NewState} ->
            set_active_once(NewState),
            {next_state, NextState, NewState#state{buffer = Remaining}};
        {error, Reason, NewState} ->
            {stop, Reason, NewState}
    end;

handle_info({tcp_closed, _}, StateName, State) ->
    error_logger:warning_msg("Socket closed while in state '~s'~n",
                             [StateName]),
    State1 = State#state{
               socket = undefined,
               buffer = <<>>,
               frame_state = undefined},
    {stop, normal, State1}.

terminate(Reason, _StateName, #state{connection_sup = Sup, socket = Socket}) ->
    case Socket of
        undefined -> ok;
        _         -> gen_tcp:close(Socket)
    end,
    case Reason of
        normal -> sys:terminate(Sup, normal);
        _      -> ok
    end,
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

set_active_once(#state{socket = Socket}) ->
    %TODO: handle return value
    ok = inet:setopts(Socket, [{active, once}]).

handle_input(expecting_frame_header,
             <<"AMQP", Protocol/unsigned, Maj/unsigned, Min/unsigned,
               Rev/unsigned, Rest/binary>>,
             #state{connection = ConnectionPid} = State)
  when Protocol =:= 0 orelse Protocol =:= 3 ->
    ok = amqp10_client_connection:protocol_header_received(
           ConnectionPid, Protocol, Maj, Min, Rev),
    handle_input(expecting_frame_header, Rest, State);
handle_input(expecting_frame_header,
             <<Length:32/unsigned, DOff:8/unsigned, Type/unsigned,
               Channel:16/unsigned, Rest/binary>>, State)
  when DOff >= 2 andalso (Type =:= 0 orelse Type =:= 1) ->
    AFS = #frame_state{frame_length = Length, channel = Channel,
                       type = frame_type(Type), data_offset = DOff},
    handle_input(expecting_extended_frame_header, Rest,
                 State#state{frame_state = AFS});
handle_input(expecting_frame_header, <<_:8/binary, _/binary>>, State) ->
    {error, invalid_protocol_header, State};


handle_input(expecting_extended_frame_header, Data,
             #state{frame_state =
                    #frame_state{data_offset = DOff}} = State) ->
    Skip = DOff * 4 - 8,
    case Data of
        <<_:Skip/binary, Rest/binary>> ->
            handle_input(expecting_frame_body, Rest, State);
        _ ->
            {ok, expecting_extended_frame_header, Data, State}
    end;

handle_input(expecting_frame_body, Data,
             #state{frame_state = #frame_state{frame_length = Length,
                                               type = FrameType,
                                               data_offset = DOff,
                                               channel = Channel}} = State) ->
    Skip = DOff * 4 - 8,
    BodyLength = Length - Skip - 8,
    case Data of
        <<FrameBody:BodyLength/binary, Rest/binary>> ->
            State1 = State#state{frame_state = undefined},
            {PerfDesc, Payload} = rabbit_amqp1_0_binary_parser:parse(FrameBody),
            Perf = rabbit_amqp1_0_framing:decode(PerfDesc),
            error_logger:info_msg("PERF ~p~n", [Perf]),
            State2 = route_frame(Channel, FrameType, {Perf, Payload}, State1),
            handle_input(expecting_frame_header, Rest, State2);
        _ ->
            {ok, expecting_frame_body, Data, State}
    end;

handle_input(StateName, Data, State) ->
    {ok, StateName, Data, State}.

route_frame(Channel, FrameType, {Performative, Payload} = Frame, State0) ->
    {DestinationPid, State} = find_destination(Channel, FrameType, Performative,
                                               State0),
    % error_logger:info_msg("ROUTING FRAME ~p -> (~p, ~p)",
    %                       [Frame, Channel, DestinationPid]),
    case Payload of
        <<>> -> ok = gen_fsm:send_event(DestinationPid, Performative);
        _ -> ok = gen_fsm:send_event(DestinationPid, Frame)
    end,
    State.

-spec find_destination(amqp10_client_types:channel(), frame_type(),
                       amqp10_client_types:amqp10_performative(), #state{}) ->
    {pid(), #state{}}.
find_destination(0, amqp, Frame, #state{connection = ConnPid} = State)
    when is_record(Frame, 'v1_0.open') orelse
         is_record(Frame, 'v1_0.close') ->
    {ConnPid, State};
find_destination(_Channel, sasl, _Frame,
                 #state{connection = ConnPid} = State) ->
    {ConnPid, State};
find_destination(Channel, amqp,
                 #'v1_0.begin'{remote_channel = {ushort, OutgoingChannel}},
                 #state{outgoing_channels = OutgoingChannels,
                        incoming_channels = IncomingChannels} = State) ->
    #{OutgoingChannel := Session} = OutgoingChannels,
    IncomingChannels1 = IncomingChannels#{Channel => Session},
    State1 = State#state{incoming_channels = IncomingChannels1},
    {Session, State1};
find_destination(Channel, amqp, _Frame,
                #state{incoming_channels = IncomingChannels} = State) ->
    #{Channel := Session} = IncomingChannels,
    {Session, State}.

frame_type(0) -> amqp;
frame_type(1) -> sasl.

-ifdef(TEST).

find_destination_test_() ->
    Pid = self(),
    State = #state{connection = Pid, outgoing_channels = #{3 => Pid}},
    StateConn = #state{connection = Pid},
    StateWithIncoming = State#state{incoming_channels = #{7 => Pid}},
    StateWithIncoming0 = State#state{incoming_channels = #{0 => Pid}},
    Tests = [{0, #'v1_0.open'{}, State, State, amqp},
             {0, #'v1_0.close'{}, State, State, amqp},
             {7, #'v1_0.begin'{remote_channel = {ushort, 3}}, State,
              StateWithIncoming, amqp},
             {0, #'v1_0.begin'{remote_channel = {ushort, 3}}, State,
              StateWithIncoming0, amqp},
             {7, #'v1_0.end'{}, StateWithIncoming, StateWithIncoming, amqp},
             {7, #'v1_0.attach'{}, StateWithIncoming, StateWithIncoming, amqp},
             {7, #'v1_0.flow'{}, StateWithIncoming, StateWithIncoming, amqp},
             {0, #'v1_0.sasl_init'{}, StateConn, StateConn, sasl}
            ],
    [?_assertMatch({Pid, NewState},
                   find_destination(Channel, Type, Frame, InputState))
     || {Channel, Frame, InputState, NewState, Type} <- Tests].

-endif.
