%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(amqp10_client_frame_reader).

-behaviour(gen_statem).

-include("amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/2,
         set_connection/2,
         close/1,
         register_session/3,
         unregister_session/4]).

%% gen_statem callbacks
-export([init/1,
         callback_mode/0,
         handle_event/4,
         code_change/4,
         terminate/3]).

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
         socket :: amqp10_client_connection:amqp10_socket() | undefined,
         buffer = <<>> :: binary(),
         frame_state :: #frame_state{} | undefined,
         connection :: pid() | undefined,
         heartbeat_timer_ref :: reference() | undefined,
         connection_config = #{} :: amqp10_client_connection:connection_config(),
         outgoing_channels = #{},
         incoming_channels = #{}}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pid(), amqp10_client_connection:connection_config()) ->
    {ok, pid()} | ignore | {error, any()}.
start_link(Sup, Config) ->
    gen_statem:start_link(?MODULE, [Sup, Config], []).

%% @private
%% @doc
%% Passes the connection process PID to the reader process.
%%
%% This function is called when a connection supervision tree is
%% started.
-spec set_connection(Reader :: pid(), ConnectionPid :: pid()) -> ok.
set_connection(Reader, Connection) ->
    gen_statem:cast(Reader, {set_connection, Connection}).

close(Reader) ->
    gen_statem:cast(Reader, close).

register_session(Reader, Session, OutgoingChannel) ->
    gen_statem:cast(Reader, {register_session, Session, OutgoingChannel}).

unregister_session(Reader, Session, OutgoingChannel, IncomingChannel) ->
    gen_statem:cast(Reader, {unregister_session, Session, OutgoingChannel, IncomingChannel}).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() ->
    [handle_event_function].

init([Sup, ConnConfig]) when is_map(ConnConfig) ->
    Port = maps:get(port, ConnConfig, 5672),
    %% combined the list of `addresses' with the value of the original `address' option if provided
    Addresses0 = maps:get(addresses, ConnConfig, []),
    Addresses  = case maps:get(address, ConnConfig, undefined) of
                     undefined -> Addresses0;
                     Address   -> Addresses0 ++ [Address]
                 end,
    Result = lists:foldl(fun (Address,  {error, _}) ->
                                gen_tcp:connect(Address, Port, ?RABBIT_TCP_OPTS);
                             (_Address, {ok, Socket}) ->
                                 {ok, Socket}
                         end,
                         {error, undefined}, Addresses),
    case Result of
        {ok, Socket0} ->
            Socket = case ConnConfig of
                         #{tls_opts := {secure_port, Opts}} ->
                             {ok, SslSock} = ssl:connect(Socket0, Opts),
                             {ssl, SslSock};
                         _ -> {tcp, Socket0}
                     end,
            State = #state{connection_sup = Sup, socket = Socket,
                           connection_config = ConnConfig},
            {ok, expecting_connection_pid, State};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_event(cast, {set_connection, ConnectionPid}, expecting_connection_pid,
             State=#state{socket = Socket}) ->
    ok = amqp10_client_connection:socket_ready(ConnectionPid, Socket),
    set_active_once(State),
    State1 = State#state{connection = ConnectionPid},
    {next_state, expecting_frame_header, State1};
handle_event(cast, {register_session, Session, OutgoingChannel}, _StateName,
             #state{socket = Socket, outgoing_channels = OutgoingChannels} = State) ->
    ok = amqp10_client_session:socket_ready(Session, Socket),
    OutgoingChannels1 = OutgoingChannels#{OutgoingChannel => Session},
    State1 = State#state{outgoing_channels = OutgoingChannels1},
    {keep_state, State1};
handle_event(cast, {unregister_session, _Session, OutgoingChannel, IncomingChannel}, _StateName,
             State=#state{outgoing_channels = OutgoingChannels,
                          incoming_channels = IncomingChannels}) ->
    OutgoingChannels1 = maps:remove(OutgoingChannel, OutgoingChannels),
    IncomingChannels1 = maps:remove(IncomingChannel, IncomingChannels),
    State1 = State#state{outgoing_channels = OutgoingChannels1,
                         incoming_channels = IncomingChannels1},
    {keep_state, State1};
handle_event(cast, close, _StateName, State = #state{socket = Socket}) ->
    _ = close_socket(Socket),
    {stop, normal, State#state{socket = undefined}};

handle_event({call, From}, _Action, _State, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};

handle_event(info, {Tcp, _, Packet}, StateName, #state{buffer = Buffer} = State)
  when Tcp == tcp orelse Tcp == ssl ->
    Data = <<Buffer/binary, Packet/binary>>,
    case handle_input(StateName, Data, State) of
        {ok, NextState, Remaining, NewState0} ->
            NewState = defer_heartbeat_timer(NewState0),
            set_active_once(NewState),
            {next_state, NextState, NewState#state{buffer = Remaining}};
        {error, Reason, NewState} ->
            {stop, Reason, NewState}
    end;

handle_event(info, {TcpError, _, Reason}, StateName, State)
  when TcpError == tcp_error orelse TcpError == ssl_error ->
    logger:warning("AMQP 1.0 connection socket errored, connection state: '~s', reason: '~p'",
                    [StateName, Reason]),
    State1 = State#state{socket = undefined,
                         buffer = <<>>,
                         frame_state = undefined},
    {stop, {error, Reason}, State1};
handle_event(info, {TcpClosed, _}, StateName, State)
  when TcpClosed == tcp_closed orelse TcpClosed == ssl_closed ->
    logger:warning("AMQP 1.0 connection socket was closed, connection state: '~s'",
                    [StateName]),
    State1 = State#state{socket = undefined,
                         buffer = <<>>,
                         frame_state = undefined},
    {stop, normal, State1};

handle_event(info, heartbeat, _StateName, #state{connection = Connection}) ->
    amqp10_client_connection:close(Connection,
                                   {resource_limit_exceeded, <<"remote idle-time-out">>}),
    % do not stop as may want to read the peer's close frame
    keep_state_and_data.

terminate(normal, _StateName, #state{connection_sup = _Sup, socket = Socket}) ->
    maybe_close_socket(Socket);
terminate(_Reason, _StateName, #state{connection_sup = _Sup, socket = Socket}) ->
    maybe_close_socket(Socket).

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_close_socket(undefined) ->
    ok;
maybe_close_socket(Socket) ->
    close_socket(Socket).

close_socket({tcp, Socket}) ->
    gen_tcp:close(Socket);
close_socket({ssl, Socket}) ->
    ssl:close(Socket).

set_active_once(#state{socket = {tcp, Socket}}) ->
    ok = inet:setopts(Socket, [{active, once}]);
set_active_once(#state{socket = {ssl, Socket}}) ->
    ok = ssl:setopts(Socket, [{active, once}]).

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
    case {Data, BodyLength} of
        {<<_:BodyLength/binary, Rest/binary>>, 0} ->
            % heartbeat
            handle_input(expecting_frame_header, Rest, State);
        {<<FrameBody:BodyLength/binary, Rest/binary>>, _} ->
            State1 = State#state{frame_state = undefined},
            {PerfDesc, Payload} = amqp10_binary_parser:parse(FrameBody),
            Perf = amqp10_framing:decode(PerfDesc),
            State2 = route_frame(Channel, FrameType, {Perf, Payload}, State1),
            handle_input(expecting_frame_header, Rest, State2);
        _ ->
            {ok, expecting_frame_body, Data, State}
    end;

handle_input(StateName, Data, State) ->
    {ok, StateName, Data, State}.

%%% LOCAL

defer_heartbeat_timer(State =
                      #state{heartbeat_timer_ref = TRef,
                             connection_config = #{idle_time_out := T}})
  when is_number(T) andalso T > 0 ->
    _ = case TRef of
            undefined -> ok;
            _ -> _ = erlang:cancel_timer(TRef)
        end,
    NewTRef = erlang:send_after(T * 2, self(), heartbeat),
    State#state{heartbeat_timer_ref = NewTRef};
defer_heartbeat_timer(State) -> State.

route_frame(Channel, FrameType, {Performative, Payload} = Frame, State0) ->
    {DestinationPid, State} = find_destination(Channel, FrameType, Performative,
                                               State0),
    ?DBG("FRAME -> ~p ~p~n ~p", [Channel, DestinationPid, Performative]),
    case Payload of
        <<>> -> ok = gen_statem:cast(DestinationPid, Performative);
        _ -> ok = gen_statem:cast(DestinationPid, Frame)
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
