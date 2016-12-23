-module(rabbit_amqp1_0_client_frame_reader).

-behaviour(gen_fsm).

-include("include/rabbit_amqp1_0_client.hrl").

%% API functions
-export([start_link/2]).

%% gen_fsm callbacks
-export([init/1,
         state_name/2,
         state_name/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define(RABBIT_TCP_OPTS, [binary, {packet, 0}, {active, once}, {nodelay, true}]).

-record(amqp_frame_state, {data_offset :: 2..255,
                           channel :: non_neg_integer(),
                           frame_length :: pos_integer()}).


-record(state, {sock :: gen_tcp:socket(),
                conn_pid :: pid(),
                amqp_frame_state :: #amqp_frame_state{} | undefined,
                buffer = <<>> :: binary()}).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
-spec start_link(pid(), {string(), pos_integer()}) ->
    {ok, pid()} | ignore | {error, any()}.
start_link(ConnPid, Endpoint) ->
    gen_fsm:start_link(?MODULE, [ConnPid, Endpoint], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([ConnPid, {Addr, Port}]) ->
    {ok, Sock} = gen_tcp:connect(Addr, Port, ?RABBIT_TCP_OPTS),
    % pass the socket to the connection
    rabbit_amqp1_0_client_connection:socket(ConnPid, Sock),
    {ok, expecting_protocol_header, #state{sock = Sock, conn_pid = ConnPid}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
state_name(expecting_protocol_header, State) ->
    {next_state, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
state_name(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(get_socket, _From, StateName, #state{sock = Sock} = State) ->
    {reply, Sock, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info({tcp, _Sock, Data}, StateName, #state{buffer = Buffer} = State) ->
    io:format("Data ~p~nBuffer~p~n", [Data, Buffer]),
    Buf = <<Buffer/binary, Data/binary>>,
    case handle_input(StateName, Buf, State) of
        {ok, NextState, Remaining, NewState} ->
            set_active_once(NewState),
            {next_state, NextState, NewState#state{buffer = Remaining}};
        {error, Reason, NewState} ->
            {stop, Reason, NewState}
    end.


% handle_info({tcp, _Sock, Data}, expecting_amqp_frame_header,
%             #state{buffer = Buffer} = State) ->
%     case <<Buffer/binary, Data/binary>> of
%         <<Length:32/unsigned, DOff:8/unsigned, 0,
%           Channel:16/unsigned, Rest/binary>> ->
%             set_active_once(State),
%             Frame = rabbit_amqp1_0_framing:decode_bin(FrameBody),
%             io:format("F ~p~n", [Frame]),

%             {next_state, expecting_amqp_frame, State#state{buffer = R}};
%         <<_:8/binary, _>> ->
%             {stop, invalid_protocol_header, State};
%         B ->
%             set_active_once(State),
%             {next_state, expecting_protocol_header, State#state{buffer = B}}
%     end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

set_active_once(#state{sock = Sock}) ->
    %TODO: handle return value
    ok = inet:setopts(Sock, [{active, once}]).

handle_input(expecting_protocol_header, <<"AMQP", 0, 1, 0, 0, R/binary>>,
             #state{conn_pid = Pid} = State) ->
    ok = rabbit_amqp1_0_client_connection:protocol_header(Pid, ?PROTOCOL_HEADER),
    handle_input(expecting_amqp_frame_header, R, State);
handle_input(expecting_protocol_header, <<_:8/binary, _>>, State) ->
    {error, invalid_protocol_header, State};
handle_input(expecting_amqp_frame_header, <<Length:32/unsigned, DOff:8/unsigned, 0,
                                            Channel:16/unsigned, Rest/binary>>,
            State) when DOff >= 2 ->
    AFS = #amqp_frame_state{frame_length = Length,
                            channel = Channel,
                            data_offset = DOff},
    handle_input(expecting_amqp_extended_header, Rest,
                 State#state{amqp_frame_state = AFS});
handle_input(expecting_amqp_extended_header, Data,
             #state{amqp_frame_state = #amqp_frame_state{data_offset = DOff}} = State) ->
    Skip = DOff * 4 - 8,
    case Data of
       <<_:Skip/binary, Rest/binary>> ->
            handle_input(expecting_amqp_frame_body, Rest, State);
        _ -> {ok, expecting_amqp_extended_header, Data, State}
    end;
handle_input(expecting_amqp_frame_body, Data,
             #state{amqp_frame_state = #amqp_frame_state{frame_length = Length,
                                                         data_offset = DOff,
                                                         channel = Channel}} = State) ->
    Skip = DOff * 4 - 8,
    BodyLength = Length - Skip - 8,
    case Data of
       <<FrameBody:BodyLength/binary, Rest/binary>> ->
            Frames = rabbit_amqp1_0_framing:decode_bin(FrameBody),
            [route_frame(Channel, Frame, State) || Frame <- Frames],
            handle_input(expecting_amqp_frame_body, Rest, State);
        _ -> {ok, expecting_amqp_extended_header, Data, State}
    end;
handle_input(StateName, Data, State) ->
    {ok, StateName, Data, State}.

route_frame(0, Frame, #state{conn_pid = ConnPid}) ->
    ok = rabbit_amqp1_0_client_connection:frame(ConnPid, Frame).
