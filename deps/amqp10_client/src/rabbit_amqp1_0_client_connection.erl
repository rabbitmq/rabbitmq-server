-module(rabbit_amqp1_0_client_connection).

-behaviour(gen_fsm).

-include("include/rabbit_amqp1_0_client.hrl").
-include("include/rabbit_amqp1_0_framing.hrl").

%% API functions
-export([start_link/3,
         socket/2,
         protocol_header/2,
         frame/2
        ]).

% states
-export([
         pre/2,
         start/2,
         hdr_sent/2,
         open_sent/2,
         opened/2
        ]).
%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-type connection_frame() :: #'v1_0.open'{} | any(). %TODO: constrain type

-record(state, {reader_pid :: pid() | undefined,
                socket :: gen_tcp:socket() | undefined}).

%%%===================================================================
%%% API functions
%%%===================================================================

socket(Pid, Sock) ->
    io:format("socket"),
    gen_fsm:send_event(Pid, {socket, Sock}).

protocol_header(Pid, Header) ->
    io:format("protocol_header"),
    gen_fsm:send_event(Pid, {protocol_header, Header}).

-spec frame(pid(), connection_frame()) -> ok.
frame(Pid, #'v1_0.open'{} = Open) ->
    gen_fsm:send_event(Pid, {open, Open});
frame(_Pid, Frame) ->
    io:format("Uknown framw ~p~n", [Frame]).

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(SupPid, Addr, Port) ->
    gen_fsm:start_link(?MODULE, [SupPid, Addr, Port], []).

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
init([SupPid, Addr, Port]) ->
    gen_fsm:send_event(self(), {start_reader, {SupPid, Addr, Port}}),
    {ok, pre, #state{}}.

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
pre({start_reader, {SupPid, Addr, Port}}, State) ->
    {ok, Reader} =
        rabbit_amqp1_0_client_connection_sup:start_reader(SupPid, self(),
                                                           {Addr, Port}),
    {next_state, start, State#state{reader_pid = Reader}}.

start({socket, Sock}, State) ->
    io:format("Start"),
    ok = gen_tcp:send(Sock, ?PROTOCOL_HEADER),
    {next_state, hdr_sent, State#state{socket = Sock}}.

hdr_sent({protocol_header, _Hdr}, #state{socket = Sock} =  State) ->
    io:format("hdr_sent"),
    ok = send_open(Sock),
    {next_state, open_sent, State#state{socket = Sock}}.

open_sent({open, Open}, State) ->
    io:format("Open ~p", [Open]),
    {next_state, opened, State}.

opened(Frame, State) ->
    io:format("Frame received ~p", [Frame]),
    {next_state, opened, State}.

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
% state_name(_Event, _From, State) ->
%     Reply = ok,
%     {reply, Reply, state_name, State}.

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
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

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

-spec send_open(gen_tcp:socket()) -> ok | {error, closed | inet:posix()}.
send_open(Sock) ->
    Open = #'v1_0.open'{container_id = {utf8, <<"test">>},
                        hostname = {utf8, <<"localhost">>},
                        max_frame_size = {uint, ?MAX_FRAME_SIZE},
                        channel_max = {ushort, 100},
                        idle_time_out = {uint, 0}
                         % outgoing_locales = [],
                         % incoming_locales = [],
                         % offered_capabilities = [],
                         % desired_capabilities = [],
                         % properties
                        },
    Encoded = rabbit_amqp1_0_framing:encode_bin(Open),
    % remove length prefix - it's an IOList!
    F = rabbit_amqp1_0_binary_generator:build_frame(0, Encoded),
    gen_tcp:send(Sock, F).
