%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.

-module(amqp_connection).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-behaviour(gen_server).

-record(state, {conn_state, drv_module, type}).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).
-export([open_channel/1, open_channel/3]).
-export([start/2, start/3, start/4, close/2]).
-export([start_link/2, start_link/3, start_link/4]).

%---------------------------------------------------------------------------
% AMQP Connection API Methods
%---------------------------------------------------------------------------

%% Starts a direct connection to the Rabbit AMQP server, assuming that
%% the server is running in the same process space.
start(User,Password) -> start(User,Password,false).
start(User,Password,ProcLink) when is_boolean(ProcLink) ->
    InitialState = #connection_state{username = User,
                                     password = Password,
                                     vhostpath = <<"/">>},
    {ok, Pid} = start_internal(InitialState, direct, ProcLink),
    {Pid, direct};

%% Starts a networked conection to a remote AMQP server.
start(User,Password,Host) -> start(User,Password,Host,<<"/">>,false).
start(User,Password,Host,VHost) -> start(User,Password,Host,VHost,false).
start(User,Password,Host,VHost,ProcLink) ->
    InitialState = #connection_state{username = User,
                                     password = Password,
                                     serverhost = Host,
                                     vhostpath = VHost},
    {ok, Pid} = start_internal(InitialState, network, ProcLink),
    {Pid, network}.

start_link(User,Password) -> start(User,Password,true).
start_link(User,Password,Host) -> start(User,Password,Host,<<"/">>,true).
start_link(User,Password,Host,VHost) -> start(User,Password,Host,VHost,true).

l2a(L) ->
    case catch list_to_existing_atom(L) of
        A when is_atom(A) ->
            A;
        _ ->
            list_to_atom(L)
    end.

build_driver_spec(DriverType) when is_list(DriverType) ->
    l2a("amqp_" ++ DriverType ++ "_driver").

start_internal(InitialState, DriverType, ProcLink) when is_atom(DriverType) ->
    DriverSpec = build_driver_spec(atom_to_list(DriverType)),
    case ProcLink of
        true ->                                 
            gen_server:start_link(?MODULE, [InitialState, DriverSpec, DriverType], []);
        false ->
            gen_server:start(?MODULE, [InitialState, DriverSpec, DriverType], [])
    end.

%% Opens a channel without having to specify a channel number.
%% This function assumes that an AMQP connection (networked or direct)
%% has already been successfully established.
open_channel( {Pid, Mode} ) -> open_channel( {Pid, Mode}, none, "").

%% Opens a channel with a specific channel number.
%% This function assumes that an AMQP connection (networked or direct)
%% has already been successfully established.
open_channel( {ConnectionPid, Mode}, ChannelNumber, OutOfBand) ->
    gen_server:call(ConnectionPid, {Mode, ChannelNumber, amqp_util:binary(OutOfBand)}).

%% Closes the AMQP connection
close( {ConnectionPid, Mode}, Close) -> gen_server:call(ConnectionPid, {Mode, Close} ).

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

%% Starts a new channel process, invokes the correct driver (network or direct)
%% to perform any environment specific channel setup and starts the
%% AMQP ChannelOpen handshake.
handle_start({ChannelNumber, OutOfBand},OpenFun,CloseFun,Do2,Do3,#state{conn_state = CS} = State) ->
    {ChannelPid, Number, CS0} = start_channel(ChannelNumber,CloseFun,Do2,Do3,CS),
    OpenFun({Number, OutOfBand}, ChannelPid, CS0),
    #'channel.open_ok'{} = amqp_channel:call(ChannelPid, #'channel.open'{out_of_band = OutOfBand}),
    {reply, ChannelPid, State#state{conn_state = CS0}}.

%% Creates a new channel process
start_channel(ChannelNumber,CloseFun,Do2,Do3, CS) ->
    ReaderPid = CS#connection_state.reader_pid,                                                             
    WriterPid = CS#connection_state.channel0_writer_pid,                                                             
    Number = assign_channel_number(ChannelNumber, CS),
    InitialState = #channel_state{parent_connection = self(),
                                  number = Number,
                                  close_fun = CloseFun,
                                  do2 = Do2, do3 = Do3,
                                  reader_pid = ReaderPid,
                                  writer_pid = WriterPid},
    process_flag(trap_exit, true),
    {ok, ChannelPid} = gen_server:start_link(amqp_channel, [InitialState], []),
    NewCS = register_channel(Number, ChannelPid, CS),
    {ChannelPid, Number, NewCS}.

assign_channel_number(none, CS) ->
    Channels = CS#connection_state.channels,
    Max = CS#connection_state.channel_max,
    allocate_channel_number(dict:fetch_keys(Channels), Max);
assign_channel_number(ChannelNumber, _State) ->
    %% TODO bug: check whether this is already taken
    ChannelNumber.

register_channel(ChannelNumber, ChannelPid, CS) ->
    Channels0 = CS#connection_state.channels,
    Channels1 =
    case dict:is_key(ChannelNumber, Channels0) of
        true ->
            exit({channel_already_registered, ChannelNumber});
        false ->
            dict:store(ChannelNumber, ChannelPid, Channels0)
    end,
    CS#connection_state{channels = Channels1}.

%% This will be called when a channel process exits and needs to be deregistered
%% This peforms the reverse mapping so that you can lookup a channel pid
%% Let's hope that this lookup doesn't get too expensive .......
unregister_channel(ChannelPid, DrvType, CS) when is_pid(ChannelPid)->
    Channels0 = CS#connection_state.channels,
    ReverseMapping = fun(_Number, Pid) -> Pid == ChannelPid end,
    Projection = dict:filter(ReverseMapping, Channels0),
    Channels1 = unregister_direct(Projection, Channels0, DrvType),
    CS#connection_state{channels = Channels1};

%% This will be called when a channel process exits and needs to be 
%% deregistered
unregister_channel(ChannelNumber, _DrvType, CS) ->
    Channels0 = CS#connection_state.channels,
    Channels1 = dict:erase(ChannelNumber, Channels0),
    CS#connection_state{channels = Channels1}.

%% TODO This differentiation is only necessary for the direct channel,
%% look into refactoring
unregister_direct(Projection, Channels0, direct) ->
    case dict:fetch_keys(Projection) of
        [] ->
            Channels0;
        [ChannelNumber|_] ->
            dict:erase(ChannelNumber, Channels0)
    end;
unregister_direct(_Projection, Channels0, _Type) ->
    Channels0.

allocate_channel_number([], _Max)-> 1;

allocate_channel_number(Channels, _Max) ->
    MaxChannel = lists:max(Channels),
    %% TODO check channel max and reallocate appropriately
    MaxChannel + 1.

close_connection(Close, From, #state{drv_module = Mod, conn_state = CS}) ->
    Mod:close_connection(Close, From, CS).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([InitialState, DrvMod, DrvType]) 
        when is_atom(DrvMod), is_atom(DrvType) ->
    CS = DrvMod:handshake(InitialState), % Connection state
    {ok, #state{conn_state = CS, drv_module = DrvMod, type = DrvType}}.

%% Starts a new channel
handle_call({_Whatever, ChannelNumber, OutOfBand}, _From, 
            #state{drv_module = Module} = State) ->
    handle_start(
        {ChannelNumber, OutOfBand},
        fun(X, Y, Z) -> Module:open_channel(X, Y, Z) end,
        fun(X)       -> Module:close_channel(X) end,
        fun(X, Y)    -> Module:do(X, Y) end,
        fun(X, Y, Z) -> Module:do(X, Y, Z) end,
        State
    );

%% Shuts the AMQP connection down
handle_call({_Mode, Close = #'connection.close'{}}, From, #state{} = State) ->
    close_connection(Close, From, State),
    {stop,normal,State}.

handle_cast(_Message, State) ->
    {noreply, State}.

%---------------------------------------------------------------------------
% Handle forced close from the broker
%---------------------------------------------------------------------------

handle_info({method, #'connection.close'{reply_code = Code,
                                         reply_text = Text},
                                 _Content}, 
            State = #connection_state{on_close_handler = OnCloseHandler}) ->
    io:format("Broker forced connection: ~p -> ~p~n", [Code, Text]),
    OnCloseHandler(State),
    {stop, normal, State};

%---------------------------------------------------------------------------
% Trap exits
%---------------------------------------------------------------------------

handle_info( {'EXIT', Pid, {amqp,Reason,Msg,Context}}, #state{} = State) ->
    io:format("Channel Peer ~p sent this message: ~p -> ~p~n",[Pid,Msg,Context]),
    {HardError, Code, Text} = rabbit_framing:lookup_amqp_exception(Reason),
    case HardError of
        false ->
            io:format("Just trapping this exit and proceding to trap an exit from the client channel process~n"),
            {noreply, State};
        true ->
            io:format("Hard error: (Code = ~p, Text = ~p)~n", [Code, Text]),
            {stop, {hard_error, {Code, Text}}, State}
    end;            

%% Just the amqp channel shutting down, so unregister this channel
handle_info( {'EXIT', Pid, normal}, #state{conn_state = CS, 
                                           type = Type} = State) ->
    NewCS = unregister_channel(Pid, Type, CS),
    {noreply, State#state{conn_state = NewCS}};
    
% This is a special case for abruptly closed socket connections
handle_info( {'EXIT', _Pid, {socket_error, Reason}}, State) ->
    {stop, {socket_error, Reason}, State};

handle_info( {'EXIT', Pid, Reason}, #state{conn_state = CS,
                                           type = Type} = State) ->
    io:format("Connection: Handling exit from ~p --> ~p~n",[Pid,Reason]),
    NewCS = unregister_channel(Pid, Type, CS),
    {noreply, State#state{conn_state = NewCS}}.

%---------------------------------------------------------------------------
% Rest of the gen_server callbacks
%---------------------------------------------------------------------------

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, #state{} = State, _Extra) ->
    State.

