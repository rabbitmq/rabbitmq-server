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
%%

-module(amqp_connection).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-behaviour(gen_server).

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
    Handshake = fun amqp_direct_driver:handshake/1,
    InitialState = #connection_state{username = User,
                                     password = Password,
                                     vhostpath = <<"/">>},
    {ok, Pid} = start_internal(InitialState, Handshake,ProcLink),
    {Pid, direct};

%% Starts a networked conection to a remote AMQP server.
start(User,Password,Host) -> start(User,Password,Host,<<"/">>,false).
start(User,Password,Host,VHost) -> start(User,Password,Host,VHost,false).
start(User,Password,Host,VHost,ProcLink) ->
    Handshake = fun amqp_network_driver:handshake/1,
    InitialState = #connection_state{username = User,
                                     password = Password,
                                     serverhost = Host,
                                     vhostpath = VHost},
    {ok, Pid} = start_internal(InitialState, Handshake,ProcLink),
    {Pid, network}.
    
start_link(User,Password) -> start(User,Password,true).
start_link(User,Password,Host) -> start(User,Password,Host,<<"/">>,true).
start_link(User,Password,Host,VHost) -> start(User,Password,Host,VHost,true).

start_internal(InitialState, Handshake,ProcLink) ->
    case ProcLink of
        true ->                                 
            gen_server:start_link(?MODULE, [InitialState, Handshake], []);
        false ->
            gen_server:start(?MODULE, [InitialState, Handshake], [])
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
handle_start({ChannelNumber, OutOfBand},OpenFun,CloseFun,Do2,Do3,State) ->
    {ChannelPid, Number,  State0} = start_channel(ChannelNumber,CloseFun,Do2,Do3,State),
    OpenFun({Number, OutOfBand}, ChannelPid, State0),
    #'channel.open_ok'{} = amqp_channel:call(ChannelPid, #'channel.open'{out_of_band = OutOfBand}),
    {reply, ChannelPid, State0}.

%% Creates a new channel process
start_channel(ChannelNumber,CloseFun,Do2,Do3,State = #connection_state{reader_pid = ReaderPid,
                                                                 channel0_writer_pid = WriterPid}) ->
    Number = assign_channel_number(ChannelNumber, State),
    InitialState = #channel_state{parent_connection = self(),
                                  number = Number,
                                  close_fun = CloseFun,
                                  do2 = Do2, do3 = Do3,
                                  reader_pid = ReaderPid,
                                  writer_pid = WriterPid},
    process_flag(trap_exit, true),
    {ok, ChannelPid} = gen_server:start_link(amqp_channel, [InitialState], []),
    NewState = register_channel(Number, ChannelPid, State),
    {ChannelPid, Number, NewState}.

assign_channel_number(none, #connection_state{channels = Channels, channel_max = Max}) ->
    allocate_channel_number(dict:fetch_keys(Channels), Max);
assign_channel_number(ChannelNumber, State) ->
    %% TODO bug: check whether this is already taken
    ChannelNumber.

register_channel(ChannelNumber, ChannelPid, State = #connection_state{channels = Channels0}) ->
    Channels1 =
            case dict:is_key(ChannelNumber, Channels0) of
                true ->
                    exit({channel_already_registered, ChannelNumber});
                false ->
                    dict:store(ChannelNumber, ChannelPid , Channels0)
            end,
    State#connection_state{channels = Channels1}.

%% This will be called when a channel process exits and needs to be deregistered
%% This peforms the reverse mapping so that you can lookup a channel pid
%% Let's hope that this lookup doesn't get too expensive .......
unregister_channel(ChannelPid, State = #connection_state{channels = Channels0}) when is_pid(ChannelPid)->
    ReverseMapping = fun(Number, Pid) -> Pid == ChannelPid end,
    Projection = dict:filter(ReverseMapping, Channels0),
    %% TODO This differentiation is only necessary for the direct channel,
    %% look into preventing the invocation of this method
    Channels1 = case dict:fetch_keys(Projection) of
                    [] ->
                        Channels0;
                    [ChannelNumber|T] ->
                        dict:erase(ChannelNumber, Channels0)
                end,
    State#connection_state{channels = Channels1};

%% This will be called when a channel process exits and needs to be deregistered
unregister_channel(ChannelNumber, State = #connection_state{channels = Channels0}) ->
    Channels1 = dict:erase(ChannelNumber, Channels0),
    State#connection_state{channels = Channels1}.

allocate_channel_number([], Max)-> 1;

allocate_channel_number(Channels, Max) ->
    MaxChannel = lists:max(Channels),
    %% TODO check channel max and reallocate appropriately
    MaxChannel + 1.

close_connection(direct, Close, From, State) ->
    amqp_direct_driver:close_connection(Close, From, State);
close_connection(network, Close, From, State) ->
    amqp_network_driver:close_connection(Close, From, State).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([InitialState, Handshake]) ->
    State = Handshake(InitialState),
    {ok, State}.

%% Starts a new network channel.
handle_call({network, ChannelNumber, OutOfBand}, From, State) ->
    handle_start({ChannelNumber, OutOfBand},
                 fun amqp_network_driver:open_channel/3,
                 fun amqp_network_driver:close_channel/1,
                 fun amqp_network_driver:do/2,
                 fun amqp_network_driver:do/3,
                 State);

%% Starts a new direct channel.
handle_call({direct, ChannelNumber, OutOfBand}, From, State) ->
    handle_start({ChannelNumber, OutOfBand},
                 fun amqp_direct_driver:open_channel/3,
                 fun amqp_direct_driver:close_channel/1,
                 fun amqp_direct_driver:do/2,
                 fun amqp_direct_driver:do/3,
                 State);

%% Shuts the AMQP connection down
handle_call({Mode, Close = #'connection.close'{}}, From, State) ->
    close_connection(Mode, Close, From, State),
    {stop,normal,State}.

handle_cast(Message, State) ->
    {noreply, State}.

%---------------------------------------------------------------------------
% Trap exits
%---------------------------------------------------------------------------

handle_info( {'EXIT', Pid, {amqp,Reason,Msg,Context}}, State) ->
    io:format("Channel Peer ~p sent this message: ~p -> ~p~n",[Pid,Msg,Context]),
    io:format("Just trapping this exit and proceding to trap an exit from the client channel process~n"),
    {noreply, State};

%% Just the amqp channel shutting down, so unregister this channel
handle_info( {'EXIT', Pid, Reason}, State) ->
    io:format("Connection: Handling exit from ~p --> ~p~n",[Pid,Reason]),
    NewState = unregister_channel(Pid, State),
    {noreply, NewState}.

%---------------------------------------------------------------------------
% Rest of the gen_server callbacks
%---------------------------------------------------------------------------

terminate(Reason, State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    State.
