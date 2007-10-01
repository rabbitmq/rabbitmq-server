-module(amqp_connection).

-include_lib("rabbit/include/rabbit.hrl").
-include_lib("rabbit/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-behaviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).
-export([open_channel/1, open_channel/3]).
-export([start/2, start/3, close/2]).

%---------------------------------------------------------------------------
% AMQP Connection API Methods
%---------------------------------------------------------------------------

%% Starts a direct connection to the Rabbit AMQP server, assuming that
%% the server is running in the same process space.
start(User, Password) ->
    Handshake = fun amqp_direct_driver:handshake/2,
    InitialState = #connection_state{username = User,
                                     password = Password,
                                     vhostpath = <<"/">>},
    {ok, Pid} = gen_server:start_link(?MODULE, [InitialState, Handshake], []),
    {Pid, direct}.

%% Starts a networked conection to a remote AMQP server.
start(User, Password, Host) ->
    Handshake = fun amqp_network_driver:handshake/2,
    InitialState = #connection_state{username = User,
                                     password = Password,
                                     serverhost = Host,
                                     vhostpath = <<"/">>},
    {ok, Pid} = gen_server:start_link(?MODULE, [InitialState, Handshake], []),
    {Pid, network}.

%% Opens a channel without having to specify a channel number.
%% This function assumes that an AMQP connection (networked or direct)
%% has already been successfully established.
open_channel( {Pid, Mode} ) ->
    open_channel( {Pid, Mode}, <<>>, "").

%% Opens a channel with a specific channel number.
%% This function assumes that an AMQP connection (networked or direct)
%% has already been successfully established.
open_channel( {ConnectionPid, Mode}, ChannelNumber, OutOfBand) ->
    gen_server:call(ConnectionPid, {Mode, ChannelNumber, amqp_util:binary(OutOfBand)}).

%% Closes the AMQP connection
close( {ConnectionPid, Mode}, Close) ->
    gen_server:call(ConnectionPid, {Mode, Close} ).

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

%% Starts a new channel process, invokes the correct driver (network or direct)
%% to perform any environment specific channel setup and starts the
%% AMQP ChannelOpen handshake.
handle_start({ChannelNumber, OutOfBand}, Driver, State) ->
    {ChannelPid, Number,  NewState} = start_channel(ChannelNumber, State),
    Driver({Number, OutOfBand}, ChannelPid, NewState),
    #'channel.open_ok'{} = amqp_channel:call(ChannelPid, #'channel.open'{out_of_band = OutOfBand}),
    {reply, ChannelPid, NewState}.

%% Creates a new channel process
start_channel(ChannelNumber, State = #connection_state{reader_pid = ReaderPid,
                                                       writer_pid = WriterPid}) ->
    Number = assign_channnel_number(ChannelNumber, State),
    InitialState = #channel_state{parent_connection = self(),
                                  number = Number,
                                  reader_pid = ReaderPid,
                                  writer_pid = WriterPid},
    process_flag(trap_exit, true),
    {ok, ChannelPid} = gen_server:start_link(amqp_channel, [InitialState], []),
    NewState = register_channel(ChannelNumber, ChannelPid, State),
    {ChannelPid, Number, NewState}.

assign_channnel_number(ChannelNumber, State) ->
    case ChannelNumber of
        <<>> ->
            allocate_channel_number(State);
        _ ->
            %% TODO bug: check whether this is already taken
            ChannelNumber
    end.

register_channel(ChannelNumber, ChannelPid, State = #connection_state{channels = Channels0}) ->
    Channels1 =
            case dict:is_key(ChannelNumber, Channels0) of
                true ->
                    exit(channel_already_registered, ChannelNumber);
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
    [ChannelNumber|T] = dict:fetch_keys(Projection),
    Channels1 = dict:erase(ChannelNumber, Channels0),
    State#connection_state{channels = Channels1};

%% This will be called when a channel process exits and needs to be deregistered
unregister_channel(ChannelNumber, State = #connection_state{channels = Channels0}) ->
    Channels1 = dict:erase(ChannelNumber, Channels0),
    State#connection_state{channels = Channels1}.

allocate_channel_number(State = #connection_state{channels = Channels0,
                                                  channel_max = ChannelMax}) ->
    List = dict:fetch_keys(Channels0),
    ChannelNumber =
        case length(List) of
            0 ->
                1;
            _ ->
                MaxChannel = lists:max(List),
                %% TODO check channel max and reallocate appropriately
                MaxChannel + 1
        end,
    ChannelNumber.

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([InitialState, Handshake]) ->
    State = Handshake(self(), InitialState),
    {ok, State}.

%% Starts a new network channel.
handle_call({network, ChannelNumber, OutOfBand}, From, State) ->
    handle_start({ChannelNumber, OutOfBand}, fun amqp_network_driver:open_channel/3, State);

%% Starts a new direct channel.
handle_call({direct, ChannelNumber, OutOfBand}, From, State) ->
    handle_start({ChannelNumber, OutOfBand}, fun amqp_direct_driver:open_channel/3, State);

%% Shuts the AMQP connection down in the network case
handle_call({network, Close = #'connection.close'{}}, From, State = #connection_state{writer_pid = Writer}) ->
    amqp_network_driver:close_connection(Close, From, State),
    {stop, shutdown, #'connection.close_ok'{}, State};

%% Shuts the AMQP connection down in the direct case
handle_call({direct, Close = #'connection.close'{}}, From, State) ->
    amqp_direct_driver:close_connection(Close, From, State),
    {stop, shutdown, #'connection.close_ok'{}, State}.

handle_cast(Message, State) ->
    {noreply, State}.

%---------------------------------------------------------------------------
% Trap exits
%---------------------------------------------------------------------------

%% Just the rabbit channel exiting, ignore this for now
handle_info( {'EXIT', Pid, normal}, State) ->
    {noreply, State};

%% Just the amqp channel shutting down, so unregister this channel
handle_info( {'EXIT', Pid, shutdown}, State) ->
    NewState = unregister_channel(Pid, State),
    {noreply, NewState};

%% TODO Don't know what's going on here, check it out
handle_info( {'EXIT', CrashedPid, Reason}, State) ->
    io:format("REAL TRAPPED EXIT.......... ~p /~p~n",[CrashedPid, Reason]),
    NewState = unregister_channel(CrashedPid, State),
    {noreply, NewState}.

%---------------------------------------------------------------------------
% Rest of the gen_server callbacks
%---------------------------------------------------------------------------

terminate(Reason, State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.
