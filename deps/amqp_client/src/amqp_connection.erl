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

%% @doc This module is responsible for maintaining a connection to an AMQP
%% broker and manages channels within the connection. This module is used to
%% open and close connections to the broker as well as creating new channels
%% within a connection. Each amqp_connection process maintains a mapping of
%% the channels that were created by that connection process. Each resulting
%% amqp_channel process is linked to the parent connection process.
-module(amqp_connection).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).
-export([open_channel/1, open_channel/2]).
-export([start_direct/0, start_direct/1, start_direct_link/0, start_direct_link/1]).
-export([start_network/0, start_network/1, start_network_link/0, start_network_link/1]).
-export([close/1, close/3]).

-record(closing, {reason, close, from = none, phase = terminate_channels}).

%%---------------------------------------------------------------------------
%% Type Definitions
%%---------------------------------------------------------------------------

%% @type amqp_params() = #amqp_params{}.
%% As defined in amqp_client.hrl. It contains the following fields:
%% <ul>
%% <li>username :: binary() - The name of a user registered with the broker, 
%%     defaults to &lt;&lt;guest"&gt;&gt;</li>
%% <li>password :: binary() - The user's password, defaults to 
%%     &lt;&lt;"guest"&gt;&gt;</li>
%% <li>virtual_host :: binary() - The name of a virtual host in the broker,
%%     defaults to &lt;&lt;"/"&gt;&gt;</li>
%% <li>host :: string() - The hostname of the broker,
%%     defaults to "localhost"</li>
%% <li>port :: integer() - The port the broker is listening on,
%%     defaults to 5672</li>
%% </ul>

%%---------------------------------------------------------------------------
%% AMQP Connection API Methods
%%---------------------------------------------------------------------------

%% @spec () -> [Connection]
%% where
%%     Connection = pid()
%% @doc Starts a direct connection to a RabbitMQ server, assuming that
%% the server is running in the same process space, and with a default
%% set of amqp_params. If a different vhost or credential set is required,
%% start_direct/1 should be used.
start_direct() -> start_direct(#amqp_params{}).

%% @spec (amqp_params()) -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a direct connection to a RabbitMQ server, assuming that
%% the server is running in the same process space.
start_direct(Params) -> start_direct_internal(Params, false).

%% @spec () -> [Connection]
%% where
%%     Connection = pid()
%% @doc Starts a direct connection to a RabbitMQ server, assuming that
%% the server is running in the same process space, and with a default
%% set of amqp_params. If a different vhost or credential set is required,
%% start_direct_link/1 should be used. The resulting
%% process is linked to the invoking process.
start_direct_link() -> start_direct_link(#amqp_params{}).

%% @spec (amqp_params()) -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a direct connection to a RabbitMQ server, assuming that
%% the server is running in the same process space. The resulting process
%% is linked to the invoking process.
start_direct_link(Params) -> start_direct_internal(Params, true).

start_direct_internal(#amqp_params{username     = User,
                                   password     = Password,
                                   virtual_host = VHost},
                      ProcLink) ->
    InitialState = #connection_state{username  = User,
                                     password  = Password,
                                     vhostpath = VHost},
    {ok, Pid} = start_internal(InitialState, amqp_direct_driver, ProcLink),
    Pid.

%% @spec () -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a networked conection to a remote AMQP server. Default
%% connection settings are used, meaning that the server is expected
%% to be at localhost:5672, with a vhost of "/" authorising a user
%% guest/guest.
start_network() -> start_network(#amqp_params{}).

%% @spec (amqp_params()) -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a networked conection to a remote AMQP server.
start_network(Params) -> start_network_internal(Params, false).

%% @spec () -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a networked conection to a remote AMQP server. Default
%% connection settings are used, meaning that the server is expected
%% to be at localhost:5672, with a vhost of "/" authorising a user
%% guest/guest. The resulting process is linked to the invoking process.
start_network_link() -> start_network_link(#amqp_params{}).

%% @spec (amqp_params()) -> [Connection]
%% where
%%      Connection = pid()
%% @doc Starts a networked connection to a remote AMQP server. The resulting 
%% process is linked to the invoking process.
start_network_link(Params) -> start_network_internal(Params, true).

start_network_internal(#amqp_params{username     = User,
                                    password     = Password,
                                    virtual_host = VHost,
                                    host         = Host,
                                    port         = Port,
                                    ssl_options  = SSLOpts},
                       ProcLink) ->
    InitialState = #connection_state{username    = User,
                                     password    = Password,
                                     serverhost  = Host,
                                     vhostpath   = VHost,
                                     port        = Port,
                                     ssl_options = SSLOpts},
    {ok, Pid} = start_internal(InitialState, amqp_network_driver, ProcLink),
    Pid.

start_internal(InitialState, Driver, _Link = true) when is_atom(Driver) ->
    gen_server:start_link(?MODULE, {InitialState, Driver}, []);
start_internal(InitialState, Driver, _Link = false) when is_atom(Driver) ->
    gen_server:start(?MODULE, {InitialState, Driver}, []).

%%---------------------------------------------------------------------------
%% Open and close channels API Methods
%%---------------------------------------------------------------------------

%% @doc Invokes open_channel(ConnectionPid, none, &lt;&lt;&gt;&gt;). 
%% Opens a channel without having to specify a channel number.
open_channel(ConnectionPid) ->
    open_channel(ConnectionPid, none).

%% @spec (ConnectionPid, ChannelNumber) -> ChannelPid
%% where
%%      ChannelNumber = integer()
%%      ConnectionPid = pid()
%%      ChannelPid = pid()
%% @doc Opens an AMQP channel.
%% This function assumes that an AMQP connection (networked or direct)
%% has already been successfully established.
open_channel(ConnectionPid, ChannelNumber) ->
    command(ConnectionPid, {open_channel, ChannelNumber}).

%% @spec (ConnectionPid) -> ok | Error
%% where
%%      ConnectionPid = pid()
%% @doc Closes the channel, invokes close(Channel, 200, &lt;&lt;"Goodbye">>).
close(ConnectionPid) ->
    close(ConnectionPid, 200, <<"Goodbye">>).

%% @spec (ConnectionPid, Code, Text) -> ok | closing
%% where
%%      ConnectionPid = pid()
%%      Code = integer()
%%      Text = binary()
%% @doc Closes the AMQP connection, allowing the caller to set the reply
%% code and text.
close(ConnectionPid, Code, Text) -> 
    Close = #'connection.close'{reply_text =  Text,
                                reply_code = Code,
                                class_id   = 0,
                                method_id  = 0},
    command(ConnectionPid, {close, Close}).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

command(ConnectionPid, Command) ->
    gen_server:call(ConnectionPid, {command, Command}, infinity).

broadcast_to_channels(Message, #connection_state{channels = Channels}) ->
    dict:map(fun({chpid, Channel}, _) -> Channel ! Message, ok;
                ({channel, _}, _)     -> ok
             end, Channels),
    ok.

closing_to_reason(#closing{reason = Reason,
                           close = #'connection.close'{reply_code = Code,
                                                       reply_text = Text}}) ->
    {Reason, Code, Text}.

%% Changes connection's state to closing.
%%
%% The closing reason (Closing#closing.reason) can be one of the following
%%     app_initiated_close - app has invoked the close/{1,3} command. In this
%%         case the close field is the method to be sent to the server after all
%%         the channels have terminated (and flushed); the from field is the
%%         process that initiated the call and to whom the server must reply.
%%         phase = terminate_channels | wait_close_ok
%%     internal_error - there was an internal error either in a channel or in
%%         the connection process. close field is the method to be sent to the
%%         server after all channels have been abruptly terminated (do not flush
%%         in this case).
%%         phase = terminate_channels | wait_close_ok
%%     server_initiated_close - server has sent 'connection.close'. close field
%%         is the method sent by the server.
%%         phase = terminate_channels | wait_socket_close
%%
%% The precedence of the closing MainReason's is as follows:
%%     app_initiated_close, internal_error, server_initiated_close
%% (i.e.: a given reason can override the currently set one if it is later
%% mentioned in the above list)
%% Also, internal_error and server_initiated_close can also override themselves
%% (i.e.: update the close method and restart the closing process)
%%
%% ChannelCloseType can be flush or abrupt
set_closing_state(ChannelCloseType, Closing, State) ->
    broadcast_to_channels(
        {connection_closing, ChannelCloseType, closing_to_reason(Closing)},
         State),
    check_trigger_all_channels_closed_event(
        State#connection_state{closing = Closing}).

%% The all_channels_closed_event is called when all channels have been closed
%% after the connection broadcasts a connection_closing message to all channels
check_trigger_all_channels_closed_event(
        #connection_state{closing = false} = State) ->
    State;
check_trigger_all_channels_closed_event(
        #connection_state{channels = Channels,
                          driver = Driver,
                          closing = Closing} = State) ->
    #closing{reason = Reason, close = Close, phase = ClosingPhase} = Closing,
    ClosingPhase = terminate_channels, % assertion
    IsChannelsEmpty = (dict:size(Channels) == 0),
    if IsChannelsEmpty ->
           NewPhase = Driver:all_channels_closed_event(Reason, Close, State),
           State#connection_state{closing = Closing#closing{phase = NewPhase}};
       true ->
           State
    end.

internal_error_closing() ->
    #closing{reason = internal_error,
             close = #'connection.close'{reply_text = <<>>,
                                         reply_code = ?INTERNAL_ERROR,
                                         class_id = 0,
                                         method_id = 0}}.

%%---------------------------------------------------------------------------
%% Channel assignment (internal plumbing)
%%---------------------------------------------------------------------------

assign_channel_number(none, #connection_state{channels = Channels}) ->
    %% TODO Implement support for channel_max from 'connection.tune'
    %% TODO Make it possible for channel numbers to be reused properly
    dict:fold(fun({channel, N}, _,  Max) when Max >= N -> Max;
                 ({channel, N}, _, _Max)               -> N;
                 ({chpid, _}, _, Max)                  -> Max
              end, 0, Channels) + 1;

assign_channel_number(ChannelNumber, State = #connection_state{channels = Channels}) ->
    case dict:is_key({channel, ChannelNumber}, Channels) of
        true  -> assign_channel_number(none, State);
        false -> ChannelNumber
    end.

register_channel(ChannelNumber, ChannelPid,
                 State = #connection_state{channels = Channels0}) ->
    case dict:is_key({channel, ChannelNumber}, Channels0) of
        true ->
            exit({channel_already_registered, ChannelNumber});
        false ->
            Channels1 = dict:store({channel, ChannelNumber},
                                   {chpid, ChannelPid}, Channels0),
            Channels2 = dict:store({chpid, ChannelPid},
                                   {channel, ChannelNumber}, Channels1),
            State#connection_state{channels = Channels2}
    end.

%% This will be called when a channel process exits and needs to be
%% deregistered. Can be called with either {channel, ChannelNumber} or
%% {chpid, ChannelPid}
unregister_channel(Channel, State = #connection_state{channels = Channels0}) ->
    Val = dict:fetch(Channel, Channels0),
    Channels1 = dict:erase(Val, dict:erase(Channel, Channels0)),
    NewState = State#connection_state{channels = Channels1},
    check_trigger_all_channels_closed_event(NewState).

%% Returns true iff channel defined either by {channel, ChannelNumber} or
%% {chpid, ChannelPid} is registered within the connection
is_registered_channel(Channel, #connection_state{channels = Channels}) ->
    dict:is_key(Channel, Channels).

%%---------------------------------------------------------------------------
%% Command handling
%%---------------------------------------------------------------------------

handle_command({open_channel, ProposedNumber}, _From,
               #connection_state{driver = Driver} = State) ->
    ChannelNumber = assign_channel_number(ProposedNumber, State),
    ChannelState = #channel_state{parent_connection = self(),
                                  number = ChannelNumber,
                                  driver = Driver},
    {ok, ChannelPid} = gen_server:start_link(amqp_channel,
                                             {ChannelState, State}, []),
    NewState = register_channel(ChannelNumber, ChannelPid, State),
    #'channel.open_ok'{} = amqp_channel:call(ChannelPid, #'channel.open'{}),
    {reply, ChannelPid, NewState};

handle_command({close, #'connection.close'{} = Close}, From, State) ->
    {noreply, set_closing_state(flush, #closing{reason = app_initiated_close,
                                                close = Close,
                                                from = From},
                                State)}.

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

%% @private
init({InitialState, Driver}) when is_atom(Driver) ->
    process_flag(trap_exit, true),
    State = Driver:handshake(InitialState),
    {ok, State#connection_state{driver = Driver}}.

%% Standard handling of a command
%% @private
handle_call({command, Command}, From,
            #connection_state{closing = Closing} = State) ->
    case Closing of
        false -> handle_command(Command, From, State);
        _     -> {reply, closing, State}
    end.

%% Handle forced close from the broker
%% Network case: terminate channels abruptly and enter closing state
%% (wait for channels to terminate and then send 'connection.close_ok')
%% @private
handle_cast({method, #'connection.close'{} = Close, none}, State) ->
    {noreply, set_closing_state(abrupt,
                                #closing{reason = server_initiated_close,
                                         close = Close},
                                State)};

%% Handle close_ok from broker: reply to app and stop
handle_cast(
        {method, #'connection.close_ok'{}, none},
        State = #connection_state{closing = #closing{from = From} = Closing}) ->
    case From of
        none -> ok;
        _    -> gen_server:reply(From, ok)
    end,
    {stop, closing_to_reason(Closing), State}.

%% Handle forced close from the broker
%% Direct case: Just stop connection here
%% @private
handle_info({connection_level_error, Code, Text},
            #connection_state{driver = Driver} = State) ->
    Driver:handle_broker_close(State),
    {stop, {server_initiated_close, Code, Text}, State};

%% This is received after we have sent 'connection.close' to the server
%% but timed out waiting for 'connection.close_ok' back
handle_info(timeout_waiting_for_close_ok,
            State = #connection_state{closing = Closing}) ->
    ?LOG_WARN("Connection ~p closing: timed out waiting for"
              "'connection.close_ok'.", [self()]),
    {stop, {timeout_waiting_for_close_ok, closing_to_reason(Closing)}, State};

%%---------------------------------------------------------------------------
%% Trap exits
%%---------------------------------------------------------------------------

%% Handle exit from writer0
%% @private
handle_info({'EXIT', Writer0Pid, Reason},
            State = #connection_state{channel0_writer_pid = Writer0Pid}) ->
    ?LOG_WARN("Connection (~p) closing: received exit signal from writer. "
              "Reason: ~p~n", [self(), Reason]),
    {stop, {writer0_died, Reason}, State};

%% Handle exit from reader0
%% @private
handle_info({'EXIT', Reader0Pid, Reason},
            State = #connection_state{channel0_reader_pid = Reader0Pid}) ->
    ?LOG_WARN("Connection (~p) closing: received exit signal from reader. "
              "Reason: ~p~n", [self(), Reason]),
    {stop, {reader0_died, Reason}, State};

%% Handle exit from main reader
%% @private
handle_info({'EXIT', MainReaderPid, Reason},
            State = #connection_state{main_reader_pid = MainReaderPid,
                                      closing = Closing}) ->
    #closing{reason = ClosingReason, phase = ClosingPhase} = Closing,
    case {ClosingReason, ClosingPhase, Reason} of
        %% Normal server initiated shutdown exit (socket has been closed after
        %% replying with 'connection.close_ok')
        {server_initiated_close, wait_socket_close, socket_closed} ->
            {stop, closing_to_reason(Closing), State};
        %% Timed out waiting for socket to close after replying with
        %% 'connection.close_ok'
        {server_initiated_close, wait_socket_close, socket_closing_timeout} ->
            ?LOG_WARN("Connection (~p) closing: timed out waiting for socket "
                      "to close after sending 'connection.close_ok", [self()]),
            {stop, {socket_closing_timeout, closing_to_reason(Closing)}, State};
        %% Main reader died
        _ ->
            ?LOG_WARN("Connection (~p) closing: received exit signal from main "
                      "reader. Reason: ~p~n", [self(), Reason]),
            {stop, {main_reader_died, Reason}, State}
    end;

%% Handle exit from other pid
%% @private
handle_info({'EXIT', Pid, Reason},
            #connection_state{closing = Closing} = State) ->
    case {is_registered_channel({chpid, Pid}, State), Reason} of
        %% Normal amqp_channel shutdown
        {true, normal} ->
            {noreply, unregister_channel({chpid, Pid}, State)};
        %% Channel terminating due to soft error
        {true, {server_initiated_close, _, _}} ->
            {noreply, unregister_channel({chpid, Pid}, State)};
        %% Channel terminating because of connection_closing
        {true, {_, _, _}} when Closing =/= false ->
            {noreply, unregister_channel({chpid, Pid}, State)};
        %% amqp_channel dies with internal reason - this takes the entire
        %% connection down
        {true, _} ->
            ?LOG_WARN("Connection (~p) closing: channel (~p) died. Reason: ~p~n",
                      [self(), Pid, Reason]),
            State1 = unregister_channel({chpid, Pid}, State),
            State2 = set_closing_state(abrupt, internal_error_closing(),
                                       State1),
            {noreply, State2};
        %% Exit signal from unknown pid
        {false, _} ->
            ?LOG_WARN("Connection (~p) closing: received unexpected exit signal "
                      "from (~p). Reason: ~p~n", [self(), Pid, Reason]),
            State1 = unregister_channel({chpid, Pid}, State),
            State2 = set_closing_state(abrupt, internal_error_closing(),
                                       State1),
            {noreply, State2}
    end.

%%---------------------------------------------------------------------------
%% Rest of the gen_server callbacks
%%---------------------------------------------------------------------------

%% @private
terminate(Reason, #connection_state{driver = Driver} = State) ->
    Driver:terminate_connection(Reason, State).
    
%% @private
code_change(_OldVsn, State, _Extra) ->
    State.
