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
    gen_server:call(ConnectionPid, {open_channel, ChannelNumber}, infinity).

%% @spec (ConnectionPid) -> ok
%% where
%%      ConnectionPid = pid()
%% @doc Closes the channel, invokes close(Channel, 200, &lt;&lt;"Goodbye">>).
close(ConnectionPid) ->
    close(ConnectionPid, 200, <<"Goodbye">>).

%% @spec (ConnectionPid, Code, Text) -> ok
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
    #'connection.close_ok'{} = gen_server:call(ConnectionPid, Close, infinity),
    ok.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

%% Starts a new channel process, invokes the correct driver
%% (network or direct) to perform any environment specific channel setup and
%% starts the AMQP ChannelOpen handshake.
handle_open_channel(ProposedNumber,
                    ConnectionState = #connection_state{driver = Driver}) ->
    ChannelNumber = assign_channel_number(ProposedNumber, ConnectionState),
    ChannelState = #channel_state{parent_connection = self(),
                                  number = ChannelNumber,
                                  driver = Driver},
    {ok, ChannelPid} =
        gen_server:start_link(amqp_channel, {ChannelState, ConnectionState}, []),
    NewConnectionState =
        register_channel(ChannelNumber, ChannelPid, ConnectionState),
    #'channel.open_ok'{} = amqp_channel:call(ChannelPid, #'channel.open'{}),
    {reply, ChannelPid, NewConnectionState}.

assign_channel_number(none, #connection_state{channels = Channels}) ->
    %% TODO Implement support for channel_max from 'connection.tune'
    %% TODO Make it possible for channel numbers to be reused properly
    lists:foldl(
        fun
            ({channel, N},  Max) when Max >= N -> Max;
            ({channel, N}, _Max)               -> N;
            ({chpid, _},    Max)               -> Max
        end, 0, dict:fetch_keys(Channels)) + 1;

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
unregister_channel(Channel,
                   State = #connection_state{channels = Channels0} ) ->
    Val = dict:fetch(Channel, Channels0),
    Channels1 = dict:erase(Val, dict:erase(Channel, Channels0)),
    State#connection_state{channels = Channels1}.

%% Returns true iff channel defined either by {channel, ChannelNumber} or
%% {chpid, ChannelPid} is registered within the connection
is_registered_channel(Channel, #connection_state{channels = Channels}) ->
    dict:is_key(Channel, Channels).


%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

%% @private
init({InitialState, Driver}) when is_atom(Driver) ->
    process_flag(trap_exit, true),
    State = Driver:handshake(InitialState),
    {ok, State#connection_state{driver = Driver} }.

%% @private
%% Starts a new channel
handle_call({open_channel, ChannelNumber}, _From, State) ->
    handle_open_channel(ChannelNumber, State);

%% @private
%% Shuts the AMQP connection down
handle_call(Close = #'connection.close'{},
            From, State = #connection_state{driver = Driver}) ->
    Driver:close_connection(Close, From, State),
    {stop, normal, State}.

%%---------------------------------------------------------------------------
%% Handle forced close from the broker
%%---------------------------------------------------------------------------

%% Don't just exit here, rather, save the close message and wait for the
%% the server to close the socket (or timeout) and have the reader process
%% send an EXIT signal to this process.
%% @private
handle_cast({method, #'connection.close'{reply_code = Code,
                                         reply_text = Text}, none},
            State = #connection_state{driver = Driver}) ->
    Driver:handle_broker_close(State),
    Reason = {server_initiated_close, Code, Text},
    {noreply, State#connection_state{close_reason = Reason,
                                     closing      = true}}.

%% This can be sent by the channel process in the direct case
%% when it receives an amqp exception from it's corresponding channel process
handle_info({connection_level_error, Code, Text}, State) ->
    {stop, {server_initiated_close, Code, Text}, State};

%%---------------------------------------------------------------------------
%% Trap exits
%%---------------------------------------------------------------------------

%% Handle exit from writer0
%% @private
handle_info({'EXIT', Writer0Pid, Reason},
            State = #connection_state{channel0_writer_pid = Writer0Pid,
                                      closing = false}) ->
    ?LOG_WARN("Connection ~p closing: received exit signal from writer. "
              "Reason: ~p~n", [self(), Reason]),
    {stop, {writer0_died, Reason}, State};

%% Handle exit from reader0
%% @private
handle_info({'EXIT', Reader0Pid, Reason},
            State = #connection_state{channel0_reader_pid = Reader0Pid,
                                      closing = false}) ->
    ?LOG_WARN("Connection ~p closing: received exit signal from reader. "
              "Reason: ~p~n", [self(), Reason]),
    {stop, {reader0_died, Reason}, State};

%% Handle exit from main reader
%% @private
handle_info({'EXIT', MainReaderPid, Reason},
            State = #connection_state{main_reader_pid = MainReaderPid,
                                      closing = false}) ->
    ?LOG_WARN("Connection (~p) closing: received exit signal from main "
              "reader. Reason: ~p~n", [self(), Reason]),
    {stop, {main_reader_died, Reason}, State};

%% Handle exit from other pid
%% @private
handle_info({'EXIT', Pid, Reason}, State = #connection_state{closing = false}) ->
    case {is_registered_channel({chpid, Pid}, State), Reason} of
        %% Normal amqp_channel shutdown
        {true, normal} ->
            {noreply, unregister_channel({chpid, Pid}, State)};
        %% amqp_channel server forced shutdown (soft error)
        {true, {server_initiated_close, _, _}} ->
            {noreply, unregister_channel({chpid, Pid}, State)};
        %% amqp_channel server forced shutdown (hard error)
        {true, Error = #amqp_error{name = ErrorName}} ->
            ?LOG_WARN("Channel peer (~p) sent this message: ~p~n",
                      [Pid, Error]),
            {_, Code, Text} = rabbit_framing:lookup_amqp_exception(ErrorName),
            {stop, {server_initiated_close, Code, Text}, State};
        %% amqp_channel dies with internal reason - this takes the entire
        %% connection down
        {true, _} ->
            ?LOG_WARN("Connection (~p) closing: channel (~p) died. Reason: ~p~n",
                      [self(), Pid, Reason]),
            {stop, {channel_died, Pid, Reason}, State};
        %% Exit signal from unknown pid
        {false, _} ->
            ?LOG_WARN("Connection (~p) closing: received unexpected exit signal "
                      "from (~p). Reason: ~p~n", [self(), Pid, Reason]),
            {stop, {unexpected_exit_signal, Pid, Reason}, State}
    end;

%% Handle exit from main reader, when closing:
%% @private
handle_info({'EXIT', MainReaderPid, Reason},
            State = #connection_state{main_reader_pid = MainReaderPid,
                                      closing = true,
                                      close_reason = CloseReason}) ->
    case Reason of
        socket_closed ->
            {stop, CloseReason, State};
        socket_closing_timeout ->
            {stop, {socket_closing_timeout, CloseReason}, State};
        _ ->
            {stop,
             {main_reader_died_while_closing_connection, Reason, CloseReason},
             State}
    end;

%% Handle exit from some other process, when closing:
%% Just ignore all other exit signals. Wait for main reader to die due to
%% the server closing the socket or the socket_closing_timeout
%% @private
handle_info({'EXIT', _Pid, _Reason},
            State = #connection_state{closing = true}) ->
    {noreply, State}.

%%---------------------------------------------------------------------------
%% Rest of the gen_server callbacks
%%---------------------------------------------------------------------------

%% @private
terminate({Error, _Pid, _Reason}, State = #connection_state{driver = Driver}) 
    when Error == channel_died;
         Error == unexpected_exit_signal ->
    Close = #'connection.close'{reply_text = <<>>,
                                reply_code = ?INTERNAL_ERROR,
                                class_id   = 0,
                                method_id  = 0},
    Driver:close_connection(Close, State),
    ok;

%% @private
terminate(_Reason, _State) ->
    ok.
    
%% @private
code_change(_OldVsn, State, _Extra) ->
    State.
