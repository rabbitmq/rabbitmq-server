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
-export([open_channel/1, open_channel/3]).
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
    gen_server:start_link(?MODULE, [InitialState, Driver], []);
start_internal(InitialState, Driver, _Link = false) when is_atom(Driver) ->
    gen_server:start(?MODULE, [InitialState, Driver], []).

%%---------------------------------------------------------------------------
%% Open and close channels API Methods
%%---------------------------------------------------------------------------

%% @doc Invokes open_channel(ConnectionPid, none, &lt;&lt;&gt;&gt;). 
%% Opens a channel without having to specify a channel number.
open_channel(ConnectionPid) ->
    open_channel(ConnectionPid, none, <<>>).

%% @spec (ConnectionPid, ChannelNumber, OutOfBand) -> ChannelPid
%% where
%%      ChannelNumber = integer()
%%      OutOfBand = binary()
%%      ConnectionPid = pid()
%%      ChannelPid = pid()
%% @doc Opens an AMQP channel.
%% This function assumes that an AMQP connection (networked or direct)
%% has already been successfully established.
open_channel(ConnectionPid, ChannelNumber, OutOfBand) ->
    gen_server:call(ConnectionPid,
                    {open_channel, ChannelNumber, OutOfBand}, infinity).

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
handle_open_channel({ChannelNumber, OutOfBand},
                    #connection_state{driver = Driver} = State) ->
    {ChannelPid, Number, NewState} = start_channel(ChannelNumber, State),
    Driver:open_channel({Number, OutOfBand}, ChannelPid, NewState),
    #'channel.open_ok'{} = amqp_channel:call(ChannelPid, #'channel.open'{}),
    {reply, ChannelPid, NewState}.

%% Creates a new channel process
start_channel(ChannelNumber,
              State = #connection_state{driver = Driver,
                                        reader_pid = ReaderPid,
                                        channel0_writer_pid = WriterPid}) ->
    ChannelState =
        #channel_state{
            parent_connection = self(),
            number = Number   = assign_channel_number(ChannelNumber, State),
            close_fun         = fun(X)       -> Driver:close_channel(X) end,
            do2               = fun(X, Y)    -> Driver:do(X, Y) end,
            do3               = fun(X, Y, Z) -> Driver:do(X, Y, Z) end,
            reader_pid        = ReaderPid,
            writer_pid        = WriterPid},
    {ok, ChannelPid} = gen_server:start_link(amqp_channel,
                                             [ChannelState], []),
    NewState = register_channel(Number, ChannelPid, State),
    {ChannelPid, Number, NewState}.

assign_channel_number(none, #connection_state{channels = Channels,
                                              channel_max = Max}) ->
    allocate_channel_number(dict:fetch_keys(Channels), Max);

assign_channel_number(ChannelNumber, _State) ->
    %% TODO bug: check whether this is already taken
    ChannelNumber.

register_channel(ChannelNumber, ChannelPid,
                 State = #connection_state{channels = Channels0}) ->
    Channels1 =
    case dict:is_key(ChannelNumber, Channels0) of
        true ->
            exit({channel_already_registered, ChannelNumber});
        false ->
            dict:store(ChannelNumber, ChannelPid, Channels0)
    end,
    State#connection_state{channels = Channels1}.

%% This will be called when a channel process exits and needs to be
%% deregistered
%% This peforms the reverse mapping so that you can lookup a channel pid
unregister_channel(ChannelPid,
                   State = #connection_state{channels = Channels0} )
        when is_pid(ChannelPid)->
    ReverseMapping = fun(_Number, Pid) -> Pid == ChannelPid end,
    Projection = dict:filter(ReverseMapping, Channels0),
    %% TODO This differentiation is only necessary for the direct channel,
    %% look into preventing the invocation of this method
    Channels1 = case dict:fetch_keys(Projection) of
                    [] ->
                        Channels0;
                    [ChannelNumber|_] ->
                        dict:erase(ChannelNumber, Channels0)
                end,
    State#connection_state{channels = Channels1};

%% This will be called when a channel process exits and needs to be
%% deregistered
unregister_channel(ChannelNumber,
                   State = #connection_state{channels = Channels0}) ->
    Channels1 = dict:erase(ChannelNumber, Channels0),
    State#connection_state{channels = Channels1}.

allocate_channel_number([], _Max)-> 1;

allocate_channel_number(Channels, _Max) ->
    MaxChannel = lists:max(Channels),
    %% TODO check channel max and reallocate appropriately
    MaxChannel + 1.

close_connection(Close, From, State = #connection_state{driver = Driver}) ->
    Driver:close_connection(Close, From, State).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

%% @private
init([InitialState, Driver]) when is_atom(Driver) ->
    process_flag(trap_exit, true),
    State = Driver:handshake(InitialState),
    {ok, State#connection_state{driver = Driver} }.

%% @private
%% Starts a new channel
handle_call({open_channel, ChannelNumber, OutOfBand}, _From, State) ->
    handle_open_channel({ChannelNumber, OutOfBand}, State);

%% @private
%% Shuts the AMQP connection down
handle_call(Close = #'connection.close'{}, From, State) ->
    close_connection(Close, From, State),
    {stop, normal, State}.

%%---------------------------------------------------------------------------
%% Handle forced close from the broker
%%---------------------------------------------------------------------------

%% @private
handle_cast({method, #'connection.close'{reply_code = Code,
                                         reply_text = Text}, _Content},
            State = #connection_state{driver = Driver}) ->
    ?LOG_WARN("Broker forced connection: ~p -> ~p~n", [Code, Text]),
    Driver:handle_broker_close(State),
    {stop, normal, State}.

%%---------------------------------------------------------------------------
%% Trap exits
%%---------------------------------------------------------------------------
%% @private
handle_info( {'EXIT', Pid, {amqp, Reason, Msg, Context}}, State) ->
    ?LOG_WARN("Channel Peer ~p sent this message: ~p -> ~p~n",
              [Pid, Msg, Context]),
    {HardError, Code, Text} = rabbit_framing:lookup_amqp_exception(Reason),
    case HardError of
        false ->
            ?LOG_DEBUG("Just trapping this exit and proceding to trap an "
                       "exit from the client channel process~n"),
            {noreply, State};
        true ->
            ?LOG_WARN("Hard error: (Code = ~p, Text = ~p)~n", [Code, Text]),
            {stop, {hard_error, {Code, Text}}, State}
    end;

%% @private
%% Just the amqp channel shutting down, so unregister this channel
handle_info( {'EXIT', Pid, normal}, State) ->
    {noreply, unregister_channel(Pid, State) };

%% @private
%% This is a special case for abruptly closed socket connections
handle_info( {'EXIT', _Pid, {socket_error, Reason}}, State) ->
    {stop, {socket_error, Reason}, State};

%% @private
handle_info( {'EXIT', _Pid, Reason = {unknown_message_type, _}}, State) ->
    {stop, Reason, State};

%% @private
handle_info( {'EXIT', _Pid, Reason = connection_socket_closed_unexpectedly},
             State) ->
    {stop, Reason, State};

%% @private
handle_info( {'EXIT', _Pid, Reason = connection_timeout}, State) ->
    {stop, Reason, State};

%% @private
handle_info( {'EXIT', Pid, Reason}, State) ->
    ?LOG_WARN("Connection: Handling exit from ~p --> ~p~n", [Pid, Reason]),
    {noreply, unregister_channel(Pid, State) }.

%%---------------------------------------------------------------------------
%% Rest of the gen_server callbacks
%%---------------------------------------------------------------------------

%% @private
terminate(_Reason, _State) ->
    ok.
    
%% @private
code_change(_OldVsn, State, _Extra) ->
    State.
