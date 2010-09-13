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

%% @private
-module(amqp_network_connection).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/3, connect/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-define(RABBIT_TCP_OPTS, [binary, {packet, 0}, {active,false}, {nodelay, true}]).
-define(SOCKET_CLOSING_TIMEOUT, 1000).
-define(CLIENT_CLOSE_TIMEOUT, 60000).
-define(HANDSHAKE_RECEIVE_TIMEOUT, 60000).

-record(state, {sup,
                params,
                sock,
                max_channel,
                heartbeat,
                framing0,
                writer0,
                main_reader,
                channel_sup_sup,
                closing = false,
                server_properties,
                channels = amqp_channel_util:new_channel_dict(),
                start_heartbeat_fun,
                start_infrastructure_fun}).

-record(closing, {reason,
                  close,
                  from = none,
                  phase = terminate_channels}).

-define(INFO_KEYS, (amqp_connection:info_keys() ++
                        [max_channel, heartbeat, sock])).

%%---------------------------------------------------------------------------
%% Internal interface
%%---------------------------------------------------------------------------

start_link(AmqpParams, ChSupSup, SIF) ->
    gen_server:start_link(?MODULE, [self(), AmqpParams, ChSupSup, SIF], []).

connect(Pid) ->
    gen_server:call(Pid, connect, infinity).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init([Sup, AmqpParams, ChSupSup, SIF]) ->
    {ok, #state{sup                      = Sup,
                params                   = AmqpParams,
                channel_sup_sup          = ChSupSup,
                start_infrastructure_fun = SIF}}.

handle_call({command, Command}, From, #state{closing = false} = State) ->
    handle_command(Command, From, State);
handle_call({command, _Command}, _From, State) ->
    {reply, closing, State};
handle_call({info, Items}, _From, State) ->
    {reply, [{Item, i(Item, State)} || Item <- Items], State};
handle_call(info_keys, _From, State) ->
    {reply, ?INFO_KEYS, State};
handle_call(connect, _From, State) ->
    {reply, ok, do_connect(State)}.

%% Standard handling of a method sent by the broker (this is received from
%% framing0)
handle_cast({method, Method, Content}, State) ->
    handle_method(Method, Content, State).

%% This is received after we have sent 'connection.close' to the server
%% but timed out waiting for 'connection.close_ok' back
handle_info(timeout_waiting_for_close_ok, State = #state{closing = Closing}) ->
    #closing{phase = wait_close_ok} = Closing, % assertion
    {stop, {timeout_waiting_for_close_ok, closing_to_reason(Closing)}, State};
%% This is received when the main reader has not sent socket_closed in time
handle_info(socket_closing_timeout, State = #state{closing = Closing}) ->
    #closing{phase = wait_socket_close} = Closing, % assertion
    {stop, {socket_closing_timeout, closing_to_reason(Closing)}, State};
%% Received from main reader
handle_info(socket_closed, State) ->
    handle_socket_closed(State);
%% DOWN signals from channels
handle_info({'DOWN', _, process, Pid, Reason}, State) ->
    handle_channel_exit(Pid, Reason, State);
%% timeout from heartbeat receiver
handle_info(timeout, State) ->
    {stop, heartbeat_timeout, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

%%---------------------------------------------------------------------------
%% Command handling
%%---------------------------------------------------------------------------

handle_command({open_channel, ProposedNumber}, _From,
               State = #state{sock            = Sock,
                              channels        = Channels,
                              max_channel     = MaxChannel,
                              main_reader     = MainReader,
                              channel_sup_sup = ChSupSup}) ->
    try amqp_channel_util:open_channel(ChSupSup, ProposedNumber, MaxChannel,
                                       [Sock, MainReader], Channels) of
        {ChannelPid, NewChannels} ->
            {reply, {ok, ChannelPid}, State#state{channels = NewChannels}}
    catch
        error:out_of_channel_numbers = Error ->
            {reply, {error, {Error, MaxChannel}}, State}
    end;

handle_command({close, #'connection.close'{} = Close}, From, State) ->
    {noreply, set_closing_state(flush, #closing{reason = app_initiated_close,
                                                close  = Close,
                                                from   = From},
                                State)}.

%%---------------------------------------------------------------------------
%% Handling methods from broker
%%---------------------------------------------------------------------------

handle_method(#'connection.close'{} = Close, none, State) ->
    {noreply, set_closing_state(abrupt,
                                #closing{reason = server_initiated_close,
                                         close  = Close},
                                State)};

handle_method(#'connection.close_ok'{}, none,
              State = #state{closing = Closing}) ->
    #closing{from = From,
             close = #'connection.close'{reply_code = ReplyCode}} = Closing,
    case From of
        none -> ok;
        _    -> gen_server:reply(From, ok)
    end,
    if ReplyCode =:= 200 -> {stop, normal, State};
       true              -> {stop, closing_to_reason(Closing), State}
    end.

%%---------------------------------------------------------------------------
%% Infos
%%---------------------------------------------------------------------------

i(server_properties, State) -> State#state.server_properties;
i(is_closing,        State) -> State#state.closing =/= false;
i(amqp_params,       State) -> State#state.params;
i(max_channel,       State) -> State#state.max_channel;
i(heartbeat,         State) -> State#state.heartbeat;
i(sock,              State) -> State#state.sock;
i(num_channels,      State) -> amqp_channel_util:num_channels(
                                   State#state.channels);
i(Item,             _State) -> throw({bad_argument, Item}).

%%---------------------------------------------------------------------------
%% Closing
%%---------------------------------------------------------------------------

%% Changes connection's state to closing.
%%
%% ChannelCloseType can be flush or abrupt
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
%% mentioned in the above list). We can rely on erlang's comparison of atoms
%% for this.
set_closing_state(ChannelCloseType, Closing, 
                  #state{closing = false,
                         channels = Channels} = State) ->
    amqp_channel_util:broadcast_to_channels(
        {connection_closing, ChannelCloseType, closing_to_reason(Closing)},
        Channels),
    check_trigger_all_channels_closed_event(State#state{closing = Closing});
%% Already closing, override situation
set_closing_state(ChannelCloseType, NewClosing,
                  #state{closing = CurClosing,
                         channels = Channels} = State) ->
    %% Do not override reason in channels (because it might cause channels
    %% to exit with different reasons) but do cause them to close abruptly
    %% if the new closing type requires it
    case ChannelCloseType of
        abrupt ->
            amqp_channel_util:broadcast_to_channels(
                {connection_closing, ChannelCloseType,
                 closing_to_reason(CurClosing)},
                Channels);
        _ -> ok
   end,
   #closing{reason = NewReason, close = NewClose} = NewClosing,
   #closing{reason = CurReason} = CurClosing,
   ResClosing =
       if
           %% Override (rely on erlang's comparison of atoms)
           NewReason >= CurReason ->
               %% Note that when overriding, we keep the current phase
               CurClosing#closing{reason = NewReason, close = NewClose};
           %% Do not override
           true ->
               CurClosing
       end,
    NewState = State#state{closing = ResClosing},
    %% Now check if it's the case that the server has sent a connection.close
    %% while we were in the closing state (for whatever reason). We need to
    %% send connection.close_ok (it might be even be the case that we are
    %% sending it again) and wait for the socket to close.
    case NewReason of
        server_initiated_close -> all_channels_closed_event(NewState);
        _                      -> NewState
    end.

%% The all_channels_closed_event is called when all channels have been closed
%% after the connection broadcasts a connection_closing message to all channels
all_channels_closed_event(#state{writer0 = Writer,
                                 closing = Closing} = State) ->
    #closing{reason = Reason, close = Close} = Closing,
    case Reason of
        server_initiated_close ->
            amqp_channel_util:do(network, Writer, #'connection.close_ok'{},
                                 none),
            erlang:send_after(?SOCKET_CLOSING_TIMEOUT, self(),
                              socket_closing_timeout),
            State#state{closing = Closing#closing{phase = wait_socket_close}};
        _ ->
            amqp_channel_util:do(network, Writer, Close, none),
            erlang:send_after(?CLIENT_CLOSE_TIMEOUT, self(),
                              timeout_waiting_for_close_ok),
            State#state{closing = Closing#closing{phase = wait_close_ok}}
    end.

closing_to_reason(#closing{reason = Reason,
                           close = #'connection.close'{reply_code = Code,
                                                       reply_text = Text}}) ->
    {Reason, Code, Text}.

internal_error_closing() ->
    #closing{reason = internal_error,
             close = #'connection.close'{reply_text = <<>>,
                                         reply_code = ?INTERNAL_ERROR,
                                         class_id = 0,
                                         method_id = 0}}.

handle_socket_closed(State = #state{
                       closing = Closing = #closing{
                                   phase = wait_socket_close}}) ->
    {stop, closing_to_reason(Closing), State};
handle_socket_closed(State) ->
    {stop, socket_closed_unexpectedly, State}.

%%---------------------------------------------------------------------------
%% Channel utilities
%%---------------------------------------------------------------------------

unregister_channel(Pid, State = #state{channels = Channels}) ->
    NewChannels = amqp_channel_util:unregister_channel_pid(Pid, Channels),
    NewState = State#state{channels = NewChannels},
    check_trigger_all_channels_closed_event(NewState).

check_trigger_all_channels_closed_event(#state{closing = false} = State) ->
    State;
check_trigger_all_channels_closed_event(#state{channels = Channels,
                                               closing  = Closing} = State) ->
    #closing{phase = terminate_channels} = Closing, %% assertion
    case amqp_channel_util:is_channel_dict_empty(Channels) of
        true  -> all_channels_closed_event(State);
        false -> State
    end.

handle_channel_exit(Pid, Reason,
            #state{channels = Channels, closing = Closing} = State) ->
    case amqp_channel_util:handle_exit(Pid, Reason, Channels, Closing) of
        stop   -> {stop, Reason, State};
        normal -> {noreply, unregister_channel(Pid, State)};
        close  -> {noreply, set_closing_state(abrupt, internal_error_closing(),
                                              unregister_channel(Pid, State))};
        other  -> {noreply, set_closing_state(abrupt, internal_error_closing(),
                                              State)}
    end.

%%---------------------------------------------------------------------------
%% Handshake
%%---------------------------------------------------------------------------

do_connect(State = #state{params = #amqp_params{host        = Host,
                                                port        = Port,
                                                ssl_options = none}}) ->
    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Sock}      -> handshake(State#state{sock = Sock});
        {error, Reason} -> ?LOG_WARN("Could not start the network driver: ~p~n",
                                     [Reason]),
                           exit(Reason)
    end;
do_connect(State = #state{params = #amqp_params{host        = Host,
                                                port        = Port,
                                                ssl_options = SslOpts}}) ->
    rabbit_misc:start_applications([crypto, ssl]),
    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Sock} ->
            case ssl:connect(Sock, SslOpts) of
                {ok, SslSock} ->
                    RabbitSslSock = #ssl_socket{ssl = SslSock, tcp = Sock},
                    handshake(State#state{sock = RabbitSslSock});
                {error, Reason} ->
                    ?LOG_WARN("Could not upgrade the network driver to ssl: "
                              "~p~n", [Reason]),
                    exit(Reason)
            end;
        {error, Reason} ->
            ?LOG_WARN("Could not start the network driver: ~p~n", [Reason]),
            exit(Reason)
    end.

handshake(State0 = #state{sock = Sock}) ->
    ok = rabbit_net:send(Sock, ?PROTOCOL_HEADER),
    State1 = start_infrastructure(State0),
    State2 = network_handshake(State1),
    start_heartbeat(State2),
    State2.

start_infrastructure(State = #state{start_infrastructure_fun = SIF,
                                    sock = Sock}) ->
    {ok, {MainReader, Framing, Writer, SHF}} = SIF(Sock),
    State#state{main_reader         = MainReader,
                framing0            = Framing,
                writer0             = Writer,
                start_heartbeat_fun = SHF}.

network_handshake(State = #state{writer0 = Writer, params = Params}) ->
    Start = handshake_recv(),
    #'connection.start'{server_properties = ServerProperties} = Start,
    ok = check_version(Start),
    amqp_channel_util:do(network, Writer, start_ok(State), none),
    Tune = handshake_recv(),
    TuneOk = negotiate_values(Tune, Params),
    amqp_channel_util:do(network, Writer, TuneOk, none),
    ConnectionOpen =
        #'connection.open'{virtual_host = Params#amqp_params.virtual_host},
    amqp_channel_util:do(network, Writer, ConnectionOpen, none),
    #'connection.open_ok'{} = handshake_recv(),
    #'connection.tune_ok'{channel_max = ChannelMax,
                          frame_max   = FrameMax,
                          heartbeat   = Heartbeat} = TuneOk,
    ?LOG_INFO("Negotiated maximums: (Channel = ~p, Frame = ~p, "
              "Heartbeat = ~p)~n",
             [ChannelMax, FrameMax, Heartbeat]),
    State#state{max_channel       = ChannelMax,
                heartbeat         = Heartbeat,
                server_properties = ServerProperties}.

start_heartbeat(#state{start_heartbeat_fun = SHF,
                       sock                = Sock,
                       heartbeat           = Heartbeat}) ->
    SHF(Sock, Heartbeat).

check_version(#'connection.start'{version_major = ?PROTOCOL_VERSION_MAJOR,
                                  version_minor = ?PROTOCOL_VERSION_MINOR}) ->
    ok;
check_version(#'connection.start'{version_major = 8,
                                  version_minor = 0}) ->
    exit({protocol_version_mismatch, 0, 8});
check_version(#'connection.start'{version_major = Major,
                                  version_minor = Minor}) ->
    exit({protocol_version_mismatch, Major, Minor}).

negotiate_values(#'connection.tune'{channel_max = ServerChannelMax,
                                    frame_max   = ServerFrameMax,
                                    heartbeat   = ServerHeartbeat},
                 #amqp_params{channel_max = ClientChannelMax,
                              frame_max   = ClientFrameMax,
                              heartbeat   = ClientHeartbeat}) ->
    #'connection.tune_ok'{
        channel_max = negotiate_max_value(ClientChannelMax, ServerChannelMax),
        frame_max   = negotiate_max_value(ClientFrameMax, ServerFrameMax),
        heartbeat   = negotiate_max_value(ClientHeartbeat, ServerHeartbeat)}.

negotiate_max_value(Client, Server) when Client =:= 0; Server =:= 0 ->
    lists:max([Client, Server]);
negotiate_max_value(Client, Server) ->
    lists:min([Client, Server]).

start_ok(#state{params = #amqp_params{username          = Username,
                                      password          = Password,
                                      client_properties = UserProps}}) ->
    LoginTable = [{<<"LOGIN">>, longstr, Username},
                  {<<"PASSWORD">>, longstr, Password}],
    #'connection.start_ok'{
        client_properties = client_properties(UserProps),
        mechanism = <<"AMQPLAIN">>,
        response = rabbit_binary_generator:generate_table(LoginTable)}.

client_properties(UserProperties) ->
    %% TODO This eagerly starts the amqp_client application in order to
    %% to get the version from the app descriptor, which may be
    %% overkill - maybe there is a more suitable point to boot the app
    rabbit_misc:start_applications([amqp_client]),
    {ok, Vsn} = application:get_key(amqp_client, vsn),
    Default = [{<<"product">>,   longstr, <<"RabbitMQ">>},
               {<<"version">>,   longstr, list_to_binary(Vsn)},
               {<<"platform">>,  longstr, <<"Erlang">>},
               {<<"copyright">>, longstr,
                <<"Copyright (C) 2007-2009 LShift Ltd., "
                  "Cohesive Financial Technologies LLC., "
                  "and Rabbit Technologies Ltd.">>},
               {<<"information">>, longstr,
                <<"Licensed under the MPL.  "
                  "See http://www.rabbitmq.com/">>}],
    lists:foldl(fun({K, _, _} = Tuple, Acc) ->
                    lists:keystore(K, 1, Acc, Tuple)
                end, Default, UserProperties).

handshake_recv() ->
    receive
        {'$gen_cast', {method, Method, _Content}} ->
            Method;
        socket_closed ->
            exit(socket_closed_unexpectedly)
    after ?HANDSHAKE_RECEIVE_TIMEOUT ->
        exit(handshake_receive_timed_out)
    end.
