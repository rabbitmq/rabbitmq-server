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

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-define(RABBIT_TCP_OPTS, [binary, {packet, 0}, {active,false}, {nodelay, true}]).
-define(SOCKET_CLOSING_TIMEOUT, 1000).
-define(CLIENT_CLOSE_TIMEOUT, 60000).
-define(HANDSHAKE_RECEIVE_TIMEOUT, 60000).

-record(nc_state, {params = #amqp_params{},
                   sock,
                   main_reader_pid,
                   channel0_writer_pid,
                   channel0_framing_pid,
                   max_channel,
                   heartbeat,
                   closing = false,
                   server_properties,
                   channels = amqp_channel_util:new_channel_dict()}).

-record(nc_closing, {reason,
                     close,
                     from = none,
                     phase = terminate_channels}).

-define(INFO_KEYS,
        (amqp_connection:info_keys() ++ [max_channel, heartbeat])).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init(AmqpParams) ->
    process_flag(trap_exit, true),
    State0 = handshake(#nc_state{params = AmqpParams}),
    {ok, State0}.

%% Standard handling of an app initiated command
handle_call({command, Command}, From, #nc_state{closing = Closing} = State) ->
    case Closing of
        false -> handle_command(Command, From, State);
        _     -> {reply, closing, State}
    end;

handle_call({info, Items}, _From, State) ->
    {reply, [{Item, i(Item, State)} || Item <- Items], State};

handle_call(info_keys, _From, State) ->
    {reply, ?INFO_KEYS, State}.

%% Standard handling of a method sent by the broker (this is received from
%% framing0)
handle_cast({method, Method, Content}, State) ->
    handle_method(Method, Content, State).

%% This is received after we have sent 'connection.close' to the server
%% but timed out waiting for 'connection.close_ok' back
handle_info(timeout_waiting_for_close_ok = Msg,
            State = #nc_state{closing = Closing}) ->
    ?LOG_WARN("Connection ~p closing: timed out waiting for"
              "'connection.close_ok'.", [self()]),
    {stop, {Msg, closing_to_reason(Closing)}, State};

%% Standard handling of exit signals
handle_info({'EXIT', Pid, Reason}, State) ->
    handle_exit(Pid, Reason, State).

terminate(_Reason, #nc_state{channel0_framing_pid = Framing0Pid,
                             channel0_writer_pid = Writer0Pid,
                             main_reader_pid = MainReaderPid}) ->
    ok = amqp_channel_util:terminate_channel_infrastructure(
                 network, {Framing0Pid, Writer0Pid}),
    case MainReaderPid of
        undefined -> ok;
        _         -> MainReaderPid ! close,
                     ok
    end.

code_change(_OldVsn, State, _Extra) ->
    State.

%%---------------------------------------------------------------------------
%% Command handling
%%---------------------------------------------------------------------------

handle_command({open_channel, ProposedNumber}, _From,
               State = #nc_state{sock = Sock,
                                 main_reader_pid = MainReader,
                                 channels = Channels,
                                 max_channel = MaxChannel}) ->
    try amqp_channel_util:open_channel(ProposedNumber, MaxChannel, network,
                                       {Sock, MainReader}, Channels) of
        {ChannelPid, NewChannels} ->
            {reply, ChannelPid, State#nc_state{channels = NewChannels}}
    catch
        error:out_of_channel_numbers = Error ->
            {reply, {Error, MaxChannel}, State}
    end;

handle_command({close, #'connection.close'{} = Close}, From, State) ->
    {noreply, set_closing_state(flush, #nc_closing{reason = app_initiated_close,
                                                   close = Close,
                                                   from = From},
                                State)}.

%%---------------------------------------------------------------------------
%% Handling methods from broker
%%---------------------------------------------------------------------------

handle_method(#'connection.close'{} = Close, none, State) ->
    {noreply, set_closing_state(abrupt,
                                #nc_closing{reason = server_initiated_close,
                                            close = Close},
                                State)};

handle_method(#'connection.close_ok'{}, none,
              State = #nc_state{closing = Closing}) ->
    #nc_closing{from = From,
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

i(server_properties, State) -> State#nc_state.server_properties;
i(is_closing,        State) -> State#nc_state.closing =/= false;
i(amqp_params,       State) -> State#nc_state.params;
i(max_channel,       State) -> State#nc_state.max_channel;
i(heartbeat,         State) -> State#nc_state.heartbeat;
i(num_channels,      State) -> amqp_channel_util:num_channels(
                                   State#nc_state.channels);
i(Item,             _State) -> throw({bad_argument, Item}).

%%---------------------------------------------------------------------------
%% Closing
%%---------------------------------------------------------------------------

%% Changes connection's state to closing.
%%
%% ChannelCloseType can be flush or abrupt
%%
%% The closing reason (Closing#nc_closing.reason) can be one of the following
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
                  #nc_state{closing = false,
                            channels = Channels} = State) ->
    amqp_channel_util:broadcast_to_channels(
        {connection_closing, ChannelCloseType, closing_to_reason(Closing)},
        Channels),
    check_trigger_all_channels_closed_event(State#nc_state{closing = Closing});
%% Already closing, override situation
set_closing_state(ChannelCloseType, NewClosing,
                  #nc_state{closing = CurClosing,
                            channels = Channels} = State) ->
    %% Do not override reason in channels (because it might cause channels to
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
   #nc_closing{reason = NewReason, close = NewClose} = NewClosing,
   #nc_closing{reason = CurReason} = CurClosing,
   ResClosing =
       if
           %% Override (rely on erlang's comparison of atoms)
           NewReason >= CurReason ->
               %% Note that when overriding, we keep the current phase
               CurClosing#nc_closing{reason = NewReason, close = NewClose};
           %% Do not override
           true ->
               CurClosing
       end,
    NewState = State#nc_state{closing = ResClosing},
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
all_channels_closed_event(#nc_state{channel0_writer_pid = Writer0,
                                    main_reader_pid = MainReader,
                                    closing = Closing} = State) ->
    #nc_closing{reason = Reason, close = Close} = Closing,
    case Reason of
        server_initiated_close ->
            amqp_channel_util:do(network, Writer0, #'connection.close_ok'{},
                                 none),
            erlang:send_after(?SOCKET_CLOSING_TIMEOUT, MainReader,
                              socket_closing_timeout),
            State#nc_state{closing =
                Closing#nc_closing{phase = wait_socket_close}};
        _ ->
            amqp_channel_util:do(network, Writer0, Close, none),
            erlang:send_after(?CLIENT_CLOSE_TIMEOUT, self(),
                              timeout_waiting_for_close_ok),
            State#nc_state{closing = Closing#nc_closing{phase = wait_close_ok}}
    end.

closing_to_reason(#nc_closing{reason = Reason,
                              close = #'connection.close'{reply_code = Code,
                                                          reply_text = Text}}) ->
    {Reason, Code, Text}.

internal_error_closing() ->
    #nc_closing{reason = internal_error,
                close = #'connection.close'{reply_text = <<>>,
                                            reply_code = ?INTERNAL_ERROR,
                                            class_id = 0,
                                            method_id = 0}}.

%%---------------------------------------------------------------------------
%% Channel utilities
%%---------------------------------------------------------------------------

unregister_channel(Pid, State = #nc_state{channels = Channels}) ->
    NewChannels = amqp_channel_util:unregister_channel_pid(Pid, Channels),
    NewState = State#nc_state{channels = NewChannels},
    check_trigger_all_channels_closed_event(NewState).

check_trigger_all_channels_closed_event(#nc_state{closing = false} = State) ->
    State;
check_trigger_all_channels_closed_event(#nc_state{channels = Channels,
                                                  closing = Closing} = State) ->
    #nc_closing{phase = terminate_channels} = Closing, % assertion
    case amqp_channel_util:is_channel_dict_empty(Channels) of
        true  -> all_channels_closed_event(State);
        false -> State
    end.

%%---------------------------------------------------------------------------
%% Trap exits
%%---------------------------------------------------------------------------

%% Handle exit from writer0
handle_exit(Writer0Pid, Reason,
            #nc_state{channel0_writer_pid = Writer0Pid} = State) ->
    ?LOG_WARN("Connection (~p) closing: received exit signal from writer0. "
              "Reason: ~p~n", [self(), Reason]),
    {stop, {writer0_died, Reason}, State};

%% Handle exit from framing0
handle_exit(Framing0Pid, Reason,
            #nc_state{channel0_framing_pid = Framing0Pid} = State) ->
    ?LOG_WARN("Connection (~p) closing: received exit signal from framing0. "
              "Reason: ~p~n", [self(), Reason]),
    {stop, {framing0_died, Reason}, State};

%% Handle exit from main reader
handle_exit(MainReaderPid, Reason,
            #nc_state{main_reader_pid = MainReaderPid,
                      closing = Closing} = State) ->
    case {Closing, Reason} of
        %% Normal server initiated shutdown exit (socket has been closed after
        %% replying with 'connection.close_ok')
        {#nc_closing{reason = server_initiated_close,
                     phase = wait_socket_close},
         socket_closed} ->
            {stop, closing_to_reason(Closing), State};
        %% Timed out waiting for socket to close after replying with
        %% 'connection.close_ok'
        {#nc_closing{reason = server_initiated_close,
                     phase = wait_socket_close},
         socket_closing_timeout} ->
            ?LOG_WARN("Connection (~p) closing: timed out waiting for socket "
                      "to close after sending 'connection.close_ok", [self()]),
            {stop, {Reason, closing_to_reason(Closing)}, State};
        %% Main reader died
        _ ->
            ?LOG_WARN("Connection (~p) closing: received exit signal from main "
                      "reader. Reason: ~p~n", [self(), Reason]),
            {stop, {main_reader_died, Reason}, State}
    end;

%% Handle exit from channel or other pid
handle_exit(Pid, Reason,
            #nc_state{channels = Channels, closing = Closing} = State) ->
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

handshake(State = #nc_state{params = #amqp_params{host = Host,
                                                  port = Port,
                                                  ssl_options = none}}) ->
    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Sock}      -> do_handshake(State#nc_state{sock = Sock});
        {error, Reason} -> ?LOG_WARN("Could not start the network driver: ~p~n",
                                     [Reason]),
                           exit(Reason)
    end;
handshake(State = #nc_state{params = #amqp_params{host = Host,
                                                  port = Port,
                                                  ssl_options = SslOpts}}) ->
    rabbit_misc:start_applications([crypto, ssl]),
    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Sock} ->
            case ssl:connect(Sock, SslOpts) of
                {ok, SslSock} ->
                    RabbitSslSock = #ssl_socket{ssl = SslSock, tcp = Sock},
                    do_handshake(State#nc_state{sock = RabbitSslSock});
                {error, Reason} ->
                    ?LOG_WARN("Could not upgrade the network driver to ssl: "
                              "~p~n", [Reason]),
                    exit(Reason)
            end;
        {error, Reason} ->
            ?LOG_WARN("Could not start the network driver: ~p~n", [Reason]),
            exit(Reason)
    end.

do_handshake(State0 = #nc_state{sock = Sock}) ->
    ok = rabbit_net:send(Sock, ?PROTOCOL_HEADER),
    {Framing0Pid, Writer0Pid} =
        amqp_channel_util:start_channel_infrastructure(network, 0, {Sock, none}),
    MainReaderPid = amqp_main_reader:start(Sock, Framing0Pid),
    State1 = State0#nc_state{channel0_framing_pid = Framing0Pid,
                             channel0_writer_pid = Writer0Pid,
                             main_reader_pid = MainReaderPid},
    State2 = network_handshake(State1),
    MainReaderPid ! {heartbeat, State2#nc_state.heartbeat},
    State2.

network_handshake(State = #nc_state{channel0_writer_pid = Writer0,
                                    params = Params}) ->
    Start = handshake_recv(State),
    #'connection.start'{server_properties = ServerProperties} = Start,
    ok = check_version(Start),
    amqp_channel_util:do(network, Writer0, start_ok(State), none),
    Tune = handshake_recv(State),
    TuneOk = negotiate_values(Tune, Params),
    amqp_channel_util:do(network, Writer0, TuneOk, none),
    ConnectionOpen =
        #'connection.open'{virtual_host = Params#amqp_params.virtual_host,
                           insist = true},
    amqp_channel_util:do(network, Writer0, ConnectionOpen, none),
    %% 'connection.redirect' not implemented (we use insist = true to cover)
    #'connection.open_ok'{} = handshake_recv(State),
    #'connection.tune_ok'{channel_max = ChannelMax,
                          frame_max   = FrameMax,
                          heartbeat   = Heartbeat} = TuneOk,
    ?LOG_INFO("Negotiated maximums: (Channel = ~p, "
              "Frame= ~p, Heartbeat=~p)~n",
             [ChannelMax, FrameMax, Heartbeat]),
    State#nc_state{max_channel = ChannelMax,
                   heartbeat = Heartbeat,
                   server_properties = ServerProperties}.

check_version(#'connection.start'{version_major = ?PROTOCOL_VERSION_MAJOR,
                                  version_minor = ?PROTOCOL_VERSION_MINOR}) ->
    ok;
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

start_ok(#nc_state{params = #amqp_params{username = Username,
                                         password = Password,
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

handshake_recv(#nc_state{main_reader_pid = MainReaderPid}) ->
    receive
        {'$gen_cast', {method, Method, _Content}} ->
            Method;
        {'EXIT', MainReaderPid, Reason} ->
            exit({main_reader_died, Reason})
    after ?HANDSHAKE_RECEIVE_TIMEOUT ->
        exit(awaiting_response_from_server_timed_out)
    end.
