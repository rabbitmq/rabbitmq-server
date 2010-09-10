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
-module(amqp_direct_connection).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/3, connect/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {sup,
                params = #amqp_params{},
                collector,
                closing = false,
                server_properties,
                channel_sup_sup,
                channels = amqp_channel_util:new_channel_dict(),
                start_infrastructure_fun}).

-record(closing, {reason,
                  close = none, %% At least one of close and reply has to be
                  reply = none, %%     none at any given moment
                  from  = none}).

-define(INFO_KEYS,
        (amqp_connection:info_keys() ++ [])).

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

handle_call({command, Command}, From, #state{closing = Closing} = State) ->
    case Closing of
        false -> handle_command(Command, From, State);
        _     -> {reply, closing, State}
    end;
handle_call({info, Items}, _From, State) ->
    {reply, [{Item, i(Item, State)} || Item <- Items], State};
handle_call(info_keys, _From, State) ->
    {reply, ?INFO_KEYS, State};
handle_call(connect, _From, State) ->
    {reply, ok, do_connect(State)}.

handle_cast(Message, State) ->
    ?LOG_WARN("Connection (~p) closing: received unexpected cast ~p~n",
              [self(), Message]),
    {noreply, set_closing_state(abrupt, internal_error_closing(), State)}.

handle_info({shutdown, Reason}, State) ->
    {_, Code, _} = Reason,
    if Code =:= 200 -> {stop, normal, State};
       true         -> {stop, Reason, State}
    end;
handle_info({'DOWN', _, process, Pid, Reason}, State) ->
    handle_channel_exit(Pid, Reason, State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

%%---------------------------------------------------------------------------
%% Command handling
%%---------------------------------------------------------------------------

handle_command({open_channel, ProposedNumber}, _From, State =
                   #state{collector       = Collector,
                          channel_sup_sup = ChSupSup,
                          params          = #amqp_params{username     = User,
                                                         virtual_host = VHost},
                          channels        = Channels}) ->
    try amqp_channel_util:open_channel(ChSupSup, ProposedNumber,
                                       ?MAX_CHANNEL_NUMBER,
                                       [User, VHost, Collector], Channels) of
        {ChannelPid, NewChannels} ->
            {reply, {ok, ChannelPid}, State#state{channels = NewChannels}}
    catch
        error:out_of_channel_numbers = Error ->
            {reply, {error, {Error, ?MAX_CHANNEL_NUMBER}}, State}
    end;

handle_command({close, Close}, From, State) ->
    {noreply, set_closing_state(flush, #closing{reason = app_initiated_close,
                                                close  = Close,
                                                from   = From},
                                State)}.

%%---------------------------------------------------------------------------
%% Infos
%%---------------------------------------------------------------------------

i(server_properties, State) -> State#state.server_properties;
i(is_closing,        State) -> State#state.closing =/= false;
i(amqp_params,       State) -> State#state.params;
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
%% The precedence of the closing MainReason's is as follows:
%%     app_initiated_close, internal_error, server_initiated_close
%% (i.e.: a given reason can override the currently set one if it is later
%% mentioned in the above list). We can rely on erlang's comparison of atoms
%% for this.
set_closing_state(ChannelCloseType, Closing,
                  State = #state{closing = false, channels = Channels}) ->
    amqp_channel_util:broadcast_to_channels(
        {connection_closing, ChannelCloseType, closing_to_reason(Closing)},
        Channels),
    check_trigger_all_channels_closed_event(State#state{closing = Closing});
%% Already closing, override situation
set_closing_state(ChannelCloseType, NewClosing,
                  State = #state{closing = CurClosing, channels = Channels}) ->
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
   ResClosing =
       if
           %% Override (rely on erlang's comparison of atoms)
           NewClosing#closing.reason >= CurClosing#closing.reason ->
               NewClosing;
           %% Do not override
           true ->
               CurClosing
       end,
   State#state{closing = ResClosing}.

%% The all_channels_closed_event is called when all channels have been closed
%% after the connection broadcasts a connection_closing message to all channels
all_channels_closed_event(State = #state{closing   = Closing,
                                         collector = Collector}) ->
    rabbit_queue_collector:delete_all(Collector),
    case Closing#closing.from of
        none -> ok;
        From -> gen_server:reply(From, ok)
    end,
    self() ! {shutdown, closing_to_reason(Closing)},
    State.

closing_to_reason(#closing{reason = Reason,
                           close  = #'connection.close'{reply_code = Code,
                                                        reply_text = Text},
                           reply  = none}) ->
    {Reason, Code, Text};
closing_to_reason(#closing{reason = Reason,
                           reply  = {_, Code, Text},
                           close  = none}) ->
    {Reason, Code, Text}.

internal_error_closing() ->
    #closing{reason = internal_error,
             reply  = {internal_error, ?INTERNAL_ERROR, <<>>}}.

%%---------------------------------------------------------------------------
%% Channel utilities
%%---------------------------------------------------------------------------

unregister_channel(Pid, State = #state{channels = Channels}) ->
    NewChannels = amqp_channel_util:unregister_channel_pid(Pid, Channels),
    NewState = State#state{channels = NewChannels},
    check_trigger_all_channels_closed_event(NewState).

check_trigger_all_channels_closed_event(State = #state{closing = false}) ->
    State;
check_trigger_all_channels_closed_event(State = #state{channels = Channels}) ->
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
%% Connecting to the broker
%%---------------------------------------------------------------------------

do_connect(State0 = #state{params = #amqp_params{username = User,
                                                 password = Pass,
                                                 virtual_host = VHost}}) ->
    case lists:keymember(rabbit, 1, application:which_applications()) of
        true  -> ok;
        false -> exit(broker_not_found_in_vm)
    end,
    rabbit_access_control:user_pass_login(User, Pass),
    rabbit_access_control:check_vhost_access(
            #user{username = User, password = Pass}, VHost),
    State1 = start_infrastructure(State0),
    State1#state{server_properties = rabbit_reader:server_properties()}.

start_infrastructure(State = #state{start_infrastructure_fun = SIF}) ->
    {ok, Collector} = SIF(),
    State#state{collector = Collector}.
