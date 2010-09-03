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

-export([start_link/2, connect/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {sup,
                params = #amqp_params{},
                collector,
                channels_manager,
                closing = false,
                server_properties,
                start_infrastructure_fun}).

-record(closing, {reason,
                  close = none, %% At least one of close and reply has to be
                  reply = none, %%     none at any given moment
                  from = none}).

-define(INFO_KEYS,
        (amqp_connection:info_keys() ++ [])).

%%---------------------------------------------------------------------------
%% Internal interface
%%---------------------------------------------------------------------------

start_link(AmqpParams, SIF) ->
    gen_server:start_link(?MODULE, [self(), AmqpParams, SIF], []).

connect(Pid) ->
    gen_server:call(Pid, connect, infinity).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init([Sup, AmqpParams, SIF]) ->
    {ok, #state{sup = Sup,
                params = AmqpParams,
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

handle_info({hard_error_in_channel, Pid, Reason}, State) ->
    ?LOG_WARN("Connection (~p) closing: channel (~p) received hard error ~p "
              "from server~n", [self(), Pid, Reason]),
    {stop, Reason, State};
handle_info({channel_internal_error, _Pid, _Reason}, State) ->
    {noreply, set_closing_state(abrupt, internal_error_closing(), State)};
handle_info(all_channels_terminated, State) ->
    handle_all_channels_terminated(State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

%%---------------------------------------------------------------------------
%% Command handling
%%---------------------------------------------------------------------------

handle_command({open_channel, ProposedNumber}, _From,
               State = #state{collector = Collector,
                              channels_manager = ChMgr,
                              params = #amqp_params{username = User,
                                                    virtual_host = VHost}}) ->
    {reply, amqp_channels_manager:open_channel(ChMgr, ProposedNumber,
                                               [User, VHost, Collector]),
     State};
handle_command({close, Close}, From, State) ->
    {noreply, set_closing_state(flush, #closing{reason = app_initiated_close,
                                                close = Close,
                                                from = From},
                                State)}.

%%---------------------------------------------------------------------------
%% Infos
%%---------------------------------------------------------------------------

i(server_properties, State) -> State#state.server_properties;
i(is_closing,        State) -> State#state.closing =/= false;
i(amqp_params,       State) -> State#state.params;
i(num_channels,      State) ->
    amqp_channels_manager:num_channels(State#state.channels_manager);
i(Item, _State) ->
    throw({bad_argument, Item}).

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
set_closing_state(ChannelCloseType, Closing, State = #state{closing = false}) ->
    NewState = State#state{closing = Closing},
    signal_connection_closing(ChannelCloseType, NewState),
    NewState;
%% Already closing, override situation
set_closing_state(ChannelCloseType, NewClosing,
                  State = #state{closing = CurClosing}) ->
    ResClosing =
        if
            %% Override (rely on erlang's comparison of atoms)
            NewClosing#closing.reason >= CurClosing#closing.reason ->
                NewClosing;
            %% Do not override
            true ->
                CurClosing
        end,
    NewState = State#state{closing = ResClosing},
    %% Do not override reason in channels (because it might cause channels to
    %% to exit with different reasons) but do cause them to close abruptly
    %% if the new closing type requires it
    case ChannelCloseType of
        abrupt -> signal_connection_closing(abrupt, NewState);
        _      -> ok
    end,
    NewState.

signal_connection_closing(ChannelCloseType, #state{channels_manager = ChMgr,
                                                   closing = Closing}) ->
    amqp_channels_manager:signal_connection_closing(ChMgr, ChannelCloseType,
                                                    closing_to_reason(Closing)).

handle_all_channels_terminated(State = #state{closing = Closing,
                                              collector = Collector}) ->
    #state{closing = #closing{}} = State, % assertion
    rabbit_queue_collector:delete_all(Collector),
    case Closing#closing.from of none -> ok;
                                 From -> gen_server:reply(From, ok)
    end,
    {stop, closing_to_reason(Closing), State}.

closing_to_reason(#closing{close = #'connection.close'{reply_code = 200},
                           reply = none}) ->
    normal;
closing_to_reason(#closing{reason = Reason,
                           close = #'connection.close'{reply_code = Code,
                                                       reply_text = Text},
                           reply = none}) ->
    {Reason, Code, Text};
closing_to_reason(#closing{reply = {_, 200, _}, close = none}) ->
    normal;
closing_to_reason(#closing{reason = Reason,
                           reply = {_, Code, Text},
                           close = none}) ->
    {Reason, Code, Text}.

internal_error_closing() ->
    #closing{reason = internal_error,
             reply = {internal_error, ?INTERNAL_ERROR, <<>>}}.

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
    ServerProperties = rabbit_reader:server_properties(),
    State1#state{server_properties = ServerProperties}.

start_infrastructure(State = #state{start_infrastructure_fun = SIF}) ->
    {ChMgr, Collector} = SIF(),
    State#state{channels_manager = ChMgr, collector = Collector}.
