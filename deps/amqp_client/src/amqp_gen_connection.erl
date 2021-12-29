%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private
-module(amqp_gen_connection).

-include("amqp_client_internal.hrl").

-behaviour(gen_server).

-export([start_link/2, connect/1, open_channel/3, hard_error_in_channel/3,
         channel_internal_error/3, server_misbehaved/2, channels_terminated/1,
         close/3, server_close/2, info/2, info_keys/0, info_keys/1,
         register_blocked_handler/2, update_secret/2]).
-export([behaviour_info/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-define(INFO_KEYS, [server_properties, is_closing, amqp_params, num_channels,
                    channel_max]).

-record(state, {module,
                module_state,
                channels_manager,
                amqp_params,
                channel_max,
                server_properties,
                %% connection.block, connection.unblock handler
                block_handler,
                closing = false %% #closing{} | false
               }).

-record(closing, {reason,
                  close,
                  from = none}).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(TypeSup, AMQPParams) ->
    gen_server:start_link(?MODULE, {TypeSup, AMQPParams}, []).

connect(Pid) ->
    gen_server:call(Pid, connect, amqp_util:call_timeout()).

open_channel(Pid, ProposedNumber, Consumer) ->
    try gen_server:call(Pid,
                         {command, {open_channel, ProposedNumber, Consumer}},
                         amqp_util:call_timeout()) of
        {ok, ChannelPid} -> ok = amqp_channel:open(ChannelPid),
                            {ok, ChannelPid};
        Error            -> Error
    catch
        _:Reason         -> {error, Reason}
    end.

hard_error_in_channel(Pid, ChannelPid, Reason) ->
    gen_server:cast(Pid, {hard_error_in_channel, ChannelPid, Reason}).

channel_internal_error(Pid, ChannelPid, Reason) ->
    gen_server:cast(Pid, {channel_internal_error, ChannelPid, Reason}).

server_misbehaved(Pid, AmqpError) ->
    gen_server:cast(Pid, {server_misbehaved, AmqpError}).

channels_terminated(Pid) ->
    gen_server:cast(Pid, channels_terminated).

close(Pid, Close, Timeout) ->
    gen_server:call(Pid, {command, {close, Close, Timeout}}, amqp_util:call_timeout()).

server_close(Pid, Close) ->
    gen_server:cast(Pid, {server_close, Close}).

update_secret(Pid, Method) ->
    gen_server:call(Pid, {command, {update_secret, Method}}, amqp_util:call_timeout()).

info(Pid, Items) ->
    gen_server:call(Pid, {info, Items}, amqp_util:call_timeout()).

info_keys() ->
    ?INFO_KEYS.

info_keys(Pid) ->
    gen_server:call(Pid, info_keys, amqp_util:call_timeout()).

%%---------------------------------------------------------------------------
%% Behaviour
%%---------------------------------------------------------------------------

behaviour_info(callbacks) ->
    [
     %% init() -> {ok, InitialState}
     {init, 0},

     %% terminate(Reason, FinalState) -> Ignored
     {terminate, 2},

     %% connect(AmqpParams, SIF, TypeSup, State) ->
     %%     {ok, ConnectParams} | {closing, ConnectParams, AmqpError, Reply} |
     %%         {error, Error}
     %% where
     %%     ConnectParams = {ServerProperties, ChannelMax, ChMgr, NewState}
     {connect, 4},

     %% do(Method, State) -> Ignored
     {do, 2},

     %% open_channel_args(State) -> OpenChannelArgs
     {open_channel_args, 1},

      %% i(InfoItem, State) -> Info
     {i, 2},

     %% info_keys() -> [InfoItem]
     {info_keys, 0},

     %% CallbackReply = {ok, NewState} | {stop, Reason, FinalState}

     %% handle_message(Message, State) -> CallbackReply
     {handle_message, 2},

     %% closing(flush|abrupt, Reason, State) -> CallbackReply
     {closing, 3},

     %% channels_terminated(State) -> CallbackReply
     {channels_terminated, 1}
    ];
behaviour_info(_Other) ->
    undefined.

callback(Function, Params, State = #state{module = Mod,
                                          module_state = MState}) ->
    case erlang:apply(Mod, Function, Params ++ [MState]) of
        {ok, NewMState}           -> {noreply,
                                      State#state{module_state = NewMState}};
        {stop, Reason, NewMState} -> {stop, Reason,
                                      State#state{module_state = NewMState}}
    end.

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init({TypeSup, AMQPParams}) ->
    %% Trapping exits since we need to make sure that the `terminate/2' is
    %% called in the case of direct connection (it does not matter for a network
    %% connection).  See bug25116.
    process_flag(trap_exit, true),
    %% connect() has to be called first, so we can use a special state here
    {ok, {TypeSup, AMQPParams}}.

handle_call(connect, _From, {TypeSup, AMQPParams}) ->
    {Type, Mod} = amqp_connection_type_sup:type_module(AMQPParams),
    {ok, MState} = Mod:init(),
    SIF = amqp_connection_type_sup:start_infrastructure_fun(
            TypeSup, self(), Type),
    State = #state{module           = Mod,
                   module_state     = MState,
                   amqp_params      = AMQPParams,
                   block_handler    = none},
    case Mod:connect(AMQPParams, SIF, TypeSup, MState) of
        {ok, Params} ->
            {reply, {ok, self()}, after_connect(Params, State)};
        {closing, #amqp_error{name = access_refused} = AmqpError, Error} ->
            {stop, {shutdown, AmqpError}, Error, State};
        {closing, Params, #amqp_error{} = AmqpError, Error} ->
            server_misbehaved(self(), AmqpError),
            {reply, Error, after_connect(Params, State)};
        {error, _} = Error ->
            {stop, {shutdown, Error}, Error, State}
    end;
handle_call({command, Command}, From, State = #state{closing = false}) ->
    handle_command(Command, From, State);
handle_call({command, _Command}, _From, State) ->
    {reply, closing, State};
handle_call({info, Items}, _From, State) ->
    {reply, [{Item, i(Item, State)} || Item <- Items], State};
handle_call(info_keys, _From, State = #state{module = Mod}) ->
    {reply, ?INFO_KEYS ++ Mod:info_keys(), State}.

after_connect({ServerProperties, ChannelMax, ChMgr, NewMState}, State) ->
    case ChannelMax of
        0 -> ok;
        _ -> amqp_channels_manager:set_channel_max(ChMgr, ChannelMax)
    end,
    State1 = State#state{server_properties = ServerProperties,
                         channel_max       = ChannelMax,
                         channels_manager  = ChMgr,
                         module_state      = NewMState},
    rabbit_misc:store_proc_name(?MODULE, i(name, State1)),
    State1.

handle_cast({method, Method, none, noflow}, State) ->
    handle_method(Method, State);
handle_cast(channels_terminated, State) ->
    handle_channels_terminated(State);
handle_cast({hard_error_in_channel, _Pid, Reason}, State) ->
    server_initiated_close(Reason, State);
handle_cast({channel_internal_error, Pid, Reason}, State) ->
    ?LOG_WARN("Connection (~p) closing: internal error in channel (~p): ~p",
              [self(), Pid, Reason]),
    internal_error(Pid, Reason, State);
handle_cast({server_misbehaved, AmqpError}, State) ->
    server_misbehaved_close(AmqpError, State);
handle_cast({server_close, #'connection.close'{} = Close}, State) ->
    server_initiated_close(Close, State);
handle_cast({register_blocked_handler, HandlerPid}, State) ->
    Ref = erlang:monitor(process, HandlerPid),
    {noreply, State#state{block_handler = {HandlerPid, Ref}}}.

%% @private
handle_info({'DOWN', _, process, BlockHandler, Reason},
            State = #state{block_handler = {BlockHandler, _Ref}}) ->
    ?LOG_WARN("Connection (~p): Unregistering connection.{blocked,unblocked} handler ~p because it died. "
              "Reason: ~p", [self(), BlockHandler, Reason]),
    {noreply, State#state{block_handler = none}};
handle_info({'EXIT', BlockHandler, Reason},
            State = #state{block_handler = {BlockHandler, Ref}}) ->
    ?LOG_WARN("Connection (~p): Unregistering connection.{blocked,unblocked} handler ~p because it died. "
              "Reason: ~p", [self(), BlockHandler, Reason]),
    erlang:demonitor(Ref, [flush]),
    {noreply, State#state{block_handler = none}};
%% propagate the exit to the module that will stop with a sensible reason logged
handle_info({'EXIT', _Pid, _Reason} = Info, State) ->
    callback(handle_message, [Info], State);
handle_info(Info, State) ->
    callback(handle_message, [Info], State).

terminate(Reason, #state{module = Mod, module_state = MState}) ->
    Mod:terminate(Reason, MState).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------------------------------------------------------
%% Infos
%%---------------------------------------------------------------------------

i(server_properties, State) -> State#state.server_properties;
i(is_closing,        State) -> State#state.closing =/= false;
i(amqp_params,       State) -> State#state.amqp_params;
i(channel_max,       State) -> State#state.channel_max;
i(num_channels,      State) -> amqp_channels_manager:num_channels(
                                 State#state.channels_manager);
i(Item, #state{module = Mod, module_state = MState}) -> Mod:i(Item, MState).

%%---------------------------------------------------------------------------
%% connection.blocked, connection.unblocked
%%---------------------------------------------------------------------------

register_blocked_handler(Pid, HandlerPid) ->
    gen_server:cast(Pid, {register_blocked_handler, HandlerPid}).

%%---------------------------------------------------------------------------
%% Command handling
%%---------------------------------------------------------------------------

handle_command({open_channel, ProposedNumber, Consumer}, _From,
               State = #state{channels_manager = ChMgr,
                              module = Mod,
                              module_state = MState}) ->
    {reply, amqp_channels_manager:open_channel(ChMgr, ProposedNumber, Consumer,
                                               Mod:open_channel_args(MState)),
     State};
handle_command({close, #'connection.close'{} = Close, Timeout}, From, State) ->
    app_initiated_close(Close, From, Timeout, State);
handle_command({update_secret, #'connection.update_secret'{} = Method}, _From,
               State = #state{module = Mod,
                              module_state = MState}) ->
    {reply, Mod:do(Method, MState), State}.

%%---------------------------------------------------------------------------
%% Handling methods from broker
%%---------------------------------------------------------------------------

handle_method(#'connection.close'{} = Close, State) ->
    server_initiated_close(Close, State);
handle_method(#'connection.close_ok'{}, State = #state{closing = Closing}) ->
    case Closing of #closing{from = none} -> ok;
                    #closing{from = From} -> gen_server:reply(From, ok)
    end,
    {stop, {shutdown, closing_to_reason(Closing)}, State};
handle_method(#'connection.blocked'{} = Blocked, State = #state{block_handler = BlockHandler}) ->
    case BlockHandler of none        -> ok;
                         {Pid, _Ref} -> Pid ! Blocked
    end,
    {noreply, State};
handle_method(#'connection.unblocked'{} = Unblocked, State = #state{block_handler = BlockHandler}) ->
    case BlockHandler of none        -> ok;
                         {Pid, _Ref} -> Pid ! Unblocked
    end,
    {noreply, State};
handle_method(#'connection.update_secret_ok'{} = _Method, State) ->
    {noreply, State};
handle_method(Other, State) ->
    server_misbehaved_close(#amqp_error{name        = command_invalid,
                                        explanation = "unexpected method on "
                                                      "channel 0",
                                        method      = element(1, Other)},
                            State).

%%---------------------------------------------------------------------------
%% Closing
%%---------------------------------------------------------------------------

app_initiated_close(Close, From, Timeout, State) ->
    case Timeout of
        infinity -> ok;
        _        -> erlang:send_after(Timeout, self(), closing_timeout)
    end,
    set_closing_state(flush, #closing{reason = app_initiated_close,
                                      close = Close,
                                      from = From}, State).

internal_error(Pid, Reason, State) ->
    Str = list_to_binary(rabbit_misc:format("~p:~p", [Pid, Reason])),
    Close = #'connection.close'{reply_text = Str,
                                reply_code = ?INTERNAL_ERROR,
                                class_id = 0,
                                method_id = 0},
    set_closing_state(abrupt, #closing{reason = internal_error, close = Close},
                      State).

server_initiated_close(Close, State) ->
    ?LOG_WARN("Connection (~p) closing: received hard error ~p "
              "from server", [self(), Close]),
    set_closing_state(abrupt, #closing{reason = server_initiated_close,
                                       close = Close}, State).

server_misbehaved_close(AmqpError, State) ->
    ?LOG_WARN("Connection (~p) closing: server misbehaved: ~p",
              [self(), AmqpError]),
    {0, Close} = rabbit_binary_generator:map_exception(0, AmqpError, ?PROTOCOL),
    set_closing_state(abrupt, #closing{reason = server_misbehaved,
                                       close = Close}, State).

set_closing_state(ChannelCloseType, NewClosing,
                  State = #state{channels_manager = ChMgr,
                                 closing = CurClosing}) ->
    ResClosing =
        case closing_priority(NewClosing) =< closing_priority(CurClosing) of
            true  -> NewClosing;
            false -> CurClosing
        end,
    ClosingReason = closing_to_reason(ResClosing),
    amqp_channels_manager:signal_connection_closing(ChMgr, ChannelCloseType,
                                                    ClosingReason),
    callback(closing, [ChannelCloseType, ClosingReason],
             State#state{closing = ResClosing}).

closing_priority(false)                                     -> 99;
closing_priority(#closing{reason = app_initiated_close})    -> 4;
closing_priority(#closing{reason = internal_error})         -> 3;
closing_priority(#closing{reason = server_misbehaved})      -> 2;
closing_priority(#closing{reason = server_initiated_close}) -> 1.

closing_to_reason(#closing{close = #'connection.close'{reply_code = 200}}) ->
    normal;
closing_to_reason(#closing{reason = Reason,
                           close = #'connection.close'{reply_code = Code,
                                                       reply_text = Text}}) ->
    {Reason, Code, Text};
closing_to_reason(#closing{reason = Reason,
                           close = {Reason, _Code, _Text} = Close}) ->
    Close.

handle_channels_terminated(State = #state{closing = Closing,
                                          module = Mod,
                                          module_state = MState}) ->
    #closing{reason = Reason, close = Close, from = From} = Closing,
    case Reason of
        server_initiated_close ->
            Mod:do(#'connection.close_ok'{}, MState);
        _ ->
            Mod:do(Close, MState)
    end,
    case callback(channels_terminated, [], State) of
        {stop, _, _} = Stop -> case From of none -> ok;
                                            _    -> gen_server:reply(From, ok)
                               end,
                               Stop;
        Other               -> Other
    end.
