%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc This module is an implementation of the amqp_gen_consumer
%% behaviour and can be used as part of the Consumer parameter when
%% opening AMQP channels. This is the default implementation selected
%% by channel. <br/>
%% <br/>
%% The Consumer parameter for this implementation is {{@module}, []@}<br/>
%% This consumer implementation keeps track of consumer tags and sends
%% the subscription-relevant messages to the registered consumers, according
%% to an internal tag dictionary.<br/>
%% <br/>
%% Send a #basic.consume{} message to the channel to subscribe a
%% consumer to a queue and send a #basic.cancel{} message to cancel a
%% subscription.<br/>
%% <br/>
%% The channel will send to the relevant registered consumers the
%% basic.consume_ok, basic.cancel_ok, basic.cancel and basic.deliver messages
%% received from the server.<br/>
%% <br/>
%% If a consumer is not registered for a given consumer tag, the message
%% is sent to the default consumer registered with
%% {@module}:register_default_consumer. If there is no default consumer
%% registered in this case, an exception occurs and the channel is abruptly
%% terminated.<br/>
-module(amqp_selective_consumer).

-include("amqp_gen_consumer_spec.hrl").

-behaviour(amqp_gen_consumer).

-export([register_default_consumer/2]).
-export([init/1, handle_consume_ok/3, handle_consume/3, handle_cancel_ok/3,
         handle_cancel/2, handle_server_cancel/2,
         handle_deliver/3, handle_deliver/4,
         handle_info/2, handle_call/3, terminate/2]).

-record(state, {consumers             = #{}, %% Tag -> ConsumerPid
                unassigned            = undefined,  %% Pid
                monitors              = #{}, %% Pid -> {Count, MRef}
                default_consumer      = none}).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

%% @spec (ChannelPid, ConsumerPid) -> ok
%% where
%%      ChannelPid = pid()
%%      ConsumerPid = pid()
%% @doc This function registers a default consumer with the channel. A
%% default consumer is used when a subscription is made via
%% amqp_channel:call(ChannelPid, #'basic.consume'{}) (rather than
%% {@module}:subscribe/3) and hence there is no consumer pid
%% registered with the consumer tag. In this case, the relevant
%% deliveries will be sent to the default consumer.
register_default_consumer(ChannelPid, ConsumerPid) ->
    amqp_channel:call_consumer(ChannelPid,
                               {register_default_consumer, ConsumerPid}).

%%---------------------------------------------------------------------------
%% amqp_gen_consumer callbacks
%%---------------------------------------------------------------------------

%% @private
init([]) ->
    {ok, #state{}}.

%% @private
handle_consume(#'basic.consume'{consumer_tag = Tag,
                                nowait       = NoWait},
               Pid, State = #state{consumers = Consumers,
                                   monitors = Monitors}) ->
    Result = case NoWait of
                 true when Tag =:= undefined orelse size(Tag) == 0 ->
                     no_consumer_tag_specified;
                 _ when is_binary(Tag) andalso size(Tag) >= 0 ->
                     case resolve_consumer(Tag, State) of
                         {consumer, _} -> consumer_tag_in_use;
                         _             -> ok
                     end;
                 _ ->
                     ok
             end,
    case {Result, NoWait} of
        {ok, true} ->
            {ok, State#state
                   {consumers = maps:put(Tag, Pid, Consumers),
                    monitors  = add_to_monitor_dict(Pid, Monitors)}};
        {ok, false} ->
            {ok, State#state{unassigned = Pid}};
        {Err, true} ->
            {error, Err, State};
        {_Err, false} ->
            %% Don't do anything (don't override existing
            %% consumers), the server will close the channel with an error.
            {ok, State}
    end.

%% @private
handle_consume_ok(BasicConsumeOk, _BasicConsume,
                  State = #state{unassigned = Pid,
                                 consumers  = Consumers,
                                 monitors   = Monitors})
  when is_pid(Pid) ->
    State1 =
        State#state{
          consumers  = maps:put(tag(BasicConsumeOk), Pid, Consumers),
          monitors   = add_to_monitor_dict(Pid, Monitors),
          unassigned = undefined},
    deliver(BasicConsumeOk, State1),
    {ok, State1}.

%% @private
%% We sent a basic.cancel.
handle_cancel(#'basic.cancel'{nowait = true},
              #state{default_consumer = none}) ->
    exit(cancel_nowait_requires_default_consumer);

handle_cancel(Cancel = #'basic.cancel'{nowait = NoWait}, State) ->
    State1 = case NoWait of
                 true  -> do_cancel(Cancel, State);
                 false -> State
             end,
    {ok, State1}.

%% @private
%% We sent a basic.cancel and now receive the ok.
handle_cancel_ok(CancelOk, _Cancel, State) ->
    State1 = do_cancel(CancelOk, State),
    %% Use old state
    deliver(CancelOk, State),
    {ok, State1}.

%% @private
%% The server sent a basic.cancel.
handle_server_cancel(Cancel = #'basic.cancel'{nowait = true}, State) ->
    State1 = do_cancel(Cancel, State),
    %% Use old state
    deliver(Cancel, State),
    {ok, State1}.

%% @private
handle_deliver(Method, Message, State) ->
    deliver(Method, Message, State),
    {ok, State}.

%% @private
handle_deliver(Method, Message, DeliveryCtx, State) ->
    deliver(Method, Message, DeliveryCtx, State),
    {ok, State}.

%% @private
handle_info({'DOWN', _MRef, process, Pid, _Info},
            State = #state{monitors         = Monitors,
                           consumers        = Consumers,
                           default_consumer = DConsumer }) ->
    case maps:find(Pid, Monitors) of
        {ok, _CountMRef} ->
            {ok, State#state{monitors = maps:remove(Pid, Monitors),
                             consumers =
                                 maps:filter(
                                   fun (_, Pid1) when Pid1 =:= Pid -> false;
                                       (_, _)                      -> true
                                   end, Consumers)}};
        error ->
            case Pid of
                DConsumer -> {ok, State#state{
                                    monitors = maps:remove(Pid, Monitors),
                                    default_consumer = none}};
                _         -> {ok, State} %% unnamed consumer went down
                                         %% before receiving consume_ok
            end
    end;
handle_info(#'basic.credit_drained'{} = Method, State) ->
    deliver_to_consumer_or_die(Method, Method, State),
    {ok, State}.

%% @private
handle_call({register_default_consumer, Pid}, _From,
            State = #state{default_consumer = PrevPid,
                           monitors         = Monitors}) ->
    Monitors1 = case PrevPid of
                    none -> Monitors;
                    _    -> remove_from_monitor_dict(PrevPid, Monitors)
                end,
    {reply, ok,
     State#state{default_consumer = Pid,
                 monitors = add_to_monitor_dict(Pid, Monitors1)}}.

%% @private
terminate(_Reason, State) ->
    State.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

deliver_to_consumer_or_die(Method, Msg, State) ->
    case resolve_consumer(tag(Method), State) of
        {consumer, Pid} -> Pid ! Msg;
        {default, Pid}  -> Pid ! Msg;
        error           -> exit(unexpected_delivery_and_no_default_consumer)
    end.

deliver(Method, State) ->
    deliver(Method, undefined, State).
deliver(Method, Message, State) ->
    Combined = if Message =:= undefined -> Method;
                  true                  -> {Method, Message}
               end,
    deliver_to_consumer_or_die(Method, Combined, State).
deliver(Method, Message, DeliveryCtx, State) ->
    Combined = if Message =:= undefined -> Method;
                  true                  -> {Method, Message, DeliveryCtx}
               end,
    deliver_to_consumer_or_die(Method, Combined, State).

do_cancel(Cancel, State = #state{consumers = Consumers,
                                 monitors  = Monitors}) ->
    Tag = tag(Cancel),
    case maps:find(Tag, Consumers) of
        {ok, Pid} -> State#state{
                       consumers = maps:remove(Tag, Consumers),
                       monitors  = remove_from_monitor_dict(Pid, Monitors)};
        error     -> %% Untracked consumer. Do nothing.
                     State
    end.

resolve_consumer(Tag, #state{consumers = Consumers,
                             default_consumer = DefaultConsumer}) ->
    case maps:find(Tag, Consumers) of
        {ok, ConsumerPid} -> {consumer, ConsumerPid};
        error             -> case DefaultConsumer of
                                 none -> error;
                                 _    -> {default, DefaultConsumer}
                             end
    end.

tag(#'basic.consume'{consumer_tag = Tag})         -> Tag;
tag(#'basic.consume_ok'{consumer_tag = Tag})      -> Tag;
tag(#'basic.cancel'{consumer_tag = Tag})          -> Tag;
tag(#'basic.cancel_ok'{consumer_tag = Tag})       -> Tag;
tag(#'basic.deliver'{consumer_tag = Tag})         -> Tag;
tag(#'basic.credit_drained'{consumer_tag = Tag})  -> Tag.

add_to_monitor_dict(Pid, Monitors) ->
    case maps:find(Pid, Monitors) of
        error               -> maps:put(Pid,
                                        {1, erlang:monitor(process, Pid)},
                                        Monitors);
        {ok, {Count, MRef}} -> maps:put(Pid, {Count + 1, MRef}, Monitors)
    end.

remove_from_monitor_dict(Pid, Monitors) ->
    case maps:get(Pid, Monitors) of
        {1, MRef}     -> erlang:demonitor(MRef),
                         maps:remove(Pid, Monitors);
        {Count, MRef} -> maps:put(Pid, {Count - 1, MRef}, Monitors)
    end.
