%% The contents of this file are subject to the Mozilla Public Licensbe
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
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
         handle_cancel/2, handle_deliver/3, handle_info/2, handle_call/3,
         terminate/2]).

-record(state, {consumers             = dict:new(), %% Tag -> ConsumerPid
                unassigned            = undefined,  %% Pid
                monitors              = dict:new(), %% Pid -> {Count, MRef}
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
handle_consume(BasicConsume, Pid, State = #state{consumers = Consumers,
                                                 monitors = Monitors}) ->
    Tag = tag(BasicConsume),
    Ok =
        case BasicConsume of
            #'basic.consume'{nowait = true}
                    when Tag =:= undefined orelse size(Tag) == 0 ->
                false; %% Async and undefined tag
            _ when is_binary(Tag) andalso size(Tag) >= 0 ->
                case resolve_consumer(Tag, State) of
                    {consumer, _} -> false; %% Tag already in use
                    _             -> true
                end;
           _ ->
               true
        end,
    case {Ok, BasicConsume} of
        {true, #'basic.consume'{nowait = true}} ->
            {ok, State#state
             {consumers = dict:store(Tag, Pid, Consumers),
              monitors  = add_to_monitor_dict(Pid, Monitors)}};
        {true, #'basic.consume'{nowait = false}} ->
            {ok, State#state{unassigned = Pid}};
        {false, #'basic.consume'{nowait = true}} ->
            {error, 'no_consumer_tag_specified', State};
        {false, #'basic.consume'{nowait = false}} ->
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
          consumers  = dict:store(tag(BasicConsumeOk), Pid, Consumers),
          monitors   = add_to_monitor_dict(Pid, Monitors),
          unassigned = undefined},
    deliver(BasicConsumeOk, State1),
    {ok, State1}.

%% @private
%% The server sent a basic.cancel.
handle_cancel(Cancel, State) ->
    State1 = do_cancel(Cancel, State),
    %% Use old state
    deliver(Cancel, State),
    {ok, State1}.

%% @private
%% We sent a basic.cancel and now receive the ok.
handle_cancel_ok(CancelOk, _Cancel, State) ->
    State1 = do_cancel(CancelOk, State),
    %% Use old state
    deliver(CancelOk, State),
    {ok, State1}.

%% @private
handle_deliver(Deliver, Message, State) ->
    deliver(Deliver, Message, State),
    {ok, State}.

%% @private
handle_info({'DOWN', _MRef, process, Pid, _Info},
            State = #state{monitors         = Monitors,
                           consumers        = Consumers,
                           default_consumer = DConsumer }) ->
    case dict:find(Pid, Monitors) of
        {ok, _CountMRef} ->
            {ok, State#state{monitors = dict:erase(Pid, Monitors),
                             consumers =
                                 dict:filter(
                                   fun (_, Pid1) when Pid1 =:= Pid -> false;
                                       (_, _)                      -> true
                                   end, Consumers)}};
        error ->
            case Pid of
                DConsumer -> {ok, State#state{
                                    monitors = dict:erase(Pid, Monitors),
                                    default_consumer = none}};
                _         -> {ok, State} %% unnamed consumer went down
                                         %% before receiving consume_ok
            end
    end.

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

deliver(Msg, State) ->
    deliver(Msg, undefined, State).
deliver(Msg, Message, State) ->
    Combined = if Message =:= undefined -> Msg;
                  true                  -> {Msg, Message}
               end,
    case resolve_consumer(tag(Msg), State) of
        {consumer, Pid} -> Pid ! Combined;
        {default, Pid}  -> Pid ! Combined;
        error           -> exit(unexpected_delivery_and_no_default_consumer)
    end.

do_cancel(Cancel, State = #state{consumers = Consumers,
                                 monitors  = Monitors}) ->
    Tag = tag(Cancel),
    case dict:find(Tag, Consumers) of
        {ok, Pid} -> State#state{
                       consumers = dict:erase(Tag, Consumers),
                       monitors  = remove_from_monitor_dict(Pid, Monitors)};
        error     -> %% Untracked consumer. Do nothing.
                     State
    end.

resolve_consumer(Tag, #state{consumers = Consumers,
                             default_consumer = DefaultConsumer}) ->
    case dict:find(Tag, Consumers) of
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
tag(#'basic.deliver'{consumer_tag = Tag})         -> Tag.

add_to_monitor_dict(Pid, Monitors) ->
    case dict:find(Pid, Monitors) of
        error               -> dict:store(Pid,
                                          {1, erlang:monitor(process, Pid)},
                                          Monitors);
        {ok, {Count, MRef}} -> dict:store(Pid, {Count + 1, MRef}, Monitors)
    end.

remove_from_monitor_dict(Pid, Monitors) ->
    case dict:fetch(Pid, Monitors) of
        {1, MRef}     -> erlang:demonitor(MRef),
                         dict:erase(Pid, Monitors);
        {Count, MRef} -> dict:store(Pid, {Count - 1, MRef}, Monitors)
    end.
