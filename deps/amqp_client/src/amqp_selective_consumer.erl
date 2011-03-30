%% The contents of this file are subject to the Mozilla Public License
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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

%% @doc This module is an implementation of the amqp_gen_consumer behaviour and
%% can be used as part of the Consumer parameter when opening AMQP
%% channels.<br/>
%% The Consumer parameter for this implementation is {{@module}, []@}<br/>
%% This consumer implementation keeps track of consumer tags and sends
%% the subscription-relevant messages to the registered consumers, according
%% to an internal tag dictionary.<br/>
%% Use {@module}:subscribe/3 to subscribe a consumer to a queue and
%% {@module}:cancel/2 to cancel a subscription.<br/>
%% The channel will send to the relevant registered consumers the
%% basic.consume_ok, basic.cancel_ok, basic.cancel and basic.deliver messages
%% received from the server.<br/>
%% If a consumer is not registered for a given consumer tag, the message
%% is sent to the default consumer registered with
%% {@module}:register_default_consumer. If there is no default consumer
%% registered in this case, an exception occurs and the channel is abrubptly
%% terminated.<br/>
%% amqp_channel:call(ChannelPid, #'basic.consume'{}) can also be used to
%% subscribe to a queue, but one must register a default consumer for messages
%% to be delivered to, beforehand. Failing to do so generates the
%% above-mentioned exception.
-module(amqp_selective_consumer).

-include("amqp_client.hrl").

-behaviour(amqp_gen_consumer).

-export([subscribe/3, cancel/2, register_default_consumer/2]).
-export([init/1, handle_consume_ok/2, handle_cancel_ok/2, handle_cancel/2,
         handle_deliver/3, handle_message/2, terminate/2]).

-record(state, {consumers           = dict:new(),   %% Tag -> ConsumerPid
                unassigned_tags     = dict:new(),   %% Tag -> Messages
                default_consumer    = none}).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

%% @type consume() = #'basic.consume'{}.
%% The AMQP method that is used to  subscribe a consumer to a queue.
%% @type consume_ok() = #'basic.consume_ok'{}.
%% The AMQP method returned in response to basic.consume.
%% @spec (ChannelPid, consume(), ConsumerPid) -> Result
%% where
%%      ChannelPid = pid()
%%      ConsumerPid = pid()
%%      Result = consume_ok() | ok | {error, command_invalid}
%% @doc Creates a subscription to a queue. This subscribes a consumer pid to
%% the queue defined in the #'basic.consume'{} method record. Note that
%% both the process invoking this method and the supplied consumer process
%% receive an acknowledgement of the subscription. The calling process will
%% receive the acknowledgement as the return value of this function, whereas
%% the consumer process will receive the notification as a message,
%% asynchronously.<br/>
%% This function returns {error, command_invalid} if consumer_tag is not
%% specified and nowait is true.<br/>
%% Attempting to subscribe with a consumer_tag that is already in use will
%% cause an exception and the channel will terminate. If nowait is set to true
%% in this case, the function will return ok, but the channel will terminate
%% with an error.
subscribe(ChannelPid, #'basic.consume'{nowait = false} = BasicConsume,
          ConsumerPid) ->
    ConsumeOk = #'basic.consume_ok'{consumer_tag = RetTag} =
        amqp_channel:call(ChannelPid, BasicConsume),
    amqp_channel:send_to_consumer(ChannelPid, {assign, ConsumerPid, RetTag}),
    ConsumeOk;
subscribe(_ChannelPid, #'basic.consume'{nowait = true, consumer_tag = Tag},
          _ConsumerPid) when Tag =:= undefined orelse size(Tag) == 0 ->
    {error, command_invalid};
subscribe(ChannelPid, #'basic.consume'{nowait = true,
                                       consumer_tag = Tag} = BasicConsume,
          ConsumerPid) when is_binary(Tag) andalso size(Tag) >= 0 ->
    ok = call_consumer(ChannelPid, {subscribe_nowait, Tag, ConsumerPid}),
    amqp_channel:call(ChannelPid, BasicConsume).

%% @type cancel() = #'basic.cancel'{}.
%% The AMQP method used to cancel a subscription.

%% @spec (ChannelPid, Cancel) -> amqp_method() | ok
%% where
%%      ChannelPid = pid()
%%      Cancel = cancel()
%% @doc This function is the same as calling
%% amqp_channel:call(ChannelPid, Cancel) and is only provided for completeness.
cancel(ChannelPid, #'basic.cancel'{} = Cancel) ->
    amqp_channel:call(ChannelPid, Cancel).

%% @spec (ChannelPid, ConsumerPid) -> ok
%% where
%%      ChannelPid = pid()
%%      ConsumerPid = pid()
%% @doc This function registers a default consumer with the channel. A default
%% consumer is used in two situations:<br/>
%% <br/>
%% 1) A subscription was made via
%% amqp_channel:call(ChannelPid, #'basic.consume'{}) (rather than
%% {@module}:subscribe/3) and hence there is no consumer pid registered with the
%% consumer tag.<br/>
%% <br/>
%% 2) The following sequence of events occurs:<br/>
%% <br/>
%% - subscribe is used with basic.consume with explicit acks<br/>
%% - some deliveries take place but are not acked<br/>
%% - a basic.cancel is issued<br/>
%% - a basic.recover{requeue = false} is issued<br/>
%% <br/>
%% Since requeue is specified to be false in the basic.recover, the spec
%% states that the message must be redelivered to "the original recipient"
%% - i.e. the same channel / consumer-tag. But the consumer is no longer
%% active. <br/>
%% <br/>
%% In these two cases, the relevant deliveries will be sent to the default
%% consumer.
register_default_consumer(ChannelPid, ConsumerPid) ->
    call_consumer(ChannelPid, {register_default_consumer, ConsumerPid}).

%%---------------------------------------------------------------------------
%% amqp_gen_consumer callbacks
%%---------------------------------------------------------------------------

%% @private
init([]) ->
    {ok, #state{}}.

%% @private
handle_consume_ok(#'basic.consume_ok'{consumer_tag = Tag} = ConsumeOk,
                  State = #state{unassigned_tags = Unassigned}) ->
    {ok,
     State#state{unassigned_tags = dict:store(Tag, [ConsumeOk], Unassigned)}}.

%% @private
handle_cancel_ok(#'basic.cancel_ok'{consumer_tag = Tag} = CancelOk, State) ->
    deliver_or_queue(Tag, CancelOk, State).

%% @private
handle_cancel(#'basic.cancel'{consumer_tag = Tag} = Cancel, State) ->
    deliver_or_queue(Tag, Cancel, State).

%% @private
handle_deliver(#'basic.deliver'{consumer_tag = Tag} = Deliver, AmqpMsg,
               State) ->
    deliver_or_queue(Tag, {Deliver, AmqpMsg}, State).

%% @private
handle_message({assign, ConsumerPid, ConsumerTag},
               State = #state{unassigned_tags = Unassigned,
                              consumers = Consumers}) ->
    Messages = dict:fetch(ConsumerTag, Unassigned),
    State1 = State#state{
                 unassigned_tags = dict:erase(ConsumerTag, Unassigned),
                 consumers = dict:store(ConsumerTag, ConsumerPid, Consumers)},
    lists:foldr(fun (Message, {ok, CurState}) ->
                        deliver(Message, ConsumerPid, ConsumerTag, CurState)
                end, {ok, State1}, Messages);
%% @private
handle_message({call, Caller, {subscribe_nowait, Tag, ConsumerPid}},
                State = #state{consumers = Consumers}) ->
    ErrorOrDefault = case resolve_consumer(Tag, State) of
                         error        -> true;
                         {default, _} -> true;
                         _            -> false
                     end,
    if ErrorOrDefault ->
           reply(Caller, ok),
           {ok, State#state{
                    consumers = dict:store(Tag, ConsumerPid, Consumers)}};
        true ->
            %% Consumer already registered. Do not override, server will close
            %% the channel.
            reply(Caller, ok),
            {ok, State}
    end;
%% @private
handle_message({call, Caller, {register_default_consumer, DefaultConsumer}},
               State) ->
    reply(Caller, ok),
    {ok, State#state{default_consumer = DefaultConsumer}}.

%% @private
terminate(Reason, #state{consumers = Consumers,
                         default_consumer = DefaultConsumer}) ->
    dict:fold(fun (_, Consumer, _) -> exit(Consumer, Reason) end, ok,
              Consumers),
    case DefaultConsumer of
        none -> ok;
        _    -> exit(DefaultConsumer, Reason)
    end.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

call_consumer(ChannelPid, Msg) ->
    Monitor = erlang:monitor(process, ChannelPid),
    amqp_channel:send_to_consumer(ChannelPid, {call, self(), Msg}),
    receive
        {'$call_consumer_reply', Reply} ->
            erlang:demonitor(Monitor, [flush]),
            Reply;
        {'DOWN', Monitor, process, ChannelPid, Reason} ->
            {channel_died, Reason}
    end.

reply(Caller, Reply) ->
    Caller ! {'$call_consumer_reply', Reply}.

deliver_or_queue(Tag, Message, State = #state{unassigned_tags = Unassigned}) ->
    case resolve_consumer(Tag, State) of
        {consumer, Consumer} ->
            deliver(Message, Consumer, Tag, State);
        {default, Consumer} ->
            deliver(Message, Consumer, Tag, State);
        {queue, Messages} ->
            NewUnassigned = dict:store(Tag, [Message | Messages], Unassigned),
            {ok, State#state{unassigned_tags = NewUnassigned}};
        error ->
            exit(unexpected_delivery_and_no_default_consumer)
    end.

resolve_consumer(Tag, #state{consumers = Consumers,
                             unassigned_tags = Unassigned,
                             default_consumer = DefaultConsumer}) ->
    case dict:find(Tag, Consumers) of
        {ok, ConsumerPid} ->
            {consumer, ConsumerPid};
        error ->
            case {dict:find(Tag, Unassigned), DefaultConsumer} of
                {{ok, Messages}, _} ->
                    {queue, Messages};
                {error, none} ->
                    error;
                {error, _} ->
                    {default, DefaultConsumer}
            end
    end.

deliver(Msg, Consumer, Tag, State = #state{consumers = Consumers}) ->
    Consumer ! Msg,
    CancelOrCancelOk = case Msg of #'basic.cancel'{}    -> true;
                                   #'basic.cancel_ok'{} -> true;
                                   _                    -> false
                       end,
    if CancelOrCancelOk ->
            {ok, State#state{consumers = dict:erase(Tag, Consumers)}};
        true ->
            {ok, State}
    end.
