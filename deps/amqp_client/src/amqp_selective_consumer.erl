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

%% @doc TODO
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

%% TODO doc
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

%% TODO doc
%% (provided for completeness)
cancel(ChannelPid, #'basic.cancel'{} = Cancel) ->
    amqp_channel:call(ChannelPid, Cancel).

%% TODO doc
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
