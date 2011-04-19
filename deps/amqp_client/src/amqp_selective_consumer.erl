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
%% <br/>
%% The Consumer parameter for this implementation is {{@module}, []@}<br/>
%% This consumer implementation keeps track of consumer tags and sends
%% the subscription-relevant messages to the registered consumers, according
%% to an internal tag dictionary.<br/>
%% <br/>
%% Use {@module}:subscribe/3 to subscribe a consumer to a queue and
%% {@module}:cancel/2 to cancel a subscription.<br/>
%% <br/>
%% The channel will send to the relevant registered consumers the
%% basic.consume_ok, basic.cancel_ok, basic.cancel and basic.deliver messages
%% received from the server.<br/>
%% <br/>
%% If a consumer is not registered for a given consumer tag, the message
%% is sent to the default consumer registered with
%% {@module}:register_default_consumer. If there is no default consumer
%% registered in this case, an exception occurs and the channel is abrubptly
%% terminated.<br/>
%% <br/>
%% amqp_channel:call(ChannelPid, #'basic.consume'{}) can also be used to
%% subscribe to a queue, but one must register a default consumer for messages
%% to be delivered to, beforehand. Failing to do so generates the
%% above-mentioned exception.<br/>
%% <br/>
%% This consumer implementation creates a link between the channel and the
%% registered consumers (either through register_default_consumer/2 or
%% through subscribe/3). A cancel (either issued by the user application or the
%% server) causes the link to be removed. In addition, registering another
%% default consumer causes the old one to be unlinked.<br/>
%% Warning! It is not recommended to rely on a consumer on killing off the
%% channel (through the exit signal). That may cause messages to get lost.
%% Always use amqp_channel:close/{1,3} for a clean shut down.
-module(amqp_selective_consumer).

-include("amqp_client.hrl").

-behaviour(amqp_gen_consumer).

-export([subscribe/3, cancel/2, register_default_consumer/2]).
-export([init/1, handle_consume_ok/3, handle_cancel_ok/3, handle_cancel/2,
         handle_deliver/2, handle_call/2, terminate/2]).

-record(state, {consumers        = dict:new(), %% Tag -> ConsumerPid
                unassigned       = dict:new(), %% BasicConsume -> [ConsumerPid]
                default_consumer = none}).

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
%%      Result = consume_ok() | ok | error
%% @doc Creates a subscription to a queue. This subscribes a consumer pid to
%% the queue defined in the #'basic.consume'{} method record. Note that
%% both the process invoking this method and the supplied consumer process
%% receive an acknowledgement of the subscription. The calling process will
%% receive the acknowledgement as the return value of this function, whereas
%% the consumer process will receive the notification as a message,
%% asynchronously.<br/>
%% <br/>
%% Attempting to subscribe with a consumer_tag that is already in use or
%% to subscribe with nowait true and not specifying a consumer_tag will
%% cause an exception and the channel will terminate, causing this function
%% to throw. If nowait is set to true the function will return 'error'
%% immediately, and the channel will be terminated by the server.
subscribe(ChannelPid, BasicConsume, ConsumerPid) ->
    ok = amqp_channel:call_consumer(ChannelPid,
                                    {subscribe, BasicConsume, ConsumerPid}),
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
    amqp_channel:call_consumer(ChannelPid,
                               {register_default_consumer, ConsumerPid}).

%%---------------------------------------------------------------------------
%% amqp_gen_consumer callbacks
%%---------------------------------------------------------------------------

%% @private
init([]) ->
    #state{}.

%% @private
handle_consume_ok(BasicConsumeOk, BasicConsume, State) ->
    State1 = assign_consumer(BasicConsume, tag(BasicConsumeOk), State),
    deliver(BasicConsumeOk, State1),
    State1.

%% @private
handle_cancel_ok(CancelOk, _Cancel, State) ->
    %% Unlink first!
    State1 = do_cancel(CancelOk, State),
    %% Use old state
    deliver(CancelOk, State),
    State1.

%% @private
handle_cancel(Cancel, State) ->
    %% Unlink first!
    State1 = do_cancel(Cancel, State),
    %% Use old state
    deliver(Cancel, State),
    State1.

%% @private
handle_deliver(Deliver, State) ->
    deliver(Deliver, State),
    State.

%% @private
handle_call({subscribe, BasicConsume, Pid},
            State = #state{consumers = Consumers, unassigned = Unassigned}) ->
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
    if Ok ->
           case BasicConsume of
               #'basic.consume'{nowait = true} ->
                   {ok,
                    State#state{consumers = dict:store(Tag, Pid, Consumers)}};
               #'basic.consume'{nowait = false} ->
                   NewUnassigned =
                       dict:update(BasicConsume, fun (Pids) -> [Pid | Pids] end,
                                   [Pid], Unassigned),
                   {ok, State#state{unassigned = NewUnassigned}}
           end;
       true ->
           %% There is an error. Don't do anything (don't override existing
           %% consumers), the server will close the channel with an error.
           {error, State}
    end;
%% @private
handle_call({register_default_consumer, Pid},
            State = #state{default_consumer = PrevPid}) ->
    case PrevPid of none -> ok;
                    _    -> unlink(PrevPid)
    end,
    link(Pid),
    {ok, State#state{default_consumer = Pid}}.

%% @private
terminate(_Reason, State) ->
    State.

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

assign_consumer(BasicConsume, Tag, State = #state{consumers = Consumers,
                                                  unassigned = Unassigned}) ->
    case dict:find(BasicConsume, Unassigned) of
        {ok, [Pid]} ->
            State#state{unassigned = dict:erase(BasicConsume, Unassigned),
                        consumers = dict:store(Tag, Pid, Consumers)};
        {ok, [Pid | RestPids]} ->
            State#state{unassigned = dict:store(BasicConsume, RestPids,
                                                Unassigned),
                        consumers = dict:store(Tag, Pid, Consumers)};
        error ->
            %% Untracked consumer (subscribed with amqp_channel:call/2)
            State
    end.

deliver(Msg, State) ->
    case resolve_consumer(tag(Msg), State) of
        {consumer, Pid} -> Pid ! Msg;
        {default, Pid}  -> Pid ! Msg;
        error           -> exit(unexpected_delivery_and_no_default_consumer)
    end.

do_cancel(Cancel, State = #state{consumers = Consumers}) ->
    Tag = tag(Cancel),
    case dict:find(Tag, Consumers) of
        {ok, Pid} -> unlink(Pid),
                     State#state{consumers = dict:erase(Tag, Consumers)};
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
tag({#'basic.deliver'{consumer_tag = Tag}, _})    -> Tag.
