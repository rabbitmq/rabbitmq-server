%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_ha_test_consumer).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([await_response/1, create/5, start/6]).

await_response(ConsumerPid) ->
    case receive {ConsumerPid, Response} -> Response end of
        {error, Reason}  -> erlang:error(Reason);
        ok               -> ok
    end.

create(Channel, Queue, TestPid, CancelOnFailover, ExpectingMsgs) ->
    ConsumerPid = spawn_link(?MODULE, start,
                             [TestPid, Channel, Queue, CancelOnFailover,
                              ExpectingMsgs + 1, ExpectingMsgs]),
    amqp_channel:subscribe(
      Channel, consume_method(Queue, CancelOnFailover), ConsumerPid),
    ConsumerPid.

start(TestPid, Channel, Queue, CancelOnFailover, LowestSeen, MsgsToConsume) ->
    error_logger:info_msg("consumer ~p on ~p awaiting ~w messages "
                          "(lowest seen = ~w, cancel-on-failover = ~w)~n",
                          [self(), Channel, MsgsToConsume, LowestSeen,
                           CancelOnFailover]),
    run(TestPid, Channel, Queue, CancelOnFailover, LowestSeen, MsgsToConsume).

run(TestPid, _Channel, _Queue, _CancelOnFailover, _LowestSeen, 0) ->
    consumer_reply(TestPid, ok);
run(TestPid, Channel, Queue, CancelOnFailover, LowestSeen, MsgsToConsume) ->
    receive
        #'basic.consume_ok'{} ->
            run(TestPid, Channel, Queue,
                CancelOnFailover, LowestSeen, MsgsToConsume);
        {Delivery = #'basic.deliver'{ redelivered = Redelivered },
         #amqp_msg{payload = Payload}} ->
            MsgNum = list_to_integer(binary_to_list(Payload)),

            ack(Delivery, Channel),

            %% we can receive any message we've already seen and,
            %% because of the possibility of multiple requeuings, we
            %% might see these messages in any order. If we are seeing
            %% a message again, we don't decrement the MsgsToConsume
            %% counter.
            if
                MsgNum + 1 == LowestSeen ->
                    run(TestPid, Channel, Queue,
                        CancelOnFailover, MsgNum, MsgsToConsume - 1);
                MsgNum >= LowestSeen ->
                    true = Redelivered, %% ASSERTION
                    run(TestPid, Channel, Queue,
                        CancelOnFailover, LowestSeen, MsgsToConsume);
                true ->
                    %% We received a message we haven't seen before,
                    %% but it is not the next message in the expected
                    %% sequence.
                    consumer_reply(TestPid,
                                   {error, {unexpected_message, MsgNum}})
            end;
        #'basic.cancel'{} when CancelOnFailover ->
            error_logger:info_msg("consumer ~p on ~p received basic.cancel: "
                                  "resubscribing to ~p on ~p~n",
                                  [self(), Channel, Queue, Channel]),
            resubscribe(TestPid, Channel, Queue, CancelOnFailover,
                        LowestSeen, MsgsToConsume);
        #'basic.cancel'{} ->
            exit(cancel_received_without_cancel_on_failover)
    end.

%%
%% Private API
%%

resubscribe(TestPid, Channel, Queue, CancelOnFailover, LowestSeen,
            MsgsToConsume) ->
    amqp_channel:subscribe(
      Channel, consume_method(Queue, CancelOnFailover), self()),
    ok = receive #'basic.consume_ok'{} -> ok
         end,
    error_logger:info_msg("re-subscripting consumer ~p on ~p complete "
                          "(received basic.consume_ok)",
                          [self(), Channel]),
    start(TestPid, Channel, Queue, CancelOnFailover, LowestSeen, MsgsToConsume).

consume_method(Queue, CancelOnFailover) ->
    Args = [{<<"x-cancel-on-ha-failover">>, bool, CancelOnFailover}],
    #'basic.consume'{queue     = Queue,
                     arguments = Args}.

ack(#'basic.deliver'{delivery_tag = DeliveryTag}, Channel) ->
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    ok.

consumer_reply(TestPid, Reply) ->
    TestPid ! {self(), Reply}.
