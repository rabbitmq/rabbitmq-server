%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_global_counters).

-include("rabbit_global_counters.hrl").

-export([
         boot_step/0,
         init/1,
         init/2,
         overview/0,
         prometheus_format/0,
         increase_protocol_counter/3,
         messages_received/2,
         messages_received_confirm/2,
         messages_routed/2,
         messages_unroutable_dropped/2,
         messages_unroutable_returned/2,
         messages_confirmed/2,
         messages_delivered/3,
         messages_delivered_consume_manual_ack/3,
         messages_delivered_consume_auto_ack/3,
         messages_delivered_get_manual_ack/3,
         messages_delivered_get_auto_ack/3,
         messages_get_empty/3,
         messages_redelivered/3,
         messages_acknowledged/3,
         publisher_created/1,
         publisher_deleted/1,
         consumer_created/1,
         consumer_deleted/1,
         messages_dead_lettered/4,
         messages_dead_lettered_confirmed/3
       ]).

%% PROTOCOL COUNTERS:
-define(MESSAGES_RECEIVED, 1).
-define(MESSAGES_RECEIVED_CONFIRM, 2).
-define(MESSAGES_ROUTED, 3).
-define(MESSAGES_UNROUTABLE_DROPPED, 4).
-define(MESSAGES_UNROUTABLE_RETURNED, 5).
-define(MESSAGES_CONFIRMED, 6).
-define(PUBLISHERS, 7).
-define(CONSUMERS, 8).
%% Note: ?NUM_PROTOCOL_COUNTERS needs to be up-to-date. See include/rabbit_global_counters.hrl
-define(PROTOCOL_COUNTERS,
            [
                {
                    messages_received_total, ?MESSAGES_RECEIVED, counter,
                    "Total number of messages received from publishers"
                },
                {
                    messages_received_confirm_total, ?MESSAGES_RECEIVED_CONFIRM, counter,
                    "Total number of messages received from publishers expecting confirmations"
                },
                {
                    messages_routed_total, ?MESSAGES_ROUTED, counter,
                    "Total number of messages routed to queues or streams"
                },
                {
                    messages_unroutable_dropped_total, ?MESSAGES_UNROUTABLE_DROPPED, counter,
                    "Total number of messages published as non-mandatory into an exchange and dropped as unroutable"
                },
                {
                    messages_unroutable_returned_total, ?MESSAGES_UNROUTABLE_RETURNED, counter,
                   "Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable"
                },
                {
                    messages_confirmed_total, ?MESSAGES_CONFIRMED, counter,
                    "Total number of messages confirmed to publishers"
                },
                {
                    publishers, ?PUBLISHERS, gauge,
                    "Current number of publishers"
                },
                {
                    consumers, ?CONSUMERS, gauge,
                    "Current number of consumers"
                }
            ]).

%% Protocol & QueueType counters:
-define(MESSAGES_DELIVERED, 1).
-define(MESSAGES_DELIVERED_CONSUME_MANUAL_ACK, 2).
-define(MESSAGES_DELIVERED_CONSUME_AUTO_ACK, 3).
-define(MESSAGES_DELIVERED_GET_MANUAL_ACK, 4).
-define(MESSAGES_DELIVERED_GET_AUTO_ACK, 5).
-define(MESSAGES_GET_EMPTY, 6).
-define(MESSAGES_REDELIVERED, 7).
-define(MESSAGES_ACKNOWLEDGED, 8).
-define(PROTOCOL_QUEUE_TYPE_COUNTERS,
            [
                {
                    messages_delivered_total, ?MESSAGES_DELIVERED, counter,
                    "Total number of messages delivered to consumers"
                },
                {
                    messages_delivered_consume_manual_ack_total, ?MESSAGES_DELIVERED_CONSUME_MANUAL_ACK, counter,
                    "Total number of messages delivered to consumers using basic.consume with manual acknowledgment"
                },
                {
                    messages_delivered_consume_auto_ack_total, ?MESSAGES_DELIVERED_CONSUME_AUTO_ACK, counter,
                    "Total number of messages delivered to consumers using basic.consume with automatic acknowledgment"
                },
                {
                    messages_delivered_get_manual_ack_total, ?MESSAGES_DELIVERED_GET_MANUAL_ACK, counter,
                    "Total number of messages delivered to consumers using basic.get with manual acknowledgment"
                },
                {
                    messages_delivered_get_auto_ack_total, ?MESSAGES_DELIVERED_GET_AUTO_ACK, counter,
                    "Total number of messages delivered to consumers using basic.get with automatic acknowledgment"
                },
                {
                    messages_get_empty_total, ?MESSAGES_GET_EMPTY, counter,
                    "Total number of times basic.get operations fetched no message"
                },
                {
                    messages_redelivered_total, ?MESSAGES_REDELIVERED, counter,
                    "Total number of messages redelivered to consumers"
                },
                {
                    messages_acknowledged_total, ?MESSAGES_ACKNOWLEDGED, counter,
                    "Total number of messages acknowledged by consumers"
                }
            ]).

boot_step() ->
    [begin
         %% Protocol counters
         Protocol = {protocol, Proto},
         init([Protocol]),
         rabbit_msg_size_metrics:init(Proto),

         %% Protocol & Queue Type counters
         init([Protocol, {queue_type, rabbit_classic_queue}]),
         init([Protocol, {queue_type, rabbit_quorum_queue}]),
         init([Protocol, {queue_type, rabbit_stream_queue}])
     end || Proto <- [amqp091, amqp10]],

    %% Dead Letter counters
    %%
    %% Streams never dead letter.
    %%
    %% Source classic queue dead letters.
    init([{queue_type, rabbit_classic_queue}, {dead_letter_strategy, disabled}],
         [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER,
          ?MESSAGES_DEAD_LETTERED_EXPIRED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_REJECTED_COUNTER]),
    init([{queue_type, rabbit_classic_queue}, {dead_letter_strategy, at_most_once}],
         [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER,
          ?MESSAGES_DEAD_LETTERED_EXPIRED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_REJECTED_COUNTER]),
    %%
    %% Source quorum queue dead letters.
    %% Only quorum queues can dead letter due to delivery-limit exceeded.
    %% Only quorum queues support dead letter strategy at-least-once.
    init([{queue_type, rabbit_quorum_queue}, {dead_letter_strategy, disabled}],
         [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER,
          ?MESSAGES_DEAD_LETTERED_EXPIRED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_REJECTED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT_COUNTER
         ]),
    init([{queue_type, rabbit_quorum_queue}, {dead_letter_strategy, at_most_once}],
         [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER,
          ?MESSAGES_DEAD_LETTERED_EXPIRED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_REJECTED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT_COUNTER
         ]),
    init([{queue_type, rabbit_quorum_queue}, {dead_letter_strategy, at_least_once}],
         [?MESSAGES_DEAD_LETTERED_CONFIRMED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_EXPIRED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_REJECTED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT_COUNTER
         ]).

init(Labels) ->
    init(Labels, []).

init(Labels = [{protocol, Protocol}, {queue_type, QueueType}], Extra) ->
    _ = seshat:new_group(?MODULE),
    Counters = seshat:new(?MODULE, Labels, ?PROTOCOL_QUEUE_TYPE_COUNTERS ++ Extra, Labels),
    persistent_term:put({?MODULE, Protocol, QueueType}, Counters);
init(Labels = [{protocol, Protocol}], Extra) ->
    _ = seshat:new_group(?MODULE),
    Counters = seshat:new(?MODULE, Labels, ?PROTOCOL_COUNTERS ++ Extra, Labels),
    persistent_term:put({?MODULE, Protocol}, Counters);
init(Labels = [{queue_type, QueueType}, {dead_letter_strategy, DLS}], DeadLetterCounters) ->
    _ = seshat:new_group(?MODULE),
    Counters = seshat:new(?MODULE, Labels, DeadLetterCounters, Labels),
    persistent_term:put({?MODULE, QueueType, DLS}, Counters).

overview() ->
    seshat:overview(?MODULE).

prometheus_format() ->
    seshat:format(?MODULE).

increase_protocol_counter(Protocol, Counter, Num) ->
    counters:add(fetch(Protocol), Counter, Num).

messages_received(Protocol, Num) ->
    counters:add(fetch(Protocol), ?MESSAGES_RECEIVED, Num).

messages_received_confirm(Protocol, Num) ->
    counters:add(fetch(Protocol), ?MESSAGES_RECEIVED_CONFIRM, Num).

messages_routed(Protocol, Num) ->
    counters:add(fetch(Protocol), ?MESSAGES_ROUTED, Num).

messages_unroutable_dropped(Protocol, Num) ->
    counters:add(fetch(Protocol), ?MESSAGES_UNROUTABLE_DROPPED, Num).

messages_unroutable_returned(Protocol, Num) ->
    counters:add(fetch(Protocol), ?MESSAGES_UNROUTABLE_RETURNED, Num).

messages_confirmed(Protocol, Num) ->
    counters:add(fetch(Protocol), ?MESSAGES_CONFIRMED, Num).

messages_delivered(Protocol, QueueType, Num) ->
    counters:add(fetch(Protocol, QueueType), ?MESSAGES_DELIVERED, Num).

messages_delivered_consume_manual_ack(Protocol, QueueType, Num) ->
    counters:add(fetch(Protocol, QueueType), ?MESSAGES_DELIVERED_CONSUME_MANUAL_ACK, Num).

messages_delivered_consume_auto_ack(Protocol, QueueType, Num) ->
    counters:add(fetch(Protocol, QueueType), ?MESSAGES_DELIVERED_CONSUME_AUTO_ACK, Num).

messages_delivered_get_manual_ack(Protocol, QueueType, Num) ->
    counters:add(fetch(Protocol, QueueType), ?MESSAGES_DELIVERED_GET_MANUAL_ACK, Num).

messages_delivered_get_auto_ack(Protocol, QueueType, Num) ->
    counters:add(fetch(Protocol, QueueType), ?MESSAGES_DELIVERED_GET_AUTO_ACK, Num).

messages_get_empty(Protocol, QueueType, Num) ->
    counters:add(fetch(Protocol, QueueType), ?MESSAGES_GET_EMPTY, Num).

messages_redelivered(Protocol, QueueType, Num) ->
    counters:add(fetch(Protocol, QueueType), ?MESSAGES_REDELIVERED, Num).

messages_acknowledged(Protocol, QueueType, Num) ->
    counters:add(fetch(Protocol, QueueType), ?MESSAGES_ACKNOWLEDGED, Num).

publisher_created(Protocol) ->
    counters:add(fetch(Protocol), ?PUBLISHERS, 1).

publisher_deleted(Protocol) ->
    counters:sub(fetch(Protocol), ?PUBLISHERS, 1).

consumer_created(Protocol) ->
    counters:add(fetch(Protocol), ?CONSUMERS, 1).

consumer_deleted(Protocol) ->
    counters:sub(fetch(Protocol), ?CONSUMERS, 1).

messages_dead_lettered(Reason, QueueType, DeadLetterStrategy, Num) ->
    Index = case Reason of
                maxlen -> ?MESSAGES_DEAD_LETTERED_MAXLEN;
                expired -> ?MESSAGES_DEAD_LETTERED_EXPIRED;
                rejected -> ?MESSAGES_DEAD_LETTERED_REJECTED;
                delivery_limit -> ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT
            end,
    counters:add(fetch(QueueType, DeadLetterStrategy), Index, Num).

messages_dead_lettered_confirmed(QTypeModule, at_least_once, Num) ->
    counters:add(fetch(QTypeModule, at_least_once), ?MESSAGES_DEAD_LETTERED_CONFIRMED, Num).

fetch(Protocol) ->
    persistent_term:get({?MODULE, Protocol}).

fetch(Elem2, Elem3) ->
    persistent_term:get({?MODULE, Elem2, Elem3}).
