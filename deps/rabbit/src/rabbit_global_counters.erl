%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbit_global_counters).

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
%% Note: ?NUM_PROTOCOL_QUEUE_TYPE_COUNTERS needs to be up-to-date. See include/rabbit_global_counters.hrl
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

-define(MESSAGES_DEAD_LETTERED_EXPIRED, 1).
-define(MESSAGES_DEAD_LETTERED_REJECTED, 2).
%% The following two counters are mutually exclusive because
%% quorum queue dead-letter-strategy at-least-once is incompatible with overflow drop-head.
-define(MESSAGES_DEAD_LETTERED_MAXLEN, 3).
-define(MESSAGES_DEAD_LETTERED_CONFIRMED, 3).
-define(MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT, 4).
-define(MESSAGES_DEAD_LETTERED_COUNTERS,
        [
         {
          messages_dead_lettered_expired_total, ?MESSAGES_DEAD_LETTERED_EXPIRED, counter,
          "Total number of messages dead-lettered due to message TTL exceeded"
         },
         {
          messages_dead_lettered_rejected_total, ?MESSAGES_DEAD_LETTERED_REJECTED, counter,
          "Total number of messages dead-lettered due to basic.reject or basic.nack"
         }
        ]).
-define(MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER,
        {
         messages_dead_lettered_maxlen_total, ?MESSAGES_DEAD_LETTERED_MAXLEN, counter,
         "Total number of messages dead-lettered due to overflow drop-head or reject-publish-dlx"
        }).
-define(MESSAGES_DEAD_LETTERED_CONFIRMED_COUNTER,
        {
         messages_dead_lettered_confirmed_total, ?MESSAGES_DEAD_LETTERED_CONFIRMED, counter,
         "Total number of messages dead-lettered and confirmed by target queues"
        }).
-define(MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT_COUNTER,
        {
         messages_dead_lettered_delivery_limit_total, ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT, counter,
         "Total number of messages dead-lettered due to delivery-limit exceeded"
        }).

boot_step() ->
    init([{protocol, amqp091}]),
    init([{protocol, amqp091}, {queue_type, rabbit_classic_queue}]),
    init([{protocol, amqp091}, {queue_type, rabbit_quorum_queue}]),
    init([{protocol, amqp091}, {queue_type, rabbit_stream_queue}]),
    init([{queue_type, rabbit_classic_queue}, {dead_letter_strategy, disabled}],
         [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER]),
    init([{queue_type, rabbit_classic_queue}, {dead_letter_strategy, at_most_once}],
         [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER]),
    init([{queue_type, rabbit_quorum_queue}, {dead_letter_strategy, disabled}],
         [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER,
          ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT_COUNTER]),
    init([{queue_type, rabbit_quorum_queue}, {dead_letter_strategy, at_most_once}],
         [?MESSAGES_DEAD_LETTERED_MAXLEN_COUNTER,
          ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT_COUNTER]),
    init([{queue_type, rabbit_quorum_queue}, {dead_letter_strategy, at_least_once}],
         [?MESSAGES_DEAD_LETTERED_CONFIRMED_COUNTER,
          ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT_COUNTER
         ]).

init(Labels) ->
    init(Labels, []).

init(Labels = [{protocol, Protocol}, {queue_type, QueueType}], Extra) ->
    _ = seshat_counters:new_group(?MODULE),
    Counters = seshat_counters:new(?MODULE, Labels, ?PROTOCOL_QUEUE_TYPE_COUNTERS ++ Extra),
    persistent_term:put({?MODULE, Protocol, QueueType}, Counters);
init(Labels = [{protocol, Protocol}], Extra) ->
    _ = seshat_counters:new_group(?MODULE),
    Counters = seshat_counters:new(?MODULE, Labels, ?PROTOCOL_COUNTERS ++ Extra),
    persistent_term:put({?MODULE, Protocol}, Counters);
init(Labels = [{queue_type, QueueType}, {dead_letter_strategy, DLS}], Extra) ->
    _ = seshat_counters:new_group(?MODULE),
    Counters = seshat_counters:new(?MODULE, Labels, ?MESSAGES_DEAD_LETTERED_COUNTERS ++ Extra),
    persistent_term:put({?MODULE, QueueType, DLS}, Counters).

overview() ->
    seshat_counters:overview(?MODULE).

prometheus_format() ->
    seshat_counters:prometheus_format(?MODULE).

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
    counters:add(fetch(Protocol), ?PUBLISHERS, -1).

consumer_created(Protocol) ->
    counters:add(fetch(Protocol), ?CONSUMERS, 1).

consumer_deleted(Protocol) ->
    counters:add(fetch(Protocol), ?CONSUMERS, -1).

messages_dead_lettered(Reason, QueueType, DeadLetterStrategy, Num) ->
    Index = case Reason of
                expired -> ?MESSAGES_DEAD_LETTERED_EXPIRED;
                rejected -> ?MESSAGES_DEAD_LETTERED_REJECTED;
                maxlen -> ?MESSAGES_DEAD_LETTERED_MAXLEN;
                delivery_limit -> ?MESSAGES_DEAD_LETTERED_DELIVERY_LIMIT
            end,
    counters:add(fetch(QueueType, DeadLetterStrategy), Index, Num).

messages_dead_lettered_confirmed(rabbit_quorum_queue, at_least_once, Num) ->
    counters:add(fetch(rabbit_quorum_queue, at_least_once), ?MESSAGES_DEAD_LETTERED_CONFIRMED, Num).

fetch(Protocol) ->
    persistent_term:get({?MODULE, Protocol}).

fetch(Elem2, Elem3) ->
    persistent_term:get({?MODULE, Elem2, Elem3}).
