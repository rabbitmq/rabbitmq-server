%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_messages_counters).
-on_load(init/0).

-export([
         init/0,
         new/3,
         messages_published/3,
         messages_routed/3,
         messages_delivered_consume_ack/3,
         messages_delivered_consume_autoack/3,
         messages_delivered_get_ack/3,
         messages_delivered_get_autoack/3,
         messages_redelivered/3,
         basic_get_empty/3,
         messages_unroutable_dropped/3,
         messages_unroutable_returned/3
       ]).


-define(MESSAGES_PUBLISHED, 1).
-define(MESSAGES_ROUTED, 2).
-define(MESSAGES_DELIVERED_CONSUME_ACK, 3).
-define(MESSAGES_DELIVERED_CONSUME_AUTOACK, 4).
-define(MESSAGES_DELIVERED_GET_ACK, 5).
-define(MESSAGES_DELIVERED_GET_AUTOACK, 6).
-define(MESSAGES_REDELIVERED, 7).
-define(BASIC_GET_EMPTY, 8).
-define(MESSAGES_UNROUTABLE_DROPPED, 9).
-define(MESSAGES_UNROUTABLE_RETURNED, 10).

-define(COUNTERS,
            [
                {
                    messages_published_total, ?MESSAGES_PUBLISHED, counter,
                    "Total number of messages published to queues and streams"
                },
                {
                    messages_routed_total, ?MESSAGES_ROUTED, counter,
                    "Total number of messages routed to queues"
                },
                {
                    messages_delivered_consume_ack_total, ?MESSAGES_DELIVERED_CONSUME_ACK, counter,
                    "Total number of messages consumed using basic.consume with manual acknowledgment"
                },
                {
                    messages_delivered_consume_autoack_total, ?MESSAGES_DELIVERED_CONSUME_AUTOACK, counter,
                    "Total number of messages consumed using basic.consume with automatic acknowledgment"
                },
                {
                    messages_delivered_get_ack_total, ?MESSAGES_DELIVERED_GET_ACK, counter,
                    "Total number of messages consumed using basic.get with manual acknowledgment"
                },
                {
                    messages_delivered_get_autoack_total, ?MESSAGES_DELIVERED_GET_AUTOACK, counter,
                    "Total number of messages consumed using basic.get with automatic acknowledgment"
                },
                {
                    messages_redelivered_total, ?MESSAGES_REDELIVERED, counter,
                    "Total number of messages redelivered to consumers"
                },
                {
                    basic_get_empty_total, ?BASIC_GET_EMPTY, counter,
                    "Total number of times basic.get operations fetched no message"
                },
                {
                    messages_unroutable_dropped_total, ?MESSAGES_UNROUTABLE_DROPPED, counter,
                    "Total number of messages published as non-mandatory into an exchange and dropped as unroutable"
                },
                {
                    messages_unroutable_returned_total, ?MESSAGES_UNROUTABLE_RETURNED, counter,
                   "Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable"
                }
            ]).

init() ->
    seshat_counters:new_group(queue),
    ok.

new(Group, Object, Fields) ->
    %% Some object could have extra metrics, i.e. queue types might have their own counters
    seshat_counters:new(Group, Object, ?COUNTERS ++ Fields).

% TODO - these are received by queues, not from clients (doesn't account for unroutable)
messages_published(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_PUBLISHED, Num).

% formerly known as queue_messages_published_total
messages_routed(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_ROUTED, Num).

messages_delivered_consume_ack(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_DELIVERED_CONSUME_ACK, Num).

messages_delivered_consume_autoack(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_DELIVERED_CONSUME_AUTOACK, Num).

messages_delivered_get_ack(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_DELIVERED_GET_ACK, Num).

messages_delivered_get_autoack(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_DELIVERED_GET_AUTOACK, Num).

% not implemented yet
messages_redelivered(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_REDELIVERED, Num).

basic_get_empty(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?BASIC_GET_EMPTY, Num).

% implemented in rabbit_core_metrics (it doesn't reach a queue)
messages_unroutable_returned(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_UNROUTABLE_RETURNED, Num).

% implemented in rabbit_core_metrics (it doesn't reach a queue)
messages_unroutable_dropped(Group, Object, Num) ->
    counters:add(seshat_counters:fetch(Group, Object), ?MESSAGES_UNROUTABLE_DROPPED, Num).
