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
    seshat_counters:new_group(global),
    new(global, global, []),
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

% TODO
%  channel_messages_redelivered_total "Total number of messages redelivered to consumers"
% 
%  connection_incoming_bytes_total "Total number of bytes received on a connection"
%  connection_outgoing_bytes_total "Total number of bytes sent on a connection"
%  connection_process_reductions_total "Total number of connection process reductions"
%  connection_incoming_packets_total "Total number of packets received on a connection"
%  connection_outgoing_packets_total "Total number of packets sent on a connection"
% 
%  io_read_ops_total "Total number of I/O read operations"
%  io_read_bytes_total "Total number of I/O bytes read"
%  io_write_ops_total "Total number of I/O write operations"
%  io_write_bytes_total "Total number of I/O bytes written"
%  io_sync_ops_total "Total number of I/O sync operations"
%  io_seek_ops_total "Total number of I/O seek operations"
%  io_open_attempt_ops_total "Total number of file open attempts"
%  io_reopen_ops_total "Total number of times files have been reopened"
% 
%  schema_db_ram_tx_total "Total number of Schema DB memory transactions"
%  schema_db_disk_tx_total "Total number of Schema DB disk transactions"
%  msg_store_read_total "Total number of Message Store read operations"
%  msg_store_write_total "Total number of Message Store write operations"
%  queue_index_read_ops_total "Total number of Queue Index read operations"
%  queue_index_write_ops_total "Total number of Queue Index write operations"
%  queue_index_journal_write_ops_total "Total number of Queue Index Journal write operations"
%  io_read_time_seconds_total "Total I/O read time"
%  io_write_time_seconds_total "Total I/O write time"
%  io_sync_time_seconds_total "Total I/O sync time"
%  io_seek_time_seconds_total "Total I/O seek time"
%  io_open_attempt_time_seconds_total "Total file open attempts time"
%  raft_term_total "Current Raft term number"
%  queue_disk_reads_total "Total number of times queue read messages from disk"
%  queue_disk_writes_total "Total number of times queue wrote messages to disk"

% DONE
%  channel_messages_published_total "Total number of messages published into an exchange on a channel"
%  channel_messages_confirmed_total "Total number of messages published into an exchange and confirmed on the channel"
%  channel_messages_unroutable_returned_total "Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable"
%  channel_messages_unroutable_dropped_total "Total number of messages published as non-mandatory into an exchange and dropped as unroutable"
%  channel_get_empty_total "Total number of times basic.get operations fetched no message"
%  channel_get_ack_total "Total number of messages fetched with basic.get in manual acknowledgement mode"
%  channel_get_total "Total number of messages fetched with basic.get in automatic acknowledgement mode"
%  channel_messages_delivered_ack_total "Total number of messages delivered to consumers in manual acknowledgement mode"
%  channel_messages_delivered_total "Total number of messages delivered to consumers in automatic acknowledgement mode"
%  queue_messages_published_total "Total number of messages published to queues"

% IGNORED (IS THIS USEFUL?)
%  channel_process_reductions_total "Total number of channel process reductions"
%  queue_process_reductions_total "Total number of queue process reductions"

% NOT NECESSARY (DON'T GO TO ZERO)
%  erlang_gc_runs_total "Total number of Erlang garbage collector runs"
%  erlang_gc_reclaimed_bytes_total "Total number of bytes of memory reclaimed by Erlang garbage collector"
%  erlang_scheduler_context_switches_total "Total number of Erlang scheduler context switches"
%  connections_opened_total "Total number of connections opened"
%  connections_closed_total "Total number of connections closed or terminated"
%  channels_opened_total "Total number of channels opened"
%  channels_closed_total "Total number of channels closed"
%  queues_declared_total "Total number of queues declared"
%  queues_created_total "Total number of queues created"
%  queues_deleted_total "Total number of queues deleted"
%  auth_attempts_total "Total number of authorization attempts"
%  auth_attempts_succeeded_total "Total number of successful authentication attempts"
%  auth_attempts_failed_total "Total number of failed authentication attempts"
%  auth_attempts_detailed_total "Total number of authorization attempts with source info"
%  auth_attempts_detailed_succeeded_total "Total number of successful authorization attempts with source info"
%  auth_attempts_detailed_failed_total "Total number of failed authorization attempts with source info"
