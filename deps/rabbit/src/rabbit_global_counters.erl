%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_global_counters).
-on_load(init/0).

-export([
         init/0,
         overview/0,
         prometheus_format/0,
         messages_published/1,
         messages_routed/1,
         messages_delivered_consume_ack/1,
         messages_delivered_consume_autoack/1,
         messages_delivered_get_ack/1,
         messages_delivered_get_autoack/1,
         messages_redelivered/1,
         basic_get_empty/1,
         messages_unroutable_dropped/1,
         messages_unroutable_returned/1
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
    %% Make this a test instead
    case lists:sort([ID || {_, ID, _, _} <- ?COUNTERS]) == lists:seq(1, length(?COUNTERS)) of
        false -> rabbit_log:critical("rabbit_global_counters indices are not a consequitive list of integers");
        true -> ok
    end,

    persistent_term:put(?MODULE,
                        counters:new(length(?COUNTERS), [write_concurrency])),
    ok.

overview() ->
    Counters = persistent_term:get(?MODULE),
    lists:foldl(
      fun ({Key, Index, _Type, _Description}, Acc) ->
              Acc#{Key => counters:get(Counters, Index)}
      end,
      #{},
      ?COUNTERS
     ).

prometheus_format() ->
    Counters = persistent_term:get(?MODULE),
    [{Key, counters:get(Counters, Index), Type, Help} ||
     {Key, Index, Type, Help} <- ?COUNTERS].

% TODO - these are received by queues, not from clients (doesn't account for unroutable)
messages_published(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_PUBLISHED, Num).

% formerly known as queue_messages_published_total
messages_routed(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_ROUTED, Num).

messages_delivered_consume_ack(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_DELIVERED_CONSUME_ACK, Num).

messages_delivered_consume_autoack(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_DELIVERED_CONSUME_AUTOACK, Num).

messages_delivered_get_ack(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_DELIVERED_GET_ACK, Num).

messages_delivered_get_autoack(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_DELIVERED_GET_AUTOACK, Num).

% not implemented yet
messages_redelivered(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_REDELIVERED, Num).

basic_get_empty(Num) ->
    counters:add(persistent_term:get(?MODULE), ?BASIC_GET_EMPTY, Num).

% implemented in rabbit_core_metrics (it doesn't reach a queue)
messages_unroutable_returned(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_UNROUTABLE_RETURNED, Num).

% implemented in rabbit_core_metrics (it doesn't reach a queue)
messages_unroutable_dropped(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_UNROUTABLE_DROPPED, Num).

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