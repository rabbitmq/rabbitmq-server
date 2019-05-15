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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%
-module(prometheus_rabbitmq_core_metrics_collector).
-export([register/0,
         deregister_cleanup/1,
         collect_mf/2,
         collect_metrics/2]).

-import(prometheus_model_helpers, [create_mf/4,
                                   create_mf/5,
                                   gauge_metric/2,
                                   counter_metric/2,
                                   untyped_metric/2]).

-include_lib("prometheus/include/prometheus.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(prometheus_collector).

-define(METRIC_NAME_PREFIX, "rabbitmq_").

% The source of these metrics are in the rabbit_core_metrics module
% The relevant files are:
% * rabbit_common/src/rabbit_core_metrics.erl
% * rabbit_common/include/rabbit_core_metrics.hrl
-define(METRICS, [
    {channel_metrics, [
        {2, channel_consumers, gauge, "Consumers on a channel", consumer_count},
        {2, channel_messages_unacked, gauge, "Delivered but not yet acknowledged messages", messages_unacknowledged},
        {2, channel_messages_unconfirmed, gauge, "Published but not yet confirmed messages", messages_unconfirmed},
        {2, channel_messages_uncommitted, gauge, "Messages received in a transaction but not yet committed", messages_uncommitted},
        {2, channel_acks_uncommitted, gauge, "Message acknowledgements in a transaction not yet committed", acks_uncommitted},
        {2, consumer_prefetch, gauge, "Limit of unacknowledged messages for each consumer", prefetch_count},
        {2, channel_prefetch, gauge, "Total limit of unacknowledged messages for all consumers on a channel", global_prefetch_count}
    ]},

    {channel_exchange_metrics, [
        {2, channel_messages_published, gauge, "Messages published into an exchange on a channel"},
        {3, channel_messages_confirmed, gauge, "Messages published into an exchange and confirmed on the channel"},
        {4, channel_messages_unroutable_returned, gauge, "Messages published as mandatory into an exchange and returned to the publisher as unroutable"},
        {5, channel_messages_unroutable_dropped, gauge, "Messages published as non-mandatory into an exchange and dropped as unroutable"}
    ]},

    {channel_process_metrics, [
        {2, erlang_process_reductions, counter, "Erlang process reductions"}
    ]},

    {channel_queue_metrics, [
        {2, channel_get_ack, counter, "Messages fetched with basic.get in manual acknowledgement mode"},
        {3, channel_get, counter, "Messages fetched with basic.get in automatic acknowledgement mode"},
        {4, channel_messages_delivered_ack, counter, "Messages delivered to consumers in manual acknowledgement mode"},
        {5, channel_messages_delivered, counter, "Messages delivered to consumers in automatic acknowledgement mode"},
        {6, channel_messages_redelivered, counter, "Messages redelivered to consumers"},
        {7, channel_messages_acked, counter, "Messages acknowledged by consumers"},
        {8, channel_get_empty, counter, "Number of times basic.get operations fetched no message"}
    ]},

    {connection_churn_metrics, [
        {2, connection_open, counter, "Connections opened"},
        {3, connection_close, counter, "Connections closed or terminated"},
        {4, channel_open, counter, "Channels opened"},
        {5, channel_close, counter, "Channels closed"},
        {6, queue_declare, counter, "Queues declared"},
        {7, queue_create, counter, "Queues created"},
        {8, queue_delete, counter, "Queues deleted"}
    ]},

    {connection_coarse_metrics, [
        {2, connection_bytes_in, counter, "Bytes received on a connection"},
        {3, connection_bytes_out, counter, "Bytes sent on a connection"},
        {4, erlang_process_reductions, counter, "Erlang process reductions"}
    ]},

    {connection_metrics, [
        {2, connection_packets_in, counter, "Packets received on a connection", recv_cnt},
        {2, connection_packets_out, counter, "Packets sent on a connection", send_cnt},
        {2, connection_packets_pending, counter, "Packets waiting to be sent on a connection", send_pend},
        {2, connection_channels, gauge, "Channels on a connection", channels}
    ]},

    {channel_queue_exchange_metrics, [
        {2, queue_messages_published, counter, "Messages published to queues"}
    ]},

    {node_coarse_metrics, [
        {2, file_descriptors_open, gauge, "Open file descriptors", fd_used},
        {2, tcp_sockets_open, gauge, "Open TCP sockets", sockets_used},
        {2, memory_used_bytes, gauge, "Memory used in bytes", mem_used},
        {2, disk_space_available_bytes, gauge, "Disk space available in bytes", disk_free},
        {2, erlang_processes_used, gauge, "Erlang processes used", proc_used},
        {2, erlang_gc_runs, counter, "Number of Erlang garbage collector runs", gc_num},
        {2, erlang_gc_bytes_reclaimed, counter, "Bytes of memory reclaimed by Erlang garbage collector", gc_bytes_reclaimed},
        {2, erlang_scheduler_context_switches, counter, "Erlang scheduler context switches", context_switches}
    ]},

    {node_metrics, [
        {2, file_descriptors_open_limit, gauge, "Open file descriptors limit", fd_total},
        {2, tcp_sockets_open_limit, gauge, "Open TCP sockets limit", sockets_total},
        {2, memory_used_limit_bytes, gauge, "Memory high watermark in bytes", mem_limit},
        {2, disk_space_available_limit_bytes, gauge, "Free disk space low watermark in bytes", disk_free_limit},
        {2, erlang_processes_limit, gauge, "Erlang processes limit", proc_total},
        {2, uptime_milliseconds, gauge, "Node uptime in milliseconds", uptime},
        {2, erlang_scheduler_run_queue, gauge, "Erlang scheduler run queue", run_queue},
        {2, erlang_net_ticktime_seconds, gauge, "Inter-node heartbeat interval in seconds", net_ticktime}
    ]},

    {node_node_metrics, [
        {2, peer_node_bytes_in, counter, "Bytes received from peer node", recv_bytes},
        {2, peer_node_bytes_out, counter, "Bytes sent to peer node", send_bytes}
    ]},

    {node_persister_metrics, [
        {2, io_read, counter, "I/O read operations", io_read_count},
        {2, io_read_bytes, gauge, "I/O bytes read", io_read_bytes},
        {2, io_read_time_microseconds, gauge, "I/O read total time in microseconds", io_read_time},
        {2, io_write, counter, "I/O write operations", io_write_count},
        {2, io_write_bytes, gauge, "I/O bytes written", io_write_bytes},
        {2, io_write_time_microseconds, gauge, "I/O write total time in microseconds", io_write_time},
        {2, io_sync, counter, "I/O sync operations", io_sync_count},
        {2, io_sync_time_microseconds, gauge, "I/O sync total time in microseconds", io_sync_time},
        {2, io_seek, counter, "I/O seek operations", io_seek_count},
        {2, io_seek_time_microseconds, gauge, "I/O seek total time in microseconds", io_seek_time},
        {2, io_open_attempt, counter, "File open attempts", io_file_handle_open_attempt_count},
        {2, io_open_attempt_time_microseconds, gauge, "File open attempts total time in microseconds", io_file_handle_open_attempt_time},
        {2, io_reopen, counter, "Number of times files have been reopened", io_reopen_count},
        {2, schema_db_ram_tx, counter, "Schema DB memory transactions", mnesia_ram_tx_count},
        {2, schema_db_disk_tx, counter, "Schema DB disk transactions", mnesia_disk_tx_count},
        {2, msg_store_read, counter, "Message store read operations", msg_store_read_count},
        {2, msg_store_write, counter, "Message store write operations", msg_store_write_count},
        {2, queue_index_read, counter, "Queue Index read operations", queue_index_read_count},
        {2, queue_index_write, counter, "Queue Index write operations", queue_index_write_count},
        {2, queue_index_journal_write, counter, "Queue Index Journal write operations", queue_index_journal_write_count}
    ]},

    {queue_coarse_metrics, [
        {2, queue_messages_ready, gauge, "Messages ready to be delivered to consumers"},
        {3, queue_messages_unacked, gauge, "Messages delivered to consumers but not yet acknowledged"},
        {4, queue_messages, gauge, "Sum of ready and unacknowledged messages - total queue depth"},
        {5, erlang_process_reductions, counter, "Erlang process reductions"}
    ]},

    {queue_metrics, [
        {2, queue_consumers, gauge, "Consumers on a queue", queue_consumers},
        {2, erlang_process_memory_bytes, gauge, "Memory in bytes used by the Erlang queue process", queue_memory},
        {2, queue_messages_bytes, gauge, "Size in bytes of ready and unacknowledged messages", queue_messages_bytes},
        {2, queue_messages_ram, gauge, "Ready and unacknowledged messages stored in memory", queue_messages_ram},
        {2, queue_messages_ready_ram, gauge, "Ready messages stored in memory", queue_messages_ready_ram},
        {2, queue_messages_ready_bytes, gauge, "Size in bytes of ready messages", queue_messages_bytes_ready},
        {2, queue_messages_unacked_ram, gauge, "Unacknowledged messages stored in memory", queue_messages_unacknowledged_ram},
        {2, queue_messages_unacked_bytes, gauge, "Size in bytes of all unacknowledged messages", queue_messages_bytes_unacknowledged},
        {2, queue_messages_persistent, gauge, "Persistent messages", queue_messages_persistent},
        {2, queue_messages_persistent_bytes, gauge, "Size in bytes of persistent messages", queue_messages_bytes_persistent},
        {2, queue_messages_paged_out, gauge, "Messages paged out to disk", queue_messages_paged_out},
        {2, queue_messages_paged_out_bytes, gauge, "Size in bytes of messages paged out to disk", queue_messages_bytes_paged_out},
        {2, queue_disk_reads, gauge, "Number of times queue read messages from disk", disk_reads},
        {2, queue_disk_writes, gauge, "Number of times queue wrote messages to disk", disk_writes}
    ]}
]).

-define(TOTALS, [
    {connection_created, connections, gauge, "Total number of connections"},
    {channel_created, channels, gauge, "Total number of channels"},
    {consumer_created, consumers, gauge, "Total number of consumers"},
    {queue_metrics, queues, gauge, "Total number of queues"}
]).

%%====================================================================
%% Collector API
%%====================================================================

register() ->
  ok = prometheus_registry:register_collector(?MODULE).

deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    [begin
         Data = ets:tab2list(Table),
         mf(Callback, Contents, Data)
     end || {Table, Contents} <- ?METRICS],
    [begin
         Size = ets:info(Table, size),
         mf_totals(Callback, Name, Type, Help, Size)
     end || {Table, Name, Type, Help} <- ?TOTALS],
    ok.

mf(Callback, Contents, Data) ->
    [begin
        Fun = fun(D) -> element(Index, D) end,
        Callback(
            create_mf(
                ?METRIC_NAME(Name),
                Help,
                catch_boolean(Type),
                ?MODULE,
                {Type, Fun, Data}
            )
        )
    end || {Index, Name, Type, Help} <- Contents],
    [begin
        Fun = fun(D) -> proplists:get_value(Key, element(Index, D)) end,
        Callback(
            create_mf(
                ?METRIC_NAME(Name),
                Help,
                catch_boolean(Type),
                ?MODULE,
                {Type, Fun, Data}
            )
        )
    end || {Index, Name, Type, Help, Key} <- Contents].

mf_totals(Callback, Name, Type, Help, Size) ->
    Callback(
        create_mf(
            ?METRIC_NAME(Name),
            Help,
            catch_boolean(Type),
            Size
        )
    ).

collect_metrics(_, {Type, Fun, Items}) ->
    [metric(Type, labels(Item), Fun(Item)) || Item <- Items].

labels(Item) ->
    label(element(1, Item)).

label(#resource{virtual_host = VHost, kind = exchange, name = Name}) ->
    [{vhost, VHost}, {exchange, Name}];
label(#resource{virtual_host = VHost, kind = queue, name = Name}) ->
    [{vhost, VHost}, {queue, Name}];
label({P, {#resource{virtual_host = QVHost, kind = queue, name = QName},
            #resource{virtual_host = EVHost, kind = exchange, name = EName}}}) when is_pid(P) ->
    %% channel_queue_exchange_metrics {channel_id, {queue_id, exchange_id}}
    [{channel, P}, {queue_vhost, QVHost}, {queue, QName},
     {exchange_vhost, EVHost}, {exchange, EName}];
label({I1, I2}) ->
    label(I1) ++ label(I2);
label(P) when is_pid(P) ->
    [{channel, P}];
label(A) when is_atom(A) ->
    [].

metric(counter, Labels, Value) ->
    emit_counter_metric_if_defined(Labels, Value);
metric(gauge, Labels, Value) ->
    emit_gauge_metric_if_defined(Labels, Value);
metric(untyped, Labels, Value) ->
    untyped_metric(Labels, Value);
metric(boolean, Labels, Value0) ->
    Value = case Value0 of
                true -> 1;
                false -> 0;
                undefined -> undefined
            end,
    untyped_metric(Labels, Value).

%%====================================================================
%% Private Parts
%%====================================================================
catch_boolean(boolean) ->
        untyped;
catch_boolean(T) ->
        T.

emit_counter_metric_if_defined(Labels, Value) ->
    case Value of
        undefined -> undefined;
        '' ->
            counter_metric(Labels, undefined);
        Value ->
            counter_metric(Labels, Value)
    end.

emit_gauge_metric_if_defined(Labels, Value) ->
  case Value of
    undefined -> undefined;
    '' ->
      gauge_metric(Labels, undefined);
    Value ->
      gauge_metric(Labels, Value)
  end.
