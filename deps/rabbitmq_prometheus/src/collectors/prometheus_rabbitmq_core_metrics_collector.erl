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

%% Because all metrics are from RabbitMQ's perspective,
%% cached for up to 5 seconds by default (configurable),
%% we prepend rabbitmq_ to all metrics emitted by this collector.
%% Some metrics are for Erlang (erlang_), Mnesia (schema_db_) or the System (io_),
%% as observed by RabbitMQ.
-define(METRIC_NAME_PREFIX, "rabbitmq_").

%% ==The source of these metrics can be found in the rabbit_core_metrics module==
%% The relevant files are:
%% * rabbit_common/src/rabbit_core_metrics.erl
%% * rabbit_common/include/rabbit_core_metrics.hrl
%%
%% ==How to determine if a metric should be of type GAUGE or COUNTER?==
%%
%% * GAUGE if you care about its value rather than rate of change
%%   - value can decrease as well as decrease
%% * COUNTER if you care about the rate of change
%%   - value can only increase
%%
%% To put it differently, if the metric is used with rate(), it's a COUNTER, otherwise it's a GAUGE.
%%
%% More info: https://prometheus.io/docs/practices/instrumentation/#counter-vs-gauge-summary-vs-histogram
-define(METRICS_RAW, [
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
        {2, channel_messages_published_total, counter, "Total number of messages published into an exchange on a channel"},
        {3, channel_messages_confirmed_total, counter, "Total number of messages published into an exchange and confirmed on the channel"},
        {4, channel_messages_unroutable_returned_total, counter, "Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable"}
    ]},

    {channel_process_metrics, [
        {2, channel_process_reductions_total, counter, "Total number of channel process reductions"}
    ]},

    {channel_queue_metrics, [
        {2, channel_get_ack_total, counter, "Total number of messages fetched with basic.get in manual acknowledgement mode"},
        {3, channel_get_total, counter, "Total number of messages fetched with basic.get in automatic acknowledgement mode"},
        {4, channel_messages_delivered_ack_total, counter, "Total number of messages delivered to consumers in manual acknowledgement mode"},
        {5, channel_messages_delivered_total, counter, "Total number of messages delivered to consumers in automatic acknowledgement mode"},
        {6, channel_messages_redelivered_total, counter, "Total number of messages redelivered to consumers"},
        {7, channel_messages_acked_total, counter, "Total number of messages acknowledged by consumers"},
        {8, channel_get_empty_total, counter, "Total number of times basic.get operations fetched no message"}
    ]},

    {connection_churn_metrics, [
        {2, connections_opened_total, counter, "Total number of connections opened"},
        {3, connections_closed_total, counter, "Total number of connections closed or terminated"},
        {4, channels_opened_total, counter, "Total number of channels opened"},
        {5, channels_closed_total, counter, "Total number of channels closed"},
        {6, queues_declared_total, counter, "Total number of queues declared"},
        {7, queues_created_total, counter, "Total number of queues created"},
        {8, queues_deleted_total, counter, "Total number of queues deleted"}
    ]},

    {connection_coarse_metrics, [
        {2, connection_incoming_bytes_total, counter, "Total number of bytes received on a connection"},
        {3, connection_outgoing_bytes_total, counter, "Total number of bytes sent on a connection"},
        {4, connection_process_reductions_total, counter, "Total number of connection process reductions"}
    ]},

    {connection_metrics, [
        {2, connection_incoming_packets_total, counter, "Total number of packets received on a connection", recv_cnt},
        {2, connection_outgoing_packets_total, counter, "Total number of packets sent on a connection", send_cnt},
        {2, connection_pending_packets, gauge, "Number of packets waiting to be sent on a connection", send_pend},
        {2, connection_channels, gauge, "Channels on a connection", channels}
    ]},

    {channel_queue_exchange_metrics, [
        {2, queue_messages_published_total, counter, "Total number of messages published to queues"}
    ]},

    {node_coarse_metrics, [
        {2, process_open_fds, gauge, "Open file descriptors", fd_used},
        {2, process_open_tcp_sockets, gauge, "Open TCP sockets", sockets_used},
        {2, process_resident_memory_bytes, gauge, "Memory used in bytes", mem_used},
        {2, disk_space_available_bytes, gauge, "Disk space available in bytes", disk_free},
        {2, erlang_processes_used, gauge, "Erlang processes used", proc_used},
        {2, erlang_gc_runs_total, counter, "Total number of Erlang garbage collector runs", gc_num},
        {2, erlang_gc_reclaimed_bytes_total, counter, "Total number of bytes of memory reclaimed by Erlang garbage collector", gc_bytes_reclaimed},
        {2, erlang_scheduler_context_switches_total, counter, "Total number of Erlang scheduler context switches", context_switches}
    ]},

    {node_metrics, [
        {2, process_max_fds, gauge, "Open file descriptors limit", fd_total},
        {2, process_max_tcp_sockets, gauge, "Open TCP sockets limit", sockets_total},
        {2, resident_memory_limit_bytes, gauge, "Memory high watermark in bytes", mem_limit},
        {2, disk_space_available_limit_bytes, gauge, "Free disk space low watermark in bytes", disk_free_limit},
        {2, erlang_processes_limit, gauge, "Erlang processes limit", proc_total},
        {2, erlang_scheduler_run_queue, gauge, "Erlang scheduler run queue", run_queue},
        {2, erlang_net_ticktime_seconds, gauge, "Inter-node heartbeat interval in seconds", net_ticktime}
    ]},

    {node_persister_metrics, [
        {2, io_read_ops_total, counter, "Total number of I/O read operations", io_read_count},
        {2, io_read_bytes_total, counter, "Total number of I/O bytes read", io_read_bytes},
        {2, io_write_ops_total, counter, "Total number of I/O write operations", io_write_count},
        {2, io_write_bytes_total, counter, "Total number of I/O bytes written", io_write_bytes},
        {2, io_sync_ops_total, counter, "Total number of I/O sync operations", io_sync_count},
        {2, io_seek_ops_total, counter, "Total number of I/O seek operations", io_seek_count},
        {2, io_open_attempt_ops_total, counter, "Total number of file open attempts", io_file_handle_open_attempt_count},
        {2, io_reopen_ops_total, counter, "Total number of times files have been reopened", io_reopen_count},
        {2, schema_db_ram_tx_total, counter, "Total number of Schema DB memory transactions", mnesia_ram_tx_count},
        {2, schema_db_disk_tx_total, counter, "Total number of Schema DB disk transactions", mnesia_disk_tx_count},
        {2, msg_store_read_total, counter, "Total number of Message Store read operations", msg_store_read_count},
        {2, msg_store_write_total, counter, "Total number of Message Store write operations", msg_store_write_count},
        {2, queue_index_read_ops_total, counter, "Total number of Queue Index read operations", queue_index_read_count},
        {2, queue_index_write_ops_total, counter, "Total number of Queue Index write operations", queue_index_write_count},
        {2, queue_index_journal_write_ops_total, counter, "Total number of Queue Index Journal write operations", queue_index_journal_write_count}
    ]},

    {queue_coarse_metrics, [
        {2, queue_messages_ready, gauge, "Messages ready to be delivered to consumers"},
        {3, queue_messages_unacked, gauge, "Messages delivered to consumers but not yet acknowledged"},
        {4, queue_messages, gauge, "Sum of ready and unacknowledged messages - total queue depth"},
        {5, queue_process_reductions_total, counter, "Total number of queue process reductions"}
    ]},

    {queue_metrics, [
        {2, queue_consumers, gauge, "Consumers on a queue", queue_consumers},
        {2, queue_process_memory_bytes, gauge, "Memory in bytes used by the Erlang queue process", queue_memory},
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
        {2, queue_disk_reads_total, counter, "Total number of times queue read messages from disk", disk_reads},
        {2, queue_disk_writes_total, counter, "Total number of times queue wrote messages to disk", disk_writes}
    ]}
]).

% Some metrics require to be converted, mostly those that represent time.
% It is a Prometheus best practice to use specific base units: https://prometheus.io/docs/practices/naming/#base-units
% Extra context: https://github.com/prometheus/docs/pull/1414#issuecomment-522337895
-define(METRICS_REQUIRING_CONVERSIONS, [
    {node_metrics, [
        {2, 1000, erlang_uptime_seconds, gauge, "Node uptime", uptime}
    ]},

    {node_persister_metrics, [
        {2, 1000000, io_read_time_seconds_total, counter, "Total I/O read time", io_read_time},
        {2, 1000000, io_write_time_seconds_total, counter, "Total I/O write time", io_write_time},
        {2, 1000000, io_sync_time_seconds_total, counter, "Total I/O sync time", io_sync_time},
        {2, 1000000, io_seek_time_seconds_total, counter, "Total I/O seek time", io_seek_time},
        {2, 1000000, io_open_attempt_time_seconds_total, counter, "Total file open attempts time", io_file_handle_open_attempt_time}
    ]}
]).

-define(METRICS, ?METRICS_RAW ++ ?METRICS_REQUIRING_CONVERSIONS).

-define(TOTALS, [
    %% ordering differs from metrics above, refer to list comprehension
    {connection_created, connections, gauge, "Connections currently open"},
    {channel_created, channels, gauge, "Channels currently open"},
    {consumer_created, consumers, gauge, "Consumers currently connected"},
    {queue_metrics, queues, gauge, "Queues available"}
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
    add_metric_family(build_info(), Callback),
    add_metric_family(identity_info(), Callback),
    ok.

build_info() ->
    {ok, PrometheusPluginVersion} = application:get_key(rabbitmq_prometheus, vsn),
    {ok, PrometheusClientVersion} = application:get_key(prometheus, vsn),
    {
        build_info,
        untyped,
        "RabbitMQ & Erlang/OTP version info",
        [{
            [
                {rabbitmq_version, rabbit_misc:version()},
                {prometheus_plugin_version, PrometheusPluginVersion},
                {prometheus_client_version, PrometheusClientVersion},
                {erlang_version, rabbit_misc:otp_release()}
            ],
            1
        }]
    }.

identity_info() ->
    {
        identity_info,
        untyped,
        "RabbitMQ node & cluster identity info",
        [{
            [
                {rabbitmq_node, node()},
                {rabbitmq_cluster, rabbit_nodes:cluster_name()}
            ],
            1
        }]
    }.

add_metric_family({Name, Type, Help, Metrics}, Callback) ->
  Callback(create_mf(?METRIC_NAME(Name), Help, Type, Metrics)).

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
    end || {Index, Name, Type, Help, Key} <- Contents],
    [begin
        Fun = fun(D) -> proplists:get_value(Key, element(Index, D)) / BaseUnitConversionFactor end,
        Callback(
            create_mf(
                ?METRIC_NAME(Name),
                Help,
                catch_boolean(Type),
                ?MODULE,
                {Type, Fun, Data}
            )
        )
    end || {Index, BaseUnitConversionFactor, Name, Type, Help, Key} <- Contents].

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
