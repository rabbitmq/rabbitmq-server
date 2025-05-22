%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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

-import(prometheus_text_format, [escape_label_value/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(prometheus_collector).
%% We prepend either rabbitmq_ or rabbitmq_detailed_ to all metrics emitted by this collector.
%% As there are also some metrics for Erlang (erlang_), Mnesia (schema_db_) or the System (io_),
%% as observed by RabbitMQ.

%% Used by `/metrics` and `/metrics/per-object`.
-define(METRIC_NAME_PREFIX, <<"rabbitmq_">>).

%% Used by `/metrics/detailed` endpoint
-define(DETAILED_METRIC_NAME_PREFIX, <<"rabbitmq_detailed_">>).
-define(CLUSTER_METRIC_NAME_PREFIX, <<"rabbitmq_cluster_">>).

%% ==The source of these metrics can be found in the rabbit_core_metrics module==
%% The relevant files are:
%% * rabbit_common/src/rabbit_core_metrics.erl
%% * rabbit_common/include/rabbit_core_metrics.hrl
%%
%% ==How to determine if a metric should be of type GAUGE or COUNTER?==
%%
%% * GAUGE if you care about its value rather than rate of change
%%   - value can increase as well as decrease
%% * COUNTER if you care about the rate of change
%%   - value can only increase
%%
%% To put it differently, if the metric is used with rate(), it's a COUNTER, otherwise it's a GAUGE.
%%
%% More info: https://prometheus.io/docs/practices/instrumentation/#counter-vs-gauge-summary-vs-histogram

% Some metrics require to be converted, mostly those that represent time.
% It is a Prometheus best practice to use specific base units: https://prometheus.io/docs/practices/naming/#base-units
% Extra context: https://github.com/prometheus/docs/pull/1414#issuecomment-522337895

-define(MILLISECOND, 1000).
-define(MICROSECOND, 1000000).

-define(METRICS_RAW, [

    %% Global metrics, as in, they contain no references to queues, virtual hosts or channel
    {connection_churn_metrics, [
        {2, undefined, connections_opened_total, counter, "Total number of connections opened"},
        {3, undefined, connections_closed_total, counter, "Total number of connections closed or terminated"},
        {4, undefined, channels_opened_total, counter, "Total number of channels opened"},
        {5, undefined, channels_closed_total, counter, "Total number of channels closed"},
        {6, undefined, queues_declared_total, counter, "Total number of queues declared"},
        {7, undefined, queues_created_total, counter, "Total number of queues created"},
        {8, undefined, queues_deleted_total, counter, "Total number of queues deleted"}
    ]},
    {node_coarse_metrics, [
        {2, undefined, process_open_fds, gauge, "Open file descriptors", fd_used},
        {2, undefined, process_resident_memory_bytes, gauge, "Memory used in bytes", mem_used},
        {2, undefined, disk_space_available_bytes, gauge, "Disk space available in bytes", disk_free},
        {2, undefined, erlang_processes_used, gauge, "Erlang processes used", proc_used},
        {2, undefined, erlang_gc_runs_total, counter, "Total number of Erlang garbage collector runs", gc_num},
        {2, undefined, erlang_gc_reclaimed_bytes_total, counter, "Total number of bytes of memory reclaimed by Erlang garbage collector", gc_bytes_reclaimed},
        {2, undefined, erlang_scheduler_context_switches_total, counter, "Total number of Erlang scheduler context switches", context_switches}
    ]},
    {node_metrics, [
        {2, undefined, process_max_fds, gauge, "Open file descriptors limit", fd_total},
        {2, undefined, resident_memory_limit_bytes, gauge, "Memory high watermark in bytes", mem_limit},
        {2, undefined, disk_space_available_limit_bytes, gauge, "Free disk space low watermark in bytes", disk_free_limit},
        {2, undefined, erlang_processes_limit, gauge, "Erlang processes limit", proc_total},
        {2, undefined, erlang_scheduler_run_queue, gauge, "Erlang scheduler run queue", run_queue},
        {2, undefined, erlang_net_ticktime_seconds, gauge, "Inter-node heartbeat interval", net_ticktime},
        {2, ?MILLISECOND, erlang_uptime_seconds, gauge, "Node uptime", uptime}
    ]},

    {node_persister_metrics, [
        {2, undefined, io_read_ops_total, counter, "Total number of I/O read operations", io_read_count},
        {2, undefined, io_read_bytes_total, counter, "Total number of I/O bytes read", io_read_bytes},
        {2, undefined, io_write_ops_total, counter, "Total number of I/O write operations", io_write_count},
        {2, undefined, io_write_bytes_total, counter, "Total number of I/O bytes written", io_write_bytes},
        {2, undefined, io_sync_ops_total, counter, "Total number of I/O sync operations", io_sync_count},
        {2, undefined, io_seek_ops_total, counter, "Total number of I/O seek operations", io_seek_count},
        {2, undefined, io_reopen_ops_total, counter, "Total number of times files have been reopened", io_reopen_count},
        {2, undefined, schema_db_ram_tx_total, counter, "Total number of Schema DB memory transactions", mnesia_ram_tx_count},
        {2, undefined, schema_db_disk_tx_total, counter, "Total number of Schema DB disk transactions", mnesia_disk_tx_count},
        {2, undefined, msg_store_read_total, counter, "Total number of Message Store read operations", msg_store_read_count},
        {2, undefined, msg_store_write_total, counter, "Total number of Message Store write operations", msg_store_write_count},
        {2, undefined, queue_index_read_ops_total, counter, "Total number of Queue Index read operations", queue_index_read_count},
        {2, undefined, queue_index_write_ops_total, counter, "Total number of Queue Index write operations", queue_index_write_count},
        {2, ?MICROSECOND, io_read_time_seconds_total, counter, "Total I/O read time", io_read_time},
        {2, ?MICROSECOND, io_write_time_seconds_total, counter, "Total I/O write time", io_write_time},
        {2, ?MICROSECOND, io_sync_time_seconds_total, counter, "Total I/O sync time", io_sync_time},
        {2, ?MICROSECOND, io_seek_time_seconds_total, counter, "Total I/O seek time", io_seek_time}
    ]},

    {auth_attempt_metrics, [
        {2, undefined, auth_attempts_total, counter, "Total number of authentication attempts"},
        {3, undefined, auth_attempts_succeeded_total, counter, "Total number of successful authentication attempts"},
        {4, undefined, auth_attempts_failed_total, counter, "Total number of failed authentication attempts"}
    ]},

    {auth_attempt_detailed_metrics, [
        {2, undefined, auth_attempts_detailed_total, counter, "Total number of authentication attempts with source info"},
        {3, undefined, auth_attempts_detailed_succeeded_total, counter, "Total number of successful authentication attempts with source info"},
        {4, undefined, auth_attempts_detailed_failed_total, counter, "Total number of failed authentication attempts with source info"}
    ]},

    %%% These metrics only reference a queue name. This is the only group where filtering (e.g. by vhost) makes sense.
    {queue_coarse_metrics, [
        {2, undefined, queue_messages_ready, gauge, "Messages ready to be delivered to consumers"},
        {3, undefined, queue_messages_unacked, gauge, "Messages delivered to consumers but not yet acknowledged"},
        {4, undefined, queue_messages, gauge, "Sum of ready and unacknowledged messages - total queue depth"},
        {5, undefined, queue_process_reductions_total, counter, "Total number of queue process reductions"}
    ]},

    {queue_consumer_count, [
        {2, undefined, queue_consumers, gauge, "Consumers on a queue", consumers}
    ]},

    {queue_metrics, [
        {2, undefined, queue_consumers, gauge, "Consumers on a queue", consumers},
        {2, undefined, queue_consumer_capacity, gauge, "Consumer capacity", consumer_capacity},
        {2, undefined, queue_consumer_utilisation, gauge, "Same as consumer capacity", consumer_utilisation},
        {2, undefined, queue_process_memory_bytes, gauge, "Memory in bytes used by the Erlang queue process", memory},
        {2, undefined, queue_messages_ram, gauge, "Ready and unacknowledged messages stored in memory", messages_ram},
        {2, undefined, queue_messages_ram_bytes, gauge, "Size of ready and unacknowledged messages stored in memory", message_bytes_ram},
        {2, undefined, queue_messages_ready_ram, gauge, "Ready messages stored in memory", messages_ready_ram},
        {2, undefined, queue_messages_unacked_ram, gauge, "Unacknowledged messages stored in memory", messages_unacknowledged_ram},
        {2, undefined, queue_messages_persistent, gauge, "Persistent messages", messages_persistent},
        {2, undefined, queue_messages_persistent_bytes, gauge, "Size in bytes of persistent messages", message_bytes_persistent},
        {2, undefined, queue_messages_bytes, gauge, "Size in bytes of ready and unacknowledged messages", message_bytes},
        {2, undefined, queue_messages_ready_bytes, gauge, "Size in bytes of ready messages", message_bytes_ready},
        {2, undefined, queue_messages_unacked_bytes, gauge, "Size in bytes of all unacknowledged messages", message_bytes_unacknowledged},
        {2, undefined, queue_messages_paged_out, gauge, "Messages paged out to disk", messages_paged_out},
        {2, undefined, queue_messages_paged_out_bytes, gauge, "Size in bytes of messages paged out to disk", message_bytes_paged_out},
        {2, undefined, queue_head_message_timestamp, gauge, "Timestamp of the first message in the queue, if any", head_message_timestamp},
        {2, undefined, queue_disk_reads_total, counter, "Total number of times queue read messages from disk", disk_reads},
        {2, undefined, queue_disk_writes_total, counter, "Total number of times queue wrote messages to disk", disk_writes},
        {2, undefined, stream_segments, counter, "Total number of stream segment files", segments}
    ]},
%%% Metrics that contain reference to a channel. Some of them also have
%%% a queue name, but in this case filtering on it doesn't make any
%%% sense, as the queue is not an object of interest here.
    {channel_metrics, [
        {2, undefined, channel_consumers, gauge, "Consumers on a channel", consumer_count},
        {2, undefined, channel_messages_unacked, gauge, "Delivered but not yet acknowledged messages", messages_unacknowledged},
        {2, undefined, channel_messages_unconfirmed, gauge, "Published but not yet confirmed messages", messages_unconfirmed},
        {2, undefined, channel_messages_uncommitted, gauge, "Messages received in a transaction but not yet committed", messages_uncommitted},
        {2, undefined, channel_acks_uncommitted, gauge, "Message acknowledgements in a transaction not yet committed", acks_uncommitted},
        {2, undefined, consumer_prefetch, gauge, "Limit of unacknowledged messages for each consumer", prefetch_count},
        {2, undefined, channel_prefetch, gauge, "Deprecated and will be removed in a future version", global_prefetch_count}
    ]},

    {channel_exchange_metrics, [
        {2, undefined, channel_messages_published_total, counter, "Total number of messages published into an exchange on a channel"},
        {3, undefined, channel_messages_confirmed_total, counter, "Total number of messages published into an exchange and confirmed on the channel"},
        {4, undefined, channel_messages_unroutable_returned_total, counter, "Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable"},
        {5, undefined, channel_messages_unroutable_dropped_total, counter, "Total number of messages published as non-mandatory into an exchange and dropped as unroutable"}
    ]},

    {channel_process_metrics, [
        {2, undefined, channel_process_reductions_total, counter, "Total number of channel process reductions"}
    ]},

    {channel_queue_metrics, [
        {2, undefined, channel_get_ack_total, counter, "Total number of messages fetched with basic.get in manual acknowledgement mode"},
        {3, undefined, channel_get_total, counter, "Total number of messages fetched with basic.get in automatic acknowledgement mode"},
        {4, undefined, channel_messages_delivered_ack_total, counter, "Total number of messages delivered to consumers in manual acknowledgement mode"},
        {5, undefined, channel_messages_delivered_total, counter, "Total number of messages delivered to consumers in automatic acknowledgement mode"},
        {6, undefined, channel_messages_redelivered_total, counter, "Total number of messages redelivered to consumers"},
        {7, undefined, channel_messages_acked_total, counter, "Total number of messages acknowledged by consumers"},
        {8, undefined, channel_get_empty_total, counter, "Total number of times basic.get operations fetched no message"}
    ]},

    {connection_coarse_metrics, [
        {2, undefined, connection_incoming_bytes_total, counter, "Total number of bytes received on a connection"},
        {3, undefined, connection_outgoing_bytes_total, counter, "Total number of bytes sent on a connection"},
        {4, undefined, connection_process_reductions_total, counter, "Total number of connection process reductions"}
    ]},

    %% the family name for this metric is stream_consumer_metrics but the real table used for data is rabbit_stream_consumer_created.
    {stream_consumer_metrics, [
        {2, undefined, stream_consumer_max_offset_lag, gauge, "Highest consumer offset lag"}
    ]},

    {connection_metrics, [
        {2, undefined, connection_incoming_packets_total, counter, "Total number of packets received on a connection", recv_cnt},
        {2, undefined, connection_outgoing_packets_total, counter, "Total number of packets sent on a connection", send_cnt},
        {2, undefined, connection_pending_packets, gauge, "Number of packets waiting to be sent on a connection", send_pend},
        {2, undefined, connection_channels, gauge, "Channels on a connection", channels}
    ]},

    {channel_queue_exchange_metrics, [
        {2, undefined, queue_messages_published_total, counter, "Total number of messages published into a queue through an exchange on a channel"}
    ]},

%%% Metrics in the following 3 groups reference a queue and/or exchange.
%%% They each have a corresponding group in the above per-channel
%%% section but here the channel is not an object of interest.
    {exchange_metrics, [
        {2, undefined, exchange_messages_published_total, counter, "Total number of messages published into an exchange"},
        {3, undefined, exchange_messages_confirmed_total, counter, "Total number of messages published into an exchange and confirmed"},
        {4, undefined, exchange_messages_unroutable_returned_total, counter, "Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable"},
        {5, undefined, exchange_messages_unroutable_dropped_total, counter, "Total number of messages published as non-mandatory into an exchange and dropped as unroutable"}
    ]},

    {queue_delivery_metrics, [
        {2, undefined, queue_get_ack_total, counter, "Total number of messages fetched from a queue with basic.get in manual acknowledgement mode"},
        {3, undefined, queue_get_total, counter, "Total number of messages fetched from a queue with basic.get in automatic acknowledgement mode"},
        {4, undefined, queue_messages_delivered_ack_total, counter, "Total number of messages delivered from a queue to consumers in manual acknowledgement mode"},
        {5, undefined, queue_messages_delivered_total, counter, "Total number of messages delivered from a queue to consumers in automatic acknowledgement mode"},
        {6, undefined, queue_messages_redelivered_total, counter, "Total number of messages redelivered from a queue to consumers"},
        {7, undefined, queue_messages_acked_total, counter, "Total number of messages acknowledged by consumers on a queue"},
        {8, undefined, queue_get_empty_total, counter, "Total number of times basic.get operations fetched no message on a queue"}
    ]},

    {queue_exchange_metrics, [
        {2, undefined, queue_exchange_messages_published_total, counter, "Total number of messages published into a queue through an exchange"}
    ]}]).

%% Metrics that can be only requested through `/metrics/detailed`
-define(METRICS_CLUSTER,[
    {vhost_status, [
        {2, undefined, vhost_status, gauge, "Whether a given vhost is running"}
    ]},
    {exchange_bindings, [
        {2, undefined, exchange_bindings, gauge, "Number of bindings for an exchange. This value is cluster-wide."}
    ]},
    {exchange_names, [
        {2, undefined, exchange_name, gauge, "Enumerates exchanges without any additional info. This value is cluster-wide. A cheaper alternative to `exchange_bindings`"}
    ]}
]).

-define(METRICS_MEMORY_BREAKDOWN, [
    {node_memory, [
        {2, undefined, memory_code_module_bytes, gauge, "Code module memory footprint", code},
        {2, undefined, memory_client_connection_reader_bytes, gauge, "Client connection reader processes footprint in bytes", connection_readers},
        {2, undefined, memory_client_connection_writer_bytes, gauge, "Client connection writer processes footprint in bytes", connection_writers},
        {2, undefined, memory_client_connection_channel_bytes, gauge, "Client connection channel processes footprint in bytes", connection_channels},
        {2, undefined, memory_client_connection_other_bytes, gauge, "Client connection other processes footprint in bytes", connection_other},
        {2, undefined, memory_classic_queue_erlang_process_bytes, gauge, "Classic queue processes footprint in bytes", queue_procs},
        {2, undefined, memory_quorum_queue_erlang_process_bytes, gauge, "Quorum queue processes footprint in bytes", quorum_queue_procs},
        {2, undefined, memory_quorum_queue_dlx_erlang_process_bytes, gauge, "Quorum queue DLX worker processes footprint in bytes", quorum_queue_dlx_procs},
        {2, undefined, memory_stream_erlang_process_bytes, gauge, "Stream processes footprint in bytes", stream_queue_procs},
        {2, undefined, memory_stream_replica_reader_erlang_process_bytes, gauge, "Stream replica reader processes footprint in bytes", stream_queue_replica_reader_procs},
        {2, undefined, memory_stream_coordinator_erlang_process_bytes, gauge, "Stream coordinator processes footprint in bytes", stream_queue_coordinator_procs},
        {2, undefined, memory_plugin_bytes, gauge, "Total plugin footprint in bytes", plugins},
        {2, undefined, memory_modern_metadata_store_bytes, gauge, "Modern metadata store footprint in bytes", metadata_store},
        {2, undefined, memory_other_erlang_process_bytes, gauge, "Other processes footprint in bytes", other_proc},
        {2, undefined, memory_metrics_bytes, gauge, "Metric table footprint in bytes", metrics},
        {2, undefined, memory_management_stats_db_bytes, gauge, "Management stats database footprint in bytes", mgmt_db},
        {2, undefined, memory_classic_metadata_store_bytes, gauge, "Classic metadata store footprint in bytes", mnesia},
        {2, undefined, memory_quorum_queue_ets_table_bytes, gauge, "Quorum queue ETS tables footprint in bytes", quorum_ets},
        {2, undefined, memory_modern_metadata_store_ets_table_bytes, gauge, "Modern metadata store ETS tables footprint in bytes", metadata_store_ets},
        {2, undefined, memory_other_ets_table_bytes, gauge, "Other ETS tables footprint in bytes", other_ets},
        {2, undefined, memory_binary_heap_bytes, gauge, "Binary heap size in bytes", binary},
        {2, undefined, memory_message_index_bytes, gauge, "Message index footprint in bytes", msg_index},
        {2, undefined, memory_atom_table_bytes, gauge, "Atom table size in bytes", atom},
        {2, undefined, memory_other_system_bytes, gauge, "Other runtime footprint in bytes", other_system},
        {2, undefined, memory_runtime_allocated_unused_bytes, gauge, "Runtime allocated but unused blocks size in bytes", allocated_unused},
        {2, undefined, memory_runtime_reserved_unallocated_bytes, gauge, "Runtime reserved but unallocated blocks size in bytes", reserved_unallocated}
    ]}]).

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

collect_mf('detailed', Callback) ->
    IncludedMFs = enabled_mfs_from_pdict(?METRICS_RAW),
    collect(true, ?DETAILED_METRIC_NAME_PREFIX, vhosts_filter_from_pdict(), IncludedMFs, Callback),
    collect(true, ?CLUSTER_METRIC_NAME_PREFIX, vhosts_filter_from_pdict(), enabled_mfs_from_pdict(?METRICS_CLUSTER), Callback),
    %% the detailed endpoint should emit queue_info only if queue metrics were requested
    MFs = proplists:get_keys(IncludedMFs),
    case lists:member(queue_coarse_metrics, MFs) orelse
         lists:member(queue_consumer_count, MFs) orelse
         lists:member(queue_metrics, MFs) of
        true ->
            emit_queue_info(?DETAILED_METRIC_NAME_PREFIX, vhosts_filter_from_pdict(), Callback);
        false -> ok
    end,
    %% identity is here to enable filtering on a cluster name (as already happens in existing dashboards)
    emit_identity_info(<<"detailed">>, Callback),
    ok;
collect_mf('per-object', Callback) ->
    collect(true, ?METRIC_NAME_PREFIX, false, ?METRICS_RAW, Callback),
    totals(Callback),
    emit_queue_info(?METRIC_NAME_PREFIX, false, Callback),
    emit_identity_info(<<"per-object">>, Callback),
    ok;
collect_mf('memory-breakdown', Callback) ->
    collect(false, ?METRIC_NAME_PREFIX, false, ?METRICS_MEMORY_BREAKDOWN, Callback),
    emit_identity_info(<<"memory-breakdown">>, Callback),
    ok;
collect_mf(_Registry, Callback) ->
    PerObjectMetrics = application:get_env(rabbitmq_prometheus, return_per_object_metrics, false),
    collect(PerObjectMetrics, ?METRIC_NAME_PREFIX, false, ?METRICS_RAW, Callback),
    totals(Callback),
    case PerObjectMetrics of
        true ->
            emit_identity_info(<<"per-object">>, Callback),
            emit_queue_info(?METRIC_NAME_PREFIX, false, Callback);
        false ->
            emit_identity_info(<<"aggregated">>, Callback)
    end,
    ok.

collect(PerObjectMetrics, Prefix, VHostsFilter, IncludedMFs, Callback) ->
    _ = [begin
         Data = get_data(Table, PerObjectMetrics, VHostsFilter),
         mf(Callback, Prefix, Contents, Data)
     end || {Table, Contents} <- IncludedMFs, not mutually_exclusive_mf(PerObjectMetrics, Table, IncludedMFs)],
    ok.

totals(Callback) ->
    _ = [begin
         Size = ets:info(Table, size),
         mf_totals(Callback, Name, Type, Help, Size)
     end || {Table, Name, Type, Help} <- ?TOTALS],
    ok.

emit_identity_info(Endpoint, Callback) ->
    add_metric_family(build_info(), Callback),
    add_metric_family(identity_info(Endpoint), Callback),
    ok.

%% Aggregated `auth``_attempt_detailed_metrics` and
%% `auth_attempt_metrics` are the same numbers. The former is just
%% more computationally intensive.
mutually_exclusive_mf(false, auth_attempt_detailed_metrics, _) ->
    true;
%% `queue_consumer_count` is a strict subset of queue metrics. They
%% read from the same table, but `queue_consumer_count` skips a lot of
%% `proplists:get_value/2` calls.
mutually_exclusive_mf(_, queue_consumer_count, MFs) ->
    lists:keymember(queue_metrics, 1, MFs);
mutually_exclusive_mf(_, _, _) ->
    false.

build_info() ->
    ProductInfo = rabbit:product_info(),
    #{product_base_version := BaseVersion} = ProductInfo,
    {ok, PrometheusPluginVersion} = application:get_key(rabbitmq_prometheus, vsn),
    {ok, PrometheusClientVersion} = application:get_key(prometheus, vsn),
    Properties0 = [
                   {rabbitmq_version, BaseVersion},
                   {prometheus_plugin_version, PrometheusPluginVersion},
                   {prometheus_client_version, PrometheusClientVersion},
                   {erlang_version, rabbit_misc:otp_release()}
                  ],
    Properties1 = case ProductInfo of
                      #{product_version := ProductVersion} ->
                          [{product_version, ProductVersion} | Properties0];
                      _ ->
                          Properties0
                  end,
    Properties = case ProductInfo of
                     #{product_name := ProductName} ->
                         [{product_name, ProductName} | Properties1];
                     _ ->
                         Properties1
                 end,
    {
        build_info,
        untyped,
        "RabbitMQ & Erlang/OTP version info",
        [{
            Properties,
            1
        }]
    }.

identity_info(Endpoint) ->
    {
        identity_info,
        untyped,
        "RabbitMQ node & cluster identity info",
        [{
            [
                {rabbitmq_node, node()},
                {rabbitmq_cluster, rabbit_nodes:cluster_name()},
                {rabbitmq_cluster_permanent_id, rabbit_nodes:persistent_cluster_id()},
                {rabbitmq_endpoint, Endpoint}
            ],
            1
        }]
    }.

membership(Pid, Members) when is_pid(Pid) ->
    case node(Pid) =:= node() of
        true ->
            case is_process_alive(Pid) of
                true -> leader;
                false -> undefined
            end;
        false ->
            case lists:member(node(), Members) of
                true -> follower;
                false -> not_a_member
            end
    end;
membership({Name, Node}, Members) ->
    case Node =:= node() of
        true ->
            case whereis(Name) of
                Pid when is_pid(Pid) ->
                    leader;
                _ ->
                    undefined
            end;
        false ->
            case lists:member(node(), Members) of
                true -> follower;
                false -> not_a_member
            end
    end;
membership(_, _Members) ->
    undefined.

emit_queue_info(Prefix, VHostsFilter, Callback) ->
    Help = <<"A metric with a constant '1' value and labels that provide some queue details">>,
    QInfos = lists:foldl(
               fun(Q, Acc) ->
                       #resource{virtual_host = VHost, name = Name} = amqqueue:get_name(Q),
                       case is_map(VHostsFilter) andalso maps:get(VHost, VHostsFilter) == false of
                           true -> Acc;
                           false ->
                               Type = amqqueue:get_type(Q),
                               TypeState = amqqueue:get_type_state(Q),
                               Members = maps:get(nodes, TypeState, []),
                               case membership(amqqueue:get_pid(Q), Members) of
                                   not_a_member ->
                                       Acc;
                                   Membership ->
                                       QInfo = [
                                                {vhost, VHost},
                                                {queue, Name},
                                                {queue_type, Type},
                                                {membership, Membership}
                                               ],
                                       [{QInfo, 1}|Acc]
                               end
                       end
               end, [], rabbit_amqqueue:list()),
    Callback(prometheus_model_helpers:create_mf(<<Prefix/binary, "queue_info">>, Help, gauge, QInfos)).

add_metric_family({Name, Type, Help, Metrics}, Callback) ->
    MN = <<?METRIC_NAME_PREFIX/binary, (prometheus_model_helpers:metric_name(Name))/binary>>,
    Callback(create_mf(MN, Help, Type, Metrics)).

mf(Callback, Prefix, Contents, Data) ->
    _ = [begin
         Fun = case Conversion of
                   undefined ->
                       fun(D) -> element(Index, D) end;
                   BaseUnitConversionFactor ->
                       fun(D) -> element(Index, D) / BaseUnitConversionFactor end
               end,
        Callback(
            create_mf(
                <<Prefix/binary, (prometheus_model_helpers:metric_name(Name))/binary>>,
                Help,
                catch_boolean(Type),
                ?MODULE,
                {Type, Fun, Data}
            )
        )
    end || {Index, Conversion, Name, Type, Help} <- Contents],
    [begin
        Fun = case Conversion of
                  undefined ->
                      fun(D) -> proplists:get_value(Key, element(Index, D)) end;
                  BaseUnitConversionFactor ->
                      fun(D) -> proplists:get_value(Key, element(Index, D)) / BaseUnitConversionFactor end
              end,
        Callback(
            create_mf(
                <<Prefix/binary, (prometheus_model_helpers:metric_name(Name))/binary>>,
                Help,
                catch_boolean(Type),
                ?MODULE,
                {Type, Fun, Data}
            )
        )
    end || {Index, Conversion, Name, Type, Help, Key} <- Contents].

mf_totals(Callback, Name, Type, Help, Size) ->
    Callback(
        create_mf(
            <<?METRIC_NAME_PREFIX/binary, (prometheus_model_helpers:metric_name(Name))/binary>>,
            Help,
            catch_boolean(Type),
            Size
        )
    ).

has_value_p('NaN') ->
    false;
has_value_p(undefined) ->
    false;
has_value_p(_) ->
    true.

collect_metrics(_, {Type, Fun, Items}) ->
    [metric(Type, labels(Item), V) || {Item, V} <- [{Item, Fun(Item)} || Item <- Items], has_value_p(V)].

labels(Item) ->
    label(element(1, Item)).

label(L) when is_binary(L) ->
    L;
label(M) when is_map(M) ->
    maps:fold(fun (K, V, Acc = <<>>) ->
                      <<Acc/binary, K/binary, "=\"", (escape_label_value(V))/binary, "\"">>;
                  (K, V, Acc) ->
                      <<Acc/binary, ",", K/binary, "=\"", (escape_label_value(V))/binary, "\"">>
              end, <<>>, M);
label(#resource{virtual_host = VHost, kind = exchange, name = Name}) ->
    <<"vhost=\"", (escape_label_value(VHost))/binary, "\",",
      "exchange=\"", (escape_label_value(Name))/binary, "\"">>;
label(#resource{virtual_host = VHost, kind = queue, name = Name}) ->
    <<"vhost=\"", (escape_label_value(VHost))/binary, "\",",
      "queue=\"", (escape_label_value(Name))/binary, "\"">>;
label({P, {#resource{virtual_host = QVHost, kind = queue, name = QName},
            #resource{virtual_host = EVHost, kind = exchange, name = EName}}}) when is_pid(P) ->
    %% channel_queue_exchange_metrics {channel_id, {queue_id, exchange_id}}
    <<"channel=\"", (iolist_to_binary(pid_to_list(P)))/binary, "\",",
      "queue_vhost=\"", (escape_label_value(QVHost))/binary, "\",",
      "queue=\"", (escape_label_value(QName))/binary, "\",",
      "exchange_vhost=\"", (escape_label_value(EVHost))/binary, "\",",
      "exchange=\"", (escape_label_value(EName))/binary, "\""
    >>;
label({RemoteAddress, Username, Protocol}) when is_binary(RemoteAddress), is_binary(Username),
                                                is_atom(Protocol) ->
    lists:filter(fun({_, V}) ->
                         V =/= <<>>
                 end, [{remote_address, RemoteAddress}, {username, Username},
                       {protocol, atom_to_binary(Protocol, utf8)}]);
label({
    #resource{kind=queue, virtual_host=VHost, name=QName},
    #resource{kind=exchange, name=ExName}
 }) ->
    %% queue_exchange_metrics {queue_id, exchange_id}
    <<"vhost=\"", (escape_label_value(VHost))/binary, "\",",
      "exchange=\"", (escape_label_value(ExName))/binary, "\",",
      "queue=\"", (escape_label_value(QName))/binary, "\"">>;
label({I1, I2}) ->
    case {label(I1), label(I2)} of
        {<<>>, L} -> L;
        {L, <<>>} -> L;
        {L1, L2} -> <<L1/binary, ",", L2/binary>>
    end;
label(P) when is_pid(P) ->
    <<"channel=\"", (iolist_to_binary(pid_to_list(P)))/binary, "\"">>;
label(A) when is_atom(A) ->
    case is_protocol(A) of
        true -> <<"protocol=\"", (atom_to_binary(A, utf8))/binary, "\"">>;
        false -> <<>>
    end.

is_protocol(P) ->
    lists:member(P, [amqp091, amqp10, mqtt, http]).

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

get_data(connection_metrics = Table, false, _) ->
    {Table, A1, A2, A3, A4} = ets:foldl(fun({_, Props}, {T, A1, A2, A3, A4}) ->
                                            {T,
                                             sum(proplists:get_value(recv_cnt, Props), A1),
                                             sum(proplists:get_value(send_cnt, Props), A2),
                                             sum(proplists:get_value(send_pend, Props), A3),
                                             sum(proplists:get_value(channels, Props), A4)}
                                    end, empty(Table), Table),
    [{Table, [{recv_cnt, A1}, {send_cnt, A2}, {send_pend, A3}, {channels, A4}]}];
get_data(channel_metrics = Table, false, _) ->
    {Table, A1, A2, A3, A4, A5, A6, A7} =
        ets:foldl(fun({_, Props}, {T, A1, A2, A3, A4, A5, A6, A7}) ->
                          {T,
                           sum(proplists:get_value(consumer_count, Props), A1),
                           sum(proplists:get_value(messages_unacknowledged, Props), A2),
                           sum(proplists:get_value(messages_unconfirmed, Props), A3),
                           sum(proplists:get_value(messages_uncommitted, Props), A4),
                           sum(proplists:get_value(acks_uncommitted, Props), A5),
                           sum(proplists:get_value(prefetch_count, Props), A6),
                           sum(proplists:get_value(global_prefetch_count, Props), A7)}
                  end, empty(Table), Table),
     [{Table, [{consumer_count, A1}, {messages_unacknowledged, A2}, {messages_unconfirmed, A3},
               {messages_uncommitted, A4}, {acks_uncommitted, A5}, {prefetch_count, A6},
               {global_prefetch_count, A7}]}];
get_data(stream_consumer_metrics = MF, false, _) ->
    Table = rabbit_stream_consumer_created, %% real table name
    try ets:foldl(fun({_, Props}, OldMax) ->
                          erlang:max(proplists:get_value(offset_lag, Props, 0), OldMax)
                  end, 0, Table) of
        MaxOffsetLag ->
            [{MF, MaxOffsetLag}]
    catch error:badarg ->
            %% rabbitmq_stream plugin is not enabled
            []
    end;
get_data(queue_consumer_count = MF, false, VHostsFilter) ->
    Table = queue_metrics, %% Real table name
    {_, A1} = ets:foldl(fun
                            ({#resource{kind = queue, virtual_host = VHost}, _, _}, Acc) when is_map(VHostsFilter), map_get(VHost, VHostsFilter) == false ->
                                 Acc;
                             ({_, Props, _}, {T, A1}) ->
                                 {T,
                                  sum(proplists:get_value(consumers, Props), A1)
                                 }
                         end, empty(MF), Table),
    [{Table, [{consumers, A1}]}];
get_data(queue_metrics = Table, false, VHostsFilter) ->
    {Table, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17} =
        ets:foldl(fun
                      ({#resource{kind = queue, virtual_host = VHost}, _, _}, Acc) when is_map(VHostsFilter), map_get(VHost, VHostsFilter) == false ->
                          Acc;
                      ({_, Props, _}, Acc) ->
                          sum_queue_metrics(Props, Acc)
                  end, empty(Table), Table),
     [{Table, [{consumers, A1}, {consumer_utilisation, A2}, {memory, A3}, {messages_ram, A4},
               {message_bytes_ram, A5}, {messages_ready_ram, A6},
               {messages_unacknowledged_ram, A7}, {messages_persistent, A8},
               {messages_bytes_persistent, A9}, {message_bytes, A10},
               {message_bytes_ready, A11}, {message_bytes_unacknowledged, A12},
               {messages_paged_out, A13}, {message_bytes_paged_out, A14},
               {disk_reads, A15}, {disk_writes, A16}, {segments, A17}]}];
get_data(Table, false, VHostsFilter) when Table == channel_exchange_metrics;
                           Table == queue_coarse_metrics;
                           Table == queue_delivery_metrics;
                           Table == channel_queue_metrics;
                           Table == connection_coarse_metrics;
                           Table == exchange_metrics;
                           Table == queue_exchange_metrics;
                           Table == channel_queue_exchange_metrics;
                           Table == channel_process_metrics ->
    Result = ets:foldl(fun
                  %% For queue_coarse_metrics
                  ({#resource{kind = queue, virtual_host = VHost}, _, _, _, _}, Acc) when is_map(VHostsFilter), map_get(VHost, VHostsFilter) == false ->
                               Acc;
                  ({#resource{kind = queue, virtual_host = VHost}, _, _, _, _, _, _, _, _}, Acc) when is_map(VHostsFilter), map_get(VHost, VHostsFilter) == false ->
                               Acc;
                  ({{#resource{kind = queue, virtual_host = VHost}, #resource{kind = exchange}}, _, _}, Acc) when is_map(VHostsFilter), map_get(VHost, VHostsFilter) == false ->
                               Acc;
                  ({_, V1}, {T, A1}) ->
                       {T, V1 + A1};
                  ({_, V1, _}, {T, A1}) ->
                       {T, V1 + A1};
                  ({_, V1, V2, V3}, {T, A1, A2, A3}) ->
                       {T, V1 + A1, V2 + A2, V3 + A3};
                  ({_, V1, V2, V3, _}, {T, A1, A2, A3}) ->
                       {T, V1 + A1, V2 + A2, V3 + A3};
                  ({_, V1, V2, V3, V4, _}, {T, A1, A2, A3, A4}) ->
                       {T, V1 + A1, V2 + A2, V3 + A3, V4 + A4};
                  ({_, V1, V2, V3, V4}, {T, A1, A2, A3, A4}) ->
                       {T, V1 + A1, V2 + A2, V3 + A3, V4 + A4};
                  ({_, V1, V2, V3, V4, V5, V6, V7, _}, {T, A1, A2, A3, A4, A5, A6, A7}) ->
                       {T, V1 + A1, V2 + A2, V3 + A3, V4 + A4, V5 + A5, V6 + A6, V7 + A7}
               end, empty(Table), Table),
    [Result];
get_data(exchange_metrics = Table, true, VHostsFilter) when is_map(VHostsFilter)->
    ets:foldl(fun
        ({#resource{kind = exchange, virtual_host = VHost}, _, _, _, _, _} = Row, Acc) when
            map_get(VHost, VHostsFilter)
        ->
            [Row | Acc];
        (_Row, Acc) ->
            Acc
    end, [], Table);
get_data(queue_delivery_metrics = Table, true, VHostsFilter) when is_map(VHostsFilter) ->
    ets:foldl(fun
        ({#resource{kind = queue, virtual_host = VHost}, _, _, _, _, _, _, _, _} = Row, Acc) when
            map_get(VHost, VHostsFilter)
        ->
            [Row | Acc];
        (_Row, Acc) ->
            Acc
    end, [], Table);
get_data(queue_exchange_metrics = Table, true, VHostsFilter) when is_map(VHostsFilter) ->
    ets:foldl(fun
        ({{
            #resource{kind = queue, virtual_host = VHost},
            #resource{kind = exchange, virtual_host = VHost}
         }, _, _} = Row, Acc) when
            map_get(VHost, VHostsFilter)
        ->
            [Row | Acc];
        (_Row, Acc) ->
            Acc
    end, [], Table);
get_data(queue_coarse_metrics = Table, true, VHostsFilter) when is_map(VHostsFilter) ->
    ets:foldl(fun
                  ({#resource{kind = queue, virtual_host = VHost}, _, _, _, _} = Row, Acc) when map_get(VHost, VHostsFilter) ->
                      [Row|Acc];
                  (_, Acc) ->
                      Acc
              end, [], Table);
get_data(MF, true, VHostsFilter) when is_map(VHostsFilter), MF == queue_metrics orelse MF == queue_consumer_count ->
    Table = queue_metrics,
    ets:foldl(fun
                  ({#resource{kind = queue, virtual_host = VHost}, _, _} = Row, Acc) when map_get(VHost, VHostsFilter) ->
                      [Row|Acc];
                  (_, Acc) ->
                      Acc
              end, [], Table);
get_data(queue_consumer_count, true, _) ->
    ets:tab2list(queue_metrics);
get_data(stream_consumer_metrics, true, _) ->
    Table = rabbit_stream_consumer_created, %% real table name
    try ets:foldl(fun({{QueueName, _Pid, _SubId}, Props}, Map0) ->
                          Value = proplists:get_value(offset_lag, Props, 0),
                          maps:update_with(
                            QueueName,
                            fun(OldMax) -> erlang:max(Value, OldMax) end,
                            Value,
                            Map0)
                  end, #{}, Table) of
        Map1 ->
            maps:to_list(Map1)
    catch error:badarg ->
            %% rabbitmq_stream plugin is not enabled
            []
    end;
get_data(vhost_status, _, _) ->
    [ { #{<<"vhost">> => VHost},
        case rabbit_vhost_sup_sup:is_vhost_alive(VHost) of
            true -> 1;
            false -> 0
        end}
      || VHost <- rabbit_vhost:list()  ];
get_data(node_memory, _, _) ->
    BreakdownPL = rabbit_vm:memory(),
    KeysOfInterest = [
        code,
        connection_readers,
        connection_writers,
        connection_channels,
        connection_other,
        queue_procs,
        quorum_queue_procs,
        quorum_queue_dlx_procs,
        stream_queue_procs,
        stream_queue_replica_reader_procs,
        stream_queue_coordinator_procs,
        plugins,
        metadata_store,
        other_proc,
        metrics,
        mgmt_db,
        mnesia,
        quorum_ets,
        metadata_store_ets,
        other_ets,
        binary,
        msg_index,
        atom,
        other_system,
        allocated_unused,
        reserved_unallocated
    ],
    Data = maps:to_list(maps:with(KeysOfInterest, maps:from_list(BreakdownPL))),
    [{node_memory, Data}];
get_data(exchange_bindings, _, _) ->
    Exchanges = lists:foldl(fun
                                (#exchange{internal = true}, Acc) ->
                                    Acc;
                                (#exchange{name = #resource{name = <<>>}}, Acc) ->
                                    Acc;
                                (#exchange{name = EName, type = EType}, Acc) ->
                                    maps:put(EName, #{type => atom_to_binary(EType), binding_count => 0}, Acc)
                            end, #{}, rabbit_exchange:list()),
    WithCount = rabbit_db_binding:fold(
                  fun (#binding{source = EName}, Acc) ->
                          case maps:is_key(EName, Acc) of
                              false -> Acc;
                              true ->
                                  maps:update_with(EName,
                                                   fun (R = #{binding_count := Cnt}) ->
                                                           R#{binding_count => Cnt + 1}
                                                   end, Acc)
                          end
                  end, Exchanges),
    maps:fold(fun(#resource{virtual_host = VHost, name = Name}, #{type := Type, binding_count := Bindings}, Acc) ->
                      [{<<"vhost=\"", VHost/binary, "\",exchange=\"", Name/binary, "\",type=\"", Type/binary, "\"">>,
                        Bindings}|Acc]
              end, [], WithCount);
get_data(exchange_names, _, _) ->
    lists:foldl(fun
                    (#exchange{internal = true}, Acc) ->
                        Acc;
                    (#exchange{name = #resource{name = <<>>}}, Acc) ->
                        Acc;
                    (#exchange{name = #resource{virtual_host = VHost, name = Name}, type = EType}, Acc) ->
                        Label = <<"vhost=\"", VHost/binary, "\",exchange=\"", Name/binary, "\",type=\"", (atom_to_binary(EType))/binary, "\"">>,
                        [{Label, 1}|Acc]
                end, [], rabbit_exchange:list());
get_data(Table, _, _) ->
    ets:tab2list(Table).


sum_queue_metrics(Props, {T, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11,
                          A12, A13, A14, A15, A16, A17}) ->
    {T,
     sum(proplists:get_value(consumers, Props), A1),
     sum(proplists:get_value(consumer_utilisation, Props), A2),
     sum(proplists:get_value(memory, Props), A3),
     sum(proplists:get_value(messages_ram, Props), A4),
     sum(proplists:get_value(message_bytes_ram, Props), A5),
     sum(proplists:get_value(messages_ready_ram, Props), A6),
     sum(proplists:get_value(messages_unacknowledged_ram, Props), A7),
     sum(proplists:get_value(messages_persistent, Props), A8),
     sum(proplists:get_value(message_bytes_persistent, Props), A9),
     sum(proplists:get_value(message_bytes, Props), A10),
     sum(proplists:get_value(message_bytes_ready, Props), A11),
     sum(proplists:get_value(message_bytes_unacknowledged, Props), A12),
     sum(proplists:get_value(messages_paged_out, Props), A13),
     sum(proplists:get_value(message_bytes_paged_out, Props), A14),
     sum(proplists:get_value(disk_reads, Props), A15),
     sum(proplists:get_value(disk_writes, Props), A16),
     sum(proplists:get_value(segments, Props), A17)
    }.

empty(T) when T == channel_queue_exchange_metrics; T == queue_exchange_metrics; T == channel_process_metrics; T == queue_consumer_count ->
    {T, 0};
empty(T) when T == connection_coarse_metrics; T == auth_attempt_metrics; T == auth_attempt_detailed_metrics ->
    {T, 0, 0, 0};
empty(T) when T == channel_exchange_metrics; T == exchange_metrics; T == queue_coarse_metrics; T == connection_metrics ->
    {T, 0, 0, 0, 0};
empty(T) when T == channel_queue_metrics; T == queue_delivery_metrics; T == channel_metrics ->
    {T, 0, 0, 0, 0, 0, 0, 0};
empty(queue_metrics = T) ->
    {T, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}.

sum(undefined, B) ->
    B;
sum('', B) ->
    B;
sum(A, B) ->
    A + B.

enabled_mfs_from_pdict(AllMFs) ->
    case get(prometheus_mf_filter) of
        undefined ->
            [];
        MFNames ->
            MFNameSet = sets:from_list(MFNames),
            [ MF || MF = {Table, _} <- AllMFs, sets:is_element(Table, MFNameSet) ]
    end.

vhosts_filter_from_pdict() ->
    case get(prometheus_vhost_filter) of
        undefined ->
            false;
        L ->
            %% Having both excluded and included hosts in this map makes some guards easier (or even possible).
            All = maps:from_list([ {VHost, false} || VHost <- rabbit_vhost:list()]),
            Enabled = maps:from_list([ {VHost, true} || VHost <- L ]),
            maps:merge(All, Enabled)
    end.
