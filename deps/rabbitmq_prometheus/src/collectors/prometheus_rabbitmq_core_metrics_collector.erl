%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
%% We prepend either rabbitmq_ or rabbitmq_detailed_ to all metrics emitted by this collector.
%% As there are also some metrics for Erlang (erlang_), Mnesia (schema_db_) or the System (io_),
%% as observed by RabbitMQ.

%% Used by `/metrics` and `/metrics/per-object`.
-define(METRIC_NAME_PREFIX, "rabbitmq_").

%% Used by `/metrics/detailed` endpoint
-define(DETAILED_METRIC_NAME_PREFIX, "rabbitmq_detailed_").

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

% Some metrics require to be converted, mostly those that represent time.
% It is a Prometheus best practice to use specific base units: https://prometheus.io/docs/practices/naming/#base-units
% Extra context: https://github.com/prometheus/docs/pull/1414#issuecomment-522337895

-define(MILLISECOND, 1000).
-define(MICROSECOND, 1000000).

-define(METRICS_RAW, [

%%% Those are global, i.e. they contain no reference to queue/vhost/channel
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
        {2, undefined, process_open_tcp_sockets, gauge, "Open TCP sockets", sockets_used},
        {2, undefined, process_resident_memory_bytes, gauge, "Memory used in bytes", mem_used},
        {2, undefined, disk_space_available_bytes, gauge, "Disk space available in bytes", disk_free},
        {2, undefined, erlang_processes_used, gauge, "Erlang processes used", proc_used},
        {2, undefined, erlang_gc_runs_total, counter, "Total number of Erlang garbage collector runs", gc_num},
        {2, undefined, erlang_gc_reclaimed_bytes_total, counter, "Total number of bytes of memory reclaimed by Erlang garbage collector", gc_bytes_reclaimed},
        {2, undefined, erlang_scheduler_context_switches_total, counter, "Total number of Erlang scheduler context switches", context_switches}
    ]},
    {node_metrics, [
        {2, undefined, process_max_fds, gauge, "Open file descriptors limit", fd_total},
        {2, undefined, process_max_tcp_sockets, gauge, "Open TCP sockets limit", sockets_total},
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
        {2, undefined, io_open_attempt_ops_total, counter, "Total number of file open attempts", io_file_handle_open_attempt_count},
        {2, undefined, io_reopen_ops_total, counter, "Total number of times files have been reopened", io_reopen_count},
        {2, undefined, schema_db_ram_tx_total, counter, "Total number of Schema DB memory transactions", mnesia_ram_tx_count},
        {2, undefined, schema_db_disk_tx_total, counter, "Total number of Schema DB disk transactions", mnesia_disk_tx_count},
        {2, undefined, msg_store_read_total, counter, "Total number of Message Store read operations", msg_store_read_count},
        {2, undefined, msg_store_write_total, counter, "Total number of Message Store write operations", msg_store_write_count},
        {2, undefined, queue_index_read_ops_total, counter, "Total number of Queue Index read operations", queue_index_read_count},
        {2, undefined, queue_index_write_ops_total, counter, "Total number of Queue Index write operations", queue_index_write_count},
        {2, undefined, queue_index_journal_write_ops_total, counter, "Total number of Queue Index Journal write operations", queue_index_journal_write_count},
        {2, ?MICROSECOND, io_read_time_seconds_total, counter, "Total I/O read time", io_read_time},
        {2, ?MICROSECOND, io_write_time_seconds_total, counter, "Total I/O write time", io_write_time},
        {2, ?MICROSECOND, io_sync_time_seconds_total, counter, "Total I/O sync time", io_sync_time},
        {2, ?MICROSECOND, io_seek_time_seconds_total, counter, "Total I/O seek time", io_seek_time},
        {2, ?MICROSECOND, io_open_attempt_time_seconds_total, counter, "Total file open attempts time", io_file_handle_open_attempt_time}
    ]},

    {ra_metrics, [
        {2, undefined, raft_term_total, counter, "Current Raft term number"},
        {3, undefined, raft_log_snapshot_index, gauge, "Raft log snapshot index"},
        {4, undefined, raft_log_last_applied_index, gauge, "Raft log last applied index"},
        {5, undefined, raft_log_commit_index, gauge, "Raft log commit index"},
        {6, undefined, raft_log_last_written_index, gauge, "Raft log last written index"},
        {7, ?MILLISECOND, raft_entry_commit_latency_seconds, gauge, "Time taken for a log entry to be committed"}
    ]},

    {auth_attempt_metrics, [
        {2, undefined, auth_attempts_total, counter, "Total number of authorization attempts"},
        {3, undefined, auth_attempts_succeeded_total, counter, "Total number of successful authentication attempts"},
        {4, undefined, auth_attempts_failed_total, counter, "Total number of failed authentication attempts"}
    ]},

    {auth_attempt_detailed_metrics, [
        {2, undefined, auth_attempts_detailed_total, counter, "Total number of authorization attempts with source info"},
        {3, undefined, auth_attempts_detailed_succeeded_total, counter, "Total number of successful authorization attempts with source info"},
        {4, undefined, auth_attempts_detailed_failed_total, counter, "Total number of failed authorization attempts with source info"}
    ]},

%%% Those metrics have reference only to a queue name. This is the only group where filtering (e.g. by vhost) makes sense.
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
        {2, undefined, queue_disk_reads_total, counter, "Total number of times queue read messages from disk", disk_reads},
        {2, undefined, queue_disk_writes_total, counter, "Total number of times queue wrote messages to disk", disk_writes}
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
        {2, undefined, channel_prefetch, gauge, "Total limit of unacknowledged messages for all consumers on a channel", global_prefetch_count}
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

    {connection_metrics, [
        {2, undefined, connection_incoming_packets_total, counter, "Total number of packets received on a connection", recv_cnt},
        {2, undefined, connection_outgoing_packets_total, counter, "Total number of packets sent on a connection", send_cnt},
        {2, undefined, connection_pending_packets, gauge, "Number of packets waiting to be sent on a connection", send_pend},
        {2, undefined, connection_channels, gauge, "Channels on a connection", channels}
    ]},

    {channel_queue_exchange_metrics, [
        {2, undefined, queue_messages_published_total, counter, "Total number of messages published to queues"}
    ]}
]).

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
    collect(true, ?DETAILED_METRIC_NAME_PREFIX, vhosts_filter_from_pdict(), enabled_mfs_from_pdict(), Callback),
    ok;
collect_mf('per-object', Callback) ->
    collect(true, ?METRIC_NAME_PREFIX, false, ?METRICS_RAW, Callback),
    totals(Callback),
    ok;
collect_mf(_Registry, Callback) ->
    PerObjectMetrics = application:get_env(rabbitmq_prometheus, return_per_object_metrics, false),
    collect(PerObjectMetrics, ?METRIC_NAME_PREFIX, false, ?METRICS_RAW, Callback),
    totals(Callback),
    ok.

collect(PerObjectMetrics, Prefix, VHostsFilter, IncludedMFs, Callback) ->
    [begin
         Data = get_data(Table, PerObjectMetrics, VHostsFilter),
         mf(Callback, Prefix, Contents, Data)
     end || {Table, Contents} <- IncludedMFs, not mutually_exclusive_mf(PerObjectMetrics, Table, IncludedMFs)].

totals(Callback) ->
    [begin
         Size = ets:info(Table, size),
         mf_totals(Callback, Name, Type, Help, Size)
     end || {Table, Name, Type, Help} <- ?TOTALS],
    add_metric_family(build_info(), Callback),
    add_metric_family(identity_info(), Callback).

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

mf(Callback, Prefix, Contents, Data) ->
    [begin
         Fun = case Conversion of
                   undefined ->
                       fun(D) -> element(Index, D) end;
                   BaseUnitConversionFactor ->
                       fun(D) -> element(Index, D) / BaseUnitConversionFactor end
               end,
        Callback(
            create_mf(
                [Prefix, prometheus_model_helpers:metric_name(Name)],
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
                [Prefix, prometheus_model_helpers:metric_name(Name)],
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
label({RemoteAddress, Username, Protocol}) when is_binary(RemoteAddress), is_binary(Username),
                                                is_atom(Protocol) ->
    lists:filter(fun({_, V}) ->
                         V =/= <<>>
                 end, [{remote_address, RemoteAddress}, {username, Username},
                       {protocol, atom_to_binary(Protocol, utf8)}]);
label({I1, I2}) ->
    label(I1) ++ label(I2);
label(P) when is_pid(P) ->
    [{channel, P}];
label(A) when is_atom(A) ->
    case is_protocol(A) of
        true -> [{protocol, atom_to_binary(A, utf8)}];
        false -> []
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
    {Table, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16} =
        ets:foldl(fun
                      ({#resource{kind = queue, virtual_host = VHost}, _, _}, Acc) when is_map(VHostsFilter), map_get(VHost, VHostsFilter) == false ->
                          Acc;
                      ({_, Props, _}, {T, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10,
                                       A11, A12, A13, A14, A15, A16}) ->
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
                           sum(proplists:get_value(disk_writes, Props), A16)
                          }
                  end, empty(Table), Table),
     [{Table, [{consumers, A1}, {consumer_utilisation, A2}, {memory, A3}, {messages_ram, A4},
               {message_bytes_ram, A5}, {messages_ready_ram, A6},
               {messages_unacknowledged_ram, A7}, {messages_persistent, A8},
               {messages_bytes_persistent, A9}, {message_bytes, A10},
               {message_bytes_ready, A11}, {message_bytes_unacknowledged, A12},
               {messages_paged_out, A13}, {message_bytes_paged_out, A14},
               {disk_reads, A15}, {disk_writes, A16}]}];
get_data(Table, false, VHostsFilter) when Table == channel_exchange_metrics;
                           Table == queue_coarse_metrics;
                           Table == channel_queue_metrics;
                           Table == connection_coarse_metrics;
                           Table == channel_queue_exchange_metrics;
                           Table == ra_metrics;
                           Table == channel_process_metrics ->
    Result = ets:foldl(fun
                  %% For queue_coarse_metrics
                  ({#resource{kind = queue, virtual_host = VHost}, _, _, _, _}, Acc) when is_map(VHostsFilter), map_get(VHost, VHostsFilter) == false ->
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
                  ({_, V1, V2, V3, V4, V5, V6}, {T, A1, A2, A3, A4, A5, A6}) ->
                       %% ra_metrics: raft_entry_commit_latency_seconds needs to be an average
                       {T, V1 + A1, V2 + A2, V3 + A3, V4 + A4, V5 + A5, accumulate_count_and_sum(V6, A6)};
                  ({_, V1, V2, V3, V4, V5, V6, V7, _}, {T, A1, A2, A3, A4, A5, A6, A7}) ->
                       {T, V1 + A1, V2 + A2, V3 + A3, V4 + A4, V5 + A5, V6 + A6, V7 + A7}
               end, empty(Table), Table),
    case Table of
        %% raft_entry_commit_latency_seconds needs to be an average
        ra_metrics ->
            {Count, Sum} = element(7, Result),
            [setelement(7, Result, division(Sum, Count))];
        _ ->
            [Result]
    end;
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
get_data(Table, _, _) ->
    ets:tab2list(Table).

division(0, 0) ->
    0;
division(A, B) ->
    A / B.

accumulate_count_and_sum(Value, {Count, Sum}) ->
    {Count + 1, Sum + Value}.

empty(T) when T == channel_queue_exchange_metrics; T == channel_process_metrics; T == queue_consumer_count ->
    {T, 0};
empty(T) when T == connection_coarse_metrics; T == auth_attempt_metrics; T == auth_attempt_detailed_metrics ->
    {T, 0, 0, 0};
empty(T) when T == channel_exchange_metrics; T == queue_coarse_metrics; T == connection_metrics ->
    {T, 0, 0, 0, 0};
empty(T) when T == ra_metrics ->
    {T, 0, 0, 0, 0, 0, {0, 0}};
empty(T) when T == channel_queue_metrics; T == channel_metrics ->
    {T, 0, 0, 0, 0, 0, 0, 0};
empty(queue_metrics = T) ->
    {T, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}.

sum(undefined, B) ->
    B;
sum('', B) ->
    B;
sum(A, B) ->
    A + B.

enabled_mfs_from_pdict() ->
    case get(prometheus_mf_filter) of
        undefined ->
            [];
        MFNames ->
            MFNameSet = sets:from_list(MFNames),
            [ MF || MF = {Table, _} <- ?METRICS_RAW, sets:is_element(Table, MFNameSet) ]
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
