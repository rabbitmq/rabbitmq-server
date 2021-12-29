## Configurable RabbitMQ metric groups

Those are metrics than can be explicitly requested via `/metrics/detailed` endpoint. 

### Generic metrics

These are some generic metrics, which do not refer to any specific
queue/connection/etc. 

#### Connection/channel/queue churn

Group `connection_churn_metrics`:

| Metric                                     | Description                                      |
|--------------------------------------------|--------------------------------------------------|
| rabbitmq_detailed_connections_opened_total | Total number of connections opened               |
| rabbitmq_detailed_connections_closed_total | Total number of connections closed or terminated |
| rabbitmq_detailed_channels_opened_total    | Total number of channels opened                  |
| rabbitmq_detailed_channels_closed_total    | Total number of channels closed                  |
| rabbitmq_detailed_queues_declared_total    | Total number of queues declared                  |
| rabbitmq_detailed_queues_created_total     | Total number of queues created                   |
| rabbitmq_detailed_queues_deleted_total     | Total number of queues deleted                   |


#### Erlang VM/Disk IO via RabbitMQ

Group `node_coarse_metrics`:

| Metric                                                    | Description                                                           |
|-----------------------------------------------------------|-----------------------------------------------------------------------|
| rabbitmq_detailed_process_open_fds                        | Open file descriptors                                                 |
| rabbitmq_detailed_process_open_tcp_sockets                | Open TCP sockets                                                      |
| rabbitmq_detailed_process_resident_memory_bytes           | Memory used in bytes                                                  |
| rabbitmq_detailed_disk_space_available_bytes              | Disk space available in bytes                                         |
| rabbitmq_detailed_erlang_processes_used                   | Erlang processes used                                                 |
| rabbitmq_detailed_erlang_gc_runs_total                    | Total number of Erlang garbage collector runs                         |
| rabbitmq_detailed_erlang_gc_reclaimed_bytes_total         | Total number of bytes of memory reclaimed by Erlang garbage collector |
| rabbitmq_detailed_erlang_scheduler_context_switches_total | Total number of Erlang scheduler context switches                     |

Group `node_metrics`:

| Metric                                             | Description                            |
|----------------------------------------------------|----------------------------------------|
| rabbitmq_detailed_process_max_fds                  | Open file descriptors limit            |
| rabbitmq_detailed_process_max_tcp_sockets          | Open TCP sockets limit                 |
| rabbitmq_detailed_resident_memory_limit_bytes      | Memory high watermark in bytes         |
| rabbitmq_detailed_disk_space_available_limit_bytes | Free disk space low watermark in bytes |
| rabbitmq_detailed_erlang_processes_limit           | Erlang processes limit                 |
| rabbitmq_detailed_erlang_scheduler_run_queue       | Erlang scheduler run queue             |
| rabbitmq_detailed_erlang_net_ticktime_seconds      | Inter-node heartbeat interval          |
| rabbitmq_detailed_erlang_uptime_seconds            | Node uptime                            |


Group `node_persister_metrics`:

| Metric                                                | Description                                          |
|-------------------------------------------------------|------------------------------------------------------|
| rabbitmq_detailed_io_read_ops_total                   | Total number of I/O read operations                  |
| rabbitmq_detailed_io_read_bytes_total                 | Total number of I/O bytes read                       |
| rabbitmq_detailed_io_write_ops_total                  | Total number of I/O write operations                 |
| rabbitmq_detailed_io_write_bytes_total                | Total number of I/O bytes written                    |
| rabbitmq_detailed_io_sync_ops_total                   | Total number of I/O sync operations                  |
| rabbitmq_detailed_io_seek_ops_total                   | Total number of I/O seek operations                  |
| rabbitmq_detailed_io_open_attempt_ops_total           | Total number of file open attempts                   |
| rabbitmq_detailed_io_reopen_ops_total                 | Total number of times files have been reopened       |
| rabbitmq_detailed_schema_db_ram_tx_total              | Total number of Schema DB memory transactions        |
| rabbitmq_detailed_schema_db_disk_tx_total             | Total number of Schema DB disk transactions          |
| rabbitmq_detailed_msg_store_read_total                | Total number of Message Store read operations        |
| rabbitmq_detailed_msg_store_write_total               | Total number of Message Store write operations       |
| rabbitmq_detailed_queue_index_read_ops_total          | Total number of Queue Index read operations          |
| rabbitmq_detailed_queue_index_write_ops_total         | Total number of Queue Index write operations         |
| rabbitmq_detailed_queue_index_journal_write_ops_total | Total number of Queue Index Journal write operations |
| rabbitmq_detailed_io_read_time_seconds_total          | Total I/O read time                                  |
| rabbitmq_detailed_io_write_time_seconds_total         | Total I/O write time                                 |
| rabbitmq_detailed_io_sync_time_seconds_total          | Total I/O sync time                                  |
| rabbitmq_detailed_io_seek_time_seconds_total          | Total I/O seek time                                  |
| rabbitmq_detailed_io_open_attempt_time_seconds_total  | Total file open attempts time                        |


#### Raft metrics

Group `ra_metrics`:

| Metric                                              | Description                                |
|-----------------------------------------------------|--------------------------------------------|
| rabbitmq_detailed_raft_term_total                   | Current Raft term number                   |
| rabbitmq_detailed_raft_log_snapshot_index           | Raft log snapshot index                    |
| rabbitmq_detailed_raft_log_last_applied_index       | Raft log last applied index                |
| rabbitmq_detailed_raft_log_commit_index             | Raft log commit index                      |
| rabbitmq_detailed_raft_log_last_written_index       | Raft log last written index                |
| rabbitmq_detailed_raft_entry_commit_latency_seconds | Time taken for a log entry to be committed |

#### Auth metrics

Group `auth_attempt_metrics`:

| Metric                                          | Description                                        |
|-------------------------------------------------|----------------------------------------------------|
| rabbitmq_detailed_auth_attempts_total           | Total number of authorization attempts             |
| rabbitmq_detailed_auth_attempts_succeeded_total | Total number of successful authentication attempts |
| rabbitmq_detailed_auth_attempts_failed_total    | Total number of failed authentication attempts     |


Group `auth_attempt_detailed_metrics` (when aggregated, it produces the same numbers as `auth_attempt_metrics` - so it's mutually exclusive with it in the aggregation mode):

| Metric                                                   | Description                                                        |
|----------------------------------------------------------|--------------------------------------------------------------------|
| rabbitmq_detailed_auth_attempts_detailed_total           | Total number of authorization attempts with source info            |
| rabbitmq_detailed_auth_attempts_detailed_succeeded_total | Total number of successful authorization attempts with source info |
| rabbitmq_detailed_auth_attempts_detailed_failed_total    | Total number of failed authorization attempts with source info     |


### Queue metrics

Each of metrics in this group refers to a single queue in its label. Amount of data and performance totally depends on the number of queues.

They are listed from least expensive to collect to the most expensive.

#### Queue coarse metrics

Group `queue_coarse_metrics`:

| Metric                                           | Description                                                  |
|--------------------------------------------------|--------------------------------------------------------------|
| rabbitmq_detailed_queue_messages_ready           | Messages ready to be delivered to consumers                  |
| rabbitmq_detailed_queue_messages_unacked         | Messages delivered to consumers but not yet acknowledged     |
| rabbitmq_detailed_queue_messages                 | Sum of ready and unacknowledged messages - total queue depth |
| rabbitmq_detailed_queue_process_reductions_total | Total number of queue process reductions                     |

#### Per-queue consumer count

Group `queue_consumer_count`. This is a strict subset of `queue_metrics` which contains only a single metric (if both `queue_consumer_count` and `queue_metrics` are requested, the former will be automatically skipped):

| Metric                            | Description          |
|-----------------------------------|----------------------|
| rabbitmq_detailed_queue_consumers | Consumers on a queue |

This is one of the more telling metrics, and having it separately allows to skip some expensive operations for extracting/exposing the other metrics from the same datasource.

#### Detailed queue metrics

Group `queue_metrics` contains all the metrics for every queue, and can be relatively expensive to produce:

| Metric                                            | Description                                                |
|---------------------------------------------------|------------------------------------------------------------|
| rabbitmq_detailed_queue_consumers                 | Consumers on a queue                                       |
| rabbitmq_detailed_queue_consumer_capacity         | Consumer capacity                                          |
| rabbitmq_detailed_queue_consumer_utilisation      | Same as consumer capacity                                  |
| rabbitmq_detailed_queue_process_memory_bytes      | Memory in bytes used by the Erlang queue process           |
| rabbitmq_detailed_queue_messages_ram              | Ready and unacknowledged messages stored in memory         |
| rabbitmq_detailed_queue_messages_ram_bytes        | Size of ready and unacknowledged messages stored in memory |
| rabbitmq_detailed_queue_messages_ready_ram        | Ready messages stored in memory                            |
| rabbitmq_detailed_queue_messages_unacked_ram      | Unacknowledged messages stored in memory                   |
| rabbitmq_detailed_queue_messages_persistent       | Persistent messages                                        |
| rabbitmq_detailed_queue_messages_persistent_bytes | Size in bytes of persistent messages                       |
| rabbitmq_detailed_queue_messages_bytes            | Size in bytes of ready and unacknowledged messages         |
| rabbitmq_detailed_queue_messages_ready_bytes      | Size in bytes of ready messages                            |
| rabbitmq_detailed_queue_messages_unacked_bytes    | Size in bytes of all unacknowledged messages               |
| rabbitmq_detailed_queue_messages_paged_out        | Messages paged out to disk                                 |
| rabbitmq_detailed_queue_messages_paged_out_bytes  | Size in bytes of messages paged out to disk                |
| rabbitmq_detailed_queue_disk_reads_total          | Total number of times queue read messages from disk        |
| rabbitmq_detailed_queue_disk_writes_total         | Total number of times queue wrote messages to disk         |

Tests show that performance difference between it and `queue_consumer_count` is approximately 8 times. E.g. on a test broker with 10k queues/producers/consumers, scrape time was ~8 second and ~1 respectively. So while it's expensive, it's not prohibitively so - especially compared to other metrics from per-connection/channel groups.

### Connection/channel metrics

All of those include Erlang PID in their label, which is rarely useful when ingested into Prometheus. And they are most expensive to produce, the most resources are spent by `/metrics/per-object` on these.

#### Connection metrics

Group `connection_coarse_metrics`:

| Metric                                                | Description                                    |
|-------------------------------------------------------|------------------------------------------------|
| rabbitmq_detailed_connection_incoming_bytes_total     | Total number of bytes received on a connection |
| rabbitmq_detailed_connection_outgoing_bytes_total     | Total number of bytes sent on a connection     |
| rabbitmq_detailed_connection_process_reductions_total | Total number of connection process reductions  |

Group `connection_metrics`:

| Metric                                              | Description                                          |
|-----------------------------------------------------|------------------------------------------------------|
| rabbitmq_detailed_connection_incoming_packets_total | Total number of packets received on a connection     |
| rabbitmq_detailed_connection_outgoing_packets_total | Total number of packets sent on a connection         |
| rabbitmq_detailed_connection_pending_packets        | Number of packets waiting to be sent on a connection |
| rabbitmq_detailed_connection_channels               | Channels on a connection                             |

#### General channel metrics

Group `channel_metrics`:

| Metric                                         | Description                                                           |
|------------------------------------------------|-----------------------------------------------------------------------|
| rabbitmq_detailed_channel_consumers            | Consumers on a channel                                                |
| rabbitmq_detailed_channel_messages_unacked     | Delivered but not yet acknowledged messages                           |
| rabbitmq_detailed_channel_messages_unconfirmed | Published but not yet confirmed messages                              |
| rabbitmq_detailed_channel_messages_uncommitted | Messages received in a transaction but not yet committed              |
| rabbitmq_detailed_channel_acks_uncommitted     | Message acknowledgements in a transaction not yet committed           |
| rabbitmq_detailed_consumer_prefetch            | Limit of unacknowledged messages for each consumer                    |
| rabbitmq_detailed_channel_prefetch             | Total limit of unacknowledged messages for all consumers on a channel |


Group `channel_process_metrics`:

| Metric                                             | Description                                |
|----------------------------------------------------|--------------------------------------------|
| rabbitmq_detailed_channel_process_reductions_total | Total number of channel process reductions |


#### Channel metrics with queue/exchange breakdowns

Group `channel_exchange_metrics`:

| Metric                                                       | Description                                                                                                  |
|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| rabbitmq_detailed_channel_messages_published_total           | Total number of messages published into an exchange on a channel                                             |
| rabbitmq_detailed_channel_messages_confirmed_total           | Total number of messages published into an exchange and confirmed on the channel                             |
| rabbitmq_detailed_channel_messages_unroutable_returned_total | Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable |
| rabbitmq_detailed_channel_messages_unroutable_dropped_total  | Total number of messages published as non-mandatory into an exchange and dropped as unroutable               |

Group `channel_queue_metrics`:

| Metric                                                 | Description                                                                       |
|--------------------------------------------------------|-----------------------------------------------------------------------------------|
| rabbitmq_detailed_channel_get_ack_total                | Total number of messages fetched with basic.get in manual acknowledgement mode    |
| rabbitmq_detailed_channel_get_total                    | Total number of messages fetched with basic.get in automatic acknowledgement mode |
| rabbitmq_detailed_channel_messages_delivered_ack_total | Total number of messages delivered to consumers in manual acknowledgement mode    |
| rabbitmq_detailed_channel_messages_delivered_total     | Total number of messages delivered to consumers in automatic acknowledgement mode |
| rabbitmq_detailed_channel_messages_redelivered_total   | Total number of messages redelivered to consumers                                 |
| rabbitmq_detailed_channel_messages_acked_total         | Total number of messages acknowledged by consumers                                |
| rabbitmq_detailed_channel_get_empty_total              | Total number of times basic.get operations fetched no message                     |

Group `channel_queue_exchange_metrics`:

| Metric                                           | Description                                  |
|--------------------------------------------------|----------------------------------------------|
| rabbitmq_detailed_queue_messages_published_total | Total number of messages published to queues |

### Virtual hosts and exchange metrics

These additional metrics can be useful when virtual hosts or exchanges are
created on a shared cluster in a self-service way. They are different
from the rest of the metrics: they are cluster-wide and not node-local.
These metrics **must not** be aggregated across cluster nodes.

Group `vhost_status`:

| Metric                        | Description                      |
|-------------------------------|----------------------------------|
| rabbitmq_cluster_vhost_status | Whether a given vhost is running |

Group `exchange_names`:

| Metric                         | Description                                                                                                                |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| rabbitmq_cluster_exchange_name | Enumerates exchanges without any additional info. This value is cluster-wide. A cheaper alternative to `exchange_bindings` |

Group `exchange_bindings`:

| Metric                             | Description                                                     |
|------------------------------------|-----------------------------------------------------------------|
| rabbitmq_cluster_exchange_bindings | Number of bindings for an exchange. This value is cluster-wide. |




