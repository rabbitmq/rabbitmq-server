# Metrics

<!-- TOC depthFrom:2 depthTo:6 withLinks:1 updateOnSave:1 orderedList:0 -->

- [RabbitMQ](#rabbitmq)
	- [Global Counters](#global-counters)
	- [Generic](#overview)
	- [Connections](#connections)
	- [Channels](#channels)
	- [Queues](#queues)
	- [Erlang via RabbitMQ](#erlang-via-rabbitmq)
	- [Disk IO](#disk-io)
	- [Raft](#raft)
- [Telemetry](#telemetry)
- [Erlang](#erlang)
	- [Mnesia](#mnesia)
	- [VM](#vm)

<!-- /TOC -->

## RabbitMQ

All metrics are in alphabetical order.

### Global Counters

These were introduced to address an inherent flaw with existing counters when
metrics are aggregated (default behaviour). When connections or channels
terminate, their metrics get garbage collected (meaning that they disappear
after a while). All counters that aggregate metrics across connections and
channels decrease, since the sum is now lower, and that in turn results rate &
irate functions in Prometheus returning nonsensical values (e.g. 4Mil msg/s).
This problem is made worse by Streams, since those values **may** be real, but
we can't trust them, which is the worst place to be in. The Global counters fix
this, and also introduce per-protocol as well as per-protocol AND queue type
metrics, which is something that many of you have requested for a while now.

<!--
To generate these:
  1. From within the rabbitmq-server repository, run the following command:
     make run-broker
  2. In vim, position the cursor where you want the metrics importing, then run:
     r !curl -s localhost:15692/metrics | grep 'HELP rabbitmq_global_'
  3. Also in vim, select all formatted lines and run:
     sort
  4. Still in vim, build & run a macro re-formats all other metrics
  5. Lastly, visual select all lines in vim and run the following command:
     Tabularize /|
-->

| Metric                                                      | Description                                                                                                  |
| ---                                                         | ---                                                                                                          |
| rabbitmq_global_messages_acknowledged_total                 | Total number of messages acknowledged by consumers                                                           |
| rabbitmq_global_messages_confirmed_total                    | Total number of messages confirmed to publishers                                                             |
| rabbitmq_global_messages_delivered_consume_auto_ack_total   | Total number of messages delivered to consumers using basic.consume with automatic acknowledgment            |
| rabbitmq_global_messages_delivered_consume_manual_ack_total | Total number of messages delivered to consumers using basic.consume with manual acknowledgment               |
| rabbitmq_global_messages_delivered_get_auto_ack_total       | Total number of messages delivered to consumers using basic.get with automatic acknowledgment                |
| rabbitmq_global_messages_delivered_get_manual_ack_total     | Total number of messages delivered to consumers using basic.get with manual acknowledgment                   |
| rabbitmq_global_messages_delivered_total                    | Total number of messages delivered to consumers                                                              |
| rabbitmq_global_messages_get_empty_total                    | Total number of times basic.get operations fetched no message                                                |
| rabbitmq_global_messages_received_confirm_total             | Total number of messages received from publishers expecting confirmations                                    |
| rabbitmq_global_messages_received_total                     | Total number of messages received from publishers                                                            |
| rabbitmq_global_messages_redelivered_total                  | Total number of messages redelivered to consumers                                                            |
| rabbitmq_global_messages_routed_total                       | Total number of messages routed to queues or streams                                                         |
| rabbitmq_global_messages_unroutable_dropped_total           | Total number of messages published as non-mandatory into an exchange and dropped as unroutable               |
| rabbitmq_global_messages_unroutable_returned_total          | Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable |
| rabbitmq_global_publishers                                  | Publishers currently connected |
| rabbitmq_global_consumers                                   | Consumers currently connected |

### Generic

| Metric                                    | Description                                        |
| ---                                       | ---                                                |
| rabbitmq_build_info                       | RabbitMQ & Erlang/OTP version info                 |
| rabbitmq_consumer_prefetch                | Limit of unacknowledged messages for each consumer |
| rabbitmq_consumers                        | Consumers currently connected                      |
| rabbitmq_disk_space_available_bytes       | Disk space available in bytes                      |
| rabbitmq_disk_space_available_limit_bytes | Free disk space low watermark in bytes             |
| rabbitmq_identity_info                    | RabbitMQ node & cluster identity info              |
| rabbitmq_process_max_fds                  | Open file descriptors limit                        |
| rabbitmq_process_max_tcp_sockets          | Open TCP sockets limit                             |
| rabbitmq_process_open_fds                 | Open file descriptors                              |
| rabbitmq_process_open_tcp_sockets         | Open TCP sockets                                   |
| rabbitmq_process_resident_memory_bytes    | Memory used in bytes                               |
| rabbitmq_resident_memory_limit_bytes      | Memory high watermark in bytes                     |

### Connections

| Metric                                       | Description                                          |
| ---                                          | ---                                                  |
| rabbitmq_connection_channels                 | Channels on a connection                             |
| rabbitmq_connection_incoming_bytes_total     | Total number of bytes received on a connection       |
| rabbitmq_connection_incoming_packets_total   | Total number of packets received on a connection     |
| rabbitmq_connection_outgoing_bytes_total     | Total number of bytes sent on a connection           |
| rabbitmq_connection_outgoing_packets_total   | Total number of packets sent on a connection         |
| rabbitmq_connection_pending_packets          | Number of packets waiting to be sent on a connection |
| rabbitmq_connection_process_reductions_total | Total number of connection process reductions        |
| rabbitmq_connections                         | Connections currently open                           |
| rabbitmq_connections_closed_total            | Total number of connections closed or terminated     |
| rabbitmq_connections_opened_total            | Total number of connections opened                   |

### Channels

| Metric                                              | Description                                                                                                  |
| ---                                                 | ---                                                                                                          |
| rabbitmq_channel_acks_uncommitted                   | Message acknowledgements in a transaction not yet committed                                                  |
| rabbitmq_channel_consumers                          | Consumers on a channel                                                                                       |
| rabbitmq_channel_get_ack_total                      | Total number of messages fetched with basic.get in manual acknowledgement mode                               |
| rabbitmq_channel_get_empty_total                    | Total number of times basic.get operations fetched no message                                                |
| rabbitmq_channel_get_total                          | Total number of messages fetched with basic.get in automatic acknowledgement mode                            |
| rabbitmq_channel_messages_acked_total               | Total number of messages acknowledged by consumers                                                           |
| rabbitmq_channel_messages_confirmed_total           | Total number of messages published into an exchange and confirmed on the channel                             |
| rabbitmq_channel_messages_delivered_ack_total       | Total number of messages delivered to consumers in manual acknowledgement mode                               |
| rabbitmq_channel_messages_delivered_total           | Total number of messages delivered to consumers in automatic acknowledgement mode                            |
| rabbitmq_channel_messages_published_total           | Total number of messages published into an exchange on a channel                                             |
| rabbitmq_channel_messages_redelivered_total         | Total number of messages redelivered to consumers                                                            |
| rabbitmq_channel_messages_unacked                   | Delivered but not yet acknowledged messages                                                                  |
| rabbitmq_channel_messages_uncommitted               | Messages received in a transaction but not yet committed                                                     |
| rabbitmq_channel_messages_unconfirmed               | Published but not yet confirmed messages                                                                     |
| rabbitmq_channel_messages_unroutable_dropped_total  | Total number of messages published as non-mandatory into an exchange and dropped as unroutable               |
| rabbitmq_channel_messages_unroutable_returned_total | Total number of messages published as mandatory into an exchange and returned to the publisher as unroutable |
| rabbitmq_channel_prefetch                           | Total limit of unacknowledged messages for all consumers on a channel                                        |
| rabbitmq_channel_process_reductions_total           | Total number of channel process reductions                                                                   |
| rabbitmq_channels                                   | Channels currently open                                                                                      |
| rabbitmq_channels_closed_total                      | Total number of channels closed                                                                              |
| rabbitmq_channels_opened_total                      | Total number of channels opened                                                                              |


### Queues

| Metric                                   | Description                                                  |
| ---                                      | ---                                                          |
| rabbitmq_queue_consumer_utilisation      | Consumer utilisation                                         |
| rabbitmq_queue_consumers                 | Consumers on a queue                                         |
| rabbitmq_queue_disk_reads_total          | Total number of times queue read messages from disk          |
| rabbitmq_queue_disk_writes_total         | Total number of times queue wrote messages to disk           |
| rabbitmq_queue_messages                  | Sum of ready and unacknowledged messages - total queue depth |
| rabbitmq_queue_messages_bytes            | Size in bytes of ready and unacknowledged messages           |
| rabbitmq_queue_messages_paged_out        | Messages paged out to disk                                   |
| rabbitmq_queue_messages_paged_out_bytes  | Size in bytes of messages paged out to disk                  |
| rabbitmq_queue_messages_persistent       | Persistent messages                                          |
| rabbitmq_queue_messages_persistent_bytes | Size in bytes of persistent messages                         |
| rabbitmq_queue_messages_published_total  | Total number of messages published to queues                 |
| rabbitmq_queue_messages_ram              | Ready and unacknowledged messages stored in memory           |
| rabbitmq_queue_messages_ram_bytes        | Size of ready and unacknowledged messages stored in memory   |
| rabbitmq_queue_messages_ready            | Messages ready to be delivered to consumers                  |
| rabbitmq_queue_messages_ready_bytes      | Size in bytes of ready messages                              |
| rabbitmq_queue_messages_ready_ram        | Ready messages stored in memory                              |
| rabbitmq_queue_messages_unacked          | Messages delivered to consumers but not yet acknowledged     |
| rabbitmq_queue_messages_unacked_bytes    | Size in bytes of all unacknowledged messages                 |
| rabbitmq_queue_messages_unacked_ram      | Unacknowledged messages stored in memory                     |
| rabbitmq_queue_process_memory_bytes      | Memory in bytes used by the Erlang queue process             |
| rabbitmq_queue_process_reductions_total  | Total number of queue process reductions                     |
| rabbitmq_queues                          | Queues available                                             |
| rabbitmq_queues_created_total            | Total number of queues created                               |
| rabbitmq_queues_declared_total           | Total number of queues declared                              |
| rabbitmq_queues_deleted_total            | Total number of queues deleted                               |



### Erlang via RabbitMQ

| Metric                                           | Description                                                     |
| ---                                              | ---                                                             |
| rabbitmq_erlang_gc_reclaimed_bytes_totalTotal    | number of bytes of memory reclaimed by Erlang garbage collector |
| rabbitmq_erlang_gc_runs_total                    | Total number of Erlang garbage collector runs                   |
| rabbitmq_erlang_net_ticktime_seconds             | Inter-node heartbeat interval in seconds                        |
| rabbitmq_erlang_processes_limit                  | Erlang processes limit                                          |
| rabbitmq_erlang_processes_used                   | Erlang processes used                                           |
| rabbitmq_erlang_scheduler_context_switches_total | Total number of Erlang scheduler context switches               |
| rabbitmq_erlang_scheduler_run_queue              | Erlang scheduler run queue                                      |
| rabbitmq_erlang_uptime_seconds                   | Node uptime                                                     |

### Disk IO

| Metric                                       | Description                                          |
| ---                                          | ---                                                  |
| rabbitmq_io_open_attempt_ops_total           | Total number of file open attempts                   |
| rabbitmq_io_open_attempt_time_seconds_total  | Total file open attempts time                        |
| rabbitmq_io_read_bytes_total                 | Total number of I/O bytes read                       |
| rabbitmq_io_read_ops_total                   | Total number of I/O read operations                  |
| rabbitmq_io_read_time_seconds_total          | Total I/O read time                                  |
| rabbitmq_io_reopen_ops_total                 | Total number of times files have been reopened       |
| rabbitmq_io_seek_ops_total                   | Total number of I/O seek operations                  |
| rabbitmq_io_seek_time_seconds_total          | Total I/O seek time                                  |
| rabbitmq_io_sync_ops_total                   | Total number of I/O sync operations                  |
| rabbitmq_io_sync_time_seconds_total          | Total I/O sync time                                  |
| rabbitmq_io_write_bytes_total                | Total number of I/O bytes written                    |
| rabbitmq_io_write_ops_total                  | Total number of I/O write operations                 |
| rabbitmq_io_write_time_seconds_total         | Total I/O write time                                 |
| rabbitmq_msg_store_read_total                | Total number of Message Store read operations        |
| rabbitmq_msg_store_write_total               | Total number of Message Store write operations       |
| rabbitmq_queue_index_journal_write_ops_total | Total number of Queue Index Journal write operations |
| rabbitmq_queue_index_read_ops_total          | Total number of Queue Index read operations          |
| rabbitmq_queue_index_write_ops_total         | Total number of Queue Index write operations         |
| rabbitmq_schema_db_disk_tx_total             | Total number of Schema DB disk transactions          |
| rabbitmq_schema_db_ram_tx_total              | Total number of Schema DB memory transactions        |

### Raft

| Metric                                     | Description                             |
| ---                                        | ---                                     |
| rabbitmq_raft_entry_commit_latency_seconds | Time taken for an entry to be committed |
| rabbitmq_raft_log_commit_index             | Raft log commit index                   |
| rabbitmq_raft_log_last_applied_index       | Raft log last applied index             |
| rabbitmq_raft_log_last_written_index       | Raft log last written index             |
| rabbitmq_raft_log_snapshot_index           | Raft log snapshot index                 |
| rabbitmq_raft_term_total                   | Current Raft term number                |

## Telemetry

| Metric                              | Description              |
| ---                                 | ---                      |
| telemetry_scrape_duration_seconds   | Scrape duration          |
| telemetry_scrape_encoded_size_bytes | Scrape size, encoded     |
| telemetry_scrape_size_bytes         | Scrape size, not encoded |

## Erlang

### Mnesia

| Metric                                 | Description                                  |
| ---                                    | ---                                          |
| erlang_mnesia_committed_transactions   | Number of committed transactions             |
| erlang_mnesia_failed_transactions      | Number of failed (i.e. aborted) transactions |
| erlang_mnesia_held_locks               | Number of held locks                         |
| erlang_mnesia_lock_queue               | Number of transactions waiting for a lock    |
| erlang_mnesia_logged_transactions      | Number of transactions logged                |
| erlang_mnesia_restarted_transactions   | Total number of transaction restarts         |
| erlang_mnesia_transaction_coordinators | Number of coordinator transactions           |
| erlang_mnesia_transaction_participants | Number of participant transactions           |


### VM

| Metric                                                  | Description                                                                                                                                                                                                      |
| ---                                                     | ---                                                                                                                                                                                                              |
| erlang_vm_allocators                                    | Allocated (carriers_size) and used (blocks_size) memory for the different allocators in the VM. See erts_alloc(3).                                                                                               |
| erlang_vm_atom_count                                    | The number of atom currently existing at the local node.                                                                                                                                                         |
| erlang_vm_atom_limit                                    | The maximum number of simultaneously existing atom at the local node.                                                                                                                                            |
| erlang_vm_dirty_cpu_schedulers                          | The number of scheduler dirty CPU scheduler threads used by the emulator.                                                                                                                                        |
| erlang_vm_dirty_cpu_schedulers_online                   | The number of dirty CPU scheduler threads online.                                                                                                                                                                |
| erlang_vm_dirty_io_schedulers                           | The number of scheduler dirty I/O scheduler threads used by the emulator.                                                                                                                                        |
| erlang_vm_dist_node_queue_size_bytes                    | The number of bytes in the output distribution queue. This queue sits between the Erlang code and the port driver.                                                                                               |
| erlang_vm_dist_node_state                               | The current state of the distribution link. The state is represented as a numerical value where `pending=1', `up_pending=2' and `up=3'.                                                                          |
| erlang_vm_dist_port_input_bytes                         | The total number of bytes read from the port.                                                                                                                                                                    |
| erlang_vm_dist_port_memory_bytes                        | The total number of bytes allocated for this port by the runtime system. The port itself can have allocated memory that is not included.                                                                         |
| erlang_vm_dist_port_output_bytes                        | The total number of bytes written to the port.                                                                                                                                                                   |
| erlang_vm_dist_port_queue_size_bytes                    | The total number of bytes queued by the port using the ERTS driver queue implementation.                                                                                                                         |
| erlang_vm_dist_proc_heap_size_words                     | The size in words of the youngest heap generation of the process. This generation includes the process stack. This information is highly implementation-dependent, and can change if the implementation changes. |
| erlang_vm_dist_proc_memory_bytes                        | The size in bytes of the process. This includes call stack, heap, and internal structures.                                                                                                                       |
| erlang_vm_dist_proc_message_queue_len                   | The number of messages currently in the message queue of the process.                                                                                                                                            |
| erlang_vm_dist_proc_min_bin_vheap_size_words            | The minimum binary virtual heap size for the process.                                                                                                                                                            |
| erlang_vm_dist_proc_min_heap_size_words                 | The minimum heap size for the process.                                                                                                                                                                           |
| erlang_vm_dist_proc_reductions                          | The number of reductions executed by the process.                                                                                                                                                                |
| erlang_vm_dist_proc_stack_size_words                    | The stack size, in words, of the process.                                                                                                                                                                        |
| erlang_vm_dist_proc_status                              | The current status of the distribution process. The status is represented as a numerical value where `exiting=1', `suspended=2', `runnable=3', `garbage_collecting=4', `running=5' and `waiting=6'.              |
| erlang_vm_dist_proc_total_heap_size_words               | The total size, in words, of all heap fragments of the process. This includes the process stack and any unreceived messages that are considered to be part of the heap.                                          |
| erlang_vm_dist_recv_avg_bytes                           | Average size of packets, in bytes, received by the socket.                                                                                                                                                       |
| erlang_vm_dist_recv_bytes                               | Number of bytes received by the socket.                                                                                                                                                                          |
| erlang_vm_dist_recv_cnt                                 | Number of packets received by the socket.                                                                                                                                                                        |
| erlang_vm_dist_recv_dvi_bytes                           | Average packet size deviation, in bytes, received by the socket.                                                                                                                                                 |
| erlang_vm_dist_recv_max_bytes                           | Size of the largest packet, in bytes, received by the socket.                                                                                                                                                    |
| erlang_vm_dist_send_avg_bytes                           | Average size of packets, in bytes, sent from the socket.                                                                                                                                                         |
| erlang_vm_dist_send_bytes                               | Number of bytes sent from the socket.                                                                                                                                                                            |
| erlang_vm_dist_send_cnt                                 | Number of packets sent from the socket.                                                                                                                                                                          |
| erlang_vm_dist_send_max_bytes                           | Size of the largest packet, in bytes, sent from the socket.                                                                                                                                                      |
| erlang_vm_dist_send_pend_bytes                          | Number of bytes waiting to be sent by the socket.                                                                                                                                                                |
| erlang_vm_ets_limit                                     | The maximum number of ETS tables allowed.                                                                                                                                                                        |
| erlang_vm_logical_processors                            | The detected number of logical processors configured in the system.                                                                                                                                              |
| erlang_vm_logical_processors_available                  | The detected number of logical processors available to the Erlang runtime system.                                                                                                                                |
| erlang_vm_logical_processors_online                     | The detected number of logical processors online on the system.                                                                                                                                                  |
| erlang_vm_memory_atom_bytes_total                       | The total amount of memory currently allocated for atoms. This memory is part of the memory presented as system memory.                                                                                          |
| erlang_vm_memory_bytes_total                            | The total amount of memory currently allocated. This is the same as the sum of the memory size for processes and system.                                                                                         |
| erlang_vm_memory_dets_tables                            | Erlang VM DETS Tables count.                                                                                                                                                                                     |
| erlang_vm_memory_ets_tables                             | Erlang VM ETS Tables count.                                                                                                                                                                                      |
| erlang_vm_memory_processes_bytes_total                  | The total amount of memory currently allocated for the Erlang processes.                                                                                                                                         |
| erlang_vm_memory_system_bytes_total                     | The total amount of memory currently allocated for the emulator that is not directly related to any Erlang process. Memory presented as processes is not included in this memory.                                |
| erlang_vm_port_count                                    | The number of ports currently existing at the local node.                                                                                                                                                        |
| erlang_vm_port_limit                                    | The maximum number of simultaneously existing ports at the local node.                                                                                                                                           |
| erlang_vm_process_count                                 | The number of processes currently existing at the local node.                                                                                                                                                    |
| erlang_vm_process_limit                                 | The maximum number of simultaneously existing processes at the local node.                                                                                                                                       |
| erlang_vm_schedulers                                    | The number of scheduler threads used by the emulator.                                                                                                                                                            |
| erlang_vm_schedulers_online                             | The number of schedulers online.                                                                                                                                                                                 |
| erlang_vm_smp_support                                   | 1 if the emulator has been compiled with SMP support, otherwise 0.                                                                                                                                               |
| erlang_vm_statistics_bytes_output_total                 | Total number of bytes output to ports.                                                                                                                                                                           |
| erlang_vm_statistics_bytes_received_total               | Total number of bytes received through ports.                                                                                                                                                                    |
| erlang_vm_statistics_context_switches                   | Total number of context switches since the system started.                                                                                                                                                       |
| erlang_vm_statistics_dirty_cpu_run_queue_length         | Length of the dirty CPU run-queue.                                                                                                                                                                               |
| erlang_vm_statistics_dirty_io_run_queue_length          | Length of the dirty IO run-queue.                                                                                                                                                                                |
| erlang_vm_statistics_garbage_collection_bytes_reclaimed | Garbage collection: bytes reclaimed.                                                                                                                                                                             |
| erlang_vm_statistics_garbage_collection_number_of_gcs   | Garbage collection: number of GCs.                                                                                                                                                                               |
| erlang_vm_statistics_garbage_collection_words_reclaimed | Garbage collection: words reclaimed.                                                                                                                                                                             |
| erlang_vm_statistics_reductions_total                   | Total reductions.                                                                                                                                                                                                |
| erlang_vm_statistics_run_queues_length_total            | Length of normal run-queues.                                                                                                                                                                                     |
| erlang_vm_statistics_runtime_milliseconds               | The sum of the runtime for all threads in the Erlang runtime system. Can be greater than wall clock time.                                                                                                        |
| erlang_vm_statistics_wallclock_time_milliseconds        | Information about wall clock. Same as erlang_vm_statistics_runtime_milliseconds except that real time is measured.                                                                                               |
| erlang_vm_statistics_wallclock_time_milliseconds        | Information about wall clock. Same as erlang_vm_statistics_runtime_milliseconds except that real time is measured.                                                                                               |
| erlang_vm_thread_pool_size                              | The number of async threads in the async thread pool used for asynchronous driver calls.                                                                                                                         |
| erlang_vm_threads                                       | 1 if the emulator has been compiled with thread support, otherwise 0.                                                                                                                                            |
| erlang_vm_time_correction                               | 1 if time correction is enabled, otherwise 0.                                                                                                                                                                    |
