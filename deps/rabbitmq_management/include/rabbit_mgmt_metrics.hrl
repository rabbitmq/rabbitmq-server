%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developer of the Original Code is Pivotal Software, Inc.
%%   Copyright (c) 2010-2015 Pivotal Software, Inc.  All rights reserved.
%%

-define(DELIVER_GET, [deliver, deliver_no_ack, get, get_no_ack]).
-define(FINE_STATS, [publish, publish_in, publish_out,
                     ack, deliver_get, confirm, return_unroutable, redeliver] ++
            ?DELIVER_GET).

%% Most come from channels as fine stats, but queues emit these directly.
-define(QUEUE_MSG_RATES, [disk_reads, disk_writes]).

-define(MSG_RATES, ?FINE_STATS ++ ?QUEUE_MSG_RATES).

-define(MSG_RATES_DETAILS, [publish_details, publish_in_details,
                            publish_out_details, ack_details,
                            deliver_get_details, confirm_details,
                            return_unroutable_details, redeliver_details,
                            deliver_details, deliver_no_ack_details,
                            get_details, get_no_ack_details,
                            disk_reads_details, disk_writes_details] ++ ?MSG_RATES).

-define(QUEUE_MSG_COUNTS, [messages, messages_ready, messages_unacknowledged]).

-define(COARSE_NODE_STATS,
        [mem_used, fd_used, sockets_used, proc_used, disk_free,
         io_read_count,  io_read_bytes,  io_read_time,
         io_write_count, io_write_bytes, io_write_time,
         io_sync_count,  io_sync_time,
         io_seek_count,  io_seek_time,
         io_reopen_count, mnesia_ram_tx_count,  mnesia_disk_tx_count,
         msg_store_read_count, msg_store_write_count,
         queue_index_journal_write_count,
         queue_index_write_count, queue_index_read_count,
         gc_num, gc_bytes_reclaimed, context_switches,
         io_file_handle_open_attempt_count, io_file_handle_open_attempt_time]).

-define(COARSE_NODE_NODE_STATS, [send_bytes, recv_bytes]).

%% Normally 0 and no history means "has never happened, don't
%% report". But for these things we do want to report even at 0 with
%% no history.
-define(ALWAYS_REPORT_STATS,
        [io_read_time, io_write_time,
         io_sync_time, sockets_used | ?QUEUE_MSG_COUNTS]).

-define(COARSE_CONN_STATS, [recv_oct, send_oct]).

-define(PROCESS_STATS, [reductions]).

-type(event_type() :: queue_stats | queue_exchange_stats | vhost_stats
                    | channel_queue_stats | channel_stats
                    | channel_exchange_stats | exchange_stats
                    | node_stats | node_node_stats | connection_stats).
-type(type() :: deliver_get | fine_stats | queue_msg_rates | queue_msg_counts
              | coarse_node_stats | coarse_node_node_stats | coarse_conn_stats
              | process_stats).

-type(table_name() :: atom()).

%% TODO remove unused tables
%% Not all events generate all metrics, so some of the tables may be deleted
-define(AGGR_TABLES, [aggr_queue_stats_fine_stats,
                      aggr_queue_stats_deliver_get,
                      aggr_queue_stats_queue_msg_counts,
                      aggr_queue_stats_queue_msg_rates,
                      aggr_queue_stats_process_stats,
                      aggr_queue_exchange_stats_fine_stats,
                      aggr_vhost_stats_deliver_get,
                      aggr_vhost_stats_fine_stats,
                      aggr_vhost_stats_queue_msg_rates,
                      aggr_vhost_stats_queue_msg_counts,
                      aggr_vhost_stats_coarse_conn_stats,
                      aggr_channel_queue_stats_deliver_get,
                      aggr_channel_queue_stats_fine_stats,
                      aggr_channel_queue_stats_queue_msg_counts,
                      aggr_channel_stats_deliver_get,
                      aggr_channel_stats_fine_stats,
                      aggr_channel_stats_queue_msg_counts,
                      aggr_channel_stats_process_stats,
                      aggr_channel_exchange_stats_deliver_get,
                      aggr_channel_exchange_stats_fine_stats,
                      aggr_exchange_stats_fine_stats,
                      aggr_node_stats_coarse_node_stats,
                      aggr_node_node_stats_coarse_node_node_stats,
                      aggr_connection_stats_coarse_conn_stats,
                      aggr_connection_stats_process_stats
                     ]).

-define(INDEX_TABLES, [aggr_queue_stats_fine_stats_index,
                       aggr_queue_stats_deliver_get_index,
                       aggr_queue_stats_queue_msg_counts_index,
                       aggr_queue_stats_queue_msg_rates_index,
                       aggr_queue_stats_process_stats_index,
                       aggr_queue_exchange_stats_fine_stats_index,
                       aggr_vhost_stats_deliver_get_index,
                       aggr_vhost_stats_fine_stats_index,
                       aggr_vhost_stats_queue_msg_rates_index,
                       aggr_vhost_stats_queue_msg_counts_index,
                       aggr_vhost_stats_coarse_conn_stats_index,
                       aggr_channel_queue_stats_deliver_get_index,
                       aggr_channel_queue_stats_fine_stats_index,
                       aggr_channel_queue_stats_queue_msg_counts_index,
                       aggr_channel_stats_deliver_get_index,
                       aggr_channel_stats_fine_stats_index,
                       aggr_channel_stats_queue_msg_counts_index,
                       aggr_channel_stats_process_stats_index,
                       aggr_channel_exchange_stats_deliver_get_index,
                       aggr_channel_exchange_stats_fine_stats_index,
                       aggr_exchange_stats_fine_stats_index,
                       aggr_node_stats_coarse_node_stats_index,
                       aggr_node_node_stats_coarse_node_node_stats_index,
                       aggr_connection_stats_coarse_conn_stats_index,
                       aggr_connection_stats_process_stats_index
                      ]).

-define(KEY_INDEX_TABLES,
        [aggr_queue_stats_fine_stats_key_index,
         aggr_queue_stats_deliver_get_key_index,
         aggr_queue_stats_queue_msg_counts_key_index,
         aggr_queue_stats_queue_msg_rates_key_index,
         aggr_queue_stats_process_stats_key_index,
         aggr_queue_exchange_stats_fine_stats_key_index,
         aggr_vhost_stats_deliver_get_key_index,
         aggr_vhost_stats_fine_stats_key_index,
         aggr_vhost_stats_queue_msg_rates_key_index,
         aggr_vhost_stats_queue_msg_counts_key_index,
         aggr_vhost_stats_coarse_conn_stats_key_index,
         aggr_channel_queue_stats_deliver_get_key_index,
         aggr_channel_queue_stats_fine_stats_key_index,
         aggr_channel_queue_stats_queue_msg_counts_key_index,
         aggr_channel_stats_deliver_get_key_index,
         aggr_channel_stats_fine_stats_key_index,
         aggr_channel_stats_queue_msg_counts_key_index,
         aggr_channel_stats_process_stats_key_index,
         aggr_channel_exchange_stats_deliver_get_key_index,
         aggr_channel_exchange_stats_fine_stats_key_index,
         aggr_exchange_stats_fine_stats_key_index,
         aggr_node_stats_coarse_node_stats_key_index,
         aggr_node_node_stats_coarse_node_node_stats_key_index,
         aggr_connection_stats_coarse_conn_stats_key_index,
         aggr_connection_stats_process_stats_key_index
        ]).

-define(PROC_STATS_TABLES,
        [channel_stats, connection_stats]).

%% Records are only used to retrieve the field position and to facilitate
%% keeping track of the data
-record(deliver_get, {deliver,
                      deliver_no_ack,
                      get,
                      get_no_ack}).
-record(fine_stats, {publish,
                     publish_in,
                     publish_out,
                     ack,
                     deliver_get,
                     confirm,
                     return_unroutable,
                     redeliver}).
-record(queue_msg_rates, {disk_reads,
                          disk_writes}).
-record(queue_msg_counts, {messages,
                           messages_ready,
                           messages_unacknowledged}).
-record(coarse_node_stats, {mem_used,
                            fd_used,
                            sockets_used,
                            proc_used,
                            disk_free,
                            io_read_count,
                            io_read_bytes,
                            io_read_time,
                            io_write_count,
                            io_write_bytes,
                            io_write_time,
                            io_sync_count,
                            io_sync_time,
                            io_seek_count,
                            io_seek_time,
                            io_reopen_count,
                            mnesia_ram_tx_count,
                            mnesia_disk_tx_count,
                            msg_store_read_count,
                            msg_store_write_count,
                            queue_index_journal_write_count,
                            queue_index_write_count,
                            queue_index_read_count,
                            gc_num,
                            gc_bytes_reclaimed,
                            context_switches,
                            io_file_handle_open_attempt_count,
                            io_file_handle_open_attempt_time}).
-record(coarse_node_node_stats, {send_bytes,
                                 recv_bytes}).
-record(coarse_conn_stats, {recv_oct,
                            send_oct}).
-record(process_stats, {reductions}).
