%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-type(event_type() :: queue_stats | queue_exchange_stats | vhost_stats
                    | channel_queue_stats | channel_stats
                    | channel_exchange_stats | exchange_stats
                    | node_stats | node_node_stats | connection_stats).
-type(type() :: deliver_get | fine_stats | queue_msg_rates | queue_msg_counts
              | coarse_node_stats | coarse_node_node_stats | coarse_conn_stats
              | process_stats).

-type(table_name() :: atom()).

-define(TABLES, [{connection_stats_coarse_conn_stats, set},
                 {vhost_stats_coarse_conn_stats, set},
                 {connection_created_stats, set},
                 {connection_stats, set},
                 {channel_created_stats, set},
                 {channel_stats, set},
                 {channel_stats_fine_stats, set},
                 {channel_exchange_stats_fine_stats, set},
                 {channel_queue_stats_deliver_stats, set},
                 {vhost_stats_fine_stats, set},
                 {queue_stats_deliver_stats, set},
                 {vhost_stats_deliver_stats, set},
                 {channel_stats_deliver_stats, set},
                 {channel_process_stats, set},
                 {queue_stats_publish, set},
                 {queue_exchange_stats_publish, set},
                 {exchange_stats_publish_out, set},
                 {exchange_stats_publish_in, set},
                 {consumer_stats, set},
                 {queue_stats, set},
                 {queue_basic_stats, set},
                 {queue_msg_stats, set},
                 {vhost_msg_stats, set},
                 {queue_process_stats, set},
                 {node_stats, set},
                 {node_coarse_stats, set},
                 {node_persister_stats, set},
                 {node_node_stats, set},
                 {node_node_coarse_stats, set},
                 {queue_msg_rates, set},
                 {vhost_msg_rates, set},
                 {connection_churn_rates, set}]).

-define(INDEX_TABLES, [consumer_stats_queue_index,
                       consumer_stats_channel_index,
                       channel_exchange_stats_fine_stats_exchange_index,
                       channel_exchange_stats_fine_stats_channel_index,
                       channel_queue_stats_deliver_stats_queue_index,
                       channel_queue_stats_deliver_stats_channel_index,
                       queue_exchange_stats_publish_queue_index,
                       queue_exchange_stats_publish_exchange_index,
                       node_node_coarse_stats_node_index]).

-define(GC_EVENTS, [connection_closed, channel_closed, consumer_deleted,
                    exchange_deleted, queue_deleted, vhost_deleted,
                    node_node_deleted, channel_consumer_deleted]).

-define(DELEGATE_PREFIX, "delegate_management_").

%%------------------------------------------------------------------------------
%% Only for documentation and testing purposes, so we keep track of the number and
%% order of the metrics
-define(connection_stats_coarse_conn_stats(Recv_oct, Send_oct, Reductions),
        {Recv_oct, Send_oct, Reductions}).
-define(vhost_stats_coarse_conn_stats(Recv_oct, Send_oct), {Recv_oct, Send_oct}).
-define(connection_created_stats(Id, Name, Props), {Id, Name, Props}).
-define(connection_stats(Id, Props), {Id, Props}).
-define(channel_created_stats(Id, Name, Props), {Id, Name, Props}).
-define(channel_consumer_created_stats(Queue, ChPid, ConsumerTag),
        {Queue, {ChPid, ConsumerTag}}).
-define(channel_stats(Id, Props), {Id, Props}).
-define(channel_stats_fine_stats(Publish, Confirm, ReturnUnroutable, DropUnroutable),
        {Publish, Confirm, ReturnUnroutable, DropUnroutable}).
-define(channel_exchange_stats_fine_stats(Publish, Confirm, ReturnUnroutable, DropUnroutable),
        {Publish, Confirm, ReturnUnroutable, DropUnroutable}).
-define(channel_queue_stats_deliver_stats(Get, Get_no_ack, Deliver, Deliver_no_ack,
                                          Redeliver, Ack, Deliver_get, Get_empty),
        {Get, Get_no_ack, Deliver, Deliver_no_ack, Redeliver, Ack, Deliver_get,
         Get_empty}).
-define(vhost_stats_fine_stats(Publish, Confirm, ReturnUnroutable, DropUnroutable),
        {Publish, Confirm, ReturnUnroutable, DropUnroutable}).
-define(queue_stats_deliver_stats(Get, Get_no_ack, Deliver, Deliver_no_ack,
                                  Redeliver, Ack, Deliver_get, Get_empty),
        {Get, Get_no_ack, Deliver, Deliver_no_ack, Redeliver, Ack, Deliver_get,
         Get_empty}).
-define(vhost_stats_deliver_stats(Get, Get_no_ack, Deliver, Deliver_no_ack,
                                  Redeliver, Ack, Deliver_get, Get_empty),
        {Get, Get_no_ack, Deliver, Deliver_no_ack, Redeliver, Ack, Deliver_get,
         Get_empty}).
-define(channel_stats_deliver_stats(Get, Get_no_ack, Deliver, Deliver_no_ack,
                                    Redeliver, Ack, Deliver_get, Get_empty),
        {Get, Get_no_ack, Deliver, Deliver_no_ack, Redeliver, Ack, Deliver_get,
         Get_empty}).
-define(channel_process_stats(Reductions), {Reductions}).
-define(queue_stats_publish(Publish), {Publish}).
-define(queue_exchange_stats_publish(Publish), {Publish}).
-define(exchange_stats_publish_out(Publish_out), {Publish_out}).
-define(exchange_stats_publish_in(Publish_in), {Publish_in}).
-define(consumer_stats(Id, Props), {Id, Props}).
-define(queue_stats(Id, Props), {Id, Props}).
-define(queue_msg_stats(Messages_ready, Messages_unacknowledged, Messages),
        {Messages_ready, Messages_unacknowledged, Messages}).
-define(vhost_msg_stats(Messages_ready, Messages_unacknowledged, Messages),
        {Messages_ready, Messages_unacknowledged, Messages}).
-define(queue_process_stats(Reductions), {Reductions}).
-define(node_stats(Id, Props), {Id, Props}).
-define(node_coarse_stats(Fd_used, Sockets_used, Mem_used, Disk_free, Proc_used,
                          Gc_num, Gc_bytes_reclaimed, Context_switches),
        {Fd_used, Sockets_used, Mem_used, Disk_free, Proc_used, Gc_num,
         Gc_bytes_reclaimed, Context_switches}).
-define(node_persister_stats(Io_read_count, Io_read_bytes, Io_read_avg_time, Io_write_count,
                             Io_write_bytes, Io_write_avg_time, Io_sync_count, Io_sync_avg_time,
                             Io_seek_count, Io_seek_avg_time, Io_reopen_count, Mnesia_ram_tx_count,
                             Mnesia_disk_tx_count, Msg_store_read_count, Msg_store_write_count,
                             Unused1, Queue_index_write_count,
                             Queue_index_read_count, Unused2,
                             Unused3),
        {Io_read_count, Io_read_bytes, Io_read_avg_time, Io_write_count, Io_write_bytes,
         Io_write_avg_time, Io_sync_count, Io_sync_avg_time, Io_seek_count, Io_seek_avg_time,
         Io_reopen_count, Mnesia_ram_tx_count, Mnesia_disk_tx_count, Msg_store_read_count,
         Msg_store_write_count, Unused1, Queue_index_write_count,
         Queue_index_read_count, Unused2,
         Unused3}).
-define(node_node_stats(Send_bytes, Recv_bytes), {Send_bytes, Recv_bytes}).
-define(node_node_coarse_stats(Send_bytes, Recv_bytes), {Send_bytes, Recv_bytes}).
-define(queue_msg_rates(Disk_reads, Disk_writes), {Disk_reads, Disk_writes}).
-define(vhost_msg_rates(Disk_reads, Disk_writes), {Disk_reads, Disk_writes}).
-define(old_aggr_stats(Id, Stats), {Id, Stats}).
-define(connection_churn_rates(Connection_created, Connection_closed, Channel_created,
                               Channel_closed, Queue_declared, Queue_created,
                               Queue_deleted),
        {Connection_created, Connection_closed, Channel_created, Channel_closed,
         Queue_declared, Queue_created, Queue_deleted}).

-define(stats_per_table(Table),
        case Table of
            connection_stats_coarse_conn_stats ->
                [recv_oct, send_oct, reductions];
            vhost_stats_coarse_conn_stats ->
                [recv_oct, send_oct];
            T when T =:= channel_stats_fine_stats;
                   T =:= channel_exchange_stats_fine_stats;
                   T =:= vhost_stats_fine_stats ->
                [publish, confirm, return_unroutable, drop_unroutable];
            T when T =:= channel_queue_stats_deliver_stats;
                   T =:= queue_stats_deliver_stats;
                   T =:= vhost_stats_deliver_stats;
                   T =:= channel_stats_deliver_stats ->
                [get, get_no_ack, deliver, deliver_no_ack, redeliver, ack, deliver_get,
                 get_empty];
            T when T =:= channel_process_stats;
                   T =:= queue_process_stats ->
                [reductions];
            T when T =:= queue_stats_publish;
                   T =:= queue_exchange_stats_publish ->
                [publish];
            exchange_stats_publish_out ->
                [publish_out];
            exchange_stats_publish_in ->
                [publish_in];
            T when T =:= queue_msg_stats;
                   T =:= vhost_msg_stats ->
                [messages_ready, messages_unacknowledged, messages];
            node_coarse_stats ->
                [fd_used, sockets_used, mem_used, disk_free, proc_used, gc_num,
                 gc_bytes_reclaimed, context_switches];
            node_persister_stats ->
                [io_read_count, io_read_bytes, io_read_avg_time, io_write_count,
                 io_write_bytes, io_write_avg_time, io_sync_count, io_sync_avg_time,
                 io_seek_count, io_seek_avg_time, io_reopen_count, mnesia_ram_tx_count,
                 mnesia_disk_tx_count, msg_store_read_count, msg_store_write_count,
                 unused1, queue_index_write_count,
                 queue_index_read_count, unused2,
                 unused3];
            node_node_coarse_stats ->
                [send_bytes, recv_bytes];
            T when T =:= queue_msg_rates;
                   T =:= vhost_msg_rates ->
                [disk_reads, disk_writes];
            T when T =:= connection_churn_rates ->
                [connection_created, connection_closed, channel_created, channel_closed, queue_declared, queue_created, queue_deleted]
        end).
%%------------------------------------------------------------------------------
