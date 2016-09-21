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
                 {old_aggr_stats, set}]).

-define(INDEX_TABLES, [consumer_stats_queue_index,
                       consumer_stats_channel_index,
                       old_aggr_stats_queue_index,
                       old_aggr_stats_channel_index,
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
-define(channel_stats_fine_stats(Publish, Confirm, Return_unroutable),
	{Publish, Confirm, Return_unroutable}).
-define(channel_exchange_stats_fine_stats(Publish, Confirm, Return_unroutable),
	{Publish, Confirm, Return_unroutable}).
-define(channel_queue_stats_deliver_stats(Get, Get_no_ack, Deliver, Deliver_no_ack,
					  Redeliver, Ack, Deliver_get),
	{Get, Get_no_ack, Deliver, Deliver_no_ack, Redeliver, Ack, Deliver_get}).
-define(vhost_stats_fine_stats(Publish, Confirm, Return_unroutable),
	{Publish, Confirm, Return_unroutable}).
-define(queue_stats_deliver_stats(Get, Get_no_ack, Deliver, Deliver_no_ack,
				  Redeliver, Ack, Deliver_get),
	{Get, Get_no_ack, Deliver, Deliver_no_ack, Redeliver, Ack, Deliver_get}).
-define(vhost_stats_deliver_stats(Get, Get_no_ack, Deliver, Deliver_no_ack,
				  Redeliver, Ack, Deliver_get),
	{Get, Get_no_ack, Deliver, Deliver_no_ack, Redeliver, Ack, Deliver_get}).
-define(channel_stats_deliver_stats(Get, Get_no_ack, Deliver, Deliver_no_ack,
				    Redeliver, Ack, Deliver_get),
	{Get, Get_no_ack, Deliver, Deliver_no_ack, Redeliver, Ack, Deliver_get}).
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
-define(node_persister_stats(Io_read_count, Io_read_bytes, Io_read_time, Io_write_count,
			     Io_write_bytes, Io_write_time, Io_sync_count, Io_sync_time,
			     Io_seek_count, Io_seek_time, Io_reopen_count, Mnesia_ram_tx_count,
			     Mnesia_disk_tx_count, Msg_store_read_count, Msg_store_write_count,
			     Queue_index_journal_write_count, Queue_index_write_count,
			     Queue_index_read_count, Io_file_handle_open_attempt_count,
			     Io_file_handle_open_attempt_time),
	{Io_read_count, Io_read_bytes, Io_read_time, Io_write_count, Io_write_bytes,
	 Io_write_time, Io_sync_count, Io_sync_time, Io_seek_count, Io_seek_time,
	 Io_reopen_count, Mnesia_ram_tx_count, Mnesia_disk_tx_count, Msg_store_read_count,
	 Msg_store_write_count, Queue_index_journal_write_count, Queue_index_write_count,
	 Queue_index_read_count, Io_file_handle_open_attempt_count,
	 Io_file_handle_open_attempt_time}).
-define(node_node_stats(Send_bytes, Recv_bytes), {Send_bytes, Recv_bytes}).
-define(node_node_coarse_stats(Send_bytes, Recv_bytes), {Send_bytes, Recv_bytes}).
-define(queue_msg_rates(Disk_reads, Disk_writes), {Disk_reads, Disk_writes}).
-define(vhost_msg_rates(Disk_reads, Disk_writes), {Disk_reads, Disk_writes}).
-define(old_aggr_stats(Id, Stats), {Id, Stats}).
%%------------------------------------------------------------------------------
