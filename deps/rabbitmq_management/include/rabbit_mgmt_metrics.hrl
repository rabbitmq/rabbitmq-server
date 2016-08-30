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

-define(TABLES, [connection_stats_coarse_conn_stats,
		 vhost_stats_coarse_conn_stats,
		 connection_created_stats,
		 connection_stats,
		 channel_created_stats,
		 channel_stats,
		 channel_stats_fine_stats,
		 channel_exchange_stats_fine_stats,
		 channel_queue_stats_deliver_stats,
		 vhost_stats_fine_stats,
		 queue_stats_deliver_stats,
		 vhost_stats_deliver_stats,
		 channel_stats_deliver_stats,
		 channel_process_stats,
		 queue_stats_publish,
		 queue_exchange_stats_publish,
		 exchange_stats_publish_out,
		 exchange_stats_publish_in,
		 consumer_stats,
		 queue_stats,
		 queue_msg_stats,
		 vhost_msg_stats,
		 queue_process_stats,
		 node_stats,
		 node_coarse_stats,
		 node_persister_stats,
		 node_node_stats,
		 node_node_coarse_stats,
		 queue_msg_rates,
		 vhost_msg_rates,
		 old_aggr_stats
		]).

-define(GC_EVENTS, [connection_closed, channel_closed, consumer_deleted,
		    exchange_deleted, queue_deleted, vhost_deleted,
		    node_node_deleted]).
