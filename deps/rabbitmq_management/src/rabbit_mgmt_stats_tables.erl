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

-module(rabbit_mgmt_stats_tables).

-include("rabbit_mgmt_metrics.hrl").

-export([aggr_table/2, aggr_tables/1, type_from_table/1,
         index/1, key_index/1]).

-spec aggr_table(event_type(), type()) -> table_name().
aggr_table(queue_stats, deliver_get) ->
    aggr_queue_stats_deliver_get;
aggr_table(queue_stats, fine_stats) ->
    aggr_queue_stats_fine_stats;
aggr_table(queue_stats, queue_msg_counts) ->
    aggr_queue_stats_queue_msg_counts;
aggr_table(queue_stats, queue_msg_rates) ->
    aggr_queue_stats_queue_msg_rates;
aggr_table(queue_stats, process_stats) ->
    aggr_queue_stats_process_stats;
aggr_table(queue_exchange_stats, fine_stats) ->
    aggr_queue_exchange_stats_fine_stats;
aggr_table(vhost_stats, deliver_get) ->
    aggr_vhost_stats_deliver_get;
aggr_table(vhost_stats, fine_stats) ->
    aggr_vhost_stats_fine_stats;
aggr_table(vhost_stats, queue_msg_rates) ->
    aggr_vhost_stats_queue_msg_rates;
aggr_table(vhost_stats, queue_msg_counts) ->
    aggr_vhost_stats_queue_msg_counts;
aggr_table(vhost_stats, coarse_conn_stats) ->
    aggr_vhost_stats_coarse_conn_stats;
aggr_table(channel_queue_stats, deliver_get) ->
    aggr_channel_queue_stats_deliver_get;
aggr_table(channel_queue_stats, fine_stats) ->
    aggr_channel_queue_stats_fine_stats;
aggr_table(channel_queue_stats, queue_msg_counts) ->
    aggr_channel_queue_stats_queue_msg_counts;
aggr_table(channel_stats, deliver_get) ->
    aggr_channel_stats_deliver_get;
aggr_table(channel_stats, fine_stats) ->
    aggr_channel_stats_fine_stats;
aggr_table(channel_stats, queue_msg_counts) ->
    aggr_channel_stats_queue_msg_counts;
aggr_table(channel_stats, process_stats) ->
    aggr_channel_stats_process_stats;
aggr_table(channel_exchange_stats, deliver_get) ->
    aggr_channel_exchange_stats_deliver_get;
aggr_table(channel_exchange_stats, fine_stats) ->
    aggr_channel_exchange_stats_fine_stats;
aggr_table(exchange_stats, fine_stats) ->
    aggr_exchange_stats_fine_stats;
aggr_table(node_stats, coarse_node_stats) ->
    aggr_node_stats_coarse_node_stats;
aggr_table(node_node_stats, coarse_node_node_stats) ->
    aggr_node_node_stats_coarse_node_node_stats;
aggr_table(connection_stats, coarse_conn_stats) ->
    aggr_connection_stats_coarse_conn_stats;
aggr_table(connection_stats, process_stats) ->
    aggr_connection_stats_process_stats.

-spec aggr_tables(event_type()) -> [{table_name(), type()}].
aggr_tables(queue_stats) ->
    [{aggr_queue_stats_fine_stats, fine_stats},
     {aggr_queue_stats_deliver_get, deliver_get},
     {aggr_queue_stats_queue_msg_counts, queue_msg_counts},
     {aggr_queue_stats_queue_msg_rates, queue_msg_rates},
     {aggr_queue_stats_process_stats, process_stats}];
aggr_tables(queue_exchange_stats) ->
    [{aggr_queue_exchange_stats_fine_stats, fine_stats}];
aggr_tables(vhost_stats) ->
    [{aggr_vhost_stats_deliver_get, deliver_get},
     {aggr_vhost_stats_fine_stats, fine_stats},
     {aggr_vhost_stats_queue_msg_rates, queue_msg_rates},
     {aggr_vhost_stats_queue_msg_counts, queue_msg_counts},
     {aggr_vhost_stats_coarse_conn_stats, coarse_conn_stats}];
aggr_tables(channel_queue_stats) ->
    [{aggr_channel_queue_stats_deliver_get, deliver_get},
     {aggr_channel_queue_stats_fine_stats, fine_stats},
     {aggr_channel_queue_stats_queue_msg_counts, queue_msg_counts}];
aggr_tables(channel_stats) ->
    [{aggr_channel_stats_deliver_get, deliver_get},
     {aggr_channel_stats_fine_stats, fine_stats},
     {aggr_channel_stats_queue_msg_counts, queue_msg_counts},
     {aggr_channel_stats_process_stats, process_stats}];
aggr_tables(channel_exchange_stats) ->
    [{aggr_channel_exchange_stats_deliver_get, deliver_get},
     {aggr_channel_exchange_stats_fine_stats, fine_stats}];
aggr_tables(exchange_stats) ->
    [{aggr_exchange_stats_fine_stats, fine_stats}];
aggr_tables(node_stats) ->
    [{aggr_node_stats_coarse_node_stats, coarse_node_stats}];
aggr_tables(node_node_stats) ->
    [{aggr_node_node_stats_coarse_node_node_stats, coarse_node_node_stats}];
aggr_tables(connection_stats) ->
    [{aggr_connection_stats_coarse_conn_stats, coarse_conn_stats},
     {aggr_connection_stats_process_stats, process_stats}].

-spec type_from_table(table_name()) -> type().
type_from_table(aggr_queue_stats_deliver_get) ->
    deliver_get;
type_from_table(aggr_queue_stats_fine_stats) ->
    fine_stats;
type_from_table(aggr_queue_stats_queue_msg_counts) ->
    queue_msg_counts;
type_from_table(aggr_queue_stats_queue_msg_rates) ->
    queue_msg_rates;
type_from_table(aggr_queue_stats_process_stats) ->
    process_stats;
type_from_table(aggr_queue_exchange_stats_fine_stats) ->
    fine_stats;
type_from_table(aggr_vhost_stats_deliver_get) ->
    deliver_get;
type_from_table(aggr_vhost_stats_fine_stats) ->
    fine_stats;
type_from_table(aggr_vhost_stats_queue_msg_rates) ->
    queue_msg_rates;
type_from_table(aggr_vhost_stats_queue_msg_counts) ->
    queue_msg_counts;
type_from_table(aggr_vhost_stats_coarse_conn_stats) ->
    coarse_conn_stats;
type_from_table(aggr_channel_queue_stats_deliver_get) ->
    deliver_get;
type_from_table(aggr_channel_queue_stats_fine_stats) ->
    fine_stats;
type_from_table(aggr_channel_queue_stats_queue_msg_counts) ->
    queue_msg_counts;
type_from_table(aggr_channel_stats_deliver_get) ->
    deliver_get;
type_from_table(aggr_channel_stats_fine_stats) ->
    fine_stats;
type_from_table(aggr_channel_stats_queue_msg_counts) ->
    queue_msg_counts;
type_from_table(aggr_channel_stats_process_stats) ->
    process_stats;
type_from_table(aggr_channel_exchange_stats_deliver_get) ->
    deliver_get;
type_from_table(aggr_channel_exchange_stats_fine_stats) ->
    fine_stats;
type_from_table(aggr_exchange_stats_fine_stats) ->
    fine_stats;
type_from_table(aggr_node_stats_coarse_node_stats) ->
    coarse_node_stats;
type_from_table(aggr_node_node_stats_coarse_node_node_stats) ->
    coarse_node_node_stats;
type_from_table(aggr_node_node_stats_coarse_conn_stats) ->
    coarse_conn_stats;
type_from_table(aggr_connection_stats_coarse_conn_stats) ->
    coarse_conn_stats;
type_from_table(aggr_connection_stats_process_stats) ->
    process_stats.

index(aggr_queue_stats_deliver_get) ->
    aggr_queue_stats_deliver_get_index;
index(aggr_queue_stats_fine_stats) ->
    aggr_queue_stats_fine_stats_index;
index(aggr_queue_stats_queue_msg_counts) ->
    aggr_queue_stats_queue_msg_counts_index;
index(aggr_queue_stats_queue_msg_rates) ->
    aggr_queue_stats_queue_msg_rates_index;
index(aggr_queue_stats_process_stats) ->
    aggr_queue_stats_process_stats_index;
index(aggr_queue_exchange_stats_fine_stats) ->
    aggr_queue_exchange_stats_fine_stats_index;
index(aggr_vhost_stats_deliver_get) ->
    aggr_vhost_stats_deliver_get_index;
index(aggr_vhost_stats_fine_stats) ->
    aggr_vhost_stats_fine_stats_index;
index(aggr_vhost_stats_queue_msg_rates) ->
    aggr_vhost_stats_queue_msg_rates_index;
index(aggr_vhost_stats_queue_msg_counts) ->
    aggr_vhost_stats_queue_msg_counts_index;
index(aggr_vhost_stats_coarse_conn_stats) ->
    aggr_vhost_stats_coarse_conn_stats_index;
index(aggr_channel_queue_stats_deliver_get) ->
    aggr_channel_queue_stats_deliver_get_index;
index(aggr_channel_queue_stats_fine_stats) ->
    aggr_channel_queue_stats_fine_stats_index;
index(aggr_channel_queue_stats_queue_msg_counts) ->
    aggr_channel_queue_stats_queue_msg_counts_index;
index(aggr_channel_stats_deliver_get) ->
    aggr_channel_stats_deliver_get_index;
index(aggr_channel_stats_fine_stats) ->
    aggr_channel_stats_fine_stats_index;
index(aggr_channel_stats_queue_msg_counts) ->
    aggr_channel_stats_queue_msg_counts_index;
index(aggr_channel_stats_process_stats) ->
    aggr_channel_stats_process_stats_index;
index(aggr_channel_exchange_stats_deliver_get) ->
    aggr_channel_exchange_stats_deliver_get_index;
index(aggr_channel_exchange_stats_fine_stats) ->
    aggr_channel_exchange_stats_fine_stats_index;
index(aggr_exchange_stats_fine_stats) ->
    aggr_exchange_stats_fine_stats_index;
index(aggr_node_stats_coarse_node_stats) ->
    aggr_node_stats_coarse_node_stats_index;
index(aggr_node_node_stats_coarse_node_node_stats) ->
    aggr_node_node_stats_coarse_node_node_stats_index;
index(aggr_connection_stats_coarse_conn_stats) ->
    aggr_connection_stats_coarse_conn_stats_index;
index(aggr_connection_stats_process_stats) ->
    aggr_connection_stats_process_stats_index.

key_index(connection_stats) ->
    connection_stats_key_index;
key_index(channel_stats) ->
    channel_stats_key_index;
key_index(aggr_queue_stats_deliver_get) ->
    aggr_queue_stats_deliver_get_key_index;
key_index(aggr_queue_stats_fine_stats) ->
    aggr_queue_stats_fine_stats_key_index;
key_index(aggr_queue_stats_queue_msg_counts) ->
    aggr_queue_stats_queue_msg_counts_key_index;
key_index(aggr_queue_stats_queue_msg_rates) ->
    aggr_queue_stats_queue_msg_rates_key_index;
key_index(aggr_queue_stats_process_stats) ->
    aggr_queue_stats_process_stats_key_index;
key_index(aggr_queue_exchange_stats_fine_stats) ->
    aggr_queue_exchange_stats_fine_stats_key_index;
key_index(aggr_vhost_stats_deliver_get) ->
    aggr_vhost_stats_deliver_get_key_index;
key_index(aggr_vhost_stats_fine_stats) ->
    aggr_vhost_stats_fine_stats_key_index;
key_index(aggr_vhost_stats_queue_msg_rates) ->
    aggr_vhost_stats_queue_msg_rates_key_index;
key_index(aggr_vhost_stats_queue_msg_counts) ->
    aggr_vhost_stats_queue_msg_counts_key_index;
key_index(aggr_vhost_stats_coarse_conn_stats) ->
    aggr_vhost_stats_coarse_conn_stats_key_index;
key_index(aggr_channel_queue_stats_deliver_get) ->
    aggr_channel_queue_stats_deliver_get_key_index;
key_index(aggr_channel_queue_stats_fine_stats) ->
    aggr_channel_queue_stats_fine_stats_key_index;
key_index(aggr_channel_queue_stats_queue_msg_counts) ->
    aggr_channel_queue_stats_queue_msg_counts_key_index;
key_index(aggr_channel_stats_deliver_get) ->
    aggr_channel_stats_deliver_get_key_index;
key_index(aggr_channel_stats_fine_stats) ->
    aggr_channel_stats_fine_stats_key_index;
key_index(aggr_channel_stats_queue_msg_counts) ->
    aggr_channel_stats_queue_msg_counts_key_index;
key_index(aggr_channel_stats_process_stats) ->
    aggr_channel_stats_process_stats_key_index;
key_index(aggr_channel_exchange_stats_deliver_get) ->
    aggr_channel_exchange_stats_deliver_get_key_index;
key_index(aggr_channel_exchange_stats_fine_stats) ->
    aggr_channel_exchange_stats_fine_stats_key_index;
key_index(aggr_exchange_stats_fine_stats) ->
    aggr_exchange_stats_fine_stats_key_index;
key_index(aggr_node_stats_coarse_node_stats) ->
    aggr_node_stats_coarse_node_stats_key_index;
key_index(aggr_node_node_stats_coarse_node_node_stats) ->
    aggr_node_node_stats_coarse_node_node_stats_key_index;
key_index(aggr_connection_stats_coarse_conn_stats) ->
    aggr_connection_stats_coarse_conn_stats_key_index;
key_index(aggr_connection_stats_process_stats) ->
    aggr_connection_stats_process_stats_key_index.
