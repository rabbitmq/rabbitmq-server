%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% These tables contain the raw metrics as stored by RabbitMQ core
-define(CORE_TABLES, [{connection_created, set},
                      {connection_metrics, set},
                      {connection_coarse_metrics, set},
                      {channel_created, set},
                      {channel_metrics, set},
                      {channel_queue_exchange_metrics, set},
                      {channel_queue_metrics, set},
                      {channel_exchange_metrics, set},
                      {channel_process_metrics, set},
                      {consumer_created, set},
                      {queue_metrics, set},
                      {queue_coarse_metrics, set},
                      {node_persister_metrics, set},
                      {node_coarse_metrics, set},
                      {node_metrics, set},
                      {node_node_metrics, set},
                      {connection_churn_metrics, set}]).

-define(CORE_EXTRA_TABLES, [{gen_server2_metrics, set},
                            {auth_attempt_metrics, set},
                            {auth_attempt_detailed_metrics, set}]).

-define(CONNECTION_CHURN_METRICS, {node(), 0, 0, 0, 0, 0, 0, 0}).

%% connection_created :: {connection_id, proplist}
%% connection_metrics :: {connection_id, proplist}
%% connection_coarse_metrics :: {connection_id, recv_oct, send_oct, reductions}
%% channel_created :: {channel_id, proplist}
%% channel_metrics :: {channel_id, proplist}
%% channel_queue_exchange_metrics :: {{channel_id, {queue_id, exchange_id}}, publish}
%% channel_queue_metrics :: {{channel_id, queue_id}, proplist}
%% channel_exchange_metrics :: {{channel_id, exchange_id}, proplist}
%% channel_process_metrics :: {channel_id, reductions}
%% consumer_created :: {{queue_id, channel_id, consumer_tag}, exclusive_consume,
%%                      ack_required, prefetch_count, args}
%% queue_metrics :: {queue_id, proplist}
%% queue_coarse_metrics :: {queue_id, messages_ready, messages_unacknowledge,
%%                          messages, reductions}
%% node_persister_metrics :: {node_id, proplist}
%% node_coarse_metrics :: {node_id, proplist}
%% node_metrics :: {node_id, proplist}
%% node_node_metrics :: {{node_id, node_id}, proplist}
%% gen_server2_metrics :: {pid, buffer_length}
%% connection_churn_metrics :: {node(), connection_created, connection_closed, channel_created, channel_closed, queue_declared, queue_created, queue_deleted}
