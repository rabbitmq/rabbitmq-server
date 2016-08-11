%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

%% These tables contain the raw metrics as stored by RabbitMQ core
-define(CORE_TABLES, [connection_created,
                      connection_metrics,
                      connection_coarse_metrics,
		      channel_created,
		      channel_metrics,
		      channel_queue_exchange_metrics,
		      channel_queue_metrics,
		      channel_exchange_metrics,
		      channel_process_metrics,
		      consumer_created]).

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
