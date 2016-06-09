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
%%   Copyright (c) 2010-2016 Pivotal Software, Inc.  All rights reserved.
%%

-record(state, {
          lookups,
          interval,
          event_refresh_ref,
          rates_mode,
          max_backlog}).

-define(FINE_STATS_TYPES, [channel_queue_stats, channel_exchange_stats,
                           channel_queue_exchange_stats]).

-define(TABLES, [queue_stats, connection_stats, channel_stats,
                 consumers_by_queue, consumers_by_channel,
                 node_stats, node_node_stats,
                 %% What the previous info item was for any given
                 %% {queue/channel/connection}
                 old_stats]).
