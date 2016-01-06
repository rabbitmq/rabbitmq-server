-record(state, {
          lookups,
          interval,
          event_refresh_ref,
          rates_mode}).

-define(FINE_STATS_TYPES, [channel_queue_stats, channel_exchange_stats,
                           channel_queue_exchange_stats]).

-define(TABLES, [queue_stats, connection_stats, channel_stats,
                 consumers_by_queue, consumers_by_channel,
                 node_stats, node_node_stats,
                 %% What the previous info item was for any given
                 %% {queue/channel/connection}
                 old_stats]).
