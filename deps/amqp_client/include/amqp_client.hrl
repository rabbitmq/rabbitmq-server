-record(connection_state, {username,
                           password,
                           serverhost,
                           vhostpath,
                           reader_pid,
                           writer_pid,
                           direct,
                           channel_max,
                           heartbeat,
                           channels = dict:new() }).

-record(channel_state, {number,
                        parent_connection,
                        reader_pid,
                        writer_pid,
                        pending_rpc,
                        pending_consumer,
                        closing = false,
                        consumers = dict:new()} ).

-record(rpc_client_state, {channel_pid,
                           ticket,
                           exchange,
                           routing_key,
                           queue,
                           consumer_tag,
                           continuations = dict:new(),
                           correlation_id = 0}).
