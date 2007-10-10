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
                        consumers = dict:new(),
                        next_delivery_tag = 0,
                        tx} ).

-record(rpc_client_state, {broker_config,
                           consumer_tag,
                           continuations = dict:new(),
                           correlation_id = 0}).

-record(rpc_handler_state, {broker_config,
                            server_name}).

-record(broker_config, {channel_pid,
                       ticket,
                       exchange,
                       routing_key,
                       content_type,
                       queue}).
