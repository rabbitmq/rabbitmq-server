%% same values as in rabbit_reader
-define(NORMAL_TIMEOUT, 3_000).
-define(CLOSING_TIMEOUT, 30_000).
-define(SILENT_CLOSE_DELAY, 3_000).

%% Allow for potentially large sets of tokens during the SASL exchange.
%% https://docs.oasis-open.org/amqp/amqp-cbs/v1.0/csd01/amqp-cbs-v1.0-csd01.html#_Toc67999915
-define(INITIAL_MAX_FRAME_SIZE, 8192).

-type protocol() :: amqp | sasl.
-type channel_number() :: non_neg_integer().
-type callback() :: handshake |
                    {frame_header, protocol()} |
                    {frame_body, protocol(), DataOffset :: pos_integer(), channel_number()}.

-record(v1_connection,
        {name :: binary(),
         container_id = none :: none | binary(),
         vhost = none :: none | rabbit_types:vhost(),
         %% server host
         host :: inet:ip_address() | inet:hostname(),
         %% client host
         peer_host :: inet:ip_address() | inet:hostname(),
         %% server port
         port :: inet:port_number(),
         %% client port
         peer_port :: inet:port_number(),
         connected_at :: integer(),
         user = unauthenticated :: unauthenticated | rabbit_types:user(),
         timeout = ?NORMAL_TIMEOUT :: non_neg_integer(),
         incoming_max_frame_size = ?INITIAL_MAX_FRAME_SIZE :: pos_integer(),
         outgoing_max_frame_size = ?INITIAL_MAX_FRAME_SIZE :: unlimited | pos_integer(),
         %% "Prior to any explicit negotiation, [...] the maximum channel number is 0." [2.4.1]
         channel_max = 0 :: non_neg_integer(),
         auth_mechanism = sasl_init_unprocessed :: sasl_init_unprocessed | {binary(), module()},
         auth_state = unauthenticated :: term(),
         credential_timer :: undefined | reference(),
         properties :: undefined | {map, list(tuple())}
        }).

-record(v1,
        {parent :: pid(),
         helper_sup :: pid(),
         writer = none :: none | pid(),
         heartbeater = none :: none | rabbit_heartbeat:heartbeaters(),
         session_sup = none :: none | pid(),
         websocket :: boolean(),
         sock :: none | rabbit_net:socket(),
         proxy_socket :: undefined | {rabbit_proxy_socket, any(), any()},
         connection :: none | #v1_connection{},
         connection_state :: waiting_amqp3100 | received_amqp3100 | waiting_sasl_init |
                             securing | waiting_amqp0100 | waiting_open | running |
                             closing | closed,
         callback :: callback(),
         recv_len = 8 :: non_neg_integer(),
         pending_recv :: boolean(),
         buf :: list(),
         buf_len :: non_neg_integer(),
         tracked_channels = maps:new() :: #{channel_number() => Session :: pid()},
         stats_timer :: rabbit_event:state()
        }).

-type state() :: #v1{}.
