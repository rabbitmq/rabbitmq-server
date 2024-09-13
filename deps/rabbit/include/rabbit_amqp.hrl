%% To enable AMQP trace logging, uncomment below line:
%-define(TRACE_AMQP, true).
-ifdef(TRACE_AMQP).
-warning("AMQP tracing is enabled").
-define(TRACE(Format, Args),
        rabbit_log:debug(
          "~s:~s/~b ~b~n" ++ Format ++ "~n",
          [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, ?LINE] ++ Args)).
-else.
-define(TRACE(_Format, _Args), ok).
-endif.

%% General consts

%% [2.8.19]
-define(MIN_MAX_FRAME_1_0_SIZE, 512).

%% for rabbit_event user_authentication_success and user_authentication_failure
-define(AUTH_EVENT_KEYS,
        [name,
         host,
         port,
         peer_host,
         peer_port,
         protocol,
         auth_mechanism,
         ssl,
         ssl_protocol,
         ssl_key_exchange,
         ssl_cipher,
         ssl_hash,
         peer_cert_issuer,
         peer_cert_subject,
         peer_cert_validity]).

-define(ITEMS,
        [pid,
         frame_max,
         timeout,
         container_id,
         vhost,
         user,
         node
        ] ++ ?AUTH_EVENT_KEYS).

-define(INFO_ITEMS,
        [connection_state,
         recv_oct,
         recv_cnt,
         send_oct,
         send_cnt
        ] ++ ?ITEMS).

%% for rabbit_event connection_created
-define(CONNECTION_EVENT_KEYS,
        [type,
         client_properties,
         connected_at,
         channel_max
        ] ++ ?ITEMS).

-include_lib("amqp10_common/include/amqp10_framing.hrl").
