%%-define(debug, true).

-ifdef(debug).
-define(DEBUG0(F), ?SAFE(rabbit_log:debug(F, []))).
-define(DEBUG(F, A), ?SAFE(rabbit_log:debug(F, A))).
-else.
-define(DEBUG0(F), ok).
-define(DEBUG(F, A), ok).
-endif.

-define(pprint(F), rabbit_log:debug("~p~n",
                                    [amqp10_framing:pprint(F)])).

-define(SAFE(F),
        ((fun() ->
                  try F
                  catch __T:__E:__ST ->
                            rabbit_log:debug("~p:~p thrown debugging~n~p~n",
                                             [__T, __E, __ST])
                  end
          end)())).

%% General consts

%% [2.8.19]
-define(MIN_MAX_FRAME_1_0_SIZE, 512).

-define(SEND_ROLE, false).
-define(RECV_ROLE, true).

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
         connected_at
        ] ++ ?ITEMS).

-include_lib("amqp10_common/include/amqp10_framing.hrl").
