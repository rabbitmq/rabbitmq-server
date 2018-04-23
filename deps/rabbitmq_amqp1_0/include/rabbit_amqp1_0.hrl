%%-define(debug, true).

-ifdef(debug).
-define(DEBUG0(F), ?SAFE(io:format(F, []))).
-define(DEBUG(F, A), ?SAFE(io:format(F, A))).
-else.
-define(DEBUG0(F), ok).
-define(DEBUG(F, A), ok).
-endif.

-define(pprint(F), io:format("~p~n", [amqp10_framing:pprint(F)])).

-define(SAFE(F),
        ((fun() ->
                  try F
                  catch __T:__E ->
                          io:format("~p:~p thrown debugging~n~p~n",
                                    [__T, __E, erlang:get_stacktrace()])
                  end
          end)())).

%% General consts

-define(FRAME_1_0_MIN_SIZE, 512).

-define(SEND_ROLE, false).
-define(RECV_ROLE, true).

%% Encoding

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(INFO_ITEMS, [pid,
                     auth_mechanism,
                     host,
                     frame_max,
                     timeout,
                     user,
                     state,
                     recv_oct,
                     recv_cnt,
                     send_oct,
                     send_cnt,
                     ssl,
                     ssl_protocol,
                     ssl_key_exchange,
                     ssl_cipher,
                     ssl_hash,
                     peer_cert_issuer,
                     peer_cert_subject,
                     peer_cert_validity,
                     node]).
