%%-define(debug, true).

-ifdef(debug).
-define(DEBUG0(F), ?SAFE(io:format(F, []))).
-define(DEBUG(F, A), ?SAFE(io:format(F, A))).
-else.
-define(DEBUG0(F), ok).
-define(DEBUG(F, A), ok).
-endif.

-define(pprint(F), io:format("~p~n", [rabbit_amqp1_0_framing:pprint(F)])).

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

-define(DESCRIBED, 0:8).
-define(DESCRIBED_BIN, <<?DESCRIBED>>).

-include_lib("rabbit_amqp1_0_framing.hrl").
