
-ifdef(debug).
-define(DEBUG0(F), io:format(F, [])).
-define(DEBUG(F, A), io:format(F, A)).
-else.
-define(DEBUG0(F), ok).
-define(DEBUG(F, A), ok).
-endif.

%% General consts

-define(FRAME_1_0_MIN_SIZE, 4096).

-define(SEND_ROLE, false).
-define(RECV_ROLE, true).

%% Encoding categories

-define(DESCRIBED, 0:8).
-define(DESCRIBED_BIN, <<?DESCRIBED>>).
-define(FIXED_0, 4).
-define(FIXED_1, 5).
-define(FIXED_2, 6).
-define(FIXED_4, 7).
-define(FIXED_8, 8).
-define(FIXED_16, 9).
-define(VAR_1, 10).
-define(VAR_4, 11).
-define(COMPOUND_1, 12).
-define(COMPOUND_4, 13).
-define(ARRAY_1, 14).
-define(ARRAY_4, 15).

-include_lib("rabbit_amqp1_0_framing.hrl").
