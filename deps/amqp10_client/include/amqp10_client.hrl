-define(AMQP_PROTOCOL_HEADER, <<"AMQP", 0, 1, 0, 0>>).
-define(SASL_PROTOCOL_HEADER, <<"AMQP", 3, 1, 0, 0>>).
-define(MIN_MAX_FRAME_SIZE, 512).
-define(MAX_MAX_FRAME_SIZE, 4294967295).
-define(FRAME_HEADER_SIZE, 8).


-define(debug, true).
-ifdef(debug).
-define(DBG(F, A), error_logger:info_msg(F, A)).
-else.
-define(DBG(F, A), ok).
-endif.


