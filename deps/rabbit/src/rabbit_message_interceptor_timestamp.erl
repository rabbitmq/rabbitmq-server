-module(rabbit_message_interceptor_timestamp).
-behaviour(rabbit_message_interceptor).

-include("mc.hrl").

-define(HEADER_TIMESTAMP, <<"timestamp_in_ms">>).

-export([intercept/4]).

intercept(Msg0, _MsgInterceptorCtx, _Group, Config) ->
    Ts = mc:get_annotation(?ANN_RECEIVED_AT_TIMESTAMP, Msg0),
    Overwrite = maps:get(overwrite, Config, false),
    Msg = rabbit_message_interceptor:set_msg_annotation(
            Msg0,
            ?HEADER_TIMESTAMP,
            Ts,
            Overwrite),
    set_msg_timestamp(Msg, Ts, Overwrite).

set_msg_timestamp(Msg, Timestamp, Overwrite) ->
    case {mc:timestamp(Msg), Overwrite} of
        {Ts, false} when is_integer(Ts) ->
            Msg;
        _ ->
            mc:set_annotation(?ANN_TIMESTAMP, Timestamp, Msg)
    end.
