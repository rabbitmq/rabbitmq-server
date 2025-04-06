-module(rabbit_mqtt_client_id_interceptor).

-behaviour(rabbit_message_interceptor).

-define(ANN_CLIENT_ID, <<"client_id">>).

-export([
    intercept/3
]).

intercept(Msg, #{client_id := ClientId}, _Config) ->
    rabbit_message_interceptor:set_msg_annotation(
        Msg,
        ?ANN_CLIENT_ID,
        ClientId,
        true);
intercept(Msg, _MsgInterceptorCtx, _Config) ->
    Msg.
