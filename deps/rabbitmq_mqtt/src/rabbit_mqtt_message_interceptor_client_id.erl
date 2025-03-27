-module(rabbit_mqtt_message_interceptor_client_id).

-behaviour(rabbit_message_interceptor).

-export([intercept/4]).

intercept(Msg,
          #{client_id := ClientId},
          incoming_message_interceptors,
          #{annotation_key := AnnotationKey}
         ) ->
    rabbit_message_interceptor:set_msg_annotation(Msg,
                                                  AnnotationKey,
                                                  ClientId,
                                                  true);
intercept(Msg, _MsgInterceptorCtx, _Group, _Config) ->
    Msg.
