-module(rabbit_message_interceptor).

-export([intercept/3,
         set_msg_annotation/4]).

-type protocol() :: amqp091 | amqp10 | mqtt310 | mqtt311 | mqtt50.

-type msg_interceptor_ctx() :: #{protocol := protocol(),
                                 vhost := binary(),
                                 username := binary(),
                                 conn_name => binary(),
                                 atom() => term()}.

-callback intercept(Msg, MsgInterceptorCtx, Group, Config) -> Msg when
    Msg :: mc:state(),
    MsgInterceptorCtx :: msg_interceptor_ctx(),
    Group :: incoming_message_interceptors | outgoing_message_interceptors,
    Config :: #{atom() := term()}.

-spec intercept(Msg, MsgInterceptorCtx, Group) -> Msg when
    Msg :: mc:state(),
    MsgInterceptorCtx :: map(),
    Group :: incoming_message_interceptors | outgoing_message_interceptors.
intercept(Msg, MsgInterceptorCtx, Group) ->
    Interceptors = persistent_term:get(Group, []),
    lists:foldl(fun({Module, Config}, Msg0) ->
                        try
                            Module:intercept(Msg0,
                                             MsgInterceptorCtx,
                                             Group,
                                             Config)
                        catch
                            error:undef ->
                                Msg0
                        end
                end, Msg , Interceptors).

-spec set_msg_annotation(mc:state(),
                         mc:ann_key(),
                         mc:ann_value(),
                         boolean()
                        ) -> mc:state().
set_msg_annotation(Msg, Key, Value, Overwrite) ->
    case {mc:x_header(Key, Msg), Overwrite} of
        {Val, false} when Val =/= undefined ->
            Msg;
        _ ->
            mc:set_annotation(Key, Value, Msg)
    end.
