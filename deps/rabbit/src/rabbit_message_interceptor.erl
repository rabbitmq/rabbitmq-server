-module(rabbit_message_interceptor).

-export([
    intercept/3,
    set_msg_annotation/4
]).

-callback intercept(
    Msg :: mc:state(),
    MsgInterceptorCtx :: map(),
    Config :: #{atom() := term()}
) -> mc:state().

-spec intercept(Msg, MsgInterceptorCtx, MsgDirection) -> Resp when
    Msg :: mc:state(),
    MsgInterceptorCtx :: map(),
    MsgDirection:: incoming | outgoing,
    Resp :: mc:state().
intercept(Msg, MsgInterceptorCtx, MsgDirection) ->
    InterceptorsList = list_to_atom(atom_to_list(MsgDirection)
                                    ++ "_message_interceptors"),
    Interceptors = persistent_term:get(InterceptorsList, []),
    lists:foldl(fun({Module, Config}, Msg0) ->
        try
            Module:intercept(Msg0, MsgInterceptorCtx, Config)
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
