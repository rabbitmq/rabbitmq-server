-module(rabbit_incoming_message_interceptor).

-export([
    intercept/3,
    set_msg_annotation/4
]).

-callback intercept(
    Msg :: mc:state(),
    ProtoMod :: module(),
    ProtoState :: term(),
    Config :: #{atom() := term()}
) -> mc:state().

-spec intercept(Msg, ProtoMod, ProtoState) -> Resp when
    Msg :: mc:state(),
    ProtoMod :: module(),
    ProtoState :: term(),
    Resp :: mc:state().
intercept(Msg, ProtoMod, ProtoState) ->
    Interceptors = persistent_term:get(incoming_message_interceptors, []),
    lists:foldl(fun({Module, Config}, Msg0) ->
        try
            Module:intercept(Msg0, ProtoMod, ProtoState, Config)
        catch
            error:undef ->
                Msg0
        end
    end, Msg , Interceptors).

-spec set_msg_annotation(mc:state(), mc:ann_key(), mc:ann_value(), boolean()) -> mc:state().
set_msg_annotation(Msg, Key, Value, Overwrite) ->
    case {mc:x_header(Key, Msg), Overwrite} of
        {Val, false} when Val =/= undefined ->
            Msg;
        _ ->
            mc:set_annotation(Key, Value, Msg)
    end.
