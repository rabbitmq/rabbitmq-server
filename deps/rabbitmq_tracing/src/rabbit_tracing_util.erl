-module(rabbit_tracing_util).

-export([coerce_env_value/2]).
-export([apply_on_node/5]).

coerce_env_value(username, Val) -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(password, Val) -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(_,        Val) -> Val.

apply_on_node(ReqData, Context, Mod, Fun, Args) ->
    case rabbit_mgmt_util:id(node, ReqData) of
        none ->
            apply(Mod, Fun, Args);
        Node0 ->
            Node = binary_to_atom(Node0, utf8),
            case rpc:call(Node, Mod, Fun, Args) of
                {badrpc, _} = Error ->
                    Msg = io_lib:format("Node ~p could not be contacted: ~p",
                                        [Node, Error]),
                    _ = rabbit_log:warning(Msg, []),
                    rabbit_mgmt_util:bad_request(list_to_binary(Msg), ReqData, Context);
                Any ->
                    Any
            end
    end.
