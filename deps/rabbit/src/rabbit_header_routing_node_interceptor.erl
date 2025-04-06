-module(rabbit_header_routing_node_interceptor).
-behaviour(rabbit_message_interceptor).

-define(HEADER_ROUTING_NODE, <<"x-routed-by">>).

-export([
    intercept/3
]).

intercept(Msg, _MsgInterceptorCtx, Config) ->
    Node = atom_to_binary(node()),
    Overwrite = maps:get(overwrite, Config, false),
    rabbit_message_interceptor:set_msg_annotation(
        Msg,
        ?HEADER_ROUTING_NODE,
        Node,
        Overwrite).
