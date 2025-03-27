-module(rabbit_message_interceptor_routing_node).
-behaviour(rabbit_message_interceptor).

-define(HEADER_ROUTING_NODE, <<"x-routed-by">>).

-export([intercept/4]).

intercept(Msg, _MsgInterceptorCtx, _Group, Config) ->
    Node = atom_to_binary(node()),
    Overwrite = maps:get(overwrite, Config, false),
    rabbit_message_interceptor:set_msg_annotation(Msg,
                                                  ?HEADER_ROUTING_NODE,
                                                  Node,
                                                  Overwrite).
