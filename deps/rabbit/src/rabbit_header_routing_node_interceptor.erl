-module(rabbit_header_routing_node_interceptor).
-behaviour(rabbit_incoming_message_interceptor).

-define(HEADER_ROUTING_NODE, <<"x-routed-by">>).

-export([
    intercept/4
]).

intercept(Msg, _ProtoMod, _ProtoState, Config) ->
    Node = atom_to_binary(node()),
    Overwrite = maps:get(overwrite, Config, false),
    rabbit_incoming_message_interceptor:set_msg_annotation(
        Msg,
        ?HEADER_ROUTING_NODE,
        Node,
        Overwrite).
