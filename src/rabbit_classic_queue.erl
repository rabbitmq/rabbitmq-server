-module(rabbit_classic_queue).
-behaviour(rabbit_queue_type).

-include("amqqueue.hrl").
-export([
         is_enabled/0,
         declare/2
         ]).

is_enabled() -> true.

declare(Q, Node) when ?amqqueue_is_classic(Q) ->
    QName = amqqueue:get_name(Q),
    VHost = amqqueue:get_vhost(Q),
    Node1 = case rabbit_queue_master_location_misc:get_location(Q)  of
              {ok, Node0}  -> Node0;
              {error, _}   -> Node
            end,
    Node1 = rabbit_mirror_queue_misc:initial_queue_node(Q, Node1),
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost, Node1) of
        {ok, _} ->
            gen_server2:call(
              rabbit_amqqueue_sup_sup:start_queue_process(Node1, Q, declare),
              {init, new}, infinity);
        {error, Error} ->
            rabbit_misc:protocol_error(internal_error,
                            "Cannot declare a queue '~s' on node '~s': ~255p",
                            [rabbit_misc:rs(QName), Node1, Error])
    end.
