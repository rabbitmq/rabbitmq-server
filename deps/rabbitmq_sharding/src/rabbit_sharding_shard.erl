-module(rabbit_sharding_shard).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([maybe_shard_exchanges/0,
         ensure_sharded_queues/1,
         maybe_update_shards/2,
         maybe_stop_sharding/1]).

-import(rabbit_misc, [r/3]).
-import(rabbit_sharding_util, [a2b/1, exchange_bin/1, make_queue_name/3,
                               routing_key/1, shards_per_node/1]).

-rabbit_boot_step({rabbit_sharding_maybe_shard,
                   [{description, "rabbit sharding maybe shard"},
                    {mfa,         {?MODULE, maybe_shard_exchanges, []}},
                    {requires,    direct_client},
                    {enables,     networking}]}).

-define(MAX_CONNECTION_CLOSE_TIMEOUT, 10000).

%% We make sure the sharded queues are created when
%% RabbitMQ starts.
maybe_shard_exchanges() ->
    [maybe_shard_exchanges(V) || V <- rabbit_vhost:list()],
    ok.

maybe_shard_exchanges(VHost) ->
    [rabbit_sharding_util:rpc_call(ensure_sharded_queues, [X]) ||
        X <- rabbit_sharding_util:sharded_exchanges(VHost)].

%% queue needs to be started on the respective node.
%% connection will be local.
%% each rabbit_sharding_shard will receive the event
%% and can declare the queue locally
ensure_sharded_queues(X) ->
    add_queues(X),
    bind_queues(X).

maybe_update_shards(OldX, NewX) ->
    maybe_unbind_queues(routing_key(OldX), routing_key(NewX), OldX),
    add_queues(NewX),
    bind_queues(NewX).

maybe_stop_sharding(OldX) ->
    unbind_queues(shards_per_node(OldX), OldX).

%% routing key didn't change. Do nothing.
maybe_unbind_queues(RK, RK, _OldX) ->
    ok;
maybe_unbind_queues(_RK, _NewRK, OldX) ->
    unbind_queues(shards_per_node(OldX), OldX).

unbind_queues(undefined, _X) ->
    ok;
unbind_queues(OldSPN, #exchange{name = XName} = X) ->
    OldRKey = routing_key(X),
    [unbind_queue(XName, OldRKey, N, node()) || N <- lists:seq(0, OldSPN-1)].

add_queues(#exchange{name = XName} = X) ->
    SPN = shards_per_node(X),
    [declare_queue(XName, N, node()) || N <- lists:seq(0, SPN-1)].

bind_queues(#exchange{name = XName} = X) ->
    RKey = routing_key(X),
    SPN = shards_per_node(X),
    [bind_queue(XName, RKey, N, node()) || N <- lists:seq(0, SPN-1)].

%%----------------------------------------------------------------------------

declare_queue(XName, N, Node) ->
    QBin = make_queue_name(exchange_bin(XName), a2b(Node), N),
    QueueName = rabbit_misc:r(v(XName), queue, QBin),
    try rabbit_amqqueue:declare(QueueName, false, false, [], none) of
        {_Reply, _Q} ->
            ok
    catch
        _Error:Reason ->
            rabbit_log:error("sharding failed to declare queue for exchange ~p"
                             " - soft error:~n~p~n",
                             [exchange_bin(XName), Reason]),
            error
    end.

bind_queue(XName, RoutingKey, N, Node) ->
    binding_action(fun rabbit_binding:add/2,
                   XName, RoutingKey, N, Node,
                   "sharding failed to bind queue ~p to exchange ~p"
                   " - soft error:~n~p~n").

unbind_queue(XName, RoutingKey, N, Node) ->
    binding_action(fun rabbit_binding:remove/2,
                   XName, RoutingKey, N, Node,
                   "sharding failed to unbind queue ~p to exchange ~p"
                   " - soft error:~n~p~n").

binding_action(F, XName, RoutingKey, N, Node, ErrMsg) ->
    QBin = make_queue_name(exchange_bin(XName), a2b(Node), N),
    QueueName = rabbit_misc:r(v(XName), queue, QBin),
    case F(#binding{source      = XName,
                    destination = QueueName,
                    key         = RoutingKey,
                    args        = []},
           fun (_X, _Q) -> ok end) of
        ok              -> ok;
        {error, Reason} ->
            rabbit_log:error(ErrMsg, [QBin, exchange_bin(XName), Reason]),
            error
    end.

v(#resource{virtual_host = VHost}) ->
    VHost.
