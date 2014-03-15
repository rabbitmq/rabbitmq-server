-module(rabbit_sharding_shard).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([maybe_shard_exchanges/0,
         ensure_sharded_queues/1,
         maybe_update_shards/2,
         maybe_stop_sharding/1]).

-rabbit_boot_step({rabbit_sharding_maybe_shard,
                   [{description, "rabbit sharding maybe shard"},
                    {mfa,         {?MODULE, maybe_shard_exchanges, []}},
                    {requires,    direct_client},
                    {enables,     networking}]}).

-import(rabbit_sharding_util, [a2b/1, exchange_bin/1,
                               routing_key/1, shards_per_node/1]).
-import(rabbit_sharding_amqp_util, [disposable_connection_calls/3]).

-import(rabbit_misc, [r/3]).

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
    maybe_add_queues(shards_per_node(OldX), shards_per_node(NewX), NewX),
    bind_queues(NewX).

maybe_stop_sharding(OldX) ->
    unbind_queues(shards_per_node(OldX), OldX).

%% routing key didn't change. Do nothing.
maybe_unbind_queues(RK, RK, _OldX) ->
    ok;
maybe_unbind_queues(_RK, _NewRK, OldX) ->
    unbind_queues(shards_per_node(OldX), OldX).

%% shards per node didn't change. Do nothing.
maybe_add_queues(SPN, SPN, _NewX) ->
    ok;
maybe_add_queues(_OldSPN, _NewSPN, NewX) ->
    add_queues(NewX).

unbind_queues(undefined, _X) ->
    ok;
unbind_queues(OldSPN, #exchange{name = XName} = X) ->
    XBin = exchange_bin(XName),
    OldRKey = routing_key(X),
    F = fun (N) ->
                QBin = rabbit_sharding_util:make_queue_name(
                         XBin, a2b(node()), N),
                [#'queue.unbind'{exchange    = XBin,
                                 queue       = QBin,
                                 routing_key = OldRKey}]
        end,
    ErrFun = fun(Code, Text) -> {error, Code, Text} end,
    disposable_connection_calls(X, lists:flatten(do_n(F, OldSPN)), ErrFun).

add_queues(#exchange{name = XName} = X) ->
    SPN = shards_per_node(X),
    RKey = routing_key(X),
    XBin = exchange_bin(XName),
    F = fun (N) ->
                QBin = rabbit_sharding_util:make_queue_name(
                         XBin, a2b(node()), N),
                [#'queue.declare'{queue = QBin, durable = true}]
        end,
    ErrFun = fun(Code, Text) -> {error, Code, Text} end,
    disposable_connection_calls(X, lists:flatten(do_n(F, SPN)), ErrFun).

bind_queues(#exchange{name = XName} = X) ->
    RKey = routing_key(X),
    SPN = shards_per_node(X),
    XBin = exchange_bin(XName),
    F = fun (N) ->
                QBin = rabbit_sharding_util:make_queue_name(
                         XBin, a2b(node()), N),
                #'queue.bind'{exchange = XBin,
                              queue = QBin,
                              routing_key = RKey}
        end,
    ErrFun = fun(Code, Text) -> {error, Code, Text} end,
    disposable_connection_calls(X, lists:flatten(do_n(F, SPN)), ErrFun).

%%----------------------------------------------------------------------------

do_n(F, N) ->
    do_n(F, 0, N, []).

do_n(_F, N, N, Acc) ->
    Acc;
do_n(F, Count, N, Acc) ->
    do_n(F, Count+1, N, [F(Count)|Acc]).
