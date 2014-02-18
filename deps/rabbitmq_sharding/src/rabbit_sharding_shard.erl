-module(rabbit_sharding_shard).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_sharding.hrl").

-export([maybe_shard_exchanges/0, ensure_sharded_queues/1]). 
-export([update_shards/2, update_shard/2, update_named_shard/2]).

-rabbit_boot_step({rabbit_sharding_maybe_shard,
                   [{description, "rabbit sharding maybe shard"},
                    {mfa,         {?MODULE, maybe_shard_exchanges, []}},
                    {requires,    direct_client},
                    {enables,     networking}]}).

-import(rabbit_sharding_util, [a2b/1, exchange_name/1]).
-import(rabbit_misc, [r/3]).

%% We make sure the sharded queues are created when
%% RabbitMQ starts.
maybe_shard_exchanges() ->
    [maybe_shard_exchanges(V) || V <- rabbit_vhost:list()],
    ok.

maybe_shard_exchanges(VHost) ->
    [rabbit_sharding_util:rpc_call(ensure_sharded_queues, [X]) || 
        X <- rabbit_sharding_util:sharded_exchanges(VHost)].

%% A particular sharding-definition has changed, we need to update
%% the shard. See rabbit_sharding_parameters:notify/4 and 
%% rabbit_sharding_parameters:notify_clear/3 for more docs.
update_named_shard(VHost, Name) ->
    [rabbit_sharding_util:rpc_call(update_shard, [X, all]) || 
        X <- rabbit_sharding_util:sharded_exchanges(VHost), 
        rabbit_sharding_util:get_policy(X) =:= Name].

%% Either the shards-per-node or routing-key parameters got
%% updated or cleared
update_shards(VHost, What) ->
    [rabbit_sharding_util:rpc_call(update_shard, [X, What]) || 
        X <- rabbit_sharding_util:sharded_exchanges(VHost)].
%% queue needs to be started on the respective node.
%% connection will be local.
%% each rabbit_sharding_shard will receive the event
%% and can declare the queue locally
ensure_sharded_queues(#exchange{name = XName} = X) ->
    RKey = rabbit_sharding_util:routing_key(X),
    F = fun (N) ->
            QBin = rabbit_sharding_util:make_queue_name(
                       exchange_name(XName), a2b(node()), N),
            [#'queue.declare'{queue = QBin, durable = true},
             #'queue.bind'{exchange = exchange_name(XName), 
                           queue = QBin, 
                           routing_key = RKey}]
        end,
    ErrFun = fun(Code, Text) -> 
                {error, Code, Text}
             end,
    SPN = rabbit_sharding_util:shards_per_node(X),
    
    rabbit_sharding_amqp_util:disposable_connection_calls(X, 
        lists:flatten(do_n(F, SPN)), ErrFun).

%% we either need to update all the shard parametes or just the
%% shards_per_node. When updating all we need to unbind the queues 
%% from the older routing key
update_shard(X, all) ->
    {ok, Old, _New} = internal_update_shard(X),
    unbind_sharded_queues(Old, X),
    ensure_sharded_queues(X);

update_shard(X, shards_per_node) ->
    internal_update_shard(X),
    ensure_sharded_queues(X).

unbind_sharded_queues(Old, #exchange{name = XName} = X) ->
    XBin = exchange_name(XName),
    OldSPN = Old#sharding.shards_per_node,
    OldRKey = Old#sharding.routing_key,
    F = fun (N) ->
            QBin = rabbit_sharding_util:make_queue_name(
                       XBin, a2b(node()), N),
            [#'queue.unbind'{exchange    = XBin, 
                             queue       = QBin, 
                             routing_key = OldRKey}]
        end,
    ErrFun = fun(Code, Text) -> 
                {error, Code, Text}
             end,
    rabbit_sharding_amqp_util:disposable_connection_calls(X, 
        lists:flatten(do_n(F, OldSPN)), ErrFun).

%%----------------------------------------------------------------------------

internal_update_shard(#exchange{name = XName} = X) ->
    case rabbit_sharding_util:get_policy(X) of
        undefined  -> {error, "Policy Not Found"};
        _PolicyName ->
            rabbit_misc:execute_mnesia_transaction(
                fun () ->
                    case mnesia:read(?SHARDING_TABLE, XName, write) of
                        [] ->
                            {error, "Shard not found"};
                        [#sharding{} = Old] -> 
                            SPN = rabbit_sharding_util:shards_per_node(X),
                            RK = rabbit_sharding_util:routing_key(X),
                            New = #sharding{name = XName,
                                            shards_per_node = SPN,
                                            routing_key     = RK},
                            mnesia:write(?SHARDING_TABLE, New, write),
                            {ok, Old, New}
                    end
                end)
    end.

do_n(F, N) ->
    do_n(F, 0, N, []).
    
do_n(_F, N, N, Acc) ->
    Acc;
do_n(F, Count, N, Acc) ->
    do_n(F, Count+1, N, [F(Count)|Acc]).