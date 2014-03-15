-module(rabbit_sharding_util).

-export([shard/1, sharded_exchanges/1]).
-export([get_policy/2, shards_per_node/1, routing_key/1]).
-export([exchange_bin/1, make_queue_name/3]).
-export([a2b/1, rpc_call/2]).

-include_lib("amqp_client/include/amqp_client.hrl").

-import(rabbit_misc, [pget/3]).

%% only shard CH or random exchanges.
shard(X = #exchange{type = 'x-consistent-hash'}) -> shard0(X);
shard(X = #exchange{type = 'x-random'})          -> shard0(X);
shard(_X)                                        -> false.

shard0(X) ->
    case get_policy(<<"sharded">>, X) of
        true -> true;
        _    -> false
    end.

sharded_exchanges(VHost) ->
    [X || X <- find_exchanges(VHost), shard(X)].

shards_per_node(X) ->
    get_policy(<<"shards-per-node">>, X).

routing_key(X) ->
    get_policy(<<"routing-key">>, X).

get_policy(Key, X) ->
    rabbit_policy:get(Key, X).

exchange_bin(#resource{name = XBin}) -> XBin.

make_queue_name(QBin, NodeBin, QNum) ->
    %% we do this to prevent unprintable characters in queue names
    QNumBin = list_to_binary(lists:flatten(io_lib:format("~p", [QNum]))),
    <<"sharding: ", QBin/binary, " - ", NodeBin/binary, " - ", QNumBin/binary>>.

rpc_call(F, Args) ->
    [rpc:call(Node, rabbit_sharding_shard, F, Args) ||
        Node <- rabbit_mnesia:cluster_nodes(running)].

a2b(A) -> list_to_binary(atom_to_list(A)).

%%----------------------------------------------------------------------------

find_exchanges(VHost) ->
    rabbit_exchange:list(VHost).

vhost(#exchange{name = #resource{virtual_host = VHost}}) -> VHost;
vhost(                 #resource{virtual_host = VHost})  -> VHost.
