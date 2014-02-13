-module(rabbit_sharding_util).

-export([shard/1, rpc_call/2, find_exchanges/1, sharded_exchanges/1]).
-export([get_policy/1, exchange_name/1, make_queue_name/3, a2b/1]).
-export([shards_per_node/1, routing_key/1, username/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include ("rabbit_sharding.hrl").

-import(rabbit_misc, [pget/3]).

%% only shard CH or random exchanges.
shard(X = #exchange{type = 'x-random'}) ->
    shard0(X);

shard(X = #exchange{type = 'x-consistent-hash'}) ->
    shard0(X);

shard(_X) ->
    false.

shard0(X) ->
    case get_policy(X) of
        undefined -> false;
        _         -> true
    end.

sharded_exchanges(VHost) ->
    [X || X <- find_exchanges(VHost), shard(X)].

rpc_call(F, Args) ->
    [rpc:call(Node, rabbit_sharding_shard, F, Args) ||
        Node <- rabbit_mnesia:cluster_nodes(running)].

make_queue_name(QBin, NodeBin, QNum) ->
    %% we do this to prevent unprintable characters in queue names
    QNumBin = list_to_binary(lists:flatten(io_lib:format("~p", [QNum]))),
    <<"sharding: ", QBin/binary, " - ", NodeBin/binary, " - ", QNumBin/binary>>.

exchange_name(#resource{name = XBin}) -> XBin.

find_exchanges(VHost) ->
    rabbit_exchange:list(VHost).

a2b(A) -> list_to_binary(atom_to_list(A)).

shards_per_node(X) ->
    get_parameter(<<"shards-per-node">>, X, ?DEFAULT_SHARDS_NUM).

%% Move routing key to sharding-definition
routing_key(X) ->
    get_parameter(<<"routing-key">>, X, ?DEFAULT_RK).

username(X) ->
    {ok, DefaultUser} = application:get_env(rabbit, default_user),
    get_parameter(<<"local-username">>, X, DefaultUser).

vhost(                 #resource{virtual_host = VHost})  -> VHost;
vhost(#exchange{name = #resource{virtual_host = VHost}}) -> VHost.

get_policy(X) ->
    rabbit_policy:get(<<"sharding-definition">>, X).

get_parameter(Parameter, X, Default) ->
    Default2 = rabbit_runtime_parameters:value(
                 vhost(X), <<"sharding">>, Parameter, Default),
    get_parameter_value(<<"sharding-definition">>, Parameter, 
        X, Default2).


get_parameter_value(Comp, Param, X, Default) ->
    case get_policy(X) of
        undefined -> Default;
        Name      ->
            case rabbit_runtime_parameters:value(
                   vhost(X), Comp, Name) of
                not_found -> Default;
                Value     -> pget(Param, Value, Default)
            end
    end.