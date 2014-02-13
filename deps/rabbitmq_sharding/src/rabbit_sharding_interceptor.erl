-module(rabbit_sharding_interceptor).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_sharding.hrl").

-behaviour(rabbit_channel_interceptor).

-export([description/0, intercept/2, applies_to/1]).

-import(rabbit_sharding_util, [a2b/1]).
-import(rabbit_misc, [r/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding interceptor"},
                    {mfa, {rabbit_registry, register,
                           [channel_interceptor, <<"sharding interceptor">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

description() ->
    [{description, <<"Sharding interceptor for channel methods">>}].

intercept(#'basic.consume'{queue = QName} = Method, VHost) ->
    {ok, QName2} = queue_name(VHost, QName),
    {ok, Method#'basic.consume'{queue = QName2}};

intercept(#'basic.get'{queue = QName} = Method, VHost) ->
    {ok, QName2} = queue_name(VHost, QName),
    {ok, Method#'basic.get'{queue = QName2}};

intercept(#'queue.delete'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, rabbit_misc:format("Can't delete sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end;

intercept(#'queue.declare'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, rabbit_misc:format("Can't declare sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end;

intercept(#'queue.bind'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, rabbit_misc:format("Can't bind sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end;

intercept(#'queue.unbind'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, rabbit_misc:format("Can't unbind sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end;

intercept(#'queue.purge'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, rabbit_misc:format("Can't purge sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end.

applies_to('basic.consume') -> true;
applies_to('basic.get') -> true;
applies_to('queue.delete') -> true;
applies_to('queue.declare') -> true;
applies_to('queue.bind') -> true;
applies_to('queue.unbind') -> true;
applies_to('queue.purge') -> true;
applies_to(_Other) -> false.

%%----------------------------------------------------------------------------

queue_name(VHost, QBin) ->
    case mnesia:dirty_read(?SHARDING_TABLE, r(VHost, exchange, QBin)) of
        []  ->
            %% Queue is not part of a shard, return unmodified name
            {ok, QBin};
        [#sharding{shards_per_node = N}] ->
            Rand = crypto:rand_uniform(0, N),
            {ok, rabbit_sharding_util:make_queue_name(QBin, a2b(node()), Rand)}
    end.

is_sharded(VHost, QBin) ->
    case mnesia:dirty_read(?SHARDING_TABLE, r(VHost, exchange, QBin)) of
        []  ->
            %% Queue is not part of a shard
            false;
        [#sharding{}] ->
            true
    end.