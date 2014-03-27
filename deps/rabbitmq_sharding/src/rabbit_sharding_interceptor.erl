-module(rabbit_sharding_interceptor).

-include_lib("rabbit_common/include/rabbit_framing.hrl").

-behaviour(rabbit_channel_interceptor).

-export([description/0, intercept/2, applies_to/1]).

%% exported for tests
-export([consumer_count/1]).

-import(rabbit_sharding_util, [a2b/1, shards_per_node/1]).
-import(rabbit_misc, [r/3, format/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding interceptor"},
                    {mfa, {rabbit_registry, register,
                           [channel_interceptor,
                            <<"sharding interceptor">>, ?MODULE]}},
                    {cleanup, {rabbit_registry, unregister,
                               [channel_interceptor,
                                <<"sharding interceptor">>]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

description() ->
    [{description, <<"Sharding interceptor for channel methods">>}].

intercept(#'basic.consume'{queue = QName} = Method, VHost) ->
    case queue_name(VHost, QName) of
        {ok, QName2} ->
            {ok, Method#'basic.consume'{queue = QName2}};
        {error, QName} ->
            {error, format("Error finding sharded queue for: ~p", [QName])}
    end;

intercept(#'basic.get'{queue = QName} = Method, VHost) ->
    case queue_name(VHost, QName) of
        {ok, QName2} ->
            {ok, Method#'basic.get'{queue = QName2}};
        {error, QName} ->
            {error, format("Error finding sharded queue for: ~p", [QName])}
    end;

intercept(#'queue.delete'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, format("Can't delete sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end;

intercept(#'queue.declare'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, format("Can't declare sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end;

intercept(#'queue.bind'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, format("Can't bind sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end;

intercept(#'queue.unbind'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, format("Can't unbind sharded queue: ~p", [QName])};
        _    ->
            {ok, Method}
    end;

intercept(#'queue.purge'{queue = QName} = Method, VHost) ->
    case is_sharded(VHost, QName) of
        true ->
            {error, format("Can't purge sharded queue: ~p", [QName])};
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

%% If the queue is not part of a shard, return unmodified name
queue_name(VHost, QBin) ->
    case lookup_exchange(VHost, QBin) of
        {ok, X}  ->
            case rabbit_sharding_util:shard(X) of
                true ->
                    least_consumers(VHost, QBin, shards_per_node(X));
                _    ->
                    {ok, QBin}
            end;
        _Error ->
            {ok, QBin}
    end.

is_sharded(VHost, QBin) ->
    case lookup_exchange(VHost, QBin) of
        {ok, X} ->
            rabbit_sharding_util:shard(X);
        _Error ->
            false
    end.

lookup_exchange(VHost, QBin) ->
    rabbit_exchange:lookup(r(VHost, exchange, QBin)).

least_consumers(VHost, QBin, N) ->
    F = fun(QNum) ->
                QBin2 = rabbit_sharding_util:make_queue_name(
                          QBin, a2b(node()), QNum),
                case consumer_count(r(VHost, queue, QBin2)) of
                    {error, E}       -> {error, E};
                    [{consumers, C}] -> {C, QBin2}
                end

        end,
    case queues_with_count(F, N) of
        []     ->
            {error, QBin};
        Queues ->
            [{_, QBin3} | _ ] = lists:sort(Queues),
            {ok, QBin3}
    end.

queues_with_count(F, N) ->
    lists:foldl(fun (C, Acc) ->
                        case F(C) of
                            {error, _} -> Acc;
                            Ret        -> [Ret|Acc]
                        end
                end, [], lists:seq(0, N-1)).

consumer_count(QName) ->
    rabbit_amqqueue:with(
      QName,
      fun(Q) ->
              rabbit_amqqueue:info(Q, [consumers])
      end).
