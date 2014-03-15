-module(rabbit_sharding_interceptor).

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(rabbit_channel_interceptor).

-export([description/0, intercept/2, applies_to/1]).

%% exported for tests
-export([consumer_count/1]).

-import(rabbit_sharding_util, [a2b/1, shards_per_node/1]).
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
    case lookup_exchange(VHost, QBin) of
        {ok, X}  ->
            {ok, least_consumers(VHost, QBin, shards_per_node(X))};
        _Error ->
            %% Queue is not part of a shard, return unmodified name
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
    rabbit_exchange:lookup(rabbit_misc:r(VHost, exchange, QBin)).

%% returns the original queue name if it fails to find a proper queue.
least_consumers(VHost, QBin, N) ->
    F = fun(QNum) ->
                QBin2 = rabbit_sharding_util:make_queue_name(
                          QBin, a2b(node()), QNum),
                case consumer_count(rabbit_misc:r(VHost, queue, QBin2)) of
                    {error, E}       -> {error, E};
                    [{consumers, C}] -> {C, QBin2}
                end

        end,
    case do_n(F, N) of
        []     ->
            QBin;
        Queues ->
            [{_, QBin3} | _ ] = lists:sort(Queues),
            QBin3
    end.

consumer_count(QName) ->
    rabbit_amqqueue:with(
      QName,
      fun(Q) ->
              rabbit_amqqueue:info(Q, [consumers])
      end).

do_n(F, N) ->
    do_n(F, 0, N, []).

do_n(_F, N, N, Acc) ->
    Acc;
do_n(F, Count, N, Acc) ->
    Acc2 = case F(Count) of
               {error, _} -> Acc;
               Ret        -> [Ret|Acc]
           end,
    do_n(F, Count+1, N, Acc2).
