-module(rabbit_sharding_exchange_decorator).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding exchange decorator"},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator, <<"sharding">>, ?MODULE]}},
                    {cleanup, {rabbit_registry, unregister,
                               [exchange_decorator, <<"sharding">>]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(rabbit_exchange_decorator).

-export([description/0, serialise_events/1]).
-export([create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, route/2, active_for/1]).

%%----------------------------------------------------------------------------

description() ->
    [{description, <<"Shard exchange decorator">>}].

serialise_events(_X) -> false.

create(transaction, _X) ->
    ok;
create(none, X) ->
    maybe_start_sharding(X),
    ok.

add_binding(_Tx, _X, _B) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.

route(_, _) -> [].

active_for(X) ->
    case shard(X) of
        true  -> noroute;
        false -> none
    end.

%% we have to remove the policy from ?SHARDING_TABLE
delete(transaction, _X, _Bs) -> ok;
delete(none, X, _Bs) ->
    maybe_stop_sharding(X),
    ok.

%% we have to remove the old policy from ?SHARDING_TABLE
%% and then add the new one.
policy_changed(OldX, NewX) ->
    maybe_update_sharding(OldX, NewX),
    ok.

%%----------------------------------------------------------------------------

maybe_update_sharding(OldX, NewX) ->
    case shard(NewX) of
        true  ->
            rabbit_sharding_util:rpc_call(maybe_update_shards, [OldX, NewX]);
        false ->
            rabbit_sharding_util:rpc_call(maybe_stop_sharding, [OldX])
    end.

maybe_start_sharding(X)->
    case shard(X) of
        true  ->
            rabbit_sharding_util:rpc_call(ensure_sharded_queues, [X]);
        false ->
            ok
    end.

maybe_stop_sharding(X) ->
    case shard(X) of
        true  ->
            rabbit_sharding_util:rpc_call(maybe_stop_sharding, [X]);
        false ->
            ok
    end.

shard(X) ->
    case sharding_up() of
        true  -> rabbit_sharding_util:shard(X);
        false -> false
    end.

sharding_up() -> is_pid(whereis(rabbit_sharding_app)).
