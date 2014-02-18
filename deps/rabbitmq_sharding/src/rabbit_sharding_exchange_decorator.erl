-module(rabbit_sharding_exchange_decorator).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding exchange decorator"},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator, <<"sharding">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-rabbit_boot_step({rabbit_sharding_exchange_decorator_mnesia,
                   [{description, "rabbit sharding exchange decorator: mnesia"},
                    {mfa, {?MODULE, setup_schema, []}},
                    {requires, database},
                    {enables, external_infrastructure}]}).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_sharding.hrl").

-behaviour(rabbit_exchange_decorator).

-export([description/0, serialise_events/1]).
-export([create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, route/2, active_for/1]).
-export([setup_schema/0]).

%%----------------------------------------------------------------------------

setup_schema() ->
    case mnesia:create_table(?SHARDING_TABLE,
                             [{attributes, record_info(fields, sharding)},
                              {record_name, sharding},
                              {type, set}]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, ?SHARDING_TABLE}} -> ok
    end.

description() ->
    [{description, <<"Shard exchange decorator">>}].

serialise_events(_X) -> false.

create(transaction, _X) ->
    ok;
create(none, X) ->
    maybe_start(X).

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
    maybe_stop(X).

%% we have to remove the old policy from ?SHARDING_TABLE
%% and then add the new one.
policy_changed(OldX, NewX) ->
    maybe_stop(OldX),
    maybe_start(NewX).

%%----------------------------------------------------------------------------

maybe_start(#exchange{name = XName} = X)->
    case shard(X) of
        true  ->
            SPN = rabbit_sharding_util:shards_per_node(X),
            RK  = rabbit_sharding_util:routing_key(X),
            Res = rabbit_misc:execute_mnesia_transaction(
                    fun () ->
                            mnesia:write(?SHARDING_TABLE,
                                         #sharding{name            = XName,
                                                   shards_per_node = SPN,
                                                   routing_key     = RK},
                                         write)
                    end),
            rabbit_sharding_util:rpc_call(ensure_sharded_queues, [X]),
            ok;
        false -> ok
    end.

maybe_stop(#exchange{name = XName} = X) ->
    case shard(X) of
        true  ->
            rabbit_misc:execute_mnesia_transaction(
              fun () ->
                      mnesia:delete({?SHARDING_TABLE, XName})
              end),
            ok;
        false -> ok
    end.

shard(X) ->
    case sharding_up() of
        true -> rabbit_sharding_util:shard(X);
        false -> false
    end.

sharding_up() -> is_pid(whereis(rabbit_sharding_app)).
