-module(rabbit_exchange_behaviour).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     %% Called *outside* mnesia transactions.
     {description,0},
     {route,3},

     %% Called *inside* mnesia transactions, must be idempotent.
     {recover,1}, %% like init, but called on server startup for durable exchanges
     {init,1}, %% like recover, but called on declaration when previously absent
     {delete,1}, %% called on deletion
     {add_binding, 2},
     {delete_binding, 2}
    ];
behaviour_info(_Other) ->
    undefined.
