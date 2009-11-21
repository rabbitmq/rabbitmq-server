-module(rabbit_exchange_type_direct).
-include("rabbit.hrl").

-behaviour(rabbit_exchange_behaviour).

-export([description/0, publish/2]).
-export([recover/1, init/1, delete/1, add_binding/2, delete_binding/2]).

description() ->
    [{name, <<"direct">>},
     {description, <<"AMQP direct exchange, as per the AMQP specification">>}].

publish(#exchange{name = Name},
        Delivery = #delivery{message = #basic_message{routing_key = RoutingKey}}) ->
    rabbit_router:deliver(rabbit_router:match_routing_key(Name, RoutingKey), Delivery).

recover(_X) -> ok.
init(_X) -> ok.
delete(_X) -> ok.
add_binding(_X, _B) -> ok.
delete_binding(_X, _B) -> ok.
