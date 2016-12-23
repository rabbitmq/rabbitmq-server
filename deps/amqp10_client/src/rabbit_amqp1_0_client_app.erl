-module(rabbit_amqp1_0_client_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    rabbit_amqp1_0_client_sup:start_link().

stop(_State) ->
    ok.
