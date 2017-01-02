-module(amqp10_client_app).

-behaviour(application).

-export([start/2,
         stop/1]).

start(_Type, _Args) ->
    amqp10_client_sup:start_link().

stop(_State) ->
    ok.
