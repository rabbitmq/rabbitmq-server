-module(rabbitmq_aws_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    rabbitmq_aws_sup:start_link().

stop(_State) ->
    ok.
