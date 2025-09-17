-module(rabbitmq_aws_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    ets:new(aws_credentials, [named_table, public, {read_concurrency, true}]),
    ets:new(aws_config, [named_table, public, {read_concurrency, true}]),
    rabbitmq_aws_sup:start_link().

stop(_State) ->
    ok.
