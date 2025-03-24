-module(rabbit_runtime_parameter_registry_test).

-behaviour(rabbit_runtime_parameter).

-export([
         validate/5,
         notify/5,
         notify_clear/4
        ]).

validate(_, _, _, _, _) ->
    ok.

notify(_, _, _, _, _) ->
    ok.

notify_clear(_, _, _, _) ->
    ok.
