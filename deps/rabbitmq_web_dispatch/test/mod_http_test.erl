-module(mod_http_test).

-behaviour(application).
-export([start/2,stop/1]).

start(_Type, _StartArgs) -> ok.

stop(_State) ->
    ok.