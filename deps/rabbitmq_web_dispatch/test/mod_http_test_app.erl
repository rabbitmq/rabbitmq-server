-module(mod_http_test_app).

-export([start/0,stop/1]).

start() ->
    mod_http:register_static_context("mod_http_test", ?MODULE, "priv/www"),
    ok.

stop(_State) ->
    ok.
