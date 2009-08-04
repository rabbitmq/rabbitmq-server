-module(rabbitmq_http_test_app).
-behaviour(application).

-export([start/2,stop/1]).

start(normal, []) ->
    rabbitmq_http_server:register_static_context("rabbitmq_http_test", ?MODULE, "priv/www"),
    {ok, spawn(fun() -> receive _ -> ok end end)}.

stop(_State) ->
    ok.


