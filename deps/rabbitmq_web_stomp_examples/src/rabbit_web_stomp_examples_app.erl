-module(rabbit_web_stomp_examples_app).

-behaviour(application).
-export([start/2,stop/1]).

start(_Type, _StartArgs) ->
    {ok, _} = rabbit_mochiweb:register_static_context(
                web_stomp_examples, "web-stomp-examples", ?MODULE,
                "priv", "WEB-STOMP: examples"),
    {ok, spawn(fun loop/0)}.

stop(_State) ->
    rabbit_mochiweb:unregister_context(web_stomp_examples),
    ok.

loop() ->
    receive
        _ -> loop()
    end.
