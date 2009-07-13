-module(mod_http_test).


-behaviour(application).
-export([start/2,stop/1]).

%%-----------------------------------------------

start(_Type, _StartArgs) ->
    io:format("Got this far........~n"),
    mod_http_web:install_static(?MODULE),
    {ok, spawn_link(fun loop/0)}.

stop(_State) ->
    ok.

%%-----------------------------------------------
loop() ->
    receive
        X -> io:format("Got an ~p~n",[X])
    end.
