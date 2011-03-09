-module(rabbit_mochiweb_test_app).
-behaviour(application).

-export([start/2,stop/1]).

start(normal, []) ->
    rabbit_mochiweb:register_static_context(
      ctxt,
      "rabbit_mochiweb_test", ?MODULE, "priv/www", "Test"),
    {ok, spawn(fun() -> receive _ -> ok end end)}.

stop(_State) ->
    ok.


