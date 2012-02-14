-module(rabbit_ws_app).

-behaviour(application).
-export([start/2, stop/1]).

%%----------------------------------------------------------------------------

-spec start(_, _) -> {ok, pid()}.
start(_Type, _StartArgs) ->
    ok = rabbit_ws_sockjs:init(),
    rabbit_ws_sup:start_link().

-spec stop(_) -> ok.
stop(_State) ->
    ok.
