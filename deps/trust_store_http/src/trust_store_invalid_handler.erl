-module(trust_store_invalid_handler).
-behaviour(cowboy_handler).

-export([init/2]).
-export([terminate/3]).

init(Req, State) ->
    %% serves some invalid JSON
    Req2 = cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, <<"{_1}}1}">>, Req),
    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.
