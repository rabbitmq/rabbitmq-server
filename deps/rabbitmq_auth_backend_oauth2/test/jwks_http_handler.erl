-module(jwks_http_handler).
-behavior(cowboy_handler).

-export([init/2, terminate/3]).

init(Req, State) ->
    {ok, Keys} = application:get_env(jwks_http, keys),
    Body = jsx:encode(#{keys => Keys}),
    Headers = #{<<"content-type">> => <<"application/json">>},
    Req2 = cowboy_req:reply(200, Headers, Body, Req),
    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.
