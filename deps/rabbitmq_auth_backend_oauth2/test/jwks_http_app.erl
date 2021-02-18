-module(jwks_http_app).

-export([start/1, stop/0]).

start(Port) ->
    Dispatch =
        cowboy_router:compile(
          [
           {'_', [
                  {"/jwks", jwks_http_handler, []}
                 ]}
          ]
         ),
    {ok, _} = cowboy:start_clear(jwks_http_listener,
                      [{port, Port}],
                      #{env => #{dispatch => Dispatch}}),
    ok.

stop() ->
    ok = cowboy:stop_listener(jwks_http_listener).
