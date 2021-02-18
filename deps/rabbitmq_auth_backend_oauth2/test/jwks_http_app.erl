-module(jwks_http_app).
-behavior(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    {ok, Port} = application:get_env(jwks_http, port),
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
    jwks_http_sup:start_link().

stop(_State) ->
    ok.
