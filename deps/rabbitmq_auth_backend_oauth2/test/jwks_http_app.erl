-module(jwks_http_app).

-export([start/2, stop/0]).

start(Port, CertsDir) ->
    Dispatch =
        cowboy_router:compile(
          [
           {'_', [
                  {"/jwks", jwks_http_handler, []}
                 ]}
          ]
         ),
    {ok, _} = cowboy:start_tls(jwks_http_listener,
                      [{port, Port},
                       {certfile, filename:join([CertsDir, "server", "cert.pem"])},
                       {keyfile, filename:join([CertsDir, "server", "key.pem"])}],
                      #{env => #{dispatch => Dispatch}}),
    ok.

stop() ->
    ok = cowboy:stop_listener(jwks_http_listener).
