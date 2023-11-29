-module(jwks_http_app).

-export([start/3, stop/0]).

start(Port, CertsDir, Mounts) ->
    ct:log("Mounts: ~p", [Mounts]),
    Dispatch =
        cowboy_router:compile(
          [
           {'_', [
                  {Mount, jwks_http_handler, [{keys, Keys}]} || {Mount,Keys} <- Mounts
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
