-module(trust_store_http).


-export([main/1]).

main([]) ->
    io:format("~nStarting trust store server ~n", []),
    {ok, _} = application:ensure_all_started(trust_store_http),
    io:format("~nTrust store server started on port ~tp ~n",
              [application:get_env(trust_store_http, port, undefined)]),
    user_drv:start(),
    timer:sleep(infinity).
