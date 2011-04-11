-module(rabbit_mochiweb_test).

-include_lib("eunit/include/eunit.hrl").

query_static_resource_test() ->
    %% TODO this is a fairly rubbish test, but not as bad as it was
    rabbit_mochiweb:register_static_context("rabbit_mochiweb_test", ?MODULE, "priv/www", "Test"),
    {ok, {_Status, _Headers, Body}} =
        http:request("http://localhost:55672/rabbit_mochiweb_test/index.html"),
    ?assert(string:str(Body, "RabbitMQ HTTP Server Test Page") /= 0).
