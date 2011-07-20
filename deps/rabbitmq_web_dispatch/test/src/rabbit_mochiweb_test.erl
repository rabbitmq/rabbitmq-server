-module(rabbit_mochiweb_test).

-include_lib("eunit/include/eunit.hrl").

query_static_resource_test() ->
    %% TODO this is a fairly rubbish test, but not as bad as it was
    rabbit_mochiweb:register_static_context(test, "rabbit_mochiweb_test",
                                            ?MODULE, "priv/www", "Test"),
    {ok, {_Status, _Headers, Body}} =
        http:request("http://localhost:55670/rabbit_mochiweb_test/index.html"),
    ?assert(string:str(Body, "RabbitMQ HTTP Server Test Page") /= 0).

add_idempotence_test() ->
    F = fun(_Req) -> ok end,
    L = {"/foo", "Foo"},
    rabbit_mochiweb_registry:add(foo, F, F, L),
    rabbit_mochiweb_registry:add(foo, F, F, L),
    ?assertEqual(
       1, length([ok || {"/foo", _, _} <-
                            rabbit_mochiweb_registry:list_all()])),
    passed.
