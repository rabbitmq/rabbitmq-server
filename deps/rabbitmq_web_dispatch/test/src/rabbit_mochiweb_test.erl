-module(rabbit_mochiweb_test).

-include_lib("eunit/include/eunit.hrl").

query_static_resource_test() ->
  %% TODO this is a fairly rubbish test
  {ok, _Result} =
        http:request("http://localhost:55672/rabbit_mochiweb_test/index.html"),
  ok.
