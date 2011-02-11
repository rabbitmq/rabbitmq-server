-module(rabbit_mochiweb_test).

-include_lib("eunit/include/eunit.hrl").

query_static_resource_test() ->
  %% TODO this is a fairly rubbish test
  {ok, _Result} =
        http:request("http://localhost:55672/rabbit_mochiweb_test/index.html"),
  ok.

relativise_test() ->
    ?assertEqual("baz",
                 rabbit_mochiweb_util:relativise("/foo/bar/bash",
                                                 "/foo/bar/baz")),
    ?assertEqual("../bax/baz",
                 rabbit_mochiweb_util:relativise("/foo/bar/bash",
                                                 "/foo/bax/baz")),
    ?assertEqual("../bax/baz",
                 rabbit_mochiweb_util:relativise("/bar/bash",
                                                 "/bax/baz")),
    ?assertEqual("..",
                 rabbit_mochiweb_util:relativise("/foo/bar/bash",
                                                 "/foo/bar")),
    ?assertEqual("../..",
                 rabbit_mochiweb_util:relativise("/foo/bar/bash",
                                                 "/foo")),
    ?assertEqual("bar/baz",
                 rabbit_mochiweb_util:relativise("/foo/bar",
                                                 "/foo/bar/baz")),
    ?assertEqual("foo", rabbit_mochiweb_util:relativise("/", "/foo")).
