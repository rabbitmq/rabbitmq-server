-module(rabbit_mochiweb_test).

-include_lib("eunit/include/eunit.hrl").

query_static_resource_test() ->
  %% TODO this is a fairly rubbish test
  {ok, _Result} =
        http:request("http://localhost:55670/rabbit_mochiweb_test/index.html"),
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

check_contexts_ok_test_() ->
    [?_test(?_assertEqual(ok,
                          rabbit_mochiweb_app:check_contexts(
                            Contexts, Listeners))) ||
        {Contexts, Listeners} <-
            [{[],             [{'*', []}]},
             {[{ctxt, lstr}], [{'*', []}, {lstr, []}]},
             {[{ctxt, '*'}],  [{'*', []}]},

             {[{ctxt1, lstr}, {ctxt2, lstr}],
              [{'*', []}, {lstr, []}]},

             {[{ctxt1, '*'}, {ctxt2, '*'}],
              [{'*', []}]}]].

check_contexts_errors_test_() ->
    [fun() ->
             {error, {_, Errs}} =
                 rabbit_mochiweb_app:check_contexts(Contexts, Listeners),
             ?_assertEqual(Expected, Errs)
     end ||
        {Contexts, Listeners, Expected} <-
            [{[{ctxt1, lstr}], [{'*', []}],
              [lstr]},
             {[{ctxt1, lstr}, {ctxt2, lstr}], [{'*', []}, {notlstr, []}],
              [lstr]},
             {[{ctxt1, lstr1}, {ctxt2, lstr2}], [{'*', []}, {lstr2, []}],
              [lstr1]},
             {[{ctxt1, lstr1}, {ctxt2, lstr2}], [{'*', []}],
              [lstr1, lstr2]}]].
