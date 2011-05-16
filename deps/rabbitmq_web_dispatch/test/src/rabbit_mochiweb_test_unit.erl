%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Mochiweb Embedding.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2011 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mochiweb_test_unit).

-include_lib("eunit/include/eunit.hrl").

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
    [?_test(?assertMatch(ok,
                         rabbit_mochiweb_app:check_contexts(
                           Listeners, Contexts))) ||
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
             {error, Err} =
                 rabbit_mochiweb_app:check_contexts(Listeners, Contexts),
             ?assertMatch({undefined_listeners, _}, Err)
     end ||
        {Contexts, Listeners} <-
            [{[{ctxt1, lstr}], [{'*', []}]},
             {[{ctxt1, lstr}, {ctxt2, lstr}], [{'*', []}, {notlstr, []}]},
             {[{ctxt1, lstr1}, {ctxt2, lstr2}], [{'*', []}, {lstr2, []}]},
             {[{ctxt1, lstr1}, {ctxt2, lstr2}], [{'*', []}]}]].
