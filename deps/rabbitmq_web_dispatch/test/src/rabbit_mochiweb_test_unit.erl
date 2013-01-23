%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
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
