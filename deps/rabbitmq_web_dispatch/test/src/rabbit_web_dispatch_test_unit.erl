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

-module(rabbit_web_dispatch_test_unit).

-include_lib("eunit/include/eunit.hrl").

relativise_test() ->
    Rel = fun rabbit_web_dispatch_util:relativise/2,
    ?assertEqual("baz",        Rel("/foo/bar/bash", "/foo/bar/baz")),
    ?assertEqual("../bax/baz", Rel("/foo/bar/bash", "/foo/bax/baz")),
    ?assertEqual("../bax/baz", Rel("/bar/bash",     "/bax/baz")),
    ?assertEqual("..",         Rel("/foo/bar/bash", "/foo/bar")),
    ?assertEqual("../..",      Rel("/foo/bar/bash", "/foo")),
    ?assertEqual("bar/baz",    Rel("/foo/bar",      "/foo/bar/baz")),
    ?assertEqual("foo",        Rel("/",             "/foo")).

unrelativise_test() ->
    Un = fun rabbit_web_dispatch_util:unrelativise/2,
    ?assertEqual("/foo/bar", Un("/foo/foo", "bar")),
    ?assertEqual("/foo/bar", Un("/foo/foo", "./bar")),
    ?assertEqual("bar",      Un("foo", "bar")),
    ?assertEqual("/baz/bar", Un("/foo/foo", "../baz/bar")).
