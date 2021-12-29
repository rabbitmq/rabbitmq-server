%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_dispatch_unit_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
                                    relativise_test,
                                    unrelativise_test
                                   ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

relativise_test(_Config) ->
    Rel = fun rabbit_web_dispatch_util:relativise/2,
    ?assertEqual("baz",        Rel("/foo/bar/bash", "/foo/bar/baz")),
    ?assertEqual("../bax/baz", Rel("/foo/bar/bash", "/foo/bax/baz")),
    ?assertEqual("../bax/baz", Rel("/bar/bash",     "/bax/baz")),
    ?assertEqual("..",         Rel("/foo/bar/bash", "/foo/bar")),
    ?assertEqual("../..",      Rel("/foo/bar/bash", "/foo")),
    ?assertEqual("bar/baz",    Rel("/foo/bar",      "/foo/bar/baz")),
    ?assertEqual("foo",        Rel("/",             "/foo")),

    passed.

unrelativise_test(_Config) ->
    Un = fun rabbit_web_dispatch_util:unrelativise/2,
    ?assertEqual("/foo/bar", Un("/foo/foo", "bar")),
    ?assertEqual("/foo/bar", Un("/foo/foo", "./bar")),
    ?assertEqual("bar",      Un("foo", "bar")),
    ?assertEqual("/baz/bar", Un("/foo/foo", "../baz/bar")),

    passed.
