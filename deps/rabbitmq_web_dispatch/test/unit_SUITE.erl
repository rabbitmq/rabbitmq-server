%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
                                    relativise_test,
                                    unrelativise_test,
                                    resolve_log_dir_test,
                                    extract_rotation_spec_test,
                                    access_log_fmt_test
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

resolve_log_dir_test(_Config) ->
    %% Neither setting configured: returns none.
    application:unset_env(rabbitmq_web_dispatch, access_log_dir),
    application:unset_env(rabbitmq_management, http_log_dir),
    ?assertEqual(none, rabbit_web_dispatch_access_log:resolve_log_dir()),

    %% Only management fallback configured.
    application:set_env(rabbitmq_management, http_log_dir, "/tmp/mgmt"),
    ?assertEqual("/tmp/mgmt", rabbit_web_dispatch_access_log:resolve_log_dir()),

    %% Web dispatch takes precedence over management.
    application:set_env(rabbitmq_web_dispatch, access_log_dir, "/tmp/dispatch"),
    ?assertEqual("/tmp/dispatch", rabbit_web_dispatch_access_log:resolve_log_dir()),

    %% Clean up.
    application:unset_env(rabbitmq_web_dispatch, access_log_dir),
    application:unset_env(rabbitmq_management, http_log_dir),
    ok.

extract_rotation_spec_test(_Config) ->
    ?assertEqual(#{}, rabbit_web_dispatch_access_log:extract_rotation_spec([])),

    ?assertEqual(#{max_no_bytes => 10485760, max_no_files => 5},
                 rabbit_web_dispatch_access_log:extract_rotation_spec(
                   [{max_no_bytes, 10485760},
                    {max_no_files, 5},
                    {level, info},
                    {file, "/var/log/rabbitmq/rabbit.log"}])),

    ?assertEqual(#{rotate_on_date => "$D0", compress_on_rotate => true},
                 rabbit_web_dispatch_access_log:extract_rotation_spec(
                   [{rotate_on_date, "$D0"},
                    {compress_on_rotate, true}])),
    ok.

access_log_fmt_test(_Config) ->
    Fmt = fun rabbit_access_log_fmt:format/2,
    Cfg = #{},

    %% Pre-formatted report (the primary code path).
    ?assertEqual(["hello", $\n],
                 Fmt(#{msg => {report, #{formatted => "hello"}}}, Cfg)),

    %% String message.
    ?assertEqual(["hi", $\n],
                 Fmt(#{msg => {string, "hi"}}, Cfg)),

    %% Format + args message.
    ?assertEqual("count: 42\n",
                 lists:flatten(Fmt(#{msg => {"count: ~B", [42]}}, Cfg))),

    %% Report without 'formatted' key is dropped.
    ?assertEqual([], Fmt(#{msg => {report, #{other => data}}}, Cfg)),

    %% Unknown message shape is dropped.
    ?assertEqual([], Fmt(#{msg => something_unexpected}, Cfg)),
    ok.
