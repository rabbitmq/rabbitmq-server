%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_semver_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
        {group, comparison},
        {group, parsing},
        {group, formatting},
        {group, prefixed_versions}
    ].

groups() ->
    [
        {comparison, [parallel], [
            eql_test,
            gt_test,
            lt_test,
            gte_test,
            lte_test,
            between_test,
            pes_test,
            edge_cases_test
        ]},
        {parsing, [parallel], [
            prefixed_version_parse_test,
            prefixed_version_normalize_test
        ]},
        {formatting, [parallel], [
            version_format_test
        ]},
        {prefixed_versions, [parallel], [
            prefixed_version_compare_test
        ]}
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.
init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

eql_test(_Config) ->
    ?assert(rabbit_semver:eql("1.0.0-alpha", "1.0.0-alpha")),
    ?assert(rabbit_semver:eql(<<"1.0.0-alpha">>, "1.0.0-alpha")),
    ?assert(rabbit_semver:eql("1.0.0-alpha", <<"1.0.0-alpha">>)),
    ?assert(rabbit_semver:eql(<<"1.0.0-alpha">>, <<"1.0.0-alpha">>)),
    ?assert(rabbit_semver:eql("v1.0.0-alpha", "1.0.0-alpha")),
    ?assert(rabbit_semver:eql("1", "1.0.0")),
    ?assert(rabbit_semver:eql("v1", "v1.0.0")),
    ?assert(rabbit_semver:eql("1.0", "1.0.0")),
    ?assert(rabbit_semver:eql("1.0.0", "1")),
    ?assert(rabbit_semver:eql("1.0.0.0", "1")),
    ?assert(rabbit_semver:eql("1.0+alpha.1", "1.0.0+alpha.1")),
    ?assert(rabbit_semver:eql("1.0-alpha.1+build.1", "1.0.0-alpha.1+build.1")),
    ?assert(rabbit_semver:eql("1.0-alpha.1+build.1", "1.0.0.0-alpha.1+build.1")),
    ?assert(rabbit_semver:eql("1.0-alpha.1+build.1", "v1.0.0.0-alpha.1+build.1")),
    ?assert(rabbit_semver:eql("aa", "aa")),
    ?assert(rabbit_semver:eql("AA.BB", "AA.BB")),
    ?assert(rabbit_semver:eql("BBB-super", "BBB-super")),
    ?assertNot(rabbit_semver:eql("1.0.0", "1.0.1")),
    ?assertNot(rabbit_semver:eql(<<"1.0.0">>, "1.0.1")),
    ?assertNot(rabbit_semver:eql("1.0.0", <<"1.0.1">>)),
    ?assertNot(rabbit_semver:eql(<<"1.0.0">>, <<"1.0.1">>)),
    ?assertNot(rabbit_semver:eql("1.0.0-alpha", "1.0.1+alpha")),
    ?assertNot(rabbit_semver:eql("1.0.0+build.1", "1.0.1+build.2")),
    ?assertNot(rabbit_semver:eql("1.0.0.0+build.1", "1.0.0.1+build.2")),
    ?assertNot(rabbit_semver:eql("FFF", "BBB")),
    ?assertNot(rabbit_semver:eql("1", "1BBBB")).

gt_test(_Config) ->
    ?assert(rabbit_semver:gt("1.0.0-alpha.1", "1.0.0-alpha")),
    ?assert(rabbit_semver:gt("1.0.0.1-alpha.1", "1.0.0.1-alpha")),
    ?assert(rabbit_semver:gt("1.0.0.4-alpha.1", "1.0.0.2-alpha")),
    ?assert(rabbit_semver:gt("1.0.0.0-alpha.1", "1.0.0-alpha")),
    ?assert(rabbit_semver:gt("1.0.0-beta.2", "1.0.0-alpha.1")),
    ?assert(rabbit_semver:gt("1.0.0-beta.11", "1.0.0-beta.2")),
    ?assert(rabbit_semver:gt("1.0.0-beta.11", "1.0.0.0-beta.2")),
    ?assert(rabbit_semver:gt("1.0.0-rc.1", "1.0.0-beta.11")),
    ?assert(rabbit_semver:gt("1.0.0-rc.1+build.1", "1.0.0-rc.1")),
    ?assert(rabbit_semver:gt("1.0.0", "1.0.0-rc.1+build.1")),
    ?assert(rabbit_semver:gt("1.0.0+0.3.7", "1.0.0")),
    ?assert(rabbit_semver:gt("1.3.7+build", "1.0.0+0.3.7")),
    ?assert(rabbit_semver:gt("1.3.7+build.2.b8f12d7", "1.3.7+build")),
    ?assert(rabbit_semver:gt("1.3.7+build.2.b8f12d7", "1.3.7.0+build")),
    ?assert(rabbit_semver:gt("1.3.7+build.11.e0f985a", "1.3.7+build.2.b8f12d7")),
    ?assert(rabbit_semver:gt("aa.cc", "aa.bb")),
    ?assertNot(rabbit_semver:gt("1.0.0-alpha", "1.0.0-alpha.1")),
    ?assertNot(rabbit_semver:gt("1.0.0-alpha", "1.0.0.0-alpha.1")),
    ?assertNot(rabbit_semver:gt("1.0.0-alpha.1", "1.0.0-beta.2")),
    ?assertNot(rabbit_semver:gt("1.0.0-beta.2", "1.0.0-beta.11")),
    ?assertNot(rabbit_semver:gt("1.0.0-beta.11", "1.0.0-rc.1")),
    ?assertNot(rabbit_semver:gt("1.0.0-rc.1", "1.0.0-rc.1+build.1")),
    ?assertNot(rabbit_semver:gt("1.0.0-rc.1+build.1", "1.0.0")),
    ?assertNot(rabbit_semver:gt("1.0.0", "1.0.0+0.3.7")),
    ?assertNot(rabbit_semver:gt("1.0.0+0.3.7", "1.3.7+build")),
    ?assertNot(rabbit_semver:gt("1.3.7+build", "1.3.7+build.2.b8f12d7")),
    ?assertNot(rabbit_semver:gt("1.3.7+build.2.b8f12d7", "1.3.7+build.11.e0f985a")),
    ?assertNot(rabbit_semver:gt("1.0.0-alpha", "1.0.0-alpha")),
    ?assertNot(rabbit_semver:gt("1", "1.0.0")),
    ?assertNot(rabbit_semver:gt("aa.bb", "aa.bb")),
    ?assertNot(rabbit_semver:gt("aa.cc", "aa.dd")),
    ?assertNot(rabbit_semver:gt("1.0", "1.0.0")),
    ?assertNot(rabbit_semver:gt("1.0.0", "1")),
    ?assertNot(rabbit_semver:gt("1.0+alpha.1", "1.0.0+alpha.1")),
    ?assertNot(rabbit_semver:gt("1.0-alpha.1+build.1", "1.0.0-alpha.1+build.1")).

lt_test(_Config) ->
    ?assert(rabbit_semver:lt("1.0.0-alpha", "1.0.0-alpha.1")),
    ?assert(rabbit_semver:lt("1.0.0-alpha", "1.0.0.0-alpha.1")),
    ?assert(rabbit_semver:lt("1.0.0-alpha.1", "1.0.0-beta.2")),
    ?assert(rabbit_semver:lt("1.0.0-beta.2", "1.0.0-beta.11")),
    ?assert(rabbit_semver:lt("1.0.0-beta.11", "1.0.0-rc.1")),
    ?assert(rabbit_semver:lt("1.0.0.1-beta.11", "1.0.0.1-rc.1")),
    ?assert(rabbit_semver:lt("1.0.0-rc.1", "1.0.0-rc.1+build.1")),
    ?assert(rabbit_semver:lt("1.0.0-rc.1+build.1", "1.0.0")),
    ?assert(rabbit_semver:lt("1.0.0", "1.0.0+0.3.7")),
    ?assert(rabbit_semver:lt("1.0.0+0.3.7", "1.3.7+build")),
    ?assert(rabbit_semver:lt("1.3.7+build", "1.3.7+build.2.b8f12d7")),
    ?assert(rabbit_semver:lt("1.3.7+build.2.b8f12d7", "1.3.7+build.11.e0f985a")),
    ?assertNot(rabbit_semver:lt("1.0.0-alpha", "1.0.0-alpha")),
    ?assertNot(rabbit_semver:lt("1", "1.0.0")),
    ?assert(rabbit_semver:lt("1", "1.0.0.1")),
    ?assert(rabbit_semver:lt("AA.DD", "AA.EE")),
    ?assertNot(rabbit_semver:lt("1.0", "1.0.0")),
    ?assertNot(rabbit_semver:lt("1.0.0.0", "1")),
    ?assertNot(rabbit_semver:lt("1.0+alpha.1", "1.0.0+alpha.1")),
    ?assertNot(rabbit_semver:lt("AA.DD", "AA.CC")),
    ?assertNot(rabbit_semver:lt("1.0-alpha.1+build.1", "1.0.0-alpha.1+build.1")),
    ?assertNot(rabbit_semver:lt("1.0.0-alpha.1", "1.0.0-alpha")),
    ?assertNot(rabbit_semver:lt("1.0.0-beta.2", "1.0.0-alpha.1")),
    ?assertNot(rabbit_semver:lt("1.0.0-beta.11", "1.0.0-beta.2")),
    ?assertNot(rabbit_semver:lt("1.0.0-rc.1", "1.0.0-beta.11")),
    ?assertNot(rabbit_semver:lt("1.0.0-rc.1+build.1", "1.0.0-rc.1")),
    ?assertNot(rabbit_semver:lt("1.0.0", "1.0.0-rc.1+build.1")),
    ?assertNot(rabbit_semver:lt("1.0.0+0.3.7", "1.0.0")),
    ?assertNot(rabbit_semver:lt("1.3.7+build", "1.0.0+0.3.7")),
    ?assertNot(rabbit_semver:lt("1.3.7+build.2.b8f12d7", "1.3.7+build")),
    ?assertNot(rabbit_semver:lt("1.3.7+build.11.e0f985a", "1.3.7+build.2.b8f12d7")).

gte_test(_Config) ->
    ?assert(rabbit_semver:gte("1.0.0-alpha", "1.0.0-alpha")),
    ?assert(rabbit_semver:gte("1", "1.0.0")),
    ?assert(rabbit_semver:gte("1.0", "1.0.0")),
    ?assert(rabbit_semver:gte("1.0.0", "1")),
    ?assert(rabbit_semver:gte("1.0.0.0", "1")),
    ?assert(rabbit_semver:gte("1.0+alpha.1", "1.0.0+alpha.1")),
    ?assert(rabbit_semver:gte("1.0-alpha.1+build.1", "1.0.0-alpha.1+build.1")),
    ?assert(rabbit_semver:gte("1.0.0-alpha.1+build.1", "1.0.0.0-alpha.1+build.1")),
    ?assert(rabbit_semver:gte("1.0.0-alpha.1", "1.0.0-alpha")),
    ?assert(rabbit_semver:gte("1.0.0-beta.2", "1.0.0-alpha.1")),
    ?assert(rabbit_semver:gte("1.0.0-beta.11", "1.0.0-beta.2")),
    ?assert(rabbit_semver:gte("aa.bb", "aa.bb")),
    ?assert(rabbit_semver:gte("dd", "aa")),
    ?assert(rabbit_semver:gte("1.0.0-rc.1", "1.0.0-beta.11")),
    ?assert(rabbit_semver:gte("1.0.0-rc.1+build.1", "1.0.0-rc.1")),
    ?assert(rabbit_semver:gte("1.0.0", "1.0.0-rc.1+build.1")),
    ?assert(rabbit_semver:gte("1.0.0+0.3.7", "1.0.0")),
    ?assert(rabbit_semver:gte("1.3.7+build", "1.0.0+0.3.7")),
    ?assert(rabbit_semver:gte("1.3.7+build.2.b8f12d7", "1.3.7+build")),
    ?assert(rabbit_semver:gte("1.3.7+build.11.e0f985a", "1.3.7+build.2.b8f12d7")),
    ?assertNot(rabbit_semver:gte("1.0.0-alpha", "1.0.0-alpha.1")),
    ?assertNot(rabbit_semver:gte("CC", "DD")),
    ?assertNot(rabbit_semver:gte("1.0.0-alpha.1", "1.0.0-beta.2")),
    ?assertNot(rabbit_semver:gte("1.0.0-beta.2", "1.0.0-beta.11")),
    ?assertNot(rabbit_semver:gte("1.0.0-beta.11", "1.0.0-rc.1")),
    ?assertNot(rabbit_semver:gte("1.0.0-rc.1", "1.0.0-rc.1+build.1")),
    ?assertNot(rabbit_semver:gte("1.0.0-rc.1+build.1", "1.0.0")),
    ?assertNot(rabbit_semver:gte("1.0.0", "1.0.0+0.3.7")),
    ?assertNot(rabbit_semver:gte("1.0.0+0.3.7", "1.3.7+build")),
    ?assertNot(rabbit_semver:gte("1.0.0", "1.0.0+build.1")),
    ?assertNot(rabbit_semver:gte("1.3.7+build", "1.3.7+build.2.b8f12d7")),
    ?assertNot(rabbit_semver:gte("1.3.7+build.2.b8f12d7", "1.3.7+build.11.e0f985a")).

lte_test(_Config) ->
    ?assert(rabbit_semver:lte("1.0.0-alpha", "1.0.0-alpha.1")),
    ?assert(rabbit_semver:lte("1.0.0-alpha.1", "1.0.0-beta.2")),
    ?assert(rabbit_semver:lte("1.0.0-beta.2", "1.0.0-beta.11")),
    ?assert(rabbit_semver:lte("1.0.0-beta.11", "1.0.0-rc.1")),
    ?assert(rabbit_semver:lte("1.0.0-rc.1", "1.0.0-rc.1+build.1")),
    ?assert(rabbit_semver:lte("1.0.0-rc.1+build.1", "1.0.0")),
    ?assert(rabbit_semver:lte("1.0.0", "1.0.0+0.3.7")),
    ?assert(rabbit_semver:lte("1.0.0+0.3.7", "1.3.7+build")),
    ?assert(rabbit_semver:lte("1.3.7+build", "1.3.7+build.2.b8f12d7")),
    ?assert(rabbit_semver:lte("1.3.7+build.2.b8f12d7", "1.3.7+build.11.e0f985a")),
    ?assert(rabbit_semver:lte("1.0.0-alpha", "1.0.0-alpha")),
    ?assert(rabbit_semver:lte("1", "1.0.0")),
    ?assert(rabbit_semver:lte("1.0", "1.0.0")),
    ?assert(rabbit_semver:lte("1.0.0", "1")),
    ?assert(rabbit_semver:lte("1.0+alpha.1", "1.0.0+alpha.1")),
    ?assert(rabbit_semver:lte("1.0.0.0+alpha.1", "1.0.0+alpha.1")),
    ?assert(rabbit_semver:lte("1.0-alpha.1+build.1", "1.0.0-alpha.1+build.1")),
    ?assert(rabbit_semver:lte("aa", "cc")),
    ?assert(rabbit_semver:lte("cc", "cc")),
    ?assertNot(rabbit_semver:lte("1.0.0-alpha.1", "1.0.0-alpha")),
    ?assertNot(rabbit_semver:lte("cc", "aa")),
    ?assertNot(rabbit_semver:lte("1.0.0-beta.2", "1.0.0-alpha.1")),
    ?assertNot(rabbit_semver:lte("1.0.0-beta.11", "1.0.0-beta.2")),
    ?assertNot(rabbit_semver:lte("1.0.0-rc.1", "1.0.0-beta.11")),
    ?assertNot(rabbit_semver:lte("1.0.0-rc.1+build.1", "1.0.0-rc.1")),
    ?assertNot(rabbit_semver:lte("1.0.0", "1.0.0-rc.1+build.1")),
    ?assertNot(rabbit_semver:lte("1.0.0+0.3.7", "1.0.0")),
    ?assertNot(rabbit_semver:lte("1.3.7+build", "1.0.0+0.3.7")),
    ?assertNot(rabbit_semver:lte("1.3.7+build.2.b8f12d7", "1.3.7+build")),
    ?assertNot(rabbit_semver:lte("1.3.7+build.11.e0f985a", "1.3.7+build.2.b8f12d7")).

between_test(_Config) ->
    ?assert(rabbit_semver:between("1.0.0-alpha", "1.0.0-alpha.3", "1.0.0-alpha.2")),
    ?assert(rabbit_semver:between("1.0.0-alpha.1", "1.0.0-beta.2", "1.0.0-alpha.25")),
    ?assert(rabbit_semver:between("1.0.0-beta.2", "1.0.0-beta.11", "1.0.0-beta.7")),
    ?assert(rabbit_semver:between("1.0.0-beta.11", "1.0.0-rc.3", "1.0.0-rc.1")),
    ?assert(rabbit_semver:between("1.0.0-rc.1", "1.0.0-rc.1+build.3", "1.0.0-rc.1+build.1")),
    ?assert(rabbit_semver:between("1.0.0.0-rc.1", "1.0.0-rc.1+build.3", "1.0.0-rc.1+build.1")),
    ?assert(rabbit_semver:between("1.0.0-rc.1+build.1", "1.0.0", "1.0.0-rc.33")),
    ?assert(rabbit_semver:between("1.0.0", "1.0.0+0.3.7", "1.0.0+0.2")),
    ?assert(rabbit_semver:between("1.0.0+0.3.7", "1.3.7+build", "1.2")),
    ?assert(rabbit_semver:between("1.3.7+build", "1.3.7+build.2.b8f12d7", "1.3.7+build.1")),
    ?assert(rabbit_semver:between("1.3.7+build.2.b8f12d7", "1.3.7+build.11.e0f985a", "1.3.7+build.10.a36faa")),
    ?assert(rabbit_semver:between("1.0.0-alpha", "1.0.0-alpha", "1.0.0-alpha")),
    ?assert(rabbit_semver:between("1", "1.0.0", "1.0.0")),
    ?assert(rabbit_semver:between("1.0", "1.0.0", "1.0.0")),
    ?assert(rabbit_semver:between("1.0", "1.0.0.0", "1.0.0.0")),
    ?assert(rabbit_semver:between("1.0.0", "1", "1")),
    ?assert(rabbit_semver:between("1.0+alpha.1", "1.0.0+alpha.1", "1.0.0+alpha.1")),
    ?assert(rabbit_semver:between("1.0-alpha.1+build.1", "1.0.0-alpha.1+build.1", "1.0.0-alpha.1+build.1")),
    ?assert(rabbit_semver:between("aaa", "ddd", "cc")),
    ?assertNot(rabbit_semver:between("1.0.0-alpha.1", "1.0.0-alpha.22", "1.0.0")),
    ?assertNot(rabbit_semver:between("1.0.0", "1.0.0-alpha.1", "2.0")),
    ?assertNot(rabbit_semver:between("1.0.0-beta.1", "1.0.0-beta.11", "1.0.0-alpha")),
    ?assertNot(rabbit_semver:between("1.0.0-beta.11", "1.0.0-rc.1", "1.0.0-rc.22")),
    ?assertNot(rabbit_semver:between("aaa", "ddd", "zzz")).

pes_test(_Config) ->
    ?assert(rabbit_semver:pes("2.6.0", "2.6")),
    ?assert(rabbit_semver:pes("2.7", "2.6")),
    ?assert(rabbit_semver:pes("2.8", "2.6")),
    ?assert(rabbit_semver:pes("2.9", "2.6")),
    ?assert(rabbit_semver:pes("A.B", "A.A")),
    ?assertNot(rabbit_semver:pes("3.0.0", "2.6")),
    ?assertNot(rabbit_semver:pes("2.5", "2.6")),
    ?assert(rabbit_semver:pes("2.6.5", "2.6.5")),
    ?assert(rabbit_semver:pes("2.6.6", "2.6.5")),
    ?assert(rabbit_semver:pes("2.6.7", "2.6.5")),
    ?assert(rabbit_semver:pes("2.6.8", "2.6.5")),
    ?assert(rabbit_semver:pes("2.6.9", "2.6.5")),
    ?assert(rabbit_semver:pes("2.6.0.9", "2.6.0.5")),
    ?assertNot(rabbit_semver:pes("2.7", "2.6.5")),
    ?assertNot(rabbit_semver:pes("2.1.7", "2.1.6.5")),
    ?assertNot(rabbit_semver:pes("A.A", "A.B")),
    ?assertNot(rabbit_semver:pes("2.5", "2.6.5")).

version_format_test(_Config) ->
    ?assertEqual(["1", [], []], rabbit_semver:format({1, {[],[]}})),
    ?assertEqual(["1", ".", "2", ".", "34", [], []], rabbit_semver:format({{1,2,34},{[],[]}})),
    ?assertEqual(<<"a">>, iolist_to_binary(rabbit_semver:format({<<"a">>, {[],[]}}))),
    ?assertEqual(<<"a.b">>, iolist_to_binary(rabbit_semver:format({{<<"a">>,<<"b">>}, {[],[]}}))),
    ?assertEqual(<<"1">>, iolist_to_binary(rabbit_semver:format({1, {[],[]}}))),
    ?assertEqual(<<"1.2">>, iolist_to_binary(rabbit_semver:format({{1,2}, {[],[]}}))),
    ?assertEqual(<<"1.2.2">>, iolist_to_binary(rabbit_semver:format({{1,2,2}, {[],[]}}))),
    ?assertEqual(<<"1.99.2">>, iolist_to_binary(rabbit_semver:format({{1,99,2}, {[],[]}}))),
    ?assertEqual(<<"1.99.2-alpha">>, iolist_to_binary(rabbit_semver:format({{1,99,2}, {[<<"alpha">>],[]}}))),
    ?assertEqual(<<"1.99.2-alpha.1">>, iolist_to_binary(rabbit_semver:format({{1,99,2}, {[<<"alpha">>,1], []}}))),
    ?assertEqual(<<"1.99.2+build.1.a36">>,
                 iolist_to_binary(rabbit_semver:format({{1,99,2}, {[], [<<"build">>, 1, <<"a36">>]}}))),
    ?assertEqual(<<"1.99.2.44+build.1.a36">>,
                 iolist_to_binary(rabbit_semver:format({{1,99,2,44}, {[], [<<"build">>, 1, <<"a36">>]}}))),
    ?assertEqual(<<"1.99.2-alpha.1+build.1.a36">>,
                 iolist_to_binary(rabbit_semver:format({{1,99,2}, {[<<"alpha">>, 1], [<<"build">>, 1, <<"a36">>]}}))),
    ?assertEqual(<<"1.99.2.44-alpha.1+build.1.a36">>,
                 iolist_to_binary(rabbit_semver:format({{1,99,2,44}, {[<<"alpha">>, 1], [<<"build">>, 1, <<"a36">>]}}))),
    ?assertEqual(<<"a-pre+build">>,
                 iolist_to_binary(rabbit_semver:format({<<"a">>, {[<<"pre">>], [<<"build">>]}}))).

prefixed_version_parse_test(_Config) ->
    ?assertMatch({<<"tanzu">>,
                  {[], [<<"rabbitmq">>,<<"v3">>,13,12,<<"dev">>,1,8,<<"g37372ff">>]}},
                 rabbit_semver:parse("tanzu+rabbitmq.v3.13.12.dev.1.8.g37372ff")),
    ?assertMatch({{4,2,0},
                  {[], [<<"beta">>,4,104,<<"ge49ed93">>]}},
                 rabbit_semver:parse("4.2.0+beta.4.104.ge49ed93")),
    ?assertMatch({{4,0,9}, {[],[]}}, rabbit_semver:parse("4.0.9")),
    ?assertMatch({{4,1,7}, {[],[]}}, rabbit_semver:parse("4.1.7")),
    ?assertMatch({{4,2,3}, {[],[]}}, rabbit_semver:parse("4.2.3")),
    ?assertMatch({{4,2,3}, {[],[<<"4">>,<<"gbbde858">>,<<"dirty">>]}},
                 rabbit_semver:parse("4.2.3+4.gbbde858.dirty")).

prefixed_version_normalize_test(_Config) ->
    ?assertMatch({{3, 13, 12, 0}, {[], _}},
                 rabbit_semver:normalize(rabbit_semver:parse("tanzu+rabbitmq.v3.13.12.dev.1.8.g37372ff"))),
    ?assertMatch({{4, 2, 0, 0}, {[], _}},
                 rabbit_semver:normalize(rabbit_semver:parse("4.2.0+beta.4.104.ge49ed93"))),
    ?assertMatch({{3, 13, 0, 0}, {[], _}},
                 rabbit_semver:normalize(rabbit_semver:parse("tanzu+rabbitmq.v3.13.dev"))),
    ?assertMatch({{3, 0, 0, 0}, {[], _}},
                 rabbit_semver:normalize(rabbit_semver:parse("tanzu+rabbitmq.v3.dev"))),
    ?assertMatch({{3, 13, 12, 5}, {[], _}},
                 rabbit_semver:normalize(rabbit_semver:parse("tanzu+rabbitmq.v3.13.12.5.dev"))),
    ?assertMatch({{<<"noversion">>, 0, 0, 0}, {[], [<<"noembedded">>]}},
                 rabbit_semver:normalize(rabbit_semver:parse("noversion+noembedded"))),
    ?assertMatch({{3, 13, 12, 0}, {[<<"prerelease">>], _}},
                 rabbit_semver:normalize(rabbit_semver:parse("tanzu-prerelease+rabbitmq.v3.13.12"))),
    ?assertMatch({{4, 0, 9, 0}, {[], []}},
                 rabbit_semver:normalize(rabbit_semver:parse("4.0.9"))),
    ?assertMatch({{4, 1, 7, 0}, {[], []}},
                 rabbit_semver:normalize(rabbit_semver:parse("4.1.7"))),
    ?assertMatch({{4, 2, 3, 0}, {[], []}},
                 rabbit_semver:normalize(rabbit_semver:parse("4.2.3"))),
    ?assertMatch({{4, 2, 3, 0}, {[], _}},
                 rabbit_semver:normalize(rabbit_semver:parse("4.2.3+4.gbbde858.dirty"))).

edge_cases_test(_Config) ->
    %% Large numbers
    ?assert(rabbit_semver:gt("999.999.999", "999.999.998")),
    ?assert(rabbit_semver:lt("0.0.0", "0.0.1")),
    %% Single component
    ?assert(rabbit_semver:eql("5", "5.0.0")),
    ?assert(rabbit_semver:gt("6", "5")),
    %% v prefix
    ?assert(rabbit_semver:eql("v0.0.0", "0.0.0")),
    ?assert(rabbit_semver:eql("v1", "1.0.0")),
    %% Build metadata
    ?assert(rabbit_semver:gt("1.0.0+zzz", "1.0.0+aaa")),
    ?assert(rabbit_semver:lt("1.0.0+001", "1.0.0+002")),
    %% Pre-release ordering
    ?assert(rabbit_semver:lt("1.0.0-alpha", "1.0.0-alpha.1")),
    ?assert(rabbit_semver:lt("1.0.0-alpha.1", "1.0.0-alpha.beta")),
    ?assert(rabbit_semver:lt("1.0.0-alpha.beta", "1.0.0-beta")),
    ?assert(rabbit_semver:lt("1.0.0-beta", "1.0.0-beta.2")),
    ?assert(rabbit_semver:lt("1.0.0-beta.2", "1.0.0-beta.11")),
    ?assert(rabbit_semver:lt("1.0.0-rc.1", "1.0.0")),
    %% Numeric pre-release comparison
    ?assert(rabbit_semver:lt("1.0.0-alpha.1", "1.0.0-alpha.2")),
    ?assert(rabbit_semver:lt("1.0.0-alpha.2", "1.0.0-alpha.10")).

prefixed_version_compare_test(_Config) ->
    ?assert(rabbit_semver:gt("4.2.0", "tanzu+rabbitmq.v3.13.12.dev.1.8.g37372ff")),
    ?assert(rabbit_semver:lt("tanzu+rabbitmq.v3.13.12.dev.1.8.g37372ff", "4.2.0")),
    ?assert(rabbit_semver:gt("tanzu+rabbitmq.v4.0.0.dev.1.8.g37372ff", "3.13.12")),
    ?assert(rabbit_semver:lt("3.13.12", "tanzu+rabbitmq.v4.0.0.dev.1.8.g37372ff")),
    ?assert(rabbit_semver:gte("tanzu+rabbitmq.v3.13.12.dev", "3.13.12")),
    ?assert(rabbit_semver:gt("tanzu+rabbitmq.v3.13.12.dev", "3.13.12")),
    ?assertNot(rabbit_semver:eql("tanzu+rabbitmq.v3.13.12.dev", "3.13.11")),
    ?assert(rabbit_semver:gt("tanzu+a.v4.0.0", "tanzu+b.v3.13.12")),
    ?assert(rabbit_semver:lt("tanzu+a.v3.13.12", "tanzu+b.v4.0.0")),
    ?assert(rabbit_semver:lt("tanzu+a.v3.13.12.dev", "tanzu+b.v3.13.12.dev")),
    ?assert(rabbit_semver:gt("3.13.12", "tanzu-prerelease+rabbitmq.v3.13.12")),
    ?assert(rabbit_semver:lt("tanzu-alpha+rabbitmq.v3.13.12", "tanzu-beta+rabbitmq.v3.13.12")),
    ?assert(rabbit_semver:gt("4.2.3", "4.1.7")),
    ?assert(rabbit_semver:gt("4.1.7", "4.0.9")),
    ?assert(rabbit_semver:lt("4.0.9", "4.1.7")),
    ?assert(rabbit_semver:lt("4.1.7", "4.2.3")),
    ?assert(rabbit_semver:gt("4.2.3+4.gbbde858.dirty", "4.2.3")),
    ?assert(rabbit_semver:lt("4.2.3", "4.2.3+4.gbbde858.dirty")),
    ?assert(rabbit_semver:lt("4.2.3+4.gbbde858.dirty", "tanzu+rabbitmq.v4.2.3")),
    ?assert(rabbit_semver:gte("4.2.3+4.gbbde858.dirty", "4.2.3")).
