%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_nodes_common_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

all_tests() ->
    [cookie_hash_does_not_crash_on_non_latin1_cookie].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(cookie_hash_does_not_crash_on_non_latin1_cookie, Config) ->
    [{prev_cookie, erlang:get_cookie()} | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(cookie_hash_does_not_crash_on_non_latin1_cookie, Config) ->
    true = erlang:set_cookie(node(), ?config(prev_cookie, Config)),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

%% Must not crash on a non-Latin-1 cookie. Hash computed
%% independently.
cookie_hash_does_not_crash_on_non_latin1_cookie(_Config) ->
    Cookie = list_to_atom([26085, 26412]),
    true = erlang:set_cookie(node(), Cookie),
    ?assertEqual("Tb7S5ldFeITmcTfTUUEZsw==",
                 rabbit_nodes_common:cookie_hash()).
