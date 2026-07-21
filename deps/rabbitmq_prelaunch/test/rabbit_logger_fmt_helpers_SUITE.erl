%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_logger_fmt_helpers_SUITE).

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
    [format_args_depth_truncates,
     format_args_depth_unlimited_is_full].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

%% A term deeply nested past a small depth limit.
deep_term() ->
    [{level1, [{level2, [{level3, [{level4, [{level5, secret}]}]}]}]}].

format(Msg, Config) ->
    Meta = #{},
    lists:flatten(rabbit_logger_fmt_helpers:format_msg(Msg, Meta, Config)).

%%%===================================================================
%%% Test cases
%%%===================================================================

%% A `{Format, Args}' message formatted with a small `depth' must be
%% truncated. OTP's logger_formatter rewrites ~p/~w to ~P/~W with the
%% configured depth, which renders deep sub-terms as `...'.
format_args_depth_truncates(_Config) ->
    Config = #{single_line => false, depth => 3},
    Formatted = format({"~p", [deep_term()]}, Config),
    ?assertNotEqual(nomatch, string:find(Formatted, "...")),
    ?assertEqual(nomatch, string:find(Formatted, "secret")).

%% With `depth => unlimited' the full term is rendered, including the
%% deepest element.
format_args_depth_unlimited_is_full(_Config) ->
    Config = #{single_line => false, depth => unlimited},
    Formatted = format({"~p", [deep_term()]}, Config),
    ?assertNotEqual(nomatch, string:find(Formatted, "secret")),
    ?assertEqual(nomatch, string:find(Formatted, "...")).
