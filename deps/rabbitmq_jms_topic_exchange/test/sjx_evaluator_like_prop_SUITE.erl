%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Property-based tests for sjx_evaluator's LIKE handling

-module(sjx_evaluator_like_prop_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(NUM_TESTS, 100).
-define(TIME_LIMIT_MS, 50).

all() ->
    [
        wildcard_heavy_patterns_always_bounded,
        literal_pattern_matching_is_exact,
        oversized_pattern_is_rejected,
        malformed_escape_never_crashes
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

wildcard_heavy_patterns_always_bounded(Config) ->
    Property = fun() -> prop_wildcard_heavy_patterns_always_bounded(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_wildcard_heavy_patterns_always_bounded(_Config) ->
    MaxRepeats = (rabbit_re:max_pattern_length() - 1) div 2,
    ?FORALL(
        {Repeats, SubjectLength},
        {range(1, MaxRepeats), range(100, 5000)},
        begin
            Selector = iolist_to_binary([lists:duplicate(Repeats, "%_"), "X"]),
            Subject = binary:copy(<<"a">>, SubjectLength),
            Headers = [{<<"p">>, longstr, Subject}],
            {ElapsedUs, _} = timer:tc(
                fun() -> sjx_evaluator:evaluate({'like', {ident, <<"p">>}, Selector, no_escape}, Headers) end),
            ElapsedUs < ?TIME_LIMIT_MS * 1000
        end).

literal_pattern_matching_is_exact(Config) ->
    Property = fun() -> prop_literal_pattern_matching_is_exact(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_literal_pattern_matching_is_exact(_Config) ->
    ?FORALL(
        {Literal, Suffix},
        {non_empty(literal_binary()), non_empty(literal_binary())},
        begin
            Matches = fun(Value) ->
                sjx_evaluator:evaluate({'like', {ident, <<"v">>}, Literal, no_escape},
                                        [{<<"v">>, longstr, Value}])
            end,
            Matches(Literal) andalso not Matches(<<Literal/binary, Suffix/binary>>)
        end).

oversized_pattern_is_rejected(Config) ->
    Property = fun() -> prop_oversized_pattern_is_rejected(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_oversized_pattern_is_rejected(_Config) ->
    ?FORALL(
        ExtraLength,
        range(1, 1000),
        begin
            Selector = binary:copy(<<"%">>, rabbit_re:max_pattern_length() + ExtraLength),
            Headers = [{<<"p">>, longstr, <<"anything">>}],
            not sjx_evaluator:evaluate({'like', {ident, <<"p">>}, Selector, no_escape}, Headers)
        end).

%% An escape argument that isn't a single byte is a shape the parser's
%% grammar allows but sjx_evaluator can't turn into a regex; evaluation
%% must return a boolean rather than raise.
malformed_escape_never_crashes(Config) ->
    Property = fun() -> prop_malformed_escape_never_crashes(Config) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?NUM_TESTS).

prop_malformed_escape_never_crashes(_Config) ->
    ?FORALL(
        Escape,
        oneof([binary(), no_escape, regex, true, false, in]),
        begin
            Headers = [{<<"colour">>, longstr, <<"blue">>}],
            is_boolean(sjx_evaluator:evaluate({'like', {ident, <<"colour">>}, <<"bl%">>, Escape}, Headers))
        end).

%% Excludes % and _, which would change LIKE semantics rather than stay literal.
literal_binary() ->
    ?LET(Chars,
         list(oneof([range($a, $z), range($A, $Z), range($0, $9),
                     $., $*, $+, $?, $^, $=, $!, $:, $$, ${, $}, $(, $), $|, $[, $], $/, $\\])),
         list_to_binary(Chars)).
