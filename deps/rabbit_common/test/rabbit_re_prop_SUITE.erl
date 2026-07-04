%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_re_prop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-compile([export_all, nowarn_export_all]).

-define(NUM_TESTS, 256).
-define(PROPER_OPTS, [{numtests, ?NUM_TESTS}, {to_file, user}]).

all() ->
    [{group, properties}].

groups() ->
    [{properties, [parallel],
      [run_agrees_with_re_for_short_inputs_prop,
       compile_returns_too_long_for_overlong_input_prop,
       run_returns_safe_atom_or_error_prop,
       matches_always_returns_boolean_prop,
       escape_produces_literal_matcher_prop,
       escape_prevents_range_inside_char_class_prop]}].

%% Property runners

run_agrees_with_re_for_short_inputs_prop(_Config) ->
    true = proper:quickcheck(run_agrees_with_re_for_short_inputs(),
                             ?PROPER_OPTS).

compile_returns_too_long_for_overlong_input_prop(_Config) ->
    true = proper:quickcheck(compile_returns_too_long_for_overlong_input(),
                             ?PROPER_OPTS).

run_returns_safe_atom_or_error_prop(_Config) ->
    true = proper:quickcheck(run_returns_safe_atom_or_error(),
                             ?PROPER_OPTS).

matches_always_returns_boolean_prop(_Config) ->
    true = proper:quickcheck(matches_always_returns_boolean(),
                             ?PROPER_OPTS).

escape_produces_literal_matcher_prop(_Config) ->
    true = proper:quickcheck(escape_produces_literal_matcher(),
                             ?PROPER_OPTS).

escape_prevents_range_inside_char_class_prop(_Config) ->
    true = proper:quickcheck(escape_prevents_range_inside_char_class(),
                             ?PROPER_OPTS).

%% Properties

run_agrees_with_re_for_short_inputs() ->
    %% On short alphanumeric inputs the helper agrees with plain `re:run/3`.
    ?FORALL({Subject, Pattern}, {short_alpha_binary(), short_alpha_binary()},
            begin
                Bare = case re:run(Subject, Pattern, [{capture, none}]) of
                           match   -> match;
                           nomatch -> nomatch
                       end,
                Helper = rabbit_re:run(Subject, Pattern),
                Bare =:= Helper
            end).

compile_returns_too_long_for_overlong_input() ->
    ?FORALL(Extra, non_neg_integer(),
            begin
                Len = rabbit_re:max_pattern_length() + Extra + 1,
                Pat = binary:copy(<<"a">>, Len),
                {error, pattern_too_long} =:= rabbit_re:compile(Pat)
            end).

run_returns_safe_atom_or_error() ->
    %% The helper returns one of the documented shapes for any binary input.
    ?FORALL({Subject, Pattern}, {binary(), binary()},
            case rabbit_re:run(Subject, Pattern) of
                match           -> true;
                nomatch         -> true;
                {match, _}      -> true;
                {error, _}      -> true;
                _Other          -> false
            end).

matches_always_returns_boolean() ->
    ?FORALL({Subject, Pattern}, {binary(), binary()},
            is_boolean(rabbit_re:matches(Subject, Pattern))).

escape_produces_literal_matcher() ->
    %% "^escape(V)$" matches V and no other string.
    ?FORALL({V, Other}, {printable_binary(), printable_binary()},
            begin
                Pattern = <<"^", (rabbit_re:escape(V))/binary, "$">>,
                case Other =:= V of
                    true  -> rabbit_re:matches(V, Pattern);
                    false -> not rabbit_re:matches(Other, Pattern)
                end
            end).

escape_prevents_range_inside_char_class() ->
    %% "X-Y" inside a "[...]" class must not form a range: the endpoints
    %% match, a character strictly between them does not.
    ?FORALL({Lo, Mid, Hi}, ordered_letters(),
            begin
                Class = <<"^[", (rabbit_re:escape(<<Lo, $-, Hi>>))/binary, "]$">>,
                rabbit_re:matches(<<Lo>>, Class)
                    andalso rabbit_re:matches(<<Hi>>, Class)
                    andalso not rabbit_re:matches(<<Mid>>, Class)
            end).

%% Generators

printable_binary() ->
    ?LET(L, list(integer(32, 126)), list_to_binary(L)).

ordered_letters() ->
    ?LET(Lo, integer($a, $x),
         ?LET(Hi, integer(Lo + 2, $z),
              ?LET(Mid, integer(Lo + 1, Hi - 1), {Lo, Mid, Hi}))).

short_alpha_binary() ->
    ?LET(L,
         list(oneof([integer($a, $z), integer($0, $9), $-])),
         list_to_binary(lists:sublist(L, 30))).
