%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(amqp_filter_sql_prop_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp10_common/include/amqp10_filter.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(ITERATIONS, 2000).

all() ->
    [
     prop_like_result_is_valid,
     prop_like_multi_percent_non_matching,
     prop_like_exact_pattern_matches_itself,
     prop_like_percent_matches_any_string
    ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.
init_per_testcase(_Testcase, Config) -> Config.
end_per_testcase(_Testcase, _Config) -> ok.

%%%===================================================================
%%% Properties
%%%===================================================================

prop_like_result_is_valid(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_like_result_is_valid_/0, [], ?ITERATIONS).

prop_like_result_is_valid_() ->
    ?FORALL({Pattern, Subject},
            {like_pattern(), lowercase_binary()},
            begin
                SQL = "k LIKE '" ++ Pattern ++ "'",
                case parse(SQL) of
                    error ->
                        true;
                    {ok, Expr} ->
                        R = eval_filter(Expr, Subject),
                        R =:= true orelse R =:= false orelse R =:= undefined
                end
            end).

prop_like_multi_percent_non_matching(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_like_multi_percent_non_matching_/0, [], ?ITERATIONS).

prop_like_multi_percent_non_matching_() ->
    ?FORALL({N, Char, SubjectLen},
            {integer(2, 100), choose($A, $Z), integer(1, 500)},
            begin
                Pattern = lists:duplicate(N, $%) ++ [Char],
                SQL = "k LIKE '" ++ Pattern ++ "'",
                Subject = binary:copy(<<"a">>, SubjectLen),
                {ok, Expr} = parse(SQL),
                false =:= eval_filter(Expr, Subject)
            end).

prop_like_exact_pattern_matches_itself(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_like_exact_pattern_matches_itself_/0, [], ?ITERATIONS).

prop_like_exact_pattern_matches_itself_() ->
    ?FORALL(Chars, non_empty(list(literal_char())),
            begin
                SQL = "k LIKE '" ++ Chars ++ "'",
                Subject = unicode:characters_to_binary(Chars),
                case parse(SQL) of
                    error ->
                        true;
                    {ok, Expr} ->
                        true =:= eval_filter(Expr, Subject)
                end
            end).

prop_like_percent_matches_any_string(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_like_percent_matches_any_string_/0, [], ?ITERATIONS).

prop_like_percent_matches_any_string_() ->
    {ok, Expr} = parse("k LIKE '%'"),
    ?FORALL(Subject, lowercase_binary(),
            true =:= eval_filter(Expr, Subject)).

%%%===================================================================
%%% Generators
%%%===================================================================

%% Excludes ' (terminates the SQL string literal) and control characters (parser rejects them).
pattern_char() ->
    ?SUCHTHAT(C, choose(32, 126), C =/= $').

like_pattern() ->
    list(frequency([{3, $%}, {3, $_}, {8, pattern_char()}])).

literal_char() ->
    ?SUCHTHAT(C, choose(32, 126),
              C =/= $' andalso C =/= $% andalso C =/= $_).

%% Lowercase ASCII — always valid UTF-8.
lowercase_binary() ->
    ?LET(Chars, list(choose($a, $z)),
         list_to_binary(Chars)).

%%%===================================================================
%%% Helpers
%%%===================================================================

eval_filter(ParsedExpr, Subject) ->
    AppProps = [{{utf8, <<"k">>}, {utf8, Subject}}],
    AP = #'v1_0.application_properties'{content = AppProps},
    Body = #'v1_0.amqp_value'{content = {symbol, <<"m">>}},
    Sections = [#'v1_0.header'{}, #'v1_0.properties'{}, AP, Body],
    Payload = iolist_to_binary([amqp10_framing:encode_bin(X) || X <- Sections]),
    Mc = mc_amqp:init_from_stream(Payload, #{}),
    rabbit_amqp_filter_sql:eval(ParsedExpr, Mc).

parse(Selector) ->
    Descriptor = {ulong, ?DESCRIPTOR_CODE_SQL_FILTER},
    Filter = {described, Descriptor, {utf8, Selector}},
    rabbit_amqp_filter_sql:parse(Filter).
