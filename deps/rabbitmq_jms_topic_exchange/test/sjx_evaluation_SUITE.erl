%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2012, 2013 Steve Powell (Zteve.Powell@gmail.com)
%% -----------------------------------------------------------------------------

%% Tests for sjx_evaluator

%% -----------------------------------------------------------------------------
-module(sjx_evaluation_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

-import(sjx_evaluator, [evaluate/2]).

%% Fixed type info for identifiers
%%
-define(TEST_TYPE_INFO,
[ {<<"JMSType">>,          longstr, <<"string">>}
, {<<"JMSCorrelationID">>, longstr, <<"string">>}
, {<<"JMSMessageID">>,     longstr, <<"string">>}
, {<<"JMSDeliveryMode">>,  longstr, <<"string">>}
, {<<"JMSPriority">>,      longstr, <<"number">>}
, {<<"JMSTimestamp">>,     longstr, <<"number">>}
]).


all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                basic_evaluate_test,
                                arithmetic_type_mismatch_test,
                                like_type_mismatch_test,
                                between_error_propagation_test,
                                arithmetic_overflow_test,
                                lookup_value_unknown_type_test,
                                in_type_mismatch_test,
                                comparison_type_mismatch_test,
                                like_range_wrapped_pattern_test,
                                between_range_form_test,
                                arithmetic_bignum_overflow_test,
                                arithmetic_long_wraparound_test
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

basic_evaluate_test(_Config) ->
    Hs = [{<<"JMSType">>, longstr, <<"car">>},
          {<<"colour">>, longstr, <<"blue">>},
          {<<"altcol">>, longstr, <<"'blue">>},
          {<<"likevar">>, longstr, <<"bl_ue">>},
          {<<"weight">>, signedint, 2501},
          {<<"WeIgHt">>, signedint, 2},
          {<<"afloat">>, float, 3.0e-2},
          {<<"abool">>, bool, false}],

    [ ?_assert(    eval(Hs, {'=', {'ident', <<"JMSType">>}, <<"car">>}                      ))
    , ?_assert(not eval(Hs, {'ident', <<"abool">>}                                          ))
    , ?_assert(    eval(Hs, {'not', {'ident', <<"abool">>}}                                 ))
    , ?_assert(    eval(Hs, {'=', {'ident', <<"colour">>}, <<"blue">>}                      ))
    , ?_assert(    eval(Hs, {'=', {'ident', <<"weight">>}, 2501}                            ))
    , ?_assert(    eval(Hs, {'=', {'ident', <<"WeIgHt">>}, 2}                               ))
    , ?_assert(    eval(Hs, {'=', 2501, {'ident', <<"weight">>}}                            ))
    , ?_assert(    eval(Hs, {'=', {'ident', <<"afloat">>}, 3.0e-2}                          ))
    , ?_assert(    eval(Hs, {'>', {'ident', <<"weight">>}, 2500}                            ))
    , ?_assert(    eval(Hs, {'<', {'ident', <<"weight">>}, 2502}                            ))
    , ?_assert(    eval(Hs, {'>=', {'ident', <<"weight">>}, 2501}                           ))
    , ?_assert(    eval(Hs, {'<=', {'ident', <<"weight">>}, 2501}                           ))
    , ?_assert(not eval(Hs, {'<=', {'ident', <<"weight">>}, 2500}                           ))
    , ?_assert(    eval(Hs, {'between', {'ident', <<"weight">>}, {'range', 0, 2501}}        ))
    , ?_assert(    eval(Hs, {'between', {'ident', <<"weight">>}, {'range', 2500, 2501}}     ))
    , ?_assert(    eval(Hs, {'between', 17, {'range', 17, 18}}                              ))
    , ?_assert(    eval(Hs, {'between', 17, {'range', 17, 17}}                              ))
    , ?_assert(    eval(Hs, {'not_between', 16, {'range', 17, 18}}                          ))
    , ?_assert(    eval(Hs, {'<', 2500, {'ident', <<"weight">>}}                            ))
    , ?_assert(    eval(Hs, {'>', 2502, {'ident', <<"weight">>}}                            ))
    , ?_assert(    eval(Hs, {'<=', 2500, {'ident', <<"weight">>}}                           ))
    , ?_assert(    eval(Hs, {'>=', 2502, {'ident', <<"weight">>}}                           ))
    , ?_assert(    eval(Hs, {'<=', 2501, {'ident', <<"weight">>}}                           ))
    , ?_assert(    eval(Hs, {'>=', 2501, {'ident', <<"weight">>}}                           ))
    , ?_assert(    eval(Hs, {'like', {'ident', <<"colour">>}, {<<"bl%">>, 'no_escape'}}     ))
    , ?_assert(    eval(Hs, {'like', {'ident', <<"likevar">>}, {<<"b_!_ue">>, <<"!">>}}     ))
    , ?_assert(    eval(Hs, {'like', {'ident', <<"colour">>}, {<<"bl_e">>, 'no_escape'}}    ))
    , ?_assert(    eval(Hs, {'not_like', {'ident', <<"colour">>}, {<<"l%">>, 'no_escape'}}  ))
    , ?_assert(not eval(Hs, {'not_like', {'ident', <<"colour">>}, {<<"bl%">>, 'no_escape'}} ))
    , ?_assert(    eval(Hs, {'in', {'ident', <<"colour">>}, [<<"blue">>, <<"green">>]}      ))
    , ?_assert(not eval(Hs, {'not_in', {'ident', <<"colour">>}, [<<"green">>, <<"blue">>]}  ))
    , ?_assert(not eval(Hs, {'in', {'ident', <<"colour">>}, [<<"bleen">>, <<"grue">>]}      ))
    , ?_assert(    eval(Hs, {'not_in', {'ident', <<"colour">>}, [<<"grue">>, <<"bleen">>]}  ))
    , ?_assert(    eval(Hs, {'not_like', {'ident', <<"altcol">>}, {<<"bl%">>, 'no_escape'}} ))
    , ?_assert(    eval(Hs, {'like', {'ident', <<"altcol">>}, {<<"'bl%">>, 'no_escape'}}    ))
    , ?_assert(    eval(Hs, {'or', {'and', {'like', {'ident', <<"colour">>}, {<<"bl%">>, 'no_escape'}}
                                         , {'>', {'ident', <<"weight">>}, 2500}}
                                 , false}                                                   ))
    , ?_assert(undefined =:= eval(Hs, {'<=', {'ident', <<"missing">>}, 2500}                ))
    , ?_assert(undefined =:= eval(Hs, {'in', {'ident', <<"missing">>}, [<<"blue">>]}        ))
    ].

eval(Hs, S) -> evaluate(S, Hs).

%% A selector with an operand of the wrong type must evaluate to `error`,
%% not raise `badarith`/`badarg`: the type of an `ident` operand is only
%% known once a message arrives, so this can't be rejected at bind time.
arithmetic_type_mismatch_test(_Config) ->
    ?assertEqual(error, evaluate({'+', <<"a">>, <<"b">>}, [])),
    ?assertEqual(error, evaluate({'-', <<"a">>, 1}, [])),
    ?assertEqual(error, evaluate({'*', 1, <<"a">>}, [])),
    ?assertEqual(error, evaluate({'/', 1, <<"a">>}, [])),
    ?assertEqual(error, evaluate({'-', <<"a">>}, [])),
    ?assertEqual(error, evaluate({'+', <<"a">>}, [])),
    ?assertEqual(error, evaluate({'-', true}, [])),

    Hs = [{<<"amount">>, longstr, <<"unknown">>}],
    ?assertEqual(error, eval(Hs, {'+', {'ident', <<"amount">>}, 1})),
    ?assertEqual(error, eval(Hs, {'>',     {'+', {'ident', <<"amount">>}, 1}, 100})),
    ?assertEqual(error, eval(Hs, {'>=',    {'+', {'ident', <<"amount">>}, 1}, 100})),
    ?assertEqual(error, eval(Hs, {'<>',    {'+', {'ident', <<"amount">>}, 1}, 100})),
    ?assertEqual(error, eval(Hs, {'not_in', {'+', {'ident', <<"amount">>}, 1}, [100]})).

%% A LIKE/NOT LIKE left-hand side is only a binary if the message header
%% actually is one; a numeric or boolean header must not raise `badarg`.
like_type_mismatch_test(_Config) ->
    Hs = [{<<"n">>, signedint, 5}, {<<"b">>, bool, true}, {<<"colour">>, longstr, <<"blue">>}],
    ?assertEqual(true,      eval(Hs, {'like', {'ident', <<"colour">>}, <<"bl%">>, no_escape})),
    ?assertEqual(error,     eval(Hs, {'like', {'ident', <<"n">>}, <<"5%">>, no_escape})),
    ?assertEqual(undefined, eval(Hs, {'not_like', {'ident', <<"n">>}, <<"5%">>, no_escape})),
    ?assertEqual(error,     eval(Hs, {'like', {'ident', <<"b">>}, regex, <<"true">>})).

%% `between`/`not_between` must propagate the `error` sentinel a
%% type-mismatched nested arithmetic operand already produces, the same
%% as `do_bin_op/3`, rather than fall through to a raw term comparison.
between_error_propagation_test(_Config) ->
    Hs = [{<<"amount">>, longstr, <<"unknown">>}],
    ?assertEqual(error,     eval(Hs, {'between',     {'+', {'ident', <<"amount">>}, 1}, 5, 10})),
    ?assertEqual(undefined, eval(Hs, {'not_between', {'+', {'ident', <<"amount">>}, 1}, 5, 10})).

%% Float arithmetic that would overflow to infinity raises `badarith`
%% in Erlang; a header value large enough is entirely publisher-chosen.
arithmetic_overflow_test(_Config) ->
    Hs = [{<<"p">>, double, 1.7e308}],
    ?assertEqual(error, eval(Hs, {'*', {'ident', <<"p">>}, 10.0})),
    ?assertEqual(error, eval(Hs, {'+', {'ident', <<"p">>}, {'ident', <<"p">>}})).

%% A header of an AMQP 0-9-1 field type this module doesn't model
%% (e.g. `timestamp`, `void`) must evaluate to `undefined`, like a
%% missing header, not raise `case_clause`.
lookup_value_unknown_type_test(_Config) ->
    Hs = [{<<"t">>, timestamp, 123}, {<<"v">>, void, undefined}, {<<"tbl">>, table, []}],
    ?assertEqual(undefined, eval(Hs, {'ident', <<"t">>})),
    ?assertEqual(undefined, eval(Hs, {'ident', <<"v">>})),
    ?assertEqual(undefined, eval(Hs, {'ident', <<"tbl">>})).

%% `IN`'s right-hand side is always parsed as a list; a non-list
%% (a hand-crafted selector) must not raise `function_clause`. `IN`
%% also has to agree with `=` on numeric equality regardless of the
%% exact int/float representation.
in_type_mismatch_test(_Config) ->
    Hs = [{<<"x">>, signedint, 5}, {<<"n">>, double, 1.0}],
    ?assertEqual(false, eval(Hs, {'in', {'ident', <<"x">>}, 5})),
    ?assertEqual(true,  eval(Hs, {'=', {'ident', <<"n">>}, 1})),
    ?assertEqual(true,  eval(Hs, {'in', {'ident', <<"n">>}, [1, 2, 3]})),
    ?assertEqual(false, eval(Hs, {'not_in', {'ident', <<"n">>}, [1, 2, 3]})).

%% `=`/`<>` only compare like-typed operands per the JMS spec; the
%% ordering operators and `between` only apply to numbers. Anything
%% else is `undefined`, not a raw cross-type Erlang term comparison
%% (which never raises, but can silently produce the wrong answer).
comparison_type_mismatch_test(_Config) ->
    Hs = [{<<"b">>, bool, true}],
    ?assertEqual(undefined, eval(Hs, {'<>', {'ident', <<"b">>}, 1})),
    ?assertEqual(undefined, eval(Hs, {'=',  {'ident', <<"b">>}, 1})),
    ?assertEqual(undefined, eval(Hs, {'>',  {'ident', <<"b">>}, 1})),
    ?assertEqual(undefined, eval(Hs, {'between', 7, 0, {'/', -1, 0}})).

%% A `{range, From, To}` third argument on `like`/`not_like` must not
%% reach `pattern_of/2` as a non-binary pattern, nor may a non-binary
%% `Patt` in the direct 4-tuple form reach `isLike/2`'s pattern clauses.
like_range_wrapped_pattern_test(_Config) ->
    ?assertEqual(error, eval([], {'like', <<"x">>, {range, 1, no_escape}})),
    ?assertEqual(error, eval([], {'not_like', <<"x">>, {range, 1, no_escape}})),
    ?assertEqual(error, eval([], {'like', <<"x">>, {range, regex, <<"(a)">>}})),
    ?assertEqual(error, eval([], {'like', <<"x">>, 1, no_escape})),
    %% `rabbit_re:compile/2`'s byte-size cap doesn't apply to a list.
    ?assertEqual(error, eval([], {'like', <<"x">>, regex, [<<"a">>]})).

%% `between`/`not_between` must still accept the `{range, From, To}`
%% form now that the rewrite is specific to those two operators.
between_range_form_test(_Config) ->
    ?assertEqual(true,  eval([], {'between', 5, {range, 1, 10}})),
    ?assertEqual(false, eval([], {'not_between', 5, {range, 1, 10}})).

%% Repeated multiplication must stay a bounded 64-bit integer
%% regardless of nesting depth, not grow into an ever-larger bignum.
arithmetic_bignum_overflow_test(_Config) ->
    Leaf = list_to_integer(lists:duplicate(100, $9)),
    Result = eval([], nest_multiply(14, Leaf)),
    ?assert(is_integer(Result)),
    ?assert(Result >= -9223372036854775808 andalso Result =< 9223372036854775807).

nest_multiply(0, Leaf) -> Leaf;
nest_multiply(N, Leaf) ->
    Half = nest_multiply(N - 1, Leaf),
    {'*', Half, Half}.

%% Mirrors Java `long` overflow; a float operand is unaffected.
arithmetic_long_wraparound_test(_Config) ->
    ?assertEqual(-9223372036854775808, eval([], {'+', 9223372036854775807, 1})),
    ?assertEqual(9223372036854775807,  eval([], {'-', -9223372036854775808, 1})),
    ?assertEqual(1,                    eval([], {'*', 9223372036854775807, 9223372036854775807})),
    ?assertEqual(-9223372036854775808, eval([], {'-', -9223372036854775808})),
    ?assertEqual(5,                    eval([], {'+', 2, 3})),
    ?assertEqual(5.5,                  eval([], {'+', 2.5, 3})).
