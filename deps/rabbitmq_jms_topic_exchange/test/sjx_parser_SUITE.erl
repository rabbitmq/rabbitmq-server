%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%% -----------------------------------------------------------------------------

%% Tests for sjx_parser — safe Erlang term parser

%% -----------------------------------------------------------------------------
-module(sjx_parser_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          atoms_test,
          quoted_atoms_test,
          integers_test,
          floats_test,
          floats_malformed_test,
          binaries_test,
          binaries_with_escapes_test,
          strings_test,
          tuples_test,
          lists_test,
          trailing_dot_test,
          whitespace_test,
          selector_ident_test,
          selector_comparison_test,
          selector_logical_test,
          selector_null_check_test,
          selector_like_test,
          selector_between_test,
          selector_in_test,
          selector_arithmetic_test,
          selector_negative_float_in_tuple_test,
          selector_complex_test,
          rejects_unknown_atoms_test,
          rejects_disallowed_atoms_test,
          rejects_non_atom_identifiers_test,
          rejects_malformed_input_test,
          rejects_excessive_nesting_test
      ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->  Config.
end_per_suite(Config) ->   Config.
init_per_group(_, Config) ->  Config.
end_per_group(_, Config) ->   Config.
init_per_testcase(_, Config) ->  Config.
end_per_testcase(_, Config) ->   Config.

%% -------------------------------------------------------------------
%% Atoms
%% -------------------------------------------------------------------

atoms_test(_) ->
    ?assertEqual({ok, true},      sjx_parser:parse_term("true.")),
    ?assertEqual({ok, false},     sjx_parser:parse_term("false.")),
    ?assertEqual({ok, ident},     sjx_parser:parse_term("ident.")),
    ?assertEqual({ok, no_escape}, sjx_parser:parse_term("no_escape.")),
    ?assertEqual({ok, regex},     sjx_parser:parse_term("regex.")).

quoted_atoms_test(_) ->
    ?assertEqual({ok, '='},  sjx_parser:parse_term("'='.")),
    ?assertEqual({ok, '<>'}, sjx_parser:parse_term("'<>'.")),
    ?assertEqual({ok, '>='}, sjx_parser:parse_term("'>='.")),
    ?assertEqual({ok, '<='}, sjx_parser:parse_term("'<='.")),
    ?assertEqual({ok, '>'},  sjx_parser:parse_term("'>'.")),
    ?assertEqual({ok, '<'},  sjx_parser:parse_term("'<'.")),
    ?assertEqual({ok, '+'},  sjx_parser:parse_term("'+'.")),
    ?assertEqual({ok, '-'},  sjx_parser:parse_term("'-'.")),
    ?assertEqual({ok, '*'},  sjx_parser:parse_term("'*'.")),
    ?assertEqual({ok, '/'},  sjx_parser:parse_term("'/'.")),
    ?assertEqual({ok, in},   sjx_parser:parse_term("'in'.")).

%% -------------------------------------------------------------------
%% Numbers
%% -------------------------------------------------------------------

integers_test(_) ->
    ?assertEqual({ok, 0},    sjx_parser:parse_term("0.")),
    ?assertEqual({ok, 42},   sjx_parser:parse_term("42.")),
    ?assertEqual({ok, 2501}, sjx_parser:parse_term("2501.")),
    ?assertEqual({ok, -5},   sjx_parser:parse_term("-5.")),
    ?assertEqual({ok, 100},  sjx_parser:parse_term("+100.")).

floats_test(_) ->
    ?assertEqual({ok, 3.14},   sjx_parser:parse_term("3.14.")),
    ?assertEqual({ok, -1.5},   sjx_parser:parse_term("-1.5.")),
    ?assertEqual({ok, 0.001},  sjx_parser:parse_term("0.001.")),
    ?assertEqual({ok, 3.0e-2}, sjx_parser:parse_term("3.0e-2.")),
    ?assertEqual({ok, 1.5e10}, sjx_parser:parse_term("1.5e10.")),
    ?assertEqual({ok, 1.0e3},  sjx_parser:parse_term("1.0E+3.")).

floats_malformed_test(_) ->
    ?assertMatch({error, _}, sjx_parser:parse_term("1.0e.")),
    ?assertMatch({error, _}, sjx_parser:parse_term("1.0e+.")),
    ?assertMatch({error, _}, sjx_parser:parse_term("1.0e-.")),
    ?assertMatch({error, _}, sjx_parser:parse_term("9.9e999.")).

%% -------------------------------------------------------------------
%% Binaries
%% -------------------------------------------------------------------

binaries_test(_) ->
    ?assertEqual({ok, <<>>},        sjx_parser:parse_term("<<>>.")),
    ?assertEqual({ok, <<"hello">>}, sjx_parser:parse_term("<<\"hello\">>.")),
    ?assertEqual({ok, <<"a b">>},   sjx_parser:parse_term("<<\"a b\">>.")),
    ?assertEqual({ok, <<"bl%">>},   sjx_parser:parse_term("<<\"bl%\">>.")),
    ?assertEqual({ok, <<"!">>},     sjx_parser:parse_term("<<\"!\">>.")),
    ?assertEqual({ok, <<"false">>}, sjx_parser:parse_term("<<\"false\">>.")).

binaries_with_escapes_test(_) ->
    ?assertEqual({ok, <<"he\nllo">>}, sjx_parser:parse_term("<<\"he\\nllo\">>.")),
    ?assertEqual({ok, <<"a\\b">>},    sjx_parser:parse_term("<<\"a\\\\b\">>.")),
    ?assertEqual({ok, <<"q\"r">>},    sjx_parser:parse_term("<<\"q\\\"r\">>."  )).

%% -------------------------------------------------------------------
%% Strings (Erlang strings = lists of character codes)
%% -------------------------------------------------------------------

strings_test(_) ->
    ?assertEqual({ok, "hello"}, sjx_parser:parse_term("\"hello\".")),
    ?assertEqual({ok, ""},      sjx_parser:parse_term("\"\".")).

%% -------------------------------------------------------------------
%% Tuples
%% -------------------------------------------------------------------

tuples_test(_) ->
    ?assertEqual({ok, {}},          sjx_parser:parse_term("{}.")),
    ?assertEqual({ok, {true}},      sjx_parser:parse_term("{true}.")),
    ?assertEqual({ok, {1, 2, 3}},   sjx_parser:parse_term("{1, 2, 3}.")),
    ?assertEqual({ok, {<<"a">>, no_escape}},
                 sjx_parser:parse_term("{<<\"a\">>, no_escape}.")).

%% -------------------------------------------------------------------
%% Lists
%% -------------------------------------------------------------------

lists_test(_) ->
    ?assertEqual({ok, []},             sjx_parser:parse_term("[].")),
    ?assertEqual({ok, [1, 2, 3]},      sjx_parser:parse_term("[1, 2, 3].")),
    ?assertEqual({ok, [<<"a">>, <<"b">>]},
                 sjx_parser:parse_term("[<<\"a\">>, <<\"b\">>].")).

%% -------------------------------------------------------------------
%% Trailing dot and whitespace handling
%% -------------------------------------------------------------------

trailing_dot_test(_) ->
    ?assertEqual({ok, true}, sjx_parser:parse_term("true.")),
    ?assertEqual({ok, true}, sjx_parser:parse_term("true")).

whitespace_test(_) ->
    ?assertEqual({ok, {ident, <<"x">>}},
                 sjx_parser:parse_term("  { ident , << \"x\" >> }  .  ")).

%% -------------------------------------------------------------------
%% Real-world selector expressions
%% -------------------------------------------------------------------

selector_ident_test(_) ->
    ?assertEqual({ok, {ident, <<"boolVal">>}},
                 sjx_parser:parse_term("{ident, <<\"boolVal\">>}.")).

selector_comparison_test(_) ->
    ?assertEqual({ok, {'=', {ident, <<"colour">>}, <<"blue">>}},
                 sjx_parser:parse_term("{'=', {ident, <<\"colour\">>}, <<\"blue\">>}.")),
    ?assertEqual({ok, {'<>', {ident, <<"x">>}, 5}},
                 sjx_parser:parse_term("{'<>', {ident, <<\"x\">>}, 5}.")),
    ?assertEqual({ok, {'>', {ident, <<"weight">>}, 2500}},
                 sjx_parser:parse_term("{'>', {ident, <<\"weight\">>}, 2500}.")),
    ?assertEqual({ok, {'<=', {ident, <<"weight">>}, 2501}},
                 sjx_parser:parse_term("{'<=', {ident, <<\"weight\">>}, 2501}.")).

selector_logical_test(_) ->
    ?assertEqual({ok, {'and', true, false}},
                 sjx_parser:parse_term("{'and', true, false}.")),
    ?assertEqual({ok, {'or', true, false}},
                 sjx_parser:parse_term("{'or', true, false}.")),
    ?assertEqual({ok, {'not', {ident, <<"x">>}}},
                 sjx_parser:parse_term("{'not', {ident, <<\"x\">>}}.")).

selector_null_check_test(_) ->
    ?assertEqual({ok, {is_null, {ident, <<"x">>}}},
                 sjx_parser:parse_term("{is_null, {ident, <<\"x\">>}}.")),
    ?assertEqual({ok, {not_null, {ident, <<"x">>}}},
                 sjx_parser:parse_term("{not_null, {ident, <<\"x\">>}}.")).

selector_like_test(_) ->
    ?assertEqual({ok, {'like', {ident, <<"colour">>}, <<"bl%">>, no_escape}},
                 sjx_parser:parse_term("{'like', {ident, <<\"colour\">>}, <<\"bl%\">>, no_escape}.")),
    ?assertEqual({ok, {'not_like', {ident, <<"colour">>}, <<"l%">>, no_escape}},
                 sjx_parser:parse_term("{'not_like', {ident, <<\"colour\">>}, <<\"l%\">>, no_escape}.")),
    ?assertEqual({ok, {'like', {ident, <<"x">>}, <<"b_!_ue">>, <<"!">>}},
                 sjx_parser:parse_term("{'like', {ident, <<\"x\">>}, <<\"b_!_ue\">>, <<\"!\">>}.")).

selector_between_test(_) ->
    ?assertEqual({ok, {'between', {ident, <<"weight">>}, {range, 0, 2501}}},
                 sjx_parser:parse_term("{'between', {ident, <<\"weight\">>}, {range, 0, 2501}}.")),
    ?assertEqual({ok, {'not_between', 16, {range, 17, 18}}},
                 sjx_parser:parse_term("{'not_between', 16, {range, 17, 18}}.")).

selector_in_test(_) ->
    ?assertEqual({ok, {'in', {ident, <<"colour">>}, [<<"blue">>, <<"green">>]}},
                 sjx_parser:parse_term("{'in', {ident, <<\"colour\">>}, [<<\"blue\">>, <<\"green\">>]}.")),
    ?assertEqual({ok, {'not_in', {ident, <<"colour">>}, [<<"grue">>]}},
                 sjx_parser:parse_term("{'not_in', {ident, <<\"colour\">>}, [<<\"grue\">>]}.")).

selector_arithmetic_test(_) ->
    ?assertEqual({ok, {'+', {ident, <<"x">>}, 1}},
                 sjx_parser:parse_term("{'+', {ident, <<\"x\">>}, 1}.")),
    ?assertEqual({ok, {'-', {ident, <<"x">>}}},
                 sjx_parser:parse_term("{'-', {ident, <<\"x\">>}}.")).

selector_negative_float_in_tuple_test(_) ->
    ?assertEqual({ok, {'>', {ident, <<"x">>}, -3.14}},
                 sjx_parser:parse_term("{'>', {ident, <<\"x\">>}, -3.14}.")),
    ?assertEqual({ok, {'between', {ident, <<"t">>}, {range, -1.5, 1.5}}},
                 sjx_parser:parse_term("{'between', {ident, <<\"t\">>}, {range, -1.5, 1.5}}.")).

selector_complex_test(_) ->
    Input = "{'or', {'and', {'like', {ident, <<\"colour\">>}, {<<\"bl%\">>, no_escape}}"
            ", {'>', {ident, <<\"weight\">>}, 2500}}"
            ", false}.",
    Expected = {'or',
                {'and',
                 {'like', {ident, <<"colour">>}, {<<"bl%">>, no_escape}},
                 {'>', {ident, <<"weight">>}, 2500}},
                false},
    ?assertEqual({ok, Expected}, sjx_parser:parse_term(Input)).

%% -------------------------------------------------------------------
%% Rejection tests
%% -------------------------------------------------------------------

rejects_unknown_atoms_test(_) ->
    ?assertMatch({error, {unknown_atom, _}},
                 sjx_parser:parse_term("{xq7fkd9_3jrvm, 42}.")),
    ?assertMatch({error, {unknown_atom, _}},
                 sjx_parser:parse_term("'zk4m_w8ntp2v'.")).

rejects_disallowed_atoms_test(_) ->
    ?assertMatch({error, {disallowed_atom, _}},
                 sjx_parser:parse_term("erlang.")),
    ?assertMatch({error, {disallowed_atom, _}},
                 sjx_parser:parse_term("'apply'.")).

rejects_non_atom_identifiers_test(_) ->
    ?assertMatch({error, _}, sjx_parser:parse_term("True.")),
    ?assertMatch({error, _}, sjx_parser:parse_term("_foo.")),
    ?assertMatch({error, _}, sjx_parser:parse_term("$a.")),
    ?assertMatch({error, _}, sjx_parser:parse_term("true..")).

rejects_malformed_input_test(_) ->
    ?assertMatch({error, _}, sjx_parser:parse_term("")),
    ?assertMatch({error, _}, sjx_parser:parse_term("{")),
    ?assertMatch({error, _}, sjx_parser:parse_term("}")),
    ?assertMatch({error, _}, sjx_parser:parse_term("{1, ")),
    ?assertMatch({error, _}, sjx_parser:parse_term("<<")),
    ?assertMatch({error, _}, sjx_parser:parse_term("<<\"unterminated")),
    ?assertMatch({error, _}, sjx_parser:parse_term("'unterminated")),
    ?assertMatch({error, _}, sjx_parser:parse_term("\"unterminated")),
    ?assertMatch({error, _}, sjx_parser:parse_term("1 2")).

rejects_excessive_nesting_test(_) ->
    %% 16 levels of nesting must succeed
    Depth16 = nest_tuples(16, "1"),
    ?assertMatch({ok, _}, sjx_parser:parse_term(Depth16)),
    %% 17 levels must be rejected
    Depth17 = nest_tuples(17, "1"),
    ?assertMatch({error, max_depth_exceeded}, sjx_parser:parse_term(Depth17)),
    %% Lists count as nesting too
    ListDepth17 = nest_lists(17, "1"),
    ?assertMatch({error, max_depth_exceeded}, sjx_parser:parse_term(ListDepth17)).

nest_tuples(0, Inner) -> Inner;
nest_tuples(N, Inner) -> "{" ++ nest_tuples(N - 1, Inner) ++ "}".

nest_lists(0, Inner) -> Inner;
nest_lists(N, Inner) -> "[" ++ nest_lists(N - 1, Inner) ++ "]".
