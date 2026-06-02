%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(login_redirect_target_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include("rabbit_mgmt.hrl").

-compile([export_all, nowarn_export_all]).

-define(NUM_TESTS, 256).

all() ->
    [{group, units}, {group, properties}].

groups() ->
    [{units, [parallel],
      [known_code_passes_through,
       default_code_is_in_known_set,
       unknown_binary_collapses_to_default,
       atom_known_code,
       atom_unknown_collapses,
       string_known_code,
       string_unknown_collapses,
       string_with_invalid_utf8_collapses,
       improper_list_collapses,
       bitstring_collapses,
       arbitrary_term_collapses,
       sanitise_passes_through_clean_location,
       sanitise_passes_through_empty_location,
       sanitise_passes_through_location_at_cap,
       sanitise_rejects_location_one_byte_over_cap,
       sanitise_rejects_carriage_return,
       sanitise_rejects_line_feed,
       sanitise_rejects_crlf_pair,
       sanitise_caps_overlong_input]},
     {properties, [parallel],
      [canonical_error_is_in_set_prop,
       sanitise_never_emits_crlf_prop,
       sanitise_output_bounded_prop]}].

%% Tests

known_code_passes_through(_Config) ->
    [eq(C, canon(C)) || C <- ?LOGIN_ERROR_CODES],
    ok.

default_code_is_in_known_set(_Config) ->
    %% Without this invariant the default would map to itself via the unknown
    %% branch, defeating the property `canonical_error_is_in_set`.
    true = lists:member(?DEFAULT_LOGIN_ERROR_CODE, ?LOGIN_ERROR_CODES),
    ok.

unknown_binary_collapses_to_default(_Config) ->
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(<<"<script>alert(1)</script>">>)),
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(<<"foo&admin=true">>)),
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(<<"foo#frag">>)),
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(<<>>)),
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(binary:copy(<<"a">>, 100_000))).

atom_known_code(_Config) ->
    eq(<<"not_authorised">>, canon(not_authorised)),
    eq(<<"token_invalid">>,  canon(token_invalid)),
    eq(<<"Not_Authorized">>, canon('Not_Authorized')).

atom_unknown_collapses(_Config) ->
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(some_unknown_atom_xyz)).

string_known_code(_Config) ->
    eq(<<"invalid_credentials">>, canon("invalid_credentials")),
    %% This token is what `rabbit_web_dispatch_access_control` returns on a
    %% failed access-token check and is asserted by the OAuth Selenium suite.
    eq(<<"Not_Authorized">>, canon("Not_Authorized")).

string_unknown_collapses(_Config) ->
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon("anything else")).

string_with_invalid_utf8_collapses(_Config) ->
    %% A codepoint above 16#10FFFF is not a valid Unicode scalar; the helper
    %% must not crash and must default safely.
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon([16#FFFF_FFFF])).

improper_list_collapses(_Config) ->
    %% `unicode:characters_to_binary/1` throws `badarg` on improper lists.
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon([1 | 2])).

bitstring_collapses(_Config) ->
    %% Non-byte-aligned bitstrings do not match the `is_binary/1` clause.
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(<<1:1>>)).

arbitrary_term_collapses(_Config) ->
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon({tuple, value})),
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(#{a => 1})),
    eq(?DEFAULT_LOGIN_ERROR_CODE, canon(42)).

sanitise_passes_through_clean_location(_Config) ->
    Loc = <<"/?error=not_authorised">>,
    eq(Loc, san(Loc)).

sanitise_passes_through_empty_location(_Config) ->
    eq(<<>>, san(<<>>)).

sanitise_passes_through_location_at_cap(_Config) ->
    AtCap = binary:copy(<<"a">>, 1024),
    eq(AtCap, san(AtCap)).

sanitise_rejects_location_one_byte_over_cap(_Config) ->
    OverCap = binary:copy(<<"a">>, 1025),
    eq(<<"/">>, san(OverCap)).

sanitise_rejects_carriage_return(_Config) ->
    eq(<<"/">>, san(<<"/?error=x\rinjected">>)).

sanitise_rejects_line_feed(_Config) ->
    eq(<<"/">>, san(<<"/?error=x\ninjected">>)).

sanitise_rejects_crlf_pair(_Config) ->
    eq(<<"/">>, san(<<"/?error=x\r\nSet-Cookie: a=b">>)).

sanitise_caps_overlong_input(_Config) ->
    Big = binary:copy(<<"a">>, 2000),
    eq(<<"/">>, san(Big)).

%% Properties

canonical_error_is_in_set_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun canonical_error_is_in_set/0, [], ?NUM_TESTS).

sanitise_never_emits_crlf_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun sanitise_never_emits_crlf/0, [], ?NUM_TESTS).

sanitise_output_bounded_prop(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun sanitise_output_bounded/0, [], ?NUM_TESTS).

canonical_error_is_in_set() ->
    ?FORALL(Reason, gen_reason(),
            lists:member(canon(Reason), ?LOGIN_ERROR_CODES)).

sanitise_never_emits_crlf() ->
    ?FORALL(Loc, gen_location(),
            nomatch =:= binary:match(san(Loc), [<<"\r">>, <<"\n">>])).

sanitise_output_bounded() ->
    ?FORALL(Loc, gen_location(),
            byte_size(san(Loc)) =< 1024).

%% Generators

gen_reason() ->
    oneof([gen_known_code(),
           gen_random_binary(),
           gen_random_atom(),
           gen_random_charlist(),
           gen_arbitrary_term()]).

gen_known_code() ->
    oneof(?LOGIN_ERROR_CODES).

gen_random_binary() ->
    binary().

gen_random_atom() ->
    elements([not_authorised, token_invalid, oops, undefined,
              some_unknown_atom_xyz]).

gen_random_charlist() ->
    list(integer(0, 1114111)).

gen_arbitrary_term() ->
    oneof([integer(),
           float(),
           {term, integer()},
           #{key => integer()}]).

gen_location() ->
    %% Include CR and LF in the byte alphabet so the property exercises
    %% the scrub branch frequently.
    ?LET(L,
         list(oneof([integer(0, 127), $\r, $\n])),
         list_to_binary(L)).

%% Helpers

canon(R) ->
    rabbit_mgmt_login:canonical_login_error(R).

san(B) ->
    rabbit_mgmt_login:sanitise_location(B).

eq(Expected, Actual) ->
    case Actual of
        Expected -> ok;
        _        -> ct:fail({expected, Expected, got, Actual})
    end.
