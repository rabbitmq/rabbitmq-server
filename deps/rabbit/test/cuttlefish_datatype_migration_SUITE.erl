%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(cuttlefish_datatype_migration_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ITERATIONS, 500).

all() ->
    [
     {group, boolean_datatype},
     {group, byte_datatype},
     {group, integer_positive},
     {group, integer_non_negative},
     {group, integer_16_bit_range},
     {group, integer_32_bit_range},
     {group, float_open_unit_interval},
     {group, equivalence_with_pre_3_8_validators},
     {group, uri_stub_validator}
    ].

groups() ->
    [
     {boolean_datatype, [parallel],
      [boolean_accepts_true_and_false,
       boolean_rejects_other_atoms,
       prop_boolean_round_trip]},
     {byte_datatype, [parallel],
      [byte_accepts_min_and_max,
       byte_rejects_out_of_range,
       prop_byte_accepts_in_range,
       prop_byte_rejects_outside_range]},
     {integer_positive, [parallel],
      [positive_accepts_one,
       positive_rejects_zero_and_negatives,
       prop_positive_accepts_positives,
       prop_positive_rejects_zero_and_below]},
     {integer_non_negative, [parallel],
      [non_negative_accepts_zero,
       non_negative_rejects_negatives,
       prop_non_negative_accepts_zero_or_more,
       prop_non_negative_rejects_negatives]},
     {integer_16_bit_range, [parallel],
      [range_16_bit_accepts_endpoints,
       range_16_bit_rejects_just_outside,
       prop_range_16_bit_accepts_in_range,
       prop_range_16_bit_rejects_outside_range]},
     {integer_32_bit_range, [parallel],
      [range_32_bit_accepts_endpoints,
       range_32_bit_rejects_just_outside,
       prop_range_32_bit_accepts_in_range,
       prop_range_32_bit_rejects_below_min]},
     {float_open_unit_interval, [parallel],
      [float_open_unit_accepts_midpoint,
       float_open_unit_rejects_endpoints]},
     {equivalence_with_pre_3_8_validators, [parallel],
      [prop_boolean_matches_legacy_enum,
       prop_byte_matches_legacy_validator,
       prop_positive_matches_legacy_validator,
       prop_non_negative_matches_legacy_validator]},
     {uri_stub_validator, [parallel],
      [uri_stub_accepts_any_string,
       prop_uri_stub_accepts_arbitrary_strings]}
    ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

boolean_accepts_true_and_false(_Config) ->
    ?assertEqual(true,  cuttlefish_datatypes:from_string("true",  boolean)),
    ?assertEqual(false, cuttlefish_datatypes:from_string("false", boolean)),
    ?assertEqual(true,  cuttlefish_datatypes:from_string(true,    boolean)),
    ?assertEqual(false, cuttlefish_datatypes:from_string(false,   boolean)).

boolean_rejects_other_atoms(_Config) ->
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("yes",  boolean)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("on",   boolean)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("",     boolean)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("True", boolean)).

prop_boolean_round_trip(_Config) ->
    run_property(
      ?FORALL(B, boolean(),
              cuttlefish_datatypes:from_string(atom_to_list(B), boolean) =:= B)).

byte_accepts_min_and_max(_Config) ->
    ?assertEqual(0,   cuttlefish_datatypes:from_string("0",   byte)),
    ?assertEqual(255, cuttlefish_datatypes:from_string("255", byte)).

byte_rejects_out_of_range(_Config) ->
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("-1",  byte)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("256", byte)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("999", byte)).

prop_byte_accepts_in_range(_Config) ->
    run_property(
      ?FORALL(N, choose(0, 255),
              cuttlefish_datatypes:from_string(integer_to_list(N), byte) =:= N)).

prop_byte_rejects_outside_range(_Config) ->
    run_property(
      ?FORALL(N, ?SUCHTHAT(I, integer(), I < 0 orelse I > 255),
              is_reject(cuttlefish_datatypes:from_string(integer_to_list(N), byte)))).

positive_accepts_one(_Config) ->
    ?assertEqual(1,  cuttlefish_datatypes:from_string("1",  {integer, positive})),
    ?assertEqual(42, cuttlefish_datatypes:from_string("42", {integer, positive})).

positive_rejects_zero_and_negatives(_Config) ->
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("0",  {integer, positive})),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("-1", {integer, positive})).

prop_positive_accepts_positives(_Config) ->
    run_property(
      ?FORALL(N, pos_integer(),
              cuttlefish_datatypes:from_string(integer_to_list(N), {integer, positive}) =:= N)).

prop_positive_rejects_zero_and_below(_Config) ->
    run_property(
      ?FORALL(N, ?SUCHTHAT(I, integer(), I =< 0),
              is_reject(cuttlefish_datatypes:from_string(integer_to_list(N), {integer, positive})))).

non_negative_accepts_zero(_Config) ->
    ?assertEqual(0, cuttlefish_datatypes:from_string("0", {integer, non_negative})),
    ?assertEqual(1, cuttlefish_datatypes:from_string("1", {integer, non_negative})).

non_negative_rejects_negatives(_Config) ->
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("-1",  {integer, non_negative})),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("-99", {integer, non_negative})).

prop_non_negative_accepts_zero_or_more(_Config) ->
    run_property(
      ?FORALL(N, non_neg_integer(),
              cuttlefish_datatypes:from_string(integer_to_list(N), {integer, non_negative}) =:= N)).

prop_non_negative_rejects_negatives(_Config) ->
    run_property(
      ?FORALL(N, ?SUCHTHAT(I, integer(), I < 0),
              is_reject(cuttlefish_datatypes:from_string(integer_to_list(N), {integer, non_negative})))).

range_16_bit_accepts_endpoints(_Config) ->
    DT = {integer, [{min, 1}, {max, 65535}]},
    ?assertEqual(1,     cuttlefish_datatypes:from_string("1",     DT)),
    ?assertEqual(65535, cuttlefish_datatypes:from_string("65535", DT)).

range_16_bit_rejects_just_outside(_Config) ->
    DT = {integer, [{min, 1}, {max, 65535}]},
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("0",     DT)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("65536", DT)).

prop_range_16_bit_accepts_in_range(_Config) ->
    DT = {integer, [{min, 1}, {max, 65535}]},
    run_property(
      ?FORALL(N, choose(1, 65535),
              cuttlefish_datatypes:from_string(integer_to_list(N), DT) =:= N)).

prop_range_16_bit_rejects_outside_range(_Config) ->
    DT = {integer, [{min, 1}, {max, 65535}]},
    run_property(
      ?FORALL(N, ?SUCHTHAT(I, integer(), I < 1 orelse I > 65535),
              is_reject(cuttlefish_datatypes:from_string(integer_to_list(N), DT)))).

range_32_bit_accepts_endpoints(_Config) ->
    DT = {integer, [{min, 1}, {max, 4294967295}]},
    ?assertEqual(1,          cuttlefish_datatypes:from_string("1",          DT)),
    ?assertEqual(4294967295, cuttlefish_datatypes:from_string("4294967295", DT)).

range_32_bit_rejects_just_outside(_Config) ->
    DT = {integer, [{min, 1}, {max, 4294967295}]},
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("0",          DT)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("4294967296", DT)).

prop_range_32_bit_accepts_in_range(_Config) ->
    DT = {integer, [{min, 1}, {max, 4294967295}]},
    run_property(
      ?FORALL(N, choose(1, 4294967295),
              cuttlefish_datatypes:from_string(integer_to_list(N), DT) =:= N)).

prop_range_32_bit_rejects_below_min(_Config) ->
    DT = {integer, [{min, 1}, {max, 4294967295}]},
    run_property(
      ?FORALL(N, ?SUCHTHAT(I, integer(), I =< 0),
              is_reject(cuttlefish_datatypes:from_string(integer_to_list(N), DT)))).

float_open_unit_accepts_midpoint(_Config) ->
    DT = {float, [{gt, 0}, {lt, 1}]},
    ?assertEqual(0.5,  cuttlefish_datatypes:from_string("0.5",  DT)),
    ?assertEqual(0.99, cuttlefish_datatypes:from_string("0.99", DT)),
    ?assertEqual(0.01, cuttlefish_datatypes:from_string("0.01", DT)).

float_open_unit_rejects_endpoints(_Config) ->
    DT = {float, [{gt, 0}, {lt, 1}]},
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("0.0",  DT)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("1.0",  DT)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("1.5",  DT)),
    ?assertMatch({error, _}, cuttlefish_datatypes:from_string("-0.5", DT)).

prop_boolean_matches_legacy_enum(_Config) ->
    run_property(
      ?FORALL(S, oneof(["true", "false", "yes", "no", "1", "0", ""]),
              normalise(cuttlefish_datatypes:from_string(S, boolean))
                  =:= normalise(cuttlefish_datatypes:from_string(S, {enum, [true, false]})))).

prop_byte_matches_legacy_validator(_Config) ->
    Legacy = fun(N) when is_integer(N) -> N >= 0 andalso N =< 255 end,
    run_property(
      ?FORALL(N, integer(),
              legacy_matches_new(N, byte, Legacy))).

prop_positive_matches_legacy_validator(_Config) ->
    Legacy = fun(N) when is_integer(N) -> N >= 1 end,
    run_property(
      ?FORALL(N, integer(),
              legacy_matches_new(N, {integer, positive}, Legacy))).

prop_non_negative_matches_legacy_validator(_Config) ->
    %% Bare-integer call sites can never reach the legacy validator's
    %% 'infinity' branch, so the equivalence check is integer-only.
    Legacy = fun(N) when is_integer(N) -> N >= 0 end,
    run_property(
      ?FORALL(N, integer(),
              legacy_matches_new(N, {integer, non_negative}, Legacy))).

uri_stub_accepts_any_string(_Config) ->
    Stub = uri_stub(),
    ?assert(Stub("https://example.com")),
    ?assert(Stub("/auth")),
    ?assert(Stub("")),
    ?assert(Stub("not a uri at all")).

prop_uri_stub_accepts_arbitrary_strings(_Config) ->
    Stub = uri_stub(),
    run_property(?FORALL(S, list(byte()), Stub(S))).

run_property(Prop) ->
    rabbit_ct_proper_helpers:run_proper(fun() -> Prop end, [], ?ITERATIONS).

legacy_matches_new(N, NewDatatype, Legacy) ->
    LegacyAccept = Legacy(N),
    NewResult = cuttlefish_datatypes:from_string(integer_to_list(N), NewDatatype),
    NewAccept = is_integer(NewResult),
    LegacyAccept =:= NewAccept andalso
        (not NewAccept orelse NewResult =:= N).

is_reject({error, _}) -> true;
is_reject(_)          -> false.

%% Error shapes differ between datatypes (enum returns {enum_name, _},
%% boolean returns {conversion, _}); accept/reject equivalence is what
%% matters here, not the error tag.
normalise({error, _}) -> error;
normalise(Value)      -> Value.

uri_stub() ->
    fun(_Uri) -> true end.
