%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(prop_stream_arg_validation_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

-define(ITERATIONS, 1000).

all() ->
    [
     prop_valid_max_age,
     prop_invalid_max_age_no_leading_digits,
     prop_invalid_max_age_invalid_unit
    ].

%% -------------------------------------------------------------------
%% Suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Properties: rabbit_amqqueue:check_max_age/1.
%% Pure function; no broker required.
%% -------------------------------------------------------------------

prop_valid_max_age(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_valid_max_age_body/0, [], ?ITERATIONS).

prop_valid_max_age_body() ->
    ValidUnits = ["Y", "M", "D", "h", "m", "s"],
    ?FORALL({N, Unit}, {pos_integer(), elements(ValidUnits)},
        begin
            MaxAge = list_to_binary(integer_to_list(N) ++ Unit),
            Result = rabbit_amqqueue:check_max_age(MaxAge),
            is_integer(Result) andalso Result > 0
        end).

prop_invalid_max_age_no_leading_digits(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_invalid_max_age_no_leading_digits_body/0, [], ?ITERATIONS).

prop_invalid_max_age_no_leading_digits_body() ->
    %% ASCII digits occupy 48..57; any other first byte means no leading digits.
    NonDigitByte = ?SUCHTHAT(C, byte(), C < $0 orelse C > $9),
    ?FORALL({First, Rest}, {NonDigitByte, binary()},
        begin
            MaxAge = <<First, Rest/binary>>,
            {error, invalid_max_age} =:= rabbit_amqqueue:check_max_age(MaxAge)
        end).

prop_invalid_max_age_invalid_unit(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun prop_invalid_max_age_invalid_unit_body/0, [], ?ITERATIONS).

prop_invalid_max_age_invalid_unit_body() ->
    ValidUnits = ["Y", "M", "D", "h", "m", "s"],
    BadUnitChar = ?SUCHTHAT(C, choose($A, $z), not lists:member([C], ValidUnits)),
    ?FORALL({N, UnitChar}, {pos_integer(), BadUnitChar},
        begin
            MaxAge = list_to_binary(integer_to_list(N) ++ [UnitChar]),
            {error, invalid_max_age} =:= rabbit_amqqueue:check_max_age(MaxAge)
        end).
