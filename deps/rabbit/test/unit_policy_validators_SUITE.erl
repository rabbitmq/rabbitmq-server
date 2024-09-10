%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_policy_validators_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
      {group, core_validators}
    ].

groups() ->
    [
      {core_validators, [parallel], [
          alternate_exchange,
          dead_letter_exchange,
          dead_letter_routing_key,
          dead_letter_strategy,
          message_ttl,
          expires,
          max_length,
          max_length_bytes,
          max_in_memory_length,
          delivery_limit,
          classic_queue_lazy_mode,
          length_limit_overflow_mode
        ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Core Validators
%% -------------------------------------------------------------------

alternate_exchange(_Config) ->
    requires_binary_value(<<"alternate-exchange">>).

dead_letter_exchange(_Config) ->
    requires_binary_value(<<"dead-letter-exchange">>).

dead_letter_routing_key(_Config) ->
    requires_binary_value(<<"dead-letter-routing-key">>).

dead_letter_strategy(_Config) ->
    test_valid_and_invalid_values(<<"dead-letter-strategy">>,
        %% valid values
        [<<"at-most-once">>, <<"at-least-once">>],
        %% invalid values
        [<<"unknown">>, <<"dead-letter-strategy">>, <<"undefined">>]).

message_ttl(_Config) ->
    requires_non_negative_integer_value(<<"message-ttl">>).

expires(_Config) ->
    requires_positive_integer_value(<<"expires">>).

max_length(_Config) ->
    requires_non_negative_integer_value(<<"max-length">>).

max_length_bytes(_Config) ->
    requires_non_negative_integer_value(<<"max-length-bytes">>).

max_in_memory_length(_Config) ->
    requires_non_negative_integer_value(<<"max-in-memory-bytes">>).

delivery_limit(_Config) ->
    requires_integer_value(<<"delivery-limit">>).

classic_queue_lazy_mode(_Config) ->
    test_valid_and_invalid_values(<<"queue-mode">>,
        %% valid values
        [<<"default">>, <<"lazy">>],
        %% invalid values
        [<<"unknown">>, <<"queue">>, <<"mode">>]).

length_limit_overflow_mode(_Config) ->
    test_valid_and_invalid_values(<<"overflow">>,
        %% valid values
        [<<"drop-head">>, <<"reject-publish">>, <<"reject-publish-dlx">>],
        %% invalid values
        [<<"unknown">>, <<"publish">>, <<"overflow">>, <<"mode">>]).

%%
%% Implementation
%%

test_valid_and_invalid_values(Mod, Key, ValidValues, InvalidValues) ->
    [begin
         ?assertEqual(ok, Mod:validate_policy([
            {Key, Val}
         ]))
     end || Val <- ValidValues],
    [begin
         ?assertMatch({error, _, _}, Mod:validate_policy([
            {Key, Val}
         ]))
     end || Val <- InvalidValues].

test_valid_and_invalid_values(Key, ValidValues, InvalidValues) ->
    test_valid_and_invalid_values(rabbit_policies, Key, ValidValues, InvalidValues).

requires_binary_value(Key) ->
    test_valid_and_invalid_values(Key,
        [<<"a.binary">>, <<"b.binary">>],
        [1, rabbit]).

requires_positive_integer_value(Key) ->
    test_valid_and_invalid_values(Key,
        [1, 1000],
        [0, -1, <<"a.binary">>]).

requires_non_negative_integer_value(Key) ->
    test_valid_and_invalid_values(Key,
        [0, 1, 1000],
        [-1000, -1, <<"a.binary">>]).

requires_integer_value(Key) ->
    test_valid_and_invalid_values(Key,
        [-1, 0, 1, 1000, -10000],
        [<<"a.binary">>, 0.1]).
