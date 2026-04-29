%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(server_named_queue_prefix_prop_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(NUM_TESTS, 1000).
-define(MAX_PREFIX_LEN, 64).

all() ->
    [
     prop_valid_prefix_returned_verbatim,
     prop_too_long_prefix_yields_error,
     prop_no_valid_prefix_yields_default,
     prop_generated_name_starts_with_prefix,
     prop_generated_name_within_amqp_limit,
     prop_different_guids_yield_different_names,
     prop_different_prefixes_yield_different_names
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

%% A non-empty prefix of at most 64 bytes is returned as {ok, Prefix}.
prop_valid_prefix_returned_verbatim(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_valid_prefix(Config) end, [], ?NUM_TESTS).

prop_valid_prefix(_Config) ->
    ?FORALL(Prefix, valid_prefix_gen(),
            begin
                Args = [{<<"x-name-prefix">>, longstr, Prefix}],
                rabbit_amqqueue:server_named_queue_prefix(Args) =:= {ok, Prefix}
            end).

%% A prefix longer than 64 bytes yields {error, {prefix_too_long, Prefix}}.
prop_too_long_prefix_yields_error(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_too_long_prefix(Config) end, [], ?NUM_TESTS).

prop_too_long_prefix(_Config) ->
    ?FORALL(Prefix, too_long_prefix_gen(),
            begin
                Args = [{<<"x-name-prefix">>, longstr, Prefix}],
                rabbit_amqqueue:server_named_queue_prefix(Args) =:=
                    {error, {prefix_too_long, Prefix}}
            end).

%% Absent, empty, or wrong-type `x-name-prefix` yields {ok, <<"amq.gen">>}.
prop_no_valid_prefix_yields_default(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_default_fallback(Config) end, [], ?NUM_TESTS).

prop_default_fallback(_Config) ->
    ?FORALL(Args, invalid_prefix_args_gen(),
            rabbit_amqqueue:server_named_queue_prefix(Args) =:= {ok, <<"amq.gen">>}).

%% rabbit_guid:binary/2 produces "prefix-<base64url>".
prop_generated_name_starts_with_prefix(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_name_starts_with_prefix(Config) end, [], ?NUM_TESTS).

prop_name_starts_with_prefix(_Config) ->
    ?FORALL({Prefix, Guid}, {valid_prefix_gen(), binary(16)},
            begin
                Generated = rabbit_guid:binary(Guid, Prefix),
                ExpectedHead = <<Prefix/binary, "-">>,
                HeadLen = byte_size(ExpectedHead),
                byte_size(Generated) >= HeadLen andalso
                    binary:part(Generated, 0, HeadLen) =:= ExpectedHead
            end).

%% The generated name fits within the 255-byte AMQP shortstr limit.
prop_generated_name_within_amqp_limit(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_name_within_limit(Config) end, [], ?NUM_TESTS).

prop_name_within_limit(_Config) ->
    ?FORALL({Prefix, Guid}, {valid_prefix_gen(), binary(16)},
            byte_size(rabbit_guid:binary(Guid, Prefix)) =< 255).

%% Two distinct GUIDs with the same prefix produce different names.
prop_different_guids_yield_different_names(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_unique_names(Config) end, [], ?NUM_TESTS).

prop_unique_names(_Config) ->
    ?FORALL({Prefix, G1, G2}, {valid_prefix_gen(), binary(16), binary(16)},
            G1 =:= G2 orelse
                rabbit_guid:binary(G1, Prefix) =/= rabbit_guid:binary(G2, Prefix)).

%% Two distinct prefixes with the same GUID produce different names.
prop_different_prefixes_yield_different_names(Config) ->
    rabbit_ct_proper_helpers:run_proper(
      fun() -> prop_unique_prefixes(Config) end, [], ?NUM_TESTS).

prop_unique_prefixes(_Config) ->
    ?FORALL({P1, P2, Guid}, {valid_prefix_gen(), valid_prefix_gen(), binary(16)},
            P1 =:= P2 orelse
                rabbit_guid:binary(Guid, P1) =/= rabbit_guid:binary(Guid, P2)).

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

valid_prefix_gen() ->
    ?SUCHTHAT(P, non_empty(binary()), byte_size(P) =< ?MAX_PREFIX_LEN).

too_long_prefix_gen() ->
    ?LET(Extra, binary(),
         <<(binary:copy(<<"x">>, ?MAX_PREFIX_LEN + 1))/binary, Extra/binary>>).

invalid_prefix_args_gen() ->
    oneof([
        %% no `x-name-prefix` at all
        list(unrelated_arg_gen()),
        %% empty binary
        ?LET(Extra, list(unrelated_arg_gen()),
             [{<<"x-name-prefix">>, longstr, <<>>} | Extra]),
        %% non-longstr type
        ?LET({Extra, Type},
             {list(unrelated_arg_gen()),
              oneof([signedint, bool, double, timestamp, byte, short, long,
                     float, binary, void])},
             [{<<"x-name-prefix">>, Type, 0} | Extra])
    ]).

unrelated_arg_gen() ->
    ?LET(Key, oneof([<<"x-queue-type">>, <<"x-max-length">>, <<"x-expires">>]),
         {Key, longstr, <<"classic">>}).
