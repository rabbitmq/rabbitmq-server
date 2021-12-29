%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_access_control_credential_validation_SUITE).

-compile(export_all).
-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, unit},
     {group, integration}
    ].

groups() ->
    [
     {integration, [], [
                        min_length_integration_fails
                        , regexp_integration_fails
                        , min_length_integration_succeeds
                        , regexp_integration_succeeds
                        , min_length_change_password_integration_fails
                        , regexp_change_password_integration_fails
                        , min_length_change_password_integration_succeeds
                        , regexp_change_password_integration_succeeds
                       ]},
     {unit, [parallel], [
                         basic_unconditionally_accepting_succeeds,
                         min_length_fails,
                         min_length_succeeds,
                         min_length_proper_fails,
                         min_length_proper_succeeds,
                         regexp_fails,
                         regexp_succeeds,
                         regexp_proper_fails,
                         regexp_proper_succeeds
                        ]}
].

suite() ->
    [
      {timetrap, {minutes, 4}}
    ].

%%
%% Setup/teardown
%%

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(integration, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 1},
        {rmq_nodename_suffix, Suffix}
    ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps());

init_per_group(unit, Config) ->
    Config.

end_per_group(integration, Config) ->
    rabbit_ct_broker_helpers:switch_credential_validator(Config, accept_everything),
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps());
end_per_group(unit, Config) ->
    Config.

-define(USERNAME, <<"abc">>).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%%
%% Test Cases
%%

basic_unconditionally_accepting_succeeds(_Config) ->
    F = fun rabbit_credential_validator_accept_everything:validate/2,

    Pwd1 = crypto:strong_rand_bytes(1),
    ?assertEqual(ok, F(?USERNAME, Pwd1)),
    Pwd2 = crypto:strong_rand_bytes(5),
    ?assertEqual(ok, F(?USERNAME, Pwd2)),
    Pwd3 = crypto:strong_rand_bytes(10),
    ?assertEqual(ok, F(?USERNAME, Pwd3)),
    Pwd4 = crypto:strong_rand_bytes(50),
    ?assertEqual(ok, F(?USERNAME, Pwd4)),
    Pwd5 = crypto:strong_rand_bytes(100),
    ?assertEqual(ok, F(?USERNAME, Pwd5)),
    Pwd6 = crypto:strong_rand_bytes(1000),
    ?assertEqual(ok, F(?USERNAME, Pwd6)).

min_length_fails(_Config) ->
    F = fun rabbit_credential_validator_min_password_length:validate/3,

    Pwd1 = crypto:strong_rand_bytes(1),
    ?assertMatch({error, _}, F(?USERNAME, Pwd1, 5)),
    Pwd2 = crypto:strong_rand_bytes(5),
    ?assertMatch({error, _}, F(?USERNAME, Pwd2, 6)),
    Pwd3 = crypto:strong_rand_bytes(10),
    ?assertMatch({error, _}, F(?USERNAME, Pwd3, 15)),
    Pwd4 = crypto:strong_rand_bytes(50),
    ?assertMatch({error, _}, F(?USERNAME, Pwd4, 60)),
    Pwd5 = undefined,
    ?assertMatch({error, _}, F(?USERNAME, Pwd5, 60)),
    Pwd6 = <<"">>,
    ?assertMatch({error, _}, F(?USERNAME, Pwd6, 60)).

min_length_succeeds(_Config) ->
    F = fun rabbit_credential_validator_min_password_length:validate/3,

    ?assertEqual(ok, F(?USERNAME, crypto:strong_rand_bytes(1), 1)),
    ?assertEqual(ok, F(?USERNAME, crypto:strong_rand_bytes(6), 6)),
    ?assertEqual(ok, F(?USERNAME, crypto:strong_rand_bytes(7), 6)),
    ?assertEqual(ok, F(?USERNAME, crypto:strong_rand_bytes(20), 20)),
    ?assertEqual(ok, F(?USERNAME, crypto:strong_rand_bytes(40), 30)),
    ?assertEqual(ok, F(?USERNAME, crypto:strong_rand_bytes(50), 50)).

min_length_proper_fails(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_min_length_fails_validation/0, [], 500).

min_length_proper_succeeds(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_min_length_passes_validation/0, [], 500).

regexp_fails(_Config) ->
    F = fun rabbit_credential_validator_password_regexp:validate/3,

    ?assertMatch({error, _}, F(?USERNAME, <<"abc">>,    "^xyz")),
    ?assertMatch({error, _}, F(?USERNAME, <<"abcdef">>, "^xyz")),
    ?assertMatch({error, _}, F(?USERNAME, <<"abcxyz">>, "^abc\\d+")).

regexp_succeeds(_Config) ->
    F = fun rabbit_credential_validator_password_regexp:validate/3,

    ?assertEqual(ok, F(?USERNAME, <<"abc">>,    "^abc")),
    ?assertEqual(ok, F(?USERNAME, <<"abcdef">>, "^abc")),
    ?assertEqual(ok, F(?USERNAME, <<"abc123">>, "^abc\\d+")).

regexp_proper_fails(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_regexp_fails_validation/0, [], 500).

regexp_proper_succeeds(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_regexp_passes_validation/0, [], 500).

min_length_integration_fails(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, min_length, 50),
    ?assertMatch(rabbit_credential_validator_min_password_length, validator_backend(Config)),
    ?assertMatch({error, "minimum required password length is 50"},
                 rabbit_ct_broker_helpers:add_user(Config, ?USERNAME, <<"_">>)).

regexp_integration_fails(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, regexp),
    ?assertMatch(rabbit_credential_validator_password_regexp, validator_backend(Config)),
    ?assertMatch({error, _}, rabbit_ct_broker_helpers:add_user(Config, ?USERNAME, <<"_">>)).

min_length_integration_succeeds(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, min_length, 5),
    ?assertMatch(rabbit_credential_validator_min_password_length, validator_backend(Config)),
    ?assertMatch(ok, rabbit_ct_broker_helpers:add_user(Config, ?USERNAME, <<"abcdefghi">>)).

regexp_integration_succeeds(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, regexp),
    ?assertMatch(rabbit_credential_validator_password_regexp, validator_backend(Config)),
    ?assertMatch(ok, rabbit_ct_broker_helpers:add_user(Config, ?USERNAME, <<"xyz12345678901">>)).

min_length_change_password_integration_fails(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, accept_everything),
    rabbit_ct_broker_helpers:add_user(Config, ?USERNAME, <<"abcdefghi">>),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, min_length, 50),
    ?assertMatch(rabbit_credential_validator_min_password_length, validator_backend(Config)),
    ?assertMatch({error, "minimum required password length is 50"},
                 rabbit_ct_broker_helpers:change_password(Config, ?USERNAME, <<"_">>)).

regexp_change_password_integration_fails(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, accept_everything),
    rabbit_ct_broker_helpers:add_user(Config, ?USERNAME, <<"abcdefghi">>),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, regexp),
    ?assertMatch(rabbit_credential_validator_password_regexp, validator_backend(Config)),
    ?assertMatch({error, _}, rabbit_ct_broker_helpers:change_password(Config, ?USERNAME, <<"_">>)).

min_length_change_password_integration_succeeds(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, accept_everything),
    rabbit_ct_broker_helpers:add_user(Config, ?USERNAME, <<"abcdefghi">>),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, min_length, 5),
    ?assertMatch(rabbit_credential_validator_min_password_length, validator_backend(Config)),
    ?assertMatch(ok, rabbit_ct_broker_helpers:change_password(Config, ?USERNAME, <<"abcdefghi">>)).

regexp_change_password_integration_succeeds(Config) ->
    rabbit_ct_broker_helpers:delete_user(Config, ?USERNAME),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, accept_everything),
    rabbit_ct_broker_helpers:add_user(Config, ?USERNAME, <<"abcdefghi">>),
    rabbit_ct_broker_helpers:switch_credential_validator(Config, regexp),
    ?assertMatch(rabbit_credential_validator_password_regexp, validator_backend(Config)),
    ?assertMatch(ok, rabbit_ct_broker_helpers:change_password(Config, ?USERNAME, <<"xyz12345678901">>)).

%%
%% PropEr
%%

prop_min_length_fails_validation() ->
    N = 5,
    F = fun rabbit_credential_validator_min_password_length:validate/3,
    ?FORALL(Val, binary(N),
            ?FORALL(Length, choose(N + 1, 100),
                   failed_validation(F(?USERNAME, Val, Length + 1)))).

prop_min_length_passes_validation() ->
    N = 20,
    F = fun rabbit_credential_validator_min_password_length:validate/3,
    ?FORALL(Val, binary(N),
            ?FORALL(Length, choose(1, N - 1),
                   passed_validation(F(?USERNAME, Val, Length)))).

prop_regexp_fails_validation() ->
    N = 5,
    F = fun rabbit_credential_validator_password_regexp:validate/3,
    ?FORALL(Val, binary(N),
            ?FORALL(Length, choose(N + 1, 100),
                   failed_validation(F(?USERNAME, Val, regexp_that_requires_length_of_at_least(Length + 1))))).

prop_regexp_passes_validation() ->
    N = 5,
    F = fun rabbit_credential_validator_password_regexp:validate/3,
    ?FORALL(Val, binary(N),
            passed_validation(F(?USERNAME, Val, regexp_that_requires_length_of_at_most(size(Val) + 1)))).

%%
%% Helpers
%%

passed_validation(ok) ->
    true;
passed_validation({error, _}) ->
    false.

failed_validation(Result) ->
    not passed_validation(Result).

regexp_that_requires_length_of_at_least(N) when is_integer(N) ->
    rabbit_misc:format("^[a-zA-Z0-9]{~p,~p}", [N, N + 10]).

regexp_that_requires_length_of_at_most(N) when is_integer(N) ->
    rabbit_misc:format("^[a-zA-Z0-9]{0,~p}", [N]).

validator_backend(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_credential_validation, backend, []).
