%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(credential_validation_SUITE).

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
                        basic_unconditionally_accepting_succeeds,
                        min_length_fails,
                        min_length_succeeds,
                        min_length_proper_fails,
                        min_length_proper_succeeds,
                        regexp_fails,
                        regexp_succeeds,
                        regexp_proper_fails,
                        regexp_proper_succeeds
                       ]},
     {unit, [parallel], [
                         min_length_integration_fails,
                         regexp_integration_fails,
                         min_length_integration_succeeds,
                         regexp_integration_succeeds
                        ]}
].

suite() ->
    [
      {timetrap, {minutes, 4}}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(integration, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 1}
    ]);

init_per_group(unit, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    switch_validator(Config, accept_everything),
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).


%%
%% Test Cases
%%

basic_unconditionally_accepting_succeeds(_Config) ->
    F = fun rabbit_credential_validator_accept_everything:validate_password/1,

    Pwd1 = crypto:strong_rand_bytes(1),
    ?assertEqual(ok, F(Pwd1)),
    Pwd2 = crypto:strong_rand_bytes(5),
    ?assertEqual(ok, F(Pwd2)),
    Pwd3 = crypto:strong_rand_bytes(10),
    ?assertEqual(ok, F(Pwd3)),
    Pwd4 = crypto:strong_rand_bytes(50),
    ?assertEqual(ok, F(Pwd4)),
    Pwd5 = crypto:strong_rand_bytes(100),
    ?assertEqual(ok, F(Pwd5)),
    Pwd6 = crypto:strong_rand_bytes(1000),
    ?assertEqual(ok, F(Pwd6)).

min_length_fails(_Config) ->
    F = fun rabbit_credential_validator_min_length:validate_password/2,

    Pwd1 = crypto:strong_rand_bytes(1),
    ?assertMatch({error, _}, F(Pwd1, 5)),
    Pwd2 = crypto:strong_rand_bytes(5),
    ?assertMatch({error, _}, F(Pwd2, 6)),
    Pwd3 = crypto:strong_rand_bytes(10),
    ?assertMatch({error, _}, F(Pwd3, 15)),
    Pwd4 = crypto:strong_rand_bytes(50),
    ?assertMatch({error, _}, F(Pwd4, 60)).

min_length_succeeds(_Config) ->
    F = fun rabbit_credential_validator_min_length:validate_password/2,

    ?assertEqual(ok, F(crypto:strong_rand_bytes(1), 1)),
    ?assertEqual(ok, F(crypto:strong_rand_bytes(6), 6)),
    ?assertEqual(ok, F(crypto:strong_rand_bytes(7), 6)),
    ?assertEqual(ok, F(crypto:strong_rand_bytes(20), 20)),
    ?assertEqual(ok, F(crypto:strong_rand_bytes(40), 30)),
    ?assertEqual(ok, F(crypto:strong_rand_bytes(50), 50)).

min_length_proper_fails(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_min_length_fails_validation/0, [], 500).

min_length_proper_succeeds(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_min_length_passes_validation/0, [], 500).

regexp_fails(_Config) ->
    F = fun rabbit_credential_validator_regexp:validate_password/2,

    ?assertMatch({error, _}, F(<<"abc">>,    "^xyz")),
    ?assertMatch({error, _}, F(<<"abcdef">>, "^xyz")),
    ?assertMatch({error, _}, F(<<"abcxyz">>, "^abc\\d+")).

regexp_succeeds(_Config) ->
    F = fun rabbit_credential_validator_regexp:validate_password/2,

    ?assertEqual(ok, F(<<"abc">>,    "^abc")),
    ?assertEqual(ok, F(<<"abcdef">>, "^abc")),
    ?assertEqual(ok, F(<<"abc123">>, "^abc\\d+")).

regexp_proper_fails(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_regexp_fails_validation/0, [], 500).

regexp_proper_succeeds(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_regexp_passes_validation/0, [], 500).

min_length_integration_fails(Config) ->
    switch_validator(Config, min_length),
    ?assertMatch({error, _}, add_user(Config, <<"abc">>, <<"ab">>)).

regexp_integration_fails(Config) ->
    switch_validator(Config, regexp),
    ?assertMatch({error, _}, add_user(Config, <<"abc">>, <<"ab">>)).

min_length_integration_succeeds(Config) ->
    switch_validator(Config, min_length),
    ?assertMatch({error, _}, add_user(Config, <<"abc">>, <<"abcdefghi">>)).

regexp_integration_succeeds(Config) ->
    switch_validator(Config, regexp),
    ?assertMatch({error, _}, add_user(Config, <<"abc">>, <<"xyz12345678901">>)).

%%
%% PropEr
%%

prop_min_length_fails_validation() ->
    N = 5,
    F = fun rabbit_credential_validator_min_length:validate_password/2,
    ?FORALL(Val, binary(N),
            ?FORALL(Length, choose(N + 1, 100),
                   failed_validation(F(Val, Length + 1)))).

prop_min_length_passes_validation() ->
    N = 20,
    F = fun rabbit_credential_validator_min_length:validate_password/2,
    ?FORALL(Val, binary(N),
            ?FORALL(Length, choose(1, N - 1),
                   passed_validation(F(Val, Length)))).

prop_regexp_fails_validation() ->
    N = 5,
    F = fun rabbit_credential_validator_regexp:validate_password/2,
    ?FORALL(Val, binary(N),
            ?FORALL(Length, choose(N + 1, 100),
                   failed_validation(F(Val, regexp_that_requires_length_of_at_least(Length + 1))))).

prop_regexp_passes_validation() ->
    N = 5,
    F = fun rabbit_credential_validator_regexp:validate_password/2,
    ?FORALL(Val, binary(N),
            passed_validation(F(Val, regexp_that_requires_length_of_at_most(size(Val) + 1)))).

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

switch_validator(Config, accept_everything) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, credential_validator,
                                  [{validation_backend, rabbit_credential_validator_accept_everything}]]);

switch_validator(Config, min_length) ->
    switch_validator(Config, min_length, 5);

switch_validator(Config, regexp) ->
    switch_validator(Config, regexp, <<"xyz\d{10,12}">>).


switch_validator(Config, min_length, MinLength) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, credential_validator,
                                  [{validation_backend, rabbit_credential_validator_min_length},
                                   {min_length,         MinLength}]]);

switch_validator(Config, regexp, RegExp) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, credential_validator,
                                  [{validation_backend, rabbit_credential_validator_regexp},
                                   {regexp,             RegExp}]]).

add_user(Config, Username, Password) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, add_user, [Username, Password]).

delete_user(Config, Username) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, delete_user, [Username]).
