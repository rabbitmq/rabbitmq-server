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
     basic_unconditionally_accepting_succeeds,
     min_length_fails,
     min_length_succeeds,
     min_length_proper_fails,
     min_length_proper_succeeds,
     regexp_fails,
     regexp_succeeds
    ].

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.


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

    ?assertMatch({error, _}, F("abc",    "^xyz")),
    ?assertMatch({error, _}, F("abcdef", "^xyz")),
    ?assertMatch({error, _}, F("abcxyz", "^abc\\d+")).

regexp_succeeds(_Config) ->
    F = fun rabbit_credential_validator_regexp:validate_password/2,

    ?assertEqual(ok, F("abc",    "^abc")),
    ?assertEqual(ok, F("abcdef", "^abc")),
    ?assertEqual(ok, F("abc123", "^abc\\d+")).

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

%%
%% Helpers
%%

passed_validation(ok) ->
    true;
passed_validation({error, _}) ->
    false.

failed_validation(Result) ->
    not passed_validation(Result).
