%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Property-based tests for `rabbit_auth_backend_cache' key invariants.
%% The cache plugin sits in front of a counting mock backend; each
%% property asserts how many cache calls reach the mock.

-module(rabbit_auth_backend_cache_prop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-compile([export_all, nowarn_export_all]).

-import(rabbit_ct_proper_helpers, [run_proper/3]).

-define(NUMTESTS, 200).

all() ->
    [
     prop_identical_authprops_hit_cache,
     prop_separates_loopback_from_non_loopback,
     prop_different_passwords_yield_different_keys,
     prop_different_usernames_yield_different_keys,
     prop_password_is_absent_from_cache_key,
     prop_redaction_is_deterministic
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config, rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_TestCase, Config) ->
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(
           Config, rabbit_auth_backend_cache_counting_mock),
    ok = rpc(Config, rabbit_auth_backend_cache_counting_mock, init, []),
    ok = rpc(Config, application, set_env,
             [rabbitmq_auth_backend_cache, cached_backend,
              rabbit_auth_backend_cache_counting_mock]),
    ok = rpc(Config, application, set_env,
             [rabbit, auth_backends, [rabbit_auth_backend_cache]]),
    ok = rpc(Config, rabbit_auth_backend_cache, clear_cache, []),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = rpc(Config, application, set_env,
             [rabbitmq_auth_backend_cache, cached_backend,
              rabbit_auth_backend_internal]),
    ok = rpc(Config, application, set_env,
             [rabbit, auth_backends, [rabbit_auth_backend_internal]]),
    ok = rpc(Config, rabbit_auth_backend_cache, clear_cache, []),
    Config.

%%%=========================================================================
%%% Properties
%%%=========================================================================

%% A repeated, byte-identical login must not be forwarded twice.
prop_identical_authprops_hit_cache(Config) ->
    run_proper(
      fun() ->
              ?FORALL({U, P, L}, {username(), password(), boolean()},
                      identical_calls_count_one(Config, U, P, L))
      end, [], ?NUMTESTS).

%% A loopback approval must never be served back to a non-loopback peer.
prop_separates_loopback_from_non_loopback(Config) ->
    run_proper(
      fun() ->
              ?FORALL({U, P}, {username(), password()},
                      loopback_separation_count_two(Config, U, P))
      end, [], ?NUMTESTS).

%% A login cached for one password must not be served at a different password.
prop_different_passwords_yield_different_keys(Config) ->
    run_proper(
      fun() ->
              ?FORALL({U, P1, P2}, {username(), password(), password()},
                      ?IMPLIES(P1 =/= P2,
                               distinct_password_count_two(Config, U, P1, P2)))
      end, [], ?NUMTESTS).

%% A login cached for one user must not be served as another user's login.
prop_different_usernames_yield_different_keys(Config) ->
    run_proper(
      fun() ->
              ?FORALL({U1, U2, P}, {username(), username(), password()},
                      ?IMPLIES(U1 =/= U2,
                               distinct_username_count_two(Config, U1, U2, P)))
      end, [], ?NUMTESTS).

%% Security invariant: the plaintext password must not be a substring of
%% the cache key, regardless of username and loopback flag.
prop_password_is_absent_from_cache_key(Config) ->
    run_proper(
      fun() ->
              ?FORALL({U, P, L},
                      {username(), non_trivial_password(), boolean()},
                      password_absent_from_key(Config, U, P, L))
      end, [], ?NUMTESTS).

%% Equality invariant: redacting the same AuthProps twice must produce
%% byte-identical keys, otherwise repeat logins miss.
prop_redaction_is_deterministic(Config) ->
    run_proper(
      fun() ->
              ?FORALL({U, P, L}, {username(), password(), boolean()},
                      redaction_deterministic(Config, U, P, L))
      end, [], ?NUMTESTS).

%%%=========================================================================
%%% Property bodies
%%%=========================================================================

identical_calls_count_one(Config, U, P, L) ->
    fresh_state(Config),
    Props = [{password, P}, {is_loopback, L}],
    {ok, _} = login(Config, U, Props),
    {ok, _} = login(Config, U, Props),
    1 =:= call_count(Config).

loopback_separation_count_two(Config, U, P) ->
    fresh_state(Config),
    {ok, _} = login(Config, U, [{password, P}, {is_loopback, true}]),
    {ok, _} = login(Config, U, [{password, P}, {is_loopback, false}]),
    2 =:= call_count(Config).

distinct_password_count_two(Config, U, P1, P2) ->
    fresh_state(Config),
    {ok, _} = login(Config, U, [{password, P1}]),
    {ok, _} = login(Config, U, [{password, P2}]),
    2 =:= call_count(Config).

distinct_username_count_two(Config, U1, U2, P) ->
    fresh_state(Config),
    {ok, _} = login(Config, U1, [{password, P}]),
    {ok, _} = login(Config, U2, [{password, P}]),
    2 =:= call_count(Config).

password_absent_from_key(Config, U, P, L) ->
    Key = cache_key(Config, user_login_authentication,
                    [U, [{password, P}, {is_loopback, L}]]),
    binary:match(term_to_binary(Key), P) =:= nomatch.

redaction_deterministic(Config, U, P, L) ->
    Args = [U, [{password, P}, {is_loopback, L}]],
    cache_key(Config, user_login_authentication, Args)
        =:= cache_key(Config, user_login_authentication, Args).

%%%=========================================================================
%%% Helpers
%%%=========================================================================

fresh_state(Config) ->
    ok = rpc(Config, rabbit_auth_backend_cache_counting_mock, reset, []),
    ok = rpc(Config, rabbit_auth_backend_cache, clear_cache, []).

login(Config, U, Props) ->
    rpc(Config, rabbit_auth_backend_cache, user_login_authentication,
        [U, Props]).

cache_key(Config, F, A) ->
    rpc(Config, rabbit_auth_backend_cache, cache_key, [F, A]).

call_count(Config) ->
    rpc(Config, rabbit_auth_backend_cache_counting_mock,
        authentication_call_count, []).

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).

%%%=========================================================================
%%% Generators
%%%=========================================================================

username() ->
    non_empty(binary()).

password() ->
    non_empty(binary()).

%% A password long enough that random collisions with hash output bytes
%% are negligible, used by the "password absent from key" property.
non_trivial_password() ->
    ?LET(B, non_empty(binary()), <<"pw-", B/binary, "-marker">>).
