%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Tests that the plaintext password never reaches the auth cache key.
%% Two angles are covered:
%%
%%  * direct inspection of the key produced by `cache_key/2', confirming
%%    the password is absent in proplist and map shapes
%%  * end-to-end behaviour: identical credentials hit, distinct
%%    credentials miss, non-password fields still differentiate

-module(rabbit_auth_backend_cache_redaction_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     password_is_not_present_in_cache_key_for_proplist_authprops,
     password_is_not_present_in_cache_key_for_map_authprops,
     non_authn_calls_are_not_redacted,
     identical_passwords_still_hit_the_cache,
     different_passwords_still_miss_the_cache,
     non_password_authprops_pass_through_unchanged
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

%% Security invariant: the plaintext password must not appear in the key
%% produced by `cache_key/2'.
password_is_not_present_in_cache_key_for_proplist_authprops(Config) ->
    Password = <<"super-secret-marker-7f3a">>,
    Props = [{password, Password}, {is_loopback, false}],
    Key = cache_key(Config, user_login_authentication, [<<"u">>, Props]),
    false = contains_binary(Key, Password).

password_is_not_present_in_cache_key_for_map_authprops(Config) ->
    Password = <<"another-secret-9c1b">>,
    Props = #{password => Password, is_loopback => true},
    Key = cache_key(Config, user_login_authentication, [<<"u">>, Props]),
    false = contains_binary(Key, Password).

%% Only authentication carries a password; other call sites must be
%% pass-through to avoid changing established cache semantics.
non_authn_calls_are_not_redacted(Config) ->
    Args = [a, b, c],
    {check_vhost_access, Args} =
        cache_key(Config, check_vhost_access, Args).

%% Equality invariant: redaction must be deterministic so identical
%% credentials still hit the cache.
identical_passwords_still_hit_the_cache(Config) ->
    Props = [{password, <<"p">>}, {is_loopback, false}],
    {ok, _} = login(Config, <<"u">>, Props),
    {ok, _} = login(Config, <<"u">>, Props),
    1 = call_count(Config).

%% Distinguishability invariant: different passwords must still produce
%% different keys.
different_passwords_still_miss_the_cache(Config) ->
    {ok, _} = login(Config, <<"u">>, [{password, <<"p1">>}]),
    {ok, _} = login(Config, <<"u">>, [{password, <<"p2">>}]),
    2 = call_count(Config).

%% Non-password AuthProps fields must remain part of the key.
non_password_authprops_pass_through_unchanged(Config) ->
    {ok, _} = login(Config, <<"u">>,
                    [{password, <<"p">>}, {is_loopback, true}]),
    {ok, _} = login(Config, <<"u">>,
                    [{password, <<"p">>}, {is_loopback, false}]),
    2 = call_count(Config).

login(Config, Username, AuthProps) ->
    rpc(Config, rabbit_auth_backend_cache, user_login_authentication,
        [Username, AuthProps]).

call_count(Config) ->
    rpc(Config, rabbit_auth_backend_cache_counting_mock,
        authentication_call_count, []).

cache_key(Config, F, A) ->
    rpc(Config, rabbit_auth_backend_cache, cache_key, [F, A]).

contains_binary(Term, Needle) when is_binary(Needle) ->
    binary:match(term_to_binary(Term), Needle) =/= nomatch.

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).
