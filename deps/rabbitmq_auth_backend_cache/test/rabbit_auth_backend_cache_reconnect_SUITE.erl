%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Integration tests for `rabbit_auth_backend_cache' key invariants:
%% identical inputs must hit the cache, semantically distinct inputs
%% must not. The cache plugin is wired to a counting mock backend; each
%% test asserts how many cache calls reached the mock.

-module(rabbit_auth_backend_cache_reconnect_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     cache_hits_with_stable_authprops,
     cache_separates_loopback_from_non_loopback,
     cache_separates_passwords,
     cache_separates_usernames,
     cache_hits_for_map_authprops,
     cache_hits_when_authprops_have_no_loopback_marker,
     refusal_for_non_loopback_does_not_leak_to_loopback
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
    %% Restore the project's PROJECT_ENV so subsequent suites are unaffected.
    ok = rpc(Config, application, set_env,
             [rabbitmq_auth_backend_cache, cached_backend,
              rabbit_auth_backend_internal]),
    ok = rpc(Config, application, set_env,
             [rabbit, auth_backends, [rabbit_auth_backend_internal]]),
    ok = rpc(Config, application, set_env,
             [rabbitmq_auth_backend_cache, cache_refusals, false]),
    ok = rpc(Config, rabbit_auth_backend_cache_counting_mock, set_mode,
             [always_ok]),
    ok = rpc(Config, rabbit_auth_backend_cache, clear_cache, []),
    Config.

cache_hits_with_stable_authprops(Config) ->
    Props = [{password, <<"p">>}, {is_loopback, false}],
    {ok, _} = login(Config, <<"u">>, Props),
    {ok, _} = login(Config, <<"u">>, Props),
    1 = call_count(Config).

cache_separates_loopback_from_non_loopback(Config) ->
    {ok, _} = login(Config, <<"u">>,
                    [{password, <<"p">>}, {is_loopback, true}]),
    {ok, _} = login(Config, <<"u">>,
                    [{password, <<"p">>}, {is_loopback, false}]),
    2 = call_count(Config).

cache_separates_passwords(Config) ->
    {ok, _} = login(Config, <<"u">>, [{password, <<"p1">>}]),
    {ok, _} = login(Config, <<"u">>, [{password, <<"p2">>}]),
    2 = call_count(Config).

cache_separates_usernames(Config) ->
    {ok, _} = login(Config, <<"u1">>, [{password, <<"p">>}]),
    {ok, _} = login(Config, <<"u2">>, [{password, <<"p">>}]),
    2 = call_count(Config).

%% Map-shaped `AuthProps' must cache like proplists; the callback spec
%% accepts both.
cache_hits_for_map_authprops(Config) ->
    Props = #{password => <<"p">>, is_loopback => true},
    {ok, _} = login(Config, <<"u">>, Props),
    {ok, _} = login(Config, <<"u">>, Props),
    1 = call_count(Config).

%% Pre-4.2 wire shape: `AuthProps' without a peer marker must still cache.
cache_hits_when_authprops_have_no_loopback_marker(Config) ->
    Props = [{password, <<"p">>}],
    {ok, _} = login(Config, <<"u">>, Props),
    {ok, _} = login(Config, <<"u">>, Props),
    1 = call_count(Config).

%% Security invariant: with `cache_refusals = true', a refusal cached
%% for a non-loopback peer must not be served to a loopback peer.
refusal_for_non_loopback_does_not_leak_to_loopback(Config) ->
    ok = rpc(Config, application, set_env,
             [rabbitmq_auth_backend_cache, cache_refusals, true]),
    ok = rpc(Config, rabbit_auth_backend_cache_counting_mock, set_mode,
             [refuse_non_loopback]),
    {refused, _, _} = login(Config, <<"u">>,
                            [{password, <<"p">>}, {is_loopback, false}]),
    {ok, _}         = login(Config, <<"u">>,
                            [{password, <<"p">>}, {is_loopback, true}]),
    2 = call_count(Config).

login(Config, Username, AuthProps) ->
    rpc(Config, rabbit_auth_backend_cache, user_login_authentication,
        [Username, AuthProps]).

call_count(Config) ->
    rpc(Config, rabbit_auth_backend_cache_counting_mock,
        authentication_call_count, []).

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).
