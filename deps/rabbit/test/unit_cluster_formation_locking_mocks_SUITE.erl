%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%
-module(unit_cluster_formation_locking_mocks_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               init_with_lock_exits_after_errors,
                               init_with_lock_ignore_after_errors,
                               init_with_lock_not_supported,
                               init_with_lock_supported
                              ]}
    ].

init_per_testcase(Testcase, Config) when Testcase == init_with_lock_exits_after_errors;
                                         Testcase == init_with_lock_not_supported;
                                         Testcase == init_with_lock_supported ->
    application:set_env(rabbit, cluster_formation,
                        [{peer_discover_backend, peer_discover_classic_config},
                         {lock_acquisition_failure_mode, fail}]),
    ok = meck:new(rabbit_peer_discovery_classic_config, [passthrough]),
    Config;
init_per_testcase(init_with_lock_ignore_after_errors, Config) ->
    application:set_env(rabbit, cluster_formation,
                        [{peer_discover_backend, peer_discover_classic_config},
                         {lock_acquisition_failure_mode, ignore}]),
    ok = meck:new(rabbit_peer_discovery_classic_config, [passthrough]),
    Config.

end_per_testcase(_, _) ->
    meck:unload(),
    application:unset_env(rabbit, cluster_formation).

init_with_lock_exits_after_errors(_Config) ->
    meck:expect(rabbit_peer_discovery_classic_config, lock, fun(_) -> {error, "test error"} end),
    ?assertEqual(
       {error, "test error"},
       rabbit_peer_discovery:join_selected_node(rabbit_peer_discovery_classic_config, missing@localhost, disc)),
    ?assert(meck:validate(rabbit_peer_discovery_classic_config)),
    passed.

%% The `aborted_feature_flags_compat_check' error means the function called
%% `rabbit_db_cluster:join/2', so it passed the locking step. The error is
%% expected because the test runs outside of a working RabbitMQ.

init_with_lock_ignore_after_errors(_Config) ->
    meck:expect(rabbit_peer_discovery_classic_config, lock, fun(_) -> {error, "test error"} end),
    ?assertEqual(
       {error, {aborted_feature_flags_compat_check, {error, feature_flags_file_not_set}}},
       rabbit_peer_discovery:join_selected_node(rabbit_peer_discovery_classic_config, missing@localhost, disc)),
    ?assert(meck:validate(rabbit_peer_discovery_classic_config)),
    passed.

init_with_lock_not_supported(_Config) ->
    meck:expect(rabbit_peer_discovery_classic_config, lock, fun(_) -> not_supported end),
    ?assertEqual(
       {error, {aborted_feature_flags_compat_check, {error, feature_flags_file_not_set}}},
       rabbit_peer_discovery:join_selected_node(rabbit_peer_discovery_classic_config, missing@localhost, disc)),
    ?assert(meck:validate(rabbit_peer_discovery_classic_config)),
    passed.

init_with_lock_supported(_Config) ->
    meck:expect(rabbit_peer_discovery_classic_config, lock, fun(_) -> {ok, data} end),
    meck:expect(rabbit_peer_discovery_classic_config, unlock, fun(data) -> ok end),
    ?assertEqual(
       {error, {aborted_feature_flags_compat_check, {error, feature_flags_file_not_set}}},
       rabbit_peer_discovery:join_selected_node(rabbit_peer_discovery_classic_config, missing@localhost, disc)),
    ?assert(meck:validate(rabbit_peer_discovery_classic_config)),
    passed.
