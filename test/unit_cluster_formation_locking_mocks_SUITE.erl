%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
    ?assertExit(cannot_acquire_startup_lock, rabbit_mnesia:init_with_lock(2, 10, fun() -> ok end)),
    ?assert(meck:validate(rabbit_peer_discovery_classic_config)),
    passed.

init_with_lock_ignore_after_errors(_Config) ->
    meck:expect(rabbit_peer_discovery_classic_config, lock, fun(_) -> {error, "test error"} end),
    ?assertEqual(ok, rabbit_mnesia:init_with_lock(2, 10, fun() -> ok end)),
    ?assert(meck:validate(rabbit_peer_discovery_classic_config)),
    passed.

init_with_lock_not_supported(_Config) ->
    meck:expect(rabbit_peer_discovery_classic_config, lock, fun(_) -> not_supported end),
    ?assertEqual(ok, rabbit_mnesia:init_with_lock(2, 10, fun() -> ok end)),
    ?assert(meck:validate(rabbit_peer_discovery_classic_config)),
    passed.

init_with_lock_supported(_Config) ->
    meck:expect(rabbit_peer_discovery_classic_config, lock, fun(_) -> {ok, data} end),
    meck:expect(rabbit_peer_discovery_classic_config, unlock, fun(data) -> ok end),
    ?assertEqual(ok, rabbit_mnesia:init_with_lock(2, 10, fun() -> ok end)),
    ?assert(meck:validate(rabbit_peer_discovery_classic_config)),
    passed.
