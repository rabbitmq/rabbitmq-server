%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-compile(export_all).

all() -> [
    obfuscate_upstream,
    obfuscate_upstream_params_network,
    obfuscate_upstream_params_network_with_char_list_password_value,
    obfuscate_upstream_params_direct,
    shutdown_flag_defaults_to_false,
    shutdown_flag_can_be_set,
    shutdown_flag_can_be_cleared,
    terminate_all_empty_scope,
    terminate_all_nonexistent_scope,
    terminate_all_shutdown_kills_non_trapping_processes,
    terminate_all_timeout_force_kill,
    terminate_all_paced_batching,
    terminate_all_timeout_kills_remaining_batches
].

init_per_suite(Config) ->
    application:ensure_all_started(credentials_obfuscation),
    Config.

end_per_suite(Config) ->
    Config.

obfuscate_upstream(_Config) ->
    Upstream = #upstream{uris = [<<"amqp://guest:password@localhost">>]},
    ObfuscatedUpstream = rabbit_federation_util:obfuscate_upstream(Upstream),
    ?assertEqual(Upstream, rabbit_federation_util:deobfuscate_upstream(ObfuscatedUpstream)),
    ok.

obfuscate_upstream_params_network(_Config) ->
    UpstreamParams = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(UpstreamParams),
    ?assertEqual(UpstreamParams, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.

obfuscate_upstream_params_network_with_char_list_password_value(_Config) ->
    Input = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = "password"}
    },
    Output = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_network{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(Input),
    ?assertEqual(Output, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.

 obfuscate_upstream_params_direct(_Config) ->
    UpstreamParams = #upstream_params{
        uri = <<"amqp://guest:password@localhost">>,
        params = #amqp_params_direct{password = <<"password">>}
    },
    ObfuscatedUpstreamParams = rabbit_federation_util:obfuscate_upstream_params(UpstreamParams),
    ?assertEqual(UpstreamParams, rabbit_federation_util:deobfuscate_upstream_params(ObfuscatedUpstreamParams)),
    ok.

shutdown_flag_defaults_to_false(_Config) ->
    ok = rabbit_federation_app_state:reset_shutting_down_marker(),
    ?assertEqual(false, rabbit_federation_app_state:is_shutting_down()),
    ok.

shutdown_flag_can_be_set(_Config) ->
    ok = rabbit_federation_app_state:reset_shutting_down_marker(),
    ?assertEqual(false, rabbit_federation_app_state:is_shutting_down()),
    ok = rabbit_federation_app_state:mark_as_shutting_down(),
    ?assertEqual(true, rabbit_federation_app_state:is_shutting_down()),
    ok = rabbit_federation_app_state:reset_shutting_down_marker(),
    ok.

shutdown_flag_can_be_cleared(_Config) ->
    ok = rabbit_federation_app_state:mark_as_shutting_down(),
    ?assertEqual(true, rabbit_federation_app_state:is_shutting_down()),
    ok = rabbit_federation_app_state:reset_shutting_down_marker(),
    ?assertEqual(false, rabbit_federation_app_state:is_shutting_down()),
    ok.

terminate_all_empty_scope(_Config) ->
    Scope = test_scope_empty,
    {ok, _} = pg:start_link(Scope),
    ?assertEqual(ok, rabbit_federation_pg:terminate_all_local_members(Scope, 1000)),
    stop_pg_scope(Scope).

terminate_all_nonexistent_scope(_Config) ->
    ?assertEqual(ok, rabbit_federation_pg:terminate_all_local_members(nonexistent_scope_xyz, 1000)),
    ok.

terminate_all_shutdown_kills_non_trapping_processes(_Config) ->
    Scope = test_scope_non_trapping,
    {ok, _} = pg:start_link(Scope),
    Pids = [spawn(fun() -> receive stop -> ok end end) || _ <- lists:seq(1, 10)],
    [pg:join(Scope, test_group, Pid) || Pid <- Pids],
    ?assertEqual(ok, rabbit_federation_pg:terminate_all_local_members(Scope, 5000)),
    timer:sleep(100),
    [?assertEqual(false, is_process_alive(Pid)) || Pid <- Pids],
    stop_pg_scope(Scope).

terminate_all_timeout_force_kill(_Config) ->
    Scope = test_scope_timeout,
    {ok, _} = pg:start_link(Scope),
    Pids = [spawn(fun() ->
        process_flag(trap_exit, true),
        receive after infinity -> ok end
    end) || _ <- lists:seq(1, 5)],
    [pg:join(Scope, test_group, Pid) || Pid <- Pids],
    ?assertEqual(ok, rabbit_federation_pg:terminate_all_local_members(Scope, 100)),
    timer:sleep(100),
    [?assertEqual(false, is_process_alive(Pid)) || Pid <- Pids],
    stop_pg_scope(Scope).

terminate_all_paced_batching(_Config) ->
    Scope = test_scope_paced,
    {ok, _} = pg:start_link(Scope),
    Pids = [spawn(fun() -> receive stop -> ok end end) || _ <- lists:seq(1, 20)],
    [pg:join(Scope, test_group, Pid) || Pid <- Pids],
    BatchSize = 5,
    ThrottleDelay = 10,
    Timeout = 5000,
    ?assertEqual(ok, rabbit_federation_pg:terminate_all_local_members(
        Scope, Timeout, BatchSize, ThrottleDelay)),
    timer:sleep(100),
    [?assertEqual(false, is_process_alive(Pid)) || Pid <- Pids],
    stop_pg_scope(Scope).

terminate_all_timeout_kills_remaining_batches(_Config) ->
    Scope = test_scope_remaining,
    {ok, _} = pg:start_link(Scope),
    Pids = [spawn(fun() ->
        process_flag(trap_exit, true),
        receive after infinity -> ok end
    end) || _ <- lists:seq(1, 15)],
    [pg:join(Scope, test_group, Pid) || Pid <- Pids],
    BatchSize = 5,
    ThrottleDelay = 10,
    Timeout = 50,
    ?assertEqual(ok, rabbit_federation_pg:terminate_all_local_members(
        Scope, Timeout, BatchSize, ThrottleDelay)),
    timer:sleep(100),
    [?assertEqual(false, is_process_alive(Pid)) || Pid <- Pids],
    stop_pg_scope(Scope).

stop_pg_scope(Scope) ->
    case whereis(Scope) of
        Pid when is_pid(Pid) ->
            unlink(Pid),
            exit(Pid, kill);
        _ -> ok
    end,
    ok.
