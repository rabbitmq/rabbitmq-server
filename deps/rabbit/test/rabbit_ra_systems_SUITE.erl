%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_ra_systems_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        coordination_defaults,
        coordination_wal_max_size_bytes_default,
        quorum_queue_defaults
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:run_setup_steps(Config),
    rabbit_ct_config_schema:init_schemas(rabbit, Config1).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

coordination_defaults(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, check_coordination_defaults, []).

coordination_wal_max_size_bytes_default(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, check_coordination_wal_max_size_bytes_default, []).

quorum_queue_defaults(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, check_quorum_queue_defaults, []).

check_coordination_defaults() ->
    %% Get the coordination Ra system config
    Config = rabbit_ra_systems:get_config(coordination),
    
    %% Verify that all expected keys are present
    ?assert(maps:is_key(wal_compute_checksums, Config)),
    ?assert(maps:is_key(segment_compute_checksums, Config)),
    ?assert(maps:is_key(wal_max_size_bytes, Config)),
    ?assert(maps:is_key(wal_max_entries, Config)),
    ?assert(maps:is_key(wal_max_batch_size, Config)),
    ?assert(maps:is_key(segment_max_size_bytes, Config)),
    ?assert(maps:is_key(segment_max_entries, Config)),
    ?assert(maps:is_key(default_max_append_entries_rpc_batch_size, Config)),
    ?assert(maps:is_key(compress_mem_tables, Config)),
    ?assert(maps:is_key(snapshot_chunk_size, Config)),
    
    %% Verify default boolean values
    ?assertEqual(true, maps:get(wal_compute_checksums, Config)),
    ?assertEqual(true, maps:get(segment_compute_checksums, Config)),
    ?assertEqual(true, maps:get(compress_mem_tables, Config)),
    
    ok.

check_coordination_wal_max_size_bytes_default() ->
    %% Get the coordination Ra system config
    Config = rabbit_ra_systems:get_config(coordination),
    
    %% Verify that wal_max_size_bytes defaults to 64 MB (64_000_000 bytes)
    WalMaxSizeBytes = maps:get(wal_max_size_bytes, Config),
    ExpectedWalMaxSizeBytes = 64_000_000,
    ?assertEqual(ExpectedWalMaxSizeBytes, WalMaxSizeBytes),
    
    ok.

check_quorum_queue_defaults() ->
    %% Get the quorum_queues Ra system config
    Config = rabbit_ra_systems:get_config(quorum_queues),
    
    %% Verify that all expected keys are present
    ?assert(maps:is_key(wal_compute_checksums, Config)),
    ?assert(maps:is_key(segment_compute_checksums, Config)),
    ?assert(maps:is_key(wal_max_size_bytes, Config)),
    ?assert(maps:is_key(wal_max_entries, Config)),
    ?assert(maps:is_key(wal_max_batch_size, Config)),
    ?assert(maps:is_key(segment_max_size_bytes, Config)),
    ?assert(maps:is_key(segment_max_entries, Config)),
    ?assert(maps:is_key(default_max_append_entries_rpc_batch_size, Config)),
    ?assert(maps:is_key(compress_mem_tables, Config)),
    ?assert(maps:is_key(snapshot_chunk_size, Config)),
    ?assert(maps:is_key(server_recovery_strategy, Config)),
    
    ok.
