%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_ra_systems).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([setup/0,
         setup/1,
         all_ra_systems/0,
         are_running/0,
         ensure_ra_system_started/1,
         ensure_ra_system_stopped/1,
         ensure_started/0,
         ensure_stopped/0]).

-type ra_system_name() :: atom().

-define(COORD_WAL_MAX_SIZE_B, 64_000_000).
-define(COORD_WAL_MAX_ENTRIES, 500_000).
-define(QUORUM_AER_MAX_RPC_SIZE, 16).
-define(QUORUM_DEFAULT_WAL_MAX_SIZE_B, 536_870_912).
-define(QUORUM_DEFAULT_WAL_MAX_ENTRIES, 500_000).
-define(QUORUM_DEFAULT_WAL_MAX_BATCH_SIZE, 4096).
%% the default min bin vheap value in OTP 26
-define(MIN_BIN_VHEAP_SIZE_DEFAULT, 46422).
-define(MIN_BIN_VHEAP_SIZE_MULT, 64).

-spec setup() -> ok | no_return().

setup() ->
    setup(rabbit_prelaunch:get_context()).

-spec setup(Context :: map()) -> ok | no_return().

setup(_) ->
    ensure_started(),
    ok.

-spec all_ra_systems() -> [ra_system_name()].

all_ra_systems() ->
    [coordination,
     quorum_queues].

-spec are_running() -> AreRunning when
      AreRunning :: boolean().

are_running() ->
    try
        %% FIXME: We hard-code the name of an internal Ra process here.
        Children = supervisor:which_children(ra_systems_sup),
        lists:all(
          fun(RaSystem) ->
                  is_ra_system_running(Children, RaSystem)
          end,
          all_ra_systems())
    catch
        exit:{noproc, _} ->
            false
    end.

is_ra_system_running(Children, RaSystem) ->
    case lists:keyfind(RaSystem, 1, Children) of
        {RaSystem, Child, _, _} -> is_pid(Child);
        false                   -> false
    end.

-spec ensure_started() -> ok | no_return().

ensure_started() ->
    ?LOG_DEBUG(
       "Starting Ra systems",
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    lists:foreach(fun ensure_ra_system_started/1, all_ra_systems()),
    ?LOG_DEBUG(
       "Ra systems started",
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    ok.

-spec ensure_ra_system_started(ra_system_name()) -> ok | no_return().

ensure_ra_system_started(RaSystem) ->
    RaSystemConfig = get_config(RaSystem),
    ?LOG_DEBUG(
       "Starting Ra system called \"~ts\" with configuration:~n~tp",
       [RaSystem, RaSystemConfig],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    case ra_system:start(RaSystemConfig) of
        {ok, _} ->
            ?LOG_DEBUG(
               "Ra system \"~ts\" ready",
               [RaSystem],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        {error, {already_started, _}} ->
            ?LOG_DEBUG(
               "Ra system \"~ts\" ready",
               [RaSystem],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            ok;
        Error ->
            ?LOG_ERROR(
               "Failed to start Ra system \"~ts\": ~tp",
               [RaSystem, Error],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            throw(Error)
    end.

-spec get_config(ra_system_name()) -> ra_system:config().
get_config(quorum_queues = RaSystem) ->
    DefaultConfig = ra_system:default_config(),
    Checksums = application:get_env(rabbit, quorum_compute_checksums, true),
    WalChecksums = application:get_env(rabbit, quorum_wal_compute_checksums, Checksums),
    SegmentChecksums = application:get_env(rabbit, quorum_segment_compute_checksums,
                                           Checksums),
    WalMaxEntries = application:get_env(rabbit, quorum_wal_max_entries,
                                        ?QUORUM_DEFAULT_WAL_MAX_ENTRIES),
    AERBatchSize = application:get_env(rabbit, quorum_max_append_entries_rpc_batch_size,
                                       ?QUORUM_AER_MAX_RPC_SIZE),
    CompressMemTables = application:get_env(rabbit, quorum_compress_mem_tables, true),
    MinBinVheapSize = case code_version:get_otp_version() of
                          OtpMaj when OtpMaj >= 27 ->
                              ?MIN_BIN_VHEAP_SIZE_DEFAULT * ?MIN_BIN_VHEAP_SIZE_MULT;
                          _ ->
                              ?MIN_BIN_VHEAP_SIZE_DEFAULT
                      end,
    SegmentMaxSizeBytes = application:get_env(rabbit, quorum_segment_max_size_bytes,
                                              maps:get(segment_max_size_bytes, DefaultConfig)),
    SegmentMaxEntries = application:get_env(rabbit, quorum_segment_max_entries,
                                            maps:get(segment_max_entries, DefaultConfig)),
    WalMaxSizeBytes = application:get_env(rabbit, quorum_wal_max_size_bytes,
                                          ?QUORUM_DEFAULT_WAL_MAX_SIZE_B),
    WalMaxBatchSize = application:get_env(rabbit, quorum_wal_max_batch_size,
                                          ?QUORUM_DEFAULT_WAL_MAX_BATCH_SIZE),
    SnapshotChunkSize = application:get_env(rabbit, quorum_snapshot_chunk_size,
                                            maps:get(snapshot_chunk_size, DefaultConfig)),

    DefaultConfig#{name => RaSystem,
                   wal_min_bin_vheap_size => MinBinVheapSize,
                   server_min_bin_vheap_size => MinBinVheapSize,
                   default_max_append_entries_rpc_batch_size => AERBatchSize,
                   wal_compute_checksums => WalChecksums,
                   wal_max_entries => WalMaxEntries,
                   segment_compute_checksums => SegmentChecksums,
                   compress_mem_tables => CompressMemTables,
                   segment_max_size_bytes => SegmentMaxSizeBytes,
                   segment_max_entries => SegmentMaxEntries,
                   wal_max_size_bytes => WalMaxSizeBytes,
                   wal_max_batch_size => WalMaxBatchSize,
                   snapshot_chunk_size => SnapshotChunkSize,
                   server_recovery_strategy => {rabbit_quorum_queue,
                                                system_recover, []}};
get_config(coordination = RaSystem) ->
    DefaultConfig = ra_system:default_config(),
    CoordDataDir = filename:join(
                     [rabbit:data_dir(), "coordination", node()]),
    DefaultConfig#{name => RaSystem,
                   data_dir => CoordDataDir,
                   wal_data_dir => CoordDataDir,
                   wal_max_size_bytes => ?COORD_WAL_MAX_SIZE_B,
                   wal_max_entries => ?COORD_WAL_MAX_ENTRIES,
                   names => ra_system:derive_names(RaSystem)}.

-spec ensure_stopped() -> ok | no_return().
ensure_stopped() ->
    ?LOG_DEBUG(
       "Stopping Ra systems",
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    %% lists:reverse/1 is used to stop systems in the same order as would be
    %% done if the ra application was terminated.
    lists:foreach(fun ensure_ra_system_stopped/1,
                  lists:reverse(all_ra_systems())),
    ?LOG_DEBUG(
       "Ra systems stopped",
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    ok.

-spec ensure_ra_system_stopped(ra_system_name()) -> ok | no_return().

ensure_ra_system_stopped(RaSystem) ->
    case ra_system:stop(RaSystem) of
        ok ->
            ok;
        {error, _} = Error ->
            ?LOG_ERROR(
               "Failed to stop Ra system \"~ts\": ~tp",
               [RaSystem, Error],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            throw(Error)
    end.
