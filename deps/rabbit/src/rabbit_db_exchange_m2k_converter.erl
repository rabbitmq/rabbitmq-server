%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_exchange_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([init_copy_to_khepri/3,
         copy_to_khepri/3,
         delete_from_khepri/3]).

-record(?MODULE, {}).

-spec init_copy_to_khepri(StoreId, MigrationId, Tables) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv},
      Priv :: #?MODULE{}.
%% @private

init_copy_to_khepri(_StoreId, _MigrationId, Tables) ->
    %% Clean up any previous attempt to copy the Mnesia table to Khepri.
    lists:foreach(fun clear_data_in_khepri/1, Tables),

    SubState = #?MODULE{},
    {ok, SubState}.

-spec copy_to_khepri(Table, Record, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
%% @private

copy_to_khepri(
  rabbit_exchange = Table, #exchange{} = Record, State) ->
    Name = Record#exchange.name,
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] key: ~0p",
       [Table, Name],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_exchange:khepri_exchange_path(Name),
    rabbit_db_m2k_converter:with_correlation_id(
      fun(CorrId) ->
              Extra = #{async => CorrId},
              ?LOG_DEBUG(
                 "Mnesia->Khepri data copy: [~0p] path: ~0p corr: ~0p",
                 [Table, Path, CorrId],
                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
              rabbit_khepri:put(Path, Record, Extra)
      end, State);
copy_to_khepri(rabbit_exchange_serial = Table,
               #exchange_serial{name = Resource, next = Serial},
               State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] key: ~0p",
       [Table, Resource],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = khepri_path:combine_with_conditions(
             rabbit_db_exchange:khepri_exchange_serial_path(Resource),
             [#if_node_exists{exists = false}]),
    rabbit_db_m2k_converter:with_correlation_id(
      fun(CorrId) ->
              Extra = #{async => CorrId},
              ?LOG_DEBUG(
                 "Mnesia->Khepri data copy: [~0p] path: ~0p corr: ~0p",
                 [Table, Path, CorrId],
                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
              rabbit_khepri:put(Path, Serial, Extra)
      end, State);
copy_to_khepri(Table, Record, State) ->
    ?LOG_DEBUG("Mnesia->Khepri unexpected record table ~0p record ~0p state ~0p",
               [Table, Record, State]),
    {error, unexpected_record}.

-spec delete_from_khepri(Table, Key, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
%% @private

delete_from_khepri(rabbit_exchange = Table, Key, State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_exchange:khepri_exchange_path(Key),
    rabbit_db_m2k_converter:with_correlation_id(
      fun(CorrId) ->
              Extra = #{async => CorrId},
              ?LOG_DEBUG(
                 "Mnesia->Khepri data delete: [~0p] path: ~0p corr: ~0p",
                 [Table, Path, CorrId],
                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
              rabbit_khepri:delete(Path, Extra)
      end, State);
delete_from_khepri(rabbit_exchange_serial = Table, Key, State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_exchange:khepri_exchange_serial_path(Key),
    rabbit_db_m2k_converter:with_correlation_id(
      fun(CorrId) ->
              Extra = #{async => CorrId},
              ?LOG_DEBUG(
                 "Mnesia->Khepri data delete: [~0p] path: ~0p corr: ~0p",
                 [Table, Path, CorrId],
                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
              rabbit_khepri:delete(Path, Extra)
      end, State).

-spec clear_data_in_khepri(Table) -> ok when
      Table :: atom().

clear_data_in_khepri(rabbit_exchange) ->
    rabbit_db_exchange:clear_exchanges_in_khepri();
clear_data_in_khepri(rabbit_exchange_serial) ->
    rabbit_db_exchange:clear_exchange_serials_in_khepri().
