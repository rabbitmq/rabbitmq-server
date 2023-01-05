%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_jms_exchange_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("rabbit_jms_topic_exchange.hrl").

-export([init_copy_to_khepri/3,
         copy_to_khepri/3,
         delete_from_khepri/3,
         clear_data_in_khepri/1]).

-record(?MODULE, {store_id :: khepri:store_id()}).

-spec init_copy_to_khepri(StoreId, MigrationId, Tables) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv},
      Priv :: #?MODULE{}.
%% @private

init_copy_to_khepri(StoreId, _MigrationId, _Tables) ->
    State = #?MODULE{store_id = StoreId},
    {ok, State}.

-spec copy_to_khepri(Table, Record, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      Priv :: #?MODULE{},
      Ret :: {ok, NewPriv} | {error, Reason},
      NewPriv :: #?MODULE{},
      Reason :: any().
%% @private

copy_to_khepri(
  ?JMS_TOPIC_TABLE = Table, #?JMS_TOPIC_RECORD{x_name = XName, x_selector_funs = BFuns},
  #?MODULE{store_id = StoreId} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] key: ~0p",
       [Table, XName],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_jms_exchange:khepri_jms_topic_exchange_path(XName),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] path: ~0p",
       [Table, Path],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case khepri:put(StoreId, Path, BFuns) of
        ok    -> {ok, State};
        Error -> Error
    end;
copy_to_khepri(Table, Record, State) ->
    ?LOG_DEBUG("Mnesia->Khepri unexpected record table ~0p record ~0p state ~0p",
               [Table, Record, State]),
    {error, unexpected_record}.

-spec delete_from_khepri(Table, Key, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      Priv :: #?MODULE{},
      Ret :: {ok, NewPriv} | {error, Reason},
      NewPriv :: #?MODULE{},
      Reason :: any().
%% @private

delete_from_khepri(?JMS_TOPIC_TABLE = Table, Key,
                   #?MODULE{store_id = StoreId} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_jms_exchange:khepri_jms_topic_exchange_path(Key),
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] path: ~0p",
       [Table, Path],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case khepri:delete(StoreId, Path) of
        ok    -> {ok, State};
        Error -> Error
    end.

clear_data_in_khepri(?JMS_TOPIC_TABLE) ->
    Path = rabbit_db_jms_exchange:khepri_jms_topic_exchange_path(),
    case rabbit_khepri:delete(Path) of
        ok ->
            ok;
        Error ->
            throw(Error)
    end.
