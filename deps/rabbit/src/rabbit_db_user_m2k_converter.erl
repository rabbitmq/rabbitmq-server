%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_user_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([init_copy_to_khepri/3,
         copy_to_khepri/3,
         delete_from_khepri/3]).

-record(?MODULE, {store_id :: khepri:store_id()}).

-spec init_copy_to_khepri(StoreId, MigrationId, Tables) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv},
      Priv :: #?MODULE{}.
%% @private

init_copy_to_khepri(StoreId, _MigrationId, Tables) ->
    %% Clean up any previous attempt to copy the Mnesia table to Khepri.
    lists:foreach(fun clear_data_in_khepri/1, Tables),

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
  rabbit_user = Table, Record,
  #?MODULE{store_id = StoreId} = State) ->
    Username = internal_user:get_username(Record),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] key: ~0p",
       [Table, Username],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_user:khepri_user_path(Username),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] path: ~0p",
       [Table, Path],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case khepri:put(StoreId, Path, Record) of
        ok    -> {ok, State};
        Error -> Error
    end;
copy_to_khepri(
  rabbit_user_permission = Table, Record,
  #?MODULE{store_id = StoreId} = State) ->
    #user_permission{
       user_vhost = #user_vhost{
                       username = Username,
                       virtual_host = VHost}} = Record,
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] user: ~0p vhost: ~0p",
       [Table, Username, VHost],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_user:khepri_user_permission_path(
             #if_all{conditions =
                         [Username,
                          #if_node_exists{exists = true}]},
             VHost),
    Extra = #{keep_while =>
                  #{rabbit_db_vhost:khepri_vhost_path(VHost) =>
                        #if_node_exists{exists = true}}},
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] path: ~0p",
       [Table, Path],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case khepri:put(StoreId, Path, Record, Extra) of
        ok    -> {ok, State};
        Error -> Error
    end;
copy_to_khepri(
  rabbit_topic_permission = Table, Record,
  #?MODULE{store_id = StoreId} = State) ->
    #topic_permission{
       topic_permission_key =
           #topic_permission_key{
              user_vhost = #user_vhost{
                              username = Username,
                              virtual_host = VHost},
              exchange = Exchange}} = Record,
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] user: ~0p vhost: ~0p",
       [Table, Username, VHost],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_user:khepri_topic_permission_path(
             #if_all{conditions =
                         [Username,
                          #if_node_exists{exists = true}]},
             VHost,
             Exchange),
    Extra = #{keep_while =>
                  #{rabbit_db_vhost:khepri_vhost_path(VHost) =>
                        #if_node_exists{exists = true}}},
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] path: ~0p",
       [Table, Path],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case khepri:put(StoreId, Path, Record, Extra) of
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

delete_from_khepri(
  rabbit_user = Table, Key,
  #?MODULE{store_id = StoreId} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_user:khepri_user_path(Key),
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] path: ~0p",
       [Table, Path],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case khepri:delete(StoreId, Path) of
        ok    -> {ok, State};
        Error -> Error
    end;
delete_from_khepri(
  rabbit_user_permission = Table, Key,
  #?MODULE{store_id = StoreId} = State) ->
    #user_vhost{
       username = Username,
       virtual_host = VHost} = Key,
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_user:khepri_user_permission_path(Username, VHost),
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] path: ~0p",
       [Table, Path],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case khepri:delete(StoreId, Path) of
        ok    -> {ok, State};
        Error -> Error
    end;
delete_from_khepri(
  rabbit_topic_permission = Table, Key,
  #?MODULE{store_id = StoreId} = State) ->
    #topic_permission_key{
       user_vhost = #user_vhost{
                       username = Username,
                       virtual_host = VHost},
       exchange = Exchange} = Key,
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_user:khepri_topic_permission_path(Username, VHost, Exchange),
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] path: ~0p",
       [Table, Path],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case khepri:delete(StoreId, Path) of
        ok    -> {ok, State};
        Error -> Error
    end.

-spec clear_data_in_khepri(Table) -> ok when
      Table :: atom().

clear_data_in_khepri(rabbit_user) ->
    Path = rabbit_db_user:khepri_users_path(),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end;
clear_data_in_khepri(_) ->
    ok.
