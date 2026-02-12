%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_msup_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include("mirrored_supervisor.hrl").

-export([init_copy_to_khepri/3,
         copy_to_khepri/3,
         delete_from_khepri/3]).

-ifdef(TEST).
-export([upgrade_mirrored_sup_record/1,
         amqqueue_v1_to_v2/1]).
-endif.

-record(?MODULE, {}).

%% The amqqueue_v1 record from before quorum queues (pre 3.8.0)
-define(AMQQUEUE_V1_TUPLE_SIZE, 19).

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

copy_to_khepri(mirrored_sup_childspec = Table,
               #mirrored_sup_childspec{key = {Group, _}} = Record0,
               State) ->
    Record = upgrade_mirrored_sup_record(Record0),
    #mirrored_sup_childspec{key = {Group, Id} = Key} = Record,
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_msup:khepri_mirrored_supervisor_path(Group, Id),
    rabbit_db_m2k_converter:with_correlation_id(
      fun(CorrId) ->
              Extra = #{async => CorrId},
              ?LOG_DEBUG(
                 "Mnesia->Khepri data copy: [~0p] path: ~0p corr: ~0p",
                 [Table, Path, CorrId],
                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
              rabbit_khepri:put(Path, Record, Extra)
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

delete_from_khepri(
  mirrored_sup_childspec = Table, {Group, Id0}, State) ->
    Id = maybe_upgrade_amqqueue(Id0),
    Key = {Group, Id},
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_msup:khepri_mirrored_supervisor_path(Group, Id),
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

clear_data_in_khepri(mirrored_sup_childspec) ->
    rabbit_db_msup:clear_in_khepri().

%% Upgrade any amqqueue_v1 records embedded in a mirrored_sup_childspec record
%% to the current amqqueue_v2 format. The v2 format was introduced in 3.8.0
%% together with quorum queues. The quorum_queue feature flag migrated records
%% in rabbit_queue and rabbit_durable_queue but not records embedded in
%% mirrored_sup_childspec. The federation queue link supervisor stores amqqueue
%% records as the mirrored supervisor child ID.
-spec upgrade_mirrored_sup_record(Record) -> Record when
      Record :: #mirrored_sup_childspec{}.
upgrade_mirrored_sup_record(
  #mirrored_sup_childspec{key = {Group, Id},
                          childspec = ChildSpec} = Record) ->
    Id1 = maybe_upgrade_amqqueue(Id),
    ChildSpec1 = upgrade_childspec(ChildSpec),
    Record#mirrored_sup_childspec{key = {Group, Id1},
                                  childspec = ChildSpec1}.

upgrade_childspec({Id, {M, F, Args}, Restart, Shutdown, Type, Modules}) ->
    Id1 = maybe_upgrade_amqqueue(Id),
    Args1 = [maybe_upgrade_amqqueue(A) || A <- Args],
    {Id1, {M, F, Args1}, Restart, Shutdown, Type, Modules};
upgrade_childspec(#{id := Id, start := {M, F, Args}} = Spec) ->
    Id1 = maybe_upgrade_amqqueue(Id),
    Args1 = [maybe_upgrade_amqqueue(A) || A <- Args],
    Spec#{id := Id1, start := {M, F, Args1}};
upgrade_childspec(Other) ->
    Other.

maybe_upgrade_amqqueue(Term) when
      is_tuple(Term),
      tuple_size(Term) =:= ?AMQQUEUE_V1_TUPLE_SIZE,
      element(1, Term) =:= amqqueue ->
    amqqueue_v1_to_v2(Term);
maybe_upgrade_amqqueue(Term) ->
    Term.

-spec amqqueue_v1_to_v2(tuple()) -> amqqueue:amqqueue().
amqqueue_v1_to_v2(V1Queue) when
      tuple_size(V1Queue) =:= ?AMQQUEUE_V1_TUPLE_SIZE,
      element(1, V1Queue) =:= amqqueue ->
    Fields = erlang:tuple_to_list(V1Queue) ++ [rabbit_classic_queue, #{}],
    V2Queue = erlang:list_to_tuple(Fields),
    true = ?is_amqqueue(V2Queue),
    V2Queue.
