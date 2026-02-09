%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2026 Broadcom. All Rights Reserved. The term “Broadcom”
%% refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_binding_m2k_converter).

-behaviour(rabbit_db_m2k_converter).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("khepri/include/khepri.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([init_copy_to_khepri/4,
         copy_to_khepri/3,
         delete_from_khepri/3]).

-define(USE_TABLE_PRIV_KEY, {?MODULE, use_table}).

-spec init_copy_to_khepri(StoreId, MigrationId, Tables, State) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, State}.
%% @private

init_copy_to_khepri(_StoreId, _MigrationId, Tables, State) ->
    %% Clean up any previous attempt to copy the Mnesia table to Khepri.
    lists:foreach(fun clear_data_in_khepri/1, Tables),

    UseTable = case mnesia:table_info(rabbit_route, size) of
                   0            -> rabbit_durable_route;
                   N when N > 0 -> rabbit_route
               end,
    ?assert(lists:member(UseTable, Tables)),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: use table ~s in ~s",
       [UseTable, ?MODULE],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    State1 = rabbit_db_m2k_converter:set_priv_data(
               ?USE_TABLE_PRIV_KEY, UseTable, State),
    {ok, State1}.

get_priv_data(State) ->
    rabbit_db_m2k_converter:get_priv_data(?USE_TABLE_PRIV_KEY, State).

-spec copy_to_khepri(Table, Record, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
%% @private

copy_to_khepri(Table, #route{binding = #binding{} = Binding}, State)
  when Table =:= rabbit_route orelse Table =:= rabbit_durable_route ->
    UseTable = get_priv_data(State),
    case Table of
        UseTable ->
            ?LOG_DEBUG(
               "Mnesia->Khepri data copy: [~0p] key: ~0p",
               [Table, Binding],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            Path = rabbit_db_binding:khepri_route_path(Binding),
            rabbit_db_m2k_converter:with_correlation_id(
              fun(CorrId) ->
                      Extra = #{async => CorrId},
                      rabbit_khepri:transaction(
                        fun() ->
                                %% Add the binding to the set at the binding's
                                %% path.
                                Set = case khepri_tx:get(Path) of
                                          {ok, Set0} ->
                                              Set0;
                                          _ ->
                                              sets:new([{version, 2}])
                                      end,
                                khepri_tx:put(
                                  Path, sets:add_element(Binding, Set))
                        end, rw, Extra)
              end, State);
        _ ->
            {ok, State}
    end;
copy_to_khepri(Table, Record, State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri unexpected record table ~0p record ~0p state ~0p",
       [Table, Record, State],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {error, unexpected_record}.

-spec delete_from_khepri(Table, Key, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
%% @private

delete_from_khepri(Table, Key, State)
  when Table =:= rabbit_route orelse Table =:= rabbit_durable_route ->
    UseTable = get_priv_data(State),
    case Table of
        UseTable ->
            ?LOG_DEBUG(
               "Mnesia->Khepri data delete: [~0p] key: ~0p",
               [Table, Key],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            Path = rabbit_db_binding:khepri_route_path(Key),
            rabbit_db_m2k_converter:with_correlation_id(
              fun(CorrId) ->
                      Extra = #{async => CorrId},
                      rabbit_khepri:delete(Path, Extra)
              end, State);
        _ ->
            {ok, State}
    end.

-spec clear_data_in_khepri(Table) -> ok when
      Table :: atom().

clear_data_in_khepri(Table)
  when Table =:= rabbit_route orelse Table =:= rabbit_durable_route ->
    rabbit_db_binding:clear_in_khepri().
