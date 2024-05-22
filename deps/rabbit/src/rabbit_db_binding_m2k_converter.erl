%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_binding_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").
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

    State = #?MODULE{},
    {ok, State}.

-spec copy_to_khepri(Table, Record, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
%% @private

copy_to_khepri(rabbit_route = Table,
               #route{binding = #binding{source = XName} = Binding},
               State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [~0p] key: ~0p",
       [Table, Binding],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_binding:khepri_route_path(Binding),
    rabbit_db_m2k_converter:with_correlation_id(
      fun(CorrId) ->
              Extra = #{async => CorrId},
              XPath = rabbit_db_binding:khepri_route_exchange_path(XName),
              ?LOG_DEBUG(
                 "Mnesia->Khepri data copy: [~0p] path: ~0p corr: ~0p",
                 [Table, Path, CorrId],
                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
              rabbit_khepri:transaction(
                fun() ->
                        %% Store the exchange's type in the exchange name
                        %% branch of the tree.
                        [#exchange{type = XType}] =
                            rabbit_db_exchange:get_in_khepri_tx(XName),
                        ok = khepri_tx:put(XPath, #{type => XType}),
                        %% Add the binding to the set at the binding's
                        %% path.
                        Set = case khepri_tx:get(Path) of
                                  {ok, Set0} ->
                                      Set0;
                                  _ ->
                                      sets:new([{version, 2}])
                              end,
                        khepri_tx:put(Path, sets:add_element(Binding, Set))
                end, rw, Extra)
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

delete_from_khepri(rabbit_route = Table, Key, State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data delete: [~0p] key: ~0p",
       [Table, Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = rabbit_db_binding:khepri_route_path(Key),
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

clear_data_in_khepri(rabbit_route) ->
    Path = rabbit_db_binding:khepri_routes_path(),
    case rabbit_khepri:delete(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.
