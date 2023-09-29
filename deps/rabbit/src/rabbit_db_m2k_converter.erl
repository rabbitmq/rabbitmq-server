%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([init_copy_to_khepri/4,
         copy_to_khepri/3,
         delete_from_khepri/3]).

-type migration() :: {mnesia_to_khepri:mnesia_table(),
                      mnesia_to_khepri:converter_mod()}.

-type migrations() :: [migration()].

-record(?MODULE, {migrations :: migrations(),
                  sub_states :: #{module() => any()}}).

-spec init_copy_to_khepri(StoreId, MigrationId, Tables, Migrations) ->
    Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Migrations :: migrations(),
      Ret :: {ok, Priv},
      Priv :: #?MODULE{}.
%% @private

init_copy_to_khepri(StoreId, MigrationId, _Tables, Migrations) ->
    TablesPerMod = lists:foldl(
                     fun
                         ({Table, Mod}, Acc) ->
                             Tables0 = maps:get(Mod, Acc, []),
                             Tables1 = Tables0 ++ [Table],
                             Acc#{Mod => Tables1};
                         (_Table, Acc) ->
                             Acc
                     end, #{}, Migrations),

    SubStates = maps:fold(
                  fun(Mod, Tables, Acc) ->
                          {ok, SubState} =
                          case Mod of
                              {ActualMod, Args} ->
                                  ActualMod:init_copy_to_khepri(
                                    StoreId, MigrationId,
                                    Tables, Args);
                              _ ->
                                  Mod:init_copy_to_khepri(
                                    StoreId, MigrationId,
                                    Tables)
                          end,
                          Acc#{Mod => SubState}
                  end, #{}, TablesPerMod),

    State = #?MODULE{migrations = Migrations,
                     sub_states = SubStates},
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
  Table, Record,
  #?MODULE{migrations = Migrations, sub_states = SubStates} = State) ->
    case proplists:get_value(Table, Migrations) of
        true ->
            {ok, State};
        Mod when Mod =/= undefined ->
            ActualMod = actual_mod(Mod),
            SubState = maps:get(Mod, SubStates),
            case ActualMod:copy_to_khepri(Table, Record, SubState) of
                {ok, SubState1} ->
                    SubStates1 = SubStates#{Mod => SubState1},
                    State1 = State#?MODULE{sub_states = SubStates1},
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end
    end.

-spec delete_from_khepri(Table, Key, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      Priv :: #?MODULE{},
      Ret :: {ok, NewPriv} | {error, Reason},
      NewPriv :: #?MODULE{},
      Reason :: any().
%% @private

delete_from_khepri(
  Table, Key,
  #?MODULE{migrations = Migrations, sub_states = SubStates} = State) ->
    case proplists:get_value(Table, Migrations) of
        true ->
            {ok, State};
        Mod when Mod =/= undefined ->
            ActualMod = actual_mod(Mod),
            SubState = maps:get(Mod, SubStates),
            case ActualMod:delete_from_khepri(Table, Key, SubState) of
                {ok, SubState1} ->
                    SubStates1 = SubStates#{Mod => SubState1},
                    State1 = State#?MODULE{sub_states = SubStates1},
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end
    end.

actual_mod({Mod, _}) -> Mod;
actual_mod(Mod)      -> Mod.
