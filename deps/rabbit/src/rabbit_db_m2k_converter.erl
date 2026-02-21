%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_m2k_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("khepri/include/khepri.hrl").
-include_lib("khepri_mnesia_migration/src/kmm_logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-callback init_copy_to_khepri(StoreId, MigrationId, Tables, State) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().

-callback copy_to_khepri(Table, Record, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().

-callback delete_from_khepri(Table, Key, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().

%% Functions for `rabbit_db_*_m2k_converter' modules to call.
-export([get_priv_data/2,
         set_priv_data/3,
         with_correlation_id/2]).

%% `mnesia_to_khepri_converter' callbacks.
-export([init_copy_to_khepri/4,
         copy_to_khepri/3,
         delete_from_khepri/3,
         finish_copy_to_khepri/1]).

-define(MAX_ASYNC_REQUESTS, 64).

-type migration() :: {mnesia_to_khepri:mnesia_table(),
                      mnesia_to_khepri:converter_mod()}.

-type migrations() :: [migration()].

-type correlation_id() :: non_neg_integer().

-type async_request_fun() :: fun((correlation_id()) -> ok | {error, any()}).

-record(?MODULE, {migrations :: migrations(),
                  priv = #{} :: map(),
                  seq_no = 0 :: correlation_id(),
                  last_acked_seq_no = 0 :: correlation_id(),
                  async_requests = #{} :: #{correlation_id() =>
                                            async_request_fun()}}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

%% `mnesia_to_khepri_converter' callbacks

-spec init_copy_to_khepri(StoreId, MigrationId, Tables, Migrations) ->
    Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Migrations :: migrations(),
      Ret :: {ok, State},
      State :: rabbit_db_m2k_converter:state().
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

    State1 = #?MODULE{migrations = Migrations},
    State2 = maps:fold(
                  fun(Mod, Tables, St) ->
                          {ok, St1} =
                          case Mod of
                              {ActualMod, Args} ->
                                  ActualMod:init_copy_to_khepri(
                                    StoreId, MigrationId,
                                    Tables, Args, St);
                              _ ->
                                  Mod:init_copy_to_khepri(
                                    StoreId, MigrationId,
                                    Tables, St)
                          end,
                          St1
                  end, State1, TablesPerMod),

    {ok, State2}.

-spec copy_to_khepri(Table, Record, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
%% @private

copy_to_khepri(
  Table, Record, #?MODULE{migrations = Migrations} = State) ->
    case proplists:get_value(Table, Migrations) of
        true ->
            {ok, State};
        Mod when Mod =/= undefined ->
            ActualMod = actual_mod(Mod),
            case ActualMod:copy_to_khepri(Table, Record, State) of
                {ok, State1} ->
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end
    end.

-spec delete_from_khepri(Table, Key, State) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().
%% @private

delete_from_khepri(
  Table, Key, #?MODULE{migrations = Migrations} = State) ->
    case proplists:get_value(Table, Migrations) of
        true ->
            {ok, State};
        Mod when Mod =/= undefined ->
            ActualMod = actual_mod(Mod),
            case ActualMod:delete_from_khepri(Table, Key, State) of
                {ok, State1} ->
                    {ok, State1};
                {error, _} = Error ->
                    Error
            end
    end.

-spec finish_copy_to_khepri(State) -> Ret when
      State :: rabbit_db_m2k_converter:state(),
      Ret :: ok.
%% @private

finish_copy_to_khepri(State) ->
    {ok, _} = wait_for_all_async_requests(State),
    ok.

-spec get_priv_data(Key, State) -> Value when
      Key :: any(),
      State :: rabbit_db_m2k_converter:state(),
      Value :: any().
%% @private

get_priv_data(Key, #?MODULE{priv = Priv}) ->
    Value = maps:get(Key, Priv),
    Value.

-spec set_priv_data(Key, Value, State) -> NewState when
      Key :: any(),
      Value :: any(),
      State :: rabbit_db_m2k_converter:state(),
      NewState :: rabbit_db_m2k_converter:state().
%% @private

set_priv_data(Key, Value, #?MODULE{priv = Priv} = State) ->
    Priv1 = Priv#{Key => Value},
    State1 = State#?MODULE{priv = Priv1},
    State1.

-spec with_correlation_id(Fun, State) -> Ret when
      Fun :: async_request_fun(),
      State :: rabbit_db_m2k_converter:state(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: rabbit_db_m2k_converter:state(),
      Reason :: any().

with_correlation_id(
  Fun,
  #?MODULE{seq_no = SeqNo0,
           last_acked_seq_no = LastAckedSeqNo} = State) ->
    case SeqNo0 - LastAckedSeqNo >= ?MAX_ASYNC_REQUESTS of
        true ->
            case wait_for_finished_async_requests(State) of
                {ok, State1} ->
                    with_correlation_id(Fun, State1);
                {error, _} = Error ->
                    Error
            end;
        false ->
            run_async_fun(Fun, State)
    end.

wait_for_finished_async_requests(State) ->
    wait_for_async_requests(State, 0).

wait_for_all_async_requests(State) ->
    wait_for_async_requests(State, infinity).

-spec wait_for_async_requests(State, Timeout) -> Ret when
      State :: state(),
      Timeout :: timeout(),
      Ret :: {ok, NewState} | {error, Reason},
      NewState :: state(),
      Reason :: any().

wait_for_async_requests(
  #?MODULE{async_requests = AsyncRequests} = State,
  Timeout)
  when AsyncRequests =/= #{} ->
    receive
        {ra_event, _, _} = RaEvent ->
            case handle_ra_event(RaEvent, State) of
                {ok, State1} ->
                    wait_for_async_requests(State1, Timeout);
                {error, _} = Error ->
                    Error
            end
    after Timeout ->
              {ok, State}
    end;
wait_for_async_requests(State, _Timeout) ->
    {ok, State}.

handle_ra_event(RaEvent, State) ->
    Correlations = rabbit_khepri:handle_async_ret(RaEvent),
    lists:foldl(
      fun({CorrelationId, Result}, {ok, State1}) ->
              #?MODULE{async_requests = AsyncRequests,
                       last_acked_seq_no = LastAcked} = State1,
              {Fun, AsyncRequests1} = maps:take(
                                        CorrelationId, AsyncRequests),
              LastAcked1 = erlang:max(CorrelationId, LastAcked),
              State2 = State1#?MODULE{last_acked_seq_no = LastAcked1,
                                      async_requests = AsyncRequests1},
              case Result of
                  ok ->
                      {ok, State2};
                  {ok, _} ->
                      {ok, State2};
                  {error, not_leader} ->
                      %% If the command failed because it was sent to
                      %% a non-leader member, retry the fun.
                      %% `rabbit_khepri:handle_async_ret/1' has updated
                      %% the leader information, so the next attempt
                      %% might be sent to the correct member.
                      run_async_fun(Fun, State2);
                  {error, _} = Error ->
                      Error
              end;
         (_Correlation, {error, _} = Error) ->
              Error
      end, {ok, State}, Correlations).

run_async_fun(
  Fun,
  #?MODULE{seq_no = SeqNo0,
           async_requests = AsyncRequests0} = State0) ->
    SeqNo = SeqNo0 + 1,
    case Fun(SeqNo) of
        ok ->
            AsyncRequests = AsyncRequests0#{SeqNo => Fun},
            State = State0#?MODULE{seq_no = SeqNo,
                                   async_requests = AsyncRequests},
            {ok, State};
        {error, _} = Error ->
            Error
    end.

actual_mod({Mod, _}) -> Mod;
actual_mod(Mod)      -> Mod.
