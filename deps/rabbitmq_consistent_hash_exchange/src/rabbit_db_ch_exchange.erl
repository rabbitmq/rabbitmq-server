%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_db_ch_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("rabbitmq_consistent_hash_exchange.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
         create/1,
         create_binding/4,
         get/1,
         delete/1,
         delete_bindings/2
        ]).

-export([
         khepri_consistent_hash_path/1,
         khepri_consistent_hash_path/2
        ]).

-define(HASH_RING_STATE_TABLE, rabbit_exchange_type_consistent_hash_ring_state).

-rabbit_mnesia_tables_to_khepri_db(
   [{?HASH_RING_STATE_TABLE, rabbit_db_ch_exchange_m2k_converter}]).

create(X) ->
    Path = khepri_consistent_hash_path(X),
    case rabbit_khepri:create(Path, #chx_hash_ring{exchange = X,
                                                   next_bucket_number = 0,
                                                   bucket_map = #{}}) of
        ok -> ok;
        {error, {khepri, mismatching_node, _}} -> ok;
        Error -> Error
    end.

create_binding(Src, Dst, Weight, UpdateFun) ->
    Path = khepri_consistent_hash_path(Src),
    case rabbit_khepri:adv_get(Path) of
        {ok, #{Path := #{data := Chx0, payload_version := Vsn}}} ->
            case UpdateFun(Chx0, Dst, Weight) of
                already_exists ->
                    already_exists;
                Chx ->
                    Path1 = khepri_path:combine_with_conditions(
                              Path, [#if_payload_version{version = Vsn}]),
                    Ret2 = rabbit_khepri:put(Path1, Chx),
                    case Ret2 of
                        ok ->
                            created;
                        {error, {khepri, mismatching_node, _}} ->
                            create_binding(Src, Dst, Weight, UpdateFun);
                        {error, _} = Error ->
                            Error
                    end
            end;
        _ ->
            case rabbit_khepri:create(Path, #chx_hash_ring{exchange = Src,
                                                       next_bucket_number = 0,
                                                       bucket_map = #{}}) of
                ok -> ok;
                {error, {khepri, mismatching_node, _}} ->
                    create_binding(Src, Dst, Weight, UpdateFun);
                Error -> throw(Error)
            end
    end.

get(XName) ->
    Path = khepri_consistent_hash_path(XName),
    case rabbit_khepri:get(Path) of
        {ok, Chx} ->
            Chx;
        _ ->
            undefined
    end.

delete(XName) ->
    rabbit_khepri:delete(khepri_consistent_hash_path(XName)).

delete_bindings(Bindings, DeleteFun) ->
    rabbit_khepri:transaction(
      fun() ->
              [delete_binding_in_khepri(Binding, DeleteFun) || Binding <- Bindings]
      end).

delete_binding_in_khepri(#binding{source = S, destination = D}, DeleteFun) ->
    Path = khepri_consistent_hash_path(S),
    case khepri_tx:get(Path) of
        {ok, Chx0} ->
            case DeleteFun(Chx0, D) of
                not_found ->
                    ok;
                Chx ->
                    ok = khepri_tx:put(Path, Chx)
            end;
        _ ->
            {not_found, S}
    end.

khepri_consistent_hash_path(#exchange{name = Name}) ->
    khepri_consistent_hash_path(Name);
khepri_consistent_hash_path(#resource{virtual_host = VHost, name = Name}) ->
    khepri_consistent_hash_path(VHost, Name).

khepri_consistent_hash_path(VHost, Name) ->
    ExchangePath = rabbit_db_exchange:khepri_exchange_path(VHost, Name),
    ExchangePath ++ [consistent_hash_ring_state].
