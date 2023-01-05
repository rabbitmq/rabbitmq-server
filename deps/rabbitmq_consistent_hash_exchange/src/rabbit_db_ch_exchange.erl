%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_db_ch_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("rabbitmq_consistent_hash_exchange.hrl").

-export([
         setup_schema/0,
         create/1,
         create_binding/4,
         get/1,
         delete/1,
         delete_bindings/2
        ]).

-export([
         khepri_consistent_hash_path/0,
         khepri_consistent_hash_path/1
        ]).

-define(HASH_RING_STATE_TABLE, rabbit_exchange_type_consistent_hash_ring_state).

-rabbit_mnesia_tables_to_khepri_db(
   [{?HASH_RING_STATE_TABLE, rabbit_db_ch_exchange_m2k_converter}]).

setup_schema() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> setup_schema_in_mnesia() end,
        khepri => ok
       }).

setup_schema_in_mnesia() ->
    _ = mnesia:create_table(?HASH_RING_STATE_TABLE, [{record_name, chx_hash_ring},
                                                 {attributes, record_info(fields, chx_hash_ring)},
                                                 {type, ordered_set}]),
    _ = mnesia:add_table_copy(?HASH_RING_STATE_TABLE, node(), ram_copies),
    rabbit_table:wait([?HASH_RING_STATE_TABLE]).

create(X) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> create_in_mnesia(X) end,
        khepri => fun() -> create_in_khepri(X) end
       }).

create_in_mnesia(X) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> create_in_mnesia_tx(X) end).

create_in_mnesia_tx(X) ->
    case mnesia:read(?HASH_RING_STATE_TABLE, X) of
        [_] -> ok;
        []  ->
            rabbit_log:debug("Consistent hashing exchange: will initialise hashing ring schema database record"),
            mnesia:write_lock_table(?HASH_RING_STATE_TABLE),
            ok = mnesia:write(?HASH_RING_STATE_TABLE, #chx_hash_ring{
                                                         exchange = X,
                                                         next_bucket_number = 0,
                                                         bucket_map = #{}}, write)
    end.

create_in_khepri(X) ->
    Path = khepri_consistent_hash_path(X),
    case rabbit_khepri:create(Path, #chx_hash_ring{exchange = X,
                                                   next_bucket_number = 0,
                                                   bucket_map = #{}}) of
        ok -> ok;
        {error, {khepri, mismatching_node, _}} -> ok;
        Error -> Error
    end.

create_binding(Src, Dst, Weight, UpdateFun) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> create_binding_in_mnesia(Src, Dst, Weight, UpdateFun) end,
        khepri => fun() -> create_binding_in_khepri(Src, Dst, Weight, UpdateFun) end
       }).

create_binding_in_mnesia(Src, Dst, Weight, UpdateFun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              create_binding_in_mnesia_tx(Src, Dst, Weight, UpdateFun)
      end).

create_binding_in_mnesia_tx(Src, Dst, Weight, UpdateFun) ->
    case mnesia:read(?HASH_RING_STATE_TABLE, Src) of
        [Chx0] ->
            case UpdateFun(Chx0, Dst, Weight) of
                already_exists ->
                    already_exists;
                Chx ->
                    ok = mnesia:write(?HASH_RING_STATE_TABLE, Chx, write),
                    created
            end;
        [] ->
            create_in_mnesia_tx(Src),
            create_binding_in_mnesia_tx(Src, Dst, Weight, UpdateFun)
    end.

create_binding_in_khepri(Src, Dst, Weight, UpdateFun) ->
    Path = khepri_consistent_hash_path(Src),
    case rabbit_khepri:adv_get(Path) of
        {ok, #{data := Chx0, payload_version := DVersion}} ->
            case UpdateFun(Chx0, Dst, Weight) of
                already_exists ->
                    already_exists;
                Chx ->
                    Path1 = khepri_path:combine_with_conditions(
                              Path, [#if_payload_version{version = DVersion}]),
                    Ret2 = rabbit_khepri:put(Path1, Chx),
                    case Ret2 of
                        ok ->
                            created;
                        {error, {khepri, mismatching_node, _}} ->
                            create_binding_in_khepri(Src, Dst, Weight, UpdateFun);
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
                    create_binding_in_khepri(Src, Dst, Weight, UpdateFun);
                Error -> throw(Error)
            end
    end.

get(XName) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_in_mnesia(XName) end,
        khepri => fun() -> get_in_khepri(XName) end
       }).

get_in_mnesia(XName) ->
    case ets:lookup(?HASH_RING_STATE_TABLE, XName) of
        []  ->
            undefined;
        [Chx] ->
            Chx
    end.

get_in_khepri(XName) ->
    Path = khepri_consistent_hash_path(XName),
    case rabbit_khepri:get(Path) of
        {ok, Chx} ->
            Chx;
        _ ->
            undefined
    end.

delete(XName) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia(XName) end,
        khepri => fun() -> delete_in_khepri(XName) end
       }).

delete_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              mnesia:write_lock_table(?HASH_RING_STATE_TABLE),
              mnesia:delete({?HASH_RING_STATE_TABLE, XName})
      end).

delete_in_khepri(XName) ->
    rabbit_khepri:delete(khepri_consistent_hash_path(XName)).

delete_bindings(Bindings, DeleteFun) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_bindings_in_mnesia(Bindings, DeleteFun) end,
        khepri => fun() -> delete_bindings_in_khepri(Bindings, DeleteFun) end
       }).

delete_bindings_in_mnesia(Bindings, DeleteFun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              [delete_binding_in_mnesia(Binding, DeleteFun) || Binding <- Bindings]
      end).

delete_binding_in_mnesia(#binding{source = S, destination = D, key = RK}, DeleteFun) ->
    rabbit_log:debug("Consistent hashing exchange: removing binding "
                     "from exchange ~ts to destination ~ts with routing key '~ts'",
                     [rabbit_misc:rs(S), rabbit_misc:rs(D), RK]),
    case mnesia:read(?HASH_RING_STATE_TABLE, S) of
        [Chx0] ->
            case DeleteFun(Chx0, D) of
                not_found ->
                    ok;
                Chx ->
                    ok = mnesia:write(?HASH_RING_STATE_TABLE, Chx, write)
            end;
        [] ->
            {not_found, S}
    end.

delete_bindings_in_khepri(Bindings, DeleteFun) ->
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
    [?MODULE, exchange_type_consistent_hash_ring_state, VHost, Name].

khepri_consistent_hash_path() ->
    [?MODULE, exchange_type_consistent_hash_ring_state].
