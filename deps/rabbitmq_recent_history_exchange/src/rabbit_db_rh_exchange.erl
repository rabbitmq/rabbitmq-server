%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_rh_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("rabbit_recent_history.hrl").

-export([
         setup_schema/0,
         get/1,
         insert/3,
         delete/0,
         delete/1
        ]).

-export([khepri_recent_history_path/1,
         khepri_recent_history_path/0]).

-rabbit_mnesia_tables_to_khepri_db(
   [{?RH_TABLE, rabbit_db_rh_exchange_m2k_converter}]).

%% -------------------------------------------------------------------
%% setup_schema().
%% -------------------------------------------------------------------

setup_schema() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> setup_schema_in_mnesia() end,
        khepri => fun() -> ok end
       }).

setup_schema_in_mnesia() ->
    _ = mnesia:create_table(?RH_TABLE,
                            [{attributes, record_info(fields, cached)},
                             {record_name, cached},
                             {type, set}]),
    _ = mnesia:add_table_copy(?RH_TABLE, node(), ram_copies),
    rabbit_table:wait([?RH_TABLE]),
    ok.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

get(XName) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_in_mnesia(XName) end,
        khepri => fun() -> get_in_khepri(XName) end
       }).

get_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> get_in_mnesia_tx(XName) end).

get_in_khepri(XName) ->
    Path = khepri_recent_history_path(XName),
    case rabbit_khepri:get(Path) of
        {ok, Cached} ->
            Cached;
        _ ->
            []
    end.

get_in_mnesia_tx(XName) ->
    case mnesia:read(?RH_TABLE, XName) of
        [] ->
            [];
        [#cached{key = XName, content=Cached}] ->
            Cached
    end.

%% -------------------------------------------------------------------
%% insert().
%% -------------------------------------------------------------------

insert(XName, Message, Length) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> insert_in_mnesia(XName, Message, Length) end,
        khepri => fun() -> insert_in_khepri(XName, Message, Length) end
       }).

insert_in_mnesia(XName, Message, Length) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Cached = get_in_mnesia_tx(XName),
              insert_in_mnesia(XName, Cached, Message, Length)
      end).

insert_in_mnesia(Key, Cached, Message, undefined) ->
    insert0_in_mnesia(Key, Cached, Message, ?KEEP_NB);
insert_in_mnesia(Key, Cached, Message, {_Type, Length}) ->
    insert0_in_mnesia(Key, Cached, Message, Length).

insert0_in_mnesia(Key, Cached, Message, Length) ->
    mnesia:write(?RH_TABLE,
                 #cached{key     = Key,
                         content = [Message|lists:sublist(Cached, Length-1)]},
                 write).

insert_in_khepri(XName, Message, Length) ->
    Path = khepri_recent_history_path(XName),
    case rabbit_khepri:adv_get(Path) of
        {ok, #{data := Cached0, payload_version := DVersion}} ->
            Cached = add_to_cache(Cached0, Message, Length),
            Path1 = khepri_path:combine_with_conditions(
                      Path, [#if_payload_version{version = DVersion}]),
            Ret = rabbit_khepri:put(Path1, Cached),
            case Ret of
                ok ->
                    ok;
                {error, {khepri, mismatching_node, _}} ->
                    insert_in_khepri(XName, Message, Length);
                {error, _} = Error ->
                    Error
            end;
        _ ->
            Cached = add_to_cache([], Message, Length),
            rabbit_khepri:put(Path, Cached)
    end.

add_to_cache(Cached, Message, undefined) ->
    add_to_cache(Cached, Message, ?KEEP_NB);
add_to_cache(Cached, Message, {_Type, Length}) ->
    add_to_cache(Cached, Message, Length);
add_to_cache(Cached, Message, Length) ->
    [Message|lists:sublist(Cached, Length-1)].

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

delete() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia() end,
        khepri => fun() -> delete_in_khepri() end
       }).

delete_in_mnesia() ->
    mnesia:delete_table(?RH_TABLE).

delete_in_khepri() ->
    rabbit_khepri:delete(khepri_recent_history_path()).

delete(XName) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia(XName) end,
        khepri => fun() -> delete_in_khepri(XName) end
       }).

delete_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              mnesia:delete(?RH_TABLE, XName, write)
      end).

delete_in_khepri(XName) ->
    rabbit_khepri:delete(khepri_recent_history_path(XName)).

%% -------------------------------------------------------------------
%% paths
%% -------------------------------------------------------------------

khepri_recent_history_path() ->
    [?MODULE, recent_history_exchange].

khepri_recent_history_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, recent_history_exchange, VHost, Name].
