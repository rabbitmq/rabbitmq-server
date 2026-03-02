%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_rh_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("rabbit_recent_history.hrl").

-export([
         get/1,
         insert/3,
         delete/0,
         delete/1
        ]).

-export([khepri_recent_history_path/1]).

-rabbit_mnesia_tables_to_khepri_db(
   [{?RH_TABLE, rabbit_db_rh_exchange_m2k_converter}]).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

get(XName) ->
    Path = khepri_recent_history_path(XName),
    case rabbit_khepri:get(Path) of
        {ok, Cached} ->
            Cached;
        _ ->
            []
    end.

%% -------------------------------------------------------------------
%% insert().
%% -------------------------------------------------------------------

insert(XName, Message, Length) ->
    Path = khepri_recent_history_path(XName),
    case rabbit_khepri:adv_get(Path) of
        {ok, #{Path := #{data := Cached0, payload_version := Vsn}}} ->
            Cached = add_to_cache(Cached0, Message, Length),
            Path1 = khepri_path:combine_with_conditions(
                      Path, [#if_payload_version{version = Vsn}]),
            Ret = rabbit_khepri:put(Path1, Cached),
            case Ret of
                ok ->
                    ok;
                {error, {khepri, mismatching_node, _}} ->
                    insert(XName, Message, Length);
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
    Path = khepri_recent_history_path(
             ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    rabbit_khepri:delete(Path).

delete(XName) ->
    Path = khepri_recent_history_path(XName),
    rabbit_khepri:delete(Path).

%% -------------------------------------------------------------------
%% paths
%% -------------------------------------------------------------------

khepri_recent_history_path(#resource{virtual_host = VHost, name = Name}) ->
    khepri_recent_history_path(VHost, Name).

khepri_recent_history_path(VHost, Name) ->
    ExchangePath = rabbit_db_exchange:khepri_exchange_path(VHost, Name),
    ExchangePath ++ [recent_history].
