%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%% -----------------------------------------------------------------------------
-module(rabbit_db_jms_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("rabbit_jms_topic_exchange.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
         create_or_update/3,
         insert/2,
         get/1,
         delete/1,
         delete/3
        ]).

-export([
         khepri_jms_topic_exchange_path/1,
         khepri_jms_topic_exchange_path/2
        ]).

-rabbit_mnesia_tables_to_khepri_db(
     [{?JMS_TOPIC_TABLE, rabbit_db_jms_exchange_m2k_converter}]).

%% -------------------------------------------------------------------
%% create_or_update().
%% -------------------------------------------------------------------

create_or_update(XName, BindingKeyAndFun, ErrorFun) ->
    do_update(XName, BindingKeyAndFun, fun put_item/2, ErrorFun).

do_update(XName, BindingKeyAndFun, UpdateFun, ErrorFun) ->
    Path = khepri_jms_topic_exchange_path(XName),
    case rabbit_khepri:adv_get(Path) of
        {ok, #{Path := #{data := BindingFuns, payload_version := Vsn}}} ->
            Path1 = khepri_path:combine_with_conditions(
                      Path, [#if_payload_version{version = Vsn}]),
            Ret = rabbit_khepri:put(Path1, UpdateFun(BindingFuns, BindingKeyAndFun)),
            case Ret of
                ok -> ok;
                {error, {khepri, mismatching_node, _}} ->
                    do_update(XName, BindingKeyAndFun, UpdateFun, ErrorFun);
                {error, _} ->
                    ErrorFun(XName)
            end;
        _Err ->
            ErrorFun(XName)
    end.

%% -------------------------------------------------------------------
%% insert().
%% -------------------------------------------------------------------

insert(XName, BFuns) ->
    ok = rabbit_khepri:put(khepri_jms_topic_exchange_path(XName), BFuns).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

get(XName) ->
    case rabbit_khepri:get(khepri_jms_topic_exchange_path(XName)) of
        {ok, BindingFuns} ->
            BindingFuns;
        _ ->
            not_found
    end.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

delete(XName) ->
    rabbit_khepri:delete(khepri_jms_topic_exchange_path(XName)).

delete(XName, BindingKeys, ErrorFun) ->
    do_update(XName, BindingKeys, fun remove_items/2, ErrorFun).

%% -------------------------------------------------------------------
%% dictionary handling
%% -------------------------------------------------------------------

% add an item to the dictionary of binding functions
put_item(Dict, {Key, Item}) -> dict:store(Key, Item, Dict).

% remove a list of keyed items from the dictionary, by key
remove_items(Dict, []) -> Dict;
remove_items(Dict, [Key | Keys]) -> remove_items(dict:erase(Key, Dict), Keys).

%% -------------------------------------------------------------------
%% paths
%% -------------------------------------------------------------------

khepri_jms_topic_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    khepri_jms_topic_exchange_path(VHost, Name).

khepri_jms_topic_exchange_path(VHost, Name) ->
    ExchangePath = rabbit_db_exchange:khepri_exchange_path(VHost, Name),
    ExchangePath ++ [jms_topic].
