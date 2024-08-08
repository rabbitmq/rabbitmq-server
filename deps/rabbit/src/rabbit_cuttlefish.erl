%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_cuttlefish).

-export([
    aggregate_props/2,
    aggregate_props/3,

    optionally_tagged_binary/2,
    optionally_tagged_string/2
]).

-type keyed_props() :: [{binary(), [{binary(), any()}]}].

-spec aggregate_props([{string(), any()}], [string()]) ->
    keyed_props().
aggregate_props(Conf, Prefix) ->
    aggregate_props(Conf, Prefix, fun(E) -> E end).

-spec aggregate_props([{string(), any()}], [string()], function()) ->
    keyed_props().
aggregate_props(Conf, Prefix, KeyFun) ->
    Pattern = Prefix ++ ["$id", "$_"],
    PrefixLen = length(Prefix),
    FlatList = lists:filtermap(
        fun(E) ->
            {K, V} = KeyFun(E),
            case cuttlefish_variable:is_fuzzy_match(K, Pattern) of
                true -> {true, {lists:nthtail(PrefixLen, K), V}};
                false -> false
            end
        end,
        Conf
    ),
    proplists:from_map(
        maps:groups_from_list(
            fun({[ID | _], _}) -> list_to_binary(ID) end,
            fun({[_ | [Setting | _]], Value}) -> {list_to_binary(Setting), Value} end,
            FlatList
        )
    ).

optionally_tagged_binary(Key, Conf) ->
    case cuttlefish:conf_get(Key, Conf) of
        undefined                            -> cuttlefish:unset();
        {encrypted, Bin} when is_binary(Bin) -> {encrypted, Bin};
        {_,         Bin} when is_binary(Bin) -> {encrypted, Bin};
        {encrypted, Str} when is_list(Str) -> {encrypted, list_to_binary(Str)};
        {_,         Str} when is_list(Str) -> {encrypted, list_to_binary(Str)};
        Bin when is_binary(Bin) -> Bin;
        Str when is_list(Str) -> list_to_binary(Str)
    end.

optionally_tagged_string(Key, Conf) ->
    case cuttlefish:conf_get(Key, Conf) of
        undefined                          -> cuttlefish:unset();
        {encrypted, Str} when is_list(Str) -> {encrypted, Str};
        {_,         Str} when is_list(Str) -> {encrypted, Str};
        {encrypted, Bin} when is_binary(Bin) -> {encrypted, binary_to_list(Bin)};
        {_,         Bin} when is_binary(Bin) -> {encrypted, binary_to_list(Bin)};
        Str when is_list(Str) -> Str;
        Bin when is_binary(Bin) -> binary_to_list(Bin)
    end.