%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_cuttlefish).

-export([
    aggregate_props/2
]).

-type keyed_props() :: [{binary(), [{binary(), any()}]}].

-spec aggregate_props([{string(), any()}], [string()]) ->
    keyed_props().
aggregate_props(Conf, Prefix) ->
    Pattern = Prefix ++ ["$id", "$_"],
    PrefixLen = length(Prefix),
    FlatList = lists:filtermap(
        fun({K, V}) ->
            case cuttlefish_variable:is_fuzzy_match(K, Pattern) of
                true -> {true, {lists:nthtail(PrefixLen, K), V}};
                _ -> false
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
