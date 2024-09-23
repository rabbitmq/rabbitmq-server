%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_schema).


-export([
    translate_oauth_resource_servers/1,
    translate_endpoint_params/2
]).

extract_key({Name,_}) -> Name.
extract_key_as_binary({Name,_}) -> list_to_binary(Name).
extract_value({_Name,V}) -> V.

-spec translate_oauth_resource_servers([{list(), binary()}]) -> map().
translate_oauth_resource_servers(Conf) ->
    Settings = cuttlefish_variable:filter_by_prefix(
        "management.oauth_resource_servers", Conf),
    rabbit_log:debug("Settings: ~p", [Settings]),
    Map = merge_list_of_maps([
        extract_resource_server_properties(Settings),
        extract_resource_server_endpoint_params(oauth_authorization_endpoint_params, Settings),
        extract_resource_server_endpoint_params(oauth_token_endpoint_params, Settings)
    ]),
    Map0 = maps:map(fun(K,V) ->
        case proplists:get_value(id, V) of
            undefined -> V ++ [{id, K}];
            _ -> V
        end end, Map),
    ResourceServers = maps:values(Map0),
    lists:foldl(fun(Elem,AccMap)-> maps:put(proplists:get_value(id, Elem), Elem, AccMap) end, #{},
        ResourceServers).

-spec translate_endpoint_params(list(), [{list(), binary()}]) -> map().
translate_endpoint_params(Variable, Conf) ->
    Params0 = cuttlefish_variable:filter_by_prefix("management." ++ Variable, Conf),
    Params = [{Param, list_to_binary(V)} || {["management", _, Param], V} <- Params0].

merge_list_of_maps(ListOfMaps) ->
    lists:foldl(fun(Elem, AccIn) -> maps:merge_with(fun(_K,V1,V2) -> V1 ++ V2 end,
        Elem, AccIn) end, #{}, ListOfMaps).


extract_resource_server_properties(Settings) ->
    KeyFun = fun extract_key/1,
    ValueFun = fun extract_value/1,

    OAuthProviders = [{Name, {list_to_atom(Key), V}}
        || {["management","oauth_resource_servers", Name, Key], V} <- Settings ],
    rabbit_log:debug("extract_resource_server_properties ~p", [Settings]),
    Result = maps:groups_from_list(KeyFun, ValueFun, OAuthProviders),
    rabbit_log:debug("extract_resource_server_properties -> ~p", [Result]),

    Result.

extract_resource_server_endpoint_params(Variable, Settings) ->
    KeyFun = fun extract_key/1,

    rabbit_log:debug("extract_resource_server_endpoint_params ~p ~p", [Variable, Settings]),
    IndexedParams = [{Name, {ParamName, list_to_binary(V)}} ||
        {["management","oauth_resource_servers", Name, EndpointVar, ParamName], V}
            <- Settings, EndpointVar == atom_to_list(Variable) ],
    maps:map(fun(_K,V)-> [{Variable, V}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, IndexedParams)).
