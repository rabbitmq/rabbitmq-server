%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(oauth2_schema).


-export([
    translate_oauth_providers/1,
    translate_resource_servers/1,
    translate_signing_keys/1,
    translate_endpoint_params/2
]).

extract_key_as_binary({Name,_}) -> list_to_binary(Name).
extract_value({_Name,V}) -> V.

-spec translate_resource_servers([{list(), binary()}]) -> map().
translate_resource_servers(Conf) ->
    Settings = cuttlefish_variable:filter_by_prefix("auth_oauth2.resource_servers", Conf),
    Map = merge_list_of_maps([
        extract_resource_server_properties(Settings),
        extract_resource_server_preferred_username_claims(Settings)
    ]),
    Map0 = maps:map(fun(K,V) ->
        case proplists:get_value(id, V) of
            undefined -> V ++ [{id, K}];
            _ -> V
        end end, Map),
    ResourceServers = maps:values(Map0),
    lists:foldl(fun(Elem,AccMap)-> maps:put(proplists:get_value(id, Elem), Elem, AccMap) end, #{},
        ResourceServers).

-spec translate_oauth_providers([{list(), binary()}]) -> map().
translate_oauth_providers(Conf) ->
    Settings = cuttlefish_variable:filter_by_prefix("auth_oauth2.oauth_providers", Conf),

    merge_list_of_maps([
        extract_oauth_providers_properties(Settings),
        extract_oauth_providers_endpoint_params(discovery_endpoint_params, Settings),
        extract_oauth_providers_endpoint_params(authorization_endpoint_params, Settings),
        extract_oauth_providers_endpoint_params(token_endpoint_params, Settings),
        extract_oauth_providers_algorithm(Settings),
        extract_oauth_providers_https(Settings),
        extract_oauth_providers_signing_keys(Settings)
        ]).

-spec translate_signing_keys([{list(), binary()}]) -> map().
translate_signing_keys(Conf) ->
    Settings = cuttlefish_variable:filter_by_prefix("auth_oauth2.signing_keys", Conf),
    ListOfKidPath = lists:map(fun({Id, Path}) -> {list_to_binary(lists:last(Id)), Path} end, Settings),
    translate_list_of_signing_keys(ListOfKidPath).

-spec translate_list_of_signing_keys([{list(), list()}]) -> map().
translate_list_of_signing_keys(ListOfKidPath) ->
    TryReadingFileFun =
        fun(Path) ->
            case file:read_file(Path) of
                {ok, Bin} ->
                    string:trim(Bin, trailing, "\n");
                _Error ->
                    %% this throws and makes Cuttlefish treak the key as invalid
                    cuttlefish:invalid("file does not exist or cannot be read by the node")
            end
        end,
    maps:map(fun(_K, Path) -> {pem, TryReadingFileFun(Path)} end, maps:from_list(ListOfKidPath)).

-spec translate_endpoint_params(list(), [{list(), binary()}]) -> map().
translate_endpoint_params(Variable, Conf) ->
    Params0 = cuttlefish_variable:filter_by_prefix("auth_oauth2." ++ Variable, Conf),
    Params = [{list_to_binary(Param), list_to_binary(V)} ||
        {["auth_oauth2", Name, Param], V} <- Params0],
    maps:from_list(Params).

validator_file_exists(Attr, Filename) ->
    case file:read_file(Filename) of
        {ok, _} ->
            Filename;
        _Error ->
            %% this throws and makes Cuttlefish treak the key as invalid
            cuttlefish:invalid(io_lib:format(
                "Invalid attribute (~p) value: file ~p does not exist or cannot be read by the node", [Attr, Filename]))
    end.
validator_https_uri(Attr, Uri) when is_binary(Uri) ->
    list_to_binary(validator_https_uri(Attr, binary_to_list(Uri)));

validator_https_uri(Attr, Uri) ->
    case string:nth_lexeme(Uri, 1, "://") == "https" of
        true -> Uri;
        false ->
            cuttlefish:invalid(io_lib:format(
                "Invalid attribute (~p) value: uri ~p must be a valid https uri", [Attr, Uri]))
    end.

merge_list_of_maps(ListOfMaps) ->
    lists:foldl(fun(Elem, AccIn) -> maps:merge_with(fun(_K,V1,V2) -> V1 ++ V2 end,
        Elem, AccIn) end, #{}, ListOfMaps).

extract_oauth_providers_properties(Settings) ->
    KeyFun = fun extract_key_as_binary/1,
    ValueFun = fun extract_value/1,

    OAuthProviders = [{Name, mapOauthProviderProperty({list_to_atom(Key), list_to_binary(V)})}
        || {["auth_oauth2","oauth_providers", Name, Key], V} <- Settings ],
    maps:groups_from_list(KeyFun, ValueFun, OAuthProviders).

extract_resource_server_properties(Settings) ->
    KeyFun = fun extract_key_as_binary/1,
    ValueFun = fun extract_value/1,

    OAuthProviders = [{Name, {list_to_atom(Key), list_to_binary(V)}}
        || {["auth_oauth2","resource_servers", Name, Key], V} <- Settings ],
    maps:groups_from_list(KeyFun, ValueFun, OAuthProviders).

mapOauthProviderProperty({Key, Value}) ->
    {Key, case Key of
        issuer -> validator_https_uri(Key, Value);
        token_endpoint -> validator_https_uri(Key, Value);
        jwks_uri -> validator_https_uri(Key, Value);
        end_session_endpoint -> validator_https_uri(Key, Value);
        authorization_endpoint -> validator_https_uri(Key, Value);
        _ -> Value
    end}.

extract_oauth_providers_https(Settings) ->
    ExtractProviderNameFun = fun extract_key_as_binary/1,

    AttributesPerProvider = [{Name, mapHttpProperty({list_to_atom(Key), V})} ||
        {["auth_oauth2","oauth_providers", Name, "https", Key], V} <- Settings ],

    maps:map(fun(_K,V)-> [{https, V}] end,
        maps:groups_from_list(ExtractProviderNameFun, fun({_, V}) -> V end, AttributesPerProvider)).

mapHttpProperty({Key, Value}) ->
    {Key, case Key of
        cacertfile -> validator_file_exists(Key, Value);
        _ -> Value
    end}.

extract_oauth_providers_algorithm(Settings) ->
    KeyFun = fun extract_key_as_binary/1,

    IndexedAlgorithms = [{Name, {Index, list_to_binary(V)}} ||
        {["auth_oauth2","oauth_providers", Name, "algorithms", Index], V} <- Settings ],
    SortedAlgorithms = lists:sort(fun({_,{AI,_}},{_,{BI,_}}) -> AI < BI end, IndexedAlgorithms),
    Algorithms = [{Name, V} || {Name, {_I, V}} <- SortedAlgorithms],
    maps:map(fun(_K,V)-> [{algorithms, V}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, Algorithms)).

extract_resource_server_preferred_username_claims(Settings) ->
    KeyFun = fun extract_key_as_binary/1,

    IndexedClaims = [{Name, {Index, list_to_binary(V)}} ||
        {["auth_oauth2","resource_servers", Name, "preferred_username_claims", Index], V} <- Settings ],
    SortedClaims = lists:sort(fun({_,{AI,_}},{_,{BI,_}}) -> AI < BI end, IndexedClaims),
    Claims = [{Name, V} || {Name, {_I, V}} <- SortedClaims],
    maps:map(fun(_K,V)-> [{preferred_username_claims, V}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, Claims)).

extract_oauth_providers_endpoint_params(Variable, Settings) ->
    KeyFun = fun extract_key_as_binary/1,

    IndexedParams = [{Name, {ParamName, list_to_binary(V)}} ||
        {["auth_oauth2","oauth_providers", Name, EndpointVar, ParamName], V} <- Settings, EndpointVar == Variable ],
    maps:map(fun(_K,V)-> [{Variable, V}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, IndexedParams)).

extract_oauth_providers_signing_keys(Settings) ->
    KeyFun = fun extract_key_as_binary/1,

    IndexedSigningKeys = [{Name, {list_to_binary(Kid), list_to_binary(V)}} ||
        {["auth_oauth2","oauth_providers", Name, "signing_keys", Kid], V} <- Settings ],
    maps:map(fun(_K,V)-> [{signing_keys, translate_list_of_signing_keys(V)}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, IndexedSigningKeys)).
