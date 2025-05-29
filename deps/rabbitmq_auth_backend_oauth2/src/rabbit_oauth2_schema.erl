%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_schema).

-define(AUTH_OAUTH2, "auth_oauth2").
-define(SCOPE_ALIASES, "scope_aliases").
-define(RESOURCE_SERVERS, "resource_servers").
-define(OAUTH_PROVIDERS, "oauth_providers").
-define(SIGNING_KEYS, "signing_keys").
-define(AUTH_OAUTH2_SCOPE_ALIASES, ?AUTH_OAUTH2 ++ "." ++ ?SCOPE_ALIASES).
-define(AUTH_OAUTH2_RESOURCE_SERVERS, ?AUTH_OAUTH2 ++ "." ++ ?RESOURCE_SERVERS).
-define(AUTH_OAUTH2_OAUTH_PROVIDERS, ?AUTH_OAUTH2 ++ "." ++ ?OAUTH_PROVIDERS).
-define(AUTH_OAUTH2_SIGNING_KEYS, ?AUTH_OAUTH2 ++ "." ++ ?SIGNING_KEYS).
-define(RESOURCE_SERVERS_SYNONYMS, #{
  "additional_scopes_key" => "extra_scopes_source"
}).

-export([
    translate_oauth_providers/1,
    translate_resource_servers/1,
    translate_signing_keys/1,
    translate_endpoint_params/2,
    translate_scope_aliases/1
]).

resource_servers_key_synonym(Key) -> maps:get(Key, ?RESOURCE_SERVERS_SYNONYMS, Key).

extract_key_as_binary({Name,_}) -> list_to_binary(Name).
extract_value({_Name,V}) -> V.

-spec translate_scope_aliases([{list(), binary()}]) -> map().
translate_scope_aliases(Conf) ->
    Settings = cuttlefish_variable:filter_by_prefix(
        ?AUTH_OAUTH2_SCOPE_ALIASES, Conf),
    maps:merge(extract_scope_alias_as_map(Settings),
        extract_scope_aliases_as_list_of_alias_scope_props(Settings)).

convert_space_separated_string_to_list_of_binaries(String) ->
    [ list_to_binary(V) || V <- string:tokens(String, " ")].

extract_scope_alias_as_map(Settings) ->
    maps:from_list([{
        list_to_binary(Alias),
        convert_space_separated_string_to_list_of_binaries(Scope)
        }
        || {[?AUTH_OAUTH2, ?SCOPE_ALIASES, Alias], Scope} <- Settings ]).

extract_scope_aliases_as_list_of_alias_scope_props(Settings) ->
    KeyFun = fun extract_key_as_binary/1,
    ValueFun = fun extract_value/1,

    List0 = [{Index, {list_to_atom(Attr), V}}
        || {[?AUTH_OAUTH2, ?SCOPE_ALIASES, Index, Attr], V} <- Settings ],
    List1 = maps:to_list(maps:groups_from_list(KeyFun, ValueFun, List0)),
    List2 = [extract_scope_alias_mapping(Proplist) || {_, Proplist} <- List1],
    maps:from_list([ V || V <- List2, V =/= {}]).

extract_scope_alias_mapping(Proplist) ->
    Alias =
        case proplists:get_value(alias, Proplist) of
            undefined -> {error, missing_alias_attribute};
            A -> list_to_binary(A)
        end,
    Scope =
        case proplists:get_value(scope, Proplist) of
            undefined -> {error, missing_scope_attribute};
            S -> convert_space_separated_string_to_list_of_binaries(S)
        end,
    case {Alias, Scope} of
        {{error, _}, _} ->
            cuttlefish:warn(
                "Skipped scope_aliases due to missing alias attribute"),
            {};
        {_, {error, _}} ->
            cuttlefish:warn(
                "Skipped scope_aliases due to missing scope attribute"),
            {};
        _ = V -> V
    end.

extract_resource_server_scope_aliases_as_list_of_props(Settings) ->
    KeyFun = fun extract_key_as_binary/1,
    ValueFun = fun extract_value/1,

    List0 = [
        {
            Name,
            {Index, {list_to_atom(Attr), V}}
        } ||
        {[
            ?AUTH_OAUTH2, ?RESOURCE_SERVERS, Name, ?SCOPE_ALIASES,
            Index, Attr
         ], V
        } <- Settings ],
    Map0 = maps:groups_from_list(KeyFun, ValueFun, List0),

    Map4 = maps:map(fun (_, L) ->
        Map2 = maps:map(fun (_, L2) -> extract_scope_alias_mapping(L2) end,
            maps:groups_from_list(KeyFun, ValueFun, L)),
        Map3 = maps:filter(fun (_,V) -> V =/= {} end, Map2),
        [{scope_aliases, maps:from_list([ V || {_, V} <- maps:to_list(Map3)])}]
        end, Map0),

    Map4.

extract_resource_server_scope_aliases_as_map(Settings) ->
    KeyFun = fun extract_key_as_binary/1,
    ValueFun = fun extract_value/1,

    List0 = [
        {
            Name,
            {
                list_to_binary(Alias),
                convert_space_separated_string_to_list_of_binaries(Scope)
            }
        } ||
        {[
            ?AUTH_OAUTH2, ?RESOURCE_SERVERS, Name, ?SCOPE_ALIASES,
            Alias
         ], Scope
        } <- Settings ],
    Map0 = maps:groups_from_list(KeyFun, ValueFun, List0),
    maps:map(fun (_, L) -> [{scope_aliases, maps:from_list(L)}] end, Map0).

-spec translate_resource_servers([{list(), binary()}]) -> map().
translate_resource_servers(Conf) ->
    Settings = cuttlefish_variable:filter_by_prefix(
        ?AUTH_OAUTH2_RESOURCE_SERVERS, Conf),
    Map = merge_list_of_maps([
        extract_resource_server_properties(Settings),
        extract_resource_server_preferred_username_claims(Settings),
        extract_resource_server_scope_aliases_as_list_of_props(Settings),
        extract_resource_server_scope_aliases_as_map(Settings)
    ]),
    Map0 = maps:map(fun(K,V) ->
        case proplists:get_value(id, V) of
            undefined -> V ++ [{id, K}];
            _ -> V
        end end, Map),
    ResourceServers = maps:values(Map0),
    lists:foldl(fun(Elem,AccMap)-> maps:put(proplists:get_value(id, Elem),
        Elem, AccMap) end, #{}, ResourceServers).

-spec translate_oauth_providers([{list(), binary()}]) -> map().
translate_oauth_providers(Conf) ->
    Settings = cuttlefish_variable:filter_by_prefix(
        ?AUTH_OAUTH2_OAUTH_PROVIDERS, Conf),

    merge_list_of_maps([
        extract_oauth_providers_properties(Settings),
        extract_oauth_providers_endpoint_params(discovery_endpoint_params,
            Settings),
        extract_oauth_providers_algorithm(Settings),
        extract_oauth_providers_https(Settings),
        extract_oauth_providers_signing_keys(Settings)
        ]).

-spec translate_signing_keys([{list(), binary()}]) -> map().
translate_signing_keys(Conf) ->
    Settings = cuttlefish_variable:filter_by_prefix(
        ?AUTH_OAUTH2_SIGNING_KEYS, Conf),
    ListOfKidPath = lists:map(fun({Id, Path}) -> {
        list_to_binary(lists:last(Id)), Path} end, Settings),
    translate_list_of_signing_keys(ListOfKidPath).

-spec translate_list_of_signing_keys([{list(), list()}]) -> map().
translate_list_of_signing_keys(ListOfKidPath) ->
    TryReadingFileFun =
        fun(Path) ->
            case file:read_file(Path) of
                {ok, Bin} ->
                    string:trim(Bin, trailing, "\n");
                _Error ->
                    cuttlefish:invalid(io_lib:format(
                        "File ~p does not exist or cannot be read by the node",
                        [Path]))
            end
        end,
    maps:map(fun(_K, Path) -> {pem, TryReadingFileFun(Path)} end,
        maps:from_list(ListOfKidPath)).

-spec translate_endpoint_params(list(), [{list(), binary()}]) ->
        [{binary(), binary()}].
translate_endpoint_params(Variable, Conf) ->
    Params0 = cuttlefish_variable:filter_by_prefix("auth_oauth2." ++ Variable,
        Conf),
    [{list_to_binary(Param), list_to_binary(V)} || {["auth_oauth2", _, Param], V}
         <- Params0].

validator_file_exists(Attr, Filename) ->
    case file:read_file(Filename) of
        {ok, _} ->
            Filename;
        _Error ->
            cuttlefish:invalid(io_lib:format(
                "Invalid attribute (~p) value: file ~p does not exist or " ++
                "cannot be read by the node", [Attr, Filename]))
    end.

validator_uri(Attr, Uri) when is_binary(Uri) ->
    validator_uri(Attr, binary_to_list(Uri));
validator_uri(Attr, Uri) when is_list(Uri) ->
    case uri_string:parse(Uri) of
        {error, _, _} = Error ->
            cuttlefish:invalid(io_lib:format(
                "Invalid attribute (~p) value: ~p (~p)", [Attr, Uri, Error]));
        _ -> Uri
    end.

validator_https_uri(Attr, Uri) when is_binary(Uri) ->
    validator_https_uri(Attr, binary_to_list(Uri));

validator_https_uri(Attr, Uri) when is_list(Uri) ->
    case string:nth_lexeme(Uri, 1, "://") == "https" of
        true -> Uri;
        false ->
            cuttlefish:invalid(io_lib:format(
                "Invalid attribute (~p) value: uri ~p must be a valid " ++
                "https uri", [Attr, Uri]))
    end.

merge_list_of_maps(ListOfMaps) ->
    lists:foldl(fun(Elem, AccIn) -> maps:merge_with(
        fun(_K,V1,V2) -> V1 ++ V2 end, Elem, AccIn) end, #{}, ListOfMaps).

extract_oauth_providers_properties(Settings) ->
    KeyFun = fun extract_key_as_binary/1,
    ValueFun = fun extract_value/1,

    OAuthProviders = [{Name, mapOauthProviderProperty(
        {
            list_to_atom(Key),
            list_to_binary(V)})
        } || {[?AUTH_OAUTH2, ?OAUTH_PROVIDERS, Name, Key], V} <- Settings ],
    maps:groups_from_list(KeyFun, ValueFun, OAuthProviders).


extract_resource_server_properties(Settings) ->
    KeyFun = fun extract_key_as_binary/1,
    ValueFun = fun extract_value/1,

    OAuthProviders = [{Name, {list_to_atom(resource_servers_key_synonym(Key)), list_to_binary(V)}}
        || {[?AUTH_OAUTH2, ?RESOURCE_SERVERS, Name, Key], V} <- Settings ],
    maps:groups_from_list(KeyFun, ValueFun, OAuthProviders).

mapOauthProviderProperty({Key, Value}) ->
    {Key, case Key of
        issuer -> validator_https_uri(Key, Value);
        token_endpoint -> validator_https_uri(Key, Value);
        tokeninfo_endpoint -> validator_https_uri(Key, Value);
        jwks_uri -> validator_https_uri(Key, Value);
        end_session_endpoint -> validator_https_uri(Key, Value);
        authorization_endpoint -> validator_https_uri(Key, Value);
        discovery_endpoint_path -> validator_uri(Key, Value);
        discovery_endpoint_params ->
            cuttlefish:invalid(io_lib:format(
                "Invalid attribute (~p) value: should be a map of Key,Value pairs",
                    [Key]));
        _ -> Value
    end}.

extract_oauth_providers_https(Settings) ->
    ExtractProviderNameFun = fun extract_key_as_binary/1,

    AttributesPerProvider = [{Name, mapHttpProperty({list_to_atom(Key), V})} ||
        {[?AUTH_OAUTH2, ?OAUTH_PROVIDERS, Name, "https", Key], V} <- Settings ],

    maps:map(fun(_K,V)-> [{https, V}] end,
        maps:groups_from_list(ExtractProviderNameFun, fun({_, V}) -> V end,
            AttributesPerProvider)).

mapHttpProperty({Key, Value}) ->
    {Key, case Key of
        cacertfile -> validator_file_exists(Key, Value);
        _ -> Value
    end}.

extract_oauth_providers_algorithm(Settings) ->
    KeyFun = fun extract_key_as_binary/1,

    IndexedAlgorithms = [{Name, {Index, list_to_binary(V)}} ||
        {[?AUTH_OAUTH2, ?OAUTH_PROVIDERS, Name, "algorithms", Index], V}
            <- Settings ],
    SortedAlgorithms = lists:sort(fun({_,{AI,_}},{_,{BI,_}}) -> AI < BI end,
        IndexedAlgorithms),
    Algorithms = [{Name, V} || {Name, {_I, V}} <- SortedAlgorithms],
    maps:map(fun(_K,V)-> [{algorithms, V}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, Algorithms)).

extract_resource_server_preferred_username_claims(Settings) ->
    KeyFun = fun extract_key_as_binary/1,

    IndexedClaims = [{Name, {Index, list_to_binary(V)}} ||
        {[?AUTH_OAUTH2, ?RESOURCE_SERVERS, Name, "preferred_username_claims",
            Index], V} <- Settings ],
    SortedClaims = lists:sort(fun({_,{AI,_}},{_,{BI,_}}) -> AI < BI end,
        IndexedClaims),
    Claims = [{Name, V} || {Name, {_I, V}} <- SortedClaims],
    maps:map(fun(_K,V)-> [{preferred_username_claims, V}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, Claims)).

extract_oauth_providers_endpoint_params(Variable, Settings) ->
    KeyFun = fun extract_key_as_binary/1,

    IndexedParams = [{Name, {list_to_binary(ParamName), list_to_binary(V)}} ||
        {["auth_oauth2","oauth_providers", Name, EndpointVar, ParamName], V}
            <- Settings, EndpointVar == atom_to_list(Variable) ],
    maps:map(fun(_K,V)-> [{Variable, V}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, IndexedParams)).

extract_oauth_providers_signing_keys(Settings) ->
    KeyFun = fun extract_key_as_binary/1,

    IndexedSigningKeys = [{Name, {list_to_binary(Kid), list_to_binary(V)}} ||
        {[?AUTH_OAUTH2, ?OAUTH_PROVIDERS, Name, ?SIGNING_KEYS, Kid], V}
            <- Settings ],
    maps:map(fun(_K,V)-> [{signing_keys, translate_list_of_signing_keys(V)}] end,
        maps:groups_from_list(KeyFun, fun({_, V}) -> V end, IndexedSigningKeys)).
