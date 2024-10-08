%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_resource_server).

-include("oauth2.hrl").

-export([
    resolve_resource_server_from_audience/1,
    new_resource_server/1
]).

-spec new_resource_server(resource_server_id()) -> resource_server().
new_resource_server(ResourceServerId) ->
    #resource_server{
        id = ResourceServerId,
        resource_server_type = undefined,
        verify_aud = true,
        scope_prefix = erlang:iolist_to_binary([ResourceServerId, <<".">>]),
        additional_scopes_key = undefined,
        preferred_username_claims = ?DEFAULT_PREFERRED_USERNAME_CLAIMS,
        scope_aliases = undefined,
        oauth_provider_id = root
    }.

-spec resolve_resource_server_from_audience(binary() | list() | none) ->
    {ok, resource_server()} |
    {error, aud_matched_many_resource_servers_only_one_allowed} |
    {error, no_matching_aud_found} |
    {error, no_aud_found} |
    {error, no_aud_found_cannot_pick_one_from_too_many_resource_servers} |
    {error, too_many_resources_with_verify_aud_false}.
resolve_resource_server_from_audience(none) ->
    translate_error_if_any(
        find_unique_resource_server_without_verify_aud(), false);

resolve_resource_server_from_audience(Audience) ->
    RootResourseServerId = get_root_resource_server_id(),
    ResourceServers = get_env(resource_servers, #{}),
    ResourceServerIds = maps:fold(fun(K, V, List) -> List ++
        [proplists:get_value(id, V, K)] end, [], ResourceServers),
    AllowedResourceServerIds = append(ResourceServerIds, RootResourseServerId),

    case find_audience(Audience, AllowedResourceServerIds) of
        {error, aud_matched_many_resource_servers_only_one_allowed} = Error ->
            Error;
        {error, no_matching_aud_found} ->
            translate_error_if_any(
                find_unique_resource_server_without_verify_aud(),
                true);
        {ok, ResourceServerId} ->
            {ok, get_resource_server(ResourceServerId)}
    end.

-spec get_root_resource_server_id() -> resource_server_id().
get_root_resource_server_id() ->
    get_env(resource_server_id, <<>>).

-spec get_root_resource_server() -> resource_server().
get_root_resource_server() ->
    ResourceServerId =
        get_root_resource_server_id(),
    ScopeAliases =
        get_env(scope_aliases),
    PreferredUsernameClaims =
        case get_env(preferred_username_claims) of
            undefined -> ?DEFAULT_PREFERRED_USERNAME_CLAIMS;
            Value ->
                Value
        end,
    ResourceServerType =
        get_env(resource_server_type),
    VerifyAud =
        get_boolean_env(verify_aud, true),
    AdditionalScopesKey =
        get_env(extra_scopes_source),
    DefaultScopePrefix =
        case ResourceServerId of
            <<>> -> undefined;
            _ -> erlang:iolist_to_binary([ResourceServerId, <<".">>])
        end,
    ScopePrefix =
        get_env(scope_prefix, DefaultScopePrefix),
    OAuthProviderId =
        case get_env(default_oauth_provider) of
            undefined -> root;
            DefaultOauthProviderId -> DefaultOauthProviderId
        end,

    #resource_server{
        id = ResourceServerId,
        resource_server_type = ResourceServerType,
        verify_aud = VerifyAud,
        scope_prefix = ScopePrefix,
        additional_scopes_key = AdditionalScopesKey,
        preferred_username_claims = PreferredUsernameClaims,
        scope_aliases = ScopeAliases,
        oauth_provider_id = OAuthProviderId
    }.

-spec get_resource_server(resource_server_id()) -> resource_server() | undefined.
get_resource_server(ResourceServerId) ->
    RootResourseServer = get_root_resource_server(),
    RootResourseServerId = RootResourseServer#resource_server.id,
    case ResourceServerId of
        <<>> -> undefined;
        RootResourseServerId -> RootResourseServer;
        _ -> get_resource_server(ResourceServerId, RootResourseServer)
    end.

-spec get_resource_server(ResourceServerId :: resource_server_id(),
    DefaultResourceServerSettings :: resource_server()) -> resource_server().
get_resource_server(ResourceServerId, RootResourseServer) when
        ResourceServerId == RootResourseServer#resource_server.id ->
    RootResourseServer;
get_resource_server(ResourceServerId, RootResourseServer) when
        ResourceServerId =/= RootResourseServer#resource_server.id ->
    ResourceServerProps =
        maps:get(ResourceServerId, get_env(resource_servers, #{}), []),
    ScopeAliases =
        proplists:get_value(scope_aliases, ResourceServerProps,
            RootResourseServer#resource_server.scope_aliases),
    PreferredUsernameClaims =
        proplists:get_value(preferred_username_claims, ResourceServerProps,
            RootResourseServer#resource_server.preferred_username_claims),
    ResourceServerType =
        proplists:get_value(resource_server_type, ResourceServerProps,
            RootResourseServer#resource_server.resource_server_type),
    VerifyAud =
        proplists:get_value(verify_aud, ResourceServerProps,
            RootResourseServer#resource_server.verify_aud),
    AdditionalScopesKey =
        proplists:get_value(extra_scopes_source, ResourceServerProps,
            RootResourseServer#resource_server.additional_scopes_key),
    RootScopePrefix = get_env(scope_prefix, undefined),
    ScopePrefix =
        proplists:get_value(scope_prefix, ResourceServerProps,
            case RootScopePrefix of
                undefined -> erlang:iolist_to_binary([ResourceServerId, <<".">>]);
                Prefix -> Prefix
            end),
    OAuthProviderId =
        proplists:get_value(oauth_provider_id, ResourceServerProps,
            RootResourseServer#resource_server.oauth_provider_id),

    #resource_server{
        id = ResourceServerId,
        resource_server_type = ResourceServerType,
        verify_aud = VerifyAud,
        scope_prefix = ScopePrefix,
        additional_scopes_key = AdditionalScopesKey,
        preferred_username_claims = PreferredUsernameClaims,
        scope_aliases = ScopeAliases,
        oauth_provider_id = OAuthProviderId
    }.

-spec find_audience(binary() | list(), list()) ->
    {ok, resource_server_id()} |
    {error, aud_matched_many_resource_servers_only_one_allowed} |
    {error, no_matching_aud_found}.
find_audience(Audience, ResourceIdList) when is_binary(Audience) ->
    AudList = binary:split(Audience, <<" ">>, [global, trim_all]),
    find_audience(AudList, ResourceIdList);
find_audience(AudList, ResourceIdList) when is_list(AudList) ->
    case intersection(AudList, ResourceIdList) of
        [One] -> {ok, One};
        [_One|_Tail] -> {error, aud_matched_many_resource_servers_only_one_allowed};
        [] -> {error, no_matching_aud_found}
    end.

-spec translate_error_if_any(
    {ok, resource_server()} |
    {error, not_found} |
    {error, found_many}, boolean()) ->
        {ok, resource_server()} |
        {error, no_aud_found} |
        {error, no_aud_found_cannot_pick_one_from_too_many_resource_servers} |
        {error, no_matching_aud_found} |
        {error, too_many_resources_with_verify_aud_false}.
translate_error_if_any(ResourceServerOrError, HasAudience) ->
    case {ResourceServerOrError, HasAudience} of
        {{ok, _} = Ok, _} ->
            Ok;
        {{error, not_found}, false} ->
            {error, no_aud_found};
        {{error, not_found}, _} ->
            {error, no_matching_aud_found};
        {{error, found_many}, false} ->
            {error, no_aud_found_cannot_pick_one_from_too_many_resource_servers};
        {{error, found_many}, _} ->
            {error, too_many_resources_with_verify_aud_false}
    end.
-spec find_unique_resource_server_without_verify_aud() ->
    {ok, resource_server()} |
    {error, not_found} |
    {error, found_many}.
find_unique_resource_server_without_verify_aud() ->
    Root = get_root_resource_server(),
    Map0 = maps:filter(fun(_K,V) -> not get_boolean_value(verify_aud, V,
        Root#resource_server.verify_aud) end, get_env(resource_servers, #{})),
    Map = case {Root#resource_server.id, Root#resource_server.verify_aud}  of
        {<<>>, _} -> Map0;
        {_, true} -> Map0;
        {Id, false} -> maps:put(Id, Root, Map0)
    end,
    case maps:size(Map) of
        0 -> {error, not_found};
        1 -> {ok, get_resource_server(lists:last(maps:keys(Map)), Root)};
        _ -> {error, found_many}
    end.

append(List, Value) ->
    case Value of
        <<>> -> List;
        _ -> List ++ [Value]
    end.
get_env(Par) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, undefined).
get_env(Par, Def) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, Def).
-spec get_boolean_env(atom(), boolean()) -> boolean().
get_boolean_env(Par, Def) ->
    case get_env(Par, Def) of
        true -> true;
        false -> false;
        _ -> true
    end.
-spec get_boolean_value(term(), list(), boolean()) -> boolean().
get_boolean_value(Key, Proplist, Def) ->
    case proplists:get_value(Key, Proplist, Def) of
        true -> true;
        false -> false;
        _ -> true
    end.
intersection(List1, List2) ->
    [I || I <- List1, lists:member(I, List2)].
