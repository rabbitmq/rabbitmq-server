%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_resource_server).

-include("oauth2.hrl").

%-include_lib("oauth2_client/include/oauth2_client.hrl").


-export([
    resolve_resource_server_id_from_audience/1,
    get_resource_server/1
]).

-spec get_resource_server(resource_server_id()) -> resource_server() | {error, term()}.
get_resource_server(ResourceServerId) ->
    case get_default_resource_server_id() of
        {error, _} ->
            get_resource_server(undefined, ResourceServerId);
        V ->
            get_resource_server(V, ResourceServerId)
    end.
get_resource_server(TopResourceServerId, ResourceServerId) ->
  when ResourceServerId =:= TopResourceServerId ->
    ScopeAlises =
        application:get_env(?APP, scope_aliases, undefined),
    PreferredUsernameClaims =
        case application:get_env(?APP, preferred_username_claims) of
            {ok, Value} ->
                append_or_return_default(Value, ?DEFAULT_PREFERRED_USERNAME_CLAIMS);
            _ -> ?DEFAULT_PREFERRED_USERNAME_CLAIMS
        end,
    ResourceServerType =
        application:get_env(?APP, resource_server_type, <<>>),
    VerifyAud =
        application:get_env(?APP, verify_aud, true),
    AdditionalScopesKey =
        case application:get_env(?APP, extra_scopes_source, undefined) of
            undefined -> {error, not_found};
            ScopeKey -> {ok, ScopeKey}
        end,
    DefaultScopePrefix =
        case get_default_resource_server_id() of
            {error, _} -> <<"">>;
            V -> erlang:iolist_to_binary([V, <<".">>])
        end,
    ScopePrefix =
        application:get_env(?APP, scope_prefix, DefaultScopePrefix).
    OAuthProviderId =
        case application:get_env(?APP, default_oauth_provider) of
            undefined -> root;
            {ok, DefaultOauthProviderId} -> DefaultOauthProviderId
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
    };

get_resource_server(TopResourceServerId, ResourceServerId) ->
  when ResourceServerId =/= TopResourceServerId ->
    ResourceServerProps =
        maps:get(ResourceServerId, application:get_env(?APP, resource_servers,
            #{}),[]),
    TopResourseServer =
        get_resource_server(TopResourceServerId, TopResourceServerId),
    ScopeAlises =
        proplists:get_value(scope_aliases, ResourceServerProps,
            TopResourseServer#resource_server.scope_aliases),
    PreferredUsernameClaims =
        proplists:get_value(preferred_username_claims, ResourceServerProps,
            TopResourseServer#resource_server.preferred_username_claims),
    ResourceServerType =
        proplists:get_value(resource_server_type, ResourceServerProps,
            TopResourseServer#resource_server.resource_server_type),
    VerifyAud =
        proplists:get_value(verify_aud, ResourceServerProps,
            TopResourseServer#resource_server.verify_aud),
    AdditionalScopesKey =
        proplists:get_value(extra_scopes_source, ResourceServerProps,
            TopResourseServer#resource_server.extra_scopes_source),
    ScopePrefix =
        proplists:get_value(scope_prefix, ResourceServerProps,
            TopResourseServer#resource_server.scope_prefix),
    OAuthProviderId =
        proplists:get_value(oauth_provider_id, ResourceServerProps,
            TopResourseServer#resource_server.oauth_provider_id),

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


-spec resolve_resource_server_id_from_audience(binary() | list() | none) ->
    resource_server() | {error, term()}.
resolve_resource_server_id_from_audience(Audience) ->
    case get_resource_server_id_for_audience(Audience) of
        {error, _} = Error -> Error;
        ResourceServerId -> get_resource_server(ResourceServerId)
    end.

get_resource_server_id_for_audience(none) ->
    case is_verify_aud() of
        true ->
            {error, missing_audience_in_token};
        false ->
            case get_default_resource_server_id() of
                {error, missing_resource_server_id_in_config} ->
                    {error, mising_audience_in_token_and_resource_server_in_config};
                V -> V
            end
    end;
get_resource_server_id_for_audience(Audience) ->
    case find_audience_in_resource_server_ids(Audience) of
        {ok, ResourceServerId} ->
            ResourceServerId;
        {error, only_one_resource_server_as_audience_found_many} = Error ->
            Error;
        {error, no_matching_aud_found} ->
            case is_verify_aud() of
                true ->
                    {error, no_matching_aud_found};
                false ->
                    case get_default_resource_server_id() of
                        {error, missing_resource_server_id_in_config} ->
                            {error, mising_audience_in_token_and_resource_server_in_config};
                        V -> V
                    end
            end
    end.

-spec get_default_resource_server_id() -> binary() | {error, term()}.
get_default_resource_server_id() ->
    case ?TOP_RESOURCE_SERVER_ID of
        undefined -> {error, missing_resource_server_id_in_config };
        {ok, ResourceServerId} -> ResourceServerId
    end.

-spec get_allowed_resource_server_ids() -> list().
get_allowed_resource_server_ids() ->
    ResourceServers = application:get_env(?APP, resource_servers, #{}),
    rabbit_log:debug("ResourceServers: ~p", [ResourceServers]),
    ResourceServerIds = maps:fold(fun(K, V, List) -> List ++
        [proplists:get_value(id, V, K)] end, [], ResourceServers),
    rabbit_log:debug("ResourceServersIds: ~p", [ResourceServerIds]),
    ResourceServerIds ++ case get_default_resource_server_id() of
       {error, _} -> [];
       ResourceServerId -> [ ResourceServerId ]
    end.

-spec find_audience_in_resource_server_ids(binary() | list()) ->
    {ok, binary()} | {error, term()}.
find_audience_in_resource_server_ids(Audience) when is_binary(Audience) ->
    find_audience_in_resource_server_ids(binary:split(Audience, <<" ">>, [global, trim_all]));
find_audience_in_resource_server_ids(AudList) when is_list(AudList) ->
    AllowedAudList = get_allowed_resource_server_ids(),
    case intersection(AudList, AllowedAudList) of
        [One] -> {ok, One};
        [_One|_Tail] -> {error, only_one_resource_server_as_audience_found_many};
        [] -> {error, no_matching_aud_found}
    end.


append_or_return_default(ListOrBinary, Default) ->
    case ListOrBinary of
        VarList when is_list(VarList) -> VarList ++ Default;
        VarBinary when is_binary(VarBinary) -> [VarBinary] ++ Default;
        _ -> Default
    end.

intersection(List1, List2) ->
    [I || I <- List1, lists:member(I, List2)].
