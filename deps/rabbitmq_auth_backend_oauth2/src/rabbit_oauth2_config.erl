%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_config).

-include_lib("oauth2_client/include/oauth2_client.hrl").

-define(APP, rabbitmq_auth_backend_oauth2).
-define(DEFAULT_PREFERRED_USERNAME_CLAIMS, [<<"sub">>, <<"client_id">>]).

-define(TOP_RESOURCE_SERVER_ID, application:get_env(?APP, resource_server_id)).
%% scope aliases map "role names" to a set of scopes

-record(internal_oauth_provider, {
    id :: oauth_provider_id(),
    default_key :: binary() | undefined,
    algorithms :: list() | undefined
}).
-type internal_oauth_provider() :: #internal_oauth_provider{}.

-record(resource_server, {
  id :: resource_server_id(),
  resource_server_type :: binary(),
  verify_aud :: boolean(),
  scope_prefix :: binary(),
  additional_scopes_key :: binary(),
  preferred_username_claims :: list(),
  scope_aliases :: undefined | map(),
  oauth_provider_id :: oauth_provider_id()
 }).

-type resource_server() :: #resource_server{}.
-type resource_server_id() :: binary() | list().

-export([
    add_signing_key/2, add_signing_key/3, replace_signing_keys/1,
    replace_signing_keys/2,
    get_signing_keys/0, get_signing_keys/1, get_signing_key/1, get_signing_key/2,
    resolve_resource_server_id_from_audience/1,
    get_oauth_provider/2,
    get_internal_oauth_provider/2,
    get_allowed_resource_server_ids/0, find_audience_in_resource_server_ids/1
    ]).
export_type([resource_server/0, internal_oauth_provider/0]).

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


%%
%% Signing Key storage:
%%
%% * Static signing keys configured via config file are stored under signing_keys attribute
%% in their respective location (under key_config for the root oauth provider and
%% directly under each oauth provider)
%% * Dynamic signing keys loaded via rabbitmqctl or via JWKS endpoint are stored under
%% jwks attribute in their respective location. However, this attribute stores the
%% combination of static signing keys and dynamic signing keys. If the same kid is
%% found in both sets, the dynamic kid overrides the static kid.
%%

-type key_type() :: json | pem | map.
-spec add_signing_key(binary(), {key_type(), binary()} ) -> map() | {error, term()}.
add_signing_key(KeyId, Key) ->
    LockId = lock(),
    try do_add_signing_key(KeyId, Key, root) of
        V -> V
    after
        unlock(LockId)
    end.

-spec add_signing_key(binary(), {key_type(), binary()}, oauth_provider_id()) ->
    map() | {error, term()}.
add_signing_key(KeyId, Key, OAuthProviderId) ->
    case lock() of
        {error, _} = Error ->
            Error;
        LockId ->
            try do_add_signing_key(KeyId, Key, OAuthProviderId) of
                V -> V
            after
                unlock(LockId)
            end
    end.

do_add_signing_key(KeyId, Key, OAuthProviderId) ->
    do_replace_signing_keys(maps:put(KeyId, Key,
        get_signing_keys_from_jwks(OAuthProviderId)), OAuthProviderId).

get_signing_keys_from_jwks(root) ->
    KeyConfig = application:get_env(?APP, key_config, []),
    proplists:get_value(jwks, KeyConfig, #{});
get_signing_keys_from_jwks(OAuthProviderId) ->
    OAuthProviders0 = application:get_env(?APP, oauth_providers, #{}),
    OAuthProvider0 = maps:get(OAuthProviderId, OAuthProviders0, []),
    proplists:get_value(jwks, OAuthProvider0, #{}).

-spec replace_signing_keys(map()) -> map() | {error, term()}.
replace_signing_keys(SigningKeys) ->
    replace_signing_keys(SigningKeys, root).

-spec replace_signing_keys(map(), oauth_provider_id()) -> map() | {error, term()}.
replace_signing_keys(SigningKeys, OAuthProviderId) ->
    case lock() of
        {error,_} = Error ->
            Error;
        LockId ->
            try do_replace_signing_keys(SigningKeys, OAuthProviderId) of
                V -> V
            after
                unlock(LockId)
            end
    end.

do_replace_signing_keys(SigningKeys, root) ->
    KeyConfig = application:get_env(?APP, key_config, []),
    KeyConfig1 = proplists:delete(jwks, KeyConfig),
    KeyConfig2 = [{jwks, maps:merge(
        proplists:get_value(signing_keys, KeyConfig1, #{}),
        SigningKeys)} | KeyConfig1],
    application:set_env(?APP, key_config, KeyConfig2),
    rabbit_log:debug("Replacing signing keys for key_config with ~p keys",
        [maps:size(SigningKeys)]),
    SigningKeys;

do_replace_signing_keys(SigningKeys, OauthProviderId) ->
    OauthProviders0 = application:get_env(?APP, oauth_providers, #{}),
    OauthProvider0 = maps:get(OauthProviderId, OauthProviders0, []),
    OauthProvider1 = proplists:delete(jwks, OauthProvider0),
    OauthProvider = [{jwks, maps:merge(
        proplists:get_value(signing_keys, OauthProvider1, #{}),
        SigningKeys)} | OauthProvider1],

    OauthProviders = maps:put(OauthProviderId, OauthProvider, OauthProviders0),
    application:set_env(?APP, oauth_providers, OauthProviders),
    rabbit_log:debug("Replacing signing keys for ~p -> ~p with ~p keys",
        [OauthProviderId, OauthProvider, maps:size(SigningKeys)]),
    SigningKeys.


-spec get_signing_keys() -> map().
get_signing_keys() ->
    get_signing_keys(root).

-spec get_signing_keys(oauth_provider_id()) -> map().
get_signing_keys(root) ->
    case application:get_env(?APP, key_config, undefined) of
        undefined ->
            #{};
        KeyConfig ->
            case proplists:get_value(jwks, KeyConfig, undefined) of
                undefined -> proplists:get_value(signing_keys, KeyConfig, #{});
                Jwks -> Jwks
            end
    end;
get_signing_keys(OauthProviderId) ->
    OauthProviders = application:get_env(?APP, oauth_providers, #{}),
    OauthProvider = maps:get(OauthProviderId, OauthProviders, []),
    case proplists:get_value(jwks, OauthProvider, undefined) of
        undefined ->
            proplists:get_value(signing_keys, OauthProvider, #{});
        Jwks ->
            Jwks
    end.

-spec resolve_resource_server_id_from_audience(binary() | list() | none) -> resource_server() | {error, term()}.
resolve_resource_server_id_from_audience(Audience) ->
    case get_resource_server_id_for_audience(Audience) of
        {error, _} = Error -> Error;
        ResourceServerId -> get_resource_server(ResourceServerId)
    end.

get_resource_server_id_for_audience(none) ->
    case is_verify_aud() of
        true ->
            {error, no_matching_aud_found};
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


-spec get_oauth_provider(oauth_provider_id(), list()) ->
    {ok, oauth_provider()} | {error, any()}.
get_oauth_provider(OAuthProviderId, RequiredAttributeList) ->
    oauth2_client:get_oauth_provider(OAuthProviderId, RequiredAttributeList).

-spec get_internal_oauth_provider(oauth_provider_id(), list()) ->
    {ok, internal_oauth_provider()} | {error, any()}.
get_internal_oauth_provider(OAuthProviderId) ->
    #internal_oauth_provider{
        id = OAuthProvider#oauth_provider.id,
        default_key = get_default_key(OAuthProvider#oauth_provider.id),
        algorithms :: get_algorithms(OAuthProvider#oauth_provider.id)
    }.

-spec get_default_key(oauth_provider_id()) -> binary() | undefined.
get_default_key(root) ->
    case application:get_env(?APP, key_config, undefined) of
        undefined ->
            undefined;
        KeyConfig ->
            proplists:get_value(default_key, KeyConfig, undefined)
    end;
get_default_key(OauthProviderId) ->
    OauthProviders = application:get_env(?APP, oauth_providers, #{}),
    case maps:get(OauthProviderId, OauthProviders, []) of
        [] ->
            undefined;
        OauthProvider ->
            proplists:get_value(default_key, OauthProvider, undefined)
    end.

-spec get_algorithms(oauth_provider_id()) -> list() | undefined.
get_algorithms(root) ->
    proplists:get_value(algorithms, application:get_env(?APP, key_config, []),
                undefined);
get_algorithms(OAuthProviderId) ->
    OAuthProviders = application:get_env(?APP, oauth_providers, #{}),
    case maps:get(OAuthProviderId, OAuthProviders, undefined) of
        undefined -> undefined;
        V -> proplists:get_value(algorithms, V, undefined)
    end.

get_signing_key(KeyId) ->
    maps:get(KeyId, get_signing_keys(root), undefined).
get_signing_key(KeyId, OAuthProviderId) ->
    maps:get(KeyId, get_signing_keys(OAuthProviderId), undefined).


append_or_return_default(ListOrBinary, Default) ->
    case ListOrBinary of
        VarList when is_list(VarList) -> VarList ++ Default;
        VarBinary when is_binary(VarBinary) -> [VarBinary] ++ Default;
        _ -> Default
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


intersection(List1, List2) ->
    [I || I <- List1, lists:member(I, List2)].

lock() ->
    Nodes   = rabbit_nodes:list_running(),
    Retries = rabbit_nodes:lock_retries(),
    LockId = case global:set_lock({oauth2_config_lock,
            rabbitmq_auth_backend_oauth2}, Nodes, Retries) of
        true  -> rabbitmq_auth_backend_oauth2;
        false -> {error, unable_to_claim_lock}
    end,
    LockId.

unlock(LockId) ->
    Nodes = rabbit_nodes:list_running(),
    global:del_lock({oauth2_config_lock, LockId}, Nodes),
    ok.
