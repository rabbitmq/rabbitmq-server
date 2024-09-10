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

-export([
    add_signing_key/2, add_signing_key/3, replace_signing_keys/1,
    replace_signing_keys/2,
    get_signing_keys/0, get_signing_keys/1, get_signing_key/1, get_signing_key/2,
    get_default_key/0,
    get_default_resource_server_id/0,
    get_resource_server_id_for_audience/1,
    get_algorithms/0, get_algorithms/1, get_default_key/1,
    get_oauth_provider_id_for_resource_server_id/1,
    get_oauth_provider/2,
    get_allowed_resource_server_ids/0, find_audience_in_resource_server_ids/1,
    is_verify_aud/0, is_verify_aud/1,
    get_additional_scopes_key/0, get_additional_scopes_key/1,
    get_default_preferred_username_claims/0, get_preferred_username_claims/0,
    get_preferred_username_claims/1,
    get_scope_prefix/0, get_scope_prefix/1,
    get_resource_server_type/0, get_resource_server_type/1,
    has_scope_aliases/1, get_scope_aliases/1
    ]).

-spec get_default_preferred_username_claims() -> list().
get_default_preferred_username_claims() -> ?DEFAULT_PREFERRED_USERNAME_CLAIMS.

-spec get_preferred_username_claims() -> list().
get_preferred_username_claims() ->
    case application:get_env(?APP, preferred_username_claims) of
      {ok, Value} ->
        append_or_return_default(Value, ?DEFAULT_PREFERRED_USERNAME_CLAIMS);
      _ -> ?DEFAULT_PREFERRED_USERNAME_CLAIMS
    end.
-spec get_preferred_username_claims(binary() | list()) -> list().
get_preferred_username_claims(ResourceServerId) ->
    ResourceServers = application:get_env(?APP, resource_servers, #{}),
    ResourceServer = maps:get(ResourceServerId, ResourceServers, []),
    case proplists:get_value(preferred_username_claims, ResourceServer, undefined) of
      undefined ->
        get_preferred_username_claims();
      Value ->
        append_or_return_default(Value, ?DEFAULT_PREFERRED_USERNAME_CLAIMS)
    end.

-spec get_default_key() -> {ok, binary()} | {error, no_default_key_configured}.
get_default_key() ->
    get_default_key(root).

-spec get_default_key(oauth_provider_id()) -> {ok, binary()} | {error, no_default_key_configured}.
get_default_key(root) ->
    case application:get_env(?APP, key_config, undefined) of
        undefined ->
            {error, no_default_key_configured};
        KeyConfig ->
            case proplists:get_value(default_key, KeyConfig, undefined) of
                undefined -> {error, no_default_key_configured};
                V -> {ok, V}
            end
    end;
get_default_key(OauthProviderId) ->
    OauthProviders = application:get_env(?APP, oauth_providers, #{}),
    case maps:get(OauthProviderId, OauthProviders, []) of
        [] ->
            {error, no_default_key_configured};
        OauthProvider ->
            case proplists:get_value(default_key, OauthProvider, undefined) of
                undefined -> {error, no_default_key_configured};
                V -> {ok, V}
            end
    end.

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

-spec get_resource_server_id_for_audience(binary() | list() | none) -> binary() | {error, term()}.
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

-spec get_oauth_provider_id_for_resource_server_id(binary()) -> oauth_provider_id().

get_oauth_provider_id_for_resource_server_id(ResourceServerId) ->
    get_oauth_provider_id_for_resource_server_id(get_default_resource_server_id(),
        ResourceServerId).
get_oauth_provider_id_for_resource_server_id(TopResourceServerId,
  ResourceServerId) when ResourceServerId =:= TopResourceServerId ->
    case application:get_env(?APP, default_oauth_provider) of
        undefined -> root;
        {ok, DefaultOauthProviderId} -> DefaultOauthProviderId
    end;
get_oauth_provider_id_for_resource_server_id(TopResourceServerId,
  ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
    case proplists:get_value(oauth_provider_id, get_resource_server_props(ResourceServerId)) of
        undefined ->
            case application:get_env(?APP, default_oauth_provider) of
                undefined -> root;
                {ok, DefaultOauthProviderId} -> DefaultOauthProviderId
            end;
        OauthProviderId -> OauthProviderId
    end.

-spec get_oauth_provider(oauth_provider_id(), list()) ->
    {ok, oauth_provider()} | {error, any()}.
get_oauth_provider(OAuthProviderId, RequiredAttributeList) ->
    oauth2_client:get_oauth_provider(OAuthProviderId, RequiredAttributeList).

-spec get_algorithms() -> list() | undefined.
get_algorithms() ->
    get_algorithms(root).

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

get_resource_server_props(ResourceServerId) ->
    ResourceServers = application:get_env(?APP, resource_servers, #{}),
    maps:get(ResourceServerId, ResourceServers, []).

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

-spec is_verify_aud() -> boolean().
is_verify_aud() -> application:get_env(?APP, verify_aud, true).

-spec is_verify_aud(binary()) -> boolean().
is_verify_aud(ResourceServerId) ->
    case get_default_resource_server_id() of
        {error, _} ->
            is_verify_aud(undefined, ResourceServerId);
        V ->
            is_verify_aud(V, ResourceServerId)
    end.
is_verify_aud(TopResourceServerId, ResourceServerId)
  when ResourceServerId =:= TopResourceServerId -> is_verify_aud();
is_verify_aud(TopResourceServerId, ResourceServerId)
  when ResourceServerId =/= TopResourceServerId ->
    proplists:get_value(verify_aud, maps:get(ResourceServerId,
        application:get_env(?APP, resource_servers, #{}), []), is_verify_aud()).

-spec get_additional_scopes_key() -> {ok, binary()} | {error, not_found}.
get_additional_scopes_key() ->
    case application:get_env(?APP, extra_scopes_source, undefined) of
        undefined -> {error, not_found};
        ScopeKey -> {ok, ScopeKey}
    end.
-spec get_additional_scopes_key(binary()) -> {ok, binary()} | {error, not_found}.
get_additional_scopes_key(ResourceServerId) ->
    case get_default_resource_server_id() of
        {error, _} ->
            get_additional_scopes_key(undefined, ResourceServerId);
        V ->
            get_additional_scopes_key(V, ResourceServerId)
    end.
get_additional_scopes_key(TopResourceServerId, ResourceServerId)
  when ResourceServerId =:= TopResourceServerId -> get_additional_scopes_key();
get_additional_scopes_key(TopResourceServerId, ResourceServerId)
  when ResourceServerId =/= TopResourceServerId ->
    ResourceServer = maps:get(ResourceServerId,
        application:get_env(?APP, resource_servers, #{}), []),
    case proplists:get_value(extra_scopes_source, ResourceServer) of
        undefined -> get_additional_scopes_key();
        <<>> -> get_additional_scopes_key();
        ScopeKey -> {ok, ScopeKey}
    end.

-spec get_scope_prefix() -> binary().
get_scope_prefix() ->
    DefaultScopePrefix = case get_default_resource_server_id() of
        {error, _} -> <<"">>;
        V -> erlang:iolist_to_binary([V, <<".">>])
    end,
    application:get_env(?APP, scope_prefix, DefaultScopePrefix).

-spec get_scope_prefix(binary()) -> binary().
get_scope_prefix(ResourceServerId) ->
    case get_default_resource_server_id() of
        {error, _} ->
            get_scope_prefix(undefined, ResourceServerId);
        V ->
            get_scope_prefix(V, ResourceServerId)
    end.
get_scope_prefix(TopResourceServerId, ResourceServerId)
  when ResourceServerId =:= TopResourceServerId -> get_scope_prefix();
get_scope_prefix(TopResourceServerId, ResourceServerId)
  when ResourceServerId =/= TopResourceServerId ->
    ResourceServer = maps:get(ResourceServerId,
        application:get_env(?APP, resource_servers, #{}), []),
    case proplists:get_value(scope_prefix, ResourceServer) of
        undefined ->
            case application:get_env(?APP, scope_prefix) of
               undefined ->  <<ResourceServerId/binary, ".">>;
               {ok, Prefix} -> Prefix
            end;
        Prefix -> Prefix
    end.

-spec get_resource_server_type() -> binary().
get_resource_server_type() -> application:get_env(?APP, resource_server_type, <<>>).

-spec get_resource_server_type(binary()) -> binary().
get_resource_server_type(ResourceServerId) ->
    case get_default_resource_server_id() of
        {error, _} ->
            get_resource_server_type(undefined, ResourceServerId);
        V ->
            get_resource_server_type(V, ResourceServerId)
    end.
get_resource_server_type(TopResourceServerId, ResourceServerId)
  when ResourceServerId =:= TopResourceServerId -> get_resource_server_type();
get_resource_server_type(TopResourceServerId, ResourceServerId)
  when ResourceServerId =/= TopResourceServerId ->
    ResourceServer = maps:get(ResourceServerId,
        application:get_env(?APP, resource_servers, #{}), []),
    proplists:get_value(resource_server_type, ResourceServer,
        get_resource_server_type()).

-spec has_scope_aliases(binary()) -> boolean().
has_scope_aliases(ResourceServerId) ->
    case get_default_resource_server_id() of
        {error, _} ->
            has_scope_aliases(undefined, ResourceServerId);
        V ->
            has_scope_aliases(V, ResourceServerId)
    end.
has_scope_aliases(TopResourceServerId, ResourceServerId)
  when ResourceServerId =:= TopResourceServerId ->
    case application:get_env(?APP, scope_aliases) of
        undefined -> false;
        _ -> true
    end;
has_scope_aliases(TopResourceServerId, ResourceServerId)
  when ResourceServerId =/= TopResourceServerId ->
    ResourceServerProps = maps:get(ResourceServerId,
        application:get_env(?APP, resource_servers, #{}),[]),
    case proplists:is_defined(scope_aliases, ResourceServerProps) of
        true -> true;
        false ->  has_scope_aliases(TopResourceServerId)
    end.

-spec get_scope_aliases(binary()) -> map().
get_scope_aliases(ResourceServerId) ->
    case get_default_resource_server_id() of
        {error, _} ->
            get_scope_aliases(undefined, ResourceServerId);
        V ->
            get_scope_aliases(V, ResourceServerId)
    end.
get_scope_aliases(TopResourceServerId, ResourceServerId)
  when ResourceServerId =:= TopResourceServerId ->
    application:get_env(?APP, scope_aliases, #{});
get_scope_aliases(TopResourceServerId, ResourceServerId)
  when ResourceServerId =/= TopResourceServerId ->
    ResourceServerProps = maps:get(ResourceServerId,
        application:get_env(?APP, resource_servers, #{}),[]),
    proplists:get_value(scope_aliases, ResourceServerProps,
        get_scope_aliases(TopResourceServerId)).


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
