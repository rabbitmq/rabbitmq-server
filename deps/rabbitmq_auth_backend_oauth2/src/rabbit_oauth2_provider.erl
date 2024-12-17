%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_oauth2_provider).

-include("oauth2.hrl").

-export([
    get_internal_oauth_provider/0, get_internal_oauth_provider/1,
    add_signing_key/2, add_signing_key/3, replace_signing_keys/1,
    replace_signing_keys/2,
    get_signing_keys/0, get_signing_keys/1, get_signing_key/1, get_signing_key/2
]).

-spec get_internal_oauth_provider() -> internal_oauth_provider().
get_internal_oauth_provider() ->
    get_internal_oauth_provider(root).

-spec get_internal_oauth_provider(oauth_provider_id()) -> internal_oauth_provider().
get_internal_oauth_provider(OAuthProviderId) ->
    #internal_oauth_provider{
        id = OAuthProviderId,
        default_key = get_default_key(OAuthProviderId),
        algorithms = get_algorithms(OAuthProviderId)
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
    KeyConfig = get_env(key_config, []),
    proplists:get_value(jwks, KeyConfig, #{});
get_signing_keys_from_jwks(OAuthProviderId) ->
    OAuthProviders0 = get_env(oauth_providers, #{}),
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
    KeyConfig = get_env(key_config, []),
    KeyConfig1 = proplists:delete(jwks, KeyConfig),
    KeyConfig2 = [{jwks, maps:merge(
        proplists:get_value(signing_keys, KeyConfig1, #{}),
        SigningKeys)} | KeyConfig1],
    set_env(key_config, KeyConfig2),
    rabbit_log:debug("Replacing signing keys for key_config with ~p keys",
        [maps:size(SigningKeys)]),
    SigningKeys;

do_replace_signing_keys(SigningKeys, OauthProviderId) ->
    OauthProviders0 = get_env(oauth_providers, #{}),
    OauthProvider0 = maps:get(OauthProviderId, OauthProviders0, []),
    OauthProvider1 = proplists:delete(jwks, OauthProvider0),
    OauthProvider = [{jwks, maps:merge(
        proplists:get_value(signing_keys, OauthProvider1, #{}),
        SigningKeys)} | OauthProvider1],

    OauthProviders = maps:put(OauthProviderId, OauthProvider, OauthProviders0),
    set_env(oauth_providers, OauthProviders),
    rabbit_log:debug("Replacing signing keys for ~p -> ~p with ~p keys",
        [OauthProviderId, OauthProvider, maps:size(SigningKeys)]),
    SigningKeys.


-spec get_signing_keys() -> map().
get_signing_keys() ->
    get_signing_keys(root).

-spec get_signing_keys(oauth_provider_id()) -> map().
get_signing_keys(root) ->
    case get_env(key_config) of
        undefined ->
            #{};
        KeyConfig ->
            case proplists:get_value(jwks, KeyConfig, undefined) of
                undefined -> proplists:get_value(signing_keys, KeyConfig, #{});
                Jwks -> Jwks
            end
    end;
get_signing_keys(OauthProviderId) ->
    OauthProviders = get_env(oauth_providers, #{}),
    OauthProvider = maps:get(OauthProviderId, OauthProviders, []),
    case proplists:get_value(jwks, OauthProvider, undefined) of
        undefined ->
            proplists:get_value(signing_keys, OauthProvider, #{});
        Jwks ->
            Jwks
    end.

get_signing_key(KeyId) ->
    maps:get(KeyId, get_signing_keys(root), undefined).
get_signing_key(KeyId, OAuthProviderId) ->
    maps:get(KeyId, get_signing_keys(OAuthProviderId), undefined).

-spec get_default_key(oauth_provider_id()) -> binary() | undefined.
get_default_key(root) ->
    case application:get_env(?APP, key_config, undefined) of
        undefined -> undefined;
        KeyConfig -> proplists:get_value(default_key, KeyConfig, undefined)
    end;
get_default_key(OauthProviderId) ->
    OauthProviders = application:get_env(?APP, oauth_providers, #{}),
    case maps:get(OauthProviderId, OauthProviders, []) of
        [] -> undefined;
        OauthProvider -> proplists:get_value(default_key, OauthProvider, undefined)
    end.

-spec get_algorithms(oauth_provider_id()) -> list() | undefined.
get_algorithms(root) ->
    proplists:get_value(algorithms, get_env(key_config, []), undefined);
get_algorithms(OAuthProviderId) ->
    OAuthProviders = get_env(oauth_providers, #{}),
    case maps:get(OAuthProviderId, OAuthProviders, undefined) of
        undefined -> undefined;
        V -> proplists:get_value(algorithms, V, undefined)
    end.

get_env(Par) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, undefined).
get_env(Par, Def) ->
    application:get_env(rabbitmq_auth_backend_oauth2, Par, Def).
set_env(Par, Value) ->
    application:set_env(rabbitmq_auth_backend_oauth2, Par, Value).


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
