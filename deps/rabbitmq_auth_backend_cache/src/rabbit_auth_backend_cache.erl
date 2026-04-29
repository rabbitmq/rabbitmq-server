%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_cache).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         update_state/2, expiry_timestamp/1,
         clear_cache_cluster_wide/0, clear_cache/0,
         init_key_salt/0, cache_key/2]).

%% API

-spec user_login_authentication(rabbit_types:username(), [term()] | map()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.
user_login_authentication(Username, AuthProps) ->
    with_cache(authn, {user_login_authentication, [Username, AuthProps]},
        fun({ok, _})          -> success;
           ({refused, _, _})  -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

-spec user_login_authorization(rabbit_types:username(), [term()] | map()) ->
    {'ok', any()} |
    {'ok', any(), any()} |
    {'refused', string(), [any()]} |
    {'error', any()}.
user_login_authorization(Username, AuthProps) ->
    with_cache(authz, {user_login_authorization, [Username, AuthProps]},
        fun({ok, _})      -> success;
           ({ok, _, _})   -> success;
           ({refused, _, _})  -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

-spec check_vhost_access(rabbit_types:auth_user(), rabbit_types:vhost(),
                         rabbit_types:authz_data()) ->
    boolean() | {'error', any()}.
check_vhost_access(#auth_user{} = AuthUser, VHostPath, AuthzData) ->
    with_cache(authz, {check_vhost_access, [AuthUser, VHostPath, AuthzData]},
        fun(true)  -> success;
           (false) -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

-spec check_resource_access(rabbit_types:auth_user(), rabbit_types:r(atom()),
                            rabbit_types:permission_atom(),
                            rabbit_types:authz_context()) ->
    boolean() | {'error', any()}.
check_resource_access(#auth_user{} = AuthUser,
                      #resource{} = Resource, Permission, AuthzContext) ->
    with_cache(authz, {check_resource_access, [AuthUser, Resource, Permission, AuthzContext]},
        fun(true)  -> success;
           (false) -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

-spec check_topic_access(rabbit_types:auth_user(), rabbit_types:r(atom()),
                         rabbit_types:permission_atom(),
                         rabbit_types:topic_access_context()) ->
    boolean() | {'error', any()}.
check_topic_access(#auth_user{} = AuthUser,
                   #resource{} = Resource, Permission, Context) ->
    with_cache(authz, {check_topic_access, [AuthUser, Resource, Permission, Context]},
        fun(true)  -> success;
            (false) -> refusal;
            ({error, _} = Err) -> Err;
            (_)                -> unknown
        end).

-spec expiry_timestamp(rabbit_types:auth_user()) -> integer() | never.
expiry_timestamp(AuthUser) ->
    Backend = get_cached_backend(authz),
    Backend:expiry_timestamp(AuthUser).

-spec update_state(rabbit_types:auth_user(), term()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.
update_state(AuthUser, NewState) ->
    Backend = get_cached_backend(authz),
    Backend:update_state(AuthUser, NewState).

%%
%% Implementation
%%

clear_cache_cluster_wide() ->
    Nodes = rabbit_nodes:list_running(),
    ?LOG_WARNING("Clearing auth_backend_cache in all nodes : ~p", [Nodes]),
    rabbit_misc:append_rpc_all_nodes(Nodes, ?MODULE, clear_cache, []).

clear_cache() ->
    {ok, AuthCache} = application:get_env(rabbitmq_auth_backend_cache,
                                          cache_module),
    ?LOG_WARNING("Clearing auth_backend_cache"),
    AuthCache:clear().

with_cache(BackendType, {F, A}, Fun) ->
    {ok, AuthCache} = application:get_env(rabbitmq_auth_backend_cache,
                                          cache_module),
    Key = cache_key(F, A),
    case AuthCache:get(Key) of
        {ok, Result} ->
            Result;
        {error, not_found} ->
            Backend = get_cached_backend(BackendType),
            {ok, TTL} = application:get_env(rabbitmq_auth_backend_cache,
                                            cache_ttl),
            BackendResult = apply(Backend, F, A),
            case should_cache(BackendResult, Fun) of
                true  -> ok = AuthCache:put(Key, BackendResult, TTL);
                false -> ok
            end,
            BackendResult
    end.

%% Keep plaintext credentials out of the cache key. The full args are still
%% passed to the underlying backend; only the lookup key is redacted.
-spec cache_key(atom(), [term()]) -> {atom(), [term()]}.
cache_key(user_login_authentication = F, [Username, AuthProps]) ->
    {F, [Username, redact_credentials(AuthProps)]};
cache_key(F, A) ->
    {F, A}.

redact_credentials(AuthProps) when is_list(AuthProps) ->
    [redact_pair(Pair) || Pair <- AuthProps];
redact_credentials(AuthProps) when is_map(AuthProps) ->
    maps:map(fun (password, V) -> hash_secret(V);
                 (_K, V)        -> V
             end, AuthProps);
redact_credentials(AuthProps) ->
    AuthProps.

redact_pair({password, V}) -> {password, hash_secret(V)};
redact_pair(Other)         -> Other.

%% Use HMAC-SHA-256 with a per-node random salt rather than a plain
%% digest. The salt defeats precomputed (rainbow-table) attacks against
%% the low-entropy plaintext if cache contents are ever exposed (memory
%% dump, debug shell). The salt is initialised once at app start (see
%% `rabbit_auth_backend_cache_app:init/1') and regenerated on node
%% restart along with the cache itself.
hash_secret(V) ->
    {hmac_sha256, crypto:mac(hmac, sha256, key_salt(), term_to_binary(V))}.

-define(KEY_SALT_PT, {?MODULE, cache_key_salt}).

%% Idempotent: a supervisor restart must not regenerate the salt or
%% existing cache entries would become unreachable.
init_key_salt() ->
    case persistent_term:get(?KEY_SALT_PT, undefined) of
        undefined ->
            persistent_term:put(?KEY_SALT_PT, crypto:strong_rand_bytes(32));
        _Existing ->
            ok
    end.

key_salt() ->
    persistent_term:get(?KEY_SALT_PT).

get_cached_backend(Type) ->
    {ok, BackendConfig} = application:get_env(rabbitmq_auth_backend_cache,
                                              cached_backend),
    case BackendConfig of
        Mod when is_atom(Mod) ->
            Mod;
        {N, Z}                ->
            case Type of
                authn -> N;
                authz -> Z
            end
    end.

should_cache(Result, Fun) ->
    {ok, CacheRefusals} = application:get_env(rabbitmq_auth_backend_cache,
                                              cache_refusals),
    case {Fun(Result), CacheRefusals} of
        {success, _}    -> true;
        {refusal, true} -> true;
        _               -> false
    end.

    
