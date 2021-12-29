%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_backend_cache).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         state_can_expire/0]).

%% API

user_login_authentication(Username, AuthProps) ->
    with_cache(authn, {user_login_authentication, [Username, AuthProps]},
        fun({ok, _})          -> success;
           ({refused, _, _})  -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

user_login_authorization(Username, AuthProps) ->
    with_cache(authz, {user_login_authorization, [Username, AuthProps]},
        fun({ok, _})      -> success;
           ({ok, _, _})   -> success;
           ({refused, _, _})  -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

check_vhost_access(#auth_user{} = AuthUser, VHostPath, AuthzData) ->
    with_cache(authz, {check_vhost_access, [AuthUser, VHostPath, AuthzData]},
        fun(true)  -> success;
           (false) -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

check_resource_access(#auth_user{} = AuthUser,
                      #resource{} = Resource, Permission, AuthzContext) ->
    with_cache(authz, {check_resource_access, [AuthUser, Resource, Permission, AuthzContext]},
        fun(true)  -> success;
           (false) -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

check_topic_access(#auth_user{} = AuthUser,
                   #resource{} = Resource, Permission, Context) ->
    with_cache(authz, {check_topic_access, [AuthUser, Resource, Permission, Context]},
        fun(true)  -> success;
            (false) -> refusal;
            ({error, _} = Err) -> Err;
            (_)                -> unknown
        end).

state_can_expire() -> false.

%%
%% Implementation
%%

with_cache(BackendType, {F, A}, Fun) ->
    {ok, AuthCache} = application:get_env(rabbitmq_auth_backend_cache,
                                          cache_module),
    case AuthCache:get({F, A}) of
        {ok, Result} ->
            Result;
        {error, not_found} ->
            Backend = get_cached_backend(BackendType),
            {ok, TTL} = application:get_env(rabbitmq_auth_backend_cache,
                                            cache_ttl),
            BackendResult = apply(Backend, F, A),
            case should_cache(BackendResult, Fun) of
                true  -> ok = AuthCache:put({F, A}, BackendResult, TTL);
                false -> ok
            end,
            BackendResult
    end.

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
