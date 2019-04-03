%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_cache).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/1,
         check_vhost_access/3, check_resource_access/3, check_topic_access/4]).

%% Implementation of rabbit_auth_backend

user_login_authentication(Username, AuthProps) ->
    with_cache(authn, {user_login_authentication, [Username, AuthProps]},
        fun({ok, _})          -> success;
           ({refused, _, _})  -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

user_login_authorization(Username) ->
    with_cache(authz, {user_login_authorization, [Username]},
        fun({ok, _})      -> success;
           ({ok, _, _})   -> success;
           ({refused, _, _})  -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

check_vhost_access(#auth_user{} = AuthUser, VHostPath, Sock) ->
    ClientAddress = extract_address(Sock),
    % the underlying backend uses the socket, but only
    % the client address is used in the key of the cache entry
    with_cache(authz, {check_vhost_access, [AuthUser, VHostPath, Sock], [AuthUser, VHostPath, ClientAddress]},
        fun(true)  -> success;
           (false) -> refusal;
           ({error, _} = Err) -> Err;
           (_)                -> unknown
        end).

check_resource_access(#auth_user{} = AuthUser,
                      #resource{} = Resource, Permission) ->
    with_cache(authz, {check_resource_access, [AuthUser, Resource, Permission]},
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

with_cache(BackendType, {F, A}, Fun) ->
    with_cache(BackendType, {F, A, A}, Fun);
with_cache(BackendType, {F, OriginalArguments, ArgumentsForCache}, Fun) ->
    {ok, AuthCache} = application:get_env(rabbitmq_auth_backend_cache,
                                          cache_module),
    case AuthCache:get({F, ArgumentsForCache}) of
        {ok, Result} ->
            Result;
        {error, not_found} ->
            Backend = get_cached_backend(BackendType),
            {ok, TTL} = application:get_env(rabbitmq_auth_backend_cache,
                                            cache_ttl),
            BackendResult = apply(Backend, F, OriginalArguments),
            case should_cache(BackendResult, Fun) of
                true  -> ok = AuthCache:put({F, ArgumentsForCache}, BackendResult, TTL);
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

extract_address(undefined) ->
    undefined;
extract_address(none) ->
    undefined;
% for native direct connections the address is set to unknown
extract_address(#authz_socket_info{peername = {unknown, _Port}}) ->
    undefined;
extract_address(#authz_socket_info{peername = {Address, _Port}}) ->
    Address;
extract_address(Sock) ->
    {ok, {Address, _Port}} = rabbit_net:peername(Sock),
    Address.
