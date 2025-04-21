%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_internal_loopback).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4]).

-export([add_user/3, add_user/4, add_user/5, delete_user/2, lookup_user/1, exists/1,
         change_password/3, clear_password/2,
         hash_password/2, change_password_hash/2, change_password_hash/3,
         set_tags/3, set_permissions/6, clear_permissions/3, set_permissions_globally/5,
         set_topic_permissions/6, clear_topic_permissions/3, clear_topic_permissions/4,
         clear_all_permissions_for_vhost/2,
         add_user_sans_validation/3, put_user/2, put_user/3,
         update_user/5,
         update_user_with_hash/5,
         add_user_sans_validation/6,
         add_user_with_pre_hashed_password_sans_validation/3
]).

-export([set_user_limits/3, clear_user_limits/3, is_over_connection_limit/1,
         is_over_channel_limit/1, get_user_limits/0, get_user_limits/1]).

-export([user_info_keys/0, perms_info_keys/0,
         user_perms_info_keys/0, vhost_perms_info_keys/0,
         user_vhost_perms_info_keys/0, all_users/0,
         user_topic_perms_info_keys/0, vhost_topic_perms_info_keys/0,
         user_vhost_topic_perms_info_keys/0,
         list_users/0, list_users/2, list_permissions/0,
         list_user_permissions/1, list_user_permissions/3,
         list_topic_permissions/0,
         list_vhost_permissions/1, list_vhost_permissions/3,
         list_user_vhost_permissions/2,
         list_user_topic_permissions/1, list_vhost_topic_permissions/1, list_user_vhost_topic_permissions/2]).

-export([expiry_timestamp/1]).

-export([hashing_module_for_user/1, expand_topic_permission/2]).

-import(rabbit_data_coercion, [to_atom/1, to_list/1, to_binary/1]).

%%----------------------------------------------------------------------------
%% Implementation of rabbit_auth_backend

hashing_module_for_user(User) ->
    rabbit_auth_backend_internal:hashing_module_for_user(User).

-define(BLANK_PASSWORD_REJECTION_MESSAGE,
        "user '~ts' attempted to log in with a blank password, which is prohibited by the internal authN backend. "
        "To use TLS/x509 certificate-based authentication, see the rabbitmq_auth_mechanism_ssl plugin and configure the client to use the EXTERNAL authentication mechanism. "
        "Alternatively change the password for the user to be non-blank.").

-define(NO_SOCKET_OR_ADDRESS_REJECTION_MESSAGE,
        "user '~ts' attempted to log in, but no socket or address was provided "
        "to the internal_loopback auth backend, so cannot verify if connection "
        "is from localhost or not.").

-define(NOT_LOOPBACK_REJECTION_MESSAGE,
        "user '~ts' attempted to log in, but the socket or address was not from "
        "loopback/localhost, which is prohibited by the internal loopback authN "
        "backend.").

%% For cases when we do not have a set of credentials,
%% namely when x509 (TLS) certificates are used. This should only be
%% possible when the EXTERNAL authentication mechanism is used, see
%% rabbit_auth_mechanism_plain:handle_response/2 and rabbit_reader:auth_phase/2.
user_login_authentication(Username, []) ->
    user_login_authentication(Username, [{password, none}]);
%% For cases when we do have a set of credentials. rabbit_auth_mechanism_plain:handle_response/2
%% performs initial validation.
user_login_authentication(Username, AuthProps) ->
    case proplists:lookup(sockOrAddr, AuthProps) of
        none -> {refused, ?NO_SOCKET_OR_ADDRESS_REJECTION_MESSAGE, [Username]};      % sockOrAddr doesn't exist
        {sockOrAddr, SockOrAddr} ->
            case rabbit_net:is_loopback(SockOrAddr) of
                true ->
                    case lists:keyfind(password, 1, AuthProps) of
                        {password, <<"">>} ->
                            {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE,
                             [Username]};
                        {password, ""} ->
                            {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE,
                             [Username]};
                        {password, none} -> %% For cases when authenticating using an x.509 certificate
                            internal_check_user_login(Username, fun(_) -> true end);
                        {password, Cleartext} ->
                            internal_check_user_login(
                              Username,
                              fun(User) ->
                                  case internal_user:get_password_hash(User) of
                                      <<Salt:4/binary, Hash/binary>> ->
                                          Hash =:= rabbit_password:salted_hash(
                                              hashing_module_for_user(User), Salt, Cleartext);
                                      _ ->
                                          false
                                  end
                              end);
                        false ->
                            case proplists:get_value(rabbit_auth_backend_internal, AuthProps, undefined) of
                                undefined -> {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE, [Username]};
                                _ -> internal_check_user_login(Username, fun(_) -> true end)
                            end
                    end;
                false ->
                    {refused, ?NOT_LOOPBACK_REJECTION_MESSAGE, [Username]}
            end

    end.


expiry_timestamp(User) ->
    rabbit_auth_backend_internal:expiry_timestamp(User).

user_login_authorization(Username, AuthProps) ->
    rabbit_auth_backend_internal:user_login_authorization(Username, AuthProps).

internal_check_user_login(Username, Fun) ->
    Refused = {refused, "user '~ts' - invalid credentials", [Username]},
    case lookup_user(Username) of
        {ok, User} ->
            Tags = internal_user:get_tags(User),
            case Fun(User) of
                true -> {ok, #auth_user{username = Username,
                                        tags     = Tags,
                                        impl     = fun() -> none end}};
                _    -> Refused
            end;
        {error, not_found} ->
            Refused
    end.

check_vhost_access(AuthUser, VHostPath, AuthzData) ->
    rabbit_auth_backend_internal:check_vhost_access(AuthUser, VHostPath, AuthzData).

check_resource_access(AuthUser, Resource, Permission, Context) ->
    rabbit_auth_backend_internal:check_resource_access(AuthUser, Resource, Permission, Context).

check_topic_access(AuthUser, Resource, Permission, Context) ->
    rabbit_auth_backend_internal:check_topic_access(AuthUser, Resource, Permission, Context).

add_user(Username, Password, ActingUser) ->
    rabbit_auth_backend_internal:add_user(Username, Password, ActingUser).

add_user(Username, Password, ActingUser, Tags) ->
    rabbit_auth_backend_internal:add_user(Username, Password, ActingUser, Tags).

add_user(Username, Password, ActingUser, Limits, Tags) ->
    rabbit_auth_backend_internal:add_user(Username, Password, ActingUser, Limits, Tags).

delete_user(Username, ActingUser) ->
    rabbit_auth_backend_internal:delete_user(Username, ActingUser).

lookup_user(Username) ->
    rabbit_auth_backend_internal:lookup_user(Username).

exists(Username) ->
    rabbit_auth_backend_internal:exists(Username).

change_password(Username, Password, ActingUser) ->
    rabbit_auth_backend_internal:change_password(Username, Password, ActingUser).

update_user(Username, Password, Tags, HashingAlgorithm, ActingUser) ->
    rabbit_auth_backend_internal:update_user(Username, Password, Tags, HashingAlgorithm, ActingUser).

clear_password(Username, ActingUser) ->
    rabbit_auth_backend_internal:clear_password(Username, ActingUser).

hash_password(HashingMod, Cleartext) ->
    rabbit_auth_backend_internal:hash_password(HashingMod, Cleartext).

change_password_hash(Username, PasswordHash) ->
    rabbit_auth_backend_internal:change_password_hash(Username, PasswordHash).

change_password_hash(Username, PasswordHash, HashingAlgorithm) ->
    rabbit_auth_backend_internal:change_password_hash(Username, PasswordHash, HashingAlgorithm).

update_user_with_hash(Username, PasswordHash, HashingAlgorithm, ConvertedTags, Limits) ->
    rabbit_auth_backend_internal:update_user_with_hash(Username, PasswordHash, HashingAlgorithm, ConvertedTags, Limits).

set_tags(Username, Tags, ActingUser) ->
    rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser).

set_permissions(Username, VHost, ConfigurePerm, WritePerm, ReadPerm, ActingUser) ->
    rabbit_auth_backend_internal:set_permissions(Username, VHost, ConfigurePerm, WritePerm, ReadPerm, ActingUser).

clear_permissions(Username, VHost, ActingUser) ->
    rabbit_auth_backend_internal:clear_permissions(Username, VHost, ActingUser).

clear_all_permissions_for_vhost(VHost, ActingUser) ->
    rabbit_auth_backend_internal:clear_all_permissions_for_vhost(VHost, ActingUser).

set_permissions_globally(Username, ConfigurePerm, WritePerm, ReadPerm, ActingUser) ->
    rabbit_auth_backend_internal:set_permissions_globally(Username, ConfigurePerm, WritePerm, ReadPerm, ActingUser).

set_topic_permissions(Username, VHost, Exchange, WritePerm, ReadPerm, ActingUser) ->
    rabbit_auth_backend_internal:set_topic_permissions(Username, VHost, Exchange, WritePerm, ReadPerm, ActingUser).

clear_topic_permissions(Username, VHost, ActingUser) ->
    rabbit_auth_backend_internal:clear_topic_permissions(Username, VHost, ActingUser).

clear_topic_permissions(Username, VHost, Exchange, ActingUser) ->
    rabbit_auth_backend_internal:clear_topic_permissions(Username, VHost, Exchange, ActingUser).

put_user(User, ActingUser) ->
    rabbit_auth_backend_internal:put_user(User, ActingUser).

put_user(User, Version, ActingUser) ->
    rabbit_auth_backend_internal:put_user(User, Version, ActingUser).

set_user_limits(Username, Definition, ActingUser) ->
    rabbit_auth_backend_internal:set_user_limits(Username, Definition, ActingUser).

clear_user_limits(Username, LimitType, ActingUser) ->
    rabbit_auth_backend_internal:clear_user_limits(Username, LimitType, ActingUser).

is_over_connection_limit(Username) ->
    rabbit_auth_backend_internal:is_over_connection_limit(Username).

is_over_channel_limit(Username) ->
    rabbit_auth_backend_internal:is_over_channel_limit(Username).

get_user_limits() ->
    rabbit_auth_backend_internal:get_user_limits().

get_user_limits(Username) ->
    rabbit_auth_backend_internal:get_user_limits(Username).

user_info_keys() ->
    rabbit_auth_backend_internal:user_info_keys().

perms_info_keys() ->
    rabbit_auth_backend_internal:perms_info_keys().

user_perms_info_keys() ->
    rabbit_auth_backend_internal:user_perms_info_keys().

vhost_perms_info_keys() ->
    rabbit_auth_backend_internal:vhost_perms_info_keys().

user_vhost_perms_info_keys() ->
    rabbit_auth_backend_internal:user_vhost_perms_info_keys().

user_topic_perms_info_keys() ->
    rabbit_auth_backend_internal:user_topic_perms_info_keys().

user_vhost_topic_perms_info_keys() ->
    rabbit_auth_backend_internal:user_vhost_topic_perms_info_keys().

vhost_topic_perms_info_keys() ->
    rabbit_auth_backend_internal:vhost_topic_perms_info_keys().

all_users() ->
    rabbit_auth_backend_internal:all_users().

list_users() ->
    rabbit_auth_backend_internal:list_users().

list_users(Reference, AggregatorPid) ->
    rabbit_auth_backend_internal:list_users(Reference, AggregatorPid).

list_permissions() ->
    rabbit_auth_backend_internal:list_permissions().

list_user_permissions(Username) ->
    rabbit_auth_backend_internal:list_user_permissions(Username).

list_user_permissions(Username, Reference, AggregatorPid) ->
    rabbit_auth_backend_internal:list_user_permissions(Username, Reference, AggregatorPid).

list_vhost_permissions(VHost) ->
    rabbit_auth_backend_internal:list_vhost_permissions(VHost).

list_vhost_permissions(VHost, Reference, AggregatorPid) ->
    rabbit_auth_backend_internal:list_vhost_permissions(VHost, Reference, AggregatorPid).

list_user_vhost_permissions(Username, VHost) ->
    rabbit_auth_backend_internal:list_user_vhost_permissions(Username, VHost).

list_topic_permissions() ->
    rabbit_auth_backend_internal:list_topic_permissions().

list_user_topic_permissions(Username) ->
    rabbit_auth_backend_internal:list_user_topic_permissions(Username).

list_vhost_topic_permissions(VHost) ->
    rabbit_auth_backend_internal:list_vhost_topic_permissions(VHost).

list_user_vhost_topic_permissions(Username, VHost) ->
    rabbit_auth_backend_internal:list_user_vhost_topic_permissions(Username, VHost).

expand_topic_permission(TopicPermission, Context) ->
    rabbit_auth_backend_internal:expand_topic_permission(TopicPermission, Context).

%%----------------------------------------------------------------------------
%% Manipulation of the user database

add_user_with_pre_hashed_password_sans_validation(Username, PasswordHash, ActingUser) ->
    rabbit_auth_backend_internal:add_user_with_pre_hashed_password_sans_validation(Username, PasswordHash, ActingUser).

add_user_sans_validation(Username, Password, ActingUser) ->
    rabbit_auth_backend_internal:add_user_sans_validation(Username, Password, ActingUser).

add_user_sans_validation(Username, PasswordHash, HashingMod, Tags, Limits, ActingUser) ->
    rabbit_auth_backend_internal:add_user_sans_validation(Username, PasswordHash, HashingMod, Tags, Limits, ActingUser).
