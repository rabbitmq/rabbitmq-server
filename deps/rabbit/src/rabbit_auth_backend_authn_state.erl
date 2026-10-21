%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_authn_state).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authz_backend).

-export([impl/3]).
-export([user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         update_state/2, expiry_timestamp/1]).

%% Carries the state of an authentication backend whose credentials expire
%% in `#user.authz_backends`, when other backends authorise. It only refuses
%% once they have expired, and cannot be configured in `auth_backends`.

%% `rabbit_access_control:update_state/2` rebuilds `#user.tags` from the
%% refreshed backends only, so the authorisation backends' tags are kept here.
-spec impl(module(), rabbit_types:auth_user(), [atom() | binary()]) -> term().
impl(ModN, #auth_user{impl = Impl, tags = ModNTags}, ModZTags) ->
    {ModN, Impl, ModNTags, ModZTags}.

user_login_authorization(_, _) ->
    {refused, "~ts cannot be used as an authorisation backend", [?MODULE]}.

check_vhost_access(#auth_user{} = AuthUser, _VHostPath, _AuthzData) ->
    credentials_valid(AuthUser).

check_resource_access(#auth_user{} = AuthUser, #resource{}, _Permission, _Context) ->
    credentials_valid(AuthUser).

check_topic_access(#auth_user{} = AuthUser, #resource{}, _Permission, _Context) ->
    credentials_valid(AuthUser).

update_state(#auth_user{impl = {ModN, Impl, ModNTags, ModZTags}} = AuthUser,
             NewState) ->
    case ModN:update_state(AuthUser#auth_user{tags = ModNTags, impl = Impl},
                           NewState) of
        {ok, #auth_user{tags = Tags1, impl = Impl1} = AuthUser1} ->
            {ok, AuthUser1#auth_user{tags = Tags1 ++ ModZTags,
                                     impl = {ModN, Impl1, Tags1, ModZTags}}};
        Else ->
            Else
    end.

expiry_timestamp(#auth_user{impl = {ModN, Impl, _ModNTags, _ModZTags}} = AuthUser) ->
    case erlang:function_exported(ModN, expiry_timestamp, 1) of
        true  -> ModN:expiry_timestamp(AuthUser#auth_user{impl = Impl});
        false -> never
    end.

credentials_valid(#auth_user{impl = {ModN, _Impl, _ModNTags, _ModZTags}} = AuthUser) ->
    case expiry_timestamp(AuthUser) of
        never ->
            true;
        Ts ->
            Ts > os:system_time(seconds) orelse
                {false, rabbit_misc:format("credentials verified by ~ts have expired",
                                           [ModN])}
    end.
