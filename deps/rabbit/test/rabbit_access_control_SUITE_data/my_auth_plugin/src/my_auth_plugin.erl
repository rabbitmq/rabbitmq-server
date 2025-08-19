%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%  Copyright (c) 2025 Broadcom. All Rights Reserved. The term “Broadcom”
%%  refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(my_auth_plugin).

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4]).
-export([expiry_timestamp/1]).

%% -------------------------------------------------------------------
%% Implementation of rabbit_authn_backend.
%% -------------------------------------------------------------------

user_login_authentication(_, _) ->
    {error, unknown_user}.

%% -------------------------------------------------------------------
%% Implementation of rabbit_authz_backend.
%% -------------------------------------------------------------------

user_login_authorization(_, _) ->
    {error, unknown_user}.

check_vhost_access(_AuthUser, _VHostPath, _AuthzData) -> true.
check_resource_access(_AuthUser, _Resource, _Permission, _Context) -> true.
check_topic_access(_AuthUser, _Resource, _Permission, _Context) -> true.

expiry_timestamp(_AuthUser) -> never.
