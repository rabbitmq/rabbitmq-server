%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_dummy).
-include("rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user/0]).
-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4]).
-export([expiry_timestamp/1]).

-spec user() -> rabbit_types:user().

%% A user to be used by the direct client when permission checks are
%% not needed. This user can do anything AMQPish.
user() -> #user{username       = <<"none">>,
                tags           = [],
                authz_backends = [{?MODULE, none}]}.

%% Implementation of rabbit_auth_backend

user_login_authentication(_, _) ->
    {refused, "cannot log in conventionally as dummy user", []}.

user_login_authorization(_, _) ->
    {refused, "cannot log in conventionally as dummy user", []}.

check_vhost_access(#auth_user{}, _VHostPath, _AuthzData) -> true.
check_resource_access(#auth_user{}, #resource{}, _Permission, _Context) -> true.
check_topic_access(#auth_user{}, #resource{}, _Permission, _Context) -> true.

expiry_timestamp(_) -> never.
