%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_authz_backend).

%% Check that a user can log in, when this backend is being used for
%% authorisation only. Authentication has already taken place
%% successfully, but we need to check that the user exists in this
%% backend, and initialise any impl field we will want to have passed
%% back in future calls to check_vhost_access/3 and
%% check_resource_access/3.
%%
%% Possible responses:
%% {ok, Impl}
%% {ok, Impl, Tags}
%%     User authorisation succeeded, and here's the impl and potential extra tags fields.
%% {error, Error}
%%     Something went wrong. Log and die.
%% {refused, Msg, Args}
%%     User authorisation failed. Log and die.
-callback user_login_authorization(rabbit_types:username(), [term()] | map()) ->
    {'ok', any()} |
    {'ok', any(), any()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

%% Given #auth_user, vhost and data (client IP for now), can a user log in to a vhost?
%% Possible responses:
%% true
%% false
%% {error, Error}
%%     Something went wrong. Log and die.
-callback check_vhost_access(AuthUser :: rabbit_types:auth_user(),
                             VHost :: rabbit_types:vhost(),
                             AuthzData :: rabbit_types:authz_data()) ->
    boolean() | {'error', any()}.

%% Given #auth_user, resource and permission, can a user access a resource?
%%
%% Possible responses:
%% true
%% false
%% {error, Error}
%%     Something went wrong. Log and die.
-callback check_resource_access(rabbit_types:auth_user(),
                                rabbit_types:r(atom()),
                                rabbit_types:permission_atom(),
                                rabbit_types:authz_context()) ->
    boolean() | {'error', any()}.

%% Given #auth_user, topic as resource, permission, and context, can a user access the topic?
%%
%% Possible responses:
%% true
%% false
%% {error, Error}
%%     Something went wrong. Log and die.
-callback check_topic_access(rabbit_types:auth_user(),
    rabbit_types:r(atom()),
    rabbit_types:permission_atom(),
    rabbit_types:topic_access_context()) ->
    boolean() | {'error', any()}.

%% Updates backend state that has expired.
%%
%% Possible responses:
%% {ok, User}
%%     Secret updated successfully, and here's the user record.
%% {error, Error}
%%     Something went wrong.
%% {refused, Msg, Args}
%%     New secret is not valid or the user cannot authenticate with it.
-callback update_state(AuthUser :: rabbit_types:auth_user(),
                       NewState :: term()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

%% Get expiry timestamp for the user.
%%
%% Possible responses:
%% never
%%     The user token/credentials never expire.
%% Timestamp
%%     The expiry time (POSIX) in seconds of the token/credentials.
-callback expiry_timestamp(AuthUser :: rabbit_types:auth_user()) ->
    integer() | never.

-optional_callbacks([update_state/2]).
