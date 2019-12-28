%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_authz_backend).

-include("rabbit.hrl").

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
                                rabbit_access_control:permission_atom(),
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
    rabbit_access_control:permission_atom(),
    rabbit_types:topic_access_context()) ->
    boolean() | {'error', any()}.

%% Returns true for backends that support state or credential expiration (e.g. use JWTs).
-callback state_can_expire() -> boolean().

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

-optional_callbacks([update_state/2]).
