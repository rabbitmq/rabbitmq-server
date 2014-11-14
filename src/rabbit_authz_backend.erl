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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_authz_backend).

-include("rabbit.hrl").

-ifdef(use_specs).

%% Check a user can log in, when this backend is being used for
%% authorisation only. Authentication has already taken place
%% successfully, but we need to check that the user exists in this
%% backend, and initialise any impl field we will want to have passed
%% back in future calls to check_vhost_access/3 and
%% check_resource_access/3.
%%
%% Possible responses:
%% {ok, Impl}
%%     User authorisation succeeded, and here's the impl field.
%% {error, Error}
%%     Something went wrong. Log and die.
%% {refused, Msg, Args}
%%     User authorisation failed. Log and die.
-callback user_login_authorization(rabbit_types:username()) ->
    {'ok', any()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

%% Given #auth_user and vhost, can a user log in to a vhost?
%% Possible responses:
%% true
%% false
%% {error, Error}
%%     Something went wrong. Log and die.
-callback check_vhost_access(rabbit_types:auth_user(),
                             rabbit_types:vhost(), rabbit_net:socket()) ->
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
                                rabbit_access_control:permission_atom()) ->
    boolean() | {'error', any()}.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{user_login_authorization, 1},
     {check_vhost_access, 3}, {check_resource_access, 3}];
behaviour_info(_Other) ->
    undefined.

-endif.
