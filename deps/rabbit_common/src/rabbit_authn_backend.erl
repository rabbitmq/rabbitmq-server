%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_authn_backend).

-include("rabbit.hrl").

%% Check a user can log in, given a username and a proplist of
%% authentication information (e.g. [{password, Password}]). If your
%% backend is not to be used for authentication, this should always
%% refuse access.
%%
%% Possible responses:
%% {ok, User}
%%     Authentication succeeded, and here's the user record.
%% {error, Error}
%%     Something went wrong. Log and die.
%% {refused, Msg, Args}
%%     Client failed authentication. Log and die.
-callback user_login_authentication(rabbit_types:username(), [term()] | map()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.
