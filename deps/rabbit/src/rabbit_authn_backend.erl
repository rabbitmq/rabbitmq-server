%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_authn_backend).

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
%%
%% If the credentials can expire, the backend must also export
%% expiry_timestamp/1 and update_state/2 as specified in rabbit_authz_backend,
%% even when other backends are used for authorisation.
-callback user_login_authentication(rabbit_types:username(), [term()] | map()) ->
    {'ok', rabbit_types:auth_user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.
