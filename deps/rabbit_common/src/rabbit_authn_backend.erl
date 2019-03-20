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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
