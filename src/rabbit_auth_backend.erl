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

-module(rabbit_auth_backend).

-ifdef(use_specs).

%% A description proplist as with auth mechanisms,
%% exchanges. Currently unused.
-callback description() -> [proplists:property()].

%% Check a user can log in, given a username and a proplist of
%% authentication information (e.g. [{password, Password}]).
%%
%% Possible responses:
%% {ok, User}
%%     Authentication succeeded, and here's the user record.
%% {error, Error}
%%     Something went wrong. Log and die.
%% {refused, Msg, Args}
%%     Client failed authentication. Log and die.
-callback check_user_login(rabbit_types:username(), [term()]) ->
    {'ok', rabbit_types:user()} |
    {'refused', string(), [any()]} |
    {'error', any()}.

%% Given #user and vhost, can a user log in to a vhost?
%% Possible responses:
%% true
%% false
%% {error, Error}
%%     Something went wrong. Log and die.
-callback check_vhost_access(rabbit_types:user(), rabbit_types:vhost()) ->
    boolean() | {'error', any()}.


%% Given #user, resource and permission, can a user access a resource?
%%
%% Possible responses:
%% true
%% false
%% {error, Error}
%%     Something went wrong. Log and die.
-callback check_resource_access(rabbit_types:user(),
                                rabbit_types:r(atom()),
                                rabbit_access_control:permission_atom()) ->
    boolean() | {'error', any()}.

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {check_user_login, 2}, {check_vhost_access, 2},
     {check_resource_access, 3}];
behaviour_info(_Other) ->
    undefined.

-endif.
