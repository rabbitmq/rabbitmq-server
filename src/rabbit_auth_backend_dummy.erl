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

-module(rabbit_auth_backend_dummy).
-include("rabbit.hrl").

-behaviour(rabbit_auth_backend).

-export([description/0]).
-export([user/0]).
-export([check_user_login/2, check_vhost_access/2, check_resource_access/3]).

-ifdef(use_specs).

-spec(user/0 :: () -> rabbit_types:user()).

-endif.

%% A user to be used by the direct client when permission checks are
%% not needed. This user can do anything AMQPish.
user() -> #user{username     = <<"dummy">>,
                tags         = [],
                auth_backend = ?MODULE,
                impl         = none}.

%% Implementation of rabbit_auth_backend

description() ->
    [{name, <<"Dummy">>},
     {description, <<"Database for the dummy user">>}].

check_user_login(_, _) ->
    {refused, "cannot log in conventionally as dummy user", []}.

check_vhost_access(#user{}, _VHostPath) -> true.
check_resource_access(#user{}, #resource{}, _Permission) -> true.
