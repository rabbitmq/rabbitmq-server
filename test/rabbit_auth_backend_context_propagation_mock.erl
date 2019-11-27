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
%% Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.
%%

%% A mock authn/authz that records information during calls. For testing purposes only.

-module(rabbit_auth_backend_context_propagation_mock).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/1,
  check_vhost_access/3, check_resource_access/3, check_topic_access/4,
  get/1, init/0]).

init() ->
  ets:new(?MODULE, [set, public, named_table]).

user_login_authentication(_, AuthProps) ->
  ets:insert(?MODULE, {authentication, AuthProps}),
  {ok, #auth_user{username = <<"dummy">>,
    tags = [],
    impl = none}}.

user_login_authorization(_) ->
  {ok, does_not_matter}.

check_vhost_access(#auth_user{}, _VHostPath, Sock) ->
  ets:insert(?MODULE, {vhost_access, Sock}),
  true.
check_resource_access(#auth_user{}, #resource{}, _Permission) ->
  true.
check_topic_access(#auth_user{}, #resource{}, _Permission, TopicContext) ->
  ets:insert(?MODULE, {topic_access, TopicContext}),
  true.

get(K) ->
  ets:lookup(?MODULE, K).
