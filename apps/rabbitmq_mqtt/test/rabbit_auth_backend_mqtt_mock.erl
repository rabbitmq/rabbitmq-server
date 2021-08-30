%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% A mock authn/authz that records information during calls. For testing purposes only.

-module(rabbit_auth_backend_mqtt_mock).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([setup/1,
         user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         state_can_expire/0,
         get/1]).

setup(CallerPid) ->
    ets:new(?MODULE, [set, public, named_table]),
    CallerPid ! ok,
    receive
        stop -> ok
    end.


user_login_authentication(_, AuthProps) ->
    ets:insert(?MODULE, {authentication, AuthProps}),
    {ok, #auth_user{username = <<"dummy">>,
                    tags     = [],
                    impl     = none}}.

user_login_authorization(_, _) ->
    io:format("login authorization"),
    {ok, does_not_matter}.

check_vhost_access(#auth_user{}, _VHostPath, AuthzData) ->
    ets:insert(?MODULE, {vhost_access, AuthzData}),
    true.
check_resource_access(#auth_user{}, #resource{}, _Permission, AuthzContext) ->
    ets:insert(?MODULE, {resource_access, AuthzContext}),
    true.
check_topic_access(#auth_user{}, #resource{}, _Permission, TopicContext) ->
    ets:insert(?MODULE, {topic_access, TopicContext}),
    true.

state_can_expire() -> false.

get(K) ->
    ets:lookup(?MODULE, K).
