%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Counting authentication and authorization backend used by
%% rabbit_auth_backend_cache_SUITE to verify that the cache backend hits
%% across reconnections that share the same credentials.

-module(rabbit_auth_backend_cache_counting_mock).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         expiry_timestamp/1]).

-export([init/0, reset/0, authentication_call_count/0]).

-define(KEY, {?MODULE, authentication_calls}).

init() ->
    reset().

reset() ->
    persistent_term:put(?KEY, counters:new(1, [])),
    ok.

authentication_call_count() ->
    counters:get(persistent_term:get(?KEY), 1).

user_login_authentication(Username, _AuthProps) ->
    counters:add(persistent_term:get(?KEY), 1, 1),
    {ok, #auth_user{username = Username,
                    tags = [],
                    impl = fun() -> none end}}.

user_login_authorization(_Username, _AuthProps) ->
    {ok, fun() -> none end, []}.

check_vhost_access(#auth_user{}, _VHostPath, _AuthzData) ->
    true.

check_resource_access(#auth_user{}, #resource{}, _Permission, _AuthzContext) ->
    true.

check_topic_access(#auth_user{}, #resource{}, _Permission, _TopicContext) ->
    true.

expiry_timestamp(_) ->
    never.
