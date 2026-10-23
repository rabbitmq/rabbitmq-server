%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Auth backend with expiring credentials used by the cache plugin's
%% tests, modelled on the JWT tokens of rabbit_auth_backend_oauth2: the
%% "password" is the expiry timestamp (POSIX seconds) of the credentials
%% and every callback refuses once it has passed. Calls are counted so
%% that tests can tell cache hits from misses.

-module(rabbit_auth_backend_cache_expiring_mock).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         expiry_timestamp/1]).

-export([init/0, call_count/0]).

-define(COUNTER_KEY, {?MODULE, calls}).

init() ->
    persistent_term:put(?COUNTER_KEY, counters:new(1, [])),
    ok.

call_count() ->
    counters:get(persistent_term:get(?COUNTER_KEY), 1).

user_login_authentication(Username, AuthProps) ->
    count(),
    Ts = proplists:get_value(password, AuthProps),
    case is_valid(Ts) of
        true  -> {ok, #auth_user{username = Username,
                                 tags     = [],
                                 impl     = fun() -> Ts end}};
        false -> {refused, "credentials have expired", []}
    end.

user_login_authorization(_Username, AuthProps) ->
    count(),
    Ts = proplists:get_value(password, AuthProps),
    case is_valid(Ts) of
        true  -> {ok, fun() -> Ts end};
        false -> {refused, "credentials have expired", []}
    end.

check_vhost_access(#auth_user{impl = Impl}, _VHostPath, _AuthzData) ->
    count(),
    is_valid(Impl()).

check_resource_access(#auth_user{impl = Impl}, #resource{}, _Permission, _AuthzContext) ->
    count(),
    is_valid(Impl()).

check_topic_access(#auth_user{impl = Impl}, #resource{}, _Permission, _TopicContext) ->
    count(),
    is_valid(Impl()).

expiry_timestamp(#auth_user{impl = Impl}) ->
    Impl().

is_valid(Ts) ->
    Ts > os:system_time(second).

count() ->
    counters:add(persistent_term:get(?COUNTER_KEY), 1, 1).
