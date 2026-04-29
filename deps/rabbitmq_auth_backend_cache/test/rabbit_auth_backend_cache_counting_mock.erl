%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Counting auth backend used by the cache plugin's tests. Tracks how
%% many authentication requests reached the backend so tests can pin
%% down cache hit/miss behaviour.
%%
%% Response modes:
%%
%%  * `always_ok' (default): every authentication request succeeds
%%  * `refuse_non_loopback': requests carrying `{is_loopback, true}' in
%%                           `AuthProps' succeed; everything else is refused. Used
%%                           in several `cache_refusals' tests

-module(rabbit_auth_backend_cache_counting_mock).
-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         expiry_timestamp/1]).

-export([init/0, reset/0, set_mode/1,
         authentication_call_count/0]).

-define(COUNTER_KEY, {?MODULE, authentication_calls}).
-define(MODE_KEY,    {?MODULE, response_mode}).

init() ->
    reset().

reset() ->
    persistent_term:put(?COUNTER_KEY, counters:new(1, [])),
    persistent_term:put(?MODE_KEY, always_ok),
    ok.

set_mode(Mode) when Mode =:= always_ok;
                    Mode =:= refuse_non_loopback ->
    persistent_term:put(?MODE_KEY, Mode),
    ok.

authentication_call_count() ->
    counters:get(persistent_term:get(?COUNTER_KEY), 1).

user_login_authentication(Username, AuthProps) ->
    counters:add(persistent_term:get(?COUNTER_KEY), 1, 1),
    case mode() of
        always_ok ->
            ok_user(Username);
        refuse_non_loopback ->
            case is_loopback(AuthProps) of
                true -> ok_user(Username);
                _    -> {refused, "denied: peer is not on loopback", []}
            end
    end.

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

mode() ->
    persistent_term:get(?MODE_KEY, always_ok).

ok_user(Username) ->
    {ok, #auth_user{username = Username,
                    tags     = [],
                    impl     = fun() -> none end}}.

is_loopback(AuthProps) when is_list(AuthProps) ->
    proplists:get_value(is_loopback, AuthProps);
is_loopback(AuthProps) when is_map(AuthProps) ->
    maps:get(is_loopback, AuthProps, undefined).
