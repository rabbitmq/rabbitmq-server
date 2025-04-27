%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_auth_backend_internal_loopback_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(NO_SOCKET_OR_ADDRESS_REJECTION_MESSAGE,
        "user '~ts' attempted to log in, but no socket or address was provided "
        "to the internal_loopback auth backend, so cannot verify if connection "
        "is from localhost or not.").

-define(NOT_LOOPBACK_REJECTION_MESSAGE,
        "user '~ts' attempted to log in, but the socket or address was not from "
        "loopback/localhost, which is prohibited by the internal loopback authN "
        "backend.").

-define(LOOPBACK_USER, #{username => <<"TestLoopbackUser">>,
                        password => <<"TestLoopbackUser">>,
                        expected_credentials => [username, password],
                        tags => [policymaker, monitoring]}).

-define(NONLOOPBACK_USER, #{username => <<"TestNonLoopbackUser">>,
                        password => <<"TestNonLoopbackUser">>,
                        expected_credentials => [username, password],
                        tags => [policymaker, monitoring]}).
-define(LOCALHOST_ADDR, {127,0,0,1}).
-define(NONLOCALHOST_ADDR, {192,168,1,1}).

all() ->
    [
        {group, localhost_connection},
        {group, nonlocalhost_connection}
    ].

groups() ->
    [
        {localhost_connection, [], [
            login_from_localhost_with_loopback_user,
            login_from_localhost_with_nonloopback_user
        ]},
        {nonlocalhost_connection, [], [
            login_from_nonlocalhost_with_loopback_user,
            login_from_nonlocalhost_with_nonloopback_user
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, rabbit_ct_broker_helpers:setup_steps() ++ [ fun setup_env/1 ]).

setup_env(Config) ->
    application:set_env(rabbit, auth_backends, [rabbit_auth_backend_internal_loopback]),
    Config.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(localhost_connection, Config) ->
    ok = rabbit_ct_broker_helpers:add_user(Config, maps:get(username, ?LOOPBACK_USER)),
    ok = rabbit_ct_broker_helpers:add_user(Config, maps:get(username, ?NONLOOPBACK_USER)),
    [{sockOrAddr, ?LOCALHOST_ADDR} | Config];
init_per_group(nonlocalhost_connection, Config) ->
    [{sockOrAddr, ?NONLOCALHOST_ADDR} | Config];
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

% Test cases for localhost connections
login_from_localhost_with_loopback_user(Config) ->
    AuthProps = build_auth_props(maps:get(password, ?LOOPBACK_USER), ?LOCALHOST_ADDR),
    {ok, _AuthUser} = rpc(Config, rabbit_auth_backend_internal_loopback, user_login_authentication,
        [maps:get(username, ?LOOPBACK_USER), AuthProps]).

login_from_localhost_with_nonloopback_user(Config) ->
    AuthProps = build_auth_props(maps:get(password, ?NONLOOPBACK_USER), ?LOCALHOST_ADDR),
    {ok, _AuthUser} = rpc(Config, rabbit_auth_backend_internal_loopback, user_login_authentication,
        [maps:get(username, ?NONLOOPBACK_USER), AuthProps]).

% Test cases for non-localhost connections
login_from_nonlocalhost_with_loopback_user(Config) ->
    AuthProps = build_auth_props(maps:get(password, ?LOOPBACK_USER), ?NONLOCALHOST_ADDR),
    {refused, _FailMsg, _FailArgs} = rpc(Config, rabbit_auth_backend_internal_loopback, user_login_authentication,
        [maps:get(username, ?LOOPBACK_USER), AuthProps]).

login_from_nonlocalhost_with_nonloopback_user(Config) ->
    AuthProps = build_auth_props(maps:get(password, ?NONLOOPBACK_USER), ?NONLOCALHOST_ADDR),
    {refused, _FailMsg, _FailArgs} = rpc(Config, rabbit_auth_backend_internal_loopback, user_login_authentication,
        [maps:get(username, ?NONLOOPBACK_USER), AuthProps]).

rpc(Config, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, M, F, A).

build_auth_props(Pass, Socket) ->
    [{password, Pass}, {sockOrAddr, Socket}].
