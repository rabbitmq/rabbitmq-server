%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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

-define(BLANK_PASSWORD_REJECTION_MESSAGE,
        "user '~ts' attempted to log in with a blank password, which is prohibited by the internal authN backend. "
        "To use TLS/x509 certificate-based authentication, see the rabbitmq_auth_mechanism_ssl plugin and configure the client to use the EXTERNAL authentication mechanism. "
        "Alternatively change the password for the user to be non-blank.").

-define(INVALID_CREDENTIALS_REJECTION_MESSAGE, "user '~ts' - invalid credentials").

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
            login_from_localhost_with_nonloopback_user,
            login_from_localhost_with_wrong_password_is_refused,
            login_from_localhost_with_blank_password_is_refused,
            login_from_localhost_with_unknown_user_is_refused,
            login_without_loopback_info_is_refused
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
    [{peer_addr, ?LOCALHOST_ADDR} | Config];
init_per_group(nonlocalhost_connection, Config) ->
    [{peer_addr, ?NONLOCALHOST_ADDR} | Config];
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

%% Security canary: a loopback connection presenting the wrong password
%% must be refused with an invalid-credentials error. Guards the salted-hash
%% comparison against an accidental fail-open regression.
login_from_localhost_with_wrong_password_is_refused(Config) ->
    AuthProps = build_auth_props(<<"not-the-right-password">>, ?LOCALHOST_ADDR),
    {refused, ?INVALID_CREDENTIALS_REJECTION_MESSAGE, _FailArgs} =
        rpc(Config, rabbit_auth_backend_internal_loopback, user_login_authentication,
            [maps:get(username, ?LOOPBACK_USER), AuthProps]).

%% Security canary: a loopback connection with a blank password must be
%% refused, even though the address is trusted.
login_from_localhost_with_blank_password_is_refused(Config) ->
    AuthProps = build_auth_props(<<"">>, ?LOCALHOST_ADDR),
    {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE, _FailArgs} =
        rpc(Config, rabbit_auth_backend_internal_loopback, user_login_authentication,
            [maps:get(username, ?LOOPBACK_USER), AuthProps]).

%% Security canary: a loopback connection for a user that does not exist
%% must be refused with an invalid-credentials error.
login_from_localhost_with_unknown_user_is_refused(Config) ->
    AuthProps = build_auth_props(<<"any-password">>, ?LOCALHOST_ADDR),
    {refused, ?INVALID_CREDENTIALS_REJECTION_MESSAGE, _FailArgs} =
        rpc(Config, rabbit_auth_backend_internal_loopback, user_login_authentication,
            [<<"NoSuchUser">>, AuthProps]).

%% Security canary: when no socket or address is provided, the backend cannot
%% determine whether the connection is from localhost and must fail closed.
login_without_loopback_info_is_refused(Config) ->
    AuthProps = [{password, maps:get(password, ?LOOPBACK_USER)}],
    {refused, ?NO_SOCKET_OR_ADDRESS_REJECTION_MESSAGE, _FailArgs} =
        rpc(Config, rabbit_auth_backend_internal_loopback, user_login_authentication,
            [maps:get(username, ?LOOPBACK_USER), AuthProps]).

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

build_auth_props(Pass, Addr) ->
    [{password, Pass}, {is_loopback, rabbit_net:is_loopback(Addr)}].
