%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(login_timeout_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [unauthenticated_connection_is_closed,
     authenticated_connection_survives_timeout].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE},
                                            {protocol, "ws"}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, set_env, [rabbitmq_stomp, login_timeout, 400]),
    Config.

end_per_testcase(_Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, unset_env, [rabbitmq_stomp, login_timeout]),
    Config.

%% A WebSocket client that never sends STOMP CONNECT must be
%% disconnected after the login timeout expires.
unauthenticated_connection_is_closed(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    {close, _} = rfc6455_client:recv(WS, 5000),
    ok.

%% A client that authenticates before the timeout must not be
%% disconnected when the timer fires.
authenticated_connection_survives_timeout(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),
    {ok, _} = rfc6455_client:recv(WS),
    timer:sleep(600),
    ok = raw_send(WS, "DISCONNECT", []),
    {close, {1000, _}} = rfc6455_client:recv(WS),
    ok.

raw_send(WS, Command, Headers) ->
    Frame = stomp:marshal(Command, Headers, <<>>),
    rfc6455_client:send(WS, Frame).
