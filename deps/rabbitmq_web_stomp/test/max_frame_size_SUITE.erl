%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(max_frame_size_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [pre_auth_frame_within_limit,
     pre_auth_frame_exceeding_limit_closes_connection,
     post_auth_frame_within_full_limit].

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
    %% A small unauthenticated limit so we can test the boundary
    %% without sending megabytes of data.
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, set_env,
      [rabbitmq_stomp, max_frame_size_unauthenticated, 512]),
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, set_env,
      [rabbitmq_stomp, max_frame_size, 1_048_576]),
    Config.

end_per_testcase(_Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, unset_env,
      [rabbitmq_stomp, max_frame_size_unauthenticated]),
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, unset_env,
      [rabbitmq_stomp, max_frame_size]),
    Config.

%% A STOMP CONNECT frame that fits within the unauthenticated limit
%% must be accepted normally.
pre_auth_frame_within_limit(Config) ->
    WS = open_ws(Config),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

%% A pre-auth frame exceeding the unauthenticated limit must cause
%% cowboy to close the connection.
pre_auth_frame_exceeding_limit_closes_connection(Config) ->
    WS = open_ws(Config),
    %% 512 + 4096 = 4608 is the cowboy max_frame_size.
    %% Send a frame larger than that before authenticating.
    Junk = list_to_binary(lists:duplicate(5000, $x)),
    ok = rfc6455_client:send(WS, Junk),
    {close, _} = rfc6455_client:recv(WS, 5000),
    ok.

%% After a successful STOMP CONNECT the frame size limit is raised
%% to the full configured value, so larger frames are accepted.
post_auth_frame_within_full_limit(Config) ->
    WS = open_ws(Config),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    Dst = "/topic/max-frame-test-" ++
        stomp:list_to_hex(binary_to_list(crypto:strong_rand_bytes(8))),
    ok = raw_send(WS, "SUBSCRIBE", [{"destination", Dst}, {"id", "s0"}]),
    %% Send a body larger than the unauthenticated limit.
    %% This must succeed because the limit was raised after CONNECT.
    Body = list_to_binary(lists:duplicate(2000, $A)),
    BodySize = integer_to_list(byte_size(Body)),
    ok = raw_send(WS, "SEND",
                  [{"destination", Dst}, {"content-length", BodySize}],
                  Body),
    {<<"MESSAGE">>, _, Body} = raw_recv(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

%%
%% Helpers
%%

open_ws(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    WS.

raw_send(WS, Command, Headers) ->
    raw_send(WS, Command, Headers, <<>>).

raw_send(WS, Command, Headers, Body) ->
    Frame = stomp:marshal(Command, Headers, Body),
    rfc6455_client:send(WS, Frame).

raw_recv(WS) ->
    {ok, P} = rfc6455_client:recv(WS),
    stomp:unmarshal(P).
