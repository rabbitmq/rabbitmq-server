%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(web_mqtt_origin_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ALLOWED, "https://allowed.example.com").
-define(ALLOWED2, "https://other.example.com").

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [no_allowlist_allows_any_origin,
     allowed_origin_connects,
     disallowed_origin_rejected,
     no_origin_header_allowed_when_allowlist_set,
     multiple_allowed_origins].

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

init_per_testcase(no_allowlist_allows_any_origin, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, unset_env, [rabbitmq_web_mqtt, allow_origins]),
    Config;
init_per_testcase(multiple_allowed_origins, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, set_env, [rabbitmq_web_mqtt, allow_origins,
                              [?ALLOWED, ?ALLOWED2]]),
    Config;
init_per_testcase(_Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, set_env, [rabbitmq_web_mqtt, allow_origins,
                              [?ALLOWED]]),
    Config.

end_per_testcase(_Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      application, unset_env, [rabbitmq_web_mqtt, allow_origins]),
    Config.

%% When no allowlist is configured, any Origin is accepted.
no_allowlist_allows_any_origin(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws",
                            self(), undefined, ["mqtt"], <<>>,
                            [{"Origin", "https://evil.example.com"}]),
    {ok, _} = rfc6455_client:open(WS),
    rfc6455_client:send_binary(WS, rabbit_ws_test_util:mqtt_3_1_1_connect_packet()),
    {binary, _ConnAck} = rfc6455_client:recv(WS, 5000),
    rfc6455_client:send_binary(WS, <<224, 0>>),
    {close, _} = rfc6455_client:recv(WS, 5000),
    ok.

%% A client presenting a matching Origin is accepted.
allowed_origin_connects(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws",
                            self(), undefined, ["mqtt"], <<>>,
                            [{"Origin", ?ALLOWED}]),
    {ok, _} = rfc6455_client:open(WS),
    rfc6455_client:send_binary(WS, rabbit_ws_test_util:mqtt_3_1_1_connect_packet()),
    {binary, _ConnAck} = rfc6455_client:recv(WS, 5000),
    rfc6455_client:send_binary(WS, <<224, 0>>),
    {close, _} = rfc6455_client:recv(WS, 5000),
    ok.

%% A client presenting a non-matching Origin gets HTTP 403.
disallowed_origin_rejected(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    Port = list_to_integer(PortStr),
    Resp = raw_ws_upgrade(Port, [{"Origin", "https://evil.example.com"}]),
    ?assertNotEqual(nomatch, binary:match(Resp, <<"403">>)).

%% When an allowlist is set, requests without an Origin header
%% are still accepted (they originate from non-browser clients).
no_origin_header_allowed_when_allowlist_set(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    Port = list_to_integer(PortStr),
    Resp = raw_ws_upgrade(Port, []),
    ?assertNotEqual(nomatch, binary:match(Resp, <<"101">>)).

%% When multiple origins are configured, each one is accepted.
multiple_allowed_origins(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    Port = list_to_integer(PortStr),

    Resp1 = raw_ws_upgrade(Port, [{"Origin", ?ALLOWED}]),
    ?assertNotEqual(nomatch, binary:match(Resp1, <<"101">>)),

    Resp2 = raw_ws_upgrade(Port, [{"Origin", ?ALLOWED2}]),
    ?assertNotEqual(nomatch, binary:match(Resp2, <<"101">>)),

    Resp3 = raw_ws_upgrade(Port, [{"Origin", "https://evil.example.com"}]),
    ?assertNotEqual(nomatch, binary:match(Resp3, <<"403">>)).

%%
%% Helpers
%%

raw_ws_upgrade(Port, Headers) ->
    {ok, Sock} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                 [binary, {active, false}]),
    Key = base64:encode(crypto:strong_rand_bytes(16)),
    OriginHd = case lists:keyfind("Origin", 1, Headers) of
        {_, Val} -> ["Origin: ", Val, "\r\n"];
        false -> []
    end,
    Req = [
        "GET /ws HTTP/1.1\r\n",
        "Host: 127.0.0.1:", integer_to_list(Port), "\r\n",
        "Upgrade: websocket\r\n",
        "Connection: Upgrade\r\n",
        OriginHd,
        "Sec-WebSocket-Key: ", Key, "\r\n",
        "Sec-WebSocket-Protocol: mqtt\r\n",
        "Sec-WebSocket-Version: 13\r\n",
        "\r\n"
    ],
    ok = gen_tcp:send(Sock, Req),
    {ok, Resp} = gen_tcp:recv(Sock, 0, 5000),
    gen_tcp:close(Sock),
    Resp.
