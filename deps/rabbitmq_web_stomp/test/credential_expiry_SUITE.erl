%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(credential_expiry_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [connection_is_closed_when_credential_expires].

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

connection_is_closed_when_credential_expires(Config) ->
    Mod = rabbit_auth_backend_internal,
    ExpiryTimestamp = os:system_time(second) + 5,
    rabbit_ct_broker_helpers:setup_meck(Config),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, meck, new, [Mod, [no_link, passthrough]]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, meck, expect, [Mod, expiry_timestamp, 1, ExpiryTimestamp]),
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    WS = rfc6455_client:new("ws://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    try
        {ok, _} = rfc6455_client:open(WS),
        ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),
        {ok, Connected} = rfc6455_client:recv(WS),
        {<<"CONNECTED">>, _, <<>>} = stomp:unmarshal(Connected),
        {error, timeout} = rfc6455_client:recv(WS, 2000),
        {ok, Payload} = rfc6455_client:recv(WS, 30000),
        {<<"ERROR">>, Headers, _} = stomp:unmarshal(Payload),
        <<"Credential expired">> = proplists:get_value(<<"message">>, Headers),
        {close, _} = rfc6455_client:recv(WS, 30000)
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, 0, meck, unload, [Mod])
    end.

raw_send(WS, Command, Headers) ->
    Frame = stomp:marshal(Command, Headers, <<>>),
    rfc6455_client:send(WS, Frame).
