%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(raw_websocket_SUITE).


-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-include_lib("eunit/include/eunit.hrl").

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 2}}
    ].

all() ->
    [{group, http_tests},
     {group, https_tests}].

groups() ->
    Tests = [
        connection,
        connection_with_protocols,
        pubsub,
        disconnect,
        http_auth
    ],
    [{https_tests, [], Tests},
     {http_tests, [], Tests}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Protocol = case Group of
        http_tests -> "ws";
        https_tests -> "wss"
    end,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE},
                                            {protocol, Protocol},
                                            {rabbitmq_ct_tls_verify, verify_none},
                                            {rabbitmq_ct_tls_fail_if_no_peer_cert, false}]),

    rabbit_ct_helpers:run_setup_steps(
        Config1,
        rabbit_ct_broker_helpers:setup_steps() ++ [fun configure_ssl/1]).

configure_ssl(Config) ->
    ErlangConfig = proplists:get_value(erlang_node_config, Config, []),
    RabbitAppConfig = proplists:get_value(rabbit, ErlangConfig, []),
    RabbitSslConfig = proplists:get_value(ssl_options, RabbitAppConfig, []),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_stomp_tls),
    rabbit_ws_test_util:update_app_env(Config, ssl_config, [{port, Port} | lists:keydelete(port, 1, RabbitSslConfig)]),
    Config.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(http_auth, Config) ->
    rabbit_ws_test_util:update_app_env(Config, use_http_auth, true),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_testcase(http_auth, Config) ->
    rabbit_ws_test_util:update_app_env(Config, use_http_auth, false),
    Config;
end_per_testcase(_, Config) -> Config.

connection(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

connection_with_protocols(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self(),
        undefined, ["v11.stomp", "v10.stomp", "v12.stomp"]),
    {ok, _} = rfc6455_client:open(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.


raw_send(WS, Command, Headers) ->
    raw_send(WS, Command, Headers, <<>>).
raw_send(WS, Command, Headers, Body) ->
    Frame = stomp:marshal(Command, Headers, Body),
    rfc6455_client:send(WS, Frame).

raw_recv(WS) ->
    {ok, P} = rfc6455_client:recv(WS),
    stomp:unmarshal(P).


pubsub(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login","guest"}, {"passcode", "guest"}]),

    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),

    Dst = "/topic/test-" ++ stomp:list_to_hex(binary_to_list(crypto:strong_rand_bytes(8))),

    ok = raw_send(WS, "SUBSCRIBE", [{"destination", Dst},
                                    {"id", "s0"}]),

    CustHdr1K = "x-custom-hdr-1",
    CustHdr1 = {CustHdr1K, "value1"},
    CustHdr2K = "x-custom-hdr-2",
    CustHdr2 = {CustHdr2K, "value2"},
    CustHdr3K = "custom-hdr-3",
    CustHdr3 = {CustHdr3K, "value3"},
    ok = raw_send(WS, "SEND", [{"destination", Dst}, {"content-length", "3"},
                               CustHdr1, CustHdr2, CustHdr3], <<"a\x00a">>),

    {<<"MESSAGE">>, H, <<"a\x00a">>} = raw_recv(WS),

    Dst = binary_to_list(proplists:get_value(<<"destination">>, H)),
    CustHdr1 = {CustHdr1K, binary_to_list(proplists:get_value(list_to_binary(CustHdr1K), H))},
    CustHdr2 = {CustHdr2K, binary_to_list(proplists:get_value(list_to_binary(CustHdr2K), H))},
    CustHdr3 = {CustHdr3K, binary_to_list(proplists:get_value(list_to_binary(CustHdr3K), H))},

    {close, _} = rfc6455_client:close(WS),
    ok.


disconnect(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login","guest"}, {"passcode", "guest"}]),

    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),

    ok = raw_send(WS, "DISCONNECT", []),
    {close, {1000, _}} = rfc6455_client:recv(WS),

    ok.

http_auth(Config) ->
    %% Intentionally put bad credentials in the CONNECT frame,
    %% and good credentials in the Authorization header, to
    %% confirm that the right credentials are picked.
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self(),
        [{login, "guest"}, {passcode, "guest"}]),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login", "bad"}, {"passcode", "bad"}]),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    {close, _} = rfc6455_client:close(WS),

    %% Confirm that if no Authorization header is provided,
    %% the default STOMP plugin credentials are used. We
    %% expect an error because the default credentials are invalid.
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, set_env,
                                      [rabbitmq_stomp, default_user,
                                        [{login, "bad-default"}, {passcode, "bad-default"}]
                                      ]),

    WS2 = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS2),
    ok = raw_send(WS2, "CONNECT", [{"login", "bad"}, {"passcode", "bad"}]),
    {<<"ERROR">>, _, _} = raw_recv(WS2),
    {close, _} = rfc6455_client:close(WS2),

    %% Confirm that we can connect if the default STOMP
    %% credentials are used.
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, set_env,
                                      [rabbitmq_stomp, default_user,
                                        [{login, "guest"}, {passcode, "guest"}]
                                      ]),

    WS3 = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS3),
    ok = raw_send(WS3, "CONNECT", [{"login", "bad"}, {"passcode", "bad"}]),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS3),
    {close, _} = rfc6455_client:close(WS3),

    ok.
