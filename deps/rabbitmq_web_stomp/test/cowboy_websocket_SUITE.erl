%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(cowboy_websocket_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, integration},
     {group, default_login_enabled},
     {group, default_login_disabled}].

groups() ->
    [
     {integration, [],
        [
            connection_fails,
            pubsub,
            pubsub_binary,
            sub_non_existent,
            disconnect,
            http_auth,
            wss_http2
      ]},
      %% rabbitmq/rabbitmq-web-stomp#110
      {default_login_enabled, [],
          [
              connection_with_explicitly_provided_correct_credentials,
              connection_with_default_login_succeeds
          ]},
      %% rabbitmq/rabbitmq-web-stomp#110
      {default_login_disabled, [],
          [
              connection_with_explicitly_provided_correct_credentials,
              connection_without_credentials_fails
          ]}
    ].

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE},
                                            {protocol, "ws"}]),
    rabbit_ct_helpers:log_environment(),
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1),
    {rmq_certsdir, CertsDir} = proplists:lookup(rmq_certsdir, Config2),
    Config3 = rabbit_ct_helpers:merge_app_env(
                Config2,
                {rabbitmq_web_stomp,
                 [{ssl_config,
                   [{cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                    {certfile, filename:join([CertsDir, "server", "cert.pem"])},
                    {keyfile, filename:join([CertsDir, "server", "key.pem"])},
                    %% We only want to ensure HTTP/2 Websocket is working.
                    {fail_if_no_peer_cert, false},
                    {versions, ['tlsv1.3']},
                    %% We hard code this port number here because it will be computed later by
                    %% rabbit_ct_broker_helpers:init_tcp_port_numbers/3 when we start the broker.
                    %% (The alternative is to first start the broker, stop the rabbitmq_web_amqp app,
                    %% configure tls_config, and then start the app again.)
                    {port, 21014}
                   ]}]}),
    rabbit_ct_helpers:run_setup_steps(Config3,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(pubsub_binary, Config) ->
    rabbit_ws_test_util:update_app_env(Config, ws_frame, binary),
    Config;
init_per_testcase(http_auth, Config) ->
    rabbit_ws_test_util:update_app_env(Config, use_http_auth, true),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_testcase(pubsub_binary, Config) ->
    rabbit_ws_test_util:update_app_env(Config, ws_frame, text),
    Config;
end_per_testcase(http_auth, Config) ->
    rabbit_ws_test_util:update_app_env(Config, use_http_auth, false),
    Config;
end_per_testcase(_, Config) -> Config.


connection_fails(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login", "uncorrect_$55"}, {"passcode", "uncorrect_$88"}]),
    {<<"ERROR">>, _, <<"Access refused for user 'uncorrect_$55'">>} = raw_recv(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

connection_with_explicitly_provided_correct_credentials(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),

    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.

connection_with_default_login_succeeds(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", []),

    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),
    {close, _} = rfc6455_client:close(WS),
    ok.
connection_without_credentials_fails(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, set_env,
                                      [rabbitmq_stomp, default_user, undefined]),

    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),

    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT"),

    {close, _} = raw_recv(WS),
    ok.

raw_send(WS, Command) ->
    raw_send(WS, Command, [], <<>>).

raw_send(WS, Command, Headers) ->
    raw_send(WS, Command, Headers, <<>>).
raw_send(WS, Command, Headers, Body) ->
    Frame = stomp:marshal(Command, Headers, Body),
    rfc6455_client:send(WS, Frame).

raw_recv(WS) ->
    case rfc6455_client:recv(WS) of
      {ok, P} -> stomp:unmarshal(P);
      Other   -> Other
    end.

pubsub(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),

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

%% Issue #6737
sub_non_existent(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),

    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),

    ok = raw_send(WS, "SUBSCRIBE", [{"destination", "/exchange/doesnotexist"},
                                    {"id", "s0"}]),

    {<<"ERROR">>, [{<<"message">>,<<"not_found">>} | _Tail ], <<"NOT_FOUND - no exchange 'doesnotexist' in vhost '/'">>} = raw_recv(WS),

    {close, _} = raw_recv(WS),
    ok.


raw_send_binary(WS, Command, Headers) ->
    raw_send_binary(WS, Command, Headers, <<>>).
raw_send_binary(WS, Command, Headers, Body) ->
    Frame = stomp:marshal(Command, Headers, Body),
    rfc6455_client:send_binary(WS, Frame).

raw_recv_binary(WS) ->
    {binary, P} = rfc6455_client:recv(WS),
    stomp:unmarshal(P).


pubsub_binary(Config) ->
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login","guest"}, {"passcode", "guest"}]),

    {<<"CONNECTED">>, _, <<>>} = raw_recv_binary(WS),

    Dst = "/topic/test-" ++ stomp:list_to_hex(binary_to_list(crypto:strong_rand_bytes(8))),

    ok = raw_send(WS, "SUBSCRIBE", [{"destination", Dst},
                                    {"id", "s0"}]),

    ok = raw_send(WS, "SEND", [{"destination", Dst},
                              {"content-length", "3"}], <<"a\x00a">>),

    {<<"MESSAGE">>, H, <<"a\x00a">>} = raw_recv_binary(WS),
    Dst = binary_to_list(proplists:get_value(<<"destination">>, H)),

    {close, _} = rfc6455_client:close(WS).

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
    %% expect an error because the default credentials are
    %% left undefined.
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      application, set_env,
                                      [rabbitmq_stomp, default_user,
                                        [{login, "bad-default"}, {passcode, "bad-default"}]
                                      ]),

Protocol = ?config(protocol, Config),
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

Protocol = ?config(protocol, Config),
    WS3 = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS3),
    ok = raw_send(WS3, "CONNECT", [{"login", "bad"}, {"passcode", "bad"}]),
    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS3),
    {close, _} = rfc6455_client:close(WS3),

    ok.

wss_http2(Config) ->
    {ok, _} = application:ensure_all_started(gun),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_stomp_tls),
    {ok, ConnPid} = gun:open("localhost", Port, #{
        transport => tls,
        tls_opts => [{verify, verify_none}],
        protocols => [http2],
        http2_opts => #{notify_settings_changed => true},
        ws_opts => #{protocols => [{<<"v12.stomp">>, gun_ws_h}]}
    }),
    {ok, http2} = gun:await_up(ConnPid),
    {notify, settings_changed, #{enable_connect_protocol := true}}
        = gun:await(ConnPid, undefined),
    StreamRef = gun:ws_upgrade(ConnPid, "/ws", []),
    {upgrade, [<<"websocket">>], _} = gun:await(ConnPid, StreamRef),
    gun:ws_send(ConnPid, StreamRef, {text, stomp:marshal("CONNECT", [{"login","guest"}, {"passcode", "guest"}])}),
    {ws, {text, P}} = gun:await(ConnPid, StreamRef),
    {<<"CONNECTED">>, _, <<>>} = stomp:unmarshal(P),
    ok.
