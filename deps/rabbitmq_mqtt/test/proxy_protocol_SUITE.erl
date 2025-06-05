%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(proxy_protocol_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TIMEOUT, 5000).

all() ->
    [
     {group, v4},
     {group, v5}
    ].

groups() ->
    [
        {v4, [], tests()},
        {v5, [], tests()}
    ].

tests() ->
    [
     proxy_protocol_v1,
     proxy_protocol_v1_tls,
     proxy_protocol_v2_local
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_certspwd, "bunnychow"},
        {rabbitmq_ct_tls_verify, verify_none},
        {start_rmq_with_plugins_disabled, true}
    ]),
    MqttConfig = mqtt_config(),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                [ fun(Conf) -> merge_app_env(MqttConfig, Conf) end ] ++
                    rabbit_ct_broker_helpers:setup_steps() ++
                    rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config2, rabbitmq_mqtt),
    Config2.

mqtt_config() ->
    {rabbitmq_mqtt, [
        {proxy_protocol,  true},
        {ssl_cert_login,  true},
        {allow_anonymous, true}]}.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(Group, Config) ->
    rabbit_ct_helpers:set_config(Config, {mqtt_version, Group}).

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

proxy_protocol_v1(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    ok = inet:send(Socket, connect_packet(Config)),
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    timer:sleep(10),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

proxy_protocol_v1_tls(Config) ->
    app_utils:start_applications([asn1, crypto, public_key, ssl]),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt_tls),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    {ok, SslSocket} = ssl:connect(Socket, [{verify, verify_none}], ?TIMEOUT),
    ok = ssl:send(SslSocket, connect_packet(Config)),
    {ok, _Packet} = ssl:recv(SslSocket, 0, ?TIMEOUT),
    timer:sleep(10),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

proxy_protocol_v2_local(Config) ->
    ProxyInfo = #{
        command => local,
        version => 2
    },
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    ok = inet:send(Socket, connect_packet(Config)),
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    timer:sleep(10),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^127.0.0.1:\\d+ -> 127.0.0.1:\\d+$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

connection_name() ->
    [{_Key, Values}] = ets:tab2list(connection_created),
    {_, Name} = lists:keyfind(name, 1, Values),
    Name.

merge_app_env(MqttConfig, Config) ->
    rabbit_ct_helpers:merge_app_env(Config, MqttConfig).

connect_packet(Config) ->
    case ?config(mqtt_version, Config) of
        v5 ->
            mqtt_5_connect_packet();
        v4 ->
            mqtt_3_1_1_connect_packet()
    end.

mqtt_3_1_1_connect_packet() ->
    <<16,
    24,
    0,
    4,
    77,
    81,
    84,
    84,
    4,
    2,
    0,
    60,
    0,
    12,
    84,
    101,
    115,
    116,
    67,
    111,
    110,
    115,
    117,
    109,
    101,
    114>>.

mqtt_5_connect_packet() ->
    <<16,
    25,
    0,
    4,
    77,
    81,
    84,
    84,
    5,
    2,
    0,
    60,
    0,
    0,
    12,
    84,
    101,
    115,
    116,
    67,
    111,
    110,
    115,
    117,
    109,
    101,
    114>>.
