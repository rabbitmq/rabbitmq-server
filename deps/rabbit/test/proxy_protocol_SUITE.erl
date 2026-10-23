%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(proxy_protocol_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-define(TIMEOUT, 5000).

all() ->
    [{group, trusted},
     {group, untrusted},
     {group, invalid},
     {group, cidr},
     {group, unconfigured}].

groups() ->
    Accepted = [proxy_protocol_v1, proxy_protocol_v1_tls, proxy_protocol_v2_local],
    [{trusted, [], Accepted},
     {untrusted, [], [proxy_protocol_v1_untrusted_source_rejected,
                     proxy_protocol_v1_tls_untrusted_source_rejected,
                     proxy_protocol_tls_untrusted_source_rejected_without_header,
                     proxy_protocol_v2_local_untrusted_source_rejected]},
     {invalid, [], [proxy_protocol_v1_untrusted_source_rejected]},
     {cidr, [], Accepted},
     {unconfigured, [], Accepted}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    TrustedProxies = case Group of
        trusted -> ["127.0.0.1", "::1"];
        untrusted -> ["10.0.0.0/8"];
        invalid -> ["not-an-ip"];
        cidr -> ["127.0.0.0/8"];
        unconfigured -> []
    end,
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group}
    ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1, [
        {rabbit, [
            {proxy_protocol, true},
            {proxy_protocol_trusted_proxies, TrustedProxies}
        ]},
        {rabbitmq_mqtt, [{proxy_protocol_trusted_proxies, ["127.0.0.1"]}]},
        {rabbitmq_stomp, [{proxy_protocol_trusted_proxies, ["127.0.0.1"]}]}
    ]),
    Config3 = rabbit_ct_helpers:set_config(Config2, {rabbitmq_ct_tls_verify, verify_none}),
    rabbit_ct_helpers:run_setup_steps(Config3,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

proxy_protocol_v1(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    ok = inet:send(Socket, <<"AMQP", 0, 0, 9, 1>>),
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    ct:pal("Connection name: ~s", [ConnectionName]),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    wait_for_connection_close(Config),
    ok.

proxy_protocol_v1_tls(Config) ->
    app_utils:start_applications([asn1, crypto, public_key, ssl]),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp_tls),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    {ok, SslSocket} = ssl:connect(Socket, [{verify, verify_none}], ?TIMEOUT),
    ok = ssl:send(SslSocket, <<"AMQP", 0, 0, 9, 1>>),
    {ok, _Packet} = ssl:recv(SslSocket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    ct:pal("Connection name: ~s", [ConnectionName]),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    wait_for_connection_close(Config),
    ok.

proxy_protocol_v2_local(Config) ->
    ProxyInfo = #{
        command => local,
        version => 2
    },
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    ok = inet:send(Socket, <<"AMQP", 0, 0, 9, 1>>),
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    ct:pal("Connection name: ~s", [ConnectionName]),
    match = re:run(ConnectionName, <<"^127.0.0.1:\\d+ -> 127.0.0.1:\\d+$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    wait_for_connection_close(Config),
    ok.

proxy_protocol_v1_untrusted_source_rejected(Config) ->
    reject_untrusted_proxy(Config, tcp_port_amqp,
                           "PROXY TCP4 10.1.2.3 192.168.1.2 80 81\r\n").

proxy_protocol_v1_tls_untrusted_source_rejected(Config) ->
    reject_untrusted_proxy(Config, tcp_port_amqp_tls,
                           "PROXY TCP4 10.1.2.3 192.168.1.2 80 81\r\n").

proxy_protocol_tls_untrusted_source_rejected_without_header(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp_tls),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port, [binary, {active, false}]),
    try
        ok = expect_closed(Socket, 1000)
    after
        gen_tcp:close(Socket)
    end.

proxy_protocol_v2_local_untrusted_source_rejected(Config) ->
    reject_untrusted_proxy(Config, tcp_port_amqp,
                           ranch_proxy_header:header(#{command => local, version => 2})).

reject_untrusted_proxy(Config, PortKey, Header) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, PortKey),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = gen_tcp:send(Socket, Header),
    ok = expect_closed(Socket, 1000),
    gen_tcp:close(Socket).

expect_closed(Socket, Timeout) ->
    case gen_tcp:recv(Socket, 7, Timeout) of
        {error, closed} ->
            ok;
        %% Closing an SSL socket before its handshake may send these alerts.
        {ok, <<21, 3, _Minor, 0, 2, 1, Alert>>} when Alert =:= 90; Alert =:= 0 ->
            expect_closed(Socket, Timeout)
    end.

connection_name() ->
    Scope = rabbit:pg_scope_amqp091_connection(),
    GetGroups = fun() ->
                    try pg:which_groups(Scope)
                    catch error:badarg -> []
                    end
                end,
    ?awaitMatch([_], GetGroups(), 30000),
    [Pid] = GetGroups(),
    {dictionary, Dict} = process_info(Pid, dictionary),
    {process_name, {rabbit_reader, ConnectionName}} = lists:keyfind(process_name, 1, Dict),
    ConnectionName.

wait_for_connection_close(Config) ->
    ?awaitMatch(
       [],
       begin
           Scope = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit, pg_scope_amqp091_connection, []),
           try rabbit_ct_broker_helpers:rpc(Config, 0, pg, which_groups, [Scope])
           catch error:badarg -> []
           end
       end,
       30000).
