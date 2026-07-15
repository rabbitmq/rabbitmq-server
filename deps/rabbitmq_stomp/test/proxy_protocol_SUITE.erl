%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(proxy_protocol_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TIMEOUT, 5000).

all() ->
    [
        {group, non_parallel_tests}
    ].

groups() ->
    [
        {non_parallel_tests, [], [
            proxy_protocol_v1,
            proxy_protocol_v1_tls,
            proxy_protocol_v2_local,
            loopback_user_via_non_loopback_proxy_is_rejected,
            loopback_user_via_local_proxy_is_accepted
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_certspwd, "bunnychow"},
        {rabbitmq_ct_tls_verify, verify_none}
    ]),
    StompConfig = stomp_config(),
    rabbit_ct_helpers:run_setup_steps(Config1,
        [ fun(Conf) -> merge_app_env(StompConfig, Conf) end ] ++
            rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps()).

stomp_config() ->
    {rabbitmq_stomp, [
        {proxy_protocol,  true}
    ]}.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

proxy_protocol_v1(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    ok = inet:send(Socket, stomp_connect_frame()),
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

proxy_protocol_v1_tls(Config) ->
    app_utils:start_applications([asn1, crypto, public_key, ssl]),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp_tls),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    {ok, SslSocket} = ssl:connect(Socket, [{verify, verify_none}], ?TIMEOUT),
    ok = ssl:send(SslSocket, stomp_connect_frame()),
    {ok, _Packet} = ssl:recv(SslSocket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

proxy_protocol_v2_local(Config) ->
    ProxyInfo = #{
        command => local,
        version => 2
    },
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    ok = inet:send(Socket, stomp_connect_frame()),
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^127.0.0.1:\\d+ -> 127.0.0.1:\\d+$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

connection_name() ->
    connection_name(50).

connection_name(0) ->
    error(no_stomp_connection_found);
connection_name(Retries) ->
    case ets:tab2list(connection_created) of
        [{_Key, Values} | _] ->
            {_, Name} = lists:keyfind(conn_name, 1, Values),
            Name;
        [] ->
            timer:sleep(50),
            connection_name(Retries - 1)
    end.

%% A loopback-only user must be evaluated against the PROXY source, not the
%% immediate peer (the proxy connection itself).
loopback_user_via_non_loopback_proxy_is_rejected(Config) ->
    ok = set_loopback_users(Config, [<<"guest">>]),
    try
        Command = connect_response_command(
            Config, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
        ?assertEqual(<<"ERROR">>, Command)
    after
        ok = set_loopback_users(Config, [])
    end.

%% A LOCAL v2 header keeps the real (loopback) ends, so guest is accepted.
loopback_user_via_local_proxy_is_accepted(Config) ->
    ok = set_loopback_users(Config, [<<"guest">>]),
    try
        Header = ranch_proxy_header:header(#{command => local, version => 2}),
        Command = connect_response_command(Config, Header),
        ?assertEqual(<<"CONNECTED">>, Command)
    after
        ok = set_loopback_users(Config, [])
    end.

connect_response_command(Config, ProxyHeader) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, ProxyHeader),
    ok = inet:send(Socket, <<"CONNECT\nlogin:guest\npasscode:guest\n\n", 0>>),
    {ok, Response} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    gen_tcp:close(Socket),
    %% The reply is a single STOMP frame; its command is the leading line.
    hd(binary:split(Response, <<"\n">>)).

set_loopback_users(Config, Users) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, loopback_users, Users]).

merge_app_env(StompConfig, Config) ->
    rabbit_ct_helpers:merge_app_env(Config, StompConfig).

stomp_connect_frame() ->
    <<"CONNECT\n",
      "login:guest\n",
      "passcode:guest\n",
      "\n",
      0>>.
