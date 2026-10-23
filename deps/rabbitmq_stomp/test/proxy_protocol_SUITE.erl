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
            proxy_protocol_untrusted_source_rejected,
            proxy_protocol_tls_untrusted_source_rejected,
            proxy_protocol_unconfigured_trusted_proxies_accepted,
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
    RabbitConfig = {rabbit, [{proxy_protocol_trusted_proxies, ["192.0.2.0/24"]}]},
    rabbit_ct_helpers:run_setup_steps(Config1,
        [ fun(Conf) -> merge_app_env(StompConfig, Conf) end,
          fun(Conf) -> merge_app_env(RabbitConfig, Conf) end ] ++
            rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps()).

stomp_config() ->
    {rabbitmq_stomp, [
        {proxy_protocol,  true},
        {proxy_protocol_trusted_proxies, ["127.0.0.1", "::1"]}
    ]}.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(Testcase, Config)
  when Testcase == proxy_protocol_v1 orelse Testcase == proxy_protocol_v1_tls
       orelse Testcase == proxy_protocol_unconfigured_trusted_proxies_accepted ->
    %% Allow guest from the spoofed 192.168.1.1 IP
    ok = set_loopback_users(Config, []),
    rabbit_ct_helpers:testcase_started(Config, Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config)
  when Testcase == proxy_protocol_v1 orelse Testcase == proxy_protocol_v1_tls
       orelse Testcase == proxy_protocol_unconfigured_trusted_proxies_accepted ->
    %% Restore default loopback restrictions
    ok = set_loopback_users(Config, [<<"guest">>]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config)
  when Testcase == loopback_user_via_non_loopback_proxy_is_rejected
       orelse Testcase == loopback_user_via_local_proxy_is_accepted ->
    %% Survives a linked exit, which would skip a `try ... after` in the test.
    ok = set_loopback_users(Config, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

proxy_protocol_untrusted_source_rejected(Config) ->
    reject_untrusted_proxy(Config, tcp_port_stomp).

proxy_protocol_tls_untrusted_source_rejected(Config) ->
    reject_untrusted_proxy(Config, tcp_port_stomp_tls).

reject_untrusted_proxy(Config, PortKey) ->
    ok = set_trusted_proxies(Config, ["192.0.2.0/24"], ["127.0.0.1", "::1"]),
    try
        Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, PortKey),
        {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port, [binary, {active, false}]),
        try
            ok = gen_tcp:send(Socket, "PROXY TCP4 192.0.2.1 192.0.2.2 80 81\r\n"),
            expect_closed(Socket)
        after
            gen_tcp:close(Socket)
        end
    after
        ok = set_trusted_proxies(Config, ["127.0.0.1", "::1"], ["192.0.2.0/24"])
    end.

proxy_protocol_unconfigured_trusted_proxies_accepted(Config) ->
    ok = set_trusted_proxies(Config, [], ["192.0.2.0/24"]),
    try
        proxy_protocol_v1(Config)
    after
        ok = set_trusted_proxies(Config, ["127.0.0.1", "::1"], ["192.0.2.0/24"])
    end.

set_trusted_proxies(Config, TrustedProxies, AmqpTrustedProxies) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                     [rabbit, proxy_protocol_trusted_proxies, AmqpTrustedProxies]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                     [rabbitmq_stomp, proxy_protocol_trusted_proxies, TrustedProxies]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, stop, [rabbitmq_stomp]),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, start, [rabbitmq_stomp]).

expect_closed(Socket) ->
    case gen_tcp:recv(Socket, 7, 1000) of
        {error, closed} ->
            ok;
        %% Closing an SSL socket before its handshake may send these alerts.
        {ok, <<21, 3, _Minor, 0, 2, 1, Alert>>} when Alert =:= 90; Alert =:= 0 ->
            expect_closed(Socket)
    end.

proxy_protocol_v1(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
                                   [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    ok = inet:send(Socket, stomp_connect_frame()),
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
                                                  ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>,
                   [{capture, none}]),
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

connection_names() ->
    [Name || {_Key, Values} <- ets:tab2list(connection_created),
             {conn_name, Name} <- [lists:keyfind(conn_name, 1, Values)]].

%% Regression guard: a rejected login must never register a
%% `connection_created` entry.
assert_no_connection_created(Config, Pattern) ->
    rabbit_ct_helpers:consistently(
      ?_assertNot(
         lists:any(
           fun(Name) -> match =:= re:run(Name, Pattern, [{capture, none}]) end,
           rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, connection_names, []))),
      200, 5).

%% A loopback-only user must be evaluated against the PROXY source, not the
%% immediate peer (the proxy connection itself).
loopback_user_via_non_loopback_proxy_is_rejected(Config) ->
    ok = set_loopback_users(Config, [<<"guest">>]),
    {Socket, Command} = connect_response(
        Config, "PROXY TCP4 10.10.10.10 10.10.10.11 8080 8081\r\n"),
    ?assertEqual(<<"ERROR">>, Command),
    %% The socket stays open: closing it deletes any `connection_created`
    %% entry, hiding the event this asserts the absence of.
    ok = assert_no_connection_created(
           Config, <<"^10.10.10.10:8080 -> 10.10.10.11:8081$">>),
    gen_tcp:close(Socket).

%% A LOCAL v2 header keeps the real (loopback) ends, so guest is accepted.
loopback_user_via_local_proxy_is_accepted(Config) ->
    ok = set_loopback_users(Config, [<<"guest">>]),
    Header = ranch_proxy_header:header(#{command => local, version => 2}),
    Command = connect_response_command(Config, Header),
    ?assertEqual(<<"CONNECTED">>, Command).

connect_response_command(Config, ProxyHeader) ->
    {Socket, Command} = connect_response(Config, ProxyHeader),
    gen_tcp:close(Socket),
    Command.

connect_response(Config, ProxyHeader) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, ProxyHeader),
    ok = inet:send(Socket, <<"CONNECT\nlogin:guest\npasscode:guest\n\n", 0>>),
    {ok, Response} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    %% The reply is a single STOMP frame; its command is the leading line.
    {Socket, hd(binary:split(Response, <<"\n">>))}.

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
