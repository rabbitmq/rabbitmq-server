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
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

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
            proxy_protocol_v2_local
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
    Config2 = rabbit_ct_helpers:run_setup_steps(Config1,
                  [ fun(Conf) -> merge_app_env(StompConfig, Conf) end ] ++
                      rabbit_ct_broker_helpers:setup_steps() ++
                      rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_broker_helpers:add_user(Config2, <<"proxy_test">>, <<"proxy_test">>),
    rabbit_ct_broker_helpers:set_full_permissions(Config2, <<"proxy_test">>, <<"/">>),
    Config2.

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
    await_connection_name_match(
      Config, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>),
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
    await_connection_name_match(
      Config, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>),
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
    await_connection_name_match(
      Config, <<"^127.0.0.1:\\d+ -> 127.0.0.1:\\d+$">>),
    gen_tcp:close(Socket),
    ok.

%% The `connection_created' ETS table is populated asynchronously by
%% the management agent; wait for an entry whose `name' matches the
%% pattern.
await_connection_name_match(Config, Pattern) ->
    ?awaitMatch(true,
                rabbit_ct_broker_helpers:rpc(
                  Config, 0, ?MODULE, has_connection_name_matching, [Pattern]),
                30_000).

has_connection_name_matching(Pattern) ->
    Connections = ets:tab2list(connection_created),
    lists:any(
      fun({_Key, Values}) ->
              case lists:keyfind(name, 1, Values) of
                  {_, Name} ->
                      re:run(Name, Pattern, [{capture, none}]) =:= match;
                  false ->
                      false
              end
      end, Connections).

merge_app_env(StompConfig, Config) ->
    rabbit_ct_helpers:merge_app_env(Config, StompConfig).

stomp_connect_frame() ->
    <<"CONNECT\n",
      "login:proxy_test\n",
      "passcode:proxy_test\n",
      "\n",
      0>>.
