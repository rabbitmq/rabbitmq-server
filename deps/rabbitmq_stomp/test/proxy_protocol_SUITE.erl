%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
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
            proxy_protocol,
            proxy_protocol_tls
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
    MqttConfig = stomp_config(),
    rabbit_ct_helpers:run_setup_steps(Config1,
        [ fun(Conf) -> merge_app_env(MqttConfig, Conf) end ] ++
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

proxy_protocol(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    ok = inet:send(Socket, stomp_connect_frame()),
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 ">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

proxy_protocol_tls(Config) ->
    app_utils:start_applications([asn1, crypto, public_key, ssl]),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp_tls),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    {ok, SslSocket} = ssl:connect(Socket, [], ?TIMEOUT),
    ok = ssl:send(SslSocket, stomp_connect_frame()),
    {ok, _Packet} = ssl:recv(SslSocket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 ">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

connection_name() ->
    Connections = ets:tab2list(connection_created),
    {_Key, Values} = lists:nth(1, Connections),
    {_, Name} = lists:keyfind(name, 1, Values),
    Name.

merge_app_env(MqttConfig, Config) ->
    rabbit_ct_helpers:merge_app_env(Config, MqttConfig).

stomp_connect_frame() ->
    <<"CONNECT\n",
      "login:guest\n",
      "passcode:guest\n",
      "\n",
      0>>.