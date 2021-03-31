%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(proxy_protocol_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(TIMEOUT, 5000).

all() ->
    [
        {group, sequential_tests}
    ].

groups() -> [
        {sequential_tests, [], [
            proxy_protocol,
            proxy_protocol_tls
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1, [
        {rabbit, [
            {proxy_protocol, true}
        ]}
    ]),
    Config3 = rabbit_ct_helpers:set_config(Config2, {rabbitmq_ct_tls_verify, verify_none}),
    rabbit_ct_helpers:run_setup_steps(Config3,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

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
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    [ok = inet:send(Socket, amqp_1_0_frame(FrameType))
        || FrameType <- [header_sasl, sasl_init, header_amqp, open, 'begin']],
    {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

proxy_protocol_tls(Config) ->
    app_utils:start_applications([asn1, crypto, public_key, ssl]),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp_tls),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
        [binary, {active, false}, {packet, raw}]),
    ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    {ok, SslSocket} = ssl:connect(Socket, [], ?TIMEOUT),
    [ok = ssl:send(SslSocket, amqp_1_0_frame(FrameType))
        || FrameType <- [header_sasl, sasl_init, header_amqp, open, 'begin']],
    {ok, _Packet} = ssl:recv(SslSocket, 0, ?TIMEOUT),
    timer:sleep(1000),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    gen_tcp:close(Socket),
    ok.

%% hex frames to send to have the connection recorded in RabbitMQ
%% use wireshark with one of the Java tests to record those
amqp_1_0_frame(header_sasl) ->
    hex_frame_to_binary("414d515003010000");
amqp_1_0_frame(header_amqp) ->
    hex_frame_to_binary("414d515000010000");
amqp_1_0_frame(sasl_init) ->
    hex_frame_to_binary("0000001902010000005341c00c01a309414e4f4e594d4f5553");
amqp_1_0_frame(open) ->
    hex_frame_to_binary("0000003f02000000005310c03202a12438306335323662332d653530662d343835352d613564302d336466643738623537633730a1096c6f63616c686f7374");
amqp_1_0_frame('begin') ->
    hex_frame_to_binary("0000002002000000005311c01305405201707fffffff707fffffff700000ffff").

hex_frame_to_binary(HexsString) ->
    Hexs = split(HexsString, []),
    Ints = [list_to_integer(Hex, 16) || Hex <- Hexs],
    Result = list_to_binary(Ints),
    Result.

split([X1, X2 | T],Acc) ->
    Byte = [[X1, X2]],
    split(T, Acc ++ Byte);
split([], Acc) ->
    Acc.

connection_name() ->
    %% the connection can take some time to show up in the ETS
    %% hence the retry
    case retry(fun connection_registered/0, 20) of
        true ->
            Connections = ets:tab2list(connection_created),
            {_Key, Values} = lists:nth(1, Connections),
            {_, Name} = lists:keyfind(name, 1, Values),
            Name;
        false ->
            error
    end.

connection_registered() ->
    length(ets:tab2list(connection_created)) > 0.

retry(_Function, 0) ->
    false;
retry(Function, Count) ->
    Result = Function(),
    case Result of
        true  ->
            true;
        false ->
            timer:sleep(100),
            retry(Function, Count - 1)
    end.
