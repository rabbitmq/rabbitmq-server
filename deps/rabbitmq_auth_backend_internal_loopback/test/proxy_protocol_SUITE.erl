%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(proxy_protocol_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-compile([export_all, nowarn_export_all]).

-define(TIMEOUT, 10000).

-define(NON_LOOPBACK_HEADER, "PROXY TCP4 203.0.113.5 127.0.0.1 40000 5672\r\n").
-define(LOOPBACK_HEADER, "PROXY TCP4 127.0.0.1 127.0.0.1 40000 5672\r\n").

-define(USERNAME, <<"TestLoopbackUser">>).
-define(PASSWORD, <<"TestLoopbackUser">>).

all() ->
    [
        {group, non_parallel_tests}
    ].

groups() ->
    [
        {non_parallel_tests, [], [
            amqp091_login_via_non_loopback_proxy_is_refused,
            amqp091_login_via_loopback_proxy_is_accepted,
            amqp091_login_via_local_proxy_is_accepted,
            amqp091_login_via_non_loopback_peer_is_refused,
            amqp10_login_via_non_loopback_proxy_is_refused,
            amqp10_login_via_loopback_proxy_is_accepted,
            stream_login_via_non_loopback_proxy_is_refused,
            stream_login_via_loopback_proxy_is_accepted
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1, [
        {rabbit, [
            {proxy_protocol, true},
            {auth_backends, [rabbit_auth_backend_internal_loopback]}
        ]},
        {rabbitmq_stream, [{proxy_protocol, true}]}
    ]),
    Config3 = rabbit_ct_helpers:run_setup_steps(Config2,
        rabbit_ct_broker_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config3, 0, rabbitmq_stream),
    ok = rabbit_ct_broker_helpers:add_user(Config3, ?USERNAME, ?PASSWORD),
    Config3.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

amqp091_login_via_non_loopback_proxy_is_refused(Config) ->
    ?assertEqual('connection.close',
                 amqp091_login(Config, ?NON_LOOPBACK_HEADER)).

amqp091_login_via_loopback_proxy_is_accepted(Config) ->
    ?assertEqual('connection.tune',
                 amqp091_login(Config, ?LOOPBACK_HEADER)).

amqp091_login_via_local_proxy_is_accepted(Config) ->
    Header = ranch_proxy_header:header(#{command => local, version => 2}),
    ?assertEqual('connection.tune', amqp091_login(Config, Header)).

amqp091_login_via_non_loopback_peer_is_refused(Config) ->
    case non_loopback_ipv4_address() of
        undefined ->
            {skip, "a non-loopback IPv4 address is required"};
        Addr ->
            ?assertEqual('connection.close',
                         amqp091_login(Config, Addr, ?LOOPBACK_HEADER))
    end.

amqp10_login_via_non_loopback_proxy_is_refused(Config) ->
    ?assertEqual(?V_1_0_SASL_CODE_AUTH,
                 amqp10_login(Config, ?NON_LOOPBACK_HEADER)).

amqp10_login_via_loopback_proxy_is_accepted(Config) ->
    ?assertEqual(?V_1_0_SASL_CODE_OK,
                 amqp10_login(Config, ?LOOPBACK_HEADER)).

stream_login_via_non_loopback_proxy_is_refused(Config) ->
    ?assertEqual(?RESPONSE_AUTHENTICATION_FAILURE,
                 stream_login(Config, ?NON_LOOPBACK_HEADER)).

stream_login_via_loopback_proxy_is_accepted(Config) ->
    ?assertEqual(?RESPONSE_CODE_OK,
                 stream_login(Config, ?LOOPBACK_HEADER)).

amqp091_login(Config, ProxyHeader) ->
    amqp091_login(Config, {127, 0, 0, 1}, ProxyHeader).

amqp091_login(Config, Addr, ProxyHeader) ->
    Socket = connect(Config, tcp_port_amqp, Addr, ProxyHeader),
    ok = gen_tcp:send(Socket, <<"AMQP", 0, 0, 9, 1>>),
    'connection.start' = recv_amqp091_method(Socket),
    %% Without this capability a refused login is a dropped socket.
    StartOk = #'connection.start_ok'{
        client_properties = [{<<"capabilities">>, table,
                              [{<<"authentication_failure_close">>, bool, true}]}],
        mechanism = <<"PLAIN">>,
        response = <<0, ?USERNAME/binary, 0, ?PASSWORD/binary>>},
    ok = gen_tcp:send(Socket, rabbit_binary_generator:build_simple_method_frame(
                                0, StartOk)),
    Reply = recv_amqp091_method(Socket),
    gen_tcp:close(Socket),
    Reply.

recv_amqp091_method(Socket) ->
    {ok, <<?FRAME_METHOD, 0:16, Size:32>>} = gen_tcp:recv(Socket, 7, ?TIMEOUT),
    {ok, <<ClassId:16, MethodId:16, _/binary>>} = gen_tcp:recv(Socket, Size + 1, ?TIMEOUT),
    rabbit_framing_amqp_0_9_1:lookup_method_name({ClassId, MethodId}).

amqp10_login(Config, ProxyHeader) ->
    Socket = connect(Config, tcp_port_amqp, {127, 0, 0, 1}, ProxyHeader),
    ok = gen_tcp:send(Socket, <<"AMQP", 3, 1, 0, 0>>),
    {ok, <<"AMQP", 3, 1, 0, 0>>} = gen_tcp:recv(Socket, 8, ?TIMEOUT),
    #'v1_0.sasl_mechanisms'{} = recv_amqp10_sasl_frame(Socket),
    SaslInit = #'v1_0.sasl_init'{
        mechanism = {symbol, <<"PLAIN">>},
        initial_response = {binary, <<0, ?USERNAME/binary, 0, ?PASSWORD/binary>>}},
    ok = gen_tcp:send(Socket, rabbit_amqp_writer:assemble_frame(
                                0, SaslInit, rabbit_amqp_sasl)),
    #'v1_0.sasl_outcome'{code = Code} = recv_amqp10_sasl_frame(Socket),
    gen_tcp:close(Socket),
    Code.

recv_amqp10_sasl_frame(Socket) ->
    {ok, <<Size:32, 2:8, 1:8, 0:16>>} = gen_tcp:recv(Socket, 8, ?TIMEOUT),
    {ok, Body} = gen_tcp:recv(Socket, Size - 8, ?TIMEOUT),
    [Performative] = amqp10_framing:decode_bin(Body),
    Performative.

stream_login(Config, ProxyHeader) ->
    Socket = connect(Config, tcp_port_stream, {127, 0, 0, 1}, ProxyHeader),
    C0 = rabbit_stream_core:init(0),
    ok = gen_tcp:send(Socket, rabbit_stream_core:frame(
                                {request, 1, {peer_properties, #{}}})),
    {{response, 1, {peer_properties, _, _}}, C1} =
        stream_test_utils:receive_stream_commands(Socket, C0),
    ok = gen_tcp:send(Socket, rabbit_stream_core:frame({request, 2, sasl_handshake})),
    {{response, 2, {sasl_handshake, _, _}}, C2} =
        stream_test_utils:receive_stream_commands(Socket, C1),
    Plain = <<0, ?USERNAME/binary, 0, ?PASSWORD/binary>>,
    ok = gen_tcp:send(Socket, rabbit_stream_core:frame(
                                {request, 3, {sasl_authenticate, <<"PLAIN">>, Plain}})),
    {{response, 3, {sasl_authenticate, Code}}, _} =
        stream_test_utils:receive_stream_commands(Socket, C2),
    gen_tcp:close(Socket),
    Code.

connect(Config, PortKey, Addr, ProxyHeader) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, PortKey),
    {ok, Socket} = gen_tcp:connect(Addr, Port,
        [binary, {active, false}, {packet, raw}, {ip, Addr}]),
    ok = gen_tcp:send(Socket, ProxyHeader),
    Socket.

non_loopback_ipv4_address() ->
    {ok, IfAddrs} = inet:getifaddrs(),
    case [A || {_, Opts} <- IfAddrs,
               Flags <- [proplists:get_value(flags, Opts, [])],
               lists:member(up, Flags), lists:member(running, Flags),
               {addr, A = {_, _, _, _}} <- Opts,
               not rabbit_net:is_loopback(A)] of
        []         -> undefined;
        [Addr | _] -> Addr
    end.
