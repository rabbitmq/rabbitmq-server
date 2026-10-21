%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(proxy_protocol_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-define(TIMEOUT, 5000).

all() ->
    [
        {group, sequential_tests}
    ].

groups() -> [
        {sequential_tests, [], [
            proxy_protocol_v1,
            proxy_protocol_v1_tls,
            proxy_protocol_v1_loopback_source_from_remote_peer,
            proxy_protocol_v1_guest_from_loopback_peer,
            proxy_protocol_v1_guest_from_remote_peer,
            proxy_protocol_v2_local,
            proxy_protocol_v1_vhost_access_peeraddr
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

proxy_protocol_v1_loopback_source_from_remote_peer(Config) ->
    case non_loopback_ipv4_address() of
        undefined ->
            {skip, "a non-loopback IPv4 address is required"};
        Addr ->
            AddrStr = inet:ntoa(Addr),
            Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
            {ok, Socket} = gen_tcp:connect(Addr, Port,
                [binary, {active, false}, {packet, raw}, {ip, Addr}]),
            {ok, {Addr, ClientPort}} = inet:sockname(Socket),
            ok = inet:send(Socket, "PROXY TCP4 127.0.0.1 " ++ AddrStr ++ " 80 81\r\n"),
            ok = inet:send(Socket, <<"AMQP", 0, 0, 9, 1>>),
            {ok, _Packet} = gen_tcp:recv(Socket, 0, ?TIMEOUT),
            ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
                ?MODULE, connection_name, []),
            ct:pal("Connection name: ~s", [ConnectionName]),
            Expected = io_lib:format("^~s:~b -> ~s:~b$", [AddrStr, ClientPort, AddrStr, Port]),
            match = re:run(ConnectionName, Expected, [{capture, none}]),
            gen_tcp:close(Socket),
            wait_for_connection_close(Config),
            ok
    end.

proxy_protocol_v1_guest_from_loopback_peer(Config) ->
    [?assertEqual(accepted, guest_login(Config, {127, 0, 0, 1}, Mechanism))
     || Mechanism <- [<<"PLAIN">>, <<"ANONYMOUS">>]],
    ok.

proxy_protocol_v1_guest_from_remote_peer(Config) ->
    case non_loopback_ipv4_address() of
        undefined ->
            {skip, "a non-loopback IPv4 address is required"};
        Addr ->
            [?assertEqual(refused, guest_login(Config, Addr, Mechanism))
             || Mechanism <- [<<"PLAIN">>, <<"ANONYMOUS">>]],
            ok
    end.

guest_login(Config, Addr, Mechanism) ->
    {ok, LoopbackUsers} = rabbit_ct_broker_helpers:rpc(Config, 0,
        application, get_env, [rabbit, loopback_users]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        application, set_env, [rabbit, loopback_users, [<<"guest">>]]),
    try
        Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
        {ok, Socket} = gen_tcp:connect(Addr, Port,
            [binary, {active, false}, {packet, raw}, {ip, Addr}], ?TIMEOUT),
        try
            {ok, {Addr, _}} = inet:sockname(Socket),
            ok = gen_tcp:send(Socket, "PROXY TCP4 127.0.0.1 127.0.0.1 40000 5672\r\n"),
            ok = gen_tcp:send(Socket, <<"AMQP", 0, 0, 9, 1>>),
            #'connection.start'{} = recv_method(Socket),
            Response = case Mechanism of
                           <<"PLAIN">> -> <<0, "guest", 0, "guest">>;
                           <<"ANONYMOUS">> -> <<>>
                       end,
            ok = send_method(Socket, #'connection.start_ok'{
                client_properties = [{<<"capabilities">>, table,
                                      [{<<"authentication_failure_close">>, bool, true}]}],
                mechanism = Mechanism,
                response = Response,
                locale = <<"en_US">>}),
            case recv_method(Socket) of
                #'connection.tune'{} ->
                    accepted;
                #'connection.close'{reply_code = 403} ->
                    refused
            end
        after
            gen_tcp:close(Socket),
            wait_for_connection_close(Config)
        end
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, 0,
            application, set_env, [rabbit, loopback_users, LoopbackUsers])
    end.

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

%% Authorization backends must be given the address of the client,
%% i.e. the one from the PROXY protocol header, not the address of the proxy.
proxy_protocol_v1_vhost_access_peeraddr(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config,
        rabbit_auth_backend_context_propagation_mock),
    AuthzData = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, vhost_access_authz_data, [Port]),
    ?assertEqual(#{peeraddr => {192, 168, 1, 1}}, AuthzData),
    wait_for_connection_close(Config),
    ok.

vhost_access_authz_data(Port) ->
    rabbit_auth_backend_context_propagation_mock:init(),
    {ok, AuthBackends} = application:get_env(rabbit, auth_backends),
    ok = application:set_env(rabbit, auth_backends,
        [rabbit_auth_backend_context_propagation_mock]),
    try
        {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port,
            [binary, {active, false}, {packet, raw}]),
        ok = inet:send(Socket, "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
        ok = inet:send(Socket, <<"AMQP", 0, 0, 9, 1>>),
        #'connection.start'{} = recv_method(Socket),
        ok = send_method(Socket, #'connection.start_ok'{
                                    client_properties = [],
                                    mechanism = <<"PLAIN">>,
                                    response = <<0, "guest", 0, "guest">>,
                                    locale = <<"en_US">>}),
        #'connection.tune'{channel_max = ChannelMax,
                           frame_max = FrameMax} = recv_method(Socket),
        ok = send_method(Socket, #'connection.tune_ok'{channel_max = ChannelMax,
                                                       frame_max = FrameMax,
                                                       heartbeat = 0}),
        ok = send_method(Socket, #'connection.open'{virtual_host = <<"/">>}),
        #'connection.open_ok'{} = recv_method(Socket),
        [{vhost_access, AuthzData}] =
            rabbit_auth_backend_context_propagation_mock:get(vhost_access),
        true = ets:delete(rabbit_auth_backend_context_propagation_mock, vhost_access),
        ok = send_method(Socket, #'connection.update_secret'{new_secret = <<"guest">>,
                                                             reason = <<"test">>}),
        #'connection.update_secret_ok'{} = recv_method(Socket),
        [{vhost_access, AuthzData}] =
            rabbit_auth_backend_context_propagation_mock:get(vhost_access),
        gen_tcp:close(Socket),
        AuthzData
    after
        ok = application:set_env(rabbit, auth_backends, AuthBackends)
    end.

send_method(Socket, Method) ->
    inet:send(Socket, rabbit_binary_generator:build_simple_method_frame(0, Method)).

recv_method(Socket) ->
    {ok, <<Type:8, 0:16, Size:32>>} = gen_tcp:recv(Socket, 7, ?TIMEOUT),
    {ok, <<Payload:Size/binary, ?FRAME_END>>} = gen_tcp:recv(Socket, Size + 1, ?TIMEOUT),
    {method, Name, Fields} = rabbit_command_assembler:analyze_frame(Type, Payload),
    rabbit_framing_amqp_0_9_1:decode_method_fields(Name, Fields).

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
