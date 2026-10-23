%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(proxy_protocol_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

all() ->
    [{group, trusted}, {group, untrusted}, {group, unconfigured}].

groups() ->
    [{Group, [], [tcp, tls]} || Group <- [trusted, untrusted, unconfigured]].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    {TrustedProxies, AmqpTrustedProxies} = case Group of
        trusted -> {["127.0.0.1", "::1"], ["192.0.2.0/24"]};
        untrusted -> {["192.0.2.0/24"], ["127.0.0.1", "::1"]};
        unconfigured -> {[], ["192.0.2.0/24"]}
    end,
    Config1 = rabbit_ct_helpers:set_config(Config,
        [{rmq_nodename_suffix, Group},
         {rabbitmq_ct_tls_verify, verify_none},
         {trusted_proxy, Group =/= untrusted}]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1,
        [{rabbit, [{proxy_protocol_trusted_proxies, AmqpTrustedProxies}]},
         {rabbitmq_stream, [{proxy_protocol, true},
                           {proxy_protocol_trusted_proxies, TrustedProxies}]}]),
    rabbit_ct_helpers:run_setup_steps(Config2, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

tcp(Config) ->
    check_proxy(Config, tcp_port_stream, gen_tcp).

tls(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    check_proxy(Config, tcp_port_stream_tls, ssl).

check_proxy(Config, PortKey, Transport) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, PortKey),
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port, [binary, {active, false}]),
    try
        ok = gen_tcp:send(Socket, "PROXY TCP4 192.0.2.1 192.0.2.2 80 81\r\n"),
        case proplists:get_value(trusted_proxy, Config) of
            false ->
                expect_closed(Socket);
            true ->
                {ok, Sock} = case Transport of
                    gen_tcp -> {ok, Socket};
                    ssl -> ssl:connect(Socket, [{verify, verify_none}], 5000)
                end,
                try
                    ok = Transport:send(Sock,
                        rabbit_stream_core:frame({request, 1, {peer_properties, #{}}})),
                    {{response, 1, {peer_properties, ?RESPONSE_CODE_OK, _}}, _} =
                        stream_test_utils:receive_stream_commands(
                            Transport, Sock, rabbit_stream_core:init(0)),
                    ok
                after
                    Transport:close(Sock)
                end
        end
    after
        gen_tcp:close(Socket)
    end.

expect_closed(Socket) ->
    case gen_tcp:recv(Socket, 7, 1000) of
        {error, closed} ->
            ok;
        %% Closing an SSL socket before its handshake may send these alerts.
        {ok, <<21, 3, _Minor, 0, 2, 1, Alert>>} when Alert =:= 90; Alert =:= 0 ->
            expect_closed(Socket)
    end.
