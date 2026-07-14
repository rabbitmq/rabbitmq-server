%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(proxy_protocol_SUITE).


-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 2}}
    ].

all() ->
    [{group, http_tests},
     {group, https_tests}].

groups() ->
    Tests = [
        proxy_protocol_v1,
        proxy_protocol_v2_local,
        loopback_user_via_non_loopback_proxy_is_rejected,
        loopback_user_via_local_proxy_is_accepted
    ],
    [{https_tests, [], Tests},
     {http_tests, [], Tests}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Protocol = case Group of
        http_tests -> "ws";
        https_tests -> "wss"
    end,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE},
                                            {protocol, Protocol},
                                            {rabbitmq_ct_tls_verify, verify_none},
                                            {rabbitmq_ct_tls_fail_if_no_peer_cert, false}]),

    Config2 = rabbit_ct_helpers:run_setup_steps(
                 Config1,
                 rabbit_ct_broker_helpers:setup_steps() ++ [
                     fun configure_proxy_protocol/1,
                     fun configure_ssl/1
                 ]),
    rabbit_ct_broker_helpers:add_user(Config2, <<"proxy_test">>, <<"proxy_test">>),
    rabbit_ct_broker_helpers:set_full_permissions(Config2, <<"proxy_test">>, <<"/">>),
    Config2.

configure_proxy_protocol(Config) ->
    rabbit_ws_test_util:update_app_env(Config, proxy_protocol, true),
    Config.

configure_ssl(Config) ->
    ErlangConfig = proplists:get_value(erlang_node_config, Config, []),
    RabbitAppConfig = proplists:get_value(rabbit, ErlangConfig, []),
    RabbitSslConfig = proplists:get_value(ssl_options, RabbitAppConfig, []),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_stomp_tls),
    rabbit_ws_test_util:update_app_env(Config, ssl_config, [{port, Port} | lists:keydelete(port, 1, RabbitSslConfig)]),
    Config.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

proxy_protocol_v1(Config) ->
    Port = list_to_integer(rabbit_ws_test_util:get_web_stomp_port_str(Config)),
    PortStr = integer_to_list(Port),

    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self(),
        undefined, [], "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    {ok, _} = rfc6455_client:open(WS),
    Frame = stomp:marshal("CONNECT", [{"login","proxy_test"}, {"passcode", "proxy_test"}], <<>>),
    rfc6455_client:send(WS, Frame),
    {ok, _P} = rfc6455_client:recv(WS),
    await_connection_name_match(
      Config, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>),
    {close, _} = rfc6455_client:close(WS),
    ok.

proxy_protocol_v2_local(Config) ->
    ProxyInfo = #{
        command => local,
        version => 2
    },

    Port = list_to_integer(rabbit_ws_test_util:get_web_stomp_port_str(Config)),
    PortStr = integer_to_list(Port),

    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self(),
        undefined, [], ranch_proxy_header:header(ProxyInfo)),
    {ok, _} = rfc6455_client:open(WS),
    Frame = stomp:marshal("CONNECT", [{"login","proxy_test"}, {"passcode", "proxy_test"}], <<>>),
    rfc6455_client:send(WS, Frame),
    {ok, _P} = rfc6455_client:recv(WS),
    await_connection_name_match(
      Config, <<"^127.0.0.1:\\d+ -> 127.0.0.1:\\d+$">>),
    {close, _} = rfc6455_client:close(WS),
    ok.

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
    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self(),
        undefined, [], ProxyHeader),
    {ok, _} = rfc6455_client:open(WS),
    Frame = stomp:marshal("CONNECT", [{"login", "guest"}, {"passcode", "guest"}], <<>>),
    rfc6455_client:send(WS, Frame),
    {ok, Response} = rfc6455_client:recv(WS),
    catch rfc6455_client:close(WS),
    %% The reply is a single STOMP frame; its command is the leading line.
    hd(binary:split(iolist_to_binary(Response), <<"\n">>)).

set_loopback_users(Config, Users) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, loopback_users, Users]).

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
