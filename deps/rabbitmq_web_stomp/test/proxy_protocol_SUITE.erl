%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(proxy_protocol_SUITE).


-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-include_lib("eunit/include/eunit.hrl").

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
        proxy_protocol
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

    rabbit_ct_helpers:run_setup_steps(
        Config1,
        rabbit_ct_broker_helpers:setup_steps() ++ [
            fun configure_proxy_protocol/1,
            fun configure_ssl/1
        ]).

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

proxy_protocol(Config) ->
    Port = list_to_integer(rabbit_ws_test_util:get_web_stomp_port_str(Config)),
    PortStr = integer_to_list(Port),

    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self(),
        undefined, [], "PROXY TCP4 192.168.1.1 192.168.1.2 80 81\r\n"),
    {ok, _} = rfc6455_client:open(WS),
    Frame = stomp:marshal("CONNECT", [{"login","guest"}, {"passcode", "guest"}], <<>>),
    rfc6455_client:send(WS, Frame),
    {ok, _P} = rfc6455_client:recv(WS),
    ConnectionName = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, connection_name, []),
    match = re:run(ConnectionName, <<"^192.168.1.1:80 -> 192.168.1.2:81$">>, [{capture, none}]),
    {close, _} = rfc6455_client:close(WS),
    ok.

connection_name() ->
    Connections = ets:tab2list(connection_created),
    {_Key, Values} = lists:nth(1, Connections),
    {_, Name} = lists:keyfind(name, 1, Values),
    Name.
