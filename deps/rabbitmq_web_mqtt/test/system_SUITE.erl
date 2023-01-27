%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-import(rabbit_ct_helpers, [eventually/1]).

all() ->
    [{group, tests}].

groups() ->
    [
     {tests, [],
      [no_websocket_subprotocol
       ,unsupported_websocket_subprotocol
       ,unacceptable_data_type
       ,handle_invalid_packets
       ,duplicate_connect
      ]}
    ].

suite() ->
    [{timetrap, {minutes, 2}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {protocol, "ws"}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

no_websocket_subprotocol(Config) ->
    websocket_subprotocol(Config, []).

unsupported_websocket_subprotocol(Config) ->
    websocket_subprotocol(Config, ["not-mqtt-protocol"]).

%% "The client MUST include “mqtt” in the list of WebSocket Sub Protocols it offers" [MQTT-6.0.0-3].
websocket_subprotocol(Config, SubProtocol) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    WS = rfc6455_client:new("ws://localhost:" ++ PortStr ++ "/ws", self(), undefined, SubProtocol),
    {_, [{http_response, Res}]} = rfc6455_client:open(WS),
    {'HTTP/1.1', 400, <<"Bad Request">>, _} = cow_http:parse_status_line(rabbit_data_coercion:to_binary(Res)),
    rfc6455_client:send_binary(WS, rabbit_ws_test_util:mqtt_3_1_1_connect_packet()),
    {close, _} = rfc6455_client:recv(WS, timer:seconds(1)).

%% "MQTT Control Packets MUST be sent in WebSocket binary data frames. If any other type
%% of data frame is received the recipient MUST close the Network Connection" [MQTT-6.0.0-1].
unacceptable_data_type(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    WS = rfc6455_client:new("ws://localhost:" ++ PortStr ++ "/ws", self(), undefined, ["mqtt"]),
    {ok, _} = rfc6455_client:open(WS),
    rfc6455_client:send(WS, "not-binary-data"),
    {close, {1003, _}} = rfc6455_client:recv(WS, timer:seconds(1)).

handle_invalid_packets(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    WS = rfc6455_client:new("ws://localhost:" ++ PortStr ++ "/ws", self(), undefined, ["mqtt"]),
    {ok, _} = rfc6455_client:open(WS),
    Bin = <<"GET / HTTP/1.1\r\nHost: www.rabbitmq.com\r\nUser-Agent: curl/7.43.0\r\nAccept: */*">>,
    rfc6455_client:send_binary(WS, Bin),
    {close, {1002, _}} = rfc6455_client:recv(WS, timer:seconds(1)).

%% "A Client can only send the CONNECT Packet once over a Network Connection.
%% The Server MUST process a second CONNECT Packet sent from a Client as a protocol
%% violation and disconnect the Client [MQTT-3.1.0-2].
duplicate_connect(Config) ->
    Url = "ws://127.0.0.1:" ++ rabbit_ws_test_util:get_web_mqtt_port_str(Config) ++ "/ws",
    WS = rfc6455_client:new(Url, self(), undefined, ["mqtt"]),
    {ok, _} = rfc6455_client:open(WS),

    %% 1st CONNECT should succeed.
    rfc6455_client:send_binary(WS, rabbit_ws_test_util:mqtt_3_1_1_connect_packet()),
    {binary, _P} = rfc6455_client:recv(WS),
    eventually(?_assertEqual(1, num_mqtt_connections(Config, 0))),

    %% 2nd CONNECT should fail.
    process_flag(trap_exit, true),
    rfc6455_client:send_binary(WS, rabbit_ws_test_util:mqtt_3_1_1_connect_packet()),
    eventually(?_assertEqual(0, num_mqtt_connections(Config, 0))),
    receive {'EXIT', WS, _} -> ok
    after 500 -> ct:fail("expected web socket to exit")
    end.

%% -------------------------------------------------------------------
%% Internal helpers
%% -------------------------------------------------------------------

%% Web mqtt connections are tracked together with mqtt connections
num_mqtt_connections(Config, Node) ->
    length(rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_mqtt, local_connection_pids, [])).
