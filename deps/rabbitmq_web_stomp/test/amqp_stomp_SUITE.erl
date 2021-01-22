%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(amqp_stomp_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(QUEUE, <<"TestQueue">>).
-define(DESTINATION, "/amq/queue/TestQueue").

all() ->
    [
    pubsub_amqp
    ].

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE},
                                            {protocol, "ws"}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{
        node = rabbit_ct_broker_helpers:get_node_config(Config1, 0, nodename)
    }),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    rabbit_ct_helpers:set_config(Config1, [
        {amqp_connection, Connection},
        {amqp_channel, Channel}
    ]).

end_per_testcase(Testcase, Config) ->
    Connection = ?config(amqp_connection, Config),
    Channel = ?config(amqp_channel, Config),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


raw_send(WS, Command, Headers) ->
    raw_send(WS, Command, Headers, <<>>).
raw_send(WS, Command, Headers, Body) ->
    Frame = stomp:marshal(Command, Headers, Body),
    rfc6455_client:send(WS, Frame).

raw_recv(WS) ->
    {ok, P} = rfc6455_client:recv(WS),
    stomp:unmarshal(P).


pubsub_amqp(Config) ->
    Ch = ?config(amqp_channel, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch, #'queue.declare'{queue = ?QUEUE, auto_delete = true}),

    PortStr = rabbit_ws_test_util:get_web_stomp_port_str(Config),
    Protocol = ?config(protocol, Config),
    WS = rfc6455_client:new(Protocol ++ "://127.0.0.1:" ++ PortStr ++ "/ws", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = raw_send(WS, "CONNECT", [{"login", "guest"}, {"passcode", "guest"}]),

    {<<"CONNECTED">>, _, <<>>} = raw_recv(WS),

    ok = raw_send(WS, "SUBSCRIBE", [{"destination", ?DESTINATION},
                                    {"id", "pubsub_amqp"},
                                    {"x-queue-name", ?QUEUE}]),

    CHK1 = <<"x-custom-hdr-1">>,
    CHV1 = <<"value1">>,
    CH1 = {CHK1, longstr, CHV1},
    CHK2 = <<"x-custom-hdr-2">>,
    CHV2 = <<"value2">>,
    CH2 = {CHK2, longstr, CHV2},
    CHK3 = <<"custom-hdr-3">>,
    CHV3 = <<"value3">>,
    CH3 = {CHK3, longstr, <<"value3">>},

    Publish = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},
    Props = #'P_basic'{headers = [CH1, CH2, CH3]},
    amqp_channel:call(Ch, Publish, #amqp_msg{props = Props, payload = <<"a\x00a">>}),

    {<<"MESSAGE">>, H, <<"a\x00a">>} = raw_recv(WS),

    {close, _} = rfc6455_client:close(WS),

    "/queue/TestQueue" = binary_to_list(proplists:get_value(<<"destination">>, H)),
    {CHK1, CHV1} = {CHK1, proplists:get_value(CHK1, H)},
    {CHK2, CHV2} = {CHK2, proplists:get_value(CHK2, H)},
    {CHK3, CHV3} = {CHK3, proplists:get_value(CHK3, H)},
    ok.
