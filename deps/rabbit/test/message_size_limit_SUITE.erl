%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(message_size_limit_SUITE).

-compile([export_all, nowarn_export_all]).
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(TIMEOUT_CHANNEL_EXCEPTION, 5000).

all() ->
    [
     {group, tests},
     {group, default}
    ].

groups() ->
    [
     {tests, [], [
                  max_message_size,
                  max_message_size_raw_frames,
                  max_message_size_on_closing_channel
                 ]},
     {default, [], [
                    default_max_message_size
                   ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 3}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Group},
                {rmq_nodes_count, 1}
              ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

max_message_size(Config) ->
    Binary2M  = gen_binary_mb(2),
    Binary4M  = gen_binary_mb(4),
    Binary6M  = gen_binary_mb(6),
    Binary10M = gen_binary_mb(10),

    ok = rabbit_ct_broker_helpers:rpc(Config, persistent_term, put, [max_message_size, 1024 * 1024 * 3]),

    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    %% Binary is within the max size limit
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary2M}),
    %% The channel process is alive
    assert_channel_alive(Ch),

    Monitor = monitor(process, Ch),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary4M}),
    assert_channel_fail_max_size(Ch, Monitor),

    %% increase the limit
    ok = rabbit_ct_broker_helpers:rpc(Config, persistent_term, put, [max_message_size, 1024 * 1024 * 8]),

    {_, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary2M}),
    assert_channel_alive(Ch1),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary4M}),
    assert_channel_alive(Ch1),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary6M}),
    assert_channel_alive(Ch1),

    Monitor1 = monitor(process, Ch1),
    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary10M}),
    assert_channel_fail_max_size(Ch1, Monitor1).

max_message_size_raw_frames(Config) ->
    MaxSize = 1024 * 1024,
    BodySize = 2 * MaxSize,
    ok = rabbit_ct_broker_helpers:rpc(Config, persistent_term, put, [max_message_size, MaxSize]),
    Sock = raw_connect(Config),
    Fragment = binary:copy(<<"x">>, 64 * 1024),
    ok = send_method(Sock, 1, #'basic.publish'{routing_key = <<"none">>}),
    ok = gen_tcp:send(Sock, rabbit_binary_generator:create_frame(
                              ?FRAME_HEADER, 1, <<60:16, 0:16, BodySize:64, 0:16>>)),
    BodyFrame = rabbit_binary_generator:create_frame(?FRAME_BODY, 1, Fragment),
    ok = gen_tcp:send(Sock, BodyFrame),
    ?assertMatch({1, #'channel.close'{reply_code = 406}}, recv_method(Sock)),
    [ok = gen_tcp:send(Sock, BodyFrame)
     || _ <- lists:seq(2, BodySize div byte_size(Fragment))],
    ok = send_method(Sock, 1, #'channel.close_ok'{}),
    ok = send_method(Sock, 2, #'channel.open'{}),
    ?assertMatch({2, #'channel.open_ok'{}}, recv_method(Sock)),
    ok = gen_tcp:close(Sock).

max_message_size_on_closing_channel(Config) ->
    MaxSize = 1024,
    BodySize = 2 * MaxSize,
    ok = rabbit_ct_broker_helpers:rpc(Config, persistent_term, put, [max_message_size, MaxSize]),
    Sock = raw_connect(Config),
    ok = send_method(Sock, 1, #'queue.declare'{queue = <<"no-such-queue">>, passive = true}),
    ?assertMatch({1, #'channel.close'{reply_code = 404}}, recv_method(Sock)),
    ok = send_method(Sock, 1, #'basic.publish'{routing_key = <<"none">>}),
    ok = gen_tcp:send(Sock, rabbit_binary_generator:create_frame(
                              ?FRAME_HEADER, 1, <<60:16, 0:16, BodySize:64, 0:16>>)),
    ok = gen_tcp:send(Sock, rabbit_binary_generator:create_frame(
                              ?FRAME_BODY, 1, binary:copy(<<"x">>, BodySize))),
    ok = send_method(Sock, 1, #'channel.close'{reply_code = 200, reply_text = <<>>,
                                               class_id = 0, method_id = 0}),
    ?assertMatch({1, #'channel.close_ok'{}}, recv_method(Sock)),
    ok = send_method(Sock, 1, #'channel.close_ok'{}),
    ok = send_method(Sock, 2, #'channel.open'{}),
    ?assertMatch({2, #'channel.open_ok'{}}, recv_method(Sock)),
    ok = gen_tcp:close(Sock).

default_max_message_size(Config) ->
    Binary15M = gen_binary_mb(15),
    Binary17M = gen_binary_mb(20),

    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    %% Binary is within the default max size limit of 16MB
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary15M}),
    %% The channel process is alive
    assert_channel_alive(Ch),

    Monitor = monitor(process, Ch),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary17M}),
    assert_channel_fail_max_size(Ch, Monitor).

%% -------------------------------------------------------------------
%% Implementation
%% -------------------------------------------------------------------

gen_binary_mb(N) ->
    B1M = << <<"_">> || _ <- lists:seq(1, 1024 * 1024) >>,
    << B1M || _ <- lists:seq(1, N) >>.

assert_channel_alive(Ch) ->
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"nope">>},
                          #amqp_msg{payload = <<"HI">>}).

raw_connect(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Sock} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    ok = gen_tcp:send(Sock, <<"AMQP", 0, 0, 9, 1>>),
    {0, #'connection.start'{}} = recv_method(Sock),
    ok = send_method(Sock, 0, #'connection.start_ok'{
                                 client_properties = [],
                                 mechanism = <<"PLAIN">>,
                                 response = <<0, "guest", 0, "guest">>,
                                 locale = <<"en_US">>}),
    {0, #'connection.tune'{channel_max = ChannelMax,
                           frame_max = FrameMax}} = recv_method(Sock),
    ok = send_method(Sock, 0, #'connection.tune_ok'{channel_max = ChannelMax,
                                                    frame_max = FrameMax,
                                                    heartbeat = 0}),
    ok = send_method(Sock, 0, #'connection.open'{virtual_host = <<"/">>}),
    {0, #'connection.open_ok'{}} = recv_method(Sock),
    ok = send_method(Sock, 1, #'channel.open'{}),
    {1, #'channel.open_ok'{}} = recv_method(Sock),
    Sock.

send_method(Sock, Channel, Method) ->
    gen_tcp:send(Sock, rabbit_binary_generator:build_simple_method_frame(Channel, Method)).

recv_method(Sock) ->
    {ok, <<Type:8, Channel:16, Size:32>>} = gen_tcp:recv(Sock, 7, 30_000),
    {ok, <<Payload:Size/binary, ?FRAME_END>>} = gen_tcp:recv(Sock, Size + 1, 30_000),
    {method, Name, Fields} = rabbit_command_assembler:analyze_frame(Type, Payload),
    {Channel, rabbit_framing_amqp_0_9_1:decode_method_fields(Name, Fields)}.

assert_channel_fail_max_size(Ch, Monitor) ->
    receive
        {'DOWN', Monitor, process, Ch,
            {shutdown,
                {server_initiated_close, 406, _Error}}} ->
            ok
    after ?TIMEOUT_CHANNEL_EXCEPTION ->
        error({channel_exception_expected, max_message_size})
    end.
