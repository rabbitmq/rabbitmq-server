%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_rabbit_writer_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-compile([export_all, nowarn_export_all]).

-define(TIMEOUT, 30_000).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [parallel], [
                          commands_are_acked_in_batches,
                          commands_with_content_are_acked_in_batches,
                          large_content_is_acked_immediately
                         ]}
    ].

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

commands_are_acked_in_batches(_Config) ->
    {Writer, Client} = start_writer(),
    send_commands_and_ack(Writer, 450),
    ?assertEqual([{200, 0}, {200, 0}], [receive_acks(Writer), receive_acks(Writer)]),
    ok = assert_qos_ok_frames_received(Client, 450),
    stop_writer(Writer, Client).

commands_with_content_are_acked_in_batches(_Config) ->
    {Writer, Client} = start_writer(),
    Method = #'basic.get_ok'{delivery_tag = 1, redelivered = false,
                             exchange = <<>>, routing_key = <<"rk">>,
                             message_count = 0},
    Content = rabbit_basic:build_content(#'P_basic'{}, <<"payload">>),
    [ok = rabbit_writer:send_command_and_ack(Writer, Method, Content)
     || _ <- lists:seq(1, 200)],
    ?assertEqual({200, 200 * byte_size(<<"payload">>)}, receive_acks(Writer)),
    Frames = iolist_to_binary(
               [rabbit_binary_generator:build_simple_method_frame(1, Method),
                rabbit_binary_generator:build_simple_content_frames(1, Content, 0)]),
    ?assertEqual({ok, binary:copy(Frames, 200)},
                 gen_tcp:recv(Client, 200 * byte_size(Frames), ?TIMEOUT)),
    stop_writer(Writer, Client).

large_content_is_acked_immediately(_Config) ->
    {Writer, Client} = start_writer(),
    Method = #'basic.get_ok'{delivery_tag = 1, redelivered = false,
                             exchange = <<>>, routing_key = <<"rk">>,
                             message_count = 0},
    Payload = binary:copy(<<"x">>, 1_048_576),
    Content = rabbit_basic:build_content(#'P_basic'{}, Payload),
    ok = rabbit_writer:send_command_and_ack(Writer, Method, Content),
    ?assertEqual({1, byte_size(Payload)}, receive_acks(Writer)),
    stop_writer(Writer, Client).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

start_writer() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(LSock),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, Sock} = gen_tcp:accept(LSock),
    ok = gen_tcp:close(LSock),
    {ok, Writer} = rabbit_writer:start_link(Sock, 1, 0, self(), ?MODULE, false),
    {Writer, Client}.

stop_writer(Writer, Client) ->
    unlink(Writer),
    exit(Writer, kill),
    ok = gen_tcp:close(Client).

send_commands_and_ack(Writer, N) ->
    [ok = rabbit_writer:send_command_and_ack(Writer, #'basic.qos_ok'{})
     || _ <- lists:seq(1, N)],
    ok.

receive_acks(Writer) ->
    receive {writer_acks, Writer, Count, Bytes} -> {Count, Bytes}
    after ?TIMEOUT -> ct:fail(ack_timeout)
    end.

assert_qos_ok_frames_received(Client, N) ->
    Frame = iolist_to_binary(
              rabbit_binary_generator:build_simple_method_frame(1, #'basic.qos_ok'{})),
    ?assertEqual({ok, binary:copy(Frame, N)},
                 gen_tcp:recv(Client, N * byte_size(Frame), ?TIMEOUT)),
    ok.
