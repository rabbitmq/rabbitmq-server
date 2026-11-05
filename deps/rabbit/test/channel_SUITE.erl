%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(channel_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          ready_for_close_with_dead_writer,
          ready_for_close_with_blocked_writer,
          terminate_safely_handles_cleanup_failures
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
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
%% Testcases
%% -------------------------------------------------------------------

ready_for_close_with_dead_writer(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, ready_for_close_with_dead_writer1, [Config]).

ready_for_close_with_dead_writer1(_Config) ->
    {Writer, Ch} = start_channel_and_writer(),
    MRef = erlang:monitor(process, Ch),

    %% Change channel to closing state (via the reader).
    rabbit_channel_common:do(Ch, #'channel.close'{reply_code = 200,
                                                  reply_text = <<"OK">>,
                                                  class_id = 0,
                                                  method_id = 0}),
    receive
        {channel_closing, Ch} -> ok
    after ?TIMEOUT ->
        throw(failed_to_receive_channel_closing)
    end,

    %% Kill the writer to simulate a closed TCP connection.
    exit(Writer, kill),

    %% The channel should stop normally despite the dead writer.
    rabbit_channel_common:ready_for_close(Ch),
    receive
        {'DOWN', MRef, process, Ch, normal} -> ok;
        {'DOWN', MRef, process, Ch, Reason} ->
            throw({channel_exited_abnormally, Reason})
    after ?TIMEOUT ->
        throw(channel_did_not_terminate)
    end,
    passed.

ready_for_close_with_blocked_writer(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, ready_for_close_with_blocked_writer1, [Config]).

ready_for_close_with_blocked_writer1(_Config) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(LSock),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                   [binary, {active, false}, {recbuf, 4096}]),
    {ok, Sock} = gen_tcp:accept(LSock),
    ok = gen_tcp:close(LSock),
    ok = inet:setopts(Sock, [{sndbuf, 4096}]),
    Me = self(),
    {ok, Writer} = rabbit_writer:start_link(Sock, 1, 0, Me, ?MODULE, false),
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    {ok, Ch} = rabbit_channel:start_link(1, Me, Writer, Me, "",
                                         rabbit_ct_broker_helpers:user(<<"guest">>),
                                         <<"/">>, [], Me,
                                         Limiter),
    MRef = erlang:monitor(process, Ch),
    ok = rabbit_channel_common:do(Ch, #'channel.open'{}),
    Deliver = #'basic.deliver'{consumer_tag = <<"ctag">>,
                               delivery_tag = 1,
                               redelivered = false,
                               exchange = <<>>,
                               routing_key = <<"rk">>},
    Content = rabbit_basic:build_content(
                #'P_basic'{}, binary:copy(<<"x">>, 1_048_576)),
    [ok = rabbit_writer:send_command(Writer, Deliver, Content)
     || _ <- lists:seq(1, 64)],
    ok = rabbit_ct_helpers:await_condition(
           fun() ->
                   {message_queue_len, Len} =
                       erlang:process_info(Writer, message_queue_len),
                   Len > 0
           end, 10_000),

    rabbit_channel_common:do(Ch, #'channel.close'{reply_code = 200,
                                                  reply_text = <<"OK">>,
                                                  class_id = 0,
                                                  method_id = 0}),
    receive
        {channel_closing, Ch} -> ok
    after ?TIMEOUT ->
        throw(failed_to_receive_channel_closing)
    end,
    rabbit_channel_common:ready_for_close(Ch),
    receive
        {'DOWN', MRef, process, Ch, normal} -> ok;
        {'DOWN', MRef, process, Ch, Reason} ->
            throw({channel_exited_abnormally, Reason})
    after 15_000 ->
        throw(channel_did_not_terminate)
    end,
    unlink(Writer),
    exit(Writer, kill),
    unlink(Limiter),
    exit(Limiter, kill),
    ok = gen_tcp:close(Client),
    passed.

terminate_safely_handles_cleanup_failures(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, terminate_safely_handles_cleanup_failures1, [Config]).

terminate_safely_handles_cleanup_failures1(_Config) ->
    {Writer, Ch} = start_channel_and_writer(),
    MRef = erlang:monitor(process, Ch),

    %% Make rabbit_queue_type:close/1 crash to simulate an unexpected
    %% failure during channel termination cleanup.
    ok = meck:new(rabbit_queue_type, [passthrough]),
    meck:expect(rabbit_queue_type, close,
                fun(_) -> error(fake_cleanup_failure) end),

    rabbit_channel_common:do(Ch, #'channel.close'{reply_code = 200,
                                                  reply_text = <<"OK">>,
                                                  class_id = 0,
                                                  method_id = 0}),
    receive
        {channel_closing, Ch} -> ok
    after ?TIMEOUT ->
        meck:unload(rabbit_queue_type),
        throw(failed_to_receive_channel_closing)
    end,

    exit(Writer, kill),

    rabbit_channel_common:ready_for_close(Ch),
    receive
        {'DOWN', MRef, process, Ch, normal} ->
            meck:unload(rabbit_queue_type),
            passed;
        {'DOWN', MRef, process, Ch, Reason} ->
            meck:unload(rabbit_queue_type),
            throw({channel_should_terminate_normally, Reason})
    after ?TIMEOUT ->
        meck:unload(rabbit_queue_type),
        throw(channel_did_not_terminate)
    end.

start_channel_and_writer() ->
    {Writer, _Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    ok = rabbit_channel_common:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.
