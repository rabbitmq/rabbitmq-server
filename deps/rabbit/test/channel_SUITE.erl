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
