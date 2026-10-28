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
          terminate_safely_handles_cleanup_failures,
          replies_taken_by_writer,
          replies_not_taken_by_writer,
          tx_commit_replies_not_taken_by_writer,
          basic_get_replies_not_taken_by_writer,
          basic_get_reply_bytes_not_taken_by_writer
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

replies_taken_by_writer(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, replies_taken_by_writer1, [Config]).

replies_taken_by_writer1(_Config) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(LSock),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, Sock} = gen_tcp:accept(LSock),
    ok = gen_tcp:close(LSock),
    Me = self(),
    {ok, Writer} = rabbit_writer:start_link(Sock, 1, 0, Me, ?MODULE, false),
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    {ok, Ch} = rabbit_channel:start_link(1, Me, Writer, Me, "",
                                         rabbit_ct_broker_helpers:user(<<"guest">>),
                                         <<"/">>, [], Me,
                                         Limiter),
    ok = rabbit_channel_common:do(Ch, #'channel.open'{}),
    ok = recv_frames(Client, #'channel.open_ok'{}, 1),
    [begin
         [ok = rabbit_channel_common:do(Ch, #'basic.qos'{})
          || _ <- lists:seq(1, 1_000)],
         ok = recv_frames(Client, #'basic.qos_ok'{}, 1_000)
     end || _ <- lists:seq(1, 10)],
    [begin unlink(P), exit(P, kill) end || P <- [Ch, Writer, Limiter]],
    ok = gen_tcp:close(Client),
    passed.

recv_frames(Client, Method, N) ->
    Frame = iolist_to_binary(
              rabbit_binary_generator:build_simple_method_frame(1, Method)),
    {ok, Data} = gen_tcp:recv(Client, N * byte_size(Frame), ?TIMEOUT),
    Data = binary:copy(Frame, N),
    ok.

replies_not_taken_by_writer(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, replies_not_taken_by_writer1, [Config]).

replies_not_taken_by_writer1(_Config) ->
    replies_not_taken_by_writer([], #'basic.qos'{}).

tx_commit_replies_not_taken_by_writer(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, tx_commit_replies_not_taken_by_writer1, [Config]).

tx_commit_replies_not_taken_by_writer1(_Config) ->
    replies_not_taken_by_writer([{#'tx.select'{}, none}], #'tx.commit'{}).

basic_get_replies_not_taken_by_writer(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, basic_get_replies_not_taken_by_writer1, [Config]).

basic_get_replies_not_taken_by_writer1(_Config) ->
    Q = <<"basic_get_replies_not_taken_by_writer">>,
    Publish = {#'basic.publish'{routing_key = Q},
               rabbit_basic:build_content(#'P_basic'{}, <<"x">>)},
    replies_not_taken_by_writer(
      [{#'queue.declare'{queue = Q, exclusive = true}, none}
       | lists:duplicate(10_000, Publish)],
      #'basic.get'{queue = Q, no_ack = true}).

basic_get_reply_bytes_not_taken_by_writer(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, basic_get_reply_bytes_not_taken_by_writer1, [Config]).

basic_get_reply_bytes_not_taken_by_writer1(_Config) ->
    Q = <<"basic_get_reply_bytes_not_taken_by_writer">>,
    Payload = binary:copy(<<"x">>, 1_048_576),
    Publish = {#'basic.publish'{routing_key = Q},
               rabbit_basic:build_content(#'P_basic'{}, Payload)},
    replies_not_taken_by_writer(
      [{#'queue.declare'{queue = Q, exclusive = true}, none}
       | lists:duplicate(40, Publish)],
      #'basic.get'{queue = Q, no_ack = true}, 100).

replies_not_taken_by_writer(SetupMethods, Method) ->
    replies_not_taken_by_writer(SetupMethods, Method, 10_000).

replies_not_taken_by_writer(SetupMethods, Method, N) ->
    Me = self(),
    Writer = spawn(fun() -> receive after infinity -> ok end end),
    {ok, Limiter} = rabbit_limiter:start_link(no_id),
    {ok, Collector} = rabbit_queue_collector:start_link(?MODULE),
    {ok, Ch} = rabbit_channel:start_link(1, Me, Writer, Me, "",
                                         rabbit_ct_broker_helpers:user(<<"guest">>),
                                         <<"/">>, [], Collector,
                                         Limiter),
    MRef = erlang:monitor(process, Ch),
    [ok = rabbit_channel_common:do(Ch, M, C)
     || {M, C} <- [{#'channel.open'{}, none} | SetupMethods]],
    [ok = rabbit_channel_common:do(Ch, Method) || _ <- lists:seq(1, N)],
    receive
        {channel_exit, 1, #amqp_error{name = resource_error}} -> ok
    after ?TIMEOUT ->
        throw(failed_to_receive_channel_exit)
    end,
    receive
        {'DOWN', MRef, process, Ch, normal} -> ok
    after ?TIMEOUT ->
        throw(channel_did_not_terminate)
    end,
    exit(Writer, kill),
    unlink(Collector),
    exit(Collector, kill),
    passed.

start_channel_and_writer() ->
    {Writer, _Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    ok = rabbit_channel_common:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.
