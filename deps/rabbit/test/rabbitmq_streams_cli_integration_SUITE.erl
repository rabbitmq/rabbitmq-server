%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbitmq_streams_cli_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
        {group, tests}
    ].

groups() ->
    [
        {tests, [], [
            shrink,
            shrink_errors_only,
            grow,
            grow_even_strategy,
            grow_with_pattern,
            grow_with_vhost_pattern,
            grow_invalid_node_filtered
        ]}
    ].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        false ->
            rabbit_ct_helpers:log_environment(),
            Config1 = rabbit_ct_helpers:run_setup_steps(Config),
            rabbit_ct_helpers:ensure_rabbitmq_streams_cmd(Config1);
        _ ->
            {skip, "growing and shrinking cannot be done in mixed mode"}
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(tests, Config0) ->
    NumNodes = 3,
    Config1 = rabbit_ct_helpers:set_config(
                Config0, [{rmq_nodes_count, NumNodes},
                          {rmq_nodes_clustered, true}]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()).

end_per_group(tests, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                    rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:testcase_started(Config0, Testcase).

end_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:testcase_finished(Config0, Testcase).

shrink(Config) ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 2),
    Nodename2 = ?config(nodename, NodeConfig),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    %% declare a stream
    StreamName = <<"shrink_stream1">>,
    #'queue.declare_ok'{} = declare_stream(Ch, StreamName),

    %% Wait for replicas to be established
    wait_for_stream_replicas(Config, StreamName, 3),

    {ok, Out1} = rabbitmq_streams(Config, 0, ["shrink", Nodename2]),
    ?assertMatch(#{{"/", "shrink_stream1"} := {2, ok}}, parse_result(Out1)),

    %% removing a node can trigger a leader election, give this stream some time
    timer:sleep(1500),

    Nodename1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    {ok, Out2} = rabbitmq_streams(Config, 0, ["shrink", Nodename1]),
    ?assertMatch(#{{"/", "shrink_stream1"} := {1, ok}}, parse_result(Out2)),

    %% removing a node can trigger a leader election, give this stream some time
    timer:sleep(1500),

    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, Out3} = rabbitmq_streams(Config, 0, ["shrink", Nodename0]),
    %% Should fail - can't remove the last replica
    ?assertMatch(#{{"/", "shrink_stream1"} := {1, error}}, parse_result(Out3)),
    ok.

shrink_errors_only(Config) ->
    Nodename2 = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    StreamName = <<"shrink_errors_only_stream">>,
    #'queue.declare_ok'{} = declare_stream(Ch, StreamName),
    wait_for_stream_replicas(Config, StreamName, 3),

    Nodename1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),

    %% Regular shrink shows the result
    {ok, Out1} = rabbitmq_streams(Config, 0, ["shrink", Nodename1]),
    ?assertMatch(#{{"/", "shrink_errors_only_stream"} := {2, ok}}, parse_result(Out1)),

    timer:sleep(500),

    %% With --errors-only, successful shrinks are not shown
    {ok, Out2} = rabbitmq_streams(Config, 0, ["shrink", Nodename2, "--errors-only"]),
    ?assertNotMatch(#{{"/", "shrink_errors_only_stream"} := _}, parse_result(Out2)),
    ok.

grow(Config) ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 2),
    Nodename2 = ?config(nodename, NodeConfig),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    %% declare a stream with 1 replica
    StreamName = <<"grow_stream1">>,
    Args = [{<<"x-initial-cluster-size">>, long, 1}],
    #'queue.declare_ok'{} = declare_stream(Ch, StreamName, Args),

    %% Wait for initial replica
    wait_for_stream_replicas(Config, StreamName, 1),

    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, Out1} = rabbitmq_streams(Config, 0, ["grow", Nodename0, "all"]),
    ?assertMatch(#{{"/", "grow_stream1"} := {2, ok}}, parse_result(Out1)),

    timer:sleep(500),

    Nodename1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    {ok, Out2} = rabbitmq_streams(Config, 0, ["grow", Nodename1, "all"]),
    ?assertMatch(#{{"/", "grow_stream1"} := {3, ok}}, parse_result(Out2)),

    %% Growing again should return empty (node already has replica)
    {ok, Out3} = rabbitmq_streams(Config, 0, ["grow", Nodename0, "all"]),
    ?assertNotMatch(#{{"/", "grow_stream1"} := _}, parse_result(Out3)),
    ok.

grow_even_strategy(Config) ->
    Nodename2 = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),

    %% Create stream with 1 replica (odd) - should NOT be grown with "even" strategy
    StreamOdd = <<"grow_even_odd">>,
    #'queue.declare_ok'{} = declare_stream(Ch, StreamOdd,
                                           [{<<"x-initial-cluster-size">>, long, 1}]),
    wait_for_stream_replicas(Config, StreamOdd, 1),

    %% Create stream with 2 replicas (even) - should be grown with "even" strategy
    StreamEven = <<"grow_even_even">>,
    #'queue.declare_ok'{} = declare_stream(Ch, StreamEven,
                                           [{<<"x-initial-cluster-size">>, long, 2}]),
    wait_for_stream_replicas(Config, StreamEven, 2),

    %% Find a node that doesn't have the even stream's replica
    TargetNode = find_node_without_replica(Config, StreamEven),

    {ok, Out} = rabbitmq_streams(Config, 0, ["grow", TargetNode, "even"]),
    Result = parse_result(Out),

    %% Only the even-replica stream should be grown (odd stream has 1 replica, not even)
    StreamEvenStr = binary_to_list(StreamEven),
    StreamOddStr = binary_to_list(StreamOdd),
    ?assertNotMatch(#{{"/", StreamOddStr} := _}, Result),
    ?assertMatch(#{{"/", StreamEvenStr} := {3, ok}}, Result),
    ok.

grow_with_pattern(Config) ->
    Nodename2 = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),

    %% Create streams with different name patterns
    StreamMatch = <<"test-pattern-match">>,
    StreamNoMatch = <<"other-stream">>,
    Args = [{<<"x-initial-cluster-size">>, long, 1}],
    #'queue.declare_ok'{} = declare_stream(Ch, StreamMatch, Args),
    #'queue.declare_ok'{} = declare_stream(Ch, StreamNoMatch, Args),
    wait_for_stream_replicas(Config, StreamMatch, 1),
    wait_for_stream_replicas(Config, StreamNoMatch, 1),

    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, Out} = rabbitmq_streams(Config, 0,
                                 ["grow", Nodename0, "all",
                                  "--queue-pattern", "^test-pattern"]),
    Result = parse_result(Out),

    %% Only matching stream should be grown
    ?assertMatch(#{{"/", "test-pattern-match"} := {2, ok}}, Result),
    ?assertNotMatch(#{{"/", "other-stream"} := _}, Result),
    ok.

grow_with_vhost_pattern(Config) ->
    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Nodename2 = rabbit_ct_broker_helpers:get_node_config(Config, 2, nodename),

    %% Create a test vhost
    TestVhost = <<"test-vhost-pattern">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, TestVhost),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, TestVhost),

    %% Create stream in default vhost
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    StreamDefault = <<"vhost-test-default">>,
    Args = [{<<"x-initial-cluster-size">>, long, 1}],
    #'queue.declare_ok'{} = declare_stream(Ch1, StreamDefault, Args),
    wait_for_stream_replicas(Config, <<"/">>, StreamDefault, 1),

    %% Create stream in test vhost
    {ok, Conn} = amqp_connection:start(#amqp_params_direct{node = Nodename2,
                                                           virtual_host = TestVhost}),
    {ok, Ch2} = amqp_connection:open_channel(Conn),
    StreamTest = <<"vhost-test-stream">>,
    #'queue.declare_ok'{} = declare_stream(Ch2, StreamTest, Args),
    wait_for_stream_replicas(Config, TestVhost, StreamTest, 1),

    %% Grow only streams in vhosts matching pattern
    {ok, Out} = rabbitmq_streams(Config, 0,
                                 ["grow", Nodename0, "all",
                                  "--vhost-pattern", "^test-vhost"]),
    Result = parse_result(Out),

    %% Only stream in test-vhost should be grown
    TestVhostStr = binary_to_list(TestVhost),
    ?assertMatch(#{{TestVhostStr, "vhost-test-stream"} := {2, ok}}, Result),
    ?assertNotMatch(#{{"/", "vhost-test-default"} := _}, Result),

    amqp_channel:close(Ch2),
    amqp_connection:close(Conn),
    rabbit_ct_broker_helpers:delete_vhost(Config, TestVhost),
    ok.

grow_invalid_node_filtered(Config) ->
    NodeConfig = rabbit_ct_broker_helpers:get_node_config(Config, 2),
    Nodename2 = ?config(nodename, NodeConfig),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Nodename2),
    StreamName = <<"grow_stream_err">>,
    Args = [{<<"x-initial-cluster-size">>, long, 1}],
    #'queue.declare_ok'{} = declare_stream(Ch, StreamName, Args),

    DummyNode = not_really_a_node@nothing,
    {error, _ExitCode, _} = rabbitmq_streams(Config, 0, ["grow", DummyNode, "all"]),
    ok.

%% Helper functions

find_node_without_replica(Config, StreamName) ->
    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, Q} = rpc:call(Nodename0, rabbit_amqqueue, lookup,
                       [rabbit_misc:r(<<"/">>, queue, StreamName)]),
    #{nodes := ReplicaNodes} = amqqueue:get_type_state(Q),
    AllNodes = [rabbit_ct_broker_helpers:get_node_config(Config, N, nodename)
                || N <- [0, 1, 2]],
    [Node | _] = AllNodes -- ReplicaNodes,
    Node.

wait_for_stream_replicas(Config, StreamName, ExpectedCount) ->
    wait_for_stream_replicas(Config, <<"/">>, StreamName, ExpectedCount).

wait_for_stream_replicas(Config, Vhost, StreamName, ExpectedCount) ->
    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_helpers:await_condition(
      fun() ->
              case rpc:call(Nodename0, rabbit_amqqueue, lookup,
                            [rabbit_misc:r(Vhost, queue, StreamName)]) of
                  {ok, Q} ->
                      #{nodes := Nodes} = amqqueue:get_type_state(Q),
                      length(Nodes) >= ExpectedCount;
                  _ ->
                      false
              end
      end, 30000).

parse_result(S) ->
    Lines = string:split(S, "\n", all),
    maps:from_list(
      [{{Vhost, QName},
        {erlang:list_to_integer(Size), case Result of
                                           "ok" -> ok;
                                           _ -> error
                                       end}}
       || [Vhost, QName, Size, Result] <-
          [string:split(L, "\t", all) || L <- Lines]]).

declare_stream(Ch, Q) ->
    declare_stream(Ch, Q, []).

declare_stream(Ch, Q, Args0) ->
    Args = [{<<"x-queue-type">>, longstr, <<"stream">>}] ++ Args0,
    amqp_channel:call(Ch, #'queue.declare'{queue = Q,
                                           durable = true,
                                           auto_delete = false,
                                           arguments = Args}).

rabbitmq_streams(Config, N, Args) ->
    rabbit_ct_broker_helpers:rabbitmq_streams(Config, N, ["--silent" | Args]).
