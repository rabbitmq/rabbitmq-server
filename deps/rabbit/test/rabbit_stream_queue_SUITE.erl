%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_queue_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-import(rabbit_ct_helpers, [await_condition/2]).
-define(WAIT, 5000).
-define(TIMEOUT, 30_000).

suite() ->
    [{timetrap, 15 * 60_000}].

all() ->
    [
     {group, single_node},
     {group, single_node_parallel_1},
     {group, single_node_parallel_2},
     {group, single_node_parallel_3},
     {group, single_node_parallel_4},
     {group, cluster_size_2},
     {group, cluster_size_2_parallel_1},
     {group, cluster_size_3},
     {group, cluster_size_3_1},
     {group, cluster_size_3_2},
     {group, cluster_size_3_3},
     {group, cluster_size_3_parallel_1},
     {group, cluster_size_3_parallel_2},
     {group, cluster_size_3_parallel_3},
     {group, cluster_size_3_parallel_4},
     {group, cluster_size_3_parallel_5},
     {group, unclustered_size_3_1},
     {group, unclustered_size_3_2},
     {group, unclustered_size_3_3},
     {group, unclustered_size_3_4}
    ].

groups() ->
    [
     {single_node, [],
      [restart_single_node,
       recover,
       format]},
     {single_node_parallel_1, [parallel], all_tests_1()},
     {single_node_parallel_2, [parallel], all_tests_2()},
     {single_node_parallel_3, [parallel], all_tests_3()},
     {single_node_parallel_4, [parallel], all_tests_4()},
     {cluster_size_2, [], [recover]},
     {cluster_size_2_parallel_1, [parallel], all_tests_1()},
     {cluster_size_3, [],
          [
           delete_down_replica,
           replica_recovery,
           leader_failover,
           leader_failover_dedupe,
           add_replicas,
           publish_coordinator_unavailable,
           leader_locator_policy,
           queue_size_on_declare,
           leader_locator_balanced,
           leader_locator_balanced_maintenance,
           select_nodes_with_least_replicas,
           recover_after_leader_and_coordinator_kill,
           restart_stream,
           format,
           rebalance
          ]},
     {cluster_size_3_1, [], [shrink_coordinator_cluster]},
     {cluster_size_3_2, [], [recover,
                             declare_with_node_down_1,
                             declare_with_node_down_2]},
     {cluster_size_3_3, [], [consume_while_deleting_replica]},
     {cluster_size_3_parallel_1, [parallel], [
                                              delete_replica,
                                              delete_last_replica,
                                              delete_classic_replica,
                                              delete_quorum_replica,
                                              consume_from_replica,
                                              initial_cluster_size_one,
                                              initial_cluster_size_two,
                                              initial_cluster_size_one_policy,
                                              leader_locator_client_local,
                                              declare_delete_same_stream
                                             ]},
     {cluster_size_3_parallel_2, [parallel], all_tests_1()},
     {cluster_size_3_parallel_3, [parallel], all_tests_2()},
     {cluster_size_3_parallel_4, [parallel], all_tests_3()},
     {cluster_size_3_parallel_5, [parallel], all_tests_4()},
     {unclustered_size_3_1, [], [add_replica]},
     {unclustered_size_3_2, [], [consume_without_local_replica]},
     {unclustered_size_3_3, [], [grow_coordinator_cluster]},
     {unclustered_size_3_4, [], [grow_then_shrink_coordinator_cluster]}
    ].

all_tests_1() ->
    [
     declare_args,
     declare_max_age,
     declare_invalid_properties,
     declare_server_named,
     declare_invalid_arg,
     declare_invalid_filter_size,
     consume_invalid_arg,
     declare_queue,
     delete_queue,
     publish,
     publish_confirm,
     consume_without_qos
    ].

all_tests_2() ->
    [
     consume,
     consume_offset,
     consume_timestamp_offset,
     consume_timestamp_last_offset,
     basic_get,
     consume_with_autoack,
     consume_and_nack,
     consume_and_ack,
     consume_and_reject,
     consume_from_last,
     consume_from_next,
     consume_from_default
    ].

all_tests_3() ->
    [
     consume_from_relative_time_offset,
     consume_credit,
     consume_credit_out_of_order_ack,
     consume_credit_multiple_ack,
     basic_cancel,
     consumer_metrics_cleaned_on_connection_close,
     consume_cancel_should_create_events,
     receive_basic_cancel_on_queue_deletion,
     keep_consuming_on_leader_restart,
     max_length_bytes,
     max_age,
     invalid_policy,
     max_age_policy
    ].

all_tests_4() ->
    [
     max_segment_size_bytes_validation,
     max_segment_size_bytes_policy,
     max_segment_size_bytes_policy_validation,
     purge,
     update_retention_policy,
     queue_info,
     tracking_status,
     restart_stream,
     dead_letter_target,
     filter_spec,
     filtering
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, [{stream_tick_interval, 256},
                                  {log, [{file, [{level, debug}]}]}]}),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_3_parallel = Group, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "not mixed versions compatible"};
        _ ->
            init_per_group1(Group, Config)
    end;
init_per_group(Group, Config) ->
    init_per_group1(Group, Config).

init_per_group1(Group, Config) ->
    ClusterSize = case Group of
                      single_node -> 1;
                      single_node_parallel_1 -> 1;
                      single_node_parallel_2 -> 1;
                      single_node_parallel_3 -> 1;
                      single_node_parallel_4 -> 1;
                      cluster_size_2 -> 2;
                      cluster_size_2_parallel_1 -> 2;
                      cluster_size_2_parallel_2 -> 2;
                      cluster_size_2_parallel_3 -> 2;
                      cluster_size_2_parallel_4 -> 2;
                      cluster_size_3 -> 3;
                      cluster_size_3_parallel_1 -> 3;
                      cluster_size_3_parallel_2 -> 3;
                      cluster_size_3_parallel_3 -> 3;
                      cluster_size_3_parallel_4 -> 3;
                      cluster_size_3_parallel_5 -> 3;
                      cluster_size_3_1 -> 3;
                      cluster_size_3_2 -> 3;
                      cluster_size_3_3 -> 3;
                      unclustered_size_3_1 -> 3;
                      unclustered_size_3_2 -> 3;
                      unclustered_size_3_3 -> 3;
                      unclustered_size_3_4 -> 3
                  end,
    Clustered = case Group of
                    unclustered_size_3_1 -> false;
                    unclustered_size_3_2 -> false;
                    unclustered_size_3_3 -> false;
                    unclustered_size_3_4 -> false;
                    _ -> true
                end,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base},
                                            {rmq_nodes_clustered, Clustered}]),
    Config1b = case Group of
                   unclustered_size_3_4 ->
                       rabbit_ct_helpers:merge_app_env(
                         Config1, {rabbit, [{stream_tick_interval, 5000}]});
                   _ ->
                       Config1
               end,
    Config1c = rabbit_ct_helpers:merge_app_env(
                 Config1b, {rabbit, [{forced_feature_flags_on_init, [
                                                                     restart_streams,
                                                                     stream_sac_coordinator_unblock_group,
                                                                     stream_update_config_command,
                                                                     stream_filtering,
                                                                     message_containers,
                                                                     quorum_queue_non_voters
                                                                    ]}]}),
    Ret = rabbit_ct_helpers:run_steps(Config1c,
                                      [fun merge_app_env/1 ] ++
                                      rabbit_ct_broker_helpers:setup_steps()),
    case Ret of
        {skip, _} ->
            Ret;
        Config2 ->
            ok = rabbit_ct_broker_helpers:rpc(
                   Config2, 0, application, set_env,
                   [rabbit, channel_tick_interval, 100]),
            Config2
    end.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(TestCase, Config)
  when TestCase == receive_basic_cancel_on_queue_deletion
       orelse TestCase == keep_consuming_on_leader_restart ->
    ClusterSize = ?config(rmq_nodes_count, Config),
    case {rabbit_ct_helpers:is_mixed_versions(), ClusterSize} of
        {true, 2} ->
            %% These 2 tests fail because the leader can be the lower version,
            %% which does not have the fix.
            {skip, "not tested in mixed-version cluster and cluster size = 2"};
        _ ->
            init_test_case(TestCase, Config)
    end;
init_per_testcase(TestCase, Config)
  when TestCase == replica_recovery
       orelse TestCase == leader_failover
       orelse TestCase == leader_failover_dedupe
       orelse TestCase == recover_after_leader_and_coordinator_kill ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            %% not supported because of machine version difference
            {skip, "mixed version clusters are not supported"};
        _ ->
            init_test_case(TestCase, Config)
    end;
init_per_testcase(TestCase, Config)
  when TestCase == filtering ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "filtering should not be used in mixed-version clusters"};
        _ ->
            init_test_case(TestCase, Config)
    end;
init_per_testcase(TestCase, Config) ->
    init_test_case(TestCase, Config).

init_test_case(TestCase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, TestCase),
    Q = rabbit_data_coercion:to_binary(TestCase),
    Config2 = rabbit_ct_helpers:set_config(Config1, [{queue_name, Q}]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

declare_args(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                             {<<"x-max-length-bytes">>, long, 2_000_000},
                                             {<<"x-max-age">>, longstr, <<"10D">>},
                                             {<<"x-stream-max-segment-size-bytes">>, long, 5_000_000},
                                             {<<"x-stream-filter-size-bytes">>, long, 32}
                                            ])),
    assert_queue_type(Server, Q, rabbit_stream_queue),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

declare_max_age(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Q = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(Config, Server, Q,
               [{<<"x-queue-type">>, longstr, <<"stream">>},
                {<<"x-max-age">>, longstr, <<"1A">>}])),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                             {<<"x-max-age">>, longstr, <<"1Y">>}])),
    assert_queue_type(Server, Q, rabbit_stream_queue),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

declare_invalid_properties(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       retry_if_coordinator_unavailable(
         Config, Server,
         #'queue.declare'{queue     = Q,
                          auto_delete = true,
                          durable   = true,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       retry_if_coordinator_unavailable(
         Config, Server,
         #'queue.declare'{queue     = Q,
                          exclusive = true,
                          durable   = true,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       retry_if_coordinator_unavailable(
         Config, Server,
         #'queue.declare'{queue     = Q,
                          durable   = false,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})).

declare_server_named(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(Config, Server,
               <<"">>, [{<<"x-queue-type">>, longstr, <<"stream">>}])).

declare_invalid_arg(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),

    ExpectedError = <<"PRECONDITION_FAILED - invalid arg 'x-overflow' for queue "
                      "'declare_invalid_arg' in vhost '/' of queue type rabbit_stream_queue">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                       {<<"x-overflow">>, longstr, <<"reject-publish">>}])).

declare_invalid_filter_size(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),

    ExpectedError = <<"PRECONDITION_FAILED - Invalid value for  x-stream-filter-size-bytes">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                   {<<"x-stream-filter-size-bytes">>, long, 256}])).

consume_invalid_arg(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ExpectedError = <<"PRECONDITION_FAILED - invalid arg 'x-priority' for queue "
                      "'consume_invalid_arg' in vhost '/' of queue type rabbit_stream_queue">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       amqp_channel:subscribe(Ch, #'basic.consume'{
                                     queue = Q,
                                     arguments = [{<<"x-priority">>, long, 10}],
                                     no_ack = false,
                                     consumer_tag = <<"ctag">>},
                              self())).

declare_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    %% Test declare an existing queue
    %% there is a very brief race condition in the osiris counter updates that could
    %% cause the message count to be reported as 1 temporarily after a new stream
    %% creation. Hence to avoid flaking we don't match on the messages counter
    %% here
    ?assertMatch({'queue.declare_ok', Q, _, 0},
                declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ?assertMatch([_], find_queue_info(Config, [])),

    %% Test declare an existing queue with different arguments
    ?assertExit(_, declare(Config, Server, Q, [])),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

find_queue_info(Config, Keys) ->
    find_queue_info(Config, 0, Keys).

find_queue_info(Config, Node, Keys) ->
    find_queue_info(?config(queue_name, Config), Config, Node, Keys).

find_queue_info(Name, Config, Node, Keys) ->
    QName = rabbit_misc:r(<<"/">>, queue, Name),
    rabbit_ct_broker_helpers:rpc(Config, Node, ?MODULE, find_queue_info_rpc,
                                 [QName, [name | Keys]]).

find_queue_info_rpc(QName, Infos) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            rabbit_amqqueue:info(Q, Infos);
        _ ->
            []
    end.

delete_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ?assertMatch(#'queue.delete_ok'{}, delete(Config, Server, Q)).

add_replicas(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server0, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-initial-cluster-size">>, long, 1}])),

    %% TODO: add lots of data so that replica is still out of sync when
    %% second request comes in
    NumMsgs = 1000,
    Data = crypto:strong_rand_bytes(1000),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    [publish(Ch, Q, Data) || _ <- lists:seq(1, NumMsgs)],
    %% wait for confirms here to ensure the next message ends up in a chunk
    %% of it's own
    amqp_channel:wait_for_confirms(Ch, 30),
    publish(Ch, Q, <<"last">>),
    amqp_channel:wait_for_confirms(Ch, 30),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])),

    check_leader_and_replicas(Config, [Server0, Server1]),

    %% it is almost impossible to reliably catch this situation.
    %% increasing number of messages published and the data size could help
    % ?assertMatch({error, {disallowed, out_of_sync_replica}} ,
    ?assertMatch(ok ,
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server2])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),

    %% validate we can read the last entry
    qos(Ch, 10, false),
    amqp_channel:subscribe(
      Ch, #'basic.consume'{queue = Q,
                           no_ack = false,
                           consumer_tag = <<"ctag">>,
                           arguments = [{<<"x-stream-offset">>, longstr, <<"last">>}]},
      self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag},
         #amqp_msg{payload = <<"last">>}} ->
            ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                                    multiple     = false})
    after 60000 ->
              flush(),
              ?assertMatch(#'queue.delete_ok'{},
                           delete(Config, Server0, Q)),
              exit(deliver_timeout)
    end,
    ?assertMatch(#'queue.delete_ok'{},
                 delete(Config, Server0, Q)),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

add_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),

    %% Let's also try the add replica command on other queue types, it should fail
    %% We're doing it in the same test for efficiency, otherwise we have to
    %% start new rabbitmq clusters every time for a minor testcase
    QClassic = <<Q/binary, "_classic">>,
    QQuorum = <<Q/binary, "_quorum">>,

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ?assertEqual({'queue.declare_ok', QClassic, 0, 0},
                 declare(Config, Server1, QClassic, [{<<"x-queue-type">>, longstr, <<"classic">>}])),
    ?assertEqual({'queue.declare_ok', QQuorum, 0, 0},
                 declare(Config, Server1, QQuorum, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, node_not_running},
                 rpc:call(Server1, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server0])),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server1, rabbit_stream_queue, add_replica,
                          [<<"/">>, QClassic, Server0])),
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server1, rabbit_stream_queue, add_replica,
                          [<<"/">>, QQuorum, Server0])),

    Config1 = rabbit_ct_broker_helpers:cluster_nodes(
                Config, Server1, [Server0]),
    timer:sleep(1000),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server1, rabbit_stream_queue, add_replica,
                          [<<"/">>, QClassic, Server0])),
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server1, rabbit_stream_queue, add_replica,
                          [<<"/">>, QQuorum, Server0])),
    ?assertEqual(ok,
                 rpc:call(Server1, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server0])),
    %% replicas must be recorded on the state, and if we publish messages then they must
    %% be stored on disk
    check_leader_and_replicas(Config1, [Server1, Server0]),
    %% And if we try again? Idempotent
    ?assertEqual(ok, rpc:call(Server1, rabbit_stream_queue, add_replica,
                              [<<"/">>, Q, Server0])),
    %% Add another node
    Config2 = rabbit_ct_broker_helpers:cluster_nodes(
                Config1, Server1, [Server2]),
    ?assertEqual(ok, rpc:call(Server1, rabbit_stream_queue, add_replica,
                              [<<"/">>, Q, Server2])),
    check_leader_and_replicas(Config2, [Server0, Server1, Server2]),
    rabbit_ct_broker_helpers:rpc(Config2, Server1, ?MODULE, delete_testcase_queue, [Q]).

delete_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server0, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    %% Not a member of the cluster, what would happen?
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, 'zen@rabbit'])),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),
    %% check it's gone
    check_leader_and_replicas(Config, [Server0, Server2]),
    %% And if we try again? Idempotent
    ?assertEqual(ok, rpc:call(Server0, rabbit_stream_queue, delete_replica,
                              [<<"/">>, Q, Server1])),
    %% Delete the last replica
    ?assertEqual(ok, rpc:call(Server0, rabbit_stream_queue, delete_replica,
                              [<<"/">>, Q, Server2])),
    check_leader_and_replicas(Config, [Server0]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

delete_last_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server0, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),

    check_leader_and_replicas(Config, [Server0, Server2], members),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server2])),
    %% check they're gone
    check_leader_and_replicas(Config, [Server0], members),
    %% delete the last one
    ?assertEqual({error, last_stream_member},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server0])),
    %% It's still here
    check_leader_and_replicas(Config, [Server0]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

grow_then_shrink_coordinator_cluster(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    _Config1 = rabbit_ct_broker_helpers:cluster_nodes(Config, Server1, [Server0, Server2]),

    rabbit_ct_helpers:await_condition(
      fun() ->
              case rpc:call(Server0, ra, members,
                            [{rabbit_stream_coordinator, Server0}]) of
                  {_, Members, _} ->
                      Nodes = lists:sort([N || {_, N} <- Members]),
                      lists:sort([Server0, Server1, Server2]) == Nodes;
                  _ ->
                      false
              end
      end, 60000),

    ok = rabbit_control_helper:command(stop_app, Server0),
    ok = rabbit_control_helper:command(forget_cluster_node, Server1, [atom_to_list(Server0)], []),
    ok = rabbit_control_helper:command(stop_app, Server2),
    ok = rabbit_control_helper:command(forget_cluster_node, Server1, [atom_to_list(Server2)], []),
    rabbit_ct_helpers:await_condition(
      fun() ->
              case rpc:call(Server1, ra, members,
                            [{rabbit_stream_coordinator, Server1}]) of
                  {_, Members, _} ->
                      Nodes = lists:sort([N || {_, N} <- Members]),
                      lists:sort([Server1]) == Nodes;
                  _ ->
                      false
              end
      end, 60000),
    ok.

grow_coordinator_cluster(Config) ->
    [Server0, Server1, _Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Config1 = rabbit_ct_broker_helpers:cluster_nodes(Config, Server1, [Server0]),
    %% at this point there _probably_ won't be a stream coordinator member on
    %% Server1

    %% check we can add a new stream replica for the previously declare stream
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server0])),
    %% also check we can declare a new stream when calling Server1
    Q2 = unicode:characters_to_binary([Q, <<"_2">>]),
    ?assertEqual({'queue.declare_ok', Q2, 0, 0},
                 declare(Config1, Server0, Q2, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    %% wait until the stream coordinator detects there is a new rabbit node
    %% and adds a new member on the new node
    rabbit_ct_helpers:await_condition(
      fun() ->
              case rpc:call(Server1, ra, members,
                            [{rabbit_stream_coordinator, Server1}]) of
                  {_, Members, _} ->
                      Nodes = lists:sort([N || {_, N} <- Members]),
                      lists:sort([Server0, Server1]) == Nodes;
                  _ ->
                      false
              end
      end, 60000),
    rabbit_ct_broker_helpers:rpc(Config1, 1, ?MODULE, delete_testcase_queue, [Q]).

shrink_coordinator_cluster(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),


    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server0, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ok = rabbit_control_helper:command(stop_app, Server2),
    ok = rabbit_control_helper:command(forget_cluster_node, Server0, [atom_to_list(Server2)], []),

    rabbit_ct_helpers:await_condition(
      fun() ->
              case rpc:call(Server0, ra, members, [{rabbit_stream_coordinator, Server0}]) of
                  {_, Members, _} ->
                      Nodes = lists:sort([N || {_, N} <- Members]),
                      lists:sort([Server0, Server1]) == Nodes;
                  _ ->
                      false
              end
      end, 60000),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

delete_classic_replica(Config) ->
    [Server0, Server1, _Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server0, Q, [{<<"x-queue-type">>, longstr, <<"classic">>}])),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, 'zen@rabbit'])),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

delete_quorum_replica(Config) ->
    [Server0, Server1, _Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server0, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, 'zen@rabbit'])),
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

delete_down_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server0, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),
    %% check it's gone
    check_leader_and_replicas(Config, [Server0, Server2], members),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    check_leader_and_replicas(Config, [Server0, Server2], members),
    %% check the folder was deleted
    QName = rabbit_misc:r(<<"/">>, queue, Q),
    StreamId = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_stream_id, [QName]),
    Server1DataDir = rabbit_ct_broker_helpers:get_node_config(Config, 1, data_dir),
    DeletedReplicaDir = filename:join([Server1DataDir, "stream", StreamId]),

    ?awaitMatch(false, filelib:is_dir(DeletedReplicaDir), ?WAIT),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

publish_coordinator_unavailable(Config) ->
    %% This testcase leaves Khepri on minority when it is enabled.
    %% If the Khepri leader is in one of the nodes being stopped, the
    %% remaining node won't be able to reply to any channel query.
    %% Let's remove it from the Khepri test suite, as it does not make
    %% much sense to test something that will randomly work
    %% depending on where the leader is placed - even though we could
    %% always select as running node the Khepri leader
    [Server0, Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server0, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    rabbit_ct_helpers:await_condition(
      fun () ->
              N = rabbit_ct_broker_helpers:rpc(Config, Server0, rabbit_nodes, list_running, []),
              length(N) == 1
      end),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, Q),
    ok = rabbit_ct_broker_helpers:async_start_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:async_start_node(Config, Server2),
    ok = rabbit_ct_broker_helpers:wait_for_async_start_node(Server1),
    ok = rabbit_ct_broker_helpers:wait_for_async_start_node(Server2),
    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, Server0, [online]),
              length(proplists:get_value(online, Info)) == 3
      end),
    % ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 506, _}}}, _},
    %%% confirms should be issued when available
    amqp_channel:wait_for_confirms(Ch, 60),
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server0),
    publish(Ch1, Q),

    #'confirm.select_ok'{} = amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),
    publish(Ch1, Q),
    amqp_channel:wait_for_confirms(Ch1, 30),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

publish(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish(Ch, Q),
    queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

publish_confirm(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, Q),
    amqp_channel:wait_for_confirms(Ch, 5),
    queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

restart_single_node(Config) ->
    [Server] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    publish(Ch, Q),
    queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),

    rabbit_control_helper:command(stop_app, Server),
    rabbit_control_helper:command(start_app, Server),

    queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    publish(Ch1, Q),
    queue_utils:wait_for_messages(Config, [[Q, <<"2">>, <<"2">>, <<"0">>]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

%% the failing case for this test relies on a particular random condition
%% please never consider this a flake
declare_with_node_down_1(Config) ->
    [Server1, Server2, Server3] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:stop_node(Config, Server2),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-initial-cluster-size">>, long, 3}])),
    check_leader_and_replicas(Config, [Server1, Server3]),
    %% Since there are not sufficient running nodes, we expect that
    %% also stopped nodes are selected as replicas.
    check_members(Config, Servers),
    rabbit_ct_broker_helpers:start_node(Config, Server2),
    check_leader_and_replicas(Config, Servers),
    ok.

declare_with_node_down_2(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:stop_node(Config, Server2),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-initial-cluster-size">>, long, 2},
                                              {<<"x-queue-leader-locator">>, longstr, <<"balanced">>}])),
    check_leader_and_replicas(Config, [Server1, Server3]),
    %% Since there are sufficient running nodes, we expect that
    %% stopped nodes are not selected as replicas.
    check_members(Config, [Server1, Server3]),
    rabbit_ct_broker_helpers:start_node(Config, Server2),
    check_leader_and_replicas(Config, [Server1, Server3]),
    ok.

recover(Config) ->
    [Server | _] = Servers0 = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    publish(Ch, Q),
    queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),

    Perm0 = permute(Servers0),
    Servers = lists:nth(rand:uniform(length(Perm0)), Perm0),
    %% Such a slow test, let's select a single random permutation and trust that over enough
    %% ci rounds any failure will eventually show up

    flush(),
    ct:pal("recover: running stop start for permutation ~w", [Servers]),
    [rabbit_ct_broker_helpers:stop_node(Config, S) || S <- Servers],
    [rabbit_ct_broker_helpers:async_start_node(Config, S) || S <- lists:reverse(Servers)],
    [ok = rabbit_ct_broker_helpers:wait_for_async_start_node(S) || S <- lists:reverse(Servers)],

    ct:pal("recover: post stop / start, waiting for messages ~w", [Servers]),
    check_leader_and_replicas(Config, Servers0),
    queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]], 60),

    %% Another single random permutation
    Perm1 = permute(Servers0),
    Servers1 = lists:nth(rand:uniform(length(Perm1)), Perm1),

    ct:pal("recover: running app stop start for permuation ~w", [Servers1]),
    [rabbit_control_helper:command(stop_app, S) || S <- Servers1],
    [rabbit_control_helper:async_command(start_app, S, [], [])
     || S <- lists:reverse(Servers1)],
    [rabbit_control_helper:wait_for_async_command(S) || S <- lists:reverse(Servers1)],

    ct:pal("recover: running app stop waiting for messages ~w", [Servers1]),
    check_leader_and_replicas(Config, Servers0),
    queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]], 60),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    publish(Ch1, Q),
    queue_utils:wait_for_messages(Config, [[Q, <<"2">>, <<"2">>, <<"0">>]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_without_qos(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ?assertExit({{shutdown, {server_initiated_close, 406, _}}, _},
                amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, consumer_tag = <<"ctag">>},
                                       self())),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_without_local_replica(Config) ->
    [Server0, Server1 | _] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    %% Add another node to the cluster, but it won't have a replica
    Config1 = rabbit_ct_broker_helpers:cluster_nodes(Config, Server1, [Server0]),
    timer:sleep(1000),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config1, Server0),
    qos(Ch1, 10, false),
    ?assertExit({{shutdown, {server_initiated_close, 406, _}}, _},
                amqp_channel:subscribe(Ch1, #'basic.consume'{queue = Q, consumer_tag = <<"ctag">>},
                                       self())),
    rabbit_ct_broker_helpers:rpc(Config1, 1, ?MODULE, delete_testcase_queue, [Q]).

consume(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),
    subscribe(Ch1, Q, false, 0),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok = amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = DeliveryTag,
                                                     multiple = false}),
            _ = amqp_channel:call(Ch1, #'basic.cancel'{consumer_tag = <<"ctag">>}),
            ok = amqp_channel:close(Ch1)
    after ?TIMEOUT ->
            ct:fail(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_offset(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Payload = << <<"1">> || _ <- lists:seq(1, 500) >>,
    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 1000)]),

    run_proper(
      fun () ->
              ?FORALL(Offset, range(0, 999),
                      begin
                          Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
                          qos(Ch1, 10, false),
                          subscribe(Ch1, Q, false, Offset),
                          receive_batch(Ch1, Offset, 999),
                          receive
                              {_,
                               #amqp_msg{props = #'P_basic'{headers = [{<<"x-stream-offset">>, long, S}]}}}
                                when S < Offset ->
                                  exit({unexpected_offset, S})
                          after 1000 ->
                                  ok
                          end,
                          amqp_channel:call(Ch1, #'basic.cancel'{consumer_tag = <<"ctag">>}),
                          true
                      end)
      end, [], 5), %% Run it only 5 times. This test times out quite often, not in the receive
%% clause but ct itself. Consume so many messages so many times could take too long
%% in some CPU configurations. Let's trust that many rounds of CI could find any real failure.
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_timestamp_offset(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"111">> || _ <- lists:seq(1, 100)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),

    Offset = erlang:system_time(second) - 60,
    amqp_channel:subscribe(
      Ch1,
      #'basic.consume'{queue = Q,
                       no_ack = false,
                       consumer_tag = <<"ctag">>,
                       arguments = [{<<"x-stream-offset">>, timestamp, Offset}]},
      self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
            ok
    after ?TIMEOUT ->
            flush(),
            exit(consume_ok_timeout)
    end,

    %% It has subscribed to a very old timestamp, so we will receive the whole stream
    receive_batch(Ch1, 0, 99),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_timestamp_last_offset(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"111">> || _ <- lists:seq(1, 100)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),

    %% Subscribe from now/future
    Offset = erlang:system_time(second) + 60,
    CTag = <<"consume_timestamp_last_offset">>,
    amqp_channel:subscribe(
      Ch1,
      #'basic.consume'{queue = Q,
                       no_ack = false,
                       consumer_tag = CTag,
                       arguments = [{<<"x-stream-offset">>, timestamp, Offset}]},
      self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
            ok
    after ?TIMEOUT ->
            exit(missing_consume_ok)
    end,

    receive
        {#'basic.deliver'{consumer_tag = CTag},
         #amqp_msg{props = #'P_basic'{headers = [{<<"x-stream-offset">>, long, S}]}}}
          when S < 100 ->
            exit({unexpected_offset, S})
    after 1000 ->
            ok
    end,

    %% Publish a few more
    [publish(Ch, Q, <<"msg2">>) || _ <- lists:seq(1, 100)],
    amqp_channel:wait_for_confirms(Ch, 5),

    %% Yeah! we got them
    receive_batch(Ch1, 100, 199),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

basic_get(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
                amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_with_autoack(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),

    ?assertExit(
       {{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
       subscribe(Ch1, Q, true, 0)),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

basic_cancel(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),
    CTag = <<"basic_cancel">>,
    subscribe(Ch1, Q, false, 0, CTag),
    rabbit_ct_helpers:await_condition(
      fun() ->
              1 == length(filter_consumers(Config, Server, CTag))
      end, 30000),
    receive
        {#'basic.deliver'{}, _} ->
            amqp_channel:call(Ch1, #'basic.cancel'{consumer_tag = CTag}),
            ?assertMatch([], filter_consumers(Config, Server, CTag))
    after ?TIMEOUT ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consumer_metrics_cleaned_on_connection_close(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Conn = rabbit_ct_client_helpers:open_connection(Config, Server),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    qos(Ch, 10, false),
    CTag = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    subscribe(Ch, Q, false, 0, CTag),
    rabbit_ct_helpers:await_condition(
      fun() ->
              1 == length(filter_consumers(Config, Server, CTag))
      end, 30000),

    ok = rabbit_ct_client_helpers:close_connection(Conn),

    rabbit_ct_helpers:await_condition(
      fun() ->
              0 == length(filter_consumers(Config, Server, CTag))
      end, 30000),

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_cancel_should_create_events(Config) ->
    HandlerMod = rabbit_list_test_event_handler,
    rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, HandlerMod),
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 gen_event,
                                 add_handler,
                                 [rabbit_event, HandlerMod, []]),
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Conn = rabbit_ct_client_helpers:open_connection(Config, Server),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    qos(Ch, 10, false),

    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
                                      gen_event,
                                      call,
                                      [rabbit_event, HandlerMod, clear_events]),

    CTag = rabbit_data_coercion:to_binary(?FUNCTION_NAME),

    ?assertEqual([], filtered_events(Config, consumer_created, CTag)),
    ?assertEqual([], filtered_events(Config, consumer_deleted, CTag)),

    subscribe(Ch, Q, false, 0, CTag),

    ?awaitMatch([{event, consumer_created, _, _, _}], filtered_events(Config, consumer_created, CTag), ?WAIT),
    ?assertEqual([], filtered_events(Config, consumer_deleted, CTag)),

    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),

    ?awaitMatch([{event, consumer_deleted, _, _, _}], filtered_events(Config, consumer_deleted, CTag), ?WAIT),

    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 gen_event,
                                 delete_handler,
                                 [rabbit_event, HandlerMod, []]),

    ok = rabbit_ct_client_helpers:close_connection(Conn),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

receive_basic_cancel_on_queue_deletion(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    qos(Ch, 10, false),
    CTag = <<"basic_cancel_notification_on_queue_deletion">>,
    subscribe(Ch, Q, false, 0, CTag),
    rabbit_ct_helpers:await_condition(
      fun() ->
              1 == length(filter_consumers(Config, Server, CTag))
      end, 30000),

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]),
    receive
        #'basic.cancel'{consumer_tag = CTag} ->
            ok
    after ?TIMEOUT ->
            exit(timeout)
    end.

recover_after_leader_and_coordinator_kill(Config) ->
    [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch1, Q, [<<"msg 1">>]),

    QName = rabbit_misc:r(<<"/">>, queue, Q),
    [begin
         Sleep = rand:uniform(10),
         {ok, {_LeaderNode, LeaderPid}} = leader_info(Config),
         {_, CoordNode} = get_stream_coordinator_leader(Config),
         kill_stream_leader_then_coordinator_leader(Config, CoordNode,
                                                    LeaderPid, Sleep),
         publish_confirm(Ch1, Q, [<<"msg">>]),
         recover_coordinator(Config, CoordNode),
         timer:sleep(Sleep),
         rabbit_ct_helpers:await_condition(
           fun () ->
                   rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                                validate_writer_pid, [QName])
           end)
     end || _Num <- lists:seq(1, 10)],

    {_, Node} = get_stream_coordinator_leader(Config),

    CState = rabbit_ct_broker_helpers:rpc(Config, Node, sys,
                                          get_state,
                                          [rabbit_stream_coordinator]),


    ct:pal("sys state ~p", [CState]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]),
    ok.

keep_consuming_on_leader_restart(Config) ->
    [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch1, Q, [<<"msg 1">>]),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    qos(Ch2, 10, false),
    subscribe(Ch2, Q, false, 0),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1}, _} ->
            ok = amqp_channel:cast(Ch2, #'basic.ack'{delivery_tag = DeliveryTag1,
                                                     multiple = false})
    after ?TIMEOUT ->
              exit(timeout)
    end,

    {ok, {LeaderNode, LeaderPid}} = leader_info(Config),

    kill_process(Config, LeaderNode, LeaderPid),

    publish_confirm(Ch1, Q, [<<"msg 2">>]),

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2}, _} ->
            ok = amqp_channel:cast(Ch2, #'basic.ack'{delivery_tag = DeliveryTag2,
                                                     multiple = false})
    after ?TIMEOUT ->
              exit(timeout)
    end,

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

leader_info(Config) ->
    Name = ?config(queue_name, Config),
    QName = rabbit_misc:r(<<"/">>, queue, Name),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_leader_info,
                                 [QName]).

validate_writer_pid(QName) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    MnesiaPid = amqqueue:get_pid(Q),
    QState = amqqueue:get_type_state(Q),
    #{name := StreamId} = QState,
    {ok, MnesiaPid} == rabbit_stream_coordinator:writer_pid(StreamId).

get_leader_info(QName) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    QState = amqqueue:get_type_state(Q),
    #{name := StreamName} = QState,
    case rabbit_stream_coordinator:members(StreamName) of
        {ok, Members} ->
            maps:fold(fun (LeaderNode, {Pid, writer}, _Acc) ->
                              {ok, {LeaderNode, Pid}};
                          (_Node, _, Acc) ->
                              Acc
                      end,
                      {error, not_found}, Members);
        _ ->
            {error, not_found}
    end.

get_stream_id(QName) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    QState = amqqueue:get_type_state(Q),
    #{name := StreamId} = QState,
    StreamId.

kill_process(Config, Node, Pid) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, ?MODULE, do_kill_process,
                                 [Pid]).

do_kill_process(Pid) ->
    exit(Pid, kill).

do_stop_start_coordinator() ->
    rabbit_stream_coordinator:stop(),
    timer:sleep(10),
    rabbit_stream_coordinator:recover().

recover_coordinator(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_stream_coordinator,
                                 recover, []).

get_stream_coordinator_leader(Config) ->
    Node = hd(rabbit_ct_broker_helpers:get_node_configs(Config, nodename)),
    rabbit_ct_helpers:await_condition(
      fun() ->
              Ret = rabbit_ct_broker_helpers:rpc(
                      Config, Node, ra_leaderboard, lookup_leader,
                      [rabbit_stream_coordinator]),
              is_tuple(Ret)
      end),
    rabbit_ct_broker_helpers:rpc(Config, Node, ra_leaderboard,
                                 lookup_leader, [rabbit_stream_coordinator]).

kill_stream_leader_then_coordinator_leader(Config, CoordLeaderNode,
                                           StreamLeaderPid, Sleep) ->
    rabbit_ct_broker_helpers:rpc(Config, CoordLeaderNode, ?MODULE,
                                 do_stop_kill, [StreamLeaderPid, Sleep]).

do_stop_kill(Pid, Sleep) ->
    exit(Pid, kill),
    timer:sleep(Sleep),
    rabbit_stream_coordinator:stop().


filter_consumers(Config, Server, CTag) ->
    CInfo = rabbit_ct_broker_helpers:rpc(Config, Server, ets, tab2list, [consumer_created]),
    lists:foldl(fun(Tuple, Acc) ->
                        Key = element(1, Tuple),
                        case Key of
                            {_, _, CTag} ->
                                [Key | Acc];
                            _ -> Acc
                        end
                end, [], CInfo).


filtered_events(Config, EventType, CTag) ->
    Events = rabbit_ct_broker_helpers:rpc(Config, 0,
                                          gen_event,
                                          call,
                                          [rabbit_event, rabbit_list_test_event_handler, get_events]),
    lists:filter(fun({event, Type, Fields, _, _}) when Type =:= EventType ->
                         proplists:get_value(consumer_tag, Fields) =:= CTag;
                    (_) ->
                         false
                 end, Events).

consume_and_reject(Config) ->
    consume_and_(Config, fun (DT) -> #'basic.reject'{delivery_tag = DT} end).
consume_and_nack(Config) ->
    consume_and_(Config, fun (DT) -> #'basic.nack'{delivery_tag = DT} end).
consume_and_ack(Config) ->
    consume_and_(Config, fun (DT) -> #'basic.ack'{delivery_tag = DT} end).

consume_and_(Config, AckFun) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),
    subscribe(Ch1, Q, false, 0),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok = amqp_channel:cast(Ch1, AckFun(DeliveryTag)),
            %% It will succeed as ack is now a credit operation. We should be
            %% able to redeclare a queue (gen_server call op) as the channel
            %% should still be open and declare is an idempotent operation
            %%
            ?assertMatch({'queue.declare_ok', Q, _MsgCount, 0},
                         declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
            queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]])
    after ?TIMEOUT ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

tracking_status(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Vhost = ?config(rmq_vhost, Config),
    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_stream_queue,
                                                  ?FUNCTION_NAME, [Vhost, Q])),
    publish_confirm(Ch, Q, [<<"msg">>]),
    ?assertMatch([[
                   {type, sequence},
                   {reference, _WriterID},
                   {value, {_Offset = 0, _Seq = 1}}
                  ]],
                 rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_stream_queue, ?FUNCTION_NAME, [Vhost, Q])),
    rabbit_ct_broker_helpers:rpc(Config, Server, ?MODULE, delete_testcase_queue, [Q]).

restart_stream(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),
    Vhost = ?config(rmq_vhost, Config),
    QName = #resource{virtual_host = Vhost,
                      kind = queue,
                      name = Q},
    %% restart the stream
    ?assertMatch({ok, _},
                 rabbit_ct_broker_helpers:rpc(Config, Server,
                                              rabbit_stream_coordinator,
                                              ?FUNCTION_NAME, [QName])),

    publish_confirm(Ch, Q, [<<"msg2">>]),
    rabbit_ct_broker_helpers:rpc(Config, Server, ?MODULE, delete_testcase_queue, [Q]),
    ok.

format(Config) ->
    %% tests rabbit_stream_queue:format/2
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Server = hd(Nodes),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),
    Vhost = ?config(rmq_vhost, Config),
    QName = #resource{virtual_host = Vhost,
                      kind = queue,
                      name = Q},
    {ok, QRecord}  = rabbit_ct_broker_helpers:rpc(Config, Server,
                                                  rabbit_amqqueue,
                                                  lookup, [QName]),
    %% restart the stream
    Fmt = rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_stream_queue,
                                       ?FUNCTION_NAME, [QRecord, #{}]),

    %% test all up case
    ?assertEqual(<<"stream">>, proplists:get_value(type, Fmt)),
    ?assertEqual(running, proplists:get_value(state, Fmt)),
    ?assertEqual(Server, proplists:get_value(leader, Fmt)),
    ?assertEqual(Server, proplists:get_value(node, Fmt)),
    ?assertEqual(Nodes, proplists:get_value(online, Fmt)),
    ?assertEqual(Nodes, proplists:get_value(members, Fmt)),

    case length(Nodes) of
        3 ->
            [_, Server2, Server3] = Nodes,
            ok = rabbit_control_helper:command(stop_app, Server3),
            ok = rabbit_control_helper:command(stop_app, Server2),

            Fmt2 = rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_stream_queue,
                                               ?FUNCTION_NAME, [QRecord, #{}]),
            ok = rabbit_control_helper:command(start_app, Server3),
            ok = rabbit_control_helper:command(start_app, Server2),
            ?assertEqual(<<"stream">>, proplists:get_value(type, Fmt2)),
            ?assertEqual(minority, proplists:get_value(state, Fmt2)),
            ?assertEqual(Server, proplists:get_value(leader, Fmt2)),
            ?assertEqual(Server, proplists:get_value(node, Fmt2)),
            ?assertEqual([Server], proplists:get_value(online, Fmt2)),
            ?assertEqual(Nodes, proplists:get_value(members, Fmt2)),
            ok;
        1 ->
            ok
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]),
    ok.


consume_from_last(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),

    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, [committed_offset]),
              %% We'll receive data from the last committed offset, let's check that is not the
              %% first offset
              proplists:get_value(committed_offset, Info) > 0
      end),

    CommittedOffset = proplists:get_value(committed_offset,
                                          find_queue_info(Config, [committed_offset])),

    %% If the offset is not provided, we're subscribing to the tail of the stream
    amqp_channel:subscribe(
      Ch1, #'basic.consume'{queue = Q,
                            no_ack = false,
                            consumer_tag = <<"ctag">>,
                            arguments = [{<<"x-stream-offset">>, longstr, <<"last">>}]},
      self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end,

    %% Check that the first received offset is greater than or equal than the committed
    %% offset. It could have moved since we checked it out - it flakes sometimes!
    %% Usually when the CommittedOffset detected is 1
    receive_batch_min_offset(Ch1, CommittedOffset, 99),

    %% Publish a few more
    [publish(Ch, Q, <<"msg2">>) || _ <- lists:seq(1, 100)],
    amqp_channel:wait_for_confirms(Ch, 5),

    %% Yeah! we got them
    receive_batch(Ch1, 100, 199),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_from_next(Config) ->
    consume_from_next(Config, [{<<"x-stream-offset">>, longstr, <<"next">>}]).

consume_from_default(Config) ->
    consume_from_next(Config, []).

consume_from_next(Config, Args) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),


    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, [committed_offset]),
              %% We'll receive data from the last committed offset, let's check that is not the
              %% first offset
              proplists:get_value(committed_offset, Info) > 0
      end),

    %% If the offset is not provided, we're subscribing to the tail of the stream
    amqp_channel:subscribe(
      Ch1, #'basic.consume'{queue = Q,
                            no_ack = false,
                            consumer_tag = <<"ctag">>,
                            arguments = Args},
      self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    after ?TIMEOUT ->
            exit(consume_ok_failed)
    end,

    %% Publish a few more
    [publish(Ch, Q, <<"msg2">>) || _ <- lists:seq(1, 100)],
    amqp_channel:wait_for_confirms(Ch, 5),

    %% Yeah! we got them
    receive_batch(Ch1, 100, 199),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_from_relative_time_offset(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),
    amqp_channel:subscribe(
      Ch1, #'basic.consume'{queue = Q,
                            no_ack = false,
                            consumer_tag = <<"ctag">>,
                            arguments = [{<<"x-stream-offset">>, longstr, <<"100s">>}]},
      self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end,

    receive_batch(Ch1, 0, 99),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_from_replica(Config) ->
    [Server1, _, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch1, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),

    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, 1, [online]),
              length(proplists:get_value(online, Info)) == 3
      end),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server3),
    qos(Ch2, 10, false),

    ok = queue_utils:wait_for_local_stream_member(Server3, <<"/">>, Q, Config),
    subscribe(Ch2, Q, false, 0),
    receive_batch(Ch2, 0, 99),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_while_deleting_replica(Config) ->
    [Server1, _, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, 1, [online]),
              length(proplists:get_value(online, Info)) == 3
      end),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server3),
    qos(Ch2, 10, false),

    CTag = atom_to_binary(?FUNCTION_NAME),
    subscribe(Ch2, Q, false, 0, CTag),

    %% Delete replica in node 3
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_stream_queue,
                                 delete_replica, [<<"/">>, Q, Server3]),

    publish_confirm(Ch1, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),

    %% no messages should be received
    receive
        #'basic.cancel'{consumer_tag = CTag} ->
            ok;
        {_, #amqp_msg{}} ->
            exit(unexpected_message)
    after ?TIMEOUT ->
            exit(missing_consumer_cancel)
    end,

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_credit(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    %% Let's publish a big batch, to ensure we have more than a chunk available
    NumMsgs = 100,
    publish_confirm(Ch, Q, [<<"msg1">> || _ <- lists:seq(1, NumMsgs)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),

    %% Let's subscribe with a small credit, easier to test
    Credit = 2,
    qos(Ch1, Credit, false),
    subscribe(Ch1, Q, false, 0),

    %% We expect to receive exactly 2 messages.
    DTag1 = receive {#'basic.deliver'{delivery_tag = Tag1}, _} -> Tag1
            after ?TIMEOUT -> ct:fail({missing_delivery, ?LINE})
            end,
    _DTag2 = receive {#'basic.deliver'{delivery_tag = Tag2}, _} -> Tag2
             after ?TIMEOUT -> ct:fail({missing_delivery, ?LINE})
             end,
    receive {#'basic.deliver'{}, _} -> ct:fail({unexpected_delivery, ?LINE})
    after 100 -> ok
    end,

    %% When we ack the 1st message, we should receive exactly 1 more message
    ok = amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = DTag1,
                                             multiple = false}),
    DTag3 = receive {#'basic.deliver'{delivery_tag = Tag3}, _} -> Tag3
            after ?TIMEOUT -> ct:fail({missing_delivery, ?LINE})
            end,
    receive {#'basic.deliver'{}, _} ->
                ct:fail({unexpected_delivery, ?LINE})
    after 100 -> ok
    end,

    %% Whenever we ack 2 messages, we should receive exactly 2 more messages.
    ok = consume_credit0(Ch1, DTag3),

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_credit0(_Ch, DTag)
  when DTag > 50 ->
    %% sufficiently tested
    ok;
consume_credit0(Ch, DTagPrev) ->
    %% Ack 2 messages.
    ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTagPrev,
                                            multiple = true}),
    %% Receive 1st message.
    receive {#'basic.deliver'{}, _} -> ok
    after ?TIMEOUT -> ct:fail({missing_delivery, ?LINE})
    end,
    %% Receive 2nd message.
    DTag = receive {#'basic.deliver'{delivery_tag = T}, _} -> T
           after ?TIMEOUT -> ct:fail({missing_delivery, ?LINE})
           end,
    %% We shouldn't receive more messages given that AMQP 0.9.1 prefetch count is 2.
    receive {#'basic.deliver'{}, _} -> ct:fail({unexpected_delivery, ?LINE})
    after 10 -> ok
    end,
    consume_credit0(Ch, DTag).

consume_credit_out_of_order_ack(Config) ->
    %% Like consume_credit but acknowledging the messages out of order.
    %% We want to ensure it doesn't behave like multiple, that is if we have
    %% credit 2 and received 10 messages, sending the ack for the message id
    %% number 10 should only increase credit by 1.
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    NumMsgs = 100,
    %% Let's publish a big batch, to ensure we have more than a chunk available
    publish_confirm(Ch, Q, [<<"msg1">> || _ <- lists:seq(1, NumMsgs)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),

    %% Let's subscribe with a small credit, easier to test
    Credit = 2,
    qos(Ch1, Credit, false),
    subscribe(Ch1, Q, false, 0),

    %% ******* This is the difference with consume_credit
    %% Receive everything, let's reverse the delivery tags here so we ack out of order
    DeliveryTags = lists:reverse(receive_batch()),

    %% We receive at least the given credit as we know there are 100 messages in the queue
    ?assert(length(DeliveryTags) >= Credit),

    %% Let's ack as many messages as we can while avoiding a positive credit for new deliveries
    {ToAck, Pending} = lists:split(length(DeliveryTags) - Credit, DeliveryTags),

    [ok = amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = DeliveryTag,
                                              multiple     = false})
     || DeliveryTag <- ToAck],

    %% Nothing here, this is good
    receive
        {#'basic.deliver'{}, _} ->
            exit(unexpected_delivery)
    after 1000 ->
            ok
    end,

    %% Let's ack one more, we should receive a new chunk
    ok = amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = hd(Pending),
                                             multiple     = false}),

    %% Yeah, here is the new chunk!
    receive
        {#'basic.deliver'{}, _} ->
            ok
    after ?TIMEOUT ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_credit_multiple_ack(Config) ->
    %% Like consume_credit but acknowledging the messages out of order.
    %% We want to ensure it doesn't behave like multiple, that is if we have
    %% credit 2 and received 10 messages, sending the ack for the message id
    %% number 10 should only increase credit by 1.
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    %% Let's publish a big batch, to ensure we have more than a chunk available
    NumMsgs = 100,
    publish_confirm(Ch, Q, [<<"msg1">> || _ <- lists:seq(1, NumMsgs)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),

    %% Let's subscribe with a small credit, easier to test
    Credit = 2,
    qos(Ch1, Credit, false),
    subscribe(Ch1, Q, false, 0),

    %% ******* This is the difference with consume_credit
    %% Receive everything, let's reverse the delivery tags here so we ack out of order
    DeliveryTag = lists:last(receive_batch()),

    ok = amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = DeliveryTag,
                                             multiple     = true}),

    %% Yeah, here is the new chunk!
    receive
        {#'basic.deliver'{}, _} ->
            ok
    after ?TIMEOUT ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

max_length_bytes(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                             {<<"x-max-length-bytes">>, long, 10000},
                                             {<<"x-stream-max-segment-size-bytes">>, long, 1000}])),

    Payload = << <<"1">> || _ <- lists:seq(1, 100) >>,

    %% 100 bytes/msg * 500 = 50000 bytes
    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 100)]),
    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 100)]),
    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 100)]),
    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 100)]),
    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 100)]),
    ensure_retention_applied(Config, Server),

    %% We don't yet have reliable metrics, as the committed offset doesn't work
    %% as a counter once we start applying retention policies.
    %% Let's wait for messages and hope these are less than the number of published ones
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 100, false),
    subscribe(Ch1, Q, false, 0),

    %% There should be ~100 messages in ~10 segments, but less check that the retention
    %% cleared just a big bunch
    ?assert(length(receive_batch()) < 200),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

max_segment_size_bytes_validation(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                             {<<"x-stream-max-segment-size-bytes">>, long, 10_000_000}])),

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                   {<<"x-stream-max-segment-size-bytes">>, long, ?MAX_STREAM_MAX_SEGMENT_SIZE + 1_000}])),

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).


max_age(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                             {<<"x-max-age">>, longstr, <<"5s">>},
                                             {<<"x-stream-max-segment-size-bytes">>, long, 250}])),

    Payload = << <<"1">> || _ <- lists:seq(1, 500) >>,

    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 100)]),

    %% there is no way around this sleep, we need to wait for retention period
    %% to pass
    timer:sleep(5000),

    %% Let's publish again so the new segments will trigger the retention policy
    [publish(Ch, Q, Payload) || _ <- lists:seq(1, 100)],
    amqp_channel:wait_for_confirms(Ch, 5),

    %% Let's give it some margin if some messages fall between segments
    queue_utils:wait_for_min_messages(Config, Q, 100),
    queue_utils:wait_for_max_messages(Config, Q, 150),

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

replica_recovery(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [Server1 | _] = lists:reverse(Nodes),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    publish_confirm(Ch1, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),
    amqp_channel:close(Ch1),

    CheckReplicaRecovered =
        fun(DownNode) ->
                rabbit_ct_helpers:await_condition(
                  fun () ->
                          ct:pal("Wait for replica to recover..."),
                          try
                              {Conn, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(Config, DownNode),
                              qos(Ch2, 10, false),
                              subscribe(Ch2, Q, false, 0),
                              receive_batch(Ch2, 0, 99),
                              amqp_connection:close(Conn),
                              true
                          catch _:_ ->
                              false
                          end
                  end, 120_000)
        end,

    Perms = permute(Nodes),
    AppPerm = lists:nth(rand:uniform(length(Perms)), Perms),
    [begin
         rabbit_control_helper:command(stop_app, DownNode),
         rabbit_control_helper:command(start_app, DownNode),
         CheckReplicaRecovered(DownNode)
     end || [DownNode | _] <- AppPerm],

    NodePerm = lists:nth(rand:uniform(length(Perms)), Perms),
    [begin
         ok = rabbit_ct_broker_helpers:stop_node(Config, DownNode),
         ok = rabbit_ct_broker_helpers:start_node(Config, DownNode),
         CheckReplicaRecovered(DownNode)
     end || DownNode <- NodePerm],
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

leader_failover(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    check_leader_and_replicas(Config, [Server1, Server2, Server3]),
    publish_confirm(Ch1, Q, [<<"msg">> || _ <- lists:seq(1, 100)]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),

    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, 1, [leader, members]),

              NewLeader = proplists:get_value(leader, Info),
              NewLeader =/= Server1
      end, 45000),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

leader_failover_dedupe(Config) ->
    %% tests that in-flight messages are automatically handled in the case where
    %% a leader change happens during publishing
    PermNodes = permute(
                  rabbit_ct_broker_helpers:get_node_configs(Config, nodename)),
    %% pick a random node order for this test
    %% realle we should run all permuations
    Nodes = lists:nth(rand:uniform(length(PermNodes)), PermNodes),
    ct:pal("~ts running with nodes ~w", [?FUNCTION_NAME, Nodes]),
    [_Server1, DownNode, PubNode] = Nodes,
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, DownNode, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    check_leader_and_replicas(Config, Nodes),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, PubNode),
    #'confirm.select_ok'{} = amqp_channel:call(Ch2, #'confirm.select'{}),

    Self= self(),
    F = fun F(N) ->
                receive
                    go ->
                        [publish(Ch2, Q, integer_to_binary(N + I))
                         || I <- lists:seq(1, 100)],
                        true = amqp_channel:wait_for_confirms(Ch2, 25),
                        F(N + 100);
                    stop ->
                        Self ! {last_msg, N},
                        ct:pal("stop"),
                        ok
                after 2 ->
                          self() ! go,
                          F(N)
                end
        end,
    Pid = spawn(fun () ->
                        amqp_channel:register_confirm_handler(Ch2, self()),
                        F(0)
                end),
    erlang:monitor(process, Pid),
    Pid ! go,
    timer:sleep(10),
    ok = rabbit_ct_broker_helpers:stop_node(Config, DownNode),
    %% this should cause a new leader to be elected and the channel on node 2
    %% to have to resend any pending messages to ensure none is lost
    rabbit_ct_helpers:await_condition(
      fun() ->
              Info = find_queue_info(Config, PubNode, [leader, members]),
              NewLeader = proplists:get_value(leader, Info),
              NewLeader =/= DownNode
      end),
    flush(),
    ?assert(erlang:is_process_alive(Pid)),
    ct:pal("stopping"),
    Pid ! stop,
    ok = rabbit_ct_broker_helpers:start_node(Config, DownNode),

    N = receive
            {last_msg, X} -> X
        after ?TIMEOUT ->
                  exit(last_msg_timeout)
        end,
    %% validate that no duplicates were written even though an internal
    %% resend might have taken place
    qos(Ch2, 100, false),
    subscribe(Ch2, Q, false, 0),
    validate_dedupe(Ch2, 1, N),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

initial_cluster_size_one(Config) ->
    [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-initial-cluster-size">>, long, 1}])),
    check_leader_and_replicas(Config, [Server1]),

    ?assertMatch(#'queue.delete_ok'{},
                 delete(Config, Server1, Q)),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

initial_cluster_size_two(Config) ->
    [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-initial-cluster-size">>, long, 2}])),

    Info = find_queue_info(Config, [leader, members]),

    ?assertEqual(Server1, proplists:get_value(leader, Info)),
    ?assertEqual(2, length(proplists:get_value(members, Info))),

    ?assertMatch(#'queue.delete_ok'{},
                 delete(Config, Server1, Q)),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

initial_cluster_size_one_policy(Config) ->
    [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    PolicyName = atom_to_binary(?FUNCTION_NAME),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, <<"initial_cluster_size_one_policy">>,
           <<"queues">>,
           [{<<"initial-cluster-size">>, 1}]),

    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-initial-cluster-size">>, long, 1}])),
    check_leader_and_replicas(Config, [Server1]),

    ?assertMatch(#'queue.delete_ok'{},
                 delete(Config, Server1, Q)),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

declare_delete_same_stream(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    [begin
         ?assertEqual({'queue.declare_ok', Q, 0, 0},
                      declare(Config, S, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
         ?assertMatch(#'queue.delete_ok'{},
                      delete(Config, S, Q))
     end || _ <- lists:seq(1, 20), S <- Servers],

    ok.

leader_locator_client_local(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),

    await_condition(
      fun () ->
              Server1 == proplists:get_value(leader, find_queue_info(Config, [leader]))
      end, 60),

    ?assertMatch(#'queue.delete_ok'{},
                 delete(Config, Server1, Q)),

    Q2 = <<Q/binary, "-2">>,
    %% Try second node
    ?assertEqual({'queue.declare_ok', Q2, 0, 0},
                 declare(Config, Server2, Q2, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),

    %% the amqqueue:pid field is updated async for khepri
    %% so we need to await the condition here
    await_condition(
      fun () ->
              Server2 == proplists:get_value(leader,
                                             find_queue_info(Q2, Config, 0, [leader]))
      end, 60),

    ?assertMatch(#'queue.delete_ok'{}, delete(Config, Server2, Q2)),


    Q3 = <<Q/binary, "-3">>,
    %% Try third node
    ?assertEqual({'queue.declare_ok', Q3, 0, 0},
                 declare(Config, Server3, Q3, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),

    await_condition(
      fun () ->
              Server3 == proplists:get_value(leader,
                                             find_queue_info(Q3, Config, 0, [leader]))
      end, 60),

    ?assertMatch(#'queue.delete_ok'{}, delete(Config, Server3, Q3)),
    ok.

leader_locator_balanced(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    Bin = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    Q1 = <<Bin/binary, "_q1">>,

    ?assertEqual({'queue.declare_ok', Q1, 0, 0},
                 declare(Config, Server1, Q1, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-queue-leader-locator">>, longstr, <<"balanced">>}])),

    Info = find_queue_info(Config, [leader]),
    Leader = proplists:get_value(leader, Info),

    ?assert(lists:member(Leader, [Server2, Server3])),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, [[Q1, Q]]).

leader_locator_balanced_maintenance(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    ?assertEqual({'queue.declare_ok', Q1, 0, 0},
                 declare(Config, Server1, Q1, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
    ?assertEqual({'queue.declare_ok', Q2, 0, 0},
                 declare(Config, Server2, Q2, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
    true = rabbit_ct_broker_helpers:mark_as_being_drained(Config, Server3),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                              {<<"x-queue-leader-locator">>, longstr, <<"balanced">>}])),

    await_condition(
      fun() ->
              Info = find_queue_info(Config, [leader]),
              Leader = proplists:get_value(leader, Info),
              lists:member(Leader, [Server1, Server2])
      end, 60000),

    true = rabbit_ct_broker_helpers:unmark_as_being_drained(Config, Server3),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, [[Q1, Q]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

select_nodes_with_least_replicas(Config) ->
    [Server1 | _ ] = Servers0 = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Servers = lists:usort(Servers0),
    Q = ?config(queue_name, Config),
    Bin = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    Q1 = <<Bin/binary, "_q1">>,
    Qs = [Q1, Q],

    [Q1Members, QMembers] =
    lists:map(fun(Q0) ->
                      ?assertEqual({'queue.declare_ok', Q0, 0, 0},
                                   declare(Config, Server1, Q0, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                                                 {<<"x-initial-cluster-size">>, long, 2}])),
                      Infos = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, info_all,
                                                           [<<"/">>, [name, members]]),
                      Name = rabbit_misc:r(<<"/">>, queue, Q0),
                      [Info] = [Props || Props <- Infos, lists:member({name, Name}, Props)],
                      proplists:get_value(members, Info)
              end, Qs),

    %% We expect that the second stream chose nodes where the first stream does not have replicas.
    ?awaitMatch(Servers,
                lists:usort(Q1Members ++ QMembers),
                30000),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, [Qs]).

leader_locator_policy(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    Bin = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    Q1 = <<Bin/binary, "_q1">>,

    PolicyName = atom_to_binary(?FUNCTION_NAME),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, Q, <<"queues">>,
           [{<<"queue-leader-locator">>, <<"balanced">>}]),

    ?assertEqual({'queue.declare_ok', Q1, 0, 0},
                 declare(Config, Server1, Q1, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                  {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Info = find_queue_info(Config, [policy, operator_policy, effective_policy_definition, leader]),
    ?assertEqual(PolicyName, proplists:get_value(policy, Info)),
    ?assertEqual('', proplists:get_value(operator_policy, Info)),
    ?assertEqual([{<<"queue-leader-locator">>, <<"balanced">>}],
                 proplists:get_value(effective_policy_definition, Info)),
    Leader = proplists:get_value(leader, Info),
    ?assert(lists:member(Leader, [Server2, Server3])),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, [[Q1, Q]]).

queue_size_on_declare(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    publish_confirm(Ch1, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),

    %% Metrics update is not synchronous, wait until metrics are updated on the leader node.
    %% Afterwards, all replicas will get the right size as they have to query the writer node
    ?awaitMatch({'queue.declare_ok', Q, 100, 0},
                declare(Config, Server1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
                60000),
    amqp_channel:close(Ch1),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    ?assertEqual({'queue.declare_ok', Q, 100, 0},
                 declare(Config, Server2, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    amqp_channel:close(Ch2),

    Ch3 = rabbit_ct_client_helpers:open_channel(Config, Server3),
    ?assertEqual({'queue.declare_ok', Q, 100, 0},
                 declare(Config, Server3, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    amqp_channel:close(Ch3),

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

repeat_until(_, 0) ->
    ct:fail("Condition did not materialize in the expected amount of attempts");
repeat_until(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> repeat_until(Fun, N - 1)
    end.

invalid_policy(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"ttl">>, <<"invalid_policy.*">>, <<"queues">>,
           [{<<"message-ttl">>, 5}]),

    Info = find_queue_info(Config, [policy, operator_policy, effective_policy_definition]),

    ?assertEqual('', proplists:get_value(policy, Info)),
    ?assertEqual('', proplists:get_value(operator_policy, Info)),
    ?assertEqual([], proplists:get_value(effective_policy_definition, Info)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl">>),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

max_age_policy(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    PolicyName = atom_to_binary(?FUNCTION_NAME),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, <<"max_age_policy.*">>, <<"queues">>,
           [{<<"max-age">>, <<"1Y">>}]),

    %% Policies are asynchronous, must wait until it has been applied everywhere
    ensure_retention_applied(Config, Server),
    ?awaitMatch(
       {PolicyName, '', [{<<"max-age">>, <<"1Y">>}]},
       begin
           Info = find_queue_info(Config, [policy, operator_policy, effective_policy_definition]),
           {proplists:get_value(policy, Info), proplists:get_value(operator_policy, Info), proplists:get_value(effective_policy_definition, Info)}
       end,
       30000),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

update_retention_policy(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    PolicyName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                             {<<"x-stream-max-segment-size-bytes">>, long, 200},
                                             {<<"x-stream-filter-size-bytes">>, long, 32}
                                            ])),
    check_leader_and_replicas(Config, Servers),

    Msgs = [<<"msg">> || _ <- lists:seq(1, 10000)], %% 3 bytes * 10000 = 30000 bytes
    publish_confirm(Ch, Q, Msgs),

    {ok, Q0} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup,
                                            [rabbit_misc:r(<<"/">>, queue, Q)]),
    %% Don't use time based retention, it's really hard to get those tests right
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, <<"update_retention_policy.*">>, <<"queues">>,
           [{<<"max-length-bytes">>, 10000}]),
    ensure_retention_applied(Config, Server),

    %% Retention policy should clear approximately 2/3 of the messages, but just to be safe
    %% let's simply check that it removed half of them
    queue_utils:wait_for_max_messages(Config, Q, 5000),

    {ok, Q1} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup,
                                            [rabbit_misc:r(<<"/">>, queue, Q)]),

    %% If there are changes only in the retention policy, processes should not be restarted
    ?assertEqual(amqqueue:get_pid(Q0), amqqueue:get_pid(Q1)),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

queue_info(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    rabbit_ct_helpers:await_condition(
      fun() ->
              Info = find_queue_info(Config, [leader, online, members]),
              lists:member(proplists:get_value(leader, Info), Servers) andalso
                  (lists:sort(Servers) == lists:sort(proplists:get_value(members, Info))) andalso
                  (lists:sort(Servers) == lists:sort(proplists:get_value(online, Info)))
      end),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

max_segment_size_bytes_policy_validation(Config) ->
    PolicyName = atom_to_binary(?FUNCTION_NAME),
    Pattern = <<PolicyName/binary, ".*">>,
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, Pattern, <<"queues">>,
           [{<<"stream-max-segment-size-bytes">>, ?MAX_STREAM_MAX_SEGMENT_SIZE - 1_000}]),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName),

    {error_string, _} = rabbit_ct_broker_helpers:rpc(
                          Config, 0,
                          rabbit_policy, set,
                          [<<"/">>,
                            PolicyName,
                            Pattern,
                           [{<<"stream-max-segment-size-bytes">>,
                             ?MAX_STREAM_MAX_SEGMENT_SIZE + 1_000}],
                           0,
                           <<"queues">>,
                           <<"acting-user">>]),
    ok.

max_segment_size_bytes_policy(Config) ->
    %% updating a policy for the segment size does not force a stream restart +
    %% config update but will pick it up the next time a stream is restarted.
    %% This is a limitation that we may want to address at some
    %% point but for now we need to set the policy _before_ creating the stream.
    PolicyName = atom_to_binary(?FUNCTION_NAME),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, <<"max_segment_size_bytes.*">>, <<"queues">>,
           [{<<"stream-max-segment-size-bytes">>, 5000}]),

    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    Info = find_queue_info(Config, [policy, operator_policy, effective_policy_definition]),

    ?assertEqual(PolicyName, proplists:get_value(policy, Info)),
    ?assertEqual('', proplists:get_value(operator_policy, Info)),
    ?assertEqual([{<<"stream-max-segment-size-bytes">>, 5000}],
                 proplists:get_value(effective_policy_definition, Info)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

purge(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
                amqp_channel:call(Ch, #'queue.purge'{queue = Q})),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

dead_letter_target(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    SourceQ = <<Q/binary, "_source">>,
    ?assertEqual({'queue.declare_ok', SourceQ, 0, 0},
                 declare(Config, Server, SourceQ, [{<<"x-queue-type">>, longstr, <<"classic">>},
                                                   {<<"x-dead-letter-exchange">>, longstr, <<>>},
                                                   {<<"x-dead-letter-routing-key">>, longstr, Q}
                                                  ])),

    publish_confirm(Ch, SourceQ, [<<"msg">>]),
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 1, false),
    CTag = <<"ctag">>,
    amqp_channel:subscribe(Ch1,
                           #'basic.consume'{queue = SourceQ,
                                            no_ack = false,
                                            consumer_tag = CTag},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
             ok
    after ?TIMEOUT ->
              exit(basic_consume_ok_timeout)
    end,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok = amqp_channel:cast(Ch1, #'basic.nack'{delivery_tag = DeliveryTag,
                                                      requeue =false,
                                                      multiple     = false}),
            queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]])
    after ?TIMEOUT ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

filter_spec(_) ->
    [begin
         FilterSpec = rabbit_stream_queue:filter_spec(Args),
         ?assert(maps:is_key(filter_spec, FilterSpec)),
         #{filter_spec := #{filters := Filters, match_unfiltered := MatchUnfiltered}} = FilterSpec,
         ?assertEqual(lists:sort(ExpectedFilters), lists:sort(Filters)),
         ?assertEqual(ExpectedMatchUnfiltered, MatchUnfiltered)
     end || {Args, ExpectedFilters, ExpectedMatchUnfiltered} <-
            [{[{<<"x-stream-filter">>,array,[{longstr,<<"apple">>},{longstr,<<"banana">>}]}],
              [<<"apple">>, <<"banana">>], false},
             {[{<<"x-stream-filter">>,longstr,<<"apple">>}],
              [<<"apple">>], false},
             {[{<<"x-stream-filter">>,longstr,<<"apple">>}, {<<"sac">>,bool,true}],
              [<<"apple">>], false},
             {[{<<"x-stream-filter">>,longstr,<<"apple">>},{<<"x-stream-match-unfiltered">>,bool,true}],
              [<<"apple">>], true}
            ]],
    ?assertEqual(#{}, rabbit_stream_queue:filter_spec([{<<"foo">>,longstr,<<"bar">>}])),
    ?assertEqual(#{}, rabbit_stream_queue:filter_spec([])),
    ok.

filtering(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ChPublish = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Config, Server, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    #'confirm.select_ok'{} = amqp_channel:call(ChPublish, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(ChPublish, self()),
    Publish = fun(FilterValue) ->
                      lists:foreach(fun(_) ->
                                            Headers = [{<<"x-stream-filter-value">>, longstr, FilterValue}],
                                            Msg = #amqp_msg{props = #'P_basic'{delivery_mode = 2,
                                                                               headers = Headers},
                                                            payload = <<"foo">>},
                                            ok = amqp_channel:cast(ChPublish,
                                                                   #'basic.publish'{routing_key = Q},
                                                                   Msg)
                                    end,lists:seq(0, 100))
              end,
    Publish(<<"apple">>),
    amqp_channel:wait_for_confirms(ChPublish, 5),
    Publish(<<"banana">>),
    amqp_channel:wait_for_confirms(ChPublish, 5),
    Publish(<<"apple">>),
    amqp_channel:wait_for_confirms(ChPublish, 5),
    amqp_channel:close(ChPublish),
    ChConsume = rabbit_ct_client_helpers:open_channel(Config, Server),
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(ChConsume, #'basic.qos'{global = false,
                                                           prefetch_count = 10})),

    CTag = <<"ctag">>,
    amqp_channel:subscribe(ChConsume, #'basic.consume'{queue = Q,
                                                       no_ack = false,
                                                       consumer_tag = CTag,
                                                       arguments = [{<<"x-stream-offset">>, long, 0},
                                                                    {<<"x-stream-filter">>, longstr, <<"banana">>}]},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
            ok
    end,

    receive_filtered_batch(ChConsume, 0, 100),
    amqp_channel:close(ChConsume),

    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).
%%----------------------------------------------------------------------------

receive_filtered_batch(_, Count, ExpectedSize) when Count =:= ExpectedSize ->
    Count;
receive_filtered_batch(Ch, Count, ExpectedSize) ->
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, #amqp_msg{}} ->
            ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                                    multiple     = false}),
            receive_filtered_batch(Ch, Count + 1, ExpectedSize)
    after ?TIMEOUT ->
              flush(),
              exit({not_enough_messages, Count})
    end.

delete_queues(Qs) when is_list(Qs) ->
    lists:foreach(fun delete_testcase_queue/1, Qs).

delete_testcase_queue(Name) ->
    QName = rabbit_misc:r(<<"/">>, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>);
        _ ->
            ok
    end.

declare(Config, Server, Q) ->
    declare(Config, Server, Q, []).

declare(Config, Server, Q, Args) ->
    retry_if_coordinator_unavailable(Config, Server, #'queue.declare'{queue     = Q,
                                                                      durable   = true,
                                                                      auto_delete = false,
                                                                      arguments = Args}).

delete(Config, Server, Q) ->
    retry_if_coordinator_unavailable(Config, Server, #'queue.delete'{queue = Q}).

retry_if_coordinator_unavailable(Config, Server, Cmd) ->
    Props = ?config(tc_group_properties, Config),
    %% Running parallel tests the coordinator could be busy answering other
    %% queries, on the assumption CI servers are slow, so let's allow a few
    %% attempts.
    Retries = case lists:member(parallel, Props) of
                  true -> 3;
                  false -> 1
              end,
    retry_if_coordinator_unavailable(Config, Server, Cmd, Retries).

retry_if_coordinator_unavailable(_, _, _, 0) ->
    exit(coordinator_unavailable);
retry_if_coordinator_unavailable(Config, Server, Cmd, Retry) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    try
        Reply = amqp_channel:call(Ch, Cmd),
        rabbit_ct_client_helpers:close_channel(Ch),
        Reply
    catch
        exit:{{shutdown, {connection_closing, {server_initiated_close, _, Msg}}}, _} = Error ->
            case re:run(Msg, ".*coordinator_unavailable.*", [{capture, none}]) of
                match ->
                    ct:pal("Attempt to execute command ~p failed, coordinator unavailable", [Cmd]),
                    retry_if_coordinator_unavailable(Config, Server, Cmd, Retry - 1);
                _ ->
                    exit(Error)
            end
    end.

assert_queue_type(Server, Q, Expected) ->
    Actual = get_queue_type(Server, Q),
    Expected = Actual.

get_queue_type(Server, Q0) ->
    QNameRes = rabbit_misc:r(<<"/">>, queue, Q0),
    {ok, Q1} = rpc:call(Server, rabbit_amqqueue, lookup, [QNameRes]),
    amqqueue:get_type(Q1).

check_leader_and_replicas(Config, Members) ->
    check_leader_and_replicas(Config, Members, online).

check_leader_and_replicas(Config, Members, Tag) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              case find_queue_info(Config, [leader, Tag]) of
                  [] ->
                      false;
                  Info ->
                      ct:pal("~ts members ~w ~tp", [?FUNCTION_NAME, Members, Info]),
                      lists:member(proplists:get_value(leader, Info), Members)
                          andalso (lists:sort(Members) ==
                                   lists:sort(proplists:get_value(Tag, Info)))
              end
      end, 60_000).

check_members(Config, ExpectedMembers) ->
    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, 0, [members]),
              Members = proplists:get_value(members, Info),
              ct:pal("~ts members ~w ~tp", [?FUNCTION_NAME, Members, Info]),
              lists:sort(ExpectedMembers) == lists:sort(Members)
      end, 20_000).

publish(Ch, Queue) ->
    publish(Ch, Queue, <<"msg">>).

publish(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).

publish_confirm(Ch, Q, Msgs) ->
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    [publish(Ch, Q, Msg) || Msg <- Msgs],
    amqp_channel:wait_for_confirms(Ch, 5).

subscribe(Ch, Queue, NoAck, Offset) ->
    subscribe(Ch, Queue, NoAck, Offset, <<"ctag">>).

subscribe(Ch, Queue, NoAck, Offset, CTag) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                consumer_tag = CTag,
                                                arguments = [{<<"x-stream-offset">>, long, Offset}]},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
             ok
    end.

qos(Ch, Prefetch, Global) ->
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = Global,
                                                    prefetch_count = Prefetch})).

validate_dedupe(Ch, N, N) ->
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag},
         #amqp_msg{payload = B}} ->
            I = binary_to_integer(B),
            ?assertEqual(N, I),
            ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                                    multiple     = false})
    after 60000 ->
              flush(),
              exit({missing_record, N})
    end;
validate_dedupe(Ch, N, M) ->
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag},
         #amqp_msg{payload = B}} ->
            I = binary_to_integer(B),
            ?assertEqual(N, I),
            ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                                    multiple     = false}),
            validate_dedupe(Ch, N + 1, M)
    after 60000 ->
              flush(),
              exit({missing_record, N})
    end.

receive_batch_min_offset(Ch, N, M) ->
    %% We are expecting values from the last committed offset - which might have increased
    %% since we queried it. Accept as first offset anything greater than the last known
    %% committed offset
    receive
        {_,
         #amqp_msg{props = #'P_basic'{headers = [{<<"x-stream-offset">>, long, S}]}}}
          when S < N ->
            exit({unexpected_offset, S});
        {#'basic.deliver'{delivery_tag = DeliveryTag},
         #amqp_msg{props = #'P_basic'{headers = [{<<"x-stream-offset">>, long, S}]}}} ->
            ct:pal("Committed offset is ~tp but as first offset got ~tp", [N, S]),
            ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                                    multiple     = false}),
            receive_batch(Ch, S + 1, M)
    after 60000 ->
              flush(),
              exit({missing_offset, N})
    end.

receive_batch(_Ch, N, M) when N > M ->
    ok;
receive_batch(Ch, N, M) ->
    receive
        {_,
         #amqp_msg{props = #'P_basic'{headers = [{<<"x-stream-offset">>, long, S}]}}}
          when S < N ->
            exit({unexpected_offset, S});
        {#'basic.deliver'{delivery_tag = DeliveryTag},
         #amqp_msg{props = #'P_basic'{headers = [{<<"x-stream-offset">>, long, N}]}}} ->
            ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                                    multiple     = false}),
            receive_batch(Ch, N + 1, M)
    after 60000 ->
              flush(),
              exit({missing_offset, N})
    end.

receive_batch() ->
    receive_batch([]).

receive_batch(Acc) ->
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            receive_batch([DeliveryTag | Acc])
    after 5000 ->
            lists:reverse(Acc)
    end.

run_proper(Fun, Args, NumTests) ->
    ?assertEqual(
       true,
       proper:counterexample(
         erlang:apply(Fun, Args),
         [{numtests, NumTests},
          {on_output, fun(".", _) -> ok; % don't print the '.'s on new lines
                         (F, A) -> ct:pal(?LOW_IMPORTANCE, F, A)
                      end}])).

flush() ->
    receive
        Any ->
            ct:pal("flush ~tp", [Any]),
            flush()
    after 0 ->
              ok
    end.

permute([]) -> [[]];
permute(L)  -> [[H|T] || H <- L, T <- permute(L--[H])].

ensure_retention_applied(Config, Server) ->
    %% Retention is asynchronous, so committing all messages doesn't mean old segments have been
    %% cleared up.
    %% Let's force a call on the retention gen_server, any pending retention would have been
    %% processed when this call returns.
    rabbit_ct_broker_helpers:rpc(Config, Server, gen_server, call, [osiris_retention, test]).

rebalance(Config) ->
    [Server0 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),

    Q1 = <<"st1">>,
    Q2 = <<"st2">>,
    Q3 = <<"st3">>,
    Q4 = <<"st4">>,
    Q5 = <<"st5">>,

    ?assertEqual({'queue.declare_ok', Q1, 0, 0},
                 declare(Config, Server0, Q1, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-initial-cluster-size">>, long, 3}])),
    ?assertEqual({'queue.declare_ok', Q2, 0, 0},
                 declare(Config, Server0, Q2, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-initial-cluster-size">>, long, 3}])),
    ?assertEqual({'queue.declare_ok', Q3, 0, 0},
                 declare(Config, Server0, Q3, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-initial-cluster-size">>, long, 3}])),
    ?assertEqual({'queue.declare_ok', Q4, 0, 0},
                 declare(Config, Server0, Q4, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-initial-cluster-size">>, long, 3}])),
    ?assertEqual({'queue.declare_ok', Q5, 0, 0},
                 declare(Config, Server0, Q5, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-initial-cluster-size">>, long, 3}])),

    NumMsgs = 100,
    Data = crypto:strong_rand_bytes(100),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    [publish(Ch, Q1, Data) || _ <- lists:seq(1, NumMsgs)],
    [publish(Ch, Q2, Data) || _ <- lists:seq(1, NumMsgs)],
    [publish(Ch, Q3, Data) || _ <- lists:seq(1, NumMsgs)],
    [publish(Ch, Q4, Data) || _ <- lists:seq(1, NumMsgs)],
    [publish(Ch, Q5, Data) || _ <- lists:seq(1, NumMsgs)],

    %% Check that we have at most 2 streams per node
    ?awaitMatch(true,
                begin
                    {ok, Summary} = rpc:call(Server0, rabbit_amqqueue, rebalance, [stream, ".*", ".*"]),
                    lists:all(fun(NodeData) ->
                                      lists:all(fun({_, V}) when is_integer(V) -> V =< 2;
                                                   (_) -> true end,
                                                NodeData)
                              end, Summary)
                end, 10000),
    ok.
