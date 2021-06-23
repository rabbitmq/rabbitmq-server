%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_queue_SUITE).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

suite() ->
    [{timetrap, 15 * 60000}].

all() ->
    [
     {group, single_node},
     {group, single_node_parallel},
     {group, cluster_size_2},
     {group, cluster_size_2_parallel},
     {group, cluster_size_3},
     {group, cluster_size_3_parallel},
     {group, unclustered_size_3_1},
     {group, unclustered_size_3_2},
     {group, unclustered_size_3_3},
     {group, cluster_size_3_1}
    ].

groups() ->
    [
     {single_node, [], [restart_single_node, recover]},
     {single_node_parallel, [parallel], all_tests()},
     {cluster_size_2, [], [recover]},
     {cluster_size_2_parallel, [parallel], all_tests()},
     {cluster_size_3, [], [recover]},
     {cluster_size_3, [],
          [restart_coordinator_without_queues,
           delete_down_replica,
           replica_recovery,
           leader_failover,
           leader_failover_dedupe,
           add_replicas,
           publish_coordinator_unavailable,
           leader_locator_policy]},
     {cluster_size_3_parallel, [parallel], [delete_replica,
                                            delete_last_replica,
                                            delete_classic_replica,
                                            delete_quorum_replica,
                                            consume_from_replica,
                                            initial_cluster_size_one,
                                            initial_cluster_size_two,
                                            initial_cluster_size_one_policy,
                                            leader_locator_client_local,
                                            leader_locator_random,
                                            leader_locator_least_leaders] ++ all_tests()},
     {unclustered_size_3_1, [], [add_replica]},
     {unclustered_size_3_2, [], [consume_without_local_replica]},
     {unclustered_size_3_3, [], [grow_coordinator_cluster]},
     {cluster_size_3_1, [], [shrink_coordinator_cluster]}
    ].

all_tests() ->
    [
     declare_args,
     declare_max_age,
     declare_invalid_properties,
     declare_server_named,
     declare_queue,
     delete_queue,
     publish,
     publish_confirm,
     consume_without_qos,
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
     consume_from_default,
     consume_from_relative_time_offset,
     consume_credit,
     consume_credit_out_of_order_ack,
     consume_credit_multiple_ack,
     basic_cancel,
     max_length_bytes,
     max_age,
     invalid_policy,
     max_age_policy,
     max_segment_size_bytes_policy,
     purge,
     update_retention_policy,
     queue_info
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, [{stream_tick_interval, 1000},
                                  {log, [{file, [{level, debug}]}]}]}),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    ClusterSize = case Group of
                      single_node -> 1;
                      single_node_parallel -> 1;
                      cluster_size_2 -> 2;
                      cluster_size_2_parallel -> 2;
                      cluster_size_3 -> 3;
                      cluster_size_3_parallel -> 3;
                      cluster_size_3_1 -> 3;
                      unclustered_size_3_1 -> 3;
                      unclustered_size_3_2 -> 3;
                      unclustered_size_3_3 -> 3
                  end,
    Clustered = case Group of
                    unclustered_size_3_1 -> false;
                    unclustered_size_3_2 -> false;
                    unclustered_size_3_3 -> false;
                    _ -> true
                end,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base},
                                            {rmq_nodes_clustered, Clustered}]),
    Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
    Ret = rabbit_ct_helpers:run_steps(Config1b,
                                      [fun merge_app_env/1 ] ++
                                      rabbit_ct_broker_helpers:setup_steps()),
    case Ret of
        {skip, _} ->
            Ret;
        Config2 ->
            EnableFF = rabbit_ct_broker_helpers:enable_feature_flag(
                         Config2, stream_queue),
            case EnableFF of
                ok ->
                    ok = rabbit_ct_broker_helpers:rpc(
                           Config2, 0, application, set_env,
                           [rabbit, channel_tick_interval, 100]),
                    Config2;
                Skip ->
                    end_per_group(Group, Config2),
                    Skip
            end
    end.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1, [{queue_name, Q}]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}).

end_per_testcase(Testcase, Config) ->
    Q = ?config(queue_name, Config),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

declare_args(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-max-length">>, long, 2000}])),
    assert_queue_type(Server, Q, rabbit_stream_queue),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

declare_max_age(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server), Q,
               [{<<"x-queue-type">>, longstr, <<"stream">>},
                {<<"x-max-age">>, longstr, <<"1A">>}])),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-max-age">>, longstr, <<"1Y">>}])),
    assert_queue_type(Server, Q, rabbit_stream_queue),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

declare_invalid_properties(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Q = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = Q,
                          auto_delete = true,
                          durable   = true,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = Q,
                          exclusive = true,
                          durable   = true,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = Q,
                          durable   = false,
                          arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]})).

declare_server_named(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               <<"">>, [{<<"x-queue-type">>, longstr, <<"stream">>}])).

declare_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    %% Test declare an existing queue
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    
    ?assertMatch([_], find_queue_info(Config, [])),
    
    %% Test declare an existing queue with different arguments
    ?assertExit(_, declare(Ch, Q, [])),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

find_queue_info(Config, Keys) ->
    find_queue_info(Config, 0, Keys).

find_queue_info(Config, Node, Keys) ->
    Name = ?config(queue_name, Config),
    QName = rabbit_misc:r(<<"/">>, queue, Name),
    Infos = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_amqqueue, info_all,
                                             [<<"/">>, [name] ++ Keys]),
    [Info] = [Props || Props <- Infos, lists:member({name, QName}, Props)],
    Info.

delete_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})).

add_replicas(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-initial-cluster-size">>, long, 1}])),

    %% TODO: add lots of data so that replica is still out of sync when
    %% second request comes in
    NumMsgs = 1000,
    Data = crypto:strong_rand_bytes(1000),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    [publish(Ch, Q, Data) || _ <- lists:seq(1, NumMsgs)],
    %% should be sufficient for the next message to fall in the next
    %% chunk
    timer:sleep(100),
    publish(Ch, Q, <<"last">>),
    amqp_channel:wait_for_confirms(Ch, 30),
    timer:sleep(1000),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])),

    timer:sleep(1000),

    %% it is almost impossible to reliably catch this situation.
    %% increasing number of messages published and the data size could help
    % ?assertMatch({error, {disallowed, out_of_sync_replica}} ,
    ?assertMatch(ok ,
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server2])),
    timer:sleep(1000),
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
                           amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
              exit(deliver_timeout)
    end,
    % ?assertMatch({error, {disallowed, out_of_sync_replica}} ,
    %              rpc:call(Server0, rabbit_stream_queue, add_replica,
    %                       [<<"/">>, Q, Server2])),
    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

add_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),

    %% Let's also try the add replica command on other queue types, it should fail
    %% We're doing it in the same test for efficiency, otherwise we have to
    %% start new rabbitmq clusters every time for a minor testcase
    QClassic = <<Q/binary, "_classic">>,
    QQuorum = <<Q/binary, "_quorum">>,

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ?assertEqual({'queue.declare_ok', QClassic, 0, 0},
                 declare(Ch, QClassic, [{<<"x-queue-type">>, longstr, <<"classic">>}])),
    ?assertEqual({'queue.declare_ok', QQuorum, 0, 0},
                 declare(Ch, QQuorum, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, node_not_running},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, QClassic, Server1])),
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, QQuorum, Server1])),

    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),
    timer:sleep(1000),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, QClassic, Server1])),
    ?assertEqual({error, quorum_queue_not_supported},
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, QQuorum, Server1])),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, add_replica,
                          [<<"/">>, Q, Server1])),
    %% replicas must be recorded on the state, and if we publish messages then they must
    %% be stored on disk
    check_leader_and_replicas(Config, [Server0, Server1]),
    %% And if we try again? Idempotent
    ?assertEqual(ok, rpc:call(Server0, rabbit_stream_queue, add_replica,
                              [<<"/">>, Q, Server1])),
    %% Add another node
    ok = rabbit_control_helper:command(stop_app, Server2),
    ok = rabbit_control_helper:command(join_cluster, Server2, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server2),
    ?assertEqual(ok, rpc:call(Server0, rabbit_stream_queue, add_replica,
                              [<<"/">>, Q, Server2])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

delete_replica(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    %% Not a member of the cluster, what would happen?
    ?assertEqual({error, node_not_running},
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
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    ?assertEqual(ok,
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),
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

grow_coordinator_cluster(Config) ->
    [Server0, Server1, _Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),

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

shrink_coordinator_cluster(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>}])),
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
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
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
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ?assertEqual({error, node_not_running},
                 rpc:call(Server0, rabbit_stream_queue, delete_replica,
                          [<<"/">>, Q, Server1])),
    %% check it isn't gone
    check_leader_and_replicas(Config, [Server0, Server1, Server2], members),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    rabbit_ct_helpers:await_condition(
      fun() ->
              ok == rpc:call(Server0, rabbit_stream_queue, delete_replica,
                             [<<"/">>, Q, Server1])
      end),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

publish_coordinator_unavailable(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    check_leader_and_replicas(Config, [Server0, Server1, Server2]),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    rabbit_ct_helpers:await_condition(
      fun () ->
              N = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mnesia, cluster_nodes, [running]),
              length(N) == 1
      end),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, Q),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 506, _}}}, _},
                amqp_channel:wait_for_confirms(Ch, 60)),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, 0, [online]),
              length(proplists:get_value(online, Info)) == 3
      end),
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
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish(Ch, Q),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

publish_confirm(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, Q),
    amqp_channel:wait_for_confirms(Ch, 5),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

restart_single_node(Config) ->
    [Server] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    publish(Ch, Q),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),

    rabbit_control_helper:command(stop_app, Server),
    rabbit_control_helper:command(start_app, Server),

    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    publish(Ch1, Q),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"2">>, <<"2">>, <<"0">>]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

recover(Config) ->
    [Server | _] = Servers0 = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    publish(Ch, Q),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]]),

    Perm0 = permute(Servers0),
    Servers = lists:nth(rand:uniform(length(Perm0)), Perm0),
    %% Such a slow test, let's select a single random permutation and trust that over enough
    %% ci rounds any failure will eventually show up

    ct:pal("recover: running stop start for permutation ~w", [Servers]),
    [rabbit_ct_broker_helpers:stop_node(Config, S) || S <- Servers],
    [rabbit_ct_broker_helpers:start_node(Config, S) || S <- lists:reverse(Servers)],
    ct:pal("recover: running stop waiting for messages ~w", [Servers]),
    check_leader_and_replicas(Config, Servers0),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]], 60),

    %% Another single random permutation
    Perm1 = permute(Servers0),
    Servers1 = lists:nth(rand:uniform(length(Perm1)), Perm1),

    ct:pal("recover: running app stop start for permuation ~w", [Servers1]),
    [rabbit_control_helper:command(stop_app, S) || S <- Servers1],
    [rabbit_control_helper:command(start_app, S) || S <- lists:reverse(Servers1)],
    ct:pal("recover: running app stop waiting for messages ~w", [Servers1]),
    check_leader_and_replicas(Config, Servers0),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]], 60),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    publish(Ch1, Q),
    quorum_queue_utils:wait_for_messages(Config, [[Q, <<"2">>, <<"2">>, <<"0">>]]),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

restart_coordinator_without_queues(Config) ->
    %% The coordinator failed to restart if stream queues were not present anymore, as
    %% they wouldn't call recover in all nodes - only the local one was restarted so
    %% the election wouldn't succeed. Fixed now, but this test checks for that failure
    [Server | _] = Servers0 = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    publish_confirm(Ch, Q, [<<"msg">>]),
    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch, #'queue.delete'{queue = Q})),

    [rabbit_ct_broker_helpers:stop_node(Config, S) || S <- Servers0],
    [rabbit_ct_broker_helpers:start_node(Config, S) || S <- lists:reverse(Servers0)],

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_without_qos(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ?assertExit({{shutdown, {server_initiated_close, 406, _}}, _},
                amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, consumer_tag = <<"ctag">>},
                                       self())),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_without_local_replica(Config) ->
    [Server0, Server1 | _] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    %% Add another node to the cluster, but it won't have a replica
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),
    timer:sleep(1000),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    qos(Ch1, 10, false),
    ?assertExit({{shutdown, {server_initiated_close, 406, _}}, _},
                amqp_channel:subscribe(Ch1, #'basic.consume'{queue = Q, consumer_tag = <<"ctag">>},
                                       self())),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),
    subscribe(Ch1, Q, false, 0),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok = amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = DeliveryTag,
                                                     multiple = false}),
            _ = amqp_channel:call(Ch1, #'basic.cancel'{consumer_tag = <<"ctag">>}),
            ok = amqp_channel:close(Ch1),
            ok
    after 5000 ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_offset(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
    after 5000 ->
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
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"111">> || _ <- lists:seq(1, 100)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),

    %% Subscribe from now/future
    Offset = erlang:system_time(second) + 60,
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
    after 5000 ->
            exit(missing_consume_ok)
    end,

    receive
        {_,
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
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
                amqp_channel:call(Ch, #'basic.get'{queue = Q})),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_with_autoack(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),

    ?assertExit(
       {{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
       subscribe(Ch1, Q, true, 0)),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_and_nack(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),
    subscribe(Ch1, Q, false, 0),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok = amqp_channel:cast(Ch1, #'basic.nack'{delivery_tag = DeliveryTag,
                                                      multiple     = false,
                                                      requeue      = true}),
            %% Nack will throw a not implemented exception. As it is a cast operation,
            %% we'll detect the conneciton/channel closure on the next call. 
            %% Let's try to redeclare and see what happens
            ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
                        declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}]))
    after 10000 ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

basic_cancel(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
    after 10000 ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

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

consume_and_reject(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),
    subscribe(Ch1, Q, false, 0),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok = amqp_channel:cast(Ch1, #'basic.reject'{delivery_tag = DeliveryTag,
                                                      requeue      = true}),
            %% Reject will throw a not implemented exception. As it is a cast operation,
            %% we'll detect the conneciton/channel closure on the next call. 
            %% Let's try to redeclare and see what happens
            ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
                        declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}]))
    after 10000 ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_and_ack(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch, Q, [<<"msg">>]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 10, false),
    subscribe(Ch1, Q, false, 0),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            ok = amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = DeliveryTag,
                                                     multiple     = false}),
            %% It will succeed as ack is now a credit operation. We should be
            %% able to redeclare a queue (gen_server call op) as the channel
            %% should still be open and declare is an idempotent operation
            %%
            ?assertMatch({'queue.declare_ok', Q, _MsgCount, 0},
                         declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
            quorum_queue_utils:wait_for_messages(Config, [[Q, <<"1">>, <<"1">>, <<"0">>]])
    after 5000 ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_from_last(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
    after 10000 ->
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
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
    [Server1, Server2 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    publish_confirm(Ch1, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),

    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, 1, [online]),
              length(proplists:get_value(online, Info)) == 3
      end),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    qos(Ch2, 10, false),

    subscribe(Ch2, Q, false, 0),
    receive_batch(Ch2, 0, 99),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_credit(Config) ->
    %% Because osiris provides one chunk on every read and we don't want to buffer
    %% messages in the broker to avoid memory penalties, the credit value won't
    %% be strict - we allow it into the negative values.
    %% We can test that after receiving a chunk, no more messages are delivered until
    %% the credit goes back to a positive value.
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    %% Let's publish a big batch, to ensure we have more than a chunk available
    NumMsgs = 100,
    publish_confirm(Ch, Q, [<<"msg1">> || _ <- lists:seq(1, NumMsgs)]),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),

    %% Let's subscribe with a small credit, easier to test
    Credit = 2,
    qos(Ch1, Credit, false),
    subscribe(Ch1, Q, false, 0),

    %% Receive everything
    DeliveryTags = receive_batch(),

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
    after 5000 ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

consume_credit_out_of_order_ack(Config) ->
    %% Like consume_credit but acknowledging the messages out of order.
    %% We want to ensure it doesn't behave like multiple, that is if we have
    %% credit 2 and received 10 messages, sending the ack for the message id
    %% number 10 should only increase credit by 1.
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
    after 5000 ->
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
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
    after 5000 ->
            exit(timeout)
    end,
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

max_length_bytes(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-max-length-bytes">>, long, 10000},
                                 {<<"x-stream-max-segment-size-bytes">>, long, 1000}])),

    Payload = << <<"1">> || _ <- lists:seq(1, 100) >>,

    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 500)]), %% 100 bytes/msg * 500 = 50000 bytes
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

max_age(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-max-age">>, longstr, <<"10s">>},
                                 {<<"x-stream-max-segment-size-bytes">>, long, 250}])),

    Payload = << <<"1">> || _ <- lists:seq(1, 500) >>,

    publish_confirm(Ch, Q, [Payload || _ <- lists:seq(1, 100)]),

    timer:sleep(10000),

    %% Let's publish again so the new segments will trigger the retention policy
    [publish(Ch, Q, Payload) || _ <- lists:seq(1, 100)],
    amqp_channel:wait_for_confirms(Ch, 5),

    timer:sleep(5000),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch1, 200, false),
    subscribe(Ch1, Q, false, 0),
    ?assertEqual(100, length(receive_batch())),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

replica_recovery(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [Server1 | _] = lists:reverse(Nodes),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    publish_confirm(Ch1, Q, [<<"msg1">> || _ <- lists:seq(1, 100)]),
    amqp_channel:close(Ch1),

    [begin
         [DownNode | _] = PNodes,
         rabbit_control_helper:command(stop_app, DownNode),
         rabbit_control_helper:command(start_app, DownNode),
         timer:sleep(6000),
         Ch2 = rabbit_ct_client_helpers:open_channel(Config, DownNode),
         qos(Ch2, 10, false),
         subscribe(Ch2, Q, false, 0),
         receive_batch(Ch2, 0, 99),
         amqp_channel:close(Ch2)
     end || PNodes <- permute(Nodes)],

    [begin
         [DownNode | _] = PNodes,
         ok = rabbit_ct_broker_helpers:stop_node(Config, DownNode),
         ok = rabbit_ct_broker_helpers:start_node(Config, DownNode),
         timer:sleep(6000),
         Ch2 = rabbit_ct_client_helpers:open_channel(Config, DownNode),
         qos(Ch2, 10, false),
         subscribe(Ch2, Q, false, 0),
         receive_batch(Ch2, 0, 99),
         amqp_channel:close(Ch2)
     end || PNodes <- permute(Nodes)],
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

leader_failover(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    check_leader_and_replicas(Config, [Server1, Server2, Server3]),
    publish_confirm(Ch1, Q, [<<"msg">> || _ <- lists:seq(1, 100)]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    timer:sleep(30000),

    rabbit_ct_helpers:await_condition(
      fun () ->
              Info = find_queue_info(Config, 1, [leader, members]),

              NewLeader = proplists:get_value(leader, Info),
              NewLeader =/= Server1
      end),
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
    ct:pal("~s running with nodes ~w", [?FUNCTION_NAME, Nodes]),
    [_Server1, DownNode, PubNode] = Nodes,
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, DownNode),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

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
    ct:pal("preinfo", []),
    rabbit_ct_helpers:await_condition(
      fun() ->
              Info = find_queue_info(Config, PubNode, [leader, members]),
              ct:pal("info ~p", [Info]),
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
        after 2000 ->
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

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                  {<<"x-initial-cluster-size">>, long, 1}])),
    check_leader_and_replicas(Config, [Server1]),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

initial_cluster_size_two(Config) ->
    [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                  {<<"x-initial-cluster-size">>, long, 2}])),

    Info = find_queue_info(Config, [leader, members]),

    ?assertEqual(Server1, proplists:get_value(leader, Info)),
    ?assertEqual(2, length(proplists:get_value(members, Info))),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

initial_cluster_size_one_policy(Config) ->
    [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"cluster-size">>, <<"initial_cluster_size_one_policy">>, <<"queues">>,
           [{<<"initial-cluster-size">>, 1}]),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                  {<<"x-initial-cluster-size">>, long, 1}])),
    check_leader_and_replicas(Config, [Server1]),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"cluster-size">>),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

leader_locator_client_local(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),

    Info = find_queue_info(Config, [leader]),

    ?assertEqual(Server1, proplists:get_value(leader, Info)),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),

    %% Try second node
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch2, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),

    Info2 = find_queue_info(Config, [leader]),
    ?assertEqual(Server2, proplists:get_value(leader, Info2)),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch2, #'queue.delete'{queue = Q})),

    %% Try third node
    Ch3 = rabbit_ct_client_helpers:open_channel(Config, Server3),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch3, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                  {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),


    Info3 = find_queue_info(Config, [leader]),
    ?assertEqual(Server3, proplists:get_value(leader, Info3)),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch3, #'queue.delete'{queue = Q})),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

leader_locator_random(Config) ->
    [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-queue-leader-locator">>, longstr, <<"random">>}])),

    Info = find_queue_info(Config, [leader]),
    Leader = proplists:get_value(leader, Info),

    ?assertMatch(#'queue.delete_ok'{},
      amqp_channel:call(Ch, #'queue.delete'{queue = Q})),

    repeat_until(
      fun() ->
              ?assertMatch(#'queue.delete_ok'{},
                           amqp_channel:call(Ch, #'queue.delete'{queue = Q})),

              ?assertEqual({'queue.declare_ok', Q, 0, 0},
                           declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                               {<<"x-queue-leader-locator">>, longstr, <<"random">>}])),

              Info2 = find_queue_info(Config, [leader]),
              Leader2 = proplists:get_value(leader, Info2),

              Leader =/= Leader2
      end, 10),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

leader_locator_least_leaders(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Q = ?config(queue_name, Config),

    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    ?assertEqual({'queue.declare_ok', Q1, 0, 0},
                 declare(Ch, Q1, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                  {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
    ?assertEqual({'queue.declare_ok', Q2, 0, 0},
                 declare(Ch, Q2, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                  {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-queue-leader-locator">>, longstr, <<"least-leaders">>}])),

    Info = find_queue_info(Config, [leader]),
    Leader = proplists:get_value(leader, Info),

    ?assert(lists:member(Leader, [Server2, Server3])),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

leader_locator_policy(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"leader-locator">>, <<"leader_locator_.*">>, <<"queues">>,
           [{<<"queue-leader-locator">>, <<"random">>}]),

    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    Info = find_queue_info(Config, [policy, operator_policy, effective_policy_definition, leader]),

    ?assertEqual(<<"leader-locator">>, proplists:get_value(policy, Info)),
    ?assertEqual('', proplists:get_value(operator_policy, Info)),
    ?assertEqual([{<<"queue-leader-locator">>, <<"random">>}],
                 proplists:get_value(effective_policy_definition, Info)),

    Leader = proplists:get_value(leader, Info),

    repeat_until(
      fun() ->
              ?assertMatch(#'queue.delete_ok'{},
                           amqp_channel:call(Ch, #'queue.delete'{queue = Q})),

              ?assertEqual({'queue.declare_ok', Q, 0, 0},
                           declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

              Info2 = find_queue_info(Config, [leader]),
              Leader2 = proplists:get_value(leader, Info2),
              Leader =/= Leader2
      end, 10),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"leader-locator">>),
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

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"ha">>, <<"invalid_policy.*">>, <<"queues">>,
           [{<<"ha-mode">>, <<"all">>}]),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"ttl">>, <<"invalid_policy.*">>, <<"queues">>,
           [{<<"message-ttl">>, 5}]),

    Info = find_queue_info(Config, [policy, operator_policy, effective_policy_definition]),

    ?assertEqual('', proplists:get_value(policy, Info)),
    ?assertEqual('', proplists:get_value(operator_policy, Info)),
    ?assertEqual([], proplists:get_value(effective_policy_definition, Info)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ha">>),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl">>),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

max_age_policy(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"age">>, <<"max_age_policy.*">>, <<"queues">>,
           [{<<"max-age">>, <<"1Y">>}]),

    Info = find_queue_info(Config, [policy, operator_policy, effective_policy_definition]),

    ?assertEqual(<<"age">>, proplists:get_value(policy, Info)),
    ?assertEqual('', proplists:get_value(operator_policy, Info)),
    ?assertEqual([{<<"max-age">>, <<"1Y">>}],
                 proplists:get_value(effective_policy_definition, Info)),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"age">>),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

update_retention_policy(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-stream-max-segment-size-bytes">>, long, 200}
                                ])),
    check_leader_and_replicas(Config, Servers),

    Msgs = [<<"msg">> || _ <- lists:seq(1, 10000)], %% 3 bytes * 10000 = 30000 bytes
    publish_confirm(Ch, Q, Msgs),

    {ok, Q0} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup,
                                            [rabbit_misc:r(<<"/">>, queue, Q)]),
    %% Don't use time based retention, it's really hard to get those tests right
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"retention">>, <<"update_retention_policy.*">>, <<"queues">>,
           [{<<"max-length-bytes">>, 10000}]),
    ensure_retention_applied(Config, Server),

    %% Retention policy should clear approximately 2/3 of the messages, but just to be safe
    %% let's simply check that it removed half of them
    quorum_queue_utils:wait_for_max_messages(Config, Q, 5000),

    {ok, Q1} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup,
                                            [rabbit_misc:r(<<"/">>, queue, Q)]),

    %% If there are changes only in the retention policy, processes should not be restarted
    ?assertEqual(amqqueue:get_pid(Q0), amqqueue:get_pid(Q1)),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"retention">>),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

queue_info(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    rabbit_ct_helpers:await_condition(
      fun() ->
              Info = find_queue_info(Config, [leader, online, members]),
              lists:member(proplists:get_value(leader, Info), Servers) andalso
                  (lists:sort(Servers) == lists:sort(proplists:get_value(members, Info))) andalso
                  (lists:sort(Servers) == lists:sort(proplists:get_value(online, Info)))
      end),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

max_segment_size_bytes_policy(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"segment">>, <<"max_segment_size_bytes.*">>, <<"queues">>,
           [{<<"stream-max-segment-size-bytes">>, 5000}]),

    Info = find_queue_info(Config, [policy, operator_policy, effective_policy_definition]),

    ?assertEqual(<<"segment">>, proplists:get_value(policy, Info)),
    ?assertEqual('', proplists:get_value(operator_policy, Info)),
    ?assertEqual([{<<"stream-max-segment-size-bytes">>, 5000}],
                 proplists:get_value(effective_policy_definition, Info)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"segment">>),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

purge(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
                amqp_channel:call(Ch, #'queue.purge'{queue = Q})),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_testcase_queue, [Q]).

%%----------------------------------------------------------------------------

delete_queues() ->
    [{ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

delete_testcase_queue(Name) ->
    QName = rabbit_misc:r(<<"/">>, queue, Name),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            {ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>);
        _ ->
            ok
    end.

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).
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
              Info = find_queue_info(Config, [leader, Tag]),
              ct:pal("~s members ~w ~p", [?FUNCTION_NAME, Members, Info]),
              lists:member(proplists:get_value(leader, Info), Members)
                  andalso (lists:sort(Members) == lists:sort(proplists:get_value(Tag, Info)))
      end, 60000).

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
            ct:pal("Committed offset is ~p but as first offset got ~p", [N, S]),
            ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                                    multiple     = false}),
            receive_batch(Ch, S + 1, M)
    after 60000 ->
              flush(),
              exit({missing_offset, N})
    end.

receive_batch(Ch, N, N) ->
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag},
         #amqp_msg{props = #'P_basic'{headers = [{<<"x-stream-offset">>, long, N}]}}} ->
            ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                                    multiple     = false})
    after 60000 ->
              flush(),
              exit({missing_offset, N})
    end;
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
            ct:pal("flush ~p", [Any]),
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
