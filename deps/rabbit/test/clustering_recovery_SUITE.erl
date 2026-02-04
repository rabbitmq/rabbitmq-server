%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(clustering_recovery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         force_standalone_boot/1,
         force_standalone_boot_and_restart/1,
         force_standalone_boot_and_restart_with_quorum_queues/1,
         recover_after_partition_with_leader/1,
         autodelete_transient_queue_after_partition_recovery_1/1,
         autodelete_durable_queue_after_partition_recovery_1/1,
         autodelete_transient_queue_after_partition_recovery_2/1,
         autodelete_durable_queue_after_partition_recovery_2/1,
         autodelete_transient_queue_after_node_loss/1,
         autodelete_durable_queue_after_node_loss/1,
         exclusive_transient_queue_after_partition_recovery_1/1,
         exclusive_durable_queue_after_partition_recovery_1/1,
         exclusive_transient_queue_after_partition_recovery_2/1,
         exclusive_durable_queue_after_partition_recovery_2/1,
         exclusive_transient_queue_after_node_loss/1,
         exclusive_durable_queue_after_node_loss/1,
         rolling_restart/1,
         rolling_kill_restart/1,
         forget_down_node/1
        ]).

-import(clustering_utils, [
                           assert_status/2,
                           assert_cluster_status/2,
                           assert_clustered/1,
                           assert_not_clustered/1
                          ]).

all() ->
    [
     {group, clustered_3_nodes},
     {group, clustered_5_nodes}
    ].

groups() ->
    [
     {clustered_3_nodes, [],
      [{cluster_size_3, [], [
        {cluster_size_3_sequential, [], [
                             force_standalone_boot,
                             force_standalone_boot_and_restart,
                             force_standalone_boot_and_restart_with_quorum_queues,
                             recover_after_partition_with_leader
                            ]},
        {cluster_size_3_parallel, [parallel], [
                             autodelete_transient_queue_after_partition_recovery_1,
                             autodelete_durable_queue_after_partition_recovery_1,
                             autodelete_transient_queue_after_partition_recovery_2,
                             autodelete_durable_queue_after_partition_recovery_2,
                             autodelete_transient_queue_after_node_loss,
                             autodelete_durable_queue_after_node_loss,
                             exclusive_transient_queue_after_partition_recovery_1,
                             exclusive_durable_queue_after_partition_recovery_1,
                             exclusive_transient_queue_after_partition_recovery_2,
                             exclusive_durable_queue_after_partition_recovery_2,
                             exclusive_transient_queue_after_node_loss,
                             exclusive_durable_queue_after_node_loss
                            ]}
                          ]}
      ]},
     {clustered_5_nodes, [],
      [{cluster_size_5, [parallel], [
                             rolling_restart,
                             rolling_kill_restart,
                             forget_down_node
                            ]}]
     }
    ].

suite() ->
    [
      %% If a testcase hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 10}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered_3_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(clustered_5_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]);
init_per_group(cluster_size_5, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 5}]);
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
        {keep_pid_file_on_exit, true}
      ]),
    Config2 = case Testcase of
                  _ when Testcase =:= autodelete_transient_queue_after_partition_recovery_1 orelse
                         Testcase =:= autodelete_durable_queue_after_partition_recovery_1 orelse
                         Testcase =:= autodelete_transient_queue_after_partition_recovery_2 orelse
                         Testcase =:= autodelete_durable_queue_after_partition_recovery_2 orelse
                         Testcase =:= exclusive_transient_queue_after_partition_recovery_1 orelse
                         Testcase =:= exclusive_durable_queue_after_partition_recovery_1 orelse
                         Testcase =:= exclusive_transient_queue_after_partition_recovery_2 orelse
                         Testcase =:= exclusive_durable_queue_after_partition_recovery_2 ->
                      rabbit_ct_helpers:merge_app_env(
                        Config1,
                        {rabbit,
                         [{cluster_partition_handling, pause_minority}]});
                  _ ->
                      Config1
              end,
    Config3 = rabbit_ct_helpers:run_steps(
                Config2,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    case Config3 of
        _ when is_list(Config3) andalso
               (Testcase =:= autodelete_transient_queue_after_partition_recovery_1 orelse
                Testcase =:= autodelete_durable_queue_after_partition_recovery_1 orelse
                Testcase =:= autodelete_transient_queue_after_partition_recovery_2 orelse
                Testcase =:= autodelete_durable_queue_after_partition_recovery_2 orelse
                Testcase =:= exclusive_transient_queue_after_partition_recovery_1 orelse
                Testcase =:= exclusive_durable_queue_after_partition_recovery_1 orelse
                Testcase =:= exclusive_transient_queue_after_partition_recovery_2 orelse
                Testcase =:= exclusive_durable_queue_after_partition_recovery_2) ->
            NewEnough = ok =:= rabbit_ct_broker_helpers:enable_feature_flag(
                                 Config3, 'rabbitmq_4.2.0'),
            case NewEnough of
                true ->
                    Config3;
                false ->
                    _ = rabbit_ct_helpers:run_steps(
                          Config3,
                          rabbit_ct_client_helpers:teardown_steps() ++
                          rabbit_ct_broker_helpers:teardown_steps()),
                    rabbit_ct_helpers:testcase_finished(Config3, Testcase),
                    {skip,
                     "The old node does not have improvements to "
                     "rabbit_amqqueue_process and rabbit_node_monitor"}
            end;
        _ ->
            %% Other testcases or failure to setup broker and client.
            Config3
    end.

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases
%% -------------------------------------------------------------------

force_standalone_boot(Config) ->
    %% Test for disaster recovery procedure command
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    assert_cluster_status({[Rabbit, Hare, Bunny], [Rabbit, Hare, Bunny], [Rabbit, Hare, Bunny]},
                          [Rabbit, Hare, Bunny]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),
    ok = force_standalone_khepri_boot(Rabbit),

    assert_cluster_status({[Rabbit], [Rabbit], [Rabbit], [Rabbit], [Rabbit]},
                          [Rabbit]),

    ok.

force_standalone_boot_and_restart(Config) ->
    %% Test for disaster recovery procedure
    %%
    %% 3-node cluster. Declare and publish to a classic queue on node 1.
    %% Stop the two remaining nodes. Force standalone boot on the node
    %% left. Restart it. Consume all the messages.
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    assert_cluster_status({[Rabbit, Hare, Bunny], [Rabbit, Hare, Bunny], [Rabbit, Hare, Bunny]},
                          [Rabbit, Hare, Bunny]),

    QName = classic_queue_name(Rabbit),
    Args = [{<<"x-queue-type">>, longstr, <<"classic">>}],
    declare_and_publish_to_queue(Config, Rabbit, QName, Args),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),
    ok = force_standalone_khepri_boot(Rabbit),

    assert_cluster_status({[Rabbit], [Rabbit], [Rabbit], [Rabbit], [Rabbit]},
                          [Rabbit]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:start_node(Config, Rabbit),

    consume_from_queue(Config, Rabbit, QName),

    ok.

force_standalone_boot_and_restart_with_quorum_queues(Config) ->
    %% Test for disaster recovery procedure
    %%
    %% 3-node cluster. Declare and publish to a classic queue on node 1.
    %% Stop the two remaining nodes. Force standalone boot on the node
    %% left. Restart it. Consume all the messages.
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    assert_cluster_status({[Rabbit, Hare, Bunny], [Rabbit, Hare, Bunny], [Rabbit, Hare, Bunny]},
                          [Rabbit, Hare, Bunny]),

    QName1 = quorum_queue_name(1),
    QName2 = quorum_queue_name(2),
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    declare_and_publish_to_queue(Config, Rabbit, QName1, Args),
    declare_and_publish_to_queue(Config, Rabbit, QName2, Args),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    ok = force_standalone_khepri_boot(Rabbit),
    ok = rabbit_ct_broker_helpers:rpc(Config, Rabbit, rabbit_quorum_queue, force_all_queues_shrink_member_to_current_member, []),

    assert_cluster_status({[Rabbit], [Rabbit], [Rabbit], [Rabbit], [Rabbit]},
                          [Rabbit]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:start_node(Config, Rabbit),

    consume_from_queue(Config, Rabbit, QName1),
    consume_from_queue(Config, Rabbit, QName2),

    ok.

recover_after_partition_with_leader(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Config1 = start_proxies(Config),

    NodeA = hd(Nodes),

    ct:pal("Prevent automatic reconnection on the common_test node"),
    application:set_env(kernel, dist_auto_connect, never),
    ct:pal("Disconnect the common_test node from RabbitMQ nodes"),
    lists:foreach(fun erlang:disconnect_node/1, Nodes),
    ct:pal(
      "Ensure RabbitMQ nodes only know about the RabbitMQ nodes "
      "(and their proxy)"),
    lists:foreach(
      fun(Node) ->
              ?awaitMatch(
                 Nodes,
                 get_connected_nodes(Config1, Node),
                 30000)
      end, Nodes),

    ct:pal("Wait for a Khepri leader to be elected"),
    ?awaitMatch({ok, _}, get_leader_node(Config1, NodeA), 30000),

    ct:pal("Query the Khepri leader nodename"),
    {ok, Leader} = get_leader_node(Config1, NodeA),
    Followers = Nodes -- [Leader],
    ct:pal("Leader: ~0p~nFollowers: ~p", [Leader, Followers]),

    lists:foreach(
      fun(Follower) ->
              ct:pal(
                ?LOW_IMPORTANCE,
                "Blocking traffic between ~ts and ~ts",
                [Leader, Follower]),
              ?assertEqual(
                 ok,
                 proxied_rpc(
                   Config1, Leader, inet_tcp_proxy_dist, block, [Follower])),
              ?assertEqual(
                 ok,
                 proxied_rpc(
                   Config1, Follower, inet_tcp_proxy_dist, block, [Leader]))
      end, Followers),

    ct:pal(
      "Ensure the leader node is disconnected from other RabbitMQ nodes"),
    ?awaitMatch(
       [Leader],
       get_connected_nodes(Config1, Leader),
       30000),
    ct:pal(
      "Ensure the follower nodes are disconnected from the leader node"),
    lists:foreach(
      fun(Follower) ->
              ?awaitMatch(
                 Followers,
                 get_connected_nodes(Config1, Follower),
                 30000)
      end, Followers),

    ct:pal("Wait for each side of the partition to have its own leader"),
    Follower1 = hd(Followers),
    ?awaitMatch(
       false,
       begin
           LeaderA = get_leader_node(Config1, Leader),
           LeaderB = get_leader_node(Config1, Follower1),
           ct:pal("LeaderA: ~0p~nLeaderB: ~0p", [LeaderA, LeaderB]),
           LeaderA =:= LeaderB
       end,
       30000),

    ct:pal("Waiting for 2 minutes"),
    timer:sleep(120000),

    ct:pal("Query Khepri status for each RabbitMQ node"),
    PerNodeStatus1 = get_per_node_khepri_status(Config1),
    ct:pal("Per-node Khepri status (during partition):~n~p", [PerNodeStatus1]),

    lists:foreach(
      fun(Follower) ->
              ct:pal(
                ?LOW_IMPORTANCE,
                "Unblocking traffic between ~ts and ~ts",
                [Leader, Follower]),
              ?assertEqual(
                 ok,
                 proxied_rpc(
                   Config1, Leader, inet_tcp_proxy_dist, allow, [Follower])),
              ?assertEqual(
                 ok,
                 proxied_rpc(
                   Config1, Follower, inet_tcp_proxy_dist, allow, [Leader]))
      end, Followers),

    ct:pal("Wait for the whole cluster to agree on the same leader"),
    ?awaitMatch(
       true,
       begin
           LeaderA = get_leader_node(Config1, Leader),
           LeaderB = get_leader_node(Config1, Follower1),
           ct:pal("LeaderA: ~0p~nLeaderB: ~0p", [LeaderA, LeaderB]),
           LeaderA =:= LeaderB
       end,
       30000),

    ct:pal("Query Khepri status for each RabbitMQ node"),
    PerNodeStatus2 = get_per_node_khepri_status(Config1),
    ct:pal("Per-node Khepri status (after recovery):~n~p", [PerNodeStatus2]),

    ct:pal("Restore automatic reconnection on the common_test node"),
    application:unset_env(kernel, dist_auto_connect),
    ok.

start_proxies(Config) ->
    %% We use intermediate Erlang nodes between the common_test control node
    %% and the RabbitMQ nodes, using `peer' standard_io communication. The
    %% goal is to make sure the common_test control node doesn't interfere
    %% with the nodes the RabbitMQ nodes can see, despite the blocking of the
    %% Erlang distribution connection.
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Proxies0 = [begin
                    {ok, Proxy, PeerNode} = peer:start_link(
                                              #{name => peer:random_name(),
                                                connection => standard_io,
                                                wait_boot => 120000}),
                    ct:pal("Proxy ~0p -> ~0p", [Proxy, PeerNode]),
                    Proxy
                end || _ <- Nodes],
    Proxies = maps:from_list(lists:zip(Nodes, Proxies0)),
    ct:pal("Proxies: ~p", [Proxies]),
    Config1 = [{proxies, Proxies} | Config],
    Config1.

proxied_rpc(Config, Node, Module, Function, Args) ->
    Proxies = ?config(proxies, Config),
    Proxy = maps:get(Node, Proxies),
    peer:call(
      Proxy, rabbit_ct_broker_helpers, rpc,
      [Config, Node, Module, Function, Args]).

get_leader_node(Config, Node) ->
    StoreId = rabbit_khepri:get_store_id(),
    Ret = case rabbit_ct_helpers:get_config(Config, proxies) of
              undefined ->
                  rabbit_ct_broker_helpers:rpc(
                    Config, Node,
                    ra_leaderboard, lookup_leader, [StoreId]);
              _ ->
                  proxied_rpc(
                    Config, Node,
                    ra_leaderboard, lookup_leader, [StoreId])
          end,
    case Ret of
        {StoreId, LeaderNode} ->
            {ok, LeaderNode};
        undefined ->
            {error, no_leader}
    end.

get_connected_nodes(Config, Node) ->
    Proxies = ?config(proxies, Config),
    Proxy = maps:get(Node, Proxies),
    Peer = peer:call(Proxy, erlang, node, []),
    OtherNodes = proxied_rpc(Config, Node, erlang, nodes, []),
    lists:sort([Node | OtherNodes -- [Peer]]).

get_per_node_khepri_status(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    maps:from_list(
      lists:map(
        fun(Node) ->
                Status = proxied_rpc(Config, Node, rabbit_khepri, status, []),
                {Node, Status}
        end, Nodes)).

rolling_restart(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Args = [{<<"x-queue-type">>, longstr, <<"classic">>}],
    [begin
         QName = classic_queue_name(N),
         declare_and_publish_to_queue(Config, N, QName, Args)
     end || N <- Nodes],

    [begin
         ok = rabbit_ct_broker_helpers:stop_node(Config, N),
         ok = rabbit_ct_broker_helpers:start_node(Config, N)
     end || N <- Nodes],

    assert_cluster_status({Nodes, Nodes, Nodes}, Nodes),
    [begin
         QName = classic_queue_name(N),
         consume_from_queue(Config, N, QName)
     end || N <- Nodes],

    ok.

rolling_kill_restart(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Args = [{<<"x-queue-type">>, longstr, <<"classic">>}],
    [begin
         QName = classic_queue_name(N),
         declare_and_publish_to_queue(Config, N, QName, Args)
     end || N <- Nodes],

    Ret0 =
        [begin
             ok = rabbit_ct_broker_helpers:kill_node(Config, N),
             {N, rabbit_ct_broker_helpers:start_node(Config, N)}
         end || N <- Nodes],
    Failed = [Pair || {_, V} = Pair <- Ret0, V =/= ok],

    ?assert(length(Failed) =< 1),

    case Failed of
        [] ->
            assert_cluster_status({Nodes, Nodes, Nodes}, Nodes),
            [begin
                 QName = classic_queue_name(N),
                 consume_from_queue(Config, N, QName)
             end || N <- Nodes];
        [{FailedNode, {error, _}}] ->
            [Node0 | _] = RemainingNodes = Nodes -- [FailedNode],
            ok = forget_cluster_node(Node0, FailedNode),
            assert_cluster_status({RemainingNodes, RemainingNodes, RemainingNodes}, RemainingNodes)
    end,
    ok.

forget_down_node(Config) ->
    [Rabbit, Hare | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = forget_cluster_node(Hare, Rabbit),

    NNodes = lists:nthtail(1, Nodes),

    assert_cluster_status({NNodes, NNodes, NNodes}, NNodes),

    ok.

autodelete_transient_queue_after_partition_recovery_1(Config) ->
    QueueDeclare = #'queue.declare'{auto_delete = true,
                                    durable = false},
    temporary_queue_after_partition_recovery_1(Config, QueueDeclare).

autodelete_durable_queue_after_partition_recovery_1(Config) ->
    QueueDeclare = #'queue.declare'{auto_delete = true,
                                    durable = true},
    temporary_queue_after_partition_recovery_1(Config, QueueDeclare).

exclusive_transient_queue_after_partition_recovery_1(Config) ->
    QueueDeclare = #'queue.declare'{exclusive = true,
                                    durable = false},
    temporary_queue_after_partition_recovery_1(Config, QueueDeclare).

exclusive_durable_queue_after_partition_recovery_1(Config) ->
    QueueDeclare = #'queue.declare'{exclusive = true,
                                    durable = true},
    temporary_queue_after_partition_recovery_1(Config, QueueDeclare).

temporary_queue_after_partition_recovery_1(Config, QueueDeclare) ->
    [_Node1, Node2 | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(
                                    Config, nodename),
    Majority = Nodes -- [Node2],
    Timeout = 60000,

    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(
                   Config, Node2),
    CMRef = erlang:monitor(process, Conn),

    %% We create an exclusive queue on node 1 and get its PID on the server
    %% side.
    ?assertMatch(#'queue.declare_ok'{}, amqp_channel:call(Ch, QueueDeclare)),
    Queues = rabbit_ct_broker_helpers:rpc(
               Config, Node2, rabbit_amqqueue, list, []),
    ?assertMatch([_], Queues),
    [Queue] = Queues,
    ct:pal("Queue = ~p", [Queue]),

    QName = amqqueue:get_name(Queue),
    QPid = amqqueue:get_pid(Queue),
    QMRef = erlang:monitor(process, QPid),
    subscribe(Ch, QName#resource.name),

    lists:foreach(
      fun(Node) ->
              rabbit_ct_broker_helpers:block_traffic_between(Node2, Node)
      end, Majority),
    clustering_utils:assert_cluster_status({Nodes, Majority}, Majority),

    clustering_utils:assert_cluster_status({Nodes, [Node2]}, [Node2]),

    %% The queue is still recorded everywhere.
    lists:foreach(
      fun(Node) ->
              Ret = rabbit_ct_broker_helpers:rpc(
                      Config, Node, rabbit_amqqueue, lookup, [QName]),
              ct:pal("Queue lookup on node ~0p: ~p", [Node, Ret]),
              ?assertEqual({ok, Queue}, Ret)
      end, Nodes),

    %% Prepare a publisher.
    {PConn,
     PCh} = rabbit_ct_client_helpers:open_connection_and_channel(
              Config, Node2),
    publish_many(PCh, QName#resource.name, 10),
    consume(10),

    %% We resolve the network partition.
    lists:foreach(
      fun(Node) ->
              rabbit_ct_broker_helpers:allow_traffic_between(
                Node2, Node)
      end, Majority),
    clustering_utils:assert_cluster_status({Nodes, Nodes}, Nodes),

    publish_many(PCh, QName#resource.name, 10),
    consume(10),

    rabbit_ct_client_helpers:close_connection_and_channel(PConn, PCh),

    %% We terminate the channel and connection: the queue should
    %% terminate and the metadata store should have no record of it.
    _ = rabbit_ct_client_helpers:close_connection_and_channel(
          Conn, Ch),

    receive
        {'DOWN', CMRef, _, _, Reason1} ->
            ct:pal("Connection ~p exited: ~p", [Conn, Reason1]),
            ?assertEqual({shutdown, normal}, Reason1),
            ok
    after Timeout ->
              ct:fail("Connection ~p still running", [Conn])
    end,
    receive
        {'DOWN', QMRef, _, _, Reason} ->
            ct:pal("Queue ~p exited: ~p", [QPid, Reason]),
            ?assertEqual(normal, Reason),
            ok
    after Timeout ->
              ct:fail("Queue ~p still running", [QPid])
    end,

    %% The queue was also deleted from the metadata store on all
    %% nodes.
    lists:foreach(
      fun(Node) ->
              ?awaitMatch(
                 {error, not_found},
                 begin
                     Ret = rabbit_ct_broker_helpers:rpc(
                             Config, Node,
                             rabbit_amqqueue, lookup, [QName]),
                     ct:pal(
                       "Queue lookup on node ~0p: ~p",
                       [Node, Ret]),
                     Ret
                 end, Timeout)
      end, Nodes),
    ok.

autodelete_transient_queue_after_partition_recovery_2(Config) ->
    QueueDeclare = #'queue.declare'{auto_delete = true,
                                    durable = false},
    temporary_queue_after_partition_recovery_2(Config, QueueDeclare).

autodelete_durable_queue_after_partition_recovery_2(Config) ->
    QueueDeclare = #'queue.declare'{auto_delete = true,
                                    durable = true},
    temporary_queue_after_partition_recovery_2(Config, QueueDeclare).

exclusive_transient_queue_after_partition_recovery_2(Config) ->
    QueueDeclare = #'queue.declare'{exclusive = true,
                                    durable = true},
    temporary_queue_after_partition_recovery_2(Config, QueueDeclare).

exclusive_durable_queue_after_partition_recovery_2(Config) ->
    QueueDeclare = #'queue.declare'{exclusive = true,
                                    durable = true},
    temporary_queue_after_partition_recovery_2(Config, QueueDeclare).

temporary_queue_after_partition_recovery_2(Config, QueueDeclare) ->
    [_Node1, Node2 | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(
                                    Config, nodename),
    Majority = Nodes -- [Node2],
    Timeout = 60000,

    {Conn1, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(
                     Config, Node2),
    CMRef1 = erlang:monitor(process, Conn1),
    {Conn2, Ch2} = rabbit_ct_client_helpers:open_connection_and_channel(
                     Config, Node2),
    CMRef2 = erlang:monitor(process, Conn2),

    %% We create an exclusive queue on node 1 and get its PID on the server
    %% side.
    ?assertMatch(#'queue.declare_ok'{}, amqp_channel:call(Ch1, QueueDeclare)),
    ?assertMatch(#'queue.declare_ok'{}, amqp_channel:call(Ch2, QueueDeclare)),
    Queues = rabbit_ct_broker_helpers:rpc(
               Config, Node2, rabbit_amqqueue, list, []),
    ?assertMatch([_, _], Queues),
    [Queue1, Queue2] = Queues,
    ct:pal("Queues = ~p", [Queues]),

    QName1 = amqqueue:get_name(Queue1),
    QPid1 = amqqueue:get_pid(Queue1),
    QMRef1 = erlang:monitor(process, QPid1),
    subscribe(Ch1, QName1#resource.name),

    QName2 = amqqueue:get_name(Queue2),
    QPid2 = amqqueue:get_pid(Queue2),
    QMRef2 = erlang:monitor(process, QPid2),
    subscribe(Ch2, QName2#resource.name),

    lists:foreach(
      fun(Node) ->
              rabbit_ct_broker_helpers:block_traffic_between(Node2, Node)
      end, Majority),
    clustering_utils:assert_cluster_status({Nodes, Majority}, Majority),
    clustering_utils:assert_cluster_status({Nodes, [Node2]}, [Node2]),

    %% The queues are still recorded everywhere.
    lists:foreach(
      fun(Node) ->
              Ret1 = rabbit_ct_broker_helpers:rpc(
                       Config, Node, rabbit_amqqueue, lookup, [QName1]),
              Ret2 = rabbit_ct_broker_helpers:rpc(
                       Config, Node, rabbit_amqqueue, lookup, [QName2]),
              ct:pal(
                "Queues lookup on node ~0p:~n  ~p~n~p",
                [Node, Ret1, Ret2]),
              ?assertEqual({ok, Queue1}, Ret1),
              ?assertEqual({ok, Queue2}, Ret2)
      end, Nodes),

    %% Publich to and consume from the queue.
    ct:pal("Open connection"),
    {_PConn, PCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                      Config, Node2),
    ct:pal("Publish messages to Q1"),
    publish_many(PCh, QName1#resource.name, 10),
    ct:pal("Publish messages to Q2"),
    publish_many(PCh, QName2#resource.name, 10),
    ct:pal("Consume all messages"),
    consume(20),

    %% Close the first consuming client to trigger the queue deletion during
    %% the network partition. Because of the network partition, the queue
    %% process exits but it couldn't delete the queue record.
    ct:pal("Close connection 1"),
    _ = spawn(fun() ->
                      rabbit_ct_client_helpers:close_connection_and_channel(
                        Conn1, Ch1)
              end),

    ct:pal("Wait for connection 1 DOWN"),
    receive
        {'DOWN', CMRef1, _, _, Reason1_1} ->
            ct:pal("Connection ~p exited: ~p", [Conn1, Reason1_1]),
            ?assertEqual({shutdown, normal}, Reason1_1),
            ok
    after Timeout ->
              ct:fail("Connection ~p still running", [Conn1])
    end,
    ct:pal("Wait for queue 1 DOWN"),
    receive
        {'DOWN', QMRef1, _, _, Reason1_2} ->
            ct:pal("Queue ~p exited: ~p", [QPid1, Reason1_2]),
            ?assertEqual(normal, Reason1_2),
            ok
    after Timeout ->
              ct:fail("Queue ~p still running", [QPid1])
    end,

    %% We sleep to let the queue record deletion reach the timeout. It should
    %% retry indefinitely.
    KhepriTimeout = rabbit_ct_broker_helpers:rpc(
                      Config, Node2, khepri_app, get_default_timeout, []),
    ct:pal("Sleep > ~b ms", [KhepriTimeout]),
    timer:sleep(KhepriTimeout + 10000),

    %% The queue process exited but the queue record should still be there. The
    %% temporary process is still trying to delete it but can't during the
    %% network partition.
    ?awaitMatch(
       {ok, _},
       begin
           Ret = rabbit_ct_broker_helpers:rpc(
                   Config, Node2, rabbit_amqqueue, lookup, [QName1]),
           ct:pal("Queue lookup on node ~0p: ~p", [Node2, Ret]),
           Ret
       end, Timeout),

    %% Close the second consuming client to trigger the queue deletion during
    %% the network partition. This time, the partition is solved while the
    %% queue process tries to delete the record.
    ct:pal("Close connection 2"),
    _ = spawn(fun() ->
                      rabbit_ct_client_helpers:close_connection_and_channel(
                        Conn2, Ch2)
              end),

    ct:pal("Wait for connection 2 DOWN"),
    receive
        {'DOWN', CMRef2, _, _, Reason2_1} ->
            ct:pal("Connection ~p exited: ~p", [Conn2, Reason2_1]),
            ?assertEqual({shutdown, normal}, Reason2_1),
            ok
    after Timeout ->
              ct:fail("Connection ~p still running", [Conn2])
    end,
    ct:pal("Wait for queue 2 DOWN"),
    receive
        {'DOWN', QMRef2, _, _, Reason2_2} ->
            ct:pal("Queue ~p exited: ~p", [QPid2, Reason2_2]),
            ?assertEqual(normal, Reason2_2),
            ok
    after Timeout ->
              ct:fail("Queue ~p still running", [QPid2])
    end,

    %% Again, the queue process exited but the queue record should still be
    %% there. The temporary process is still trying to delete it but can't
    %% during the network partition.
    ?awaitMatch(
       {ok, _},
       begin
           Ret = rabbit_ct_broker_helpers:rpc(
                   Config, Node2, rabbit_amqqueue, lookup, [QName2]),
           ct:pal("Queue lookup on node ~0p: ~p", [Node2, Ret]),
           Ret
       end, Timeout),

    %% We resolve the network partition.
    lists:foreach(
      fun(Node) ->
              ct:pal("Allow traffic with ~s", [Node]),
              rabbit_ct_broker_helpers:allow_traffic_between(
                Node2, Node)
      end, Majority),
    ct:pal("Cluster status"),
    clustering_utils:assert_cluster_status({Nodes, Nodes}, Nodes),

    %% The first queue was deleted from the metadata store on all nodes.
    lists:foreach(
      fun(Node) ->
              ?awaitMatch(
                 {error, not_found},
                 begin
                     Ret = rabbit_ct_broker_helpers:rpc(
                             Config, Node, rabbit_amqqueue, lookup, [QName1]),
                     ct:pal("Queue lookup on node ~0p: ~p", [Node, Ret]),
                     Ret
                 end, Timeout)
      end, Nodes),

    %% The second queue was deleted from the metadata store on all nodes.
    lists:foreach(
      fun(Node) ->
              ?awaitMatch(
                 {error, not_found},
                 begin
                     Ret = rabbit_ct_broker_helpers:rpc(
                             Config, Node, rabbit_amqqueue, lookup, [QName2]),
                     ct:pal("Queue lookup on node ~0p: ~p", [Node, Ret]),
                     Ret
                 end, Timeout)
      end, Nodes),
    ok.

autodelete_transient_queue_after_node_loss(Config) ->
    QueueDeclare = #'queue.declare'{auto_delete = true,
                                    durable = false},
    temporary_queue_after_node_loss(Config, QueueDeclare).

autodelete_durable_queue_after_node_loss(Config) ->
    QueueDeclare = #'queue.declare'{auto_delete = true,
                                    durable = true},
    temporary_queue_after_node_loss(Config, QueueDeclare).

exclusive_transient_queue_after_node_loss(Config) ->
    QueueDeclare = #'queue.declare'{exclusive = true,
                                    durable = false},
    temporary_queue_after_node_loss(Config, QueueDeclare).

exclusive_durable_queue_after_node_loss(Config) ->
    QueueDeclare = #'queue.declare'{exclusive = true,
                                    durable = true},
    temporary_queue_after_node_loss(Config, QueueDeclare).

temporary_queue_after_node_loss(Config, QueueDeclare) ->
    [Node1, Node2, Node3] = Nodes = rabbit_ct_broker_helpers:get_node_configs(
                                      Config, nodename),
    Majority = Nodes -- [Node2],
    Timeout = 60000,

    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(
                    Config, Node2),

    %% We create an exclusive queue on node 1.
    ?assertMatch(#'queue.declare_ok'{}, amqp_channel:call(Ch, QueueDeclare)),
    Queues = rabbit_ct_broker_helpers:rpc(
               Config, Node2, rabbit_amqqueue, list, []),
    ?assertMatch([_], Queues),
    [Queue] = Queues,
    ct:pal("Queue = ~p", [Queue]),

    QName = amqqueue:get_name(Queue),

    %% We kill the node.
    rabbit_ct_broker_helpers:kill_node(Config, Node2),

    ct:pal("Wait for new Khepri leader to be elected"),
    lists:foreach(
      fun(Node) ->
              ?awaitMatch(
                 {ok, LeaderNode}
                   when LeaderNode =:= Node1 orelse LeaderNode =:= Node3,
                 get_leader_node(Config, Node),
                 Timeout)
      end, Majority),

    clustering_utils:assert_cluster_status(
      {Nodes, Majority}, Majority),

    %% The queue is still recorded on the remaining nodes.
    lists:foreach(
      fun(Node) ->
              Ret = rabbit_ct_broker_helpers:rpc(
                      Config, Node, rabbit_amqqueue, lookup, [QName]),
              ct:pal("Queue lookup on node ~0p: ~p", [Node, Ret]),
              ?assertEqual({ok, Queue}, Ret)
      end, Majority),

    %% We remove the lost node from the cluster.
    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:forget_cluster_node(
         Config, Node3, Node2)),
    clustering_utils:assert_cluster_status(
      {Majority, Majority}, Majority),

    %% The queue was deleted from the metadata store on remaining
    %% nodes.
    lists:foreach(
      fun(Node) ->
              ?awaitMatch(
                 {error, not_found},
                 begin
                     Ret = rabbit_ct_broker_helpers:rpc(
                             Config, Node,
                             rabbit_amqqueue, lookup, [QName]),
                     ct:pal(
                       "Queue lookup on node ~0p: ~p",
                       [Node, Ret]),
                     Ret
                 end, Timeout)
      end, Majority),
    ok.

%% -------------------------------------------------------------------
%% Internal utils
%% -------------------------------------------------------------------
declare_and_publish_to_queue(Config, Node, QName, Args) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Node),
    declare(Ch, QName, Args),
    publish_many(Ch, QName, 10),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

quorum_queue_name(Number) ->
    list_to_binary(io_lib:format("quorum_queue_~p", [Number])).

classic_queue_name(Node) ->
    list_to_binary(io_lib:format("classic_queue_~p", [Node])).

declare(Ch, Name, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{durable = true,
                                           queue   = Name,
                                           arguments = Args}).

consume_from_queue(Config, Node, QName) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Node),
    subscribe(Ch, QName),
    consume(10),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

force_standalone_khepri_boot(Node) ->
    rabbit_control_helper:command(force_standalone_khepri_boot, Node, []).

forget_cluster_node(Node, Removee) ->
    rabbit_control_helper:command(forget_cluster_node, Node, [atom_to_list(Removee)], []).

publish_many(Ch, QName, N) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [amqp_channel:cast(Ch, #'basic.publish'{routing_key = QName},
                       #amqp_msg{props = #'P_basic'{delivery_mode = 2}})
     || _ <- lists:seq(1, N)],
    amqp_channel:wait_for_confirms(Ch).

subscribe(Ch, QName) ->
    CTag = <<"ctag">>,
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName,
                                                consumer_tag = CTag},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
            ok
    after 30000 ->
            exit(consume_ok_timeout)
    end.

consume(0) ->
    ok;
consume(N) ->
    receive
        {#'basic.deliver'{consumer_tag = <<"ctag">>}, _} ->
            consume(N - 1)
    after 30000 ->
            exit(deliver_timeout)
    end.
