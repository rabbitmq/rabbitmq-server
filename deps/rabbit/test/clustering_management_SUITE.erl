%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(clustering_management_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(clustering_utils, [
                           assert_status/2,
                           assert_cluster_status/2,
                           assert_clustered/1,
                           assert_not_clustered/1
                          ]).

all() ->
    [
     {group, mnesia_store},
     {group, khepri_store}
    ].

groups() ->
    [{mnesia_store, [], [
                         {unclustered_2_nodes, [],
                          [
                           {cluster_size_2, [], [
                                                 classic_config_discovery_node_list
                                                ]}
                          ]},
                         {unclustered_3_nodes, [],
                          [
                           {cluster_size_3, [], [
                                                 join_and_part_cluster,
                                                 join_cluster_bad_operations,
                                                 join_to_start_interval,
                                                 forget_cluster_node,
                                                 change_cluster_node_type,
                                                 change_cluster_when_node_offline,
                                                 update_cluster_nodes,
                                                 force_reset_node
                                                ]}
                          ]},
                         {clustered_2_nodes, [],
                          [
                           {cluster_size_2, [], [
                                                 forget_removes_things,
                                                 reset_removes_things,
                                                 forget_offline_removes_things,
                                                 forget_unavailable_node,
                                                 force_boot,
                                                 status_with_alarm,
                                                 pid_file_and_await_node_startup,
                                                 await_running_count,
                                                 start_with_invalid_schema_in_path,
                                                 persistent_cluster_id,
                                                 reset_last_disc_node
                                                ]}
                          ]}
                        ]},
     {khepri_store, [], [
                         {clustered_2_nodes, [],
                          [
                           {cluster_size_2, [], [
                                                 change_cluster_node_type_in_khepri,
                                                 forget_node_in_khepri,
                                                 forget_removes_things_in_khepri,
                                                 reset_in_khepri,
                                                 reset_removes_things_in_khepri,
                                                 reset_in_minority,
                                                 force_boot_in_khepri,
                                                 status_with_alarm,
                                                 pid_file_and_await_node_startup_in_khepri,
                                                 await_running_count_in_khepri,
                                                 start_with_invalid_schema_in_path,
                                                 persistent_cluster_id,
                                                 stop_start_cluster_node,
                                                 restart_cluster_node,
                                                 unsupported_forget_cluster_node_offline,
                                                 unsupported_update_cluster_nodes

                                                ]}
                          ]},
                         {clustered_3_nodes, [],
                          [{cluster_size_3, [], [
                                                 forget_unavailable_node,
                                                 forget_unavailable_node_in_minority
                                                ]}]},
                         {unclustered_3_nodes, [],
                          [
                           {cluster_size_3, [], [
                                                 join_and_part_cluster_in_khepri,
                                                 join_cluster_bad_operations_in_khepri,
                                                 join_cluster_in_minority,
                                                 join_cluster_with_rabbit_stopped,
                                                 force_reset_node_in_khepri,
                                                 join_to_start_interval,
                                                 forget_cluster_node_in_khepri,
                                                 start_nodes_in_reverse_order,
                                                 start_nodes_in_stop_order_in_khepri,
                                                 start_nodes_in_stop_order_with_force_boot
                                                ]}
                          ]}
                        ]}
    ].

suite() ->
    [
      %% If a testcase hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 5}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config, {rabbit, [
                          {mnesia_table_loading_retry_limit, 2},
                          {mnesia_table_loading_retry_timeout,1000}
                         ]}),
    rabbit_ct_helpers:run_setup_steps(Config1).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mnesia_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, mnesia}]);
init_per_group(khepri_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, khepri}]);
init_per_group(unclustered_2_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]);
init_per_group(unclustered_3_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]);
init_per_group(clustered_2_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(clustered_3_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(clustered_4_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(cluster_size_2, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 2}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]);
init_per_group(cluster_size_4, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 4}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(create_bad_schema = Testcase, Config) ->
    case rabbit_ct_broker_helpers:configured_metadata_store(Config) of
        mnesia ->
            init_per_testcase0(Testcase, Config);
        _ ->
            {skip, "Mnesia operations not supported by Khepri"}
    end;
init_per_testcase(Testcase, Config) ->
    init_per_testcase0(Testcase, Config).

init_per_testcase0(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
        {keep_pid_file_on_exit, true}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

start_with_invalid_schema_in_path(Config) ->
    [Rabbit, Hare] = cluster_members(Config),
    stop_app(Config, Rabbit),
    stop_app(Config, Hare),

    create_bad_schema(Rabbit, Hare, Config),

    spawn(fun() -> start_app(Config, Hare) end),
    case start_app(Config, Rabbit) of
        ok  -> ok;
        ErrRabbit -> error({unable_to_start_with_bad_schema_in_work_dir, ErrRabbit})
    end.

persistent_cluster_id(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
      false ->
        [Rabbit, Hare] = cluster_members(Config),
        ClusterIDA1 = rpc:call(Rabbit, rabbit_nodes, persistent_cluster_id, []),
        ClusterIDB1 = rpc:call(Hare, rabbit_nodes, persistent_cluster_id, []),
        ?assertEqual(ClusterIDA1, ClusterIDB1),

        rabbit_ct_broker_helpers:restart_node(Config, Rabbit),
        ClusterIDA2 = rpc:call(Rabbit, rabbit_nodes, persistent_cluster_id, []),
        rabbit_ct_broker_helpers:restart_node(Config, Hare),
        ClusterIDB2 = rpc:call(Hare, rabbit_nodes, persistent_cluster_id, []),
        ?assertEqual(ClusterIDA1, ClusterIDA2),
        ?assertEqual(ClusterIDA2, ClusterIDB2);
      _ ->
        %% skip the test in mixed version mode
        {skip, "Should not run in mixed version environments"}
    end.

create_bad_schema(Rabbit, Hare, Config) ->
    {ok, RabbitMnesiaDir} = rpc:call(Rabbit, application, get_env, [mnesia, dir]),
    {ok, HareMnesiaDir} = rpc:call(Hare, application, get_env, [mnesia, dir]),
    %% Make sure we don't use the current dir:
    PrivDir = ?config(priv_dir, Config),
    ct:pal("Priv dir ~tp~n", [PrivDir]),
    ok = filelib:ensure_dir(filename:join(PrivDir, "file")),

    ok = rpc:call(Rabbit, file, set_cwd, [PrivDir]),
    ok = rpc:call(Hare, file, set_cwd, [PrivDir]),

    ok = rpc:call(Rabbit, application, unset_env, [mnesia, dir]),
    ok = rpc:call(Hare, application, unset_env, [mnesia, dir]),
    ok = rpc:call(Rabbit, mnesia, create_schema, [[Rabbit, Hare]]),
    ok = rpc:call(Rabbit, mnesia, start, []),
    {atomic,ok} = rpc:call(Rabbit, mnesia, create_table,
                                   [rabbit_queue, [{ram_copies, [Rabbit, Hare]}]]),
    stopped = rpc:call(Rabbit, mnesia, stop, []),
    ok = rpc:call(Rabbit, application, set_env, [mnesia, dir, RabbitMnesiaDir]),
    ok = rpc:call(Hare, application, set_env, [mnesia, dir, HareMnesiaDir]).

join_and_part_cluster(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Config, Rabbit, Bunny),
    assert_clustered([Rabbit, Bunny]),

    stop_join_start(Config, Hare, Bunny, true),
    assert_cluster_status(
      {[Bunny, Hare, Rabbit], [Bunny, Rabbit], [Bunny, Hare, Rabbit]},
      [Rabbit, Hare, Bunny]),

    %% Allow clustering with already clustered node
    ok = stop_app(Config, Rabbit),
    ok = join_cluster(Config, Rabbit, Hare),
    ok = start_app(Config, Rabbit),

    stop_reset_start(Config, Rabbit),
    assert_not_clustered(Rabbit),
    assert_cluster_status({[Bunny, Hare], [Bunny], [Bunny, Hare]},
                          [Hare, Bunny]),

    stop_reset_start(Config, Hare),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny).

stop_start_cluster_node(Config) ->
    [Rabbit, Hare] = cluster_members(Config),

    assert_clustered([Rabbit, Hare]),

    ok = stop_app(Config, Rabbit),
    ok = start_app(Config, Rabbit),

    assert_clustered([Rabbit, Hare]),

    ok = stop_app(Config, Hare),
    ok = start_app(Config, Hare),

    assert_clustered([Rabbit, Hare]).

restart_cluster_node(Config) ->
    [Rabbit, Hare] = cluster_members(Config),

    assert_clustered([Rabbit, Hare]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:start_node(Config, Hare),

    assert_clustered([Rabbit, Hare]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:start_node(Config, Rabbit),

    assert_clustered([Rabbit, Hare]).

join_and_part_cluster_in_khepri(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Config, Rabbit, Bunny),
    assert_clustered([Rabbit, Bunny]),

    stop_join_start(Config, Hare, Bunny),
    assert_clustered([Rabbit, Bunny, Hare]),

    %% Allow clustering with already clustered node
    ok = stop_app(Config, Rabbit),
    ?assertEqual(ok, join_cluster(Config, Rabbit, Hare)),
    ok = start_app(Config, Rabbit),

    assert_clustered([Rabbit, Bunny, Hare]),

    stop_reset_start(Config, Bunny),
    assert_not_clustered(Bunny),
    assert_clustered([Hare, Rabbit]),

    stop_reset_start(Config, Rabbit),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare).

join_cluster_bad_operations(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),

    UsePrelaunch = rabbit_ct_broker_helpers:rpc(
                     Config, Hare,
                     erlang, function_exported,
                     [rabbit_prelaunch, get_context, 0]),

    %% Nonexistent node
    ok = stop_app(Config, Rabbit),
    assert_failure(fun () -> join_cluster(Config, Rabbit, non@existent) end),
    ok = start_app(Config, Rabbit),
    assert_not_clustered(Rabbit),

    %% Trying to cluster with mnesia running
    assert_failure(fun () -> join_cluster(Config, Rabbit, Bunny) end),
    assert_not_clustered(Rabbit),

    %% Trying to cluster the node with itself
    ok = stop_app(Config, Rabbit),
    assert_failure(fun () -> join_cluster(Config, Rabbit, Rabbit) end),
    ok = start_app(Config, Rabbit),
    assert_not_clustered(Rabbit),

    %% Do not let the node leave the cluster or reset if it's the only
    %% ram node
    stop_join_start(Config, Hare, Rabbit, true),
    assert_cluster_status({[Rabbit, Hare], [Rabbit], [Rabbit, Hare]},
                          [Rabbit, Hare]),
    ok = stop_app(Config, Hare),
    assert_failure(fun () -> join_cluster(Config, Rabbit, Bunny) end),
    assert_failure(fun () -> reset(Config, Rabbit) end),
    ok = start_app(Config, Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit], [Rabbit, Hare]},
                          [Rabbit, Hare]),

    %% Cannot start RAM-only node first
    ok = stop_app(Config, Rabbit),
    ok = stop_app(Config, Hare),
    assert_failure(fun () -> start_app(Config, Hare) end),
    ok = start_app(Config, Rabbit),
    case UsePrelaunch of
        true ->
            ok = start_app(Config, Hare);
        false ->
            %% The Erlang VM has stopped after previous rabbit app failure
            ok = rabbit_ct_broker_helpers:start_node(Config, Hare)
    end,
    ok.

join_cluster_bad_operations_in_khepri(Config) ->
    [Rabbit, _Hare, Bunny] = cluster_members(Config),

    %% Nonexistent node
    ok = stop_app(Config, Rabbit),
    assert_failure(fun () -> join_cluster(Config, Rabbit, non@existent) end),
    ok = start_app(Config, Rabbit),
    assert_not_clustered(Rabbit),

    %% Trying to cluster with mnesia running
    assert_failure(fun () -> join_cluster(Config, Rabbit, Bunny) end),
    assert_not_clustered(Rabbit),

    %% Trying to cluster the node with itself
    ok = stop_app(Config, Rabbit),
    assert_failure(fun () -> join_cluster(Config, Rabbit, Rabbit) end),
    ok = start_app(Config, Rabbit),
    assert_not_clustered(Rabbit),

    ok.

%% This tests that the nodes in the cluster are notified immediately of a node
%% join, and not just after the app is started.
join_to_start_interval(Config) ->
    [Rabbit, Hare, _Bunny] = cluster_members(Config),

    ok = stop_app(Config, Rabbit),
    ok = join_cluster(Config, Rabbit, Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Rabbit, Hare]),
    ok = start_app(Config, Rabbit),
    assert_clustered([Rabbit, Hare]).

join_cluster_in_minority(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Config, Bunny, Rabbit),
    assert_clustered([Rabbit, Bunny]),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    ok = stop_app(Config, Hare),
    ?assertEqual(ok, join_cluster(Config, Hare, Rabbit, false)),

    ok = rabbit_ct_broker_helpers:start_node(Config, Bunny),
    ?assertEqual(ok, join_cluster(Config, Hare, Rabbit, false)),
    ?assertEqual(ok, start_app(Config, Hare)),

    assert_clustered([Rabbit, Bunny, Hare]).

join_cluster_with_rabbit_stopped(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Config, Bunny, Rabbit),
    assert_clustered([Rabbit, Bunny]),
    ok = stop_app(Config, Bunny),

    ok = stop_app(Config, Hare),
    ?assertEqual(ok, join_cluster(Config, Hare, Rabbit, false)),

    ok = start_app(Config, Bunny),
    ?assertEqual(ok, join_cluster(Config, Hare, Rabbit, false)),
    ?assertEqual(ok, start_app(Config, Hare)),

    assert_clustered([Rabbit, Bunny, Hare]).

forget_cluster_node(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),

    %% Trying to remove a node not in the cluster should fail
    assert_failure(fun () -> forget_cluster_node(Config, Hare, Rabbit) end),

    stop_join_start(Config, Rabbit, Hare),
    assert_clustered([Rabbit, Hare]),

    %% Trying to remove an online node should fail
    assert_failure(fun () -> forget_cluster_node(Config, Hare, Rabbit) end),

    ok = stop_app(Config, Rabbit),
    %% We're passing the --offline flag, but Hare is online
    assert_failure(fun () -> forget_cluster_node(Config, Hare, Rabbit, true) end),
    %% Removing some nonexistent node will fail
    assert_failure(fun () -> forget_cluster_node(Config, Hare, non@existent) end),
    ok = forget_cluster_node(Config, Hare, Rabbit),
    assert_not_clustered(Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Rabbit]),

    %% Now we can't start Rabbit since it thinks that it's still in the cluster
    %% with Hare, while Hare disagrees.
    assert_failure(fun () -> start_app(Config, Rabbit) end),

    ok = reset(Config, Rabbit),
    ok = start_app(Config, Rabbit),
    assert_not_clustered(Rabbit),

    %% Now we remove Rabbit from an offline node.
    stop_join_start(Config, Bunny, Hare),
    stop_join_start(Config, Rabbit, Hare),
    assert_clustered([Rabbit, Hare, Bunny]),
    ok = stop_app(Config, Hare),
    ok = stop_app(Config, Rabbit),
    ok = stop_app(Config, Bunny),
    %% This is fine but we need the flag
    assert_failure(fun () -> forget_cluster_node(Config, Hare, Bunny) end),
    %% Also fails because hare node is still running
    assert_failure(fun () -> forget_cluster_node(Config, Hare, Bunny, true) end),
    %% But this works
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = forget_cluster_node(Config, Hare, Bunny, true),
    ok = rabbit_ct_broker_helpers:start_node(Config, Hare),
    ok = start_app(Config, Rabbit),
    %% Bunny still thinks its clustered with Rabbit and Hare
    assert_failure(fun () -> start_app(Config, Bunny) end),
    ok = reset(Config, Bunny),
    ok = start_app(Config, Bunny),
    assert_not_clustered(Bunny),
    assert_clustered([Rabbit, Hare]).

forget_cluster_node_in_khepri(Config) ->
    [Rabbit, Hare, _Bunny] = cluster_members(Config),

    %% Trying to remove a node not in the cluster should fail
    assert_failure(fun () -> forget_cluster_node(Config, Hare, Rabbit) end),

    stop_join_start(Config, Rabbit, Hare),
    assert_clustered([Rabbit, Hare]),

    %% Trying to remove an online node should fail
    assert_failure(fun () -> forget_cluster_node(Config, Hare, Rabbit) end),

    ok = stop_app(Config, Rabbit),
    %% Removing some nonexistent node will fail
    assert_failure(fun () -> forget_cluster_node(Config, Hare, non@existent) end),
    ok = forget_cluster_node(Config, Hare, Rabbit),
    assert_not_clustered(Hare),

    ok = start_app(Config, Rabbit),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare).

unsupported_forget_cluster_node_offline(Config) ->
    [Rabbit, Hare] = cluster_members(Config),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = stop_app(Config, Rabbit),
    Ret0 = rabbit_ct_broker_helpers:rabbitmqctl(Config, Hare,
                                                ["forget_cluster_node", "--offline", Rabbit]),
    is_not_supported(Ret0).

forget_removes_things(Config) ->
    test_removes_things(Config, fun (R, H) -> ok = forget_cluster_node(Config, H, R) end).

reset_removes_things(Config) ->
    test_removes_things(Config, fun (R, _H) -> ok = reset(Config, R) end).

test_removes_things(Config, LoseRabbit) ->
    Unmirrored = <<"unmirrored-queue">>,
    [Rabbit, Hare | _] = cluster_members(Config),
    RCh = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    declare(RCh, Unmirrored),
    ok = stop_app(Config, Rabbit),

    HCh = rabbit_ct_client_helpers:open_channel(Config, Hare),
    {'EXIT',{{shutdown,{server_initiated_close,404,_}}, _}} =
        (catch declare(HCh, Unmirrored)),

    ok = LoseRabbit(Rabbit, Hare),
    HCh2 = rabbit_ct_client_helpers:open_channel(Config, Hare),
    declare(HCh2, Unmirrored),
    ok.

forget_node_in_khepri(Config) ->
    [Rabbit, Hare] = cluster_members(Config),

    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Rabbit, Hare]},
                          [Rabbit, Hare]),

    ok = stop_app(Config, Rabbit),
    ok = forget_cluster_node(Config, Hare, Rabbit),

    assert_cluster_status({[Hare], [Hare]}, [Hare]),

    ok.

forget_removes_things_in_khepri(Config) ->
    ClassicQueue = <<"classic-queue">>,
    [Rabbit, Hare | _] = cluster_members(Config),

    RCh = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    ?assertMatch(#'queue.declare_ok'{}, declare(RCh, ClassicQueue)),

    ok = stop_app(Config, Rabbit),
    ok = forget_cluster_node(Config, Hare, Rabbit),

    HCh = rabbit_ct_client_helpers:open_channel(Config, Hare),
    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       declare_passive(HCh, ClassicQueue)),

    ok.

forget_unavailable_node(Config) ->
    [Rabbit, Hare | _] = Nodes = cluster_members(Config),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ?assertMatch(ok, forget_cluster_node(Config, Hare, Rabbit)),

    NNodes = lists:nthtail(1, Nodes),

    assert_cluster_status({NNodes, NNodes}, NNodes).

forget_unavailable_node_in_minority(Config) ->
    All = [Rabbit, Hare, Bunny] = cluster_members(Config),

    assert_cluster_status({All, All}, All),

    %% Find out the raft status of the soon to be only
    %% running node
    RaftStatus = get_raft_status(Config, Hare),

    %% Stop other two nodes
    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),
    %% Wait until Mnesia has detected both nodes down
    ?awaitMatch(
       [Hare],
       rabbit_ct_broker_helpers:rpc(Config, Hare,
                                    rabbit_mnesia, cluster_nodes, [running]),
       30000),

    %% If Hare was the leader, it is able to forget one of the nodes. Change takes place as soon as it is written on the log. The other membership change will be rejected until the last change has consensus.
    ct:pal("Initial Raft status: ~p", [RaftStatus]),
    case RaftStatus of
        leader ->
            ?assertMatch(ok, forget_cluster_node(Config, Hare, Rabbit)),
            not_permitted(forget_cluster_node(Config, Hare, Bunny));
        follower ->
            %% Follower might have been promoted before the second node goes down, check the status again
            RaftStatus1 = get_raft_status(Config, Hare),
            ct:pal("Latest Raft status: ~p", [RaftStatus1]),
            case RaftStatus1 of
                leader ->
                    ?assertMatch(ok, forget_cluster_node(Config, Hare, Rabbit)),
                    not_permitted(forget_cluster_node(Config, Hare, Bunny));
                _ ->
                    is_in_minority(forget_cluster_node(Config, Hare, Rabbit))
            end
    end.

not_permitted(Ret) ->
    ?assertMatch({error, 69, _}, Ret),
    {error, _, Msg} = Ret,
    ?assertMatch(match, re:run(Msg, ".*not_permitted.*", [{capture, none}])).

get_raft_status(Config, Node) ->
    AllStatus = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_khepri, status, []),
    case lists:filter(fun(S) ->
                              proplists:get_value(<<"Node Name">>, S) == Node
                      end, AllStatus) of
        [NodeStatus] ->
            proplists:get_value(<<"Raft State">>, NodeStatus);
        [] ->
            unknown
    end.

reset_in_khepri(Config) ->
    ClassicQueue = <<"classic-queue">>,
    [Rabbit, Hare | _] = cluster_members(Config),

    RCh = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    ?assertMatch(#'queue.declare_ok'{}, declare(RCh, ClassicQueue)),

    stop_app(Config, Hare),
    ok = reset(Config, Hare),

    %% Rabbit is a 1-node cluster. The classic queue is still there.
    assert_cluster_status({[Rabbit], [Rabbit]}, [Rabbit]),
    ?assertMatch(#'queue.declare_ok'{}, declare_passive(RCh, ClassicQueue)),

    %% Can't reset a running node
    ?assertMatch({error, 64, _}, reset(Config, Rabbit)),

    %% Start Hare, it should work as standalone node.
    start_app(Config, Hare),

    assert_cluster_status({[Hare], [Hare]}, [Hare]),

    ok.

reset_removes_things_in_khepri(Config) ->
    ClassicQueue = <<"classic-queue">>,
    [Rabbit, Hare | _] = cluster_members(Config),

    RCh = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    ?assertMatch(#'queue.declare_ok'{}, declare(RCh, ClassicQueue)),

    stop_app(Config, Rabbit),
    ok = reset(Config, Rabbit),

    assert_cluster_status({[Hare], [Hare]}, [Hare]),

    start_app(Config, Rabbit),
    assert_cluster_status({[Rabbit], [Rabbit]}, [Rabbit]),

    %% The classic queue was declared in Rabbit, once that node is reset
    %% the queue needs to be removed from the rest of the cluster
    HCh = rabbit_ct_client_helpers:open_channel(Config, Hare),
    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       declare_passive(HCh, ClassicQueue)),

    ok.

reset_in_minority(Config) ->
    [Rabbit, Hare | _] = cluster_members(Config),

    rabbit_ct_broker_helpers:stop_node(Config, Hare),

    ok = rpc:call(Rabbit, application, set_env,
                  [rabbit, khepri_leader_wait_retry_timeout, 1000]),
    ok = rpc:call(Rabbit, application, set_env,
                  [rabbit, khepri_leader_wait_retry_limit, 3]),
    stop_app(Config, Rabbit),

    is_in_minority(reset(Config, Rabbit)),

    ok.

is_in_minority(Ret) ->
    ?assertMatch({error, 75, _}, Ret),
    {error, _, Msg} = Ret,
    ?assertMatch(match, re:run(Msg, ".*timed out.*minority.*", [{capture, none}])).

reset_last_disc_node(Config) ->
    Servers = [Rabbit, Hare | _] = cluster_members(Config),

    stop_app(Config, Hare),
    ?assertEqual(ok, change_cluster_node_type(Config, Hare, ram)),
    start_app(Config, Hare),

    case rabbit_ct_broker_helpers:enable_feature_flag(Config, Servers, khepri_db) of
        ok ->
            %% The reset works after the switch to Khepri because the RAM node was
            %% implicitly converted to a disc one as Khepri always writes data on disc.
            stop_app(Config, Rabbit),
            ?assertEqual(ok, reset(Config, Rabbit)),
            start_app(Config, Rabbit),
            assert_not_clustered(Rabbit),
            assert_not_clustered(Hare),
            ok;
        {skip, _} = Skip ->
            Skip
    end.

forget_offline_removes_things(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    Unmirrored = <<"unmirrored-queue">>,
    X = <<"X">>,
    RCh = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    declare(RCh, Unmirrored),

    amqp_channel:call(RCh, #'exchange.declare'{durable     = true,
                                               exchange    = X,
                                               auto_delete = true}),
    amqp_channel:call(RCh, #'queue.bind'{queue    = Unmirrored,
                                         exchange = X}),
    ok = rabbit_ct_broker_helpers:stop_broker(Config, Rabbit),

    HCh = rabbit_ct_client_helpers:open_channel(Config, Hare),
    {'EXIT',{{shutdown,{server_initiated_close,404,_}}, _}} =
        (catch declare(HCh, Unmirrored)),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = forget_cluster_node(Config, Hare, Rabbit, true),
    ok = rabbit_ct_broker_helpers:start_node(Config, Hare),

    HCh2 = rabbit_ct_client_helpers:open_channel(Config, Hare),
    declare(HCh2, Unmirrored),
    {'EXIT',{{shutdown,{server_initiated_close,404,_}}, _}} =
        (catch amqp_channel:call(HCh2,#'exchange.declare'{durable     = true,
                                                          exchange    = X,
                                                          auto_delete = true,
                                                          passive     = true})),
    ok.

set_ha_policy(Config, QName, Master, Slaves) ->
    Nodes = [list_to_binary(atom_to_list(N)) || N <- [Master | Slaves]],
    HaPolicy = {<<"nodes">>, Nodes},
    rabbit_ct_broker_helpers:set_ha_policy(Config, Master, QName, HaPolicy),
    await_followers(QName, Master, Slaves).

await_followers(QName, Master, Slaves) ->
    await_followers_0(QName, Master, Slaves, 10).

await_followers_0(QName, Master, Slaves0, Tries) ->
    {ok, Queue} = await_followers_lookup_queue(QName, Master),
    SPids = amqqueue:get_slave_pids(Queue),
    ActMaster = amqqueue:qnode(Queue),
    ActSlaves = lists:usort([node(P) || P <- SPids]),
    Slaves1 = lists:usort(Slaves0),
    await_followers_1(QName, ActMaster, ActSlaves, Master, Slaves1, Tries).

await_followers_1(QName, _ActMaster, _ActSlaves, _Master, _Slaves, 0) ->
    error({timeout_waiting_for_followers, QName});
await_followers_1(QName, ActMaster, ActSlaves, Master, Slaves, Tries) ->
    case {Master, Slaves} of
        {ActMaster, ActSlaves} ->
            ok;
        _                      ->
            timer:sleep(250),
            await_followers_0(QName, Master, Slaves, Tries - 1)
    end.

await_followers_lookup_queue(QName, Master) ->
    await_followers_lookup_queue(QName, Master, 10).

await_followers_lookup_queue(QName, _Master, 0) ->
    error({timeout_looking_up_queue, QName});
await_followers_lookup_queue(QName, Master, Tries) ->
    RpcArgs = [rabbit_misc:r(<<"/">>, queue, QName)],
    case rpc:call(Master, rabbit_amqqueue, lookup, RpcArgs) of
        {error, not_found} ->
            timer:sleep(250),
            await_followers_lookup_queue(QName, Master, Tries - 1);
        {ok, Q} ->
            {ok, Q}
    end.

force_boot(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    {error, _, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
      ["force_boot"]),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    {error, _} = rabbit_ct_broker_helpers:start_node(Config, Rabbit),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
      ["force_boot"]),
    ok = rabbit_ct_broker_helpers:start_node(Config, Rabbit),
    ok.

force_boot_in_khepri(Config) ->
    [Rabbit, _Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    stop_app(Config, Rabbit),
    %% It executes force boot for mnesia, currently Khepri does nothing
    ?assertMatch({ok, []}, rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit, ["force_boot"])),
    ok.

change_cluster_node_type(Config) ->
    [Rabbit, Hare, _Bunny] = cluster_members(Config),

    %% Trying to change the node to the ram type when not clustered should always fail
    ok = stop_app(Config, Rabbit),
    assert_failure(fun () -> change_cluster_node_type(Config, Rabbit, ram) end),
    ok = start_app(Config, Rabbit),

    ok = stop_app(Config, Rabbit),
    join_cluster(Config, Rabbit, Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Rabbit, Hare]),
    change_cluster_node_type(Config, Rabbit, ram),
    assert_cluster_status({[Rabbit, Hare], [Hare], [Rabbit, Hare], [Hare], [Hare]},
                          [Rabbit, Hare]),
    change_cluster_node_type(Config, Rabbit, disc),

    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Rabbit, Hare]),
    change_cluster_node_type(Config, Rabbit, ram),
    ok = start_app(Config, Rabbit),
    assert_cluster_status({[Rabbit, Hare], [Hare], [Hare, Rabbit]},
                          [Rabbit, Hare]),

    %% Changing to ram when you're the only ram node should fail
    ok = stop_app(Config, Hare),
    assert_failure(fun () -> change_cluster_node_type(Config, Hare, ram) end),
    ok = start_app(Config, Hare).

change_cluster_node_type_in_khepri(Config) ->
    [Rabbit, Hare] = cluster_members(Config),

    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Rabbit, Hare]},
                          [Rabbit, Hare]),

    ok = stop_app(Config, Rabbit),
    {error, 69, Msg} = change_cluster_node_type(Config, Rabbit, ram),
    ?assertEqual(
       match,
       re:run(
         Msg, "Feature `ram_node_type` is deprecated",
         [{capture, none}])),

    ok = change_cluster_node_type(Config, Rabbit, disc),
    ok = start_app(Config, Rabbit),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Rabbit, Hare]},
                          [Rabbit, Hare]).

change_cluster_when_node_offline(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),

    %% Cluster the three notes
    stop_join_start(Config, Rabbit, Hare),
    assert_clustered([Rabbit, Hare]),

    stop_join_start(Config, Bunny, Hare),
    assert_clustered([Rabbit, Hare, Bunny]),

    %% Bring down Rabbit, and remove Bunny from the cluster while
    %% Rabbit is offline
    ok = stop_app(Config, Rabbit),
    ok = stop_app(Config, Bunny),
    ok = reset(Config, Bunny),
    assert_cluster_status({[Bunny], [Bunny], []}, [Bunny]),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]}, [Hare]),
    assert_cluster_status(
      {[Rabbit, Hare, Bunny], [Hare], [Rabbit, Hare, Bunny], [Rabbit, Hare, Bunny], [Hare, Bunny]}, [Rabbit]),

    %% Bring Rabbit back up
    ok = start_app(Config, Rabbit),
    assert_clustered([Rabbit, Hare]),
    ok = start_app(Config, Bunny),
    assert_not_clustered(Bunny),

    %% Now the same, but Rabbit is a RAM node, and we bring up Bunny
    %% before
    ok = stop_app(Config, Rabbit),
    ok = change_cluster_node_type(Config, Rabbit, ram),
    ok = start_app(Config, Rabbit),
    stop_join_start(Config, Bunny, Hare),
    assert_cluster_status(
      {[Rabbit, Hare, Bunny], [Hare, Bunny], [Rabbit, Hare, Bunny]},
      [Rabbit, Hare, Bunny]),
    ok = stop_app(Config, Rabbit),
    ok = stop_app(Config, Bunny),
    ok = reset(Config, Bunny),
    ok = start_app(Config, Bunny),
    assert_not_clustered(Bunny),
    assert_cluster_status({[Rabbit, Hare], [Hare], [Hare]}, [Hare]),
    assert_cluster_status(
      {[Rabbit, Hare, Bunny], [Hare, Bunny], [Hare, Bunny]},
      [Rabbit]),
    ok = start_app(Config, Rabbit),
    assert_cluster_status({[Rabbit, Hare], [Hare], [Rabbit, Hare]},
                          [Rabbit, Hare]),
    assert_not_clustered(Bunny).

update_cluster_nodes(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),

    %% Mnesia is running...
    assert_failure(fun () -> update_cluster_nodes(Config, Rabbit, Hare) end),

    ok = stop_app(Config, Rabbit),
    ok = join_cluster(Config, Rabbit, Hare),
    ok = stop_app(Config, Bunny),
    ok = join_cluster(Config, Bunny, Hare),
    ok = start_app(Config, Bunny),
    stop_reset_start(Config, Hare),
    assert_failure(fun () -> start_app(Config, Rabbit) end),
    %% Bogus node
    assert_failure(fun () -> update_cluster_nodes(Config, Rabbit, non@existent) end),
    %% Inconsistent node
    assert_failure(fun () -> update_cluster_nodes(Config, Rabbit, Hare) end),
    ok = update_cluster_nodes(Config, Rabbit, Bunny),
    ok = start_app(Config, Rabbit),
    assert_not_clustered(Hare),
    assert_clustered([Rabbit, Bunny]).

unsupported_update_cluster_nodes(Config) ->
    [Rabbit, Hare] = cluster_members(Config),

    %% Mnesia is running...
    assert_failure(fun () -> update_cluster_nodes(Config, Rabbit, Hare) end),

    ok = stop_app(Config, Rabbit),
    Ret = update_cluster_nodes(Config, Rabbit, Hare),
    is_not_supported(Ret).

is_not_supported(Ret) ->
    ?assertMatch({error, _, _}, Ret),
    {error, _, Msg} = Ret,
    ?assertMatch(match, re:run(Msg, ".*not_supported.*", [{capture, none}])).

classic_config_discovery_node_list(Config) ->
    [Rabbit, Hare] = cluster_members(Config),

    ok = stop_app(Config, Hare),
    ok = reset(Config, Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, {[Rabbit], disc}]),
    ok = start_app(Config, Hare),
    assert_clustered([Rabbit, Hare]),

    ok = stop_app(Config, Hare),
    ok = reset(Config, Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, {[Rabbit], ram}]),
    ok = start_app(Config, Hare),
    assert_cluster_status({[Rabbit, Hare], [Rabbit], [Rabbit, Hare]},
                          [Rabbit, Hare]),

    %% List of nodes [node()] is equivalent to {[node()], disk}
    ok = stop_app(Config, Hare),
    ok = reset(Config, Hare),
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, [Rabbit]]),
    ok = start_app(Config, Hare),
    assert_clustered([Rabbit, Hare]),

    ok = stop_app(Config, Hare),
    ok = reset(Config, Hare),
    %% If we use an invalid cluster_nodes conf, the node fails to start.
    ok = rpc:call(Hare, application, set_env,
                  [rabbit, cluster_nodes, "Yes, please"]),
    assert_failure(fun () -> start_app(Config, Hare) end),
    assert_not_clustered(Rabbit).

force_reset_node(Config) ->
    [Rabbit, Hare, _Bunny] = cluster_members(Config),

    stop_join_start(Config, Rabbit, Hare),
    stop_app(Config, Rabbit),
    force_reset(Config, Rabbit),
    %% Hare thinks that Rabbit is still clustered
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Hare]),
    %% %% ...but it isn't
    assert_cluster_status({[Rabbit], [Rabbit], []}, [Rabbit]),
    %% We can rejoin Rabbit and Hare
    update_cluster_nodes(Config, Rabbit, Hare),
    start_app(Config, Rabbit),
    assert_clustered([Rabbit, Hare]).

force_reset_node_in_khepri(Config) ->
    [Rabbit, Hare, _Bunny] = cluster_members(Config),

    stop_join_start(Config, Rabbit, Hare),
    stop_app(Config, Rabbit),
    ok = force_reset(Config, Rabbit),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]}, [Hare]),
    %% Khepri is stopped, so it won't report anything.
    assert_status({[Rabbit], [], [Rabbit], [Rabbit], []}, [Rabbit]),
    %% Hare thinks that Rabbit is still clustered
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Hare]},
                          [Hare]),
    ok = start_app(Config, Rabbit),
    assert_not_clustered(Rabbit),
    %% We can rejoin Rabbit and Hare. Unlike with Mnesia, we try to solve the
    %% inconsistency instead of returning an error.
    ok = stop_app(Config, Rabbit),
    ?assertEqual(ok, join_cluster(Config, Rabbit, Hare, false)),
    ok = start_app(Config, Rabbit),
    assert_cluster_status({[Rabbit, Hare], [Rabbit, Hare], [Rabbit, Hare]},
                          [Rabbit, Hare]).

status_with_alarm(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),

    %% Given: an alarm is raised each node.
    rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
      ["set_vm_memory_high_watermark", "0.000000001"]),
    rabbit_ct_broker_helpers:rabbitmqctl(Config, Hare,
      ["set_disk_free_limit", "2048G"]),

    %% When: we ask for alarm status
    S = rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                          rabbit_alarm, get_alarms, []),
    R = rabbit_ct_broker_helpers:rpc(Config, Hare,
                                          rabbit_alarm, get_alarms, []),

    %% Then: both nodes have printed alarm information for eachother.
    ok = alarm_information_on_each_node(S, Rabbit, Hare),
    ok = alarm_information_on_each_node(R, Rabbit, Hare).

alarm_information_on_each_node(Result, Rabbit, Hare) ->
    %% Example result:
    %% [{{resource_limit,disk,'rmq-ct-status_with_alarm-2-24240@localhost'},
    %%           []},
    %% {{resource_limit,memory,'rmq-ct-status_with_alarm-1-24120@localhost'},
    %%           []}]
    Alarms = [A || {A, _} <- Result],
    ?assert(lists:member({resource_limit, memory, Rabbit}, Alarms)),
    ?assert(lists:member({resource_limit, disk, Hare}, Alarms)),

    ok.

pid_file_and_await_node_startup(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    RabbitConfig = rabbit_ct_broker_helpers:get_node_config(Config,Rabbit),
    RabbitPidFile = ?config(pid_file, RabbitConfig),
    %% ensure pid file is readable
    {ok, _} = file:read_file(RabbitPidFile),
    %% ensure wait works on running node
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
      ["wait", RabbitPidFile]),
    %% stop both nodes
    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    %% starting first node fails - it was not the last node to stop
    {error, _} = rabbit_ct_broker_helpers:start_node(Config, Rabbit),
    PreviousPid = pid_from_file(RabbitPidFile),
    %% start first node in the background
    spawn_link(fun() ->
        rabbit_ct_broker_helpers:start_node(Config, Rabbit)
    end),
    Attempts = 200,
    Timeout = 50,
    wait_for_pid_file_to_change(RabbitPidFile, PreviousPid, Attempts, Timeout),
    {error, _, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
      ["wait", RabbitPidFile]).

pid_file_and_await_node_startup_in_khepri(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    RabbitConfig = rabbit_ct_broker_helpers:get_node_config(Config,Rabbit),
    RabbitPidFile = ?config(pid_file, RabbitConfig),
    %% ensure pid file is readable
    {ok, _} = file:read_file(RabbitPidFile),
    %% ensure wait works on running node
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
      ["wait", RabbitPidFile]),
    %% stop both nodes
    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    %% start first node in the background. It will wait for Khepri
    %% and then Mnesia tables (which will already be available)
    spawn_link(fun() ->
        rabbit_ct_broker_helpers:start_node(Config, Rabbit)
    end),
    PreviousPid = pid_from_file(RabbitPidFile),
    Attempts = 200,
    Timeout = 50,
    wait_for_pid_file_to_change(RabbitPidFile, PreviousPid, Attempts, Timeout),
    %% The node is blocked waiting for Khepri, so this will timeout. Mnesia
    %% alone would fail here as it wasn't the last node to stop
    %% Let's make it a short wait.
    {error, timeout, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
                                                               ["wait", RabbitPidFile], 10000).

await_running_count(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    RabbitConfig = rabbit_ct_broker_helpers:get_node_config(Config,Rabbit),
    RabbitPidFile = ?config(pid_file, RabbitConfig),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
      ["wait", RabbitPidFile]),
    %% stop both nodes
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    %% start one node in the background
    rabbit_ct_broker_helpers:start_node(Config, Rabbit),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
                                                   ["wait", RabbitPidFile]),
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                                  rabbit_nodes,
                                                  await_running_count, [1, 30000])),
    ?assertEqual({error, timeout},
                 rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                              rabbit_nodes,
                                              await_running_count, [2, 1000])),
    ?assertEqual({error, timeout},
                 rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                              rabbit_nodes,
                                              await_running_count, [5, 1000])),
    rabbit_ct_broker_helpers:start_node(Config, Hare),
    %% this now succeeds
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                                  rabbit_nodes,
                                                  await_running_count, [2, 30000])),
    %% this still succeeds
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                                  rabbit_nodes,
                                                  await_running_count, [1, 30000])),
    %% this still fails
    ?assertEqual({error, timeout},
                 rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                              rabbit_nodes,
                                              await_running_count, [5, 1000])).

await_running_count_in_khepri(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    RabbitConfig = rabbit_ct_broker_helpers:get_node_config(Config,Rabbit),
    RabbitPidFile = ?config(pid_file, RabbitConfig),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
      ["wait", RabbitPidFile]),
    %% stop both nodes
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    %% start one node in the background
    %% One khepri node in minority won't finish starting up, but will wait a reasonable
    %% amount of time for a new leader to be elected. Hopefully on that time
    %% a second (or more) node is brought up so they can reach consensus
    %% Kind of similar to the wait for tables that we had on mnesia
    rabbit_ct_broker_helpers:async_start_node(Config, Rabbit),
    rabbit_ct_broker_helpers:start_node(Config, Hare),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
                                                   ["wait", RabbitPidFile]),
    %% this now succeeds
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                                  rabbit_nodes,
                                                  await_running_count, [2, 30000])),
    %% this still succeeds
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                                  rabbit_nodes,
                                                  await_running_count, [1, 30000])),
    %% this still fails
    ?assertEqual({error, timeout},
                 rabbit_ct_broker_helpers:rpc(Config, Rabbit,
                                              rabbit_nodes,
                                              await_running_count, [5, 1000])).

start_nodes_in_reverse_order(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Config, Rabbit, Bunny),
    stop_join_start(Config, Hare, Bunny),
    assert_clustered([Rabbit, Hare, Bunny]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    spawn(fun() -> ok = rabbit_ct_broker_helpers:start_node(Config, Bunny) end),
    ok = rabbit_ct_broker_helpers:start_node(Config, Hare),
    assert_cluster_status({[Bunny, Hare, Rabbit], [Bunny, Hare, Rabbit], [Bunny, Hare]},
                          [Bunny, Hare]),

    ok = rabbit_ct_broker_helpers:start_node(Config, Rabbit),
    assert_clustered([Rabbit, Hare, Bunny]).

%% Test booting nodes in the wrong order for Mnesia. Interesting...
start_nodes_in_stop_order(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Config, Rabbit, Bunny),
    stop_join_start(Config, Hare, Bunny),
    assert_clustered([Rabbit, Hare, Bunny]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    Self = self(),
    spawn(fun() ->
                  Reply = rabbit_ct_broker_helpers:start_node(Config, Rabbit),
                  Self ! {start_node_reply, Reply}
          end),
    ?assertMatch({error, {skip, _}}, rabbit_ct_broker_helpers:start_node(Config, Hare)),
    receive
        {start_node_reply, Reply} ->
            ?assertMatch({error, {skip, _}}, Reply)
    end.

start_nodes_in_stop_order_in_khepri(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Config, Rabbit, Bunny),
    stop_join_start(Config, Hare, Bunny),
    assert_clustered([Rabbit, Hare, Bunny]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    ok = rabbit_ct_broker_helpers:async_start_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:async_start_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:async_start_node(Config, Bunny),

    ?assertMatch(ok, rabbit_ct_broker_helpers:wait_for_async_start_node(Rabbit)),
    ?assertMatch(ok, rabbit_ct_broker_helpers:wait_for_async_start_node(Hare)),
    ?assertMatch(ok, rabbit_ct_broker_helpers:wait_for_async_start_node(Bunny)).

%% TODO test force_boot with Khepri involved
start_nodes_in_stop_order_with_force_boot(Config) ->
    [Rabbit, Hare, Bunny] = cluster_members(Config),
    assert_not_clustered(Rabbit),
    assert_not_clustered(Hare),
    assert_not_clustered(Bunny),

    stop_join_start(Config, Rabbit, Bunny),
    stop_join_start(Config, Hare, Bunny),
    assert_clustered([Rabbit, Hare, Bunny]),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Rabbit),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    {ok, []} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Rabbit,
                                                    ["force_boot"]),

    spawn(fun() -> rabbit_ct_broker_helpers:start_node(Config, Rabbit) end),
    ok = rabbit_ct_broker_helpers:start_node(Config, Hare),
    assert_cluster_status({[Bunny, Hare, Rabbit], [Bunny, Hare, Rabbit], [Rabbit, Hare]},
                          [Rabbit, Hare]),

    ok = rabbit_ct_broker_helpers:start_node(Config, Bunny),
    assert_clustered([Rabbit, Hare, Bunny]).

%% ----------------------------------------------------------------------------
%% Internal utils
%% ----------------------------------------------------------------------------

wait_for_pid_file_to_change(_, 0, _, _) ->
    error(timeout_waiting_for_pid_file_to_have_running_pid);
wait_for_pid_file_to_change(PidFile, PreviousPid, Attempts, Timeout) ->
    Pid = pid_from_file(PidFile),
    case Pid =/= undefined andalso Pid =/= PreviousPid of
        true  -> ok;
        false ->
            ct:sleep(Timeout),
            wait_for_pid_file_to_change(PidFile,
                                        PreviousPid,
                                        Attempts - 1,
                                        Timeout)
    end.

pid_from_file(PidFile) ->
    case file:read_file(PidFile) of
        {ok, Content} ->
            string:strip(binary_to_list(Content), both, $\n);
        {error, enoent} ->
            undefined
    end.

cluster_members(Config) ->
    rabbit_ct_broker_helpers:get_node_configs(Config, nodename).

assert_failure(Fun) ->
    case catch Fun() of
        {error, _Code, Reason}         -> Reason;
        {error, Reason}                -> Reason;
        {error_string, Reason}         -> Reason;
        {badrpc, {'EXIT', Reason}}     -> Reason;
        %% Failure to start an app result in node shutdown
        {badrpc, nodedown}             -> nodedown;
        {badrpc_multi, Reason, _Nodes} -> Reason;
        Other                          -> error({expected_failure, Other})
    end.

stop_app(Config, Node) ->
    case rabbit_ct_broker_helpers:rabbitmqctl(Config, Node, ["stop_app"]) of
        {ok, _} -> ok;
        Error   -> Error
    end.

start_app(Config, Node) ->
    case rabbit_ct_broker_helpers:rabbitmqctl(Config, Node, ["start_app"]) of
        {ok, _} -> ok;
        Error   -> Error
    end.

join_cluster(Config, Node, To) ->
    join_cluster(Config, Node, To, false).

join_cluster(Config, Node, To, Ram) ->
    Cmd = case Ram of
              true ->
                  ["join_cluster", "--ram", atom_to_list(To)];
              false ->
                  ["join_cluster", atom_to_list(To)]
          end,
    case rabbit_ct_broker_helpers:rabbitmqctl(Config, Node, Cmd) of
        {ok, _} -> ok;
        Error   -> Error
    end.

reset(Config, Node) ->
    case rabbit_ct_broker_helpers:rabbitmqctl(Config, Node, ["reset"]) of
        {ok, _} -> ok;
        Error   -> Error
    end.

force_reset(Config, Node) ->
    Ret = rabbit_ct_broker_helpers:rabbitmqctl(
            Config, Node, ["force_reset"]),
    case Ret of
        {ok, _} -> ok;
        Error   -> Error
    end.

forget_cluster_node(Config, Node, Removee, RemoveWhenOffline) ->
    Cmd = case RemoveWhenOffline of
              true ->
                  ["forget_cluster_node", "--offline",
                   atom_to_list(Removee)];
              false ->
                  ["forget_cluster_node",
                   atom_to_list(Removee)]
          end,
    case rabbit_ct_broker_helpers:rabbitmqctl(Config, Node, Cmd) of
        {ok, _} -> ok;
        Error   -> Error
    end.

forget_cluster_node(Config, Node, Removee) ->
    forget_cluster_node(Config, Node, Removee, false).

change_cluster_node_type(Config, Node, Type) ->
    Ret = rabbit_ct_broker_helpers:rabbitmqctl(
            Config, Node, ["change_cluster_node_type", atom_to_list(Type)]),
    case Ret of
        {ok, _} -> ok;
        Error   -> Error
    end.

update_cluster_nodes(Config, Node, DiscoveryNode) ->
    Ret = rabbit_ct_broker_helpers:rabbitmqctl(
            Config, Node,
            ["update_cluster_nodes", atom_to_list(DiscoveryNode)]),
    case Ret of
        {ok, _} -> ok;
        Error   -> Error
    end.

stop_join_start(Config, Node, ClusterTo, Ram) ->
    ok = stop_app(Config, Node),
    ok = join_cluster(Config, Node, ClusterTo, Ram),
    ok = start_app(Config, Node).

stop_join_start(Config, Node, ClusterTo) ->
    stop_join_start(Config, Node, ClusterTo, false).

stop_reset_start(Config, Node) ->
    ok = stop_app(Config, Node),
    ok = reset(Config, Node),
    ok = start_app(Config, Node).

declare(Ch, Name) ->
    Res = amqp_channel:call(Ch, #'queue.declare'{durable = true,
                                                 queue   = Name}),
    amqp_channel:call(Ch, #'queue.bind'{queue    = Name,
                                        exchange = <<"amq.fanout">>}),
    Res.

declare_passive(Ch, Name) ->
    amqp_channel:call(Ch, #'queue.declare'{durable = true,
                                           passive = true,
                                           queue   = Name}).
