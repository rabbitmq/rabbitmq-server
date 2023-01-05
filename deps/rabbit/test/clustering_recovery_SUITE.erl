%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(clustering_recovery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

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
                         {clustered_3_nodes, [],
                          [{cluster_size_3, [], [
                                                 force_shrink_quorum_queue,
                                                 force_shrink_all_quorum_queues
                                                ]}
                          ]}
                        ]},
     {khepri_store, [], [
                         {clustered_3_nodes, [],
                          [{cluster_size_3, [], [
                                                 force_standalone_boot,
                                                 force_standalone_boot_and_restart,
                                                 force_standalone_boot_and_restart_with_quorum_queues
                                                ]}
                          ]},
                         {clustered_5_nodes, [],
                          [{cluster_size_5, [], [
                                                 rolling_restart,
                                                 rolling_kill_restart,
                                                 forget_down_node
                                                ]}]
                         }
                        ]}
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
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(khepri_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, khepri}]);
init_per_group(mnesia_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, mnesia}]);
init_per_group(clustered_3_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(clustered_5_nodes, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]);
init_per_group(cluster_size_5, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 5}]).

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
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases
%% -------------------------------------------------------------------

force_shrink_all_quorum_queues(Config) ->
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    
    QName1 = quorum_queue_name(1),
    QName2 = quorum_queue_name(2),
    QName3 = quorum_queue_name(3),
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    declare_and_publish_to_queue(Config, Rabbit, QName1, Args),
    declare_and_publish_to_queue(Config, Rabbit, QName2, Args),
    declare_and_publish_to_queue(Config, Rabbit, QName3, Args),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    ?assertExit(
       {{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
       amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName1,
                                                   consumer_tag = <<"ctag">>},
                              self())),
    
    ok = rabbit_ct_broker_helpers:rpc(Config, Rabbit, rabbit_quorum_queue, force_all_queues_shrink_member_to_current_member, []),

    ok = consume_from_queue(Config, Rabbit, QName1),
    ok = consume_from_queue(Config, Rabbit, QName2),
    ok = consume_from_queue(Config, Rabbit, QName3),

    ok.

force_shrink_quorum_queue(Config) ->
    [Rabbit, Hare, Bunny] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    
    QName1 = quorum_queue_name(1),
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    declare_and_publish_to_queue(Config, Rabbit, QName1, Args),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Hare),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Bunny),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    ?assertExit(
       {{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
       amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName1,
                                                   consumer_tag = <<"ctag">>},
                              self())),
    
    ok = rabbit_ct_broker_helpers:rpc(Config, Rabbit, rabbit_quorum_queue, force_shrink_member_to_current_member, [<<"/">>, QName1]),

    ok = consume_from_queue(Config, Rabbit, QName1).

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
    after 10000 ->
            exit(consume_ok_timeout)
    end.

consume(0) ->
    ok;
consume(N) ->
    receive
        {#'basic.deliver'{consumer_tag = <<"ctag">>}, _} ->
            consume(N - 1)
    after 10000 ->
            exit(deliver_timeout)
    end.
