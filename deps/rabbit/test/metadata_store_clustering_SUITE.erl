%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(metadata_store_clustering_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
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

         join_khepri_khepri_cluster/1,
         join_mnesia_khepri_cluster/1,
         join_mnesia_khepri_cluster_reverse/1,
         join_khepri_mnesia_cluster/1,
         join_khepri_mnesia_cluster_reverse/1,

         join_khepri_khepri_khepri_cluster/1,
         join_mnesia_khepri_khepri_cluster/1,
         join_mnesia_khepri_khepri_cluster_reverse/1,
         join_khepri_mnesia_khepri_cluster/1,
         join_khepri_mnesia_khepri_cluster_reverse/1,
         join_khepri_khepri_mnesia_cluster/1,
         join_khepri_khepri_mnesia_cluster_reverse/1,
         join_mnesia_mnesia_khepri_cluster/1,
         join_mnesia_mnesia_khepri_cluster_reverse/1,
         join_mnesia_khepri_mnesia_cluster/1,
         join_mnesia_khepri_mnesia_cluster_reverse/1,
         join_khepri_mnesia_mnesia_cluster/1,
         join_khepri_mnesia_mnesia_cluster_reverse/1,

         join_khepri_while_in_minority/1
        ]).

suite() ->
    [{timetrap, 5 * 60_000}].

all() ->
    [
      {group, unclustered}
    ].

groups() ->
    [
     {unclustered, [], [{cluster_size_2, [], cluster_size_2_tests()},
                        {cluster_size_3, [], cluster_size_3_tests()},
                        {cluster_size_5, [], cluster_size_5_tests()}]}
    ].

cluster_size_2_tests() ->
    [
     join_khepri_khepri_cluster,
     join_mnesia_khepri_cluster,
     join_mnesia_khepri_cluster_reverse,
     join_khepri_mnesia_cluster,
     join_khepri_mnesia_cluster_reverse
    ].

cluster_size_3_tests() ->
    [
     join_khepri_khepri_khepri_cluster,
     join_mnesia_khepri_khepri_cluster,
     join_mnesia_khepri_khepri_cluster_reverse,
     join_khepri_mnesia_khepri_cluster,
     join_khepri_mnesia_khepri_cluster_reverse,
     join_khepri_khepri_mnesia_cluster,
     join_khepri_khepri_mnesia_cluster_reverse,
     join_mnesia_mnesia_khepri_cluster,
     join_mnesia_mnesia_khepri_cluster_reverse,
     join_mnesia_khepri_mnesia_cluster,
     join_mnesia_khepri_mnesia_cluster_reverse,
     join_khepri_mnesia_mnesia_cluster,
     join_khepri_mnesia_mnesia_cluster_reverse
    ].

cluster_size_5_tests() ->
    [
     join_khepri_while_in_minority
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            %% Khepri is not yet compatible with mixed version testing and this
            %% suite enables Khepri.
            {skip, "This suite does not yet support mixed version testing"};
        false ->
            rabbit_ct_helpers:run_setup_steps(Config, [])
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(unclustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, mnesia},
                                          {rmq_nodes_clustered, false},
                                          {tcp_ports_base},
                                          {net_ticktime, 10}]);
init_per_group(cluster_size_2, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 2}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]);
init_per_group(cluster_size_5, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 5}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, Testcase},
                                            {queue_name, Q}
                                           ]),
    Config2 = rabbit_ct_helpers:testcase_started(Config1, Testcase),
    rabbit_ct_helpers:run_steps(Config2,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                    rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
                                          rabbit_ct_client_helpers:teardown_steps() ++
                                              rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

join_khepri_khepri_cluster(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag( Config, Servers, khepri_db),
    case Ret of
        ok               -> join_size_2_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_khepri_mnesia_cluster(Config) ->
    [Server0, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0], khepri_db),
    case Ret of
        ok               -> join_size_2_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_khepri_mnesia_cluster_reverse(Config) ->
    [Server0, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0], khepri_db),
    case Ret of
        ok               -> join_size_2_cluster(Config, lists:reverse(Servers));
        {skip, _} = Skip -> Skip
    end.

join_mnesia_khepri_cluster(Config) ->
    [_, Server1] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1], khepri_db),
    case Ret of
        ok               -> join_size_2_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_mnesia_khepri_cluster_reverse(Config) ->
    [_, Server1] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1], khepri_db),
    case Ret of
        ok               -> join_size_2_cluster(Config, lists:reverse(Servers));
        {skip, _} = Skip -> Skip
    end.

join_khepri_khepri_khepri_cluster(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, Servers, khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_mnesia_khepri_khepri_cluster(Config) ->
    [_, Server1, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1, Server2], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_mnesia_khepri_khepri_cluster_reverse(Config) ->
    [_, Server1, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1, Server2], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, lists:reverse(Servers));
        {skip, _} = Skip -> Skip
    end.

join_khepri_mnesia_khepri_cluster(Config) ->
    [Server0, _, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0, Server2], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_khepri_mnesia_khepri_cluster_reverse(Config) ->
    [Server0, _, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0, Server2], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, lists:reverse(Servers));
        {skip, _} = Skip -> Skip
    end.

join_khepri_khepri_mnesia_cluster(Config) ->
    [Server0, Server1, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0, Server1], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_khepri_khepri_mnesia_cluster_reverse(Config) ->
    [Server0, Server1, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0, Server1], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, lists:reverse(Servers));
        {skip, _} = Skip -> Skip
    end.

join_mnesia_mnesia_khepri_cluster(Config) ->
    [_, _, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server2], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_mnesia_mnesia_khepri_cluster_reverse(Config) ->
    [_, _, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server2], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, lists:reverse(Servers));
        {skip, _} = Skip -> Skip
    end.

join_mnesia_khepri_mnesia_cluster(Config) ->
    [_, Server1, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_mnesia_khepri_mnesia_cluster_reverse(Config) ->
    [_, Server1, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, lists:reverse(Servers));
        {skip, _} = Skip -> Skip
    end.

join_khepri_mnesia_mnesia_cluster(Config) ->
    [Server0, _, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, Servers);
        {skip, _} = Skip -> Skip
    end.

join_khepri_mnesia_mnesia_cluster_reverse(Config) ->
    [Server0, _, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ret = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0], khepri_db),
    case Ret of
        ok               -> join_size_3_cluster(Config, lists:reverse(Servers));
        {skip, _} = Skip -> Skip
    end.

join_size_2_cluster(Config, [Server0, Server1]) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q)),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    ok = rabbit_control_helper:command(stop_app, Server1),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    Ret = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    case Ret of
        ok ->
            ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

            ok = rabbit_control_helper:command(start_app, Server1),
            ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, []));
        {error, 69, <<"Error:\nincompatible_feature_flags">>} ->
            {skip, "'khepri_db' feature flag is unsupported"}
    end.

join_size_3_cluster(Config, [Server0, Server1, Server2]) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q)),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    ok = rabbit_control_helper:command(stop_app, Server1),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    Ret1 = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    case Ret1 of
        ok ->
            ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

            ok = rabbit_control_helper:command(start_app, Server1),
            ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

            ok = rabbit_control_helper:command(stop_app, Server2),
            ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

            Ret2 = rabbit_control_helper:command(join_cluster, Server2, [atom_to_list(Server0)], []),
            case Ret2 of
                ok ->
                    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

                    ok = rabbit_control_helper:command(start_app, Server2),
                    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, []));
                {error, 69, <<"Error:\nincompatible_feature_flags">>} ->
                    {skip, "'khepri_db' feature flag is unsupported"}
            end;
        {error, 69, <<"Error:\nincompatible_feature_flags">>} ->
            {skip, "'khepri_db' feature flag is unsupported"}
    end.

declare(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = []}).

join_khepri_while_in_minority(Config) ->
    [Node1 | ClusteredNodes] = rabbit_ct_broker_helpers:get_node_configs(
                                 Config, nodename),
    [NodeToJoin | OtherNodes] = ClusteredNodes,

    %% Cluster nodes 2 to 5.
    ct:pal("Cluster nodes ~p", [ClusteredNodes]),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 ok,
                 rabbit_control_helper:command(
                   join_cluster, Node, [atom_to_list(NodeToJoin)], []))
      end, OtherNodes),
    lists:foreach(
      fun(Node) ->
              ?awaitMatch(
                 ClusteredNodes,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(
                     Config, Node, rabbit_nodes, list_members, [])),
                 30000)
      end, ClusteredNodes),

    %% Enable Khepri on all nodes. Only `Node2' is given here because it is
    %% clustered with `OtherNodes'.
    ct:pal("Enable `khepri_db` on nodes ~0p and ~0p", [Node1, NodeToJoin]),
    Ret1 = rabbit_ct_broker_helpers:enable_feature_flag(
             Config, [Node1, NodeToJoin], khepri_db),
    case Ret1 of
        ok ->
            StoreId = rabbit_khepri:get_store_id(),
            LeaderId = rabbit_ct_broker_helpers:rpc(
                         Config, NodeToJoin,
                         ra_leaderboard, lookup_leader, [StoreId]),
            {StoreId, LeaderNode} = LeaderId,

            %% Stop all clustered nodes except one follower to create a
            %% minority. In other words, we stop two followers, then the
            %% leader.
            %%
            %% Using `lists:reverse/1', we keep the last running followe only
            %% to see how clustering works if the first nodes in the cluster
            %% are down.
            Followers = ClusteredNodes -- [LeaderNode],
            [FollowerToKeep | FollowersToStop] = lists:reverse(Followers),

            lists:foreach(
              fun(Node) ->
                      ct:pal("Stop node ~0p", [Node]),
                      ok = rabbit_ct_broker_helpers:stop_node(Config, Node)
              end, FollowersToStop ++ [LeaderNode]),

            %% Try and fail to cluster `Node1' with the others.
            ct:pal("Try to cluster node ~0p with ~0p", [Node1, FollowerToKeep]),
            Ret2 = rabbit_control_helper:command(
                     join_cluster, Node1, [atom_to_list(FollowerToKeep)], []),
            ?assertMatch({error, 75, _}, Ret2),
            {error, _, Msg} = Ret2,
            ?assertEqual(
               match,
               re:run(
                 Msg, "Khepri cluster could be in minority",
                 [{capture, none}])),

            %% `Node1' should still be up and running correctly.
            ct:pal("Open a connection + channel to node ~0p", [Node1]),
            {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(
                           Config, Node1),

            QName = atom_to_binary(?FUNCTION_NAME),
            QArgs = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
            ct:pal("Declare queue ~0p", [QName]),
            amqp_channel:call(
              Ch, #'queue.declare'{durable = true,
                                   queue = QName,
                                   arguments = QArgs}),

            ct:pal("Enable publish confirms"),
            amqp_channel:call(Ch, #'confirm.select'{}),

            ct:pal("Publish a message to queue ~0p", [QName]),
            amqp_channel:cast(
              Ch,
              #'basic.publish'{routing_key = QName},
              #amqp_msg{props = #'P_basic'{delivery_mode = 2}}),
            amqp_channel:wait_for_confirms(Ch),

            ct:pal("Subscribe to queue ~0p", [QName]),
            CTag = <<"ctag">>,
            amqp_channel:subscribe(
              Ch,
              #'basic.consume'{queue = QName,
                               consumer_tag = CTag},
              self()),
            receive
                #'basic.consume_ok'{consumer_tag = CTag} ->
                    ok
            after 10000 ->
                      exit(consume_ok_timeout)
            end,

            ct:pal("Consume a message from queue ~0p", [QName]),
            receive
                {#'basic.deliver'{consumer_tag = <<"ctag">>}, _} ->
                    ok
            after 10000 ->
                      exit(deliver_timeout)
            end,

            ct:pal("Close channel + connection"),
            rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

            ok;
        {skip, _} = Skip ->
            Skip
    end.
