%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqp_connection_uniqueness_partitions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp10_common/include/amqp10_sole_conn.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_amqp_util,
        [has_capability/2]).

-import(rabbit_ct_broker_helpers,
        [enable_feature_flag/2,
         rpc/5]).

-define(SOLE_CONN_MOD, rabbit_amqp_sole_conn).
-define(NODE_COUNT, 3).
-define(NET_TICKTIME_S, 5).

all() ->
    [{group, cluster}].

groups() ->
    [{cluster, [],
      [refuse_connection_accepts_conflicting_id_on_majority_side_after_partition,
       refuse_connection_accepts_conflicting_id_on_majority_side_after_leader_partition]}].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "mixed version clusters are not supported"};
        _ ->
            {ok, _} = application:ensure_all_started(amqp10_client),
            rabbit_ct_helpers:log_environment(),
            Config
    end.

end_per_suite(Config) ->
    Config.

init_per_group(cluster, Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, ?NODE_COUNT},
                         {rmq_nodes_clustered, true},
                         {rmq_nodename_suffix, Suffix},
                         {net_ticktime, ?NET_TICKTIME_S}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                [fun rabbit_ct_broker_helpers:configure_dist_proxy/1] ++
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),

    case enable_feature_flag(Config2, 'rabbitmq_4.4.0') of
        ok ->
            Config2;
        _ ->
            tear_down_cluster(Config2),
            {skip, "sole conn feature not available cluster-wide"}
    end.

end_per_group(_, Config) ->
    tear_down_cluster(Config).

tear_down_cluster(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

%% -------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------

%% Isolates a sole_conn Raft *follower* by network partition (the leader
%% stays reachable by the other two nodes, so no re-election is needed).
%% A connection with a given container-id is established on the
%% soon-to-be-isolated node, then, once isolated, a *conflicting* connection
%% with the same container-id is established on the majority side. Because
%% the majority side cannot reach the isolated node to verify the first
%% connection is still alive, it treats it as dead and lets the new
%% connection through: both connections are live simultaneously, a genuine
%% split-brain. Once the partition heals, the isolated node catches up on
%% the Raft log and the sole_conn kill trigger (registered on all Raft
%% members) fires there too, closing the stale first connection.
refuse_connection_accepts_conflicting_id_on_majority_side_after_partition(Config) ->
    AllNodes = all_nodes(Config),

    %% Eagerly bootstrap the sole_conn Khepri store on all nodes, instead of
    %% relying on the lazy bootstrap triggered by the first `acquire'
    ct:pal("Starting the sole_conn Khepri store on ~p", [AllNodes]),
    StartResult = rpc(Config, 0, ?SOLE_CONN_MOD, start, []),
    ?assertEqual(lists:sort([{N, ok} || N <- AllNodes]), lists:sort(StartResult)),

    {LeaderNode, FollowerNodes} = leader_and_followers(Config),
    Isolated = hd(FollowerNodes),
    [MajorityA, MajorityB] = AllNodes -- [Isolated],
    ct:pal("Leader: ~p, isolating follower: ~p", [LeaderNode, Isolated]),

    ContainerId = atom_to_binary(?FUNCTION_NAME),

    %% Open a connection to the node that is about to be isolated
    OpnConf1 = conn_config(Config, Isolated, ContainerId),
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                assert_has_sole_cap(OffCaps1),
                assert_has_weak_policy(Props1)
    after 10000 -> ct:fail(opened_timeout)
    end,

    ct:pal("Isolating node ~p", [Isolated]),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, MajorityA),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, MajorityB),

    %% The majority side cannot reach the isolated node to verify
    %% Connection1 is still alive, so it treats it as dead and lets a
    %% conflicting connection with the same container-id through
    OpnConf2 = conn_config(Config, MajorityA, ContainerId),
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                ?assertNot(has_field(?AMQP_ERROR_CONNECTION_ESTABLISHMENT_FAILED, Props2)),
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 10000 -> ct:fail(opened_timeout)
    end,

    %% Connection1 is still alive and oblivious on the isolated side: no
    %% close should have arrived yet -- genuine split-brain
    receive {amqp10_event, {connection, Connection1, {closed, _}}} ->
                ct:fail(unexpected_close_before_partition_heals)
    after 2000 -> ok
    end,

    ct:pal("Healing partition for node ~p", [Isolated]),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, MajorityA),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, MajorityB),

    %% Once the isolated node catches back up on the Raft log, the
    %% kill_connection_sproc trigger fires there too and closes the stale
    %% Connection1, resolving the split-brain
    receive {amqp10_event, {connection, Connection1,
                            {closed, #'v1_0.close'{
                                        error = #'v1_0.error'{
                                                   condition = ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
                                                   info = {map, Info}
                                                  }}}}} ->
                ?assert(lists:member({?SOLE_CONN_ENFORCEMENT, true}, Info))
    after 30000 -> ct:fail(stale_connection_not_killed_after_heal)
    end,

    %% Connection2 must have remained unaffected throughout
    ok = amqp_utils:close_connection_sync(Connection2).

%% Same as refuse_connection_accepts_conflicting_id_on_majority_side_after_partition,
%% except the isolated node is the current Raft *leader* rather than a
%% follower: the majority side (the two remaining nodes) must first elect a
%% new leader among themselves before it can accept any new connection.
%% Once it does, the rest of the scenario is identical: the majority side
%% cannot reach the isolated (former leader) node to verify the first
%% connection is still alive, so it treats it as dead and lets a conflicting
%% connection with the same container-id through, and the stale connection
%% is closed once the isolated node rejoins and catches up.
refuse_connection_accepts_conflicting_id_on_majority_side_after_leader_partition(Config) ->
    AllNodes = all_nodes(Config),

    ct:pal("Starting the sole_conn Khepri store on ~p", [AllNodes]),
    StartResult = rpc(Config, 0, ?SOLE_CONN_MOD, start, []),
    ?assertEqual(lists:sort([{N, ok} || N <- AllNodes]), lists:sort(StartResult)),

    {LeaderNode, _FollowerNodes} = leader_and_followers(Config),
    Isolated = LeaderNode,
    [MajorityA, MajorityB] = AllNodes -- [Isolated],
    ct:pal("Isolating current leader ~p", [Isolated]),

    ContainerId = atom_to_binary(?FUNCTION_NAME),

    %% Open a connection to the node that is about to be isolated
    OpnConf1 = conn_config(Config, Isolated, ContainerId),
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps1,
                                        properties = Props1}}}} ->
                assert_has_sole_cap(OffCaps1),
                assert_has_weak_policy(Props1)
    after 10000 -> ct:fail(opened_timeout)
    end,

    ct:pal("Isolating node ~p", [Isolated]),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, MajorityA),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, MajorityB),

    %% The majority side just lost its leader: wait for it to elect a new
    %% one among the two remaining nodes before it can accept new
    %% connections. Querying via a majority node (never the isolated one,
    %% which may or may not still be node 0) keeps this from ever blocking
    %% on an unreachable node.
    ct:pal("Waiting for the majority side to elect a new leader"),
    await_new_leader(Config, MajorityA, Isolated),

    %% The majority side cannot reach the isolated (former leader) node to
    %% verify Connection1 is still alive, so it treats it as dead and lets a
    %% conflicting connection with the same container-id through
    OpnConf2 = conn_config(Config, MajorityA, ContainerId),
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2,
                            {opened, #'v1_0.open'{
                                        offered_capabilities = OffCaps2,
                                        properties = Props2}}}} ->
                ?assertNot(has_field(?AMQP_ERROR_CONNECTION_ESTABLISHMENT_FAILED, Props2)),
                assert_has_sole_cap(OffCaps2),
                assert_has_weak_policy(Props2)
    after 10000 -> ct:fail(opened_timeout)
    end,

    %% Connection1 is still alive and oblivious on the isolated side: no
    %% close should have arrived yet -- genuine split-brain
    receive {amqp10_event, {connection, Connection1, {closed, _}}} ->
                ct:fail(unexpected_close_before_partition_heals)
    after 2000 -> ok
    end,

    ct:pal("Healing partition for node ~p", [Isolated]),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, MajorityA),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, MajorityB),

    %% Once the isolated node rejoins and catches back up on the Raft log,
    %% the kill_connection_sproc trigger fires there too and closes the
    %% stale Connection1, resolving the split-brain
    receive {amqp10_event, {connection, Connection1,
                            {closed, #'v1_0.close'{
                                        error = #'v1_0.error'{
                                                   condition = ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
                                                   info = {map, Info}
                                                  }}}}} ->
                ?assert(lists:member({?SOLE_CONN_ENFORCEMENT, true}, Info))
    after 30000 -> ct:fail(stale_connection_not_killed_after_heal)
    end,

    %% Connection2 must have remained unaffected throughout
    ok = amqp_utils:close_connection_sync(Connection2).

%% -------------------------------------------------------------------
%% Internal Helpers
%% -------------------------------------------------------------------

all_nodes(Config) ->
    [rabbit_ct_broker_helpers:get_node_config(Config, N, nodename) ||
     N <- lists:seq(0, ?NODE_COUNT - 1)].

node_index(Config, NodeName) ->
    IndexedNodes = lists:zip(lists:seq(0, ?NODE_COUNT - 1), all_nodes(Config)),
    {Index, NodeName} = lists:keyfind(NodeName, 2, IndexedNodes),
    Index.

conn_config(Config, NodeName, ContainerId) ->
    NodeIndex = node_index(Config, NodeName),
    ConnConf = amqp_utils:connection_config(NodeIndex, Config),
    ConnConf#{
      container_id => ContainerId,
      desired_capabilities => [?CAP_SOLE_CONN],
      notify_with_performative => true
     }.

%% Waits for the sole_conn Raft cluster to settle on a single leader, then
%% returns {LeaderNode, FollowerNodes}.
leader_and_followers(Config) ->
    IsSettled = fun() ->
                        Status = rpc(Config, 0, ?SOLE_CONN_MOD, status, []),
                        is_list(Status) andalso
                            length(Status) =:= ?NODE_COUNT andalso
                            has_one_leader(Status) andalso
                            has_expected_followers(Status)
                end,
    rabbit_ct_helpers:await_condition(IsSettled),
    Status = rpc(Config, 0, ?SOLE_CONN_MOD, status, []),
    [Leader] = [proplists:get_value(<<"Node Name">>, M) ||
                M <- Status, proplists:get_value(<<"Raft State">>, M) =:= leader],
    Followers = [proplists:get_value(<<"Node Name">>, M) ||
                 M <- Status, proplists:get_value(<<"Raft State">>, M) =:= follower],
    {Leader, Followers}.

has_one_leader(Status) ->
    length([ok || M <- Status,
                  proplists:get_value(<<"Raft State">>, M) =:= leader]) =:= 1.

has_expected_followers(Status) ->
    length([ok || M <- Status,
                  proplists:get_value(<<"Raft State">>, M) =:= follower])
        =:= ?NODE_COUNT - 1.

%% Waits for QueryNode to see a leader other than ExcludedNode. Queries
%% ra_leaderboard directly -- a fast, purely local lookup -- rather than
%% status/0, which iterates every member with an erpc timeout of its own
%% (?RPC_TIMEOUT, 30s) and would be impractically slow to poll while a
%% member is unreachable.
await_new_leader(Config, QueryNode, ExcludedNode) ->
    StoreId = rpc(Config, QueryNode, ?SOLE_CONN_MOD, get_store_id, []),
    IsSettled = fun() ->
                        case rpc(Config, QueryNode, ra_leaderboard,
                                 lookup_leader, [StoreId]) of
                            {StoreId, Node} when Node =/= ExcludedNode -> true;
                            _ -> false
                        end
                end,
    rabbit_ct_helpers:await_condition(IsSettled, 30000).

assert_has_sole_cap(Caps) ->
    ?assert(has_capability(?CAP_SOLE_CONN, Caps)).

assert_has_weak_policy({map, Props}) ->
    ExpectedPair = {?SOLE_CONN_DETECTION_POLICY, ?SOLE_CONN_DETECTION_POLICY_WEAK},
    ?assert(lists:member(ExpectedPair, Props)).

has_field(Field, {map, Props}) ->
    case lists:keyfind(Field, 1, Props) of
        false ->
            false;
        _ ->
            true
    end.
