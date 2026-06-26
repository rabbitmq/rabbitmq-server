-module(unit_queue_location_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("stdlib/include/assert.hrl").

all() ->
    [
     {group, generic},
     {group, classic},
     {group, az_aware}
    ].

groups() ->
    [
     {generic, [], generic_tests()},
     {classic, [], classic_tests()},
     {az_aware, [], az_aware_tests()}
    ].

generic_tests() -> [
                    default_strategy,
                    policy_key_precedence,
                    policy_key_fallback
                   ].

classic_tests() -> [
                    classic_balanced_below_threshold,
                    classic_balanced_above_threshold
                   ].


az_aware_tests() -> [
                     az_aware_no_tag,
                     az_aware_three_azs,
                     az_aware_two_azs,
                     az_aware_null_az_untagged,
                     az_aware_all_nodes_untagged,
                     az_aware_above_threshold_random,
                     az_aware_below_threshold_balanced,
                     az_aware_fewer_nodes_than_size,
                     az_aware_running_nodes_preferred,
                     az_aware_node_tags_for_nodes_offline_nodes
                    ].

default_strategy(_Config) ->
    ok = meck:new(rabbit_queue_type_util, [passthrough]),
    ok = meck:expect(rabbit_queue_type_util, args_policy_lookup,
                     fun(<<"queue-leader-locator">>, _, _) -> undefined;
                        (<<"queue-master-locator">>, _, _) -> undefined
                     end),
    ?assertEqual(<<"client-local">>, rabbit_queue_location:leader_locator(queue)),
    ok = meck:unload(rabbit_queue_type_util).

policy_key_precedence(_Config) ->
    ok = meck:new(rabbit_queue_type_util, [passthrough]),
    ok = meck:expect(rabbit_queue_type_util, args_policy_lookup,
                     fun(<<"queue-leader-locator">>, _, _) -> <<"balanced">>;
                        (<<"queue-master-locator">>, _, _) -> <<"min-masters">>
                     end),
    ?assertEqual(<<"balanced">>, rabbit_queue_location:leader_locator(queue)),
    ok = meck:unload(rabbit_queue_type_util).

policy_key_fallback(_Config) ->
    ok = meck:new(rabbit_queue_type_util, [passthrough]),
    ok = meck:expect(rabbit_queue_type_util, args_policy_lookup,
                     fun(<<"queue-leader-locator">>, _, _) -> undefined;
                        (<<"queue-master-locator">>, _, _) -> <<"min-masters">>
                     end),
    ?assertEqual(<<"balanced">>, rabbit_queue_location:leader_locator(queue)),
    ok = meck:unload(rabbit_queue_type_util).

classic_balanced_below_threshold(_Config) ->
    ok = meck:new(rabbit_queue_location, [passthrough]),
    ok = meck:expect(rabbit_queue_location, node, fun() -> node1 end),
    ok = meck:new(rabbit_maintenance, [passthrough]),
    ok = meck:expect(rabbit_maintenance, filter_out_drained_nodes_local_read, fun(N) -> N end),
    AllNodes = [node1, node2, node3, node4, node5],
    RunningNodes = AllNodes,
    QueueType = rabbit_classic_queue,
    GetQueues = fun() -> unused_because_mecked end,
    QueueCount = 2,
    QueueCountStartRandom = 1000,
    {PotentialLeaders, _} = rabbit_queue_location:select_members(
                              1,
                              QueueType,
                              AllNodes,
                              RunningNodes,
                              QueueCount,
                              QueueCountStartRandom,
                              GetQueues,
                              undefined),
    %% all running nodes should be considered
    ?assertEqual(RunningNodes, PotentialLeaders),
    % a few different distributions of queues across nodes
    % case 1
    ok = meck:expect(rabbit_queue_location, queues_per_node, fun(_, _) ->
                                                                     #{node1 => 5,
                                                                       node2 => 1,
                                                                       node3 => 5}
                                                             end),
    ?assertEqual(node2, rabbit_queue_location:leader_node(<<"balanced">>,
                                                          PotentialLeaders,
                                                          RunningNodes,
                                                          QueueCount,
                                                          QueueCountStartRandom,
                                                          GetQueues)),
    % case 2
    ok = meck:expect(rabbit_queue_location, queues_per_node, fun(_, _) ->
                                                                     #{node1 => 0,
                                                                       node2 => 1,
                                                                       node3 => 5}
                                                             end),
    ?assertEqual(node1, rabbit_queue_location:leader_node(<<"balanced">>,
                                                          PotentialLeaders,
                                                          RunningNodes,
                                                          QueueCount,
                                                          QueueCountStartRandom,
                                                          GetQueues)),
    % case 3
    ok = meck:expect(rabbit_queue_location, queues_per_node, fun(_, _) ->
                                                                     #{node1 => 100,
                                                                       node2 => 100,
                                                                       node3 => 99}
                                                             end),
    ?assertEqual(node3, rabbit_queue_location:leader_node(<<"balanced">>,
                                                          PotentialLeaders,
                                                          RunningNodes,
                                                          QueueCount,
                                                          QueueCountStartRandom,
                                                          GetQueues)),

    ok = meck:unload([rabbit_queue_location, rabbit_maintenance]).

classic_balanced_above_threshold(_Config) ->
    ok = meck:new(rabbit_maintenance, [passthrough]),
    ok = meck:expect(rabbit_maintenance, filter_out_drained_nodes_local_read, fun(N) -> N end),
    AllNodes = [node1, node2, node3],
    RunningNodes = AllNodes,
    QueueType = rabbit_classic_queue,
    GetQueues =  fun() -> [] end, %rabbit_queue_location:get_queues_for_type(QueueType),
    QueueCount = 1230,
    QueueCountStartRandom = 1000,
    Locations = [begin
                     {Members, _} = rabbit_queue_location:select_members(
                                      1,
                                      QueueType,
                                      AllNodes,
                                      RunningNodes,
                                      QueueCount,
                                      QueueCountStartRandom,
                                      GetQueues,
                                      undefined),
                     rabbit_queue_location:leader_node(<<"balanced">>,
                                                       Members,
                                                       RunningNodes,
                                                       QueueCount,
                                                       QueueCountStartRandom,
                                                       GetQueues)
                 end || _ <- lists:seq(1, 30)],
    %% given we selected a random location 30 times with 3 possible options,
    %% we would have to be very unlucky not to see all 3 nodes in the results
    ?assertEqual([node1, node2, node3], lists:sort(lists:uniq(Locations))),
    ok = meck:unload([rabbit_maintenance]).

%% No placement tag (undefined): the existing algorithm is used unchanged.
az_aware_no_tag(_Config) ->
    ok = meck:new(rabbit_queue_location, [passthrough]),
    ok = meck:expect(rabbit_queue_location, node, fun() -> n1 end),
    ok = meck:new(rabbit_queue_type, [passthrough]),
    ok = meck:expect(rabbit_queue_type, get_nodes, fun({fake_queue, N}) -> [N] end),
    AllNodes = [n1, n2, n3, n4, n5],
    RunningNodes = AllNodes,
    QueueType = rabbit_quorum_queue,
    GetQueues = fun() -> [] end,
    QueueCount = 0,
    QueueCountStartRandom = 1000,
    {Members, _} = rabbit_queue_location:select_members(3, QueueType, AllNodes, RunningNodes,
                                                        QueueCount, QueueCountStartRandom,
                                                        GetQueues, undefined),
    %% n1 is always the local (first) member; 3 members total.
    ?assertEqual(3, length(Members)),
    ?assert(lists:member(n1, Members)),
    ok = meck:unload([rabbit_queue_location, rabbit_queue_type]).

%% Three AZs: 3 nodes in az1, 2 in az2, 2 in az3.
%% A 3-member QQ must end up with one member per AZ.
az_aware_three_azs(_Config) ->
    ok = meck:new(rabbit_member_placement_az, [passthrough]),
    ok = meck:expect(rabbit_member_placement_az, node_tags_for_nodes,
                     fun([n1, n2, n3, n4, n5, n6, n7], <<"az">>) ->
                             #{n1 => <<"az1">>, n2 => <<"az1">>, n3 => <<"az1">>,
                               n4 => <<"az2">>, n5 => <<"az2">>,
                               n6 => <<"az3">>, n7 => <<"az3">>}
                     end),
    ok = meck:new(rabbit_queue_type, [passthrough]),
    ok = meck:expect(rabbit_queue_type, get_nodes, fun({fake_queue, N}) -> [N] end),
    AllNodes = [n1, n2, n3, n4, n5, n6, n7],
    RunningNodes = AllNodes,
    QueueType = rabbit_quorum_queue,
    GetQueues = fun() -> [] end,
    QueueCount = 0,
    QueueCountStartRandom = 1000,
    {Members, _} = rabbit_queue_location:select_members(3, QueueType, AllNodes, RunningNodes,
                                                        QueueCount, QueueCountStartRandom,
                                                        GetQueues, <<"az">>),
    ?assertEqual(3, length(Members)),
    AZMap = #{n1 => <<"az1">>, n2 => <<"az1">>, n3 => <<"az1">>,
              n4 => <<"az2">>, n5 => <<"az2">>,
              n6 => <<"az3">>, n7 => <<"az3">>},
    AZsUsed = lists:sort(lists:uniq([maps:get(N, AZMap) || N <- Members])),
    ?assertEqual([<<"az1">>, <<"az2">>, <<"az3">>], AZsUsed),
    ok = meck:unload([rabbit_member_placement_az, rabbit_queue_type]).

%% Two AZs with 2 nodes each; 3-member QQ: both AZs must appear in the result.
az_aware_two_azs(_Config) ->
    ok = meck:new(rabbit_member_placement_az, [passthrough]),
    ok = meck:expect(rabbit_member_placement_az, node_tags_for_nodes,
                     fun([n1, n2, n3, n4], <<"az">>) ->
                             #{n1 => <<"az1">>, n2 => <<"az1">>,
                               n3 => <<"az2">>, n4 => <<"az2">>}
                     end),
    ok = meck:new(rabbit_queue_type, [passthrough]),
    ok = meck:expect(rabbit_queue_type, get_nodes, fun({fake_queue, N}) -> [N] end),
    AllNodes = [n1, n2, n3, n4],
    RunningNodes = AllNodes,
    QueueType = rabbit_quorum_queue,
    GetQueues = fun() -> [] end,
    QueueCount = 0,
    QueueCountStartRandom = 1000,
    {Members, _} = rabbit_queue_location:select_members(3, QueueType, AllNodes, RunningNodes,
                                                        QueueCount, QueueCountStartRandom,
                                                        GetQueues, <<"az">>),
    ?assertEqual(3, length(Members)),
    AZMap = #{n1 => <<"az1">>, n2 => <<"az1">>, n3 => <<"az2">>, n4 => <<"az2">>},
    AZsUsed = lists:uniq([maps:get(N, AZMap) || N <- Members]),
    ?assertEqual(2, length(AZsUsed)),
    ok = meck:unload([rabbit_member_placement_az, rabbit_queue_type]).

%% Untagged nodes are treated as a distinct "null AZ" (undefined) and are eligible.
az_aware_null_az_untagged(_Config) ->
    ok = meck:new(rabbit_member_placement_az, [passthrough]),
    ok = meck:expect(rabbit_member_placement_az, node_tags_for_nodes,
                     fun([n1, n2, n3], <<"az">>) ->
                             #{n1 => <<"az1">>, n2 => <<"az2">>, n3 => undefined}
                     end),
    ok = meck:new(rabbit_queue_type, [passthrough]),
    ok = meck:expect(rabbit_queue_type, get_nodes, fun({fake_queue, N}) -> [N] end),
    AllNodes = [n1, n2, n3],
    RunningNodes = AllNodes,
    QueueType = rabbit_quorum_queue,
    GetQueues = fun() -> [] end,
    QueueCount = 0,
    QueueCountStartRandom = 1000,
    {Members, _} = rabbit_queue_location:select_members(3, QueueType, AllNodes, RunningNodes,
                                                        QueueCount, QueueCountStartRandom,
                                                        GetQueues, <<"az">>),
    %% All three nodes should be selected: one per "AZ" (az1, az2, null).
    ?assertEqual(lists:sort([n1, n2, n3]), lists:sort(Members)),
    ok = meck:unload([rabbit_member_placement_az, rabbit_queue_type]).

%% When no node has the placement tag, fall back to the default algorithm.
%% Uses 5 nodes with size=3 so the guard length(AllNodes) =< Size does not trigger early.
az_aware_all_nodes_untagged(_Config) ->
    ok = meck:new(rabbit_queue_location, [passthrough]),
    ok = meck:expect(rabbit_queue_location, node, fun() -> n1 end),
    ok = meck:new(rabbit_member_placement_az, [passthrough]),
    ok = meck:expect(rabbit_member_placement_az, node_tags_for_nodes,
                     fun([n1, n2, n3, n4, n5], <<"az">>) ->
                             #{n1 => undefined, n2 => undefined, n3 => undefined,
                               n4 => undefined, n5 => undefined}
                     end),
    ok = meck:new(rabbit_queue_type, [passthrough]),
    ok = meck:expect(rabbit_queue_type, get_nodes, fun({fake_queue, N}) -> [N] end),
    AllNodes = [n1, n2, n3, n4, n5],
    RunningNodes = AllNodes,
    QueueType = rabbit_quorum_queue,
    GetQueues = fun() -> [] end,
    QueueCount = 0,
    QueueCountStartRandom = 1000,
    {Members, _} = rabbit_queue_location:select_members(3, QueueType, AllNodes, RunningNodes,
                                                        QueueCount, QueueCountStartRandom,
                                                        GetQueues, <<"az">>),
    %% Falls back to default balanced: n1 (local node) + 2 others from 5 nodes.
    ?assertEqual(3, length(Members)),
    ?assert(lists:member(n1, Members)),
    ok = meck:unload([rabbit_queue_location, rabbit_member_placement_az, rabbit_queue_type]).

%% Above the queue count threshold: random selection within the preferred AZ.
%% Each result must have exactly one member per AZ.
az_aware_above_threshold_random(_Config) ->
    ok = meck:new(rabbit_member_placement_az, [passthrough]),
    ok = meck:expect(rabbit_member_placement_az, node_tags_for_nodes,
                     fun([n1, n2, n3, n4, n5, n6, n7], <<"az">>) ->
                             #{n1 => <<"az1">>, n2 => <<"az1">>, n3 => <<"az1">>,
                               n4 => <<"az2">>, n5 => <<"az2">>,
                               n6 => <<"az3">>, n7 => <<"az3">>}
                     end),
    AllNodes = [n1, n2, n3, n4, n5, n6, n7],
    RunningNodes = AllNodes,
    QueueType = rabbit_quorum_queue,
    GetQueues = fun() -> [] end,
    QueueCount = 5000,
    QueueCountStartRandom = 1000,
    AZMap = #{n1 => <<"az1">>, n2 => <<"az1">>, n3 => <<"az1">>,
              n4 => <<"az2">>, n5 => <<"az2">>,
              n6 => <<"az3">>, n7 => <<"az3">>},
    AllResults = [begin
                      {Members, _} = rabbit_queue_location:select_members(
                                       3, QueueType, AllNodes, RunningNodes,
                                       QueueCount, QueueCountStartRandom,
                                       GetQueues, <<"az">>),
                      Members
                  end || _ <- lists:seq(1, 50)],
    %% Every result must have exactly one member from each of the three AZs.
    ?assert(lists:all(fun(Members) ->
                              AZsUsed = lists:sort(lists:uniq([maps:get(N, AZMap) || N <- Members])),
                              AZsUsed =:= [<<"az1">>, <<"az2">>, <<"az3">>]
                      end, AllResults)),
    ok = meck:unload(rabbit_member_placement_az).

%% Below the queue count threshold: pick the least-loaded node within the preferred AZ.
az_aware_below_threshold_balanced(_Config) ->
    ok = meck:new(rabbit_member_placement_az, [passthrough]),
    ok = meck:expect(rabbit_member_placement_az, node_tags_for_nodes,
                     fun([n1, n2, n3, n4, n5], <<"az">>) ->
                             #{n1 => <<"az1">>, n2 => <<"az1">>,
                               n3 => <<"az2">>, n4 => <<"az2">>,
                               n5 => <<"az3">>}
                     end),
    ok = meck:new(rabbit_queue_type, [passthrough]),
    %% n3 has 5 existing QQ members, n4 has 1 — so n4 should be preferred within az2.
    ok = meck:expect(rabbit_queue_type, get_nodes,
                     fun({fake_queue, N}) -> [N] end),
    AllNodes = [n1, n2, n3, n4, n5],
    RunningNodes = AllNodes,
    QueueType = rabbit_quorum_queue,
    FakeQueues = fake_queues([{n3, 5}, {n4, 1}]),
    GetQueues = fun() -> FakeQueues end,
    QueueCount = 0,
    QueueCountStartRandom = 1000,
    {Members, _} = rabbit_queue_location:select_members(3, QueueType, AllNodes, RunningNodes,
                                                        QueueCount, QueueCountStartRandom,
                                                        GetQueues, <<"az">>),
    ?assertEqual(3, length(Members)),
    %% n4 (1 queue) must be preferred over n3 (5 queues) within az2.
    ?assert(lists:member(n4, Members)),
    %% n5 is the only node in az3 and must always be selected.
    ?assert(lists:member(n5, Members)),
    ok = meck:unload([rabbit_member_placement_az, rabbit_queue_type]).

%% When AllNodes < requested size the AZ-aware guard is bypassed and all nodes are used.
az_aware_fewer_nodes_than_size(_Config) ->
    ok = meck:new(rabbit_queue_location, [passthrough]),
    ok = meck:expect(rabbit_queue_location, node, fun() -> n1 end),
    AllNodes = [n1, n2],
    RunningNodes = AllNodes,
    QueueType = rabbit_quorum_queue,
    GetQueues = fun() -> [] end,
    QueueCount = 0,
    QueueCountStartRandom = 1000,
    {Members, _} = rabbit_queue_location:select_members(3, QueueType, AllNodes, RunningNodes,
                                                        QueueCount, QueueCountStartRandom,
                                                        GetQueues, <<"az">>),
    ?assertEqual(lists:sort(AllNodes), lists:sort(Members)),
    ok = meck:unload(rabbit_queue_location).

%% Running nodes are preferred over stopped nodes within the same AZ.
az_aware_running_nodes_preferred(_Config) ->
    ok = meck:new(rabbit_member_placement_az, [passthrough]),
    ok = meck:expect(rabbit_member_placement_az, node_tags_for_nodes,
                     fun([n1, n2, n3, n4], <<"az">>) ->
                             #{n1 => <<"az1">>, n2 => <<"az2">>,
                               n3 => <<"az2">>, n4 => <<"az3">>}
                     end),
    ok = meck:new(rabbit_queue_type, [passthrough]),
    ok = meck:expect(rabbit_queue_type, get_nodes, fun({fake_queue, N}) -> [N] end),
    AllNodes = [n1, n2, n3, n4],
    %% n2 is stopped; n3 is in the same AZ (az2) and running.
    RunningNodes = [n1, n3, n4],
    QueueType = rabbit_quorum_queue,
    GetQueues = fun() -> [] end,
    QueueCount = 0,
    QueueCountStartRandom = 1000,
    {Members, _} = rabbit_queue_location:select_members(3, QueueType, AllNodes, RunningNodes,
                                                        QueueCount, QueueCountStartRandom,
                                                        GetQueues, <<"az">>),
    ?assertEqual(3, length(Members)),
    %% n3 (running, az2) must be chosen over n2 (stopped, az2).
    ?assert(lists:member(n3, Members)),
    ?assertNot(lists:member(n2, Members)),
    ok = meck:unload([rabbit_member_placement_az, rabbit_queue_type]).

az_aware_node_tags_for_nodes_offline_nodes(_Config) ->
    ok = meck:new(rabbit_db_node_metadata, [passthrough]),
    ok = meck:expect(rabbit_db_node_metadata, get,
                     fun(n1) -> #{node_tags => [{<<"az">>, <<"az1">>}]};
                        (n2) -> #{node_tags => [{<<"az">>, <<"az2">>}]};
                        (n3) -> #{};
                        (n4) -> #{}
                     end),
    Res1 = rabbit_member_placement_az:node_tags_for_nodes([n1, n2, n3, n4], <<"az">>),
    ?assertEqual(#{n1 => <<"az1">>, n2 => <<"az2">>, n3 => undefined, n4 => undefined}, Res1),
    ok = meck:unload([rabbit_db_node_metadata]).

%% Build a list of fake queue terms, one per node per count.
fake_queues(NodeCounts) ->
    lists:flatmap(fun({Node, Count}) ->
                          [{fake_queue, Node} || _ <- lists:seq(1, Count)]
                  end, NodeCounts).
