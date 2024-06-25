-module(unit_queue_location_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("stdlib/include/assert.hrl").

all() ->
    [
     {group, generic},
     {group, classic}
    ].

groups() ->
    [
     {generic, [], generic_tests()},
     {classic, [], classic_tests()}
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
                              GetQueues),
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
                                      GetQueues),
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
