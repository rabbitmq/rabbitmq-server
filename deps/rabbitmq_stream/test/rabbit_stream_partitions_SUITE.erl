%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2025 Broadcom. All Rights Reserved.
%% The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_partitions_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").
-include_lib("rabbit/src/rabbit_stream_sac_coordinator.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(NET_TICKTIME_S, 5).
-define(TRSPT, gen_tcp).
-define(CORR_ID, 1).
-define(SAC_STATE, rabbit_stream_sac_coordinator).

-record(node, {name :: node(), stream_port :: pos_integer()}).

all() ->
    [{group, cluster}].

groups() ->
    [{cluster, [],
      [simple_sac_consumer_should_get_disconnected_on_network_partition,
       simple_sac_consumer_should_get_disconnected_on_coord_leader_network_partition,
       super_stream_sac_consumer_should_get_disconnected_on_network_partition,
       super_stream_sac_consumer_should_get_disconnected_on_coord_leader_network_partition]}
    ].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "mixed version clusters are not supported"};
        _ ->
            rabbit_ct_helpers:log_environment(),
            Config
    end.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:run_setup_steps(
                Config,
                [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]),
    rabbit_ct_helpers:set_config(Config1,
                                 [{rmq_nodename_suffix, Group},
                                  {net_ticktime, ?NET_TICKTIME_S}]).
end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, TestCase),
    Config2 = rabbit_ct_helpers:set_config(
                Config1, [{rmq_nodes_clustered, true},
                          {rmq_nodes_count, 3},
                          {tcp_ports_base}
                         ]),
    rabbit_ct_helpers:run_setup_steps(
      Config2,
      [fun(StepConfig) ->
               rabbit_ct_helpers:merge_app_env(StepConfig,
                                               {aten,
                                                [{poll_interval,
                                                  1000}]})
       end,
       fun(StepConfig) ->
               rabbit_ct_helpers:merge_app_env(StepConfig,
                                               {rabbit,
                                                [{stream_cmd_timeout, 5000},
                                                 {stream_sac_disconnected_timeout,
                                                  2000}]})
       end]
      ++ rabbit_ct_broker_helpers:setup_steps()).

end_per_testcase(TestCase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_finished(Config, TestCase),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:teardown_steps()).


simple_sac_consumer_should_get_disconnected_on_network_partition(Config) ->
    init_coordinator(Config),
    CL = coordinator_leader(Config),

    S = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    init_stream(Config, CL, S),

    [L, F1, F2] = topology(Config, S),

    %% the stream leader and the coordinator leader are on the same node
    %% another node will be isolated
    ?assertEqual(L#node.name, coordinator_leader(Config)),

    log("Stream leader and coordinator leader are on ~p", [L#node.name]),

    {ok, So0, C0_00} = stream_test_utils:connect(Config, 0),
    {ok, So1, C1_00} = stream_test_utils:connect(Config, 1),
    {ok, So2, C2_00} = stream_test_utils:connect(Config, 2),

    C0_01 = register_sac(So0, C0_00, S, 0),
    C0_02 = receive_consumer_update(So0, C0_01),

    C1_01 = register_sac(So1, C1_00, S, 1),
    C2_01 = register_sac(So2, C2_00, S, 2),
    SubIdToState0 = #{0 => {So0, C0_02},
                      1 => {So1, C1_01},
                      2 => {So2, C2_01}},

    Consumers1 = query_consumers(Config, S),
    assertSize(3, Consumers1),
    assertConsumersConnected(Consumers1),

    LN = L#node.name,
    F1N = F1#node.name,
    F2N = F2#node.name,

    Isolated = F1N,
    {value, DisconnectedConsumer} =
        lists:search(fun(#consumer{pid = ConnPid}) ->
                             rpc(Config, erlang, node, [ConnPid]) =:= Isolated
                     end, Consumers1),
    #consumer{subscription_id = DiscSubId} = DisconnectedConsumer,

    log("Isolating node ~p", [Isolated]),

    rabbit_ct_broker_helpers:block_traffic_between(Isolated, LN),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, F2N),

    wait_for_disconnected_consumer(Config, LN, S),
    wait_for_presumed_down_consumer(Config, LN, S),

    log("Node ~p rejoins cluster", [Isolated]),

    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, LN),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, F2N),

    wait_for_all_consumers_connected(Config, LN, S),

    Consumers2 = query_consumers(Config, LN, S),
    log("Consumers after partition resolution: ~p", [Consumers2]),
    log("Disconnected consumer: ~p", [DisconnectedConsumer]),
    %% the disconnected, then presumed down consumer is cancelled,
    %% because the stream member on its node has been restarted
    assertSize(2, Consumers2),
    assertConsumersConnected(Consumers2),
    ?assertMatch([DisconnectedConsumer],
                 Consumers1 -- Consumers2),

    %% assert the cancelled consumer received a metadata update frame
    SubIdToState1 =
        maps:fold(fun(K, {S0, C0}, Acc) when K == DiscSubId ->
                          log("Expecting metadata update for disconnected consumer"),
                          C1 = receive_metadata_update(S0, C0),
                          log("Received metadata update"),
                          Acc#{K => {S0, C1}};
                     (K, {S0, C0}, Acc) ->
                          Acc#{K => {S0, C0}}
                  end, #{}, SubIdToState0),

    log("Deleting stream"),
    delete_stream(stream_port(Config, 0), S),

    %% online consumers should receive a metadata update frame (stream deleted)
    %% we unqueue the this frame before closing the connection
    %% directly closing the connection of the cancelled consumer
    maps:foreach(fun(K, {S0, C0}) when K /= DiscSubId ->
                         log("Expecting frame in consumer ~p", [K]),
                         {Cmd1, C1} = receive_commands(S0, C0),
                         log("Received ~p", [Cmd1]),
                         log("Closing"),
                         {ok, _} = stream_test_utils:close(S0, C1);
                    (K, {S0, C0}) ->
                         log("Closing ~p", [K]),
                         {ok, _} = stream_test_utils:close(S0, C0)
                 end, SubIdToState1),

    ok.

simple_sac_consumer_should_get_disconnected_on_coord_leader_network_partition(Config) ->
    init_coordinator(Config),
    CL = coordinator_leader(Config),
    [CF1, CF2] = all_nodes(Config) -- [CL],

    S = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    init_stream(Config, CF1, S),
    [L, _F1, _F2] = topology(Config, S),

    %% the stream leader and the coordinator leader are not on the same node
    %% the coordinator leader node will be isolated
    ?assertNotEqual(L#node.name, CL),

    log("Stream leader and coordinator leader are on ~p", [L#node.name]),

    {ok, So0, C0_00} = stream_test_utils:connect(Config, CL),
    {ok, So1, C1_00} = stream_test_utils:connect(Config, CF1),
    {ok, So2, C2_00} = stream_test_utils:connect(Config, CF2),

    C0_01 = register_sac(So0, C0_00, S, 0),
    C0_02 = receive_consumer_update(So0, C0_01),

    C1_01 = register_sac(So1, C1_00, S, 1),
    C2_01 = register_sac(So2, C2_00, S, 2),
    SubIdToState0 = #{0 => {So0, C0_02},
                      1 => {So1, C1_01},
                      2 => {So2, C2_01}},

    Consumers1 = query_consumers(Config, S),
    assertSize(3, Consumers1),
    assertConsumersConnected(Consumers1),

    %% N1 is the coordinator leader
    Isolated = CL,
    NotIsolated = CF1,
    {value, DisconnectedConsumer} =
        lists:search(fun(#consumer{pid = ConnPid}) ->
                             rpc(Config, erlang, node, [ConnPid]) =:= Isolated
                     end, Consumers1),
    #consumer{subscription_id = DiscSubId} = DisconnectedConsumer,

    log("Isolating node ~p", [Isolated]),

    rabbit_ct_broker_helpers:block_traffic_between(Isolated, CF1),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, CF2),

    wait_for_disconnected_consumer(Config, NotIsolated, S),
    wait_for_presumed_down_consumer(Config, NotIsolated, S),

    log("Node ~p rejoins cluster", [Isolated]),

    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, CF1),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, CF2),

    wait_for_coordinator_ready(Config),

    wait_for_all_consumers_connected(Config, NotIsolated, S),

    Consumers2 = query_consumers(Config, NotIsolated, S),

    log("Consumers after partition resolution ~p", [Consumers2]),
    log("Disconnected consumer: ~p", [DisconnectedConsumer]),
    %% the disconnected, then presumed down consumer is cancelled,
    %% because the stream member on its node has been restarted
    assertSize(2, Consumers2),
    assertConsumersConnected(Consumers2),
    assertEmpty(lists:filter(fun(C) ->
                                     same_consumer(DisconnectedConsumer, C)
                             end, Consumers2)),

    [#consumer{subscription_id = ActiveSubId}] =
        lists:filter(fun(#consumer{status = St}) ->
                             St =:= {connected, active}
                     end, Consumers2),

    SubIdToState1 =
        maps:fold(fun(K, {S0, C0}, Acc) when K == DiscSubId ->
                          log("Expecting metadata update for disconnected consumer"),
                          %% cancelled consumer received a metadata update
                          C1 = receive_metadata_update(S0, C0),
                          log("Received metadata update"),
                          Acc#{K => {S0, C1}};
                     (K, {S0, C0}, Acc) when K == ActiveSubId ->
                          log("Expecting consumer update for promoted consumer"),
                          %% promoted consumer should have received consumer update
                          C1 = receive_consumer_update_and_respond(S0, C0),
                          log("Received consumer update"),
                          Acc#{K => {S0, C1}};
                     (K, {S0, C0}, Acc) ->
                          Acc#{K => {S0, C0}}
                  end, #{}, SubIdToState0),

    log("Deleting stream"),
    delete_stream(L#node.stream_port, S),

    %% online consumers should receive a metadata update frame (stream deleted)
    %% we unqueue this frame before closing the connection
    %% directly closing the connection of the cancelled consumer
    maps:foreach(fun(K, {S0, C0}) when K /= DiscSubId ->
                         log("Expecting frame in consumer ~p", [K]),
                         {Cmd1, C1} = receive_commands(S0, C0),
                         log("Received ~p", [Cmd1]),
                         log("Closing"),
                         {ok, _} = stream_test_utils:close(S0, C1);
                    (K, {S0, C0}) ->
                         log("Closing ~p", [K]),
                         {ok, _} = stream_test_utils:close(S0, C0)
                 end, SubIdToState1),

    ok.

super_stream_sac_consumer_should_get_disconnected_on_network_partition(Config) ->
    init_coordinator(Config),
    CL = coordinator_leader(Config),

    Ss = rabbit_data_coercion:to_binary(?FUNCTION_NAME),

    Partition = init_super_stream(Config, CL, Ss, 1, CL),
    [L, F1, F2] = topology(Config, Partition),

    wait_for_coordinator_ready(Config),

    %% we expect the stream leader and the coordinator leader to be on the same node
    %% another node will be isolated
    ?assertEqual(L#node.name, CL),

    log("Stream leader and coordinator leader are on ~p", [L#node.name]),

    {ok, So0, C0_00} = stream_test_utils:connect(L#node.stream_port),
    {ok, So1, C1_00} = stream_test_utils:connect(F1#node.stream_port),
    {ok, So2, C2_00} = stream_test_utils:connect(F2#node.stream_port),

    C0_01 = register_sac(So0, C0_00, Partition, 0, Ss),
    C0_02 = receive_consumer_update(So0, C0_01),

    C1_01 = register_sac(So1, C1_00, Partition, 1, Ss),
    C2_01 = register_sac(So2, C2_00, Partition, 2, Ss),
    SubIdToState0 = #{0 => {So0, C0_02},
                      1 => {So1, C1_01},
                      2 => {So2, C2_01}},

    Consumers1 = query_consumers(Config, Partition),
    assertSize(3, Consumers1),
    assertConsumersConnected(Consumers1),

    LN = L#node.name,
    F1N = F1#node.name,
    F2N = F2#node.name,

    Isolated = F1N,
    NotIsolated = F2N,
    {value, DisconnectedConsumer} =
        lists:search(fun(#consumer{pid = ConnPid}) ->
                             rpc(Config, erlang, node, [ConnPid]) =:= Isolated
                     end, Consumers1),
    #consumer{subscription_id = DiscSubId} = DisconnectedConsumer,

    log("Isolating node ~p", [Isolated]),

    rabbit_ct_broker_helpers:block_traffic_between(Isolated, LN),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, F2N),

    wait_for_disconnected_consumer(Config, NotIsolated, Partition),
    wait_for_presumed_down_consumer(Config, NotIsolated, Partition),

    log("Node ~p rejoins cluster", [Isolated]),

    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, LN),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, F2N),

    wait_for_coordinator_ready(Config),

    wait_for_all_consumers_connected(Config, NotIsolated, Partition),

    Consumers2 = query_consumers(Config, NotIsolated, Partition),
    log("Consumers after partition resolution: ~p", [Consumers2]),
    log("Disconnected consumer: ~p", [DisconnectedConsumer]),

    %% the disconnected, then presumed down consumer is cancelled,
    %% because the stream member on its node has been restarted
    assertSize(2, Consumers2),
    assertConsumersConnected(Consumers2),
    assertEmpty(lists:filter(fun(C) ->
                                     same_consumer(DisconnectedConsumer, C)
                             end, Consumers2)),

    SubIdToState1 =
        maps:fold(fun(K, {S0, C0}, Acc) when K == DiscSubId ->
                          log("Expecting metadata update for disconnected consumer"),
                          %% cancelled consumer received a metadata update
                          C1 = receive_metadata_update(S0, C0),
                          log("Received metadata update"),
                          Acc#{K => {S0, C1}};
                     (K, {S0, C0}, Acc) ->
                          Acc#{K => {S0, C0}}
                  end, #{}, SubIdToState0),

    log("Deleting super stream"),
    delete_super_stream(L#node.stream_port, Ss),

    %% online consumers should receive a metadata update frame (stream deleted)
    %% we unqueue this frame before closing the connection
    %% directly closing the connection of the cancelled consumer
    maps:foreach(fun(K, {S0, C0}) when K /= DiscSubId ->
                         log("Expecting frame in consumer ~p", [K]),
                         {Cmd1, C1} = receive_commands(S0, C0),
                         log("Received ~p", [Cmd1]),
                         log("Closing"),
                         {ok, _} = stream_test_utils:close(S0, C1);
                    (K, {S0, C0}) ->
                         log("Closing ~p", [K]),
                         {ok, _} = stream_test_utils:close(S0, C0)
                 end, SubIdToState1),
    ok.

super_stream_sac_consumer_should_get_disconnected_on_coord_leader_network_partition(Config) ->
    init_coordinator(Config),
    CL = coordinator_leader(Config),
    [CF1, _] = all_nodes(Config) -- [CL],
    Ss = rabbit_data_coercion:to_binary(?FUNCTION_NAME),
    Partition = init_super_stream(Config, CL, Ss, 2, CF1),
    [L, F1, F2] = topology(Config, Partition),

    wait_for_coordinator_ready(Config),

    %% check stream leader and coordinator are not on the same node
    %% the coordinator leader node will be isolated
    ?assertNotEqual(L#node.name, CL),

    log("Stream leader and coordinator leader are on ~p", [L#node.name]),

    {ok, So0, C0_00} = stream_test_utils:connect(L#node.stream_port),
    {ok, So1, C1_00} = stream_test_utils:connect(F1#node.stream_port),
    {ok, So2, C2_00} = stream_test_utils:connect(F2#node.stream_port),

    C0_01 = register_sac(So0, C0_00, Partition, 0, Ss),
    C0_02 = receive_consumer_update(So0, C0_01),

    C1_01 = register_sac(So1, C1_00, Partition, 1, Ss),

    %% former active gets de-activated
    C0_03 = receive_consumer_update_and_respond(So0, C0_02),

    %% gets activated
    C1_02 = receive_consumer_update_and_respond(So1, C1_01),

    C2_01 = register_sac(So2, C2_00, Partition, 2, Ss),
    SubIdToState0 = #{0 => {So0, C0_03},
                      1 => {So1, C1_02},
                      2 => {So2, C2_01}},

    Consumers1 = query_consumers(Config, Partition),
    assertSize(3, Consumers1),
    assertConsumersConnected(Consumers1),

    LN = L#node.name,
    F1N = F1#node.name,
    F2N = F2#node.name,

    Isolated = F1N,
    NotIsolated = F2N,
    {value, DisconnectedConsumer} =
        lists:search(fun(#consumer{pid = ConnPid}) ->
                             rpc(Config, erlang, node, [ConnPid]) =:= Isolated
                     end, Consumers1),
    #consumer{subscription_id = DiscSubId} = DisconnectedConsumer,

    log("Isolating node ~p", [Isolated]),

    rabbit_ct_broker_helpers:block_traffic_between(Isolated, LN),
    rabbit_ct_broker_helpers:block_traffic_between(Isolated, F2N),

    wait_for_disconnected_consumer(Config, NotIsolated, Partition),
    wait_for_presumed_down_consumer(Config, NotIsolated, Partition),

    log("Node ~p rejoins cluster", [Isolated]),

    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, LN),
    rabbit_ct_broker_helpers:allow_traffic_between(Isolated, F2N),

    wait_for_coordinator_ready(Config),

    wait_for_all_consumers_connected(Config, NotIsolated, Partition),

    Consumers2 = query_consumers(Config, NotIsolated, Partition),
    log("Consumers after partition resolution: ~p", [Consumers2]),
    log("Disconnected consumer: ~p", [DisconnectedConsumer]),

    %% the disconnected, then presumed down consumer is cancelled,
    %% because the stream member on its node has been restarted
    assertSize(2, Consumers2),
    assertConsumersConnected(Consumers2),
    assertEmpty(lists:filter(fun(C) ->
                                     same_consumer(DisconnectedConsumer, C)
                             end, Consumers2)),

    [#consumer{subscription_id = ActiveSubId}] =
        lists:filter(fun(#consumer{status = St}) ->
                             St =:= {connected, active}
                     end, Consumers2),

    SubIdToState1 =
        maps:fold(fun(K, {S0, C0}, Acc) when K == DiscSubId ->
                          log("Expecting metadata update for disconnected consumer"),
                          %% cancelled consumer received a metadata update
                          C1 = receive_metadata_update(S0, C0),
                          log("Received metadata update"),
                          Acc#{K => {S0, C1}};
                     (K, {S0, C0}, Acc) when K == ActiveSubId ->
                          log("Expecting consumer update for promoted consumer"),
                          %% promoted consumer should have received consumer update
                          C1 = receive_consumer_update_and_respond(S0, C0),
                          log("Received consumer update"),
                          Acc#{K => {S0, C1}};
                     (K, {S0, C0}, Acc) ->
                          Acc#{K => {S0, C0}}
                  end, #{}, SubIdToState0),

    log("Deleting super stream"),
    delete_super_stream(L#node.stream_port, Ss),

    %% online consumers should receive a metadata update frame (stream deleted)
    %% we unqueue this frame before closing the connection
    %% directly closing the connection of the cancelled consumer
    maps:foreach(fun(K, {S0, C0}) when K /= DiscSubId ->
                         log("Expecting frame in consumer ~p", [K]),
                         {Cmd1, C1} = receive_commands(S0, C0),
                         log("Received ~p", [Cmd1]),
                         log("Closing"),
                         {ok, _} = stream_test_utils:close(S0, C1);
                    (K, {S0, C0}) ->
                         log("Closing ~p", [K]),
                         {ok, _} = stream_test_utils:close(S0, C0)
                 end, SubIdToState1),
    ok.

same_consumer(#consumer{owner = P1, subscription_id = Id1},
              #consumer{owner = P2, subscription_id = Id2})
  when P1 == P2 andalso Id1 == Id2 ->
    true;
same_consumer(_, _) ->
    false.

cluster_nodes(Config) ->
    lists:map(fun(N) ->
                      #node{name = node_config(Config, N, nodename),
                            stream_port = stream_port(Config, N)}
              end, lists:seq(0, node_count(Config) - 1)).

node_count(Config) ->
   test_server:lookup_config(rmq_nodes_count, Config).

nodename(Config, N) ->
    node_config(Config, N, nodename).

stream_port(Config, N) ->
    node_config(Config, N, tcp_port_stream).

node_config(Config, N, K) ->
    rabbit_ct_broker_helpers:get_node_config(Config, N, K).

topology(Config, St) ->
    Members = stream_members(Config, St),
    LN = leader(Members),
    Nodes = cluster_nodes(Config),
    [L] = lists:filter(fun(#node{name = N}) ->
                               N =:= LN
                       end, Nodes),
    [F1, F2] = lists:filter(fun(#node{name = N}) ->
                                    N =/= LN
                            end, Nodes),

    [L, F1, F2].

leader(Members) ->
    maps:fold(fun(Node, {_, writer}, _Acc) ->
                      Node;
                 (_, _, Acc) ->
                      Acc
              end, undefined, Members).

stream_members(Config, Stream) ->
    {ok, Q} = rpc(Config, rabbit_amqqueue, lookup, [Stream, <<"/">>]),
    #{name := StreamId} = amqqueue:get_type_state(Q),
    State = rpc(Config, rabbit_stream_coordinator, state, []),
    {ok, Members} = rpc(Config, rabbit_stream_coordinator, query_members,
                        [StreamId, State]),
    Members.

init_coordinator(Config) ->
    %% to make sure the coordinator is initialized
    init_stream(Config, 0, <<"dummy">>),
    delete_stream(stream_port(Config, 0), <<"dummy">>),
    wait_for_coordinator_ready(Config).

init_stream(Config, N, St) ->
    {ok, S, C0} = stream_test_utils:connect(stream_port(Config, N)),
    {ok, C1} = stream_test_utils:create_stream(S, C0, St),
    NC = node_count(Config),
    wait_for_members(S, C1, St, NC),
    {ok, _} = stream_test_utils:close(S, C1).

delete_stream(Port, St) ->
    {ok, S, C0} = stream_test_utils:connect(Port),
    {ok, C1} = stream_test_utils:delete_stream(S, C0, St),
    {ok, _} = stream_test_utils:close(S, C1).

init_super_stream(Config, Node, Ss, PartitionIndex, ExpectedNode) ->
    {ok, S, C0} = stream_test_utils:connect(Config, Node),
    NC = node_count(Config),
    Partitions = [unicode:characters_to_binary([Ss, <<"-">>, integer_to_binary(N)])
                  || N <- lists:seq(0, NC - 1)],
    Bks = [integer_to_binary(N) || N <- lists:seq(0, NC - 1)],
    SsCreationFrame = request({create_super_stream, Ss, Partitions, Bks, #{}}),
    ok = ?TRSPT:send(S, SsCreationFrame),
    {Cmd1, C1} = receive_commands(S, C0),
    ?assertMatch({response, ?CORR_ID, {create_super_stream, ?RESPONSE_CODE_OK}},
                 Cmd1),
    [wait_for_members(S, C1, P, NC) || P <- Partitions],
    Partition = lists:nth(PartitionIndex, Partitions),
    [#node{name = LN} | _] = topology(Config, Partition),
    P = case LN of
            ExpectedNode ->
                Partition;
            _ ->
                enforce_stream_leader_on_node(Config, S, C1,
                                              Partitions, Partition,
                                              ExpectedNode, 10)
        end,
    {ok, _} = stream_test_utils:close(S, C1),
    P.


enforce_stream_leader_on_node(_, _, _, _, _, _, 0) ->
    ct:fail("could not create super stream partition on chosen node");
enforce_stream_leader_on_node(Config, S, C,
                              Partitions, Partition, Node, Count) ->
    CL = coordinator_leader(Config),
    NC = node_count(Config),
    [begin
         case P of
             Partition ->
                 restart_stream(Config, CL, P, Node);
             _ ->
                 restart_stream(Config, CL, P, undefined)
         end,
         wait_for_members(S, C, P, NC)
     end || P <- Partitions],
    [#node{name = LN} | _] = topology(Config, Partition),
    case LN of
        Node ->
            Partition;
        _ ->
            timer:sleep(500),
            enforce_stream_leader_on_node(Config, S, C,
                                          Partitions, Partition, Node,
                                          Count - 1)
    end.

delete_super_stream(Port, Ss) ->
    {ok, S, C0} = stream_test_utils:connect(Port),
    SsDeletionFrame = request({delete_super_stream, Ss}),
    ok = ?TRSPT:send(S, SsDeletionFrame),
    {Cmd1, C1} = receive_commands(S, C0),
    ?assertMatch({response, ?CORR_ID, {delete_super_stream, ?RESPONSE_CODE_OK}},
                 Cmd1),
    {ok, _} = stream_test_utils:close(S, C1).

register_sac(S, C0, St, SubId, SuperStream) ->
    register_sac0(S, C0, St, SubId, #{<<"super-stream">> => SuperStream}).

register_sac(S, C0, St, SubId) ->
    register_sac0(S, C0, St, SubId, #{}).

register_sac0(S, C0, St, SubId, Args) ->
    SacSubscribeFrame = request({subscribe, SubId, St,
                                 first, 1,
                                 Args#{<<"single-active-consumer">> => <<"true">>,
                                       <<"name">> => name()}}),
    ok = ?TRSPT:send(S, SacSubscribeFrame),
    {Cmd1, C1} = receive_commands(S, C0),
    ?assertMatch({response, ?CORR_ID, {subscribe, ?RESPONSE_CODE_OK}},
                 Cmd1),
    C1.

receive_consumer_update(S, C0) ->
    {Cmd, C1} = receive_commands(S, C0),
    ?assertMatch({request, _CorrId, {consumer_update, _SubId, _Status}},
                 Cmd),
    C1.

receive_consumer_update_and_respond(S, C0) ->
    {Cmd, C1} = receive_commands(S, C0),
    ?assertMatch({request, _CorrId, {consumer_update, _SubId, _Status}},
                 Cmd),
    {request, CorrId, {consumer_update, _SubId, _Status}} = Cmd,
    Frame = response(CorrId, {consumer_update, ?RESPONSE_CODE_OK, first}),
    ok = ?TRSPT:send(S, Frame),
    C1.

receive_metadata_update(S, C0) ->
    {Cmd, C1} = receive_commands(S, C0),
    ?assertMatch({metadata_update, _, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE},
                 Cmd),
    C1.

unsubscribe(S, C0) ->
    {ok, C1} = stream_test_utils:unsubscribe(S, C0, sub_id()),
    C1.

query_consumers(Config, Stream) ->
    query_consumers(Config, 0, Stream).

query_consumers(Config, Node, Stream) ->
    Key = group_key(Stream),
    #?SAC_STATE{groups = #{Key := #group{consumers = Consumers}}} =
        rpc(Config, Node, rabbit_stream_coordinator, sac_state, []),
    Consumers.


all_nodes(Config) ->
    lists:map(fun(N) ->
                      nodename(Config, N)
              end, lists:seq(0, node_count(Config) - 1)).

coordinator_status(Config) ->
    rpc(Config, rabbit_stream_coordinator, status, []).

coordinator_leader(Config) ->
    Status = coordinator_status(Config),
    case lists:search(fun(St) ->
                             RS = proplists:get_value(<<"Raft State">>, St,
                                                      undefined),
                             RS == leader
                     end, Status) of
        {value, Leader} ->
            proplists:get_value(<<"Node Name">>, Leader, undefined);
        _ ->
            undefined
    end.

restart_stream(Config, Node, S, undefined) ->
    rpc(Config, Node, rabbit_stream_queue, restart_stream, [<<"/">>, S, #{}]);
restart_stream(Config, Node, S, Leader) ->
    Opts = #{preferred_leader_node => Leader},
    rpc(Config, Node, rabbit_stream_queue, restart_stream, [<<"/">>, S, Opts]).


rpc(Config, M, F, A) ->
    rpc(Config, 0, M, F, A).

rpc(Config, Node, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, M, F, A).

group_key(Stream) ->
    {<<"/">>, Stream, name()}.

request(Cmd) ->
    request(?CORR_ID, Cmd).

request(CorrId, Cmd) ->
    rabbit_stream_core:frame({request, CorrId, Cmd}).

response(CorrId, Cmd) ->
    rabbit_stream_core:frame({response, CorrId, Cmd}).

receive_commands(S, C) ->
    receive_commands(?TRSPT, S, C).

receive_commands(Transport, S, C) ->
   stream_test_utils:receive_stream_commands(Transport, S, C).

sub_id() ->
    0.

name() ->
    <<"app">>.

wait_for_members(S, C, St, ExpectedCount) ->
    T = ?TRSPT,
    GetStreamNodes =
        fun() ->
           MetadataFrame = request({metadata, [St]}),
           ok = gen_tcp:send(S, MetadataFrame),
           {CmdMetadata, _} = receive_commands(T, S, C),
           {response, 1,
            {metadata, _Nodes, #{St := {Leader = {_H, _P}, Replicas}}}} =
               CmdMetadata,
           [Leader | Replicas]
        end,
    rabbit_ct_helpers:await_condition(fun() ->
                                         length(GetStreamNodes()) == ExpectedCount
                                      end).

wait_for_disconnected_consumer(Config, Node, Stream) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Cs = query_consumers(Config, Node, Stream),
              log("Expecting a disconnected consumer: ~p", [Cs]),
              lists:any(fun(#consumer{status = {disconnected, _}}) ->
                                true;
                           (_) ->
                                false
                        end, Cs)
      end).

wait_for_presumed_down_consumer(Config, Node, Stream) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Cs = query_consumers(Config, Node, Stream),
              log("Expecting a presumed-down consumer: ~p", [Cs]),
              lists:any(fun(#consumer{status = {presumed_down, _}}) ->
                                true;
                           (_) ->
                                false
                        end, Cs)
      end).

wait_for_all_consumers_connected(Config, Node, Stream) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Cs = query_consumers(Config, Node, Stream),
              log("Expecting connected consumers: ~p", [Cs]),
              lists:all(fun(#consumer{status = {connected, _}}) ->
                                true;
                           (_) ->
                                false
                        end, Cs)
      end, 30_000).

wait_for_coordinator_ready(Config) ->
    NC = node_count(Config),
    rabbit_ct_helpers:await_condition(
      fun() ->
              Status = coordinator_status(Config),
              log("Coordinator status: ~p", [Status]),
              lists:all(fun(St) ->
                                RS = proplists:get_value(<<"Raft State">>, St,
                                                         undefined),
                                RS == leader orelse RS == follower
                        end, Status) andalso length(Status) == NC
      end).

assertConsumersConnected(Consumers) when length(Consumers) > 0 ->
    lists:foreach(fun(#consumer{status = St}) ->
                          ?assertMatch({connected, _}, St,
                                       "Consumer should be connected")
                  end, Consumers);
assertConsumersConnected(_) ->
    ?assert(false, "The consumer list is empty").

assertSize(Expected, []) ->
    ?assertEqual(Expected, 0);
assertSize(Expected, Map) when is_map(Map) ->
    ?assertEqual(Expected, maps:size(Map));
assertSize(Expected, List) when is_list(List) ->
    ?assertEqual(Expected, length(List)).

assertEmpty(Data) ->
    assertSize(0, Data).

log(Format) ->
    ct:pal(Format).

log(Format, Args) ->
    ct:pal(Format, Args).
