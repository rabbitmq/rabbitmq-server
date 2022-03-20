%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(dynamic_ha_SUITE).

%% rabbit_tests:test_dynamic_mirroring() is a unit test which should
%% test the logic of what all the policies decide to do, so we don't
%% need to exhaustively test that here. What we need to test is that:
%%
%% * Going from non-mirrored to mirrored works and vice versa
%% * Changing policy can add / remove mirrors and change the master
%% * Adding a node will create a new mirror when there are not enough nodes
%%   for the policy
%% * Removing a node will not create a new mirror even if the policy
%%   logic wants it (since this gives us a good way to lose messages
%%   on cluster shutdown, by repeated failover to new nodes)
%%
%% The first two are change_policy, the last two are change_cluster

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-define(QNAME, <<"ha.test">>).
-define(POLICY, <<"^ha.test$">>). %% " emacs
-define(VHOST, <<"/">>).

all() ->
    [
      {group, unclustered},
      {group, clustered}
    ].

groups() ->
    [
      {unclustered, [], [
          {cluster_size_5, [], [
              change_cluster
            ]}
        ]},
      {clustered, [], [
          {cluster_size_2, [], [
              vhost_deletion,
              force_delete_if_no_master,
              promote_on_shutdown,
              promote_on_failure,
              follower_recovers_after_vhost_failure,
              follower_recovers_after_vhost_down_and_up,
              master_migrates_on_vhost_down,
              follower_recovers_after_vhost_down_and_master_migrated,
              queue_survive_adding_dead_vhost_mirror,
              dynamic_mirroring
            ]},
          {cluster_size_3, [], [
              change_policy,
              rapid_change,
              nodes_policy_should_pick_master_from_its_params,
              promote_follower_after_standalone_restart,
              queue_survive_adding_dead_vhost_mirror,
              rebalance_all,
              rebalance_exactly,
              rebalance_nodes,
              rebalance_multiple_blocked
            ]}
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(unclustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]);
init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(cluster_size_2, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 2}]);
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
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
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
%% Test Cases
%% -------------------------------------------------------------------

dynamic_mirroring(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, dynamic_mirroring1, [Config]).

dynamic_mirroring1(_Config) ->
    %% Just unit tests of the node selection logic, see multi node
    %% tests for the rest...
    Test = fun ({NewM, NewSs, ExtraSs}, Policy, Params,
                {MNode, SNodes, SSNodes}, All) ->
                   {ok, M} = rabbit_mirror_queue_misc:module(Policy),
                   {NewM, NewSs0} = M:suggested_queue_nodes(
                                      Params, MNode, SNodes, SSNodes, All),
                   NewSs1 = lists:sort(NewSs0),
                   case dm_list_match(NewSs, NewSs1, ExtraSs) of
                       ok    -> ok;
                       error -> exit({no_match, NewSs, NewSs1, ExtraSs})
                   end
           end,

    Test({a,[b,c],0},<<"all">>,'_',{a,[],   []},   [a,b,c]),
    Test({a,[b,c],0},<<"all">>,'_',{a,[b,c],[b,c]},[a,b,c]),
    Test({a,[b,c],0},<<"all">>,'_',{a,[d],  [d]},  [a,b,c]),

    N = fun (Atoms) -> [list_to_binary(atom_to_list(A)) || A <- Atoms] end,

    %% Add a node
    Test({a,[b,c],0},<<"nodes">>,N([a,b,c]),{a,[b],[b]},[a,b,c,d]),
    Test({b,[a,c],0},<<"nodes">>,N([a,b,c]),{b,[a],[a]},[a,b,c,d]),
    %% Add two nodes and drop one
    Test({a,[b,c],0},<<"nodes">>,N([a,b,c]),{a,[d],[d]},[a,b,c,d]),
    %% Don't try to include nodes that are not running
    Test({a,[b],  0},<<"nodes">>,N([a,b,f]),{a,[b],[b]},[a,b,c,d]),
    %% If we can't find any of the nodes listed then just keep the master
    Test({a,[],   0},<<"nodes">>,N([f,g,h]),{a,[b],[b]},[a,b,c,d]),
    %% And once that's happened, still keep the master even when not listed,
    %% if nothing is synced
    Test({a,[b,c],0},<<"nodes">>,N([b,c]),  {a,[], []}, [a,b,c,d]),
    Test({a,[b,c],0},<<"nodes">>,N([b,c]),  {a,[b],[]}, [a,b,c,d]),
    %% But if something is synced we can lose the master - but make
    %% sure we pick the new master from the nodes which are synced!
    Test({b,[c],  0},<<"nodes">>,N([b,c]),  {a,[b],[b]},[a,b,c,d]),
    Test({b,[c],  0},<<"nodes">>,N([c,b]),  {a,[b],[b]},[a,b,c,d]),

    Test({a,[],   1},<<"exactly">>,2,{a,[],   []},   [a,b,c,d]),
    Test({a,[],   2},<<"exactly">>,3,{a,[],   []},   [a,b,c,d]),
    Test({a,[c],  0},<<"exactly">>,2,{a,[c],  [c]},  [a,b,c,d]),
    Test({a,[c],  1},<<"exactly">>,3,{a,[c],  [c]},  [a,b,c,d]),
    Test({a,[c],  0},<<"exactly">>,2,{a,[c,d],[c,d]},[a,b,c,d]),
    Test({a,[c,d],0},<<"exactly">>,3,{a,[c,d],[c,d]},[a,b,c,d]),

    passed.

%% Does the first list match the second where the second is required
%% to have exactly Extra superfluous items?
dm_list_match([],     [],      0)     -> ok;
dm_list_match(_,      [],     _Extra) -> error;
dm_list_match([H|T1], [H |T2], Extra) -> dm_list_match(T1, T2, Extra);
dm_list_match(L1,     [_H|T2], Extra) -> dm_list_match(L1, T2, Extra - 1).

change_policy(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),

    %% When we first declare a queue with no policy, it's not HA.
    amqp_channel:call(ACh, #'queue.declare'{queue = ?QNAME}),
    timer:sleep(200),
    assert_followers(A, ?QNAME, {A, ''}),

    %% Give it policy "all", it becomes HA and gets all mirrors
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, ?POLICY, <<"all">>),
    assert_followers(A, ?QNAME, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),

    %% Give it policy "nodes", it gets specific mirrors
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, ?POLICY,
      {<<"nodes">>, [rabbit_misc:atom_to_binary(A),
                     rabbit_misc:atom_to_binary(B)]}),
    assert_followers(A, ?QNAME, {A, [B]}, [{A, [B, C]}]),

    %% Now explicitly change the mirrors
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, ?POLICY,
      {<<"nodes">>, [rabbit_misc:atom_to_binary(A),
                     rabbit_misc:atom_to_binary(C)]}),
    assert_followers(A, ?QNAME, {A, [C]}, [{A, [B, C]}]),

    %% Clear the policy, and we go back to non-mirrored
    ok = rabbit_ct_broker_helpers:clear_policy(Config, A, ?POLICY),
    assert_followers(A, ?QNAME, {A, ''}),

    %% Test switching "away" from an unmirrored node
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, ?POLICY,
      {<<"nodes">>, [rabbit_misc:atom_to_binary(B),
                     rabbit_misc:atom_to_binary(C)]}),
    assert_followers(A, ?QNAME, {B, [C]}, [{A, []}, {A, [B]}, {A, [C]}, {A, [B, C]}]),

    ok.

change_cluster(Config) ->
    [A, B, C, D, E] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:cluster_nodes(Config, [A, B, C]),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),

    amqp_channel:call(ACh, #'queue.declare'{queue = ?QNAME}),
    assert_followers(A, ?QNAME, {A, ''}),

    %% Give it policy exactly 4, it should mirror to all 3 nodes
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, ?POLICY, {<<"exactly">>, 4}),
    assert_followers(A, ?QNAME, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),

    %% Add D and E, D or E joins in
    rabbit_ct_broker_helpers:cluster_nodes(Config, [A, D, E]),
    assert_followers(A, ?QNAME, [{A, [B, C, D]}, {A, [B, C, E]}], [{A, [B, C]}]),

    %% Remove one, the other joins in
    rabbit_ct_broker_helpers:stop_node(Config, D),
    assert_followers(A, ?QNAME, [{A, [B, C, D]}, {A, [B, C, E]}], [{A, [B, C]}]),

    ok.

rapid_change(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    {_Pid, MRef} = spawn_monitor(
                     fun() ->
                             [rapid_amqp_ops(ACh, I) || I <- lists:seq(1, 100)]
                     end),
    rapid_loop(Config, A, MRef),
    ok.

rapid_amqp_ops(Ch, I) ->
    Payload = list_to_binary(integer_to_list(I)),
    amqp_channel:call(Ch, #'queue.declare'{queue = ?QNAME}),
    amqp_channel:cast(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = ?QNAME},
                      #amqp_msg{payload = Payload}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue    = ?QNAME,
                                                no_ack   = true}, self()),
    receive #'basic.consume_ok'{} -> ok
    end,
    receive {#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
            ok
    end,
    amqp_channel:call(Ch, #'queue.delete'{queue = ?QNAME}).

rapid_loop(Config, Node, MRef) ->
    receive
        {'DOWN', MRef, process, _Pid, normal} ->
            ok;
        {'DOWN', MRef, process, _Pid, Reason} ->
            exit({amqp_ops_died, Reason})
    after 0 ->
            rabbit_ct_broker_helpers:set_ha_policy(Config, Node, ?POLICY,
              <<"all">>),
            ok = rabbit_ct_broker_helpers:clear_policy(Config, Node, ?POLICY),
            rapid_loop(Config, Node, MRef)
    end.

queue_survive_adding_dead_vhost_mirror(Config) ->
    rabbit_ct_broker_helpers:force_vhost_failure(Config, 1, <<"/">>),
    NodeA = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ChA = rabbit_ct_client_helpers:open_channel(Config, NodeA),
    QName = <<"queue_survive_adding_dead_vhost_mirror-q-1">>,
    amqp_channel:call(ChA, #'queue.declare'{queue = QName}),
    Q = find_queue(QName, NodeA),
    Pid = proplists:get_value(pid, Q),
    rabbit_ct_broker_helpers:set_ha_policy_all(Config),
    %% Queue should not fail
    Q1 = find_queue(QName, NodeA),
    Pid = proplists:get_value(pid, Q1).

%% Vhost deletion needs to successfully tear down policies and queues
%% with policies. At least smoke-test that it doesn't blow up.
vhost_deletion(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_broker_helpers:set_ha_policy_all(Config),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    amqp_channel:call(ACh, #'queue.declare'{queue = <<"vhost_deletion-q">>}),
    ok = rpc:call(A, rabbit_vhost, delete, [<<"/">>, <<"acting-user">>]),
    ok.

force_delete_if_no_master(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<"^ha.nopromote">>,
      <<"all">>),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    [begin
         amqp_channel:call(ACh, #'queue.declare'{queue   = Q,
                                                 durable = true}),
         rabbit_ct_client_helpers:publish(ACh, Q, 10)
     end || Q <- [<<"ha.nopromote.test1">>, <<"ha.nopromote.test2">>]],
    ok = rabbit_ct_broker_helpers:restart_node(Config, B),
    ok = rabbit_ct_broker_helpers:stop_node(Config, A),

    BCh = rabbit_ct_client_helpers:open_channel(Config, B),
    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(
         BCh, #'queue.declare'{queue   = <<"ha.nopromote.test1">>,
                               durable = true})),

    BCh1 = rabbit_ct_client_helpers:open_channel(Config, B),
    ?assertExit(
        {{shutdown, {server_initiated_close, 404, _}}, _},
        amqp_channel:call(
            BCh1, #'queue.declare'{queue   = <<"ha.nopromote.test2">>,
                                   durable = true})),
    BCh2 = rabbit_ct_client_helpers:open_channel(Config, B),
    #'queue.delete_ok'{} =
        amqp_channel:call(BCh2, #'queue.delete'{queue = <<"ha.nopromote.test1">>}),
    %% Delete with if_empty will fail, since we don't know if the queue is empty
    ?assertExit(
        {{shutdown, {server_initiated_close, 406, _}}, _},
        amqp_channel:call(BCh2, #'queue.delete'{queue = <<"ha.nopromote.test2">>,
                                                if_empty = true})),
    BCh3 = rabbit_ct_client_helpers:open_channel(Config, B),
    #'queue.delete_ok'{} =
        amqp_channel:call(BCh3, #'queue.delete'{queue = <<"ha.nopromote.test2">>}),
    ok.

promote_on_failure(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<"^ha.promote">>,
      <<"all">>, [{<<"ha-promote-on-failure">>, <<"always">>}]),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<"^ha.nopromote">>,
      <<"all">>, [{<<"ha-promote-on-failure">>, <<"when-synced">>}]),

    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    [begin
         amqp_channel:call(ACh, #'queue.declare'{queue   = Q,
                                                 durable = true}),
         rabbit_ct_client_helpers:publish(ACh, Q, 10)
     end || Q <- [<<"ha.promote.test">>, <<"ha.nopromote.test">>]],
    ok = rabbit_ct_broker_helpers:restart_node(Config, B),
    ok = rabbit_ct_broker_helpers:kill_node(Config, A),
    BCh = rabbit_ct_client_helpers:open_channel(Config, B),
    #'queue.declare_ok'{message_count = 0} =
        amqp_channel:call(
          BCh, #'queue.declare'{queue   = <<"ha.promote.test">>,
                                durable = true}),
    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(
         BCh, #'queue.declare'{queue   = <<"ha.nopromote.test">>,
                               durable = true})),
    ok = rabbit_ct_broker_helpers:start_node(Config, A),
    ACh2 = rabbit_ct_client_helpers:open_channel(Config, A),
    #'queue.declare_ok'{message_count = 10} =
        amqp_channel:call(
          ACh2, #'queue.declare'{queue   = <<"ha.nopromote.test">>,
                                 durable = true}),
    ok.

promote_on_shutdown(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<"^ha.promote">>,
      <<"all">>, [{<<"ha-promote-on-shutdown">>, <<"always">>}]),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<"^ha.nopromote">>,
      <<"all">>),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<"^ha.nopromoteonfailure">>,
      <<"all">>, [{<<"ha-promote-on-failure">>, <<"when-synced">>},
                  {<<"ha-promote-on-shutdown">>, <<"always">>}]),

    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    [begin
         amqp_channel:call(ACh, #'queue.declare'{queue   = Q,
                                                 durable = true}),
         rabbit_ct_client_helpers:publish(ACh, Q, 10)
     end || Q <- [<<"ha.promote.test">>,
                  <<"ha.nopromote.test">>,
                  <<"ha.nopromoteonfailure.test">>]],
    ok = rabbit_ct_broker_helpers:restart_node(Config, B),
    ok = rabbit_ct_broker_helpers:stop_node(Config, A),
    BCh = rabbit_ct_client_helpers:open_channel(Config, B),
    BCh1 = rabbit_ct_client_helpers:open_channel(Config, B),
    #'queue.declare_ok'{message_count = 0} =
        amqp_channel:call(
          BCh, #'queue.declare'{queue   = <<"ha.promote.test">>,
                                durable = true}),
    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(
         BCh, #'queue.declare'{queue   = <<"ha.nopromote.test">>,
                               durable = true})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(
         BCh1, #'queue.declare'{queue   = <<"ha.nopromoteonfailure.test">>,
                               durable = true})),
    ok = rabbit_ct_broker_helpers:start_node(Config, A),
    ACh2 = rabbit_ct_client_helpers:open_channel(Config, A),
    #'queue.declare_ok'{message_count = 10} =
        amqp_channel:call(
          ACh2, #'queue.declare'{queue   = <<"ha.nopromote.test">>,
                                 durable = true}),
    #'queue.declare_ok'{message_count = 10} =
        amqp_channel:call(
          ACh2, #'queue.declare'{queue   = <<"ha.nopromoteonfailure.test">>,
                                 durable = true}),
    ok.

nodes_policy_should_pick_master_from_its_params(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    ?assertEqual(true, apply_policy_to_declared_queue(Config, Ch, [A], [all])),
    %% --> Master: A
    %%     Slaves: [B, C] or [C, B]
    SSPids = ?awaitMatch(SSPids when is_list(SSPids),
                                     proplists:get_value(synchronised_slave_pids,
                                                         find_queue(?QNAME, A)),
                                     10000),

    %% Choose mirror that isn't the first sync mirror. Cover a bug that always
    %% chose the first, even if it was not part of the policy
    LastSlave = node(lists:last(SSPids)),
    ?assertEqual(true, apply_policy_to_declared_queue(Config, Ch, [A],
        [{nodes, [LastSlave]}])),
    %% --> Master: B or C (depends on the order of current mirrors )
    %%     Slaves: []

    %% Now choose a new master that isn't synchronised. The previous
    %% policy made sure that the queue only runs on one node (the last
    %% from the initial synchronised list). Thus, by taking the first
    %% node from this list, we know it is not synchronised.
    %%
    %% Because the policy doesn't cover any synchronised mirror, RabbitMQ
    %% should instead use an existing synchronised mirror as the new master,
    %% even though that isn't in the policy.
    ?assertEqual(true, apply_policy_to_declared_queue(Config, Ch, [A],
                                                      [{nodes, [LastSlave, A]}])),
    %% --> Master: B or C (same as previous policy)
    %%     Slaves: [A]

    NewMaster = node(erlang:hd(SSPids)),
    ?assertEqual(true, apply_policy_to_declared_queue(Config, Ch, [A],
        [{nodes, [NewMaster]}])),
    %% --> Master: B or C (the other one compared to previous policy)
    %%     Slaves: []

    amqp_channel:call(Ch, #'queue.delete'{queue = ?QNAME}),
    _ = rabbit_ct_broker_helpers:clear_policy(Config, A, ?POLICY).

follower_recovers_after_vhost_failure(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:set_ha_policy_all(Config),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    QName = <<"follower_recovers_after_vhost_failure-q">>,
    amqp_channel:call(ACh, #'queue.declare'{queue = QName}),
    timer:sleep(500),
    assert_followers(A, QName, {A, [B]}, [{A, []}]),

    %% Crash vhost on a node hosting a mirror
    {ok, Sup} = rabbit_ct_broker_helpers:rpc(Config, B, rabbit_vhost_sup_sup, get_vhost_sup, [<<"/">>]),
    exit(Sup, foo),

    assert_followers(A, QName, {A, [B]}, [{A, []}]).

follower_recovers_after_vhost_down_and_up(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:set_ha_policy_all(Config),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    QName = <<"follower_recovers_after_vhost_down_and_up-q">>,
    amqp_channel:call(ACh, #'queue.declare'{queue = QName}),
    timer:sleep(200),
    assert_followers(A, QName, {A, [B]}, [{A, []}]),

    %% Crash vhost on a node hosting a mirror
    rabbit_ct_broker_helpers:force_vhost_failure(Config, B, <<"/">>),
    %% rabbit_ct_broker_helpers:force_vhost_failure/2 will retry up to 10 times to
    %% make sure that the top vhost supervision tree process did go down. MK.
    timer:sleep(500),
    %% Vhost is back up
    case rabbit_ct_broker_helpers:rpc(Config, B, rabbit_vhost_sup_sup, start_vhost, [<<"/">>]) of
      {ok, _Sup} -> ok;
      {error,{already_started, _Sup}} -> ok
    end,

    assert_followers(A, QName, {A, [B]}, [{A, []}]).

master_migrates_on_vhost_down(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:set_ha_policy_all(Config),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    QName = <<"master_migrates_on_vhost_down-q">>,
    amqp_channel:call(ACh, #'queue.declare'{queue = QName}),
    timer:sleep(500),
    assert_followers(A, QName, {A, [B]}, [{A, []}]),

    %% Crash vhost on the node hosting queue master
    rabbit_ct_broker_helpers:force_vhost_failure(Config, A, <<"/">>),
    timer:sleep(500),
    assert_followers(A, QName, {B, []}).

follower_recovers_after_vhost_down_and_master_migrated(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:set_ha_policy_all(Config),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    QName = <<"follower_recovers_after_vhost_down_and_master_migrated-q">>,
    amqp_channel:call(ACh, #'queue.declare'{queue = QName}),
    timer:sleep(500),
    assert_followers(A, QName, {A, [B]}, [{A, []}]),
    %% Crash vhost on the node hosting queue master
    rabbit_ct_broker_helpers:force_vhost_failure(Config, A, <<"/">>),
    timer:sleep(500),
    assert_followers(B, QName, {B, []}),

    %% Restart the vhost on the node (previously) hosting queue master
    case rabbit_ct_broker_helpers:rpc(Config, A, rabbit_vhost_sup_sup, start_vhost, [<<"/">>]) of
      {ok, _Sup} -> ok;
      {error,{already_started, _Sup}} -> ok
    end,
    timer:sleep(500),
    assert_followers(B, QName, {B, [A]}, [{B, []}]).

random_policy(Config) ->
    run_proper(fun prop_random_policy/1, [Config]).

failing_random_policies(Config) ->
    [A, B | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    %% Those set of policies were found as failing by PropEr in the
    %% `random_policy` test above. We add them explicitly here to make
    %% sure they get tested.
    ?assertEqual(true, test_random_policy(Config, Nodes,
        [{nodes, [A, B]}, {nodes, [A]}])),
    ?assertEqual(true, test_random_policy(Config, Nodes,
        [{exactly, 3}, undefined, all, {nodes, [B]}])),
    ?assertEqual(true, test_random_policy(Config, Nodes,
        [all, undefined, {exactly, 2}, all, {exactly, 3}, {exactly, 3},
          undefined, {exactly, 3}, all])).

promote_follower_after_standalone_restart(Config) ->
    %% Tests that mirrors can be brought up standalone after forgetting the rest
    %% of the cluster. Slave ordering should be irrelevant.
    %% https://github.com/rabbitmq/rabbitmq-server/issues/1213
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),

    rabbit_ct_broker_helpers:set_ha_policy(Config, A, ?POLICY, <<"all">>),
    amqp_channel:call(Ch, #'queue.declare'{queue = ?QNAME,
                                           durable = true}),

    rabbit_ct_client_helpers:publish(Ch, ?QNAME, 15),
    rabbit_ct_client_helpers:close_channel(Ch),

    rabbit_ct_helpers:await_condition(fun() ->
                                        15 =:= proplists:get_value(messages, find_queue(?QNAME, A))
                                      end, 60000),

    rabbit_ct_broker_helpers:stop_node(Config, C),
    rabbit_ct_broker_helpers:stop_node(Config, B),
    rabbit_ct_broker_helpers:stop_node(Config, A),

    %% Restart one mirror
    forget_cluster_node(Config, B, C),
    forget_cluster_node(Config, B, A),

    ok = rabbit_ct_broker_helpers:start_node(Config, B),
    rabbit_ct_helpers:await_condition(fun() ->
                                        15 =:= proplists:get_value(messages, find_queue(?QNAME, B))
                                      end, 60000),
    ok = rabbit_ct_broker_helpers:stop_node(Config, B),

    %% Restart the other
    forget_cluster_node(Config, C, B),
    forget_cluster_node(Config, C, A),

    ok = rabbit_ct_broker_helpers:start_node(Config, C),
    15 = proplists:get_value(messages, find_queue(?QNAME, C)),
    ok = rabbit_ct_broker_helpers:stop_node(Config, C),

    ok.

rebalance_all(Config) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),

    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    Q4 = <<"q4">>,
    Q5 = <<"q5">>,

    amqp_channel:call(ACh, #'queue.declare'{queue = Q1}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q2}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q3}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q4}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q5}),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<"q.*">>, <<"all">>),
    timer:sleep(1000),

    rabbit_ct_client_helpers:publish(ACh, Q1, 5),
    rabbit_ct_client_helpers:publish(ACh, Q2, 3),
    assert_followers(A, Q1, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),
    assert_followers(A, Q2, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),
    assert_followers(A, Q3, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),
    assert_followers(A, Q4, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),
    assert_followers(A, Q5, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),

    {ok, Summary} = rpc:call(A, rabbit_amqqueue, rebalance, [classic, ".*", ".*"]),

    %% Check that we have at most 2 queues per node
    Condition1 = fun() ->
                     lists:all(fun(NodeData) ->
                              lists:all(fun({_, V}) when is_integer(V) -> V =< 2;
                                           (_) -> true end,
                                        NodeData)
                      end, Summary)
                 end,
    rabbit_ct_helpers:await_condition(Condition1, 60000),

    %% Check that Q1 and Q2 haven't moved
    assert_followers(A, Q1, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),
    assert_followers(A, Q2, {A, [B, C]}, [{A, []}, {A, [B]}, {A, [C]}]),

    ok.

rebalance_exactly(Config) ->
    [A, _, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),

    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    Q4 = <<"q4">>,
    Q5 = <<"q5">>,

    amqp_channel:call(ACh, #'queue.declare'{queue = Q1}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q2}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q3}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q4}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q5}),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<"q.*">>, {<<"exactly">>, 2}),
    timer:sleep(1000),

    %% Rebalancing happens with existing mirrors. Thus, before we
    %% can verify it works as expected, we need the queues to be on
    %% different mirrors.
    %%
    %% We only test Q3, Q4 and Q5 because the first two are expected to
    %% stay where they are.
    ensure_queues_are_mirrored_on_different_mirrors([Q3, Q4, Q5], A, ACh),

    rabbit_ct_client_helpers:publish(ACh, Q1, 5),
    rabbit_ct_client_helpers:publish(ACh, Q2, 3),

    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q1, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q2, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q3, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q4, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q5, A)))),

    {ok, Summary} = rpc:call(A, rabbit_amqqueue, rebalance, [classic, ".*", ".*"]),

    %% Check that we have at most 2 queues per node
    Condition1 = fun() ->
                     lists:all(fun(NodeData) ->
                                   lists:all(fun({_, V}) when is_integer(V) -> V =< 2;
                                                (_) -> true end,
                                             NodeData)
                               end, Summary)
                 end,
    rabbit_ct_helpers:await_condition(Condition1, 60000),

    %% Check that Q1 and Q2 haven't moved
    Condition2 = fun () ->
                    A =:= node(proplists:get_value(pid, find_queue(Q1, A))) andalso
                    A =:= node(proplists:get_value(pid, find_queue(Q2, A)))
                 end,
    rabbit_ct_helpers:await_condition(Condition2, 40000),

    ok.

ensure_queues_are_mirrored_on_different_mirrors(Queues, Master, Ch) ->
    SNodes = [node(SPid)
              || Q <- Queues,
                 SPid <- proplists:get_value(slave_pids, find_queue(Q, Master))],
    UniqueSNodes = lists:usort(SNodes),
    case UniqueSNodes of
        [_] ->
            %% All passed queues are on the same mirror. Let's redeclare
            %% one of them and test again.
            Q = hd(Queues),
            amqp_channel:call(Ch, #'queue.delete'{queue = Q}),
            amqp_channel:call(Ch, #'queue.declare'{queue = Q}),
            ensure_queues_are_mirrored_on_different_mirrors(Queues, Master, Ch);
        _ ->
            ok
    end.

rebalance_nodes(Config) ->
    [A, B, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),

    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    Q4 = <<"q4">>,
    Q5 = <<"q5">>,

    amqp_channel:call(ACh, #'queue.declare'{queue = Q1}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q2}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q3}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q4}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q5}),
    rabbit_ct_broker_helpers:set_ha_policy(
      Config, A, <<"q.*">>,
      {<<"nodes">>, [rabbit_misc:atom_to_binary(A), rabbit_misc:atom_to_binary(B)]}),
    timer:sleep(1000),

    rabbit_ct_client_helpers:publish(ACh, Q1, 5),
    rabbit_ct_client_helpers:publish(ACh, Q2, 3),

    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q1, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q2, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q3, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q4, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q5, A)))),

    {ok, Summary} = rpc:call(A, rabbit_amqqueue, rebalance, [classic, ".*", ".*"]),

    %% Check that we have at most 3 queues per node
    ?assert(lists:all(fun(NodeData) ->
                              lists:all(fun({_, V}) when is_integer(V) -> V =< 3;
                                           (_) -> true end,
                                        NodeData)
                      end, Summary)),
    %% Check that Q1 and Q2 haven't moved
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q1, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q2, A)))),

    ok.

rebalance_multiple_blocked(Config) ->
    [A, _, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ACh = rabbit_ct_client_helpers:open_channel(Config, A),
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    Q4 = <<"q4">>,
    Q5 = <<"q5">>,
    amqp_channel:call(ACh, #'queue.declare'{queue = Q1}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q2}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q3}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q4}),
    amqp_channel:call(ACh, #'queue.declare'{queue = Q5}),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q1, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q2, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q3, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q4, A)))),
    ?assertEqual(A, node(proplists:get_value(pid, find_queue(Q5, A)))),
    ?assert(rabbit_ct_broker_helpers:rpc(
              Config, A,
              ?MODULE, rebalance_multiple_blocked1, [Config])).

rebalance_multiple_blocked1(_) ->
    Parent = self(),
    Fun = fun() ->
                  Parent ! rabbit_amqqueue:rebalance(classic, ".*", ".*")
          end,
    spawn(Fun),
    spawn(Fun),
    Rets = [receive Ret1 -> Ret1 end,
            receive Ret2 -> Ret2 end],
    lists:member({error, rebalance_in_progress}, Rets).

%%----------------------------------------------------------------------------

assert_followers(RPCNode, QName, Exp) ->
    assert_followers(RPCNode, QName, Exp, []).

assert_followers(RPCNode, QName, Exp, PermittedIntermediate) ->
    assert_followers0(RPCNode, QName, Exp,
                  [{get(previous_exp_m_node), get(previous_exp_s_nodes)} |
                   PermittedIntermediate], 1000).

assert_followers0(_RPCNode, _QName, [], _PermittedIntermediate, _Attempts) ->
    error(invalid_expectation);
assert_followers0(RPCNode, QName, [{ExpMNode, ExpSNodes}|T], PermittedIntermediate, Attempts) ->
    case assert_followers1(RPCNode, QName, {ExpMNode, ExpSNodes}, PermittedIntermediate, Attempts, nofail) of
        ok ->
            ok;
        failed ->
            assert_followers0(RPCNode, QName, T, PermittedIntermediate, Attempts - 1)
    end;
assert_followers0(RPCNode, QName, {ExpMNode, ExpSNodes}, PermittedIntermediate, Attempts) ->
    assert_followers1(RPCNode, QName, {ExpMNode, ExpSNodes}, PermittedIntermediate, Attempts, fail).

assert_followers1(_RPCNode, _QName, _Exp, _PermittedIntermediate, 0, fail) ->
    error(give_up_waiting_for_followers);
assert_followers1(_RPCNode, _QName, _Exp, _PermittedIntermediate, 0, nofail) ->
    failed;
assert_followers1(RPCNode, QName, {ExpMNode, ExpSNodes}, PermittedIntermediate, Attempts, FastFail) ->
    Q = find_queue(QName, RPCNode),
    Pid = proplists:get_value(pid, Q),
    SPids = proplists:get_value(slave_pids, Q),
    ActMNode = node(Pid),
    ActSNodes = case SPids of
                    '' -> '';
                    _  -> [node(SPid) || SPid <- SPids]
                end,
    case ExpMNode =:= ActMNode andalso equal_list(ExpSNodes, ActSNodes) of
        false ->
            %% It's an async change, so if nothing has changed let's
            %% just wait - of course this means if something does not
            %% change when expected then we time out the test which is
            %% a bit tedious
            case [{PermMNode, PermSNodes} || {PermMNode, PermSNodes} <- PermittedIntermediate,
                           PermMNode =:= ActMNode,
                           equal_list(PermSNodes, ActSNodes)] of
                [] ->
                    case FastFail of
                        fail ->
                            ct:fail("Expected ~p / ~p, got ~p / ~p~nat ~p~n",
                                    [ExpMNode, ExpSNodes, ActMNode, ActSNodes,
                                    get_stacktrace()]);
                        nofail ->
                            failed
                    end;
                State  ->
                    ct:pal("Waiting to leave state ~p~n Waiting for ~p~n",
                           [State, {ExpMNode, ExpSNodes}]),
                    timer:sleep(200),
                    assert_followers1(RPCNode, QName, {ExpMNode, ExpSNodes},
                                   PermittedIntermediate,
                                   Attempts - 1, FastFail)
            end;
        true ->
            put(previous_exp_m_node, ExpMNode),
            put(previous_exp_s_nodes, ExpSNodes),
            ok
    end.

equal_list('',    '')   -> true;
equal_list('',    _Act) -> false;
equal_list(_Exp,  '')   -> false;
equal_list([],    [])   -> true;
equal_list(_Exp,  [])   -> false;
equal_list([],    _Act) -> false;
equal_list([H|T], Act)  -> case lists:member(H, Act) of
                               true  -> equal_list(T, Act -- [H]);
                               false -> false
                           end.

find_queue(QName, RPCNode) ->
    find_queue(QName, RPCNode, 1000).

find_queue(QName, RPCNode, 0) -> error({did_not_find_queue, QName, RPCNode});
find_queue(QName, RPCNode, Attempts) ->
    Qs = rpc:call(RPCNode, rabbit_amqqueue, info_all, [?VHOST], infinity),
    case find_queue0(QName, Qs) of
        did_not_find_queue -> timer:sleep(100),
                              find_queue(QName, RPCNode, Attempts - 1);
        Q -> Q
    end.

find_queue0(QName, Qs) ->
    case [Q || Q <- Qs, proplists:get_value(name, Q) =:=
                   rabbit_misc:r(?VHOST, queue, QName)] of
        [R] -> R;
        []  -> did_not_find_queue
    end.

get_stacktrace() ->
    try
        throw(e)
    catch
        _:e:Stacktrace ->
            Stacktrace
    end.

%%----------------------------------------------------------------------------
run_proper(Fun, Args) ->
    ?assertEqual(true,
      proper:counterexample(erlang:apply(Fun, Args),
        [{numtests, 25},
          {on_output, fun(F, A) -> ct:pal(?LOW_IMPORTANCE, F, A) end}])).

prop_random_policy(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(
              Config, nodename),
    ?FORALL(
       Policies, non_empty(list(policy_gen(Nodes))),
       test_random_policy(Config, Nodes, Policies)).

apply_policy_to_declared_queue(Config, Ch, Nodes, Policies) ->
    [NodeA | _] = Nodes,
    amqp_channel:call(Ch, #'queue.declare'{queue = ?QNAME}),
    %% Add some load so mirrors can be busy synchronising
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, 100000),
    %% Apply policies in parallel on all nodes
    apply_in_parallel(Config, Nodes, Policies),
    %% Give it some time to generate all internal notifications
    timer:sleep(2000),
    %% Check the result
    wait_for_last_policy(?QNAME, NodeA, Policies, 30).

test_random_policy(Config, Nodes, Policies) ->
    [NodeA | _] = Nodes,
    Ch = rabbit_ct_client_helpers:open_channel(Config, NodeA),
    Result = apply_policy_to_declared_queue(Config, Ch, Nodes, Policies),
    %% Cleanup
    amqp_channel:call(Ch, #'queue.delete'{queue = ?QNAME}),
    _ = rabbit_ct_broker_helpers:clear_policy(Config, NodeA, ?POLICY),
    Result.

apply_in_parallel(Config, Nodes, Policies) ->
    Self = self(),
    [spawn_link(fun() ->
                        [begin

                             apply_policy(Config, N, Policy)
                         end || Policy <- Policies],
                        Self ! parallel_task_done
                end) || N <- Nodes],
    [receive
         parallel_task_done ->
             ok
     end || _ <- Nodes].

%% Proper generators
policy_gen(Nodes) ->
    %% Stop mirroring needs to be called often to trigger rabbitmq-server#803
    frequency([{3, undefined},
               {1, all},
               {1, {nodes, nodes_gen(Nodes)}},
               {1, {exactly, choose(1, 3)}}
              ]).

nodes_gen(Nodes) ->
    ?LET(List, non_empty(list(oneof(Nodes))),
         sets:to_list(sets:from_list(List))).

%% Checks
wait_for_last_policy(QueueName, NodeA, TestedPolicies, Tries) ->
    %% Ensure the owner/master is able to process a call request,
    %% which means that all pending casts have been processed.
    %% Use the information returned by owner/master to verify the
    %% test result
    Info = find_queue(QueueName, NodeA),
    Pid = proplists:get_value(pid, Info),
    Node = node(Pid),
    %% Gets owner/master
    case rpc:call(Node, gen_server, call, [Pid, info], 5000) of
        {badrpc, _} ->
            %% The queue is probably being migrated to another node.
            %% Let's wait a bit longer.
            timer:sleep(1000),
            wait_for_last_policy(QueueName, NodeA, TestedPolicies, Tries - 1);
        Result  ->
            FinalInfo = case Result of
                            {ok, I} -> I;
                            _ when is_list(Result) ->
                                Result
                        end,
            %% The last policy is the final state
            LastPolicy = lists:last(TestedPolicies),
            case verify_policy(LastPolicy, FinalInfo) of
                true ->
                    true;
                false when Tries =:= 1 ->
                    Policies = rpc:call(Node, rabbit_policy, list, [], 5000),
                    ct:pal(
                      "Last policy not applied:~n"
                      "  Queue node:          ~s (~p)~n"
                      "  Queue info:          ~p~n"
                      "  Configured policies: ~p~n"
                      "  Tested policies:     ~p",
                      [Node, Pid, FinalInfo, Policies, TestedPolicies]),
                    false;
                false ->
                    timer:sleep(1000),
                    wait_for_last_policy(QueueName, NodeA, TestedPolicies,
                      Tries - 1)
            end
    end.

verify_policy(undefined, Info) ->
    %% If the queue is not mirrored, it returns ''
    '' == proplists:get_value(slave_pids, Info);
verify_policy(all, Info) ->
    2 == length(proplists:get_value(slave_pids, Info));
verify_policy({exactly, 1}, Info) ->
    %% If the queue is mirrored, it returns a list
    [] == proplists:get_value(slave_pids, Info);
verify_policy({exactly, N}, Info) ->
    (N - 1) == length(proplists:get_value(slave_pids, Info));
verify_policy({nodes, Nodes}, Info) ->
    Master = node(proplists:get_value(pid, Info)),
    Slaves = [node(P) || P <- proplists:get_value(slave_pids, Info)],
    lists:sort(Nodes) == lists:sort([Master | Slaves]).

%% Policies
apply_policy(Config, N, undefined) ->
    _ = rabbit_ct_broker_helpers:clear_policy(Config, N, ?POLICY);
apply_policy(Config, N, all) ->
    rabbit_ct_broker_helpers:set_ha_policy(
      Config, N, ?POLICY, <<"all">>,
      [{<<"ha-sync-mode">>, <<"automatic">>}]);
apply_policy(Config, N, {nodes, Nodes}) ->
    NNodes = [rabbit_misc:atom_to_binary(Node) || Node <- Nodes],
    rabbit_ct_broker_helpers:set_ha_policy(
      Config, N, ?POLICY, {<<"nodes">>, NNodes},
      [{<<"ha-sync-mode">>, <<"automatic">>}]);
apply_policy(Config, N, {exactly, Exactly}) ->
    rabbit_ct_broker_helpers:set_ha_policy(
      Config, N, ?POLICY, {<<"exactly">>, Exactly},
      [{<<"ha-sync-mode">>, <<"automatic">>}]).

forget_cluster_node(Config, Node, NodeToRemove) ->
    rabbit_ct_broker_helpers:rabbitmqctl(
      Config, Node, ["forget_cluster_node", "--offline", NodeToRemove]).
