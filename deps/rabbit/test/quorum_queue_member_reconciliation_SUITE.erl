%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.


-module(quorum_queue_member_reconciliation_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-compile([nowarn_export_all, export_all]).

-import(rabbit_ct_helpers, [consistently/3]).

-define(RA_SYSTEM, quorum_queues).
-define(FORCE_DELETE_TABLE, rabbit_qq_force_delete_pending).

%% The reconciler has two modes of triggering itself
%% - timer based
%% - event based
%% The default config of this test has Interval very short - 5 second which is lower than
%% wait_until timeout. Meaninig that even if all domain triggers (node_up/down, policy_set, etc)
%% are disconnected tests would be still green.
%% So to test triggers it is essential to set Interval high enough (the very default value of 60 minutes is perfect)
%%
%% TODO: test `policy_set` trigger

all() ->
    [
     {group, unclustered},
     {group, unclustered_triggers},
     {group, clustered_force_delete}
    ].

groups() ->
    [
     {unclustered, [], %% low interval, even if triggers do not work all tests should pass
      [
       {quorum_queue_3, [], [auto_grow, auto_grow_drained_node, auto_shrink]}
      ]},
     %% uses an interval longer than `wait_until` (30s by default)
     {unclustered_triggers, [],
      [
       %% see also `auto_grow_drained_node`
       {quorum_queue_3, [], [auto_grow, auto_shrink]}
      ]},
     %% Unlike the groups above, these need a formed cluster: a member is leaked
     %% by taking one of its nodes down during a force delete.
     {clustered_force_delete, [],
      [
       {quorum_queue_3, [], [leaked_member_is_eventually_force_deleted,
                             stale_incarnation_is_not_deleted,
                             unverifiable_member_is_dropped,
                             force_delete_member_uid_guard]}
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(unclustered, Config0) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000},
                                   {quorum_membership_reconciliation_enabled, true},
                                   {quorum_membership_reconciliation_auto_remove, true},
                                   {quorum_membership_reconciliation_interval, 5000},
                                   {quorum_membership_reconciliation_trigger_interval, 2000},
                                   {quorum_membership_reconciliation_target_group_size, 3}]}),
    rabbit_ct_helpers:set_config(Config1, [{rmq_nodes_clustered, false}]);
init_per_group(unclustered_triggers, Config0) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000},
                                   {quorum_membership_reconciliation_enabled, true},
                                   {quorum_membership_reconciliation_auto_remove, true},
                                   {quorum_membership_reconciliation_interval, 50000},
                                   {quorum_membership_reconciliation_trigger_interval, 2000},
                                   {quorum_membership_reconciliation_target_group_size, 3}]}),
    %% shrink timeout is set here because without it, when a node stopped right after a queue was created,
    %% the test will pass without any triggers because cluster change will likely happen before the trigger_interval,
    %% scheduled in response to queue_created event.
    %% See also a comment in `auto_shrink/1`.
    rabbit_ct_helpers:set_config(Config1, [{rmq_nodes_clustered, false},
                                           {quorum_membership_reconciliation_interval, 50000},
                                           {shrink_timeout, 2000}]);
init_per_group(clustered_force_delete, Config0) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000},
                                   {quorum_queue_force_delete_retry_interval, 3000}]}),
    rabbit_ct_helpers:set_config(Config1, [{rmq_nodes_clustered, true},
                                           {force_delete_group, true}]);
init_per_group(Group, Config) ->
    ClusterSize = 3,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    rabbit_ct_helpers:run_steps(Config1,
                                [fun merge_app_env/1 ] ++
                                    rabbit_ct_broker_helpers:setup_steps()).

end_per_group(unclustered, Config) ->
    Config;
end_per_group(unclustered_triggers, Config) ->
    Config;
end_per_group(clustered_force_delete, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    case is_force_delete_group(Config) andalso
        not rabbit_ct_broker_helpers:is_feature_flag_enabled(
              Config, track_qq_members_uids) of
        true ->
            {skip, "force delete retry requires track_qq_members_uids ff"};
        false ->
            init_per_testcase0(Testcase, Config)
    end.

init_per_testcase0(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>},
                                            {alt_2_queue_name, <<Q/binary, "_alt_2">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    %% The reconciliation groups start unclustered and join nodes as they go, so
    %% they have to be put back. The force delete group runs against a cluster
    %% that must stay formed for the next case.
    case is_force_delete_group(Config) of
        true ->
            ok;
        false ->
            [Server0, Server1, Server2] =
                rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
            Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
            amqp_channel:call(Ch, #'queue.delete'{
                                     queue = rabbit_data_coercion:to_binary(Testcase)}),
            reset_nodes([Server2, Server0], Server1)
    end,
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

auto_grow(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),

    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% There is only one node in the cluster at the moment
    {ok, Members, _} = ra:members({queue_utils:ra_name(QQ), Server1}),
    ?assertEqual(1, length(Members)),

    add_server_to_cluster(Server0, Server1),
    %% With 2 nodes in the cluster, target group size is not reached, so no
    %% new members should be available. Verify this holds over multiple
    %% reconciliation cycles.
    consistently(
        ?_assertEqual(1, length(element(2, ra:members({queue_utils:ra_name(QQ), Server1})))),
        1000, 4),

    add_server_to_cluster(Server2, Server1),
    %% With 3 nodes in the cluster, target size is met so eventually it should
    %% be 3 members
    wait_until(fun() ->
                       {ok, M, _} = ra:members({queue_utils:ra_name(QQ), Server1}),
                       3 =:= length(M)
               end).

auto_grow_drained_node(Config) ->
    %% NOTE: with large Interval (larger than wait_until) test will fail.
    %% the reason is that entering/exiting drain state does not emit events
    %% and even if they did via gen_event, they going to be only local to that node.
    %% so reconciliator has no choice but to wait full Interval
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),

    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% There is only one node in the cluster at the moment
    {ok, Members, _} = ra:members({queue_utils:ra_name(QQ), Server1}),
    ?assertEqual(1, length(Members)),

    add_server_to_cluster(Server0, Server1),
    %% mark Server0 as drained, which should mean the node is not a candiate
    %% for qq membership
    rabbit_ct_broker_helpers:mark_as_being_drained(Config, Server0),
    rabbit_ct_helpers:await_condition(
        fun () -> rabbit_ct_broker_helpers:is_being_drained_local_read(Config, Server0) end,
        10000),
    add_server_to_cluster(Server2, Server1),
    %% We have 3 nodes, but one is drained, so it will not be considered.
    %% Verify this holds over multiple reconciliation cycles.
    consistently(
        ?_assertEqual(1, length(element(2, ra:members({queue_utils:ra_name(QQ), Server1})))),
        1000, 5),

    rabbit_ct_broker_helpers:unmark_as_being_drained(Config, Server0),
    rabbit_ct_helpers:await_condition(
        fun () -> not rabbit_ct_broker_helpers:is_being_drained_local_read(Config, Server0) end,
        10000),
    %% We have 3 nodes, none is being drained, so we should grow membership to 3
    wait_until(fun() ->
                       {ok, M, _} = ra:members({queue_utils:ra_name(QQ), Server1}),
                       3 =:= length(M)
               end).

auto_shrink(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    add_server_to_cluster(Server0, Server1),
    add_server_to_cluster(Server2, Server1),

    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    wait_until(fun() ->
                       {ok, M, _} = ra:members({queue_utils:ra_name(QQ),
                                                Server1}),
                       3 =:= length(M)
               end),

    %% QQ member reconciliation does not act immediately but rather after a scheduled delay.
    %% So if this test wants to test that the reconciliator reacts to, say, node_down or a similar event,
    %% it has to wait at least a trigger_interval ms to pass before removing node. Otherwise
    %% the shrink effect would come from the previous trigger.
    %%
    %% When a `queue_created` trigger set up a timer to fire after a trigger_interval, the queue has 3 members
    %% and stop_app executes much quicker than the trigger_interval. Therefore the number of members
    %% will be updated even without a node_down event.

    timer:sleep(rabbit_ct_helpers:get_config(Config, shrink_timeout, 0)),

    ok = rabbit_control_helper:command(stop_app, Server2),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_cluster, forget_member,
                                      [Server2, false]),
    %% with one node 'forgotten', eventually the membership will shrink to 2
    wait_until(fun() ->
                       {ok, M, _} = ra:members({queue_utils:ra_name(QQ),
                                                Server1}),
                       2 =:= length(M)
               end).

%% A quorum queue member left behind on a node that was down during a force
%% delete is eventually force-deleted once the node returns.
leaked_member_is_eventually_force_deleted(Config) ->
    [Server0, _Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    RaName = queue_utils:ra_name(QQ),

    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    wait_until(fun() ->
                       3 =:= length(element(2, ra:members({RaName, Server0})))
               end),

    %% Server2's member is present before we take the node down.
    UId = uid_of(Config, Server2, RaName),
    ?assert(is_binary(UId)),

    %% Take Server2 down, then delete the queue from Server0. The delete still
    %% reaches quorum (2 of 3), so the record is removed, but the force delete of
    %% Server2's member fails because the node is unreachable.
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QQ}),

    %% The leaked member is recorded for retry on Server0.
    wait_until(fun() ->
                       [] =/= pending_lookup(Config, Server0, RaName, Server2)
               end),

    %% Bring Server2 back and the retry loop must eventually remove the member
    %% and drop the pending entry.
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
    wait_until(fun() ->
                       undefined =:= uid_of(Config, Server2, RaName)
               end),
    wait_until(fun() ->
                       [] =:= pending_lookup(Config, Server0, RaName, Server2)
               end).

%% A retry must never delete a member that belongs to a newer incarnation of the
%% same queue name: the UID guard drops the stale entry instead.
stale_incarnation_is_not_deleted(Config) ->
    [Server0, _Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    RaName = queue_utils:ra_name(QQ),
    QName = rabbit_misc:r(<<"/">>, queue, QQ),

    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    wait_until(fun() ->
                       3 =:= length(element(2, ra:members({RaName, Server0})))
               end),

    CurrentUId = uid_of(Config, Server2, RaName),
    ?assert(is_binary(CurrentUId)),

    %% Schedule a retry for Server2's member but tagged with a UID that does not
    %% match the live incarnation.
    StaleUId = <<"stale-incarnation-uid">>,
    ?assertNotEqual(StaleUId, CurrentUId),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, Server0,
           rabbit_quorum_queue_periodic_membership_reconciliation, schedule_force_delete,
           [QName, [{{RaName, Server2}, StaleUId}]]),

    %% The stale entry is dropped without touching the live member.
    wait_until(fun() ->
                       [] =:= pending_lookup(Config, Server0, RaName, Server2)
               end),
    ?assertEqual(CurrentUId, uid_of(Config, Server2, RaName)),
    ?assertEqual(3, length(element(2, ra:members({RaName, Server0})))).

%% A member with no recorded UID cannot be told apart from a newer incarnation,
%% so it is dropped from the retry set rather than deleted.
unverifiable_member_is_dropped(Config) ->
    [Server0, _Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    RaName = queue_utils:ra_name(QQ),
    QName = rabbit_misc:r(<<"/">>, queue, QQ),

    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    wait_until(fun() ->
                       3 =:= length(element(2, ra:members({RaName, Server0})))
               end),

    CurrentUId = uid_of(Config, Server2, RaName),
    ?assert(is_binary(CurrentUId)),

    %% Schedule a retry for Server2's member with no UID, as a queue record
    %% predating the feature flag would produce.
    ok = rabbit_ct_broker_helpers:rpc(
           Config, Server0,
           rabbit_quorum_queue_periodic_membership_reconciliation, schedule_force_delete,
           [QName, [{{RaName, Server2}, undefined}]]),

    %% The entry is dropped and the live member is left alone.
    wait_until(fun() ->
                       [] =:= pending_lookup(Config, Server0, RaName, Server2)
               end),
    ?assertEqual(CurrentUId, uid_of(Config, Server2, RaName)),
    ?assertEqual(3, length(element(2, ra:members({RaName, Server0})))).

%% The UID guard that protects a retry from deleting a newer incarnation is
%% evaluated on the member's own node, in the same call as the delete.
force_delete_member_uid_guard(Config) ->
    [Server0, _Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    RaName = queue_utils:ra_name(QQ),
    ServerId = {RaName, Server2},

    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    wait_until(fun() ->
                       3 =:= length(element(2, ra:members({RaName, Server0})))
               end),

    CurrentUId = uid_of(Config, Server2, RaName),
    ?assert(is_binary(CurrentUId)),

    %% A UID that does not match the live incarnation leaves the member alone.
    ?assertEqual({skipped, superseded},
                 force_delete_member(Config, Server0, ServerId,
                                     <<"stale-incarnation-uid">>)),
    ?assertEqual(CurrentUId, uid_of(Config, Server2, RaName)),

    %% The matching UID deletes it.
    ?assertEqual(ok, force_delete_member(Config, Server0, ServerId, CurrentUId)),
    ?assertEqual(undefined, uid_of(Config, Server2, RaName)),

    %% A member that is already gone is reported as such rather than deleted again.
    ?assertEqual({skipped, gone},
                 force_delete_member(Config, Server0, ServerId, CurrentUId)).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

is_force_delete_group(Config) ->
    true =:= rabbit_ct_helpers:get_config(Config, force_delete_group, false).

uid_of(Config, Node, RaName) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, ra_directory, uid_of,
                                 [?RA_SYSTEM, RaName]).

force_delete_member(Config, Node, ServerId, ExpectedUId) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_quorum_queue,
                                 force_delete_member, [ServerId, ExpectedUId]).

pending_lookup(Config, Node, RaName, MemberNode) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, dets, lookup,
                                 [?FORCE_DELETE_TABLE, {RaName, MemberNode}]).

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

reset_nodes([], _Leader) ->
    ok;
reset_nodes([Node| Nodes], Leader) ->
    ok = rabbit_control_helper:command(stop_app, Node),
    case rabbit_control_helper:command(forget_cluster_node, Leader, [atom_to_list(Node)]) of
        ok -> ok;
        {error, _, <<"Error:\n{:not_a_cluster_node, ~c\"The node selected is not in the cluster.\"}">>} -> ok
    end,
    ok = rabbit_control_helper:command(reset, Node),
    ok = rabbit_control_helper:command(start_app, Node),
    reset_nodes(Nodes, Leader).

add_server_to_cluster(Server, Leader) ->
    ok = rabbit_control_helper:command(stop_app, Server),
    ok = rabbit_control_helper:command(join_cluster, Server, [atom_to_list(Leader)], []),
    rabbit_control_helper:command(start_app, Server).

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

wait_until(Condition) ->
    wait_until(Condition, 60).

wait_until(Condition, 0) ->
    ?assertEqual(true, Condition());
wait_until(Condition, N) ->
    case Condition() of
        true ->
            ok;
        _ ->
            timer:sleep(500),
            wait_until(Condition, N - 1)
    end.


delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].
