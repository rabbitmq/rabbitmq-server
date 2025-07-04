%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(quorum_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbit/src/rabbit_fifo.hrl").

-import(queue_utils, [wait_for_messages_ready/3,
                      wait_for_messages_pending_ack/3,
                      wait_for_messages_total/3,
                      wait_for_messages/2,
                      dirty_query/3,
                      ra_name/1]).

-import(clustering_utils, [
                           assert_cluster_status/2,
                           assert_clustered/1
                          ]).

-compile([nowarn_export_all, export_all]).


-define(NET_TICKTIME_S, 5).
-define(DEFAULT_AWAIT, 10_000).
-define(TIMEOUT, 30_000).

suite() ->
    [{timetrap, 5 * 60_000}].

all() ->
    [
      {group, single_node},
      {group, unclustered},
      {group, clustered}
    ].

groups() ->
    [
     {single_node, [], all_tests() ++
                       memory_tests() ++
                       [node_removal_is_quorum_critical,
                        format]},
     {unclustered, [], [
                        {uncluster_size_2, [], [add_member]}
                       ]},
     {clustered, [], [
                      {cluster_size_2, [], [add_member_2,
                                            add_member_not_running,
                                            add_member_classic,
                                            add_member_wrong_type,
                                            add_member_already_a_member,
                                            add_member_not_found,
                                            delete_member_not_running,
                                            delete_member_classic,
                                            delete_member_wrong_type,
                                            delete_member_queue_not_found,
                                            delete_member,
                                            delete_member_not_a_member,
                                            delete_member_member_already_deleted,
                                            node_removal_is_quorum_critical]
                       ++ memory_tests()},
                      {cluster_size_3, [], [
                                            cleanup_data_dir,
                                            channel_handles_ra_event,
                                            declare_during_node_down,
                                            simple_confirm_availability_on_leader_change,
                                            publishing_to_unavailable_queue,
                                            confirm_availability_on_leader_change,
                                            recover_from_single_failure,
                                            recover_from_multiple_failures,
                                            leadership_takeover,
                                            delete_declare,
                                            delete_member_during_node_down,
                                            metrics_cleanup_on_leadership_takeover,
                                            metrics_cleanup_on_leader_crash,
                                            consume_in_minority,
                                            get_in_minority,
                                            reject_after_leader_transfer,
                                            shrink_all,
                                            rebalance,
                                            node_removal_is_not_quorum_critical,
                                            leader_locator_client_local,
                                            leader_locator_balanced,
                                            leader_locator_balanced_maintenance,
                                            leader_locator_balanced_random_maintenance,
                                            leader_locator_policy,
                                            status,
                                            format,
                                            add_member_2,
                                            single_active_consumer_priority_take_over,
                                            single_active_consumer_priority_take_over_return,
                                            single_active_consumer_priority_take_over_requeue,
                                            single_active_consumer_priority,
                                            force_shrink_member_to_current_member,
                                            force_all_queues_shrink_member_to_current_member,
                                            force_vhost_queues_shrink_member_to_current_member,
                                            force_checkpoint_on_queue,
                                            force_checkpoint,
                                            policy_repair,
                                            gh_12635,
                                            replica_states
                                           ]
                       ++ all_tests()},
                      {cluster_size_5, [], [start_queue,
                                            start_queue_concurrent,
                                            quorum_cluster_size_3,
                                            quorum_cluster_size_7,
                                            node_removal_is_not_quorum_critical,
                                            select_nodes_with_least_replicas,
                                            select_nodes_with_least_replicas_node_down,
                                            subscribe_from_each


                                           ]},
                      {clustered_with_partitions, [],
                       [
                        reconnect_consumer_and_publish,
                        reconnect_consumer_and_wait,
                        reconnect_consumer_and_wait_channel_down
                       ]}
                     ]}
    ].

all_tests() ->
    [
     declare_args,
     declare_invalid_properties,
     declare_server_named,
     declare_invalid_arg_1,
     declare_invalid_arg_2,
     declare_invalid_arg_3,
     relaxed_argument_equivalence_checks_on_qq_redeclare,
     consume_invalid_arg_1,
     consume_invalid_arg_2,
     start_queue,
     long_name,
     stop_queue,
     restart_queue,
     restart_all_types,
     stop_start_rabbit_app,
     publish_and_restart,
     subscribe_should_fail_when_global_qos_true,
     dead_letter_to_classic_queue,
     dead_letter_with_memory_limit,
     dead_letter_to_quorum_queue,
     dead_letter_from_classic_to_quorum_queue,
     dead_letter_policy,
     cleanup_queue_state_on_channel_after_publish,
     cleanup_queue_state_on_channel_after_subscribe,
     sync_queue,
     cancel_sync_queue,
     idempotent_recover,
     server_system_recover,
     vhost_with_quorum_queue_is_deleted,
     vhost_with_default_queue_type_declares_quorum_queue,
     node_wide_default_queue_type_declares_quorum_queue,
     delete_immediately_by_resource,
     consume_redelivery_count,
     subscribe_redelivery_count,
     message_bytes_metrics,
     queue_length_limit_drop_head,
     queue_length_limit_reject_publish,
     queue_length_limit_policy_cleared,
     subscribe_redelivery_limit,
     subscribe_redelivery_limit_disable,
     subscribe_redelivery_limit_many,
     subscribe_redelivery_policy,
     subscribe_redelivery_limit_with_dead_letter,
     purge,
     consumer_metrics,
     invalid_policy,
     pre_existing_invalid_policy,
     delete_if_empty,
     delete_if_unused,
     queue_ttl,
     peek,
     oldest_entry_timestamp,
     peek_with_wrong_queue_type,
     message_ttl,
     message_ttl_policy,
     per_message_ttl,
     per_message_ttl_mixed_expiry,
     per_message_ttl_expiration_too_high,
     consumer_priorities,
     cancel_consumer_gh_3729,
     cancel_consumer_gh_12424,
     cancel_and_consume_with_same_tag,
     validate_messages_on_queue,
     amqpl_headers,
     priority_queue_fifo,
     priority_queue_2_1_ratio,
     requeue_multiple_true,
     requeue_multiple_false,
     subscribe_from_each,
     dont_leak_file_handles,
     leader_health_check
    ].

memory_tests() ->
    [
     memory_alarm_rolls_wal,
     reclaim_memory_with_wrong_queue_type
    ].

-define(SUPNAME, ra_server_sup_sup).
%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 256}]}),
    rabbit_ct_helpers:run_setup_steps(Config1, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(unclustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]);
init_per_group(clustered_with_partitions, Config0) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "clustered_with_partitions is too unreliable in mixed mode"};
        false ->
            Config1 = rabbit_ct_helpers:run_setup_steps(
                       Config0,
                       [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]),
            Config2 = rabbit_ct_helpers:set_config(Config1,
                                                   [{net_ticktime, ?NET_TICKTIME_S}]),
            Config2
    end;
init_per_group(Group, Config) ->
    ClusterSize = case Group of
                      single_node -> 1;
                      uncluster_size_2 -> 2;
                      cluster_size_2 -> 2;
                      cluster_size_3 -> 3;
                      cluster_size_5 -> 5
                  end,
    IsMixed = rabbit_ct_helpers:is_mixed_versions(),
    case ClusterSize of
        2 when IsMixed ->
            {skip, "cluster size 2 isn't mixed versions compatible"};
        _ ->
            Config1 = rabbit_ct_helpers:set_config(Config,
                                                   [{rmq_nodes_count, ClusterSize},
                                                    {rmq_nodename_suffix, Group},
                                                    {tcp_ports_base, {skip_n_nodes, ClusterSize}},
                                                    {net_ticktime, ?NET_TICKTIME_S}
                                                   ]),
            Ret = rabbit_ct_helpers:run_steps(Config1,
                                              [fun merge_app_env/1 ] ++
                                              rabbit_ct_broker_helpers:setup_steps()),
            case Ret of
                {skip, _} ->
                    Ret;
                Config2 ->
                    Res = rabbit_ct_broker_helpers:enable_feature_flag(
                            Config2, 'rabbitmq_4.0.0'),
                    ct:pal("rabbitmq_4.0.0 enable result ~p", [Res]),
                    ok = rabbit_ct_broker_helpers:rpc(
                           Config2, 0, application, set_env,
                           [rabbit, channel_tick_interval, 100]),
                    Config2
            end
    end.

end_per_group(clustered, Config) ->
    Config;
end_per_group(unclustered, Config) ->
    Config;
end_per_group(clustered_with_partitions, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) when Testcase == reconnect_consumer_and_publish;
                                         Testcase == reconnect_consumer_and_wait;
                                         Testcase == reconnect_consumer_and_wait_channel_down ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{rmq_nodes_count, 3},
                                            {rmq_nodename_suffix, Testcase},
                                            {tcp_ports_base, {skip_n_nodes, 3}},
                                            {queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    rabbit_ct_helpers:run_steps(
      Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(T, Config)
  when T =:= leader_locator_balanced orelse
       T =:= leader_locator_policy ->
    Vsn0 = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_fifo, version, []),
    Vsn1 = rabbit_ct_broker_helpers:rpc(Config, 1, rabbit_fifo, version, []),
    case Vsn0 =:= Vsn1 of
        true ->
            Config1 = rabbit_ct_helpers:testcase_started(Config, T),
            Q = rabbit_data_coercion:to_binary(T),
            Config2 = rabbit_ct_helpers:set_config(
                        Config1, [{queue_name, Q},
                                  {alt_queue_name, <<Q/binary, "_alt">>},
                                  {alt_2_queue_name, <<Q/binary, "_alt_2">>}]),
            rabbit_ct_helpers:run_steps(Config2,
                                        rabbit_ct_client_helpers:setup_steps());
        false ->
            {skip, "machine versions must be the same for desired leader location to work"}
    end;
init_per_testcase(Testcase, Config) ->
    ClusterSize = ?config(rmq_nodes_count, Config),
    IsMixed = rabbit_ct_helpers:is_mixed_versions(),
    RabbitMQ3 = case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
                    ok -> false;
                    _ -> true
                end,
    SameKhepriMacVers = (
      rabbit_ct_broker_helpers:do_nodes_run_same_ra_machine_version(
        Config, khepri_machine)),
    case Testcase of
        node_removal_is_not_quorum_critical when IsMixed ->
            {skip, "node_removal_is_not_quorum_critical isn't mixed versions compatible"};
        simple_confirm_availability_on_leader_change when IsMixed ->
            {skip, "simple_confirm_availability_on_leader_change isn't mixed versions compatible"};
        confirm_availability_on_leader_change when IsMixed ->
            {skip, "confirm_availability_on_leader_change isn't mixed versions compatible"};
        recover_from_single_failure when IsMixed ->
            %% In a 3.8/3.9 cluster this will pass only if the failure occurs on the 3.8 node
            {skip, "recover_from_single_failure isn't mixed versions compatible"};
        shrink_all when IsMixed ->
            %% In a 3.8/3.9 cluster only the first shrink will work as expected
            {skip, "skrink_all isn't mixed versions compatible"};
        delete_immediately_by_resource when IsMixed andalso ClusterSize == 3 ->
            {skip, "delete_immediately_by_resource isn't mixed versions compatible"};
        queue_ttl when IsMixed andalso ClusterSize == 3 ->
            {skip, "queue_ttl isn't mixed versions compatible"};
        start_queue when IsMixed andalso ClusterSize == 5 ->
            {skip, "start_queue isn't mixed versions compatible"};
        start_queue_concurrent when IsMixed andalso ClusterSize == 5 ->
            {skip, "start_queue_concurrent isn't mixed versions compatible"};
        leader_locator_client_local when IsMixed ->
            {skip, "leader_locator_client_local isn't mixed versions compatible because "
             "delete_declare isn't mixed versions reliable"};
        leader_locator_balanced_random_maintenance when IsMixed ->
            {skip, "leader_locator_balanced_random_maintenance isn't mixed versions compatible because "
             "delete_declare isn't mixed versions reliable"};
        leadership_takeover when not SameKhepriMacVers ->
            {skip, "leadership_takeover will fail with a mix of Khepri state "
             "machine versions"};
        reclaim_memory_with_wrong_queue_type when IsMixed ->
            {skip, "reclaim_memory_with_wrong_queue_type isn't mixed versions compatible"};
        peek_with_wrong_queue_type when IsMixed ->
            {skip, "peek_with_wrong_queue_type isn't mixed versions compatible"};
        cancel_consumer_gh_3729 when IsMixed andalso RabbitMQ3 ->
            {skip, "this test is not compatible with RabbitMQ 3.13.x"};
        _ ->
            Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
            rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
            Q = rabbit_data_coercion:to_binary(Testcase),
            Config2 = rabbit_ct_helpers:set_config(Config1,
                                                   [{queue_name, Q},
                                                    {alt_queue_name, <<Q/binary, "_alt">>},
                                                    {alt_2_queue_name, <<Q/binary, "_alt_2">>}
                                                   ]),
            rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps())
    end.

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

end_per_testcase(Testcase, Config) when Testcase == reconnect_consumer_and_publish;
                                        Testcase == reconnect_consumer_and_wait;
                                        Testcase == reconnect_consumer_and_wait_channel_down ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase);
end_per_testcase(Testcase, Config) ->
    % catch delete_queues(),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

declare_args(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                     {<<"x-max-length">>, long, 2000},
                     {<<"x-max-length-bytes">>, long, 2000}]),
    assert_queue_type(Server, LQ, rabbit_quorum_queue),

    DQ = <<"classic-declare-args-q">>,
    declare(Ch, DQ, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    assert_queue_type(Server, DQ, rabbit_classic_queue),

    DQ2 = <<"classic-q2">>,
    declare(Ch, DQ2),
    assert_queue_type(Server, DQ2, rabbit_classic_queue).

declare_invalid_properties(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    LQ = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = LQ,
                          auto_delete = true,
                          durable   = true,
                          arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = LQ,
                          exclusive = true,
                          durable   = true,
                          arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]})),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       amqp_channel:call(
         rabbit_ct_client_helpers:open_channel(Config, Server),
         #'queue.declare'{queue     = LQ,
                          durable   = false,
                          arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]})).

declare_server_named(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               <<"">>, [{<<"x-queue-type">>, longstr, <<"quorum">>}])).

declare_invalid_arg_1(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ExpectedError = <<"PRECONDITION_FAILED - invalid arg 'x-queue-mode' for queue "
                      "'declare_invalid_arg_1' in vhost '/' of queue type rabbit_quorum_queue">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                       %% only available in classic queues
                       {<<"x-queue-mode">>, longstr, <<"default">>}])).

declare_invalid_arg_2(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ExpectedError = <<"PRECONDITION_FAILED - invalid arg 'x-queue-type' for queue 'declare_invalid_arg_2'"
                      " in vhost '/': \"unsupported queue type 'fake-queue-type'\"">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"fake-queue-type">>}])).

declare_invalid_arg_3(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ExpectedError = <<"PRECONDITION_FAILED - invalid arg 'x-max-length' for queue 'declare_invalid_arg_3'"
                      " in vhost '/': {value_negative,-5}">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                       {<<"x-max-length">>, long, -5}])).


relaxed_argument_equivalence_checks_on_qq_redeclare(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, quorum_relaxed_checks_on_redeclaration, true]),
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    VHost = <<"redeclarevhost">>,
    User = ?config(rmq_username, Config),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Node, VHost, User),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VHost),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    Q = atom_to_binary(?FUNCTION_NAME, utf8),

    %% Declare a quorum queue
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                 {<<"x-expires">>, long, 1000}])),

    %% re-declare it as a classic queue, which is OK on this node because we've opted in
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>},
                                 {<<"x-expires">>, long, 1000}])),
    %% re-declare it as classic queue with classic only arguments ignored: OK
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>},
                                 {<<"x-max-priority">>, byte, 5},
                                 {<<"x-expires">>, long, 1000}])),
    %% re-declare it as a classic queue, with shared arguments not part of original queue: this should fail
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>},
                       {<<"x-message-ttl">>, long, 5},
                       {<<"x-expires">>, long, 1000}])),

    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, quorum_relaxed_checks_on_redeclaration, false]),
    ok.

consume_invalid_arg_1(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ExpectedError = <<"PRECONDITION_FAILED - invalid arg 'x-stream-offset' for queue "
                      "'consume_invalid_arg_1' in vhost '/' of queue type rabbit_quorum_queue">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       amqp_channel:subscribe(Ch, #'basic.consume'{
                                     queue = Q,
                                     arguments = [{<<"x-stream-offset">>, longstr, <<"last">>}],
                                     no_ack = false,
                                     consumer_tag = <<"ctag">>},
                              self())).

consume_invalid_arg_2(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ExpectedError = <<"PRECONDITION_FAILED - invalid arg 'x-priority' for queue 'consume_invalid_arg_2'"
                      " in vhost '/': \"expected integer, got longstr\"">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, ExpectedError}}, _},
       amqp_channel:subscribe(Ch, #'basic.consume'{
                                     queue = Q,
                                     arguments = [{<<"x-priority">>, longstr, <<"important">>}],
                                     no_ack = false,
                                     consumer_tag = <<"ctag">>},
                              self())).

start_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    Children = length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),

    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Check that the application and one ra node are up
    ?awaitMatch({ra, _, _},
                lists:keyfind(ra, 1,
                              rpc:call(Server, application, which_applications, [])),
                ?DEFAULT_AWAIT),
    Expected = Children + 1,
    ?awaitMatch(Expected,
                length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),
                ?DEFAULT_AWAIT),

    %% Test declare an existing queue
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Test declare with same arguments
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Test declare an existing queue with different arguments
    ?assertExit(_, declare(Ch, LQ, [])),

    %% Check that the application and process are still up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))),
    ?assertMatch(Expected,
                 length(rpc:call(Server, supervisor, which_children, [?SUPNAME]))),

    ok.


long_name(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    %% 64 + chars
    VHost = <<"long_name_vhost____________________________________">>,
    QName = atom_to_binary(?FUNCTION_NAME, utf8),
    User = ?config(rmq_username, Config),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Node, VHost, User),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VHost),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node,
                                                              VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    %% long name
    LongName = binary:copy(QName, 240 div byte_size(QName)),
    ?assertEqual({'queue.declare_ok', LongName, 0, 0},
                 declare(Ch, LongName,
                         [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ok.

start_queue_concurrent(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    LQ = ?config(queue_name, Config),
    Self = self(),
    [begin
         _ = spawn_link(fun () ->
                                {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Server),
                                %% Test declare an existing queue
                                ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                                             declare(Ch, LQ,
                                                     [{<<"x-queue-type">>,
                                                       longstr,
                                                       <<"quorum">>}])),
                                timer:sleep(100),
                                rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
                                Self ! {done, Server}
                        end)
     end || Server <- Servers],

    [begin
         receive {done, Server} -> ok
         after ?TIMEOUT -> exit({await_done_timeout, Server})
         end
     end || Server <- Servers],


    ok.

quorum_cluster_size_3(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "quorum_cluster_size_3 test isn't mixed version reliable"};
        false ->
            quorum_cluster_size_x(Config, 3, 3)
    end.

quorum_cluster_size_7(Config) ->
    quorum_cluster_size_x(Config, 7, 5).

quorum_cluster_size_x(Config, Max, Expected) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    RaName = ra_name(QQ),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-quorum-initial-group-size">>, long, Max}])),
    ?awaitMatch({ok, Members, _} when length(Members) == Expected,
                ra:members({RaName, Server}),
                ?DEFAULT_AWAIT),
    ?awaitMatch(MembersQ when length(MembersQ) == Expected,
                begin
                    Info = rpc:call(Server, rabbit_quorum_queue, infos,
                                    [rabbit_misc:r(<<"/">>, queue, QQ)]),
                    proplists:get_value(members, Info)
                end, ?DEFAULT_AWAIT).

stop_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    %% The stream coordinator is also a ra process, we need to ensure the quorum tests
    %% are not affected by any other ra cluster that could be added in the future
    Children = length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Check that the application and one ra node are up
    ?awaitMatch({ra, _, _},
                lists:keyfind(ra, 1,
                              rpc:call(Server, application, which_applications, [])),
                ?DEFAULT_AWAIT),
    Expected = Children + 1,
    ?awaitMatch(Expected,
                length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),
                ?DEFAULT_AWAIT),

    %% Delete the quorum queue
    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch, #'queue.delete'{queue = LQ})),
    %% Check that the application and process are down
    ?awaitMatch(Children,
                length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),
                30000),
    ?awaitMatch({ra, _, _},
                lists:keyfind(ra, 1,
                              rpc:call(Server, application, which_applications, [])),
                ?DEFAULT_AWAIT).

restart_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    %% The stream coordinator is also a ra process, we need to ensure the quorum tests
    %% are not affected by any other ra cluster that could be added in the future
    Children = length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server),

    %% Check that the application and one ra node are up
    %% The node has just been restarted, let's give it a bit of time to be ready if needed
    ?awaitMatch({ra, _, _},
                lists:keyfind(ra, 1,
                              rpc:call(Server, application, which_applications, [])),
                ?DEFAULT_AWAIT),
    Expected = Children + 1,
    ?assertMatch(Expected,
                 length(rpc:call(Server, supervisor, which_children, [?SUPNAME]))),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    delete_queues(Ch2, [LQ]).

idempotent_recover(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% kill default vhost to trigger recovery
    [{_, SupWrapperPid, _, _} | _] = rpc:call(Server, supervisor,
                                              which_children,
                                              [rabbit_vhost_sup_sup]),
    [{_, Pid, _, _} | _] = rpc:call(Server, supervisor,
                                    which_children,
                                    [SupWrapperPid]),
    %% kill the vhost process to trigger recover
    rpc:call(Server, erlang, exit, [Pid, kill]),


    %% validate quorum queue is still functional
    ?awaitMatch({ok, _, _},
                begin
                    RaName = ra_name(LQ),
                    ra:members({RaName, Server})
                end, ?DEFAULT_AWAIT),
    %% validate vhosts are running - or rather validate that at least one
    %% vhost per cluster is running
    ?awaitMatch(true,
                begin
                    Is = rpc:call(Server, rabbit_vhost,info_all, []),
                    lists:all(fun (I) ->
                                      #{cluster_state := ServerStatuses} = maps:from_list(I),
                                      maps:get(Server, maps:from_list(ServerStatuses)) =:= running
                              end, Is)
                end, ?DEFAULT_AWAIT),
    ok.

server_system_recover(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(LQ),
    _ = ra:members({RaName, Server}),
    EtsPid = ct_rpc:call(Server, erlang, whereis, [ra_log_ets]),
    ?assert(is_pid(EtsPid)),

    true = ct_rpc:call(Server, erlang, exit, [EtsPid, kill]),

    %% validate quorum queue is still functional
    ?awaitMatch({ok, _, _},
                begin
                    %% there is a small chance that a quorum queue process will crash
                    %% due to missing ETS table, in this case we need to keep
                    %% retrying awaiting the restart
                    catch ra:members({RaName, Server})
                end, ?DEFAULT_AWAIT),
    ok.

vhost_with_quorum_queue_is_deleted(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    VHost = <<"vhost2">>,
    QName = atom_to_binary(?FUNCTION_NAME, utf8),
    RaName = binary_to_atom(<<VHost/binary, "_", QName/binary>>, utf8),
    User = ?config(rmq_username, Config),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Node, VHost, User),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VHost),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node,
                                                              VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    UId = rpc:call(Node, ra_directory, where_is, [quorum_queues, RaName]),
    ?assert(UId =/= undefined),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
    %% validate quorum queues got deleted
    undefined = rpc:call(Node, ra_directory, where_is, [quorum_queues, RaName]),
    ok.

vhost_with_default_queue_type_declares_quorum_queue(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    VHost = atom_to_binary(?FUNCTION_NAME, utf8),
    QName = atom_to_binary(?FUNCTION_NAME, utf8),
    User = ?config(rmq_username, Config),

    AddVhostArgs = [VHost, #{default_queue_type => <<"quorum">>}, User],
    ok = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_vhost, add,
                                      AddVhostArgs),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VHost),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual({'queue.declare_ok', QName, 0, 0}, declare(Ch, QName, [])),
    assert_queue_type(Node, VHost, QName, rabbit_quorum_queue),
    %% declaring again without a queue arg is ok
    ?assertEqual({'queue.declare_ok', QName, 0, 0}, declare(Ch, QName, [])),
    %% also using an explicit queue type should be ok
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    %% passive should work without x-queue-type
    ?assertEqual({'queue.declare_ok', QName, 0, 0}, declare_passive(Ch, QName, [])),
    %% passive with x-queue-type also should work
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare_passive(Ch, QName, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% declaring an exclusive queue should declare a classic queue
    QNameEx = iolist_to_binary([QName, <<"_exclusive">>]),
    ?assertEqual({'queue.declare_ok', QNameEx, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue = QNameEx,
                                                        exclusive = true,
                                                        durable = true,
                                                        arguments = []})),
    assert_queue_type(Node, VHost, QNameEx, rabbit_classic_queue),

    %% transient declares should also fall back to classic queues
    QNameTr = iolist_to_binary([QName, <<"_transient">>]),
    ?assertEqual({'queue.declare_ok', QNameTr, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue = QNameTr,
                                                        exclusive = false,
                                                        durable = false,
                                                        arguments = []})),
    assert_queue_type(Node, VHost, QNameTr, rabbit_classic_queue),

    %% auto-delete declares should also fall back to classic queues
    QNameAd = iolist_to_binary([QName, <<"_delete">>]),
    ?assertEqual({'queue.declare_ok', QNameAd, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue = QNameAd,
                                                        exclusive = false,
                                                        auto_delete = true,
                                                        durable = true,
                                                        arguments = []})),
    assert_queue_type(Node, VHost, QNameAd, rabbit_classic_queue),
    amqp_connection:close(Conn),
    ok.

node_wide_default_queue_type_declares_quorum_queue(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
    true ->
        {skip, "node_wide_default_queue_type_declares_quorum_queue test isn't mixed version compatible"};
    false ->
        node_wide_default_queue_type_declares_quorum_queue0(Config)
    end.

node_wide_default_queue_type_declares_quorum_queue0(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rpc:call(Node, application, set_env, [rabbit, default_queue_type, rabbit_quorum_queue]),
    VHost = atom_to_binary(?FUNCTION_NAME, utf8),
    QName = atom_to_binary(?FUNCTION_NAME, utf8),
    User = ?config(rmq_username, Config),

    AddVhostArgs = [VHost, #{}, User],
    ok = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_vhost, add,
        AddVhostArgs),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VHost),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node, VHost),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ?assertEqual({'queue.declare_ok', QName, 0, 0}, declare(Ch, QName, [])),
    assert_queue_type(Node, VHost, QName, rabbit_quorum_queue),
    ?assertEqual({'queue.declare_ok', QName, 0, 0}, declare(Ch, QName, [])),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
        declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({'queue.declare_ok', QName, 0, 0}, declare_passive(Ch, QName, [])),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
        declare_passive(Ch, QName, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    amqp_connection:close(Conn),

    rpc:call(Node, application, set_env, [rabbit, default_queue_type, rabbit_classic_queue]),
    ok.

restart_all_types(Config) ->
    %% Test the node restart with both types of queues (quorum and classic) to
    %% ensure there are no regressions
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    %% The stream coordinator is also a ra process, we need to ensure the quorum tests
    %% are not affected by any other ra cluster that could be added in the future
    Children = rpc:call(Server, supervisor, which_children, [?SUPNAME]),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ1 = <<"restart_all_types-qq1">>,
    ?assertEqual({'queue.declare_ok', QQ1, 0, 0},
                 declare(Ch, QQ1, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    QQ2 = <<"restart_all_types-qq2">>,
    ?assertEqual({'queue.declare_ok', QQ2, 0, 0},
                 declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    CQ1 = <<"restart_all_types-classic1">>,
    ?assertEqual({'queue.declare_ok', CQ1, 0, 0}, declare(Ch, CQ1, [])),
    rabbit_ct_client_helpers:publish(Ch, CQ1, 1),
    CQ2 = <<"restart_all_types-classic2">>,
    ?assertEqual({'queue.declare_ok', CQ2, 0, 0}, declare(Ch, CQ2, [])),
    rabbit_ct_client_helpers:publish(Ch, CQ2, 1),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server),

    %% Check that the application and two ra nodes are up. Queues are restored
    %% after the broker is marked as "ready", that's why we need to wait for
    %% the condition.
    ?awaitMatch({ra, _, _},
                lists:keyfind(ra, 1,
                              rpc:call(Server, application, which_applications, [])),
                ?DEFAULT_AWAIT),
    Expected = length(Children) + 2,
    ?awaitMatch(Expected,
                length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),
                60000),
    %% Check the classic queues restarted correctly
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ1, no_ack = false}),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ2, no_ack = false}),
    delete_queues(Ch2, [QQ1, QQ2, CQ1, CQ2]).

delete_queues(Ch, Queues) ->
    [amqp_channel:call(Ch, #'queue.delete'{queue = Q}) ||  Q <- Queues],
    ok.

stop_start_rabbit_app(Config) ->
    %% Test start/stop of rabbit app with both types of queues (quorum and
    %%  classic) to ensure there are no regressions
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    %% The stream coordinator is also a ra process, we need to ensure the quorum tests
    %% are not affected by any other ra cluster that could be added in the future
    Children = length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ1 = <<"stop_start_rabbit_app-qq">>,
    ?assertEqual({'queue.declare_ok', QQ1, 0, 0},
                 declare(Ch, QQ1, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    QQ2 = <<"quorum-q2">>,
    ?assertEqual({'queue.declare_ok', QQ2, 0, 0},
                 declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    CQ1 = <<"stop_start_rabbit_app-classic">>,
    ?assertEqual({'queue.declare_ok', CQ1, 0, 0}, declare(Ch, CQ1, [])),
    rabbit_ct_client_helpers:publish(Ch, CQ1, 1),
    CQ2 = <<"stop_start_rabbit_app-classic2">>,
    ?assertEqual({'queue.declare_ok', CQ2, 0, 0}, declare(Ch, CQ2, [])),
    rabbit_ct_client_helpers:publish(Ch, CQ2, 1),

    ?assertEqual(ok, rabbit_control_helper:command(stop_app, Server)),
    %% Check the ra application has stopped (thus its supervisor and queues)
    rabbit_ct_helpers:await_condition(
        fun() ->
            Apps = rpc:call(Server, application, which_applications, []),
            %% we expect the app to NOT be running
            case lists:keyfind(ra, 1, Apps) of
                false      -> true;
                {ra, _, _} -> false
            end
        end, 30000),

    ?assertEqual(ok, rabbit_control_helper:command(start_app, Server)),

    %% Check that the application and two ra nodes are up
    rabbit_ct_helpers:await_condition(
        fun() ->
            Apps = rpc:call(Server, application, which_applications, []),
            case lists:keyfind(ra, 1, Apps) of
                false      -> false;
                {ra, _, _} -> true
            end
        end, 30000),
    Expected = Children + 2,
    ?assertMatch(Expected,
                 length(rpc:call(Server, supervisor, which_children, [?SUPNAME]))),
    %% Check the classic queues restarted correctly
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ1, no_ack = false}),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ2, no_ack = false}),
    delete_queues(Ch2, [QQ1, QQ2, CQ1, CQ2]).

publish_confirm(Ch, QName) ->
    publish_confirm(Ch, QName, 2500).

publish_confirm(Ch, QName, Timeout) ->
    publish(Ch, QName),
    amqp_channel:register_confirm_handler(Ch, self()),
    ct:pal("waiting for confirms from ~ts", [QName]),
    receive
        #'basic.ack'{} ->
            ct:pal("CONFIRMED! ~ts", [QName]),
            ok;
        #'basic.nack'{} ->
            ct:pal("NOT CONFIRMED! ~ts", [QName]),
            fail
    after Timeout ->
              flush(1),
              exit(confirm_timeout)
    end.

publish_and_restart(Config) ->
    %% Test the node restart with both types of queues (quorum and classic) to
    %% ensure there are no regressions
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    RaName = ra_name(QQ),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server),

    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    publish(rabbit_ct_client_helpers:open_channel(Config, Server), QQ),
    wait_for_messages_ready(Servers, RaName, 2),
    wait_for_messages_pending_ack(Servers, RaName, 0).

consume_in_minority(Config) ->
    [Server0, Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(
                                    Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    RaName = binary_to_atom(<<"%2F_", QQ/binary>>),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_quorum_queue:stop_server({RaName, Server1}),
    ok = rabbit_quorum_queue:stop_server({RaName, Server2}),

    ?assertExit(
       {{shutdown,
         {connection_closing,
          {server_initiated_close, 541,
           <<"INTERNAL_ERROR - failed consuming from quorum queue "
             "'consume_in_minority' in vhost '/'", _Reason/binary>>}}}, _},
       amqp_channel:subscribe(Ch, #'basic.consume'{queue = QQ}, self())),

    ok = rabbit_quorum_queue:restart_server({RaName, Server1}),
    ok = rabbit_quorum_queue:restart_server({RaName, Server2}).

get_in_minority(Config) ->
    [Server0, Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(
                                    Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    RaName = binary_to_atom(<<"%2F_", QQ/binary>>),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_quorum_queue:stop_server({RaName, Server1}),
    ok = rabbit_quorum_queue:stop_server({RaName, Server2}),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                   no_ack = false})),

    ok = rabbit_quorum_queue:restart_server({RaName, Server1}),
    ok = rabbit_quorum_queue:restart_server({RaName, Server2}).

single_active_consumer_priority_take_over(Config) ->
    check_quorum_queues_v4_compat(Config),

    [Server0, Server1, _Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QName = ?config(queue_name, Config),
    Q1 = <<QName/binary, "_1">>,
    RaNameQ1 = binary_to_atom(<<"%2F", "_", Q1/binary>>, utf8),
    QueryFun = fun rabbit_fifo:query_single_active_consumer/1,
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>},
            {<<"x-single-active-consumer">>, bool, true}],
    ?assertEqual({'queue.declare_ok', Q1, 0, 0}, declare(Ch1, Q1, Args)),
    ok = subscribe(Ch1, Q1, false, <<"ch1-ctag1">>, [{"x-priority", byte, 1}]),
    ?assertMatch({ok, {_, {value, {<<"ch1-ctag1">>, _}}}, _},
                 rpc:call(Server0, ra, local_query, [RaNameQ1, QueryFun])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch2, #'confirm.select'{}),
    publish_confirm(Ch2, Q1),
    %% higher priority consumer attaches
    ok = subscribe(Ch2, Q1, false, <<"ch2-ctag1">>, [{"x-priority", byte, 3}]),

    %% Q1 should still have Ch1 as consumer as it has pending messages
    ?assertMatch({ok, {_, {value, {<<"ch1-ctag1">>, _}}}, _},
                 rpc:call(Server0, ra, local_query,
                          [RaNameQ1, QueryFun])),

    %% ack the message
    receive
        {#'basic.deliver'{consumer_tag = <<"ch1-ctag1">>,
                          delivery_tag = DeliveryTag}, _} ->
            amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = DeliveryTag,
                                                multiple     = false})
    after ?TIMEOUT ->
              flush(1),
              exit(basic_deliver_timeout)
    end,

    ?awaitMatch({ok, {_, {value, {<<"ch2-ctag1">>, _}}}, _},
                rpc:call(Server0, ra, local_query, [RaNameQ1, QueryFun]),
               ?DEFAULT_AWAIT),
    ok.

single_active_consumer_priority_take_over_return(Config) ->
    single_active_consumer_priority_take_over_base(20, Config).

single_active_consumer_priority_take_over_requeue(Config) ->
    single_active_consumer_priority_take_over_base(-1, Config).

single_active_consumer_priority_take_over_base(DelLimit, Config) ->
    check_quorum_queues_v4_compat(Config),

    [Server0, Server1, _Server2] = Nodes =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    MinMacVers = lists:min([V || {ok, V} <-
                                 erpc:multicall(Nodes, rabbit_fifo, version, [])]),
    if MinMacVers < 7 ->
           throw({skip, "single_active_consumer_priority_take_over_base needs a higher machine verison"});
       true ->
           ok
    end,

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QName = ?config(queue_name, Config),
    Q1 = <<QName/binary, "_1">>,
    RaNameQ1 = binary_to_atom(<<"%2F", "_", Q1/binary>>, utf8),
    QueryFun = fun rabbit_fifo:query_single_active_consumer/1,
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>},
            {<<"x-delivery-limit">>, long, DelLimit},
            {<<"x-single-active-consumer">>, bool, true}],
    ?assertEqual({'queue.declare_ok', Q1, 0, 0}, declare(Ch1, Q1, Args)),
    ok = subscribe(Ch1, Q1, false, <<"ch1-ctag1">>, [{"x-priority", byte, 1}]),
    ?assertMatch({ok, {_, {value, {<<"ch1-ctag1">>, _}}}, _},
                 rpc:call(Server0, ra, local_query, [RaNameQ1, QueryFun])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch2, #'confirm.select'{}),
    publish_confirm(Ch2, Q1),
    %% higher priority consumer attaches
    ok = subscribe(Ch2, Q1, false, <<"ch2-ctag1">>, [{"x-priority", byte, 3}]),

    %% Q1 should still have Ch1 as consumer as it has pending messages
    ?assertMatch({ok, {_, {value, {<<"ch1-ctag1">>, _}}}, _},
                 rpc:call(Server0, ra, local_query,
                          [RaNameQ1, QueryFun])),

    %% ack the message
    receive
        {#'basic.deliver'{consumer_tag = <<"ch1-ctag1">>,
                          delivery_tag = DeliveryTag}, _} ->
            amqp_channel:cast(Ch1, #'basic.nack'{delivery_tag = DeliveryTag})
    after ?TIMEOUT ->
              flush(1),
              exit(basic_deliver_timeout)
    end,

    ?awaitMatch({ok, {_, {value, {<<"ch2-ctag1">>, _}}}, _},
                rpc:call(Server0, ra, local_query, [RaNameQ1, QueryFun]),
               ?DEFAULT_AWAIT),
    receive
        {#'basic.deliver'{consumer_tag = <<"ch2-ctag1">>,
                          delivery_tag = DeliveryTag2}, _} ->
            amqp_channel:cast(Ch1, #'basic.ack'{delivery_tag = DeliveryTag2})
    after ?TIMEOUT ->
              flush(1),
              exit(basic_deliver_timeout_2)
    end,
    ok.

single_active_consumer_priority(Config) ->
    check_quorum_queues_v4_compat(Config),
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Ch3 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    QName = ?config(queue_name, Config),
    Q1 = <<QName/binary, "_1">>,
    Q2 = <<QName/binary, "_2">>,
    Q3 = <<QName/binary, "_3">>,
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>},
            {<<"x-single-active-consumer">>, bool, true}],
    ?assertEqual({'queue.declare_ok', Q1, 0, 0}, declare(Ch1, Q1, Args)),
    ?assertEqual({'queue.declare_ok', Q2, 0, 0}, declare(Ch2, Q2, Args)),
    ?assertEqual({'queue.declare_ok', Q3, 0, 0}, declare(Ch3, Q3, Args)),

    ok = subscribe(Ch1, Q1, false, <<"ch1-ctag1">>, [{"x-priority", byte, 3}]),
    ok = subscribe(Ch1, Q2, false, <<"ch1-ctag2">>, [{"x-priority", byte, 2}]),
    ok = subscribe(Ch1, Q3, false, <<"ch1-ctag3">>, [{"x-priority", byte, 1}]),


    ok = subscribe(Ch2, Q1, false, <<"ch2-ctag1">>, [{"x-priority", byte, 1}]),
    ok = subscribe(Ch2, Q2, false, <<"ch2-ctag2">>, [{"x-priority", byte, 3}]),
    ok = subscribe(Ch2, Q3, false, <<"ch2-ctag3">>, [{"x-priority", byte, 2}]),

    ok = subscribe(Ch3, Q1, false, <<"ch3-ctag1">>, [{"x-priority", byte, 2}]),
    ok = subscribe(Ch3, Q2, false, <<"ch3-ctag2">>, [{"x-priority", byte, 1}]),
    ok = subscribe(Ch3, Q3, false, <<"ch3-ctag3">>, [{"x-priority", byte, 3}]),


    RaNameQ1 = binary_to_atom(<<"%2F", "_", Q1/binary>>, utf8),
    RaNameQ2 = binary_to_atom(<<"%2F", "_", Q2/binary>>, utf8),
    RaNameQ3 = binary_to_atom(<<"%2F", "_", Q3/binary>>, utf8),
    %% assert each queue has a different consumer
    QueryFun = fun rabbit_fifo:query_single_active_consumer/1,

    %% Q1 should have the consumer on Ch1
    ?assertMatch({ok, {_, {value, {<<"ch1-ctag1">>, _}}}, _},
                rpc:call(Server0, ra, local_query, [RaNameQ1, QueryFun])),

    %% Q2 Ch2
    ?assertMatch({ok, {_, {value, {<<"ch2-ctag2">>, _}}}, _},
                rpc:call(Server1, ra, local_query, [RaNameQ2, QueryFun])),

    %% Q3 Ch3
    ?assertMatch({ok, {_, {value, {<<"ch3-ctag3">>, _}}}, _},
                rpc:call(Server2, ra, local_query, [RaNameQ3, QueryFun])),

    %% close Ch3
    _ = rabbit_ct_client_helpers:close_channel(Ch3),
    flush(100),

    %% assert Q3 has Ch2 (priority 2) as consumer
    ?assertMatch({ok, {_, {value, {<<"ch2-ctag3">>, _}}}, _},
                rpc:call(Server2, ra, local_query, [RaNameQ3, QueryFun])),

    %% close Ch2
    _ = rabbit_ct_client_helpers:close_channel(Ch2),
    flush(100),

    %% assert all queues as has Ch1 as consumer
    ?assertMatch({ok, {_, {value, {<<"ch1-ctag1">>, _}}}, _},
                rpc:call(Server0, ra, local_query, [RaNameQ1, QueryFun])),
    ?assertMatch({ok, {_, {value, {<<"ch1-ctag2">>, _}}}, _},
                rpc:call(Server0, ra, local_query, [RaNameQ2, QueryFun])),
    ?assertMatch({ok, {_, {value, {<<"ch1-ctag3">>, _}}}, _},
                rpc:call(Server0, ra, local_query, [RaNameQ3, QueryFun])),
    ok.

force_shrink_member_to_current_member(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "Should not run in mixed version environments"};
        _ ->
            [Server0, Server1, Server2] =
            rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

            Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
            QQ = ?config(queue_name, Config),
            ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                         declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

            RaName = ra_name(QQ),
            rabbit_ct_client_helpers:publish(Ch, QQ, 3),
            wait_for_messages_ready([Server0], RaName, 3),

            {ok, Q0} = rpc:call(Server0, rabbit_amqqueue, lookup, [QQ, <<"/">>]),
            #{nodes := Nodes0} = amqqueue:get_type_state(Q0),
            ?assertEqual(3, length(Nodes0)),

            rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                         force_shrink_member_to_current_member, [<<"/">>, QQ]),

            wait_for_messages_ready([Server0], RaName, 3),

            {ok, Q1} = rpc:call(Server0, rabbit_amqqueue, lookup, [QQ, <<"/">>]),
            #{nodes := Nodes1} = amqqueue:get_type_state(Q1),
            ?assertEqual(1, length(Nodes1)),

            %% grow queues back to all nodes
            [rpc:call(Server0, rabbit_quorum_queue, grow, [S, <<"/">>, <<".*">>, all]) || S <- [Server1, Server2]],

            wait_for_messages_ready([Server0], RaName, 3),
            {ok, Q2} = rpc:call(Server0, rabbit_amqqueue, lookup, [QQ, <<"/">>]),
            #{nodes := Nodes2} = amqqueue:get_type_state(Q2),
            ?assertEqual(3, length(Nodes2))
    end.

force_all_queues_shrink_member_to_current_member(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "Should not run in mixed version environments"};
        _ ->
            [Server0, Server1, Server2] =
            rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

            Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
            QQ = ?config(queue_name, Config),
            AQ = ?config(alt_queue_name, Config),
            ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                         declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
            ?assertEqual({'queue.declare_ok', AQ, 0, 0},
                         declare(Ch, AQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

            QQs = [QQ, AQ],

            [begin
                 RaName = ra_name(Q),
                 rabbit_ct_client_helpers:publish(Ch, Q, 3),
                 wait_for_messages_ready([Server0], RaName, 3),
                 {ok, Q0} = rpc:call(Server0, rabbit_amqqueue, lookup, [Q, <<"/">>]),
                 #{nodes := Nodes0} = amqqueue:get_type_state(Q0),
                 ?assertEqual(3, length(Nodes0))
             end || Q <- QQs],

            rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                         force_all_queues_shrink_member_to_current_member, []),

            [begin
                 RaName = ra_name(Q),
                 wait_for_messages_ready([Server0], RaName, 3),
                 {ok, Q0} = rpc:call(Server0, rabbit_amqqueue, lookup, [Q, <<"/">>]),
                 #{nodes := Nodes0} = amqqueue:get_type_state(Q0),
                 ?assertEqual(1, length(Nodes0))
             end || Q <- QQs],

            %% grow queues back to all nodes
            [rpc:call(Server0, rabbit_quorum_queue, grow, [S, <<"/">>, <<".*">>, all]) || S <- [Server1, Server2]],

            [begin
                 RaName = ra_name(Q),
                 wait_for_messages_ready([Server0], RaName, 3),
                 {ok, Q0} = rpc:call(Server0, rabbit_amqqueue, lookup, [Q, <<"/">>]),
                 #{nodes := Nodes0} = amqqueue:get_type_state(Q0),
                 ?assertEqual(3, length(Nodes0))
             end || Q <- QQs]
    end.

force_vhost_queues_shrink_member_to_current_member(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "Should not run in mixed version environments"};
        _ ->
            [Server0, Server1, Server2] =
                rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

            Ch0 = rabbit_ct_client_helpers:open_channel(Config, Server0),
            QQ = ?config(queue_name, Config),
            AQ = ?config(alt_queue_name, Config),
            ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                         declare(Ch0, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
            ?assertEqual({'queue.declare_ok', AQ, 0, 0},
                         declare(Ch0, AQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

            QQs = [QQ, AQ],

            VHost1 = <<"/">>,
            VHost2 = <<"another-vhost">>,
            VHosts = [VHost1, VHost2],

            User = ?config(rmq_username, Config),
            ok = rabbit_ct_broker_helpers:add_vhost(Config, Server0, VHost2, User),
            ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VHost2),
            Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, Server0, VHost2),
            {ok, Ch1} = amqp_connection:open_channel(Conn1),
                ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                         declare(Ch1, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
            ?assertEqual({'queue.declare_ok', AQ, 0, 0},
                         declare(Ch1, AQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

            [rabbit_ct_client_helpers:publish(Ch, Q, 3) || Q <- QQs, Ch <- [Ch0, Ch1]],

            [begin
                QQRes = rabbit_misc:r(VHost, queue, Q),
                {ok, RaName} = rpc:call(Server0, rabbit_queue_type_util, qname_to_internal_name, [QQRes]),
                wait_for_messages_ready([Server0], RaName, 3),
                {ok, Q0} = rpc:call(Server0, rabbit_amqqueue, lookup, [Q, VHost]),
                #{nodes := Nodes0} = amqqueue:get_type_state(Q0),
                ?assertEqual(3, length(Nodes0))
            end || Q <- QQs, VHost <- VHosts],

            rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                force_vhost_queues_shrink_member_to_current_member, [VHost2]),

            [begin
                QQRes = rabbit_misc:r(VHost, queue, Q),
                {ok, RaName} = rpc:call(Server0, rabbit_queue_type_util, qname_to_internal_name, [QQRes]),
                wait_for_messages_ready([Server0], RaName, 3),
                {ok, Q0} = rpc:call(Server0, rabbit_amqqueue, lookup, [Q, VHost]),
                #{nodes := Nodes0} = amqqueue:get_type_state(Q0),
                case VHost of
                    VHost1 -> ?assertEqual(3, length(Nodes0));
                    VHost2 -> ?assertEqual(1, length(Nodes0))
                end
            end || Q <- QQs, VHost <- VHosts],

            %% grow queues back to all nodes in VHost2 only
            [rpc:call(Server0, rabbit_quorum_queue, grow, [S, VHost2, <<".*">>, all]) || S <- [Server1, Server2]],

            [begin
                QQRes = rabbit_misc:r(VHost, queue, Q),
                {ok, RaName} = rpc:call(Server0, rabbit_queue_type_util, qname_to_internal_name, [QQRes]),
                wait_for_messages_ready([Server0], RaName, 3),
                {ok, Q0} = rpc:call(Server0, rabbit_amqqueue, lookup, [Q, VHost]),
                #{nodes := Nodes0} = amqqueue:get_type_state(Q0),
                ?assertEqual(3, length(Nodes0))
            end || Q <- QQs, VHost <- VHosts]
    end.

force_checkpoint_on_queue(Config) ->
    check_quorum_queues_v4_compat(Config),

    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    RaName = ra_name(QQ),
    QName = rabbit_misc:r(<<"/">>, queue, QQ),

    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    N = 20_000,
    rabbit_ct_client_helpers:publish(Ch, QQ, N),
    wait_for_messages_ready([Server0], RaName, N),

    %% The state before any checkpoints
    rabbit_ct_helpers:await_condition(
      fun() ->
          {ok, State, _} = rpc:call(Server0, ra, member_overview, [{RaName, Server0}]),
          #{log := #{latest_checkpoint_index := LCI}} = State,
          LCI =:= undefined
      end),
    rabbit_ct_helpers:await_condition(
      fun() ->
          {ok, State, _} = rpc:call(Server1, ra, member_overview, [{RaName, Server1}]),
          #{log := #{latest_checkpoint_index := LCI}} = State,
          LCI =:= undefined
      end),
    rabbit_ct_helpers:await_condition(
      fun() ->
          {ok, State, _} = rpc:call(Server2, ra, member_overview, [{RaName, Server2}]),
          #{log := #{latest_checkpoint_index := LCI}} = State,
          LCI =:= undefined
      end),

    {ok, State0, _} = rpc:call(Server0, ra, member_overview, [{RaName, Server0}]),
    ct:pal("Ra server state before forcing a checkpoint: ~tp~n", [State0]),

    %% wait for longer than ?CHECK_MIN_INTERVAL_MS ms
    timer:sleep(?CHECK_MIN_INTERVAL_MS + 1000),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
        force_checkpoint_on_queue, [QName]),

    %% Wait for initial checkpoint and make sure it's not 0
    rabbit_ct_helpers:await_condition(
      fun() ->
          {ok, State, _} = rpc:call(Server0, ra, member_overview, [{RaName, Server0}]),
          ct:pal("Ra server state post forced checkpoint: ~tp~n", [State]),
          #{log := #{latest_checkpoint_index := LCI}} = State,
          (LCI =/= undefined) andalso (LCI >= N)
      end),
    rabbit_ct_helpers:await_condition(
      fun() ->
          {ok, State, _} = rpc:call(Server1, ra, member_overview, [{RaName, Server1}]),
          ct:pal("Ra server state post forced checkpoint: ~tp~n", [State]),
          #{log := #{latest_checkpoint_index := LCI}} = State,
          (LCI =/= undefined) andalso (LCI >= N)
      end),
    rabbit_ct_helpers:await_condition(
      fun() ->
          {ok, State, _} = rpc:call(Server2, ra, member_overview, [{RaName, Server2}]),
          ct:pal("Ra server state post forced checkpoint: ~tp~n", [State]),
          #{log := #{latest_checkpoint_index := LCI}} = State,
          (LCI =/= undefined) andalso (LCI >= N)
      end).

force_checkpoint(Config) ->
    check_quorum_queues_v4_compat(Config),

    [Server0, _Server1, _Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    QQName = rabbit_misc:r(<<"/">>, queue, QQ),
    CQ = <<"force_checkpoint_cq">>,
    RaName = ra_name(QQ),

    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-queue-type">>, longstr, <<"classic">>}])),

    rabbit_ct_client_helpers:publish(Ch, QQ, 3),
    wait_for_messages_ready([Server0], RaName, 3),

    ForceCheckpointRes = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
        force_checkpoint, [<<".*">>, <<".*">>]),
    ExpectedRes = [{QQName, {ok}}],

    % Result should only have quorum queue
    ?assertEqual(ExpectedRes, ForceCheckpointRes).

% Tests that, if the process of a QQ is dead in the moment of declaring a policy
% that affects such queue, when the process is made available again, the policy
% will eventually get applied. (https://github.com/rabbitmq/rabbitmq-server/issues/7863)
policy_repair(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "Should not run in mixed version environments"};
        _ ->
            [Server0, _Server1, _Server2] = Servers =
                rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
            Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
            #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

            QQ = ?config(queue_name, Config),
            ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                         declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
            RaName = ra_name(QQ),
            ExpectedMaxLength1 = 10,
            Priority1 = 1,
            ok = rabbit_ct_broker_helpers:rpc(
                Config,
                0,
                rabbit_policy,
                set,
                [
                    <<"/">>,
                    <<QQ/binary, "_1">>,
                    QQ,
                    [{<<"max-length">>, ExpectedMaxLength1}, {<<"overflow">>, <<"reject-publish">>}],
                    Priority1,
                    <<"quorum_queues">>,
                    <<"acting-user">>
                ]),

            % Wait for the policy to apply
            QueryFun = fun rabbit_fifo:overview/1,
            ?awaitMatch({ok, {_, #{config := #{max_length := ExpectedMaxLength1}}}, _},
                        rpc:call(Server0, ra, local_query, [RaName, QueryFun]),
                        ?DEFAULT_AWAIT),

            % Check the policy has been applied
            %   Insert MaxLength1 + some messages but after consuming all messages only
            %   MaxLength1 are retrieved.
            %   Checking twice to ensure consistency
            publish_confirm_many(Ch, QQ, ExpectedMaxLength1 + 1),
            % +1 because QQs let one pass
            wait_for_messages_ready(Servers, RaName, ExpectedMaxLength1 + 1),
            fail = publish_confirm(Ch, QQ),
            fail = publish_confirm(Ch, QQ),
            consume_all(Ch, QQ),

            % Set higher priority policy, allowing more messages
            ExpectedMaxLength2 = 20,
            Priority2 = 2,
            ok = rabbit_ct_broker_helpers:rpc(
                Config,
                0,
                rabbit_policy,
                set,
                [
                    <<"/">>,
                    <<QQ/binary, "_2">>,
                    QQ,
                    [{<<"max-length">>, ExpectedMaxLength2}, {<<"overflow">>, <<"reject-publish">>}],
                    Priority2,
                    <<"quorum_queues">>,
                    <<"acting-user">>
                ]),

            % Wait for the policy to apply
            ?awaitMatch({ok, {_, #{config := #{max_length := ExpectedMaxLength2}}}, _},
                        rpc:call(Server0, ra, local_query, [RaName, QueryFun]),
                        ?DEFAULT_AWAIT),

            % Check the policy has been applied
            %   Insert MaxLength2 + some messages but after consuming all messages only
            %   MaxLength2 are retrieved.
            %   Checking twice to ensure consistency.
            % + 1 because QQs let one pass
            publish_confirm_many(Ch, QQ, ExpectedMaxLength2 + 1),
            wait_for_messages_ready(Servers, RaName, ExpectedMaxLength2 + 1),
            fail = publish_confirm(Ch, QQ),
            fail = publish_confirm(Ch, QQ),
            consume_all(Ch, QQ),

            % Ensure the queue process is unavailable
            lists:foreach(fun(Srv) -> ensure_qq_proc_dead(Config, Srv, RaName) end, Servers),

            % Add policy with higher priority, allowing even more messages.
            ExpectedMaxLength3 = 30,
            Priority3 = 3,
            ok = rabbit_ct_broker_helpers:rpc(
                Config,
                0,
                rabbit_policy,
                set,
                [
                    <<"/">>,
                    <<QQ/binary, "_3">>,
                    QQ,
                    [{<<"max-length">>, ExpectedMaxLength3}, {<<"overflow">>, <<"reject-publish">>}],
                    Priority3,
                    <<"quorum_queues">>,
                    <<"acting-user">>
                ]),

            % Restart the queue process.
            {ok, Queue} =
                rabbit_ct_broker_helpers:rpc(
                    Config,
                    0,
                    rabbit_amqqueue,
                    lookup,
                    [{resource, <<"/">>, queue, QQ}]),
            lists:foreach(
                fun(Srv) ->
                    rabbit_ct_broker_helpers:rpc(
                        Config,
                        Srv,
                        rabbit_quorum_queue,
                        recover,
                        [foo, [Queue]]
                    )
                end,
                Servers),

            % Wait for the queue to be available again.
            lists:foreach(fun(Srv) ->
                rabbit_ct_helpers:await_condition(
                    fun () ->
                        is_pid(
                            rabbit_ct_broker_helpers:rpc(
                                Config,
                                Srv,
                                erlang,
                                whereis,
                                [RaName]))
                    end)
                end,
                Servers),

            % Wait for the policy to apply
            ?awaitMatch({ok, {_, #{config := #{max_length := ExpectedMaxLength3}}}, _},
                        rpc:call(Server0, ra, local_query, [RaName, QueryFun]),
                        ?DEFAULT_AWAIT),

            % Check the policy has been applied
            %   Insert MaxLength3 + some messages but after consuming all messages only
            %   MaxLength3 are retrieved.
            %   Checking twice to ensure consistency.
            % + 1 because QQs let one pass
            publish_confirm_many(Ch, QQ, ExpectedMaxLength3 + 1),
            wait_for_messages_ready(Servers, RaName, ExpectedMaxLength3 + 1),
            fail = publish_confirm(Ch, QQ),
            fail = publish_confirm(Ch, QQ),
            consume_all(Ch, QQ)
    end.

subscribe_from_each(Config) ->

    [Server0 | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    [begin
         publish_confirm(Ch, QQ)
     end || _ <- Servers],
    timer:sleep(100),
    %% roll the wal to force consumer messages to be read from disk
    [begin
         ok = rpc:call(S, ra_log_wal, force_roll_over, [ra_log_wal])
     end || S <- Servers],

    [begin
         ct:pal("NODE ~p", [S]),
         C = rabbit_ct_client_helpers:open_channel(Config, S),
         qos(C, 1, false),
         subscribe(C, QQ, false),
         receive
             {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
                 amqp_channel:call(C, #'basic.ack'{delivery_tag = DeliveryTag})
         after 5000 ->
                   flush(1),
                   ct:fail("basic.deliver timeout")
         end,
         timer:sleep(256),
         rabbit_ct_client_helpers:close_channel(C),
         flush(1)

     end || S <- Servers],

    ok.

dont_leak_file_handles(Config) ->
    check_quorum_queues_v4_compat(Config),

    [Server0 | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    [begin
         publish_confirm(Ch, QQ)
     end || _ <- Servers],
    timer:sleep(100),
    %% roll the wal to force consumer messages to be read from disk
    [begin
         ok = rpc:call(S, ra_log_wal, force_roll_over, [ra_log_wal])
     end || S <- Servers],
    timer:sleep(256),

    C = rabbit_ct_client_helpers:open_channel(Config, Server0),
    [_, NCh1] = rpc:call(Server0, rabbit_channel, list, []),
    qos(C, 1, false),
    subscribe(C, QQ, false),
    [begin
         receive
             {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
                 amqp_channel:call(C, #'basic.ack'{delivery_tag = DeliveryTag})
         after 5000 ->
                   flush(1),
                   ct:fail("basic.deliver timeout")
         end
     end || _ <- Servers],
    flush(1),
    [{_, MonBy2}] = rpc:call(Server0, erlang, process_info, [NCh1, [monitored_by]]),
    NumMonRefsBefore = length([M || M <- MonBy2, is_reference(M)]),
    %% delete queue
    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QQ})),
    [{_, MonBy3}] = rpc:call(Server0, erlang, process_info, [NCh1, [monitored_by]]),
    NumMonRefsAfter = length([M || M <- MonBy3, is_reference(M)]),
    %% this isn't an ideal way to assert this but every file handle creates
    %% a monitor that (currenlty?) is a reference so we assert that we have
    %% fewer reference monitors after
    ?assert(NumMonRefsAfter < NumMonRefsBefore),

    rabbit_ct_client_helpers:close_channel(C),
    ok.

gh_12635(Config) ->
    check_quorum_queues_v4_compat(Config),

    % https://github.com/rabbitmq/rabbitmq-server/issues/12635
    [Server0, _Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, quorum_min_checkpoint_interval, 1]),

    Ch0 = rabbit_ct_client_helpers:open_channel(Config, Server0),
    #'confirm.select_ok'{} = amqp_channel:call(Ch0, #'confirm.select'{}),
    QQ = ?config(queue_name, Config),
    RaName = ra_name(QQ),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch0, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% stop member to simulate slow or down member
    ok = rpc:call(Server2, ra, stop_server, [quorum_queues, {RaName, Server2}]),

    publish_confirm(Ch0, QQ),
    publish_confirm(Ch0, QQ),

    %% a QQ will not take checkpoints more frequently than every 1s
    timer:sleep(1000),
    %% force a checkpoint on leader
    ok = rpc:call(Server0, ra, cast_aux_command, [{RaName, Server0}, force_checkpoint]),
    rabbit_ct_helpers:await_condition(
      fun () ->
              {ok, #{log := Log}, _} = rpc:call(Server0, ra, member_overview, [{RaName, Server0}]),
              undefined =/= maps:get(latest_checkpoint_index, Log)
      end),

    %% publish 1 more message
    publish_confirm(Ch0, QQ),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    %% subscribe then cancel, this will assign the messages against the consumer
    %% but as the member is down they will not be delivered
    qos(Ch2, 100, false),
    subscribe(Ch2, QQ, false),
    rabbit_ct_client_helpers:close_channel(Ch2),
    flush(100),
    %% purge
    #'queue.purge_ok'{} = amqp_channel:call(Ch0, #'queue.purge'{queue = QQ}),

    rabbit_ct_helpers:await_condition(
      fun () ->
              {ok, #{log := Log}, _} = rpc:call(Server0, ra, member_overview, [{RaName, Server0}]),
              undefined =/= maps:get(snapshot_index, Log)
      end),
    %% restart the down member
    ok = rpc:call(Server2, ra, restart_server, [quorum_queues, {RaName, Server2}]),
    Pid2 = rpc:call(Server2, erlang, whereis, [RaName]),
    ?assert(is_pid(Pid2)),
    Ref = erlang:monitor(process, Pid2),
    receive
        {'DOWN',Ref, process,_, _} ->
            ct:fail("unexpected DOWN")
    after 500 ->
              ok
    end,
    flush(1),
    ok.

priority_queue_fifo(Config) ->
    %% testing: if hi priority messages are published before lo priority
    %% messages they are always consumed first (fifo)
    check_quorum_queues_v4_compat(Config),
    [Server0 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Queue = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Queue, 0, 0},
                 declare(Ch, Queue,
                         [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ExpectedHi =
        [begin
             MsgP5 = integer_to_binary(P),
             ok = amqp_channel:cast(Ch, #'basic.publish'{routing_key = Queue},
                                    #amqp_msg{props = #'P_basic'{priority = P},
                                              payload = MsgP5}),
             MsgP5
             %% high priority is > 4
         end || P <- lists:seq(5, 10)],

    ExpectedLo =
        [begin
             MsgP1 = integer_to_binary(P),
             ok = amqp_channel:cast(Ch, #'basic.publish'{routing_key = Queue},
                                    #amqp_msg{props = #'P_basic'{priority = P},
                                              payload = MsgP1}),
             MsgP1
         end || P <- lists:seq(0, 4)],

    validate_queue(Ch, Queue, ExpectedHi ++ ExpectedLo),
    ok.

priority_queue_2_1_ratio(Config) ->
    %% testing: if lo priority messages are published before hi priority
    %% messages are consumed in a 2:1 hi to lo ratio
    check_quorum_queues_v4_compat(Config),
    [Server0 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Queue = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Queue, 0, 0},
                 declare(Ch, Queue,
                         [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ExpectedLo =
        [begin
             MsgP1 = integer_to_binary(P),
             ok = amqp_channel:cast(Ch, #'basic.publish'{routing_key = Queue},
                                    #amqp_msg{props = #'P_basic'{priority = P},
                                              payload = MsgP1}),
             MsgP1
         end || P <- lists:seq(0, 4)],
    ExpectedHi =
        [begin
             MsgP5 = integer_to_binary(P),
             ok = amqp_channel:cast(Ch, #'basic.publish'{routing_key = Queue},
                                    #amqp_msg{props = #'P_basic'{priority = P},
                                              payload = MsgP5}),
             MsgP5
             %% high priority is > 4
         end || P <- lists:seq(5, 14)],

    Expected = lists_interleave(ExpectedLo, ExpectedHi),

    validate_queue(Ch, Queue, Expected),
    ok.

reject_after_leader_transfer(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    RaName = binary_to_atom(<<"%2F_", QQ/binary>>, utf8),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, QQ),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    {#'basic.get_ok'{delivery_tag = Tag}, #amqp_msg{}} =
        basic_get(Ch2, QQ, false, 10),

    ServerId1 = {RaName, Server1},
    ct:pal("transfer leadership ~p",
           [rabbit_ct_broker_helpers:rpc(Config, 0, ra,
                                         transfer_leadership, [ServerId1, ServerId1])]),
    ok = amqp_channel:call(Ch2, #'basic.reject'{delivery_tag = Tag,
                                                requeue = true}),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),

    {#'basic.get_ok'{delivery_tag = Tag2}, #amqp_msg{}} =
        basic_get(Ch2, QQ, false, 10),

    ok = amqp_channel:call(Ch2, #'basic.reject'{delivery_tag = Tag2,
                                                requeue = true}),
    ok.

shrink_all(Config) ->
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    AQ = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({'queue.declare_ok', AQ, 0, 0},
                 declare(Ch, AQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ?awaitMatch([{_, {ok, 2}}, {_, {ok, 2}}],
                rpc:call(Server0, rabbit_quorum_queue, shrink_all, [Server2]),
                ?DEFAULT_AWAIT),
    ?awaitMatch([{_, {ok, 1}}, {_, {ok, 1}}],
                rpc:call(Server0, rabbit_quorum_queue, shrink_all, [Server1]),
                ?DEFAULT_AWAIT),
    ?awaitMatch([{_, {error, 1, last_node}},
                 {_, {error, 1, last_node}}],
                rpc:call(Server0, rabbit_quorum_queue, shrink_all, [Server0]),
                ?DEFAULT_AWAIT),
    ok.

rebalance(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "rebalance test isn't mixed version compatible"};
        false ->
            rebalance0(Config)
    end.

rebalance0(Config) ->
    [Server0, _, _] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),

    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    Q4 = <<"q4">>,
    Q5 = <<"q5">>,

    ?assertEqual({'queue.declare_ok', Q1, 0, 0},
                 declare(Ch, Q1, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({'queue.declare_ok', Q2, 0, 0},
                 declare(Ch, Q2, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    {ok, _, {_, Leader1}} = ?awaitMatch({ok, _, {_, _}},
                                        ra:members({ra_name(Q1), Server0}),
                                        ?DEFAULT_AWAIT),
    {ok, _, {_, Leader2}} = ?awaitMatch({ok, _, {_, _}},
                                        ra:members({ra_name(Q2), Server0}),
                                        ?DEFAULT_AWAIT),
    rabbit_ct_client_helpers:publish(Ch, Q1, 3),
    rabbit_ct_client_helpers:publish(Ch, Q2, 2),

    ?assertEqual({'queue.declare_ok', Q3, 0, 0},
                 declare(Ch, Q3, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({'queue.declare_ok', Q4, 0, 0},
                 declare(Ch, Q4, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({'queue.declare_ok', Q5, 0, 0},
                 declare(Ch, Q5, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Q1 and Q2 should not have moved leader, as these are the queues with more
    %% log entries and we allow up to two queues per node (3 nodes, 5 queues)
    ?awaitMatch({ok, _, {_, Leader1}}, ra:members({ra_name(Q1), Server0}), ?DEFAULT_AWAIT),
    ?awaitMatch({ok, _, {_, Leader2}}, ra:members({ra_name(Q2), Server0}), ?DEFAULT_AWAIT),

    %% Check that we have at most 2 queues per node
    ?awaitMatch(true,
                begin
                    {ok, Summary} = rpc:call(Server0, rabbit_amqqueue, rebalance, [quorum, ".*", ".*"]),
                    lists:all(fun(NodeData) ->
                                      lists:all(fun({_, V}) when is_integer(V) -> V =< 2;
                                                   (_) -> true end,
                                                NodeData)
                              end, Summary)
                end, ?DEFAULT_AWAIT),
    ok.

subscribe_should_fail_when_global_qos_true(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    qos(Ch, 10, true),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    try subscribe(Ch, QQ, false) of
        _ -> exit(subscribe_should_not_pass)
    catch
        _:_ = Err ->
        ct:pal("subscribe_should_fail_when_global_qos_true caught an error: ~tp", [Err])
    end,
    ok.

dead_letter_to_classic_queue(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    CQ = <<"classic-dead_letter_to_classic_queue">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, CQ}
                                 ])),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    test_dead_lettering(true, Config, Ch, Servers, ra_name(QQ), QQ, CQ).

dead_letter_with_memory_limit(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    CQ = <<"classic-dead_letter_with_memory_limit">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 0},
                                  {<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, CQ}
                                 ])),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    test_dead_lettering(true, Config, Ch, Servers, ra_name(QQ), QQ, CQ).

test_dead_lettering(PolicySet, Config, Ch, Servers, RaName, Source, Destination) ->
    publish(Ch, Source),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages(Config, [[Destination, <<"0">>, <<"0">>, <<"0">>]]),
    DeliveryTag = basic_get_tag(Ch, Source, false),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 1),
    wait_for_messages(Config, [[Destination, <<"0">>, <<"0">>, <<"0">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    case PolicySet of
        true ->
            wait_for_messages(Config, [[Destination, <<"1">>, <<"1">>, <<"0">>]]),
            _ = basic_get_tag(Ch, Destination, true);
        false ->
            wait_for_messages(Config, [[Destination, <<"0">>, <<"0">>, <<"0">>]])
    end.

dead_letter_policy(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    CQ = <<"classic-dead_letter_policy">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"dlx">>, <<"dead_letter.*">>, <<"queues">>,
           [{<<"dead-letter-exchange">>, <<"">>},
            {<<"dead-letter-routing-key">>, CQ}]),
    RaName = ra_name(QQ),
    test_dead_lettering(true, Config, Ch, Servers, RaName, QQ, CQ),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"dlx">>),
    test_dead_lettering(false, Config, Ch, Servers, RaName, QQ, CQ).

invalid_policy(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"max-age">>, <<"invalid_policy.*">>, <<"queues">>,
           [{<<"max-age">>, <<"5s">>}]),
    Info = rpc:call(Server, rabbit_quorum_queue, infos,
                    [rabbit_misc:r(<<"/">>, queue, QQ)]),
    ?assertEqual('', proplists:get_value(policy, Info)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"max-age">>).

pre_existing_invalid_policy(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"max-age">>, <<"invalid_policy.*">>, <<"queues">>,
           [{<<"max-age">>, <<"5s">>}]),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    Info = rpc:call(Server, rabbit_quorum_queue, infos,
                    [rabbit_misc:r(<<"/">>, queue, QQ)]),
    ?assertEqual('', proplists:get_value(policy, Info)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"max-age">>),
    ok.

dead_letter_to_quorum_queue(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    QQ2 = <<"dead_letter_to_quorum_queue-q2">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, QQ2}
                                 ])),
    ?assertEqual({'queue.declare_ok', QQ2, 0, 0},
                 declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    RaName2 = ra_name(QQ2),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages_ready(Servers, RaName2, 0),
    wait_for_messages_pending_ack(Servers, RaName2, 0),
    DeliveryTag = basic_get_tag(Ch, QQ, false),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 1),
    wait_for_messages_ready(Servers, RaName2, 0),
    wait_for_messages_pending_ack(Servers, RaName2, 0),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages_ready(Servers, RaName2, 1),
    wait_for_messages_pending_ack(Servers, RaName2, 0),

    {#'basic.get_ok'{delivery_tag = _Tag},
     #amqp_msg{} = Msg} = basic_get(Ch, QQ2, false, 1),
    ct:pal("Msg ~p", [Msg]),
    flush(1000),
    ok.

dead_letter_from_classic_to_quorum_queue(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    CQ = <<"classic-q-dead_letter_from_classic_to_quorum_queue">>,
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, QQ}
                                 ])),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    publish(Ch, CQ),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),
    DeliveryTag = basic_get_tag(Ch, CQ, false),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"0">>, <<"1">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    _ = basic_get_tag(Ch, QQ, false),
    rabbit_ct_client_helpers:close_channel(Ch).

cleanup_queue_state_on_channel_after_publish(Config) ->
    %% Declare/delete the queue in one channel and publish on a different one,
    %% to verify that the cleanup is propagated through channels
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% The stream coordinator is also a ra process, we need to ensure the quorum tests
    %% are not affected by any other ra cluster that could be added in the future
    Children = length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch1, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    publish(Ch2, QQ),
    Res = dirty_query(Servers, RaName, fun rabbit_fifo:query_consumer_count/1),
    ct:pal ("Res ~tp", [Res]),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages_ready(Servers, RaName, 1),
    [NCh1, NCh2] = rpc:call(Server, rabbit_channel, list, []),
    %% Check the channel state contains the state for the quorum queue on
    %% channel 1 and 2
    wait_for_cleanup(Server, NCh1, 0),
    wait_for_cleanup(Server, NCh2, 1),
    %% then delete the queue and wait for the process to terminate
    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch1, #'queue.delete'{queue = QQ})),
    rabbit_ct_helpers:await_condition(
      fun() ->
              Children == length(rpc:call(Server, supervisor, which_children,
                                          [?SUPNAME]))
      end, 30000),
    %% Check that all queue states have been cleaned
    wait_for_cleanup(Server, NCh2, 0),
    wait_for_cleanup(Server, NCh1, 0).

cleanup_queue_state_on_channel_after_subscribe(Config) ->
    %% Declare/delete the queue and publish in one channel, while consuming on a
    %% different one to verify that the cleanup is propagated through channels
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% The stream coordinator is also a ra process, we need to ensure the quorum tests
    %% are not affected by any other ra cluster that could be added in the future
    Children = length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch1, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    publish(Ch1, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    subscribe(Ch2, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 1),
            amqp_channel:cast(Ch2, #'basic.ack'{delivery_tag = DeliveryTag,
                                                multiple     = true}),
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 0)
    end,
    [NCh1, NCh2] = rpc:call(Server, rabbit_channel, list, []),
    %% Check the channel state contains the state for the quorum queue on channel 1 and 2
    wait_for_cleanup(Server, NCh1, 1),
    wait_for_cleanup(Server, NCh2, 1),
    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch1, #'queue.delete'{queue = QQ})),
    rabbit_ct_helpers:await_condition(
      fun() ->
              Children == length(rpc:call(Server, supervisor, which_children, [?SUPNAME]))
      end, 30000),
    %% Check that all queue states have been cleaned
    wait_for_cleanup(Server, NCh1, 0),
    wait_for_cleanup(Server, NCh2, 0).

recover_from_single_failure(Config) ->
    [Server, Server1, Server2] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    Running = Servers -- [Server2],
    assert_cluster_status({Servers, Servers, Running}, Running),
    RaName = ra_name(QQ),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready([Server, Server1], RaName, 3),
    wait_for_messages_pending_ack([Server, Server1], RaName, 0),

    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
    wait_for_messages_ready(Servers, RaName, 3),
    wait_for_messages_pending_ack(Servers, RaName, 0).

recover_from_multiple_failures(Config) ->
    [Server1, Server, Server2] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    Running = Servers -- [Server1],
    assert_cluster_status({Servers, Servers, Running}, Running),

    RaName = ra_name(QQ),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    assert_cluster_status({Servers, Servers, [Server]}, [Server]),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    wait_for_messages_ready([Server], RaName, 3),
    wait_for_messages_pending_ack([Server], RaName, 0),

    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),

    %% there is an assumption here that the messages were not lost and were
    %% recovered when a quorum was restored. Not the best test perhaps.
    wait_for_messages_ready(Servers, RaName, 6),
    wait_for_messages_pending_ack(Servers, RaName, 0).

publishing_to_unavailable_queue(Config) ->
    %% publishing to an unavailable queue but with a reachable member should result
    %% in the initial enqueuer command that is send syncronously to set up
    %% the enqueuer session timing out and the message being nacked
    [Server, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    TCh = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    RaName = binary_to_atom(<<"%2F_", QQ/binary>>, utf8),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(TCh, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    rabbit_quorum_queue:stop_server({RaName, Server1}),
    rabbit_quorum_queue:stop_server({RaName, Server2}),

    ct:pal("opening channel to ~w", [Server]),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish_many(Ch, QQ, 1),
    %% this should result in a nack
    ok = receive
             #'basic.ack'{}  -> fail;
             #'basic.nack'{} -> ok
         after 90000 ->
                   flush(1),
                   exit(confirm_timeout)
         end,
    rabbit_quorum_queue:restart_server({RaName, Server1}),
    publish_many(Ch, QQ, 1),
    %% this should now be acked
    %% check we get at least on ack
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 90000 ->
                   flush(1),
                   exit(confirm_timeout)
         end,
    flush(1),
    rabbit_quorum_queue:restart_server({RaName, Server2}),
    ok.

leadership_takeover(Config) ->
    %% Kill nodes in succession forcing the takeover of leadership, and all messages that
    %% are in the queue.
    [Server, Server1, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_control_helper:command(stop_app, Server1),
    Running = Servers -- [Server1],
    assert_cluster_status({Servers, Servers, Running}, Running),

    RaName = ra_name(QQ),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    wait_for_messages_ready([Server], RaName, 3),
    wait_for_messages_pending_ack([Server], RaName, 0),

    ok = rabbit_control_helper:command(stop_app, Server2),
    ok = rabbit_control_helper:command(start_app, Server1),
    ok = rabbit_control_helper:command(stop_app, Server),
    ok = rabbit_control_helper:command(start_app, Server2),
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(start_app, Server),

    wait_for_messages_ready([Server2, Server], RaName, 3),
    wait_for_messages_pending_ack([Server2, Server], RaName, 0),

    ok = rabbit_control_helper:command(start_app, Server1),
    wait_for_messages_ready(Servers, RaName, 3),
    wait_for_messages_pending_ack(Servers, RaName, 0).

metrics_cleanup_on_leadership_takeover(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "metrics_cleanup_on_leadership_takeover test isn't mixed version compatible"};
        false ->
            metrics_cleanup_on_leadership_takeover0(Config)
    end.

metrics_cleanup_on_leadership_takeover0(Config) ->
    %% Queue core metrics should be deleted from a node once the leadership is transferred
    %% to another follower
    [Server, _, _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    wait_for_messages_ready([Server], RaName, 3),
    wait_for_messages_pending_ack([Server], RaName, 0),
    {ok, _, {_, Leader}} = ra:members({RaName, Server}),
    QRes = rabbit_misc:r(<<"/">>, queue, QQ),
    rabbit_ct_helpers:await_condition(
      fun() ->
              case rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]) of
                  [{QRes, 3, 0, 3, _}] -> true;
                  _ -> false
              end
      end, 30000),
    force_leader_change(Servers, QQ),
    rabbit_ct_helpers:await_condition(
      fun () ->
              [] =:= rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]) andalso
                  [] =:= rpc:call(Leader, ets, lookup, [queue_metrics, QRes])
      end, 30000),
    ok.

metrics_cleanup_on_leader_crash(Config) ->
    %% Queue core metrics should be deleted from a node once the leadership is transferred
    %% to another follower
    [Server | _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    {ok, _, {Name, Leader}} = ra:members({RaName, Server}),
    QRes = rabbit_misc:r(<<"/">>, queue, QQ),
    rabbit_ct_helpers:await_condition(
      fun() ->
              case rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]) of
                  [{QRes, 3, 0, 3, _}] -> true;
                  _ -> false
              end
      end, 30000),
    Pid = rpc:call(Leader, erlang, whereis, [Name]),
    rpc:call(Leader, erlang, exit, [Pid, kill]),
    [Other | _] = lists:delete(Leader, Servers),
    catch ra:trigger_election(Other),
    %% kill it again just in case it came straight back up again
    catch rpc:call(Leader, erlang, exit, [Pid, kill]),

    %% this isn't a reliable test as the leader can be restarted so quickly
    %% after a crash it is elected leader of the next term as well.
    rabbit_ct_helpers:await_condition(
      fun() ->
              [] == rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes])
      end, 30000),
    ok.


delete_declare(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "delete_declare isn't mixed version reliable"};
        false ->
            delete_declare0(Config)
    end.

delete_declare0(Config) ->
    %% Delete cluster in ra is asynchronous, we have to ensure that we handle that in rmq
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config,
                                                                   nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 3),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QQ})),
    %% the actual data deletions happen after the call has returned as a quorum
    %% queue leader waits for all nodes to confirm they replicated the poison
    %% pill before terminating itself.
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            %% when in mixed versions the QQ may not be able to apply the posion
            %% pill for all nodes so need to wait longer for forced delete to
            %% happen
            timer:sleep(10000);
        false ->
            timer:sleep(1000)
    end,

    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Ensure that is a new queue and it's empty
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 0).

sync_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    {error, _, _} =
        rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, [<<"sync_queue">>, QQ]),
    ok.

cancel_sync_queue(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    {error, _, _} =
        rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, [<<"cancel_sync_queue">>, QQ]),
    ok.

%% Test case for https://github.com/rabbitmq/rabbitmq-server/issues/5141
%% Tests backwards compatibility in 3.9 / 3.8 mixed version cluster.
%% Server 1 runs a version AFTER queue type interface got introduced.
%% Server 2 runs a version BEFORE queue type interface got introduced.
channel_handles_ra_event(Config) ->
    Server1 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Server2 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    Q1 = ?config(queue_name, Config),
    Q2 = ?config(alt_queue_name, Config),
    ?assertMatch({'queue.declare_ok', Q1, 0, 0},
                 declare(Ch2, Q1,
                         [{<<"x-queue-type">>, longstr, <<"quorum">>},
                          {<<"x-quorum-initial-group-size">>, long, 1}])),
    ?assertMatch({'queue.declare_ok', Q2, 0, 0},
                 declare(Ch2, Q2,
                         [{<<"x-queue-type">>, longstr, <<"quorum">>},
                          {<<"x-quorum-initial-group-size">>, long, 1}])),
    publish(Ch1, Q1),
    publish(Ch1, Q2),
    wait_for_messages(Config, [[Q1, <<"1">>, <<"1">>, <<"0">>]]),
    wait_for_messages(Config, [[Q2, <<"1">>, <<"1">>, <<"0">>]]),
    ?assertEqual(1, basic_get_tag(Ch1, Q1, false)),
    ?assertEqual(2, basic_get_tag(Ch1, Q2, false)).

declare_during_node_down(Config) ->
    [DownServer, Server, _] = Servers = rabbit_ct_broker_helpers:get_node_configs(
                                          Config, nodename),

    stop_node(Config, DownServer),
    Running = Servers -- [DownServer],
    assert_cluster_status({Servers, Servers, Running}, Running),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    %% Since there are not sufficient running nodes, we expect that
    %% also stopped nodes are selected as replicas.
    UniqueMembers = lists:usort(Servers),
    ?awaitMatch(UniqueMembers,
                begin
                    {ok, Members0, _} = ra:members({RaName, Server}),
                    Members = lists:map(fun({_, N}) -> N end, Members0),
                    lists:usort(Members)
                end, 30_000),
    rabbit_ct_broker_helpers:start_node(Config, DownServer),
    assert_clustered(Servers),

    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),

    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            %% stop here if mixexd
            ok;
        false ->
            %% further assertions that we can consume from the newly
            %% started member
            SubCh = rabbit_ct_client_helpers:open_channel(Config, DownServer),
            subscribe(SubCh, QQ, false),
            receive_and_ack(Ch),
            wait_for_messages_ready(Servers, RaName, 0),
            ok
    end.

simple_confirm_availability_on_leader_change(Config) ->
    [Node1, Node2, _Node3] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% declare a queue on node2 - this _should_ host the leader on node 2
    DCh = rabbit_ct_client_helpers:open_channel(Config, Node2),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(DCh, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    erlang:process_flag(trap_exit, true),
    %% open a channel to another node
    Ch = rabbit_ct_client_helpers:open_channel(Config, Node1),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    ok = publish_confirm(Ch, QQ),

    %% stop the node hosting the leader
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node2),
    Running = Servers -- [Node2],
    assert_cluster_status({Servers, Servers, Running}, Running),

    %% this should not fail as the channel should detect the new leader and
    %% resend to that
    ok = publish_confirm(Ch, QQ),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
    ok.

confirm_availability_on_leader_change(Config) ->
    [Node1, Node2, _Node3] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% declare a queue on node2 - this _should_ host the leader on node 2
    DCh = rabbit_ct_client_helpers:open_channel(Config, Node2),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(DCh, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    erlang:process_flag(trap_exit, true),
    Publisher = spawn_link(
                  fun () ->
                          %% open a channel to another node
                          Ch = rabbit_ct_client_helpers:open_channel(Config, Node1),
                          #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
                          ConfirmLoop = fun Loop() ->
                                                ok = publish_confirm(Ch, QQ, 15000),
                                                receive
                                                    {done, P} ->
                                                        P ! publisher_done,
                                                        ok
                                                after 0 ->
                                                        Loop()
                                                end
                                        end,
                          ConfirmLoop()
                  end),

    %% Instead of waiting a random amount of time, let's wait
    %% until (at least) 100 new messages are published.
    wait_for_new_messages(Config, Node1, QQ, 100),
    %% stop the node hosting the leader
    stop_node(Config, Node2),
    Running = Servers -- [Node2],
    assert_cluster_status({Servers, Servers, Running}, Running),

    %% this should not fail as the channel should detect the new leader and
    %% resend to that
    wait_for_new_messages(Config, Node1, QQ, 100),
    Publisher ! {done, self()},
    receive
        publisher_done ->
            ok;
        {'EXIT', Publisher, Err} ->
            ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
            exit(Err)
    after ?TIMEOUT ->
              ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
              flush(100),
              exit(nothing_received_from_publisher_process)
    end,
    ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
    ok.

wait_for_new_messages(Config, Node, Name, Increase) ->
    wait_for_new_messages(Config, Node, Name, Increase, 60000).

wait_for_new_messages(Config, Node, Name, Increase, Timeout) ->
    Infos = rabbit_ct_broker_helpers:rabbitmqctl_list(
              Config, Node, ["list_queues", "name", "messages"]),
    case [Props || Props <- Infos, hd(Props) == Name] of
        [[Name, Msgs0]] ->
            Msgs = binary_to_integer(Msgs0),
            queue_utils:wait_for_min_messages(Config, Name, Msgs + Increase);
        _ when Timeout >= 0 ->
            Sleep = 200,
            timer:sleep(Sleep),
            wait_for_new_messages(
              Config, Node, Name, Increase, Timeout - Sleep)
    end.

flush(T) ->
    receive X ->
                ct:pal("flushed ~p", [X]),
                flush(T)
    after T ->
              ok
    end.


add_member_not_running(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ct:pal("add_member_not_running config ~tp", [Config]),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({error, node_not_running},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, 'rabbit@burrow', voter, 5000])).

add_member_classic(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, CQ, Server, voter, 5000])).

add_member_wrong_type(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', SQ, 0, 0},
                 declare(Ch, SQ, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ?assertEqual({error, not_quorum_queue},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, SQ, Server, voter, 5000])).

add_member_already_a_member(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    %% idempotent by design
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server, voter, 5000])).

add_member_not_found(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    QQ = ?config(queue_name, Config),
    ?assertEqual({error, not_found},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server, voter, 5000])).

add_member(Config) ->
    [Server0, Server1] = Servers0 =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({error, node_not_running},
                 rpc:call(Server0, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server1, voter, 5000])),
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),
    ?assertEqual(ok, rpc:call(Server1, rabbit_quorum_queue, add_member,
                              [<<"/">>, QQ, Server1, voter, 5000])),
    Info = rpc:call(Server0, rabbit_quorum_queue, infos,
                    [rabbit_misc:r(<<"/">>, queue, QQ)]),
    Servers = lists:sort(Servers0),
    ?assertEqual(Servers, lists:sort(proplists:get_value(online, Info, []))).

add_member_2(Config) ->
    %% this tests a scenario where an older node version is running a QQ
    %% and a member is added on a newer node version (for mixe testing)

    %% we dont validate the ff was enabled as this test should pass either way
    _ = rabbit_ct_broker_helpers:enable_feature_flag(Config, quorum_queue_non_voters),
    [Server0, Server1 | _] = _Servers0 =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-quorum-initial-group-size">>, long, 1}])),
    ?assertEqual(ok, rpc:call(Server0, rabbit_quorum_queue, add_member,
                              [<<"/">>, QQ, Server0, 5000])),
    Info = rpc:call(Server0, rabbit_quorum_queue, infos,
                    [rabbit_misc:r(<<"/">>, queue, QQ)]),
    Servers = lists:sort([Server0, Server1]),
    ?assertEqual(Servers, lists:sort(proplists:get_value(online, Info, []))).

delete_member_not_running(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    %% it should be possible to delete members that are not online (e.g. decomissioned)
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, 'rabbit@burrow'])).

delete_member_classic(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, CQ, Server])).

delete_member_wrong_type(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', SQ, 0, 0},
                 declare(Ch, SQ, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    ?assertEqual({error, not_quorum_queue},
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, SQ, Server])).

delete_member_queue_not_found(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    QQ = ?config(queue_name, Config),
    ?assertEqual({error, not_found},
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])).

delete_member(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    NServers = length(Servers),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?awaitMatch(NServers, count_online_nodes(Server, <<"/">>, QQ), ?DEFAULT_AWAIT),
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])).

delete_member_not_a_member(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    NServers = length(Servers),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?awaitMatch(NServers, count_online_nodes(Server, <<"/">>, QQ), ?DEFAULT_AWAIT),
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])),
    %% idempotent by design
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])).

delete_member_member_already_deleted(Config) ->
    [Server, Server2] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    NServers = length(Servers),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    RaName = ra_name(QQ),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?awaitMatch(NServers, count_online_nodes(Server, <<"/">>, QQ), ?DEFAULT_AWAIT),
    ServerId = {RaName, Server},
    ServerId2 = {RaName, Server2},
    %% use are APU directory to simulate situation where the ra:remove_server/2
    %% call timed out but later succeeded
    ?assertMatch(ok,
                 rpc:call(Server2, ra, leave_and_terminate,
                          [quorum_queues, ServerId, ServerId2])),

    %% idempotent by design
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server2])),
    {ok, Q} = rpc:call(Server, rabbit_amqqueue, lookup, [QQ, <<"/">>]),
    #{nodes := Nodes} = amqqueue:get_type_state(Q),
    ?assertEqual(1, length(Nodes)),
    ok.

delete_member_during_node_down(Config) ->
    [DownServer, Server, Remove] = Servers = rabbit_ct_broker_helpers:get_node_configs(
                                               Config, nodename),

    stop_node(Config, DownServer),
    Running = Servers -- [DownServer],
    assert_cluster_status({Servers, Servers, Running}, Running),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?awaitMatch(2, count_online_nodes(Server, <<"/">>, QQ), ?DEFAULT_AWAIT),
    ?assertEqual(ok, rpc:call(Server, rabbit_quorum_queue, delete_member,
                              [<<"/">>, QQ, Remove])),

    rabbit_ct_broker_helpers:start_node(Config, DownServer),
    ?assertEqual(ok, rpc:call(Server, rabbit_quorum_queue, repair_amqqueue_nodes,
                              [<<"/">>, QQ])),
    ok.

%% These tests check if node removal would cause any queues to lose (or not lose)
%% their quorum. See rabbitmq/rabbitmq-cli#389 for background.

node_removal_is_quorum_critical(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    NServers = length(Servers),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?awaitMatch(NServers, count_online_nodes(Server, <<"/">>, QName), ?DEFAULT_AWAIT),
    [begin
         Qs = rpc:call(S, rabbit_quorum_queue, list_with_minimum_quorum, []),
         ?assertEqual([QName], queue_names(Qs))
     end || S <- Servers].

node_removal_is_not_quorum_critical(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?awaitMatch(3, count_online_nodes(Server, <<"/">>, QName), ?DEFAULT_AWAIT),
    Qs = rpc:call(Server, rabbit_quorum_queue, list_with_minimum_quorum, []),
    ?assertEqual([], Qs).


cleanup_data_dir(Config) ->
    %% With Khepri this test needs to run in a 3-node cluster, otherwise the queue can't
    %% be deleted in minority
    %%
    %% This test is slow, but also checks that we handle properly errors when
    %% trying to delete a queue in minority. A case clause there had gone
    %% previously unnoticed.

    [Server2, Server1, Server3] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?awaitMatch(3, count_online_nodes(Server1, <<"/">>, QQ), ?DEFAULT_AWAIT),

    UId1 = proplists:get_value(ra_name(QQ), rpc:call(Server1, ra_directory, list_registered, [quorum_queues])),
    UId2 = proplists:get_value(ra_name(QQ), rpc:call(Server2, ra_directory, list_registered, [quorum_queues])),
    DataDir1 = rpc:call(Server1, ra_env, server_data_dir, [quorum_queues, UId1]),
    DataDir2 = rpc:call(Server2, ra_env, server_data_dir, [quorum_queues, UId2]),
    ?assert(filelib:is_dir(DataDir1)),
    ?assert(filelib:is_dir(DataDir2)),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    assert_cluster_status({Servers, Servers, [Server1, Server3]}, [Server1]),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QQ})),
    %% data dir 1 should be force deleted at this point
    ?assert(not filelib:is_dir(DataDir1)),
    ?assert(filelib:is_dir(DataDir2)),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
    assert_clustered(Servers),

    ?assertEqual(ok,
                 rpc:call(Server2, rabbit_quorum_queue, cleanup_data_dir, [])),
    ?awaitMatch(false, filelib:is_dir(DataDir2), 30000),
    ok.

reconnect_consumer_and_publish(Config) ->
    [Server | _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    {ok, _, {_, Leader}} = ra:members({RaName, Server}),
    [F1, F2] = lists:delete(Leader, Servers),
    ChF = rabbit_ct_client_helpers:open_channel(Config, F1),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    subscribe(ChF, QQ, false),
    receive
        {#'basic.deliver'{redelivered = false}, _} ->
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 1)
    end,
    Up = [Leader, F2],
    rabbit_ct_broker_helpers:block_traffic_between(F1, Leader),
    rabbit_ct_broker_helpers:block_traffic_between(F1, F2),
    wait_for_messages_ready(Up, RaName, 1),
    wait_for_messages_pending_ack(Up, RaName, 0),
    wait_for_messages_ready([F1], RaName, 0),
    wait_for_messages_pending_ack([F1], RaName, 1),
    rabbit_ct_broker_helpers:allow_traffic_between(F1, Leader),
    rabbit_ct_broker_helpers:allow_traffic_between(F1, F2),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 2),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered = false}, _} ->
            amqp_channel:cast(ChF, #'basic.ack'{delivery_tag = DeliveryTag,
                                                multiple     = false}),
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 1)
    end,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2,
                          redelivered = true}, _} ->
            amqp_channel:cast(ChF, #'basic.ack'{delivery_tag = DeliveryTag2,
                                                multiple     = false}),
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 0)
    end.

reconnect_consumer_and_wait(Config) ->
    [Server | _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    {ok, _, {_, Leader}} = ra:members({RaName, Server}),
    [F1, F2] = lists:delete(Leader, Servers),
    ChF = rabbit_ct_client_helpers:open_channel(Config, F1),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    subscribe(ChF, QQ, false),
    receive
        {#'basic.deliver'{redelivered  = false}, _} ->
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 1)
    end,
    Up = [Leader, F2],
    rabbit_ct_broker_helpers:block_traffic_between(F1, Leader),
    rabbit_ct_broker_helpers:block_traffic_between(F1, F2),
    wait_for_messages_ready(Up, RaName, 1),
    wait_for_messages_pending_ack(Up, RaName, 0),
    wait_for_messages_ready([F1], RaName, 0),
    wait_for_messages_pending_ack([F1], RaName, 1),
    rabbit_ct_broker_helpers:allow_traffic_between(F1, Leader),
    rabbit_ct_broker_helpers:allow_traffic_between(F1, F2),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 1),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered = true}, _} ->
            amqp_channel:cast(ChF, #'basic.ack'{delivery_tag = DeliveryTag,
                                                multiple     = false}),
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 0)
    end.

reconnect_consumer_and_wait_channel_down(Config) ->
    [Server | _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    {ok, _, {_, Leader}} = ra:members({RaName, Server}),
    [F1, F2] = lists:delete(Leader, Servers),
    ChF = rabbit_ct_client_helpers:open_channel(Config, F1),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    subscribe(ChF, QQ, false),
    receive
        {#'basic.deliver'{redelivered  = false}, _} ->
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 1)
    end,
    Up = [Leader, F2],
    rabbit_ct_broker_helpers:block_traffic_between(F1, Leader),
    rabbit_ct_broker_helpers:block_traffic_between(F1, F2),
    wait_for_messages_ready(Up, RaName, 1),
    wait_for_messages_pending_ack(Up, RaName, 0),
    wait_for_messages_ready([F1], RaName, 0),
    wait_for_messages_pending_ack([F1], RaName, 1),
    rabbit_ct_client_helpers:close_channel(ChF),
    rabbit_ct_broker_helpers:allow_traffic_between(F1, Leader),
    rabbit_ct_broker_helpers:allow_traffic_between(F1, F2),
    %% Let's give it a few seconds to ensure it doesn't attempt to
    %% deliver to the down channel - it shouldn't be monitored
    %% at this time!
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0).

delete_immediately_by_resource(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    %% The stream coordinator is also a ra process, we need to ensure the quorum tests
    %% are not affected by any other ra cluster that could be added in the future
    Children = length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),

    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    Cmd2 = ["eval", "rabbit_amqqueue:delete_immediately_by_resource([rabbit_misc:r(<<\"/\">>, queue, <<\"" ++ binary_to_list(QQ) ++ "\">>)])."],
    ?assertEqual({ok, "ok\n"}, rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd2)),

    %% Check that the application and process are down
    ?awaitMatch(Children,
                length(rpc:call(Server, supervisor, which_children, [?SUPNAME])),
                60000),
    ?awaitMatch({ra, _, _}, lists:keyfind(ra, 1,
                                          rpc:call(Server, application, which_applications, [])),
                                          ?DEFAULT_AWAIT).

subscribe_redelivery_count(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ,
                         [
                          {<<"x-queue-type">>, longstr, <<"quorum">>},
                          {<<"x-max-in-memory-length">>, long, 0}
                         ])),

    RaName = ra_name(QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    subscribe(Ch, QQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch(undefined, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple = false,
                                                requeue = true})
    after ?TIMEOUT ->
              exit(basic_deliver_timeout)
    end,

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ct:pal("H1 ~p", [H1]),
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple = false,
                                                requeue = true})
    after ?TIMEOUT ->
              flush(1),
              exit(basic_deliver_timeout_2)
    end,

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H2}}} ->
            ?assertMatch({DCHeader, _, 2}, rabbit_basic:header(DCHeader, H2)),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag2,
                                               multiple = false}),
            ct:pal("wait_for_messages_ready", []),
            wait_for_messages_ready(Servers, RaName, 0),
            ct:pal("wait_for_messages_pending_ack", []),
            wait_for_messages_pending_ack(Servers, RaName, 0)
    after ?TIMEOUT ->
              flush(500),
              exit(basic_deliver_timeout_3)
    end.

subscribe_redelivery_limit(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-delivery-limit">>, long, 1}])),

    publish(Ch, QQ),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch(undefined, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple = false,
                                                requeue = true})
    end,

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple = false,
                                                requeue = true})
    end,

    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    receive
        {#'basic.deliver'{redelivered  = true}, #amqp_msg{}} ->
            throw(unexpected_redelivery)
    after 5000 ->
            ok
    end.

subscribe_redelivery_limit_disable(Config) ->
    check_quorum_queues_v4_compat(Config),

    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-delivery-limit">>, long, -1}])),
    publish(Ch, QQ),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch(undefined, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple = false,
                                                requeue = true})
    end,

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    %% set an operator policy, this should always win
    ok = rabbit_ct_broker_helpers:set_operator_policy(
           Config, 0, <<"delivery-limit">>, QQ, <<"queues">>,
           [{<<"delivery-limit">>, 0}]),

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2,
                          redelivered = true},
         #amqp_msg{props = #'P_basic'{}}} ->
            % ?assertMatch(undefined, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag2,
                                                multiple = false,
                                                requeue = true})
    after ?TIMEOUT ->
              flush(1),
              ct:fail("message did not arrive as expected")
    end,
    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    ok = rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"delivery-limit">>),
    ok.

%% Test that consumer credit is increased correctly.
subscribe_redelivery_limit_many(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-delivery-limit">>, long, 1}])),

    publish_many(Ch, QQ, 5),
    wait_for_messages(Config, [[QQ, <<"5">>, <<"5">>, <<"0">>]]),

    qos(Ch, 2, false),
    subscribe(Ch, QQ, false),
    wait_for_messages(Config, [[QQ, <<"5">>, <<"3">>, <<"2">>]]),

    nack(Ch, false, true),
    nack(Ch, false, true),
    wait_for_messages(Config, [[QQ, <<"5">>, <<"3">>, <<"2">>]]),

    nack(Ch, false, true),
    nack(Ch, false, true),
    wait_for_messages(Config, [[QQ, <<"3">>, <<"1">>, <<"2">>]]),

    nack(Ch, false, true),
    nack(Ch, false, true),
    wait_for_messages(Config, [[QQ, <<"3">>, <<"1">>, <<"2">>]]),

    nack(Ch, false, true),
    nack(Ch, false, true),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),

    nack(Ch, false, true),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),

    nack(Ch, false, true),
    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    ok.

subscribe_redelivery_policy(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"delivery-limit">>, <<".*">>, <<"queues">>,
           [{<<"delivery-limit">>, 1}]),

    publish(Ch, QQ),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch(undefined, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple = false,
                                                requeue = true})
    end,

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple = false,
                                                requeue = true})
    end,

    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    receive
        {#'basic.deliver'{redelivered  = true}, #amqp_msg{}} ->
            throw(unexpected_redelivery)
    after 5000 ->
            ok
    end,
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"delivery-limit">>).

subscribe_redelivery_limit_with_dead_letter(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    DLX = <<"subcribe_redelivery_limit_with_dead_letter_dlx">>,
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-delivery-limit">>, long, 1},
                                  {<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, DLX}
                                 ])),
    ?assertEqual({'queue.declare_ok', DLX, 0, 0},
                 declare(Ch, DLX, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    publish(Ch, QQ),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch(undefined, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple = false,
                                                requeue = true})
    end,

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple = false,
                                                requeue = true})
    end,

    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    wait_for_messages(Config, [[DLX, <<"1">>, <<"1">>, <<"0">>]]).

consume_redelivery_count(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),

    DCHeader = <<"x-delivery-count">>,

    {#'basic.get_ok'{delivery_tag = DeliveryTag,
                     redelivered = false},
     #amqp_msg{props = #'P_basic'{headers = H0}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                           no_ack = false}),
    ?assertMatch(undefined, rabbit_basic:header(DCHeader, H0)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple = false,
                                        requeue = true}),
    %% wait for requeuing
    {#'basic.get_ok'{delivery_tag = DeliveryTag1,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H1}}} =
        basic_get(Ch, QQ, false, 300),

    ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                        multiple = false,
                                        requeue = true}),

    {#'basic.get_ok'{delivery_tag = DeliveryTag2,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H2}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 2}, rabbit_basic:header(DCHeader, H2)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag2,
                                        multiple = false,
                                        requeue = true}),
    ok.

message_bytes_metrics(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    {ok, _, {_, Leader}} = ra:members({RaName, Server}),
    QRes = rabbit_misc:r(<<"/">>, queue, QQ),

    publish(Ch, QQ),

    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    rabbit_ct_helpers:await_condition(
      fun() ->
              {3, 3, 0} == get_message_bytes(Leader, QRes)
      end, 30000),

    subscribe(Ch, QQ, false),

    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 1),
    rabbit_ct_helpers:await_condition(
      fun() ->
              {3, 0, 3} == get_message_bytes(Leader, QRes)
      end, 30000),

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple = false,
                                                requeue = false}),
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 0),
            rabbit_ct_helpers:await_condition(
              fun() ->
                      {0, 0, 0} == get_message_bytes(Leader, QRes)
              end, 30000)
    end,

    %% Let's publish and then close the consumer channel. Messages must be
    %% returned to the queue
    publish(Ch, QQ),

    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 1),
    rabbit_ct_helpers:await_condition(
      fun() ->
              {3, 0, 3} == get_message_bytes(Leader, QRes)
      end, 30000),

    rabbit_ct_client_helpers:close_channel(Ch),

    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    rabbit_ct_helpers:await_condition(
      fun() ->
              {3, 3, 0} == get_message_bytes(Leader, QRes)
      end, 30000),
    ok.

memory_alarm_rolls_wal(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    #{wal_data_dir := WalDataDir} = ra_system:fetch(quorum_queues, Server),
    [Wal0] = filelib:wildcard(WalDataDir ++ "/*.wal"),
    rabbit_ct_broker_helpers:set_alarm(Config, Server, memory),
    rabbit_ct_helpers:await_condition(
      fun() -> rabbit_ct_broker_helpers:get_alarms(Config, Server) =/= [] end,
      30000
     ),
    rabbit_ct_helpers:await_condition(
      fun() ->
              List = filelib:wildcard(WalDataDir ++ "/*.wal"),
              %% There is a small time window where there could be no
              %% file, but we need to wait for it to ensure it is not
              %% roll over again later on
              [Wal0] =/= List andalso [] =/= List
      end, 30000),
    Wal1 = lists:last(lists:sort(filelib:wildcard(WalDataDir ++ "/*.wal"))),

    %% roll over shouldn't happen if we trigger a new alarm in less than
    %% min_wal_roll_over_interval
    rabbit_ct_broker_helpers:set_alarm(Config, Server, memory),
    rabbit_ct_helpers:await_condition(
      fun() -> rabbit_ct_broker_helpers:get_alarms(Config, Server) =/= [] end,
      30000
     ),
    timer:sleep(1000),
    Wal2 = lists:last(lists:sort(filelib:wildcard(WalDataDir ++ "/*.wal"))),
    ?assert(Wal1 == Wal2),
    lists:foreach(fun (Node) ->
        ok = rabbit_ct_broker_helpers:clear_alarm(Config, Node, memory)
    end, rabbit_ct_broker_helpers:get_node_configs(Config, nodename)),
    ?awaitMatch([], rabbit_ct_broker_helpers:get_alarms(Config, Server), ?DEFAULT_AWAIT),
    ok.

reclaim_memory_with_wrong_queue_type(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    %% legacy, special case for classic queues
    ?assertMatch({error, classic_queue_not_supported},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              reclaim_memory, [<<"/">>, CQ])),
    SQ = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', SQ, 0, 0},
                 declare(Ch, SQ, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    %% all other (future) queue types get the same error
    ?assertMatch({error, not_quorum_queue},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              reclaim_memory, [<<"/">>, SQ])),
    ok.

queue_length_limit_drop_head(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-length">>, long, 1}])),

    RaName = ra_name(QQ),
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = <<"msg1">>}),
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = <<"msg2">>}),
    wait_for_consensus(QQ, Config),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages_total(Servers, RaName, 1),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})).

queue_length_limit_reject_publish(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    RaName = ra_name(QQ),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-length">>, long, 1},
                                  {<<"x-overflow">>, longstr, <<"reject-publish">>}])),

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    ok = publish_confirm(Ch, QQ),
    ok = publish_confirm(Ch, QQ),
    %% give the channel some time to process the async reject_publish notification
    %% now that we are over the limit it should start failing
    wait_for_messages_total(Servers, RaName, 2),
    fail = publish_confirm(Ch, QQ),
    %% remove all messages
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = _}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = _}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})),
    %% publish should be allowed again now
    ok = publish_confirm(Ch, QQ),
    ok.

queue_length_limit_policy_cleared(Config) ->
    check_quorum_queues_v4_compat(Config),

    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"max-length">>, QQ, <<"queues">>,
           [{<<"max-length">>, 2},
            {<<"overflow">>, <<"reject-publish">>}]),
    timer:sleep(1000),
    RaName = ra_name(QQ),
    QueryFun = fun rabbit_fifo:overview/1,
    ?awaitMatch({ok, {_, #{config := #{max_length := 2}}}, _},
                rpc:call(Server, ra, local_query, [RaName, QueryFun]),
                ?DEFAULT_AWAIT),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    ok = publish_confirm(Ch, QQ),
    ok = publish_confirm(Ch, QQ),
    ok = publish_confirm(Ch, QQ), %% QQs allow one message above the limit
    wait_for_messages_ready(Servers, RaName, 3),
    fail = publish_confirm(Ch, QQ),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"max-length">>),
    ?awaitMatch({ok, {_, #{config := #{max_length := undefined}}}, _},
                rpc:call(Server, ra, local_query, [RaName, QueryFun]),
                ?DEFAULT_AWAIT),
    ok = publish_confirm(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 4).

purge(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,

    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),

    {'queue.purge_ok', 2} = amqp_channel:call(Ch, #'queue.purge'{queue = QQ}),

    ?assertEqual([0], dirty_query([Server], RaName, fun rabbit_fifo:query_messages_total/1)).

peek(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 2}])),

    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,

    QName = rabbit_misc:r(<<"/">>, queue, QQ),
    ?assertMatch({error, no_message_at_pos},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              peek, [1, QName])),
    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),

    ?assertMatch({ok, [_|_]},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              peek, [1, QName])),
    ?assertMatch({ok, [_|_]},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              peek, [2, QName])),
    ?assertMatch({error, no_message_at_pos},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              peek, [3, QName])),

    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),
    ok.

oldest_entry_timestamp(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 2}])),

    Msg1 = <<"msg1">>,
    VHost = <<"%2F">>,
    ServerId = binary_to_atom(<<VHost/binary, "_", QQ/binary>>, utf8),

    ?assertMatch({ok, Ts} when is_integer(Ts),
                 rabbit_ct_broker_helpers:rpc(Config, 0, ra,
                                              aux_command,
                                              [ServerId, oldest_entry_timestamp])),
    publish(Ch, QQ, Msg1),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),

    ?assertMatch({ok, Ts} when is_integer(Ts),
                 rabbit_ct_broker_helpers:rpc(Config, 0, ra,
                                              aux_command,
                                              [ServerId, oldest_entry_timestamp])),
    ?assertMatch({ok, Ts} when is_integer(Ts),
                 rabbit_ct_broker_helpers:rpc(Config, 0, ra,
                                              aux_command,
                                              [ServerId, oldest_entry_timestamp])),

    {'queue.purge_ok', 1} = amqp_channel:call(Ch, #'queue.purge'{queue = QQ}),
    Now = erlang:system_time(millisecond),
    timer:sleep(100),
    ?assertMatch({ok, Ts2} when Ts2 > Now,
                 rabbit_ct_broker_helpers:rpc(Config, 0, ra,
                                              aux_command,
                                              [ServerId, oldest_entry_timestamp])),

    ok.

-define(STATUS_MATCH(N, T),
        [{<<"Node Name">>, N},
         {<<"Raft State">>, _},
         {<<"Membership">>, _},
         {<<"Last Log Index">>, _},
         {<<"Last Written">>, _},
         {<<"Last Applied">>, _},
         {<<"Commit Index">>, _},
         {<<"Snapshot Index">>, _},
         {<<"Term">>, T},
         {<<"Machine Version">>, _}
        ]).

status(Config) ->
    [Server | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 2}])),

    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,

    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),

    [N1, N2, N3] = lists:sort(Nodes),

    %% check that nodes are returned and that at least the term isn't
    %% defaulted (i.e. there was an error)
    ?assertMatch([?STATUS_MATCH(N1, T1),
                  ?STATUS_MATCH(N2, T2),
                  ?STATUS_MATCH(N3, T3)
                 ] when T1 /= <<>> andalso
                        T2 /= <<>> andalso
                        T3 /= <<>>,
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              status, [<<"/">>, QQ])),
    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),
    ok.

format(Config) ->
    %% tests rabbit_quorum_queue:format/2
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Server = case Nodes of
                 [N] ->
                     N;
                 [_, N | _] ->
                     N
             end,

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    Vhost = ?config(rmq_vhost, Config),
    QName = #resource{virtual_host = Vhost,
                      kind = queue,
                      name = Q},
    {ok, QRecord}  = rabbit_ct_broker_helpers:rpc(Config, Server,
                                                  rabbit_amqqueue,
                                                  lookup, [QName]),
    %% restart the quorum
    Fmt = rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_quorum_queue,
                                       ?FUNCTION_NAME, [QRecord, #{}]),

    %% test all up case
    ?assertMatch(
       T when T =:= <<"quorum">> orelse T =:= quorum,
       proplists:get_value(type, Fmt)),
    ?assertEqual(running, proplists:get_value(state, Fmt)),
    ?assertEqual(Server, proplists:get_value(leader, Fmt)),
    ?assertEqual(Server, proplists:get_value(node, Fmt)),
    ?assertEqual(Nodes, proplists:get_value(online, Fmt)),
    ?assertEqual(Nodes, proplists:get_value(members, Fmt)),

    case length(Nodes) of
        3 ->
            [Server1, _Server2, Server3] = Nodes,
            ok = rabbit_control_helper:command(stop_app, Server1),
            ok = rabbit_control_helper:command(stop_app, Server3),

            Fmt2 = rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_quorum_queue,
                                               ?FUNCTION_NAME, [QRecord, #{}]),
            ok = rabbit_control_helper:command(start_app, Server1),
            ok = rabbit_control_helper:command(start_app, Server3),
            ?assertMatch(
               T when T =:= <<"quorum">> orelse T =:= quorum,
               proplists:get_value(type, Fmt2)),
            ?assertEqual(minority, proplists:get_value(state, Fmt2)),
            ?assertEqual(Server, proplists:get_value(leader, Fmt2)),
            ?assertEqual(Server, proplists:get_value(node, Fmt2)),
            ?assertEqual([Server], proplists:get_value(online, Fmt2)),
            ?assertEqual(Nodes, proplists:get_value(members, Fmt2)),
            ok;
        1 ->
            ok
    end,
    ?assertMatch(#'queue.delete_ok'{},
                  amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
    ok.

peek_with_wrong_queue_type(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),

    CQName = rabbit_misc:r(<<"/">>, queue, CQ),
    %% legacy, special case for classic queues
    ?assertMatch({error, classic_queue_not_supported},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              peek, [1, CQName])),
    SQ = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', SQ, 0, 0},
                 declare(Ch, SQ, [{<<"x-queue-type">>, longstr, <<"stream">>}])),

    SQName = rabbit_misc:r(<<"/">>, queue, SQ),
    %% all other (future) queue types get the same error
    ?assertMatch({error, not_quorum_queue},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue,
                                              peek, [1, SQName])),
    ok.

message_ttl(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-message-ttl">>, long, 2000}])),

    %% Checking for messages that are short-lived could cause intermitent
    %% failures, as these could have expired before the check takes
    %% place. Thus, we're goig to split this testcase in two:
    %% 1. Check that messages published with ttl reach the queue
    %%    This can be easily achieved by having a consumer already
    %%    subscribed to the queue.
    %% 2. Check that messages published eventually disappear from the
    %%    queue.

    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,

    %% 1. Subscribe, publish and consume two messages
    subscribe(Ch, QQ, false),
    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"2">>, <<"0">>, <<"2">>]]),
    receive_and_ack(Ch),
    receive_and_ack(Ch),
    cancel(Ch),
    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),

    %% 2. Publish two messages and wait until queue is empty
    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    ok.

receive_and_ack(Ch) ->
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                               multiple = false})
    after ?TIMEOUT ->
              flush(1),
              ct:fail("receive_and_ack timed out", [])
    end.

message_ttl_policy(Config) ->
    %% Using ttl is very difficult to guarantee 100% test rate success, unless
    %% using really high ttl values. Previously, this test used 1s and 3s ttl,
    %% but expected to see first the messages in the queue and then the messages
    %% gone. A slow CI run, would fail the first assertion as the messages would
    %% have been dropped when the message count was performed. It happened
    %% from time to time making this test case flaky.

    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:set_policy(Config, 0, <<"msg-ttl">>,
                                             QQ, <<"queues">>,
                                             [{<<"message-ttl">>, 1000}]),
    VHost = <<"%2F">>,
    RaName = binary_to_atom(<<VHost/binary, "_", QQ/binary>>, utf8),

    ?awaitMatch({ok, #{machine := #{config := #{msg_ttl := 1000}}}, _},
                rpc:call(Server, ra, member_overview, [RaName]),
                ?DEFAULT_AWAIT),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,

    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),

    ok = rabbit_ct_broker_helpers:set_policy(Config, 0, <<"msg-ttl">>,
                                             QQ, <<"queues">>,
                                             [{<<"message-ttl">>, 1000}]),
    {ok, Overview2, _} = rpc:call(Server, ra, member_overview, [RaName]),
    ?assertMatch(#{machine := #{config := #{msg_ttl := 1000}}}, Overview2),
    publish(Ch, QQ, Msg1),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),
    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    ok.

per_message_ttl(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    Msg1 = <<"msg1">>,

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props = #'P_basic'{delivery_mode = 2,
                                                        expiration = <<"1000">>},
                                     payload = Msg1}),
    amqp_channel:wait_for_confirms(Ch, 5),
    %% we know the message got to the queue in 2s it should be gone
    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    ok.

per_message_ttl_mixed_expiry(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    Msg1 = <<"msg1">>,
    Msg2 = <<"msg2">>,

    %% message with no expiration
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props = #'P_basic'{delivery_mode = 2},
                                     payload = Msg1}),
    %% followed by message with expiration
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props = #'P_basic'{delivery_mode = 2,
                                                        expiration = <<"100">>},
                                     payload = Msg2}),


    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),
    %% twice the expiry interval
    timer:sleep(100 * 2),
    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag},
         #amqp_msg{payload = Msg1}} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                               multiple     = false})
    after ?TIMEOUT ->
              flush(10),
              ct:fail("basic deliver timeout")
    end,


    %% the second message should NOT be received as it has expired
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = Msg2}} ->
              flush(10),
              ct:fail("unexpected delivery")
    after 500 ->
              ok
    end,
    ok.

per_message_ttl_expiration_too_high(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    MonitorRef = erlang:monitor(process, Ch),

    ok =  amqp_channel:cast(Ch, #'basic.publish'{},
                            #amqp_msg{props = #'P_basic'{expiration = integer_to_binary(10*365*24*60*60*1000+1)}}),
    receive
        {'DOWN', MonitorRef, process, Ch,
         {shutdown, {server_initiated_close, 406, <<"PRECONDITION_FAILED - invalid expiration", _/binary>>}}} ->
            ok
    after ?TIMEOUT ->
              ct:fail("expected channel error")
    end.

consumer_metrics(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch1, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    subscribe(Ch1, QQ, false),

    RaName = ra_name(QQ),
    {ok, _, {_, Leader}} = ra:members({RaName, Server}),
    QNameRes = rabbit_misc:r(<<"/">>, queue, QQ),
    ?awaitMatch(1,
                begin
                    case rpc:call(Leader, ets, lookup, [queue_metrics, QNameRes]) of
                        [{_, PropList, _}] ->
                            proplists:get_value(consumers, PropList, undefined);
                        _ ->
                            undefined
                    end
                end,
                30000).

delete_if_empty(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, QQ),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),
    %% Try to delete the quorum queue
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
                amqp_channel:call(Ch, #'queue.delete'{queue = QQ,
                                                      if_empty = true})).

delete_if_unused(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    publish(Ch, QQ),
    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),
    %% Try to delete the quorum queue
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 540, _}}}, _},
                amqp_channel:call(Ch, #'queue.delete'{queue = QQ,
                                                      if_unused = true})).

queue_ttl(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),

    %% Set policy to 10 seconds.
    PolicyName = <<"my-queue-ttl-policy">>,
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, QQ, <<"queues">>,
           [{<<"expires">>, 10000}]),
    %% Set queue arg to 1 second.
    QArgs = [{<<"x-queue-type">>, longstr, <<"quorum">>},
             {<<"x-expires">>, long, 1000}],
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, QArgs)),
    %% The minimum should take effect.
    ?awaitMatch(
       {'EXIT', {{shutdown,
                  {server_initiated_close,404,
                   <<"NOT_FOUND - no queue 'queue_ttl' in vhost '/'">>}},
                 _}},
       catch amqp_channel:call(Ch, #'queue.declare'{
                                      queue = QQ,
                                      passive = true,
                                      durable = true,
                                      auto_delete = false,
                                      arguments = QArgs}),
       5_000),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName).

consumer_priorities(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    qos(Ch, 2, false),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% consumer with default priority
    Tag1 = <<"ctag1">>,
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QQ,
                                                no_ack = false,
                                                consumer_tag = Tag1},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = Tag1} ->
             ok
    end,
    %% consumer with higher priority
    Tag2 = <<"ctag2">>,
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QQ,
                                                arguments = [{"x-priority", long, 10}],
                                                no_ack = false,
                                                consumer_tag = Tag2},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = Tag2} ->
             ok
    end,

    publish(Ch, QQ),
    %% Tag2 should receive the message
    DT1 = receive
              {#'basic.deliver'{delivery_tag = D1,
                                consumer_tag = Tag2}, _} ->
                  D1
          after ?TIMEOUT ->
                    flush(100),
                    ct:fail("basic.deliver timeout")
          end,
    publish(Ch, QQ),
    %% Tag2 should receive the message
    receive
        {#'basic.deliver'{delivery_tag = _,
                          consumer_tag = Tag2}, _} ->
            ok
    after ?TIMEOUT ->
              flush(100),
              ct:fail("basic.deliver timeout")
    end,

    publish(Ch, QQ),
    %% Tag1 should receive the message as Tag2 has maxed qos
    receive
        {#'basic.deliver'{delivery_tag = _,
                          consumer_tag = Tag1}, _} ->
            ok
    after ?TIMEOUT ->
              flush(100),
              ct:fail("basic.deliver timeout")
    end,

    ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DT1,
                                        multiple = false}),
    publish(Ch, QQ),
    %% Tag2 should receive the message
    receive
        {#'basic.deliver'{delivery_tag = _,
                          consumer_tag = Tag2}, _} ->
            ok
    after ?TIMEOUT ->
              flush(100),
              ct:fail("basic.deliver timeout")
    end,

    ok.

cancel_consumer_gh_3729(Config) ->
    %% Test the scenario where a message is published to a quorum queue
    %% but the consumer has been cancelled
    %% https://github.com/rabbitmq/rabbitmq-server/pull/3746
    QQ = ?config(queue_name, Config),

    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    ExpectedDeclareRslt0 = #'queue.declare_ok'{queue = QQ, message_count = 0, consumer_count = 0},
    DeclareRslt0 = declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    ?assertMatch(ExpectedDeclareRslt0, DeclareRslt0),

    ok = publish(Ch, QQ),

    ok = subscribe(Ch, QQ, false),

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            R = #'basic.reject'{delivery_tag = DeliveryTag, requeue = true},
            ok = amqp_channel:cast(Ch, R)
    after ?TIMEOUT ->
        flush(100),
        ct:fail("basic.deliver timeout")
    end,

    ok = cancel(Ch),

    receive
        #'basic.cancel_ok'{consumer_tag = <<"ctag">>} -> ok
    after ?TIMEOUT ->
        flush(100),
        ct:fail("basic.cancel_ok timeout")
    end,

    D = #'queue.declare'{queue = QQ, passive = true,
                         arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]},

    F = fun() ->
            #'queue.declare_ok'{queue = QQ,
                                message_count = MC,
                                consumer_count = CC} = amqp_channel:call(Ch, D),
            ct:pal("Mc ~b CC ~b", [MC, CC]),
            MC =:= 1 andalso CC =:= 0
        end,
    rabbit_ct_helpers:await_condition(F, 30000),

    ok = rabbit_ct_client_helpers:close_channel(Ch).

cancel_consumer_gh_12424(Config) ->
    QQ = ?config(queue_name, Config),

    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    ExpectedDeclareRslt0 = #'queue.declare_ok'{queue = QQ, message_count = 0, consumer_count = 0},
    DeclareRslt0 = declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    ?assertMatch(ExpectedDeclareRslt0, DeclareRslt0),

    ok = publish(Ch, QQ),

    ok = subscribe(Ch, QQ, false),

    DeliveryTag = receive
                      {#'basic.deliver'{delivery_tag = DT}, _} ->
                          DT
                  after ?TIMEOUT ->
                            flush(100),
                            ct:fail("basic.deliver timeout")
                  end,

    ok = cancel(Ch),

    R = #'basic.reject'{delivery_tag = DeliveryTag, requeue = false},
    ok = amqp_channel:cast(Ch, R),
    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),

    ok.

    %% Test the scenario where a message is published to a quorum queue
cancel_and_consume_with_same_tag(Config) ->
    %% https://github.com/rabbitmq/rabbitmq-server/issues/5927
    QQ = ?config(queue_name, Config),

    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    ExpectedDeclareRslt0 = #'queue.declare_ok'{queue = QQ, message_count = 0, consumer_count = 0},
    DeclareRslt0 = declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    ?assertMatch(ExpectedDeclareRslt0, DeclareRslt0),

    ok = publish(Ch, QQ, <<"msg1">>),

    ok = subscribe(Ch, QQ, false),

    DeliveryTag = receive
                      {#'basic.deliver'{delivery_tag = D},
                       #amqp_msg{payload = <<"msg1">>}} ->
                          D
                  after ?TIMEOUT ->
                            flush(100),
                            ct:fail("basic.deliver timeout")
                  end,

    ok = cancel(Ch),

    ok = subscribe(Ch, QQ, false),

    ok = publish(Ch, QQ, <<"msg2">>),

    receive
        {#'basic.deliver'{delivery_tag = _},
         #amqp_msg{payload = <<"msg2">>}} ->
            ok
    after ?TIMEOUT ->
              flush(100),
              ct:fail("basic.deliver timeout 2")
    end,


    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                       multiple = true}),

    ok = cancel(Ch),



    ok.

validate_messages_on_queue(Config) ->
    QQ = ?config(queue_name, Config),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    #'queue.declare_ok'{} = declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    Messages = [begin
                    M = <<I:8000/integer>>,
                    publish(Ch, QQ, M),
                    M
                end || I <- lists:seq(1, 200)],
    amqp_channel:wait_for_confirms_or_die(Ch),
    validate_queue(Ch, QQ, Messages),

    ok.

amqpl_headers(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    Headers1Sent = undefined,
    Headers2Sent = [],
    [ok = amqp_channel:cast(
            Ch,
            #'basic.publish'{routing_key = QQ},
            #amqp_msg{props = #'P_basic'{headers = HeadersSent,
                                         delivery_mode = 2}}) ||
     HeadersSent <- [Headers1Sent, Headers2Sent]],
    RaName = ra_name(QQ),
    wait_for_messages_ready(Servers, RaName, 2),

    {#'basic.get_ok'{},
     #amqp_msg{props = #'P_basic'{headers = Headers1Received}}
    } = amqp_channel:call(Ch, #'basic.get'{queue = QQ}),

    {#'basic.get_ok'{delivery_tag = DeliveryTag},
     #amqp_msg{props = #'P_basic'{headers = Headers2Received}}
    } = amqp_channel:call(Ch, #'basic.get'{queue = QQ}),

    ?assertEqual(Headers1Sent, Headers1Received),
    ?assertEqual(Headers2Sent, Headers2Received),

    ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                            multiple = true}).

leader_health_check(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    %% check empty vhost
    ?assertEqual([],
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
            [<<".*">>, VHost1])),
    ?assertEqual([],
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
            [<<".*">>, across_all_vhosts])),

    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),

    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost2),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    Qs1 = [<<"Q.1">>, <<"Q.2">>, <<"Q.3">>],
    Qs2 = [<<"Q.4">>, <<"Q.5">>, <<"Q.6">>],

    %% in vhost1
    [?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch1, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}]))
        || Q <- Qs1],

    %% in vhost2
    [?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch2, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}]))
        || Q <- Qs2],

    %% test sucessful health checks in vhost1, vhost2, across_all_vhosts
    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<".*">>, VHost1])),
    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.*">>, VHost1])),
    [?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [Q, VHost1])) || Q <- Qs1],

    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<".*">>, VHost2])),
    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.*">>, VHost2])),
    [?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [Q, VHost2])) || Q <- Qs2],

    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<".*">>, across_all_vhosts])),
    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.*">>, across_all_vhosts])),

    %% clear leaderboard
    Qs = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list, []),

    [{_Q1_ClusterName, _Q1Res},
     {_Q2_ClusterName, _Q2Res},
     {_Q3_ClusterName, _Q3Res},
     {_Q4_ClusterName, _Q4Res},
     {_Q5_ClusterName, _Q5Res},
     {_Q6_ClusterName, _Q6Res}] = QQ_Clusters =
        lists:usort(
            [begin
                {ClusterName, _} = amqqueue:get_pid(Q),
                {ClusterName, amqqueue:get_name(Q)}
            end
                || Q <- Qs, amqqueue:get_type(Q) == rabbit_quorum_queue]),

    [Q1Data, Q2Data, Q3Data, Q4Data, Q5Data, Q6Data] = QQ_Data =
        [begin
            rabbit_ct_broker_helpers:rpc(Config, 0, ra_leaderboard, clear, [Q_ClusterName]),
            rabbit_ct_broker_helpers:rpc(Config, 0, amqqueue, to_printable, [Q_Res, rabbit_quorum_queue])
         end
            || {Q_ClusterName, Q_Res} <- QQ_Clusters],

    %% test failed health checks in vhost1, vhost2, across_all_vhosts
    ?assertEqual([Q1Data], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.1">>, VHost1])),
    ?assertEqual([Q2Data], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.2">>, VHost1])),
    ?assertEqual([Q3Data], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.3">>, VHost1])),
    ?assertEqual([Q1Data, Q2Data, Q3Data],
        lists:usort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                        [<<".*">>, VHost1]))),
    ?assertEqual([Q1Data, Q2Data, Q3Data],
        lists:usort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                        [<<"Q.*">>, VHost1]))),

    ?assertEqual([Q4Data], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.4">>, VHost2])),
    ?assertEqual([Q5Data], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.5">>, VHost2])),
    ?assertEqual([Q6Data], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                                      [<<"Q.6">>, VHost2])),
    ?assertEqual([Q4Data, Q5Data, Q6Data],
        lists:usort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                        [<<".*">>, VHost2]))),
    ?assertEqual([Q4Data, Q5Data, Q6Data],
        lists:usort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                        [<<"Q.*">>, VHost2]))),

    ?assertEqual(QQ_Data,
        lists:usort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                        [<<"Q.*">>, across_all_vhosts]))),
    ?assertEqual(QQ_Data,
        lists:usort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_quorum_queue, leader_health_check,
                        [<<"Q.*">>, across_all_vhosts]))),

    %% cleanup
    [?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch1, #'queue.delete'{queue = Q}))
        || Q <- Qs1],
    [?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch1, #'queue.delete'{queue = Q}))
        || Q <- Qs2],

    amqp_connection:close(Conn1),
    amqp_connection:close(Conn2).


leader_locator_client_local(Config) ->
    [Server1 | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = ?config(queue_name, Config),

    [begin
         Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
         ?assertEqual({'queue.declare_ok', Q, 0, 0},
                      declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                      {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
         ?assertMatch({ok, _, {_, Server = _Leader}},
                      ra:members({ra_name(Q), Server1})),
         ?assertMatch(#'queue.delete_ok'{},
                      amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
     end || Server <- Servers].

leader_locator_balanced(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Qs = [?config(queue_name, Config),
          ?config(alt_queue_name, Config),
          ?config(alt_2_queue_name, Config)],

    Leaders = [begin
                   ?assertMatch({'queue.declare_ok', Q, 0, 0},
                                declare(Ch, Q,
                                        [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                         {<<"x-queue-leader-locator">>, longstr, <<"balanced">>}])),
                   {ok, _, {_, Leader}} = ra:members({ra_name(Q), Server}),
                   Leader
               end || Q <- Qs],
    ?assertEqual(3, sets:size(sets:from_list(Leaders))),

    [?assertMatch(#'queue.delete_ok'{},
                  amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
     || Q <- Qs].

leader_locator_balanced_maintenance(Config) ->
    [S1, S2, S3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, S1),
    Qs = [?config(queue_name, Config),
          ?config(alt_queue_name, Config),
          ?config(alt_2_queue_name, Config)],

    true = rabbit_ct_broker_helpers:mark_as_being_drained(Config, S2),
    Leaders = [begin
                   ?assertMatch({'queue.declare_ok', Q, 0, 0},
                                declare(Ch, Q,
                                        [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                         {<<"x-queue-leader-locator">>, longstr, <<"balanced">>}])),
                   {ok, _, {_, Leader}} = ra:members({ra_name(Q), S1}),
                   Leader
               end || Q <- Qs],
    ?assert(lists:member(S1, Leaders)),
    ?assertNot(lists:member(S2, Leaders)),
    ?assert(lists:member(S3, Leaders)),

    true = rabbit_ct_broker_helpers:unmark_as_being_drained(Config, S2),
    [?assertMatch(#'queue.delete_ok'{},
                  amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
     || Q <- Qs].

leader_locator_balanced_random_maintenance(Config) ->
    [S1, S2, _S3] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, S1),
    Q = ?config(queue_name, Config),

    true = rabbit_ct_broker_helpers:mark_as_being_drained(Config, S2),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, queue_leader_locator, <<"balanced">>]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, queue_count_start_random_selection, 0]),

    Leaders = [begin
                   ?assertMatch({'queue.declare_ok', Q, 0, 0},
                                declare(Ch, Q,
                                        [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                         {<<"x-quorum-initial-group-size">>, long, 2}])),
                   {ok, [{_, R1}, {_, R2}], {_, Leader}} = ra:members({ra_name(Q), S1}),
                   ?assert(lists:member(R1, Servers)),
                   ?assert(lists:member(R2, Servers)),
                   ?assertMatch(#'queue.delete_ok'{},
                                amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
                   Leader
               end || _ <- lists:seq(1, 10)],

    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                      [rabbit, queue_leader_locator]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                      [rabbit, queue_count_start_random_selection]),
    true = rabbit_ct_broker_helpers:unmark_as_being_drained(Config, S2),
    %% assert after resetting maintenance mode else other tests may also fail
    ?assertNot(lists:member(S2, Leaders)),
    ok.

leader_locator_policy(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Qs = [?config(queue_name, Config),
          ?config(alt_queue_name, Config),
          ?config(alt_2_queue_name, Config)],
    PolicyName = <<"my-leader-locator">>,
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, <<"leader_locator_policy_.*">>, <<"queues">>,
           [{<<"queue-leader-locator">>, <<"balanced">>}]),

    Leaders = [begin
                   ?assertMatch({'queue.declare_ok', Q, 0, 0},
                                declare(Ch, Q,
                                        [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
                   {ok, _, {_, Leader}} = ra:members({ra_name(Q), Server}),
                   Leader
               end || Q <- Qs],

    [?assertMatch(#'queue.delete_ok'{},
                  amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
     || Q <- Qs],
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName),

    ?assertEqual(3, length(lists:usort(Leaders))),
    ok.

select_nodes_with_least_replicas(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Qs = [?config(queue_name, Config),
          ?config(alt_queue_name, Config)],
    Members = [begin
                   ?assertMatch({'queue.declare_ok', Q, 0, 0},
                                declare(Ch, Q,
                                        [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                         {<<"x-quorum-initial-group-size">>, long, 3}])),
                   {ok, Members0, _} = ra:members({ra_name(Q), Server}),
                   ?assertEqual(3, length(Members0)),
                   lists:map(fun({_, N}) -> N end, Members0)
               end || Q <- Qs],
    %% Assert that second queue selected the nodes where first queue does not have replicas.
    ?assertEqual(5, sets:size(sets:from_list(lists:flatten(Members)))),

    [?assertMatch(#'queue.delete_ok'{},
                  amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
     || Q <- Qs].

select_nodes_with_least_replicas_node_down(Config) ->
    [S1, S2 | _ ] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ?assertEqual(ok, rabbit_control_helper:command(stop_app, S2)),
    RunningNodes = lists:delete(S2, Servers),
    Ch = rabbit_ct_client_helpers:open_channel(Config, S1),
    Qs = [?config(queue_name, Config),
          ?config(alt_queue_name, Config)],

    Members = [begin
                   ?assertMatch({'queue.declare_ok', Q, 0, 0},
                                declare(Ch, Q,
                                        [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                         {<<"x-quorum-initial-group-size">>, long, 3}])),
                   {ok, Members0, _} = ra:members({ra_name(Q), S1}),
                   ?assertEqual(3, length(Members0)),
                   lists:map(fun({_, N}) -> N end, Members0)
               end || Q <- Qs],
    %% Assert that
    %% 1. no replicas got placed on a node which is down because there are sufficient running nodes, and
    %% 2. second queue selected the nodes where first queue does not have replicas.
    ?assert(same_elements(lists:flatten(Members), RunningNodes)),

    ?assertEqual(ok, rabbit_control_helper:command(start_app, S2)),
    [?assertMatch(#'queue.delete_ok'{},
                  amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
     || Q <- Qs].

requeue_multiple_true(Config) ->
    check_quorum_queues_v4_compat(Config),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-delivery-limit">>, long, 3}])),
    Num = 100,
    Payloads = [integer_to_binary(N) || N <- lists:seq(1, Num)],
    [publish(Ch, QQ, P) || P <- Payloads],

    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QQ}, self()),
    receive #'basic.consume_ok'{} -> ok
    end,

    DTags = [receive {#'basic.deliver'{redelivered = false,
                                       delivery_tag = D},
                      #amqp_msg{payload = P0}} ->
                         ?assertEqual(P, P0),
                         D
             after ?TIMEOUT -> ct:fail({basic_deliver_timeout, P, ?LINE})
             end || P <- Payloads],

    %% Requeue all messages.
    ok = amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = lists:last(DTags),
                                             multiple = true,
                                             requeue = true}),

    %% We expect to get all messages re-delivered in the order in which we requeued
    %% (which is the same order as messages were sent to us previously).
    [receive {#'basic.deliver'{redelivered = true},
              #amqp_msg{payload = P1}} ->
                 ?assertEqual(P, P1)
     after ?TIMEOUT -> ct:fail({basic_deliver_timeout, P, ?LINE})
     end || P <- Payloads],

    ?assertEqual(#'queue.delete_ok'{message_count = 0},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QQ})).

requeue_multiple_false(Config) ->
    check_quorum_queues_v4_compat(Config),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-delivery-limit">>, long, 3}])),
    Num = 100,
    Payloads = [integer_to_binary(N) || N <- lists:seq(1, Num)],
    [publish(Ch, QQ, P) || P <- Payloads],

    amqp_channel:subscribe(Ch, #'basic.consume'{queue = QQ}, self()),
    receive #'basic.consume_ok'{} -> ok
    end,

    DTags = [receive {#'basic.deliver'{redelivered = false,
                                       delivery_tag = D},
                      #amqp_msg{payload = P0}} ->
                         ?assertEqual(P, P0),
                         D
             after ?TIMEOUT -> ct:fail({basic_deliver_timeout, P, ?LINE})
             end || P <- Payloads],

    %% The delivery tags we received via AMQP 0.9.1 are ordered from 1-100.
    %% Sanity check:
    ?assertEqual(lists:seq(1, Num), DTags),

    %% Requeue each message individually in random order.
    Tuples = [{rand:uniform(), D} || D <- DTags],
    DTagsShuffled = [D || {_, D} <- lists:sort(Tuples)],
    [ok = amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = D,
                                              multiple = false,
                                              requeue = true})
     || D <- DTagsShuffled],

    %% We expect to get all messages re-delivered in the order in which we requeued.
    [receive {#'basic.deliver'{redelivered = true},
              #amqp_msg{payload = P1}} ->
                 ?assertEqual(integer_to_binary(D), P1)
     after ?TIMEOUT -> ct:fail({basic_deliver_timeout, ?LINE})
     end || D <- DTagsShuffled],

    ?assertEqual(#'queue.delete_ok'{message_count = 0},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QQ})).

replica_states(Config) ->
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),

    [?assertEqual({'queue.declare_ok', Q, 0, 0},
                 declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"quorum">>}]))
        || Q <- [<<"Q1">>, <<"Q2">>, <<"Q3">>]],

    Qs = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list, []),

    [Q1_ClusterName, Q2_ClusterName, Q3_ClusterName] =
        [begin
            {ClusterName, _} = amqqueue:get_pid(Q),
            ClusterName
         end
            || Q <- Qs, amqqueue:get_type(Q) == rabbit_quorum_queue],

    Result1 = rabbit_misc:append_rpc_all_nodes(Servers, rabbit_quorum_queue, all_replica_states, []),
    ct:pal("all replica states: ~tp", [Result1]),

    lists:map(fun({_Node, ReplicaStates}) ->
                ?assert(maps:is_key(Q1_ClusterName, ReplicaStates)),
                ?assert(maps:is_key(Q2_ClusterName, ReplicaStates)),
                ?assert(maps:is_key(Q3_ClusterName, ReplicaStates))
             end, Result1),

    %% Unregister a few queues (same outcome of 'noproc')
    rabbit_ct_broker_helpers:rpc(Config, Server, erlang, unregister, [Q2_ClusterName]),
    rabbit_ct_broker_helpers:rpc(Config, Server, erlang, unregister, [Q3_ClusterName]),

    ?assert(undefined == rabbit_ct_broker_helpers:rpc(Config, Server, erlang, whereis, [Q2_ClusterName])),
    ?assert(undefined == rabbit_ct_broker_helpers:rpc(Config, Server, erlang, whereis, [Q3_ClusterName])),

    Result2 = rabbit_misc:append_rpc_all_nodes(Servers, rabbit_quorum_queue, all_replica_states, []),
    ct:pal("replica states with a node missing Q1 and Q2: ~tp", [Result2]),

    lists:map(fun({Node, ReplicaStates}) ->
                if Node == Server ->
                    ?assert(maps:is_key(Q1_ClusterName, ReplicaStates)),
                    ?assertNot(maps:is_key(Q2_ClusterName, ReplicaStates)),
                    ?assertNot(maps:is_key(Q3_ClusterName, ReplicaStates));
                true ->
                    ?assert(maps:is_key(Q1_ClusterName, ReplicaStates)),
                    ?assert(maps:is_key(Q2_ClusterName, ReplicaStates)),
                    ?assert(maps:is_key(Q3_ClusterName, ReplicaStates))
                end
             end, Result2).

%%----------------------------------------------------------------------------

same_elements(L1, L2)
  when is_list(L1), is_list(L2) ->
    lists:usort(L1) =:= lists:usort(L2).

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

declare_passive(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue = Q,
                                           durable = true,
                                           auto_delete = false,
                                           passive = true,
                                           arguments = Args}).

set_up_vhost(Config, VHost) ->
    rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost).

assert_queue_type(Server, Q, Expected) ->
    assert_queue_type(Server, <<"/">>, Q, Expected).

assert_queue_type(Server, VHost, Q, Expected) ->
    Actual = get_queue_type(Server, VHost, Q),
    Expected = Actual.

get_queue_type(Server, VHost, Q0) ->
    QNameRes = rabbit_misc:r(VHost, queue, Q0),
    {ok, Q1} = rpc:call(Server, rabbit_amqqueue, lookup, [QNameRes]),
    amqqueue:get_type(Q1).

count_online_nodes(Server, VHost, Q0) ->
    QNameRes = rabbit_misc:r(VHost, queue, Q0),
    Info = rpc:call(Server, rabbit_quorum_queue, infos, [QNameRes, [online]]),
    length(proplists:get_value(online, Info, [])).

publish_many(Ch, Queue, Count) ->
    [publish(Ch, Queue) || _ <- lists:seq(1, Count)].

publish(Ch, Queue) ->
    publish(Ch, Queue, <<"msg">>).

publish(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).

basic_get_tag(Ch, Queue, NoAck) ->
    {GetOk, _} = Reply = amqp_channel:call(Ch, #'basic.get'{queue = Queue,
                                                            no_ack = NoAck}),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg">>}}, Reply),
    GetOk#'basic.get_ok'.delivery_tag.

consume_empty(Ch, Queue, NoAck) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Queue,
                                                    no_ack = NoAck})).

subscribe(Ch, Queue, NoAck) ->
    subscribe(Ch, Queue, NoAck, <<"ctag">>, []).

subscribe(Ch, Queue, NoAck, Tag, Args) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                arguments = Args,
                                                consumer_tag = Tag},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = Tag} ->
             ok
    after ?TIMEOUT ->
              flush(100),
              exit(subscribe_timeout)
    end.

qos(Ch, Prefetch, Global) ->
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = Global,
                                                    prefetch_count = Prefetch})).

cancel(Ch) ->
    ?assertMatch(#'basic.cancel_ok'{consumer_tag = <<"ctag">>},
                 amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = <<"ctag">>})).

receive_basic_deliver(Redelivered) ->
    receive
        {#'basic.deliver'{redelivered = R}, _} when R == Redelivered ->
            ok
    end.

nack(Ch, Multiple, Requeue) ->
    receive {#'basic.deliver'{delivery_tag = DeliveryTag}, #amqp_msg{}} ->
                amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                    multiple     = Multiple,
                                                    requeue      = Requeue})
    after ?TIMEOUT ->
              flush(10),
              ct:fail("basic deliver timeout")
    end.

wait_for_cleanup(Server, Channel, Number) ->
    wait_for_cleanup(Server, Channel, Number, 120).

wait_for_cleanup(Server, Channel, Number, 0) ->
    ?assertEqual(length(rpc:call(Server, rabbit_channel, list_queue_states, [Channel])),
                Number);
wait_for_cleanup(Server, Channel, Number, N) ->
    case length(rpc:call(Server, rabbit_channel, list_queue_states, [Channel])) of
        Length when Number == Length ->
            ok;
        _ ->
            timer:sleep(250),
            wait_for_cleanup(Server, Channel, Number, N - 1)
    end.

force_leader_change([Server | _] = Servers, Q) ->
    RaName = ra_name(Q),
    {ok, _, {_, Leader}} = ra:members({RaName, Server}),
    [F1, _] = Servers -- [Leader],
    ok = rpc:call(F1, ra, trigger_election, [{RaName, F1}]),
    case ra:members({RaName, Leader}) of
        {ok, _, {_, Leader}} ->
            %% Leader has been re-elected
            force_leader_change(Servers, Q);
        {ok, _, _} ->
            %% Leader has changed
            ok
    end.

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

stop_node(Config, Server) ->
    rabbit_ct_broker_helpers:rabbitmqctl(Config, Server, ["stop"]).

get_message_bytes(Leader, QRes) ->
    case rpc:call(Leader, ets, lookup, [queue_metrics, QRes]) of
        [{QRes, Props, _}] ->
            {proplists:get_value(message_bytes, Props),
             proplists:get_value(message_bytes_ready, Props),
             proplists:get_value(message_bytes_unacknowledged, Props)};
        _ ->
            []
    end.

wait_for_consensus(Name, Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    RaName = ra_name(Name),
    {ok, _, _} = ra:members({RaName, Server}).

queue_names(Records) ->
    [begin
         #resource{name = Name} = amqqueue:get_name(Q),
         Name
     end || Q <- Records].


validate_queue(Ch, Queue, ExpectedMsgs) ->
    qos(Ch, length(ExpectedMsgs), false),
    subscribe(Ch, Queue, false),
    [begin
         receive
             {#'basic.deliver'{delivery_tag = DeliveryTag1,
                               redelivered = false},
              #amqp_msg{payload = M}} ->
                 amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1,
                                                    multiple = false})
         after ?TIMEOUT ->
                   flush(10),
                   exit({validate_queue_timeout, M})
         end
     end || M <- ExpectedMsgs],
    ok.

basic_get(_, _, _, 0) ->
    empty;
basic_get(Ch, Q, NoAck, Attempt) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = Q, no_ack = NoAck}) of
        {#'basic.get_ok'{}, #amqp_msg{}} = R ->
            R;
        _ ->
            timer:sleep(100),
            basic_get(Ch, Q, NoAck, Attempt - 1)
    end.

check_quorum_queues_v4_compat(Config) ->
    case rabbit_ct_broker_helpers:is_feature_flag_enabled(Config, 'rabbitmq_4.0.0') of
        true ->
            ok;
        false ->
            throw({skip, "test needs feature flag rabbitmq_4.0.0"})
    end.

lists_interleave([], _List) ->
    [];
lists_interleave([Item | Items], List)
  when is_list(List) ->
    {Left, Right} = lists:split(2, List),
    Left ++ [Item | lists_interleave(Items, Right)].

publish_confirm_many(Ch, Queue, Count) ->
    lists:foreach(fun(_) -> publish_confirm(Ch, Queue) end, lists:seq(1, Count)).

consume_all(Ch, QQ) ->
    Consume = fun C(Acc) ->
        case amqp_channel:call(Ch, #'basic.get'{queue = QQ}) of
            {#'basic.get_ok'{}, Msg} ->
                C([Msg | Acc]);
            _ ->
                Acc
        end
    end,
    Consume([]).

ensure_qq_proc_dead(Config, Server, RaName) ->
    case rabbit_ct_broker_helpers:rpc(Config, Server, erlang, whereis, [RaName]) of
        undefined ->
            ok;
        Pid ->
            rabbit_ct_broker_helpers:rpc(Config, Server, erlang, exit, [Pid, kill]),
            %% Give some time for the supervisor to restart the process
            timer:sleep(500),
            ensure_qq_proc_dead(Config, Server, RaName)
    end.

lsof_rpc() ->
    Cmd = rabbit_misc:format(
            "lsof -p ~ts", [os:getpid()]),
    os:cmd(Cmd).
