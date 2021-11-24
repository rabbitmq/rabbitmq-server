%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(quorum_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(quorum_queue_utils, [wait_for_messages_ready/3,
                             wait_for_messages_pending_ack/3,
                             wait_for_messages_total/3,
                             wait_for_messages/2,
                             dirty_query/3,
                             ra_name/1]).

-compile([nowarn_export_all, export_all]).
-compile(export_all).

-define(DEFAULT_AWAIT, 10000).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
      {group, single_node},
      {group, unclustered},
      {group, clustered}
    ].

groups() ->
    [
     {single_node, [], all_tests()
                       ++ memory_tests()
                       ++ [node_removal_is_quorum_critical]},
     {unclustered, [], [
                        {uncluster_size_2, [], [add_member]}
                       ]},
     {clustered, [], [
                      {cluster_size_2, [], [add_member_not_running,
                                            add_member_classic,
                                            add_member_already_a_member,
                                            add_member_not_found,
                                            delete_member_not_running,
                                            delete_member_classic,
                                            delete_member_queue_not_found,
                                            delete_member,
                                            delete_member_not_a_member,
                                            node_removal_is_quorum_critical,
                                            cleanup_data_dir]
                       ++ memory_tests()},
                      {cluster_size_3, [], [
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
                                            shrink_all,
                                            rebalance,
                                            file_handle_reservations,
                                            file_handle_reservations_above_limit,
                                            node_removal_is_not_quorum_critical
                                            ]
                       ++ all_tests()},
                      {cluster_size_5, [], [start_queue,
                                            start_queue_concurrent,
                                            quorum_cluster_size_3,
                                            quorum_cluster_size_7,
                                            node_removal_is_not_quorum_critical
                                           ]},
                      {clustered_with_partitions, [], [
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
     vhost_with_quorum_queue_is_deleted,
     delete_immediately_by_resource,
     consume_redelivery_count,
     subscribe_redelivery_count,
     message_bytes_metrics,
     queue_length_limit_drop_head,
     queue_length_limit_reject_publish,
     subscribe_redelivery_limit,
     subscribe_redelivery_policy,
     subscribe_redelivery_limit_with_dead_letter,
     queue_length_in_memory_limit_basic_get,
     queue_length_in_memory_limit_subscribe,
     queue_length_in_memory_limit,
     queue_length_in_memory_limit_returns,
     queue_length_in_memory_bytes_limit_basic_get,
     queue_length_in_memory_bytes_limit_subscribe,
     queue_length_in_memory_bytes_limit,
     queue_length_in_memory_purge,
     in_memory,
     consumer_metrics,
     invalid_policy,
     delete_if_empty,
     delete_if_unused,
     queue_ttl,
     peek,
     consumer_priorities,
     cancel_consumer_gh_3729
    ].

memory_tests() ->
    [
     memory_alarm_rolls_wal
    ].

-define(SUPNAME, ra_server_sup_sup).
%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000}]}),
    rabbit_ct_helpers:merge_app_env(
      Config1, {aten, [{poll_interval, 1000}]}),
    rabbit_ct_helpers:run_setup_steps(Config1, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(unclustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]);
init_per_group(clustered_with_partitions, Config0) ->
    case rabbit_ct_helpers:is_mixed_versions(Config0) of
        true ->
            {skip, "clustered_with_partitions is too unreliable in mixed mode"};
        false ->
            Config = rabbit_ct_helpers:run_setup_steps(
                       Config0,
                       [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]),
            rabbit_ct_helpers:set_config(Config, [{net_ticktime, 10}])
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
                                                    {tcp_ports_base}]),
            Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
            Ret = rabbit_ct_helpers:run_steps(Config1b,
                                              [fun merge_app_env/1 ] ++
                                              rabbit_ct_broker_helpers:setup_steps()),
            case Ret of
                {skip, _} ->
                    Ret;
                Config2 ->
                    EnableFF = rabbit_ct_broker_helpers:enable_feature_flag(
                                 Config2, quorum_queue),
                    case EnableFF of
                        ok ->
                            ok = rabbit_ct_broker_helpers:rpc(
                                   Config2, 0, application, set_env,
                                   [rabbit, channel_tick_interval, 100]),
                            %% HACK: the larger cluster sizes benefit for a bit
                            %% more time after clustering before running the
                            %% tests.
                            timer:sleep(ClusterSize * 1000),
                            Config2;
                        Skip ->
                            end_per_group(Group, Config2),
                            Skip
                    end
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

init_per_testcase(node_removal_is_not_quorum_critical, _) ->
    {skip, "testcase is not mixed versions compatible"};
init_per_testcase(Testcase, Config) when Testcase == reconnect_consumer_and_publish;
                                         Testcase == reconnect_consumer_and_wait;
                                         Testcase == reconnect_consumer_and_wait_channel_down ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{rmq_nodes_count, 3},
                                            {rmq_nodename_suffix, Testcase},
                                            {tcp_ports_base},
                                            {queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    Ret = rabbit_ct_helpers:run_steps(
            Config2,
            rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps()),
    case Ret of
        {skip, _} ->
            Ret;
        Config3 ->
            EnableFF = rabbit_ct_broker_helpers:enable_feature_flag(
                         Config3, quorum_queue),
            case EnableFF of
                ok ->
                    Config3;
                Skip ->
                    end_per_testcase(Testcase, Config3),
                    Skip
            end
    end;
init_per_testcase(Testcase, Config) ->
    ClusterSize = ?config(rmq_nodes_count, Config),
    IsMixed = rabbit_ct_helpers:is_mixed_versions(Config),
    case Testcase of
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
        _ ->
            Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
            rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
            Q = rabbit_data_coercion:to_binary(Testcase),
            Config2 = rabbit_ct_helpers:set_config(Config1,
                                                   [{queue_name, Q},
                                                    {alt_queue_name, <<Q/binary, "_alt">>}
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
                                timer:sleep(500),
                                rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
                                Self ! {done, Server}
                        end)
     end || Server <- Servers],

    [begin
         receive {done, Server} -> ok
         after 10000 -> exit({await_done_timeout, Server})
         end
     end || Server <- Servers],


    ok.

quorum_cluster_size_3(Config) ->
    case rabbit_ct_helpers:is_mixed_versions(Config) of
        true ->
            {skip, "quorum_cluster_size_3 tests isn't mixed version reliable"};
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
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))),
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
        end),

    ?assertEqual(ok, rabbit_control_helper:command(start_app, Server)),

    %% Check that the application and two ra nodes are up
    rabbit_ct_helpers:await_condition(
        fun() ->
            Apps = rpc:call(Server, application, which_applications, []),
            case lists:keyfind(ra, 1, Apps) of
                false      -> false;
                {ra, _, _} -> true
            end
        end),
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
    ct:pal("waiting for confirms from ~s", [QName]),
    receive
        #'basic.ack'{} ->
            ct:pal("CONFIRMED! ~s", [QName]),
            ok;
        #'basic.nack'{} ->
            ct:pal("NOT CONFIRMED! ~s", [QName]),
            fail
    after Timeout ->
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
    [Server0, Server1, Server2] =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),

    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                   no_ack = false})),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
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
    case rabbit_ct_helpers:is_mixed_versions(Config) of
        true ->
            {skip, "rebalance tests isn't mixed version compatible"};
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
        ct:pal("subscribe_should_fail_when_global_qos_true caught an error: ~p", [Err])
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
    DeliveryTag = consume(Ch, Source, false),
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
            _ = consume(Ch, Destination, true);
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
           Config, 0, <<"ha">>, <<"invalid_policy.*">>, <<"queues">>,
           [{<<"ha-mode">>, <<"all">>}]),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"ttl">>, <<"invalid_policy.*">>, <<"queues">>,
           [{<<"message-ttl">>, 5}]),
    Info = rpc:call(Server, rabbit_quorum_queue, infos,
                    [rabbit_misc:r(<<"/">>, queue, QQ)]),
    ?assertEqual('', proplists:get_value(policy, Info)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ha">>),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl">>).

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
    DeliveryTag = consume(Ch, QQ, false),
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
    _ = consume(Ch, QQ2, false).

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
    DeliveryTag = consume(Ch, CQ, false),
    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"0">>, <<"1">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    _ = consume(Ch, QQ, false),
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
    ct:pal ("Res ~p", [Res]),
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
    wait_until(fun() ->
                       Children == length(rpc:call(Server, supervisor, which_children,
                                                   [?SUPNAME]))
               end),
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
    wait_until(fun() ->
                       Children == length(rpc:call(Server, supervisor, which_children, [?SUPNAME]))
               end),
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
    [Server, Server1, Server2] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    RaName = ra_name(QQ),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),

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
    %% publishing to an unavialable queue but with a reachable member should result
    %% in the initial enqueuer session timing out and the message being nacked
    [Server, Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    TCh = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(TCh, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),

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
                   exit(confirm_timeout)
         end,
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    timer:sleep(2000),
    publish_many(Ch, QQ, 1),
    %% this should now be acked
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 90000 ->
                   exit(confirm_timeout)
         end,
    %% check we get at least on ack
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
    ok.

leadership_takeover(Config) ->
    %% Kill nodes in succession forcing the takeover of leadership, and all messages that
    %% are in the queue.
    [Server, Server1, Server2] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    RaName = ra_name(QQ),

    publish(Ch, QQ),
    publish(Ch, QQ),
    publish(Ch, QQ),

    wait_for_messages_ready([Server], RaName, 3),
    wait_for_messages_pending_ack([Server], RaName, 0),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),

    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server),

    wait_for_messages_ready([Server2, Server], RaName, 3),
    wait_for_messages_pending_ack([Server2, Server], RaName, 0),

    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    wait_for_messages_ready(Servers, RaName, 3),
    wait_for_messages_pending_ack(Servers, RaName, 0).

metrics_cleanup_on_leadership_takeover(Config) ->
    case rabbit_ct_helpers:is_mixed_versions(Config) of
        true ->
            {skip, "metrics_cleanup_on_leadership_takeover tests isn't mixed version compatible"};
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
    wait_until(
      fun() ->
              case rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]) of
                  [{QRes, 3, 0, 3, _}] -> true;
                  _ -> false
              end
      end),
    force_leader_change(Servers, QQ),
    wait_until(fun () ->
                       [] =:= rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]) andalso
                       [] =:= rpc:call(Leader, ets, lookup, [queue_metrics, QRes])
               end),
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

    wait_for_messages_ready([Server], RaName, 3),
    wait_for_messages_pending_ack([Server], RaName, 0),
    {ok, _, {Name, Leader}} = ra:members({RaName, Server}),
    QRes = rabbit_misc:r(<<"/">>, queue, QQ),
    wait_until(
      fun() ->
              case rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes]) of
                  [{QRes, 3, 0, 3, _}] -> true;
                  _ -> false
              end
      end),
    Pid = rpc:call(Leader, erlang, whereis, [Name]),
    rpc:call(Leader, erlang, exit, [Pid, kill]),
    [Other | _] = lists:delete(Leader, Servers),
    catch ra:trigger_election(Other),
    %% kill it again just in case it came straight back up again
    catch rpc:call(Leader, erlang, exit, [Pid, kill]),

    %% this isn't a reliable test as the leader can be restarted so quickly
    %% after a crash it is elected leader of the next term as well.
    wait_until(
      fun() ->
              [] == rpc:call(Leader, ets, lookup, [queue_coarse_metrics, QRes])
      end),
    ok.


delete_declare(Config) ->
    case rabbit_ct_helpers:is_mixed_versions(Config) of
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
    case rabbit_ct_helpers:is_mixed_versions(Config) of
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

declare_during_node_down(Config) ->
    [Server, DownServer, _] = Servers = rabbit_ct_broker_helpers:get_node_configs(
                                    Config, nodename),

    stop_node(Config, DownServer),
    % rabbit_ct_broker_helpers:stop_node(Config, DownServer),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    timer:sleep(2000),
    rabbit_ct_broker_helpers:start_node(Config, DownServer),
    publish(Ch, QQ),
    wait_for_messages_ready(Servers, RaName, 1),
    ok.

simple_confirm_availability_on_leader_change(Config) ->
    [Node1, Node2, _Node3] =
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
    %% this should not fail as the channel should detect the new leader and
    %% resend to that
    ok = publish_confirm(Ch, QQ),
    ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
    ok.

confirm_availability_on_leader_change(Config) ->
    [Node1, Node2, _Node3] =
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

    timer:sleep(500),
    %% stop the node hosting the leader
    stop_node(Config, Node2),
    %% this should not fail as the channel should detect the new leader and
    %% resend to that
    timer:sleep(500),
    Publisher ! {done, self()},
    receive
        publisher_done ->
            ok;
        {'EXIT', Publisher, Err} ->
            ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
            exit(Err)
    after 30000 ->
              ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
              flush(100),
              exit(nothing_received_from_publisher_process)
    end,
    ok = rabbit_ct_broker_helpers:start_node(Config, Node2),
    ok.

flush(T) ->
    receive X ->
                ct:pal("flushed ~w", [X]),
                flush(T)
    after T ->
              ok
    end.


add_member_not_running(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ct:pal("add_member_not_running config ~p", [Config]),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({error, node_not_running},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, 'rabbit@burrow', 5000])).

add_member_classic(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, CQ, Server, 5000])).

add_member_already_a_member(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    %% idempotent by design
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server, 5000])).

add_member_not_found(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    QQ = ?config(queue_name, Config),
    ?assertEqual({error, not_found},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server, 5000])).

add_member(Config) ->
    [Server0, Server1] = Servers0 =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({error, node_not_running},
                 rpc:call(Server0, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server1, 5000])),
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),
    ?assertEqual(ok, rpc:call(Server0, rabbit_quorum_queue, add_member,
                              [<<"/">>, QQ, Server1, 5000])),
    Info = rpc:call(Server0, rabbit_quorum_queue, infos,
                    [rabbit_misc:r(<<"/">>, queue, QQ)]),
    Servers = lists:sort(Servers0),
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

delete_member_queue_not_found(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    QQ = ?config(queue_name, Config),
    ?assertEqual({error, not_found},
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])).

delete_member(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    timer:sleep(100),
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])).

delete_member_not_a_member(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    timer:sleep(100),
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])),
    %% idempotent by design
    ?assertEqual(ok,
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])).

delete_member_during_node_down(Config) ->
    [Server, DownServer, Remove] = rabbit_ct_broker_helpers:get_node_configs(
                                    Config, nodename),

    stop_node(Config, DownServer),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    timer:sleep(200),
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
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    timer:sleep(100),
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
    timer:sleep(100),
    Qs = rpc:call(Server, rabbit_quorum_queue, list_with_minimum_quorum, []),
    ?assertEqual([], Qs).


file_handle_reservations(Config) ->
    case rabbit_ct_helpers:is_mixed_versions(Config) of
        true ->
            {skip, "file_handle_reservations tests isn't mixed version compatible"};
        false ->
            file_handle_reservations0(Config)
    end.

file_handle_reservations0(Config) ->
    Servers = [Server1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    RaName = ra_name(QQ),
    {ok, _, {_, Leader}} = ra:members({RaName, Server1}),
    [Follower1, Follower2] = Servers -- [Leader],
    ?assertEqual([{files_reserved, 5}],
                 rpc:call(Leader, file_handle_cache, info, [[files_reserved]])),
    ?assertEqual([{files_reserved, 2}],
                 rpc:call(Follower1, file_handle_cache, info, [[files_reserved]])),
    ?assertEqual([{files_reserved, 2}],
                 rpc:call(Follower2, file_handle_cache, info, [[files_reserved]])),
    force_leader_change(Servers, QQ),
    {ok, _, {_, Leader0}} = ra:members({RaName, Server1}),
    [Follower01, Follower02] = Servers -- [Leader0],
    ?assertEqual([{files_reserved, 5}],
                 rpc:call(Leader0, file_handle_cache, info, [[files_reserved]])),
    ?assertEqual([{files_reserved, 2}],
                 rpc:call(Follower01, file_handle_cache, info, [[files_reserved]])),
    ?assertEqual([{files_reserved, 2}],
                 rpc:call(Follower02, file_handle_cache, info, [[files_reserved]])).

file_handle_reservations_above_limit(Config) ->
    [S1, S2, S3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, S1),
    QQ = ?config(queue_name, Config),
    QQ2 = ?config(alt_queue_name, Config),

    Limit = rpc:call(S1, file_handle_cache, get_limit, []),

    ok = rpc:call(S1, file_handle_cache, set_limit, [3]),
    ok = rpc:call(S2, file_handle_cache, set_limit, [3]),
    ok = rpc:call(S3, file_handle_cache, set_limit, [3]),

    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({'queue.declare_ok', QQ2, 0, 0},
                 declare(Ch, QQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rpc:call(S1, file_handle_cache, set_limit, [Limit]),
    ok = rpc:call(S2, file_handle_cache, set_limit, [Limit]),
    ok = rpc:call(S3, file_handle_cache, set_limit, [Limit]).

cleanup_data_dir(Config) ->
    %% This test is slow, but also checks that we handle properly errors when
    %% trying to delete a queue in minority. A case clause there had gone
    %% previously unnoticed.

    [Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    timer:sleep(100),

    UId1 = proplists:get_value(ra_name(QQ), rpc:call(Server1, ra_directory, list_registered, [quorum_queues])),
    UId2 = proplists:get_value(ra_name(QQ), rpc:call(Server2, ra_directory, list_registered, [quorum_queues])),
    DataDir1 = rpc:call(Server1, ra_env, server_data_dir, [quorum_queues, UId1]),
    DataDir2 = rpc:call(Server2, ra_env, server_data_dir, [quorum_queues, UId2]),
    ?assert(filelib:is_dir(DataDir1)),
    ?assert(filelib:is_dir(DataDir2)),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),

    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QQ})),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    %% data dir 1 should be force deleted at this point
    ?assert(not filelib:is_dir(DataDir1)),
    ?assert(filelib:is_dir(DataDir2)),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
    timer:sleep(2000),

    ?assertEqual(ok,
                 rpc:call(Server2, rabbit_quorum_queue, cleanup_data_dir, [])),
    ?assert(not filelib:is_dir(DataDir2)),
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
    timer:sleep(5000),
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
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

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
                                                multiple     = false,
                                                requeue      = true})
    after 5000 ->
              exit(basic_deliver_timeout)
    end,

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    after 5000 ->
              exit(basic_deliver_timeout_2)
    end,

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H2}}} ->
            ?assertMatch({DCHeader, _, 2}, rabbit_basic:header(DCHeader, H2)),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag2,
                                               multiple     = false}),
            ct:pal("wait_for_messages_ready", []),
            wait_for_messages_ready(Servers, RaName, 0),
            ct:pal("wait_for_messages_pending_ack", []),
            wait_for_messages_pending_ack(Servers, RaName, 0)
    after 5000 ->
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
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    receive
        {#'basic.deliver'{redelivered  = true}, #amqp_msg{}} ->
            throw(unexpected_redelivery)
    after 2000 ->
            ok
    end.

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
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[QQ, <<"0">>, <<"0">>, <<"0">>]]),
    receive
        {#'basic.deliver'{redelivered  = true}, #amqp_msg{}} ->
            throw(unexpected_redelivery)
    after 2000 ->
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
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
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
    ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H0)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    %% wait for requeuing
    timer:sleep(500),

    {#'basic.get_ok'{delivery_tag = DeliveryTag1,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H1}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                        multiple     = false,
                                        requeue      = true}),

    {#'basic.get_ok'{delivery_tag = DeliveryTag2,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H2}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 2}, rabbit_basic:header(DCHeader, H2)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag2,
                                        multiple     = false,
                                        requeue      = true}),
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
    wait_until(fun() ->
                       {3, 3, 0} == get_message_bytes(Leader, QRes)
               end),

    subscribe(Ch, QQ, false),

    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 1),
    wait_until(fun() ->
                       {3, 0, 3} == get_message_bytes(Leader, QRes)
               end),

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = false}),
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 0),
            wait_until(fun() ->
                               {0, 0, 0} == get_message_bytes(Leader, QRes)
                       end)
    end,

    %% Let's publish and then close the consumer channel. Messages must be
    %% returned to the queue
    publish(Ch, QQ),

    wait_for_messages_ready(Servers, RaName, 0),
    wait_for_messages_pending_ack(Servers, RaName, 1),
    wait_until(fun() ->
                       {3, 0, 3} == get_message_bytes(Leader, QRes)
               end),

    rabbit_ct_client_helpers:close_channel(Ch),

    wait_for_messages_ready(Servers, RaName, 1),
    wait_for_messages_pending_ack(Servers, RaName, 0),
    wait_until(fun() ->
                       {3, 3, 0} == get_message_bytes(Leader, QRes)
               end),
    ok.

memory_alarm_rolls_wal(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    #{wal_data_dir := WalDataDir} = ra_system:fetch(quorum_queues, Server),
    [Wal0] = filelib:wildcard(WalDataDir ++ "/*.wal"),
    rabbit_ct_broker_helpers:set_alarm(Config, Server, memory),
    rabbit_ct_helpers:await_condition(
        fun() -> rabbit_ct_broker_helpers:get_alarms(Config, Server) =/= [] end
    ),
    timer:sleep(1000),
    [Wal1] = filelib:wildcard(WalDataDir ++ "/*.wal"),
    ?assert(Wal0 =/= Wal1),
    %% roll over shouldn't happen if we trigger a new alarm in less than
    %% min_wal_roll_over_interval
    rabbit_ct_broker_helpers:set_alarm(Config, Server, memory),
    rabbit_ct_helpers:await_condition(
        fun() -> rabbit_ct_broker_helpers:get_alarms(Config, Server) =/= [] end
    ),
    timer:sleep(1000),
    [Wal2] = filelib:wildcard(WalDataDir ++ "/*.wal"),
    ?assert(Wal1 == Wal2),
    ok = rpc:call(Server, rabbit_alarm, clear_alarm,
                  [{{resource_limit, memory, Server}, []}]),
    timer:sleep(1000),
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

queue_length_in_memory_limit_basic_get(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 1}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg1}),
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = <<"msg2">>}),

    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),

    ?assertEqual([{1, byte_size(Msg1)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})).

queue_length_in_memory_limit_subscribe(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 1}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,
    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),

    ?assertEqual([{1, byte_size(Msg1)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = false},
         #amqp_msg{payload = Msg1}} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1,
                                               multiple     = false})
    end,
    ?assertEqual([{0, 0}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2,
                          redelivered  = false},
         #amqp_msg{payload = Msg2}} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag2,
                                               multiple     = false})
    end.

queue_length_in_memory_limit(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 2}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,
    Msg3 = <<"msg111">>,
    Msg4 = <<"msg1111">>,
    Msg5 = <<"msg1111">>,


    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    publish(Ch, QQ, Msg3),
    wait_for_messages(Config, [[QQ, <<"3">>, <<"3">>, <<"0">>]]),

    ?assertEqual([{2, byte_size(Msg1) + byte_size(Msg2)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})),

    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),
    publish(Ch, QQ, Msg4),
    wait_for_messages(Config, [[QQ, <<"3">>, <<"3">>, <<"0">>]]),

    ?assertEqual([{2, byte_size(Msg2) + byte_size(Msg4)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),
    publish(Ch, QQ, Msg5),
    wait_for_messages(Config, [[QQ, <<"4">>, <<"4">>, <<"0">>]]),
    ExpectedMsgs = [Msg2, Msg3, Msg4, Msg5],
    validate_queue(Ch, QQ, ExpectedMsgs),
    ok.

queue_length_in_memory_limit_returns(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 2}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,
    Msg3 = <<"msg111">>,
    Msg4 = <<"msg111">>,
    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),

    ?assertEqual([{2, byte_size(Msg1) + byte_size(Msg2)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = false})),

    {#'basic.get_ok'{delivery_tag = DTag2}, #amqp_msg{payload = Msg2}} =
        amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                           no_ack = false}),

    publish(Ch, QQ, Msg3),
    publish(Ch, QQ, Msg4),

    %% Ensure that returns are subject to in memory limits too
    wait_for_messages(Config, [[QQ, <<"4">>, <<"2">>, <<"2">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DTag2,
                                        multiple     = true,
                                        requeue      = true}),
    wait_for_messages(Config, [[QQ, <<"4">>, <<"4">>, <<"0">>]]),

    ?assertEqual([{2, byte_size(Msg3) + byte_size(Msg4)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)).

queue_length_in_memory_bytes_limit_basic_get(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-bytes">>, long, 6}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg1}),
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QQ},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = <<"msg2">>}),

    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),

    ?assertEqual([{1, byte_size(Msg1)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg2">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})).

queue_length_in_memory_bytes_limit_subscribe(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-bytes">>, long, 6}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,
    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),

    ?assertEqual([{1, byte_size(Msg1)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    subscribe(Ch, QQ, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = false},
         #amqp_msg{payload = Msg1}} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1,
                                               multiple     = false})
    end,
    ?assertEqual([{0, 0}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2,
                          redelivered  = false},
         #amqp_msg{payload = Msg2}} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag2,
                                               multiple     = false})
    end.

queue_length_in_memory_bytes_limit(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-bytes">>, long, 12}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,
    Msg3 = <<"msg111">>,
    Msg4 = <<"msg1111">>,

    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    publish(Ch, QQ, Msg3),
    wait_for_messages(Config, [[QQ, <<"3">>, <<"3">>, <<"0">>]]),

    ?assertEqual([{2, byte_size(Msg1) + byte_size(Msg2)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QQ,
                                                    no_ack = true})),

    wait_for_messages(Config, [[QQ, <<"2">>, <<"2">>, <<"0">>]]),
    publish(Ch, QQ, Msg4),
    wait_for_messages(Config, [[QQ, <<"3">>, <<"3">>, <<"0">>]]),

    ?assertEqual([{2, byte_size(Msg2) + byte_size(Msg4)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)).

queue_length_in_memory_purge(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-max-in-memory-length">>, long, 2}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,
    Msg3 = <<"msg111">>,

    publish(Ch, QQ, Msg1),
    publish(Ch, QQ, Msg2),
    publish(Ch, QQ, Msg3),
    wait_for_messages(Config, [[QQ, <<"3">>, <<"3">>, <<"0">>]]),

    ?assertEqual([{2, byte_size(Msg1) + byte_size(Msg2)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    {'queue.purge_ok', 3} = amqp_channel:call(Ch, #'queue.purge'{queue = QQ}),

    ?assertEqual([{0, 0}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)).

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

in_memory(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    RaName = ra_name(QQ),
    Msg1 = <<"msg1">>,
    Msg2 = <<"msg11">>,

    publish(Ch, QQ, Msg1),

    wait_for_messages(Config, [[QQ, <<"1">>, <<"1">>, <<"0">>]]),
    ?assertEqual([{1, byte_size(Msg1)}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    subscribe(Ch, QQ, false),

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    ?assertEqual([{0, 0}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    publish(Ch, QQ, Msg2),

    wait_for_messages(Config, [[QQ, <<"2">>, <<"0">>, <<"2">>]]),
    ?assertEqual([{0, 0}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)),

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, #amqp_msg{}} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                               multiple     = false})
    end,

    wait_for_messages(Config, [[QQ, <<"1">>, <<"0">>, <<"1">>]]),
    ?assertEqual([{0, 0}],
                 dirty_query([Server], RaName, fun rabbit_fifo:query_in_memory_usage/1)).

consumer_metrics(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch1, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    subscribe(Ch1, QQ, false),

    RaName = ra_name(QQ),
    {ok, _, {_, Leader}} = ra:members({RaName, Server}),
    timer:sleep(5000),
    QNameRes = rabbit_misc:r(<<"/">>, queue, QQ),
    [{_, PropList, _}] = rpc:call(Leader, ets, lookup, [queue_metrics, QNameRes]),
    ?assertMatch([{consumers, 1}], lists:filter(fun({Key, _}) ->
                                                        Key == consumers
                                                end, PropList)).

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
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                  {<<"x-expires">>, long, 1000}])),
    timer:sleep(5500),
    %% check queue no longer exists
    ?assertExit(
       {{shutdown,
         {server_initiated_close,404,
          <<"NOT_FOUND - no queue 'queue_ttl' in vhost '/'">>}},
        _},
       amqp_channel:call(Ch, #'queue.declare'{queue = QQ,
                                              passive = true,
                                              durable = true,
                                              auto_delete = false,
                                              arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                           {<<"x-expires">>, long, 1000}]})),
    ok.

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
          after 5000 ->
                    flush(100),
                    ct:fail("basic.deliver timeout")
          end,
    publish(Ch, QQ),
    %% Tag2 should receive the message
    receive
        {#'basic.deliver'{delivery_tag = _,
                          consumer_tag = Tag2}, _} ->
            ok
    after 5000 ->
              flush(100),
              ct:fail("basic.deliver timeout")
    end,

    publish(Ch, QQ),
    %% Tag1 should receive the message as Tag2 has maxed qos
    receive
        {#'basic.deliver'{delivery_tag = _,
                          consumer_tag = Tag1}, _} ->
            ok
    after 5000 ->
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
    after 5000 ->
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
    after 1000 ->
        flush(100),
        ct:fail("basic.deliver timeout")
    end,

    ok = cancel(Ch),

    D = #'queue.declare'{queue = QQ, passive = true, arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
    #'queue.declare_ok'{queue = QQ, message_count = 0, consumer_count = 1} = amqp_channel:call(Ch, D),

    receive
        #'basic.cancel_ok'{consumer_tag = <<"ctag">>} -> ok
    after 1000 ->
        flush(100),
        ct:fail("basic.cancel_ok timeout")
    end,

    F = fun() ->
            #'queue.declare_ok'{queue = QQ,
                                message_count = MC,
                                consumer_count = CC} = amqp_channel:call(Ch, D),
            MC =:= 1 andalso CC =:= 0
        end,
    wait_until(F),

    ok = rabbit_ct_client_helpers:close_channel(Ch).

%%----------------------------------------------------------------------------

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

assert_queue_type(Server, Q, Expected) ->
    Actual = get_queue_type(Server, Q),
    Expected = Actual.

get_queue_type(Server, Q0) ->
    QNameRes = rabbit_misc:r(<<"/">>, queue, Q0),
    {ok, Q1} = rpc:call(Server, rabbit_amqqueue, lookup, [QNameRes]),
    amqqueue:get_type(Q1).

publish_many(Ch, Queue, Count) ->
    [publish(Ch, Queue) || _ <- lists:seq(1, Count)].

publish(Ch, Queue) ->
    publish(Ch, Queue, <<"msg">>).

publish(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).

consume(Ch, Queue, NoAck) ->
    {GetOk, _} = Reply = amqp_channel:call(Ch, #'basic.get'{queue = Queue,
                                                            no_ack = NoAck}),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg">>}}, Reply),
    GetOk#'basic.get_ok'.delivery_tag.

consume_empty(Ch, Queue, NoAck) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = Queue,
                                                    no_ack = NoAck})).

subscribe(Ch, Queue, NoAck) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
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

wait_for_cleanup(Server, Channel, Number) ->
    wait_for_cleanup(Server, Channel, Number, 60).

wait_for_cleanup(Server, Channel, Number, 0) ->
    ?assertEqual(length(rpc:call(Server, rabbit_channel, list_queue_states, [Channel])),
                Number);
wait_for_cleanup(Server, Channel, Number, N) ->
    case length(rpc:call(Server, rabbit_channel, list_queue_states, [Channel])) of
        Length when Number == Length ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_cleanup(Server, Channel, Number, N - 1)
    end.

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
         after 2000 ->
                   flush(10),
                   exit({validate_queue_timeout, M})
         end
     end || M <- ExpectedMsgs],
    ok.
