%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(quorum_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-import(quorum_queue_utils, [wait_for_messages_ready/3,
                             wait_for_messages_pending_ack/3,
                             wait_for_messages_total/3,
                             wait_for_messages/2,
                             dirty_query/3,
                             ra_name/1]).

-compile(export_all).

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
     {single_node, [], all_tests()},
     {single_node, [], memory_tests()},
     {unclustered, [], [
                        {cluster_size_2, [], [add_member]}
                       ]},
     {clustered, [], [
                      {cluster_size_2, [], [cleanup_data_dir]},
                      {cluster_size_2, [], [add_member_not_running,
                                            add_member_classic,
                                            add_member_already_a_member,
                                            add_member_not_found,
                                            delete_member_not_running,
                                            delete_member_classic,
                                            delete_member_not_found,
                                            delete_member]
                       ++ all_tests()},
                      {cluster_size_2, [], memory_tests()},
                      {cluster_size_3, [], [
                                            declare_during_node_down,
                                            simple_confirm_availability_on_leader_change,
                                            publishing_to_unavailable_queue,
                                            confirm_availability_on_leader_change,
                                            recover_from_single_failure,
                                            recover_from_multiple_failures,
                                            leadership_takeover,
                                            delete_declare,
                                            metrics_cleanup_on_leadership_takeover,
                                            metrics_cleanup_on_leader_crash,
                                            consume_in_minority,
                                            shrink_all
                                            ]},
                      {cluster_size_5, [], [start_queue,
                                            start_queue_concurrent,
                                            quorum_cluster_size_3,
                                            quorum_cluster_size_7
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
     declare_invalid_args,
     declare_invalid_properties,
     start_queue,
     stop_queue,
     restart_queue,
     restart_all_types,
     stop_start_rabbit_app,
     publish_and_restart,
     subscribe_should_fail_when_global_qos_true,
     dead_letter_to_classic_queue,
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
     subscribe_redelivery_limit,
     subscribe_redelivery_policy,
     subscribe_redelivery_limit_with_dead_letter
    ].

memory_tests() ->
    [
     memory_alarm_rolls_wal
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(unclustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, false}]);
init_per_group(clustered_with_partitions, Config) ->
    rabbit_ct_helpers:set_config(Config, [{net_ticktime, 10}]);
init_per_group(Group, Config) ->
    ClusterSize = case Group of
                      single_node -> 1;
                      cluster_size_2 -> 2;
                      cluster_size_3 -> 3;
                      cluster_size_5 -> 5
                  end,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
    Config2 = rabbit_ct_helpers:run_steps(Config1b,
                                          [fun merge_app_env/1 ] ++
                                          rabbit_ct_broker_helpers:setup_steps()),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config2, quorum_queue) of
        ok ->
            ok = rabbit_ct_broker_helpers:rpc(
                   Config2, 0, application, set_env,
                   [rabbit, channel_queue_cleanup_interval, 100]),
            %% HACK: the larger cluster sizes benefit for a bit more time
            %% after clustering before running the tests.
            case Group of
                cluster_size_5 ->
                    timer:sleep(5000),
                    Config2;
                _ ->
                    Config2
            end;
        Skip ->
            end_per_group(Group, Config2),
            Skip
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
                                            {tcp_ports_base},
                                            {queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    Config3 = rabbit_ct_helpers:run_steps(
                Config2,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps() ++
                [fun rabbit_ct_broker_helpers:enable_dist_proxy/1,
                 fun rabbit_ct_broker_helpers:cluster_nodes/1]),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config3, quorum_queue) of
        ok ->
            Config3;
        Skip ->
            end_per_testcase(Testcase, Config3),
            Skip
    end;
init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

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
    catch delete_queues(),
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
    assert_queue_type(Server, LQ, quorum),

    DQ = <<"classic-declare-args-q">>,
    declare(Ch, DQ, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    assert_queue_type(Server, DQ, classic),

    DQ2 = <<"classic-q2">>,
    declare(Ch, DQ2),
    assert_queue_type(Server, DQ2, classic).

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

declare_invalid_args(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    LQ = ?config(queue_name, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-expires">>, long, 2000}])),
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-message-ttl">>, long, 2000}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-max-priority">>, long, 2000}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-overflow">>, longstr, <<"reject-publish">>}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-queue-mode">>, longstr, <<"lazy">>}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-quorum-initial-group-size">>, longstr, <<"hop">>}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(rabbit_ct_client_helpers:open_channel(Config, Server),
               LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                    {<<"x-quorum-initial-group-size">>, long, 0}])).

start_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Check that the application and one ra node are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))),
    ?assertMatch([_], rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])),

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
    ?assertMatch([_], rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])).

start_queue_concurrent(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    LQ = ?config(queue_name, Config),
    Self = self(),
    [begin
         _ = spawn_link(fun () ->
                                {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, Server),
                                %% Test declare an existing queue
                                ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                                             declare(Ch, LQ,
                                                     [{<<"x-queue-type">>,
                                                       longstr,
                                                       <<"quorum">>}])),
                                Self ! {done, Server}
                        end)
     end || Server <- Servers],

    [begin
         receive {done, Server} -> ok
         after 5000 -> exit({await_done_timeout, Server})
         end
     end || Server <- Servers],


    ok.

quorum_cluster_size_3(Config) ->
    quorum_cluster_size_x(Config, 3, 3).

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
    {ok, Members, _} = ra:members({RaName, Server}),
    ?assertEqual(Expected, length(Members)),
    Info = rpc:call(Server, rabbit_quorum_queue, infos,
                    [rabbit_misc:r(<<"/">>, queue, QQ)]),
    MembersQ = proplists:get_value(members, Info),
    ?assertEqual(Expected, length(MembersQ)).

stop_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    %% Check that the application and one ra node are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))),
    ?assertMatch([_], rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])),

    %% Delete the quorum queue
    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch, #'queue.delete'{queue = LQ})),
    %% Check that the application and process are down
    wait_until(fun() ->
                       [] == rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])
               end),
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))).

restart_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    LQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', LQ, 0, 0},
                 declare(Ch, LQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server),

    %% Check that the application and one ra node are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))),
    ?assertMatch([_], rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])).

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

    timer:sleep(1000),
    %% validate quorum queue is still functional
    RaName = ra_name(LQ),
    {ok, _, _} = ra:members({RaName, Server}),
    %% validate vhosts are running - or rather validate that at least one
    %% vhost per cluster is running
    [begin
         #{cluster_state := ServerStatuses} = maps:from_list(I),
         ?assertMatch(#{Server := running}, maps:from_list(ServerStatuses))
     end || I <- rpc:call(Server, rabbit_vhost,info_all, [])],
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

    UId = rpc:call(Node, ra_directory, where_is, [RaName]),
    ?assert(UId =/= undefined),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
    %% validate quorum queues got deleted
    undefined = rpc:call(Node, ra_directory, where_is, [RaName]),
    ok.

restart_all_types(Config) ->
    %% Test the node restart with both types of queues (quorum and classic) to
    %% ensure there are no regressions
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

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

    %% Check that the application and two ra nodes are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))),
    ?assertMatch([_,_], rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])),
    %% Check the classic queues restarted correctly
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ1, no_ack = false}),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ2, no_ack = false}),
    delete_queues(Ch2, [QQ1, QQ2, CQ1, CQ2]).

delete_queues(Ch, Queues) ->
    [amqp_channel:call(Ch, #'queue.delete'{queue = Q}) ||  Q <- Queues].

stop_start_rabbit_app(Config) ->
    %% Test start/stop of rabbit app with both types of queues (quorum and
    %%  classic) to ensure there are no regressions
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

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

    rabbit_control_helper:command(stop_app, Server),
    %% Check the ra application has stopped (thus its supervisor and queues)
    ?assertMatch(false, lists:keyfind(ra, 1,
                                      rpc:call(Server, application, which_applications, []))),

    rabbit_control_helper:command(start_app, Server),

    %% Check that the application and two ra nodes are up
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))),
    ?assertMatch([_,_], rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])),
    %% Check the classic queues restarted correctly
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ1, no_ack = false}),
    {#'basic.get_ok'{}, #amqp_msg{}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = CQ2, no_ack = false}),
    delete_queues(Ch2, [QQ1, QQ2, CQ1, CQ2]).

publish_confirm(Ch, QName) ->
    publish(Ch, QName),
    amqp_channel:register_confirm_handler(Ch, self()),
    ct:pal("waiting for confirms from ~s", [QName]),
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 2500 ->
                   exit(confirm_timeout)
         end,
    ct:pal("CONFIRMED! ~s", [QName]),
    ok.

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
    timer:sleep(500),
    Result = rpc:call(Server0, rabbit_quorum_queue, shrink_all, [Server2]),
    ?assertMatch([{_, {ok, 2}}, {_, {ok, 2}}], Result),
    Result1 = rpc:call(Server0, rabbit_quorum_queue, shrink_all, [Server1]),
    ?assertMatch([{_, {ok, 1}}, {_, {ok, 1}}], Result1),
    Result2 = rpc:call(Server0, rabbit_quorum_queue, shrink_all, [Server0]),
    ?assertMatch([{_, {error, 1, last_node}},
                  {_, {error, 1, last_node}}], Result2),
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
                       [] == rpc:call(Server, supervisor, which_children,
                                      [ra_server_sup_sup])
               end),
    %% Check that all queue states have been cleaned
    wait_for_cleanup(Server, NCh1, 0),
    wait_for_cleanup(Server, NCh2, 0).

cleanup_queue_state_on_channel_after_subscribe(Config) ->
    %% Declare/delete the queue and publish in one channel, while consuming on a
    %% different one to verify that the cleanup is propagated through channels
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

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
                       [] == rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])
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
    [Server, Server1, Server2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    TCh = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(TCh, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish_many(Ch, QQ, 300),
    timer:sleep(1000),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    %% check we get at least on ack
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 30000 ->
                   exit(confirm_timeout)
         end,
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
    timer:sleep(1000),
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
    publish_confirm(Ch, QQ),

    %% stop the node hosting the leader
    stop_node(Config, Node2),
    %% this should not fail as the channel should detect the new leader and
    %% resend to that
    publish_confirm(Ch, QQ),
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
    Pid = spawn_link(fun () ->
                             %% open a channel to another node
                             Ch = rabbit_ct_client_helpers:open_channel(Config, Node1),
                             #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
                             ConfirmLoop = fun Loop() ->
                                                     publish_confirm(Ch, QQ),
                                                     receive {done, P} ->
                                                                 P ! done,
                                                                 ok
                                                     after 0 -> Loop() end
                                             end,
                             ConfirmLoop()
                       end),

    timer:sleep(500),
    %% stop the node hosting the leader
    stop_node(Config, Node2),
    %% this should not fail as the channel should detect the new leader and
    %% resend to that
    timer:sleep(500),
    Pid ! {done, self()},
    receive
        done -> ok;
        {'EXIT', Pid, Err} ->
            exit(Err)
    after 5500 ->
              flush(100),
              exit(bah)
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
                          [<<"/">>, QQ, 'rabbit@burrow'])).

add_member_classic(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0}, declare(Ch, CQ, [])),
    ?assertEqual({error, classic_queue_not_supported},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, CQ, Server])).

add_member_already_a_member(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({error, already_a_member},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server])).

add_member_not_found(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    QQ = ?config(queue_name, Config),
    ?assertEqual({error, not_found},
                 rpc:call(Server, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server])).

add_member(Config) ->
    [Server0, Server1] = Servers0 =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ?assertEqual({error, node_not_running},
                 rpc:call(Server0, rabbit_quorum_queue, add_member,
                          [<<"/">>, QQ, Server1])),
    ok = rabbit_control_helper:command(stop_app, Server1),
    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    rabbit_control_helper:command(start_app, Server1),
    ?assertEqual(ok, rpc:call(Server0, rabbit_quorum_queue, add_member,
                              [<<"/">>, QQ, Server1])),
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
    ?assertEqual({error, node_not_running},
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

delete_member_not_found(Config) ->
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
                          [<<"/">>, QQ, Server])),
    ?assertEqual({error, not_a_member},
                 rpc:call(Server, rabbit_quorum_queue, delete_member,
                          [<<"/">>, QQ, Server])).


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

    [{_, UId1}] = rpc:call(Server1, ra_directory, list_registered, []),
    [{_, UId2}] = rpc:call(Server2, ra_directory, list_registered, []),
    DataDir1 = rpc:call(Server1, ra_env, server_data_dir, [UId1]),
    DataDir2 = rpc:call(Server2, ra_env, server_data_dir, [UId2]),
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
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    Cmd2 = ["eval", "rabbit_amqqueue:delete_immediately_by_resource([rabbit_misc:r(<<\"/\">>, queue, <<\"" ++ binary_to_list(QQ) ++ "\">>)])."],
    ?assertEqual({ok, "ok\n"}, rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd2)),

    %% Check that the application and process are down
    wait_until(fun() ->
                       [] == rpc:call(Server, supervisor, which_children, [ra_server_sup_sup])
               end),
    ?assertMatch({ra, _, _}, lists:keyfind(ra, 1,
                                           rpc:call(Server, application, which_applications, []))).

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
    end,

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    end,

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H2}}} ->
            ?assertMatch({DCHeader, _, 2}, rabbit_basic:header(DCHeader, H2)),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag2,
                                               multiple     = false}),
            wait_for_messages_ready(Servers, RaName, 0),
            wait_for_messages_pending_ack(Servers, RaName, 0)
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
    WalDataDir = rpc:call(Server, ra_env, wal_data_dir, []),
    [Wal0] = filelib:wildcard(WalDataDir ++ "/*.wal"),
    ok = rpc:call(Server, rabbit_alarm, set_alarm,
                  [{{resource_limit, memory, Server}, []}]),
    timer:sleep(1000),
    [Wal1] = filelib:wildcard(WalDataDir ++ "/*.wal"),
    ?assert(Wal0 =/= Wal1),
    %% roll over shouldn't happen if we trigger a new alarm in less than
    %% min_wal_roll_over_interval
    ok = rpc:call(Server, rabbit_alarm, set_alarm,
                  [{{resource_limit, memory, Server}, []}]),
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
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = <<"msg">>}).

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
