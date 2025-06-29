%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(cluster_minority_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     {group, client_operations},
     {group, cluster_operation}
    ].

groups() ->
    [
     {client_operations, [], [open_connection,
                              open_channel,
                              declare_exchange,
                              delete_exchange,
                              declare_binding,
                              delete_binding,
                              declare_queue,
                              delete_queue,
                              publish_to_exchange,
                              publish_and_consume_to_local_classic_queue,
                              consume_from_queue,
                              add_vhost,
                              update_vhost,
                              delete_vhost,
                              add_user,
                              update_user,
                              delete_user,
                              set_policy,
                              delete_policy,
                              export_definitions
                             ]},
     {cluster_operation, [], [add_node_when_seed_node_is_leader,
                              add_node_when_seed_node_is_follower,
                              remove_node_when_seed_node_is_leader,
                              remove_node_when_seed_node_is_follower]},
     {feature_flags, [], [enable_feature_flag]}
    ].

suite() ->
    [
      %% If a testcase hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 5}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    case rabbit_ct_broker_helpers:configured_metadata_store(Config) of
        mnesia ->
            %% This SUITE is meant to test how Khepri behaves in a minority,
            %% so mnesia should be skipped.
            {skip, "Minority testing not supported by mnesia"};
        _ ->
            rabbit_ct_helpers:run_setup_steps(
              Config,
              [
               fun rabbit_ct_broker_helpers:configure_dist_proxy/1
              ])
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config0) when Group == client_operations;
                                    Group == feature_flags ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 5},
                                                    {rmq_nodename_suffix, Group},
                                                    {tcp_ports_base},
                                                    {net_ticktime, 5}]),
    Config1 = rabbit_ct_helpers:run_steps(Config,
                                          rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps()),
    case Config1 of
        {skip, _} ->
            Config1;
        _ ->
            %% Before partitioning the cluster, create resources that can be used in
            %% the test cases. They're needed for delete and consume operations, which can list
            %% them but fail to operate anything else.
            %%
            %% To be used in delete_policy
            ok = rabbit_ct_broker_helpers:set_policy(Config1, 0, <<"policy-to-delete">>, <<".*">>, <<"queues">>, [{<<"max-age">>, <<"1Y">>}]),
            Ch = rabbit_ct_client_helpers:open_channel(Config1, 0),
            %% To be used in consume_from_queue
            #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = <<"test-queue">>,
                                                                           arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]}),
            %% To be used in consume_from_queue
            #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = <<"test-queue-delete-classic">>,
                                                                           durable = true,
                                                                           arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]}),
            #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = <<"test-queue-delete-stream">>,
                                                                           durable = true,
                                                                           arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),
            #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = <<"test-queue-delete-quorum">>,
                                                                           durable = true,
                                                                           arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
            %% To be used in delete_binding
            #'exchange.bind_ok'{} = amqp_channel:call(Ch, #'exchange.bind'{destination = <<"amq.fanout">>,
                                                                           source = <<"amq.direct">>,
                                                                           routing_key = <<"binding-to-be-deleted">>}),
            %% To be used in delete_exchange
            #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = <<"exchange-to-be-deleted">>}),

            %% Lower the default Khepri command timeout. By default this is set
            %% to 30s in `rabbit_khepri:setup/1' which makes the cases in this
            %% group run unnecessarily slow.
            [ok = rabbit_ct_broker_helpers:rpc(
                    Config1, N,
                    application, set_env,
                    [khepri, default_timeout, 100]) || N <- lists:seq(0, 4)],

            %% Create partition
            partition_5_node_cluster(Config1),
            Config1
    end;
init_per_group(_Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 5},
                                                    {rmq_nodes_clustered, false},
                                                    {tcp_ports_base},
                                                    {net_ticktime, 5}]),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config, {rabbit, [{forced_feature_flags_on_init, []},
                                  {khepri_leader_wait_retry_timeout, 30000}]}),
    Config1.

end_per_group(Group, Config) when Group == client_operations;
                                  Group == feature_flags ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config)
  when Testcase =:= add_node_when_seed_node_is_leader orelse
       Testcase =:= add_node_when_seed_node_is_follower orelse
       Testcase =:= remove_node_when_seed_node_is_leader orelse
       Testcase =:= remove_node_when_seed_node_is_follower ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, Testcase}]),
    rabbit_ct_helpers:run_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config)
  when Testcase =:= add_node_when_seed_node_is_leader orelse
       Testcase =:= add_node_when_seed_node_is_follower orelse
       Testcase =:= remove_node_when_seed_node_is_leader orelse
       Testcase =:= remove_node_when_seed_node_is_follower ->
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------
open_connection(Config) ->
    [A, B | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ConnA = rabbit_ct_client_helpers:open_unmanaged_connection(Config, A, <<"/">>),
    ConnB = rabbit_ct_client_helpers:open_unmanaged_connection(Config, B, <<"/">>),
    rabbit_ct_client_helpers:close_connection(ConnA),
    rabbit_ct_client_helpers:close_connection(ConnB).

open_channel(Config) ->
    [A, B | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ChA = rabbit_ct_client_helpers:open_channel(Config, A),
    ChB = rabbit_ct_client_helpers:open_channel(Config, B),
    rabbit_ct_client_helpers:close_channel(ChA),
    rabbit_ct_client_helpers:close_channel(ChB).

declare_exchange(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'exchange.declare'{exchange = <<"test-exchange">>})).

delete_exchange(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'exchange.delete'{exchange = <<"exchange-to-be-deleted">>})).

declare_binding(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'exchange.bind'{destination = <<"amq.fanout">>,
                                                       source = <<"amq.direct">>,
                                                       routing_key = <<"key">>})).

delete_binding(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'exchange.unbind'{destination = <<"amq.fanout">>,
                                                         source = <<"amq.direct">>,
                                                         routing_key = <<"binding-to-be-deleted">>})).

declare_queue(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'queue.declare'{queue = <<"test-queue-2">>})).

delete_queue(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, A),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch1, #'queue.delete'{queue = <<"test-queue-delete-classic">>})),
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, A),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch2, #'queue.delete'{queue = <<"test-queue-delete-stream">>})),
    Conn3 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, A),
    {ok, Ch3} = amqp_connection:open_channel(Conn3),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch3, #'queue.delete'{queue = <<"test-queue-delete-quorum">>})),
    ok.

publish_to_exchange(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertEqual(ok, amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"test-queue-2">>},
                                       #amqp_msg{payload = <<"msg">>})).

publish_and_consume_to_local_classic_queue(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertEqual(ok, amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"test-queue">>},
                                       #amqp_msg{payload = <<"msg">>})),
    ?assertMatch({#'basic.get_ok'{}, _},
                 amqp_channel:call(Ch, #'basic.get'{queue  = <<"test-queue">>})).

consume_from_queue(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertMatch(#'basic.consume_ok'{},
                 amqp_channel:call(Ch, #'basic.consume'{queue = <<"test-queue">>})).

add_vhost(Config) ->
    ?assertMatch({error, timeout},
                 rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost1">>)).

update_vhost(Config) ->
    ?assertThrow({error, timeout},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, update_tags,
                                              [<<"/">>, [carrots], <<"user">>])).

delete_vhost(Config) ->
    ?assertError(
       {erpc, timeout},
       rabbit_ct_broker_helpers:rpc(
         Config, 0,
         rabbit_vhost, delete, [<<"vhost1">>, <<"acting-user">>], 1_000)).

add_user(Config) ->
    ?assertMatch({error, timeout},
                 rabbit_ct_broker_helpers:add_user(Config, <<"user1">>)).

update_user(Config) ->
    ?assertMatch({error, timeout},
                 rabbit_ct_broker_helpers:set_user_tags(Config, 0, <<"user1">>, [<<"admin">>])).

delete_user(Config) ->
    ?assertMatch({error, timeout},
                 rabbit_ct_broker_helpers:delete_user(Config, <<"user1">>)).

set_policy(Config) ->
    ?assertError(_, rabbit_ct_broker_helpers:set_policy(Config, 0, <<"max-age-policy">>, <<".*">>, <<"queues">>, [{<<"max-age">>, <<"1Y">>}])).

delete_policy(Config) ->
    ?assertError(_, rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"policy-to-delete">>)).

add_node_when_seed_node_is_leader(Config) ->
    [A, B, C, _D, E] = rabbit_ct_broker_helpers:get_node_configs(
                         Config, nodename),

    %% Three node cluster: A, B, C
    Cluster = [A, B, C],
    Config1 = rabbit_ct_broker_helpers:cluster_nodes(Config, Cluster),

    AMember = {rabbit_khepri:get_store_id(), A},
    _ = ra:transfer_leadership(AMember, AMember),
    clustering_utils:assert_cluster_status({Cluster, Cluster}, Cluster),

    %% Minority partition: A
    partition_3_node_cluster(Config1),

    Pong = ra:ping(AMember, 10000),
    ct:pal("Member A state: ~0p", [Pong]),
    case Pong of
        {pong, State} when State =/= follower andalso State =/= candidate ->
            Ret = rabbit_control_helper:command(
                    join_cluster, E, [atom_to_list(A)], []),
            ?assertMatch({error, _, _}, Ret),
            {error, _, Msg} = Ret,
            ?assertEqual(
               match,
               re:run(
                 Msg, "(Khepri cluster could be in minority|\\{:rabbit, \\{\\{:error, :timeout\\})",
                 [{capture, none}]));
        Ret ->
            ct:pal("A is not the expected leader: ~p", [Ret]),
            {skip, "Node A was not elected leader"}
    end.

add_node_when_seed_node_is_follower(Config) ->
    [A, B, C, _D, E] = rabbit_ct_broker_helpers:get_node_configs(
                         Config, nodename),

    %% Three node cluster: A, B, C
    Cluster = [A, B, C],
    Config1 = rabbit_ct_broker_helpers:cluster_nodes(Config, Cluster),

    CMember = {rabbit_khepri:get_store_id(), C},
    ra:transfer_leadership(CMember, CMember),
    clustering_utils:assert_cluster_status({Cluster, Cluster}, Cluster),

    %% Minority partition: A
    partition_3_node_cluster(Config1),

    AMember = {rabbit_khepri:get_store_id(), A},
    Pong = ra:ping(AMember, 10000),
    ct:pal("Member A state: ~0p", [Pong]),
    case Pong of
        {pong, State}
          when State =:= follower orelse State =:= pre_vote ->
            Ret = rabbit_control_helper:command(
                    join_cluster, E, [atom_to_list(A)], []),
            ?assertMatch({error, _, _}, Ret),
            {error, _, Msg} = Ret,
            ?assertEqual(
               match,
               re:run(
                 Msg, "Khepri cluster could be in minority",
                 [{capture, none}]));
        {pong, await_condition} ->
            Ret = rabbit_control_helper:command(
                    join_cluster, E, [atom_to_list(A)], []),
            ?assertMatch({error, _, _}, Ret),
            {error, _, Msg} = Ret,
            ?assertEqual(
               match,
               re:run(
                 Msg, "\\{:rabbit, \\{\\{:error, :timeout\\}",
                 [{capture, none}])),
            clustering_utils:assert_cluster_status(
              {Cluster, Cluster}, Cluster);
        Ret ->
            ct:pal("A is not the expected follower: ~p", [Ret]),
            {skip, "Node A was not a follower"}
    end.

remove_node_when_seed_node_is_leader(Config) ->
    [A, B, C | _] = rabbit_ct_broker_helpers:get_node_configs(
                      Config, nodename),

    %% Three node cluster: A, B, C
    Cluster = [A, B, C],
    Config1 = rabbit_ct_broker_helpers:cluster_nodes(Config, Cluster),

    AMember = {rabbit_khepri:get_store_id(), A},
    ra:transfer_leadership(AMember, AMember),
    clustering_utils:assert_cluster_status({Cluster, Cluster}, Cluster),

    %% Minority partition: A
    partition_3_node_cluster(Config1),

    Pong = ra:ping(AMember, 10000),
    ct:pal("Member A state: ~0p", [Pong]),
    case Pong of
        {pong, leader} ->
            ?awaitMatch(
               {ok, #{cluster_change_permitted := true}, _},
               rabbit_ct_broker_helpers:rpc(
                 Config1, A, ra, member_overview, [AMember]),
               60000),
            ?awaitMatch(
               ok,
               rabbit_control_helper:command(
                 forget_cluster_node, A, [atom_to_list(B)], []),
               60000);
        Ret ->
            ct:pal("A is not the expected leader: ~p", [Ret]),
            {skip, "Node A was not a leader"}
    end.

remove_node_when_seed_node_is_follower(Config) ->
    [A, B, C | _] = rabbit_ct_broker_helpers:get_node_configs(
                      Config, nodename),

    %% Three node cluster: A, B, C
    Cluster = [A, B, C],
    Config1 = rabbit_ct_broker_helpers:cluster_nodes(Config, Cluster),

    CMember = {rabbit_khepri:get_store_id(), C},
    ra:transfer_leadership(CMember, CMember),
    clustering_utils:assert_cluster_status({Cluster, Cluster}, Cluster),

    %% Minority partition: A
    partition_3_node_cluster(Config1),

    AMember = {rabbit_khepri:get_store_id(), A},
    Pong = ra:ping(AMember, 10000),
    ct:pal("Member A state: ~0p", [Pong]),
    case Pong of
        {pong, State}
          when State =:= follower orelse State =:= pre_vote ->
            Ret = rabbit_control_helper:command(
                    forget_cluster_node, A, [atom_to_list(B)], []),
            ?assertMatch({error, _, _}, Ret),
            {error, _, Msg} = Ret,
            ?assertEqual(
               match,
               re:run(
                 Msg, "Khepri cluster could be in minority",
                 [{capture, none}]));
        {pong, await_condition} ->
            Ret = rabbit_control_helper:command(
                    forget_cluster_node, A, [atom_to_list(B)], []),
            ?assertMatch(ok, Ret);
        Ret ->
            ct:pal("A is not the expected leader: ~p", [Ret]),
            {skip, "Node A was not a leader"}
    end.

enable_feature_flag(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ?assertMatch({error, missing_clustered_nodes}, rabbit_ct_broker_helpers:rpc(Config, A, rabbit_feature_flags, enable, [khepri_db])).

export_definitions(Config) ->
    Definitions = rabbit_ct_broker_helpers:rpc(
                    Config, 0,
                    rabbit_definitions, all_definitions, []),
    ?assert(is_map(Definitions)).

%% -------------------------------------------------------------------
%% Internal helpers.
%% -------------------------------------------------------------------

partition_3_node_cluster(Config) ->
    [A, B, C | _] = rabbit_ct_broker_helpers:get_node_configs(
                              Config, nodename),
    Cluster = [A, B, C],
    clustering_utils:assert_cluster_status({Cluster, Cluster}, Cluster),
    NodePairs = [{A, B},
                 {A, C}],
    [rabbit_ct_broker_helpers:block_traffic_between(X, Y) || {X, Y} <- NodePairs],
    %% Wait for the network partition to happen
    clustering_utils:assert_cluster_status({Cluster, [B, C]}, [B, C]).

partition_5_node_cluster(Config) ->
    [A, B, C, D, E] = All = rabbit_ct_broker_helpers:get_node_configs(
                              Config, nodename),
    %% Wait for the cluster to be ready
    clustering_utils:assert_cluster_status({All, All}, All),
    %% Minority partition A, B
    NodePairs = [{A, C},
                 {A, D},
                 {A, E},
                 {B, C},
                 {B, D},
                 {B, E}],
    [rabbit_ct_broker_helpers:block_traffic_between(X, Y) || {X, Y} <- NodePairs],
    %% Wait for the network partition to happen
    clustering_utils:assert_cluster_status({All, [C, D, E]}, [C, D, E]).

join_3_node_cluster(Config)->
    [A, B, C | _] = rabbit_ct_broker_helpers:get_node_configs(
                      Config, nodename),
    %% Minority partition A
    NodePairs = [{A, B},
                 {A, C}],
    [rabbit_ct_broker_helpers:allow_traffic_between(X, Y) || {X, Y} <- NodePairs].
