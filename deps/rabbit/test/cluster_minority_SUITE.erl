%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(cluster_minority_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, client_operations},
     {group, cluster_operation_add},
     {group, cluster_operation_remove}
    ].

groups() ->
    [
     {client_operations, [], [open_connection,
                              open_channel,
                              declare_exchange,
                              declare_binding,
                              declare_queue,
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
                              delete_policy
                             ]},
     {cluster_operation_add, [], [add_node]},
     {cluster_operation_remove, [], [remove_node]},
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

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, khepri}]),
    rabbit_ct_helpers:run_setup_steps(Config,
                                      [
                                       fun rabbit_ct_broker_helpers:configure_dist_proxy/1
                                      ]).

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
            %% Before partitioning the cluster, create a policy and queue that can be used in
            %% the test cases. They're needed for delete and consume operations, which can list
            %% them but fail to operate anything else.
            %%
            %% To be used in delete_policy
            ok = rabbit_ct_broker_helpers:set_policy(Config1, 0, <<"policy-to-delete">>, <<".*">>, <<"queues">>, [{<<"max-age">>, <<"1Y">>}]),
            Ch = rabbit_ct_client_helpers:open_channel(Config1, 0),
            %% To be used in consume_from_queue
            #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = <<"test-queue">>,
                                                                           arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]}),

            %% Create partition
            partition_5_node_cluster(Config1),
            Config1
    end;
init_per_group(Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{rmq_nodes_count, 5},
                                                    {rmq_nodename_suffix, Group},
                                                    {rmq_nodes_clustered, false},
                                                    {tcp_ports_base},
                                                    {net_ticktime, 5}]),
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                    rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps() ++
                                    rabbit_ct_client_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

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

declare_binding(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'exchange.bind'{destination = <<"amq.fanout">>,
                                                       source = <<"amq.direct">>,
                                                       routing_key = <<"key">>})).

declare_queue(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    ?assertExit({{shutdown, {connection_closing, {server_initiated_close, 541, _}}}, _},
                amqp_channel:call(Ch, #'queue.declare'{queue = <<"test-queue-2">>})).

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
    ?assertMatch({error, {timeout, _}},
                 rabbit_ct_broker_helpers:add_vhost(Config, <<"vhost1">>)).

update_vhost(Config) ->
    ?assertThrow({error, {timeout, _}},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, update_tags,
                                              [<<"/">>, [carrots], <<"user">>])).

delete_vhost(Config) ->
    ?assertMatch({'EXIT', _}, rabbit_ct_broker_helpers:delete_vhost(Config, <<"vhost1">>)).

add_user(Config) ->
    ?assertMatch({error, {timeout, _}},
                 rabbit_ct_broker_helpers:add_user(Config, <<"user1">>)).

update_user(Config) ->
    ?assertMatch({error, {timeout, _}},
                 rabbit_ct_broker_helpers:set_user_tags(Config, 0, <<"user1">>, [<<"admin">>])).

delete_user(Config) ->
    ?assertMatch({error, {timeout, _}},
                 rabbit_ct_broker_helpers:delete_user(Config, <<"user1">>)).

set_policy(Config) ->
    ?assertError(_, rabbit_ct_broker_helpers:set_policy(Config, 0, <<"max-age-policy">>, <<".*">>, <<"queues">>, [{<<"max-age">>, <<"1Y">>}])).

delete_policy(Config) ->
    ?assertError(_, rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"policy-to-delete">>)).

add_node(Config) ->
    [A, B, C, D, _E] = rabbit_ct_broker_helpers:get_node_configs(
                         Config, nodename),
    
    %% Three node cluster: A, B, C
    ok = rabbit_control_helper:command(stop_app, B),
    ok = rabbit_control_helper:command(join_cluster, B, [atom_to_list(A)], []),
    rabbit_control_helper:command(start_app, B),

    ok = rabbit_control_helper:command(stop_app, C),
    ok = rabbit_control_helper:command(join_cluster, C, [atom_to_list(A)], []),
    rabbit_control_helper:command(start_app, C),
    
    %% Minority partition: A
    Cluster = [A, B, C],
    partition_3_node_cluster(Config),

    ok = rabbit_control_helper:command(stop_app, D),
    %% The command is appended to the log, but it will be dropped once the connectivity
    %% is restored
    ?assertMatch(ok,
                 rabbit_control_helper:command(join_cluster, D, [atom_to_list(A)], [])),
    timer:sleep(10000),
    join_3_node_cluster(Config),
    clustering_utils:assert_cluster_status({Cluster, Cluster}, Cluster).

remove_node(Config) ->
    [A, B, C | _] = rabbit_ct_broker_helpers:get_node_configs(
                      Config, nodename),
    
    %% Three node cluster: A, B, C
    ok = rabbit_control_helper:command(stop_app, B),
    ok = rabbit_control_helper:command(join_cluster, B, [atom_to_list(A)], []),
    rabbit_control_helper:command(start_app, B),

    ok = rabbit_control_helper:command(stop_app, C),
    ok = rabbit_control_helper:command(join_cluster, C, [atom_to_list(A)], []),
    rabbit_control_helper:command(start_app, C),
    
    %% Minority partition: A
    partition_3_node_cluster(Config),
    Cluster = [A, B, C],

    ok = rabbit_control_helper:command(forget_cluster_node, A, [atom_to_list(B)], []),
    timer:sleep(10000),
    join_3_node_cluster(Config),
    clustering_utils:assert_cluster_status({Cluster, Cluster}, Cluster).

enable_feature_flag(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ?assertMatch({error, missing_clustered_nodes}, rabbit_ct_broker_helpers:rpc(Config, A, rabbit_feature_flags, enable, [khepri_db])).


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
