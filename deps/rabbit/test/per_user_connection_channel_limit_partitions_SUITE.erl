%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_user_connection_channel_limit_partitions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(rabbit_ct_client_helpers, [open_unmanaged_connection/2
                                   ]).

all() ->
    [
     {group, net_ticktime_1}
    ].

groups() ->
    [
     {net_ticktime_1, [], [
          cluster_full_partition_with_autoheal
     ]}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 8}}
    ].

%% see partitions_SUITE
-define(DELAY, 12000).

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config, [fun rabbit_ct_broker_helpers:configure_dist_proxy/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(net_ticktime_1 = Group, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            %% In a mixed 3.8/3.9 cluster, changes to rabbit_core_ff.erl imply that some
            %% feature flag related migrations cannot occur, and therefore user_limits
            %% cannot be enabled in a 3.8/3.9 mixed cluster
            {skip, "group is not mixed version compatible"};
        _ ->
            Config1 = rabbit_ct_helpers:set_config(Config, [{net_ticktime, 1}]),
            init_per_multinode_group(Group, Config1, 3)
    end.

init_per_multinode_group(Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    Config2 = rabbit_ct_helpers:run_steps(
                Config1, rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    EnableFF = rabbit_ct_broker_helpers:enable_feature_flag(
                 Config2, user_limits),
    case EnableFF of
        ok ->
            Config2;
        Skip ->
            end_per_group(Group, Config2),
            Skip
    end.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

cluster_full_partition_with_autoheal(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    rabbit_ct_broker_helpers:set_partition_handling_mode_globally(Config, autoheal),

    ?assertEqual(0, count_connections_in(Config, Username)),
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% 6 connections, 2 per node
    Conn1 = open_unmanaged_connection(Config, A),
    Conn2 = open_unmanaged_connection(Config, A),
    Conn3 = open_unmanaged_connection(Config, B),
    Conn4 = open_unmanaged_connection(Config, B),
    Conn5 = open_unmanaged_connection(Config, C),
    Conn6 = open_unmanaged_connection(Config, C),

    _Chans1 = [_|_] = open_channels(Conn1, 5),
    _Chans3 = [_|_] = open_channels(Conn3, 5),
    _Chans5 = [_|_] = open_channels(Conn5, 5),
    ?awaitMatch({6, 15},
                {count_connections_in(Config, Username),
                 count_channels_in(Config, Username)},
                60000, 3000),

    %% B drops off the network, non-reachable by either A or C
    rabbit_ct_broker_helpers:block_traffic_between(A, B),
    rabbit_ct_broker_helpers:block_traffic_between(B, C),
    timer:sleep(?DELAY),

    %% A and C are still connected, so 4 connections are tracked
    %% All connections to B are dropped
    ?awaitMatch({4, 10},
                {count_connections_in(Config, Username),
                 count_channels_in(Config, Username)},
                60000, 3000),

    rabbit_ct_broker_helpers:allow_traffic_between(A, B),
    rabbit_ct_broker_helpers:allow_traffic_between(B, C),
    timer:sleep(?DELAY),

    %% during autoheal B's connections were dropped
    ?awaitMatch({4, 10},
                {count_connections_in(Config, Username),
                 count_channels_in(Config, Username)},
                60000, 3000),

    lists:foreach(fun (Conn) ->
                          (catch rabbit_ct_client_helpers:close_connection(Conn))
                  end, [Conn1, Conn2, Conn3, Conn4,
                        Conn5, Conn6]),
    ?awaitMatch({0, 0},
                {count_connections_in(Config, Username),
                 count_channels_in(Config, Username)},
                60000, 3000),

    passed.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

open_channels(Conn, N) ->
    [begin
        {ok, Ch} = amqp_connection:open_channel(Conn),
        Ch
     end || _ <- lists:seq(1, N)].

count_connections_in(Config, Username) ->
    length(connections_in(Config, Username)).

connections_in(Config, Username) ->
    connections_in(Config, 0, Username).
connections_in(Config, NodeIndex, Username) ->
    tracked_list_of_user(Config, NodeIndex, rabbit_connection_tracking, Username).

count_channels_in(Config, Username) ->
    Channels = channels_in(Config, Username),
    length([Ch || Ch = #tracked_channel{username = Username0} <- Channels,
                  Username =:= Username0]).

channels_in(Config, Username) ->
    channels_in(Config, 0, Username).
channels_in(Config, NodeIndex, Username) ->
    tracked_list_of_user(Config, NodeIndex, rabbit_channel_tracking, Username).

tracked_list_of_user(Config, NodeIndex, TrackingMod, Username) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 TrackingMod,
                                 list_of_user, [Username]).
