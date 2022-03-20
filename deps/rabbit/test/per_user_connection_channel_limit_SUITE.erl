%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_user_connection_channel_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

all() ->
    [
     {group, cluster_size_1_network},
     {group, cluster_size_2_network},
     {group, cluster_size_2_direct}
    ].

groups() ->
    ClusterSize1Tests = [
        most_basic_single_node_connection_and_channel_count,
        single_node_single_user_connection_and_channel_count,
        single_node_multiple_users_connection_and_channel_count,
        single_node_list_in_user,
        single_node_single_user_limit,
        single_node_single_user_zero_limit,
        single_node_single_user_clear_limits,
        single_node_multiple_users_clear_limits,
        single_node_multiple_users_limit,
        single_node_multiple_users_zero_limit

    ],
    ClusterSize2Tests = [
        most_basic_cluster_connection_and_channel_count,
        cluster_single_user_connection_and_channel_count,
        cluster_multiple_users_connection_and_channel_count,
        cluster_node_restart_connection_and_channel_count,
        cluster_node_list_on_node,
        cluster_single_user_limit,
        cluster_single_user_limit2,
        cluster_single_user_zero_limit,
        cluster_single_user_clear_limits,
        cluster_multiple_users_clear_limits,
        cluster_multiple_users_zero_limit
    ],
    [
      {cluster_size_1_network, [], ClusterSize1Tests},
      {cluster_size_2_network, [], ClusterSize2Tests},
      {cluster_size_2_direct,  [], ClusterSize2Tests}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 8}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1_network, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_multinode_group(cluster_size_1_network, Config1, 1);
init_per_group(cluster_size_2_network, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            %% In a mixed 3.8/3.9 cluster, changes to rabbit_core_ff.erl imply that some
            %% feature flag related migrations cannot occur, and therefore user_limits
            %% cannot be enabled in a 3.8/3.9 mixed cluster
            {skip, "cluster_size_2_network is not mixed version compatible"};
        _ ->
            Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
            init_per_multinode_group(cluster_size_2_network, Config1, 2)
    end;
init_per_group(cluster_size_2_direct, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "cluster_size_2_network is not mixed version compatible"};
        _ ->
            Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
            init_per_multinode_group(cluster_size_2_direct, Config1, 2)
    end;

init_per_group(cluster_rename, Config) ->
    init_per_multinode_group(cluster_rename, Config, 2).

init_per_multinode_group(Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    case Group of
        cluster_rename ->
            % The broker is managed by {init,end}_per_testcase().
            Config1;
        _ ->
            Config2 = rabbit_ct_helpers:run_steps(
                        Config1, rabbit_ct_broker_helpers:setup_steps() ++
                                 rabbit_ct_client_helpers:setup_steps()),
            EnableFF = rabbit_ct_broker_helpers:enable_feature_flag(
                         Config2, user_limits),
            case EnableFF of
                ok ->
                    Config2;
                {skip, _} = Skip ->
                    end_per_group(Group, Config2),
                    Skip;
                Other ->
                    end_per_group(Group, Config2),
                    {skip, Other}
            end
    end.

end_per_group(cluster_rename, Config) ->
    % The broker is managed by {init,end}_per_testcase().
    Config;
end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    clear_all_connection_tracking_tables(Config),
    clear_all_channel_tracking_tables(Config),
    Config.

end_per_testcase(Testcase, Config) ->
    clear_all_connection_tracking_tables(Config),
    clear_all_channel_tracking_tables(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

clear_all_connection_tracking_tables(Config) ->
    [rabbit_ct_broker_helpers:rpc(Config,
        N,
        rabbit_connection_tracking,
        clear_tracking_tables,
        []) || N <- rabbit_ct_broker_helpers:get_node_configs(Config, nodename)].

clear_all_channel_tracking_tables(Config) ->
    [rabbit_ct_broker_helpers:rpc(Config,
        N,
        rabbit_channel_tracking,
        clear_tracking_tables,
        []) || N <- rabbit_ct_broker_helpers:get_node_configs(Config, nodename)].

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

most_basic_single_node_connection_and_channel_count(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    [Conn] = open_connections(Config, [0]),
    [Chan] = open_channels(Conn, 1),
    
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 1
        end),
    close_channels([Chan]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),
    close_connections([Conn]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0
        end).

single_node_single_user_connection_and_channel_count(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    [Conn1] = open_connections(Config, [0]),
    [Chan1] = open_channels(Conn1, 1),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 1
        end),
    close_channels([Chan1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),
    close_connections([Conn1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0
        end),

    [Conn2] = open_connections(Config, [0]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 5
        end),

    [Conn3] = open_connections(Config, [0]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 2 andalso
            count_channels_of_user(Config, Username) =:= 10
        end),

    [Conn4] = open_connections(Config, [0]),
    _Chans4 = [_|_] = open_channels(Conn4, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 3 andalso
            count_channels_of_user(Config, Username) =:= 15
        end),

    close_connections([Conn4]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 2 andalso
            count_channels_of_user(Config, Username) =:= 10
        end),

    [Conn5] = open_connections(Config, [0]),
    Chans5 = [_|_] = open_channels(Conn5, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 3 andalso
            count_channels_of_user(Config, Username) =:= 15
        end),

    close_channels(Chans2 ++ Chans3 ++ Chans5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn5]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0
        end).

single_node_multiple_users_connection_and_channel_count(Config) ->
    Username1 = <<"guest1">>,
    Username2 = <<"guest2">>,

    set_up_user(Config, Username1),
    set_up_user(Config, Username2),

    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username1) =:= 0
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    [Conn1] = open_connections(Config, [{0, Username1}]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1 andalso
            count_channels_of_user(Config, Username1) =:= 5
        end),
    close_channels(Chans1),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0
        end),
    ?assertEqual(0, count_channels_of_user(Config, Username1)),
    close_connections([Conn1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username1) =:= 0
        end),

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 1 andalso
            count_channels_of_user(Config, Username2) =:= 5
        end),

    [Conn3] = open_connections(Config, [{0, Username1}]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1 andalso
            count_channels_of_user(Config, Username1) =:= 5
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 1 andalso
            count_channels_of_user(Config, Username2) =:= 5
        end),

    [Conn4] = open_connections(Config, [{0, Username1}]),
    _Chans4 = [_|_] = open_channels(Conn4, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 2 andalso
            count_channels_of_user(Config, Username1) =:= 10
        end),

    close_connections([Conn4]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1 andalso
            count_channels_of_user(Config, Username1) =:= 5
        end),

    [Conn5] = open_connections(Config, [{0, Username2}]),
    Chans5 = [_|_] = open_channels(Conn5, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 2 andalso
            count_channels_of_user(Config, Username2) =:= 10
        end),

    [Conn6] = open_connections(Config, [{0, Username2}]),
    Chans6 = [_|_] = open_channels(Conn6, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 3 andalso
            count_channels_of_user(Config, Username2) =:= 15
        end),

    close_channels(Chans2 ++ Chans3 ++ Chans5 ++ Chans6),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),

    rabbit_ct_broker_helpers:delete_user(Config, Username1),
    rabbit_ct_broker_helpers:delete_user(Config, Username2).

single_node_list_in_user(Config) ->
    Username1 = <<"guest1">>,
    Username2 = <<"guest2">>,

    set_up_user(Config, Username1),
    set_up_user(Config, Username2),

    rabbit_ct_helpers:await_condition(
    fun () ->
        length(connections_in(Config, Username1)) =:= 0 andalso
        length(connections_in(Config, Username2)) =:= 0
    end),

    ?assertEqual(0, length(channels_in(Config, Username1))),
    ?assertEqual(0, length(channels_in(Config, Username2))),

    [Conn1] = open_connections(Config, [{0, Username1}]),
    [Chan1] = open_channels(Conn1, 1),
    [#tracked_connection{username = Username1}] = connections_in(Config, Username1),
    [#tracked_channel{username = Username1}] = channels_in(Config, Username1),
    close_channels([Chan1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(channels_in(Config, Username1)) =:= 0
        end),
    close_connections([Conn1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(connections_in(Config, Username1)) =:= 0
        end),

    [Conn2] = open_connections(Config, [{0, Username2}]),
    [Chan2] = open_channels(Conn2, 1),
    [#tracked_connection{username = Username2}] = connections_in(Config, Username2),
    [#tracked_channel{username = Username2}] = channels_in(Config, Username2),

    [Conn3] = open_connections(Config, [{0, Username1}]),
    [Chan3] = open_channels(Conn3, 1),
    [#tracked_connection{username = Username1}] = connections_in(Config, Username1),
    [#tracked_channel{username = Username1}] = channels_in(Config, Username1),

    [Conn4] = open_connections(Config, [{0, Username1}]),
    [_Chan4] = open_channels(Conn4, 1),
    close_connections([Conn4]),
    [#tracked_connection{username = Username1}] = connections_in(Config, Username1),
    [#tracked_channel{username = Username1}] = channels_in(Config, Username1),

    [Conn5, Conn6] = open_connections(Config, [{0, Username2}, {0, Username2}]),
    [Chan5] = open_channels(Conn5, 1),
    [Chan6] = open_channels(Conn6, 1),
    [<<"guest1">>, <<"guest2">>] =
      lists:usort(lists:map(fun (#tracked_connection{username = V}) -> V end,
                     all_connections(Config))),
    [<<"guest1">>, <<"guest2">>] =
      lists:usort(lists:map(fun (#tracked_channel{username = V}) -> V end,
                      all_channels(Config))),

    close_channels([Chan2, Chan3, Chan5, Chan6]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(all_channels(Config)) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(all_connections(Config)) =:= 0
        end),

    rabbit_ct_broker_helpers:delete_user(Config, Username1),
    rabbit_ct_broker_helpers:delete_user(Config, Username2).

most_basic_cluster_connection_and_channel_count(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 5
        end),
    ?assertEqual(1, count_connections_of_user(Config, Username)),
    ?assertEqual(5, count_channels_of_user(Config, Username)),

    [Conn2] = open_connections(Config, [1]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?assertEqual(2, count_connections_of_user(Config, Username)),
    ?assertEqual(10, count_channels_of_user(Config, Username)),

    [Conn3] = open_connections(Config, [1]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    ?assertEqual(3, count_connections_of_user(Config, Username)),
    ?assertEqual(15, count_channels_of_user(Config, Username)),

    close_channels(Chans1 ++ Chans2 ++ Chans3),
    ?awaitMatch(0, count_channels_of_user(Config, Username), 60000, 3000),

    close_connections([Conn1, Conn2, Conn3]),
    ?awaitMatch(0, count_connections_of_user(Config, Username), 60000, 3000).

cluster_single_user_connection_and_channel_count(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    [Conn1] = open_connections(Config, [0]),
    _Chans1 = [_|_] = open_channels(Conn1, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 5
        end),
    
    close_connections([Conn1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    [Conn2] = open_connections(Config, [1]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 5
        end),

    [Conn3] = open_connections(Config, [0]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 2 andalso
            count_channels_of_user(Config, Username) =:= 10
        end),

    [Conn4] = open_connections(Config, [1]),
    Chans4 = [_|_] = open_channels(Conn4, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 3 andalso
            count_channels_of_user(Config, Username) =:= 15
        end),

    close_channels(Chans2 ++ Chans3 ++ Chans4),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn4]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0
        end).

cluster_multiple_users_connection_and_channel_count(Config) ->
    Username1 = <<"guest1">>,
    Username2 = <<"guest2">>,

    set_up_user(Config, Username1),
    set_up_user(Config, Username2),

    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    [Conn1] = open_connections(Config, [{0, Username1}]),
    _Chans1 = [_|_] = open_channels(Conn1, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1 andalso
            count_channels_of_user(Config, Username1) =:= 5
        end),
    close_connections([Conn1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username1) =:= 0
        end),

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 1 andalso
            count_channels_of_user(Config, Username2) =:= 5
        end),

    [Conn3] = open_connections(Config, [{1, Username1}]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1 andalso
            count_channels_of_user(Config, Username1) =:= 5
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 1 andalso
            count_channels_of_user(Config, Username2) =:= 5
        end),

    [Conn4] = open_connections(Config, [{0, Username1}]),
    _Chans4 = [_|_] = open_channels(Conn4, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 2 andalso
            count_channels_of_user(Config, Username1) =:= 10
        end),

    close_connections([Conn4]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1 andalso
            count_channels_of_user(Config, Username1) =:= 5
        end),

    [Conn5] = open_connections(Config, [{1, Username2}]),
    Chans5 = [_|_] = open_channels(Conn5, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 2 andalso
            count_channels_of_user(Config, Username2) =:= 10
        end),

    [Conn6] = open_connections(Config, [{0, Username2}]),
    Chans6 = [_|_] = open_channels(Conn6, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 3 andalso
            count_channels_of_user(Config, Username2) =:= 15
        end),

    close_channels(Chans2 ++ Chans3 ++ Chans5 ++ Chans6),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),

    rabbit_ct_broker_helpers:delete_user(Config, Username1),
    rabbit_ct_broker_helpers:delete_user(Config, Username2).

cluster_node_restart_connection_and_channel_count(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    [Conn1] = open_connections(Config, [0]),
    _Chans1 = [_|_] = open_channels(Conn1, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 5
        end),
    close_connections([Conn1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    [Conn2] = open_connections(Config, [1]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 5
        end),

    [Conn3] = open_connections(Config, [0]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 2 andalso
            count_channels_of_user(Config, Username) =:= 10
        end),

    [Conn4] = open_connections(Config, [1]),
    _Chans4 = [_|_] = open_channels(Conn4, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 3 andalso
            count_channels_of_user(Config, Username) =:= 15
        end),

    [Conn5] = open_connections(Config, [1]),
    Chans5 = [_|_] = open_channels(Conn5, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 4 andalso
            count_channels_of_user(Config, Username) =:= 20
        end),

    rabbit_ct_broker_helpers:restart_broker(Config, 1),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1 andalso
            count_channels_of_user(Config, Username) =:= 5
        end),

    close_channels(Chans2 ++ Chans3 ++ Chans5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn4, Conn5]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0
        end).

cluster_node_list_on_node(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    rabbit_ct_helpers:await_condition(
        fun () ->
            length(all_connections(Config)) =:= 0 andalso
            length(all_channels(Config)) =:= 0 andalso
            length(connections_on_node(Config, 0)) =:= 0 andalso
            length(channels_on_node(Config, 0)) =:= 0
        end),

    [Conn1] = open_connections(Config, [0]),
    _Chans1 = [_|_] = open_channels(Conn1, 5),
    [#tracked_connection{node = A}] = connections_on_node(Config, 0),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length([Ch || Ch <- channels_on_node(Config, 0), Ch#tracked_channel.node =:= A]) =:= 5
        end),
    close_connections([Conn1]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(connections_on_node(Config, 0)) =:= 0 andalso
            length(channels_on_node(Config, 0)) =:= 0
        end),

    [Conn2] = open_connections(Config, [1]),
    _Chans2 = [_|_] = open_channels(Conn2, 5),
    [#tracked_connection{node = B}] = connections_on_node(Config, 1),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length([Ch || Ch <- channels_on_node(Config, 1), Ch#tracked_channel.node =:= B]) =:= 5
        end),

    [Conn3] = open_connections(Config, [0]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(connections_on_node(Config, 0)) =:= 1 andalso
            length(channels_on_node(Config, 0)) =:= 5
        end),

    [Conn4] = open_connections(Config, [1]),
    _Chans4 = [_|_] = open_channels(Conn4, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(connections_on_node(Config, 1)) =:= 2 andalso
            length(channels_on_node(Config, 1)) =:= 10
        end),

    close_connections([Conn4]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(connections_on_node(Config, 1)) =:= 1 andalso
            length(channels_on_node(Config, 1)) =:= 5
        end),

    [Conn5] = open_connections(Config, [0]),
    Chans5 = [_|_] = open_channels(Conn5, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(connections_on_node(Config, 0)) =:= 2 andalso
            length(channels_on_node(Config, 0)) =:= 10
        end),

    rabbit_ct_broker_helpers:stop_broker(Config, 1),

    rabbit_ct_helpers:await_condition(
        fun () ->
            length(all_connections(Config)) =:= 2 andalso
            length(all_channels(Config)) =:= 10
        end),

    close_channels(Chans3 ++ Chans5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(all_channels(Config)) =:= 0
        end),

    close_connections([Conn3, Conn5]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            length(all_connections(Config)) =:= 0
        end),

    rabbit_ct_broker_helpers:start_broker(Config, 1).

single_node_single_user_limit(Config) ->
    single_node_single_user_limit_with(Config, 5, 25),
    single_node_single_user_limit_with(Config, -1, -1).

single_node_single_user_limit_with(Config, ConnLimit, ChLimit) ->
    Username = proplists:get_value(rmq_username, Config),
    set_user_connection_and_channel_limit(Config, Username, 3, 15),

    ?assertEqual(0, count_connections_of_user(Config, Username)),
    ?assertEqual(0, count_channels_of_user(Config, Username)),

    [Conn1, Conn2, Conn3] = Conns1 = open_connections(Config, [0, 0, 0]),
    [_Chans1, Chans2, Chans3] = [open_channels(Conn, 5) || Conn <- Conns1],

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_channel_is_rejected(Conn1),

    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(Conn1) =:= false andalso
            is_process_alive(Conn2) andalso
            is_process_alive(Conn3)
        end),

    set_user_connection_and_channel_limit(Config, Username, ConnLimit, ChLimit),
    [Conn4, Conn5] = Conns2 = open_connections(Config, [0, 0]),
    [Chans4, Chans5] = [open_channels(Conn, 5) || Conn <- Conns2],

    close_channels(Chans2 ++ Chans3 ++ Chans4 ++ Chans5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn1, Conn2, Conn3, Conn4, Conn5]),
    ?awaitMatch(0, count_connections_of_user(Config, Username), 60000, 3000),

    set_user_connection_and_channel_limit(Config, Username,  -1, -1).

single_node_single_user_zero_limit(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    set_user_connection_and_channel_limit(Config, Username, 0, 0),

    ?assertEqual(0, count_connections_of_user(Config, Username)),
    ?assertEqual(0, count_channels_of_user(Config, Username)),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config),
    expect_that_client_connection_is_rejected(Config),
    expect_that_client_connection_is_rejected(Config),

    %% with limit = 0 no channels are allowed
    set_user_connection_and_channel_limit(Config, Username, 1, 0),
    [ConnA] = open_connections(Config, [0]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1
        end),
    expect_that_client_channel_is_rejected(ConnA),
    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(ConnA) =:= false andalso
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    set_user_connection_and_channel_limit(Config, Username, -1, -1),
    [Conn1, Conn2] = Conns1 = open_connections(Config, [0, 0]),
    [Chans1, Chans2] = [open_channels(Conn, 5) || Conn <- Conns1],
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 2 andalso
            count_channels_of_user(Config, Username) =:= 10
        end),

    close_channels(Chans1 ++ Chans2),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn1, Conn2]),
    ?awaitMatch(0, count_connections_of_user(Config, Username), 60000, 3000).

single_node_single_user_clear_limits(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    set_user_connection_and_channel_limit(Config, Username, 3, 15),

    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    [Conn1, Conn2, Conn3] = Conns1 = open_connections(Config, [0, 0, 0]),
    [_Chans1, Chans2, Chans3] = [open_channels(Conn, 5) || Conn <- Conns1],

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_channel_is_rejected(Conn1),

    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(Conn1) =:= false andalso
            is_process_alive(Conn2) andalso
            is_process_alive(Conn3)
        end),

    %% reach limit again
    [Conn4] = open_connections(Config, [{0, Username}]),
    Chans4 = [_|_] = open_channels(Conn4, 5),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 3 andalso
            count_channels_of_user(Config, Username) =:= 15
        end),

    clear_all_user_limits(Config, Username),

    [Conn5, Conn6, Conn7] = Conns2 = open_connections(Config, [0, 0, 0]),
    [Chans5, Chans6, Chans7] = [open_channels(Conn, 5) || Conn <- Conns2],

    close_channels(Chans2 ++ Chans3 ++ Chans4 ++ Chans5 ++ Chans6 ++ Chans7),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn4, Conn5, Conn6, Conn7]),
    ?awaitMatch(0, count_connections_of_user(Config, Username), 5000, 1000),

    set_user_connection_and_channel_limit(Config, Username,  -1, -1).

single_node_multiple_users_clear_limits(Config) ->
    Username1 = <<"guest1">>,
    Username2 = <<"guest2">>,

    set_up_user(Config, Username1),
    set_up_user(Config, Username2),

    set_user_connection_and_channel_limit(Config, Username1, 0, 0),
    set_user_connection_and_channel_limit(Config, Username2, 0, 0),

    ?assertEqual(0, count_connections_of_user(Config, Username1)),
    ?assertEqual(0, count_connections_of_user(Config, Username2)),
    ?assertEqual(0, count_channels_of_user(Config, Username1)),
    ?assertEqual(0, count_channels_of_user(Config, Username2)),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0, Username1),
    expect_that_client_connection_is_rejected(Config, 0, Username2),
    expect_that_client_connection_is_rejected(Config, 0, Username1),

    %% with limit = 0 no channels are allowed
    set_user_connection_and_channel_limit(Config, Username1, 1, 0),
    set_user_connection_and_channel_limit(Config, Username2, 1, 0),
    [ConnA, ConnB] = open_connections(Config, [{0, Username1}, {0, Username2}]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1
        end),
    expect_that_client_channel_is_rejected(ConnA),
    expect_that_client_channel_is_rejected(ConnB),
    
    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(ConnA) =:= false andalso
            is_process_alive(ConnB) =:= false
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    clear_all_user_limits(Config, Username1),
    set_user_channel_limit_only(Config, Username2, -1),
    set_user_connection_limit_only(Config, Username2, -1),

    [Conn1, Conn2] = Conns1 = open_connections(Config, [{0, Username1}, {0, Username1}]),
    [Chans1, Chans2] = [open_channels(Conn, 5) || Conn <- Conns1],

    close_channels(Chans1 ++ Chans2),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    close_connections([Conn1, Conn2]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),

    set_user_connection_and_channel_limit(Config, Username1, -1, -1),
    set_user_connection_and_channel_limit(Config, Username2, -1, -1).

single_node_multiple_users_limit(Config) ->
    Username1 = <<"guest1">>,
    Username2 = <<"guest2">>,

    set_up_user(Config, Username1),
    set_up_user(Config, Username2),

    set_user_connection_and_channel_limit(Config, Username1, 2, 10),
    set_user_connection_and_channel_limit(Config, Username2, 2, 10),

    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    [Conn1, Conn2, Conn3, Conn4] = Conns1 = open_connections(Config, [
        {0, Username1},
        {0, Username1},
        {0, Username2},
        {0, Username2}]),

    [_Chans1, Chans2, Chans3, Chans4] = [open_channels(Conn, 5) || Conn <- Conns1],

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, Username1),
    expect_that_client_connection_is_rejected(Config, 0, Username2),
    expect_that_client_channel_is_rejected(Conn1),
    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(Conn1) =:= false andalso
            is_process_alive(Conn3) =:= true
        end),

    [Conn5] = open_connections(Config, [0]),
    Chans5 = [_|_] = open_channels(Conn5, 5),

    set_user_connection_and_channel_limit(Config, Username1, 5, 25),
    set_user_connection_and_channel_limit(Config, Username2, -10, -50),

    [Conn6, Conn7, Conn8, Conn9, Conn10] = Conns2 = open_connections(Config, [
        {0, Username1},
        {0, Username1},
        {0, Username1},
        {0, Username2},
        {0, Username2}]),

    [Chans6, Chans7, Chans8, Chans9, Chans10] = [open_channels(Conn, 5) || Conn <- Conns2],

    close_channels(Chans2 ++ Chans3 ++ Chans4 ++ Chans5 ++ Chans6 ++
                   Chans7 ++ Chans8 ++ Chans9 ++ Chans10),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn4, Conn5, Conn6,
                       Conn7, Conn8, Conn9, Conn10]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),

    set_user_connection_and_channel_limit(Config, Username1, -1, -1),
    set_user_connection_and_channel_limit(Config, Username2, -1, -1),

    rabbit_ct_broker_helpers:delete_user(Config, Username1),
    rabbit_ct_broker_helpers:delete_user(Config, Username2).


single_node_multiple_users_zero_limit(Config) ->
    Username1 = <<"guest1">>,
    Username2 = <<"guest2">>,

    set_up_user(Config, Username1),
    set_up_user(Config, Username2),

    set_user_connection_and_channel_limit(Config, Username1, 0, 0),
    set_user_connection_and_channel_limit(Config, Username2, 0, 0),

    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0, Username1),
    expect_that_client_connection_is_rejected(Config, 0, Username2),
    expect_that_client_connection_is_rejected(Config, 0, Username1),

    %% with limit = 0 no channels are allowed
    set_user_connection_and_channel_limit(Config, Username1, 1, 0),
    set_user_connection_and_channel_limit(Config, Username2, 1, 0),
    [ConnA, ConnB] = open_connections(Config, [{0, Username1}, {0, Username2}]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1
        end),
    expect_that_client_channel_is_rejected(ConnA),
    expect_that_client_channel_is_rejected(ConnB),
    
    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(ConnA) =:= false andalso
            is_process_alive(ConnB) =:= false
        end),
    
    ?assertEqual(false, is_process_alive(ConnA)),
    ?assertEqual(false, is_process_alive(ConnB)),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    set_user_connection_and_channel_limit(Config, Username1, -1, -1),
    [Conn1, Conn2] = Conns1 = open_connections(Config, [{0, Username1}, {0, Username1}]),
    [Chans1, Chans2] = [open_channels(Conn, 5) || Conn <- Conns1],

    close_channels(Chans1 ++ Chans2),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    close_connections([Conn1, Conn2]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),

    set_user_connection_and_channel_limit(Config, Username1, -1, -1),
    set_user_connection_and_channel_limit(Config, Username2, -1, -1).


cluster_single_user_limit(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    set_user_connection_limit_only(Config, Username, 2),
    set_user_channel_limit_only(Config, Username, 10),

    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    %% here connections and channels are opened to different nodes
    [Conn1, Conn2] = Conns1 = open_connections(Config, [{0, Username}, {1, Username}]),
    [_Chans1, Chans2] = [open_channels(Conn, 5) || Conn <- Conns1],

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, Username),
    expect_that_client_connection_is_rejected(Config, 1, Username),
    expect_that_client_channel_is_rejected(Conn1),
    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(Conn1) =:= false andalso
            is_process_alive(Conn2) =:= true
        end),

    set_user_connection_and_channel_limit(Config, Username, 5, 25),

    [Conn3, Conn4] = Conns2 = open_connections(Config, [{0, Username}, {0, Username}]),
    [Chans3, Chans4] = [open_channels(Conn, 5) || Conn <- Conns2],

    close_channels(Chans2 ++ Chans3 ++ Chans4),
    ?awaitMatch(0, count_channels_of_user(Config, Username), 60000, 3000),

    close_connections([Conn2, Conn3, Conn4]),
    ?awaitMatch(0, count_connections_of_user(Config, Username), 60000, 3000),

    set_user_connection_and_channel_limit(Config, Username,  -1, -1).

cluster_single_user_limit2(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    set_user_connection_and_channel_limit(Config, Username, 2, 10),

    ?assertEqual(0, count_connections_of_user(Config, Username)),
    ?assertEqual(0, count_channels_of_user(Config, Username)),

    %% here a limit is reached on one node first
    [Conn1, Conn2] = Conns1 = open_connections(Config, [{0, Username}, {0, Username}]),
    [_Chans1, Chans2] = [open_channels(Conn, 5) || Conn <- Conns1],

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, Username),
    expect_that_client_connection_is_rejected(Config, 1, Username),
    expect_that_client_channel_is_rejected(Conn1),
    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(Conn1) =:= false andalso
            is_process_alive(Conn2) =:= true
        end),

    set_user_connection_and_channel_limit(Config, Username, 5, 25),

    [Conn3, Conn4, Conn5, Conn6, {error, not_allowed}] = open_connections(Config, [
        {1, Username},
        {1, Username},
        {1, Username},
        {1, Username},
        {1, Username}]),

    [Chans3, Chans4, Chans5, Chans6, [{error, not_allowed}]] =
        [open_channels(Conn, 1) || Conn <- [Conn3, Conn4, Conn5, Conn6, Conn1]],

    close_channels(Chans2 ++ Chans3 ++ Chans4 ++ Chans5 ++ Chans6),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn4, Conn5, Conn6]),
    ?awaitMatch(0, count_connections_of_user(Config, Username), 5000, 1000),

    set_user_connection_and_channel_limit(Config, Username,  -1, -1).


cluster_single_user_zero_limit(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    set_user_connection_and_channel_limit(Config, Username, 0, 0),

    ?assertEqual(0, count_connections_of_user(Config, Username)),
    ?assertEqual(0, count_channels_of_user(Config, Username)),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_connection_is_rejected(Config, 1),
    expect_that_client_connection_is_rejected(Config, 0),

    %% with limit = 0 no channels are allowed
    set_user_connection_and_channel_limit(Config, Username, 1, 0),
    [ConnA] = open_connections(Config, [0]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 1
        end),
    expect_that_client_channel_is_rejected(ConnA),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),
    ?assertEqual(false, is_process_alive(ConnA)),

    set_user_connection_and_channel_limit(Config, Username, -1, -1),
    [Conn1, Conn2, Conn3, Conn4] = Conns1 = open_connections(Config, [0, 1, 0, 1]),
    [Chans1, Chans2, Chans3, Chans4] = [open_channels(Conn, 5) || Conn <- Conns1],

    close_channels(Chans1 ++ Chans2 ++ Chans3 ++ Chans4),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0
        end),

    set_user_connection_and_channel_limit(Config, Username, -1, -1).

cluster_single_user_clear_limits(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    set_user_connection_and_channel_limit(Config, Username, 2, 10),

    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0 andalso
            count_channels_of_user(Config, Username) =:= 0
        end),

    %% here a limit is reached on one node first
    [Conn1, Conn2] = Conns1 = open_connections(Config, [{0, Username}, {0, Username}]),
    [_Chans1, Chans2] = [open_channels(Conn, 5) || Conn <- Conns1],

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, Username),
    expect_that_client_connection_is_rejected(Config, 1, Username),
    expect_that_client_channel_is_rejected(Conn1),
    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(Conn1) =:= false andalso
            is_process_alive(Conn2) =:= true
        end),
    clear_all_user_limits(Config, Username),

    [Conn3, Conn4, Conn5, Conn6, Conn7] = open_connections(Config, [
        {1, Username},
        {1, Username},
        {1, Username},
        {1, Username},
        {1, Username}]),

    [Chans3, Chans4, Chans5, Chans6, Chans7] =
        [open_channels(Conn, 1) || Conn <- [Conn3, Conn4, Conn5, Conn6, Conn7]],

    close_channels(Chans2 ++ Chans3 ++ Chans4 ++ Chans5 ++ Chans6 ++ Chans7),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username) =:= 0
        end),

    close_connections([Conn2, Conn3, Conn4, Conn5, Conn6, Conn7]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username) =:= 0
        end),

    set_user_connection_and_channel_limit(Config, Username,  -1, -1).

cluster_multiple_users_clear_limits(Config) ->
    Username1 = <<"guest1">>,
    Username2 = <<"guest2">>,

    set_up_user(Config, Username1),
    set_up_user(Config, Username2),

    set_user_connection_and_channel_limit(Config, Username1, 0, 0),
    set_user_connection_and_channel_limit(Config, Username2, 0, 0),

    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0, Username1),
    expect_that_client_connection_is_rejected(Config, 0, Username2),
    expect_that_client_connection_is_rejected(Config, 1, Username1),
    expect_that_client_connection_is_rejected(Config, 1, Username2),

    %% with limit = 0 no channels are allowed
    set_user_connection_and_channel_limit(Config, Username1, 1, 0),
    set_user_connection_and_channel_limit(Config, Username2, 1, 0),
    [ConnA, ConnB] = open_connections(Config, [{0, Username1}, {1, Username2}]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 1 andalso
            count_connections_of_user(Config, Username2) =:= 1
        end),
    expect_that_client_channel_is_rejected(ConnA),
    
    rabbit_ct_helpers:await_condition(
        fun () ->
            is_process_alive(ConnA) =:= false andalso
            is_process_alive(ConnB) =:= true
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 1
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),
    close_connections([ConnB]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username2) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),
    ?assertEqual(false, is_process_alive(ConnB)),

    clear_all_user_limits(Config, Username1),
    clear_all_user_limits(Config, Username2),

    [Conn1, Conn2, Conn3, Conn4] = Conns1 = open_connections(Config, [
        {0, Username1},
        {0, Username2},
        {1, Username1},
        {1, Username2}]),

    [Chans1, Chans2, Chans3, Chans4] = [open_channels(Conn, 5) || Conn <- Conns1],

    close_channels(Chans1 ++ Chans2 ++ Chans3 ++ Chans4),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),

    set_user_connection_and_channel_limit(Config, Username1, -1, -1),
    set_user_connection_and_channel_limit(Config, Username2, -1, -1).

cluster_multiple_users_zero_limit(Config) ->
    Username1 = <<"guest1">>,
    Username2 = <<"guest2">>,

    set_up_user(Config, Username1),
    set_up_user(Config, Username2),

    set_user_connection_and_channel_limit(Config, Username1, 0, 0),
    set_user_connection_and_channel_limit(Config, Username2, 0, 0),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 0
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0, Username1),
    expect_that_client_connection_is_rejected(Config, 0, Username2),
    expect_that_client_connection_is_rejected(Config, 1, Username1),
    expect_that_client_connection_is_rejected(Config, 1, Username2),

    %% with limit = 0 no channels are allowed
    set_user_connection_and_channel_limit(Config, Username1, 1, 0),
    set_user_connection_and_channel_limit(Config, Username2, 1, 0),
    [ConnA, ConnB] = open_connections(Config, [{0, Username1}, {1, Username2}]),
    
    expect_that_client_channel_is_rejected(ConnA),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_connections_of_user(Config, Username1) =:= 0 andalso
            count_connections_of_user(Config, Username2) =:= 1
        end),
    rabbit_ct_helpers:await_condition(
        fun () ->
            count_channels_of_user(Config, Username1) =:= 0 andalso
            count_channels_of_user(Config, Username2) =:= 0
        end),
    ?assertEqual(false, is_process_alive(ConnA)),
    ?assertEqual(true, is_process_alive(ConnB)),
    close_connections([ConnB]),
    rabbit_ct_helpers:await_condition(
      fun () ->
              count_connections_of_user(Config, Username2) =:= 0 andalso
              count_channels_of_user(Config, Username2) =:= 0
      end),
    ?assertEqual(false, is_process_alive(ConnB)),

    set_user_connection_and_channel_limit(Config, Username1, -1, -1),
    set_user_connection_and_channel_limit(Config, Username2, -1, -1),

    [Conn1, Conn2, Conn3, Conn4] = Conns1 = open_connections(Config, [
        {0, Username1},
        {0, Username2},
        {1, Username1},
        {1, Username2}]),

    [Chans1, Chans2, Chans3, Chans4] = [open_channels(Conn, 5) || Conn <- Conns1],

    close_channels(Chans1 ++ Chans2 ++ Chans3 ++ Chans4),
    ?awaitMatch(0, count_channels_of_user(Config, Username1), 60000, 3000),
    ?awaitMatch(0, count_channels_of_user(Config, Username2), 60000, 3000),

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    ?awaitMatch(0, count_connections_of_user(Config, Username1), 60000, 3000),
    ?awaitMatch(0, count_connections_of_user(Config, Username2), 60000, 3000),

    set_user_connection_and_channel_limit(Config, Username1, -1, -1),
    set_user_connection_and_channel_limit(Config, Username2, -1, -1).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

open_connections(Config, NodesAndUsers) ->
    % Randomly select connection type
    OpenConnectionFun = case ?config(connection_type, Config) of
        network -> open_unmanaged_connection;
        direct  -> open_unmanaged_connection_direct
    end,
    Conns = lists:map(fun
      ({Node, User}) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node,
                                                     User, User);
      (Node) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node)
      end, NodesAndUsers),
    timer:sleep(100),
    Conns.

close_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          rabbit_ct_client_helpers:close_connection(Conn)
      end, Conns).

open_channels(Conn, N) ->
    [open_channel(Conn) || _ <- lists:seq(1, N)].

open_channel(Conn) when is_pid(Conn) ->
    try amqp_connection:open_channel(Conn) of
      {ok, Ch} -> Ch;
      {error, _} ->
            {error, not_allowed}
    catch
      _:_Error -> {error, not_allowed}
   end.

close_channels(Channels = [_|_]) ->
    [rabbit_ct_client_helpers:close_channel(Ch) || Ch <- Channels].

count_connections_of_user(Config, Username) ->
    count_connections_in(Config, Username, 0).
count_connections_in(Config, Username, NodeIndex) ->
    count_user_tracked_items(Config, NodeIndex, rabbit_connection_tracking, Username).

count_channels_of_user(Config, Username) ->
    count_channels_in(Config, Username, 0).
count_channels_in(Config, Username, NodeIndex) ->
    count_user_tracked_items(Config, NodeIndex, rabbit_channel_tracking, Username).

count_user_tracked_items(Config, NodeIndex, TrackingMod, Username) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 TrackingMod,
                                 count_tracked_items_in, [{user, Username}]).

connections_in(Config, Username) ->
    connections_in(Config, 0, Username).
connections_in(Config, NodeIndex, Username) ->
    tracked_list_of_user(Config, NodeIndex, rabbit_connection_tracking, Username).

channels_in(Config, Username) ->
    channels_in(Config, 0, Username).
channels_in(Config, NodeIndex, Username) ->
    tracked_list_of_user(Config, NodeIndex, rabbit_channel_tracking, Username).

tracked_list_of_user(Config, NodeIndex, TrackingMod, Username) ->
   rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                TrackingMod,
                                list_of_user, [Username]).

connections_on_node(Config) ->
    connections_on_node(Config, 0).
connections_on_node(Config, NodeIndex) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, NodeIndex, nodename),
    tracked_items_on_node(Config, NodeIndex, rabbit_connection_tracking, Node).

channels_on_node(Config) ->
    channels_on_node(Config, 0).
channels_on_node(Config, NodeIndex) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, NodeIndex, nodename),
    tracked_items_on_node(Config, NodeIndex, rabbit_channel_tracking, Node).

tracked_items_on_node(Config, NodeIndex, TrackingMod, NodeForListing) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 TrackingMod,
                                 list_on_node, [NodeForListing]).

all_connections(Config) ->
    all_connections(Config, 0).
all_connections(Config, NodeIndex) ->
    all_tracked_items(Config, NodeIndex, rabbit_connection_tracking).

all_channels(Config) ->
    all_channels(Config, 0).
all_channels(Config, NodeIndex) ->
    all_tracked_items(Config, NodeIndex, rabbit_channel_tracking).

all_tracked_items(Config, NodeIndex, TrackingMod) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 TrackingMod,
                                 list, []).

set_up_user(Config, Username) ->
    VHost = proplists:get_value(rmq_vhost, Config),
    rabbit_ct_broker_helpers:add_user(Config, Username),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username, VHost),
    set_user_connection_and_channel_limit(Config, Username, -1, -1).

set_user_connection_and_channel_limit(Config, Username, ConnLimit, ChLimit) ->
    set_user_connection_and_channel_limit(Config, 0, Username, ConnLimit, ChLimit).

set_user_connection_and_channel_limit(Config, NodeIndex, Username, ConnLimit, ChLimit) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    ok = rabbit_ct_broker_helpers:control_action(
      set_user_limits, Node, [rabbit_data_coercion:to_list(Username)] ++
      ["{\"max-connections\": " ++ integer_to_list(ConnLimit) ++ "," ++
       " \"max-channels\": " ++ integer_to_list(ChLimit) ++ "}"]).

set_user_connection_limit_only(Config, Username, ConnLimit) ->
    set_user_connection_limit_only(Config, 0, Username, ConnLimit).

set_user_connection_limit_only(Config, NodeIndex, Username, ConnLimit) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
             Config, NodeIndex, nodename),
    ok = rabbit_ct_broker_helpers:control_action(
      set_user_limits, Node, [rabbit_data_coercion:to_list(Username)] ++
      ["{\"max-connections\": " ++ integer_to_list(ConnLimit) ++ "}"]).

set_user_channel_limit_only(Config, Username, ChLimit) ->
    set_user_channel_limit_only(Config, 0, Username, ChLimit).

set_user_channel_limit_only(Config, NodeIndex, Username, ChLimit) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
             Config, NodeIndex, nodename),
    ok = rabbit_ct_broker_helpers:control_action(
      set_user_limits, Node, [rabbit_data_coercion:to_list(Username)] ++
      ["{\"max-channels\": " ++ integer_to_list(ChLimit) ++ "}"]).

clear_all_user_limits(Config, Username) ->
    clear_all_user_limits(Config, 0, Username).
clear_all_user_limits(Config, NodeIndex, Username) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    ok = rabbit_ct_broker_helpers:control_action(
        clear_user_limits, Node, [rabbit_data_coercion:to_list(Username), "all"]).

expect_that_client_connection_is_rejected(Config) ->
    expect_that_client_connection_is_rejected(Config, 0).

expect_that_client_connection_is_rejected(Config, NodeIndex) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex).

expect_that_client_connection_is_rejected(Config, NodeIndex, User) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex, User, User).

expect_that_client_channel_is_rejected(Conn) ->
    {error, not_allowed} = open_channel(Conn).
