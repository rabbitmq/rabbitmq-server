%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_user_connection_tracking_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, cluster_size_1_network},
     {group, cluster_size_2_network},
     {group, cluster_size_1_direct},
     {group, cluster_size_2_direct}
    ].

groups() ->
    ClusterSize1Tests = [
        single_node_list_of_user,
        single_node_user_deletion_forces_connection_closure
    ],
    ClusterSize2Tests = [
        cluster_user_deletion_forces_connection_closure
    ],
    [
      {cluster_size_1_network, [], ClusterSize1Tests},
      {cluster_size_2_network, [], ClusterSize2Tests},
      {cluster_size_1_direct, [], ClusterSize1Tests},
      {cluster_size_2_direct, [], ClusterSize2Tests}
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
    rabbit_ct_helpers:run_setup_steps(Config, [
                                               fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1
                                              ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1_network, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_multinode_group(cluster_size_1_network, Config1, 1);
init_per_group(cluster_size_2_network, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_multinode_group(cluster_size_2_network, Config1, 2);
init_per_group(cluster_size_1_direct, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
    init_per_multinode_group(cluster_size_1_direct, Config1, 1);
init_per_group(cluster_size_2_direct, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
    init_per_multinode_group(cluster_size_2_direct, Config1, 2).

init_per_multinode_group(_Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                    rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    clear_all_connection_tracking_tables(Config),
    Config.

end_per_testcase(Testcase, Config) ->
    clear_all_connection_tracking_tables(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

clear_all_connection_tracking_tables(Config) ->
    [rabbit_ct_broker_helpers:rpc(Config,
        N,
        rabbit_connection_tracking,
        clear_tracked_connection_tables_for_this_node,
        []) || N <- rabbit_ct_broker_helpers:get_node_configs(Config, nodename)].

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------
single_node_list_of_user(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, length(connections_in(Config, Username))),
    ?assertEqual(0, length(connections_in(Config, Username2))),

    [Conn1] = open_connections(Config, [0]),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),
    close_connections([Conn1]),
    ?assertEqual(0, length(connections_in(Config, Username))),

    [Conn2] = open_connections(Config, [{0, Username2}]),
    [#tracked_connection{username = Username2}] = connections_in(Config, Username2),

    [Conn3] = open_connections(Config, [0]),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),

    [Conn4] = open_connections(Config, [0]),
    kill_connections([Conn4]),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),

    [Conn5] = open_connections(Config, [0]),
    [Username, Username] =
        lists:map(fun (#tracked_connection{username = U}) -> U end,
                  connections_in(Config, Username)),

    close_connections([Conn2, Conn3, Conn5]),
    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?assertEqual(0, length(all_connections(Config))).

single_node_user_deletion_forces_connection_closure(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    [Conn1] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_in(Config, Username)),

    [_Conn2] = open_connections(Config, [{0, Username2}]),
    ?assertEqual(1, count_connections_in(Config, Username2)),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, Username)).

cluster_user_deletion_forces_connection_closure(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    [Conn1] = open_connections(Config, [{0, Username}]),
    ?assertEqual(1, count_connections_in(Config, Username)),

    [_Conn2] = open_connections(Config, [{1, Username2}]),
    ?assertEqual(1, count_connections_in(Config, Username2)),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, Username)).

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
    timer:sleep(500),
    Conns.

close_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          rabbit_ct_client_helpers:close_connection(Conn)
      end, Conns),
    timer:sleep(500).

kill_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          (catch exit(Conn, please_terminate))
      end, Conns),
    timer:sleep(500).


count_connections_in(Config, Username) ->
    length(connections_in(Config, Username)).

connections_in(Config, Username) ->
    connections_in(Config, 0, Username).
connections_in(Config, NodeIndex, Username) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list_of_user, [Username]).

all_connections(Config) ->
    all_connections(Config, 0).
all_connections(Config, NodeIndex) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list, []).

set_up_vhost(Config, VHost) ->
    rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    set_vhost_connection_limit(Config, VHost, -1).

set_vhost_connection_limit(Config, VHost, Count) ->
    set_vhost_connection_limit(Config, 0, VHost, Count).

set_vhost_connection_limit(Config, NodeIndex, VHost, Count) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    ok = rabbit_ct_broker_helpers:control_action(
      set_vhost_limits, Node,
      ["{\"max-connections\": " ++ integer_to_list(Count) ++ "}"],
      [{"-p", binary_to_list(VHost)}]).

await_running_node_refresh(_Config, _NodeIndex) ->
    timer:sleep(250).

expect_that_client_connection_is_rejected(Config) ->
    expect_that_client_connection_is_rejected(Config, 0).

expect_that_client_connection_is_rejected(Config, NodeIndex) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex).

expect_that_client_connection_is_rejected(Config, NodeIndex, VHost) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex, VHost).
