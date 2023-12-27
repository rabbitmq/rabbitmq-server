%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_user_connection_tracking_SUITE).

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(AWAIT_TIMEOUT, 30000).

all() ->
    [
     {group, mnesia_store},
     {group, khepri_store}
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
     {mnesia_store, [], [
                         {cluster_size_1_network, [], ClusterSize1Tests},
                         {cluster_size_2_network, [], ClusterSize2Tests},
                         {cluster_size_1_direct, [], ClusterSize1Tests},
                         {cluster_size_2_direct, [], ClusterSize2Tests}
                        ]},
     {khepri_store, [], [
                         {cluster_size_1_network, [], ClusterSize1Tests},
                         {cluster_size_2_network, [], ClusterSize2Tests},
                         {cluster_size_1_direct, [], ClusterSize1Tests},
                         {cluster_size_2_direct, [], ClusterSize2Tests}
                        ]}
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

init_per_group(mnesia_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, mnesia}]);
init_per_group(khepri_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, khepri}]);
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

end_per_group(Group, Config) when Group == mnesia_store; Group == khepri_store ->
    % The broker is managed by {init,end}_per_testcase().
    Config;
end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------
single_node_list_of_user(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    [Conn1] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, Username), ?AWAIT_TIMEOUT),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),
    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username), ?AWAIT_TIMEOUT),

    [Conn2] = open_connections(Config, [{0, Username2}]),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),
    [#tracked_connection{username = Username2}] = connections_in(Config, Username2),

    [Conn3] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, Username), ?AWAIT_TIMEOUT),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),

    [Conn4] = open_connections(Config, [0]),
    kill_connections([Conn4]),
    ?awaitMatch(1, count_connections_in(Config, Username), ?AWAIT_TIMEOUT),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),

    [Conn5] = open_connections(Config, [0]),
    ?awaitMatch(2, count_connections_in(Config, Username), ?AWAIT_TIMEOUT),
    [Username, Username] =
        lists:map(fun (#tracked_connection{username = U}) -> U end,
                  connections_in(Config, Username)),

    close_connections([Conn2, Conn3, Conn5]),
    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, length(all_connections(Config)), ?AWAIT_TIMEOUT).

single_node_user_deletion_forces_connection_closure(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    [Conn1] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, Username), ?AWAIT_TIMEOUT),

    [_Conn2] = open_connections(Config, [{0, Username2}]),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username), ?AWAIT_TIMEOUT).

cluster_user_deletion_forces_connection_closure(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    [Conn1] = open_connections(Config, [{0, Username}]),
    ?awaitMatch(1, count_connections_in(Config, Username), ?AWAIT_TIMEOUT),

    [_Conn2] = open_connections(Config, [{1, Username2}]),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username), ?AWAIT_TIMEOUT).

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
    Conns.

close_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          rabbit_ct_client_helpers:close_connection(Conn)
      end, Conns).

kill_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          (catch exit(Conn, please_terminate))
      end, Conns).


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
