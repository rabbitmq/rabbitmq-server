%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(per_user_connection_tracking_SUITE).

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(AWAIT_TIMEOUT, 30000).

all() ->
    [
     {group, tests}
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
     {tests, [], [
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
    init_per_multinode_group(cluster_size_2_direct, Config1, 2);
init_per_group(tests, Config) ->
    Config.

init_per_multinode_group(_Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                    rabbit_ct_client_helpers:setup_steps()).

end_per_group(tests, Config) ->
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
    Username1 = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "-1"),
    Username2 = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "-2"),

    Vhost = proplists:get_value(rmq_vhost, Config),

    [ begin
          rabbit_ct_broker_helpers:add_user(Config, U),
          rabbit_ct_broker_helpers:set_full_permissions(Config, U, Vhost)
      end || U <- [Username1, Username2]],

    ?assertEqual(0, count_connections_in(Config, Username1)),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    [Conn1] = open_connections(Config, [{0, Username1}]),
    ?awaitMatch(1, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT),
    [#tracked_connection{username = Username1}] = connections_in(Config, Username1),
    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT),

    [Conn2] = open_connections(Config, [{0, Username2}]),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),
    [#tracked_connection{username = Username2}] = connections_in(Config, Username2),

    [Conn3] = open_connections(Config, [{0, Username1}]),
    ?awaitMatch(1, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT),
    [#tracked_connection{username = Username1}] = connections_in(Config, Username1),

    [Conn4] = open_connections(Config, [{0, Username1}]),
    kill_connections([Conn4]),
    ?awaitMatch(1, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT),
    [#tracked_connection{username = Username1}] = connections_in(Config, Username1),

    [Conn5] = open_connections(Config, [{0, Username1}]),
    ?awaitMatch(2, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT),
    [Username1, Username1] =
        lists:map(fun (#tracked_connection{username = U}) -> U end,
                  connections_in(Config, Username1)),

    close_connections([Conn2, Conn3, Conn5]),
    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, length(all_connections(Config)), ?AWAIT_TIMEOUT).

single_node_user_deletion_forces_connection_closure(Config) ->
    Username1 = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "-1"),
    Username2 = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "-2"),

    Vhost = proplists:get_value(rmq_vhost, Config),

    [ begin
          rabbit_ct_broker_helpers:add_user(Config, U),
          rabbit_ct_broker_helpers:set_full_permissions(Config, U, Vhost)
      end || U <- [Username1, Username2]],

    ?assertEqual(0, count_connections_in(Config, Username1)),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    [Conn1] = open_connections(Config, [{0, Username1}]),
    ?awaitMatch(1, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT),

    [_Conn2] = open_connections(Config, [{0, Username2}]),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT).

cluster_user_deletion_forces_connection_closure(Config) ->
    Username1 = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "-1"),
    Username2 = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "-2"),

    Vhost = proplists:get_value(rmq_vhost, Config),

    [ begin
          rabbit_ct_broker_helpers:add_user(Config, U),
          rabbit_ct_broker_helpers:set_full_permissions(Config, U, Vhost)
      end || U <- [Username1, Username2]],

    ?assertEqual(0, count_connections_in(Config, Username1)),
    ?assertEqual(0, count_connections_in(Config, Username2)),

    [Conn1] = open_connections(Config, [{0, Username1}]),
    ?awaitMatch(1, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT),

    [_Conn2] = open_connections(Config, [{1, Username2}]),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?AWAIT_TIMEOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username1), ?AWAIT_TIMEOUT).

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
