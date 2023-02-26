%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(vhost_SUITE).

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
        vhost_is_created_with_default_limits,
        vhost_is_created_with_operator_policies,
        vhost_is_created_with_default_user,
        single_node_vhost_deletion_forces_connection_closure,
        vhost_failure_forces_connection_closure,
        vhost_creation_idempotency,
        vhost_update_idempotency,
        parse_tags
    ],
    ClusterSize2Tests = [
        cluster_vhost_deletion_forces_connection_closure,
        vhost_failure_forces_connection_closure,
        vhost_failure_forces_connection_closure_on_failure_node,
        node_starts_with_dead_vhosts,
        node_starts_with_dead_vhosts_with_mirrors,
        vhost_creation_idempotency
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

%% see partitions_SUITE
-define(DELAY, 9000).

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
    Config.

end_per_testcase(Testcase, Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,
    case Testcase of
        cluster_vhost_deletion_forces_connection_closure -> ok;
        single_node_vhost_deletion_forces_connection_closure -> ok;
        _ ->
            delete_vhost(Config, VHost2)
    end,
    delete_vhost(Config, VHost1),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

delete_vhost(Config, VHost) ->
    case rabbit_ct_broker_helpers:delete_vhost(Config, VHost) of
        ok                          -> ok;
        {error, {no_such_vhost, _}} -> ok
    end.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

single_node_vhost_deletion_forces_connection_closure(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    [_Conn2] = open_connections(Config, [{0, VHost2}]),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost1)).

vhost_failure_forces_connection_closure(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    [_Conn2] = open_connections(Config, [{0, VHost2}]),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:force_vhost_failure(Config, VHost2),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost1)).


vhost_failure_forces_connection_closure_on_failure_node(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    [_Conn20] = open_connections(Config, [{0, VHost2}]),
    [_Conn21] = open_connections(Config, [{1, VHost2}]),
    ?assertEqual(2, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:force_vhost_failure(Config, 0, VHost2),
    timer:sleep(200),
    %% Vhost2 connection on node 1 is still alive
    ?assertEqual(1, count_connections_in(Config, VHost2)),
    %% Vhost1 connection on node 0 is still alive
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost1)).


cluster_vhost_deletion_forces_connection_closure(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    [_Conn2] = open_connections(Config, [{1, VHost2}]),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost1)).

node_starts_with_dead_vhosts(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 1, VHost1),
    {ok, Chan} = amqp_connection:open_channel(Conn),

    QName = <<"node_starts_with_dead_vhosts-q-1">>,
    amqp_channel:call(Chan, #'queue.declare'{queue = QName, durable = true}),
    rabbit_ct_client_helpers:publish(Chan, QName, 10),

    DataStore1 = rabbit_ct_broker_helpers:rpc(
        Config, 1, rabbit_vhost, msg_store_dir_path, [VHost1]),

    rabbit_ct_broker_helpers:stop_node(Config, 1),

    file:write_file(filename:join(DataStore1, "recovery.dets"), <<"garbage">>),

    %% The node should start without a vhost
    ok = rabbit_ct_broker_helpers:start_node(Config, 1),

    timer:sleep(3000),

    ?assertEqual(true, rabbit_ct_broker_helpers:rpc(Config, 1,
                        rabbit_vhost_sup_sup, is_vhost_alive, [VHost2])).

node_starts_with_dead_vhosts_with_mirrors(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    true = rabbit_ct_broker_helpers:rpc(Config, 1,
                rabbit_vhost_sup_sup, is_vhost_alive, [VHost1]),
    true = rabbit_ct_broker_helpers:rpc(Config, 1,
                rabbit_vhost_sup_sup, is_vhost_alive, [VHost2]),
    [] = rabbit_ct_broker_helpers:rpc(Config, 1,
                rabbit_vhost_sup_sup, check, []),

    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost1),
    {ok, Chan} = amqp_connection:open_channel(Conn),

    QName = <<"node_starts_with_dead_vhosts_with_mirrors-q-0">>,
    amqp_channel:call(Chan, #'queue.declare'{queue = QName, durable = true}),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
             rabbit_policy, set,
             [VHost1, <<"mirror">>, <<".*">>, [{<<"ha-mode">>, <<"all">>}],
              0, <<"queues">>, <<"acting-user">>]),

    %% Wait for the queue to start a mirror
    timer:sleep(500),

    rabbit_ct_client_helpers:publish(Chan, QName, 10),

    {ok, Q} = rabbit_ct_broker_helpers:rpc(
                Config, 0,
                rabbit_amqqueue, lookup,
                [rabbit_misc:r(VHost1, queue, QName)], infinity),

    Node1 = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),

    [Pid] = amqqueue:get_sync_slave_pids(Q),

    Node1 = node(Pid),

    DataStore1 = rabbit_ct_broker_helpers:rpc(
        Config, 1, rabbit_vhost, msg_store_dir_path, [VHost1]),

    rabbit_ct_broker_helpers:stop_node(Config, 1),

    file:write_file(filename:join(DataStore1, "recovery.dets"), <<"garbage">>),

    %% The node should start without a vhost
    ok = rabbit_ct_broker_helpers:start_node(Config, 1),

    timer:sleep(3000),

    ?assertEqual(true, rabbit_ct_broker_helpers:rpc(Config, 1,
                        rabbit_vhost_sup_sup, is_vhost_alive, [VHost2])).

vhost_creation_idempotency(Config) ->
    VHost = <<"idempotency-test">>,
    try
        ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),
        ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),
        ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost))
    after
        rabbit_ct_broker_helpers:delete_vhost(Config, VHost)
    end.

vhost_update_idempotency(Config) ->
    VHost = <<"update-idempotency-test">>,
    ActingUser = <<"acting-user">>,
    try
        % load the dummy event handler on the node
        ok = rabbit_ct_broker_helpers:rpc(Config, 0, test_rabbit_event_handler, okay, []),

        ok = rabbit_ct_broker_helpers:rpc(Config, 0, gen_event, add_handler,
                                          [rabbit_event, test_rabbit_event_handler, []]),

        ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),

        ?assertMatch({vhost,VHost, _, #{tags := [private,replicate]}},
                     rabbit_ct_broker_helpers:rpc(Config, 0,
                                                  rabbit_vhost, update_tags,
                                                  [VHost, [private, replicate], ActingUser])),
        ?assertMatch({vhost,VHost, _, #{tags := [private,replicate]}},
                     rabbit_ct_broker_helpers:rpc(Config, 0,
                                                  rabbit_vhost, update_tags,
                                                  [VHost, [replicate, private], ActingUser])),

        Events = rabbit_ct_broker_helpers:rpc(Config, 0,
                                              gen_event, call,
                                              [rabbit_event, test_rabbit_event_handler, events, 100]),
        ct:pal(?LOW_IMPORTANCE, "Events: ~p", [lists:reverse(Events)]),
        TagsSetEvents = lists:filter(fun
                                         (#event{type = vhost_tags_set}) -> true;
                                         (_) -> false
                                     end, Events),
        ?assertMatch([#event{type = vhost_tags_set,
                             props = [{name, VHost},
                                      {tags, [private, replicate]},
                                      {user_who_performed_action, ActingUser}]}],
                     TagsSetEvents)
    after
        rabbit_ct_broker_helpers:rpc(Config, 0,
                                     gen_event, delete_handler, [rabbit_event, test_rabbit_event_handler, []]),
        rabbit_ct_broker_helpers:delete_vhost(Config, VHost)
    end.

vhost_is_created_with_default_limits(Config) ->
    VHost = <<"vhost1">>,
    Limits = [{<<"max-connections">>, 10}, {<<"max-queues">>, 1}],
    Pattern = [{<<"pattern">>, ".*"}],
    Env = [{vhosts, [{<<"id">>, Limits++Pattern}]}],
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, 0,
                            application, set_env, [rabbit, default_limits, Env])),
    ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),
    ?assertEqual(Limits, rabbit_ct_broker_helpers:rpc(Config, 0,
                            rabbit_vhost_limit, list, [VHost])).

vhost_is_created_with_operator_policies(Config) ->
    VHost = <<"vhost1">>,
    PolicyName = <<"default-operator-policy">>,
    Definition = [{<<"expires">>, 10}],
    Env = [{operator, [{PolicyName, Definition}]}],
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, 0,
                            application, set_env, [rabbit, default_policies, Env])),
    ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),
    ?assertNotEqual(not_found, rabbit_ct_broker_helpers:rpc(Config, 0,
                            rabbit_policy, lookup_op, [VHost, PolicyName])).

vhost_is_created_with_default_user(Config) ->
    VHost = <<"vhost1">>,
    Username = <<"banana">>,
    Perm = "apple",
    Tags = [arbitrary],
    Pwd = "SECRET",
    Env = [{Username, [{<<"configure">>, Perm}, {<<"tags">>, [arbitrary]}, {<<"password">>, Pwd}]}],
    WantUser = [{user, Username},{tags, Tags}],
    WantPermissions = [[{vhost, VHost}, {configure, list_to_binary(Perm)}, {write, <<".*">>}, {read, <<".*">>}]],
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, 0,
                            application, set_env, [rabbit, default_users, Env])),
    ?assertEqual(false, rabbit_ct_broker_helpers:rpc(Config, 0,
                            rabbit_auth_backend_internal, exists, [Username])),
    ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),
    ct:pal("HAVE: ~p", [rabbit_ct_broker_helpers:rpc(Config, 0,
                            rabbit_auth_backend_internal, list_user_permissions, [Username])]),
    ct:pal("WANT: ~p", [WantPermissions]),
    ?assertEqual(WantPermissions, rabbit_ct_broker_helpers:rpc(Config, 0,
                            rabbit_auth_backend_internal, list_user_permissions, [Username])),
    HaveUser = lists:search(
              fun (U) ->
                  case proplists:get_value(user, U) of
                      Username  -> true;
                      undefined -> false
                  end
              end,
              rabbit_ct_broker_helpers:rpc(Config, 0,
                                             rabbit_auth_backend_internal, list_users, [])
            ),
    ?assertEqual({value, WantUser}, HaveUser),
    ?assertMatch({ok, _}, rabbit_ct_broker_helpers:rpc(Config, 0,
                            rabbit_auth_backend_internal, user_login_authentication, [Username, [{password, list_to_binary(Pwd)}]])),
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, 0,
                    application, unset_env, [rabbit, default_users])),
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, 0,
                            rabbit_auth_backend_internal, delete_user, [Username,
                                                                        <<"acting-user">>])).

parse_tags(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, parse_tags1, [Config]).

parse_tags1(_Config) ->
    ?assertEqual([], rabbit_vhost:parse_tags(<<"">>)),
    ?assertEqual([], rabbit_vhost:parse_tags("")),
    ?assertEqual([], rabbit_vhost:parse_tags([])),
    ?assertEqual([a, b], rabbit_vhost:parse_tags(<<"a,b">>)),
    ?assertEqual([a3, b3], rabbit_vhost:parse_tags("a3,b3")),
    ?assertEqual([tag1, tag2], rabbit_vhost:parse_tags([<<"tag1">>, <<"tag2">>])).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

open_connections(Config, NodesAndVHosts) ->
    % Randomly select connection type
    OpenConnectionFun = case ?config(connection_type, Config) of
        network -> open_unmanaged_connection;
        direct  -> open_unmanaged_connection_direct
    end,
    Conns = lists:map(fun
      ({Node, VHost}) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node,
            VHost);
      (Node) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node)
      end, NodesAndVHosts),
    timer:sleep(500),
    Conns.

close_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          rabbit_ct_client_helpers:close_connection(Conn)
      end, Conns),
    timer:sleep(500).

count_connections_in(Config, VHost) ->
    count_connections_in(Config, VHost, 0).
count_connections_in(Config, VHost, NodeIndex) ->
    timer:sleep(200),
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 count_tracked_items_in, [{vhost, VHost}]).

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

expect_that_client_connection_is_rejected(Config) ->
    expect_that_client_connection_is_rejected(Config, 0).

expect_that_client_connection_is_rejected(Config, NodeIndex) ->
    {error, _} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex).

expect_that_client_connection_is_rejected(Config, NodeIndex, VHost) ->
    {error, _} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex, VHost).
