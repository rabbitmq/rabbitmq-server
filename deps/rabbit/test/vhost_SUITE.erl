%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(vhost_SUITE).

-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(AWAIT_TIMEOUT, 30000).

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
        vhost_update_default_queue_type_undefined,
        vhost_deletion,
        parse_tags
    ],
    ClusterSize2Tests = [
        cluster_vhost_deletion_forces_connection_closure,
        vhost_failure_forces_connection_closure,
        vhost_failure_forces_connection_closure_on_failure_node,
        node_starts_with_dead_vhosts,
        vhost_creation_idempotency,
        vhost_update_idempotency,
        vhost_update_default_queue_type_undefined,
        vhost_deletion
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

    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),

    [_Conn2] = open_connections(Config, [{0, VHost2}]),
    ?awaitMatch(1, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT).

vhost_failure_forces_connection_closure(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),

    [_Conn2] = open_connections(Config, [{0, VHost2}]),
    ?awaitMatch(1, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:force_vhost_failure(Config, VHost2),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT).


vhost_failure_forces_connection_closure_on_failure_node(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),

    [_Conn20] = open_connections(Config, [{0, VHost2}]),
    [_Conn21] = open_connections(Config, [{1, VHost2}]),
    ?awaitMatch(2, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:force_vhost_failure(Config, 0, VHost2),
    %% Vhost2 connection on node 1 is still alive
    ?awaitMatch(1, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),
    %% Vhost1 connection on node 0 is still alive
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT).


cluster_vhost_deletion_forces_connection_closure(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT),

    [_Conn2] = open_connections(Config, [{1, VHost2}]),
    ?awaitMatch(1, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT_TIMEOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT_TIMEOUT).

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

    ?awaitMatch(
       true,
       rabbit_ct_broker_helpers:rpc(Config, 1,
                                    rabbit_vhost_sup_sup, is_vhost_alive, [VHost2]),
       ?AWAIT_TIMEOUT).

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

vhost_update_default_queue_type_undefined(Config) ->
    VHost = <<"update-default_queue_type-with-undefined-test">>,
    Description = <<"rmqfpas-105 test vhost">>,
    Tags = [replicate, private],
    DefaultQueueType = quorum,
    Trace = false,
    ActingUser = <<"acting-user">>,
    try
        ?assertMatch(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),

        PutVhostArgs0 = [VHost, Description, Tags, DefaultQueueType, Trace, ActingUser],
        ?assertMatch(ok,
                     rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, put_vhost, PutVhostArgs0)),

        PutVhostArgs1 = [VHost, Description, Tags, undefined, Trace, ActingUser],
        ?assertMatch(ok,
                     rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, put_vhost, PutVhostArgs1)),

        V = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        ?assertMatch(#{default_queue_type := DefaultQueueType}, vhost:get_metadata(V))
    after
        rabbit_ct_broker_helpers:delete_vhost(Config, VHost)
    end.

vhost_deletion(Config) ->
    VHost = <<"deletion-vhost">>,
    ActingUser = <<"acting-user">>,
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    set_up_vhost(Config, VHost),

    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost),
    {ok, Chan} = amqp_connection:open_channel(Conn),

    %% Declare some resources under the vhost. These should be deleted when the
    %% vhost is deleted.
    QName = <<"vhost-deletion-queue">>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Chan, #'queue.declare'{queue = QName, durable = true}),
    XName = <<"vhost-deletion-exchange">>,
    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Chan,
                                 #'exchange.declare'{exchange = XName,
                                                     durable = true,
                                                     type = <<"direct">>}),
    RoutingKey = QName,
    #'queue.bind_ok'{} = amqp_channel:call(
                           Chan,
                           #'queue.bind'{exchange = XName,
                                         queue = QName,
                                         routing_key = RoutingKey}),
    PolicyName = <<"ttl-policy">>,
    rabbit_ct_broker_helpers:set_policy_in_vhost(
      Config, Node, VHost,
      PolicyName, <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 20}],
      ActingUser),

    % Load the dummy event handler module on the node.
    ok = rabbit_ct_broker_helpers:rpc(Config, Node, test_rabbit_event_handler, okay, []),
    ok = rabbit_ct_broker_helpers:rpc(Config, Node, gen_event, add_handler,
                                      [rabbit_event, test_rabbit_event_handler, []]),
    try
        rabbit_ct_broker_helpers:delete_vhost(Config, VHost),

        Events0 = rabbit_ct_broker_helpers:rpc(Config, Node,
                                               gen_event, call,
                                               [rabbit_event, test_rabbit_event_handler, events, 1000]),
        ct:pal(
          ?LOW_IMPORTANCE,
          "Events emitted during deletion: ~p", [lists:reverse(Events0)]),

        %% Reorganize the event props into maps for easier matching.
        Events = [{Type, maps:from_list(Props)} ||
                  #event{type = Type, props = Props} <- Events0],

        ?assertMatch(#{user := <<"guest">>, vhost := VHost},
                     proplists:get_value(permission_deleted, Events)),

        ?assertMatch(#{source_name := XName,
                       source_kind := exchange,
                       destination_name := QName,
                       destination_kind := queue,
                       routing_key := RoutingKey,
                       vhost := VHost},
                     proplists:get_value(binding_deleted, Events)),

        ?assertMatch(#{name := #resource{name = QName,
                                         kind = queue,
                                         virtual_host = VHost}},
                     proplists:get_value(queue_deleted, Events)),

        ?assertEqual(
          lists:sort([<<>>, <<"amq.direct">>, <<"amq.fanout">>, <<"amq.headers">>,
                      <<"amq.match">>, <<"amq.rabbitmq.trace">>, <<"amq.topic">>,
                      <<"vhost-deletion-exchange">>]),
          lists:sort(lists:filtermap(
                       fun ({exchange_deleted,
                             #{name := #resource{name = Name}}}) ->
                               {true, Name};
                           (_Event) ->
                               false
                       end, Events))),

        ?assertMatch(
          {value, {parameter_cleared, #{name := <<"limits">>,
                                        vhost := VHost}}},
          lists:search(
            fun ({parameter_cleared, #{component := <<"vhost-limits">>}}) ->
                    true;
                (_Event) ->
                    false
            end, Events)),
        ?assertMatch(#{name := <<"limits">>, vhost := VHost},
                     proplists:get_value(vhost_limits_cleared, Events)),
        ?assertMatch(#{name := PolicyName, vhost := VHost},
                     proplists:get_value(policy_cleared, Events)),

        ?assertMatch(#{name := VHost,
                       user_who_performed_action := ActingUser},
                     proplists:get_value(vhost_deleted, Events)),
        ?assertMatch(#{name := VHost,
                       node := Node,
                       user_who_performed_action := ?INTERNAL_USER},
                     proplists:get_value(vhost_down, Events)),

        ?assert(proplists:is_defined(channel_closed, Events)),
        ?assert(proplists:is_defined(connection_closed, Events)),

        %% VHost deletion is not idempotent - we return an error - but deleting
        %% the same vhost again should not cause any more resources to be
        %% deleted. So we should see no new events in the `rabbit_event'
        %% handler.
        ?assertEqual(
          {error, {no_such_vhost, VHost}},
          rabbit_ct_broker_helpers:delete_vhost(Config, VHost)),
        ?assertEqual(
          Events0,
          rabbit_ct_broker_helpers:rpc(
            Config, Node,
            gen_event, call,
            [rabbit_event, test_rabbit_event_handler, events, 1000]))
    after
        rabbit_ct_broker_helpers:rpc(Config, Node,
                                     gen_event, delete_handler, [rabbit_event, test_rabbit_event_handler, []])
    end.

vhost_is_created_with_default_limits(Config) ->
    VHost = <<"vhost1">>,
    Limits = [{<<"max-connections">>, 10}, {<<"max-queues">>, 1}],
    Pattern = [{<<"pattern">>, ".*"}],
    Env = [{vhosts, [{<<"id">>, Limits++Pattern}]}],
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, 0,
                            application, set_env, [rabbit, default_limits, Env])),
    try
        ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),
        ?assertEqual(Limits, rabbit_ct_broker_helpers:rpc(Config, 0,
                                rabbit_vhost_limit, list, [VHost]))
    after
        rabbit_ct_broker_helpers:rpc(
          Config, 0,
          application, unset_env, [rabbit, default_limits])
    end.

vhost_is_created_with_operator_policies(Config) ->
    VHost = <<"vhost1">>,
    PolicyName = <<"default-operator-policy">>,
    Definition = [{<<"expires">>, 10}],
    Env = [{operator, [{PolicyName, Definition}]}],
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, 0,
                            application, set_env, [rabbit, default_policies, Env])),
    try
        ?assertEqual(ok, rabbit_ct_broker_helpers:add_vhost(Config, VHost)),
        ?assertNotEqual(not_found, rabbit_ct_broker_helpers:rpc(Config, 0,
                                rabbit_policy, lookup_op, [VHost, PolicyName]))
    after
        rabbit_ct_broker_helpers:rpc(
          Config, 0,
          application, unset_env, [rabbit, default_policies])
    end.

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
    ?assertEqual(true, lists:member(
              WantUser,
              rabbit_ct_broker_helpers:rpc(Config, 0,
                                             rabbit_auth_backend_internal, list_users, [])
            )),
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
    lists:map(fun
      ({Node, VHost}) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node,
            VHost);
      (Node) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node)
      end, NodesAndVHosts).

close_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          rabbit_ct_client_helpers:close_connection(Conn)
      end, Conns).

count_connections_in(Config, VHost) ->
    count_connections_in(Config, VHost, 0).
count_connections_in(Config, VHost, NodeIndex) ->
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
