%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_only_http_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_ct_client_helpers, [close_connection/1, close_channel/1,
                                   open_unmanaged_connection/1]).
-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2, test_item/2,
                                assert_keys/2, assert_no_keys/2,
                                http_get/2, http_get/3, http_get/5,
                                http_get_as_proplist/2,
                                http_put/4, http_put/6,
                                http_post/4, http_post/6,
                                http_upload_raw/8,
                                http_delete/3, http_delete/5,
                                http_put_raw/4, http_post_accept_json/4,
                                req/4, auth_header/2,
                                assert_permanent_redirect/3,
                                uri_base_from/2, format_for_upload/1,
                                amqp_port/1]).

-import(rabbit_misc, [pget/2]).

-define(COLLECT_INTERVAL, 1000).
-define(PATH_PREFIX, "/custom-prefix").

-compile(export_all).

all() ->
    [
     {group, all_tests_with_prefix},
     {group, all_tests_without_prefix},
     {group, stats_disabled_on_request}
    ].

groups() ->
    [
     {all_tests_with_prefix, [], all_tests()},
     {all_tests_without_prefix, [], all_tests()},
     {stats_disabled_on_request, [], [disable_with_disable_stats_parameter_test]},
     {invalid_config, [], [invalid_config_test]}
    ].

all_tests() -> [
    overview_test,
    nodes_test,
    vhosts_test,
    connections_test,
    exchanges_test,
    queues_test,
    mirrored_queues_test,
    quorum_queues_test,
    queues_well_formed_json_test,
    permissions_vhost_test,
    permissions_connection_channel_consumer_test,
    consumers_cq_test,
    consumers_qq_test,
    arguments_test,
    queue_actions_test,
    exclusive_queue_test,
    connections_channels_pagination_test,
    exchanges_pagination_test,
    exchanges_pagination_permissions_test,
    queue_pagination_test,
    queue_pagination_columns_test,
    queues_pagination_permissions_test,
    samples_range_test,
    sorting_test,
    columns_test,
    if_empty_unused_test,
    queues_enable_totals_test,
    double_encoded_json_test
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
merge_app_env(Config, DisableStats) ->
    Config1 = rabbit_ct_helpers:merge_app_env(Config,
                                    {rabbit, [
                                              {collect_statistics_interval, ?COLLECT_INTERVAL}
                                             ]}),
    rabbit_ct_helpers:merge_app_env(Config1,
                                    {rabbitmq_management, [
                                     {disable_management_stats, DisableStats},
                                     {sample_retention_policies,
                                          [{global,   [{605, 1}]},
                                           {basic,    [{605, 1}]},
                                           {detailed, [{10, 1}]}]
                                     }]}).

start_broker(Config) ->
    Setup0 = rabbit_ct_broker_helpers:setup_steps(),
    Setup1 = rabbit_ct_client_helpers:setup_steps(),
    Steps = Setup0 ++ Setup1,
    rabbit_ct_helpers:run_setup_steps(Config, Steps).

finish_init(Group, Config) when Group == all_tests_with_prefix ->
    finish_init(Group, Config, true);
finish_init(Group, Config) when Group == all_tests_without_prefix ->
    finish_init(Group, Config, true);
finish_init(Group, Config) when Group == stats_disabled_on_request ->
    finish_init(Group, Config, false);
finish_init(Group, Config) when Group == invalid_config_test ->
    finish_init(Group, Config, true).

finish_init(Group, Config, DisableStats) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    NodeConf = [{rmq_nodename_suffix, Group}],
    Config1 = rabbit_ct_helpers:set_config(Config, NodeConf),
    merge_app_env(Config1, DisableStats).

init_per_group(all_tests_with_prefix=Group, Config0) ->
    PathConfig = {rabbitmq_management, [{path_prefix, ?PATH_PREFIX}]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, PathConfig),
    Config2 = finish_init(Group, Config1),
    Config3 = start_broker(Config2),
    Nodes = rabbit_ct_broker_helpers:get_node_configs(
              Config3, nodename),
    Ret = rabbit_ct_broker_helpers:rpc(
            Config3, 0,
            rabbit_feature_flags,
            is_supported_remotely,
            [Nodes, [quorum_queue], 60000]),
    case Ret of
        true ->
            ok = rabbit_ct_broker_helpers:rpc(
                    Config3, 0, rabbit_feature_flags, enable, [quorum_queue]),
            Config3;
        false ->
            end_per_group(Group, Config3),
            {skip, "Quorum queues are unsupported"}
    end;
init_per_group(Group, Config0) ->
    Config1 = finish_init(Group, Config0),
    Config2 = start_broker(Config1),
    Nodes = rabbit_ct_broker_helpers:get_node_configs(
              Config2, nodename),
    Ret = rabbit_ct_broker_helpers:rpc(
            Config2, 0,
            rabbit_feature_flags,
            is_supported_remotely,
            [Nodes, [quorum_queue], 60000]),
    case Ret of
        true ->
            ok = rabbit_ct_broker_helpers:rpc(
                    Config2, 0, rabbit_feature_flags, enable, [quorum_queue]),
            Config2;
        false ->
            end_per_group(Group, Config2),
            {skip, "Quorum queues are unsupported"}
    end.

end_per_group(_, Config) ->
    inets:stop(),
    Teardown0 = rabbit_ct_client_helpers:teardown_steps(),
    Teardown1 = rabbit_ct_broker_helpers:teardown_steps(),
    Steps = Teardown0 ++ Teardown1,
    rabbit_ct_helpers:run_teardown_steps(Config, Steps).

init_per_testcase(Testcase = permissions_vhost_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost">>),
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost1">>),
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost2">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase);

init_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:close_all_connections(Config, 0, <<"rabbit_mgmt_only_http_SUITE:init_per_testcase">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:close_all_connections(Config, 0, <<"rabbit_mgmt_only_http_SUITE:end_per_testcase">>),
    Config1 = end_per_testcase0(Testcase, Config),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

end_per_testcase0(Testcase = queues_enable_totals_test, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                 [rabbitmq_management, enable_queue_totals]),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase0(Testcase = queues_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"downvhost">>),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase0(Testcase = permissions_vhost_test, Config) ->
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost1">>),
    rabbit_ct_broker_helpers:delete_vhost(Config, <<"myvhost2">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"myuser1">>),
    rabbit_ct_broker_helpers:delete_user(Config, <<"myuser2">>),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase0(_, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

overview_test(Config) ->
    Overview = http_get(Config, "/overview"),
    ?assert(maps:is_key(node, Overview)),
    ?assert(maps:is_key(object_totals, Overview)),
    ?assert(not maps:is_key(queue_totals, Overview)),
    ?assert(not maps:is_key(churn_rates, Overview)),
    ?assert(not maps:is_key(message_stats, Overview)),
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags,     <<"management">>}], {group, '2xx'}),
    OverviewU = http_get(Config, "/overview", "myuser", "myuser", ?OK),
    ?assert(maps:is_key(node, OverviewU)),
    ?assert(maps:is_key(object_totals, OverviewU)),
    ?assert(not maps:is_key(queue_totals, OverviewU)),
    ?assert(not maps:is_key(churn_rates, OverviewU)),
    ?assert(not maps:is_key(message_stats, OverviewU)),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    passed.

nodes_test(Config) ->
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/users/monitor", [{password, <<"monitor">>},
                                        {tags, <<"monitoring">>}], {group, '2xx'}),
    DiscNode = #{type => <<"disc">>, running => true},
    assert_list([DiscNode], http_get(Config, "/nodes")),
    assert_list([DiscNode], http_get(Config, "/nodes", "monitor", "monitor", ?OK)),
    http_get(Config, "/nodes", "user", "user", ?NOT_AUTHORISED),
    [Node] = http_get(Config, "/nodes"),
    Path = "/nodes/" ++ binary_to_list(maps:get(name, Node)),
    NodeReply = http_get(Config, Path, ?OK),
    assert_item(DiscNode, NodeReply),
    ?assert(not maps:is_key(channel_closed, NodeReply)),
    ?assert(not maps:is_key(disk_free, NodeReply)),
    ?assert(not maps:is_key(fd_used, NodeReply)),
    ?assert(not maps:is_key(io_file_handle_open_attempt_avg_time, NodeReply)),
    ?assert(maps:is_key(name, NodeReply)),
    assert_item(DiscNode, http_get(Config, Path, "monitor", "monitor", ?OK)),
    http_get(Config, Path, "user", "user", ?NOT_AUTHORISED),
    http_delete(Config, "/users/user", {group, '2xx'}),
    http_delete(Config, "/users/monitor", {group, '2xx'}),
    passed.

%% This test is rather over-verbose as we're trying to test understanding of
%% Webmachine
vhosts_test(Config) ->
    assert_list([#{name => <<"/">>}], http_get(Config, "/vhosts")),
    %% Create a new one
    http_put(Config, "/vhosts/myvhost", none, {group, '2xx'}),
    %% PUT should be idempotent
    http_put(Config, "/vhosts/myvhost", none, {group, '2xx'}),
    %% Check it's there
    [GetFirst | _] = GetAll = http_get(Config, "/vhosts"),
    assert_list([#{name => <<"/">>}, #{name => <<"myvhost">>}], GetAll),
    ?assert(not maps:is_key(message_stats, GetFirst)),
    ?assert(not maps:is_key(messages_ready_details, GetFirst)),
    ?assert(not maps:is_key(recv_oct, GetFirst)),
    ?assert(maps:is_key(cluster_state, GetFirst)),

    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            %% these won't pass for older 3.8 nodes
            ok;
        false ->
            %% PUT can update metadata (description, tags)
            Desc0 = "desc 0",
            Meta0 = [
                {description, Desc0},
                {tags,        "tag1,tag2"}
            ],
            http_put(Config, "/vhosts/myvhost", Meta0, {group, '2xx'}),
            #{description := Desc1, tags := Tags1} = http_get(Config, "/vhosts/myvhost", ?OK),
            ?assertEqual(Desc0, Desc1),
            ?assertEqual([<<"tag1">>, <<"tag2">>], Tags1),

            Desc2 = "desc 2",
            Meta2 = [
                {description, Desc2},
                {tags,        "tag3"}
            ],
            http_put(Config, "/vhosts/myvhost", Meta2, {group, '2xx'}),
            #{description := Desc3, tags := Tags3} = http_get(Config, "/vhosts/myvhost", ?OK),
            ?assertEqual(Desc2, Desc3),
            ?assertEqual([<<"tag3">>], Tags3)
    end,

    %% Check individually
    Get = http_get(Config, "/vhosts/%2F", ?OK),
    assert_item(#{name => <<"/">>}, Get),
    assert_item(#{name => <<"myvhost">>},http_get(Config, "/vhosts/myvhost")),
    ?assert(not maps:is_key(message_stats, Get)),
    ?assert(not maps:is_key(messages_ready_details, Get)),
    ?assert(not maps:is_key(recv_oct, Get)),
    ?assert(maps:is_key(cluster_state, Get)),

    %% Crash it
    rabbit_ct_broker_helpers:force_vhost_failure(Config, <<"myvhost">>),
    [NodeData] = http_get(Config, "/nodes"),
    Node = binary_to_atom(maps:get(name, NodeData), utf8),
    assert_item(#{name => <<"myvhost">>, cluster_state => #{Node => <<"stopped">>}},
                http_get(Config, "/vhosts/myvhost")),

    %% Restart it
    http_post(Config, "/vhosts/myvhost/start/" ++ atom_to_list(Node), [], {group, '2xx'}),
    assert_item(#{name => <<"myvhost">>, cluster_state => #{Node => <<"running">>}},
                http_get(Config, "/vhosts/myvhost")),

    %% Delete it
    http_delete(Config, "/vhosts/myvhost", {group, '2xx'}),
    %% It's not there
    http_get(Config, "/vhosts/myvhost", ?NOT_FOUND),
    http_delete(Config, "/vhosts/myvhost", ?NOT_FOUND),

    passed.

connections_test(Config) ->
    {Conn, _Ch} = open_connection_and_channel(Config),
    LocalPort = local_port(Conn),
    Path = binary_to_list(
             rabbit_mgmt_format:print(
               "/connections/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w",
               [LocalPort, amqp_port(Config)])),
    timer:sleep(1500),
    Connection = http_get(Config, Path, ?OK),
    ?assertEqual(1, maps:size(Connection)),
    ?assert(maps:is_key(name, Connection)),
    ?assert(not maps:is_key(recv_oct_details, Connection)),
    http_delete(Config, Path, {group, '2xx'}),
    %% TODO rabbit_reader:shutdown/2 returns before the connection is
    %% closed. It may not be worth fixing.
    Fun = fun() ->
                  try
                      http_get(Config, Path, ?NOT_FOUND),
                      true
                  catch
                      _:_ ->
                          false
                  end
          end,
    wait_until(Fun, 60),
    close_connection(Conn),
    passed.

exchanges_test(Config) ->
    %% Can list exchanges
    http_get(Config, "/exchanges", {group, '2xx'}),
    %% Can pass booleans or strings
    Good = [{type, <<"direct">>}, {durable, <<"true">>}],
    http_put(Config, "/vhosts/myvhost", none, {group, '2xx'}),
    http_get(Config, "/exchanges/myvhost/foo", ?NOT_FOUND),
    http_put(Config, "/exchanges/myvhost/foo", Good, {group, '2xx'}),
    http_put(Config, "/exchanges/myvhost/foo", Good, {group, '2xx'}),
    http_get(Config, "/exchanges/%2F/foo", ?NOT_FOUND),
    Exchange = http_get(Config, "/exchanges/myvhost/foo"),
    assert_item(#{name => <<"foo">>,
                  vhost => <<"myvhost">>,
                  type => <<"direct">>,
                  durable => true,
                  auto_delete => false,
                  internal => false,
                  arguments => #{}},
                Exchange),
    ?assert(not maps:is_key(message_stats, Exchange)),
    http_put(Config, "/exchanges/badvhost/bar", Good, ?NOT_FOUND),
    http_put(Config, "/exchanges/myvhost/bar", [{type, <<"bad_exchange_type">>}],
             ?BAD_REQUEST),
    http_put(Config, "/exchanges/myvhost/bar", [{type, <<"direct">>},
                                                {durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put(Config, "/exchanges/myvhost/foo", [{type, <<"direct">>}],
             ?BAD_REQUEST),

    http_delete(Config, "/exchanges/myvhost/foo", {group, '2xx'}),
    http_delete(Config, "/exchanges/myvhost/foo", ?NOT_FOUND),

    http_delete(Config, "/vhosts/myvhost", {group, '2xx'}),
    http_get(Config, "/exchanges/badvhost", ?NOT_FOUND),
    passed.

queues_test(Config) ->
    Good = [{durable, true}],
    GoodQQ = [{durable, true}, {arguments, [{'x-queue-type', 'quorum'}]}],
    http_get(Config, "/queues/%2F/foo", ?NOT_FOUND),
    http_put(Config, "/queues/%2F/foo", GoodQQ, {group, '2xx'}),
    http_put(Config, "/queues/%2F/foo", GoodQQ, {group, '2xx'}),

    rabbit_ct_broker_helpers:add_vhost(Config, <<"downvhost">>),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"downvhost">>),
    http_put(Config, "/queues/downvhost/foo", Good, {group, '2xx'}),
    http_put(Config, "/queues/downvhost/bar", Good, {group, '2xx'}),

    rabbit_ct_broker_helpers:force_vhost_failure(Config, <<"downvhost">>),
    %% The vhost is down
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    DownVHost = #{name => <<"downvhost">>, tracing => false, cluster_state => #{Node => <<"stopped">>}},
    assert_item(DownVHost, http_get(Config, "/vhosts/downvhost")),

    DownQueues = http_get(Config, "/queues/downvhost"),
    DownQueue  = http_get(Config, "/queues/downvhost/foo"),

    assert_list([#{name        => <<"bar">>,
                   vhost       => <<"downvhost">>,
                   state       => <<"stopped">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{}},
                 #{name        => <<"foo">>,
                   vhost       => <<"downvhost">>,
                   state       => <<"stopped">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{}}], DownQueues),
    assert_item(#{name        => <<"foo">>,
                  vhost       => <<"downvhost">>,
                  state       => <<"stopped">>,
                  durable     => true,
                  auto_delete => false,
                  exclusive   => false,
                  arguments   => #{}}, DownQueue),

    http_put(Config, "/queues/badvhost/bar", Good, ?NOT_FOUND),
    http_put(Config, "/queues/%2F/bar",
             [{durable, <<"troo">>}],
             ?BAD_REQUEST),
    http_put(Config, "/queues/%2F/foo",
             [{durable, false}],
             ?BAD_REQUEST),

    Policy = [{pattern,    <<"baz">>},
              {definition, [{<<"ha-mode">>, <<"all">>}]}],
    http_put(Config, "/policies/%2F/HA", Policy, {group, '2xx'}),
    http_put(Config, "/queues/%2F/baz", Good, {group, '2xx'}),
    Queues = http_get(Config, "/queues/%2F"),
    Queue = http_get(Config, "/queues/%2F/foo"),

    NodeBin = atom_to_binary(Node, utf8),
    assert_list([#{name        => <<"baz">>,
                   vhost       => <<"/">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{},
                   node        => NodeBin,
                   slave_nodes => [],
                   synchronised_slave_nodes => []},
                 #{name        => <<"foo">>,
                   vhost       => <<"/">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{'x-queue-type' => <<"quorum">>},
                   leader      => NodeBin,
                   members     => [NodeBin]}], Queues),
    assert_item(#{name        => <<"foo">>,
                  vhost       => <<"/">>,
                  durable     => true,
                  auto_delete => false,
                  exclusive   => false,
                  arguments   => #{'x-queue-type' => <<"quorum">>},
                  leader      => NodeBin,
                  members     => [NodeBin]}, Queue),

    ?assert(not maps:is_key(messages, Queue)),
    ?assert(not maps:is_key(message_stats, Queue)),
    ?assert(not maps:is_key(messages_details, Queue)),
    ?assert(not maps:is_key(reductions_details, Queue)),
    ?assertEqual(NodeBin, maps:get(leader, Queue)),
    ?assertEqual([NodeBin], maps:get(members, Queue)),
    ?assertEqual([NodeBin], maps:get(online, Queue)),

    http_delete(Config, "/queues/%2F/foo", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/baz", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/foo", ?NOT_FOUND),
    http_get(Config, "/queues/badvhost", ?NOT_FOUND),

    http_delete(Config, "/queues/downvhost/foo", {group, '2xx'}),
    http_delete(Config, "/queues/downvhost/bar", {group, '2xx'}),
    passed.

queues_enable_totals_test(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, enable_queue_totals, true]),

    Good = [{durable, true}],
    GoodQQ = [{durable, true}, {arguments, [{'x-queue-type', 'quorum'}]}],
    http_get(Config, "/queues/%2F/foo", ?NOT_FOUND),
    http_put(Config, "/queues/%2F/foo", GoodQQ, {group, '2xx'}),

    Policy = [{pattern,    <<"baz">>},
              {definition, [{<<"ha-mode">>, <<"all">>}]}],
    http_put(Config, "/policies/%2F/HA", Policy, {group, '2xx'}),
    http_put(Config, "/queues/%2F/baz", Good, {group, '2xx'}),

    {Conn, Ch} = open_connection_and_channel(Config),
    Publish = fun(Q) ->
                      amqp_channel:call(
                        Ch, #'basic.publish'{exchange = <<"">>,
                                             routing_key = Q},
                        #amqp_msg{payload = <<"message">>})
              end,
    Publish(<<"baz">>),
    Publish(<<"foo">>),
    Publish(<<"foo">>),

    Fun = fun() ->
                  length(rabbit_ct_broker_helpers:rpc(Config, 0, ets, tab2list,
                                                      [queue_coarse_metrics])) == 2
          end,
    wait_until(Fun, 60),

    Queues = http_get(Config, "/queues/%2F"),
    Queue = http_get(Config, "/queues/%2F/foo"),

    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    NodeBin = atom_to_binary(Node, utf8),
    assert_list([#{name        => <<"baz">>,
                   vhost       => <<"/">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => false,
                   arguments   => #{},
                   node        => NodeBin,
                   slave_nodes => [],
                   messages    => 1,
                   messages_ready => 1,
                   messages_unacknowledged => 0,
                   synchronised_slave_nodes => []},
                 #{name        => <<"foo">>,
                   vhost       => <<"/">>,
                   durable     => true,
                   auto_delete => false,
                   exclusive   => null,
                   arguments   => #{'x-queue-type' => <<"quorum">>},
                   leader      => NodeBin,
                   messages    => 2,
                   messages_ready => 2,
                   messages_unacknowledged => 0,
                   members     => [NodeBin]}], Queues),
    assert_item(#{name        => <<"foo">>,
                  vhost       => <<"/">>,
                  durable     => true,
                  auto_delete => false,
                  exclusive   => false,
                  arguments   => #{'x-queue-type' => <<"quorum">>},
                  leader      => NodeBin,
                  members     => [NodeBin]}, Queue),

    ?assert(not maps:is_key(messages, Queue)),
    ?assert(not maps:is_key(messages_ready, Queue)),
    ?assert(not maps:is_key(messages_unacknowledged, Queue)),
    ?assert(not maps:is_key(message_stats, Queue)),
    ?assert(not maps:is_key(messages_details, Queue)),
    ?assert(not maps:is_key(reductions_details, Queue)),

    http_delete(Config, "/queues/%2F/foo", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/baz", {group, '2xx'}),
    close_connection(Conn),

    passed.

mirrored_queues_test(Config) ->
    Policy = [{pattern,    <<".*">>},
              {definition, [{<<"ha-mode">>, <<"all">>}]}],
    http_put(Config, "/policies/%2F/HA", Policy, {group, '2xx'}),

    Good = [{durable, true}, {arguments, []}],
    http_get(Config, "/queues/%2f/ha", ?NOT_FOUND),
    http_put(Config, "/queues/%2f/ha", Good, {group, '2xx'}),

    {Conn, Ch} = open_connection_and_channel(Config),
    Publish = fun() ->
                      amqp_channel:call(
                        Ch, #'basic.publish'{exchange = <<"">>,
                                             routing_key = <<"ha">>},
                        #amqp_msg{payload = <<"message">>})
              end,
    Publish(),
    Publish(),

    Queue = http_get(Config, "/queues/%2f/ha?lengths_age=60&lengths_incr=5&msg_rates_age=60&msg_rates_incr=5&data_rates_age=60&data_rates_incr=5"),

    %% It's really only one node, but the only thing that matters in this test is to verify the
    %% key exists
    Nodes = lists:sort(rabbit_ct_broker_helpers:get_node_configs(Config, nodename)),

    ?assert(not maps:is_key(messages, Queue)),
    ?assert(not maps:is_key(messages_details, Queue)),
    ?assert(not maps:is_key(reductions_details, Queue)),
    ?assert(true, lists:member(maps:get(node, Queue), Nodes)),
    ?assertEqual([], get_nodes(slave_nodes, Queue)),
    ?assertEqual([], get_nodes(synchronised_slave_nodes, Queue)),

    http_delete(Config, "/queues/%2f/ha", {group, '2xx'}),
    close_connection(Conn).

quorum_queues_test(Config) ->
    Good = [{durable, true}, {arguments, [{'x-queue-type', 'quorum'}]}],
    http_get(Config, "/queues/%2f/qq", ?NOT_FOUND),
    http_put(Config, "/queues/%2f/qq", Good, {group, '2xx'}),

    {Conn, Ch} = open_connection_and_channel(Config),
    Publish = fun() ->
                      amqp_channel:call(
                        Ch, #'basic.publish'{exchange = <<"">>,
                                             routing_key = <<"qq">>},
                        #amqp_msg{payload = <<"message">>})
              end,
    Publish(),
    Publish(),

    Queue = http_get(Config, "/queues/%2f/qq?lengths_age=60&lengths_incr=5&msg_rates_age=60&msg_rates_incr=5&data_rates_age=60&data_rates_incr=5"),

    %% It's really only one node, but the only thing that matters in this test is to verify the
    %% key exists
    Nodes = lists:sort(rabbit_ct_broker_helpers:get_node_configs(Config, nodename)),

    ?assert(not maps:is_key(messages, Queue)),
    ?assert(not maps:is_key(messages_details, Queue)),
    ?assert(not maps:is_key(reductions_details, Queue)),
    ?assert(true, lists:member(maps:get(leader, Queue, undefined), Nodes)),
    ?assertEqual(Nodes, get_nodes(members, Queue)),
    ?assertEqual(Nodes, get_nodes(online, Queue)),

    http_delete(Config, "/queues/%2f/qq", {group, '2xx'}),
    close_connection(Conn).

get_nodes(Tag, Queue) ->
    lists:sort([binary_to_atom(B, utf8) || B <- maps:get(Tag, Queue)]).

queues_well_formed_json_test(Config) ->
    %% TODO This test should be extended to the whole API
    Good = [{durable, true}],
    http_put(Config, "/queues/%2F/foo", Good, {group, '2xx'}),
    http_put(Config, "/queues/%2F/baz", Good, {group, '2xx'}),

    Queues = http_get_as_proplist(Config, "/queues/%2F"),
    %% Ensure keys are unique
    [begin
     Sorted = lists:sort(Q),
     Sorted = lists:usort(Q)
     end || Q <- Queues],

    http_delete(Config, "/queues/%2F/foo", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/baz", {group, '2xx'}),
    passed.

permissions_vhost_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/users/myadmin", [{password, <<"myadmin">>},
                                        {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/myuser", [{password, <<"myuser">>},
                                       {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost1", none, {group, '2xx'}),
    http_put(Config, "/vhosts/myvhost2", none, {group, '2xx'}),
    http_put(Config, "/permissions/myvhost1/myuser", PermArgs, {group, '2xx'}),
    http_put(Config, "/permissions/myvhost1/guest", PermArgs, {group, '2xx'}),
    http_put(Config, "/permissions/myvhost2/guest", PermArgs, {group, '2xx'}),
    assert_list([#{name => <<"/">>},
                 #{name => <<"myvhost1">>},
                 #{name => <<"myvhost2">>}], http_get(Config, "/vhosts", ?OK)),
    assert_list([#{name => <<"myvhost1">>}],
                http_get(Config, "/vhosts", "myuser", "myuser", ?OK)),
    http_put(Config, "/queues/myvhost1/myqueue", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/myvhost2/myqueue", QArgs, {group, '2xx'}),
    Test1 =
        fun(Path) ->
                Results = http_get(Config, Path, "myuser", "myuser", ?OK),
                [case maps:get(vhost, Result) of
                     <<"myvhost2">> ->
                         throw({got_result_from_vhost2_in, Path, Result});
                     _ ->
                         ok
                 end || Result <- Results]
        end,
    Test2 =
        fun(Path1, Path2) ->
                http_get(Config, Path1 ++ "/myvhost1/" ++ Path2, "myuser", "myuser",
                         ?OK),
                http_get(Config, Path1 ++ "/myvhost2/" ++ Path2, "myuser", "myuser",
                         ?NOT_AUTHORISED)
        end,
    Test3 =
        fun(Path1) ->
                http_get(Config, Path1 ++ "/myvhost1/", "myadmin", "myadmin",
                         ?OK)
        end,
    Test1("/exchanges"),
    Test2("/exchanges", ""),
    Test2("/exchanges", "amq.direct"),
    Test3("/exchanges"),
    Test1("/queues"),
    Test2("/queues", ""),
    Test3("/queues"),
    Test2("/queues", "myqueue"),
    Test1("/bindings"),
    Test2("/bindings", ""),
    Test3("/bindings"),
    Test2("/queues", "myqueue/bindings"),
    Test2("/exchanges", "amq.default/bindings/source"),
    Test2("/exchanges", "amq.default/bindings/destination"),
    Test2("/bindings", "e/amq.default/q/myqueue"),
    Test2("/bindings", "e/amq.default/q/myqueue/myqueue"),
    http_delete(Config, "/vhosts/myvhost1", {group, '2xx'}),
    http_delete(Config, "/vhosts/myvhost2", {group, '2xx'}),
    http_delete(Config, "/users/myuser", {group, '2xx'}),
    http_delete(Config, "/users/myadmin", {group, '2xx'}),
    passed.

%% Opens a new connection and a channel on it.
%% The channel is not managed by rabbit_ct_client_helpers and
%% should be explicitly closed by the caller.
open_connection_and_channel(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),
    {ok, Ch}   = amqp_connection:open_channel(Conn),
    {Conn, Ch}.

get_conn(Config, Username, Password) ->
    Port       = amqp_port(Config),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{
                                          port     = Port,
                      username = list_to_binary(Username),
                      password = list_to_binary(Password)}),
    LocalPort = local_port(Conn),
    ConnPath = rabbit_misc:format(
                 "/connections/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w",
                 [LocalPort, Port]),
    ChPath = rabbit_misc:format(
               "/channels/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w%20(1)",
               [LocalPort, Port]),
    ConnChPath = rabbit_misc:format(
                   "/connections/127.0.0.1%3A~w%20-%3E%20127.0.0.1%3A~w/channels",
                   [LocalPort, Port]),
    {Conn, ConnPath, ChPath, ConnChPath}.

permissions_connection_channel_consumer_test(Config) ->
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/users/user", [{password, <<"user">>},
                                     {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/permissions/%2F/user", PermArgs, {group, '2xx'}),
    http_put(Config, "/users/monitor", [{password, <<"monitor">>},
                                        {tags, <<"monitoring">>}], {group, '2xx'}),
    http_put(Config, "/permissions/%2F/monitor", PermArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test", #{}, {group, '2xx'}),

    {Conn1, UserConn, UserCh, UserConnCh} = get_conn(Config, "user", "user"),
    {Conn2, MonConn, MonCh, MonConnCh} = get_conn(Config, "monitor", "monitor"),
    {Conn3, AdmConn, AdmCh, AdmConnCh} = get_conn(Config, "guest", "guest"),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),
    {ok, Ch3} = amqp_connection:open_channel(Conn3),
    [amqp_channel:subscribe(
       Ch, #'basic.consume'{queue = <<"test">>}, self()) ||
        Ch <- [Ch1, Ch2, Ch3]],
    timer:sleep(1500),
    AssertLength = fun (Path, User, Len) ->
                           Res = http_get(Config, Path, User, User, ?OK),
                           rabbit_ct_helpers:await_condition(
                               fun () ->
                                   Len =:= length(Res)
                               end)
                   end,
    AssertDisabled = fun(Path) ->
                         http_get(Config, Path, "user", "user", ?BAD_REQUEST),
                         http_get(Config, Path, "monitor", "monitor", ?BAD_REQUEST),
                         http_get(Config, Path, "guest", "guest", ?BAD_REQUEST),
                         http_get(Config, Path, ?BAD_REQUEST)
                 end,

    AssertLength("/connections", "user", 1),
    AssertLength("/connections", "monitor", 3),
    AssertLength("/connections", "guest", 3),

    AssertDisabled("/channels"),
    AssertDisabled("/consumers"),
    AssertDisabled("/consumers/%2F"),

    AssertRead = fun(Path, UserStatus) ->
                         http_get(Config, Path, "user", "user", UserStatus),
                         http_get(Config, Path, "monitor", "monitor", ?OK),
                         http_get(Config, Path, ?OK)
                 end,

    AssertRead(UserConn, ?OK),
    AssertRead(MonConn, ?NOT_AUTHORISED),
    AssertRead(AdmConn, ?NOT_AUTHORISED),
    AssertDisabled(UserCh),
    AssertDisabled(MonCh),
    AssertDisabled(AdmCh),
    AssertRead(UserConnCh, ?OK),
    AssertRead(MonConnCh, ?NOT_AUTHORISED),
    AssertRead(AdmConnCh, ?NOT_AUTHORISED),

    AssertClose = fun(Path, User, Status) ->
                          http_delete(Config, Path, User, User, Status)
                  end,
    AssertClose(UserConn, "monitor", ?NOT_AUTHORISED),
    AssertClose(MonConn, "user", ?NOT_AUTHORISED),
    AssertClose(AdmConn, "guest", {group, '2xx'}),
    AssertClose(MonConn, "guest", {group, '2xx'}),
    AssertClose(UserConn, "user", {group, '2xx'}),

    http_delete(Config, "/users/user", {group, '2xx'}),
    http_delete(Config, "/users/monitor", {group, '2xx'}),
    http_get(Config, "/connections/foo", ?NOT_FOUND),
    http_get(Config, "/channels/foo", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/test", {group, '2xx'}),
    passed.

consumers_cq_test(Config) ->
    consumers_test(Config, [{'x-queue-type', <<"classic">>}]).

consumers_qq_test(Config) ->
    consumers_test(Config, [{'x-queue-type', <<"quorum">>}]).

consumers_test(Config, Args) ->
    http_delete(Config, "/queues/%2F/test", [?NO_CONTENT, ?NOT_FOUND]),
    QArgs = [{auto_delete, false}, {durable, true},
             {arguments, Args}],
    http_put(Config, "/queues/%2F/test", QArgs, {group, '2xx'}),
    {Conn, _ConnPath, _ChPath, _ConnChPath} = get_conn(Config, "guest", "guest"),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:subscribe(
      Ch, #'basic.consume'{queue        = <<"test">>,
                           no_ack       = false,
                           consumer_tag = <<"my-ctag">> }, self()),
    timer:sleep(1500),

    http_get(Config, "/consumers", ?BAD_REQUEST),

    amqp_connection:close(Conn),
    http_delete(Config, "/queues/%2F/test", {group, '2xx'}),
    passed.

defs(Config, Key, URI, CreateMethod, Args) ->
    defs(Config, Key, URI, CreateMethod, Args,
         fun(URI2) -> http_delete(Config, URI2, {group, '2xx'}) end).

defs_v(Config, Key, URI, CreateMethod, Args) ->
    Rep1 = fun (S, S2) -> re:replace(S, "<vhost>", S2, [{return, list}]) end,
    ReplaceVHostInArgs = fun(M, V2) -> maps:map(fun(vhost, _) -> V2;
                                             (_, V1)    -> V1 end, M) end,

    %% Test against default vhost
    defs(Config, Key, Rep1(URI, "%2F"), CreateMethod, ReplaceVHostInArgs(Args, <<"/">>)),

    %% Test against new vhost
    http_put(Config, "/vhosts/test", none, {group, '2xx'}),
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/permissions/test/guest", PermArgs, {group, '2xx'}),
    DeleteFun0 = fun(URI2) ->
                     http_delete(Config, URI2, {group, '2xx'})
                 end,
    DeleteFun1 = fun(_) ->
                    http_delete(Config, "/vhosts/test", {group, '2xx'})
                 end,
    defs(Config, Key, Rep1(URI, "test"),
         CreateMethod, ReplaceVHostInArgs(Args, <<"test">>),
         DeleteFun0, DeleteFun1).

create(Config, CreateMethod, URI, Args) ->
    case CreateMethod of
        put        -> http_put(Config, URI, Args, {group, '2xx'}),
                      URI;
        put_update -> http_put(Config, URI, Args, {group, '2xx'}),
                      URI;
        post       -> Headers = http_post(Config, URI, Args, {group, '2xx'}),
                      rabbit_web_dispatch_util:unrelativise(
                        URI, pget("location", Headers))
    end.

defs(Config, Key, URI, CreateMethod, Args, DeleteFun) ->
    defs(Config, Key, URI, CreateMethod, Args, DeleteFun, DeleteFun).

defs(Config, Key, URI, CreateMethod, Args, DeleteFun0, DeleteFun1) ->
    %% Create the item
    URI2 = create(Config, CreateMethod, URI, Args),
    %% Make sure it ends up in definitions
    Definitions = http_get(Config, "/definitions", ?OK),
    true = lists:any(fun(I) -> test_item(Args, I) end, maps:get(Key, Definitions)),

    %% Delete it
    DeleteFun0(URI2),

    %% Post the definitions back, it should get recreated in correct form
    http_post(Config, "/definitions", Definitions, {group, '2xx'}),
    assert_item(Args, http_get(Config, URI2, ?OK)),

    %% And delete it again
    DeleteFun1(URI2),

    passed.

register_parameters_and_policy_validator(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_runtime_parameters_util, register, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_runtime_parameters_util, register_policy_validator, []).

unregister_parameters_and_policy_validator(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_runtime_parameters_util, unregister_policy_validator, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mgmt_runtime_parameters_util, unregister, []).

arguments_test(Config) ->
    XArgs = [{type, <<"headers">>},
             {arguments, [{'alternate-exchange', <<"amq.direct">>}]}],
    QArgs = [{arguments, [{'x-expires', 1800000}]}],
    BArgs = [{routing_key, <<"">>},
             {arguments, [{'x-match', <<"all">>},
                          {foo, <<"bar">>}]}],
    http_put(Config, "/exchanges/%2F/myexchange", XArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/arguments_test", QArgs, {group, '2xx'}),
    http_post(Config, "/bindings/%2F/e/myexchange/q/arguments_test", BArgs, {group, '2xx'}),
    Definitions = http_get(Config, "/definitions", ?OK),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/arguments_test", {group, '2xx'}),
    http_post(Config, "/definitions", Definitions, {group, '2xx'}),
    #{'alternate-exchange' := <<"amq.direct">>} =
        maps:get(arguments, http_get(Config, "/exchanges/%2F/myexchange", ?OK)),
    #{'x-expires' := 1800000} =
        maps:get(arguments, http_get(Config, "/queues/%2F/arguments_test", ?OK)),

    ArgsTable = [{<<"foo">>,longstr,<<"bar">>}, {<<"x-match">>, longstr, <<"all">>}],
    Hash = table_hash(ArgsTable),
    PropertiesKey = [$~] ++ Hash,

    assert_item(
        #{'x-match' => <<"all">>, foo => <<"bar">>},
        maps:get(arguments,
            http_get(Config, "/bindings/%2F/e/myexchange/q/arguments_test/" ++
            PropertiesKey, ?OK))
    ),
    http_delete(Config, "/exchanges/%2F/myexchange", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/arguments_test", {group, '2xx'}),
    passed.

table_hash(Table) ->
    binary_to_list(rabbit_mgmt_format:args_hash(Table)).

queue_actions_test(Config) ->
    http_put(Config, "/queues/%2F/q", #{}, {group, '2xx'}),
    http_post(Config, "/queues/%2F/q/actions", [{action, sync}], {group, '2xx'}),
    http_post(Config, "/queues/%2F/q/actions", [{action, cancel_sync}], {group, '2xx'}),
    http_post(Config, "/queues/%2F/q/actions", [{action, change_colour}], ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/q", {group, '2xx'}),
    passed.

double_encoded_json_test(Config) ->
    Payload = rabbit_json:encode(rabbit_json:encode(#{<<"durable">> => true, <<"auto_delete">> => false})),
    %% double-encoded JSON response is a 4xx, e.g. a Bad Request, and not a 500
    http_put_raw(Config, "/queues/%2F/q", Payload, {group, '4xx'}),
    passed.

exclusive_queue_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    timer:sleep(2000),
    Path = "/queues/%2F/" ++ rabbit_http_util:quote_plus(QName),
    Queue = http_get(Config, Path),
    assert_item(#{name        => QName,
                  vhost       => <<"/">>,
                  durable     => false,
                  auto_delete => false,
                  exclusive   => true,
                  arguments   => #{}}, Queue),
    amqp_channel:close(Ch),
    close_connection(Conn),
    passed.

connections_channels_pagination_test(Config) ->
    %% this test uses "unmanaged" (by Common Test helpers) connections to avoid
    %% connection caching
    Conn      = open_unmanaged_connection(Config),
    {ok, Ch}  = amqp_connection:open_channel(Conn),
    Conn1     = open_unmanaged_connection(Config),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    Conn2     = open_unmanaged_connection(Config),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    rabbit_ct_helpers:await_condition(
        fun() ->
            PageOfTwo = http_get(Config, "/connections?page=1&page_size=2", ?OK),
            3 == maps:get(total_count, PageOfTwo) andalso
            3 == maps:get(filtered_count, PageOfTwo) andalso
            2 == maps:get(item_count, PageOfTwo) andalso
            1 == maps:get(page, PageOfTwo) andalso
            2 == maps:get(page_size, PageOfTwo) andalso
            2 == maps:get(page_count, PageOfTwo) andalso
            lists:all(fun(C) ->
                              not maps:is_key(recv_oct_details, C)
                      end, maps:get(items, PageOfTwo))
        end),

    http_get(Config, "/channels?page=2&page_size=2", ?BAD_REQUEST),

    amqp_channel:close(Ch),
    amqp_connection:close(Conn),
    amqp_channel:close(Ch1),
    amqp_connection:close(Conn1),
    amqp_channel:close(Ch2),
    amqp_connection:close(Conn2),

    passed.

exchanges_pagination_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/guest", PermArgs, {group, '2xx'}),
    http_get(Config, "/exchanges/vh1?page=1&page_size=2", ?OK),
    http_put(Config, "/exchanges/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/exchanges/vh1/test1", QArgs, {group, '2xx'}),
    http_put(Config, "/exchanges/%2F/test2_reg", QArgs, {group, '2xx'}),
    http_put(Config, "/exchanges/vh1/reg_test3", QArgs, {group, '2xx'}),

    %% for stats to update
    timer:sleep(1500),

    Total     = length(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list_names, [])),

    PageOfTwo = http_get(Config, "/exchanges?page=1&page_size=2", ?OK),
    ?assertEqual(Total, maps:get(total_count, PageOfTwo)),
    ?assertEqual(Total, maps:get(filtered_count, PageOfTwo)),
    ?assertEqual(2, maps:get(item_count, PageOfTwo)),
    ?assertEqual(1, maps:get(page, PageOfTwo)),
    ?assertEqual(2, maps:get(page_size, PageOfTwo)),
    ?assertEqual(round(Total / 2), maps:get(page_count, PageOfTwo)),
    Items1 = maps:get(items, PageOfTwo),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, Items1)),
    assert_list([#{name => <<"">>, vhost => <<"/">>},
                 #{name => <<"amq.direct">>, vhost => <<"/">>}
                ], Items1),

    ByName = http_get(Config, "/exchanges?page=1&page_size=2&name=reg", ?OK),
    ?assertEqual(Total, maps:get(total_count, ByName)),
    ?assertEqual(2, maps:get(filtered_count, ByName)),
    ?assertEqual(2, maps:get(item_count, ByName)),
    ?assertEqual(1, maps:get(page, ByName)),
    ?assertEqual(2, maps:get(page_size, ByName)),
    ?assertEqual(1, maps:get(page_count, ByName)),
    Items2 = maps:get(items, ByName),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, Items2)),
    assert_list([#{name => <<"test2_reg">>, vhost => <<"/">>},
                 #{name => <<"reg_test3">>, vhost => <<"vh1">>}
                ], Items2),

    RegExByName = http_get(Config,
                           "/exchanges?page=1&page_size=2&name=%5E(?=%5Ereg)&use_regex=true",
                           ?OK),
    ?assertEqual(Total, maps:get(total_count, RegExByName)),
    ?assertEqual(1, maps:get(filtered_count, RegExByName)),
    ?assertEqual(1, maps:get(item_count, RegExByName)),
    ?assertEqual(1, maps:get(page, RegExByName)),
    ?assertEqual(2, maps:get(page_size, RegExByName)),
    ?assertEqual(1, maps:get(page_count, RegExByName)),
    Items3 = maps:get(items, RegExByName),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, Items3)),
    assert_list([#{name => <<"reg_test3">>, vhost => <<"vh1">>}
                ], Items3),


    http_get(Config, "/exchanges?page=1000", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=-1", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=1&page_size=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/exchanges?page=1&page_size=501", ?BAD_REQUEST), %% max 500 allowed
    http_get(Config, "/exchanges?page=-1&page_size=-2", ?BAD_REQUEST),
    http_delete(Config, "/exchanges/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/exchanges/vh1/test1", {group, '2xx'}),
    http_delete(Config, "/exchanges/%2F/test2_reg", {group, '2xx'}),
    http_delete(Config, "/exchanges/vh1/reg_test3", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),
    passed.

exchanges_pagination_permissions_test(Config) ->
    http_put(Config, "/users/admin",   [{password, <<"admin">>},
                                        {tags, <<"administrator">>}], {group, '2xx'}),
    http_put(Config, "/users/non-admin", [{password, <<"non-admin">>},
                                          {tags, <<"management">>}], {group, '2xx'}),
    Perms = [{configure, <<".*">>},
             {write,     <<".*">>},
             {read,      <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/non-admin",   Perms, {group, '2xx'}),
    http_put(Config, "/permissions/%2F/admin",   Perms, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/admin",   Perms, {group, '2xx'}),
    QArgs = #{},
    http_put(Config, "/exchanges/%2F/test0", QArgs, "admin", "admin", {group, '2xx'}),
    http_put(Config, "/exchanges/vh1/test1", QArgs, "non-admin", "non-admin", {group, '2xx'}),

    %% for stats to update
    timer:sleep(1500),

    FirstPage = http_get(Config, "/exchanges?page=1&name=test1", "non-admin", "non-admin", ?OK),

    ?assertEqual(8, maps:get(total_count, FirstPage)),
    ?assertEqual(1, maps:get(item_count, FirstPage)),
    ?assertEqual(1, maps:get(page, FirstPage)),
    ?assertEqual(100, maps:get(page_size, FirstPage)),
    ?assertEqual(1, maps:get(page_count, FirstPage)),
    Items = maps:get(items, FirstPage),
    ?assert(lists:all(fun(C) ->
                              not maps:is_key(message_stats, C)
                      end, Items)),
    assert_list([#{name => <<"test1">>, vhost => <<"vh1">>}
                ], Items),
    http_delete(Config, "/exchanges/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/exchanges/vh1/test1", {group, '2xx'}),
    http_delete(Config, "/users/admin", {group, '2xx'}),
    http_delete(Config, "/users/non-admin", {group, '2xx'}),
    passed.



queue_pagination_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/guest", PermArgs, {group, '2xx'}),

    http_get(Config, "/queues/vh1?page=1&page_size=2", ?OK),

    http_put(Config, "/queues/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/test1", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test2_reg", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/reg_test3", QArgs, {group, '2xx'}),

    %% for stats to update
    timer:sleep(1500),

    Total     = length(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list_names, [])),

    PageOfTwo = http_get(Config, "/queues?page=1&page_size=2", ?OK),
    ?assertEqual(Total, maps:get(total_count, PageOfTwo)),
    ?assertEqual(Total, maps:get(filtered_count, PageOfTwo)),
    ?assertEqual(2, maps:get(item_count, PageOfTwo)),
    ?assertEqual(1, maps:get(page, PageOfTwo)),
    ?assertEqual(2, maps:get(page_size, PageOfTwo)),
    ?assertEqual(2, maps:get(page_count, PageOfTwo)),
    Items1 = maps:get(items, PageOfTwo),
    ?assert(lists:all(fun(C) ->
                              not maps:is_key(message_stats, C)
                      end, Items1)),
    assert_list([#{name => <<"test0">>, vhost => <<"/">>},
                 #{name => <<"test2_reg">>, vhost => <<"/">>}
                ], Items1),

    SortedByName = http_get(Config, "/queues?sort=name&page=1&page_size=2", ?OK),
    ?assertEqual(Total, maps:get(total_count, SortedByName)),
    ?assertEqual(Total, maps:get(filtered_count, SortedByName)),
    ?assertEqual(2, maps:get(item_count, SortedByName)),
    ?assertEqual(1, maps:get(page, SortedByName)),
    ?assertEqual(2, maps:get(page_size, SortedByName)),
    ?assertEqual(2, maps:get(page_count, SortedByName)),
    Items2 = maps:get(items, SortedByName),
    ?assert(lists:all(fun(C) ->
                              not maps:is_key(message_stats, C)
                      end, Items2)),
    assert_list([#{name => <<"reg_test3">>, vhost => <<"vh1">>},
                 #{name => <<"test0">>, vhost => <<"/">>}
                ], Items2),

    FirstPage = http_get(Config, "/queues?page=1", ?OK),
    ?assertEqual(Total, maps:get(total_count, FirstPage)),
    ?assertEqual(Total, maps:get(filtered_count, FirstPage)),
    ?assertEqual(4, maps:get(item_count, FirstPage)),
    ?assertEqual(1, maps:get(page, FirstPage)),
    ?assertEqual(100, maps:get(page_size, FirstPage)),
    ?assertEqual(1, maps:get(page_count, FirstPage)),
    Items3 = maps:get(items, FirstPage),
    ?assert(lists:all(fun(C) ->
                              not maps:is_key(message_stats, C)
                      end, Items3)),
    assert_list([#{name => <<"test0">>, vhost => <<"/">>},
                 #{name => <<"test1">>, vhost => <<"vh1">>},
                 #{name => <<"test2_reg">>, vhost => <<"/">>},
                 #{name => <<"reg_test3">>, vhost =><<"vh1">>}
                ], Items3),

    ReverseSortedByName = http_get(Config,
                                   "/queues?page=2&page_size=2&sort=name&sort_reverse=true",
                                   ?OK),
    ?assertEqual(Total, maps:get(total_count, ReverseSortedByName)),
    ?assertEqual(Total, maps:get(filtered_count, ReverseSortedByName)),
    ?assertEqual(2, maps:get(item_count, ReverseSortedByName)),
    ?assertEqual(2, maps:get(page, ReverseSortedByName)),
    ?assertEqual(2, maps:get(page_size, ReverseSortedByName)),
    ?assertEqual(2, maps:get(page_count, ReverseSortedByName)),
    Items4 = maps:get(items, ReverseSortedByName),
    ?assert(lists:all(fun(C) ->
                              not maps:is_key(message_stats, C)
                      end, Items4)),
    assert_list([#{name => <<"test0">>, vhost => <<"/">>},
                 #{name => <<"reg_test3">>, vhost => <<"vh1">>}
                ], Items4),

    ByName = http_get(Config, "/queues?page=1&page_size=2&name=reg", ?OK),
    ?assertEqual(Total, maps:get(total_count, ByName)),
    ?assertEqual(2, maps:get(filtered_count, ByName)),
    ?assertEqual(2, maps:get(item_count, ByName)),
    ?assertEqual(1, maps:get(page, ByName)),
    ?assertEqual(2, maps:get(page_size, ByName)),
    ?assertEqual(1, maps:get(page_count, ByName)),
    Items5 = maps:get(items, ByName),
    ?assert(lists:all(fun(C) ->
                              not maps:is_key(message_stats, C)
                      end, Items5)),
    assert_list([#{name => <<"test2_reg">>, vhost => <<"/">>},
                 #{name => <<"reg_test3">>, vhost => <<"vh1">>}
                ], Items5),

    RegExByName = http_get(Config,
                           "/queues?page=1&page_size=2&name=%5E(?=%5Ereg)&use_regex=true",
                           ?OK),
    ?assertEqual(Total, maps:get(total_count, RegExByName)),
    ?assertEqual(1, maps:get(filtered_count, RegExByName)),
    ?assertEqual(1, maps:get(item_count, RegExByName)),
    ?assertEqual(1, maps:get(page, RegExByName)),
    ?assertEqual(2, maps:get(page_size, RegExByName)),
    ?assertEqual(1, maps:get(page_count, RegExByName)),
    Items6 = maps:get(items, RegExByName),
    ?assert(lists:all(fun(C) ->
                              not maps:is_key(message_stats, C)
                      end, Items6)),
    assert_list([#{name => <<"reg_test3">>, vhost => <<"vh1">>}
                ], Items6),

    http_get(Config, "/queues?page=1000", ?BAD_REQUEST),
    http_get(Config, "/queues?page=-1", ?BAD_REQUEST),
    http_get(Config, "/queues?page=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/queues?page=1&page_size=not_an_integer_value", ?BAD_REQUEST),
    http_get(Config, "/queues?page=1&page_size=501", ?BAD_REQUEST), %% max 500 allowed
    http_get(Config, "/queues?page=-1&page_size=-2", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/test1", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/test2_reg", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/reg_test3", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),
    passed.

queue_pagination_columns_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, [?CREATED, ?NO_CONTENT]),
    http_put(Config, "/permissions/vh1/guest", PermArgs, [?CREATED, ?NO_CONTENT]),

    http_get(Config, "/queues/vh1?columns=name&page=1&page_size=2", ?OK),
    http_put(Config, "/queues/%2F/queue_a", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/queue_b", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/queue_c", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/queue_d", QArgs, {group, '2xx'}),
    PageOfTwo = http_get(Config, "/queues?columns=name&page=1&page_size=2", ?OK),
    ?assertEqual(4, maps:get(total_count, PageOfTwo)),
    ?assertEqual(4, maps:get(filtered_count, PageOfTwo)),
    ?assertEqual(2, maps:get(item_count, PageOfTwo)),
    ?assertEqual(1, maps:get(page, PageOfTwo)),
    ?assertEqual(2, maps:get(page_size, PageOfTwo)),
    ?assertEqual(2, maps:get(page_count, PageOfTwo)),
    assert_list([#{name => <<"queue_a">>},
                 #{name => <<"queue_c">>}
    ], maps:get(items, PageOfTwo)),

    ColumnNameVhost = http_get(Config, "/queues/vh1?columns=name&page=1&page_size=2", ?OK),
    ?assertEqual(2, maps:get(total_count, ColumnNameVhost)),
    ?assertEqual(2, maps:get(filtered_count, ColumnNameVhost)),
    ?assertEqual(2, maps:get(item_count, ColumnNameVhost)),
    ?assertEqual(1, maps:get(page, ColumnNameVhost)),
    ?assertEqual(2, maps:get(page_size, ColumnNameVhost)),
    ?assertEqual(1, maps:get(page_count, ColumnNameVhost)),
    assert_list([#{name => <<"queue_b">>},
                 #{name => <<"queue_d">>}
    ], maps:get(items, ColumnNameVhost)),

    ColumnsNameVhost = http_get(Config, "/queues?columns=name,vhost&page=2&page_size=2", ?OK),
    ?assertEqual(4, maps:get(total_count, ColumnsNameVhost)),
    ?assertEqual(4, maps:get(filtered_count, ColumnsNameVhost)),
    ?assertEqual(2, maps:get(item_count, ColumnsNameVhost)),
    ?assertEqual(2, maps:get(page, ColumnsNameVhost)),
    ?assertEqual(2, maps:get(page_size, ColumnsNameVhost)),
    ?assertEqual(2, maps:get(page_count, ColumnsNameVhost)),
    assert_list([
        #{name  => <<"queue_b">>,
          vhost => <<"vh1">>},
        #{name  => <<"queue_d">>,
          vhost => <<"vh1">>}
    ], maps:get(items, ColumnsNameVhost)),


    http_delete(Config, "/queues/%2F/queue_a", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/queue_b", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/queue_c", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/queue_d", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),
    passed.

queues_pagination_permissions_test(Config) ->
    http_delete(Config, "/vhosts/vh1", [?NO_CONTENT, ?NOT_FOUND]),
    http_delete(Config, "/queues/%2F/test0", [?NO_CONTENT, ?NOT_FOUND]),
    http_delete(Config, "/users/admin", [?NO_CONTENT, ?NOT_FOUND]),
    http_delete(Config, "/users/non-admin", [?NO_CONTENT, ?NOT_FOUND]),

    http_put(Config, "/users/non-admin", [{password, <<"non-admin">>},
                                          {tags, <<"management">>}], {group, '2xx'}),
    http_put(Config, "/users/admin",   [{password, <<"admin">>},
                                        {tags, <<"administrator">>}], {group, '2xx'}),
    Perms = [{configure, <<".*">>},
             {write,     <<".*">>},
             {read,      <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/non-admin",   Perms, {group, '2xx'}),
    http_put(Config, "/permissions/%2F/admin",   Perms, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/admin",   Perms, {group, '2xx'}),
    QArgs = #{},
    http_put(Config, "/queues/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/test1", QArgs, "non-admin","non-admin", {group, '2xx'}),
    FirstPage = http_get(Config, "/queues?page=1", "non-admin", "non-admin", ?OK),
    ?assertEqual(1, maps:get(total_count, FirstPage)),
    ?assertEqual(1, maps:get(item_count, FirstPage)),
    ?assertEqual(1, maps:get(page, FirstPage)),
    ?assertEqual(100, maps:get(page_size, FirstPage)),
    ?assertEqual(1, maps:get(page_count, FirstPage)),
    assert_list([#{name => <<"test1">>, vhost => <<"vh1">>}
                ], maps:get(items, FirstPage)),

    FirstPageAdm = http_get(Config, "/queues?page=1", "admin", "admin", ?OK),
    ?assertEqual(2, maps:get(total_count, FirstPageAdm)),
    ?assertEqual(2, maps:get(item_count, FirstPageAdm)),
    ?assertEqual(1, maps:get(page, FirstPageAdm)),
    ?assertEqual(100, maps:get(page_size, FirstPageAdm)),
    ?assertEqual(1, maps:get(page_count, FirstPageAdm)),
    assert_list([#{name => <<"test1">>, vhost => <<"vh1">>},
                 #{name => <<"test0">>, vhost => <<"/">>}
                ], maps:get(items, FirstPageAdm)),

    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/test1","admin","admin", {group, '2xx'}),
    http_delete(Config, "/users/admin", {group, '2xx'}),
    http_delete(Config, "/users/non-admin", {group, '2xx'}),
    passed.

samples_range_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),

    %% Connections
    rabbit_ct_helpers:await_condition(
        fun() ->
            1 == length(http_get(Config, "/connections?lengths_age=60&lengths_incr=1", ?OK))
        end),
    [Connection] = http_get(Config, "/connections?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(maps:is_key(name, Connection)),

    amqp_channel:close(Ch),
    amqp_connection:close(Conn),

    %% Exchanges
    [Exchange1 | _] = http_get(Config, "/exchanges?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(not maps:is_key(message_stats, Exchange1)),
    Exchange2 = http_get(Config, "/exchanges/%2F/amq.direct?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(not maps:is_key(message_stats, Exchange2)),

    %% Nodes
    [Node] = http_get(Config, "/nodes?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(not maps:is_key(channel_closed_details, Node)),
    ?assert(not maps:is_key(channel_closed, Node)),
    ?assert(not maps:is_key(disk_free, Node)),
    ?assert(not maps:is_key(io_read_count, Node)),

    %% Overview
    Overview = http_get(Config, "/overview?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(maps:is_key(node, Overview)),
    ?assert(maps:is_key(object_totals, Overview)),
    ?assert(not maps:is_key(queue_totals, Overview)),
    ?assert(not maps:is_key(churn_rates, Overview)),
    ?assert(not maps:is_key(message_stats, Overview)),

    %% Queues
    http_put(Config, "/queues/%2F/test0", #{}, {group, '2xx'}),

    rabbit_ct_helpers:await_condition(
        fun() ->
            1 == length(http_get(Config, "/queues/%2F?lengths_age=60&lengths_incr=1", ?OK))
        end),

    [Queue1] = http_get(Config, "/queues/%2F?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(not maps:is_key(message_stats, Queue1)),
    ?assert(not maps:is_key(messages_details, Queue1)),
    ?assert(not maps:is_key(reductions_details, Queue1)),

    Queue2 = http_get(Config, "/queues/%2F/test0?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(not maps:is_key(message_stats, Queue2)),
    ?assert(not maps:is_key(messages_details, Queue2)),
    ?assert(not maps:is_key(reductions_details, Queue2)),

    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),

    %% Vhosts

    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),

    rabbit_ct_helpers:await_condition(
        fun() ->
            length(http_get(Config, "/vhosts?lengths_age=60&lengths_incr=1", ?OK)) > 1
        end),

    [VHost | _] = http_get(Config, "/vhosts?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(not maps:is_key(message_stats, VHost)),
    ?assert(not maps:is_key(messages_ready_details, VHost)),
    ?assert(not maps:is_key(recv_oct, VHost)),
    ?assert(maps:is_key(cluster_state, VHost)),

    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),

    passed.

disable_with_disable_stats_parameter_test(Config) ->
    {Conn, Ch} = open_connection_and_channel(Config),

    %% Ensure we have some queue and exchange stats, needed later
    http_put(Config, "/queues/%2F/test0", #{}, {group, '2xx'}),
    timer:sleep(1500),
    amqp_channel:call(Ch, #'basic.publish'{exchange = <<>>,
                                           routing_key = <<"test0">>},
                      #amqp_msg{payload = <<"message">>}),

    %% Channels.

    timer:sleep(1500),
    %% Check first that stats are available
    http_get(Config, "/channels", ?OK),
    %% Now we can disable them
    http_get(Config, "/channels?disable_stats=true", ?BAD_REQUEST),


    %% Connections.

    %% Check first that stats are available
    [ConnectionStats] = http_get(Config, "/connections", ?OK),
    ?assert(maps:is_key(recv_oct_details, ConnectionStats)),
    %% Now we can disable them
    [Connection] = http_get(Config, "/connections?disable_stats=true", ?OK),
    ?assert(maps:is_key(name, Connection)),
    ?assert(not maps:is_key(recv_oct_details, Connection)),

    amqp_channel:close(Ch),
    amqp_connection:close(Conn),

    %% Exchanges.

    %% Check first that stats are available
    %% Exchange stats aren't published - even as 0 - until some messages have gone
    %% through. At the end of this test we ensure that at least the default exchange
    %% has something to show.
    ExchangeStats = http_get(Config, "/exchanges", ?OK),
    ?assert(lists:any(fun(E) ->
                              maps:is_key(message_stats, E)
                      end, ExchangeStats)),
    %% Now we can disable them
    Exchanges = http_get(Config, "/exchanges?disable_stats=true", ?OK),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, Exchanges)),
    Exchange = http_get(Config, "/exchanges/%2F/amq.direct?disable_stats=true&lengths_age=60&lengths_incr=1", ?OK),
    ?assert(not maps:is_key(message_stats, Exchange)),

    %% Nodes.

    %% Check that stats are available
    [NodeStats] = http_get(Config, "/nodes", ?OK),
    ?assert(maps:is_key(channel_closed_details, NodeStats)),
    %% Now we can disable them
    [Node] = http_get(Config, "/nodes?disable_stats=true", ?OK),
    ?assert(not maps:is_key(channel_closed_details, Node)),
    ?assert(not maps:is_key(channel_closed, Node)),
    ?assert(not maps:is_key(disk_free, Node)),
    ?assert(not maps:is_key(io_read_count, Node)),


    %% Overview.

    %% Check that stats are available
    OverviewStats = http_get(Config, "/overview", ?OK),
    ?assert(maps:is_key(message_stats, OverviewStats)),
    %% Now we can disable them
    Overview = http_get(Config, "/overview?disable_stats=true&lengths_age=60&lengths_incr=1", ?OK),
    ?assert(not maps:is_key(queue_totals, Overview)),
    ?assert(not maps:is_key(churn_rates, Overview)),
    ?assert(not maps:is_key(message_stats, Overview)),

    %% Queues.

    %% Check that stats are available
    [QueueStats] = http_get(Config, "/queues/%2F?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(maps:is_key(message_stats, QueueStats)),
    %% Now we can disable them
    [Queue] = http_get(Config, "/queues/%2F?disable_stats=true", ?OK),
    ?assert(not maps:is_key(message_stats, Queue)),
    ?assert(not maps:is_key(messages_details, Queue)),
    ?assert(not maps:is_key(reductions_details, Queue)),

    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),

    %% Vhosts.

    %% Check that stats are available
    VHostStats = http_get(Config, "/vhosts?lengths_age=60&lengths_incr=1", ?OK),
    ?assert(lists:all(fun(E) ->
                              maps:is_key(message_stats, E) and
                                  maps:is_key(messages_ready_details, E)
                      end, VHostStats)),
    %% Now we can disable them
    VHosts = http_get(Config, "/vhosts?disable_stats=true&lengths_age=60&lengths_incr=1", ?OK),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E) and
                                  not maps:is_key(messages_ready_details, E)
                      end, VHosts)),

    passed.

sorting_test(Config) ->
    QArgs = #{},
    PermArgs = [{configure, <<".*">>}, {write, <<".*">>}, {read, <<".*">>}],
    http_put(Config, "/vhosts/vh1", none, {group, '2xx'}),
    http_put(Config, "/permissions/vh1/guest", PermArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test0", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/test1", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test2", QArgs, {group, '2xx'}),
    http_put(Config, "/queues/vh1/test3", QArgs, {group, '2xx'}),
    List1 = http_get(Config, "/queues", ?OK),
    assert_list([#{name => <<"test0">>},
                 #{name => <<"test2">>},
                 #{name => <<"test1">>},
                 #{name => <<"test3">>}], List1),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, List1)),
    List2 = http_get(Config, "/queues?sort=name", ?OK),
    assert_list([#{name => <<"test0">>},
                 #{name => <<"test1">>},
                 #{name => <<"test2">>},
                 #{name => <<"test3">>}], List2),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, List2)),
    List3 = http_get(Config, "/queues?sort=vhost", ?OK),
    assert_list([#{name => <<"test0">>},
                 #{name => <<"test2">>},
                 #{name => <<"test1">>},
                 #{name => <<"test3">>}], List3),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, List3)),
    List4 = http_get(Config, "/queues?sort_reverse=true", ?OK),
    assert_list([#{name => <<"test3">>},
                 #{name => <<"test1">>},
                 #{name => <<"test2">>},
                 #{name => <<"test0">>}], List4),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, List4)),
    List5 = http_get(Config, "/queues?sort=name&sort_reverse=true", ?OK),
    assert_list([#{name => <<"test3">>},
                 #{name => <<"test2">>},
                 #{name => <<"test1">>},
                 #{name => <<"test0">>}], List5),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, List5)),
    List6 = http_get(Config, "/queues?sort=vhost&sort_reverse=true", ?OK),
    assert_list([#{name => <<"test3">>},
                 #{name => <<"test1">>},
                 #{name => <<"test2">>},
                 #{name => <<"test0">>}], List6),
    ?assert(lists:all(fun(E) ->
                              not maps:is_key(message_stats, E)
                      end, List6)),
    %% Rather poor but at least test it doesn't blow up with dots
    http_get(Config, "/queues?sort=owner_pid_details.name", ?OK),
    http_delete(Config, "/queues/%2F/test0", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/test1", {group, '2xx'}),
    http_delete(Config, "/queues/%2F/test2", {group, '2xx'}),
    http_delete(Config, "/queues/vh1/test3", {group, '2xx'}),
    http_delete(Config, "/vhosts/vh1", {group, '2xx'}),
    passed.

columns_test(Config) ->
    Path = "/queues/%2F/columns.test",
    TTL = 30000,
    http_delete(Config, Path, [{group, '2xx'}, 404]),
    http_put(Config, Path, [{arguments, [{<<"x-message-ttl">>, TTL}]}],
             {group, '2xx'}),
    Item = #{arguments => #{'x-message-ttl' => TTL}, name => <<"columns.test">>},
    timer:sleep(2000),
    [Item] = http_get(Config, "/queues?columns=arguments.x-message-ttl,name", ?OK),
    Item = http_get(Config, "/queues/%2F/columns.test?columns=arguments.x-message-ttl,name", ?OK),
    ?assert(not maps:is_key(message_stats, Item)),
    ?assert(not maps:is_key(messages_details, Item)),
    ?assert(not maps:is_key(reductions_details, Item)),
    http_delete(Config, Path, {group, '2xx'}),
    passed.

if_empty_unused_test(Config) ->
    http_put(Config, "/exchanges/%2F/test", #{}, {group, '2xx'}),
    http_put(Config, "/queues/%2F/test", #{}, {group, '2xx'}),
    http_post(Config, "/bindings/%2F/e/test/q/test", #{}, {group, '2xx'}),
    http_post(Config, "/exchanges/%2F/amq.default/publish",
              msg(<<"test">>, #{}, <<"Hello world">>), ?OK),
    http_delete(Config, "/queues/%2F/test?if-empty=true", ?BAD_REQUEST),
    http_delete(Config, "/exchanges/%2F/test?if-unused=true", ?BAD_REQUEST),
    http_delete(Config, "/queues/%2F/test/contents", {group, '2xx'}),

    {Conn, _ConnPath, _ChPath, _ConnChPath} = get_conn(Config, "guest", "guest"),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = <<"test">> }, self()),
    http_delete(Config, "/queues/%2F/test?if-unused=true", ?BAD_REQUEST),
    amqp_connection:close(Conn),

    http_delete(Config, "/queues/%2F/test?if-empty=true", {group, '2xx'}),
    http_delete(Config, "/exchanges/%2F/test?if-unused=true", {group, '2xx'}),
    passed.

invalid_config_test(Config) ->
    {Conn, _Ch} = open_connection_and_channel(Config),

    timer:sleep(1500),

    %% Check first that stats aren't available (configured on test setup)
    http_get(Config, "/channels", ?BAD_REQUEST),
    http_get(Config, "/connections", ?BAD_REQUEST),
    http_get(Config, "/exchanges", ?BAD_REQUEST),

    %% Now we can set an invalid config, stats are still available (defaults to 'false')
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, disable_management_stats, 50]),
    http_get(Config, "/channels", ?OK),
    http_get(Config, "/connections", ?OK),
    http_get(Config, "/exchanges", ?OK),

    %% Set a valid config again
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management, disable_management_stats, true]),
    http_get(Config, "/channels", ?BAD_REQUEST),
    http_get(Config, "/connections", ?BAD_REQUEST),
    http_get(Config, "/exchanges", ?BAD_REQUEST),

    %% Now we can set an invalid config in the agent, stats are still available
    %% (defaults to 'false')
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbitmq_management_agent, disable_metrics_collector, "koala"]),
    http_get(Config, "/channels", ?OK),
    http_get(Config, "/connections", ?OK),
    http_get(Config, "/exchanges", ?OK),

    amqp_connection:close(Conn),

    passed.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

msg(Key, Headers, Body) ->
    msg(Key, Headers, Body, <<"string">>).

msg(Key, Headers, Body, Enc) ->
    #{exchange         => <<"">>,
      routing_key      => Key,
      properties       => #{delivery_mode => 2,
                           headers        =>  Headers},
      payload          => Body,
      payload_encoding => Enc}.

local_port(Conn) ->
    [{sock, Sock}] = amqp_connection:info(Conn, [sock]),
    {ok, Port} = inet:port(Sock),
    Port.

spawn_invalid(_Config, 0) ->
    ok;
spawn_invalid(Config, N) ->
    Self = self(),
    spawn(fun() ->
                  timer:sleep(rand:uniform(250)),
                  {ok, Sock} = gen_tcp:connect("localhost", amqp_port(Config), [list]),
                  ok = gen_tcp:send(Sock, "Some Data"),
                  receive_msg(Self)
          end),
    spawn_invalid(Config, N-1).

receive_msg(Self) ->
    receive
        {tcp, _, [$A, $M, $Q, $P | _]} ->
            Self ! done
    after
        60000 ->
            Self ! no_reply
    end.

wait_for_answers(0) ->
    ok;
wait_for_answers(N) ->
    receive
        done ->
            wait_for_answers(N-1);
        no_reply ->
            throw(no_reply)
    end.

publish(Ch) ->
    amqp_channel:call(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = <<"myqueue">>},
                      #amqp_msg{payload = <<"message">>}),
    receive
        stop_publish ->
            ok
    after 50 ->
        publish(Ch)
    end.

wait_until(_Fun, 0) ->
    ?assert(wait_failed);
wait_until(Fun, N) ->
    case Fun() of
    true ->
        timer:sleep(1500);
    false ->
        timer:sleep(?COLLECT_INTERVAL + 100),
        wait_until(Fun, N - 1)
    end.

http_post_json(Config, Path, Body, Assertion) ->
    http_upload_raw(Config,  post, Path, Body, "guest", "guest",
                    Assertion, [{"Content-Type", "application/json"}]).
