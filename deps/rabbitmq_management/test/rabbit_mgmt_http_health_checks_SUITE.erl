%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_http_health_checks_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-import(rabbit_ct_client_helpers, [close_connection/1, close_channel/1,
                                   open_unmanaged_connection/1]).
-import(rabbit_mgmt_test_util, [assert_list/2, assert_item/2, test_item/2,
                                assert_keys/2, assert_no_keys/2,
                                http_get/2, http_get/3, http_get/5,
                                req/4, auth_header/2]).

-import(rabbit_misc, [pget/2]).

-define(COLLECT_INTERVAL, 1000).
-define(PATH_PREFIX, "/custom-prefix").

-compile(export_all).

all() ->
    [
     {group, all_tests},
     {group, single_node}
    ].

groups() ->
    [
     {all_tests, [], all_tests()},
     {single_node, [], [is_quorum_critical_single_node_test,
                        is_mirror_sync_critical_single_node_test]}
    ].

all_tests() -> [
                health_checks_test,
                is_quorum_critical_test,
                is_mirror_sync_critical_test
               ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------
init_per_group(Group, Config0) ->
    PathConfig = {rabbitmq_management, [{path_prefix, ?PATH_PREFIX}]},
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, PathConfig),
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    ClusterSize = case Group of
                      all_tests -> 3;
                      single_node -> 1
                  end,
    NodeConf = [{rmq_nodename_suffix, Group},
                {rmq_nodes_count, ClusterSize},
                {tcp_ports_base}],
    Config2 = rabbit_ct_helpers:set_config(Config1, NodeConf),
    Config3 = rabbit_ct_helpers:run_setup_steps(Config2,
                                                rabbit_ct_broker_helpers:setup_steps() ++
                                                    rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_broker_helpers:enable_feature_flag(Config3, quorum_queue),
    Config3.

end_per_group(_, Config) ->
    inets:stop(),
    Teardown0 = rabbit_ct_client_helpers:teardown_steps(),
    Teardown1 = rabbit_ct_broker_helpers:teardown_steps(),
    Steps = Teardown0 ++ Teardown1,
    rabbit_ct_helpers:run_teardown_steps(Config, Steps).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(is_quorum_critical_test = Testcase, Config) ->
    [_, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    _ = rabbit_ct_broker_helpers:start_node(Config, Server2),
    _ = rabbit_ct_broker_helpers:start_node(Config, Server3),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(is_mirror_sync_critical_test = Testcase, Config) ->
    [_, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    _ = rabbit_ct_broker_helpers:start_node(Config, Server2),
    _ = rabbit_ct_broker_helpers:start_node(Config, Server3),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ha">>),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
health_checks_test(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mgmt),
    http_get(Config, "/health/checks/certificate-expiration/1/days", ?OK),
    http_get(Config, io_lib:format("/health/checks/port-listener/~p", [Port]), ?OK),
    http_get(Config, "/health/checks/protocol-listener/http", ?OK),
    http_get(Config, "/health/checks/virtual-hosts", ?OK),
    http_get(Config, "/health/checks/node-is-mirror-sync-critical", ?OK),
    http_get(Config, "/health/checks/node-is-quorum-critical", ?OK),
    passed.

is_quorum_critical_single_node_test(Config) ->
    Check0 = http_get(Config, "/health/checks/node-is-quorum-critical", ?OK),
    ?assertEqual(<<"single node cluster">>, maps:get(reason, Check0)),
    ?assertEqual(<<"ok">>, maps:get(status, Check0)),
    
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    QName = <<"is_quorum_critical_single_node_test">>,
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                                        durable   = true,
                                                        auto_delete = false,
                                                        arguments = Args})),
    Check1 = http_get(Config, "/health/checks/node-is-quorum-critical", ?OK),
    ?assertEqual(<<"single node cluster">>, maps:get(reason, Check1)),

    passed.

is_quorum_critical_test(Config) ->
    Check0 = http_get(Config, "/health/checks/node-is-quorum-critical", ?OK),
    ?assertEqual(false, maps:is_key(reason, Check0)),
    ?assertEqual(<<"ok">>, maps:get(status, Check0)),
    
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Args = [{<<"x-queue-type">>, longstr, <<"quorum">>}],
    QName = <<"is_quorum_critical_test">>,
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                                        durable   = true,
                                                        auto_delete = false,
                                                        arguments = Args})),
    Check1 = http_get(Config, "/health/checks/node-is-quorum-critical", ?OK),
    ?assertEqual(false, maps:is_key(reason, Check1)),
    
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server3),

    Body = http_get_failed(Config, "/health/checks/node-is-quorum-critical"),
    ?assertEqual(<<"failed">>, maps:get(<<"status">>, Body)),
    ?assertEqual(true, maps:is_key(<<"reason">>, Body)),
    [Queue] = maps:get(<<"queues">>, Body),
    ?assertEqual(QName, maps:get(<<"name">>, Queue)),

    passed.

is_mirror_sync_critical_single_node_test(Config) ->
    Check0 = http_get(Config, "/health/checks/node-is-mirror-sync-critical", ?OK),
    ?assertEqual(<<"single node cluster">>, maps:get(reason, Check0)),
    ?assertEqual(<<"ok">>, maps:get(status, Check0)),
    
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"ha">>, <<"is_mirror_sync.*">>, <<"queues">>,
           [{<<"ha-mode">>, <<"all">>}]),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = <<"is_mirror_sync_critical_single_node_test">>,
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                                        durable   = true,
                                                        auto_delete = false,
                                                        arguments = []})),
    Check1 = http_get(Config, "/health/checks/node-is-mirror-sync-critical", ?OK),
    ?assertEqual(<<"single node cluster">>, maps:get(reason, Check1)),

    passed.

%% TODO clear policy

is_mirror_sync_critical_test(Config) ->
    Path = "/health/checks/node-is-mirror-sync-critical",
    Check0 = http_get(Config, Path, ?OK),
    ?assertEqual(false, maps:is_key(reason, Check0)),
    ?assertEqual(<<"ok">>, maps:get(status, Check0)),
    
    ok = rabbit_ct_broker_helpers:set_policy(
            Config, 0, <<"ha">>, <<"is_mirror_sync.*">>, <<"queues">>,
            [{<<"ha-mode">>, <<"all">>}]),
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    QName = <<"is_mirror_sync_critical_test">>,
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                                        durable   = true,
                                                        auto_delete = false,
                                                        arguments = []})),
    rabbit_ct_helpers:await_condition(
      fun() ->
              {ok, {{_, Code, _}, _, _}} = req(Config, get, Path, [auth_header("guest", "guest")]),
              Code == ?OK
      end),
    Check1 = http_get(Config, Path, ?OK),
    ?assertEqual(false, maps:is_key(reason, Check1)),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server3),

    Body = http_get_failed(Config, Path),
    ?assertEqual(<<"failed">>, maps:get(<<"status">>, Body)),
    ?assertEqual(true, maps:is_key(<<"reason">>, Body)),
    [Queue] = maps:get(<<"queues">>, Body),
    ?assertEqual(QName, maps:get(<<"name">>, Queue)),

    passed.

http_get_failed(Config, Path) ->
    {ok, {{_, Code, _}, _, ResBody}} = req(Config, get, Path, [auth_header("guest", "guest")]),
    ?assertEqual(Code, 503),
    rabbit_json:decode(rabbit_data_coercion:to_binary(ResBody)).

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].
