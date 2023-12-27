%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(metadata_store_migration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).
-compile(export_all).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
     {group, khepri_migration}
    ].

groups() ->
    [
     {khepri_migration, [], [
                             from_mnesia_to_khepri
                            ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(khepri_migration = Group, Config0) ->
    rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia},
                                            {rmq_nodes_count, 1},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    rabbit_ct_helpers:run_steps(Config, rabbit_ct_broker_helpers:setup_steps() ++ rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    Config2 = rabbit_ct_helpers:testcase_finished(Config1, Testcase),
    rabbit_ct_helpers:run_steps(Config2,
                                rabbit_ct_broker_helpers:teardown_steps()).


%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
from_mnesia_to_khepri(Config) ->
    %% 1) Ensure there is at least one entry on each Mnesia table
    %% 2) Enable the Khepri feature flag
    %% 3) Check that all listings return the same values than before the migration

    %% 1)
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_vhost, add, [<<"test">>, none]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_auth_backend_internal, set_topic_permissions,
           [<<"guest">>, <<"/">>, <<"amq.topic">>, "^t", "^t", <<"acting-user">>]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_policy, set,
           [<<"/">>, <<"policy">>, <<".*">>, [{<<"max-length">>, 100}], 0, <<"queues">>, none]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_runtime_parameters, set_global,
           [<<"test-global-rt">>, <<"good">>, none]),

    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"test">>,
                                           durable = true}),
    amqp_channel:call(Ch, #'queue.declare'{queue = <<"test-transient">>,
                                           durable = false}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = <<"test">>,
                                                             routing_key = <<"test">>}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = <<"test">>,
                                                             routing_key = <<"test">>}),
    rabbit_ct_client_helpers:close_channel(Ch),

    VHosts = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, list, [])),
    ?assertMatch(VHosts, lists:sort([<<"/">>, <<"test">>])),
    Users = rabbit_ct_broker_helpers:rpc(
              Config, 0, rabbit_auth_backend_internal, list_users, []),
    ?assertMatch([_], Users),
    UserPermissions = rabbit_ct_broker_helpers:rpc(
                        Config, 0, rabbit_auth_backend_internal,
                        list_user_permissions, [<<"guest">>]),
    ?assertMatch([_], UserPermissions),
    TopicPermissions = rabbit_ct_broker_helpers:rpc(
                         Config, 0, rabbit_auth_backend_internal,
                         list_user_topic_permissions, [<<"guest">>]),
    ?assertMatch([_], TopicPermissions),
    Policies = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, list, []),
    ?assertMatch([_], Policies),
    GlobalRuntimeParameters = lists:sort(rabbit_ct_broker_helpers:rpc(
                                           Config, 0, rabbit_runtime_parameters, list_global, [])),
    GRPNames = [proplists:get_value(name, RT) || RT <- GlobalRuntimeParameters],
    ?assert(lists:member('test-global-rt', GRPNames)),
    ?assert(lists:member('internal_cluster_id', GRPNames)),
    Queues = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list, [])),
    ?assertMatch([_, _], Queues),
    Exchanges = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list, [])),
    ?assertEqual(14, length(Exchanges)),
    Bindings = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ?assertEqual(4, length(Bindings)),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Maintenance = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_maintenance, get, [Server]),
    ?assertNot(undefined == Maintenance),

    %% 2)
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, Servers, khepri_db),

    %% 3)
    VHostsK = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_vhost, list, [])),
    ?assertEqual(VHosts, VHostsK),
    UsersK = rabbit_ct_broker_helpers:rpc(
              Config, 0, rabbit_auth_backend_internal, list_users, []),
    ?assertEqual(Users, UsersK),
    UserPermissionsK = rabbit_ct_broker_helpers:rpc(
                        Config, 0, rabbit_auth_backend_internal,
                        list_user_permissions, [<<"guest">>]),
    ?assertEqual(UserPermissions, UserPermissionsK),
    TopicPermissionsK = rabbit_ct_broker_helpers:rpc(
                          Config, 0, rabbit_auth_backend_internal,
                          list_user_topic_permissions, [<<"guest">>]),
    ?assertEqual(TopicPermissions, TopicPermissionsK),
    PoliciesK = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, list, []),
    ?assertEqual(Policies, PoliciesK),
    GlobalRuntimeParametersK = lists:sort(rabbit_ct_broker_helpers:rpc(
                                            Config, 0, rabbit_runtime_parameters, list_global, [])),
    ?assertMatch(GlobalRuntimeParametersK, GlobalRuntimeParameters),
    QueuesK = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list, [])),
    ?assertEqual(Queues, QueuesK),
    ExchangesK = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, list, [])),
    ?assertEqual(Exchanges, ExchangesK),
    BindingsK = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ?assertEqual(Bindings, BindingsK),
    MaintenanceK = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_maintenance, get, [Server]),
    ?assertEqual(MaintenanceK, Maintenance),

    ok.

