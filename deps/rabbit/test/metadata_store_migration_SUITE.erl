%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(metadata_store_migration_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("amqqueue.hrl").

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
                             from_mnesia_to_khepri,
                             amqqueue_v1_mirrored_supervisor_migration
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

init_per_group(khepri_migration, Config0) ->
    rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia},
                                            {rmq_nodes_count, 1},
                                            {tcp_ports_base}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           {rmq_nodename_suffix, Testcase}),
    rabbit_ct_helpers:run_steps(Config1,
                                rabbit_ct_broker_helpers:setup_steps()
                                ++ rabbit_ct_client_helpers:setup_steps()).

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
    ?assertEqual(15, length(Exchanges)),
    Bindings = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ?assertEqual(4, length(Bindings)),
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Maintenance = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_maintenance, get, [Server]),
    ?assertNot(undefined == Maintenance),

    PluginsDataDir = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_plugins, user_provided_plugins_data_dir, []),
    ok = file:make_dir(PluginsDataDir),
    PluginDataFile = filename:join(PluginsDataDir, "test.txt"),
    ok = file:write_file(PluginDataFile, "test content"),

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

    %% plugin data file should be preserved
    ?assertEqual({ok, <<"test content">>}, file:read_file(PluginDataFile)),

    ok.

%% Verify that amqqueue_v1 records (tuple size 19) embedded in
%% mirrored_sup_childspec Mnesia records are upgraded to v2 (tuple size 21)
%% during the Mnesia-to-Khepri migration.
%%
%% Such v1 records may still exist embedded if they were created in an old
%% RabbitMQ version, where classic queues used the amqqueue_v1 record format.
%% Migration to Khepri is a good opportunity to get rid of the old format.
amqqueue_v1_mirrored_supervisor_migration(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE,
               amqqueue_v1_mirrored_supervisor_migration1, [Config]).

%% Callback used by rabbit_db_msup:khepri_mirrored_supervisor_path/2
%% to convert the mirrored supervisor child ID to a Khepri path.
%% This mimics rabbit_federation_queue_link_sup_sup:id_to_khepri_path/1
%% but is defined here so the test does not depend on the federation plugin.
id_to_khepri_path(Id) when ?is_amqqueue(Id) ->
    #resource{virtual_host = VHost, name = Name} = amqqueue:get_name(Id),
    [queue, VHost, Name].

amqqueue_v1_mirrored_supervisor_migration1(_Config) ->
    QName = #resource{virtual_host = <<"/">>, kind = queue,
                      name = <<"v1-test-queue">>},
    V1Queue = make_v1_queue(QName),
    19 = tuple_size(V1Queue),

    %% Use this test module as the Group so that id_to_khepri_path/1
    %% defined above is called during the migration, avoiding a
    %% dependency on the federation plugin.
    Group = ?MODULE,
    ChildSpec = {V1Queue,
                 {rabbit_federation_link_sup, start_link,
                  [rabbit_federation_queue_link, V1Queue]},
                 transient, infinity, supervisor,
                 [rabbit_federation_link_sup]},
    MSupRecord = {mirrored_sup_childspec,
                  {Group, V1Queue},
                  undefined,
                  ChildSpec},

    %% Insert the v1 record directly into the mirrored_sup_childspec
    %% Mnesia table, simulating a leftover from a pre-3.8 installation.
    {atomic, ok} = mnesia:transaction(
                     fun() ->
                             mnesia:write(mirrored_sup_childspec, MSupRecord, write)
                     end),

    %% Verify the record is in Mnesia.
    {atomic, [MSupRecord]} =
    mnesia:transaction(
      fun() ->
              mnesia:read(mirrored_sup_childspec, {Group, V1Queue})
      end),

    %% Enable Khepri. This triggers the Mnesia-to-Khepri migration.
    %% Before the fix, this would crash with a function_clause error
    %% in id_to_khepri_path/1 because the ?is_amqqueue guard expects
    %% tuple size 21.
    ok = rabbit_feature_flags:enable(khepri_db),

    %% Compute the expected upgraded v2 record so we can look it up
    %% in Khepri after migration.
    V2Queue = rabbit_db_msup_m2k_converter:amqqueue_v1_to_v2(V1Queue),
    ?assertEqual(21, tuple_size(V2Queue)),

    %% Verify the record was migrated to Khepri with the v1 record
    %% upgraded to v2.
    Path = rabbit_db_msup:khepri_mirrored_supervisor_path(Group, V2Queue),
    {ok, MigratedRecord} = rabbit_khepri:get(Path),

    %% The migrated record should exist and contain a valid
    %% mirrored_sup_childspec.
    {mirrored_sup_childspec, {_, MigratedId}, _, _} = MigratedRecord,

    %% The Id in the key must be a v2 amqqueue record (tuple size 21).
    ?assert(?is_amqqueue(MigratedId)),
    ?assertEqual(QName, amqqueue:get_name(MigratedId)),

    passed.

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

%% Build an amqqueue_v1 record (tuple size 19)
%% as it would have been stored on RabbitMQ 3.7.x or earlier,
%% before the quorum_queue feature flag added the `type` and
%% `type_state` fields in 3.8.0.
-spec make_v1_queue(rabbit_amqqueue:name()) -> tuple().
make_v1_queue(QName) ->
    Policy = [{vhost, <<"/">>},
              {name, <<"test-policy">>},
              {pattern, <<"^v1-">>},
              {'apply-to', <<"queues">>},
              {definition, [{<<"federation-upstream-set">>, <<"all">>}]},
              {priority, 0}],
    {amqqueue,
     QName,
     true,           %% durable
     false,          %% auto_delete
     none,           %% exclusive_owner
     [],             %% arguments
     none,           %% pid
     none,           %% slave_pids
     none,           %% sync_slave_pids
     none,           %% recoverable_slaves
     Policy,         %% policy
     undefined,      %% operator_policy
     none,           %% gm_pids
     none,           %% decorators
     none,           %% state
     0,              %% policy_version
     [],             %% slave_pids_pending_shutdown
     <<"/">>,        %% vhost
     #{user => <<"guest">>}  %% options
    }.

