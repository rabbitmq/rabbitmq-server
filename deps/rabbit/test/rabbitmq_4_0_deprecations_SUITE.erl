%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbitmq_4_0_deprecations_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("amqp_client/include/amqp_client.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         when_global_qos_is_permitted_by_default/1,
         when_global_qos_is_not_permitted_from_conf/1,

         set_policy_when_cmq_is_permitted_by_default/1,
         set_policy_when_cmq_is_not_permitted_from_conf/1,

         when_transient_nonexcl_is_permitted_by_default/1,
         when_transient_nonexcl_is_not_permitted_from_conf/1,

         when_queue_master_locator_is_permitted_by_default/1,
         when_queue_master_locator_is_not_permitted_from_conf/1
        ]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
     {group, global_qos},
     {group, classic_queue_mirroring},
     {group, transient_nonexcl_queues},
     {group, queue_master_locator}
    ].

groups() ->
    [
     {global_qos, [],
      [when_global_qos_is_permitted_by_default,
       when_global_qos_is_not_permitted_from_conf]},
     {classic_queue_mirroring, [],
      [set_policy_when_cmq_is_permitted_by_default,
       set_policy_when_cmq_is_not_permitted_from_conf]},
     {transient_nonexcl_queues, [],
      [when_transient_nonexcl_is_permitted_by_default,
       when_transient_nonexcl_is_not_permitted_from_conf]},
     {queue_master_locator, [],
      [when_queue_master_locator_is_permitted_by_default,
       when_queue_master_locator_is_not_permitted_from_conf]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    logger:set_primary_config(level, debug),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    Config.

init_per_group(global_qos, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1});
init_per_group(classic_queue_mirroring, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1});
init_per_group(transient_nonexcl_queues, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1});
init_per_group(queue_master_locator, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1}).

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(
  when_global_qos_is_not_permitted_from_conf = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbit,
                 [{permit_deprecated_features, #{global_qos => false}}]}),
    init_per_testcase1(Testcase, Config1);
init_per_testcase(
  set_policy_when_cmq_is_not_permitted_from_conf = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbit,
                 [{permit_deprecated_features,
                   #{classic_queue_mirroring => false}}]}),
    init_per_testcase1(Testcase, Config1);
init_per_testcase(
    when_transient_nonexcl_is_not_permitted_from_conf = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbit,
                 [{permit_deprecated_features,
                   #{transient_nonexcl_queues => false}}]}),
    init_per_testcase1(Testcase, Config1);
init_per_testcase(
    when_queue_master_locator_is_not_permitted_from_conf = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbit,
                 [{permit_deprecated_features,
                   #{queue_master_locator => false}}]}),
    init_per_testcase1(Testcase, Config1);
init_per_testcase(Testcase, Config) ->
    init_per_testcase1(Testcase, Config).

init_per_testcase1(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
        {keep_pid_file_on_exit, true}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Global QoS.
%% -------------------------------------------------------------------

when_global_qos_is_permitted_by_default(Config) ->
    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ExistingServerChs = list_server_channels(Config, NodeA),
    ClientCh = rabbit_ct_client_helpers:open_channel(Config, NodeA),
    [ServerCh] = list_server_channels(Config, NodeA) -- ExistingServerChs,

    ?assertNot(is_prefetch_limited(ServerCh)),

    %% It's possible to request global QoS and it is accepted by the server.
    ?assertMatch(
       #'basic.qos_ok'{},
       amqp_channel:call(
         ClientCh,
         #'basic.qos'{global = true, prefetch_count = 10})),
    ?assert(is_prefetch_limited(ServerCh)),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `global_qos`: Feature `global_qos` is deprecated",
          "By default, this feature can still be used for now."])).

when_global_qos_is_not_permitted_from_conf(Config) ->
    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ExistingServerChs = list_server_channels(Config, NodeA),
    ClientCh = rabbit_ct_client_helpers:open_channel(Config, NodeA),
    [ServerCh] = list_server_channels(Config, NodeA) -- ExistingServerChs,

    ?assertNot(is_prefetch_limited(ServerCh)),

    %% It's possible to request global QoS but it is ignored by the server.
    ?assertMatch(
       #'basic.qos_ok'{},
       amqp_channel:call(
         ClientCh,
         #'basic.qos'{global = true, prefetch_count = 10})),
    ?assertNot(is_prefetch_limited(ServerCh)),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `global_qos`: Feature `global_qos` is deprecated",
          "Its use is not permitted per the configuration"])).

list_server_channels(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_channel, list, []).

is_prefetch_limited(ServerCh) ->
    GenServer2State = sys:get_state(ServerCh),
    ChState = element(4, GenServer2State),
    ct:pal("Server channel (~p) state: ~p", [ServerCh, ChState]),
    LimiterState = element(3, ChState),
    element(3, LimiterState).

%% -------------------------------------------------------------------
%% Classic queue mirroring.
%% -------------------------------------------------------------------

set_policy_when_cmq_is_permitted_by_default(Config) ->
    set_cmq_policy(Config).

set_policy_when_cmq_is_not_permitted_from_conf(Config) ->
    set_cmq_policy(Config).

set_cmq_policy(Config) ->
    %% CMQ have been removed, any attempt to set a policy
    %% should fail as any other unknown policy.
    ?assertError(
       {badmatch,
        {error_string,
         "Validation failed\n\n[{<<\"ha-mode\">>,<<\"all\">>}] are not recognised policy settings" ++ _}},
       rabbit_ct_broker_helpers:set_policy(
         Config, 0, <<"ha">>, <<".*">>, <<"queues">>, [{<<"ha-mode">>, <<"all">>}])),

    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ?assertNot(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `classic_queue_mirroring`: Classic mirrored queues have been removed."])).

%% -------------------------------------------------------------------
%% Transient non-exclusive queues.
%% -------------------------------------------------------------------

when_transient_nonexcl_is_permitted_by_default(Config) ->
    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, NodeA),

    QName = list_to_binary(atom_to_list(?FUNCTION_NAME)),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch,
         #'queue.declare'{queue = QName,
                          durable = false,
                          exclusive = false})),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `transient_nonexcl_queues`: Feature `transient_nonexcl_queues` is deprecated",
          "By default, this feature can still be used for now."])).

when_transient_nonexcl_is_not_permitted_from_conf(Config) ->
    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, NodeA),

    QName = list_to_binary(atom_to_list(?FUNCTION_NAME)),
    ?assertExit(
       {{shutdown,
         {connection_closing,
          {server_initiated_close, 541,
           <<"INTERNAL_ERROR - Feature `transient_nonexcl_queues` is "
             "deprecated.", _/binary>>}}}, _},
       amqp_channel:call(
         Ch,
         #'queue.declare'{queue = QName,
                          durable = false,
                          exclusive = false})),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `transient_nonexcl_queues`: Feature `transient_nonexcl_queues` is deprecated",
          "Its use is not permitted per the configuration"])).

%% -------------------------------------------------------------------
%% (x-)queue-master-locator
%% -------------------------------------------------------------------

when_queue_master_locator_is_permitted_by_default(Config) ->
    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, NodeA),

    QName = list_to_binary(atom_to_list(?FUNCTION_NAME)),
    ?assertEqual(
       {'queue.declare_ok', QName, 0, 0},
       amqp_channel:call(
         Ch,
         #'queue.declare'{queue = QName,
                          arguments = [{<<"x-queue-master-locator">>, longstr, <<"client-local">>}]})),

    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:set_policy(
         Config, 0, <<"client-local">>, <<".*">>, <<"queues">>, [{<<"queue-master-locator">>, <<"client-local">>}])),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `queue_master_locator`: queue-master-locator is deprecated"])).

when_queue_master_locator_is_not_permitted_from_conf(Config) ->
    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, NodeA),

    QName = list_to_binary(atom_to_list(?FUNCTION_NAME)),
    ?assertExit(
       {{shutdown,
         {connection_closing,
          {server_initiated_close, 541,
           <<"INTERNAL_ERROR - Feature `queue_master_locator` is "
             "deprecated.", _/binary>>}}}, _},
       amqp_channel:call(
         Ch,
         #'queue.declare'{queue = QName,
                          arguments = [{<<"x-queue-master-locator">>, longstr, <<"client-local">>}]})),

    ?assertError(
       {badmatch,
        {error_string,
         "Validation failed\n\nuse of deprecated queue-master-locator argument is not permitted\n"}},
       rabbit_ct_broker_helpers:set_policy(
         Config, 0, <<"client-local">>, <<".*">>, <<"queues">>, [{<<"queue-master-locator">>, <<"client-local">>}])),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `queue_master_locator`: Feature `queue_master_locator` is deprecated",
          "Its use is not permitted per the configuration"])).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

log_file_contains_message(Config, Node, Messages) ->
    _ = catch rabbit_ct_broker_helpers:rpc(
                Config, Node, rabbit_logger_std_h, filesync, [rmq_1_file_1]),
    _ = catch rabbit_ct_broker_helpers:rpc(
                Config, Node, rabbit_logger_std_h, filesync, [rmq_2_file_1]),
    LogLocations = rabbit_ct_broker_helpers:rpc(
                     Config, Node, rabbit, log_locations, []),
    LogFiles = [LogLocation ||
                LogLocation <- LogLocations,
                filelib:is_regular(LogLocation)],
    ?assertNotEqual([], LogFiles),
    lists:any(
      fun(LogFile) ->
              {ok, Content} = file:read_file(LogFile),
              find_messages(Content, Messages)
      end, LogFiles).

find_messages(Content, [Message | Rest]) ->
    case string:find(Content, Message) of
        nomatch   -> false;
        Remaining -> find_messages(Remaining, Rest)
    end;
find_messages(_Content, []) ->
    true.
