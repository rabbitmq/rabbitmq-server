%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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

         join_when_ram_node_type_is_permitted_by_default/1,
         join_when_ram_node_type_is_not_permitted_from_conf/1,

         set_policy_when_cmq_is_permitted_by_default/1,
         set_policy_when_cmq_is_not_permitted_from_conf/1,

         when_transient_nonexcl_is_permitted_by_default/1,
         when_transient_nonexcl_is_not_permitted_from_conf/1
        ]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
     {group, mnesia_store},
     {group, khepri_store}
    ].

groups() ->
    Groups = [
              {global_qos, [],
               [when_global_qos_is_permitted_by_default,
                when_global_qos_is_not_permitted_from_conf]},
              {ram_node_type, [],
               [join_when_ram_node_type_is_permitted_by_default,
                join_when_ram_node_type_is_not_permitted_from_conf]},
              {classic_queue_mirroring, [],
               [set_policy_when_cmq_is_permitted_by_default,
                set_policy_when_cmq_is_not_permitted_from_conf]},
              {transient_nonexcl_queues, [],
               [when_transient_nonexcl_is_permitted_by_default,
                when_transient_nonexcl_is_not_permitted_from_conf]}
             ],
    [{mnesia_store, [], Groups},
     {khepri_store, [], Groups}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    logger:set_primary_config(level, debug),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_helpers:redirect_logger_to_ct_logs/1]).

end_per_suite(Config) ->
    Config.

init_per_group(mnesia_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, mnesia}]);
init_per_group(khepri_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store, khepri}]);
init_per_group(global_qos, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1});
init_per_group(ram_node_type, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 2},
                                          {rmq_nodes_clustered, false}]);
init_per_group(classic_queue_mirroring, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1});
init_per_group(transient_nonexcl_queues, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1});
init_per_group(_Group, Config) ->
    Config.

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
  join_when_ram_node_type_is_not_permitted_from_conf = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbit,
                 [{permit_deprecated_features, #{ram_node_type => false}}]}),
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
%% RAM node type.
%% -------------------------------------------------------------------

join_when_ram_node_type_is_permitted_by_default(Config) ->
    case ?config(metadata_store, Config) of
        mnesia ->
            join_when_ram_node_type_is_permitted_by_default_mnesia(Config);
        khepri ->
            join_when_ram_node_type_is_permitted_by_default_khepri(Config)
    end.

join_when_ram_node_type_is_permitted_by_default_mnesia(Config) ->
    [NodeA, NodeB] = rabbit_ct_broker_helpers:get_node_configs(
                       Config, nodename),

    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(
                Config, NodeA, ["stop_app"]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(
                Config, NodeA, ["join_cluster", "--ram", atom_to_list(NodeB)]),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(
                Config, NodeA, ["start_app"]),

    ?assertEqual([NodeA, NodeB], get_all_nodes(Config, NodeA)),
    ?assertEqual([NodeA, NodeB], get_all_nodes(Config, NodeB)),
    ?assertEqual([NodeB], get_disc_nodes(Config, NodeA)),
    ?assertEqual([NodeB], get_disc_nodes(Config, NodeB)),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `ram_node_type`: Feature `ram_node_type` is "
          "deprecated",
          "By default, this feature can still be used for now."])),

    %% Change the advanced configuration file to turn off RAM node type.
    ConfigFilename0 = rabbit_ct_broker_helpers:get_node_config(
                        Config, NodeA, erlang_node_config_filename),
    ConfigFilename = ConfigFilename0 ++ ".config",
    {ok, [ConfigContent0]} = file:consult(ConfigFilename),
    ConfigContent1 = rabbit_ct_helpers:merge_app_env_in_erlconf(
                       ConfigContent0,
                       {rabbit, [{permit_deprecated_features,
                                  #{ram_node_type => false}}]}),
    ConfigContent2 = lists:flatten(io_lib:format("~p.~n", [ConfigContent1])),
    ok = file:write_file(ConfigFilename, ConfigContent2),
    ?assertEqual({ok, [ConfigContent1]}, file:consult(ConfigFilename)),

    %% Restart the node and see if it was correctly converted to a disc node.
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(
                Config, NodeA, ["stop_app"]),
    Ret = rabbit_ct_broker_helpers:rabbitmqctl(Config, NodeA, ["start_app"]),

    case Ret of
        {ok, _} ->
            ?assertEqual([NodeA, NodeB], get_all_nodes(Config, NodeA)),
            ?assertEqual([NodeA, NodeB], get_all_nodes(Config, NodeB)),
            ?assertEqual([NodeA, NodeB], get_disc_nodes(Config, NodeA)),
            ?assertEqual([NodeA, NodeB], get_disc_nodes(Config, NodeB));
        {error, 69, Message} ->
            Ret1 = re:run(
                     Message, "incompatible_feature_flags",
                     [{capture, none}]),
            case Ret1 of
                match ->
                    {skip, "Incompatible feature flags between nodes A and B"};
                _ ->
                    throw(Ret)
            end
    end.

join_when_ram_node_type_is_permitted_by_default_khepri(Config) ->
    [NodeA, NodeB] = rabbit_ct_broker_helpers:get_node_configs(
                       Config, nodename),

    ok = rabbit_control_helper:command(stop_app, NodeA),
    ?assertMatch(
       {error, 70,
        <<"Error:\nError: `ram` node type is unsupported", _/binary>>},
       rabbit_control_helper:command_with_output(
         join_cluster, NodeA,
         [atom_to_list(NodeB)], [{"--ram", true}])),
    ok = rabbit_control_helper:command(start_app, NodeA),

    ?assertEqual([NodeA], get_all_nodes(Config, NodeA)),
    ?assertEqual([NodeB], get_all_nodes(Config, NodeB)),
    ?assertEqual([NodeA], get_disc_nodes(Config, NodeA)),
    ?assertEqual([NodeB], get_disc_nodes(Config, NodeB)).

join_when_ram_node_type_is_not_permitted_from_conf(Config) ->
    case ?config(metadata_store, Config) of
        mnesia ->
            join_when_ram_node_type_is_not_permitted_from_conf_mnesia(Config);
        khepri ->
            join_when_ram_node_type_is_not_permitted_from_conf_khepri(Config)
    end.

join_when_ram_node_type_is_not_permitted_from_conf_mnesia(Config) ->
    [NodeA, NodeB] = rabbit_ct_broker_helpers:get_node_configs(
                       Config, nodename),

    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(
                Config, NodeA, ["stop_app"]),
    Ret = rabbit_ct_broker_helpers:rabbitmqctl(
            Config, NodeA, ["join_cluster", "--ram", atom_to_list(NodeB)]),
    case Ret of
        {ok, _} ->
            {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(
                        Config, NodeA, ["start_app"]),

            ?assertEqual([NodeA, NodeB], get_all_nodes(Config, NodeA)),
            ?assertEqual([NodeA, NodeB], get_all_nodes(Config, NodeB)),
            ?assertEqual([NodeA, NodeB], get_disc_nodes(Config, NodeA)),
            ?assertEqual([NodeA, NodeB], get_disc_nodes(Config, NodeB)),

            ?assert(
               log_file_contains_message(
                 Config, NodeA,
                 ["Deprecated features: `ram_node_type`: Feature `ram_node_type` is deprecated",
                  "Its use is not permitted per the configuration"]));
        {error, 69, Message} ->
            Ret1 = re:run(
                     Message, "incompatible_feature_flags",
                     [{capture, none}]),
            case Ret1 of
                match ->
                    {skip, "Incompatible feature flags between nodes A and B"};
                _ ->
                    throw(Ret)
            end
    end.

join_when_ram_node_type_is_not_permitted_from_conf_khepri(Config) ->
    [NodeA, NodeB] = rabbit_ct_broker_helpers:get_node_configs(
                       Config, nodename),

    ok = rabbit_control_helper:command(stop_app, NodeA),
    ?assertMatch(
       {error, 70,
        <<"Error:\nError: `ram` node type is unsupported", _/binary>>},
       rabbit_control_helper:command_with_output(
         join_cluster, NodeA,
         [atom_to_list(NodeB)], [{"--ram", true}])),
    ok = rabbit_control_helper:command(start_app, NodeA),

    ?assertEqual([NodeA], get_all_nodes(Config, NodeA)),
    ?assertEqual([NodeB], get_all_nodes(Config, NodeB)),
    ?assertEqual([NodeA], get_disc_nodes(Config, NodeA)),
    ?assertEqual([NodeB], get_disc_nodes(Config, NodeB)).

get_all_nodes(Config, Node) ->
    lists:sort(
      rabbit_ct_broker_helpers:rpc(
        Config, Node, rabbit_mnesia, cluster_nodes, [all])).

get_disc_nodes(Config, Node) ->
    lists:sort(
      rabbit_ct_broker_helpers:rpc(
        Config, Node, rabbit_mnesia, cluster_nodes, [disc])).

%% -------------------------------------------------------------------
%% Classic queue mirroring.
%% -------------------------------------------------------------------

set_policy_when_cmq_is_permitted_by_default(Config) ->
    case ?config(metadata_store, Config) of
        mnesia ->
            set_policy_when_cmq_is_permitted_by_default_mnesia(Config);
        khepri ->
            set_policy_when_cmq_is_permitted_by_default_khepri(Config)
    end.

set_policy_when_cmq_is_permitted_by_default_mnesia(Config) ->
    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:set_ha_policy(
         Config, 0, <<".*">>, <<"all">>)),

    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `classic_queue_mirroring`: Classic mirrored "
          "queues are deprecated.",
          "By default, they can still be used for now."])),

    %% Change the advanced configuration file to turn off classic queue
    %% mirroring.
    ConfigFilename0 = rabbit_ct_broker_helpers:get_node_config(
                        Config, NodeA, erlang_node_config_filename),
    ConfigFilename = ConfigFilename0 ++ ".config",
    {ok, [ConfigContent0]} = file:consult(ConfigFilename),
    ConfigContent1 = rabbit_ct_helpers:merge_app_env_in_erlconf(
                       ConfigContent0,
                       {rabbit, [{permit_deprecated_features,
                                  #{classic_queue_mirroring => false}}]}),
    ConfigContent2 = lists:flatten(io_lib:format("~p.~n", [ConfigContent1])),
    ok = file:write_file(ConfigFilename, ConfigContent2),
    ?assertEqual({ok, [ConfigContent1]}, file:consult(ConfigFilename)),

    %% Restart the node and see if it was correctly converted to a disc node.
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(
                Config, NodeA, ["stop_app"]),
    {error, 69, Message} = rabbit_ct_broker_helpers:rabbitmqctl(
                             Config, NodeA, ["start_app"]),
    Ret = re:run(
            Message,
            ":failed_to_deny_deprecated_features, "
            "\\[:classic_queue_mirroring\\]",
            [{capture, none}]),
    ?assertEqual(match, Ret).

set_policy_when_cmq_is_permitted_by_default_khepri(Config) ->
    ?assertError(
       {badmatch,
        {error_string,
         "Validation failed\n\nClassic mirrored queues are deprecated." ++ _}},
       rabbit_ct_broker_helpers:set_ha_policy(
         Config, 0, <<".*">>, <<"all">>)).

set_policy_when_cmq_is_not_permitted_from_conf(Config) ->
    ?assertError(
       {badmatch,
        {error_string,
         "Validation failed\n\nClassic mirrored queues are deprecated." ++ _}},
       rabbit_ct_broker_helpers:set_ha_policy(
         Config, 0, <<".*">>, <<"all">>)),

    [NodeA] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ?assert(
       log_file_contains_message(
         Config, NodeA,
         ["Deprecated features: `classic_queue_mirroring`: Classic mirrored queues are deprecated.",
          "Their use is not permitted per the configuration"])).

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
