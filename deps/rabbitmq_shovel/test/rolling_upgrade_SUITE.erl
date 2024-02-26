%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rolling_upgrade_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         child_id_format/1]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
     {group, mnesia_store},
     {group, khepri_store}
    ].

groups() ->
    [{mnesia_store, [], [child_id_format]},
     {khepri_store, [], [child_id_format]}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mnesia_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store__manual, mnesia}]);
init_per_group(khepri_store, Config) ->
    rabbit_ct_helpers:set_config(Config, [{metadata_store__manual, khepri}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = 4,
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodes_count, ClusterSize},
                 {rmq_nodes_clustered, false},
                 {rmq_nodename_suffix, Testcase},
                 {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
                 {ignored_crashes,
                  ["process is stopped by supervisor",
                   "broker forced connection closure with reason 'shutdown'"]}
                ]),
    rabbit_ct_helpers:run_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

child_id_format(Config) ->
    [NewRefNode,
     OldNode,
     NewNode,
     NodeWithQueues] = rabbit_ct_broker_helpers:get_node_configs(
                         Config, nodename),

    %% We build this test on the assumption that `rabbit_ct_broker_helpers'
    %% starts nodes this way:
    %%   Node 1: the primary copy of RabbitMQ the test is started from
    %%   Node 2: the secondary umbrella (if any)
    %%   Node 3: the primary copy
    %%   Node 4: the secondary umbrella
    %%   ...
    %%
    %% Therefore, `Pouet' will use the primary copy, `OldNode' the secondary
    %% umbrella, `NewRefNode' the primary copy, and `NodeWithQueues' the
    %% secondary umbrella.

    %% Declare source and target queues on a node that won't run the shovel.
    ct:pal("Declaring queues on node ~s", [NodeWithQueues]),
    SourceQName = <<"source-queue">>,
    TargetQName = <<"target-queue">>,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(
                   Config, NodeWithQueues),
    lists:foreach(
      fun(QName) ->
              ?assertEqual(
                 {'queue.declare_ok', QName, 0, 0},
                 amqp_channel:call(
                   Ch, #'queue.declare'{queue = QName, durable = true}))
      end, [SourceQName, TargetQName]),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),

    %% Declare a dynamic shovel on the old node.
    ct:pal("Declaring queues on node ~s", [OldNode]),
    VHost = <<"/">>,
    ShovelName = <<"test-shovel">>,
    shovel_test_utils:set_param(
      Config,
      OldNode,
      NodeWithQueues,
      ShovelName,
      [{<<"src-queue">>,  SourceQName},
       {<<"dest-queue">>, TargetQName}]),

    %% We declare the same shovel on a new node that won't be clustered with
    %% the rest. It is only used as a reference node to determine which ID
    %% format the new version is using.
    ct:pal("Declaring queues on node ~s (as a reference)", [NewRefNode]),
    shovel_test_utils:set_param(
      Config,
      NewRefNode,
      NodeWithQueues,
      ShovelName,
      [{<<"src-queue">>,  SourceQName},
       {<<"dest-queue">>, TargetQName}]),

    %% Verify the format of the child ID. Some versions of RabbitMQ 3.11.x and
    %% 3.12.x use a temporary experimental format that was erroneously
    %% backported from a work-in-progress happening in the main branch.
    ct:pal("Checking mirrored_supervisor child ID formats"),
    [{Id0, _, _, _}] = rabbit_ct_broker_helpers:rpc(
                         Config, NewRefNode,
                         mirrored_supervisor, which_children,
                         [rabbit_shovel_dyn_worker_sup_sup]),
    PrimaryIdType = case Id0 of
                        {VHost, ShovelName} ->
                            ct:pal(
                              "The nodes from the primary umbrella are using "
                              "the NORMAL mirrored_supervisor child ID format "
                              "natively"),
                            normal;
                        {[VHost, ShovelName], {VHost, ShovelName}} ->
                            ct:pal(
                              "The nodes from the primary umbrella are using "
                              "the TEMPORARY EXPERIMENTAL mirrored_supervisor "
                              "child ID format natively"),
                            temp_exp
                    end,

    [{Id1, _, _, _}] = rabbit_ct_broker_helpers:rpc(
                         Config, OldNode,
                         mirrored_supervisor, which_children,
                         [rabbit_shovel_dyn_worker_sup_sup]),
    SecondaryIdType = case Id1 of
                          {VHost, ShovelName} ->
                              ct:pal(
                                "The nodes from the secondary umbrella are "
                                "using the NORMAL mirrored_supervisor child "
                                "ID format natively"),
                              normal;
                          {[VHost, ShovelName], {VHost, ShovelName}} ->
                              ct:pal(
                                "The nodes from the secondary umbrella are "
                                "using the TEMPORARY EXPERIMENTAL "
                                "mirrored_supervisor child ID format "
                                "natively"),
                              temp_exp
                      end,
    if
        PrimaryIdType =/= SecondaryIdType ->
            ct:pal(
              "The mirrored_supervisor child ID format is changing between "
              "the primary and the secondary umbrellas!");
        true ->
            ok
    end,

    %% Verify that the supervisors exist on all nodes.
    ct:pal(
      "Checking running mirrored_supervisor children on old node ~s",
      [OldNode]),
    lists:foreach(
      fun(Node) ->
              ?assertMatch(
                 [{Id, _, _, _}]
                   when (SecondaryIdType =:= normal andalso
                         Id =:= {VHost, ShovelName}) orelse
                        (SecondaryIdType =:= temp_exp andalso
                         Id =:= {[VHost, ShovelName], {VHost, ShovelName}}),
                 rabbit_ct_broker_helpers:rpc(
                   Config, Node,
                   mirrored_supervisor, which_children,
                   [rabbit_shovel_dyn_worker_sup_sup]))
      end, [OldNode]),

    %% Simulate a rolling upgrade by:
    %% 1. adding new nodes to the old cluster
    %% 2. stopping the old nodes
    %%
    %% After that, the supervisors run on the new code.
    ct:pal("Clustering nodes ~s and ~s", [OldNode, NewNode]),
    Config1 = rabbit_ct_broker_helpers:cluster_nodes(
                Config, [OldNode, NewNode]),
    ok = rabbit_ct_broker_helpers:stop_broker(Config1, OldNode),
    ok = rabbit_ct_broker_helpers:reset_node(Config1, OldNode),

    shovel_test_utils:await_shovel(Config, NewNode, ShovelName),

    case ?config(metadata_store__manual, Config) of
        mnesia ->
            ok;
        khepri ->
            ok = rabbit_ct_broker_helpers:enable_feature_flag(
                   Config, [NewNode], khepri_db)
    end,

    %% Verify that the supervisors still use the same IDs.
    ct:pal(
      "Checking running mirrored_supervisor children on new node ~s",
      [NewNode]),
    lists:foreach(
      fun(Node) ->
              ?assertMatch(
                 [{Id, _, _, _}]
                   when (SecondaryIdType =:= normal andalso
                         Id =:= {VHost, ShovelName}) orelse
                        (SecondaryIdType =:= temp_exp andalso
                         Id =:= {[VHost, ShovelName], {VHost, ShovelName}}),
                 rabbit_ct_broker_helpers:rpc(
                   Config1, Node,
                   mirrored_supervisor, which_children,
                   [rabbit_shovel_dyn_worker_sup_sup]))
      end, [NewNode]),

    case ?config(metadata_store__manual, Config) of
        mnesia ->
            ok;
        khepri ->
            Path = rabbit_db_msup:khepri_mirrored_supervisor_path(),
            ?assertMatch(
               {ok,
                #{[rabbit_db_msup, mirrored_supervisor_childspec,
                   rabbit_shovel_dyn_worker_sup_sup, VHost, ShovelName] := _}},
               rabbit_ct_broker_helpers:rpc(
                 Config, NewNode, rabbit_khepri, list,
                 [Path ++ [?KHEPRI_WILDCARD_STAR_STAR]]))
    end.
