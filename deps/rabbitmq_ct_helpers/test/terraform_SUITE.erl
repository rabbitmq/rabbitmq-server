%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(terraform_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2,

         run_code_on_one_vm/1, do_run_code_on_one_vm/1,
         run_code_on_three_vms/1, do_run_code_on_three_vms/1,
         run_one_rabbitmq_node/1,
         run_four_rabbitmq_nodes/1
        ]).

all() ->
    [
     {group, direct_vms},
     {group, autoscaling_group}
    ].

groups() ->
    [
     {direct_vms, [parallel], [{group, run_code},
                               {group, run_rabbitmq}]},
     {autoscaling_group, [parallel], [{group, run_code},
                                      {group, run_rabbitmq}]},

     {run_code, [parallel], [run_code_on_one_vm,
                             run_code_on_three_vms]},
     {run_rabbitmq, [parallel], [run_one_rabbitmq_node,
                                 run_four_rabbitmq_nodes]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(autoscaling_group, Config) ->
    TfConfigDir = rabbit_ct_vm_helpers:aws_autoscaling_group_module(Config),
    rabbit_ct_helpers:set_config(
      Config, {terraform_config_dir, TfConfigDir});
init_per_group(Group, Config) ->
    rabbit_ct_helpers:set_config(
      Config, {run_rabbitmq, Group =:= run_rabbitmq}).

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    RunRabbitMQ = ?config(run_rabbitmq, Config),
    InstanceCount = case Testcase of
                        run_code_on_three_vms    -> 3;
                        run_three_rabbitmq_nodes -> 3;
                        % We want more RabbitMQs than VMs.
                        run_four_rabbitmq_nodes  -> 3;
                        _                        -> 1
                    end,
    InstanceName = rabbit_ct_helpers:testcase_absname(Config, Testcase),
    ClusterSize = case Testcase of
                      run_one_rabbitmq_node    -> 1;
                      run_three_rabbitmq_nodes -> 3;
                      % We want more RabbitMQs than VMs.
                      run_four_rabbitmq_nodes  -> 4;
                      _                        -> 0
                  end,
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{terraform_instance_count, InstanceCount},
                 {terraform_instance_name, InstanceName},
                 {rmq_nodename_suffix, Testcase},
                 {rmq_nodes_count, ClusterSize}]),
    case RunRabbitMQ of
        false ->
            rabbit_ct_helpers:run_steps(
              Config1,
              rabbit_ct_vm_helpers:setup_steps());
        true ->
            rabbit_ct_helpers:run_steps(
              Config1,
              [fun rabbit_ct_broker_helpers:run_make_dist/1] ++
              rabbit_ct_vm_helpers:setup_steps() ++
              rabbit_ct_broker_helpers:setup_steps_for_vms())
    end.

end_per_testcase(Testcase, Config) ->
    RunRabbitMQ = ?config(run_rabbitmq, Config),
    Config1 = case RunRabbitMQ of
                  false ->
                      rabbit_ct_helpers:run_steps(
                        Config,
                        rabbit_ct_vm_helpers:teardown_steps());
                  true ->
                      rabbit_ct_helpers:run_steps(
                        Config,
                        rabbit_ct_broker_helpers:teardown_steps_for_vms() ++
                        rabbit_ct_vm_helpers:teardown_steps())
              end,
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Run arbitrary code.
%% -------------------------------------------------------------------

run_code_on_one_vm(Config) ->
    rabbit_ct_vm_helpers:rpc_all(Config,
                                 ?MODULE, do_run_code_on_one_vm, [node()]).

do_run_code_on_one_vm(CTMaster) ->
    CTPeer = node(),
    ct:pal("Testcase running on ~ts", [CTPeer]),
    ?assertNotEqual(CTMaster, CTPeer),
    ?assertEqual(pong, net_adm:ping(CTMaster)).

run_code_on_three_vms(Config) ->
    rabbit_ct_vm_helpers:rpc_all(Config,
                                 ?MODULE, do_run_code_on_three_vms, [node()]).

do_run_code_on_three_vms(CTMaster) ->
    CTPeer = node(),
    ct:pal("Testcase running on ~ts", [CTPeer]),
    ?assertNotEqual(CTMaster, CTPeer),
    ?assertEqual(pong, net_adm:ping(CTMaster)).

%% -------------------------------------------------------------------
%% Run RabbitMQ node.
%% -------------------------------------------------------------------

run_one_rabbitmq_node(Config) ->
    CTPeers = rabbit_ct_vm_helpers:get_ct_peers(Config),
    ?assertEqual([false],
                 [rabbit:is_running(CTPeer) || CTPeer <- CTPeers]),
    RabbitMQNodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ?assertEqual([true],
                 [rabbit:is_running(RabbitMQNode) || RabbitMQNode <- RabbitMQNodes]).

run_four_rabbitmq_nodes(Config) ->
    CTPeers = rabbit_ct_vm_helpers:get_ct_peers(Config),
    ?assertEqual([false, false, false],
                 [rabbit:is_running(CTPeer) || CTPeer <- CTPeers]),
    RabbitMQNodes = lists:sort(
                      rabbit_ct_broker_helpers:get_node_configs(
                        Config, nodename)),
    ?assertEqual([true, true, true, true],
                 [rabbit:is_running(Node) || Node <- RabbitMQNodes]),

    ?assertEqual([true, true, true, true],
                 rabbit_ct_broker_helpers:rpc_all(
                   Config, rabbit_db_cluster, is_clustered, [])),
    ClusteredNodes = lists:sort(
                       rabbit_ct_broker_helpers:rpc(
                         Config, 0, rabbit_nodes, list_running, [])),
    ?assertEqual(ClusteredNodes, RabbitMQNodes).
