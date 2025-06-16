%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(delete_shovel_command_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(export_all).

-define(CMD, 'Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteShovelCommand').

all() ->
    [
      {group, non_parallel_tests},
      {group, cluster_size_2}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               delete_not_found,
                               delete,
                               delete_internal,
                               delete_internal_owner
                              ]},
     {cluster_size_2, [], [
                               clear_param_on_different_node
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_2, Config) ->
    init_per_multinode_group(cluster_size_2, Config, 2);
init_per_group(Group, Config) ->
    init_per_multinode_group(Group, Config, 1).

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
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
delete_not_found(Config) ->
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A, vhost => <<"/">>, force => false},
    {error, _} = ?CMD:run([<<"myshovel">>], Opts).

delete(Config) ->
    shovel_test_utils:set_param(
      Config,
      <<"myshovel">>, [{<<"src-queue">>,  <<"src">>},
                       {<<"dest-queue">>, <<"dest">>}]),
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A, vhost => <<"/">>, force => false},
    ok = ?CMD:run([<<"myshovel">>], Opts),
    [] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status,
                                      status, []).

delete_internal(Config) ->
    shovel_test_utils:set_param(
      Config,
      <<"myshovel">>, [{<<"src-queue">>,  <<"src">>},
                       {<<"internal">>, true},
                       {<<"dest-queue">>, <<"dest">>}]),
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A, vhost => <<"/">>, force => false},
    {badrpc,
     {'EXIT',
      {amqp_error, resource_locked,
       "Cannot delete protected shovel 'myshovel' in virtual host '/'.",
       none}}}  = ?CMD:run([<<"myshovel">>], Opts),
    [_] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status,
                                       status, []),

    ForceOpts = #{node => A, vhost => <<"/">>, force => true},
    ok  = ?CMD:run([<<"myshovel">>], ForceOpts),
    [] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status,
                                       status, []).

delete_internal_owner(Config) ->
    shovel_test_utils:set_param(
      Config,
      <<"myshovel">>, [{<<"src-queue">>,  <<"src">>},
                       {<<"internal">>, true},
                       {<<"internal_owner">>, [{<<"name">>, <<"src">>},
                                               {<<"kind">>, <<"queue">>},
                                               {<<"virtual_host">>, <<"/">>}]},
                       {<<"dest-queue">>, <<"dest">>}]),
    [A] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Opts = #{node => A, vhost => <<"/">>, force => false},
    ?assertMatch(
      {badrpc, {'EXIT', {amqp_error, resource_locked, _, none}}},
      ?CMD:run([<<"myshovel">>], Opts)
    ),
    [_] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status,
                                       status, []),

    ForceOpts = #{node => A, vhost => <<"/">>, force => true},
    ok  = ?CMD:run([<<"myshovel">>], ForceOpts),
    [] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_shovel_status,
                                       status, []).

clear_param_on_different_node(Config) ->
    shovel_test_utils:set_param(
      Config,
      <<"myshovel">>, [{<<"src-queue">>,  <<"src">>},
                       {<<"dest-queue">>, <<"dest">>}]),
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [_] = rabbit_ct_broker_helpers:rpc(Config, A, rabbit_shovel_status,
                                       status, []),
    [] = rabbit_ct_broker_helpers:rpc(Config, B, rabbit_shovel_status,
                                      status, []),
    shovel_test_utils:clear_param(Config, B, <<"myshovel">>),
    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, A, rabbit_shovel_status,
                                                  status, []), "Deleted shovel still reported on node A"),
    ?assertEqual([], rabbit_ct_broker_helpers:rpc(Config, B, rabbit_shovel_status,
                                                  status, []), "Deleted shovel still reported on node B").
