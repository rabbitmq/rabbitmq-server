%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(many_node_ha_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, cluster_size_6}
    ].

groups() ->
    [
      {cluster_size_6, [], [
          kill_intermediate
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

init_per_group(cluster_size_6, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 6}
      ]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++ [
        fun rabbit_ct_broker_helpers:set_ha_policy_all/1
      ]).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

kill_intermediate(Config) ->
    [A, B, C, D, E, F] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    Msgs            = rabbit_ct_helpers:cover_work_factor(Config, 20000),
    MasterChannel   = rabbit_ct_client_helpers:open_channel(Config, A),
    ConsumerChannel = rabbit_ct_client_helpers:open_channel(Config, E),
    ProducerChannel = rabbit_ct_client_helpers:open_channel(Config, F),
    Queue = <<"test">>,
    amqp_channel:call(MasterChannel, #'queue.declare'{queue       = Queue,
                                                      auto_delete = false}),

    %% TODO: this seems *highly* timing dependant - the assumption being
    %% that the kill will work quickly enough that there will still be
    %% some messages in-flight that we *must* receive despite the intervening
    %% node deaths. It would be nice if we could find a means to do this
    %% in a way that is not actually timing dependent.

    %% Worse still, it assumes that killing the master will cause a
    %% failover to Slave1, and so on. Nope.

    ConsumerPid = rabbit_ha_test_consumer:create(ConsumerChannel,
                                                 Queue, self(), false, Msgs),

    ProducerPid = rabbit_ha_test_producer:create(ProducerChannel,
                                                 Queue, self(), false, Msgs),

    %% create a killer for the master and the first 3 slaves
    [rabbit_ct_broker_helpers:kill_node_after(Config, Node, Time) ||
        {Node, Time} <- [{A, 50},
                         {B, 50},
                         {C, 100},
                         {D, 100}]],

    %% verify that the consumer got all msgs, or die, or time out
    rabbit_ha_test_producer:await_response(ProducerPid),
    rabbit_ha_test_consumer:await_response(ConsumerPid),
    ok.
