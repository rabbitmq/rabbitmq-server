%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(simple_ha_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, cluster_size_2},
      {group, cluster_size_3}
    ].

groups() ->
    [
      {cluster_size_2, [], [
          rapid_redeclare,
          declare_synchrony
        ]},
      {cluster_size_3, [], [
          consume_survives_stop,
          consume_survives_sigkill,
          consume_survives_policy,
          auto_resume,
          auto_resume_no_ccn_client,
          confirms_survive_stop,
          confirms_survive_sigkill,
          confirms_survive_policy
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
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 2}
      ]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 3}
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
%% Testcases.
%% -------------------------------------------------------------------

rapid_redeclare(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    Queue = <<"test">>,
    [begin
         amqp_channel:call(Ch, #'queue.declare'{queue   = Queue,
                                                durable = true}),
         amqp_channel:call(Ch, #'queue.delete'{queue  = Queue})
     end || _I <- lists:seq(1, 20)],
    ok.

%% Check that by the time we get a declare-ok back, the slaves are up
%% and in Mnesia.
declare_synchrony(Config) ->
    [Rabbit, Hare] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    RabbitCh = rabbit_ct_client_helpers:open_channel(Config, Rabbit),
    HareCh = rabbit_ct_client_helpers:open_channel(Config, Hare),
    Q = <<"mirrored-queue">>,
    declare(RabbitCh, Q),
    amqp_channel:call(RabbitCh, #'confirm.select'{}),
    amqp_channel:cast(RabbitCh, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props = #'P_basic'{delivery_mode = 2}}),
    amqp_channel:wait_for_confirms(RabbitCh),
    rabbit_ct_broker_helpers:kill_node(Config, Rabbit),

    #'queue.declare_ok'{message_count = 1} = declare(HareCh, Q),
    ok.

declare(Ch, Name) ->
    amqp_channel:call(Ch, #'queue.declare'{durable = true, queue = Name}).

consume_survives_stop(Cf)     -> consume_survives(Cf, fun stop/2,    true).
consume_survives_sigkill(Cf)  -> consume_survives(Cf, fun sigkill/2, true).
consume_survives_policy(Cf)   -> consume_survives(Cf, fun policy/2,  true).
auto_resume(Cf)               -> consume_survives(Cf, fun sigkill/2, false).
auto_resume_no_ccn_client(Cf) -> consume_survives(Cf, fun sigkill/2, false,
                                                  false).

confirms_survive_stop(Cf)    -> confirms_survive(Cf, fun stop/2).
confirms_survive_sigkill(Cf) -> confirms_survive(Cf, fun sigkill/2).
confirms_survive_policy(Cf)  -> confirms_survive(Cf, fun policy/2).

%%----------------------------------------------------------------------------

consume_survives(Config, DeathFun, CancelOnFailover) ->
    consume_survives(Config, DeathFun, CancelOnFailover, true).

consume_survives(Config,
                 DeathFun, CancelOnFailover, CCNSupported) ->
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Msgs = rabbit_ct_helpers:cover_work_factor(Config, 20000),
    Channel1 = rabbit_ct_client_helpers:open_channel(Config, A),
    Channel2 = rabbit_ct_client_helpers:open_channel(Config, B),
    Channel3 = rabbit_ct_client_helpers:open_channel(Config, C),

    %% declare the queue on the master, mirrored to the two slaves
    Queue = <<"test">>,
    amqp_channel:call(Channel1, #'queue.declare'{queue       = Queue,
                                                 auto_delete = false}),

    %% start up a consumer
    ConsCh = case CCNSupported of
                 true  -> Channel2;
                 false -> Port = rabbit_ct_broker_helpers:get_node_config(
                            Config, B, tcp_port_amqp),
                          open_incapable_channel(Port)
             end,
    ConsumerPid = rabbit_ha_test_consumer:create(
                    ConsCh, Queue, self(), CancelOnFailover, Msgs),

    %% send a bunch of messages from the producer
    ProducerPid = rabbit_ha_test_producer:create(Channel3, Queue,
                                                 self(), false, Msgs),
    DeathFun(Config, A),
    %% verify that the consumer got all msgs, or die - the await_response
    %% calls throw an exception if anything goes wrong....
    rabbit_ha_test_consumer:await_response(ConsumerPid),
    rabbit_ha_test_producer:await_response(ProducerPid),
    ok.

confirms_survive(Config, DeathFun) ->
    [A, B, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Msgs = rabbit_ct_helpers:cover_work_factor(Config, 20000),
    Node1Channel = rabbit_ct_client_helpers:open_channel(Config, A),
    Node2Channel = rabbit_ct_client_helpers:open_channel(Config, B),

    %% declare the queue on the master, mirrored to the two slaves
    Queue = <<"test">>,
    amqp_channel:call(Node1Channel,#'queue.declare'{queue       = Queue,
                                                    auto_delete = false,
                                                    durable     = true}),

    %% send a bunch of messages from the producer
    ProducerPid = rabbit_ha_test_producer:create(Node2Channel, Queue,
                                                 self(), true, Msgs),
    DeathFun(Config, A),
    rabbit_ha_test_producer:await_response(ProducerPid),
    ok.

stop(Config, Node) ->
    rabbit_ct_broker_helpers:stop_node_after(Config, Node, 50).

sigkill(Config, Node) ->
    rabbit_ct_broker_helpers:kill_node_after(Config, Node, 50).

policy(Config, Node)->
    Nodes = [
      rabbit_misc:atom_to_binary(N)
      || N <- rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
         N =/= Node],
    rabbit_ct_broker_helpers:set_ha_policy(Config, Node, <<".*">>,
      {<<"nodes">>, Nodes}).

open_incapable_channel(NodePort) ->
    Props = [{<<"capabilities">>, table, []}],
    {ok, ConsConn} =
        amqp_connection:start(#amqp_params_network{port              = NodePort,
                                                   client_properties = Props}),
    {ok, Ch} = amqp_connection:open_channel(ConsConn),
    Ch.
