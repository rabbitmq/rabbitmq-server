%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(simple_ha_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(DELAY, 8000).

all() ->
    [
      {group, cluster_size_2},
      {group, cluster_size_3}
    ].

groups() ->
    RejectTests = [
      rejects_survive_stop,
      rejects_survive_policy
    ],
    [
      {cluster_size_2, [], [
          rapid_redeclare,
          declare_synchrony,
          clean_up_exclusive_queues
        ]},
      {cluster_size_3, [], [
          consume_survives_stop,
          consume_survives_policy,
          auto_resume,
          auto_resume_no_ccn_client,
          confirms_survive_stop,
          confirms_survive_policy,
          {overflow_reject_publish, [], RejectTests},
          {overflow_reject_publish_dlx, [], RejectTests}
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
      ]);
init_per_group(overflow_reject_publish, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {overflow, <<"reject-publish">>}
      ]);
init_per_group(overflow_reject_publish_dlx, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {overflow, <<"reject-publish-dlx">>}
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

%% Check that by the time we get a declare-ok back, the mirrors are up
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

%% Ensure that exclusive queues are cleaned up when part of ha cluster
%% and node is killed abruptly then restarted
clean_up_exclusive_queues(Config) ->
    QName = <<"excl">>,
    rabbit_ct_broker_helpers:set_ha_policy(Config, 0, <<".*">>, <<"all">>),
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ChA = rabbit_ct_client_helpers:open_channel(Config, A),
    amqp_channel:call(ChA, #'queue.declare'{queue = QName,
                                            exclusive = true}),
    ok = rabbit_ct_broker_helpers:kill_node(Config, A),
    timer:sleep(?DELAY),
    [] = rabbit_ct_broker_helpers:rpc(Config, B, rabbit_amqqueue, list, []),
    ok = rabbit_ct_broker_helpers:start_node(Config, A),
    timer:sleep(?DELAY),
    [[],[]] = rabbit_ct_broker_helpers:rpc_all(Config, rabbit_amqqueue, list, []),
    ok.

consume_survives_stop(Cf)     -> consume_survives(Cf, fun stop/2,    true).
consume_survives_sigkill(Cf)  -> consume_survives(Cf, fun sigkill/2, true).
consume_survives_policy(Cf)   -> consume_survives(Cf, fun policy/2,  true).
auto_resume(Cf)               -> consume_survives(Cf, fun sigkill/2, false).
auto_resume_no_ccn_client(Cf) -> consume_survives(Cf, fun sigkill/2, false,
                                                  false).

confirms_survive_stop(Cf)    -> confirms_survive(Cf, fun stop/2).
confirms_survive_policy(Cf)  -> confirms_survive(Cf, fun policy/2).

rejects_survive_stop(Cf) -> rejects_survive(Cf, fun stop/2).
rejects_survive_policy(Cf) -> rejects_survive(Cf, fun policy/2).

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

    %% declare the queue on the master, mirrored to the two mirrors
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
    ct:pal("awaiting produce ~w", [ProducerPid]),
    rabbit_ha_test_producer:await_response(ProducerPid),
    ct:pal("awaiting consumer ~w", [ConsumerPid]),
    rabbit_ha_test_consumer:await_response(ConsumerPid),
    ok.

confirms_survive(Config, DeathFun) ->
    [A, B, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Msgs = rabbit_ct_helpers:cover_work_factor(Config, 20000),
    Node1Channel = rabbit_ct_client_helpers:open_channel(Config, A),
    Node2Channel = rabbit_ct_client_helpers:open_channel(Config, B),

    %% declare the queue on the master, mirrored to the two mirrors
    Queue = <<"test">>,
    amqp_channel:call(Node1Channel,#'queue.declare'{queue       = Queue,
                                                    auto_delete = false,
                                                    durable     = true}),

    %% send one message to ensure the channel is flowing
    amqp_channel:register_confirm_handler(Node1Channel, self()),
    #'confirm.select_ok'{} = amqp_channel:call(Node1Channel, #'confirm.select'{}),

    Payload = <<"initial message">>,
    ok = amqp_channel:call(Node1Channel,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{payload = Payload}),

    ok = receive
             #'basic.ack'{multiple = false} -> ok;
             #'basic.nack'{multiple = false} -> message_nacked
         after
             5000 -> confirm_not_received
         end,

    %% send a bunch of messages from the producer
    ProducerPid = rabbit_ha_test_producer:create(Node2Channel, Queue,
                                                 self(), true, Msgs),
    DeathFun(Config, A),
    rabbit_ha_test_producer:await_response(ProducerPid),
    ok.

rejects_survive(Config, DeathFun) ->
    [A, B, _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Msgs = rabbit_ct_helpers:cover_work_factor(Config, 20000),
    Node1Channel = rabbit_ct_client_helpers:open_channel(Config, A),
    Node2Channel = rabbit_ct_client_helpers:open_channel(Config, B),

    %% declare the queue on the master, mirrored to the two mirrors
    XOverflow = ?config(overflow, Config),
    Queue = <<"test_rejects", "_", XOverflow/binary>>,
    amqp_channel:call(Node1Channel,#'queue.declare'{queue       = Queue,
                                                    auto_delete = false,
                                                    durable     = true,
                                                    arguments = [{<<"x-max-length">>, long, 1},
                                                                 {<<"x-overflow">>, longstr, XOverflow}]}),

    amqp_channel:register_confirm_handler(Node1Channel, self()),
    #'confirm.select_ok'{} = amqp_channel:call(Node1Channel, #'confirm.select'{}),

    Payload = <<"there can be only one">>,
    ok = amqp_channel:call(Node1Channel,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{payload = Payload}),

    ok = receive
             #'basic.ack'{multiple = false} -> ok;
             #'basic.nack'{multiple = false} -> message_nacked
         after
             5000 -> confirm_not_received
         end,

    %% send a bunch of messages from the producer. They should all be nacked, as the queue is full.
    ProducerPid = rabbit_ha_test_producer:create(Node2Channel, Queue,
                                                 self(), true, Msgs, nacks),
    DeathFun(Config, A),
    rabbit_ha_test_producer:await_response(ProducerPid),

    {#'basic.get_ok'{}, #amqp_msg{payload = Payload}} =
        amqp_channel:call(Node2Channel, #'basic.get'{queue = Queue}),
    %% There is only one message.
    #'basic.get_empty'{} = amqp_channel:call(Node2Channel, #'basic.get'{queue = Queue}),
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

declare_exclusive(Ch, QueueName, Args) ->
    Declare = #'queue.declare'{queue = QueueName,
        exclusive = true,
        arguments = Args
    },
    #'queue.declare_ok'{} = amqp_channel:call(Ch, Declare).

subscribe(Ch, QueueName) ->
    ConsumeOk  = amqp_channel:call(Ch, #'basic.consume'{queue = QueueName,
                                                        no_ack = true}),
    #'basic.consume_ok'{} = ConsumeOk,
    receive ConsumeOk -> ok after ?DELAY -> throw(consume_ok_timeout) end.

receive_cancels(Cancels) ->
    receive
        #'basic.cancel'{} = C ->
            receive_cancels([C|Cancels])
    after ?DELAY ->
        Cancels
    end.

receive_messages(All) ->
    receive
        {#'basic.deliver'{}, Msg} ->
            receive_messages([Msg|All])
    after ?DELAY ->
        lists:reverse(All)
    end.
