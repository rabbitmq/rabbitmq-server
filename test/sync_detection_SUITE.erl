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

-module(sync_detection_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(LOOP_RECURSION_DELAY, 100).

all() ->
    [
      {group, cluster_size_2},
      {group, cluster_size_3}
    ].

groups() ->
    [
      {cluster_size_2, [], [
          slave_synchronization
        ]},
      {cluster_size_3, [], [
          slave_synchronization_ttl
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
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 2}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, ClusterSize},
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++ [
        fun rabbit_ct_broker_helpers:set_ha_policy_two_pos/1,
        fun rabbit_ct_broker_helpers:set_ha_policy_two_pos_batch_sync/1
      ]).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

slave_synchronization(Config) ->
    [Master, Slave] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    Channel = rabbit_ct_client_helpers:open_channel(Config, Master),
    Queue = <<"ha.two.test">>,
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = Queue,
                                                    auto_delete = false}),

    %% The comments on the right are the queue length and the pending acks on
    %% the master.
    rabbit_ct_broker_helpers:stop_broker(Config, Slave),

    %% We get and ack one message when the slave is down, and check that when we
    %% start the slave it's not marked as synced until ack the message.  We also
    %% publish another message when the slave is up.
    send_dummy_message(Channel, Queue),                                 % 1 - 0
    {#'basic.get_ok'{delivery_tag = Tag1}, _} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue}),        % 0 - 1

    rabbit_ct_broker_helpers:start_broker(Config, Slave),

    slave_unsynced(Master, Queue),
    send_dummy_message(Channel, Queue),                                 % 1 - 1
    slave_unsynced(Master, Queue),

    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag1}),      % 1 - 0

    slave_synced(Master, Queue),

    %% We restart the slave and we send a message, so that the slave will only
    %% have one of the messages.
    rabbit_ct_broker_helpers:stop_broker(Config, Slave),
    rabbit_ct_broker_helpers:start_broker(Config, Slave),

    send_dummy_message(Channel, Queue),                                 % 2 - 0

    slave_unsynced(Master, Queue),

    %% We reject the message that the slave doesn't have, and verify that it's
    %% still unsynced
    {#'basic.get_ok'{delivery_tag = Tag2}, _} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue}),        % 1 - 1
    slave_unsynced(Master, Queue),
    amqp_channel:cast(Channel, #'basic.reject'{ delivery_tag = Tag2,
                                                requeue      = true }), % 2 - 0
    slave_unsynced(Master, Queue),
    {#'basic.get_ok'{delivery_tag = Tag3}, _} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue}),        % 1 - 1
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag3}),      % 1 - 0
    slave_synced(Master, Queue),
    {#'basic.get_ok'{delivery_tag = Tag4}, _} =
        amqp_channel:call(Channel, #'basic.get'{queue = Queue}),        % 0 - 1
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag4}),      % 0 - 0
    slave_synced(Master, Queue).

slave_synchronization_ttl(Config) ->
    [Master, Slave, DLX] = rabbit_ct_broker_helpers:get_node_configs(Config,
      nodename),
    Channel = rabbit_ct_client_helpers:open_channel(Config, Master),
    DLXChannel = rabbit_ct_client_helpers:open_channel(Config, DLX),

    %% We declare a DLX queue to wait for messages to be TTL'ed
    DLXQueue = <<"dlx-queue">>,
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = DLXQueue,
                                                    auto_delete = false}),

    TestMsgTTL = 5000,
    Queue = <<"ha.two.test">>,
    %% Sadly we need fairly high numbers for the TTL because starting/stopping
    %% nodes takes a fair amount of time.
    Args = [{<<"x-message-ttl">>, long, TestMsgTTL},
            {<<"x-dead-letter-exchange">>, longstr, <<>>},
            {<<"x-dead-letter-routing-key">>, longstr, DLXQueue}],
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = Queue,
                                                    auto_delete = false,
                                                    arguments   = Args}),

    slave_synced(Master, Queue),

    %% All unknown
    rabbit_ct_broker_helpers:stop_broker(Config, Slave),
    send_dummy_message(Channel, Queue),
    send_dummy_message(Channel, Queue),
    rabbit_ct_broker_helpers:start_broker(Config, Slave),
    slave_unsynced(Master, Queue),
    wait_for_messages(DLXQueue, DLXChannel, 2),
    slave_synced(Master, Queue),

    %% 1 unknown, 1 known
    rabbit_ct_broker_helpers:stop_broker(Config, Slave),
    send_dummy_message(Channel, Queue),
    rabbit_ct_broker_helpers:start_broker(Config, Slave),
    slave_unsynced(Master, Queue),
    send_dummy_message(Channel, Queue),
    slave_unsynced(Master, Queue),
    wait_for_messages(DLXQueue, DLXChannel, 2),
    slave_synced(Master, Queue),

    %% %% both known
    send_dummy_message(Channel, Queue),
    send_dummy_message(Channel, Queue),
    slave_synced(Master, Queue),
    wait_for_messages(DLXQueue, DLXChannel, 2),
    slave_synced(Master, Queue),

    ok.

send_dummy_message(Channel, Queue) ->
    Payload = <<"foo">>,
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Queue},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}).

slave_pids(Node, Queue) ->
    {ok, Q} = rpc:call(Node, rabbit_amqqueue, lookup,
                       [rabbit_misc:r(<<"/">>, queue, Queue)]),
    SSP = synchronised_slave_pids,
    [{SSP, Pids}] = rpc:call(Node, rabbit_amqqueue, info, [Q, [SSP]]),
    case Pids of
        '' -> [];
        _  -> Pids
    end.

%% The mnesia synchronization takes a while, but we don't want to wait for the
%% test to fail, since the timetrap is quite high.
wait_for_sync_status(Status, Node, Queue) ->
    Max = 90000 / ?LOOP_RECURSION_DELAY,
    wait_for_sync_status(0, Max, Status, Node, Queue).

wait_for_sync_status(N, Max, Status, Node, Queue) when N >= Max ->
    erlang:error({sync_status_max_tries_failed,
                  [{queue, Queue},
                   {node, Node},
                   {expected_status, Status},
                   {max_tried, Max}]});
wait_for_sync_status(N, Max, Status, Node, Queue) ->
    Synced = length(slave_pids(Node, Queue)) =:= 1,
    case Synced =:= Status of
        true  -> ok;
        false -> timer:sleep(?LOOP_RECURSION_DELAY),
                 wait_for_sync_status(N + 1, Max, Status, Node, Queue)
    end.

slave_synced(Node, Queue) ->
    wait_for_sync_status(true, Node, Queue).

slave_unsynced(Node, Queue) ->
    wait_for_sync_status(false, Node, Queue).

wait_for_messages(Queue, Channel, N) ->
    Sub = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = CTag} = amqp_channel:call(Channel, Sub),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    lists:foreach(
      fun (_) -> receive
                     {#'basic.deliver'{delivery_tag = Tag}, _Content} ->
                         amqp_channel:cast(Channel,
                                           #'basic.ack'{delivery_tag = Tag})
                 end
      end, lists:seq(1, N)),
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = CTag}).
