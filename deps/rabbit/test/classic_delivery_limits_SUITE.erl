%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(classic_delivery_limits_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(quorum_queue_utils, [wait_for_messages/2, wait_for_messages/3]).

all() ->
    [
     {group, cluster_size_1_network},
     {group, cluster_size_2_network}
    ].

groups() ->
    ClusterSize1Tests = [
        subscribe_redelivery_count,
        subscribe_redelivery_limit,
        subscribe_redelivery_policy,
        subscribe_redelivery_operator_policy,
        subscribe_redelivery_limit_with_dlx,
        consume_redelivery_count
    ],
    ClusterSize2Tests = [
        subscribe_redelivery_count,
        subscribe_redelivery_limit,
        subscribe_redelivery_operator_policy,
        subscribe_redelivery_limit_with_dlx,
        consume_redelivery_count,
        subscribe_redelivery_limit_with_mirror_promotion
    ],
    [
      {cluster_size_1_network, [], ClusterSize1Tests},
      {cluster_size_2_network, [], ClusterSize2Tests}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 8}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1_network, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_node_group(cluster_size_1_network, Config1, 1);
init_per_group(cluster_size_2_network, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_node_group(cluster_size_2_network, Config1, 2).

init_per_node_group(Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    Config2 =
      rabbit_ct_helpers:run_steps(Config1,
          rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_broker_helpers:enable_feature_flag(Config2, classic_delivery_limits),
    init_per_multinode_group(Group, Config2).

init_per_multinode_group(cluster_size_2_network, Config) ->
    rabbit_ct_broker_helpers:set_ha_policy_all(Config,
        [{<<"ha-sync-mode">>, <<"automatic">>}]);
init_per_multinode_group(_Group, Config) -> Config.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    rabbit_ct_helpers:set_config(Config, {queue_name, Q}).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------
subscribe_redelivery_count(Config) ->
    [Node | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-queue-type">>, longstr, <<"classic">>}])),

    publish(Ch, CQ),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, CQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = true})
    end,

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    end,

    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H2}}} ->
            ?assertMatch({DCHeader, _, 2}, rabbit_basic:header(DCHeader, H2)),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag2,
                                               multiple     = false}),
            wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]])
    end.

subscribe_redelivery_limit(Config) ->
    [Node | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-queue-type">>, longstr, <<"classic">>},
                                  {<<"x-delivery-limit">>, long, 1}])),

    publish(Ch, CQ),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, CQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[CQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    receive
        {#'basic.deliver'{redelivered  = true}, #amqp_msg{}} ->
            throw(unexpected_redelivery)
    after 2000 ->
            ok
    end.

subscribe_redelivery_policy(Config) ->
    [Node | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-queue-type">>, longstr, <<"classic">>}])),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, <<"delivery-limit">>, <<".*">>, <<"queues">>,
           [{<<"delivery-limit">>, 1}]),

    publish(Ch, CQ),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, CQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[CQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    receive
        {#'basic.deliver'{redelivered  = true}, #amqp_msg{}} ->
            throw(unexpected_redelivery)
    after 2000 ->
            ok
    end,
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"delivery-limit">>).

subscribe_redelivery_operator_policy(Config) ->
    [Node | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-queue-type">>, longstr, <<"classic">>}])),

    ok = rabbit_ct_broker_helpers:set_operator_policy(
           Config, 0, <<"delivery-limit">>, <<".*">>, <<"queues">>,
           [{<<"delivery-limit">>, 1}]),

    publish(Ch, CQ),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, CQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[CQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    receive
        {#'basic.deliver'{redelivered  = true}, #amqp_msg{}} ->
            throw(unexpected_redelivery)
    after 2000 ->
            ok
    end,
    ok = rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"delivery-limit">>).

subscribe_redelivery_limit_with_dlx(Config) ->
    [Node | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    CQ = ?config(queue_name, Config),
    DLX = <<"dlx_queue">>,
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-queue-type">>, longstr, <<"classic">>},
                                  {<<"x-delivery-limit">>, long, 1},
                                  {<<"x-dead-letter-exchange">>, longstr, <<>>},
                                  {<<"x-dead-letter-routing-key">>, longstr, DLX}
                                 ])),
    ?assertEqual({'queue.declare_ok', DLX, 0, 0},
                 declare(Ch, DLX, [{<<"x-queue-type">>, longstr, <<"classic">>}])),

    publish(Ch, CQ),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, CQ, false),

    DCHeader = <<"x-delivery-count">>,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{headers = H0}}} ->
            ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H0)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[CQ, <<"1">>, <<"0">>, <<"1">>]]),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag1,
                          redelivered  = true},
         #amqp_msg{props = #'P_basic'{headers = H1}}} ->
            ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                                multiple     = false,
                                                requeue      = true})
    end,

    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    wait_for_messages(Config, [[DLX, <<"1">>, <<"1">>, <<"0">>]]),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Node),
    DAHeaderDLX = <<"delivery-attempts">>,
    {#'basic.get_ok'{delivery_tag = DeliveryTag2,
                     redelivered = false},
     #amqp_msg{props = #'P_basic'{headers = H2}}} =
        amqp_channel:call(Ch2, #'basic.get'{queue  = DLX,
                                            no_ack = false}),
    ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H2)),
    {<<"x-death">>, array, [{table, DLXTable}]} = rabbit_basic:header(<<"x-death">>, H2),
    ?assertMatch({DAHeaderDLX, _, 1}, rabbit_basic:header(DAHeaderDLX, DLXTable)),
    ?assertMatch({<<"count">>, _, 1}, rabbit_basic:header(<<"count">>, DLXTable)),
    ?assertMatch({<<"reason">>, _, <<"expired">>}, rabbit_basic:header(<<"reason">>, DLXTable)),
    ?assertMatch({<<"queue">>, _, CQ}, rabbit_basic:header(<<"queue">>, DLXTable)),
    amqp_channel:cast(Ch2, #'basic.nack'{delivery_tag = DeliveryTag2,
                                         multiple     = false,
                                         requeue      = false}),

    wait_for_messages(Config, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),
    wait_for_messages(Config, [[DLX, <<"0">>, <<"0">>, <<"0">>]]).

consume_redelivery_count(Config) ->
    [Node | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-queue-type">>, longstr, <<"classic">>}])),

    publish(Ch, CQ),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),

    DCHeader = <<"x-delivery-count">>,

    {#'basic.get_ok'{delivery_tag = DeliveryTag,
                     redelivered = false},
     #amqp_msg{props = #'P_basic'{headers = H0}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = CQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H0)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    %% wait for requeuing
    timer:sleep(500),

    {#'basic.get_ok'{delivery_tag = DeliveryTag1,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H1}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = CQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                        multiple     = false,
                                        requeue      = true}),

    {#'basic.get_ok'{delivery_tag = DeliveryTag2,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H2}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = CQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 2}, rabbit_basic:header(DCHeader, H2)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag2,
                                        multiple     = false,
                                        requeue      = true}),
    ok.

subscribe_redelivery_limit_with_mirror_promotion(Config) ->
    [Node1, Node2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% Stop replica node
    ok = rabbit_ct_broker_helpers:stop_node(Config, Node2),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Node1),
    CQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', CQ, 0, 0},
                 declare(Ch, CQ, [{<<"x-queue-type">>, longstr, <<"classic">>}])),

    ok = rabbit_ct_broker_helpers:set_operator_policy(
           Config, 0, <<"delivery-limit">>, <<".*">>, <<"queues">>,
           [{<<"delivery-limit">>, 3}]),

    publish(Ch, CQ),
    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),

    DCHeader = <<"x-delivery-count">>,
    {#'basic.get_ok'{delivery_tag = DeliveryTag,
                     redelivered = false},
     #amqp_msg{props = #'P_basic'{headers = H0}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = CQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 0}, rabbit_basic:header(DCHeader, H0)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),

    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),

    {#'basic.get_ok'{delivery_tag = DeliveryTag1,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H1}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = CQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 1}, rabbit_basic:header(DCHeader, H1)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag1,
                                        multiple     = false,
                                        requeue      = true}),

    wait_for_messages(Config, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),

    {#'basic.get_ok'{delivery_tag = DeliveryTag2,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H2}}} =
        amqp_channel:call(Ch, #'basic.get'{queue = CQ,
                                           no_ack = false}),
    ?assertMatch({DCHeader, _, 2}, rabbit_basic:header(DCHeader, H2)),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag2,
                                        multiple     = false,
                                        requeue      = true}),

    %% Start node and wait for sync.
    ok = rabbit_ct_broker_helpers:start_node(Config, Node2),

    timer:sleep(500),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Node1),

    %% Mirror promoted on a different node - ensure delivery-count is consistent
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Node2),

    wait_for_messages(Config, 1, [[CQ, <<"1">>, <<"1">>, <<"0">>]]),

    %% Ensure delivery count now at 3
    {#'basic.get_ok'{delivery_tag = DeliveryTag3,
                     redelivered = true},
     #amqp_msg{props = #'P_basic'{headers = H3}}} =
        amqp_channel:call(Ch1, #'basic.get'{queue = CQ,
                                            no_ack = false}),
    ?assertMatch({DCHeader, _, 3}, rabbit_basic:header(DCHeader, H3)),
    amqp_channel:cast(Ch1, #'basic.nack'{delivery_tag = DeliveryTag3,
                                        multiple     = false,
                                        requeue      = true}),

    wait_for_messages(Config, 1, [[CQ, <<"0">>, <<"0">>, <<"0">>]]),

    ok = rabbit_ct_broker_helpers:start_node(Config, Node1),
    ok = rabbit_ct_broker_helpers:clear_operator_policy(Config, 1, <<"delivery-limit">>).

%% -----------------------------------------------------------------------------

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

delete_queues() ->
   [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
    || Q <- rabbit_amqqueue:list()].

publish(Ch, Queue) ->
    publish(Ch, Queue, <<"msg">>).

publish(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 1},
                                     payload = Msg}).

subscribe(Ch, Queue, NoAck) ->
    subscribe(Ch, Queue, NoAck, <<"ctag">>).
subscribe(Ch, Queue, NoAck, CTag) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                consumer_tag = CTag},
                          self()),
    receive
       #'basic.consume_ok'{consumer_tag = CTag} ->
            ok
    end.
