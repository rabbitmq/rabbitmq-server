%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_transform_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(quorum_queue_utils, [wait_for_messages/2]).

-define(NET_TICKTIME, 60).

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    ClusterSize1Tests = [
        successful_supported_transform,
        unsuccessful_unsupported_transform,
        unsuccessful_supported_transform,
        validate_transform_safety_checks
    ],
    ClusterSize3Tests = [
        successful_supported_transform,
        unsuccessful_unsupported_transform,
        unsuccessful_supported_transform,
        unsuccessful_supported_mirror_transform
    ],
    [
      {cluster_size_1, [], ClusterSize1Tests},
      {cluster_size_3, [], ClusterSize3Tests}
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

init_per_group(cluster_size_1, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_multinode_group(cluster_size_1, Config1, 1, []);
init_per_group(cluster_size_3, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    Config2 = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, ?NET_TICKTIME}]),
    Policy = [fun rabbit_ct_broker_helpers:set_ha_policy_all/1],
    init_per_multinode_group(cluster_size_3, Config2, 3, Policy).

init_per_multinode_group(_Group, Config, NodeCount, Policy) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    Config2 =
      rabbit_ct_helpers:run_steps(Config1,
          rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps() ++ Policy),
    Config2.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, {queue_name, Q}),
    setup_queues(Config1).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------
successful_supported_transform(Config) ->
    [Node1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_table, delete, [rabbit_transform])),

    %% explictly trigger successful transform
    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_core_ff, classic_delivery_limits_migration,
        [classic_delivery_limits, [], enable])),

    ?assertEqual(true, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_transform, exists, [message_properties, message_properties_v2])),
    rabbit_ct_broker_helpers:rpc(Config, Node1, rabbit_transform, delete_version,
        [message_properties, message_properties_v2]),
    ?assertEqual({ok, {'transform', message_properties, [], #{}}},
        rabbit_ct_broker_helpers:rpc(Config, Node1, rabbit_transform, lookup, [message_properties])),
    ok = rabbit_ct_broker_helpers:rpc(Config, Node1, rabbit_transform, delete, [message_properties]),
    ?assertEqual(false, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_transform, exists, [message_properties, message_properties_v2])).

unsuccessful_unsupported_transform(Config) ->
    [Node1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Queues = get_classic_queues(Config, Node1),
    Fun = fun(MsgProp) -> ok, MsgProp end,
    TF = #transform{} = rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_transform, new, [undefined_transform, undefined_transform_v1]),
    ?assertEqual([{error, {unsupported_transform, {undefined_transform, undefined_transform_v1}}}],
        rabbit_ct_broker_helpers:rpc(Config, Node1, rabbit_amqqueue, transform, [TF, Fun, Queues])),
    ?assertEqual(true, ensure_all_queues_alive(Config, Node1, Queues)),
    ?assertEqual(false, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_transform, exists, [unsupported_transform, unsupported_transform_v1])).

unsuccessful_supported_transform(Config) ->
    [Node1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_table, delete, [rabbit_transform])),

    Queues = get_classic_queues(Config, Node1),
    Fun = fun(MsgProp) -> throw({error, unsuccessful_transform}), MsgProp end,
    TF = #transform{} = rabbit_transform:new(message_properties, message_properties_v2),
    ?assertEqual([{error, unsuccessful_transform}], rabbit_ct_broker_helpers:rpc(Config,
        Node1, rabbit_amqqueue, transform, [TF, Fun, Queues])),
    ?assertEqual(true, ensure_all_queues_alive(Config, Node1, Queues)),
    ?assertEqual(false, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_transform, exists, [message_properties, message_properties_v2])).

unsuccessful_supported_mirror_transform(Config) ->
    [Node1 | _] = _Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Queues = get_classic_queues(Config, Node1),
    Fun = fun(MsgProp) -> throw(mirror_queue_error), MsgProp end,
    TF = #transform{} = rabbit_transform:new(message_properties, message_properties_v2),
    Replies = rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_amqqueue, transform, [TF, Fun, Queues]),

    ErrorReplies = lists:flatten([R || R <- Replies, R =:= {error, mirror_queue_error}]),
    % ?assertEqual(length(Nodes), length(ErrorReplies)),
    ?assertEqual(1, length(ErrorReplies)),
    ?assertEqual(true, ensure_all_queues_alive(Config, Node1, Queues)),
    ?assertEqual(false, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_transform, exists, [message_properties, message_properties_v2])).

validate_transform_safety_checks(Config) ->
    [Node1 | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% 3 queues with 10 messages each
    Queues = get_classic_queues(Config, Node1),
    FirstQName = rabbit_misc:rs(amqqueue:get_name(hd(Queues))),

    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_transform, check_safety, [Queues])),

    %% verify 'queue_transform_message_threshold'
    OriginalMsgThreshold =
        rabbit_ct_broker_helpers:rpc(Config, Node1,
            application, get_env, [rabbit, queue_transform_memory_threshold]),

    rabbit_ct_broker_helpers:rpc(Config, Node1,
        application, set_env, [rabbit, queue_transform_message_threshold, 9]),

    ?assertEqual({FirstQName, transform_message_threshold_exceeded},
                  rabbit_ct_broker_helpers:rpc(Config, Node1,
                      rabbit_transform, check_safety, [Queues])),

    rabbit_ct_broker_helpers:rpc(Config, Node1,
       application, set_env, [rabbit, queue_transform_message_threshold,
       OriginalMsgThreshold]),

    %% verify 'queue_transform_memory_threshold'
    OriginalMemThreshold =
        rabbit_ct_broker_helpers:rpc(Config, Node1,
            application, get_env, [rabbit, queue_transform_memory_threshold]),

    rabbit_ct_broker_helpers:rpc(Config, Node1,
        application, set_env, [rabbit, queue_transform_memory_threshold, 100]),

    ?assertEqual({FirstQName, transform_memory_threshold_exceeded},
        rabbit_ct_broker_helpers:rpc(Config, Node1,
            rabbit_transform, check_safety, [Queues])),

    rabbit_ct_broker_helpers:rpc(Config, Node1,
        application, set_env, [rabbit, queue_transform_memory_threshold,
        OriginalMemThreshold]),

    %% verify 'queue_transform_total_message_threshold'
    OriginalTotalMsgThreshold =
        rabbit_ct_broker_helpers:rpc(Config, Node1,
            application, get_env, [rabbit, queue_transform_total_message_threshold]),

    rabbit_ct_broker_helpers:rpc(Config, Node1,
        application, set_env, [rabbit, queue_transform_total_message_threshold, 25]),

    ?assertEqual(transform_total_message_threshold_exceeded,
        rabbit_ct_broker_helpers:rpc(Config, Node1,
                 rabbit_transform, check_safety, [Queues])),

    rabbit_ct_broker_helpers:rpc(Config, Node1,
        application, set_env, [rabbit, queue_transform_total_message_threshold,
        OriginalTotalMsgThreshold]),

    ?assertEqual(ok, rabbit_ct_broker_helpers:rpc(Config, Node1,
        rabbit_transform, check_safety, [Queues])).

%% -----------------------------------------------------------------------------
setup_queues(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    CQ = ?config(queue_name, Config),
    [begin
        ?assertEqual({'queue.declare_ok', Q, 0, 0},
                  declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>}])),
        publish(Ch, Q, 10),
        Q
     end
      || Q <- [<< CQ/binary, N/binary >> || N <- [<< "1" >>, <<"2">>, <<"3">>]]],
    Config.

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

get_classic_queues(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, ?MODULE, get_classic_queues_remote, []).

get_classic_queues_remote() ->
    mnesia:dirty_match_object(rabbit_queue,
        amqqueue:pattern_match_on_type(rabbit_classic_queue)).

delete_queues() ->
   [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
    || Q <- rabbit_amqqueue:list()].

publish(Ch, Queue, N) ->
    [publish_msg(Ch, Queue, <<"msg">>) || _ <- lists:seq(1, N)].

publish_msg(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 1},
                                     payload = Msg}).

ensure_all_queues_alive(Config, Node, Queues) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, ?MODULE, ensure_all_queues_alive_remote, [Queues]).

ensure_all_queues_alive_remote(Queues) ->
    lists:all(fun(Q) -> erlang:is_process_alive(amqqueue:get_pid(Q)) end, Queues).
