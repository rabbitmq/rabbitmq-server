%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%
%%
-module(queue_parallel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

-import(quorum_queue_utils, [wait_for_messages/2]).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    AllTests = [publish,
                consume,
                consume_first_empty,
                consume_from_empty_queue,
                consume_and_autoack,
                subscribe,
                subscribe_with_autoack,
                consume_and_ack,
                consume_and_multiple_ack,
                subscribe_and_ack,
                subscribe_and_multiple_ack,
                subscribe_and_requeue_multiple_nack,
                subscribe_and_nack,
                subscribe_and_requeue_nack,
                subscribe_and_multiple_nack,
                consume_and_requeue_nack,
                consume_and_nack,
                consume_and_requeue_multiple_nack,
                consume_and_multiple_nack,
                basic_cancel,
                purge,
                basic_recover,
                delete_immediately_by_resource
               ],
    [
     {parallel_tests, [], 
      [
       {classic_queue, [parallel], AllTests ++ [delete_immediately_by_pid_succeeds]},
       {mirrored_queue, [parallel], AllTests ++ [delete_immediately_by_pid_succeeds]},
       {quorum_queue, [parallel], AllTests ++ [delete_immediately_by_pid_fails]}
      ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 3}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(classic_queue, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
       {queue_durable, true}]);
init_per_group(quorum_queue, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, quorum_queue) of
        ok ->
            rabbit_ct_helpers:set_config(
              Config,
              [{queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
               {queue_durable, true}]);
        Skip ->
            Skip
    end;
init_per_group(mirrored_queue, Config) ->
    rabbit_ct_broker_helpers:set_ha_policy(Config, 0, <<"^max_length.*queue">>,
        <<"all">>, [{<<"ha-sync-mode">>, <<"automatic">>}]),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{is_mirrored, true},
                         {queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
                         {queue_durable, true}]),
    rabbit_ct_helpers:run_steps(Config1, []);
init_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            ClusterSize = 2,
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Group},
                {rmq_nodes_count, ClusterSize}
              ]),
            rabbit_ct_helpers:run_steps(Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps());
        false ->
            rabbit_ct_helpers:run_steps(Config, [])
    end.

end_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps());
        false ->
            Config
    end.

init_per_testcase(Testcase, Config) ->
    Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
    Q = rabbit_data_coercion:to_binary(io_lib:format("~p_~p", [Group, Testcase])),
    Q2 = rabbit_data_coercion:to_binary(io_lib:format("~p_~p_2", [Group, Testcase])),
    Config1 = rabbit_ct_helpers:set_config(Config, [{queue_name, Q},
                                                    {queue_name_2, Q2}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name, Config)}),
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name_2, Config)}),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

publish(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]).

consume(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]).

consume_first_empty(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    consume_empty(Ch, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    consume(Ch, QName, [<<"msg1">>]),
    rabbit_ct_client_helpers:close_channel(Ch).

consume_from_empty_queue(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    consume_empty(Ch, QName).

consume_and_autoack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    consume(Ch, QName, true, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]]),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]]).

subscribe(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                    prefetch_count = 10})),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),

    subscribe(Ch, QName, false),
    receive_basic_deliver(false),
    wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),

    %% validate we can retrieve the consumers
    Consumers = rpc:call(Server, rabbit_amqqueue, consumers_all, [<<"/">>]),
    [Consumer] = lists:filter(fun(Props) ->
                                      Resource = proplists:get_value(queue_name, Props),
                                      QName == Resource#resource.name
                              end, Consumers),
    ?assert(is_pid(proplists:get_value(channel_pid, Consumer))),
    ?assert(is_binary(proplists:get_value(consumer_tag, Consumer))),
    ?assertEqual(true, proplists:get_value(ack_required, Consumer)),
    ?assertEqual(10, proplists:get_value(prefetch_count, Consumer)),
    ?assertEqual([], proplists:get_value(arguments, Consumer)),

    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]).

subscribe_with_autoack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>]),
    wait_for_messages(Config, [[QName, <<"2">>, <<"2">>, <<"0">>]]),
    subscribe(Ch, QName, true),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]]),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]]).

consume_and_ack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    [DeliveryTag] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]]).

consume_and_multiple_ack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"3">>, <<"0">>]]),
    [_, _, DeliveryTag] = consume(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"0">>, <<"3">>]]),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                       multiple     = true}),
    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]]).

subscribe_and_ack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
            wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]])
    end.

subscribe_and_multiple_ack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"3">>, <<"0">>]]),
    subscribe(Ch, QName, false),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            wait_for_messages(Config, [[QName, <<"3">>, <<"0">>, <<"3">>]]),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                               multiple     = true}),
            wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]])
    end.

subscribe_and_requeue_multiple_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"3">>, <<"0">>]]),
    subscribe(Ch, QName, false),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages(Config, [[QName, <<"3">>, <<"0">>, <<"3">>]]),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = true,
                                                requeue      = true}),
            receive_basic_deliver(true),
            receive_basic_deliver(true),
            receive
                {#'basic.deliver'{delivery_tag = DeliveryTag1,
                                  redelivered  = true}, _} ->
                    wait_for_messages(Config, [[QName, <<"3">>, <<"0">>, <<"3">>]]),
                    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1,
                                                       multiple     = true}),
                    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]])
            end
    end.

consume_and_requeue_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>]),
    wait_for_messages(Config, [[QName, <<"2">>, <<"2">>, <<"0">>]]),
    [DeliveryTag] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"2">>, <<"1">>, <<"1">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    wait_for_messages(Config, [[QName, <<"2">>, <<"2">>, <<"0">>]]).

consume_and_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    [DeliveryTag] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]]).

consume_and_requeue_multiple_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"3">>, <<"0">>]]),
    [_, _, DeliveryTag] = consume(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"0">>, <<"3">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = true,
                                        requeue      = true}),
    wait_for_messages(Config, [[QName, <<"3">>, <<"3">>, <<"0">>]]).

consume_and_multiple_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"3">>, <<"0">>]]),
    [_, _, DeliveryTag] = consume(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"0">>, <<"3">>]]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = true,
                                        requeue      = false}),
    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]]).

subscribe_and_requeue_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = true}),
            receive
                {#'basic.deliver'{delivery_tag = DeliveryTag1,
                                  redelivered  = true}, _} ->
                    wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
                    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1}),
                    wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]])
            end
    end.

subscribe_and_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = false}),
            wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]])
    end.

subscribe_and_multiple_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, [[QName, <<"3">>, <<"3">>, <<"0">>]]),
    subscribe(Ch, QName, false),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages(Config, [[QName, <<"3">>, <<"0">>, <<"3">>]]),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = true,
                                                requeue      = false}),
            wait_for_messages(Config, [[QName, <<"0">>, <<"0">>, <<"0">>]])
    end.

%% TODO test with single active
basic_cancel(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
            amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = <<"ctag">>}),
            Consumers = rpc:call(Server, rabbit_amqqueue, consumers_all, [<<"/">>]),
            wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
            ?assertEqual([], lists:filter(fun(Props) ->
                                                  Resource = proplists:get_value(queue_name, Props),
                                                  QName == Resource#resource.name
                                          end, Consumers)),
            publish(Ch, QName, [<<"msg2">>, <<"msg3">>]),
            wait_for_messages(Config, [[QName, <<"3">>, <<"2">>, <<"1">>]]),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
            wait_for_messages(Config, [[QName, <<"2">>, <<"2">>, <<"0">>]])
    after 5000 ->
              exit(basic_deliver_timeout)
    end.

purge(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>]),
    wait_for_messages(Config, [[QName, <<"2">>, <<"2">>, <<"0">>]]),
    [_] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"2">>, <<"1">>, <<"1">>]]),
    {'queue.purge_ok', 1} = amqp_channel:call(Ch, #'queue.purge'{queue = QName}),
    wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]).

basic_recover(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    [_] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"0">>, <<"1">>]]),
    amqp_channel:cast(Ch, #'basic.recover'{requeue = true}),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]).

delete_immediately_by_pid_fails(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    Cmd = ["eval", "{ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(<<\"/\">>, queue, <<\"" ++ binary_to_list(QName) ++ "\">>)), Pid = rabbit_amqqueue:pid_of(Q), rabbit_amqqueue:delete_immediately([Pid])."],
    {ok, Msg} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd),
    ?assertEqual(match, re:run(Msg, ".*error.*", [{capture, none}])),

    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                                        durable   = Durable,
                                                        passive   = true,
                                                        auto_delete = false,
                                                        arguments = Args})).

delete_immediately_by_pid_succeeds(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    Cmd = ["eval", "{ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(<<\"/\">>, queue, <<\"" ++ binary_to_list(QName) ++ "\">>)), Pid = rabbit_amqqueue:pid_of(Q), rabbit_amqqueue:delete_immediately([Pid])."],
    {ok, Msg} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd),
    ?assertEqual(match, re:run(Msg, ".*ok.*", [{capture, none}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                              durable   = Durable,
                                              passive   = true,
                                              auto_delete = false,
                                              arguments = Args})).

delete_immediately_by_resource(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    Cmd = ["eval", "rabbit_amqqueue:delete_immediately_by_resource([rabbit_misc:r(<<\"/\">>, queue, <<\"" ++ binary_to_list(QName) ++ "\">>)])."],
    ?assertEqual({ok, "ok\n"}, rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd)),

    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                              durable   = Durable,
                                              passive   = true,
                                              auto_delete = false,
                                              arguments = Args})).

%%%%%%%%%%%%%%%%%%%%%%%%
%% Test helpers
%%%%%%%%%%%%%%%%%%%%%%%%
declare_queue(Ch, Config, QName) ->
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                   arguments = Args,
                                                                   durable = Durable}).

publish(Ch, QName, Payloads) ->
    [amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload})
     || Payload <- Payloads].

consume(Ch, QName, Payloads) ->
    consume(Ch, QName, false, Payloads).

consume(Ch, QName, NoAck, Payloads) ->
    [begin
         {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = Payload}} =
             amqp_channel:call(Ch, #'basic.get'{queue = QName,
                                                no_ack = NoAck}),
         DTag
     end || Payload <- Payloads].

consume_empty(Ch, QName) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = QName})).

subscribe(Ch, Queue, NoAck) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end.

receive_basic_deliver(Redelivered) ->
    receive
        {#'basic.deliver'{redelivered = R}, _} when R == Redelivered ->
            ok
    end.
