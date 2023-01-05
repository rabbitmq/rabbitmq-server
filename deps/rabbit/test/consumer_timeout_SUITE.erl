%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(consumer_timeout_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(CONSUMER_TIMEOUT, 3000).
-define(RECEIVE_TIMEOUT, 5000).

-define(GROUP_CONFIG,
        #{global_consumer_timeout => [{rabbit, [{consumer_timeout, ?CONSUMER_TIMEOUT}]},
                                      {queue_policy, []},
                                      {queue_arguments, []}],
          queue_policy_consumer_timeout => [{rabbit, []},
                                            {queue_policy, [{<<"consumer-timeout">>, ?CONSUMER_TIMEOUT}]},
                                            {queue_arguments, []}],
          queue_argument_consumer_timeout => [{rabbit, []},
                                              {queue_policy, []},
                                              {queue_arguments, [{<<"x-consumer-timeout">>, long, ?CONSUMER_TIMEOUT}]}]}).

-import(queue_utils, [wait_for_messages/2]).

all() ->
    [
     {group, global_consumer_timeout},
     {group, queue_policy_consumer_timeout},
     {group, queue_argument_consumer_timeout}
    ].

groups() ->
    AllTests = [consumer_timeout,
                consumer_timeout_no_basic_cancel_capability,
                consumer_timeout_basic_get],

    AllTestsParallel = [
       {classic_queue, [parallel], AllTests},
       {mirrored_queue, [parallel], AllTests},
       {quorum_queue, [parallel], AllTests}
      ],
    [
     {global_consumer_timeout, [], AllTestsParallel},
     {queue_policy_consumer_timeout, [], AllTestsParallel},
     {queue_argument_consumer_timeout, [], AllTestsParallel}
    ].

suite() ->
    [
      {timetrap, {minutes, 7}}
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
      [{policy_type, <<"classic_queues">>},
       {queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
       {queue_durable, true}]);
init_per_group(quorum_queue, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{policy_type, <<"quorum_queues">>},
       {queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
       {queue_durable, true}]);
init_per_group(mirrored_queue, Config) ->
    case rabbit_ct_broker_helpers:configured_metadata_store(Config) of
        {khepri, _} ->
            {skip, <<"Classic queue mirroring not supported by Khepri">>};
        mnesia ->
            rabbit_ct_broker_helpers:set_ha_policy(Config, 0, <<"^max_length.*queue">>,
                                                   <<"all">>, [{<<"ha-sync-mode">>, <<"automatic">>}]),
            Config1 = rabbit_ct_helpers:set_config(
                        Config, [{policy_type, <<"classic_queues">>},
                                 {is_mirrored, true},
                                 {queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
                                 {queue_durable, true}]),
            rabbit_ct_helpers:run_steps(Config1, [])
    end;
init_per_group(Group, Config0) ->
    case lists:member({group, Group}, all()) of
        true ->
            GroupConfig = maps:get(Group, ?GROUP_CONFIG),
            ClusterSize = 3,
            Config = rabbit_ct_helpers:merge_app_env(
                       Config0, {rabbit, [{channel_tick_interval, 1000},
                                          {quorum_tick_interval, 1000}] ++ ?config(rabbit, GroupConfig)}),
            Config1 = rabbit_ct_helpers:set_config(
                        Config, [ {rmq_nodename_suffix, Group},
                                  {rmq_nodes_count, ClusterSize}
                                ] ++ GroupConfig),
            rabbit_ct_helpers:run_steps(Config1,
                                        rabbit_ct_broker_helpers:setup_steps() ++
                                        rabbit_ct_client_helpers:setup_steps());
        false ->
            rabbit_ct_helpers:run_steps(Config0, [])
    end.

end_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            case ?config(queue_policy, Config) of
                [] -> ok;
                _Policy ->
                    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"consumer_timeout_queue_test_policy">>)
            end,
            rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps());
        false ->
            Config
    end.

init_per_testcase(Testcase, Config) ->
    Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
    Q = rabbit_data_coercion:to_binary(io_lib:format("~p_~tp", [Group, Testcase])),
    Q2 = rabbit_data_coercion:to_binary(io_lib:format("~p_~p_2", [Group, Testcase])),
    Config1 = rabbit_ct_helpers:set_config(Config, [{queue_name, Q},
                                                    {queue_name_2, Q2}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name, Config)}),
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name_2, Config)}),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

consumer_timeout(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false),
    erlang:monitor(process, Conn),
    erlang:monitor(process, Ch),
    receive
        {'DOWN', _, process, Ch, _} -> ok
    after ?RECEIVE_TIMEOUT ->
              flush(1),
              exit(channel_exit_expected)
    end,
    receive
        {'DOWN', _, process, Conn, _} ->
              flush(1),
              exit(unexpected_connection_exit)
    after 2000 ->
              ok
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

consumer_timeout_basic_get(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    [_DelTag] = consume(Ch, QName, [<<"msg1">>]),
    erlang:monitor(process, Conn),
    erlang:monitor(process, Ch),
    receive
        {'DOWN', _, process, Ch, _} -> ok
    after ?RECEIVE_TIMEOUT ->
              flush(1),
              exit(channel_exit_expected)
    end,
    receive
        {'DOWN', _, process, Conn, _} ->
              flush(1),
              exit(unexpected_connection_exit)
    after 2000 ->
              ok
    end,
    ok.


-define(CLIENT_CAPABILITIES,
    [{<<"publisher_confirms">>,           bool, true},
     {<<"exchange_exchange_bindings">>,   bool, true},
     {<<"basic.nack">>,                   bool, true},
     {<<"consumer_cancel_notify">>,       bool, false},
     {<<"connection.blocked">>,           bool, true},
     {<<"authentication_failure_close">>, bool, true}]).

consumer_timeout_no_basic_cancel_capability(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Props = [{<<"capabilities">>, table, ?CLIENT_CAPABILITIES}],
    AmqpParams = #amqp_params_network{port = Port,
                                      host = "localhost",
                                      virtual_host = <<"/">>,
                                      client_properties = Props
                                      },
    {ok, Conn} = amqp_connection:start(AmqpParams),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    erlang:monitor(process, Conn),
    erlang:monitor(process, Ch),
    subscribe(Ch, QName, false),
    receive
        {#'basic.deliver'{delivery_tag = _,
                          redelivered  = false}, _} ->
            %% do nothing with the delivery should trigger timeout
            ok
    after ?RECEIVE_TIMEOUT ->
              exit(deliver_timeout)
    end,
    receive
        {'DOWN', _, process, Ch, _} -> ok
    after ?RECEIVE_TIMEOUT ->
              flush(1),
              exit(channel_exit_expected)
    end,
    receive
        {'DOWN', _, process, Conn, _} ->
              flush(1),
              exit(unexpected_connection_exit)
    after 2000 ->
              ok
    end,
    ok.
%%%%%%%%%%%%%%%%%%%%%%%%
%% Test helpers
%%%%%%%%%%%%%%%%%%%%%%%%

declare_queue(Ch, Config, QName) ->
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    case ?config(queue_policy, Config) of
        [] -> ok;
        Policy ->
            rabbit_ct_broker_helpers:set_policy(Config, 0, <<"consumer_timeout_queue_test_policy">>,
                                                <<".*">>, ?config(policy_type, Config), Policy)
    end,
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                   arguments = Args ++ ?config(queue_arguments, Config),
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

subscribe(Ch, Queue, NoAck) ->
    subscribe(Ch, Queue, NoAck, <<"ctag">>).

subscribe(Ch, Queue, NoAck, Ctag) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                consumer_tag = Ctag
                                               },
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = Ctag} ->
             ok
    end.

flush(T) ->
    receive X ->
                ct:pal("flushed ~w", [X]),
                flush(T)
    after T ->
              ok
    end.
