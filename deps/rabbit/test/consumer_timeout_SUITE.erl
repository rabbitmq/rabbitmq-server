%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(consumer_timeout_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(CONSUMER_TIMEOUT, 2000).
%% Sometimes CI machines are really slow,
%% expecting CONSUMER_TIMEOUT*2 might not be enough
-define(RECEIVE_TIMEOUT, 30_000).

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
    %% Consumer timeouts are only supported for quorum queues.
    %% Classic queues and stream queues do not support consumer timeouts.
    AllTests = [consumer_timeout_with_basic_cancel_capability,
                consumer_timeout_no_basic_cancel_capability,
                consumer_timeout_basic_get,
                consumer_cancel_ok_after_timeout_removes_consumer,
                consumer_removed_when_cancel_ok_never_arrives,
                erlang_client_answers_cancel_only_for_known_consumers,
                consumer_cancel_ok_after_queue_delete,
                unsolicited_cancel_ok_keeps_active_consumer,
                consumer_timeout_late_ack_after_cancel_ok,
                consumer_timeout_erlang_client_answers_with_cancel_ok,
                server_advertises_accept_consumer_cancel_ok,
                consumer_timeout_response_close_channel],

    AllTestsParallel = [
       {quorum_queue, [], AllTests}
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

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:run_setup_steps(Config0),
    ClusterSize = 3,
    Config2 = rabbit_ct_helpers:set_config(
                Config1, [{rmq_nodename_suffix, consumer_timeout},
                          {rmq_nodes_count, ClusterSize}]),
    Config3 = rabbit_ct_helpers:merge_app_env(
                Config2, {rabbit, [{channel_tick_interval, 256},
                                   {quorum_tick_interval, 256}]}),
    Config4 = rabbit_ct_helpers:run_steps(Config3,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                rabbit_ct_client_helpers:setup_steps()),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config4, 'rabbitmq_4.3.0') of
        ok ->
            Config4;
        {skip, _} = Skip ->
            end_per_suite(Config4),
            Skip
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(quorum_queue, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{policy_type, <<"quorum_queues">>},
       {queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
       {queue_durable, true}]);
init_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            GroupConfig = maps:get(Group, ?GROUP_CONFIG),
            %% Set the global consumer_timeout if specified
            case ?config(rabbit, GroupConfig) of
                [{consumer_timeout, Timeout}] ->
                    ok = rabbit_ct_broker_helpers:rpc(
                           Config, 0, application, set_env,
                           [rabbit, consumer_timeout, Timeout]);
                [] ->
                    ok = rabbit_ct_broker_helpers:rpc(
                           Config, 0, application, unset_env,
                           [rabbit, consumer_timeout])
            end,
            rabbit_ct_helpers:set_config(Config, GroupConfig);
        false ->
            Config
    end.

end_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            case ?config(queue_policy, Config) of
                [] -> ok;
                _Policy ->
                    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"consumer_timeout_queue_test_policy">>)
            end,
            Config;
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

%% Test consumer timeout with consumer_cancel_notify capability enabled (default).
%% When the consumer times out, the server should send a basic.cancel to the consumer
%% instead of closing the channel.
consumer_timeout_with_basic_cancel_capability(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    amqp_channel:call(Ch, #'confirm.select'{}),
    publish(Ch, QName, [<<"msg1">>]),
    %% Settle the publish before polling `list_queues/0`: on a freshly-declared
    %% quorum queue the local node's view of the message count can lag
    %% the actual write by enough for `wait_for_messages/2` to time out at 0/0/0.
    amqp_channel:wait_for_confirms_or_die(Ch, 30),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    Ctag = <<"ctag">>,
    subscribe(Ch, QName, false, Ctag),
    erlang:monitor(process, Conn),
    erlang:monitor(process, Ch),
    %% Receive the delivery but don't acknowledge it
    receive
        {#'basic.deliver'{delivery_tag = _,
                          consumer_tag = Ctag,
                          redelivered  = false}, _} ->
            %% do nothing with the delivery, should trigger timeout
            ok
    after ?RECEIVE_TIMEOUT ->
              flush(1),
              exit(deliver_timeout)
    end,
    %% Should receive basic.cancel from server due to consumer timeout
    receive
        #'basic.cancel'{consumer_tag = Ctag, nowait = true} ->
            ok
    after ?RECEIVE_TIMEOUT ->
              flush(1),
              exit(basic_cancel_expected)
    end,
    %% Channel and connection should remain open
    receive
        {'DOWN', _, process, Ch, Reason} ->
              flush(1),
              exit({unexpected_channel_exit, Reason})
    after 1000 ->
              ok
    end,
    receive
        {'DOWN', _, process, Conn, Reason2} ->
              flush(1),
              exit({unexpected_connection_exit, Reason2})
    after 1000 ->
              ok
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

%% Test consumer timeout with basic.get (manual acknowledgement mode).
%% When a message is fetched via basic.get and not acknowledged within the timeout,
%% the channel should be closed (since basic.get doesn't have a consumer tag to cancel).
consumer_timeout_basic_get(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    amqp_channel:call(Ch, #'confirm.select'{}),
    publish(Ch, QName, [<<"msg1">>]),
    amqp_channel:wait_for_confirms_or_die(Ch, 30),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    %% Fetch message via basic.get without acknowledging
    [_DelTag] = consume(Ch, QName, [<<"msg1">>]),
    erlang:monitor(process, Conn),
    erlang:monitor(process, Ch),
    %% Channel should close due to timeout
    receive
        {'DOWN', _, process, Ch, _} -> ok
    after ?RECEIVE_TIMEOUT ->
              flush(1),
              exit(channel_exit_expected)
    end,
    %% Connection should remain open
    receive
        {'DOWN', _, process, Conn, _} ->
              flush(1),
              exit(unexpected_connection_exit)
    after 1000 ->
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

%% Test consumer timeout without consumer_cancel_notify capability.
%% When the consumer times out and the client doesn't support consumer_cancel_notify,
%% the server should close the channel instead of sending basic.cancel.
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
    amqp_channel:call(Ch, #'confirm.select'{}),
    publish(Ch, QName, [<<"msg1">>]),
    amqp_channel:wait_for_confirms_or_die(Ch, 30),
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
    %% Connection should remain open
    receive
        {'DOWN', _, process, Conn, _} ->
              flush(1),
              exit(unexpected_connection_exit)
    after 1000 ->
              ok
    end.

%% A client that answers the server's `basic.cancel` with `basic.cancel_ok`
%% has its consumer removed from the queue, the channel and the metrics.
consumer_cancel_ok_after_timeout_removes_consumer(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ok = amqp_channel:set_server_properties(Ch, []),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish_and_confirm(Ch, QName, [<<"m1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false, <<"ctag">>),
    _ = receive_delivery(<<"ctag">>, false),
    await_basic_cancel(<<"ctag">>),
    ?assertEqual(1, channel_consumer_count(Config)),
    ok = amqp_channel:cast(Ch, #'basic.cancel_ok'{consumer_tag = <<"ctag">>}),
    ?awaitMatch(#{consumers := 0,
                  quorum_queue_consumers := 0,
                  consumer_metrics := []},
                consumer_state(Config, QName), 5000),
    ?assertEqual(0, channel_consumer_count(Config)),
    ?assert(is_process_alive(Ch)),
    ?assert(is_process_alive(Conn)),
    amqp_connection:close(Conn).

consumer_removed_when_cancel_ok_never_arrives(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ok = amqp_channel:set_server_properties(Ch, []),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish_and_confirm(Ch, QName, [<<"m1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false, <<"ctag">>),
    _ = receive_delivery(<<"ctag">>, false),
    await_basic_cancel(<<"ctag">>),
    timer:sleep(3000),
    ?assertMatch(#{quorum_queue_consumers := 1}, consumer_state(Config, QName)),
    ?assertEqual(1, channel_consumer_count(Config)),
    ?awaitMatch(#{consumers := 0,
                  quorum_queue_consumers := 0,
                  consumer_metrics := []},
                consumer_state(Config, QName), ?RECEIVE_TIMEOUT),
    ?assertEqual(0, channel_consumer_count(Config)),
    ?assert(is_process_alive(Ch)),
    amqp_connection:close(Conn).

erlang_client_answers_cancel_only_for_known_consumers(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    ok = amqp_selective_consumer:register_default_consumer(Ch, self()),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    subscribe(Ch, QName, false, <<"ctag">>),
    _ = erlang:trace(Ch, true, ['receive']),
    try
        inject_server_cancel(Ch, <<"unknown">>),
        await_basic_cancel(<<"unknown">>),
        ?assertNot(client_sent_cancel_ok(Ch, <<"unknown">>, 1000)),
        inject_server_cancel(Ch, <<"ctag">>),
        await_basic_cancel(<<"ctag">>),
        ?assert(client_sent_cancel_ok(Ch, <<"ctag">>, ?RECEIVE_TIMEOUT))
    after
        _ = erlang:trace(Ch, false, ['receive'])
    end,
    ?assert(is_process_alive(Ch)),
    amqp_connection:close(Conn).

%% The Erlang client answers the server's `basic.cancel` on its own.
consumer_timeout_erlang_client_answers_with_cancel_ok(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish_and_confirm(Ch, QName, [<<"m1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false, <<"ctag">>),
    _ = receive_delivery(<<"ctag">>, false),
    await_basic_cancel(<<"ctag">>),
    ?awaitMatch(#{consumers := 0,
                  quorum_queue_consumers := 0,
                  consumer_metrics := []},
                consumer_state(Config, QName), ?RECEIVE_TIMEOUT),
    ?assert(is_process_alive(Ch)),
    amqp_connection:close(Conn).

unsolicited_cancel_ok_keeps_active_consumer(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    subscribe(Ch, QName, false, <<"ctag">>),
    ok = amqp_channel:cast(Ch, #'basic.cancel_ok'{consumer_tag = <<"ctag">>}),
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 10}),
    ?assertEqual(1, channel_consumer_count(Config)),
    ?assertMatch(#{quorum_queue_consumers := 1}, consumer_state(Config, QName)),
    ?assert(is_process_alive(Ch)),
    amqp_connection:close(Conn).

%% A `basic.cancel_ok` for a consumer the channel has already removed is ignored.
consumer_cancel_ok_after_queue_delete(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, Ch2} = amqp_connection:open_channel(Conn),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    subscribe(Ch, QName, false, <<"ctag">>),
    #'queue.delete_ok'{} = amqp_channel:call(Ch2, #'queue.delete'{queue = QName}),
    await_basic_cancel(<<"ctag">>),
    ok = amqp_channel:cast(Ch, #'basic.cancel_ok'{consumer_tag = <<"ctag">>}),
    timer:sleep(1000),
    ?assert(is_process_alive(Ch)),
    ?assert(is_process_alive(Conn)),
    amqp_connection:close(Conn).

%% The returned message is not delivered again to the cancelled consumer
%% after a late `basic.ack` for the timed-out delivery.
consumer_timeout_late_ack_after_cancel_ok(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish_and_confirm(Ch, QName, [<<"m1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    subscribe(Ch, QName, false, <<"ctag">>),
    DTag = receive_delivery(<<"ctag">>, false),
    await_basic_cancel(<<"ctag">>),
    ok = amqp_channel:cast(Ch, #'basic.cancel_ok'{consumer_tag = <<"ctag">>}),
    ?awaitMatch(#{quorum_queue_consumers := 0}, consumer_state(Config, QName),
                ?RECEIVE_TIMEOUT),
    ok = amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag}),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    receive
        {#'basic.deliver'{}, _} ->
            exit(delivery_to_cancelled_consumer)
    after 1000 ->
              ok
    end,
    ?assert(is_process_alive(Ch)),
    amqp_connection:close(Conn).

%% With `consumer_timeout_response` set to `close_channel`, the channel is closed
%% even for a client that supports `consumer_cancel_notify`.
consumer_timeout_response_close_channel(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                      [rabbit, consumer_timeout_response, close_channel]),
    try
        Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
        {ok, Ch} = amqp_connection:open_channel(Conn),
        QName = ?config(queue_name, Config),
        declare_queue(Ch, Config, QName),
        publish_and_confirm(Ch, QName, [<<"m1">>]),
        wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
        erlang:monitor(process, Ch),
        subscribe(Ch, QName, false, <<"ctag">>),
        _ = receive_delivery(<<"ctag">>, false),
        receive
            {'DOWN', _, process, Ch, {shutdown, {server_initiated_close, 406, _}}} ->
                ok;
            #'basic.cancel'{} ->
                exit(unexpected_basic_cancel)
        after ?RECEIVE_TIMEOUT ->
                  exit(channel_close_expected)
        end,
        ?awaitMatch(#{consumers := 0,
                      quorum_queue_consumers := 0,
                      consumer_metrics := []},
                    consumer_state(Config, QName), ?RECEIVE_TIMEOUT),
        ?assert(is_process_alive(Conn)),
        amqp_connection:close(Conn)
    after
        ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env,
                                          [rabbit, consumer_timeout_response])
    end.

server_advertises_accept_consumer_cancel_ok(Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {server_properties, Props} = lists:keyfind(server_properties, 1,
                                               amqp_connection:info(Conn, [server_properties])),
    {<<"capabilities">>, table, Capabilities} = lists:keyfind(<<"capabilities">>, 1, Props),
    ?assertEqual({<<"accept_consumer_cancel_ok">>, bool, true},
                 lists:keyfind(<<"accept_consumer_cancel_ok">>, 1, Capabilities)),
    amqp_connection:close(Conn).

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
publish_and_confirm(Ch, QName, Payloads) ->
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    publish(Ch, QName, Payloads),
    amqp_channel:wait_for_confirms_or_die(Ch, 30).

receive_delivery(CTag, Redelivered) ->
    receive
        {#'basic.deliver'{delivery_tag = DTag,
                          consumer_tag = CTag,
                          redelivered = Redelivered}, _} ->
            DTag
    after ?RECEIVE_TIMEOUT ->
              flush(1),
              exit({deliver_timeout, CTag, Redelivered})
    end.

await_basic_cancel(CTag) ->
    receive
        #'basic.cancel'{consumer_tag = CTag} ->
            ok
    after ?RECEIVE_TIMEOUT ->
              flush(1),
              exit(basic_cancel_expected)
    end.

inject_server_cancel(Ch, CTag) ->
    gen_server:cast(Ch, {method, #'basic.cancel'{consumer_tag = CTag, nowait = true},
                         none, noflow}).

client_sent_cancel_ok(Ch, CTag, Timeout) ->
    receive
        {trace, Ch, 'receive',
         {'$gen_cast', {cast, #'basic.cancel_ok'{consumer_tag = CTag}, _, _, _}}} ->
            true
    after Timeout ->
              false
    end.

channel_consumer_count(Config) ->
    lists:sum([proplists:get_value(consumer_count, Info)
               || Info <- rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_channel,
                                                       info_all, [[consumer_count]])]).

consumer_state(Config, QName) ->
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QRes]),
    [{consumers, Consumers}] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue,
                                                            info, [Q, [consumers]]),
    {ok, #{machine := #{num_consumers := NumQQConsumers}}, _} =
        ra:member_overview(amqqueue:get_pid(Q)),
    Metrics = [E || E <- rabbit_ct_broker_helpers:rpc(Config, 0, ets, tab2list,
                                                      [consumer_created]),
                    element(1, element(1, E)) =:= QRes],
    #{consumers => Consumers,
      quorum_queue_consumers => NumQQConsumers,
      consumer_metrics => Metrics}.

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
