%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(publisher_confirms_parallel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(TIMEOUT, 60000).

-import(queue_utils, [wait_for_messages/2]).

all() ->
    [
     {group, tests}
    ].

groups() ->
    PublisherConfirmTests = [publisher_confirms,
                             publisher_confirms_with_deleted_queue,
                             confirm_select_ok,
                             confirm_nowait,
                             confirm_ack,
                             confirm_acks,
                             confirm_after_mandatory_bug,
                             confirm_mandatory_unroutable,
                             confirm_unroutable_message],
    [
     {tests, [],
      [
       {classic_queue, [parallel], PublisherConfirmTests ++ [confirm_nack]},
       {quorum_queue, [parallel], PublisherConfirmTests}
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
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
       {queue_durable, true}]);
init_per_group(Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia}]),
    init_per_group0(Group, Config).

init_per_group0(Group, Config) ->
    ClusterSize = 3,
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodename_suffix, Group},
                                                    {rmq_nodes_count, ClusterSize}
                                                   ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
                                          rabbit_ct_broker_helpers:setup_steps() ++
                                          rabbit_ct_client_helpers:setup_steps()),
    Config2.

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

%% To enable confirms, a client sends the confirm.select method
publisher_confirms(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    amqp_channel:wait_for_confirms(Ch, 5),
    amqp_channel:unregister_confirm_handler(Ch),
    ok.

publisher_confirms_with_deleted_queue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, QName, [<<"msg1">>]),
    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    amqp_channel:wait_for_confirms_or_die(Ch, 5),
    amqp_channel:unregister_confirm_handler(Ch).

%% Depending on whether no-wait was set or not, the broker may respond with a confirm.select-ok
confirm_select_ok(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    ?assertEqual(#'confirm.select_ok'{}, amqp_channel:call(Ch, #'confirm.select'{nowait = false})).

confirm_nowait(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    ?assertEqual(ok, amqp_channel:call(Ch, #'confirm.select'{nowait = true})).

%% The broker then confirms messages as it handles them by sending a basic.ack on the same channel.
%% The delivery-tag field contains the sequence number of the confirmed message.
confirm_ack(Config) ->
    %% Ensure we receive an ack and not a nack
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, QName, [<<"msg1">>]),
    receive
        #'basic.ack'{delivery_tag = 1} ->
            ok
    after ?TIMEOUT ->
            throw(missing_ack)
    end.

%% The broker may also set the multiple field in basic.ack to indicate that all messages up to
%% and including the one with the sequence number have been handled.
confirm_acks(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>, <<"msg4">>]),
    receive_many(lists:seq(1, 4)).

confirm_after_mandatory_bug(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    ok = amqp_channel:call(Ch, #'basic.publish'{routing_key = QName,
                                                mandatory = true}, #amqp_msg{payload = <<"msg1">>}),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    publish(Ch, QName, [<<"msg2">>]),
    true = amqp_channel:wait_for_confirms(Ch, 1),
    ok.

%% For unroutable messages, the broker will issue a confirm once the exchange verifies a message
%% won't route to any queue (returns an empty list of queues).
%% If the message is also published as mandatory, the basic.return is sent to the client before
%% basic.ack.
confirm_mandatory_unroutable(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    amqp_channel:register_return_handler(Ch, self()),
    ok = amqp_channel:call(Ch, #'basic.publish'{routing_key = QName,
                                                mandatory = true}, #amqp_msg{payload = <<"msg1">>}),
    receive
        {#'basic.return'{}, _} ->
            ok
    after ?TIMEOUT ->
            throw(missing_return)
    end,
    receive
        #'basic.ack'{delivery_tag = 1} ->
            ok
    after ?TIMEOUT ->
            throw(missing_ack)
    end.

confirm_unroutable_message(Config) ->
    %% Ensure we receive a nack for an unroutable message
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, QName, [<<"msg1">>]),
    receive
        {#'basic.return'{}, _} ->
            throw(unexpected_basic_return);
        #'basic.ack'{delivery_tag = 1} ->
            ok
    after ?TIMEOUT ->
            throw(missing_ack)
    end.

%% In exceptional cases when the broker is unable to handle messages successfully,
%% instead of a basic.ack, the broker will send a basic.nack.
%% basic.nack will only be delivered if an internal error occurs in the Erlang process
%% responsible for a queue.
%% This test crashes the queue before it has time to answer, but it only works for classic
%% queues. On quorum queues the followers will take over and rabbit_fifo_client will resend
%% any pending messages.
confirm_nack(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, confirm_nack1, [Config]).

confirm_nack1(Config) ->
    {_Writer, _Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    ok = rabbit_channel:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_open_ok)
    end,
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName1 = ?config(queue_name, Config),
    QName2 = ?config(queue_name_2, Config),
    DeclareBindDurableQueue =
        fun(QName) ->
                rabbit_channel:do(Ch, #'queue.declare'{durable = Durable,
                                                       queue = QName,
                                                       arguments = Args}),
                receive #'queue.declare_ok'{} ->
                        rabbit_channel:do(Ch, #'queue.bind'{
                                                 queue = QName,
                                                 exchange = <<"amq.direct">>,
                                                 routing_key = <<"confirms-magic">>}),
                        receive #'queue.bind_ok'{} -> ok
                        after ?TIMEOUT -> throw(failed_to_bind_queue)
                        end
                after ?TIMEOUT -> throw(failed_to_declare_queue)
                end
        end,
    %% Declare and bind two queues
    DeclareBindDurableQueue(QName1),
    DeclareBindDurableQueue(QName2),
    %% Get the first one's pid (we'll crash it later)
    {ok, Q1} = rabbit_amqqueue:lookup(rabbit_misc:r(<<"/">>, queue, QName1)),
    QPid1 = amqqueue:get_pid(Q1),
    %% Enable confirms
    rabbit_channel:do(Ch, #'confirm.select'{}),
    receive
        #'confirm.select_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_enable_confirms)
    end,
    %% stop the queue
    ok = gen_server:stop(QPid1, shutdown, 5000),
    %% Publish a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"amq.direct">>,
                                           routing_key = <<"confirms-magic">>
                                          },
                      rabbit_basic:build_content(
                        #'P_basic'{delivery_mode = 2}, <<"">>)),
    %% Wait for a nack
    receive
        #'basic.nack'{} -> ok;
        #'basic.ack'{}  -> throw(received_ack_instead_of_nack)
    after ?TIMEOUT-> throw(did_not_receive_nack)
    end,
    receive
        #'basic.ack'{} -> throw(received_ack_when_none_expected)
    after 1000 -> ok
    end,
    %% Cleanup
    unlink(Ch),
    ok = rabbit_channel:shutdown(Ch),
    passed.


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

publish(Ch, QName, Payloads, Headers) ->
    [amqp_channel:call(Ch, #'basic.publish'{routing_key = QName},
                       #amqp_msg{payload = Payload,
                                 props = #'P_basic'{headers = Headers}})
     || Payload <- Payloads].

consume(Ch, QName, Payloads) ->
    [begin
         {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = Payload}} =
             amqp_channel:call(Ch, #'basic.get'{queue = QName}),
         DTag
     end || Payload <- Payloads].

consume_empty(Ch, QName) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

receive_many([]) ->
    ok;
receive_many(DTags) ->
    receive
        #'basic.ack'{delivery_tag = DTag, multiple = true} ->
            receive_many(DTags -- lists:seq(1, DTag));
        #'basic.ack'{delivery_tag = DTag, multiple = false} ->
            receive_many(DTags -- [DTag])
    after ?TIMEOUT ->
            throw(missing_ack)
    end.
