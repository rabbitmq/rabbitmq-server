%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
%%
%% For the full spec see: https://www.rabbitmq.com/confirms.html
%%
-module(publisher_confirms_parallel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT, 60000).

-import(quorum_queue_utils, [wait_for_messages/2]).

all() ->
    [
     {group, publisher_confirm_tests}
    ].

groups() ->
    [
     {publisher_confirm_tests, [],
      [
       {classic_queue,
        [parallel],
        [publisher_confirms,
         publisher_confirms_with_deleted_queue,
         confirm_select_ok,
         confirm_nowait,
         confirm_ack,
         confirm_acks,
         confirm_mandatory_unroutable,
         confirm_unroutable_message,
         confirm_nack]
       },
       {mirrored_queue,
        [parallel],
        [publisher_confirms_with_deleted_queue,
         confirm_select_ok,
         confirm_nowait,
         confirm_ack,
         confirm_acks,
         confirm_mandatory_unroutable,
         confirm_unroutable_message]
       },
       {quorum_queue,
        [parallel],
        [publisher_confirms,
         publisher_confirms_with_deleted_queue,
         confirm_select_ok,
         confirm_nowait,
         confirm_ack,
         confirm_acks,
         confirm_mandatory_unroutable,
         confirm_unroutable_message,
         confirm_minority]
       }
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

%% To enable confirms, a client sends the confirm.select method
publisher_confirms(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, [[QName, <<"1">>, <<"1">>, <<"0">>]]),
    amqp_channel:wait_for_confirms(Ch, 5000),
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
    amqp_channel:wait_for_confirms_or_die(Ch, 5000),
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
    after 5000 ->
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
    after 5000 ->
            throw(missing_return)
    end,
    receive
        #'basic.ack'{delivery_tag = 1} ->
            ok
    after 5000 ->
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
    after 5000 ->
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
                                                 routing_key = "confirms-magic" }),
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
    %% Publish a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"amq.direct">>,
                                           routing_key = "confirms-magic"
                                          },
                      rabbit_basic:build_content(
                        #'P_basic'{delivery_mode = 2}, <<"">>)),
    %% We must not kill the queue before the channel has processed the
    %% 'publish'.
    ok = rabbit_channel:flush(Ch),
    %% Crash the queue
    QPid1 ! boom,
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

%% The closest to a nack behaviour that we can get on quorum queues is not answering while
%% the cluster is in minority. Once the cluster recovers, a 'basic.ack' will be issued.
confirm_minority(Config) ->
    [_A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    ok = rabbit_ct_broker_helpers:stop_node(Config, B),
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),
    publish(Ch, QName, [<<"msg1">>]),
    receive
        #'basic.nack'{} -> throw(unexpected_nack);
        #'basic.ack'{} -> throw(unexpected_ack)
    after 30000 ->
            ok
    end,
    ok = rabbit_ct_broker_helpers:start_node(Config, B),
    receive
        #'basic.nack'{} -> throw(unexpected_nack);
        #'basic.ack'{} -> ok
    after 60000 ->
            throw(missing_ack)
    end.

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

sync_mirrors(QName, Config) ->
    case ?config(is_mirrored, Config) of
        true ->
            rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, [<<"sync_queue">>, QName]);
        _ -> ok
    end.

receive_many([]) ->
    ok;
receive_many(DTags) ->
    receive
        #'basic.ack'{delivery_tag = DTag, multiple = true} ->
            receive_many(DTags -- lists:seq(1, DTag));
        #'basic.ack'{delivery_tag = DTag, multiple = false} ->
            receive_many(DTags -- [DTag])
    after 5000 ->
            throw(missing_ack)
    end.
