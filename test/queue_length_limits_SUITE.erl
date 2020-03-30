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

-module(queue_length_limits_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT_LIST_OPS_PASS, 5000).
-define(TIMEOUT, 30000).
-define(TIMEOUT_CHANNEL_EXCEPTION, 5000).

-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    MaxLengthTests = [max_length_default,
                      max_length_bytes_default,
                      max_length_drop_head,
                      max_length_bytes_drop_head,
                      max_length_reject_confirm,
                      max_length_bytes_reject_confirm,
                      max_length_drop_publish,
                      max_length_drop_publish_requeue,
                      max_length_bytes_drop_publish],
    [
      {parallel_tests, [parallel], [
          {max_length_classic, [], MaxLengthTests},
          {max_length_quorum, [], [max_length_default,
                                   max_length_bytes_default]
          },
          {max_length_mirrored, [], MaxLengthTests}
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

init_per_group(max_length_classic, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
       {queue_durable, false}]);
init_per_group(max_length_quorum, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, quorum_queue) of
        ok ->
            rabbit_ct_helpers:set_config(
              Config,
              [{queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
               {queue_durable, true}]);
        Skip ->
            Skip
    end;
init_per_group(max_length_mirrored, Config) ->
    rabbit_ct_broker_helpers:set_ha_policy(Config, 0, <<"^max_length.*queue">>,
        <<"all">>, [{<<"ha-sync-mode">>, <<"automatic">>}]),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{is_mirrored, true},
                         {queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
                         {queue_durable, false}]),
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

end_per_group(max_length_mirrored, Config) ->
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"^max_length.*queue">>),
    Config1 = rabbit_ct_helpers:set_config(Config, [{is_mirrored, false}]),
    Config1;
end_per_group(queue_max_length, Config) ->
    Config;
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
    Config1 = rabbit_ct_helpers:set_config(Config, [{queue_name, Q}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config)
  when Testcase == max_length_drop_publish; Testcase == max_length_bytes_drop_publish;
       Testcase == max_length_drop_publish_requeue;
       Testcase == max_length_reject_confirm; Testcase == max_length_bytes_reject_confirm;
       Testcase == max_length_drop_head; Testcase == max_length_bytes_drop_head;
       Testcase == max_length_default; Testcase == max_length_bytes_default ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name, Config)}),
    rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

max_length_bytes_drop_head(Config) ->
    max_length_bytes_drop_head(Config, [{<<"x-overflow">>, longstr, <<"drop-head">>}]).

max_length_bytes_default(Config) ->
    max_length_bytes_drop_head(Config, []).

max_length_bytes_drop_head(Config, ExtraArgs) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),

    MaxLengthBytesArgs = [{<<"x-max-length-bytes">>, long, 100}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthBytesArgs ++ Args ++ ExtraArgs, durable = Durable}),

    %% 80 bytes payload
    Payload1 = << <<"1">> || _ <- lists:seq(1, 80) >>,
    Payload2 = << <<"2">> || _ <- lists:seq(1, 80) >>,
    Payload3 = << <<"3">> || _ <- lists:seq(1, 80) >>,
    check_max_length_drops_head(Config, QName, Ch, Payload1, Payload2, Payload3).

max_length_drop_head(Config) ->
    max_length_drop_head(Config, [{<<"x-overflow">>, longstr, <<"drop-head">>}]).

max_length_default(Config) ->
    %% Defaults to drop_head
    max_length_drop_head(Config, []).

max_length_drop_head(Config, ExtraArgs) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),

    MaxLengthArgs = [{<<"x-max-length">>, long, 1}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthArgs ++ Args ++ ExtraArgs, durable = Durable}),

    check_max_length_drops_head(Config, QName, Ch, <<"1">>, <<"2">>, <<"3">>).

max_length_reject_confirm(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    QName = ?config(queue_name, Config),
    Durable = ?config(queue_durable, Config),
    MaxLengthArgs = [{<<"x-max-length">>, long, 1}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthArgs ++ OverflowArgs ++ Args, durable = Durable}),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    check_max_length_drops_publish(Config, QName, Ch, <<"1">>, <<"2">>, <<"3">>),
    check_max_length_rejects(Config, QName, Ch, <<"1">>, <<"2">>, <<"3">>).

max_length_bytes_reject_confirm(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    QNameBytes = ?config(queue_name, Config),
    Durable = ?config(queue_durable, Config),
    MaxLengthBytesArgs = [{<<"x-max-length-bytes">>, long, 100}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QNameBytes, arguments = MaxLengthBytesArgs ++ OverflowArgs ++ Args, durable = Durable}),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

    %% 80 bytes payload
    Payload1 = << <<"1">> || _ <- lists:seq(1, 80) >>,
    Payload2 = << <<"2">> || _ <- lists:seq(1, 80) >>,
    Payload3 = << <<"3">> || _ <- lists:seq(1, 80) >>,

    check_max_length_drops_publish(Config, QNameBytes, Ch, Payload1, Payload2, Payload3),
    check_max_length_rejects(Config, QNameBytes, Ch, Payload1, Payload2, Payload3).

max_length_drop_publish(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    MaxLengthArgs = [{<<"x-max-length">>, long, 1}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthArgs ++ OverflowArgs ++ Args, durable = Durable}),
    %% If confirms are not enable, publishes will still be dropped in reject-publish mode.
    check_max_length_drops_publish(Config, QName, Ch, <<"1">>, <<"2">>, <<"3">>).

max_length_drop_publish_requeue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    MaxLengthArgs = [{<<"x-max-length">>, long, 1}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthArgs ++ OverflowArgs ++ Args, durable = Durable}),
    %% If confirms are not enable, publishes will still be dropped in reject-publish mode.
    check_max_length_requeue(Config, QName, Ch, <<"1">>, <<"2">>).

max_length_bytes_drop_publish(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QNameBytes = ?config(queue_name, Config),
    MaxLengthBytesArgs = [{<<"x-max-length-bytes">>, long, 100}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QNameBytes, arguments = MaxLengthBytesArgs ++ OverflowArgs ++ Args, durable = Durable}),

    %% 80 bytes payload
    Payload1 = << <<"1">> || _ <- lists:seq(1, 80) >>,
    Payload2 = << <<"2">> || _ <- lists:seq(1, 80) >>,
    Payload3 = << <<"3">> || _ <- lists:seq(1, 80) >>,

    check_max_length_drops_publish(Config, QNameBytes, Ch, Payload1, Payload2, Payload3).

%% -------------------------------------------------------------------
%% Implementation
%% -------------------------------------------------------------------

check_max_length_requeue(Config, QName, Ch, Payload1, Payload2) ->
    sync_mirrors(QName, Config),

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    %% A single message is published and consumed
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:wait_for_confirms(Ch, 5000),

    {#'basic.get_ok'{delivery_tag = DeliveryTag},
     #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Another message is published
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    amqp_channel:wait_for_confirms(Ch, 5000),

    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload2}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

check_max_length_drops_publish(Config, QName, Ch, Payload1, Payload2, Payload3) ->
    sync_mirrors(QName, Config),

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    %% A single message is published and consumed
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:wait_for_confirms(Ch, 5000),

    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Message 2 is dropped, message 1 stays
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    amqp_channel:wait_for_confirms(Ch, 5000),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Messages 2 and 3 are dropped, message 1 stays
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload3}),
    amqp_channel:wait_for_confirms(Ch, 5000),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

check_max_length_rejects(Config, QName, Ch, Payload1, Payload2, Payload3) ->
    sync_mirrors(QName, Config),
    amqp_channel:register_confirm_handler(Ch, self()),
    flush(),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    %% First message can be enqueued and acks
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    receive #'basic.ack'{} -> ok
    after 1000 -> error(expected_ack)
    end,

    %% The message cannot be enqueued and nacks
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    receive #'basic.nack'{} -> ok
    after 1000 -> error(expected_nack)
    end,

    %% The message cannot be enqueued and nacks
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload3}),
    receive #'basic.nack'{} -> ok
    after 1000 -> error(expected_nack)
    end,

    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Now we can publish message 2.
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    receive #'basic.ack'{} -> ok
    after 1000 -> error(expected_ack)
    end,

    {#'basic.get_ok'{}, #amqp_msg{payload = Payload2}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

check_max_length_drops_head(Config, QName, Ch, Payload1, Payload2, Payload3) ->
    sync_mirrors(QName, Config),

    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    %% A single message is published and consumed
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:wait_for_confirms(Ch, 5000),

    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Message 1 is replaced by message 2
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    amqp_channel:wait_for_confirms(Ch, 5000),

    {#'basic.get_ok'{}, #amqp_msg{payload = Payload2}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Messages 1 and 2 are replaced
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload3}),
    amqp_channel:wait_for_confirms(Ch, 5000),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload3}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

sync_mirrors(QName, Config) ->
    case rabbit_ct_helpers:get_config(Config, is_mirrored) of
        true ->
            rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, [<<"sync_queue">>, QName]);
        _ -> ok
    end.

flush() ->
    receive _ -> flush()
    after 10 -> ok
    end.
