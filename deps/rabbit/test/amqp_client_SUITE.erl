%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqp_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_ct_broker_helpers,
        [get_node_config/3,
         rpc/4,
         rpc/5,
         drain_node/2,
         revive_node/2
        ]).
-import(rabbit_ct_helpers,
        [eventually/1, eventually/3]).
-import(event_recorder,
        [assert_event_type/2,
         assert_event_prop/2]).

all() ->
    [
      {group, cluster_size_1},
      {group, cluster_size_3},
      {group, metrics}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       reliable_send_receive_with_outcomes_classic_queue,
       reliable_send_receive_with_outcomes_quorum_queue,
       sender_settle_mode_unsettled,
       sender_settle_mode_unsettled_fanout,
       sender_settle_mode_mixed,
       quorum_queue_rejects,
       receiver_settle_mode_first,
       publishing_to_non_existing_queue_should_settle_with_released,
       open_link_to_non_existing_destination_should_end_session,
       roundtrip_with_drain_classic_queue,
       roundtrip_with_drain_quorum_queue,
       roundtrip_with_drain_stream,
       amqp_stream_amqpl,
       message_headers_conversion,
       multiple_sessions,
       server_closes_link_classic_queue,
       server_closes_link_quorum_queue,
       server_closes_link_stream,
       server_closes_link_exchange,
       link_target_classic_queue_deleted,
       link_target_quorum_queue_deleted,
       target_queues_deleted_accepted,
       events,
       sync_get_unsettled_classic_queue,
       sync_get_unsettled_quorum_queue,
       sync_get_unsettled_stream,
       sync_get_unsettled_2_classic_queue,
       sync_get_unsettled_2_quorum_queue,
       sync_get_unsettled_2_stream,
       sync_get_settled_classic_queue,
       sync_get_settled_quorum_queue,
       sync_get_settled_stream,
       timed_get_classic_queue,
       timed_get_quorum_queue,
       timed_get_stream,
       stop_classic_queue,
       stop_quorum_queue,
       stop_stream,
       single_active_consumer_classic_queue,
       single_active_consumer_quorum_queue,
       detach_requeues,
       resource_alarm_before_session_begin,
       resource_alarm_after_session_begin,
       max_message_size_client_to_server,
       max_message_size_server_to_client,
       receive_transfer_flow_order,
       global_counters,
       stream_filtering,
       available_messages_classic_queue,
       available_messages_quorum_queue,
       available_messages_stream,
       incoming_message_interceptors,
       trace,
       user_id,
       message_ttl,
       plugin,
       idle_time_out_on_server,
       idle_time_out_on_client,
       idle_time_out_too_short,
       rabbit_status_connection_count,
       handshake_timeout,
       credential_expires,
       attach_to_exclusive_queue,
       classic_priority_queue
      ]},

     {cluster_size_3, [shuffle],
      [
       last_queue_confirms,
       target_queue_deleted,
       target_classic_queue_down,
       async_notify_settled_classic_queue,
       async_notify_settled_quorum_queue,
       async_notify_settled_stream,
       async_notify_unsettled_classic_queue,
       async_notify_unsettled_quorum_queue,
       async_notify_unsettled_stream,
       link_flow_control,
       classic_queue_on_old_node,
       classic_queue_on_new_node,
       quorum_queue_on_old_node,
       quorum_queue_on_new_node,
       maintenance,
       list_connections
      ]},

     {metrics, [shuffle],
      [
       auth_attempt_metrics
      ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Nodes = case Group of
                cluster_size_3 -> 3;
                _ -> 1
            end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, Nodes},
                         {rmq_nodename_suffix, Suffix}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(T, Config)
  when T =:= message_headers_conversion orelse
       T =:= roundtrip_with_drain_quorum_queue orelse
       T =:= timed_get_quorum_queue orelse
       T =:= available_messages_quorum_queue ->
    case rabbit_ct_broker_helpers:rpc(
           Config, rabbit_feature_flags, is_enabled, [credit_api_v2]) of
        true ->
            rabbit_ct_helpers:testcase_started(Config, T);
        false ->
            {skip, "Receiving with drain from quorum queues in credit API v1 have a known "
             "bug that they reply with send_drained before delivering the message."}
    end;
init_per_testcase(T, Config)
  when T =:= classic_queue_on_new_node orelse
       T =:= quorum_queue_on_new_node ->
    %% If node 1 runs 4.x, this is the new no-op plugin.
    %% If node 1 runs 3.x, this is the old real plugin.
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 1, rabbitmq_amqp1_0),
    rabbit_ct_helpers:testcase_started(Config, T);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Assert that every testcase cleaned up.
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, []))),
    %% Assert that global counters count correctly.
    eventually(?_assertMatch(#{publishers := 0,
                               consumers := 0},
                             get_global_counters(Config))),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

reliable_send_receive_with_outcomes_classic_queue(Config) ->
    reliable_send_receive_with_outcomes(<<"classic">>, Config).

reliable_send_receive_with_outcomes_quorum_queue(Config) ->
    reliable_send_receive_with_outcomes(<<"quorum">>, Config).

reliable_send_receive_with_outcomes(QType, Config) ->
    Outcomes = [
                accepted,
                modified,
                {modified, true, false, #{<<"fruit">> => <<"banana">>}},
                {modified, false, true, #{}},
                rejected,
                released
               ],
    [ok = reliable_send_receive(QType, Outcome, Config) || Outcome <- Outcomes].

reliable_send_receive(QType, Outcome, Config) ->
    OutcomeBin = if is_atom(Outcome) ->
                        atom_to_binary(Outcome);
                    is_tuple(Outcome) ->
                        O1 = atom_to_binary(element(1, Outcome)),
                        O2 = atom_to_binary(element(2, Outcome)),
                        <<O1/binary, "_", O2/binary>>
                 end,
    ct:pal("~s testing ~s", [?FUNCTION_NAME, OutcomeBin]),

    QName = <<QType/binary, OutcomeBin/binary>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    %% reliable send and consume
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    flush(credited),
    DTag1 = <<"dtag-1">>,
    %% create an unsettled message,
    %% link will be in "mixed" mode by default
    Body = <<"body-1">>,
    Msg1 = amqp10_msg:new(DTag1, Body, false),

    %% Use the 2 byte AMQP boolean encoding, see AMQP §1.6.2
    True = {boolean, true},
    Msg2 = amqp10_msg:set_headers(#{durable => True}, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = wait_for_settlement(DTag1),

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    flush("post sender close"),

    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    {ok, Session2} = amqp10_client:begin_session_sync(Connection2),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session2, <<"test-receiver">>, Address, unsettled),
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([Body], amqp10_msg:body(Msg)),
    ?assertEqual(true, amqp10_msg:header(durable, Msg)),

    ok = amqp10_client:settle_msg(Receiver, Msg, Outcome),
    flush("post accept"),

    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session2),
    ok = amqp10_client:close_connection(Connection2),
    ok = delete_queue(Config, QName).

%% Tests that confirmations are returned correctly
%% when sending many messages async to a quorum queue.
sender_settle_mode_unsettled(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    %% Send many messages aync.
    NumMsgs = 30,
    DTags = [begin
                 DTag = integer_to_binary(N),
                 Msg = amqp10_msg:new(DTag, <<"body">>, false),
                 ok = amqp10_client:send_msg(Sender, Msg),
                 DTag
             end  || N <- lists:seq(1, NumMsgs)],

    %% Wait for confirms.
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 5000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = NumMsgs},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

sender_settle_mode_unsettled_fanout(Config) ->
    QNames = [<<"q1">>, <<"q2">>, <<"q3">>],
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    [begin
         #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
         #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                                  exchange = <<"amq.fanout">>})
     end || QName <- QNames],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/amq.fanout">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    %% Send many messages aync.
    NumMsgs = 20,
    DTags = [begin
                 DTag = integer_to_binary(N),
                 Msg = amqp10_msg:new(DTag, <<"body">>, false),
                 ok = amqp10_client:send_msg(Sender, Msg),
                 DTag
             end  || N <- lists:seq(1, NumMsgs)],

    %% Wait for confirms.
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 5000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    [?assertEqual(#'queue.delete_ok'{message_count = NumMsgs},
                  amqp_channel:call(Ch, #'queue.delete'{queue = QName})) ||
     QName <- QNames],
    ok = rabbit_ct_client_helpers:close_channel(Ch).

%% Tests that confirmations are returned correctly
%% when sending many messages async to a quorum queue where
%% every 3rd message is settled by the sender.
sender_settle_mode_mixed(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, mixed),
    ok = wait_for_credit(Sender),

    %% Send many messages aync.
    NumMsgs = 30,
    DTags = lists:filtermap(
              fun(N) ->
                      DTag = integer_to_binary(N),
                      {Settled, Ret} = case N rem 3 of
                                           0 -> {true, false};
                                           _ -> {false, {true, DTag}}
                                       end,
                      Msg = amqp10_msg:new(DTag, <<"body">>, Settled),
                      ok = amqp10_client:send_msg(Sender, Msg),
                      Ret
              end,  lists:seq(1, NumMsgs)),
    20 = length(DTags),

    %% Wait for confirms.
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 5000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = NumMsgs},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

quorum_queue_rejects(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                  {<<"x-max-length">>, long, 1},
                                                  {<<"x-overflow">>, longstr, <<"reject-publish">>}
                                                 ]}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, mixed),
    ok = wait_for_credit(Sender),

    %% Quorum queue's x-max-length limit is known to be off by 1.
    %% Therefore, we expect the first 2 messages to be accepted.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag a">>, <<>>, false)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag b">>, <<>>, false)),
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 5000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- [<<"tag a">>, <<"tag b">>]],

    %% From now on the quorum queue should reject our publishes.
    %% Send many messages aync.
    NumMsgs = 20,
    DTags = [begin
                 DTag = integer_to_binary(N),
                 Msg = amqp10_msg:new(DTag, <<"body">>, false),
                 ok = amqp10_client:send_msg(Sender, Msg),
                 DTag
             end  || N <- lists:seq(1, NumMsgs)],
    %% Since our sender settle mode is mixed, let's also test sending one as settled.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag c">>, <<>>, true)),
    %% and the final one as unsettled again
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag d">>, <<>>, false)),

    [receive {amqp10_disposition, {rejected, DTag}} -> ok
     after 5000 -> ct:fail({missing_rejected, DTag})
     end || DTag <- DTags ++ [<<"tag d">>]],

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),

    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

receiver_settle_mode_first(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, settled),
    ok = wait_for_credit(Sender),

    %% Send 10 messages.
    [begin
         DTag = Body = integer_to_binary(N),
         Msg = amqp10_msg:new(DTag, Body, true),
         ok = amqp10_client:send_msg(Sender, Msg)
     end  || N <- lists:seq(1, 10)],
    ok = amqp10_client:detach_link(Sender),
    flush("post sender close"),

    %% Receive the first 9 messages.
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, unsettled),
    ok = amqp10_client:flow_link_credit(Receiver, 9, never),
    Msgs_1_to_9 = receive_messages(Receiver, 9),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    assert_messages(QName, 10, 9, Config),

    %% What follows is white box testing: We want to hit a few different branches in the
    %% server code. Although this test is a bit artificial, the AMQP spec does not prohibit
    %% clients to ack in such ranges.

    %% 1. Ack a range smaller than the number of unacked messages where some delivery IDs
    %% are outside the [min, max] range of unacked messages.
    {Msgs_1_to_7, [Msg8, Msg9]} = lists:split(7, Msgs_1_to_9),
    DeliveryIdMsg8 = amqp10_msg:delivery_id(Msg8),
    DeliveryIdMsg9 = amqp10_msg:delivery_id(Msg9),
    ?assertEqual(DeliveryIdMsg9, serial_number_increment(DeliveryIdMsg8)),
    Last1 = serial_number_increment(serial_number_increment(DeliveryIdMsg9)),
    ok = amqp10_client_session:disposition(
           Receiver, DeliveryIdMsg8, Last1, true, accepted),
    assert_messages(QName, 8, 7, Config),

    %% 2. Ack a range smaller than the number of unacked messages where all delivery IDs
    %% are inside the [min, max] range of unacked messages.
    [Msg1, Msg2, _Msg3, Msg4, _Msg5, Msg6, Msg7] = Msgs_1_to_7,
    DeliveryIdMsg4 = amqp10_msg:delivery_id(Msg4),
    DeliveryIdMsg6 = amqp10_msg:delivery_id(Msg6),
    ok = amqp10_client_session:disposition(
           Receiver, DeliveryIdMsg4, DeliveryIdMsg6, true, accepted),
    assert_messages(QName, 5, 4, Config),

    %% 3. Ack a range larger than the number of unacked messages where all delivery IDs
    %% are inside the [min, max] range of unacked messages.
    DeliveryIdMsg2 = amqp10_msg:delivery_id(Msg2),
    DeliveryIdMsg7 = amqp10_msg:delivery_id(Msg7),
    ok = amqp10_client_session:disposition(
           Receiver, DeliveryIdMsg2, DeliveryIdMsg7, true, accepted),
    assert_messages(QName, 2, 1, Config),

    %% Consume the last message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    [Msg10] = receive_messages(Receiver, 1),
    ?assertEqual([<<"10">>], amqp10_msg:body(Msg10)),

    %% 4. Ack a range larger than the number of unacked messages where some delivery IDs
    %% are outside the [min, max] range of unacked messages.
    DeliveryIdMsg1 = amqp10_msg:delivery_id(Msg1),
    DeliveryIdMsg10 = amqp10_msg:delivery_id(Msg10),
    Last2 = serial_number_increment(DeliveryIdMsg10),
    ok = amqp10_client_session:disposition(
           Receiver, DeliveryIdMsg1, Last2, true, accepted),
    assert_messages(QName, 0, 0, Config),

    %% 5. Ack single delivery ID when there are no unacked messages.
    ok = amqp10_client_session:disposition(
           Receiver, DeliveryIdMsg1, DeliveryIdMsg1, true, accepted),

    %% 6. Ack multiple delivery IDs when there are no unacked messages.
    ok = amqp10_client_session:disposition(
           Receiver, DeliveryIdMsg1, DeliveryIdMsg6, true, accepted),
    assert_messages(QName, 0, 0, Config),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok = delete_queue(Config, QName).

publishing_to_non_existing_queue_should_settle_with_released(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    QName = <<"queue does not exist">>,
    Address = <<"/exchange/amq.direct/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    DTag1 = <<"dtag-1">>,
    %% create an unsettled message,
    %% link will be in "mixed" mode by default
    Msg1 = amqp10_msg:new(DTag1, <<"body-1">>, false),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = wait_for_settlement(DTag1, released),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),
    ok = flush("post sender close").

open_link_to_non_existing_destination_should_end_session(Config) ->
    OpnConf = connection_config(Config),
    Name = "non-existing-destination",
    Addresses = ["/exchange/" ++ Name ++ "/bar",
                 "/amq/queue/" ++ Name],
    SenderLinkName = <<"test-sender">>,
    [begin
         {ok, Connection} = amqp10_client:open_connection(OpnConf),
         {ok, Session} = amqp10_client:begin_session_sync(Connection),
         ct:pal("Address ~p", [Address]),
         {ok, _} = amqp10_client:attach_sender_link(
                     Session, SenderLinkName, list_to_binary(Address)),
         wait_for_session_end(Session),
         ok = amqp10_client:close_connection(Connection),
         flush("post sender close")
     end || Address <- Addresses],
    ok.

roundtrip_with_drain_classic_queue(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    roundtrip_with_drain(Config, <<"classic">>, QName).

roundtrip_with_drain_quorum_queue(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    roundtrip_with_drain(Config, <<"quorum">>, QName).

roundtrip_with_drain_stream(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    roundtrip_with_drain(Config, <<"stream">>, QName).

roundtrip_with_drain(Config, QueueType, QName)
  when is_binary(QueueType) ->
    Address = <<"/amq/queue/", QName/binary>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    Args = [{<<"x-queue-type">>, longstr, QueueType}],
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = Args}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    OutMsg = amqp10_msg:new(<<"tag-1">>, <<"my-body">>, false),
    ok = amqp10_client:send_msg(Sender, OutMsg),
    ok = wait_for_accepts(1),

    flush("pre-receive"),
    % create a receiver link
    TerminusDurability = none,
    Filter = consume_from_first(QueueType),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, unsettled,
                       TerminusDurability, Filter),

    % grant credit and drain
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),

    % wait for a delivery
    receive {amqp10_msg, Receiver, InMsg} ->
                ok = amqp10_client:accept_msg(Receiver, InMsg)
    after 2000 ->
              Reason = delivery_timeout,
              flush(Reason),
              ct:fail(Reason)
    end,
    OutMsg2 = amqp10_msg:new(<<"tag-2">>, <<"my-body2">>, false),
    ok = amqp10_client:send_msg(Sender, OutMsg2),
    ok = wait_for_settlement(<<"tag-2">>),

    %% no delivery should be made at this point
    receive {amqp10_msg, _, _} -> ct:fail(unexpected_delivery)
    after 500 -> ok
    end,

    flush("final"),
    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),
    ok = delete_queue(Config, QName).

%% Send a message with a body containing a single AMQP 1.0 value section
%% to a stream and consume via AMQP 0.9.1.
amqp_stream_amqpl(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QName = atom_to_binary(?FUNCTION_NAME),

    amqp_channel:call(Ch, #'queue.declare'{
                             queue = QName,
                             durable = true,
                             arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),

    Address = <<"/amq/queue/", QName/binary>>,
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),
    OutMsg = amqp10_msg:new(<<"my-tag">>, {'v1_0.amqp_value', {binary, <<0, 255>>}}, true),
    ok = amqp10_client:send_msg(Sender, OutMsg),
    flush("final"),
    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),

    #'basic.qos_ok'{} =  amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                            prefetch_count = 1}),
    CTag = <<"my-tag">>,
    #'basic.consume_ok'{} = amqp_channel:subscribe(
                              Ch,
                              #'basic.consume'{
                                 queue = QName,
                                 consumer_tag = CTag,
                                 arguments = [{<<"x-stream-offset">>, longstr, <<"first">>}]},
                              self()),
    receive
        {#'basic.deliver'{consumer_tag = CTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{type = <<"amqp-1.0">>}}} ->
            ok
    after 5000 ->
              ct:fail(basic_deliver_timeout)
    end,
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

message_headers_conversion(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    Address = <<"/amq/queue/", QName/binary>>,
    %% declare a quorum queue
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch, #'queue.declare'{
                             queue = QName,
                             durable = true,
                             arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),

    amqp10_to_amqp091_header_conversion(Session, Ch, QName, Address),
    amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address),

    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = delete_queue(Config, QName),
    ok = amqp10_client:close_connection(Connection).

amqp10_to_amqp091_header_conversion(Session,Ch, QName, Address) -> 
    {ok, Sender} = create_amqp10_sender(Session, Address),

    OutMsg1 = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, false),
    OutMsg2 = amqp10_msg:set_application_properties(
                #{"string" => "string-val",
                  "int" => 2,
                  "bool" => false},
                OutMsg1),
    OutMsg3 = amqp10_msg:set_message_annotations(
                #{"x-string" => "string-value",
                  "x-int" => 3,
                  "x-bool" => true},
                OutMsg2),
    ok = amqp10_client:send_msg(Sender, OutMsg3),
    ok = wait_for_accepts(1),

    {ok, Headers} = amqp091_get_msg_headers(Ch, QName),

    ?assertEqual({longstr, <<"string-val">>}, rabbit_misc:table_lookup(Headers, <<"string">>)),
    ?assertEqual({unsignedint, 2}, rabbit_misc:table_lookup(Headers, <<"int">>)),
    ?assertEqual({bool, false}, rabbit_misc:table_lookup(Headers, <<"bool">>)),

    ?assertEqual({longstr, <<"string-value">>}, rabbit_misc:table_lookup(Headers, <<"x-string">>)),
    ?assertEqual({unsignedint, 3}, rabbit_misc:table_lookup(Headers, <<"x-int">>)),
    ?assertEqual({bool, true}, rabbit_misc:table_lookup(Headers, <<"x-bool">>)).

amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address) -> 
    Amqp091Headers = [{<<"x-forwarding">>, array, 
                       [{table, [{<<"uri">>, longstr,
                                  <<"amqp://localhost/%2F/upstream">>}]}]},
                      {<<"x-string">>, longstr, "my-string"},
                      {<<"x-int">>, long, 92},
                      {<<"x-bool">>, bool, true},
                      {<<"string">>, longstr, "my-str"},
                      {<<"int">>, long, 101},
                      {<<"bool">>, bool, false}],

    amqp_channel:cast(
      Ch,
      #'basic.publish'{routing_key = QName},
      #amqp_msg{props = #'P_basic'{headers = Amqp091Headers},
                payload = <<"foobar">>}),

    {ok, [Msg]} = drain_queue(Session, Address, 1),
    Amqp10MA = amqp10_msg:message_annotations(Msg),
    ?assertEqual(<<"my-string">>, maps:get(<<"x-string">>, Amqp10MA, undefined)),
    ?assertEqual(92, maps:get(<<"x-int">>, Amqp10MA, undefined)),
    ?assertEqual(true, maps:get(<<"x-bool">>, Amqp10MA, undefined)),

    Amqp10Props = amqp10_msg:application_properties(Msg),
    ?assertEqual(<<"my-str">>, maps:get(<<"string">>, Amqp10Props, undefined)),
    ?assertEqual(101, maps:get(<<"int">>, Amqp10Props, undefined)),
    ?assertEqual(false, maps:get(<<"bool">>, Amqp10Props, undefined)).

%% Test sending and receiving concurrently on multiple sessions of the same connection.
multiple_sessions(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    %% Create 2 sessions on the same connection.
    {ok, Session1} = amqp10_client:begin_session(Connection),
    {ok, Session2} = amqp10_client:begin_session(Connection),

    %% Receive on each session.
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Qs = [Q1, Q2],
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session1, <<"receiver link 1">>, Q1, settled, configuration),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session2, <<"receiver link 2">>, Q2, settled, configuration),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    NMsgsPerSender = 20,
    NMsgsPerReceiver = NMsgsPerSender * 2, % due to fanout
    ok = amqp10_client:flow_link_credit(Receiver1, NMsgsPerReceiver, never),
    ok = amqp10_client:flow_link_credit(Receiver2, NMsgsPerReceiver, never),
    flush("receiver attached"),

    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    [#'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                              exchange = <<"amq.fanout">>})
     || QName <- Qs],
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    %% Send on each session.
    TargetAddr = <<"/exchange/amq.fanout">>,
    {ok, Sender1} = amqp10_client:attach_sender_link_sync(
                      Session1, <<"sender link 1">>, TargetAddr, settled, configuration),
    ok = wait_for_credit(Sender1),
    {ok, Sender2} = amqp10_client:attach_sender_link_sync(
                      Session2, <<"sender link 2">>, TargetAddr, settled, configuration),
    ok = wait_for_credit(Sender2),

    %% Send concurrently.
    Group1 = <<"group 1">>,
    Group2 = <<"group 2">>,
    spawn_link(?MODULE, send_messages_with_group_id, [Sender1, NMsgsPerSender, Group1]),
    spawn_link(?MODULE, send_messages_with_group_id, [Sender2, NMsgsPerSender, Group2]),

    Q1Msgs = receive_messages(Receiver1, NMsgsPerReceiver),
    Q2Msgs = receive_messages(Receiver2, NMsgsPerReceiver),
    ExpectedBodies = [integer_to_binary(I) || I <- lists:seq(1, NMsgsPerSender)],
    [begin
         {G1Msgs, G2Msgs} = lists:partition(
                              fun(Msg) ->
                                      #{group_id := GroupId} = amqp10_msg:properties(Msg),
                                      case GroupId of
                                          Group1 -> true;
                                          Group2 -> false
                                      end
                              end, Msgs),
         [begin
              Bodies = [begin
                            [Bin] = amqp10_msg:body(M),
                            Bin
                        end || M <- GMsgs],
              ?assertEqual(ExpectedBodies, Bodies)
          end || GMsgs <- [G1Msgs, G2Msgs]]
     end || Msgs <- [Q1Msgs, Q2Msgs]],

    %% Clean up.
    [ok = amqp10_client:detach_link(Link) || Link <- [Receiver1, Receiver2, Sender1, Sender2]],
    ok = end_session_sync(Session1),
    ok = end_session_sync(Session2),
    ok = amqp10_client:close_connection(Connection),
    [ok = delete_queue(Config, Q) || Q <- Qs].

server_closes_link_classic_queue(Config) ->
    server_closes_link(<<"classic">>, Config).

server_closes_link_quorum_queue(Config) ->
    server_closes_link(<<"quorum">>, Config).

server_closes_link_stream(Config) ->
    server_closes_link(<<"stream">>, Config).

server_closes_link(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,

    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing ATTACH frame from server")
    end,
    ok = amqp10_client:flow_link_credit(Receiver, 5, never),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    flush(credited),
    DTag = <<0>>,
    Body = <<"body">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag, Body, false)),
    ok = wait_for_settlement(DTag),

    receive {amqp10_msg, Receiver, Msg} ->
                ?assertEqual([Body], amqp10_msg:body(Msg))
    after 5000 -> ct:fail("missing msg")
    end,

    [SessionPid] = rpc(Config, rabbit_amqp_session, list_local, []),
    %% Received delivery is unsettled.
    eventually(?_assertEqual(
                  1,
                  begin
                      #{outgoing_unsettled_map := UnsettledMap} = gen_server_state(SessionPid),
                      maps:size(UnsettledMap)
                  end)),

    %% Server closes the link endpoint due to some AMQP 1.0 external condition:
    %% In this test, the external condition is that an AMQP 0.9.1 client deletes the queue.
    delete_queue(Config, QName),

    %% We expect that the server closes the link endpoints,
    %% i.e. the server sends us DETACH frames.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our outgoing link")
    end,

    receive {amqp10_event, {link, Receiver, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our incoming link")
    end,

    %% Our client has not and will not settle the delivery since the source queue got deleted and
    %% the link detached with an error condition. Nevertheless the server session should clean up its
    %% session state by removing the unsettled delivery from its session state.
    eventually(?_assertEqual(
                  0,
                  begin
                      #{outgoing_unsettled_map := UnsettledMap} = gen_server_state(SessionPid),
                      maps:size(UnsettledMap)
                  end)),

    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

server_closes_link_exchange(Config) ->
    XName = atom_to_binary(?FUNCTION_NAME),
    QName = <<"my queue">>,
    RoutingKey = <<"my routing key">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = XName}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                             exchange = XName,
                                                             routing_key = RoutingKey}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/", XName/binary, "/", RoutingKey/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    ?assertMatch(#{publishers := 1}, get_global_counters(Config)),

    DTag1 = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_settlement(DTag1),

    %% Server closes the link endpoint due to some AMQP 1.0 external condition:
    %% In this test, the external condition is that an AMQP 0.9.1 client deletes the exchange.
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = XName}),

    %% When we publish the next message, we expect:
    %% 1. that the message is released because the exchange doesn't exist anymore, and
    DTag2 = <<255>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, false)),
    ok = wait_for_settlement(DTag2, released),
    %% 2. that the server closes the link, i.e. sends us a DETACH frame.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our outgoing link")
    end,
    ?assertMatch(#{publishers := 0}, get_global_counters(Config)),

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

link_target_classic_queue_deleted(Config) ->
    link_target_queue_deleted(<<"classic">>, Config).

link_target_quorum_queue_deleted(Config) ->
    link_target_queue_deleted(<<"quorum">>, Config).

link_target_queue_deleted(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    flush(credited),
    DTag1 = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_settlement(DTag1),

    %% Mock delivery to the target queue to do nothing.
    rabbit_ct_broker_helpers:setup_meck(Config, [?MODULE]),
    Mod = rabbit_queue_type,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, deliver, fun ?MODULE:rabbit_queue_type_deliver_noop/4]),

    %% Send 2nd message.
    DTag2 = <<2>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, false)),
    receive {amqp10_disposition, Unexpected} -> ct:fail({unexpected_disposition, Unexpected})
    after 200 -> ok
    end,

    %% Now, the server AMQP session contains a delivery that did not get confirmed by the target queue.
    %% If we now delete that target queue, RabbitMQ must not reply to us with ACCEPTED.
    %% Instead, we expect RabbitMQ to reply with RELEASED since no queue ever received our 2nd message.
    delete_queue(Config, QName),
    ok = wait_for_settlement(DTag2, released),

    %% After the 2nd message got released, we additionally expect RabbitMQ to close the link given
    %% that the target link endpoint - the queue - got deleted.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our outgoing link")
    end,

    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

rabbit_queue_type_deliver_noop(_TargetQs, _Msg, _Opts, QTypeState) ->
    Actions = [],
    {ok, QTypeState, Actions}.

target_queues_deleted_accepted(Config) ->
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    QNames = [Q1, Q2, Q3],
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    [begin
         #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
         #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                                  exchange = <<"amq.fanout">>})
     end || QName <- QNames],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/amq.fanout/ignored">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),
    flush(credited),

    DTag1 = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_settlement(DTag1),

    %% Mock to deliver only to q1.
    rabbit_ct_broker_helpers:setup_meck(Config, [?MODULE]),
    Mod = rabbit_queue_type,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, deliver, fun ?MODULE:rabbit_queue_type_deliver_to_q1/4]),

    %% Send 2nd message.
    DTag2 = <<2>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, false)),
    receive {amqp10_disposition, Disp1} -> ct:fail({unexpected_disposition, Disp1})
    after 200 -> ok
    end,

    %% Now, the server AMQP session contains a delivery that got confirmed by only q1.
    %% If we delete q2, we should still receive no DISPOSITION since q3 hasn't confirmed.
    ?assertEqual(#'queue.delete_ok'{message_count = 1},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q2})),
    receive {amqp10_disposition, Disp2} -> ct:fail({unexpected_disposition, Disp2})
    after 100 -> ok
    end,
    %% If we delete q3, RabbitMQ should reply with ACCEPTED since at least one target queue (q1) confirmed.
    ?assertEqual(#'queue.delete_ok'{message_count = 1},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q3})),
    receive {amqp10_disposition, {accepted, DTag2}} -> ok
    after 5000 -> ct:fail(accepted_timeout)
    end,

    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q1})),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

rabbit_queue_type_deliver_to_q1(Qs, Msg, Opts, QTypeState) ->
    %% Drop q2 and q3.
    3 = length(Qs),
    Q1 = lists:filter(fun({Q, _RouteInos}) ->
                              amqqueue:get_name(Q) =:= rabbit_misc:r(<<"/">>, queue, <<"q1">>)
                      end, Qs),
    1 = length(Q1),
    meck:passthrough([Q1, Msg, Opts, QTypeState]).

events(Config) ->
    ok = event_recorder:start(Config),

    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{properties => #{<<"ignore-maintenance">> => {boolean, true}}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> ct:fail(opened_timeout)
    end,
    ok = close_connection_sync(Connection),

    [E0, E1, E2] = event_recorder:get_events(Config),
    ok = event_recorder:stop(Config),

    assert_event_type(user_authentication_success, E0),
    Protocol = {protocol, {1, 0}},
    assert_event_prop([{name, <<"guest">>},
                       {auth_mechanism, <<"PLAIN">>},
                       {ssl, false},
                       Protocol],
                      E0),

    assert_event_type(connection_created, E1),
    Node = get_node_config(Config, 0, nodename),
    assert_event_prop(
      [Protocol,
       {node, Node},
       {vhost, <<"/">>},
       {user, <<"guest">>},
       {type, network}],
      E1),
    Props = E1#event.props,
    Name = proplists:lookup(name, Props),
    Pid = proplists:lookup(pid, Props),
    ClientProperties = {client_properties, List} = proplists:lookup(client_properties, Props),
    ?assert(lists:member(
              {<<"product">>, longstr, <<"AMQP 1.0 client from the RabbitMQ Project">>},
              List)),
    ?assert(lists:member(
              {<<"ignore-maintenance">>, bool, true},
              List)),

    assert_event_type(connection_closed, E2),
    assert_event_prop(
      [{node, Node},
       Name,
       Pid,
       ClientProperties],
      E2).

sync_get_unsettled_classic_queue(Config) ->
    sync_get_unsettled(<<"classic">>, Config).

sync_get_unsettled_quorum_queue(Config) ->
    sync_get_unsettled(<<"quorum">>, Config).

sync_get_unsettled_stream(Config) ->
    sync_get_unsettled(<<"stream">>, Config).

%% Test synchronous get, figure 2.43 with sender settle mode unsettled.
sync_get_unsettled(QType, Config) ->
    SenderSettleMode = unsettled,
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 1 receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    %% Grant 1 credit to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),

    %% Since the queue has no messages yet, we shouldn't receive any message.
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail("received unexpected message ~p", [Unexp1])
    after 10 -> ok
    end,

    %% Let's send 4 messages to the queue.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag1">>, <<"m1">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag2">>, <<"m2">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag3">>, <<"m3">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag4">>, <<"m4">>, true)),

    %% Since we previously granted only 1 credit, we should get only the 1st message.
    M1 = receive {amqp10_msg, Receiver, Msg1} ->
                     ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),
                     Msg1
         after 5000 -> ct:fail("missing m1")
         end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 10 -> ok
    end,

    %% Synchronously get the 2nd message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    M2 = receive {amqp10_msg, Receiver, Msg2} ->
                     ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2)),
                     Msg2
         after 5000 -> ct:fail("missing m2")
         end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp3 -> ct:fail("received unexpected message ~p", [Unexp3])
    after 10 -> ok
    end,

    %% Accept the first 2 messages.
    ok = amqp10_client:accept_msg(Receiver, M1),
    ok = amqp10_client:accept_msg(Receiver, M2),
    %% Settlements should not top up credit. We are still out of credits.
    receive {amqp10_msg, _, _} = Unexp4 -> ct:fail("received unexpected message ~p", [Unexp4])
    after 10 -> ok
    end,

    %% Synchronously get the 3rd message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    receive {amqp10_msg, Receiver, Msg3} ->
                ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3))
    after 5000 -> ct:fail("missing m3")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp5 -> ct:fail("received unexpected message ~p", [Unexp5])
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

sync_get_unsettled_2_classic_queue(Config) ->
    sync_get_unsettled_2(<<"classic">>, Config).

sync_get_unsettled_2_quorum_queue(Config) ->
    sync_get_unsettled_2(<<"quorum">>, Config).

sync_get_unsettled_2_stream(Config) ->
    sync_get_unsettled_2(<<"stream">>, Config).

%% Synchronously get 2 messages from queue.
sync_get_unsettled_2(QType, Config) ->
    SenderSettleMode = unsettled,
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach a sender and a receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    %% Grant 2 credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),

    %% Let's send 5 messages to the queue.
    [ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true)) ||
     Bin <- [<<"m1">>, <<"m2">>, <<"m3">>, <<"m4">>, <<"m5">>]],

    %% We should receive exactly 2 messages.
    receive {amqp10_msg, Receiver, Msg1} -> ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1))
    after 5000 -> ct:fail("missing m1")
    end,
    receive {amqp10_msg, Receiver, Msg2} -> ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2))
    after 5000 -> ct:fail("missing m2")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail("received unexpected message ~p", [Unexp1])
    after 50 -> ok
    end,

    %% Grant 2 more credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),
    %% Again, we should receive exactly 2 messages.
    receive {amqp10_msg, Receiver, Msg3} -> ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3))
    after 5000 -> ct:fail("missing m3")
    end,
    receive {amqp10_msg, Receiver, Msg4} -> ?assertEqual([<<"m4">>], amqp10_msg:body(Msg4))
    after 5000 -> ct:fail("missing m4")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 50 -> ok
    end,

    %% Grant 2 more credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),

    %% We should receive the last (5th) message.
    receive {amqp10_msg, Receiver, Msg5} -> ?assertEqual([<<"m5">>], amqp10_msg:body(Msg5))
    after 5000 -> ct:fail("missing m5")
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

sync_get_settled_classic_queue(Config) ->
    sync_get_settled(<<"classic">>, Config).

sync_get_settled_quorum_queue(Config) ->
    sync_get_settled(<<"quorum">>, Config).

sync_get_settled_stream(Config) ->
    sync_get_settled(<<"stream">>, Config).

%% Test synchronous get, figure 2.43 with sender settle mode settled.
sync_get_settled(QType, Config) ->
    SenderSettleMode = settled,
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 1 receivers to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"my receiver">>, Address, SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    %% Grant 1 credit to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),

    %% Since the queue has no messages yet, we shouldn't receive any message.
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail("received unexpected message ~p", [Unexp1])
    after 10 -> ok
    end,

    %% Let's send 3 messages to the queue.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag1">>, <<"m1">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag2">>, <<"m2">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag3">>, <<"m3">>, true)),

    %% Since we previously granted only 1 credit, we should get only the 1st message.
    receive {amqp10_msg, Receiver, Msg1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1))
    after 5000 -> ct:fail("missing m1")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 10 -> ok
    end,

    %% Synchronously get the 2nd message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    receive {amqp10_msg, Receiver, Msg2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2))
    after 5000 -> ct:fail("missing m2")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp3 -> ct:fail("received unexpected message ~p", [Unexp3])
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

timed_get_classic_queue(Config) ->
    timed_get(<<"classic">>, Config).

timed_get_quorum_queue(Config) ->
    timed_get(<<"quorum">>, Config).

timed_get_stream(Config) ->
    timed_get(<<"stream">>, Config).

%% Synchronous get with a timeout, figure 2.44.
timed_get(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach a sender and a receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, false),

    Timeout = 10,
    receive Unexpected0 -> ct:fail("received unexpected ~p", [Unexpected0])
    after Timeout -> ok
    end,

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,

    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"my tag">>, <<"my msg">>, true)),

    %% Since our consumer didn't grant any new credit, we shouldn't receive the message we
    %% just sent.
    receive Unexpected1 -> ct:fail("received unexpected ~p", [Unexpected1])
    after 50 -> ok
    end,

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),
    receive {amqp10_msg, Receiver, Msg1} -> ?assertEqual([<<"my msg">>], amqp10_msg:body(Msg1))
    after 5000 -> ct:fail("missing 'my msg'")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

stop_classic_queue(Config) ->
    stop(<<"classic">>, Config).

stop_quorum_queue(Config) ->
    stop(<<"quorum">>, Config).

stop_stream(Config) ->
    stop(<<"stream">>, Config).

%% Test stopping a link, figure 2.46.
stop(QType, Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    %% Attach 1 sender and 1 receiver to the queue.
    OpnConf0 = connection_config(Config),
    NumSent = 300,
    %% Allow in flight messages to be received after stopping the link.
    OpnConf = OpnConf0#{transfer_limit_margin => -NumSent},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, settled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    ok = amqp10_client:flow_link_credit(Receiver, 10, 5),
    ok = send_messages(Sender, NumSent, true),

    %% Let's await the first 20 messages.
    NumReceived = 20,
    Msgs = receive_messages(Receiver, NumReceived),

    %% Stop the link.
    %% "Stopping the transfers on a given link is accomplished by updating
    %% the link-credit to be zero and sending the updated flow state." [2.6.10]
    ok = amqp10_client:stop_receiver_link(Receiver),
    %% "It is possible that some transfers could be in flight at the time the flow
    %% state is sent, so incoming transfers could still arrive on the link." [2.6.10]
    NumInFlight = count_received_messages(Receiver),

    ct:pal("After receiving the first ~b messages and stopping the link, "
           "we received ~b more in flight messages", [NumReceived, NumInFlight]),
    ?assert(NumInFlight > 0,
            "expected some in flight messages, but there were actually none"),
    ?assert(NumInFlight < NumSent - NumReceived,
            "expected the link to stop, but actually received all messages"),

    %% Check that contents of the first 20 messages are correct.
    FirstMsg = hd(Msgs),
    LastMsg = lists:last(Msgs),
    ?assertEqual([integer_to_binary(NumSent)], amqp10_msg:body(FirstMsg)),
    ?assertEqual([integer_to_binary(NumSent - NumReceived + 1)], amqp10_msg:body(LastMsg)),

    %% Let's resume the link.
    ok = amqp10_client:flow_link_credit(Receiver, 50, 40),

    %% We expect to receive all remaining messages.
    NumRemaining = NumSent - NumReceived - NumInFlight,
    ct:pal("Waiting for the remaining ~b messages", [NumRemaining]),
    Msgs1 = receive_messages(Receiver, NumRemaining),
    ?assertEqual([<<"1">>], amqp10_msg:body(lists:last(Msgs1))),

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = amqp10_client:close_connection(Connection).

single_active_consumer_classic_queue(Config) ->
    single_active_consumer(<<"classic">>, Config).

single_active_consumer_quorum_queue(Config) ->
    single_active_consumer(<<"quorum">>, Config).

single_active_consumer(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-single-active-consumer">>, bool, true},
                                                  {<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    %% Attach 1 sender and 2 receivers to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    flush(sender_attached),

    %% The 1st consumer will become active.
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session,
                        <<"test-receiver-1">>,
                        Address,
                        unsettled),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    ok = amqp10_client:flow_link_credit(Receiver1, 2, never),

    %% The 2nd consumer will become inactive.
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session,
                        <<"test-receiver-2">>,
                        Address,
                        unsettled),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    ok = amqp10_client:flow_link_credit(Receiver2, 2, never),

    NumMsgs = 5,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true))
     end || N <- lists:seq(1, NumMsgs)],

    %% Only the active consumer should receive messages.
    receive {amqp10_msg, Receiver1, Msg1} -> ?assertEqual([<<"1">>], amqp10_msg:body(Msg1))
    after 5000 -> ct:fail("missing 1st msg")
    end,
    receive {amqp10_msg, Receiver1, Msg2} -> ?assertEqual([<<"2">>], amqp10_msg:body(Msg2))
    after 5000 -> ct:fail("missing 2nd msg")
    end,
    receive {amqp10_event, {link, Receiver1, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive Unexpected0 -> ct:fail("received unexpected ~p", [Unexpected0])
    after 10 -> ok
    end,

    %% Cancelling the active consumer should cause the inactive to become active.
    ok = amqp10_client:detach_link(Receiver1),
    receive {amqp10_event, {link, Receiver1, {detached, normal}}} -> ok
    after 5000 -> ct:fail("missing detached")
    end,
    receive {amqp10_msg, Receiver2, Msg3} -> ?assertEqual([<<"3">>], amqp10_msg:body(Msg3))
    after 5000 -> ct:fail("missing 3rd msg")
    end,
    receive {amqp10_msg, Receiver2, Msg4} -> ?assertEqual([<<"4">>], amqp10_msg:body(Msg4))
    after 5000 -> ct:fail("missing 4th msg")
    end,
    receive {amqp10_event, {link, Receiver2, credit_exhausted}} -> ok
    after 5000 -> ct:fail("expected credit_exhausted")
    end,
    receive Unexpected1 -> ct:fail("received unexpected ~p", [Unexpected1])
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Receiver2),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    delete_queue(Config, QName).

%% "A session endpoint can choose to unmap its output handle for a link. In this case, the endpoint MUST
%% send a detach frame to inform the remote peer that the handle is no longer attached to the link endpoint.
%% If both endpoints do this, the link MAY return to a fully detached state. Note that in this case the
%% link endpoints MAY still indirectly communicate via the session, as there could still be active deliveries
%% on the link referenced via delivery-id." [2.6.4]
%%
%% "The disposition performative MAY refer to deliveries on links that are no longer attached. As long as
%% the links have not been closed or detached with an error then the deliveries are still "live" and the
%% updated state MUST be applied." [2.7.6]
%%
%% Although the spec allows to settle delivery IDs on detached links, RabbitMQ does not respect the 'closed'
%% field of the DETACH frame and therefore handles every DETACH frame as closed. Since the link is closed,
%% we expect every outstanding delivery to be requeued.
%%
%% In addition to consumer cancellation, detaching a link therefore causes in flight deliveries to be requeued.
%%
%% Note that this behaviour is different from merely consumer cancellation in AMQP legacy:
%% "After a consumer is cancelled there will be no future deliveries dispatched to it. Note that there can
%% still be "in flight" deliveries dispatched previously. Cancelling a consumer will neither discard nor requeue them."
%% [https://www.rabbitmq.com/consumers.html#unsubscribing]
detach_requeues(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),

    %% Attach 1 sender and 2 receivers to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, settled),
    ok = wait_for_credit(Sender),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"recv 1">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"recv 2">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(attached),

    ok = amqp10_client:flow_link_credit(Receiver1, 50, never),
    ok = amqp10_client:flow_link_credit(Receiver2, 50, never),

    %% Let's send 4 messages to the queue.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag1">>, <<"m1">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag2">>, <<"m2">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag3">>, <<"m3">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag4">>, <<"m4">>, true)),
    ok = amqp10_client:detach_link(Sender),

    %% The queue should serve round robin.
    [Msg1, Msg3] = receive_messages(Receiver1, 2),
    [Msg2, Msg4] = receive_messages(Receiver2, 2),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),
    ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3)),
    ?assertEqual([<<"m4">>], amqp10_msg:body(Msg4)),

    %% Let's detach the 1st receiver.
    ok = amqp10_client:detach_link(Receiver1),
    receive {amqp10_event, {link, Receiver1, {detached, normal}}} -> ok
    after 5000 -> ct:fail("missing detached")
    end,

    %% Since Receiver1 hasn't settled its 2 deliveries,
    %% we expect them to be re-queued and re-delivered to Receiver2.
    [Msg1b, Msg3b] = receive_messages(Receiver2, 2),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1b)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3b)),

    %% Receiver2 accepts all 4 messages.
    ok = amqp10_client_session:disposition(
           Receiver2,
           amqp10_msg:delivery_id(Msg2),
           amqp10_msg:delivery_id(Msg3b),
           true, accepted),

    %% Double check that there are no in flight deliveries in the server session.
    [SessionPid] = rpc(Config, rabbit_amqp_session, list_local, []),
    eventually(?_assertEqual(
                  0,
                  begin
                      #{outgoing_unsettled_map := UnsettledMap} = gen_server_state(SessionPid),
                      maps:size(UnsettledMap)
                  end)),

    ok = amqp10_client:detach_link(Receiver2),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

resource_alarm_before_session_begin(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),

    %% Set memory alarm before beginning the session.
    DefaultWatermark = rpc(Config, vm_memory_monitor, get_vm_memory_high_watermark, []),
    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0]),
    timer:sleep(100),

    {ok, Session1} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session1, <<"test-sender">>, Address, unsettled),
    %% We should still receive link credit since the target queue is fine.
    ok = wait_for_credit(Sender),
    %% However, RabbitMQ's incoming window shouldn't allow our client to send any TRANSFER.
    %% In other words, the client is limited by session flow control, but not by link flow control.
    Tag1 = <<"tag1">>,
    Msg1 = amqp10_msg:new(Tag1, <<"m1">>, false),
    ?assertEqual({error, remote_incoming_window_exceeded},
                 amqp10_client:send_msg(Sender, Msg1)),

    %% Set additionally disk alarm.
    DefaultDiskFreeLimit = rpc(Config, rabbit_disk_monitor, get_disk_free_limit, []),
    ok = rpc(Config, rabbit_disk_monitor, set_disk_free_limit, [999_000_000_000_000]), % 999 TB
    timer:sleep(100),

    ?assertEqual({error, remote_incoming_window_exceeded},
                 amqp10_client:send_msg(Sender, Msg1)),

    %% Clear memory alarm.
    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [DefaultWatermark]),
    timer:sleep(100),

    ?assertEqual({error, remote_incoming_window_exceeded},
                 amqp10_client:send_msg(Sender, Msg1)),

    %% Clear disk alarm.
    ok = rpc(Config, rabbit_disk_monitor, set_disk_free_limit, [DefaultDiskFreeLimit]),
    timer:sleep(100),

    %% All alarms are cleared now.
    %% Hence, RabbitMQ should open its incoming window allowing our client to send TRANSFERs.
    ?assertEqual(ok,
                 amqp10_client:send_msg(Sender, Msg1)),
    ok = wait_for_settlement(Tag1),

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session1),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

resource_alarm_after_session_begin(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    Address = <<"/amq/queue/", QName/binary>>,
    OpnConf = connection_config(Config),

    {ok, Connection1} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session_sync(Connection1),
    {ok, Sender} = amqp10_client:attach_sender_link(Session1, <<"sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session1, <<"receiver 1">>, Address, unsettled),

    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t1">>, <<"m1">>, false)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t2">>, <<"m2">>, false)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t3">>, <<"m3">>, false)),
    ok = wait_for_accepts(3),

    %% Set memory alarm.
    DefaultWatermark = rpc(Config, vm_memory_monitor, get_vm_memory_high_watermark, []),
    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0]),
    timer:sleep(100),

    %% Our existing receiver should still be able to receive.
    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),
    ok = amqp10_client:accept_msg(Receiver1, Msg1),

    %% Attaching a new receiver to the same session and receiving should also work.
    {ok, Receiver2} = amqp10_client:attach_receiver_link(Session1, <<"receiver 2">>, Address, unsettled),
    {ok, Msg2} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2)),
    ok = amqp10_client:accept_msg(Receiver2, Msg2),

    %% Even creating a new connection and receiving should work.
    {ok, Connection2} = amqp10_client:open_connection(OpnConf#{container_id => <<"my container 2">>}),
    {ok, Session2} = amqp10_client:begin_session_sync(Connection2),
    {ok, Receiver3} = amqp10_client:attach_receiver_link(Session2, <<"receiver 3">>, Address, unsettled),
    {ok, Msg3} = amqp10_client:get_msg(Receiver3),
    ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3)),
    ok = amqp10_client:accept_msg(Receiver3, Msg3),

    %% However, we shouldn't be able to send any TRANSFER.
    Msg4 = amqp10_msg:new(<<"t4">>, <<"m4">>, false),
    ?assertEqual({error, remote_incoming_window_exceeded},
                 amqp10_client:send_msg(Sender, Msg4)),

    %% Clear memory alarm.
    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [DefaultWatermark]),
    timer:sleep(100),

    %% Now, we should be able to send again.
    ?assertEqual(ok,
                 amqp10_client:send_msg(Sender, Msg4)),
    ok = wait_for_settlement(<<"t4">>),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver1),
    ok = amqp10_client:detach_link(Receiver2),
    ok = amqp10_client:detach_link(Receiver3),
    ok = end_session_sync(Session1),
    ok = end_session_sync(Session2),
    ok = amqp10_client:close_connection(Connection1),
    ok = amqp10_client:close_connection(Connection2),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

auth_attempt_metrics(Config) ->
    open_and_close_connection(Config),
    [Attempt1] = rpc(Config, rabbit_core_metrics, get_auth_attempts, []),
    ?assertEqual(false, proplists:is_defined(remote_address, Attempt1)),
    ?assertEqual(false, proplists:is_defined(username, Attempt1)),
    ?assertEqual(<<"amqp10">>, proplists:get_value(protocol, Attempt1)),
    ?assertEqual(1, proplists:get_value(auth_attempts, Attempt1)),
    ?assertEqual(0, proplists:get_value(auth_attempts_failed, Attempt1)),
    ?assertEqual(1, proplists:get_value(auth_attempts_succeeded, Attempt1)),

    rpc(Config, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    ok = rpc(Config, application, set_env, [rabbit, track_auth_attempt_source, true]),
    open_and_close_connection(Config),
    Attempts = rpc(Config, rabbit_core_metrics, get_auth_attempts_by_source, []),
    [Attempt2] = lists:filter(fun(Props) ->
                                      proplists:is_defined(remote_address, Props)
                              end, Attempts),
    ?assertEqual(<<>>, proplists:get_value(remote_address, Attempt2)),
    ?assertEqual(<<"guest">>, proplists:get_value(username, Attempt2)),
    ?assertEqual(<<"amqp10">>, proplists:get_value(protocol, Attempt2)),
    ?assertEqual(1, proplists:get_value(auth_attempts, Attempt2)),
    ?assertEqual(0, proplists:get_value(auth_attempts_failed, Attempt2)),
    ?assertEqual(1, proplists:get_value(auth_attempts_succeeded, Attempt2)).

max_message_size_client_to_server(Config) ->
    DefaultMaxMessageSize = rpc(Config, persistent_term, get, [max_message_size]),
    %% Limit the server to only accept messages up to 2KB.
    MaxMessageSize = 2_000,
    ok = rpc(Config, persistent_term, put, [max_message_size, MaxMessageSize]),

    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    Address = <<"/amq/queue/", QName/binary>>,
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address, mixed),
    ok = wait_for_credit(Sender),

    PayloadSmallEnough = binary:copy(<<0>>, MaxMessageSize - 10),
    ?assertEqual(ok,
                 amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t1">>, PayloadSmallEnough, false))),
    ok = wait_for_settlement(<<"t1">>),

    PayloadTooLarge = binary:copy(<<0>>, MaxMessageSize + 1),
    ?assertEqual({error, message_size_exceeded},
                 amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t2">>, PayloadTooLarge, false))),
    ?assertEqual({error, message_size_exceeded},
                 amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t3">>, PayloadTooLarge, true))),

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = rpc(Config, persistent_term, put, [max_message_size, DefaultMaxMessageSize]).

max_message_size_server_to_client(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    Address = <<"/amq/queue/", QName/binary>>,
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    MaxMessageSize = 2000,
    %% Leave a bit of headroom for additional sections sent from RabbitMQ to us,
    %% e.g. message annotations with routing key and exchange name.
    PayloadSmallEnough = binary:copy(<<0>>, MaxMessageSize - 200),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t1">>, PayloadSmallEnough, false)),
    ok = wait_for_settlement(<<"t1">>),

    AttachArgs = #{max_message_size => MaxMessageSize,
                   name => <<"test-receiver">>,
                   role => {receiver, #{address => Address,
                                        durable => configuration}, self()},
                   snd_settle_mode => unsettled,
                   rcv_settle_mode => first,
                   filter => #{}},
    {ok, Receiver} = amqp10_client:attach_link(Session, AttachArgs),
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([PayloadSmallEnough], amqp10_msg:body(Msg)),

    PayloadTooLarge = binary:copy(<<0>>, MaxMessageSize + 1),
    %% The sending link has no maximum message size set.
    %% Hence, sending this large message from client to server should work.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t2">>, PayloadTooLarge, false)),
    ok = wait_for_settlement(<<"t2">>),

    %% However, the receiving link has a maximum message size set.
    %% Hence, when the server attempts to deliver this large message,
    %% it should throw link error message-size-exceeded.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_LINK_ERROR_MESSAGE_SIZE_EXCEEDED}}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive expected error")
    end,

    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

%% This test ensures that the server sends us TRANSFER and FLOW frames in the correct order
%% even if the server is temporarily not allowed to send us any TRANSFERs due to our incoming
%% window being closed.
receive_transfer_flow_order(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    Address = <<"/amq/queue/", QName/binary>>,
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    DTag = <<"my tag">>,
    Body = <<"my body">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag, Body, false)),
    ok = wait_for_settlement(DTag),
    ok = amqp10_client:detach_link(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail(missing_attached)
    end,
    flush(receiver_attached),

    %% Close our incoming window.
    gen_statem:cast(Session, {flow_session, #'v1_0.flow'{incoming_window = {uint, 0}}}),

    ok = amqp10_client:flow_link_credit(Receiver, 2, never, true),
    %% Given our incoming window is closed, we shouldn't receive the TRANSFER yet, and threfore
    %% must not yet receive the FLOW that comes thereafter with drain=true, credit=0, and advanced delivery-count.
    receive Unexpected -> ct:fail({unexpected, Unexpected})
    after 300 -> ok
    end,

    %% Open our incoming window
    gen_statem:cast(Session, {flow_session, #'v1_0.flow'{incoming_window = {uint, 5}}}),
    %% Important: We should first receive the TRANSFER,
    %% and only thereafter the FLOW (and hence the credit_exhausted notification).
    receive First ->
                {amqp10_msg, Receiver, Msg} = First,
                ?assertEqual([Body], amqp10_msg:body(Msg))
    after 5000 -> ct:fail("timeout receiving message")
    end,
    receive Second ->
                ?assertEqual({amqp10_event, {link, Receiver, credit_exhausted}}, Second)
    after 5000 -> ct:fail("timeout receiving credit_exhausted")
    end,

    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ok = delete_queue(Config, QName).

last_queue_confirms(Config) ->
    ClassicQ = <<"my classic queue">>,
    QuorumQ = <<"my quorum queue">>,
    Qs = [ClassicQ, QuorumQ],
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = ClassicQ}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QuorumQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                  {<<"x-quorum-initial-group-size">>, long, 3}
                                                 ]}),
    [#'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                              exchange = <<"amq.fanout">>})
     || QName <- Qs],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    AddressFanout = <<"/exchange/amq.fanout">>,
    {ok, SenderFanout} = amqp10_client:attach_sender_link(
                           Session, <<"sender-1">>, AddressFanout, unsettled),
    ok = wait_for_credit(SenderFanout),

    AddressClassicQ = <<"/amq/queue/", ClassicQ/binary>>,
    {ok, SenderClassicQ} = amqp10_client:attach_sender_link(
                             Session, <<"sender-2">>, AddressClassicQ, unsettled),
    ok = wait_for_credit(SenderClassicQ),

    DTag1 = <<"t1">>,
    ok = amqp10_client:send_msg(SenderFanout, amqp10_msg:new(DTag1, <<"m1">>, false)),
    receive {amqp10_disposition, {accepted, DTag1}} -> ok
    after 5000 -> ct:fail({missing_accepted, DTag1})
    end,

    %% Make quorum queue unavailable.
    ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 1),

    DTag2 = <<"t2">>,
    DTag3 = <<"t3">>,
    ok = amqp10_client:send_msg(SenderFanout, amqp10_msg:new(DTag2, <<"m2">>, false)),
    ok = amqp10_client:send_msg(SenderClassicQ, amqp10_msg:new(DTag3, <<"m3">>, false)),

    %% Since quorum queue is down, we should only get a confirmation for m3.
    receive {amqp10_disposition, {accepted, DTag3}} -> ok
    after 5000 -> ct:fail({missing_accepted, DTag3})
    end,
    receive {amqp10_disposition, Unexpected} -> ct:fail({unexpected_disposition, Unexpected})
    after 200 -> ok
    end,

    ok = rabbit_ct_broker_helpers:start_node(Config, 1),
    ok = rabbit_ct_broker_helpers:start_node(Config, 2),
    %% Since the quorum queue has become available, we should now get a confirmation for m2.
    receive {amqp10_disposition, {accepted, DTag2}} -> ok
    after 10_000 -> ct:fail({missing_accepted, DTag2})
    end,

    ok = amqp10_client:detach_link(SenderClassicQ),
    ok = amqp10_client:detach_link(SenderFanout),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = 3},
                 amqp_channel:call(Ch, #'queue.delete'{queue = ClassicQ})),
    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QuorumQ})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

target_queue_deleted(Config) ->
    ClassicQ = <<"my classic queue">>,
    QuorumQ = <<"my quorum queue">>,
    Qs = [ClassicQ, QuorumQ],
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = ClassicQ}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QuorumQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                  %% Use 2 replica quorum queue, such that we can stop 1 node
                                                  %% later to make quorum queue unavailable, but still have
                                                  %% 2 out of 3 nodes running for Khepri being available.
                                                  {<<"x-quorum-initial-group-size">>, long, 2}
                                                 ]}),
    [#'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                              exchange = <<"amq.fanout">>})
     || QName <- Qs],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    Address = <<"/exchange/amq.fanout">>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    DTag1 = <<"t1">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    receive {amqp10_disposition, {accepted, DTag1}} -> ok
    after 5000 -> ct:fail({missing_accepted, DTag1})
    end,

    N0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    RaName = ra_name(QuorumQ),
    ServerId0 = {RaName, N0},
    {ok, Members, _Leader} = ra:members(ServerId0),
    ?assertEqual(2, length(Members)),
    [{RaName, ReplicaNode}] = Members -- [ServerId0],
    ct:pal("Stopping node ~s to make quorum queue unavailable...", [ReplicaNode]),
    ok = rabbit_ct_broker_helpers:stop_node(Config, ReplicaNode),
    flush("quorum queue is down"),

    DTag2 = <<"t2">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, false)),
    %% Target classic queue should receive m2.
    assert_messages(ClassicQ, 2, 0, Config),
    %% Delete target classic queue. (Khepri is still available.)
    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = ClassicQ})),

    %% Since quorum queue is down, we should still receive no DISPOSITION.
    receive {amqp10_disposition, Unexpected} -> ct:fail({unexpected_disposition, Unexpected})
    after 100 -> ok
    end,

    ok = rabbit_ct_broker_helpers:start_node(Config, ReplicaNode),
    %% Since the quorum queue has become available, we should now get a confirmation for m2.
    receive {amqp10_disposition, {accepted, DTag2}} -> ok
    after 10_000 -> ct:fail({missing_accepted, DTag2})
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QuorumQ})),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

target_classic_queue_down(Config) ->
    ClassicQueueNode = 2,
    Ch = rabbit_ct_client_helpers:open_channel(Config, ClassicQueueNode),
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = <<"/amq/queue/", QName/binary>>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]}),
    ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, ClassicQueueNode),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session, <<"receiver 1">>, Address),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    DTag1 = <<"t1">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_settlement(DTag1),

    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),

    %% Make classic queue down.
    flush("stopping node"),
    ok = rabbit_ct_broker_helpers:stop_node(Config, ClassicQueueNode),

    %% We expect that the server closes links that receive from classic queues that are down.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_ILLEGAL_STATE},
    receive {amqp10_event, {link, Receiver1, {detached, ExpectedError}}} -> ok
    after 10_000 -> ct:fail({missing_event, ?LINE})
    end,
    %% However the server should not close links that send to classic queues that are down.
    receive Unexpected -> ct:fail({unexpected, Unexpected})
    after 20 -> ok
    end,
    %% Instead, the server should reject messages that are sent to classic queues that are down.
    DTag2 = <<"t2">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, false)),
    ok = wait_for_settlement(DTag2, rejected),

    ok = rabbit_ct_broker_helpers:start_node(Config, ClassicQueueNode),
    %% Now that the classic queue is up again, we should be able to attach a new receiver
    %% and be able to send to and receive from the classic queue.
    {ok, Receiver2} = amqp10_client:attach_receiver_link(Session, <<"receiver 2">>, Address),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    DTag3 = <<"t3">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag3, <<"m3">>, false)),
    ok = wait_for_settlement(DTag3),
    {ok, Msg3} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3)),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver2),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    delete_queue(Config, QName).

async_notify_settled_classic_queue(Config) ->
    %% TODO Bump old version in mixed version tests to 3.13.x,
    %% require ff message_containers and always run this test case.
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, message_containers) of
        ok -> async_notify(settled, <<"classic">>, Config);
        {skip, _} = Skip -> Skip
    end.

async_notify_settled_quorum_queue(Config) ->
    async_notify(settled, <<"quorum">>, Config).

async_notify_settled_stream(Config) ->
    async_notify(settled, <<"stream">>, Config).

async_notify_unsettled_classic_queue(Config) ->
    %% TODO Bump old version in mixed version tests to 3.13.x,
    %% require ff message_containers and always run this test case.
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, message_containers) of
        ok -> async_notify(unsettled, <<"classic">>, Config);
        {skip, _} = Skip -> Skip
    end.

async_notify_unsettled_quorum_queue(Config) ->
    async_notify(unsettled, <<"quorum">>, Config).

async_notify_unsettled_stream(Config) ->
    async_notify(unsettled, <<"stream">>, Config).

%% Test asynchronous notification, figure 2.45.
async_notify(SenderSettleMode, QType, Config) ->
    %% Place queue leader on the old node.
    Ch = rabbit_ct_client_helpers:open_channel(Config, 1),
    QName = atom_to_binary(?FUNCTION_NAME),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    %% Connect AMQP client to the new node causing queue client to run the new code.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% Send 30 messages to the queue.
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    NumMsgs = 30,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, false))
     end || N <- lists:seq(1, NumMsgs)],
    %% Wait for last message to be confirmed.
    ok = wait_for_settlement(integer_to_binary(NumMsgs)),
    flush(settled),
    ok = detach_link_sync(Sender),

    case QType of
        <<"stream">> ->
            %% If it is a stream we need to wait until there is a local member
            %% on the node we want to subscibe from before proceeding.
            rabbit_ct_helpers:await_condition(
              fun() -> rabbit_ct_broker_helpers:rpc(
                         Config, 0, ?MODULE, has_local_member,
                         [rabbit_misc:r(<<"/">>, queue, QName)])
              end, 30_000);
        _ ->
            ok
    end,
    Filter = consume_from_first(QType),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address,
                       SenderSettleMode, configuration, Filter),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,

    %% Initially, grant 10 credits to the sending queue.
    %% Whenever the sum of credits and number of unsettled messages drops below 5, renew back to 10.
    ok = amqp10_client:flow_link_credit(Receiver, 10, 5),

    %% We should receive all messages.
    Accept = case SenderSettleMode of
                 settled -> false;
                 unsettled -> true
             end,
    Msgs = receive_all_messages(Receiver, Accept),
    FirstMsg = hd(Msgs),
    LastMsg = lists:last(Msgs),
    ?assertEqual([<<"1">>], amqp10_msg:body(FirstMsg)),
    ?assertEqual([integer_to_binary(NumMsgs)], amqp10_msg:body(LastMsg)),

    %% No further messages should be delivered.
    receive Unexpected -> ct:fail({received_unexpected_message, Unexpected})
    after 50 -> ok
    end,

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% For TRANSFERS from AMQP client to RabbitMQ, this test asserts that a single slow link receiver
%% (slow queue) does not impact other link receivers (fast queues) on the **same** session.
%% (This is unlike AMQP legacy where a single slow queue will block the entire connection.)
link_flow_control(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    CQ = <<"cq">>,
    QQ = <<"qq">>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = CQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    AddressCQ = <<"/amq/queue/", CQ/binary>>,
    AddressQQ = <<"/amq/queue/", QQ/binary>>,
    {ok, ReceiverCQ} = amqp10_client:attach_receiver_link(Session, <<"cq-receiver">>, AddressCQ, settled),
    {ok, ReceiverQQ} = amqp10_client:attach_receiver_link(Session, <<"qq-receiver">>, AddressQQ, settled),
    {ok, SenderCQ} = amqp10_client:attach_sender_link(Session, <<"cq-sender">>, AddressCQ, settled),
    {ok, SenderQQ} = amqp10_client:attach_sender_link(Session, <<"qq-sender">>, AddressQQ, settled),
    ok = wait_for_credit(SenderCQ),
    ok = wait_for_credit(SenderQQ),
    flush(attached),

    %% Send and receive a single message on both queues.
    ok = amqp10_client:send_msg(SenderCQ, amqp10_msg:new(<<0>>, <<0>>, true)),
    ok = amqp10_client:send_msg(SenderQQ, amqp10_msg:new(<<1>>, <<1>>, true)),
    {ok, Msg0} = amqp10_client:get_msg(ReceiverCQ),
    ?assertEqual([<<0>>], amqp10_msg:body(Msg0)),
    {ok, Msg1} = amqp10_client:get_msg(ReceiverQQ),
    ?assertEqual([<<1>>], amqp10_msg:body(Msg1)),

    %% Make quorum queue unavailable.
    ok = rabbit_ct_broker_helpers:stop_node(Config, 2),
    ok = rabbit_ct_broker_helpers:stop_node(Config, 1),

    NumMsgs = 1000,
    %% Since the quorum queue is unavailable, we expect our quorum queue sender to run
    %% out of credits and RabbitMQ should not grant our quorum queue sender any new credits.
    ok = assert_link_credit_runs_out(SenderQQ, NumMsgs),
    %% Despite the quorum queue being unavailable, the classic queue can perfectly receive messages.
    %% So, we expect that on the same AMQP session, link credit will be renewed for our classic queue sender.
    ok = send_messages(SenderCQ, NumMsgs, true),

    %% Check that all 1k messages can be received from the classic queue.
    ok = amqp10_client:flow_link_credit(ReceiverCQ, NumMsgs, never),
    ReceivedCQ = receive_messages(ReceiverCQ, NumMsgs),
    FirstMsg = hd(ReceivedCQ),
    LastMsg = lists:last(ReceivedCQ),
    ?assertEqual([integer_to_binary(NumMsgs)], amqp10_msg:body(FirstMsg)),
    ?assertEqual([<<"1">>], amqp10_msg:body(LastMsg)),

    %% We expect still that RabbitMQ won't grant our quorum queue sender any new credits.
    receive {amqp10_event, {link, SenderQQ, credited}} ->
                ct:fail({unexpected_credited, ?LINE})
    after 5 -> ok
    end,

    %% Make quorum queue available again.
    ok = rabbit_ct_broker_helpers:start_node(Config, 1),
    ok = rabbit_ct_broker_helpers:start_node(Config, 2),

    %% Now, we exepct that the messages sent earlier make it actually into the quorum queue.
    %% Therefore, RabbitMQ should grant our quorum queue sender more credits.
    receive {amqp10_event, {link, SenderQQ, credited}} ->
                ct:pal("quorum queue sender got credited")
    after 30_000 -> ct:fail({credited_timeout, ?LINE})
    end,

    [ok = amqp10_client:detach_link(Link) || Link <- [ReceiverCQ, ReceiverQQ, SenderCQ, SenderQQ]],
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    delete_queue(Config, QQ),
    delete_queue(Config, CQ).

classic_queue_on_old_node(Config) ->
    %% TODO Bump old version in mixed version tests to 3.13.x,
    %% require ff message_containers and always run this test case.
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, message_containers) of
        ok -> queue_and_client_different_nodes(1, 0, <<"classic">>, Config);
        {skip, _} = Skip -> Skip
    end.

classic_queue_on_new_node(Config) ->
    queue_and_client_different_nodes(0, 1, <<"classic">>, Config).

quorum_queue_on_old_node(Config) ->
    queue_and_client_different_nodes(1, 0, <<"quorum">>, Config).

quorum_queue_on_new_node(Config) ->
    queue_and_client_different_nodes(0, 1, <<"quorum">>, Config).

%% In mixed version tests, run the queue leader with old code
%% and queue client with new code, or vice versa.
queue_and_client_different_nodes(QueueLeaderNode, ClientNode, QueueType, Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, QueueLeaderNode),
    QName = atom_to_binary(?FUNCTION_NAME),
    #'queue.declare_ok'{} =  amqp_channel:call(
                               Ch, #'queue.declare'{queue = QName,
                                                    durable = true,
                                                    arguments = [{<<"x-queue-type">>, longstr, QueueType}]}),
    %% Connect AMQP client to the new (or old) node causing queue client to run the new (or old) code.
    OpnConf = connection_config(ClientNode, Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    %% Let's test with many messages to make sure we're not
    %% impacted by RabbitMQ internal credit based flow control.
    NumMsgs = 1100,
    ok = send_messages(Sender, NumMsgs, true),

    %% Grant credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, NumMsgs, never),

    %% We should receive all messages.
    Msgs = receive_messages(Receiver, NumMsgs),
    FirstMsg = hd(Msgs),
    LastMsg = lists:last(Msgs),
    ?assertEqual([integer_to_binary(NumMsgs)], amqp10_msg:body(FirstMsg)),
    ?assertEqual([<<"1">>], amqp10_msg:body(LastMsg)),

    ok = amqp10_client_session:disposition(
           Receiver,
           amqp10_msg:delivery_id(FirstMsg),
           amqp10_msg:delivery_id(LastMsg),
           true,
           accepted),

    CreditApiV2 = rpc(Config, rabbit_feature_flags, is_enabled, [credit_api_v2]),
    case QueueType =:= <<"quorum">> andalso not CreditApiV2 of
        true ->
            ct:pal("Quorum queues in credit API v1 have a known bug that they "
                   "reply with send_drained before delivering the message.");
        false ->
            %% Send another message and drain.
            Tag = <<"tag">>,
            Body = <<"body">>,
            ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Tag, Body, false)),
            ok = wait_for_settlement(Tag),
            ok = amqp10_client:flow_link_credit(Receiver, 999, never, true),
            [Msg] = receive_messages(Receiver, 1),
            ?assertEqual([Body], amqp10_msg:body(Msg)),
            receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
            after 5000 -> ct:fail("expected credit_exhausted")
            end,
            ok = amqp10_client:accept_msg(Receiver, Msg)
    end,

    ExpectedReadyMsgs = 0,
    ?assertEqual(#'queue.delete_ok'{message_count = ExpectedReadyMsgs},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = amqp10_client:close_connection(Connection).

maintenance(Config) ->
    {ok, C0} = amqp10_client:open_connection(connection_config(0, Config)),
    {ok, C2} = amqp10_client:open_connection(connection_config(2, Config)),
    receive {amqp10_event, {connection, C0, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {amqp10_event, {connection, C2, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = drain_node(Config, 2),
    receive
        {amqp10_event,
         {connection, C2,
          {closed,
           {internal_error, <<"Connection forced: \"Node was put into maintenance mode\"">>}}}} ->
            ok
    after 5000 ->
              flush(?LINE),
              ct:fail({missing_event, ?LINE})
    end,
    ok = revive_node(Config, 2),

    ok = close_connection_sync(C0).

%% rabbitmqctl list_connections
%% should list both AMQP 1.0 and AMQP 0.9.1 connections.
list_connections(Config) ->
    %% Close any open AMQP 0.9.1 connections from previous test cases.
    [ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, Node) || Node <- [0, 1, 2]],

    Connection091 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, C0} = amqp10_client:open_connection(connection_config(0, Config)),
    {ok, C2} = amqp10_client:open_connection(connection_config(2, Config)),
    receive {amqp10_event, {connection, C0, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {amqp10_event, {connection, C2, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, StdOut} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["list_connections", "--silent", "protocol"]),
    Protocols0 = re:split(StdOut, <<"\n">>, [trim]),
    %% Remove any whitespaces.
    Protocols1 = [binary:replace(Subject, <<" ">>, <<>>, [global]) || Subject <- Protocols0],
    Protocols = lists:sort(Protocols1),
    ?assertEqual([<<"{0,9,1}">>,
                  <<"{1,0}">>,
                  <<"{1,0}">>],
                 Protocols),

    ok = rabbit_ct_client_helpers:close_connection(Connection091),
    ok = close_connection_sync(C0),
    ok = close_connection_sync(C2).

global_counters(Config) ->
    #{publishers := 0,
      consumers := 0,
      messages_received_total := Received0,
      messages_received_confirm_total := ReceivedConfirm0,
      messages_confirmed_total := Confirmed0,
      messages_routed_total := Routed0,
      messages_unroutable_dropped_total := UnroutableDropped0,
      messages_unroutable_returned_total := UnroutableReturned0} = get_global_counters(Config),

    #{messages_delivered_total := CQDelivered0,
      messages_redelivered_total := CQRedelivered0,
      messages_acknowledged_total := CQAcknowledged0} = get_global_counters(Config, rabbit_classic_queue),

    #{messages_delivered_total := QQDelivered0,
      messages_redelivered_total := QQRedelivered0,
      messages_acknowledged_total := QQAcknowledged0} = get_global_counters(Config, rabbit_quorum_queue),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    CQ = <<"my classic queue">>,
    QQ = <<"my quorum queue">>,
    CQAddress = <<"/amq/queue/", CQ/binary>>,
    QQAddress = <<"/amq/queue/", QQ/binary>>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = CQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]}),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QQ,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, CQSender} = amqp10_client:attach_sender_link(Session, <<"test-sender-cq">>, CQAddress),
    {ok, QQSender} = amqp10_client:attach_sender_link(Session, <<"test-sender-qq">>, QQAddress),
    ok = wait_for_credit(CQSender),
    ok = wait_for_credit(QQSender),
    {ok, CQReceiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver-cq">>, CQAddress, settled),
    {ok, QQReceiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver-qq">>, QQAddress, unsettled),
    ok = amqp10_client:send_msg(CQSender, amqp10_msg:new(<<0>>, <<"m0">>, true)),
    ok = amqp10_client:send_msg(QQSender, amqp10_msg:new(<<1>>, <<"m1">>, false)),
    ok = wait_for_settlement(<<1>>),

    {ok, Msg0} = amqp10_client:get_msg(CQReceiver),
    ?assertEqual([<<"m0">>], amqp10_msg:body(Msg0)),

    {ok, Msg1} = amqp10_client:get_msg(QQReceiver),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),
    ok = amqp10_client:accept_msg(QQReceiver, Msg1),

    #{publishers := 2,
      consumers := 2,
      messages_received_total := Received1,
      messages_received_confirm_total := ReceivedConfirm1,
      messages_confirmed_total := Confirmed1,
      messages_routed_total := Routed1,
      messages_unroutable_dropped_total := UnroutableDropped1,
      messages_unroutable_returned_total := UnroutableReturned1} = get_global_counters(Config),
    ?assertEqual(Received0 + 2, Received1),
    ?assertEqual(ReceivedConfirm0 + 1, ReceivedConfirm1),
    ?assertEqual(Confirmed0 + 1, Confirmed1),
    ?assertEqual(Routed0 + 2, Routed1),
    ?assertEqual(UnroutableDropped0, UnroutableDropped1),
    ?assertEqual(UnroutableReturned0, UnroutableReturned1),

    #{messages_delivered_total := CQDelivered1,
      messages_redelivered_total := CQRedelivered1,
      messages_acknowledged_total := CQAcknowledged1} = get_global_counters(Config, rabbit_classic_queue),
    ?assertEqual(CQDelivered0 + 1, CQDelivered1),
    ?assertEqual(CQRedelivered0, CQRedelivered1),
    ?assertEqual(CQAcknowledged0, CQAcknowledged1),

    #{messages_delivered_total := QQDelivered1,
      messages_redelivered_total := QQRedelivered1,
      messages_acknowledged_total := QQAcknowledged1} = get_global_counters(Config, rabbit_quorum_queue),
    ?assertEqual(QQDelivered0 + 1, QQDelivered1),
    ?assertEqual(QQRedelivered0, QQRedelivered1),
    ?assertEqual(QQAcknowledged0 + 1, QQAcknowledged1),

    %% Test re-delivery.
    ok = amqp10_client:send_msg(QQSender, amqp10_msg:new(<<2>>, <<"m2">>, false)),
    ok = wait_for_settlement(<<2>>),
    {ok, Msg2a} = amqp10_client:get_msg(QQReceiver),
    ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2a)),
    %% Releasing causes the message to be requeued.
    ok = amqp10_client:settle_msg(QQReceiver, Msg2a, released),
    %% The message should be re-delivered.
    {ok, Msg2b} = amqp10_client:get_msg(QQReceiver),
    ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2b)),
    #{messages_delivered_total := QQDelivered2,
      messages_redelivered_total := QQRedelivered2,
      messages_acknowledged_total := QQAcknowledged2} = get_global_counters(Config, rabbit_quorum_queue),
    %% m2 was delivered 2 times
    ?assertEqual(QQDelivered1 + 2, QQDelivered2),
    %% m2 was re-delivered 1 time
    ?assertEqual(QQRedelivered1 + 1, QQRedelivered2),
    %% Releasing a message shouldn't count as acknowledged.
    ?assertEqual(QQAcknowledged1, QQAcknowledged2),
    ok = amqp10_client:accept_msg(QQReceiver, Msg2b),

    %% Server closes the link endpoint due to some AMQP 1.0 external condition:
    %% In this test, the external condition is that an AMQP 0.9.1 client deletes the queue.
    %% Gauges for publishers and consumers should be decremented.
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QQ}),
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, QQSender, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our sending link")
    end,
    receive {amqp10_event, {link, QQReceiver, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail("server did not close our receiving link")
    end,
    ?assertMatch(#{publishers := 1,
                   consumers := 1},
                 get_global_counters(Config)),

    %% Gauges for publishers and consumers should also be decremented for normal link detachments.
    ok = detach_link_sync(CQSender),
    ok = detach_link_sync(CQReceiver),
    ?assertMatch(#{publishers := 0,
                   consumers := 0},
                 get_global_counters(Config)),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = CQ}),

    flush("testing unroutable..."),
    %% Send 2 messages to the fanout exchange that has no bound queues.
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender-fanout">>, <<"/exchange/amq.fanout/ignored">>),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<3>>, <<"m3">>, true)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<4>>, <<"m4">>, false)),
    ok = wait_for_settlement(<<4>>, released),
    #{messages_unroutable_dropped_total := UnroutableDropped2,
      messages_unroutable_returned_total := UnroutableReturned2} = get_global_counters(Config),
    %% m3 was dropped
    ?assertEqual(UnroutableDropped1 + 1, UnroutableDropped2),
    %% m4 was returned
    ?assertEqual(UnroutableReturned1 + 1, UnroutableReturned2),

    ok = rabbit_ct_client_helpers:close_channel(Ch),
    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection).

stream_filtering(Config) ->
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, ?FUNCTION_NAME),
    Stream = atom_to_binary(?FUNCTION_NAME),
    Address = <<"/amq/queue/", Stream/binary>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:call(Ch, #'queue.declare'{
                             queue = Stream,
                             durable = true,
                             arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),
    wait_for_credit(Sender),

    %% We are going to publish several waves of messages with and without filter values.
    %% We will then create subscriptions with various filter options
    %% and make sure we receive only what we asked for and not all the messages.
    WaveCount = 1000,
    %% logic to publish a wave of messages with or without a filter value
    Publish = fun(FilterValue) ->
                      lists:foreach(fun(Seq) ->
                                            {AppProps, Anns} =
                                            case FilterValue of
                                                undefined ->
                                                    {#{}, #{}};
                                                _ ->
                                                    {#{<<"filter">> => FilterValue},
                                                     #{<<"x-stream-filter-value">> => FilterValue}}
                                            end,
                                            FilterBin = rabbit_data_coercion:to_binary(FilterValue),
                                            SeqBin = integer_to_binary(Seq),
                                            DTag = <<FilterBin/binary, SeqBin/binary>>,
                                            Msg0 = amqp10_msg:new(DTag, <<"my-body">>, false),
                                            Msg1 = amqp10_msg:set_application_properties(AppProps, Msg0),
                                            Msg2 = amqp10_msg:set_message_annotations(Anns, Msg1),
                                            ok = amqp10_client:send_msg(Sender, Msg2),
                                            ok = wait_for_settlement(DTag)
                                    end, lists:seq(1, WaveCount))
              end,

    %% Publish messages with the "apple" filter value.
    Publish(<<"apple">>),
    %% Publish messages with no filter value.
    Publish(undefined),
    %% Publish messages with the "orange" filter value.
    Publish(<<"orange">>),
    ok = amqp10_client:detach_link(Sender),

    % filtering on "apple"
    TerminusDurability = none,
    {ok, AppleReceiver} = amqp10_client:attach_receiver_link(
                            Session,
                            <<"test-receiver-1">>,
                            Address,
                            unsettled,
                            TerminusDurability,
                            #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                              <<"rabbitmq:stream-filter">> => <<"apple">>}),
    ok = amqp10_client:flow_link_credit(AppleReceiver, 100, 10),
    AppleMessages = receive_all_messages(AppleReceiver, true),
    %% we should get less than all the waves combined
    ?assert(length(AppleMessages) < WaveCount * 3),
    %% client-side filtering
    AppleFilteredMessages = lists:filter(fun(Msg) ->
                                                 AP = amqp10_msg:application_properties(Msg),
                                                 maps:get(<<"filter">>, AP) =:= <<"apple">>
                                         end, AppleMessages),
    ?assertEqual(WaveCount, length(AppleFilteredMessages)),
    ok = amqp10_client:detach_link(AppleReceiver),

    %% filtering on "apple" and "orange"
    {ok, AppleOrangeReceiver} = amqp10_client:attach_receiver_link(
                                  Session,
                                  <<"test-receiver-2">>,
                                  Address,
                                  unsettled,
                                  TerminusDurability,
                                  #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                                    <<"rabbitmq:stream-filter">> => [<<"apple">>, <<"orange">>]}),
    ok = amqp10_client:flow_link_credit(AppleOrangeReceiver, 100, 10),
    AppleOrangeMessages = receive_all_messages(AppleOrangeReceiver, true),
    %% we should get less than all the waves combined
    ?assert(length(AppleOrangeMessages) < WaveCount * 3),
    %% client-side filtering
    AppleOrangeFilteredMessages = lists:filter(fun(Msg) ->
                                                       AP = amqp10_msg:application_properties(Msg),
                                                       Filter = maps:get(<<"filter">>, AP),
                                                       Filter =:= <<"apple">> orelse Filter =:= <<"orange">>
                                               end, AppleOrangeMessages),
    ?assertEqual(WaveCount * 2, length(AppleOrangeFilteredMessages)),
    ok = amqp10_client:detach_link(AppleOrangeReceiver),

    %% filtering on "apple" and messages without a filter value
    {ok, AppleUnfilteredReceiver} = amqp10_client:attach_receiver_link(
                                      Session,
                                      <<"test-receiver-3">>,
                                      Address,
                                      unsettled,
                                      TerminusDurability,
                                      #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                                        <<"rabbitmq:stream-filter">> => <<"apple">>,
                                        <<"rabbitmq:stream-match-unfiltered">> => {boolean, true}}),
    ok = amqp10_client:flow_link_credit(AppleUnfilteredReceiver, 100, 10),

    AppleUnfilteredMessages = receive_all_messages(AppleUnfilteredReceiver, true),
    %% we should get less than all the waves combined
    ?assert(length(AppleUnfilteredMessages) < WaveCount * 3),
    %% client-side filtering
    AppleUnfilteredFilteredMessages = lists:filter(fun(Msg) ->
                                                           AP = amqp10_msg:application_properties(Msg),
                                                           not maps:is_key(<<"filter">>, AP) orelse
                                                           maps:get(<<"filter">>, AP) =:= <<"apple">>
                                                   end, AppleUnfilteredMessages),
    ?assertEqual(WaveCount * 2, length(AppleUnfilteredFilteredMessages)),
    ok = amqp10_client:detach_link(AppleUnfilteredReceiver),

    delete_queue(Config, Stream),
    ok = amqp10_client:close_connection(Connection).

available_messages_classic_queue(Config) ->
    available_messages(<<"classic">>, Config).

available_messages_quorum_queue(Config) ->
    available_messages(<<"quorum">>, Config).

available_messages_stream(Config) ->
    available_messages(<<"stream">>, Config).

available_messages(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 1 receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 5000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    ?assertEqual(0, get_available_messages(Receiver)),

    ok = send_messages(Sender, 3, false),
    %% We know that Streams only return an approximation for available messages.
    %% The committed Stream offset is queried by chunk ID.
    %% So, we wait here a bit such that the 4th message goes into its own new chunk.
    timer:sleep(50),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"4">>, <<"4">>, false)),
    ok = wait_for_accepts(4),

    OutputHandle = element(4, Receiver),
    Flow = #'v1_0.flow'{
              %% Grant 1 credit to the sending queue.
              link_credit = {uint, 1},
              %% Request sending queue to send us a FLOW including available messages.
              echo = true},
    ok = amqp10_client_session:flow(Session, OutputHandle, Flow, never),
    receive_messages(Receiver, 1),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    eventually(?_assertEqual(3, get_available_messages(Receiver))),

    %% Send a FLOW with echo=false and drain=false from client to server.
    %% Even if the server doesn't reply with a FLOW, our client lib should
    %% maintain the 'available' variable correctly.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, false),
    receive_messages(Receiver, 1),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    ?assertEqual(2, get_available_messages(Receiver)),

    %% We force the queue to send us a FLOW including available messages
    %% by granting more credits than messages being available and drain=true.
    ok = amqp10_client:flow_link_credit(Receiver, 99, never, true),
    receive_messages(Receiver, 2),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    ?assertEqual(0, get_available_messages(Receiver)),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

incoming_message_interceptors(Config) ->
    Key = {rabbit, ?FUNCTION_NAME},
    ok = rpc(Config, persistent_term, put, [Key, [{set_header_routing_node, false}]]),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = <<"/queue/", QName/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver">>, Address),
    Address = <<"/queue/", QName/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address, settled),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag">>, <<"body">>, true)),

    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"body">>], amqp10_msg:body(Msg)),
    Node = atom_to_binary(get_node_config(Config, 0, nodename)),
    ?assertMatch(#{<<"x-routed-by">> := Node},
                 amqp10_msg:message_annotations(Msg)),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    delete_queue(Config, QName),
    true = rpc(Config, persistent_term, erase, [Key]).

trace(Config) ->
    Node = atom_to_binary(get_node_config(Config, 0, nodename)),
    TraceQ = <<"my trace queue">>,
    Q = <<"my queue">>,
    Qs = [Q, TraceQ],
    RoutingKey = <<"my routing key">>,
    Payload = <<"my payload">>,
    CorrelationId = <<"my correlation 👀"/utf8>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    [#'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = Q0}) || Q0 <- Qs],
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue       = TraceQ,
                                             exchange    = <<"amq.rabbitmq.trace">>,
                                             routing_key = <<"#">>}),
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue       = Q,
                                             exchange    = <<"amq.direct">>,
                                             routing_key = RoutingKey}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),

    %% We expect traced messages for sessions created before and
    %% sessions created after tracing is enabled.
    {ok, SessionSender} = amqp10_client:begin_session_sync(Connection),
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_on"]),
    {ok, SessionReceiver} = amqp10_client:begin_session_sync(Connection),

    {ok, Sender} = amqp10_client:attach_sender_link(SessionSender,
                                                    <<"test-sender">>,
                                                    <<"/exchange/amq.direct/", RoutingKey/binary>>),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(SessionReceiver,
                                                        <<"test-receiver">>,
                                                        <<"/amq/queue/", Q/binary>>),
    Msg0 = amqp10_msg:new(<<"tag 1">>, Payload, true),
    Msg = amqp10_msg:set_properties(#{correlation_id => CorrelationId}, Msg0),
    ok = amqp10_client:send_msg(Sender, Msg),
    {ok, _} = amqp10_client:get_msg(Receiver),

    timer:sleep(20),
    {#'basic.get_ok'{routing_key = <<"publish.amq.direct">>},
     #amqp_msg{props = #'P_basic'{headers = PublishHeaders},
               payload = Payload}} =
    amqp_channel:call(Ch, #'basic.get'{queue = TraceQ}),
    ?assertMatch(#{<<"exchange_name">> := <<"amq.direct">>,
                   <<"routing_keys">> := [RoutingKey],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Node,
                   <<"vhost">> := <<"/">>,
                   <<"channel">> := 1,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"correlation_id">> := CorrelationId},
                   <<"routed_queues">> := [Q]},
                 rabbit_misc:amqp_table(PublishHeaders)),

    {#'basic.get_ok'{routing_key = <<"deliver.", Q/binary>>},
     #amqp_msg{props = #'P_basic'{headers = DeliverHeaders},
               payload = Payload}} =
    amqp_channel:call(Ch, #'basic.get'{queue = TraceQ}),
    ?assertMatch(#{<<"exchange_name">> := <<"amq.direct">>,
                   <<"routing_keys">> := [RoutingKey],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Node,
                   <<"vhost">> := <<"/">>,
                   <<"channel">> := 2,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"correlation_id">> := CorrelationId},
                   <<"redelivered">> := 0},
                 rabbit_misc:amqp_table(DeliverHeaders)),

    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_off"]),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag 2">>, Payload, true)),
    {ok, _} = amqp10_client:get_msg(Receiver),
    timer:sleep(20),
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = TraceQ})),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(SessionSender),
    ok = end_session_sync(SessionReceiver),
    ok = amqp10_client:close_connection(Connection),
    [delete_queue(Config, Q0) || Q0 <- Qs].

%% https://www.rabbitmq.com/validated-user-id.html
user_id(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/amq.direct/some-routing-key">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    flush(attached),

    Msg1 = amqp10_msg:set_properties(#{user_id => <<"guest">>},
                                     amqp10_msg:new(<<"t1">>, <<"m1">>, true)),
    ok = amqp10_client:send_msg(Sender, Msg1),
    receive Unexpected -> ct:fail({received_unexpected_message, Unexpected})
    after 10 -> ok
    end,

    Msg2 = amqp10_msg:set_properties(#{user_id => <<"fake user">>},
                                     amqp10_msg:new(<<"t2">>, <<"m2">>, true)),
    ok = amqp10_client:send_msg(Sender, Msg2),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
              description = {utf8, <<"user_id property set to 'fake user' but authenticated user was 'guest'">>}}}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive expected error")
    end,

    ok = amqp10_client:close_connection(Connection).

message_ttl(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = <<"/amq/queue/", QName/binary>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver">>, Address),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    M1 = amqp10_msg:set_headers(#{ttl => 1}, amqp10_msg:new(<<"t1">>, <<"m1">>, false)),
    M2 = amqp10_msg:set_headers(#{ttl => 60 * 1000}, amqp10_msg:new(<<"t2">>, <<"m2">>, false)),
    ok = amqp10_client:send_msg(Sender, M1),
    ok = amqp10_client:send_msg(Sender, M2),
    ok = wait_for_accepts(2),
    %% Wait for the first message to expire.
    timer:sleep(50),
    flush(pre_receive),
    ok = amqp10_client:flow_link_credit(Receiver, 2, never, true),
    receive {amqp10_msg, Receiver, Msg} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(Msg))
    after 5000 -> ct:fail(delivery_timeout)
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive Unexpected -> ct:fail({received_unexpected_message, Unexpected})
    after 5 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:close_connection(Connection),
    ok = delete_queue(Config, QName).

%% For backward compatibility, deployment tools should be able to
%% enable and disable the deprecated no-op AMQP 1.0 plugin.
plugin(Config) ->
    Node = 0,
    Plugin = rabbitmq_amqp1_0,
    %% rabbit/Makefile and rabbit/BUILD.bazel declare a test dependency on the rabbitmq_amqp1_0 plugin.
    %% Therefore, we first disable, and then enable.
    ?assertEqual(ok, rabbit_ct_broker_helpers:disable_plugin(Config, Node, Plugin)),
    ?assertEqual(ok, rabbit_ct_broker_helpers:enable_plugin(Config, Node, Plugin)).

%% Test that the idle timeout threshold is exceeded on the server
%% when no frames are sent from client to server.
idle_time_out_on_server(Config) ->
    App = rabbit,
    Par = heartbeat,
    {ok, DefaultVal} = rpc(Config, application, get_env, [App, Par]),
    %% Configure RabbitMQ to use an idle-time-out of 1 second.
    ok = rpc(Config, application, set_env, [App, Par, 1]),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Mock the server socket to not have received any bytes.
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_net,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, getstat, 2, {ok, [{recv_oct, 999}]}]),
    %% The server "SHOULD try to gracefully close the connection using a close
    %% frame with an error explaining why" [2.4.5].
    %% Since we chose a heartbeat value of 1 second, the server should easily
    %% close the connection within 5 seconds.
    receive
        {amqp10_event,
         {connection, Connection,
          {closed,
           {resource_limit_exceeded,
            <<"no frame received from client within idle timeout threshold">>}}}} -> ok
    after 5000 ->
              ct:fail({missing_event, ?LINE})
    end,

    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = rpc(Config, application, set_env, [App, Par, DefaultVal]).

%% Test that the idle timeout threshold is exceeded on the client
%% when no frames are sent from server to client.
idle_time_out_on_client(Config) ->
    OpnConf0 = connection_config(Config),
    %% Request the server to send us frames every second.
    OpnConf = OpnConf0#{idle_time_out => 1000},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    receive Unexpected -> ct:fail({unexpected, Unexpected})
    after 3100 -> ok
    end,
    ?assert(is_process_alive(Connection)),
    %% All good, the server sent us frames every second.

    %% Mock the server to not send anything.
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_net,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, send, 2, ok]),

    %% Our client should time out within less than 5 seconds given that the
    %% idle-time-out is 1 second.
    receive
        {amqp10_event,
         {connection, Connection,
          {closed,
           {resource_limit_exceeded,
            <<"remote idle-time-out">>}}}} -> ok
    after 5000 ->
              ct:fail({missing_event, ?LINE})
    end,

    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]).

%% Test that RabbitMQ does not support idle timeouts smaller than 1 second.
idle_time_out_too_short(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{idle_time_out => 900},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, _}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end.

rabbit_status_connection_count(Config) ->
    %% Close any open AMQP 0.9.1 connections from previous test cases.
    ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, String} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["status"]),
    ?assertNotEqual(nomatch, string:find(String, "Connection count: 1")),

    ok = amqp10_client:close_connection(Connection).

handshake_timeout(Config) ->
    App = rabbit,
    Par = ?FUNCTION_NAME,
    {ok, DefaultVal} = rpc(Config, application, get_env, [App, Par]),
    ok = rpc(Config, application, set_env, [App, Par, 200]),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Socket} = gen_tcp:connect("localhost", Port, [{active, false}]),
    ?assertEqual({error, closed}, gen_tcp:recv(Socket, 0, 400)),
    ok = rpc(Config, application, set_env, [App, Par, DefaultVal]).

credential_expires(Config) ->
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_auth_backend_internal,
    ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
    ExpiryTimestamp = os:system_time(second) + 3,
    ok = rpc(Config, meck, expect, [Mod, expiry_timestamp, 1, ExpiryTimestamp]),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 2000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Since we don't renew our credential, we expect the server to close our connection.
    receive
        {amqp10_event,
         {connection, Connection,
          {closed,
           {unauthorized_access, <<"credential expired">>}}}} -> ok
    after 10_000 ->
              flush(?LINE),
              ct:fail({missing_event, ?LINE})
    end,

    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]).

%% Attaching to an exclusive source queue should fail.
attach_to_exclusive_queue(Config) ->
    QName = <<"my queue">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = QName,
                                                   durable = true,
                                                   exclusive = true}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/amq/queue/", QName/binary>>,
    {ok, _Receiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver">>, Address),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
              description = {utf8, <<"cannot obtain exclusive access to locked "
                                     "queue 'my queue' in vhost '/'">>}}}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:close_connection(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

classic_priority_queue(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = <<"/amq/queue/", QName/binary>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-max-priority">>, long, 10}]}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    Out1 = amqp10_msg:set_headers(#{priority => 3,
                                    durable => true}, amqp10_msg:new(<<"t1">>, <<"low prio">>, false)),
    Out2 = amqp10_msg:set_headers(#{priority => 5,
                                    durable => true}, amqp10_msg:new(<<"t2">>, <<"high prio">>, false)),
    ok = amqp10_client:send_msg(Sender, Out1),
    ok = amqp10_client:send_msg(Sender, Out2),
    ok = wait_for_accepts(2),
    flush(accepted),

    %% The high prio message should be delivered first.
    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session, <<"receiver 1">>, Address, unsettled),
    {ok, In1} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"high prio">>], amqp10_msg:body(In1)),
    ?assertEqual(5, amqp10_msg:header(priority, In1)),
    ?assert(amqp10_msg:header(durable, In1)),
    ok = amqp10_client:accept_msg(Receiver1, In1),

    {ok, Receiver2} = amqp10_client:attach_receiver_link(Session, <<"receiver 2">>, Address, settled),
    {ok, In2} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"low prio">>], amqp10_msg:body(In2)),
    ?assertEqual(3, amqp10_msg:header(priority, In2)),
    ?assert(amqp10_msg:header(durable, In2)),

    ok = amqp10_client:detach_link(Receiver1),
    ok = amqp10_client:detach_link(Receiver2),
    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = amqp10_client:close_connection(Connection),
    ok = delete_queue(Config, QName).

%% internal
%%

receive_all_messages(Receiver, Accept) ->
    receive_all_messages0(Receiver, Accept, []).

receive_all_messages0(Receiver, Accept, Acc) ->
    receive {amqp10_msg, Receiver, Msg} ->
                case Accept of
                    true -> ok = amqp10_client:accept_msg(Receiver, Msg);
                    false -> ok
                end,
                receive_all_messages0(Receiver, Accept, [Msg | Acc])
    after 1000 ->
              lists:reverse(Acc)
    end.

connection_config(Config) ->
    connection_config(0, Config).

connection_config(Node, Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>}}.

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~p flushed: ~p~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

open_and_close_connection(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> ct:fail(opened_timeout)
    end,
    ok = close_connection_sync(Connection).

% before we can send messages we have to wait for credit from the server
wait_for_credit(Sender) ->
    receive
        {amqp10_event, {link, Sender, credited}} ->
            ok
    after 5000 ->
              flush("wait_for_credit timed out"),
              ct:fail(credited_timeout)
    end.

detach_link_sync(Link) ->
    ok = amqp10_client:detach_link(Link),
    ok = wait_for_link_detach(Link).

wait_for_link_detach(Link) ->
    receive
        {amqp10_event, {link, Link, {detached, normal}}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_link_detach timed out"),
              ct:fail({link_detach_timeout, Link})
    end.

end_session_sync(Session) ->
    ok = amqp10_client:end_session(Session),
    ok = wait_for_session_end(Session).

wait_for_session_end(Session) ->
    receive
        {amqp10_event, {session, Session, {ended, _}}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_session_end timed out"),
              ct:fail({session_end_timeout, Session})
    end.

close_connection_sync(Connection) ->
    ok = amqp10_client:close_connection(Connection),
    ok = wait_for_connection_close(Connection).

wait_for_connection_close(Connection) ->
    receive
        {amqp10_event, {connection, Connection, {closed, normal}}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_connection_close timed out"),
              ct:fail({connection_close_timeout, Connection})
    end.

wait_for_settlement(Tag) ->
    wait_for_settlement(Tag, accepted).

wait_for_settlement(Tag, State) ->
    receive
        {amqp10_disposition, {State, Tag}} ->
            ok
    after 5000 ->
              flush("wait_for_settlement timed out"),
              ct:fail({settled_timeout, Tag})
    end.

wait_for_accepts(0) ->
    ok;
wait_for_accepts(N) ->
    receive
        {amqp10_disposition,{accepted,_}} ->
            wait_for_accepts(N - 1)
    after 5000 ->
              ct:fail({missing_accepted, N})
    end.

delete_queue(Config, QName) -> 
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

amqp091_get_msg_headers(Channel, QName) -> 
    {#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{ headers= Headers}}}
        = amqp_channel:call(Channel, #'basic.get'{queue = QName, no_ack = true}),
    {ok, Headers}.

create_amqp10_sender(Session, Address) -> 
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),
    {ok, Sender}.

drain_queue(Session, Address, N) ->
    flush("Before drain_queue"),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       settled,
                       configuration),
    ok = amqp10_client:flow_link_credit(Receiver, 1000, never, true),
    Msgs = receive_messages(Receiver, N),
    flush("after drain"),
    ok = amqp10_client:detach_link(Receiver),
    {ok, Msgs}.

receive_messages(Receiver, N) ->
    receive_messages0(Receiver, N, []).

receive_messages0(_Receiver, 0, Acc) ->
    lists:reverse(Acc);
receive_messages0(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} -> 
            receive_messages0(Receiver, N - 1, [Msg | Acc])
    after 5000  ->
              ct:fail({timeout, {num_received, length(Acc)}, {num_missing, N}})
    end.

count_received_messages(Receiver) ->
    count_received_messages0(Receiver, 0).

count_received_messages0(Receiver, Count) ->
    receive
        {amqp10_msg, Receiver, _Msg} ->
            count_received_messages0(Receiver, Count + 1)
    after 200 ->
              Count
    end.

send_messages(_Sender, 0, _Settled) ->
    ok;
send_messages(Sender, Left, Settled) ->
    Bin = integer_to_binary(Left),
    Msg = amqp10_msg:new(Bin, Bin, Settled),
    case amqp10_client:send_msg(Sender, Msg) of
        ok ->
            send_messages(Sender, Left - 1, Settled);
        {error, insufficient_credit} ->
            ok = wait_for_credit(Sender),
            %% The credited event we just processed could have been received some time ago,
            %% i.e. we might have 0 credits right now. This happens in the following scenario:
            %% 1. We (test case proc) send a message successfully, the client session proc decrements remaining link credit from 1 to 0.
            %% 2. The server grants our client session proc new credits.
            %% 3. The client session proc sends us (test case proc) a credited event.
            %% 4. We didn't even notice that we ran out of credits temporarily. We send the next message, it succeeds,
            %%    but do not process the credited event in our mailbox.
            %% So, we must be defensive here and assume that the next amqp10_client:send/2 call might return {error, insufficient_credit}
            %% again causing us then to really wait to receive a credited event (instead of just processing an old credited event).
            send_messages(Sender, Left, Settled)
    end.

assert_link_credit_runs_out(_Sender, 0) ->
    ct:fail(sufficient_link_credit);
assert_link_credit_runs_out(Sender, Left) ->
    Bin = integer_to_binary(Left),
    Msg = amqp10_msg:new(Bin, Bin, true),
    case amqp10_client:send_msg(Sender, Msg) of
        ok ->
            assert_link_credit_runs_out(Sender, Left - 1);
        {error, insufficient_credit} ->
            receive {amqp10_event, {link, Sender, credited}} ->
                        ct:pal("credited with ~b messages left", [Left]),
                        assert_link_credit_runs_out(Sender, Left - 1)
            after 500 ->
                      ct:pal("insufficient link credit with ~b messages left", [Left]),
                      ok
            end
    end.

send_messages_with_group_id(Sender, N, GroupId) ->
    [begin
         Bin = integer_to_binary(I),
         Msg0 = amqp10_msg:new(Bin, Bin, true),
         Props = #{group_id => GroupId},
         Msg = amqp10_msg:set_properties(Props, Msg0),
         ok = amqp10_client:send_msg(Sender, Msg)
     end || I <- lists:seq(1, N)].

assert_messages(QNameBin, NumTotalMsgs, NumUnackedMsgs, Config) ->
    Vhost = ?config(rmq_vhost, Config),
    eventually(
      ?_assertEqual(
         lists:sort([{messages, NumTotalMsgs}, {messages_unacknowledged, NumUnackedMsgs}]),
         begin
             {ok, Q} = rpc(Config, rabbit_amqqueue, lookup, [QNameBin, Vhost]),
             Infos = rpc(Config, rabbit_amqqueue, info, [Q, [messages, messages_unacknowledged]]),
             lists:sort(Infos)
         end
        ), 500, 5).

serial_number_increment(S) ->
    case S + 1 of
        16#ffffffff + 1 -> 0;
        S1 -> S1
    end.

consume_from_first(<<"stream">>) ->
    #{<<"rabbitmq:stream-offset-spec">> => <<"first">>};
consume_from_first(_) ->
    #{}.

%% Return the formatted state of a gen_server via sys:get_status/1.
%% (sys:get_state/1 is unformatted)
gen_server_state(Pid) ->
    {status, _, _, L0} = sys:get_status(Pid, 20_000),
    L1 = lists:last(L0),
    {data, L2} = lists:last(L1),
    proplists:get_value("State", L2).

get_global_counters(Config) ->
    get_global_counters0(Config, [{protocol, amqp10}]).

get_global_counters(Config, QType) ->
    get_global_counters0(Config, [{protocol, amqp10},
                                  {queue_type, QType}]).

get_global_counters0(Config, Key) ->
    Overview = rpc(Config, rabbit_global_counters, overview, []),
    maps:get(Key, Overview).

get_available_messages({link_ref, receiver, Session, OutputHandle}) ->
    {status, _Pid, _Mod, [_, _, _, _, Misc]} = sys:get_status(Session),
    [State] = [S || {data, [{"State", S}]} <- Misc],
    {_StateName, StateData} = State,
    {ok, Links} = maps:find(links, StateData),
    {ok, Link} = maps:find(OutputHandle, Links),
    {ok, Available} = maps:find(available, Link),
    Available.

ra_name(Q) ->
    binary_to_atom(<<"%2F_", Q/binary>>).

has_local_member(QName) ->
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            #{name := StreamId} = amqqueue:get_type_state(Q),
            case rabbit_stream_coordinator:local_pid(StreamId) of
                {ok, Pid} ->
                    is_process_alive(Pid);
                {error, _} ->
                    false
            end;
        {error, _} ->
            false
    end.
