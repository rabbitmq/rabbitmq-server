%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqp_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

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
-import(amqp_utils,
        [init/1, init/2,
         close/1,
         connection_config/1, connection_config/2,
         web_amqp/1,
         flush/1,
         wait_for_credit/1,
         wait_for_accepts/1,
         send_messages/3, send_messages/4,
         detach_link_sync/1,
         end_session_sync/1,
         wait_for_session_end/1,
         close_connection_sync/1]).

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
       durable_field_classic_queue,
       durable_field_quorum_queue,
       durable_field_stream,
       invalid_transfer_settled_flag,
       quorum_queue_rejects,
       receiver_settle_mode_first,
       publishing_to_non_existing_queue_should_settle_with_released,
       open_link_to_non_existing_destination_should_end_session,
       roundtrip_with_drain_classic_queue,
       roundtrip_with_drain_quorum_queue,
       roundtrip_with_drain_stream,
       drain_many_classic_queue,
       drain_many_quorum_queue,
       drain_many_stream,
       amqp_stream_amqpl,
       amqp_quorum_queue_amqpl,
       message_headers_conversion,
       multiple_sessions,
       server_closes_link_classic_queue,
       server_closes_link_quorum_queue,
       server_closes_link_stream,
       server_closes_link_exchange_settled,
       server_closes_link_exchange_unsettled,
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
       priority_classic_queue,
       priority_quorum_queue,
       consumer_priority_classic_queue,
       consumer_priority_quorum_queue,
       single_active_consumer_classic_queue,
       single_active_consumer_quorum_queue,
       single_active_consumer_priority_quorum_queue,
       single_active_consumer_drain_classic_queue,
       single_active_consumer_drain_quorum_queue,
       detach_requeues_one_session_classic_queue,
       detach_requeues_one_session_quorum_queue,
       detach_requeues_drop_head_classic_queue,
       resource_alarm_before_session_begin,
       resource_alarm_after_session_begin,
       resource_alarm_send_many,
       max_message_size_client_to_server,
       max_message_size_server_to_client,
       global_counters,
       stream_bloom_filter,
       available_messages_classic_queue,
       available_messages_quorum_queue,
       available_messages_stream,
       message_interceptors,
       trace_classic_queue,
       trace_stream,
       user_id,
       message_ttl,
       plugin,
       idle_time_out_on_server,
       idle_time_out_on_client,
       idle_time_out_too_short,
       handshake_timeout,
       credential_expires,
       attach_to_exclusive_queue,
       dynamic_target_short_link_name,
       dynamic_target_long_link_name,
       dynamic_source_rpc,
       dynamic_terminus_delete,
       modified_classic_queue,
       modified_quorum_queue,
       modified_dead_letter_headers_exchange,
       modified_dead_letter_history,
       dead_letter_headers_exchange,
       dead_letter_reject,
       dead_letter_reject_message_order_classic_queue,
       dead_letter_reject_message_order_quorum_queue,
       dead_letter_reject_many_message_order_classic_queue,
       dead_letter_reject_many_message_order_quorum_queue,
       accept_multiple_message_order_classic_queue,
       accept_multiple_message_order_quorum_queue,
       release_multiple_message_order_classic_queue,
       release_multiple_message_order_quorum_queue,
       immutable_bare_message,
       receive_many_made_available_over_time_classic_queue,
       receive_many_made_available_over_time_quorum_queue,
       receive_many_made_available_over_time_stream,
       receive_many_auto_flow_classic_queue,
       receive_many_auto_flow_quorum_queue,
       receive_many_auto_flow_stream,
       incoming_window_closed_transfer_flow_order,
       incoming_window_closed_stop_link,
       incoming_window_closed_close_link,
       incoming_window_closed_rabbitmq_internal_flow_classic_queue,
       incoming_window_closed_rabbitmq_internal_flow_quorum_queue,
       tcp_back_pressure_rabbitmq_internal_flow_classic_queue,
       tcp_back_pressure_rabbitmq_internal_flow_quorum_queue,
       session_flow_control_default_max_frame_size,
       session_flow_control_small_max_frame_size,
       session_max_per_connection,
       link_max_per_session,
       reserved_annotation,
       x_cc_annotation_exchange,
       x_cc_annotation_exchange_routing_key_empty,
       x_cc_annotation_queue,
       x_cc_annotation_null,
       bad_x_cc_annotation_exchange,
       decimal_types
      ]},

     {cluster_size_3, [shuffle],
      [
       dead_letter_into_stream,
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
       leader_transfer_quorum_queue_credit_single,
       leader_transfer_quorum_queue_credit_batches,
       leader_transfer_stream_credit_single,
       leader_transfer_stream_credit_batches,
       leader_transfer_quorum_queue_send,
       leader_transfer_stream_send,
       list_connections,
       detach_requeues_two_connections_classic_queue,
       detach_requeues_two_connections_quorum_queue,
       attach_to_down_quorum_queue
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
    rabbit_ct_helpers:merge_app_env(
      Config, {rabbit, [{quorum_tick_interval, 1000},
                        {stream_tick_interval, 1000}
                       ]}).

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
       T =:= drain_many_quorum_queue orelse
       T =:= timed_get_quorum_queue orelse
       T =:= available_messages_quorum_queue ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        _ ->
            {skip, "Receiving with drain from quorum queues in credit API v1 have a known "
             "bug that they reply with send_drained before delivering the message."}
    end;
init_per_testcase(single_active_consumer_drain_quorum_queue = T, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        _ ->
            {skip, "Draining a SAC inactive quorum queue consumer with credit API v1 "
             "is known to be unsupported."}
    end;
init_per_testcase(T, Config)
  when T =:= incoming_window_closed_close_link orelse
       T =:= incoming_window_closed_rabbitmq_internal_flow_classic_queue orelse
       T =:= incoming_window_closed_rabbitmq_internal_flow_quorum_queue orelse
       T =:= tcp_back_pressure_rabbitmq_internal_flow_classic_queue orelse
       T =:= tcp_back_pressure_rabbitmq_internal_flow_quorum_queue ->
    %% The new RabbitMQ internal flow control
    %% writer proc <- session proc <- queue proc
    %% is only available with credit API v2.
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        _ ->
            {skip, "Feature flag rabbitmq_4.0.0 is disabled"}
    end;
init_per_testcase(T, Config)
  when T =:= modified_quorum_queue orelse
       T =:= modified_dead_letter_headers_exchange orelse
       T =:= modified_dead_letter_history ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        _ ->
            {skip, "Feature flag rabbitmq_4.0.0 is disabled, but needed for "
             "the new #modify{} command being sent to quorum queues."}
    end;
init_per_testcase(T, Config)
  when T =:= detach_requeues_one_session_classic_queue orelse
       T =:= detach_requeues_drop_head_classic_queue orelse
       T =:= detach_requeues_two_connections_classic_queue orelse
       T =:= single_active_consumer_classic_queue ->
    %% Cancel API v2 reuses feature flag rabbitmq_4.0.0.
    %% In 3.13, with cancel API v1, when a receiver detaches with unacked messages, these messages
    %% will remain unacked and unacked message state will be left behind in the server session
    %% process state.
    %% In contrast, cancel API v2 in 4.x will requeue any unacked messages if the receiver detaches.
    %% We skip the single active consumer tests because these test cases assume that detaching a
    %% receiver link will requeue unacked messages.
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        _ ->
            {skip, "Cancel API v2 is disabled due to feature flag rabbitmq_4.0.0 being disabled."}
    end;
init_per_testcase(T, Config)
  when T =:= detach_requeues_one_session_quorum_queue orelse
       T =:= single_active_consumer_quorum_queue orelse
       T =:= detach_requeues_two_connections_quorum_queue ->
    %% Feature flag rabbitmq_4.0.0 enables the consumer removal API.
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        Skip ->
            Skip
    end;
init_per_testcase(T, Config)
  when T =:= leader_transfer_quorum_queue_credit_single orelse
       T =:= leader_transfer_quorum_queue_credit_batches orelse
       T =:= async_notify_unsettled_classic_queue orelse
       T =:= leader_transfer_stream_credit_single orelse
       T =:= dead_letter_into_stream orelse
       T =:= classic_queue_on_new_node orelse
       T =:= leader_transfer_quorum_queue_send orelse
       T =:= last_queue_confirms orelse
       T =:= leader_transfer_stream_credit_batches orelse
       T =:= leader_transfer_stream_send ->
    %% These test cases flake with feature flag 'rabbitmq_4.0.0' disabled.
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        Skip ->
            Skip
    end;
init_per_testcase(T = immutable_bare_message, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        _ ->
            {skip, "RabbitMQ is known to wrongfully modify the bare message with feature "
             "flag rabbitmq_4.0.0 disabled"}
    end;
init_per_testcase(T = dead_letter_reject, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, message_containers_deaths_v2) of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        _ ->
            {skip, "This test is known to fail with feature flag message_containers_deaths_v2 disabled "
             "due bug https://github.com/rabbitmq/rabbitmq-server/issues/11159"}
    end;
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Clean up any queues, connections, and sessions.
    rpc(Config, ?MODULE, delete_queues, []),
    ok = rpc(Config, rabbit_networking, close_all_connections, [<<"test finished">>]),
    eventually(?_assertEqual([], rpc(Config, rabbit_amqp_session, list_local, []))),
    %% Assert that global counters count correctly.
    eventually(?_assertMatch(#{publishers := 0,
                               consumers := 0},
                             get_global_counters(Config))),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

reliable_send_receive_with_outcomes_classic_queue(Config) ->
    reliable_send_receive_with_outcomes(<<"classic">>, Config).

reliable_send_receive_with_outcomes_quorum_queue(Config) ->
    reliable_send_receive_with_outcomes(<<"quorum">>, Config).

reliable_send_receive_with_outcomes(QType, Config) ->
    Outcomes = [
                accepted,
                modified,
                {modified, true, false, #{<<"x-fruit">> => <<"banana">>}},
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

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"pair">>),
    QName = <<QType/binary, OutcomeBin/binary>>,
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),

    %% reliable send and consume
    Address = rabbitmq_amqp_address:queue(QName),
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
    ok = wait_for_accepted(DTag1),

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
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
    ok = delete_queue(Session2, QName),
    ok = end_session_sync(Session2),
    ok = close_connection_sync(Connection2).

%% We test the modified outcome with classic queues.
%% We expect that classic queues implement field undeliverable-here incorrectly
%% by discarding (if true) or requeueing (if false).
%% Fields delivery-failed and message-annotations are not implemented.
modified_classic_queue(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{type := <<"classic">>}} = rabbitmq_amqp_client:declare_queue(
                                       LinkPair, QName,
                                       #{arguments => #{<<"x-queue-type">> => {utf8, <<"classic">>}}}),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    Msg1 = amqp10_msg:new(<<"tag1">>, <<"m1">>, true),
    Msg2 = amqp10_msg:new(<<"tag2">>, <<"m2">>, true),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = amqp10_client:detach_link(Sender),

    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address, unsettled),

    {ok, M1} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m1">>], amqp10_msg:body(M1)),
    ok = amqp10_client:settle_msg(Receiver, M1, {modified, false, true, #{}}),

    {ok, M2a} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2a)),
    ok = amqp10_client:settle_msg(Receiver, M2a,
                                  {modified, false, false, #{}}),

    {ok, M2b} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2b)),
    ok = amqp10_client:settle_msg(Receiver, M2b,
                                  {modified, true, false, #{<<"x-opt-key">> => <<"val">>}}),

    {ok, M2c} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2c)),
    ok = amqp10_client:settle_msg(Receiver, M2c, modified),

    ok = amqp10_client:detach_link(Receiver),
    ?assertMatch({ok, #{message_count := 1}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

%% We test the modified outcome with quorum queues.
%% We expect that quorum queues implement field
%% * delivery-failed correctly
%% * undeliverable-here incorrectly by discarding (if true) or requeueing (if false)
%% * message-annotations correctly
modified_quorum_queue(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{type := <<"quorum">>}} = rabbitmq_amqp_client:declare_queue(
                                      LinkPair, QName,
                                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}}}),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    Msg1 = amqp10_msg:new(<<"tag1">>, <<"m1">>, true),
    Msg2 = amqp10_msg:new(<<"tag2">>, <<"m2">>, true),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = amqp10_client:detach_link(Sender),

    Receiver1Name = <<"receiver 1">>,
    Receiver2Name = <<"receiver 2">>,
    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session, Receiver1Name, Address, unsettled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(Session, Receiver2Name, Address, unsettled),

    {ok, M1} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m1">>], amqp10_msg:body(M1)),
    ?assertMatch(#{delivery_count := 0,
                   first_acquirer := true},
                 amqp10_msg:headers(M1)),
    ok = amqp10_client:settle_msg(Receiver1, M1, {modified, false, true, #{}}),

    {ok, M2a} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2a)),
    ?assertMatch(#{delivery_count := 0,
                   first_acquirer := true},
                 amqp10_msg:headers(M2a)),
    ok = amqp10_client:settle_msg(Receiver1, M2a, {modified, false, false, #{}}),

    {ok, M2b} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2b)),
    ?assertMatch(#{delivery_count := 0,
                   first_acquirer := false},
                 amqp10_msg:headers(M2b)),
    ok = amqp10_client:settle_msg(Receiver1, M2b, {modified, true, false, #{}}),

    {ok, M2c} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2c)),
    ?assertMatch(#{delivery_count := 1,
                   first_acquirer := false},
                 amqp10_msg:headers(M2c)),
    ok = amqp10_client:settle_msg(
           Receiver1, M2c,
           {modified, true, false,
            %% Test that a history of requeue events can be tracked as described in
            %% https://rabbitmq.com/blog/2024/10/11/modified-outcome
            #{<<"x-opt-requeued-by">> => {array, utf8, [{utf8, Receiver1Name}]},
              <<"x-opt-requeue-reason">> => {list, [{utf8, <<"reason 1">>}]},
              <<"x-opt-my-map">> => {map, [
                                           {{utf8, <<"k1">>}, {byte, -1}},
                                           {{utf8, <<"k2">>}, {ulong, 2}}
                                          ]}}}),

    {ok, M2d} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2d)),
    ?assertMatch(#{delivery_count := 2,
                   first_acquirer := false},
                 amqp10_msg:headers(M2d)),
    #{<<"x-opt-requeued-by">> := {array, utf8, L0},
      <<"x-opt-requeue-reason">> := L1,
      <<"x-opt-my-map">> := L2} = amqp10_msg:message_annotations(M2d),
    ok = amqp10_client:settle_msg(
           Receiver1, M2d,
           {modified, false, false,
            #{<<"x-opt-requeued-by">> => {array, utf8, [{utf8, Receiver2Name} | L0]},
              <<"x-opt-requeue-reason">> => {list, [{symbol, <<"reason 2">>} | L1]},
              <<"x-opt-my-map">> => {map, L2 ++ [{{symbol, <<"k3">>}, {symbol, <<"val 3">>}}]},
              <<"x-other">> => 99}}),

    {ok, M2e} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2e)),
    ?assertMatch(#{delivery_count := 2,
                   first_acquirer := false},
                 amqp10_msg:headers(M2e)),
    ?assertMatch(#{<<"x-opt-requeued-by">> := {array, utf8, [{utf8, Receiver2Name}, {utf8, Receiver1Name}]},
                   <<"x-opt-requeue-reason">> := [{symbol, <<"reason 2">>}, {utf8, <<"reason 1">>}],
                   <<"x-opt-my-map">> := [
                                          {{utf8, <<"k1">>}, {byte, -1}},
                                          {{utf8, <<"k2">>}, {ulong, 2}},
                                          {{symbol, <<"k3">>}, {symbol, <<"val 3">>}}
                                         ],
                   <<"x-other">> := 99}, amqp10_msg:message_annotations(M2e)),
    ok = amqp10_client:settle_msg(Receiver1, M2e, modified),

    %% Test that we can consume via AMQP 0.9.1
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    {#'basic.get_ok'{},
     #amqp_msg{payload = <<"m2">>,
               props = #'P_basic'{headers = Headers}}
    } = amqp_channel:call(Ch, #'basic.get'{queue = QName, no_ack = true}),
    %% We expect to receive only modified AMQP 1.0 message annotations that are of simple types
    %% (i.e. excluding list, map, array).
    ?assertEqual({value, {<<"x-other">>, long, 99}},
                 lists:keysearch(<<"x-other">>, 1, Headers)),
    ?assertEqual({value, {<<"x-delivery-count">>, long, 5}},
                 lists:keysearch(<<"x-delivery-count">>, 1, Headers)),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    ok = amqp10_client:detach_link(Receiver1),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

%% Test that a message can be routed based on the message-annotations
%% provided in the modified outcome as described in
%% https://rabbitmq.com/blog/2024/10/11/modified-outcome
modified_dead_letter_headers_exchange(Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    HeadersXName = <<"my headers exchange">>,
    AlternateXName = <<"my alternate exchange">>,
    SourceQName = <<"source quorum queue">>,
    AppleQName = <<"dead letter classic queue receiving apples">>,
    BananaQName = <<"dead letter quorum queue receiving bananas">>,
    TrashQName = <<"trash queue receiving anything that doesn't match">>,

    ok = rabbitmq_amqp_client:declare_exchange(
           LinkPair,
           HeadersXName,
           #{type => <<"headers">>,
             arguments => #{<<"alternate-exchange">> => {utf8, AlternateXName}}}),

    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, AlternateXName, #{type => <<"fanout">>}),

    {ok, #{type := <<"quorum">>}} = rabbitmq_amqp_client:declare_queue(
                                      LinkPair,
                                      SourceQName,
                                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                                       <<"x-overflow">> => {utf8, <<"reject-publish">>},
                                                       <<"x-dead-letter-strategy">> => {utf8, <<"at-least-once">>},
                                                       <<"x-dead-letter-exchange">> => {utf8, HeadersXName}}}),
    {ok, #{type := <<"classic">>}} = rabbitmq_amqp_client:declare_queue(
                                       LinkPair,
                                       AppleQName,
                                       #{arguments => #{<<"x-queue-type">> => {utf8, <<"classic">>}}}),
    {ok, #{type := <<"quorum">>}} = rabbitmq_amqp_client:declare_queue(
                                      LinkPair,
                                      BananaQName,
                                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}}}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, TrashQName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(
           LinkPair, AppleQName, HeadersXName, <<>>,
           #{<<"x-fruit">> => {utf8, <<"apple">>},
             <<"x-match">> => {utf8, <<"any-with-x">>}}),
    ok = rabbitmq_amqp_client:bind_queue(
           LinkPair, BananaQName, HeadersXName, <<>>,
           #{<<"x-fruit">> => {utf8, <<"banana">>},
             <<"x-match">> => {utf8, <<"any-with-x">>}}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, TrashQName, AlternateXName, <<>>, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, rabbitmq_amqp_address:queue(SourceQName)),
    wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, rabbitmq_amqp_address:queue(SourceQName), unsettled),
    {ok, ReceiverApple} = amqp10_client:attach_receiver_link(
                            Session, <<"receiver apple">>, rabbitmq_amqp_address:queue(AppleQName), unsettled),
    {ok, ReceiverBanana} = amqp10_client:attach_receiver_link(
                             Session, <<"receiver banana">>, rabbitmq_amqp_address:queue(BananaQName), unsettled),
    {ok, ReceiverTrash} = amqp10_client:attach_receiver_link(
                            Session, <<"receiver trash">>, rabbitmq_amqp_address:queue(TrashQName), unsettled),

    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t1">>, <<"m1">>)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t2">>, <<"m2">>)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_message_annotations(
                                          #{"x-fruit" => <<"apple">>},
                                          amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_message_annotations(
                                          #{"x-fruit" => <<"apple">>},
                                          amqp10_msg:new(<<"t4">>, <<"m4">>))),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t5">>, <<"m5">>)),
    ok = wait_for_accepts(5),

    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    ?assertMatch(#{delivery_count := 0,
                   first_acquirer := true},
                 amqp10_msg:headers(Msg1)),
    ok = amqp10_client:settle_msg(Receiver, Msg1, {modified, true, true, #{<<"x-fruit">> => <<"banana">>}}),
    {ok, MsgBanana1} = amqp10_client:get_msg(ReceiverBanana),
    ?assertEqual([<<"m1">>], amqp10_msg:body(MsgBanana1)),
    ?assertMatch(#{delivery_count := 1,
                   first_acquirer := false},
                 amqp10_msg:headers(MsgBanana1)),
    ok = amqp10_client:accept_msg(ReceiverBanana, MsgBanana1),

    {ok, Msg2} = amqp10_client:get_msg(Receiver),
    ok = amqp10_client:settle_msg(Receiver, Msg2, {modified, true, true, #{<<"x-fruit">> => <<"apple">>}}),
    {ok, MsgApple1} = amqp10_client:get_msg(ReceiverApple),
    ?assertEqual([<<"m2">>], amqp10_msg:body(MsgApple1)),
    ?assertMatch(#{delivery_count := 1,
                   first_acquirer := false},
                 amqp10_msg:headers(MsgApple1)),
    ok = amqp10_client:accept_msg(ReceiverApple, MsgApple1),

    {ok, Msg3} = amqp10_client:get_msg(Receiver),
    ok = amqp10_client:settle_msg(Receiver, Msg3, {modified, false, true, #{}}),
    {ok, MsgApple2} = amqp10_client:get_msg(ReceiverApple),
    ?assertEqual([<<"m3">>], amqp10_msg:body(MsgApple2)),
    ?assertMatch(#{delivery_count := 0,
                   first_acquirer := false},
                 amqp10_msg:headers(MsgApple2)),
    ok = amqp10_client:accept_msg(ReceiverApple, MsgApple2),

    {ok, Msg4} = amqp10_client:get_msg(Receiver),
    ok = amqp10_client:settle_msg(Receiver, Msg4, {modified, false, true, #{<<"x-fruit">> => <<"banana">>}}),
    {ok, MsgBanana2} = amqp10_client:get_msg(ReceiverBanana),
    ?assertEqual([<<"m4">>], amqp10_msg:body(MsgBanana2)),
    ?assertMatch(#{delivery_count := 0,
                   first_acquirer := false},
                 amqp10_msg:headers(MsgBanana2)),
    ok = amqp10_client:accept_msg(ReceiverBanana, MsgBanana2),

    {ok, Msg5} = amqp10_client:get_msg(Receiver),
    %% This message should be routed via the alternate exchange to the trash queue.
    ok = amqp10_client:settle_msg(Receiver, Msg5, {modified, false, true, #{<<"x-fruit">> => <<"strawberry">>}}),
    {ok, MsgTrash} = amqp10_client:get_msg(ReceiverTrash),
    ?assertEqual([<<"m5">>], amqp10_msg:body(MsgTrash)),
    ?assertMatch(#{delivery_count := 0,
                   first_acquirer := false},
                 amqp10_msg:headers(MsgTrash)),
    ok = amqp10_client:accept_msg(ReceiverTrash, MsgTrash),

    ok = detach_link_sync(Sender),
    ok = detach_link_sync(Receiver),
    ok = detach_link_sync(ReceiverApple),
    ok = detach_link_sync(ReceiverBanana),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, SourceQName),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, AppleQName),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, BananaQName),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, TrashQName),
    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, HeadersXName),
    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, AlternateXName),
    ok = close(Init).

%% Test that custom dead lettering event tracking works as described in
%% https://rabbitmq.com/blog/2024/10/11/modified-outcome
modified_dead_letter_history(Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    Q1 = <<"qq 1">>,
    Q2 = <<"qq 2">>,

    {ok, _} = rabbitmq_amqp_client:declare_queue(
                LinkPair, Q1,
                #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                 <<"x-dead-letter-strategy">> => {utf8, <<"at-most-once">>},
                                 <<"x-dead-letter-exchange">> => {utf8, <<"amq.fanout">>}}}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(
                LinkPair, Q2,
                #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                 <<"x-dead-letter-strategy">> => {utf8, <<"at-most-once">>},
                                 <<"x-dead-letter-exchange">> => {utf8, <<>>}}}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, Q2, <<"amq.fanout">>, <<>>, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, rabbitmq_amqp_address:queue(Q1)),
    wait_for_credit(Sender),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, rabbitmq_amqp_address:queue(Q1), unsettled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, rabbitmq_amqp_address:queue(Q2), unsettled),

    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t">>, <<"m">>)),
    ok = wait_for_accepts(1),
    ok = detach_link_sync(Sender),

    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    ?assertMatch(#{delivery_count := 0,
                   first_acquirer := true},
                 amqp10_msg:headers(Msg1)),
    ok = amqp10_client:settle_msg(
           Receiver1, Msg1,
           {modified, true, true,
            #{<<"x-opt-history-list">> => {list, [{utf8, <<"l1">>}]},
              <<"x-opt-history-map">> => {map, [{{symbol, <<"k1">>}, {byte, -1}}]},
              <<"x-opt-history-array">> => {array, utf8, [{utf8, <<"a1">>}]}}
           }),

    {ok, Msg2} = amqp10_client:get_msg(Receiver2),
    ?assertMatch(#{delivery_count := 1,
                   first_acquirer := false},
                 amqp10_msg:headers(Msg2)),
    #{<<"x-opt-history-list">> := L1,
      <<"x-opt-history-map">> := L2,
      <<"x-opt-history-array">> := {array, utf8, L0}
     } = amqp10_msg:message_annotations(Msg2),
    ok = amqp10_client:settle_msg(
           Receiver2, Msg2,
           {modified, true, true,
            #{<<"x-opt-history-list">> => {list, [{int, -99} | L1]},
              <<"x-opt-history-map">> => {map, [{{symbol, <<"k2">>}, {symbol, <<"v2">>}} | L2]},
              <<"x-opt-history-array">> => {array, utf8, [{utf8, <<"a2">>} | L0]},
              <<"x-other">> => -99}}),

    {ok, Msg3} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m">>], amqp10_msg:body(Msg3)),
    ?assertMatch(#{delivery_count := 2,
                   first_acquirer := false},
                 amqp10_msg:headers(Msg3)),
    ?assertMatch(#{<<"x-opt-history-array">> := {array, utf8, [{utf8, <<"a2">>}, {utf8, <<"a1">>}]},
                   <<"x-opt-history-list">> := [{int, -99}, {utf8, <<"l1">>}],
                   <<"x-opt-history-map">> := [{{symbol, <<"k2">>}, {symbol, <<"v2">>}},
                                               {{symbol, <<"k1">>}, {byte, -1}}],
                   <<"x-other">> := -99}, amqp10_msg:message_annotations(Msg3)),
    ok = amqp10_client:accept_msg(Receiver1, Msg3),

    ok = detach_link_sync(Receiver1),
    ok = detach_link_sync(Receiver2),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, Q1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, Q2),
    ok = close(Init).

%% Tests that confirmations are returned correctly
%% when sending many messages async to a quorum queue.
sender_settle_mode_unsettled(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {_, Session, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}}},
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    Address = rabbitmq_amqp_address:queue(QName),
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
     after 30000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    ?assertMatch({ok, #{message_count := NumMsgs}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

sender_settle_mode_unsettled_fanout(Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QNames = [<<"q1">>, <<"q2">>, <<"q3">>],
    [begin
         {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
         ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, <<"amq.fanout">>, <<>>, #{})
     end || QName <- QNames],

    Address = rabbitmq_amqp_address:exchange(<<"amq.fanout">>),
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
     after 30000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    [?assertMatch({ok, #{message_count := NumMsgs}},
                  rabbitmq_amqp_client:delete_queue(LinkPair, QName))
     || QName <- QNames],
    ok = close(Init).

%% Tests that confirmations are returned correctly
%% when sending many messages async to a quorum queue where
%% every 3rd message is settled by the sender.
sender_settle_mode_mixed(Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}}},
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, mixed),
    ok = wait_for_credit(Sender),

    %% Send many messages async.
    %% The last message (31) will be sent unsettled.
    NumMsgs = 31,
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
              end, lists:seq(1, NumMsgs)),
    21 = length(DTags),

    %% Wait for confirms.
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 30000 -> ct:fail({missing_accepted, DTag})
     end || DTag <- DTags],

    ok = amqp10_client:detach_link(Sender),
    ?assertMatch({ok, #{message_count := NumMsgs}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

durable_field_classic_queue(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    durable_field(Config, <<"classic">>, QName).

durable_field_quorum_queue(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    durable_field(Config, <<"quorum">>, QName).

durable_field_stream(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    durable_field(Config, <<"stream">>, QName).

durable_field(Config, QueueType, QName)
  when is_binary(QueueType) ->
    Address = rabbitmq_amqp_address:queue(QName),
    {_Connection, Session, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QueueType}}},
    {ok, #{type := QueueType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, unsettled),
    wait_for_credit(Sender),

    ok = amqp10_client:send_msg(Sender,
                                amqp10_msg:set_headers(
                                  #{durable => true},
                                  amqp10_msg:new(<<"t1">>, <<"durable">>))),
    ok = amqp10_client:send_msg(Sender,
                                amqp10_msg:set_headers(
                                  #{durable => false},
                                  amqp10_msg:new(<<"t2">>, <<"non-durable">>))),
    %% Even though the AMQP spec defines durable=false as default
    %% (i.e. durable is false if the field is omitted on the wire),
    %% we expect our AMQP Erlang library to be safe by default,
    %% and therefore send the message as durable=true on behalf of us.
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:new(<<"t3">>, <<"lib publishes as durable by default">>)),
    %% When we expliclitly publish without a header section, RabbitMQ should interpret
    %% durable as false according to the AMQP spec.
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:from_amqp_records(
             [#'v1_0.transfer'{delivery_tag = {binary, <<"t4">>},
                               settled = false,
                               message_format = {uint, 0}},
              #'v1_0.data'{content = <<"publish without header section">>}])),

    ok = wait_for_accepts(4),
    ok = detach_link_sync(Sender),
    flush(sent),

    Filter = consume_from_first(QueueType),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, unsettled,
                       none, Filter),

    ok = amqp10_client:flow_link_credit(Receiver, 4, never),
    [M1, M2, M3, M4] = Msgs = receive_messages(Receiver, 4),
    ?assertEqual(<<"durable">>, amqp10_msg:body_bin(M1)),
    ?assertMatch(#{durable := true}, amqp10_msg:headers(M1)),
    ?assertEqual(<<"non-durable">>, amqp10_msg:body_bin(M2)),
    ?assertMatch(#{durable := false}, amqp10_msg:headers(M2)),
    ?assertEqual(<<"lib publishes as durable by default">>, amqp10_msg:body_bin(M3)),
    ?assertMatch(#{durable := true}, amqp10_msg:headers(M3)),
    ?assertEqual(<<"publish without header section">>, amqp10_msg:body_bin(M4)),
    ?assertMatch(#{durable := false}, amqp10_msg:headers(M4)),
    [ok = amqp10_client:accept_msg(Receiver, M) || M <- Msgs],

    ok = detach_link_sync(Receiver),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    close(Init).

invalid_transfer_settled_flag(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session(Connection),
    {ok, Session2} = amqp10_client:begin_session(Connection),
    TargetAddr = rabbitmq_amqp_address:exchange(<<"amq.fanout">>),
    {ok, SenderSettled} = amqp10_client:attach_sender_link_sync(
                            Session1, <<"link 1">>, TargetAddr, settled),
    {ok, SenderUnsettled} = amqp10_client:attach_sender_link_sync(
                              Session2, <<"link 2">>, TargetAddr, unsettled),
    ok = wait_for_credit(SenderSettled),
    ok = wait_for_credit(SenderUnsettled),

    ok = amqp10_client:send_msg(SenderSettled, amqp10_msg:new(<<"tag1">>, <<"m1">>, false)),
    receive
        {amqp10_event,
         {session, Session1,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
              description = {utf8, Description1}}}}} ->
            ?assertEqual(
               <<"sender settle mode is 'settled' but transfer settled flag is interpreted as being 'false'">>,
               Description1)
    after 30000 -> flush(missing_ended),
                  ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:send_msg(SenderUnsettled, amqp10_msg:new(<<"tag2">>, <<"m2">>, true)),
    receive
        {amqp10_event,
         {session, Session2,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
              description = {utf8, Description2}}}}} ->
            ?assertEqual(
               <<"sender settle mode is 'unsettled' but transfer settled flag is interpreted as being 'true'">>,
               Description2)
    after 30000 -> flush(missing_ended),
                  ct:fail({missing_event, ?LINE})
    end,

    ok = close_connection_sync(Connection).

quorum_queue_rejects(Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                              <<"x-max-length">> => {ulong, 1},
                              <<"x-overflow">> => {utf8, <<"reject-publish">>}}},
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, mixed),
    ok = wait_for_credit(Sender),

    %% Quorum queue's x-max-length limit is known to be off by 1.
    %% Therefore, we expect the first 2 messages to be accepted.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag a">>, <<>>, false)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag b">>, <<>>, false)),
    [receive {amqp10_disposition, {accepted, DTag}} -> ok
     after 30000 -> ct:fail({missing_accepted, DTag})
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
     after 30000 -> ct:fail({missing_rejected, DTag})
     end || DTag <- DTags ++ [<<"tag d">>]],

    ok = amqp10_client:detach_link(Sender),
    ?assertMatch({ok, #{message_count := 2}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

receiver_settle_mode_first(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Connection, Session, LinkPair} = init(Config),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
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
    after 30000 -> ct:fail("expected credit_exhausted")
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
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = amqp10_client:end_session(Session),
    ok = close_connection_sync(Connection).

publishing_to_non_existing_queue_should_settle_with_released(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    QName = <<"queue does not exist">>,
    Address = rabbitmq_amqp_address:exchange(<<"amq.direct">>, QName),
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
    ok = close_connection_sync(Connection),
    ok = flush("post sender close").

open_link_to_non_existing_destination_should_end_session(Config) ->
    OpnConf = connection_config(Config),
    Name = atom_to_binary(?FUNCTION_NAME),
    Addresses = [rabbitmq_amqp_address:exchange(Name, <<"bar">>),
                 rabbitmq_amqp_address:queue(Name)],
    SenderLinkName = <<"test-sender">>,
    [begin
         {ok, Connection} = amqp10_client:open_connection(OpnConf),
         {ok, Session} = amqp10_client:begin_session_sync(Connection),
         ct:pal("Address ~s", [Address]),
         {ok, _} = amqp10_client:attach_sender_link(
                     Session, SenderLinkName, Address),
         wait_for_session_end(Session),
         ok = close_connection_sync(Connection),
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
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection, Session, LinkPair} = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QueueType}}},
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
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
    after 30000 ->
              Reason = delivery_timeout,
              flush(Reason),
              ct:fail(Reason)
    end,
    OutMsg2 = amqp10_msg:new(<<"tag-2">>, <<"my-body2">>, false),
    ok = amqp10_client:send_msg(Sender, OutMsg2),
    ok = wait_for_accepted(<<"tag-2">>),

    %% no delivery should be made at this point
    receive {amqp10_msg, _, _} -> ct:fail(unexpected_delivery)
    after 500 -> ok
    end,

    flush("final"),
    ok = amqp10_client:detach_link(Sender),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = close_connection_sync(Connection).

drain_many_classic_queue(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    drain_many(Config, <<"classic">>, QName).

drain_many_quorum_queue(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    drain_many(Config, <<"quorum">>, QName).

drain_many_stream(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    drain_many(Config, <<"stream">>, QName).

drain_many(Config, QueueType, QName)
  when is_binary(QueueType) ->
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection, Session, LinkPair} = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QueueType}}},
    {ok, #{type := QueueType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    Num = 5000,
    ok = send_messages(Sender, Num, false),
    ok = wait_for_accepts(Num),

    TerminusDurability = none,
    Filter = consume_from_first(QueueType),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, settled,
                       TerminusDurability, Filter),

    ok = amqp10_client:flow_link_credit(Receiver, Num - 1, never, true),
    ?assertEqual(Num - 1, count_received_messages(Receiver)),
    flush("drained 1"),

    ok = amqp10_client:flow_link_credit(Receiver, Num, never, true),
    receive_messages(Receiver, 1),
    flush("drained 2"),

    ok = send_messages(Sender, Num, false),
    ok = wait_for_accepts(Num),
    receive {amqp10_msg, _, _} = Unexpected1 -> ct:fail({unexpected, Unexpected1})
    after 10 -> ok
    end,

    %% Let's send a couple of FLOW frames in sequence.
    ok = amqp10_client:flow_link_credit(Receiver, 0, never, false),
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, false),
    ok = amqp10_client:flow_link_credit(Receiver, Num div 2, never, false),
    ok = amqp10_client:flow_link_credit(Receiver, Num, never, false),
    ok = amqp10_client:flow_link_credit(Receiver, Num, never, true),
    %% Eventually, we should receive all messages.
    receive_messages(Receiver, Num),
    flush("drained 3"),

    ok = send_messages(Sender, 1, false),
    ok = wait_for_accepts(1),
    %% Our receiver shouldn't have any credit left to consume this message.
    receive {amqp10_msg, _, _} = Unexpected2 -> ct:fail({unexpected, Unexpected2})
    after 30 -> ok
    end,

    %% Grant a huge amount of credits.
    ok = amqp10_client:flow_link_credit(Receiver, 2_000_000_000, never, true),
    %% We expect the server to send us the last message and
    %% to advance the delivery-count promptly.
    receive {amqp10_msg, _, _} -> ok
    after 30000 -> ct:fail({missing_delivery, ?LINE})
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = close_connection_sync(Connection).

amqp_stream_amqpl(Config) ->
    amqp_amqpl(<<"stream">>, Config).

amqp_quorum_queue_amqpl(Config) ->
    amqp_amqpl(<<"quorum">>, Config).

%% Send messages with different body sections to a queue and consume via AMQP 0.9.1.
amqp_amqpl(QType, Config) ->
    {Connection, Session, LinkPair} = init(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    %% single amqp-value section
    Body1 = #'v1_0.amqp_value'{content = {binary, <<0, 255>>}},
    Body2 = #'v1_0.amqp_value'{content = false},
    %% single amqp-sequene section
    Body3 = [#'v1_0.amqp_sequence'{content = [{binary, <<0, 255>>}]}],
    %% multiple amqp-sequene sections
    Body4 = [#'v1_0.amqp_sequence'{content = [{long, -1}]},
             #'v1_0.amqp_sequence'{content = [true, {utf8, <<"🐇"/utf8>>}]}],
    %% single data section
    Body5 = [#'v1_0.data'{content = <<0, 255>>}],
    %% multiple data sections
    Body6 = [#'v1_0.data'{content = <<0, 1>>},
             #'v1_0.data'{content = <<2, 3>>}],

    %% Send only header and body sections
    [ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<>>, Body, true)) ||
     Body <- [Body1, Body2, Body3, Body4, Body5, Body6]],
    %% Send with application-properties
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_application_properties(
             #{"my int" => {int, -2}},
             amqp10_msg:new(<<>>, Body1, true))),
    %% Send with properties
    CorrelationID = <<"my correlation ID">>,
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{correlation_id => CorrelationID},
             amqp10_msg:new(<<>>, Body1, true))),
    %% Send with both properties and application-properties
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{correlation_id => CorrelationID},
             amqp10_msg:set_application_properties(
               #{"my long" => -9_000_000_000},
               amqp10_msg:new(<<>>, Body1, true)))),
    %% Send with footer
    Footer = #'v1_0.footer'{content = [{{symbol, <<"x-my footer">>}, {ubyte, 255}}]},
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:from_amqp_records(
             [#'v1_0.transfer'{delivery_tag = {binary, <<>>},
                               settled = true,
                               message_format = {uint, 0}},
              Body1,
              Footer])),
    %% Send with an array value in message annotations.
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_message_annotations(
             #{<<"x-array">> => {array, utf8, [{utf8, <<"e1">>},
                                               {utf8, <<"e2">>}]}},
             amqp10_msg:new(<<>>, Body1, true))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_headers(
             #{durable => false},
             amqp10_msg:new(<<>>, Body1, true))),

    ok = amqp10_client:detach_link(Sender),
    flush(detached),

    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'basic.qos_ok'{} =  amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                            prefetch_count = 100}),
    CTag = <<"my-tag">>,
    Args = case QType of
               <<"stream">> -> [{<<"x-stream-offset">>, longstr, <<"first">>}];
               <<"quorum">> -> []
           end,
    #'basic.consume_ok'{} = amqp_channel:subscribe(
                              Ch,
                              #'basic.consume'{
                                 queue = QName,
                                 consumer_tag = CTag,
                                 arguments = Args},
                              self()),

    receive {#'basic.deliver'{consumer_tag = CTag,
                              redelivered  = false},
             #amqp_msg{payload = Payload1,
                       props = #'P_basic'{delivery_mode = DelMode2,
                                          type = <<"amqp-1.0">>}}} ->
                ?assertEqual([Body1], amqp10_framing:decode_bin(Payload1)),
                ?assertEqual(2, DelMode2)
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload2,
                          props = #'P_basic'{type = <<"amqp-1.0">>}}} ->
                ?assertEqual([Body2], amqp10_framing:decode_bin(Payload2))
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload3,
                          props = #'P_basic'{type = <<"amqp-1.0">>}}} ->
                ?assertEqual(Body3, amqp10_framing:decode_bin(Payload3))
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload4,
                          props = #'P_basic'{type = <<"amqp-1.0">>}}} ->
                ?assertEqual(Body4, amqp10_framing:decode_bin(Payload4))
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload5,
                          props = #'P_basic'{type = undefined}}} ->
                ?assertEqual(<<0, 255>>, Payload5)
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload6,
                          props = #'P_basic'{type = undefined}}} ->
                %% We expect that RabbitMQ concatenates the binaries of multiple data sections.
                ?assertEqual(<<0, 1, 2, 3>>, Payload6)
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload7,
                          props = #'P_basic'{headers = Headers7}}} ->
                ?assertEqual([Body1], amqp10_framing:decode_bin(Payload7)),
                ?assertEqual({signedint, -2}, rabbit_misc:table_lookup(Headers7, <<"my int">>))
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload8,
                          props = #'P_basic'{correlation_id = Corr8}}} ->
                ?assertEqual([Body1], amqp10_framing:decode_bin(Payload8)),
                ?assertEqual(CorrelationID, Corr8)
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload9,
                          props = #'P_basic'{headers = Headers9,
                                             correlation_id = Corr9}}} ->
                ?assertEqual([Body1], amqp10_framing:decode_bin(Payload9)),
                ?assertEqual(CorrelationID, Corr9),
                ?assertEqual({long, -9_000_000_000}, rabbit_misc:table_lookup(Headers9, <<"my long">>))
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload10}} ->
                %% RabbitMQ converts the entire AMQP encoded body including the footer
                %% to AMQP legacy payload.
                ?assertEqual([Body1, Footer], amqp10_framing:decode_bin(Payload10))
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload11,
                          props = #'P_basic'{headers = Headers11}}} ->
                ?assertEqual([Body1], amqp10_framing:decode_bin(Payload11)),
                ?assertEqual({array, [{longstr, <<"e1">>}, {longstr, <<"e2">>}]},
                             rabbit_misc:table_lookup(Headers11, <<"x-array">>))
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,
    receive {_, #amqp_msg{payload = Payload12,
                          props = #'P_basic'{delivery_mode = DelMode1}}} ->
                ?assertEqual([Body1], amqp10_framing:decode_bin(Payload12)),
                ?assertNotEqual(2, DelMode1)
    after 30000 -> ct:fail({missing_deliver, ?LINE})
    end,

    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = close_connection_sync(Connection).

message_headers_conversion(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    %% declare a quorum queue
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    amqp_channel:call(Ch, #'queue.declare'{
                             queue = QName,
                             durable = true,
                             arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),

    amqp10_to_amqp091_header_conversion(Session, Ch, QName, Address),
    amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address),

    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok = delete_queue(Session, QName),
    ok = close_connection_sync(Connection).

amqp10_to_amqp091_header_conversion(Session,Ch, QName, Address) ->
    {ok, Sender} = create_amqp10_sender(Session, Address),

    OutMsg1 = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, false),
    OutMsg2 = amqp10_msg:set_application_properties(
                #{"string" => "string-val",
                  "long" => -2,
                  "uint" => {uint, 2},
                  "bool" => false},
                OutMsg1),
    OutMsg3 = amqp10_msg:set_message_annotations(
                #{"x-string" => "string-value",
                  "x-long" => -3,
                  "x-uint" => {uint, 3},
                  "x-bool" => true},
                OutMsg2),
    OutMsg = amqp10_msg:set_headers(
               #{durable => true,
                 priority => 7,
                 ttl => 88000},
               OutMsg3),
    ok = amqp10_client:send_msg(Sender, OutMsg),
    ok = wait_for_accepts(1),

    {#'basic.get_ok'{},
     #amqp_msg{props = #'P_basic'{headers = Headers,
                                  delivery_mode = DeliveryMode,
                                  priority = Priority,
                                  expiration = Expiration}}
    } = amqp_channel:call(Ch, #'basic.get'{queue = QName, no_ack = true}),

    %% assert application properties
    ?assertEqual({longstr, <<"string-val">>}, rabbit_misc:table_lookup(Headers, <<"string">>)),
    ?assertEqual({long, -2}, rabbit_misc:table_lookup(Headers, <<"long">>)),
    ?assertEqual({unsignedint, 2}, rabbit_misc:table_lookup(Headers, <<"uint">>)),
    ?assertEqual({bool, false}, rabbit_misc:table_lookup(Headers, <<"bool">>)),
    %% assert message annotations
    ?assertEqual({longstr, <<"string-value">>}, rabbit_misc:table_lookup(Headers, <<"x-string">>)),
    ?assertEqual({long, -3}, rabbit_misc:table_lookup(Headers, <<"x-long">>)),
    ?assertEqual({unsignedint, 3}, rabbit_misc:table_lookup(Headers, <<"x-uint">>)),
    ?assertEqual({bool, true}, rabbit_misc:table_lookup(Headers, <<"x-bool">>)),
    %% assert headers
    ?assertEqual(2, DeliveryMode),
    ?assertEqual(7, Priority),
    ?assertEqual(<<"88000">>, Expiration).

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
      #amqp_msg{props = #'P_basic'{delivery_mode = 2,
                                   priority = 5,
                                   headers = Amqp091Headers},
                payload = <<"foobar">>}),

    {ok, [Msg]} = drain_queue(Session, Address, 1),

    ?assertMatch(#{durable := true,
                   priority := 5},
                 amqp10_msg:headers(Msg)),

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
    after 30000 -> ct:fail("missing attached")
    end,
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    NMsgsPerSender = 20,
    NMsgsPerReceiver = NMsgsPerSender * 2, % due to fanout
    ok = amqp10_client:flow_link_credit(Receiver1, NMsgsPerReceiver, never),
    ok = amqp10_client:flow_link_credit(Receiver2, NMsgsPerReceiver, never),
    flush("receiver attached"),

    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    [#'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                              exchange = <<"amq.fanout">>})
     || QName <- Qs],
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    %% Send on each session.
    TargetAddr = rabbitmq_amqp_address:exchange(<<"amq.fanout">>),
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
    [ok = delete_queue(Session1, Q) || Q <- Qs],
    ok = end_session_sync(Session1),
    ok = end_session_sync(Session2),
    ok = close_connection_sync(Connection).

server_closes_link_classic_queue(Config) ->
    server_closes_link(<<"classic">>, Config).

server_closes_link_quorum_queue(Config) ->
    server_closes_link(<<"quorum">>, Config).

server_closes_link_stream(Config) ->
    server_closes_link(<<"stream">>, Config).

server_closes_link(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),

    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail("missing ATTACH frame from server")
    end,
    ok = amqp10_client:flow_link_credit(Receiver, 5, never),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    flush(credited),
    DTag = <<0>>,
    Body = <<"body">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag, Body, false)),
    ok = wait_for_accepted(DTag),

    receive {amqp10_msg, Receiver, Msg} ->
                ?assertEqual([Body], amqp10_msg:body(Msg))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,

    [SessionPid] = rpc(Config, rabbit_amqp_session, list_local, []),
    %% Received delivery is unsettled.
    eventually(?_assertEqual(
                  1,
                  begin
                      #{outgoing_unsettled_map := UnsettledMap} = formatted_state(SessionPid),
                      maps:size(UnsettledMap)
                  end)),

    %% Server closes the link endpoint due to some AMQP 1.0 external condition:
    %% In this test, the external condition is that an AMQP 0.9.1 client deletes the queue.
    ok = delete_queue(Session, QName),

    %% We expect that the server closes the link endpoints,
    %% i.e. the server sends us DETACH frames.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender, {detached, ExpectedError}}} -> ok
    after 30000 -> ct:fail("server did not close our outgoing link")
    end,

    receive {amqp10_event, {link, Receiver, {detached, ExpectedError}}} -> ok
    after 30000 -> ct:fail("server did not close our incoming link")
    end,

    %% Our client has not and will not settle the delivery since the source queue got deleted and
    %% the link detached with an error condition. Nevertheless the server session should clean up its
    %% session state by removing the unsettled delivery from its session state.
    eventually(?_assertEqual(
                  0,
                  begin
                      #{outgoing_unsettled_map := UnsettledMap} = formatted_state(SessionPid),
                      maps:size(UnsettledMap)
                  end)),

    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

server_closes_link_exchange_settled(Config) ->
    server_closes_link_exchange(true, Config).

server_closes_link_exchange_unsettled(Config) ->
    server_closes_link_exchange(false, Config).

server_closes_link_exchange(Settled, Config) ->
    XName = atom_to_binary(?FUNCTION_NAME),
    QName = <<"my queue">>,
    RoutingKey = <<"my routing key">>,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = XName}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                             exchange = XName,
                                                             routing_key = RoutingKey}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:exchange(XName, RoutingKey),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    ?assertMatch(#{publishers := 1}, get_global_counters(Config)),

    DTag1 = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_accepted(DTag1),

    %% Server closes the link endpoint due to some AMQP 1.0 external condition:
    %% In this test, the external condition is that an AMQP 0.9.1 client deletes the exchange.
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = XName}),

    %% When we publish the next message, we expect:
    %% 1. that the message is released because the exchange doesn't exist anymore, and
    DTag2 = <<255>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag2, <<"m2">>, Settled)),
    case Settled of
        true ->
            ok;
        false ->
            ok = wait_for_settlement(DTag2, released)
    end,
    %% 2. that the server closes the link, i.e. sends us a DETACH frame.
    receive {amqp10_event,
             {link, Sender,
              {detached, #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_NOT_FOUND}}}} -> ok
    after 30000 -> ct:fail("server did not close our outgoing link")
    end,
    ?assertMatch(#{publishers := 0}, get_global_counters(Config)),

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

link_target_classic_queue_deleted(Config) ->
    link_target_queue_deleted(<<"classic">>, Config).

link_target_quorum_queue_deleted(Config) ->
    link_target_queue_deleted(<<"quorum">>, Config).

link_target_queue_deleted(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    flush(credited),
    DTag1 = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_accepted(DTag1),

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
    ok = delete_queue(Session, QName),
    ok = wait_for_settlement(DTag2, released),

    %% After the 2nd message got released, we additionally expect RabbitMQ to close the link given
    %% that the target link endpoint - the queue - got deleted.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender, {detached, ExpectedError}}} -> ok
    after 30000 -> ct:fail("server did not close our outgoing link")
    end,

    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

rabbit_queue_type_deliver_noop(_TargetQs, _Msg, _Opts, QTypeState) ->
    Actions = [],
    {ok, QTypeState, Actions}.

target_queues_deleted_accepted(Config) ->
    Q1 = <<"q1">>,
    Q2 = <<"q2">>,
    Q3 = <<"q3">>,
    QNames = [Q1, Q2, Q3],
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    [begin
         #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
         #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{queue = QName,
                                                                  exchange = <<"amq.fanout">>})
     end || QName <- QNames],

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:exchange(<<"amq.fanout">>, <<"ignored">>),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),
    flush(credited),

    DTag1 = <<1>>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_accepted(DTag1),

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
    after 30000 -> ct:fail(accepted_timeout)
    end,

    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q1})),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

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
    after 30000 -> ct:fail(opened_timeout)
    end,
    ok = close_connection_sync(Connection),

    Events = event_recorder:get_events(Config),
    ok = event_recorder:stop(Config),
    ct:pal("Recorded events: ~p", [Events]),

    Proto = case web_amqp(Config) of
                true -> {'Web AMQP', {1, 0}};
                false -> {1, 0}
            end,
    AuthProps = [{name, <<"guest">>},
                 {auth_mechanism, <<"PLAIN">>},
                 {ssl, false},
                 {protocol, Proto}],
    ?assertMatch(
       {value, _},
       find_event(user_authentication_success, AuthProps, Events)),

    Node = get_node_config(Config, 0, nodename),
    ConnectionCreatedProps = [{protocol, Proto},
                              {node, Node},
                              {vhost, <<"/">>},
                              {user, <<"guest">>},
                              {container_id, <<"my container">>},
                              {type, network}],
    {value, ConnectionCreatedEvent} = find_event(
                                        connection_created,
                                        ConnectionCreatedProps, Events),
    Props = ConnectionCreatedEvent#event.props,
    Name = proplists:lookup(name, Props),
    Pid = proplists:lookup(pid, Props),
    ClientProperties = {client_properties, List} = proplists:lookup(client_properties, Props),
    ?assert(lists:member(
              {<<"product">>, longstr, <<"AMQP 1.0 client">>},
              List)),
    ?assert(lists:member(
              {<<"ignore-maintenance">>, bool, true},
              List)),

    ConnectionClosedProps = [{node, Node},
                             Name,
                             Pid,
                             ClientProperties],
    ?assertMatch(
       {value, _},
       find_event(connection_closed, ConnectionClosedProps, Events)),
    ok.

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
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 1 receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
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
         after 30000 -> ct:fail("missing m1")
         end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 10 -> ok
    end,

    %% Synchronously get the 2nd message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    M2 = receive {amqp10_msg, Receiver, Msg2} ->
                     ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2)),
                     Msg2
         after 30000 -> ct:fail("missing m2")
         end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
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
    after 30000 -> ct:fail("missing m3")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp5 -> ct:fail("received unexpected message ~p", [Unexp5])
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

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
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach a sender and a receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    %% Grant 2 credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),

    %% Let's send 5 messages to the queue.
    [ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true)) ||
     Bin <- [<<"m1">>, <<"m2">>, <<"m3">>, <<"m4">>, <<"m5">>]],

    %% We should receive exactly 2 messages.
    receive {amqp10_msg, Receiver, Msg1} -> ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1))
    after 30000 -> ct:fail("missing m1")
    end,
    receive {amqp10_msg, Receiver, Msg2} -> ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2))
    after 30000 -> ct:fail("missing m2")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail("received unexpected message ~p", [Unexp1])
    after 50 -> ok
    end,

    %% Grant 2 more credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),
    %% Again, we should receive exactly 2 messages.
    receive {amqp10_msg, Receiver, Msg3} -> ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3))
    after 30000 -> ct:fail("missing m3")
    end,
    receive {amqp10_msg, Receiver, Msg4} -> ?assertEqual([<<"m4">>], amqp10_msg:body(Msg4))
    after 30000 -> ct:fail("missing m4")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 50 -> ok
    end,

    %% Grant 2 more credits to the sending queue.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never),

    %% We should receive the last (5th) message.
    receive {amqp10_msg, Receiver, Msg5} -> ?assertEqual([<<"m5">>], amqp10_msg:body(Msg5))
    after 30000 -> ct:fail("missing m5")
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

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
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 1 receivers to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"my receiver">>, Address, SenderSettleMode),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
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
    after 30000 -> ct:fail("missing m1")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail("received unexpected message ~p", [Unexp2])
    after 10 -> ok
    end,

    %% Synchronously get the 2nd message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),
    receive {amqp10_msg, Receiver, Msg2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2))
    after 30000 -> ct:fail("missing m2")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,
    receive {amqp10_msg, _, _} = Unexp3 -> ct:fail("received unexpected message ~p", [Unexp3])
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

timed_get_classic_queue(Config) ->
    timed_get(<<"classic">>, Config).

timed_get_quorum_queue(Config) ->
    timed_get(<<"quorum">>, Config).

timed_get_stream(Config) ->
    timed_get(<<"stream">>, Config).

%% Synchronous get with a timeout, figure 2.44.
timed_get(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach a sender and a receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session,
                       <<"test-receiver">>,
                       Address,
                       unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    flush(receiver_attached),

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, false),

    Timeout = 10,
    receive Unexpected0 -> ct:fail("received unexpected ~p", [Unexpected0])
    after Timeout -> ok
    end,

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,

    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"my tag">>, <<"my msg">>, true)),

    %% Since our consumer didn't grant any new credit, we shouldn't receive the message we
    %% just sent.
    receive Unexpected1 -> ct:fail("received unexpected ~p", [Unexpected1])
    after 50 -> ok
    end,

    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),
    receive {amqp10_msg, Receiver, Msg1} -> ?assertEqual([<<"my msg">>], amqp10_msg:body(Msg1))
    after 30000 -> ct:fail("missing 'my msg'")
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

stop_classic_queue(Config) ->
    stop(<<"classic">>, Config).

stop_quorum_queue(Config) ->
    stop(<<"quorum">>, Config).

stop_stream(Config) ->
    stop(<<"stream">>, Config).

%% Test stopping a link, figure 2.46.
stop(QType, Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
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
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address, settled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
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

    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

consumer_priority_classic_queue(Config) ->
    consumer_priority(<<"classic">>, Config).

consumer_priority_quorum_queue(Config) ->
    consumer_priority(<<"quorum">>, Config).

consumer_priority(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {_, Session, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    %% We test what our RabbitMQ docs state:
    %% "Consumers which do not specify a value have priority 0.
    %% Larger numbers indicate higher priority, and both positive and negative numbers can be used."
    {ok, ReceiverDefaultPrio} = amqp10_client:attach_receiver_link(
                                  Session,
                                  <<"default prio consumer">>,
                                  Address,
                                  unsettled),
    {ok, ReceiverHighPrio} = amqp10_client:attach_receiver_link(
                               Session,
                               <<"high prio consumer">>,
                               Address,
                               unsettled,
                               none,
                               #{},
                               #{<<"rabbitmq:priority">> => {int, 2_000_000_000}}),
    {ok, ReceiverLowPrio} = amqp10_client:attach_receiver_link(
                              Session,
                              <<"low prio consumer">>,
                              Address,
                              unsettled,
                              none,
                              #{},
                              #{<<"rabbitmq:priority">> => {int, -2_000_000_000}}),
    ok = amqp10_client:flow_link_credit(ReceiverDefaultPrio, 1, never),
    ok = amqp10_client:flow_link_credit(ReceiverHighPrio, 2, never),
    ok = amqp10_client:flow_link_credit(ReceiverLowPrio, 1, never),

    NumMsgs = 5,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin))
     end || N <- lists:seq(1, NumMsgs)],
    ok = wait_for_accepts(NumMsgs),

    receive {amqp10_msg, Rec1, Msg1} ->
                ?assertEqual(<<"1">>, amqp10_msg:body_bin(Msg1)),
                ?assertEqual(ReceiverHighPrio, Rec1),
                ok = amqp10_client:accept_msg(Rec1, Msg1)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Rec2, Msg2} ->
                ?assertEqual(<<"2">>, amqp10_msg:body_bin(Msg2)),
                ?assertEqual(ReceiverHighPrio, Rec2),
                ok = amqp10_client:accept_msg(Rec2, Msg2)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Rec3, Msg3} ->
                ?assertEqual(<<"3">>, amqp10_msg:body_bin(Msg3)),
                ?assertEqual(ReceiverDefaultPrio, Rec3),
                ok = amqp10_client:accept_msg(Rec3, Msg3)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Rec4, Msg4} ->
                ?assertEqual(<<"4">>, amqp10_msg:body_bin(Msg4)),
                ?assertEqual(ReceiverLowPrio, Rec4),
                ok = amqp10_client:accept_msg(Rec4, Msg4)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, _, _} = Unexpected ->
                ct:fail({unexpected_msg, Unexpected, ?LINE})
    after 5 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(ReceiverDefaultPrio),
    ok = amqp10_client:detach_link(ReceiverHighPrio),
    ok = amqp10_client:detach_link(ReceiverLowPrio),
    {ok, #{message_count := 1}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

single_active_consumer_priority_quorum_queue(Config) ->
    QType = <<"quorum">>,
    QName = atom_to_binary(?FUNCTION_NAME),
    {Connection, Session1, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType},
                              <<"x-single-active-consumer">> => true}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    %% Send 6 messages.
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(Session1, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    NumMsgs = 6,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true))
     end || N <- lists:seq(1, NumMsgs)],
    ok = amqp10_client:detach_link(Sender),

    %% The 1st consumer (with default prio 0) will become active.
    {ok, Recv1} = amqp10_client:attach_receiver_link(
                    Session1, <<"receiver 1">>, Address, unsettled),
    receive {amqp10_event, {link, Recv1, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, Msg1} = amqp10_client:get_msg(Recv1),
    ?assertEqual([<<"1">>], amqp10_msg:body(Msg1)),

    %% The 2nd consumer should take over thanks to higher prio.
    {ok, Recv2} = amqp10_client:attach_receiver_link(
                    Session1, <<"receiver 2">>, Address, unsettled, none, #{},
                    #{<<"rabbitmq:priority">> => {int, 1}}),
    receive {amqp10_event, {link, Recv2, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    flush("attched receiver 2"),

    %% To ensure in-order processing and to avoid interrupting the 1st consumer during
    %% its long running task processing, neither of the 2 consumers should receive more
    %% messages until the 1st consumer settles all outstanding messages.
    ?assertEqual({error, timeout}, amqp10_client:get_msg(Recv1, 5)),
    ?assertEqual({error, timeout}, amqp10_client:get_msg(Recv2, 5)),
    ok = amqp10_client:accept_msg(Recv1, Msg1),
    receive {amqp10_msg, R1, Msg2} ->
                ?assertEqual([<<"2">>], amqp10_msg:body(Msg2)),
                ?assertEqual(Recv2, R1),
                ok = amqp10_client:accept_msg(Recv2, Msg2)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,

    %% Attaching with same prio should not take over.
    {ok, Session2} = amqp10_client:begin_session_sync(Connection),
    {ok, Recv3} = amqp10_client:attach_receiver_link(
                    Session2, <<"receiver 3">>, Address, unsettled, none, #{},
                    #{<<"rabbitmq:priority">> => {int, 1}}),
    receive {amqp10_event, {link, Recv3, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ?assertEqual({error, timeout}, amqp10_client:get_msg(Recv3, 5)),
    ok = end_session_sync(Session2),

    {ok, Recv4} = amqp10_client:attach_receiver_link(
                    Session1, <<"receiver 4">>, Address, unsettled, none, #{},
                    #{<<"rabbitmq:priority">> => {int, 1}}),
    receive {amqp10_event, {link, Recv4, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, Recv5} = amqp10_client:attach_receiver_link(
                    Session1, <<"receiver 5">>, Address, unsettled, none, #{},
                    #{<<"rabbitmq:priority">> => {int, 1}}),
    receive {amqp10_event, {link, Recv5, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    flush("attched receivers 4 and 5"),

    ok = amqp10_client:flow_link_credit(Recv4, 1, never),
    ok = amqp10_client:flow_link_credit(Recv5, 2, never),

    %% Stop the active consumer.
    ok = amqp10_client:detach_link(Recv2),
    receive {amqp10_event, {link, Recv2, {detached, normal}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    %% The 5th consumer should become the active one because it is up,
    %% has highest prio (1), and most credits (2).
    receive {amqp10_msg, R2, Msg3} ->
                ?assertEqual([<<"3">>], amqp10_msg:body(Msg3)),
                ?assertEqual(Recv5, R2),
                ok = amqp10_client:accept_msg(Recv5, Msg3)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, R3, Msg4} ->
                ?assertEqual([<<"4">>], amqp10_msg:body(Msg4)),
                ?assertEqual(Recv5, R3),
                ok = amqp10_client:accept_msg(Recv5, Msg4)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,

    %% Stop the active consumer.
    ok = amqp10_client:detach_link(Recv5),
    receive {amqp10_event, {link, Recv5, {detached, normal}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    %% The 4th consumer should become the active one because it is up,
    %% has highest prio (1), and most credits (1).
    receive {amqp10_msg, R4, Msg5} ->
                ?assertEqual([<<"5">>], amqp10_msg:body(Msg5)),
                ?assertEqual(Recv4, R4),
                ok = amqp10_client:accept_msg(Recv4, Msg5)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,

    %% Stop the active consumer.
    ok = amqp10_client:detach_link(Recv4),
    receive {amqp10_event, {link, Recv4, {detached, normal}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    %% The only up consumer left is the 1st one (prio 0) which still has 1 credit.
    receive {amqp10_msg, R5, Msg6} ->
                ?assertEqual([<<"6">>], amqp10_msg:body(Msg6)),
                ?assertEqual(Recv1, R5),
                ok = amqp10_client:accept_msg(Recv1, Msg6)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,

    ok = amqp10_client:detach_link(Recv1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

single_active_consumer_classic_queue(Config) ->
    single_active_consumer(<<"classic">>, Config).

single_active_consumer_quorum_queue(Config) ->
    single_active_consumer(<<"quorum">>, Config).

single_active_consumer(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {_, Session, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType},
                              <<"x-single-active-consumer">> => true}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    %% Attach 1 sender and 2 receivers to the queue.
    Address = rabbitmq_amqp_address:queue(QName),
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
    after 30000 -> ct:fail("missing attached")
    end,
    ok = amqp10_client:flow_link_credit(Receiver1, 3, never),

    %% The 2nd consumer will become inactive.
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session,
                        <<"test-receiver-2">>,
                        Address,
                        unsettled),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    ok = amqp10_client:flow_link_credit(Receiver2, 3, never),

    NumMsgs = 5,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true))
     end || N <- lists:seq(1, NumMsgs)],

    %% Only the active consumer should receive messages.
    M1 = receive {amqp10_msg, Receiver1, Msg1} -> ?assertEqual([<<"1">>], amqp10_msg:body(Msg1)),
                                                  Msg1
         after 30000 -> ct:fail({missing_msg, ?LINE})
         end,
    receive {amqp10_msg, Receiver1, Msg2} -> ?assertEqual([<<"2">>], amqp10_msg:body(Msg2))
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver1, Msg3} -> ?assertEqual([<<"3">>], amqp10_msg:body(Msg3))
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, Receiver1, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,
    receive Unexpected0 -> ct:fail("received unexpected ~p", [Unexpected0])
    after 10 -> ok
    end,

    %% Accept only msg 1
    ok = amqp10_client:accept_msg(Receiver1, M1),

    %% Cancelling the active consumer should cause the inactive to become active.
    ok = amqp10_client:detach_link(Receiver1),
    receive {amqp10_event, {link, Receiver1, {detached, normal}}} -> ok
    after 30000 -> ct:fail("missing detached")
    end,

    %% Since Receiver 1 didn't settle msg 2 and msg 3 but detached the link,
    %% both messages should have been requeued.
    %% With single-active-consumer, we expect the original message order to be retained.
    M2b = receive {amqp10_msg, Receiver2, Msg2b} -> ?assertEqual([<<"2">>], amqp10_msg:body(Msg2b)),
                                                    Msg2b
          after 30000 -> ct:fail({missing_msg, ?LINE})
          end,
    receive {amqp10_msg, Receiver2, Msg3b} -> ?assertEqual([<<"3">>], amqp10_msg:body(Msg3b))
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    M4 = receive {amqp10_msg, Receiver2, Msg4} -> ?assertEqual([<<"4">>], amqp10_msg:body(Msg4)),
                                                  Msg4
         after 30000 -> ct:fail({missing_msg, ?LINE})
         end,
    receive {amqp10_event, {link, Receiver2, credit_exhausted}} -> ok
    after 30000 -> ct:fail("expected credit_exhausted")
    end,
    receive Unexpected1 -> ct:fail("received unexpected ~p", [Unexpected1])
    after 10 -> ok
    end,

    %% Receiver2 accepts all 3 messages it received.
    ok = amqp10_client_session:disposition(
           Receiver2,
           amqp10_msg:delivery_id(M2b),
           amqp10_msg:delivery_id(M4),
           true, accepted),
    %% This should leave us with Msg5 in the queue.
    assert_messages(QName, 1, 0, Config),

    ok = amqp10_client:detach_link(Receiver2),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

single_active_consumer_drain_classic_queue(Config) ->
    single_active_consumer_drain(<<"classic">>, Config).

single_active_consumer_drain_quorum_queue(Config) ->
    single_active_consumer_drain(<<"quorum">>, Config).

single_active_consumer_drain(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {_, Session, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType},
                              <<"x-single-active-consumer">> => true}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    %% Attach 1 sender and 2 receivers to the queue.
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),

    %% The 1st consumer will become active.
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session,
                        <<"test-receiver-1">>,
                        Address,
                        unsettled),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    %% The 2nd consumer will become inactive.
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session,
                        <<"test-receiver-2">>,
                        Address,
                        unsettled),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    flush(attached),

    %% Drain both active and inactive consumer for the 1st time.
    ok = amqp10_client:flow_link_credit(Receiver1, 100, never, true),
    ok = amqp10_client:flow_link_credit(Receiver2, 100, never, true),
    receive {amqp10_event, {link, Receiver1, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {amqp10_event, {link, Receiver2, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Send 2 messages.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"dtag1">>, <<"m1">>)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"dtag2">>, <<"m2">>)),
    ok = wait_for_accepts(2),

    %% No consumer should receive a message since both should have 0 credits.
    receive Unexpected0 -> ct:fail("received unexpected ~p", [Unexpected0])
    after 10 -> ok
    end,

    %% Drain both active and inactive consumer for the 2nd time.
    ok = amqp10_client:flow_link_credit(Receiver1, 200, never, true),
    ok = amqp10_client:flow_link_credit(Receiver2, 200, never, true),

    %% Only the active consumer should receive messages.
    receive {amqp10_msg, Receiver1, Msg1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),
                ok = amqp10_client:accept_msg(Receiver1, Msg1)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver1, Msg2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(Msg2)),
                ok = amqp10_client:accept_msg(Receiver1, Msg2)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, Receiver1, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {amqp10_event, {link, Receiver2, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Cancelling the active consumer should cause the inactive to become active.
    ok = amqp10_client:detach_link(Receiver1),
    receive {amqp10_event, {link, Receiver1, {detached, normal}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Send 1 more message.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"dtag3">>, <<"m3">>)),
    ok = wait_for_accepted(<<"dtag3">>),

    %% Our 2nd (now active) consumer should have 0 credits.
    receive Unexpected1 -> ct:fail("received unexpected ~p", [Unexpected1])
    after 10 -> ok
    end,

    %% Drain for the 3rd time.
    ok = amqp10_client:flow_link_credit(Receiver2, 300, never, true),

    receive {amqp10_msg, Receiver2, Msg3} ->
                ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3)),
                ok = amqp10_client:accept_msg(Receiver2, Msg3)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, Receiver2, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:detach_link(Receiver2),
    receive {amqp10_event, {link, Receiver2, {detached, normal}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ?assertMatch({ok, #{message_count := 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

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
%% That's okay given that AMQP receivers can stop a link (figure 2.46) before detaching.
%%
%% Note that this behaviour is different from merely consumer cancellation in
%% AMQP legacy:
%% "After a consumer is cancelled there will be no future deliveries dispatched to it.
%% Note that there can still be "in flight" deliveries dispatched previously.
%% Cancelling a consumer will neither discard nor requeue them."
%% [https://www.rabbitmq.com/docs/consumers#unsubscribing]
detach_requeues_one_session_classic_queue(Config) ->
    detach_requeue_one_session(<<"classic">>, Config).

detach_requeues_one_session_quorum_queue(Config) ->
    detach_requeue_one_session(<<"quorum">>, Config).

detach_requeue_one_session(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 2 receivers to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address, settled),
    ok = wait_for_credit(Sender),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"recv 1">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"recv 2">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
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
    after 30000 -> ct:fail("missing detached")
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
    assert_messages(QName, 0, 0, Config),

    %% Double check that there are no in flight deliveries in the server session.
    [SessionPid] = rpc(Config, rabbit_amqp_session, list_local, []),
    eventually(?_assertEqual(
                  0,
                  begin
                      #{outgoing_unsettled_map := UnsettledMap} = formatted_state(SessionPid),
                      maps:size(UnsettledMap)
                  end)),

    ok = amqp10_client:detach_link(Receiver2),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{message_count = 0} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

detach_requeues_drop_head_classic_queue(Config) ->
    QName1 = <<"q1">>,
    QName2 = <<"q2">>,
    Addr1 = rabbitmq_amqp_address:queue(QName1),
    Addr2 = rabbitmq_amqp_address:queue(QName2),
    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  QName1,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"classic">>},
                                   <<"x-max-length">> => {ulong, 1},
                                   <<"x-overflow">> => {utf8, <<"drop-head">>},
                                   <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                   <<"x-dead-letter-routing-key">> => {utf8, QName2}
                                  }}),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Addr1, settled),
    ok = wait_for_credit(Sender),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session, <<"recv 1">>, Addr1, unsettled),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    {ok, Receiver2} = amqp10_client:attach_receiver_link(Session, <<"recv 2">>, Addr2, unsettled),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
    end,
    flush(attached),

    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag1">>, <<"m1">>, true)),
    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),

    %% x-max-length in classic queues takes only ready but not unacked messages into account.
    %% Since there are 0 ready messages, m2 will be queued.
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"tag2">>, <<"m2">>, true)),
    %% Now, we have 2 messages in total: m1 is unacked and m2 is ready.
    assert_messages(QName1, 2, 1, Config),

    ok = amqp10_client:detach_link(Sender),

    %% Detaching the link should requeue m1.
    %% Since x-max-length is now exceeded, m1 should be dead-lettered to q2.
    ok = amqp10_client:detach_link(Receiver1),
    receive {amqp10_event, {link, Receiver1, {detached, normal}}} -> ok
    after 30000 -> ct:fail("missing detached")
    end,
    assert_messages(QName1, 1, 0, Config), %% m2
    assert_messages(QName2, 1, 0, Config), %% m1

    {ok, Msg1DeadLettered} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1DeadLettered)),
    ok = amqp10_client:accept_msg(Receiver2, Msg1DeadLettered),
    assert_messages(QName2, 0, 0, Config),

    ok = amqp10_client:detach_link(Receiver2),
    {ok, #{message_count := 1}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = close(Init).

detach_requeues_two_connections_classic_queue(Config) ->
    detach_requeues_two_connections(<<"classic">>, Config).

detach_requeues_two_connections_quorum_queue(Config) ->
    detach_requeues_two_connections(<<"quorum">>, Config).

detach_requeues_two_connections(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = <<"/queue/", QName/binary>>,

    %% Connect to new node.
    OpnConf0 = connection_config(0, Config),
    {ok, Connection0} = amqp10_client:open_connection(OpnConf0),
    {ok, Session0} = amqp10_client:begin_session_sync(Connection0),
    %% Connect to old node.
    OpnConf1 = connection_config(1, Config),
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    {ok, Session1} = amqp10_client:begin_session_sync(Connection1),

    %% Declare queue on old node.
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session1, <<"my link pair">>),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    flush(queue_declared),

    %% Attach 1 sender and 2 receivers.
    {ok, Sender} = amqp10_client:attach_sender_link(Session0, <<"sender">>, Address, settled),
    ok = wait_for_credit(Sender),

    {ok, Receiver0} = amqp10_client:attach_receiver_link(Session0, <<"receiver 0">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver0, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client_session:flow(Session0, 1, never),
    ok = amqp10_client:flow_link_credit(Receiver0, 50, never),
    %% Wait for credit being applied to the queue.
    timer:sleep(100),

    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session1, <<"receiver 1">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver1, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver1, 40, never),
    %% Wait for credit being applied to the queue.
    timer:sleep(100),

    NumMsgs = 6,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, true))
     end || N <- lists:seq(1, NumMsgs)],
    ok = amqp10_client:detach_link(Sender),

    %% The queue should serve round robin. Msg3 and Msg5 are in the server session's
    %% outgoing-pending queue since we previously set Receiver0's incoming-window to 1.
    [Msg1] = receive_messages(Receiver0, 1),
    [Msg2, Msg4, Msg6] = receive_messages(Receiver1, 3),
    ?assertEqual([<<"1">>], amqp10_msg:body(Msg1)),
    ?assertEqual([<<"2">>], amqp10_msg:body(Msg2)),
    ?assertEqual([<<"4">>], amqp10_msg:body(Msg4)),
    ?assertEqual([<<"6">>], amqp10_msg:body(Msg6)),
    %% no delivery should be made at this point
    receive {amqp10_msg, _, _} -> ct:fail(unexpected_delivery)
    after 50 -> ok
    end,

    %% Let's detach the receiver on the new node. (Internally on the server,
    %% this sends a consumer removal message from the new node to the old node).
    ok = amqp10_client:detach_link(Receiver0),
    receive {amqp10_event, {link, Receiver0, {detached, normal}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Since Receiver0 hasn't settled any deliveries,
    %% we expect all 3 messages to be re-queued and re-delivered to Receiver1.
    [Msg1b, Msg3, Msg5] = receive_messages(Receiver1, 3),
    ?assertEqual([<<"1">>], amqp10_msg:body(Msg1b)),
    ?assertEqual([<<"3">>], amqp10_msg:body(Msg3)),
    ?assertEqual([<<"5">>], amqp10_msg:body(Msg5)),

    %% Receiver1 accepts all 6 messages.
    ok = amqp10_client_session:disposition(
           Receiver1,
           amqp10_msg:delivery_id(Msg2),
           amqp10_msg:delivery_id(Msg5),
           true, accepted),
    assert_messages(QName, 0, 0, Config),

    %% Double check that there are no in flight deliveries in the server session.
    [SessionPid] = rpc(Config, rabbit_amqp_session, list_local, []),
    eventually(?_assertEqual(
                  {0, 0},
                  begin
                      #{outgoing_unsettled_map := UnsettledMap,
                        outgoing_pending := QueueLen} = formatted_state(SessionPid),
                      {maps:size(UnsettledMap), QueueLen}
                  end)),

    ok = amqp10_client:detach_link(Receiver1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session0),
    ok = end_session_sync(Session1),
    ok = close_connection_sync(Connection0),
    ok = close_connection_sync(Connection1).

resource_alarm_before_session_begin(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),

    %% Set memory alarm before beginning the session.
    DefaultWatermark = rpc(Config, vm_memory_monitor, get_vm_memory_high_watermark, []),
    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0]),
    timer:sleep(100),

    {ok, Session1} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),
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
    ok = wait_for_accepted(Tag1),

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session1),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

resource_alarm_after_session_begin(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    Address = rabbitmq_amqp_address:queue(QName),
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
    ok = wait_for_accepted(<<"t4">>),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver1),
    ok = amqp10_client:detach_link(Receiver2),
    ok = amqp10_client:detach_link(Receiver3),
    ok = end_session_sync(Session1),
    ok = end_session_sync(Session2),
    ok = close_connection_sync(Connection1),
    ok = close_connection_sync(Connection2),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

%% Test case for
%% https://github.com/rabbitmq/rabbitmq-server/issues/12816
resource_alarm_send_many(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    Address = rabbitmq_amqp_address:queue(QName),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% Send many messages while a memory alarm kicks in.
    %% Our expectations are:
    %% 1. At some point, our client's remote-incoming-window should be exceeded because
    %%    RabbitMQ sets its incoming-window to 0 when the alarm kicks in.
    %% 2. No crash.
    {Pid, Ref} = spawn_monitor(?MODULE,
                               send_until_remote_incoming_window_exceeded,
                               [Session, Address]),
    DefaultWatermark = rpc(Config, vm_memory_monitor, get_vm_memory_high_watermark, []),
    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0]),
    receive {'DOWN', Ref, process, Pid, Reason} ->
                ?assertEqual(normal, Reason)
    after 30_000 ->
              ct:fail(send_timeout)
    end,

    %% Clear memory alarm.
    ok = rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [DefaultWatermark]),
    timer:sleep(100),

    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

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
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    Address = rabbitmq_amqp_address:queue(QName),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address, mixed),
    ok = wait_for_credit(Sender),

    PayloadSmallEnough = binary:copy(<<0>>, MaxMessageSize - 20),
    ?assertEqual(ok,
                 amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t1">>, PayloadSmallEnough, false))),
    ok = wait_for_accepted(<<"t1">>),

    PayloadTooLarge = binary:copy(<<0>>, MaxMessageSize + 1),
    ?assertEqual({error, message_size_exceeded},
                 amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t2">>, PayloadTooLarge, false))),
    ?assertEqual({error, message_size_exceeded},
                 amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t3">>, PayloadTooLarge, true))),

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok = rpc(Config, persistent_term, put, [max_message_size, DefaultMaxMessageSize]).

max_message_size_server_to_client(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    Address = rabbitmq_amqp_address:queue(QName),
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
    ok = wait_for_accepted(<<"t1">>),

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
    ok = wait_for_accepted(<<"t2">>),

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
    after 30000 -> flush(missing_ended),
                  ct:fail("did not receive expected error")
    end,

    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

last_queue_confirms(Config) ->
    ClassicQ = <<"my classic queue">>,
    QuorumQ = <<"my quorum queue">>,
    Qs = [ClassicQ, QuorumQ],
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
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

    AddressFanout = rabbitmq_amqp_address:exchange(<<"amq.fanout">>),
    {ok, SenderFanout} = amqp10_client:attach_sender_link(
                           Session, <<"sender-1">>, AddressFanout, unsettled),
    ok = wait_for_credit(SenderFanout),

    AddressClassicQ = rabbitmq_amqp_address:queue(ClassicQ),
    {ok, SenderClassicQ} = amqp10_client:attach_sender_link(
                             Session, <<"sender-2">>, AddressClassicQ, unsettled),
    ok = wait_for_credit(SenderClassicQ),

    DTag1 = <<"t1">>,
    ok = amqp10_client:send_msg(SenderFanout, amqp10_msg:new(DTag1, <<"m1">>, false)),
    receive {amqp10_disposition, {accepted, DTag1}} -> ok
    after 30000 -> ct:fail({missing_accepted, DTag1})
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
    after 30000 -> ct:fail({missing_accepted, DTag3})
    end,
    receive {amqp10_disposition, Unexpected} -> ct:fail({unexpected_disposition, Unexpected})
    after 200 -> ok
    end,

    ok = rabbit_ct_broker_helpers:start_node(Config, 1),
    ok = rabbit_ct_broker_helpers:start_node(Config, 2),
    %% Since the quorum queue has become available, we should now get a confirmation for m2.
    receive {amqp10_disposition, {accepted, DTag2}} -> ok
    after 30_000 -> ct:fail({missing_accepted, DTag2})
    end,

    ok = amqp10_client:detach_link(SenderClassicQ),
    ok = amqp10_client:detach_link(SenderFanout),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = 3},
                 amqp_channel:call(Ch, #'queue.delete'{queue = ClassicQ})),
    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QuorumQ})),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

target_queue_deleted(Config) ->
    ClassicQ = <<"my classic queue">>,
    QuorumQ = <<"my quorum queue">>,
    Qs = [ClassicQ, QuorumQ],
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
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

    Address = rabbitmq_amqp_address:exchange(<<"amq.fanout">>),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    DTag1 = <<"t1">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    receive {amqp10_disposition, {accepted, DTag1}} -> ok
    after 30000 -> ct:fail({missing_accepted, DTag1})
    end,

    N0 = get_node_config(Config, 0, nodename),
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
    after 30_000 -> ct:fail({missing_accepted, DTag2})
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    ?assertEqual(#'queue.delete_ok'{message_count = 2},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QuorumQ})),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

target_classic_queue_down(Config) ->
    ClassicQueueNode = 2,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(
                   Config, ClassicQueueNode),
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session, <<"receiver 1">>, Address),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address, unsettled),
    ok = wait_for_credit(Sender),

    DTag1 = <<"t1">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag1, <<"m1">>, false)),
    ok = wait_for_accepted(DTag1),

    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),

    %% Make classic queue down.
    flush("stopping node"),
    ok = rabbit_ct_broker_helpers:stop_node(Config, ClassicQueueNode),

    %% We expect that the server closes links that receive from classic queues that are down.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_ILLEGAL_STATE},
    receive {amqp10_event, {link, Receiver1, {detached, ExpectedError}}} -> ok
    after 30_000 -> ct:fail({missing_event, ?LINE})
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
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    DTag3 = <<"t3">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag3, <<"m3">>, false)),
    ok = wait_for_accepted(DTag3),
    {ok, Msg3} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m3">>], amqp10_msg:body(Msg3)),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver2),
    ok = delete_queue(Session, QName),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

async_notify_settled_classic_queue(Config) ->
    async_notify(settled, <<"classic">>, Config).

async_notify_settled_quorum_queue(Config) ->
    async_notify(settled, <<"quorum">>, Config).

async_notify_settled_stream(Config) ->
    async_notify(settled, <<"stream">>, Config).

async_notify_unsettled_classic_queue(Config) ->
    %% This test flakes with feature flag 'rabbitmq_4.0.0' disabled.
    %% Link flow control in classic queues with credit API v1 is known to be broken:
    %% https://github.com/rabbitmq/rabbitmq-server/issues/2597
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0'),
    async_notify(unsettled, <<"classic">>, Config).

async_notify_unsettled_quorum_queue(Config) ->
    async_notify(unsettled, <<"quorum">>, Config).

async_notify_unsettled_stream(Config) ->
    async_notify(unsettled, <<"stream">>, Config).

%% Test asynchronous notification, figure 2.45.
async_notify(SenderSettleMode, QType, Config) ->
    %% Place queue leader on the old node.
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(
                   Config, 1),
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
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    NumMsgs = 30,
    [begin
         Bin = integer_to_binary(N),
         ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Bin, Bin, false))
     end || N <- lists:seq(1, NumMsgs)],
    %% Wait for last message to be confirmed.
    ok = wait_for_accepted(integer_to_binary(NumMsgs)),
    flush(settled),
    ok = detach_link_sync(Sender),

    ok = wait_for_local_member(QType, QName, Config),
    Filter = consume_from_first(QType),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address,
                       SenderSettleMode, configuration, Filter),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
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
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% For TRANSFERS from AMQP client to RabbitMQ, this test asserts that a single slow link receiver
%% (slow queue) does not impact other link receivers (fast queues) on the **same** session.
%% (This is unlike AMQP legacy where a single slow queue will block the entire connection.)
link_flow_control(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
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
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    AddressCQ = rabbitmq_amqp_address:queue(CQ),
    AddressQQ = rabbitmq_amqp_address:queue(QQ),
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
    ok = delete_queue(Session, QQ),
    ok = delete_queue(Session, CQ),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

classic_queue_on_old_node(Config) ->
    queue_and_client_different_nodes(1, 0, <<"classic">>, Config).

classic_queue_on_new_node(Config) ->
    queue_and_client_different_nodes(0, 1, <<"classic">>, Config).

quorum_queue_on_old_node(Config) ->
    queue_and_client_different_nodes(1, 0, <<"quorum">>, Config).

quorum_queue_on_new_node(Config) ->
    Versions = rabbit_ct_broker_helpers:rpc_all(Config, rabbit_fifo, version, []),
    case lists:usort(Versions) of
        [_] ->
            %% all are one version, go ahead with the test
            queue_and_client_different_nodes(0, 1, <<"quorum">>, Config);
        _ ->
            {skip, "this test cannot pass with mixed QQ machine versions"}
    end.

%% In mixed version tests, run the queue leader with old code
%% and queue client with new code, or vice versa.
queue_and_client_different_nodes(QueueLeaderNode, ClientNode, QueueType, Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(
                   Config, QueueLeaderNode),
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
    after 30000 -> ct:fail("missing attached")
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

    case rpc(Config, rabbit_feature_flags, is_enabled, ['rabbitmq_4.0.0']) of
        true ->
            %% Send another message and drain.
            Tag = <<"tag">>,
            Body = <<"body">>,
            ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Tag, Body, false)),
            ok = wait_for_accepted(Tag),
            ok = amqp10_client:flow_link_credit(Receiver, 999, never, true),
            [Msg] = receive_messages(Receiver, 1),
            ?assertEqual([Body], amqp10_msg:body(Msg)),
            receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
            after 30000 -> ct:fail("expected credit_exhausted")
            end,
            ok = amqp10_client:accept_msg(Receiver, Msg);
        false ->
            ct:pal("Both quorum queues and classic queues in credit API v1
                   have a known bug that they reply with send_drained
                   before delivering the message.")
    end,

    ExpectedReadyMsgs = 0,
    ?assertEqual(#'queue.delete_ok'{message_count = ExpectedReadyMsgs},
                 amqp_channel:call(Ch, #'queue.delete'{queue = QName})),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok = close_connection_sync(Connection).

maintenance(Config) ->
    {ok, C0} = amqp10_client:open_connection(connection_config(0, Config)),
    {ok, C2} = amqp10_client:open_connection(connection_config(2, Config)),
    receive {amqp10_event, {connection, C0, opened}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {amqp10_event, {connection, C2, opened}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = drain_node(Config, 2),
    receive
        {amqp10_event,
         {connection, C2,
          {closed,
           {internal_error, <<"Connection forced: \"Node was put into maintenance mode\"">>}}}} ->
            ok
    after 30000 ->
              flush(?LINE),
              ct:fail({missing_event, ?LINE})
    end,
    ok = revive_node(Config, 2),

    ok = close_connection_sync(C0).

%% https://github.com/rabbitmq/rabbitmq-server/issues/11841
leader_transfer_quorum_queue_credit_single(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    leader_transfer_credit(QName, <<"quorum">>, 1, Config).

leader_transfer_quorum_queue_credit_batches(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    leader_transfer_credit(QName, <<"quorum">>, 3, Config).

leader_transfer_stream_credit_single(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    leader_transfer_credit(QName, <<"stream">>, 1, Config).

leader_transfer_stream_credit_batches(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    leader_transfer_credit(QName, <<"stream">>, 3, Config).

leader_transfer_credit(QName, QType, Credit, Config) ->
    %% Create queue with leader on node 1.
    {_, _, LinkPair1} = Init = init(1, Config),
    ?awaitMatch(
       {ok, #{type := QType}},
       rabbitmq_amqp_client:declare_queue(
         LinkPair1,
         QName,
         #{arguments => #{<<"x-queue-type">> => {utf8, QType},
                          <<"x-queue-leader-locator">> => {utf8, <<"client-local">>}}}),
       60_000, 5000),
    ok = close(Init),

    OpnConf = connection_config(0, Config),
    {ok, Connection0} = amqp10_client:open_connection(OpnConf),
    try
        {ok, Session0} = amqp10_client:begin_session_sync(Connection0),
        Address = rabbitmq_amqp_address:queue(QName),
        {ok, Sender} = amqp10_client:attach_sender_link(
                         Session0, <<"test-sender">>, Address),
        ok = wait_for_credit(Sender),

        NumMsgs = 30,
        ok = send_messages(Sender, NumMsgs, false),
        ok = wait_for_accepts(NumMsgs),
        ok = detach_link_sync(Sender),

        %% Consume from a follower.
        ok = wait_for_local_member(QType, QName, Config),
        Filter = consume_from_first(QType),
        {ok, Receiver} = amqp10_client:attach_receiver_link(
                           Session0, <<"receiver">>, Address,
                           settled, configuration, Filter),
        flush(receiver_attached),
        %% Top up credits very often during the leader change.
        ok = amqp10_client:flow_link_credit(Receiver, Credit, Credit),

        %% After receiving the 1st message, let's move the leader away from node 1.
        receive_messages(Receiver, 1),
        ok = drain_node(Config, 1),
        %% We expect to receive all remaining messages.
        receive_messages(Receiver, NumMsgs - 1),

        ok = revive_node(Config, 1),
        ok = amqp10_client:detach_link(Receiver),
        ok = delete_queue(Session0, QName),
        ok = end_session_sync(Session0)
    after
        ok = close_connection_sync(Connection0)
    end.

leader_transfer_quorum_queue_send(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    leader_transfer_send(QName, <<"quorum">>, Config).

leader_transfer_stream_send(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    leader_transfer_send(QName, <<"stream">>, Config).

%% Test a leader transfer while we send to the queue.
leader_transfer_send(QName, QType, Config) ->
    %% Create queue with leader on node 1.
    {_, _, LinkPair1} = Init = init(1, Config),
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(
                               LinkPair1,
                               QName,
                               #{arguments => #{<<"x-queue-type">> => {utf8, QType},
                                                <<"x-queue-leader-locator">> => {utf8, <<"client-local">>}}}),
    ok = close(Init),

    %% Send from a follower.
    OpnConf = connection_config(0, Config),
    {ok, Connection0} = amqp10_client:open_connection(OpnConf),
    try
        {ok, Session0} = amqp10_client:begin_session_sync(Connection0),
        Address = rabbitmq_amqp_address:queue(QName),
        {ok, Sender} = amqp10_client:attach_sender_link(
                         Session0, <<"test-sender">>, Address),
        ok = wait_for_credit(Sender),

        NumMsgs = 500,
        ok = send_messages(Sender, NumMsgs, false),
        ok = rabbit_ct_broker_helpers:kill_node(Config, 1),
        ok = wait_for_accepts(NumMsgs),

        ok = rabbit_ct_broker_helpers:start_node(Config, 1),
        ok = detach_link_sync(Sender),
        ok = delete_queue(Session0, QName),
        ok = end_session_sync(Session0)
    after
        close_connection_sync(Connection0)
    end.

%% rabbitmqctl list_connections
%% should list both (Web) AMQP 1.0 and AMQP 0.9.1 connections.
list_connections(Config) ->
    %% Close any open AMQP 0.9.1 connections from previous test cases.
    [ok = rabbit_ct_client_helpers:close_channels_and_connection(Config, Node) || Node <- [0, 1, 2]],

    Connection091 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    ContainerId0 = <<"ID 0">>,
    ContainerId2 = <<"ID 2">>,
    Cfg0 = maps:put(container_id, ContainerId0, connection_config(0, Config)),
    Cfg2 = maps:put(container_id, ContainerId2, connection_config(2, Config)),
    {ok, C0} = amqp10_client:open_connection(Cfg0),
    {ok, C2} = amqp10_client:open_connection(Cfg2),
    receive {amqp10_event, {connection, C0, opened}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {amqp10_event, {connection, C2, opened}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, StdOut0} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["list_connections", "--silent", "protocol"]),
    Protocols0 = re:split(StdOut0, <<"\n">>, [trim]),
    %% Remove any whitespaces.
    Protocols1 = [binary:replace(Subject, <<" ">>, <<>>, [global]) || Subject <- Protocols0],
    Protocols = lists:sort(Protocols1),
    Expected = case web_amqp(Config) of
                   true ->
                       [<<"{'WebAMQP',{1,0}}">>, <<"{'WebAMQP',{1,0}}">>, <<"{0,9,1}">>];
                   false ->
                       [<<"{0,9,1}">>, <<"{1,0}">>, <<"{1,0}">>]
               end,
    ?assertEqual(Expected, Protocols),

    %% CLI should list AMQP 1.0 container-id
    {ok, StdOut1} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["list_connections", "--silent", "container_id"]),
    ContainerIds0 = re:split(StdOut1, <<"\n">>, [trim]),
    ContainerIds = lists:sort(ContainerIds0),
    ?assertEqual([<<>>, ContainerId0, ContainerId2],
                 ContainerIds),

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

    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    CQ = <<"my classic queue">>,
    QQ = <<"my quorum queue">>,
    CQAddress = rabbitmq_amqp_address:queue(CQ),
    QQAddress = rabbitmq_amqp_address:queue(QQ),
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
    ok = wait_for_accepted(<<1>>),

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
    ok = wait_for_accepted(<<2>>),
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
    after 30000 -> ct:fail("server did not close our sending link")
    end,
    receive {amqp10_event, {link, QQReceiver, {detached, ExpectedError}}} -> ok
    after 30000 -> ct:fail("server did not close our receiving link")
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
                     Session, <<"test-sender-fanout">>,
                     rabbitmq_amqp_address:exchange(<<"amq.fanout">>, <<"ignored">>)),
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

    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok = amqp10_client:detach_link(Sender),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

stream_bloom_filter(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(Stream),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    amqp_channel:call(Ch, #'queue.declare'{
                             queue = Stream,
                             durable = true,
                             arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

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
                                            ok = wait_for_accepted(DTag)
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

    ok = delete_queue(Session, Stream),
    ok = close_connection_sync(Connection).

available_messages_classic_queue(Config) ->
    available_messages(<<"classic">>, Config).

available_messages_quorum_queue(Config) ->
    available_messages(<<"quorum">>, Config).

available_messages_stream(Config) ->
    available_messages(<<"stream">>, Config).

available_messages(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),

    %% Attach 1 sender and 1 receiver to the queue.
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"test-receiver">>, Address),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail("missing attached")
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
    Flow0 = #'v1_0.flow'{
               %% Grant 1 credit to the sending queue.
               link_credit = {uint, 1},
               %% Request sending queue to send us a FLOW including available messages.
               echo = true},
    ok = amqp10_client_session:flow_link(Session, OutputHandle, Flow0, never),
    receive_messages(Receiver, 1),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    eventually(?_assertEqual(3, get_available_messages(Receiver))),

    %% Send a FLOW with echo=false and drain=false from client to server.
    %% Even if the server doesn't reply with a FLOW, our client lib should
    %% maintain the 'available' variable correctly.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, false),
    receive_messages(Receiver, 1),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ?assertEqual(2, get_available_messages(Receiver)),

    %% We force the queue to send us a FLOW including available messages
    %% by granting more credits than messages being available and drain=true.
    ok = amqp10_client:flow_link_credit(Receiver, 99, never, true),
    receive_messages(Receiver, 2),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ?assertEqual(0, get_available_messages(Receiver)),

    ok = send_messages(Sender, 5000, false),
    %% We know that Streams only return an approximation for available messages.
    %% The committed Stream offset is queried by chunk ID.
    %% So, we wait here a bit such that the 4th message goes into its own new chunk.
    timer:sleep(50),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"last dtag">>, <<"last msg">>, false)),
    ok = wait_for_accepts(5001),

    Flow1 = #'v1_0.flow'{
               link_credit = {uint, 0},
               echo = false},
    Flow2 = #'v1_0.flow'{
               link_credit = {uint, 1},
               echo = true},
    %% Send both FLOW frames in sequence.
    ok = amqp10_client_session:flow_link(Session, OutputHandle, Flow1, never),
    ok = amqp10_client_session:flow_link(Session, OutputHandle, Flow2, never),
    receive_messages(Receiver, 1),
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    eventually(?_assertEqual(5000, get_available_messages(Receiver))),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

message_interceptors(Config) ->
    Key = message_interceptors,
    ok = rpc(Config, persistent_term, put,
             [Key, [{rabbit_msg_interceptor_routing_node, #{overwrite => false}},
                    {rabbit_msg_interceptor_timestamp, #{overwrite => false,
                                                         incoming => true}}]]),
    Stream = <<"my stream">>,
    QQName = <<"my quorum queue">>,
    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{type := <<"stream">>}} = rabbitmq_amqp_client:declare_queue(
                                      LinkPair,
                                      Stream,
                                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}}),
    {ok, #{type := <<"quorum">>}} = rabbitmq_amqp_client:declare_queue(
                                      LinkPair,
                                      QQName,
                                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}}}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, Stream, <<"amq.fanout">>, <<"ignored">>, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QQName, <<"amq.fanout">>, <<"ignored">>, #{}),
    {ok, StreamReceiver} = amqp10_client:attach_receiver_link(
                             Session, <<"stream receiver">>, rabbitmq_amqp_address:queue(Stream)),
    {ok, QQReceiver} = amqp10_client:attach_receiver_link(
                         Session, <<"qq receiver">>, rabbitmq_amqp_address:queue(QQName)),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"sender">>, rabbitmq_amqp_address:exchange(<<"amq.fanout">>)),
    ok = wait_for_credit(Sender),

    NowMillis = os:system_time(millisecond),
    Tag = <<"tag">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(Tag, <<"body">>)),
    ok = wait_for_accepted(Tag),

    {ok, Msg1} = amqp10_client:get_msg(StreamReceiver),
    {ok, Msg2} = amqp10_client:get_msg(QQReceiver),
    ?assertEqual([<<"body">>], amqp10_msg:body(Msg1)),
    ?assertEqual([<<"body">>], amqp10_msg:body(Msg2)),

    Node = atom_to_binary(get_node_config(Config, 0, nodename)),
    #{<<"x-routed-by">> := Node,
      <<"x-opt-rabbitmq-received-time">> := Millis} = amqp10_msg:message_annotations(Msg1),
    ?assertMatch(
       #{<<"x-routed-by">> := Node,
         <<"x-opt-rabbitmq-received-time">> := Millis},  amqp10_msg:message_annotations(Msg2)),
    ?assert(Millis < NowMillis + 4000),
    ?assert(Millis > NowMillis - 4000),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(StreamReceiver),
    ok = amqp10_client:detach_link(QQReceiver),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, Stream),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QQName),
    ok = close(Init),
    ok = rpc(Config, persistent_term, put, [Key, []]).

trace_classic_queue(Config) ->
    trace(atom_to_binary(?FUNCTION_NAME), <<"classic">>, Config).

trace_stream(Config) ->
    trace(atom_to_binary(?FUNCTION_NAME), <<"stream">>, Config).

trace(Q, QType, Config) ->
    Node = atom_to_binary(get_node_config(Config, 0, nodename)),
    TraceQ = <<"my trace queue">>,
    Qs = [Q, TraceQ],
    RoutingKey = <<"my routing key">>,
    Payload = <<"my payload">>,
    CorrelationId = <<"my correlation 👀"/utf8>>,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = Q,
                                     durable = true,
                                     arguments = [{<<"x-queue-type">>, longstr, QType}]}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = TraceQ}),
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

    {ok, Receiver} = amqp10_client:attach_receiver_link(SessionReceiver,
                                                        <<"test-receiver">>,
                                                        rabbitmq_amqp_address:queue(Q)),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, Sender} = amqp10_client:attach_sender_link(
                     SessionSender,
                     <<"test-sender">>,
                     rabbitmq_amqp_address:exchange(<<"amq.direct">>, RoutingKey)),
    ok = wait_for_credit(Sender),
    Msg0 = amqp10_msg:new(<<"tag 1">>, Payload, true),
    Msg = amqp10_msg:set_message_annotations(
            #{<<"x-cc">> => {list, [{utf8, <<"my CC key">>}]}},
            amqp10_msg:set_properties(#{correlation_id => CorrelationId}, Msg0)),
    ok = amqp10_client:send_msg(Sender, Msg),
    {ok, _} = amqp10_client:get_msg(Receiver),

    timer:sleep(20),
    {#'basic.get_ok'{routing_key = <<"publish.amq.direct">>},
     #amqp_msg{props = #'P_basic'{headers = PublishHeaders},
               payload = Payload}} =
    amqp_channel:call(Ch, #'basic.get'{queue = TraceQ}),
    ?assertMatch(#{<<"exchange_name">> := <<"amq.direct">>,
                   <<"routing_keys">> := [RoutingKey, <<"my CC key">>],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Node,
                   <<"vhost">> := <<"/">>,
                   <<"channel">> := 0,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"correlation_id">> := CorrelationId},
                   <<"routed_queues">> := [Q]},
                 rabbit_misc:amqp_table(PublishHeaders)),

    {#'basic.get_ok'{routing_key = <<"deliver.", Q/binary>>},
     #amqp_msg{props = #'P_basic'{headers = DeliverHeaders},
               payload = Payload}} =
    amqp_channel:call(Ch, #'basic.get'{queue = TraceQ}),
    ?assertMatch(#{<<"exchange_name">> := <<"amq.direct">>,
                   <<"routing_keys">> := [RoutingKey, <<"my CC key">>],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Node,
                   <<"vhost">> := <<"/">>,
                   <<"channel">> := 1,
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
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    [delete_queue(SessionSender, Q0) || Q0 <- Qs],
    ok = end_session_sync(SessionSender),
    ok = end_session_sync(SessionReceiver),
    ok = close_connection_sync(Connection).

%% https://www.rabbitmq.com/docs/validated-user-id
user_id(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:exchange(<<"amq.direct">>, <<"some-routing-key">>),
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
    after 30000 -> flush(missing_ended),
                  ct:fail("did not receive expected error")
    end,

    ok = close_connection_sync(Connection).

message_ttl(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
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
    after 30000 -> ct:fail(delivery_timeout)
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    receive Unexpected -> ct:fail({received_unexpected_message, Unexpected})
    after 5 -> ok
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    ok = delete_queue(Session, QName),
    ok = close_connection_sync(Connection).

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
    Mod = rabbit_net,
    {ok, DefaultVal} = rpc(Config, application, get_env, [App, Par]),
    try
        %% Configure RabbitMQ to use an idle-time-out of 1 second.
        ok = rpc(Config, application, set_env, [App, Par, 1]),

        OpnConf = connection_config(Config),
        {ok, Connection} = amqp10_client:open_connection(OpnConf),
        receive {amqp10_event, {connection, Connection, opened}} -> ok
        after 30000 -> ct:fail({missing_event, ?LINE})
        end,

        %% Mock the server socket to not have received any bytes.
        rabbit_ct_broker_helpers:setup_meck(Config),
        ok = rpc(Config, meck, new, [Mod, [no_link, passthrough]]),
        ok = rpc(Config, meck, expect, [Mod, getstat, fun(_Sock, [recv_oct]) ->
                                                              {ok, [{recv_oct, 999}]};
                                                         (Sock, Opts) ->
                                                              meck:passthrough([Sock, Opts])
                                                      end]),

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
        after 30000 ->
                  ct:fail({missing_event, ?LINE})
        end
    after
        ?assert(rpc(Config, meck, validate, [Mod])),
        ok = rpc(Config, meck, unload, [Mod]),
        ok = rpc(Config, application, set_env, [App, Par, DefaultVal])
    end.

%% Test that the idle timeout threshold is exceeded on the client
%% when no frames are sent from server to client.
idle_time_out_on_client(Config) ->
    OpnConf0 = connection_config(Config),
    %% Request the server to send us frames every second.
    OpnConf = OpnConf0#{idle_time_out => 1000},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    receive Unexpected -> ct:fail({unexpected, Unexpected})
    after 3100 -> ok
    end,
    ?assert(is_process_alive(Connection)),
    %% All good, the server sent us frames every second.

    %% Mock the server to not send anything.
    %% Mocking gen_tcp:send/2 allows this test to work for
    %% * AMQP: https://github.com/rabbitmq/rabbitmq-server/blob/v4.1.0-beta.3/deps/rabbit_common/src/rabbit_net.erl#L174
    %% * AMQP over WebSocket: https://github.com/ninenines/ranch/blob/2.1.0/src/ranch_tcp.erl#L191
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = gen_tcp,
    ok = rpc(Config, meck, new, [Mod, [unstick, no_link, passthrough]]),
    ok = rpc(Config, meck, expect, [Mod, send, 2, ok]),

    %% Our client should time out soon given that the idle-time-out is 1 second.
    receive
        {amqp10_event,
         {connection, Connection,
          {closed, _}}} -> ok
    after 30000 ->
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
    after 30000 -> ct:fail({missing_event, ?LINE})
    end.

handshake_timeout(Config) ->
    App = rabbit,
    Par = ?FUNCTION_NAME,
    {ok, DefaultVal} = rpc(Config, application, get_env, [App, Par]),
    ok = rpc(Config, application, set_env, [App, Par, 200]),
    case web_amqp(Config) of
        true ->
            Port = get_node_config(Config, 0, tcp_port_web_amqp),
            Uri = "ws://127.0.0.1:" ++ integer_to_list(Port) ++ "/ws",
            Ws = rfc6455_client:new(Uri, self(), undefined, ["amqp"]),
            {ok, [{http_response, Resp}]} = rfc6455_client:open(Ws),
            ?assertNotEqual(nomatch, string:prefix(Resp, "HTTP/1.1 101 Switching Protocols")),
            ?assertMatch({close, _}, rfc6455_client:recv(Ws, 1000));
        false ->
            Port = get_node_config(Config, 0, tcp_port_amqp),
            {ok, Socket} = gen_tcp:connect("localhost", Port, [{active, false}]),
            ?assertEqual({error, closed}, gen_tcp:recv(Socket, 0, 1000))
    end,
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
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Since we don't renew our credential, we expect the server to close our connection.
    receive
        {amqp10_event,
         {connection, Connection,
          {closed,
           {unauthorized_access, <<"credential expired">>}}}} -> ok
    after 30_000 ->
              flush(?LINE),
              ct:fail({missing_event, ?LINE})
    end,

    ?assert(rpc(Config, meck, validate, [Mod])),
    ok = rpc(Config, meck, unload, [Mod]).

%% Attaching to an exclusive source queue should fail.
attach_to_exclusive_queue(Config) ->
    QName = <<"my queue">>,
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{queue = QName,
                                                   durable = true,
                                                   exclusive = true}),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, _Receiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver">>, Address),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
              description = {utf8, <<"cannot obtain exclusive access to locked "
                                     "queue 'my queue' in vhost '/'">>}}}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = close_connection_sync(Connection),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch).

dynamic_target_short_link_name(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{container_id := <<"my-container">>,
                        notify_with_performative => true},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% "The address of the target MUST NOT be set" [3.5.4]
    Target = #{address => undefined,
               dynamic => true,
               capabilities => [<<"temporary-queue">>]},
    ShortLinkName = <<"my/sender">>,
    AttachArgs = #{name => ShortLinkName,
                   role => {sender, Target},
                   snd_settle_mode => mixed,
                   rcv_settle_mode => first},
    {ok, Sender} = amqp10_client:attach_link(Session, AttachArgs),
    Addr = receive {amqp10_event, {link, Sender, {attached, Attach}}} ->
                       #'v1_0.attach'{
                          target = #'v1_0.target'{
                                      address = {utf8, Address},
                                      dynamic = true}} = Attach,
                       Address
           after 30000 -> ct:fail({missing_event, ?LINE})
           end,
    %% The client doesn't really care what the address looks like.
    %% However let's do whitebox testing here and check the address format.
    %% We expect the address to contain both container ID and link name since they are short.
    ?assertMatch(<<"/queues/amq.dyn-my-container-my%2Fsender-", _GUID/binary>>, Addr),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t1">>, <<"m1">>)),
    ok = wait_for_accepted(<<"t1">>),

    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"my-receiver">>, Addr, unsettled),
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual(<<"m1">>, amqp10_msg:body_bin(Msg)),
    ok = amqp10_client:accept_msg(Receiver, Msg),

    %% The exclusive queue should be deleted when we close our connection.
    ?assertMatch([_ExclusiveQueue], rpc(Config, rabbit_amqqueue, list, [])),
    ok = close_connection_sync(Connection),
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, []))),
    ok.

dynamic_target_long_link_name(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{container_id := <<"my-container">>,
                        notify_with_performative => true},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% "The address of the target MUST NOT be set" [3.5.4]
    Target = #{address => undefined,
               dynamic => true,
               capabilities => [<<"temporary-queue">>]},
    LongLinkName = binary:copy(<<"z">>, 200),
    AttachArgs = #{name => LongLinkName,
                   role => {sender, Target},
                   snd_settle_mode => mixed,
                   rcv_settle_mode => first},
    {ok, Sender} = amqp10_client:attach_link(Session, AttachArgs),
    Addr = receive {amqp10_event, {link, Sender, {attached, Attach}}} ->
                       #'v1_0.attach'{
                          target = #'v1_0.target'{
                                      address = {utf8, Address},
                                      dynamic = true}} = Attach,
                       Address
           after 30000 -> ct:fail({missing_event, ?LINE})
           end,
    %% The client doesn't really care what the address looks like.
    %% However let's do whitebox testing here and check the address format.
    %% We expect the address to not contain the long link name.
    ?assertMatch(<<"/queues/amq.dyn.gen-", _GUID/binary>>, Addr),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t1">>, <<"m1">>)),
    ok = wait_for_accepted(<<"t1">>),

    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"my-receiver">>, Addr, unsettled),
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual(<<"m1">>, amqp10_msg:body_bin(Msg)),
    ok = amqp10_client:accept_msg(Receiver, Msg),
    flush(accepted),

    %% Since RabbitMQ uses the delete-on-close lifetime policy, the exclusive queue should be
    %% "deleted at the point that the link which caused its creation ceases to exist" [3.5.10]
    ok = amqp10_client:detach_link(Sender),
    receive {amqp10_event, {link, Receiver, {detached, Detach}}} ->
                ?assertMatch(
                   #'v1_0.detach'{error = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED}},
                   Detach)
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = close_connection_sync(Connection).

%% Test the following RPC workflow:
%% RPC client -> queue         -> RPC server
%% RPC server -> dynamic queue -> RPC client
dynamic_source_rpc(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{container_id := <<"rpc-client">>,
                        notify_with_performative => true},
    {ok, ConnectionClient} = amqp10_client:open_connection(OpnConf),
    {ok, SessionClient} = amqp10_client:begin_session_sync(ConnectionClient),

    %% "The address of the source MUST NOT be set" [3.5.3]
    Source = #{address => undefined,
               dynamic => true,
               capabilities => [<<"temporary-queue">>],
               durable => none},
    AttachArgs = #{name => <<"rpc-client-receiver🥕"/utf8>>,
                   role => {receiver, Source, self()},
                   snd_settle_mode => unsettled,
                   rcv_settle_mode => first,
                   filter => #{}},
    {ok, ReceiverClient} = amqp10_client:attach_link(SessionClient, AttachArgs),
    RespAddr = receive {amqp10_event, {link, ReceiverClient, {attached, Attach}}} ->
                           #'v1_0.attach'{
                              source = #'v1_0.source'{
                                          address = {utf8, Address},
                                          dynamic = true}} = Attach,
                           Address
               after 30000 -> ct:fail({missing_event, ?LINE})
               end,
    %% The client doesn't really care what the address looks like.
    %% However let's do whitebox testing here and check the address format.
    %% We expect the address to contain both container ID and link name since they are short.
    ?assertMatch(<<"/queues/amq.dyn-rpc-client-rpc-client-receiver", _CarrotAndGUID/binary>>,
                 RespAddr),

    %% Let's use a separate connection for the RPC server.
    {_, SessionServer, LinkPair} = RpcServer = init(Config),
    ReqQName = atom_to_binary(?FUNCTION_NAME),
    ReqAddr = rabbitmq_amqp_address:queue(ReqQName),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, ReqQName, #{}),
    {ok, ReceiverServer} = amqp10_client:attach_receiver_link(SessionServer, <<"rpc-server-receiver">>, ReqAddr, unsettled),
    {ok, SenderServer} = amqp10_client:attach_sender_link(SessionServer, <<"rpc-server-sender">>, null),
    ok = wait_for_credit(SenderServer),

    {ok, SenderClient} = amqp10_client:attach_sender_link(SessionClient, <<"rpc-client-sender">>, ReqAddr),
    wait_for_credit(SenderClient),
    flush(attached),

    ok = amqp10_client:send_msg(
           SenderClient,
           amqp10_msg:set_properties(
             #{reply_to => RespAddr},
             amqp10_msg:new(<<"t1">>, <<"hello">>))),
    ok = wait_for_accepted(<<"t1">>),

    {ok, ReqMsg} = amqp10_client:get_msg(ReceiverServer),
    ReqBody = amqp10_msg:body_bin(ReqMsg),
    RespBody = string:uppercase(ReqBody),
    #{reply_to := ReplyTo} = amqp10_msg:properties(ReqMsg),
    ok = amqp10_client:send_msg(
           SenderServer,
           amqp10_msg:set_properties(
             #{to => ReplyTo},
             amqp10_msg:new(<<"t2">>, RespBody))),
    ok = wait_for_accepted(<<"t2">>),
    ok = amqp10_client:accept_msg(ReceiverServer, ReqMsg),

    {ok, RespMsg} = amqp10_client:get_msg(ReceiverClient),
    ?assertEqual(<<"HELLO">>, amqp10_msg:body_bin(RespMsg)),
    ok = amqp10_client:accept_msg(ReceiverClient, RespMsg),

    ok = detach_link_sync(ReceiverServer),
    ok = detach_link_sync(SenderClient),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, ReqQName),
    ok = detach_link_sync(SenderServer),
    ok = close(RpcServer),
    ok = close_connection_sync(ConnectionClient).

dynamic_terminus_delete(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session_sync(Connection),
    {ok, Session2} = amqp10_client:begin_session_sync(Connection),

    Terminus = #{address => undefined,
                 dynamic => true,
                 capabilities => [<<"temporary-queue">>],
                 durable => none},
    RcvAttachArgs = #{role => {receiver, Terminus, self()},
                      snd_settle_mode => unsettled,
                      rcv_settle_mode => first,
                      filter => #{}},
    SndAttachArgs = #{role => {sender, Terminus},
                      snd_settle_mode => mixed,
                      rcv_settle_mode => first},
    RcvAttachArgs1 = RcvAttachArgs#{name => <<"receiver 1">>},
    RcvAttachArgs2 = RcvAttachArgs#{name => <<"receiver 2">>},
    RcvAttachArgs3 = RcvAttachArgs#{name => <<"receiver 3">>},
    SndAttachArgs1 = SndAttachArgs#{name => <<"sender 1">>},
    SndAttachArgs2 = SndAttachArgs#{name => <<"sender 2">>},
    SndAttachArgs3 = SndAttachArgs#{name => <<"sender 3">>},
    {ok, _R1} = amqp10_client:attach_link(Session1, RcvAttachArgs1),
    {ok, _R2} = amqp10_client:attach_link(Session2, RcvAttachArgs2),
    {ok, R3} = amqp10_client:attach_link(Session2, RcvAttachArgs3),
    {ok, _S1} = amqp10_client:attach_link(Session1, SndAttachArgs1),
    {ok, _S2} = amqp10_client:attach_link(Session2, SndAttachArgs2),
    {ok, S3} = amqp10_client:attach_link(Session2, SndAttachArgs3),
    [receive {amqp10_event, {link, _LinkRef, attached}} -> ok
     after 30000 -> ct:fail({missing_event, ?LINE})
     end
     || _ <- lists:seq(1, 6)],

    %% We should now have 6 exclusive queues.
    ?assertEqual(6, rpc(Config, rabbit_amqqueue, count, [])),

    %% Since RabbitMQ uses the delete-on-close lifetime policy, the exclusive queue should be
    %% "deleted at the point that the link which caused its creation ceases to exist" [3.5.10]
    ok = detach_link_sync(R3),
    ok = detach_link_sync(S3),
    ?assertEqual(4, rpc(Config, rabbit_amqqueue, count, [])),

    %% When a session is ended, the sessions's links cease to exist.
    ok = end_session_sync(Session2),
    eventually(?_assertEqual(2, rpc(Config, rabbit_amqqueue, count, []))),

    %% When a connection is closed, the connection's links cease to exist.
    ok = close_connection_sync(Connection),
    eventually(?_assertEqual(0, rpc(Config, rabbit_amqqueue, count, []))),
    ok.

priority_classic_queue(Config) ->
    QArgs = #{<<"x-queue-type">> => {utf8, <<"classic">>},
              <<"x-max-priority">> => {ulong, 10}},
    priority(QArgs, Config).

priority_quorum_queue(Config) ->
    QArgs = #{<<"x-queue-type">> => {utf8, <<"quorum">>}},
    priority(QArgs, Config).

priority(QArgs, Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{arguments => QArgs}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    %% We don't set a priority on Msg1.
    %% According to the AMQP spec, the default priority is 4.
    Msg1 = amqp10_msg:set_headers(
             #{durable => true},
             amqp10_msg:new(<<"t1">>, <<"low prio">>)),
    %% Quorum queues implement 2 distinct priority levels.
    %% "if 2 distinct priorities are implemented, then levels 0 to 4 are equivalent,
    %% and levels 5 to 9 are equivalent and levels 4 and 5 are distinct." [§3.2.1]
    %% Therefore, when we set a priority of 5 on Msg2, Msg2 will have a higher priority
    %% than the default priority 4 of Msg1.
    Msg2 = amqp10_msg:set_headers(
             #{priority => 5,
               durable => true},
             amqp10_msg:new(<<"t2">>, <<"high prio">>)),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = wait_for_accepts(2),
    flush(accepted),

    %% The high prio Msg2 should overtake the low prio Msg1 and therefore be delivered first.
    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session, <<"receiver 1">>, Address, unsettled),
    {ok, In1} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"high prio">>], amqp10_msg:body(In1)),
    ?assertEqual(5, amqp10_msg:header(priority, In1)),
    ?assert(amqp10_msg:header(durable, In1)),
    ok = amqp10_client:accept_msg(Receiver1, In1),

    {ok, Receiver2} = amqp10_client:attach_receiver_link(Session, <<"receiver 2">>, Address, settled),
    {ok, In2} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"low prio">>], amqp10_msg:body(In2)),
    ?assert(amqp10_msg:header(durable, In2)),

    ok = amqp10_client:detach_link(Receiver1),
    ok = amqp10_client:detach_link(Receiver2),
    ok = amqp10_client:detach_link(Sender),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

dead_letter_headers_exchange(Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QName1 = <<"q1">>,
    QName2 = <<"q2">>,
    {ok, _} = rabbitmq_amqp_client:declare_queue(
                LinkPair,
                QName1,
                #{arguments => #{<<"x-dead-letter-exchange">> => {utf8, <<"amq.headers">>},
                                 <<"x-message-ttl">> => {ulong, 0}}}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),
    ok =  rabbitmq_amqp_client:bind_queue(LinkPair, QName2, <<"amq.headers">>, <<>>,
                                          #{<<"my key">> => {uint, 5},
                                            <<"x-my key">> => {uint, 6},
                                            <<"x-match">> => {utf8, <<"all-with-x">>}}),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"test-sender">>, rabbitmq_amqp_address:queue(QName1)),
    wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"my receiver">>, rabbitmq_amqp_address:queue(QName2), settled),

    %% Test M1 with properties section.
    M1 = amqp10_msg:set_message_annotations(
           #{<<"x-my key">> => 6},
           amqp10_msg:set_properties(
             #{message_id => <<"my ID">>},
             amqp10_msg:set_application_properties(
               #{<<"my key">> => 5},
               amqp10_msg:new(<<"tag 1">>, <<"m1">>, false)))),
    %% Test M2 without properties section.
    M2 = amqp10_msg:set_message_annotations(
           #{<<"x-my key">> => 6},
           amqp10_msg:set_application_properties(
             #{<<"my key">> => 5},
             amqp10_msg:new(<<"tag 2">>, <<"m2">>, false))),
    %% M3 should be dropped due to missing x-header.
    M3 = amqp10_msg:set_application_properties(
           #{<<"my key">> => 5},
           amqp10_msg:new(<<"tag 3">>, <<"m3">>, false)),
    %% M4 should be dropped due to missing header.
    M4 = amqp10_msg:set_message_annotations(
           #{<<"x-my key">> => 6},
           amqp10_msg:new(<<"tag 4">>, <<"m4">>, false)),

    Now = os:system_time(millisecond),
    [ok = amqp10_client:send_msg(Sender, M) || M <- [M1, M2, M3, M4]],
    ok = wait_for_accepts(4),
    flush(accepted),

    ok = amqp10_client:flow_link_credit(Receiver, 4, never),
    [Msg1, Msg2] = receive_messages(Receiver, 2),
    ?assertEqual(<<"m1">>, amqp10_msg:body_bin(Msg1)),
    ?assertEqual(<<"m2">>, amqp10_msg:body_bin(Msg2)),
    ?assertEqual(#{message_id => <<"my ID">>}, amqp10_msg:properties(Msg1)),
    ?assertEqual(0, maps:size(amqp10_msg:properties(Msg2))),
    case rpc(Config, rabbit_feature_flags, is_enabled, [message_containers_deaths_v2]) of
        true ->
            ?assertMatch(
               #{<<"x-first-death-queue">> := QName1,
                 <<"x-first-death-exchange">> := <<>>,
                 <<"x-first-death-reason">> := <<"expired">>,
                 <<"x-last-death-queue">> := QName1,
                 <<"x-last-death-exchange">> := <<>>,
                 <<"x-last-death-reason">> := <<"expired">>,
                 <<"x-opt-deaths">> := {array,
                                        map,
                                        [{map,
                                          [
                                           {{symbol, <<"queue">>}, {utf8, QName1}},
                                           {{symbol, <<"reason">>}, {symbol, <<"expired">>}},
                                           {{symbol, <<"count">>}, {ulong, 1}},
                                           {{symbol, <<"first-time">>}, {timestamp, Timestamp}},
                                           {{symbol, <<"last-time">>}, {timestamp, Timestamp}},
                                           {{symbol, <<"exchange">>},{utf8, <<>>}},
                                           {{symbol, <<"routing-keys">>}, {array, utf8, [{utf8, QName1}]}}
                                          ]}]}
                } when is_integer(Timestamp) andalso
                       Timestamp > Now - 5000 andalso
                       Timestamp < Now + 5000,
                       amqp10_msg:message_annotations(Msg1));
        false ->
            ok
    end,

    %% We expect M3 and M4 to get dropped.
    receive Unexp -> ct:fail({unexpected, Unexp})
    after 10 -> ok
    end,

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = close(Init).

dead_letter_reject(Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QName1 = <<"q1">>,
    QName2 = <<"q2">>,
    QName3 = <<"q3">>,
    {ok, #{type := <<"quorum">>}} = rabbitmq_amqp_client:declare_queue(
                                      LinkPair,
                                      QName1,
                                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                                       <<"x-message-ttl">> => {ulong, 20},
                                                       <<"x-overflow">> => {utf8, <<"reject-publish">>},
                                                       <<"x-dead-letter-strategy">> => {utf8, <<"at-least-once">>},
                                                       <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                                       <<"x-dead-letter-routing-key">> => {utf8, QName2}
                                                      }}),
    {ok, #{type := <<"quorum">>}} = rabbitmq_amqp_client:declare_queue(
                                      LinkPair,
                                      QName2,
                                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                                       <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                                       <<"x-dead-letter-routing-key">> => {utf8, QName3}
                                                      }}),
    {ok, #{type := <<"classic">>}} = rabbitmq_amqp_client:declare_queue(
                                       LinkPair,
                                       QName3,
                                       #{arguments => #{<<"x-queue-type">> => {utf8, <<"classic">>},
                                                        <<"x-message-ttl">> => {ulong, 20},
                                                        <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                                        <<"x-dead-letter-routing-key">> => {utf8, QName1}
                                                       }}),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, rabbitmq_amqp_address:queue(QName2), unsettled),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"sender">>, rabbitmq_amqp_address:queue(QName1), unsettled),
    wait_for_credit(Sender),
    Tag = <<"my tag">>,
    Body = <<"my body">>,
    M = amqp10_msg:new(Tag, Body),
    ok = amqp10_client:send_msg(Sender, M),
    ok = wait_for_accepted(Tag),

    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    ?assertMatch(#{delivery_count := 0}, amqp10_msg:headers(Msg1)),
    ok = amqp10_client:settle_msg(Receiver, Msg1, rejected),

    {ok, Msg2} = amqp10_client:get_msg(Receiver),
    ?assertMatch(#{delivery_count := 1,
                   first_acquirer := false}, amqp10_msg:headers(Msg2)),
    ok = amqp10_client:settle_msg(Receiver, Msg2,
                                  {modified, true, true,
                                   #{<<"x-opt-thekey">> => <<"val">>}}),

    {ok, Msg3} = amqp10_client:get_msg(Receiver),
    ?assertMatch(#{delivery_count := 2,
                   first_acquirer := false}, amqp10_msg:headers(Msg3)),
    ?assertEqual(Body, amqp10_msg:body_bin(Msg3)),
    Annotations = amqp10_msg:message_annotations(Msg3),
    ?assertMatch(
       #{<<"x-opt-thekey">> := <<"val">>,
         <<"x-first-death-queue">> := QName1,
         <<"x-first-death-exchange">> := <<>>,
         <<"x-first-death-reason">> := <<"expired">>,
         <<"x-last-death-queue">> := QName1,
         <<"x-last-death-exchange">> := <<>>,
         <<"x-last-death-reason">> := <<"expired">>},
       Annotations),
    %% The array should be ordered by death recency.
    {ok, {array, map, [D1, D3, D2]}} = maps:find(<<"x-opt-deaths">>, Annotations),
    {map, [
           {{symbol, <<"queue">>}, {utf8, QName1}},
           {{symbol, <<"reason">>}, {symbol, <<"expired">>}},
           {{symbol, <<"count">>}, {ulong, 3}},
           {{symbol, <<"first-time">>}, {timestamp, Ts1}},
           {{symbol, <<"last-time">>}, {timestamp, Ts2}},
           {{symbol, <<"exchange">>},{utf8, <<>>}},
           {{symbol, <<"routing-keys">>}, {array, utf8, [{utf8, QName1}]}}
          ]} = D1,
    {map, [
           {{symbol, <<"queue">>}, {utf8, QName2}},
           {{symbol, <<"reason">>}, {symbol, <<"rejected">>}},
           {{symbol, <<"count">>}, {ulong, 2}},
           {{symbol, <<"first-time">>}, {timestamp, Ts3}},
           {{symbol, <<"last-time">>}, {timestamp, Ts4}},
           {{symbol, <<"exchange">>},{utf8, <<>>}},
           {{symbol, <<"routing-keys">>}, {array, utf8, [{utf8, QName2}]}}
          ]} = D2,
    {map, [
           {{symbol, <<"queue">>}, {utf8, QName3}},
           {{symbol, <<"reason">>}, {symbol, <<"expired">>}},
           {{symbol, <<"count">>}, {ulong, 2}},
           {{symbol, <<"first-time">>}, {timestamp, Ts5}},
           {{symbol, <<"last-time">>}, {timestamp, Ts6}},
           {{symbol, <<"exchange">>},{utf8, <<>>}},
           {{symbol, <<"routing-keys">>}, {array, utf8, [{utf8, QName3}]}}
          ]} = D3,
    ?assertEqual([Ts1, Ts3, Ts5, Ts4, Ts6, Ts2],
                 lists:sort([Ts1, Ts2, Ts3, Ts4, Ts5, Ts6])),
    ok = amqp10_client:settle_msg(Receiver, Msg3, accepted),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName3),
    ok = close(Init).

dead_letter_reject_message_order_classic_queue(Config) ->
    dead_letter_reject_message_order(<<"classic">>, Config).

dead_letter_reject_message_order_quorum_queue(Config) ->
    dead_letter_reject_message_order(<<"quorum">>, Config).

dead_letter_reject_message_order(QType, Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QName1 = <<"q1">>,
    QName2 = <<"q2">>,
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(
                               LinkPair,
                               QName1,
                               #{arguments => #{<<"x-queue-type">> => {utf8, QType},
                                                <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                                <<"x-dead-letter-routing-key">> => {utf8, QName2}
                                               }}),
    %% We don't care about the target dead letter queue type.
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"sender">>, rabbitmq_amqp_address:queue(QName1), settled),
    wait_for_credit(Sender),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, rabbitmq_amqp_address:queue(QName1), unsettled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, rabbitmq_amqp_address:queue(QName2), unsettled),

    [begin
         Bin = integer_to_binary(N),
         Msg = amqp10_msg:new(Bin, Bin, true),
         ok = amqp10_client:send_msg(Sender, Msg)
     end || N <- lists:seq(1, 5)],

    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    {ok, Msg2} = amqp10_client:get_msg(Receiver1),
    {ok, _Msg3} = amqp10_client:get_msg(Receiver1),
    {ok, Msg4} = amqp10_client:get_msg(Receiver1),
    {ok, Msg5} = amqp10_client:get_msg(Receiver1),
    assert_messages(QName1, 5, 5, Config),

    %% Reject messages in the following order: 2, 3, 4, 1, 5
    ok = amqp10_client_session:disposition(
           Receiver1,
           amqp10_msg:delivery_id(Msg2),
           amqp10_msg:delivery_id(Msg4),
           true,
           rejected),
    ok = amqp10_client_session:disposition(
           Receiver1,
           amqp10_msg:delivery_id(Msg1),
           amqp10_msg:delivery_id(Msg5),
           true,
           rejected),

    assert_messages(QName1, 0, 0, Config),
    %% All 5 messages should be in the dead letter queue.
    assert_messages(QName2, 5, 0, Config),

    {ok, MsgDead2} = amqp10_client:get_msg(Receiver2),
    {ok, MsgDead3} = amqp10_client:get_msg(Receiver2),
    {ok, MsgDead4} = amqp10_client:get_msg(Receiver2),
    {ok, MsgDead1} = amqp10_client:get_msg(Receiver2),
    {ok, MsgDead5} = amqp10_client:get_msg(Receiver2),
    assert_messages(QName2, 5, 5, Config),

    %% Messages should be dead lettered in the order we rejected.
    ?assertEqual(<<"2">>, amqp10_msg:body_bin(MsgDead2)),
    ?assertEqual(<<"3">>, amqp10_msg:body_bin(MsgDead3)),
    ?assertEqual(<<"4">>, amqp10_msg:body_bin(MsgDead4)),
    ?assertEqual(<<"1">>, amqp10_msg:body_bin(MsgDead1)),
    ?assertEqual(<<"5">>, amqp10_msg:body_bin(MsgDead5)),

    %% Accept all messages in the dead letter queue.
    ok = amqp10_client_session:disposition(
           Receiver2,
           amqp10_msg:delivery_id(MsgDead2),
           amqp10_msg:delivery_id(MsgDead5),
           true,
           accepted),
    assert_messages(QName2, 0, 0, Config),

    ok = amqp10_client:detach_link(Receiver1),
    ok = amqp10_client:detach_link(Receiver2),
    ok = amqp10_client:detach_link(Sender),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = close(Init).

dead_letter_reject_many_message_order_classic_queue(Config) ->
    dead_letter_reject_many_message_order(<<"classic">>, Config).

dead_letter_reject_many_message_order_quorum_queue(Config) ->
    dead_letter_reject_many_message_order(<<"quorum">>, Config).

dead_letter_reject_many_message_order(QType, Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QName1 = <<"q1">>,
    QName2 = <<"q2">>,
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(
                               LinkPair,
                               QName1,
                               #{arguments => #{<<"x-queue-type">> => {utf8, QType},
                                                <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                                <<"x-dead-letter-routing-key">> => {utf8, QName2}
                                               }}),
    %% We don't care about the target dead letter queue type.
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session, <<"sender">>, rabbitmq_amqp_address:queue(QName1), settled),
    wait_for_credit(Sender),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, rabbitmq_amqp_address:queue(QName1), unsettled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, rabbitmq_amqp_address:queue(QName2), unsettled),

    Num = 100,
    Bins = [integer_to_binary(N) || N <- lists:seq(1, Num)],
    [begin
         Msg = amqp10_msg:new(Bin, Bin, true),
         ok = amqp10_client:send_msg(Sender, Msg)
     end || Bin <- Bins],

    ok = amqp10_client:flow_link_credit(Receiver1, Num, never),
    Msgs = receive_messages(Receiver1, Num),
    [begin
         {ExpectedBody, Msg} = Elem,
         ?assertEqual(ExpectedBody, amqp10_msg:body_bin(Msg))
     end || Elem <- lists:zip(Bins, Msgs)],
    assert_messages(QName1, Num, Num, Config),

    %% Accept the 2nd message.
    ok = amqp10_client:accept_msg(Receiver1, lists:nth(2, Msgs)),
    %% Reject all other messages.
    %% Here, we intentionally settle a range larger than the number of unacked messages.
    ok = amqp10_client_session:disposition(
           Receiver1,
           amqp10_msg:delivery_id(hd(Msgs)),
           amqp10_msg:delivery_id(lists:last(Msgs)),
           true,
           rejected),

    assert_messages(QName1, 0, 0, Config),
    assert_messages(QName2, Num - 1, 0, Config),

    ok = amqp10_client:flow_link_credit(Receiver2, Num, never),
    DeadLetteredMsgs = receive_messages(Receiver2, Num - 1),
    %% Messages should be dead lettered in the order we rejected.
    ExpectedBodies = [hd(Bins) | lists:nthtail(2, Bins)],
    [begin
         {ExpectedBody, Msg} = Elem,
         ?assertEqual(ExpectedBody, amqp10_msg:body_bin(Msg))
     end || Elem <- lists:zip(ExpectedBodies, DeadLetteredMsgs)],
    assert_messages(QName2, Num - 1, Num - 1, Config),

    %% Accept the 10th message in the dead letter queue.
    ok = amqp10_client:accept_msg(Receiver2, lists:nth(10, DeadLetteredMsgs)),
    assert_messages(QName2, Num - 2, Num - 2, Config),
    %% Accept all other messages.
    %% Here, we intentionally settle a range larger than the number of unacked messages.
    ok = amqp10_client_session:disposition(
           Receiver2,
           amqp10_msg:delivery_id(hd(DeadLetteredMsgs)),
           amqp10_msg:delivery_id(lists:last(DeadLetteredMsgs)),
           true,
           accepted),
    assert_messages(QName2, 0, 0, Config),

    ok = amqp10_client:detach_link(Receiver1),
    ok = amqp10_client:detach_link(Receiver2),
    ok = amqp10_client:detach_link(Sender),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = close(Init).

%% Dead letter from a quorum queue into a stream.
dead_letter_into_stream(Config) ->
    {Connection0, Session0, LinkPair0} = init(0, Config),
    {Connection1, Session1, LinkPair1} = init(1, Config),
    QName0 = <<"q0">>,
    QName1 = <<"q1">>,
    {ok, #{type := <<"quorum">>}} = rabbitmq_amqp_client:declare_queue(
                                      LinkPair0,
                                      QName0,
                                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                                       <<"x-quorum-initial-group-size">> => {ulong, 1},
                                                       <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                                       <<"x-dead-letter-routing-key">> => {utf8, QName1}
                                                      }}),
    ?awaitMatch(
       {ok, #{type := <<"stream">>}},
       rabbitmq_amqp_client:declare_queue(
         LinkPair1,
         QName1,
         #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>},
                          <<"x-initial-cluster-size">> => {ulong, 1}
                         }}),
       60_000, 5000),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session1, <<"receiver">>, <<"/amq/queue/", QName1/binary>>,
                       settled, configuration,
                       #{<<"rabbitmq:stream-offset-spec">> => <<"first">>}),
    {ok, Sender} = amqp10_client:attach_sender_link(
                     Session0, <<"sender">>, rabbitmq_amqp_address:queue(QName0)),
    wait_for_credit(Sender),
    Ttl = 10,
    M = amqp10_msg:set_headers(
          #{durable => true,
            ttl => Ttl},
          amqp10_msg:new(<<"tag">>, <<"msg">>, true)),
    Now = os:system_time(millisecond),
    ok = amqp10_client:send_msg(Sender, M),

    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual(<<"msg">>, amqp10_msg:body_bin(Msg)),
    ?assertMatch(
       #{<<"x-first-death-queue">> := QName0,
         <<"x-first-death-exchange">> := <<>>,
         <<"x-first-death-reason">> := <<"expired">>,
         <<"x-last-death-queue">> := QName0,
         <<"x-last-death-exchange">> := <<>>,
         <<"x-last-death-reason">> := <<"expired">>,
         <<"x-opt-deaths">> := {array,
                                map,
                                [{map,
                                  [
                                   {{symbol, <<"ttl">>}, {uint, Ttl}},
                                   {{symbol, <<"queue">>}, {utf8, QName0}},
                                   {{symbol, <<"reason">>}, {symbol, <<"expired">>}},
                                   {{symbol, <<"count">>}, {ulong, 1}},
                                   {{symbol, <<"first-time">>}, {timestamp, Timestamp}},
                                   {{symbol, <<"last-time">>}, {timestamp, Timestamp}},
                                   {{symbol, <<"exchange">>},{utf8, <<>>}},
                                   {{symbol, <<"routing-keys">>}, {array, utf8, [{utf8, QName0}]}}
                                  ]}]}
        } when is_integer(Timestamp) andalso
               Timestamp > Now - 5000 andalso
               Timestamp < Now + 5000,
               amqp10_msg:message_annotations(Msg)),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair0, QName0),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair1, QName1),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair0),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = end_session_sync(Session0),
    ok = end_session_sync(Session1),
    ok = close_connection_sync(Connection0),
    ok = close_connection_sync(Connection1).

accept_multiple_message_order_classic_queue(Config) ->
    accept_multiple_message_order(<<"classic">>, Config).

accept_multiple_message_order_quorum_queue(Config) ->
    accept_multiple_message_order(<<"quorum">>, Config).

accept_multiple_message_order(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address, settled),
    ok = wait_for_credit(Sender),
    [begin
         Bin = integer_to_binary(N),
         Msg = amqp10_msg:new(Bin, Bin, true),
         ok = amqp10_client:send_msg(Sender, Msg)
     end || N <- lists:seq(1, 5)],
    ok = amqp10_client:detach_link(Sender),
    assert_messages(QName, 5, 0, Config),

    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, QName, unsettled),
    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    {ok, Msg2} = amqp10_client:get_msg(Receiver),
    {ok, _Msg3} = amqp10_client:get_msg(Receiver),
    {ok, Msg4} = amqp10_client:get_msg(Receiver),
    {ok, Msg5} = amqp10_client:get_msg(Receiver),
    assert_messages(QName, 5, 5, Config),

    %% Accept messages out of order.
    ok = amqp10_client_session:disposition(
           Receiver,
           amqp10_msg:delivery_id(Msg2),
           amqp10_msg:delivery_id(Msg4),
           true,
           accepted),
    assert_messages(QName, 2, 2, Config),

    ok = amqp10_client:accept_msg(Receiver, Msg5),
    assert_messages(QName, 1, 1, Config),

    ok = amqp10_client:accept_msg(Receiver, Msg1),
    assert_messages(QName, 0, 0, Config),

    ok = amqp10_client:detach_link(Receiver),
    ?assertMatch({ok, #{message_count := 0}}, rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

release_multiple_message_order_classic_queue(Config) ->
    release_multiple_message_order(<<"classic">>, Config).

release_multiple_message_order_quorum_queue(Config) ->
    release_multiple_message_order(<<"quorum">>, Config).

release_multiple_message_order(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address, settled),
    ok = wait_for_credit(Sender),
    [begin
         Bin = integer_to_binary(N),
         Msg = amqp10_msg:new(Bin, Bin, true),
         ok = amqp10_client:send_msg(Sender, Msg)
     end || N <- lists:seq(1, 4)],
    ok = amqp10_client:detach_link(Sender),
    assert_messages(QName, 4, 0, Config),

    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, QName, unsettled),
    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    {ok, Msg2} = amqp10_client:get_msg(Receiver),
    {ok, Msg3} = amqp10_client:get_msg(Receiver),
    {ok, Msg4} = amqp10_client:get_msg(Receiver),
    assert_messages(QName, 4, 4, Config),

    %% Release messages out of order.
    ok = amqp10_client_session:disposition(
           Receiver,
           amqp10_msg:delivery_id(Msg2),
           amqp10_msg:delivery_id(Msg3),
           true,
           released),
    %% Both messages should be requeued and redelivered.
    assert_messages(QName, 4, 2, Config),

    {ok, Msg2b} = amqp10_client:get_msg(Receiver),
    {ok, Msg3b} = amqp10_client:get_msg(Receiver),
    assert_messages(QName, 4, 4, Config),
    ?assertEqual([<<"2">>], amqp10_msg:body(Msg2b)),
    ?assertEqual([<<"3">>], amqp10_msg:body(Msg3b)),

    ok = amqp10_client_session:disposition(
           Receiver,
           amqp10_msg:delivery_id(Msg4),
           amqp10_msg:delivery_id(Msg3b),
           true,
           accepted),
    assert_messages(QName, 1, 1, Config),

    ok = amqp10_client:accept_msg(Receiver, Msg1),
    assert_messages(QName, 0, 0, Config),

    ok = amqp10_client:detach_link(Receiver),
    ?assertMatch({ok, #{message_count := 0}}, rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).


%% This test asserts the following §3.2 requirement:
%% "The bare message is immutable within the AMQP network. That is, none of the sections can be
%% changed by any node acting as an AMQP intermediary. If a section of the bare message is
%% omitted, one MUST NOT be inserted by an intermediary. The exact encoding of sections of the
%% bare message MUST NOT be modified. This preserves message hashes, HMACs and signatures based
%% on the binary encoding of the bare message."
immutable_bare_message(Config) ->
    footer_checksum(crc32, Config),
    footer_checksum(adler32, Config).

footer_checksum(FooterOpt, Config) ->
    ExpectedKey = case FooterOpt of
                      crc32 -> <<"x-opt-crc-32">>;
                      adler32 -> <<"x-opt-adler-32">>
                  end,

    {_, Session, LinkPair} = Init = init(Config),
    QName = atom_to_binary(FooterOpt),
    Addr = rabbitmq_amqp_address:queue(QName),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    RecvAttachArgs = #{name => <<"my receiver">>,
                       role => {receiver, #{address => Addr,
                                            durable => configuration}, self()},
                       snd_settle_mode => settled,
                       rcv_settle_mode => first,
                       filter => #{},
                       footer_opt => FooterOpt},
    SndAttachArgs = #{name => <<"my sender">>,
                      role => {sender, #{address => Addr,
                                         durable => configuration}},
                      snd_settle_mode => mixed,
                      rcv_settle_mode => first,
                      footer_opt => FooterOpt},
    {ok, Receiver} = amqp10_client:attach_link(Session, RecvAttachArgs),
    {ok, Sender} = amqp10_client:attach_link(Session, SndAttachArgs),
    wait_for_credit(Sender),

    Now = erlang:system_time(millisecond),
    %% with properties and application-properties
    M1 = amqp10_msg:set_headers(
           #{durable => true,
             priority => 7,
             ttl => 100_000},
           amqp10_msg:set_delivery_annotations(
             #{"x-a" => "b"},
             amqp10_msg:set_message_annotations(
               #{"x-string" => "string-value",
                 "x-int" => 3,
                 "x-bool" => true},
               amqp10_msg:set_properties(
                 #{message_id => {ulong, 999},
                   user_id => <<"guest">>,
                   to => Addr,
                   subject => <<"high prio">>,
                   reply_to => rabbitmq_amqp_address:queue(<<"a">>),
                   correlation_id => <<"correlation">>,
                   content_type => <<"text/plain">>,
                   content_encoding => <<"some encoding">>,
                   absolute_expiry_time => Now + 100_000,
                   creation_time => Now,
                   group_id => <<"my group ID">>,
                   group_sequence => 16#ff_ff_ff_ff,
                   reply_to_group_id => <<"other group ID">>},
                 amqp10_msg:set_application_properties(
                   #{"string" => "string-val",
                     "int" => 2,
                     "true" => true,
                     "false" => false},
                   amqp10_msg:new(<<"t1">>, <<"m1">>)))))),
    ok = amqp10_client:send_msg(Sender, M1),
    ok = wait_for_accepted(<<"t1">>),

    {ok, Msg1} = amqp10_client:get_msg(Receiver),
    ?assertEqual(<<"m1">>, amqp10_msg:body_bin(Msg1)),

    %% without properties
    M2 = amqp10_msg:set_application_properties(
           #{"string" => "string-val",
             "int" => 2,
             "true" => true,
             "false" => false},
           amqp10_msg:new(<<"t2">>, <<"m2">>)),
    ok = amqp10_client:send_msg(Sender, M2),
    ok = wait_for_accepted(<<"t2">>),

    {ok, Msg2} = amqp10_client:get_msg(Receiver),
    ?assertEqual(<<"m2">>, amqp10_msg:body_bin(Msg2)),

    %% bare message consists of single data section
    M3 = amqp10_msg:new(<<"t3">>, <<"m3">>),
    ok = amqp10_client:send_msg(Sender, M3),
    ok = wait_for_accepted(<<"t3">>),

    {ok, Msg3} = amqp10_client:get_msg(Receiver),
    ?assertEqual(<<"m3">>, amqp10_msg:body_bin(Msg3)),

    %% bare message consists of multiple data sections
    M4 = amqp10_msg:new(<<"t4">>, [#'v1_0.data'{content = <<"m4 a">>},
                                   #'v1_0.data'{content = <<"m4 b">>}]),
    ok = amqp10_client:send_msg(Sender, M4),
    ok = wait_for_accepted(<<"t4">>),

    {ok, Msg4} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m4 a">>, <<"m4 b">>], amqp10_msg:body(Msg4)),

    %% bare message consists of multiple sequence sections
    M5 = amqp10_msg:new(<<"t5">>,
                        [#'v1_0.amqp_sequence'{content = [{ubyte, 255}]},
                         %% Our serialiser uses 2 byte boolean encoding
                         #'v1_0.amqp_sequence'{content = [{boolean, true}, {boolean, false}]},
                         %% Our serialiser uses 1 byte boolean encoding
                         #'v1_0.amqp_sequence'{content = [true, false, undefined]}]),
    ok = amqp10_client:send_msg(Sender, M5),
    ok = wait_for_accepted(<<"t5">>),

    {ok, Msg5} = amqp10_client:get_msg(Receiver),
    ?assertEqual([#'v1_0.amqp_sequence'{content = [{ubyte, 255}]},
                  %% Our parser returns the Erlang boolean term.
                  %% However, the important assertion is that RabbitMQ sent us back
                  %% the bare message unmodified, i.e. that the checksum holds.
                  #'v1_0.amqp_sequence'{content = [true, false]},
                  #'v1_0.amqp_sequence'{content = [true, false, undefined]}],
                 amqp10_msg:body(Msg5)),

    %% with footer
    M6 = amqp10_msg:from_amqp_records(
           [#'v1_0.transfer'{delivery_tag = {binary, <<"t6">>},
                             settled = false,
                             message_format = {uint, 0}},
            #'v1_0.properties'{correlation_id = {ulong, 16#ff_ff_ff_ff_ff_ff_ff_ff}},
            #'v1_0.data'{content = <<"m6 a">>},
            #'v1_0.data'{content = <<"m6 b">>},
            #'v1_0.footer'{
               content = [
                          {{symbol, <<"x-opt-rabbit">>}, {char, $🐇}},
                          {{symbol, <<"x-opt-carrot">>}, {char, $🥕}}
                         ]}]),
    ok = amqp10_client:send_msg(Sender, M6),
    ok = wait_for_accepted(<<"t6">>),

    {ok, Msg6} = amqp10_client:get_msg(Receiver),
    ?assertEqual([<<"m6 a">>, <<"m6 b">>], amqp10_msg:body(Msg6)),
    ?assertMatch(#{ExpectedKey := _,
                   <<"x-opt-rabbit">> := $🐇,
                   <<"x-opt-carrot">> := $🥕},
                 amqp10_msg:footer(Msg6)),

    %% We only sanity check here that the footer annotation we received from the server
    %% contains a checksum. The AMQP Erlang client library will assert for us that the
    %% received checksum matches the actual checksum.
    lists:foreach(fun(Msg) ->
                          Map = amqp10_msg:footer(Msg),
                          {ok, Checksum} = maps:find(ExpectedKey, Map),
                          ?assert(is_integer(Checksum))
                  end, [Msg1, Msg2, Msg3, Msg4, Msg5, Msg6]),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:detach_link(Sender),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

receive_many_made_available_over_time_classic_queue(Config) ->
    receive_many_made_available_over_time(<<"classic">>, Config).

receive_many_made_available_over_time_quorum_queue(Config) ->
    receive_many_made_available_over_time(<<"quorum">>, Config).

receive_many_made_available_over_time_stream(Config) ->
    receive_many_made_available_over_time(<<"stream">>, Config).

%% This test grants many credits to the queue once while
%% messages are being made available at the source over time.
receive_many_made_available_over_time(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection, Session, LinkPair} = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    %% Send first batch of messages.
    ok = send_messages(Sender, 10, false),
    ok = wait_for_accepts(10),
    Filter = consume_from_first(QType),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address,
                       settled, configuration, Filter),
    flush(attached),
    %% Grant many credits to the queue once.
    ok = amqp10_client:flow_link_credit(Receiver, 5000, never),
    %% We expect to receive the first batch of messages.
    receive_messages(Receiver, 10),

    %% Make next batch of messages available.
    ok = send_messages(Sender, 2990, false),
    ok = wait_for_accepts(2990),
    %% We expect to receive this batch of messages.
    receive_messages(Receiver, 2990),

    %% Make next batch of messages available.
    ok = send_messages(Sender, 1999, false),
    ok = wait_for_accepts(1999),
    %% We expect to receive this batch of messages.
    receive_messages(Receiver, 1999),

    %% Make next batch of messages available.
    ok = send_messages(Sender, 2, false),
    ok = wait_for_accepts(2),
    %% At this point, we only have 2 messages in the queue, but only 1 credit left.
    ?assertEqual(1, count_received_messages(Receiver)),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = close_connection_sync(Connection).

receive_many_auto_flow_classic_queue(Config) ->
    receive_many_auto_flow(<<"classic">>, Config).

receive_many_auto_flow_quorum_queue(Config) ->
    receive_many_auto_flow(<<"quorum">>, Config).

receive_many_auto_flow_stream(Config) ->
    receive_many_auto_flow(<<"stream">>, Config).

receive_many_auto_flow(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection, Session, LinkPair} = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"test-sender">>, Address),
    wait_for_credit(Sender),

    %% Send many messages.
    Num = 10_000,
    ok = send_messages(Sender, Num, false),
    ok = wait_for_accepts(Num),

    Filter = consume_from_first(QType),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address,
                       settled, configuration, Filter),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail(missing_attached)
    end,
    flush(receiver_attached),

    %% Let's auto top up relatively often, but in large batches.
    ok = amqp10_client:flow_link_credit(Receiver, 1300, 1200),
    receive_messages(Receiver, Num),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = close_connection_sync(Connection).

%% This test ensures that the server sends us TRANSFER and FLOW frames in the correct order
%% even if the server is temporarily not allowed to send us any TRANSFERs due to our session
%% incoming-window being closed.
incoming_window_closed_transfer_flow_order(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    Address = rabbitmq_amqp_address:queue(QName),
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    DTag = <<"my tag">>,
    Body = <<"my body">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag, Body, false)),
    ok = wait_for_accepted(DTag),
    ok = amqp10_client:detach_link(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail(missing_attached)
    end,
    flush(receiver_attached),

    ok = close_incoming_window(Session),
    ok = amqp10_client:flow_link_credit(Receiver, 2, never, true),
    %% Given our incoming window is closed, we shouldn't receive the TRANSFER yet, and therefore
    %% must not yet receive the FLOW that comes thereafter with drain=true, credit=0, and advanced delivery-count.
    receive Unexpected -> ct:fail({unexpected, Unexpected})
    after 300 -> ok
    end,

    %% Open our incoming window
    ok = amqp10_client_session:flow(Session, 5, never),
    %% Important: We should first receive the TRANSFER,
    %% and only thereafter the FLOW (and hence the credit_exhausted notification).
    receive First ->
                {amqp10_msg, Receiver, Msg} = First,
                ?assertEqual([Body], amqp10_msg:body(Msg))
    after 30000 -> ct:fail("timeout receiving message")
    end,
    receive Second ->
                ?assertEqual({amqp10_event, {link, Receiver, credit_exhausted}}, Second)
    after 30000 -> ct:fail("timeout receiving credit_exhausted")
    end,

    ok = delete_queue(Session, QName),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

incoming_window_closed_stop_link(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{transfer_limit_margin => -1},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t1">>, <<"m1">>, false)),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(<<"t2">>, <<"m2">>, false)),
    ok = wait_for_accepted(<<"t1">>),
    ok = wait_for_accepted(<<"t2">>),
    ok = amqp10_client:detach_link(Sender),

    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail(missing_attached)
    end,
    flush(receiver_attached),

    ok = close_incoming_window(Session),
    %% We first grant a credit in drain mode.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),
    %% Then, we change our mind and stop the link.
    ok = amqp10_client:stop_receiver_link(Receiver),
    %% Given our incoming window is closed, we shouldn't receive any TRANSFER.
    receive {amqp10_msg, _, _} = Unexp1 -> ct:fail({?LINE, unexpected_msg, Unexp1})
    after 10 -> ok
    end,

    %% Open our incoming window
    ok = amqp10_client_session:flow(Session, 5, never),

    %% Since we decreased link credit dynamically, we may or may not receive the 1st message.
    receive {amqp10_msg, Receiver, Msg1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1))
    after 500 -> ok
    end,
    %% We must not receive the 2nd message.
    receive {amqp10_msg, _, _} = Unexp2 -> ct:fail({?LINE, unexpected_msg, Unexp2})
    after 5 -> ok
    end,

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close({Connection, Session, LinkPair}).

%% Test that we can close a link while our session incoming-window is closed.
incoming_window_closed_close_link(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    DTag = <<"my tag">>,
    Body = <<"my body">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:new(DTag, Body, false)),
    ok = wait_for_accepted(DTag),
    ok = amqp10_client:detach_link(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address, unsettled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail(missing_attached)
    end,
    flush(receiver_attached),

    ok = close_incoming_window(Session),
    ok = amqp10_client:flow_link_credit(Receiver, 2, never, true),
    %% Given our incoming window is closed, we shouldn't receive the TRANSFER yet, and therefore
    %% must not yet receive the FLOW that comes thereafter with drain=true, credit=0, and advanced delivery-count.
    receive Unexpected1 -> ct:fail({unexpected, Unexpected1})
    after 300 -> ok
    end,
    %% Close the link while our session incoming-window is closed.
    ok = detach_link_sync(Receiver),
    %% Open our incoming window.
    ok = amqp10_client_session:flow(Session, 5, never),
    %% Given that both endpoints have now destroyed the link, we do not
    %% expect to receive any TRANSFER or FLOW frame referencing the destroyed link.
    receive Unexpected2 -> ct:fail({unexpected, Unexpected2})
    after 300 -> ok
    end,

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

incoming_window_closed_rabbitmq_internal_flow_classic_queue(Config) ->
    incoming_window_closed_rabbitmq_internal_flow(<<"classic">>, Config).

incoming_window_closed_rabbitmq_internal_flow_quorum_queue(Config) ->
    incoming_window_closed_rabbitmq_internal_flow(<<"quorum">>, Config).

incoming_window_closed_rabbitmq_internal_flow(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    %% Send many messages.
    Num = 5_000,
    ok = send_messages(Sender, Num, false),
    ok = wait_for_accepts(Num),
    ok = detach_link_sync(Sender),

    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address, settled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail(missing_attached)
    end,
    flush(receiver_attached),

    ok = close_incoming_window(Session),
    %% Grant all link credit at once.
    ok = amqp10_client:flow_link_credit(Receiver, Num, never),
    %% Given our incoming window is closed, we shouldn't receive any TRANSFER yet.
    receive Unexpected -> ct:fail({unexpected, Unexpected})
    after 200 -> ok
    end,

    %% Here, we do a bit of white box testing: We assert that RabbitMQ has some form of internal
    %% flow control by checking that the queue did not send all its messages to the server session
    %% process. In other words, there should be ready messages in the queue.
    MsgsReady = ready_messages(QName, Config),
    ?assert(MsgsReady > 0),

    %% Open our incoming window.
    ok = amqp10_client_session:flow(Session, 100, 50),
    receive_messages(Receiver, Num),

    ok = detach_link_sync(Receiver),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

tcp_back_pressure_rabbitmq_internal_flow_classic_queue(Config) ->
    tcp_back_pressure_rabbitmq_internal_flow(<<"classic">>, Config).

tcp_back_pressure_rabbitmq_internal_flow_quorum_queue(Config) ->
    tcp_back_pressure_rabbitmq_internal_flow(<<"quorum">>, Config).

%% Test that RabbitMQ can handle clients that do not receive fast enough
%% causing TCP back-pressure to the server. RabbitMQ's internal flow control
%% writer proc <- session proc <- queue proc
%% should be able to protect the server by having the queue not send out all messages at once.
tcp_back_pressure_rabbitmq_internal_flow(QType, Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    OpnConf0 = connection_config(Config),
    %% We also want to test the code path where large messages are split into multiple transfer frames.
    OpnConf = OpnConf0#{max_frame_size => 600},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),

    QProps = #{arguments => #{<<"x-queue-type">> => {utf8, QType}}},
    {ok, #{type := QType}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    %% Send many messages.
    %% The messages should be somewhat large to fill up buffers causing TCP back-pressure.
    BodySuffix = binary:copy(<<"x">>, 1000),
    Num = 5000,
    ok = send_messages(Sender, Num, false, BodySuffix),
    ok = wait_for_accepts(Num),
    ok = detach_link_sync(Sender),

    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address, settled),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 30000 -> ct:fail(missing_attached)
    end,
    flush(receiver_attached),

    {_GenStatemStateSession, StateSession} = formatted_state(Session),
    Socket = case web_amqp(Config) of
                 true ->
                     #{socket := {ws, GunPid, _GunStreamRef}} = StateSession,
                     {_GenStatemStateGun, StateGun} = formatted_state(GunPid),
                     %% https://github.com/ninenines/gun/blob/2.1.0/src/gun.erl#L315
                     element(12, StateGun);
                 false ->
                     #{socket := {tcp, Sock}} = StateSession,
                     Sock
             end,
    ?assert(is_port(Socket)),

    %% Provoke TCP back-pressure from client to server by:
    %% 1. using very small buffers
    ok = inet:setopts(Socket, [{recbuf, 256},
                               {buffer, 256}]),
    %% 2. stopping reading from the socket
    Mod = inet,
    ok = meck:new(Mod, [unstick, no_link, passthrough]),
    ok = meck:expect(Mod, setopts, fun(_Sock, [{active, once}]) ->
                                           ok;
                                      (Sock, Opts) ->
                                           meck:passthrough([Sock, Opts])
                                   end),

    ok = amqp10_client:flow_link_credit(Receiver, Num, never),
    %% We give the queue time to send messages to the session proc and writer proc.
    timer:sleep(2000),

    %% Here, we do a bit of white box testing: We assert that RabbitMQ has some form of internal
    %% flow control by checking that the queue sent some but, more importantly, not all its
    %% messages to the server session and writer processes. In other words, there should be
    %% ready messages in the queue.
    MsgsReady = ready_messages(QName, Config),
    ?assert(MsgsReady > 0),
    ?assert(MsgsReady < Num),

    ?assert(meck:validate(Mod)),
    ok = meck:unload(Mod),
    %% Rely on end_per_testcase/2 to delete the queue and to close the connection.
    ok.

session_flow_control_default_max_frame_size(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {_, Session, LinkPair} = Init = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    Num = 1000,
    ok = send_messages(Sender, Num, false),
    ok = wait_for_accepts(Num),

    ok = amqp10_client_session:flow(Session, 2, never),
    %% Grant link credit worth of all messages that we are going to receive
    %% in this test case.
    ok = amqp10_client:flow_link_credit(Receiver, Num * 2, never),

    [Msg1000, Msg999] = receive_messages(Receiver, 2),
    ?assertEqual(<<"1000">>, amqp10_msg:body_bin(Msg1000)),
    ?assertEqual(<<"999">>, amqp10_msg:body_bin(Msg999)),
    receive {amqp10_msg, _, _} = Unexpected0 ->
                ct:fail({unexpected_msg, Unexpected0, ?LINE})
    after 50 -> ok
    end,

    ok = amqp10_client_session:flow(Session, 1, never),
    [Msg998] = receive_messages(Receiver, 1),
    ?assertEqual(<<"998">>, amqp10_msg:body_bin(Msg998)),
    receive {amqp10_msg, _, _} = Unexpected1 ->
                ct:fail({unexpected_msg, Unexpected1, ?LINE})
    after 50 -> ok
    end,

    ok = amqp10_client_session:flow(Session, 0, never),
    receive {amqp10_msg, _, _} = Unexpected2 ->
                ct:fail({unexpected_msg, Unexpected2, ?LINE})
    after 50 -> ok
    end,

    %% When the client automatically widens the session window,
    %% we should receive all remaining messages.
    ok = amqp10_client_session:flow(Session, 2, 1),
    receive_messages(Receiver, Num - 3),

    %% Let's test with a different auto renew session flow config (100, 100).
    ok = amqp10_client_session:flow(Session, 0, never),
    ok = send_messages(Sender, Num, false),
    ok = wait_for_accepts(Num),
    receive {amqp10_msg, _, _} = Unexpected3 ->
                ct:fail({unexpected_msg, Unexpected3, ?LINE})
    after 50 -> ok
    end,
    ok = amqp10_client_session:flow(Session, 100, 100),
    receive_messages(Receiver, Num),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

%% Test session flow control with large messages split into multiple transfer frames.
session_flow_control_small_max_frame_size(Config) ->
    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{max_frame_size => 1000},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"pair">>),

    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address),
    receive {amqp10_event, {link, Receiver, attached}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    Suffix = binary:copy(<<"x">>, 2500),
    Num = 10,
    ok = send_messages(Sender, Num, false, Suffix),
    ok = wait_for_accepts(Num),

    %% 1 message of size ~2500 bytes gets split into 3 transfer frames
    %% because each transfer frame has max size of 1000 bytes.
    %% Hence, if we set our incoming-window to 3, we should receive exactly 1 message.
    ok = amqp10_client_session:flow(Session, 3, never),
    %% Grant plenty of link credit.
    ok = amqp10_client:flow_link_credit(Receiver, Num * 5, never),
    receive {amqp10_msg, Receiver, Msg10} ->
                ?assertEqual(<<"10", Suffix/binary>>,
                             amqp10_msg:body_bin(Msg10))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, _, _} = Unexpected0 ->
                ct:fail({unexpected_msg, Unexpected0, ?LINE})
    after 50 -> ok
    end,

    %% When the client automatically widens the session window,
    %% we should receive all remaining messages.
    ok = amqp10_client_session:flow(Session, 2, 1),
    Msgs = receive_messages(Receiver, Num - 1),
    Msg1 = lists:last(Msgs),
    ?assertEqual(<<"1", Suffix/binary>>,
                 amqp10_msg:body_bin(Msg1)),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close_connection_sync(Connection).

session_max_per_connection(Config) ->
    App = rabbit,
    Par = session_max_per_connection,
    {ok, Default} = rpc(Config, application, get_env, [App, Par]),
    %% Let's allow only 1 session per connection.
    ok = rpc(Config, application, set_env, [App, Par, 1]),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 30000 -> ct:fail(opened_timeout)
    end,
    %% The 1st session should succeed.
    {ok, _Session1} = amqp10_client:begin_session_sync(Connection),
    %% The 2nd session should fail.
    {ok, _Session2} = amqp10_client:begin_session(Connection),
    receive {amqp10_event, {connection, Connection, {closed, Reason}}} ->
                ?assertEqual(
                   {framing_error, <<"channel number (1) exceeds maximum channel number (0)">>},
                   Reason)
    after 30000 -> ct:fail(missing_closed)
    end,

    ok = rpc(Config, application, set_env, [App, Par, Default]).

link_max_per_session(Config) ->
    App = rabbit,
    Par = link_max_per_session,
    {ok, Default} = rpc(Config, application, get_env, [App, Par]),
    %% Let's allow only 1 link per session.
    ok = rpc(Config, application, set_env, [App, Par, 1]),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 30000 -> ct:fail(opened_timeout)
    end,
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address1 = rabbitmq_amqp_address:exchange(<<"amq.direct">>, <<"k1">>),
    Address2 = rabbitmq_amqp_address:exchange(<<"amq.direct">>, <<"k2">>),
    %% The 1st link should succeed.
    {ok, Link1} = amqp10_client:attach_sender_link_sync(Session, <<"link-1">>, Address1),
    ok = wait_for_credit(Link1),
    %% Since the 2nd link should fail, we expect our session process to die.
    ?assert(is_process_alive(Session)),
    {ok, _Link2} = amqp10_client:attach_sender_link(Session, <<"link-2">>, Address2),
    eventually(?_assertNot(is_process_alive(Session))),

    flush(test_succeeded),
    ok = rpc(Config, application, set_env, [App, Par, Default]).

reserved_annotation(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    TargetAddr = rabbitmq_amqp_address:exchange(<<"amq.fanout">>),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(
                     Session, <<"sender">>, TargetAddr, settled),
    ok = wait_for_credit(Sender),

    Msg = amqp10_msg:set_message_annotations(
            #{<<"reserved-key">> => 1},
            amqp10_msg:new(<<"tag">>, <<"payload">>, true)),
    ok = amqp10_client:send_msg(Sender, Msg),
    receive
        {amqp10_event,
         {session, Session,
          {ended,
           #'v1_0.error'{description = {utf8, Description}}}}} ->
            ?assertMatch(
               <<"{reserved_annotation_key,{symbol,<<\"reserved-key\">>}}", _/binary>>,
               Description)
    after 30000 -> flush(missing_ended),
                  ct:fail({missing_event, ?LINE})
    end,
    ok = close_connection_sync(Connection).

%% Test that x-cc routing keys work together with target address
%% /exchanges/:exchange/:routing-key
x_cc_annotation_exchange(Config) ->
    QName1 = <<"queue 1">>,
    QName2 = <<"queue 2">>,
    {Connection, Session, LinkPair} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName1, #{}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName1, <<"amq.direct">>, <<"key 1">>, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName2, <<"amq.direct">>, <<"key 2">>, #{}),
    Address = rabbitmq_amqp_address:exchange(<<"amq.direct">>, <<"key 1">>),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    Payload = <<"my message">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_message_annotations(
                                          #{<<"x-cc">> => {list, [{utf8, <<"key 2">>}]}},
                                          amqp10_msg:new(<<"tag">>, Payload))),
    ok = wait_for_accepted(<<"tag">>),
    ok = amqp10_client:detach_link(Sender),

    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, rabbitmq_amqp_address:queue(QName1), settled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, rabbitmq_amqp_address:queue(QName2), settled),
    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    {ok, Msg2} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([Payload], amqp10_msg:body(Msg1)),
    ?assertEqual([Payload], amqp10_msg:body(Msg2)),

    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Test that x-cc routing keys work together with target address
%% /exchanges/:exchange
x_cc_annotation_exchange_routing_key_empty(Config) ->
    QName1 = <<"queue 1">>,
    QName2 = <<"queue 2">>,
    {Connection, Session, LinkPair} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName1, #{}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName1, <<"amq.direct">>, <<"key 1">>, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName2, <<"amq.direct">>, <<"key 2">>, #{}),
    AddressEmptyRoutingKey = rabbitmq_amqp_address:exchange(<<"amq.direct">>),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, AddressEmptyRoutingKey),
    ok = wait_for_credit(Sender),

    Payload = <<"my message">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_message_annotations(
                                          #{<<"x-cc">> => {list, [{utf8, <<"key 1">>},
                                                                  {utf8, <<"key 2">>}]}},
                                          amqp10_msg:new(<<"tag">>, Payload))),
    ok = wait_for_accepted(<<"tag">>),
    ok = amqp10_client:detach_link(Sender),

    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, rabbitmq_amqp_address:queue(QName1), settled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, rabbitmq_amqp_address:queue(QName2), settled),
    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    {ok, Msg2} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([Payload], amqp10_msg:body(Msg1)),
    ?assertEqual([Payload], amqp10_msg:body(Msg2)),

    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Test that x-cc routing keys work together with target address
%% /queues/:queue
x_cc_annotation_queue(Config) ->
    QName1 = <<"queue 1">>,
    QName2 = <<"queue 2">>,
    Address1 = rabbitmq_amqp_address:queue(QName1),
    Address2 = rabbitmq_amqp_address:queue(QName2),
    {Connection, Session, LinkPair} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName1, #{}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address1),
    ok = wait_for_credit(Sender),

    Payload = <<"my message">>,
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_message_annotations(
                                          #{<<"x-cc">> => {list, [{utf8, QName2}]}},
                                          amqp10_msg:new(<<"tag">>, Payload))),
    ok = wait_for_accepted(<<"tag">>),
    ok = amqp10_client:detach_link(Sender),

    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session, <<"receiver 1">>, Address1, settled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(Session, <<"receiver 2">>, Address2, settled),
    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    {ok, Msg2} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([Payload], amqp10_msg:body(Msg1)),
    ?assertEqual([Payload], amqp10_msg:body(Msg2)),

    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Test that x-cc routing keys work together with target address 'null'
x_cc_annotation_null(Config) ->
    QName1 = <<"queue 1">>,
    QName2 = <<"queue 2">>,
    QAddress1 = rabbitmq_amqp_address:queue(QName1),
    QAddress2 = rabbitmq_amqp_address:queue(QName2),
    {Connection, Session, LinkPair} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName1, #{}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName1, <<"amq.direct">>, <<"key-1">>, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName2, <<"amq.direct">>, <<"🗝️-2"/utf8>>, #{}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, null),
    ok = wait_for_credit(Sender),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(Session, <<"receiver 1">>, QAddress1, settled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(Session, <<"receiver 2">>, QAddress2, settled),

    Msg1 = amqp10_msg:set_message_annotations(
             #{<<"x-cc">> => {list, [{utf8, <<"key-1">>},
                                     {utf8, <<"key-3">>}]}},
             amqp10_msg:set_properties(
               #{to => rabbitmq_amqp_address:exchange(<<"amq.direct">>, <<"🗝️-2"/utf8>>)},
               amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = wait_for_accepted(<<"t1">>),
    {ok, R1M1} = amqp10_client:get_msg(Receiver1),
    {ok, R2M1} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R1M1)),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R2M1)),

    Msg2 = amqp10_msg:set_message_annotations(
             #{<<"x-cc">> => {list, [{utf8, <<"🗝️-2"/utf8>>},
                                     {utf8, <<"key-1">>}]}},
             amqp10_msg:set_properties(
               #{to => rabbitmq_amqp_address:exchange(<<"amq.direct">>)},
               amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = wait_for_accepted(<<"t2">>),
    {ok, R1M2} = amqp10_client:get_msg(Receiver1),
    {ok, R2M2} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m2">>], amqp10_msg:body(R1M2)),
    ?assertEqual([<<"m2">>], amqp10_msg:body(R2M2)),

    Msg3 = amqp10_msg:set_message_annotations(
             #{<<"x-cc">> => {list, [{utf8, QName1}]}},
             amqp10_msg:set_properties(
               #{to => rabbitmq_amqp_address:queue(QName2)},
               amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = amqp10_client:send_msg(Sender, Msg3),
    ok = wait_for_accepted(<<"t3">>),
    {ok, R1M3} = amqp10_client:get_msg(Receiver1),
    {ok, R2M3} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R1M3)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R2M3)),

    Msg4 = amqp10_msg:set_message_annotations(
             %% We send a symbol instead of utf8..
             #{<<"x-cc">> => {list, [{symbol, QName1}]}},
             amqp10_msg:set_properties(
               #{to => rabbitmq_amqp_address:queue(QName2)},
               amqp10_msg:new(<<"t4">>, <<"m4">>))),
    ok = amqp10_client:send_msg(Sender, Msg4),
    %% "If the source of the link supports the rejected outcome, and the message has not
    %% already been settled by the sender, then the routing node MUST reject the message.
    %% In this case the error field of rejected MUST contain the error which would have been communicated
    %% in the detach which would have be sent if a link to the same address had been attempted."
    %% https://docs.oasis-open.org/amqp/anonterm/v1.0/cs01/anonterm-v1.0-cs01.html#doc-routingerrors
    receive {amqp10_disposition, {{rejected, Error}, <<"t4">>}} ->
                ?assertMatch(
                   #'v1_0.error'{
                      condition = ?V_1_0_AMQP_ERROR_INVALID_FIELD,
                      description = {utf8, <<"bad value for 'x-cc' message-annotation:", _/binary>>}},
                   Error)
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:detach_link(Receiver1),
    ok = amqp10_client:detach_link(Receiver2),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

bad_x_cc_annotation_exchange(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),

    Address = rabbitmq_amqp_address:exchange(<<"amq.direct">>, <<"key-1">>),
    {ok, Sender1} = amqp10_client:attach_sender_link(Session, <<"sender 1">>, Address),
    ok = wait_for_credit(Sender1),
    ok = amqp10_client:send_msg(
           Sender1,
           amqp10_msg:set_message_annotations(
             %% We send an array instead of a list.
             #{<<"x-cc">> => {array, utf8, [{utf8, <<"🗝️-2"/utf8>>}]}},
             amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = wait_for_settlement(<<"t1">>, released),
    receive {amqp10_event, {link, Sender1, {detached, Error1}}} ->
                ?assertMatch(
                   #'v1_0.error'{
                      condition = ?V_1_0_AMQP_ERROR_INVALID_FIELD,
                      description = {utf8, <<"bad value for 'x-cc' message-annotation: "
                                             "{array,utf8,[{utf8,<<\"🗝️-2"/utf8, _Rest/binary>>}},
                   Error1)
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, Sender2} = amqp10_client:attach_sender_link(Session, <<"sender 2">>, Address),
    ok = wait_for_credit(Sender2),
    ok = amqp10_client:send_msg(
           Sender2,
           amqp10_msg:set_message_annotations(
             %% We include a non-utf8 type in the list.
             #{<<"x-cc">> => {list, [{symbol, <<"key-3">>}]}},
             amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = wait_for_settlement(<<"t2">>, released),
    receive {amqp10_event, {link, Sender2, {detached, Error2}}} ->
                ?assertEqual(
                   #'v1_0.error'{
                      condition = ?V_1_0_AMQP_ERROR_INVALID_FIELD,
                      description = {utf8, <<"bad value for 'x-cc' message-annotation: "
                                             "{list,[{symbol,<<\"key-3\">>}]}">>}},
                   Error2)
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Test that RabbitMQ can store and forward AMQP decimal types.
decimal_types(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {_, Session, LinkPair} = Init = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(
                LinkPair, QName,
                #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>}}}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    Decimal32Zero = <<16#22, 16#50, 0, 0>>,
    Decimal64Zero = <<16#22, 16#34, 0, 0, 0, 0, 0, 0>>,
    Decimal128Zero = <<16#22, 16#08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>,
    Decimal3242 = <<16#22, 16#50, 16#00, 16#2A>>, % 42
    Decimal32NaN = <<16#7C, 0, 0, 0>>,
    Body = #'v1_0.amqp_value'{content = {list, [{as_is, 16#74, Decimal32Zero},
                                                {as_is, 16#84, Decimal64Zero},
                                                {as_is, 16#94, Decimal128Zero}]}},
    MsgAnns = #{<<"x-decimal-32">> => {as_is, 16#74, Decimal3242},
                <<"x-decimal-64">> => {as_is, 16#84, Decimal64Zero},
                <<"x-decimal-128">> => {as_is, 16#94, Decimal128Zero},
                <<"x-list">> => {list, [{as_is, 16#94, Decimal128Zero}]},
                <<"x-map">> => {map, [{{utf8, <<"key-1">>},
                                       {as_is, 16#94, Decimal128Zero}}]}},
    AppProps = #{<<"decimal-32">> => {as_is, 16#74, Decimal32NaN}},
    Msg0 = amqp10_msg:set_message_annotations(
             MsgAnns,
             amqp10_msg:set_application_properties(
               AppProps,
               amqp10_msg:new(<<"tag">>, Body))),
    ok = amqp10_client:send_msg(Sender, Msg0),
    ok = wait_for_accepted(<<"tag">>),
    ok = amqp10_client:send_msg(Sender, Msg0),
    ok = wait_for_accepted(<<"tag">>),
    ok = detach_link_sync(Sender),

    %% Consume the first message via AMQP 1.0
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address, unsettled),
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual(Body, amqp10_msg:body(Msg)),
    ?assertMatch(#{<<"x-decimal-32">> := {as_is, 16#74, Decimal3242},
                   <<"x-decimal-64">> := {as_is, 16#84, Decimal64Zero},
                   <<"x-decimal-128">> := {as_is, 16#94, Decimal128Zero},
                   <<"x-list">> := [{as_is, 16#94, Decimal128Zero}],
                   <<"x-map">> := [{{utf8, <<"key-1">>},
                                    {as_is, 16#94, Decimal128Zero}}]},
                 amqp10_msg:message_annotations(Msg)),
    ?assertEqual(AppProps, amqp10_msg:application_properties(Msg)),
    ok = amqp10_client:accept_msg(Receiver, Msg),
    ok = detach_link_sync(Receiver),

    %% Consume the second message via AMQP 0.9.1
    %% We expect to receive the message without any crashes.
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{}},
                 amqp_channel:call(Ch, #'basic.get'{queue = QName, no_ack = true})),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close(Init).

%% Attach a receiver to an unavailable quorum queue.
attach_to_down_quorum_queue(Config) ->
    QName = <<"q-down">>,
    Address = rabbitmq_amqp_address:queue(QName),

    %% Create quorum queue with single replica on node 2.
    {_, _, LinkPair2} = Init2 = init(2, Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(
                LinkPair2,
                QName,
                #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                 <<"x-quorum-initial-group-size">> => {ulong, 1}
                                }}),
    ok = close(Init2),

    %% Make quorum queue unavailable.
    ok = rabbit_ct_broker_helpers:stop_broker(Config, 2),

    OpnConf = connection_config(0, Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session0} = amqp10_client:begin_session_sync(Connection),
    flush(attaching_receiver),
    {ok, _Receiver} = amqp10_client:attach_receiver_link(
                        Session0, <<"receiver">>, Address),
    receive
        {amqp10_event,
         {session, Session0,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
              description = {utf8, Desc}}}}} ->
            ?assertMatch(
               <<"failed consuming from quorum queue 'q-down' in vhost '/'", _Reason/binary>>,
               Desc)
    after 9000 ->
              ct:fail({missing_event, ?LINE})
    end,

    ok = rabbit_ct_broker_helpers:start_broker(Config, 2),

    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(
                       Session, <<"my link pair">>),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = close({Connection, Session, LinkPair}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

receive_all_messages(Receiver, Accept) ->
    receive_all_messages0(Receiver, Accept, []).

receive_all_messages0(Receiver, Accept, Acc) ->
    receive {amqp10_msg, Receiver, Msg} ->
                case Accept of
                    true -> ok = amqp10_client:accept_msg(Receiver, Msg);
                    false -> ok
                end,
                receive_all_messages0(Receiver, Accept, [Msg | Acc])
    after 2000 ->
              lists:reverse(Acc)
    end.

open_and_close_connection(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 30000 -> ct:fail(opened_timeout)
    end,
    ok = close_connection_sync(Connection).

wait_for_accepted(Tag) ->
    wait_for_settlement(Tag, accepted).

wait_for_settlement(Tag, State) ->
    receive
        {amqp10_disposition, {State, Tag}} ->
            ok
    after 30000 ->
              flush("wait_for_settlement timed out"),
              ct:fail({settled_timeout, Tag})
    end.

delete_queue(Session, QName) ->
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(
                       Session, <<"delete queue">>),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair).

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
    after 30000  ->
              ct:fail({timeout, {num_received, length(Acc)}, {num_missing, N}})
    end.

count_received_messages(Receiver) ->
    count_received_messages0(Receiver, 0).

count_received_messages0(Receiver, Count) ->
    receive
        {amqp10_msg, Receiver, _Msg} ->
            count_received_messages0(Receiver, Count + 1)
    after 2000 ->
              Count
    end.

send_until_remote_incoming_window_exceeded(Session, Address) ->
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address, settled),
    ok = wait_for_credit(Sender),
    ok = send_until_remote_incoming_window_exceeded0(Sender, 100_000),
    ok = amqp10_client:detach_link(Sender).

send_until_remote_incoming_window_exceeded0(_Sender, 0) ->
    ct:fail(remote_incoming_window_never_exceeded);
send_until_remote_incoming_window_exceeded0(Sender, Left) ->
    Bin = integer_to_binary(Left),
    Msg = amqp10_msg:new(Bin, Bin, true),
    case amqp10_client:send_msg(Sender, Msg) of
        ok ->
            send_until_remote_incoming_window_exceeded0(Sender, Left - 1);
        {error, insufficient_credit} ->
            ok = wait_for_credit(Sender),
            send_until_remote_incoming_window_exceeded0(Sender, Left);
        {error, remote_incoming_window_exceeded = Reason} ->
            ct:pal("~s: ~b messages left", [Reason, Left]),
            ok
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
            after 1000 ->
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
    assert_messages(QNameBin, NumTotalMsgs, NumUnackedMsgs, Config, 0).

assert_messages(QNameBin, NumTotalMsgs, NumUnackedMsgs, Config, Node) ->
    Vhost = ?config(rmq_vhost, Config),
    eventually(
      ?_assertEqual(
         lists:sort([{messages, NumTotalMsgs}, {messages_unacknowledged, NumUnackedMsgs}]),
         begin
             {ok, Q} = rpc(Config, Node, rabbit_amqqueue, lookup, [QNameBin, Vhost]),
             Infos = rpc(Config, Node, rabbit_amqqueue, info, [Q, [messages, messages_unacknowledged]]),
             lists:sort(Infos)
         end
        ), 500, 10).

serial_number_increment(S) ->
    case S + 1 of
        16#ffffffff + 1 -> 0;
        S1 -> S1
    end.

consume_from_first(<<"stream">>) ->
    #{<<"rabbitmq:stream-offset-spec">> => <<"first">>};
consume_from_first(_) ->
    #{}.

%% Return the formatted state of a gen_server or gen_statem via sys:get_status/1.
%% (sys:get_state/1 is unformatted)
formatted_state(Pid) ->
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

ready_messages(QName, Config)
  when is_binary(QName) ->
    {ok, Q} = rpc(Config, rabbit_amqqueue, lookup, [QName, <<"/">>]),
    {ok, MsgsReady, _ConsumerCount} = rpc(Config, rabbit_queue_type, stat, [Q]),
    ?assert(is_integer(MsgsReady)),
    ct:pal("Queue ~s has ~b ready messages.", [QName, MsgsReady]),
    MsgsReady.

ra_name(Q) ->
    binary_to_atom(<<"%2F_", Q/binary>>).

wait_for_local_member(<<"stream">>, QName, Config) ->
    %% If it is a stream we need to wait until there is a local member
    %% on the node we want to subscribe from before proceeding.
    ok = queue_utils:wait_for_local_stream_member(0, <<"/">>, QName, Config);
wait_for_local_member(_, _, _) ->
    ok.

-spec find_event(Type, Props, Events) -> Ret when
      Type :: atom(),
      Props :: proplists:proplist(),
      Events :: [#event{}],
      Ret :: {value, #event{}} | false.

find_event(Type, Props, Events) when is_list(Props), is_list(Events) ->
    lists:search(
      fun(#event{type = EventType, props = EventProps}) ->
              Type =:= EventType andalso
                lists:all(
                  fun(Prop) ->
                          lists:member(Prop, EventProps)
                  end, Props)
      end, Events).

close_incoming_window(Session) ->
    amqp10_client_session:flow(Session, 0, never).
