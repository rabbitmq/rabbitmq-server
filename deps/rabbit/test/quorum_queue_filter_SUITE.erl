%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(quorum_queue_filter_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_client/include/amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_filtex.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/1]).
-import(amqp_utils,
        [init/1,
         connection_config/1,
         close/1,
         flush/1,
         wait_for_credit/1,
         wait_for_accepts/1,
         send_messages/3,
         detach_link_sync/1,
         end_session_sync/1,
         close_connection_sync/1]).

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       %% property filter expressions
       property_message_groups_attach_empty_queue,
       property_message_groups_attach_non_empty_queue,
       property_message_groups_round_robin,
       property_message_groups_requeue,
       property_message_groups_same_filter,
       property_ttl,
       property_purge,
       property_filter_field_names_policy,
       property_dead_letter_at_most_once,
       property_properties_section,
       property_application_properties_section,
       property_multiple_sections,
       property_filter_few_messages_from_many,
       property_string_modifier,
       %% JMS message selectors
       jms_application_properties,
       %% miscellaneous
       mutually_exclusive_queue_args
      ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:merge_app_env(
      Config, {rabbit, [{quorum_tick_interval, 1000},
                        {stream_tick_interval, 1000}
                       ]}).

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, Suffix}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Assert that every testcase cleaned up.
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, []))),
    %% Wait for sessions to terminate before starting the next test case.
    eventually(?_assertEqual([], rpc(Config, rabbit_amqp_session, list_local, []))),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% Consumers attach to an empty queue and filter messages by group-id.
property_message_groups_attach_empty_queue(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair, QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {array, symbol,
                                                                  [{symbol, <<"group-id">>}]}
                                  }}),

    FilterRed = #{<<"red filter">> => #filter{descriptor = ?DESCRIPTOR_NAME_PROPERTIES_FILTER,
                                              value = {map, [{{symbol, <<"group-id">>},
                                                              {utf8, <<"red">>}}]}}},
    FilterBlue = #{<<"blue filter">> => #filter{descriptor = ?DESCRIPTOR_NAME_PROPERTIES_FILTER,
                                                value = {map, [{{symbol, <<"group-id">>},
                                                                {utf8, <<"blue">>}}]}}},
    {ok, ReceiverRed} = amqp10_client:attach_receiver_link(
                          Session, <<"receiver red">>, Address,
                          unsettled, none, FilterRed),
    {ok, ReceiverBlue} = amqp10_client:attach_receiver_link(
                           Session, <<"receiver blue">>, Address,
                           unsettled, none, FilterBlue),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"green">>},
                     amqp10_msg:new(<<"t4">>, <<"m4">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t5">>, <<"m5">>))),
    ok = wait_for_accepts(5),
    ok = detach_link_sync(Sender),
    flush(sent),

    ok = amqp10_client:flow_link_credit(ReceiverBlue, 10, never, true),
    receive {amqp10_msg, ReceiverBlue, M1} ->
                ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M1)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverBlue, M2} ->
                ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M2)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverBlue, M5} ->
                ?assertEqual(<<"m5">>, amqp10_msg:body_bin(M5)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M5)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(ReceiverBlue, ?LINE),

    ok = amqp10_client:flow_link_credit(ReceiverRed, 10, never, true),
    receive {amqp10_msg, ReceiverRed, M3} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3)),
                ok = amqp10_client:accept_msg(ReceiverRed, M3)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(ReceiverRed, ?LINE),

    ok = detach_link_sync(ReceiverBlue),
    ok = detach_link_sync(ReceiverRed),
    %% 1 green message
    ?assertMatch({ok, #{message_count := 1}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

%% Consumers attach to an non-empty queue and filter messages by group-id.
property_message_groups_attach_non_empty_queue(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {list, [{symbol, <<"group-id">>}]}
                                  }}),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"green">>},
                     amqp10_msg:new(<<"t4">>, <<"m4">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t5">>, <<"m5">>))),
    ok = wait_for_accepts(5),
    ok = detach_link_sync(Sender),
    flush(sent),

    FilterRed = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                  {map, [{{symbol, <<"group-id">>}, {utf8, <<"red">>}}]}},
    FilterBlue = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                   {map, [{{symbol, <<"group-id">>}, {utf8, <<"blue">>}}]}},
    {ok, ReceiverRed} = amqp10_client:attach_receiver_link(
                          Session, <<"receiver red">>, Address,
                          unsettled, none, FilterRed),
    {ok, ReceiverBlue} = amqp10_client:attach_receiver_link(
                           Session, <<"receiver blue">>, Address,
                           unsettled, none, FilterBlue),

    ok = amqp10_client:flow_link_credit(ReceiverBlue, 10, never, true),
    receive {amqp10_msg, ReceiverBlue, M1} ->
                ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M1)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverBlue, M2} ->
                ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M2)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverBlue, M5} ->
                ?assertEqual(<<"m5">>, amqp10_msg:body_bin(M5)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M5)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(ReceiverBlue, ?LINE),

    ok = amqp10_client:flow_link_credit(ReceiverRed, 10, never, true),
    receive {amqp10_msg, ReceiverRed, M3} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3)),
                ok = amqp10_client:accept_msg(ReceiverRed, M3)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(ReceiverRed, ?LINE),

    ok = detach_link_sync(ReceiverBlue),
    ok = detach_link_sync(ReceiverRed),
    %% 1 green message
    ?assertMatch({ok, #{message_count := 1}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

property_message_groups_round_robin(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {list, [{symbol, <<"group-id">>}]}
                                  }}),

    FilterRed = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                  {map, [{{symbol, <<"group-id">>}, {utf8, <<"red">>}}]}},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        settled, none, FilterRed),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        settled, none, FilterRed),
    {ok, Receiver3} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 3">>, Address,
                        settled, none, FilterRed),

    ok = amqp10_client:flow_link_credit(Receiver1, 10, never),
    ok = amqp10_client:flow_link_credit(Receiver2, 10, never),
    ok = amqp10_client:flow_link_credit(Receiver3, 10, never),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    flush(sender_credited),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = wait_for_accepts(3),

    ReceiverMsg1 = receive {amqp10_msg, RecM1, M1} ->
                               ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1)),
                               RecM1
                   after 5000 -> ct:fail({missing_msg, ?LINE})
                   end,
    ReceiverMsg2 = receive {amqp10_msg, RecM2, M2} ->
                               ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2)),
                               RecM2
                   after 5000 -> ct:fail({missing_msg, ?LINE})
                   end,
    ReceiverMsg3 = receive {amqp10_msg, RecM3, M3} ->
                               ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3)),
                               RecM3
                   after 5000 -> ct:fail({missing_msg, ?LINE})
                   end,

    %% We assert that RabbitMQ delivered the three red messages round robin.
    %% However, we don't care which receiver received first.
    ?assertEqual(lists:usort([Receiver1, Receiver2, Receiver3]),
                 lists:usort([ReceiverMsg1, ReceiverMsg2, ReceiverMsg3])),

    ok = detach_link_sync(Sender),
    ok = detach_link_sync(Receiver1),
    ok = detach_link_sync(Receiver2),
    ok = detach_link_sync(Receiver3),
    ?assertMatch({ok, #{message_count := 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

property_message_groups_requeue(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {list, [{symbol, <<"group-id">>}]}
                                  }}),

    FilterRed = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                  {map, [{{symbol, <<"group-id">>}, {utf8, <<"red">>}}]}},
    FilterBlue = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                   {map, [{{symbol, <<"group-id">>}, {utf8, <<"blue">>}}]}},
    {ok, ReceiverRed} = amqp10_client:attach_receiver_link(
                          Session, <<"receiver red">>, Address,
                          unsettled, none, FilterRed),
    {ok, ReceiverBlue} = amqp10_client:attach_receiver_link(
                           Session, <<"receiver blue">>, Address,
                           unsettled, none, FilterBlue),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"green">>},
                     amqp10_msg:new(<<"t4">>, <<"m4">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t5">>, <<"m5">>))),
    ok = wait_for_accepts(5),
    ok = detach_link_sync(Sender),
    flush(sent),

    ok = amqp10_client:flow_link_credit(ReceiverBlue, 10, never, true),
    Msg1a = receive {amqp10_msg, ReceiverBlue, M1a} ->
                        ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1a)),
                        M1a
            after 5000 -> ct:fail({missing_msg, ?LINE})
            end,
    Msg2 = receive {amqp10_msg, ReceiverBlue, M2} ->
                       ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2)),
                       M2
           after 5000 -> ct:fail({missing_msg, ?LINE})
           end,
    Msg5a = receive {amqp10_msg, ReceiverBlue, M5a} ->
                        ?assertEqual(<<"m5">>, amqp10_msg:body_bin(M5a)),
                        M5a
            after 5000 -> ct:fail({missing_msg, ?LINE})
            end,
    ok = assert_credit_exhausted(ReceiverBlue, ?LINE),

    ok = amqp10_client:flow_link_credit(ReceiverRed, 10, never, true),
    Msg3a = receive {amqp10_msg, ReceiverRed, M3a} ->
                        ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3a)),
                        M3a
            after 5000 -> ct:fail({missing_msg, ?LINE})
            end,
    ok = assert_credit_exhausted(ReceiverRed, ?LINE),

    flush(settling),
    ok = amqp10_client:settle_msg(ReceiverRed, Msg3a, released),
    ok = amqp10_client:settle_msg(ReceiverBlue, Msg2, accepted),
    ok = amqp10_client:settle_msg(ReceiverBlue, Msg5a, released),
    ok = amqp10_client:settle_msg(ReceiverBlue, Msg1a, released),

    %% We expect to receive requeued messages in the requeued order.
    ok = amqp10_client:flow_link_credit(ReceiverBlue, 10, never, true),
    receive {amqp10_msg, ReceiverBlue, M5b} ->
                ?assertEqual(<<"m5">>, amqp10_msg:body_bin(M5b)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M5b)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverBlue, M1b} ->
                ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1b)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M1b)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(ReceiverBlue, ?LINE),

    ok = amqp10_client:flow_link_credit(ReceiverRed, 10, never, true),
    receive {amqp10_msg, ReceiverRed, M3b} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3b)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M3b)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(ReceiverRed, ?LINE),

    ok = detach_link_sync(ReceiverBlue),
    ok = detach_link_sync(ReceiverRed),
    %% 1 green message
    ?assertMatch({ok, #{message_count := 1}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

%% Attach two consumers with the same filter.
property_message_groups_same_filter(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {list, [{symbol, <<"group-id">>}]}
                                  }}),

    FilterRed = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                  {map, [{{symbol, <<"group-id">>}, {utf8, <<"red">>}}]}},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        unsettled, none, FilterRed),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        unsettled, none, FilterRed),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t4">>, <<"m4">>))),
    ok = wait_for_accepts(4),
    flush(sent),

    ok = amqp10_client:flow_link_credit(Receiver1, 3, never),
    Msg1 = receive {amqp10_msg, Receiver1, M1} ->
                       ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1)),
                       M1
           after 5000 -> ct:fail({missing_msg, ?LINE})
           end,
    receive {amqp10_msg, Receiver1, M2} ->
                ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2))
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    Msg3 = receive {amqp10_msg, Receiver1, M3} ->
                       ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3)),
                       M3
           after 5000 -> ct:fail({missing_msg, ?LINE})
           end,
    ok = assert_credit_exhausted(Receiver1, ?LINE),
    ok = amqp10_client_session:disposition(
           Receiver1,
           amqp10_msg:delivery_id(Msg1),
           amqp10_msg:delivery_id(Msg3),
           true, accepted),
    {ok, M4} = amqp10_client:get_msg(Receiver1),
    ?assertEqual(<<"m4">>, amqp10_msg:body_bin(M4)),
    ok = amqp10_client:accept_msg(Receiver1, M4),

    ok = detach_link_sync(Receiver1),
    ok = detach_link_sync(Receiver2),
    ok = detach_link_sync(Sender),
    %% 1 green message
    ?assertMatch({ok, #{message_count := 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

property_ttl(Config) ->
    SourceQ = <<"source quorum queue">>,
    DeadLetterQ = <<"dead letter queue">>,
    SourceQAddr = rabbitmq_amqp_address:queue(SourceQ),
    DeadLetterQAddr = rabbitmq_amqp_address:queue(DeadLetterQ),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, DeadLetterQ, #{}),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  SourceQ,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {list, [{symbol, <<"group-id">>}]},
                                   <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                   <<"x-dead-letter-routing-key">> => {utf8, DeadLetterQ}
                                  }}),

    FilterRed = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                  {map, [{{symbol, <<"group-id">>}, {utf8, <<"red">>}}]}},
    {ok, ReceiverRed} = amqp10_client:attach_receiver_link(
                          Session, <<"receiver red">>, SourceQAddr,
                          unsettled, none, FilterRed),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, SourceQAddr),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_headers(
                     #{ttl => 6000},
                     amqp10_msg:set_properties(
                       #{group_id => <<"red">>},
                       amqp10_msg:new(<<"t1">>, <<"m1">>)))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_headers(
                     #{ttl => 4000},
                     amqp10_msg:set_properties(
                       #{group_id => <<"red">>},
                       amqp10_msg:new(<<"t2">>, <<"m2">>)))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_headers(
                     #{ttl => 1},
                     amqp10_msg:set_properties(
                       #{group_id => <<"blue">>},
                       amqp10_msg:new(<<"t3">>, <<"m3">>)))),
    ok = wait_for_accepts(3),
    ok = detach_link_sync(Sender),
    flush(sent),

    {ok, M1} = amqp10_client:get_msg(ReceiverRed),
    {ok, M2} = amqp10_client:get_msg(ReceiverRed),
    ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1)),
    ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2)),
    ok = amqp10_client:settle_msg(ReceiverRed, M1, released),
    %% When we detach, M2 should also be requeued.
    ok = detach_link_sync(ReceiverRed),

    {ok, ReceiverDead} = amqp10_client:attach_receiver_link(
                           Session, <<"dead letter queue receiver">>,
                           DeadLetterQAddr, settled),

    timer:sleep(5000),
    ok = amqp10_client:flow_link_credit(ReceiverDead, 10, never, false),
    receive {amqp10_msg, ReceiverDead, M3Dead} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3Dead))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverDead, M2Dead} ->
                ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2Dead))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverDead, M1Dead} ->
                ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1Dead))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(ReceiverDead, 10, never, true),
    ok = assert_credit_exhausted(ReceiverDead, ?LINE),

    ok = detach_link_sync(ReceiverDead),
    ?assertMatch({ok, #{message_count := 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, SourceQ)),
    ?assertMatch({ok, #{message_count := 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, DeadLetterQ)),
    ok = close(Init).

property_purge(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {list, [{symbol, <<"group-id">>}]}
                                  }}),

    FilterRed = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                  {map, [{{symbol, <<"group-id">>}, {utf8, <<"red">>}}]}},
    {ok, ReceiverRed} = amqp10_client:attach_receiver_link(
                          Session, <<"receiver red">>, Address,
                          unsettled, none, FilterRed),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"blue">>},
                     amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t4">>, <<"m4">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:new(<<"t5">>, <<"m5">>))),
    ok = wait_for_accepts(5),
    ok = detach_link_sync(Sender),
    flush(sent),

    ok = amqp10_client:flow_link_credit(ReceiverRed, 3, never),
    receive {amqp10_msg, ReceiverRed, M1} ->
                ?assertEqual(<<"m1">>, amqp10_msg:body_bin(M1)),
                ok = amqp10_client:accept_msg(ReceiverRed, M1)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverRed, M3} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3))
                %% Keeps this message checked out.
                %% Checked out messages should not get purged.
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverRed, M4} ->
                ?assertEqual(<<"m4">>, amqp10_msg:body_bin(M4)),
                ok = amqp10_client:settle_msg(ReceiverRed, M4, released)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ?assertMatch({ok, #{message_count := 3}}, %% m2, m4, m5
                 rabbitmq_amqp_client:purge_queue(LinkPair, QName)),
    ok = detach_link_sync(ReceiverRed),
    %% ready messages
    ?assertMatch({ok, #{message_count := 1}}, %% m3
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

property_filter_field_names_policy(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {Connection, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair, QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true
                                  }}),

    FilterRed = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
                  {map, [{{symbol, <<"group-id">>}, {utf8, <<"red">>}}]}},
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver red">>, Address,
                       unsettled, none, FilterRed),

    {ok, Sender1} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender1),
    %% Since (x-)filter-field-names isn't set on the quorum queue, this message's
    %% metadata in the quorum queue won't include the group ID.
    ok = amqp10_client:send_msg(
           Sender1, amqp10_msg:set_properties(
                      #{group_id => <<"red">>},
                      amqp10_msg:new(<<"t1">>, <<"m1">>))),
    receive {amqp10_disposition, {accepted, <<"t1">>}} -> ok
    after 9000 -> ct:fail({missing_accepted, ?LINE})
    end,
    ok = detach_link_sync(Sender1),

    %% Hence a consumer filtering on group-id won't receive this message.
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),
    ok = assert_credit_exhausted(Receiver, ?LINE),
    assert_no_msg_received(?LINE),

    %% However, if we apply a policy including group-id in filter-field-names,
    %% a new session should include the group ID in the message metadata.
    PolicyName = <<"qq-policy">>,
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 0, PolicyName, QName, <<"queues">>,
           [{<<"filter-field-names">>, [<<"user-id">>, <<"group-id">>]}]),

    {ok, Session2} = amqp10_client:begin_session(Connection),
    {ok, Sender2} = amqp10_client:attach_sender_link(Session2, <<"sender2">>, Address),
    ok = wait_for_credit(Sender2),
    ok = amqp10_client:send_msg(
           Sender2, amqp10_msg:set_properties(
                      #{group_id => <<"red">>},
                      amqp10_msg:new(<<"t2">>, <<"m2">>))),
    receive {amqp10_disposition, {accepted, <<"t2">>}} -> ok
    after 9000 -> ct:fail({missing_accepted, ?LINE})
    end,
    ok = detach_link_sync(Sender2),
    ok = end_session_sync(Session2),

    ok = amqp10_client:flow_link_credit(Receiver, 2, never, true),
    receive {amqp10_msg, Receiver, M2} ->
                ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2)),
                ?assertMatch(#{group_id := <<"red">>}, amqp10_msg:properties(M2)),
                ok = amqp10_client:accept_msg(Receiver, M2)
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(Receiver, ?LINE),
    ok = detach_link_sync(Receiver),

    ?assertMatch({ok, #{message_count := 1}}, % m1
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName).

%% Test that dead lettering at most once to a quorum queue with filtering enabled works.
property_dead_letter_at_most_once(Config) ->
    {_, Session, LinkPair} = Init = init(Config),
    QName1 = <<"q1">>,
    QName2 = <<"q2">>,
    Addr1 = rabbitmq_amqp_address:queue(QName1),
    Addr2 = rabbitmq_amqp_address:queue(QName2),
    {ok, _} = rabbitmq_amqp_client:declare_queue(
                LinkPair, QName1,
                #{arguments => #{<<"x-queue-type">> => {utf8, <<"classic">>},
                                 <<"x-dead-letter-exchange">> => {utf8, <<>>},
                                 <<"x-dead-letter-routing-key">> => {utf8, QName2},
                                 <<"x-message-ttl">> => {ulong, 0}}}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(
                LinkPair, QName2,
                #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                 <<"x-filter-enabled">> => true,
                                 <<"x-filter-field-names">> => {list, [{symbol, <<"group-id">>}]}
                                }}),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Addr1),
    wait_for_credit(Sender),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:set_application_properties(
                       #{<<"weight">> => 3},
                       amqp10_msg:new(<<"t1">>, <<"red light">>)))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_properties(
                     #{group_id => <<"red">>},
                     amqp10_msg:set_application_properties(
                       #{<<"weight">> => 99},
                       amqp10_msg:new(<<"t2">>, <<"red heavy">>)))),
    ok = wait_for_accepts(2),
    ok = detach_link_sync(Sender),

    Filter = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER =>
               {map, [{{symbol, <<"group-id">>}, {utf8, <<"&s:ed">>}}]},
               ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER =>
               {map, [{{utf8, <<"weight">>}, {ubyte, 99}}]}
              },
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver red heavy">>, Addr2,
                       unsettled, none, Filter),

    ok = amqp10_client:flow_link_credit(Receiver, 2, never, false),
    receive {amqp10_msg, Receiver, Msg} ->
                ?assertEqual(<<"red heavy">>, amqp10_msg:body_bin(Msg)),
                ok = amqp10_client:accept_msg(Receiver, Msg)
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,

    ok = detach_link_sync(Receiver),
    ?assertMatch({ok, #{message_count := 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName1)),
    ?assertMatch({ok, #{message_count := 1}}, % red light message
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName2)),
    ok = close(Init).

property_properties_section(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{notify_with_performative => true},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair, QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => property_field_names()
                                  }}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    Now = erlang:system_time(millisecond),
    To = rabbitmq_amqp_address:exchange(<<"some exchange">>, <<"routing key">>),
    ReplyTo = rabbitmq_amqp_address:queue(<<"some queue">>),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{message_id => {ulong, 999},
               user_id => <<"guest">>,
               to => To,
               subject => <<"🐇"/utf8>>,
               reply_to => ReplyTo,
               correlation_id => <<"corr-123">>,
               content_type => <<"text/plain">>,
               content_encoding => <<"some encoding">>,
               absolute_expiry_time => Now + 100_000,
               creation_time => Now,
               group_id => <<"my group ID">>,
               group_sequence => 16#ff_ff_ff_ff,
               reply_to_group_id => <<"other group ID">>},
             amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:new(<<"t2">>, <<"m2">>)),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{group_id => <<"my group ID">>},
             amqp10_msg:new(<<"t3">>, <<"m3">>))),

    ok = wait_for_accepts(3),
    ok = detach_link_sync(Sender),
    flush(sent),

    PropsFilter1 = [
                    {{symbol, <<"message-id">>}, {ulong, 999}},
                    {{symbol, <<"user-id">>}, {binary, <<"guest">>}},
                    {{symbol, <<"subject">>}, {utf8, <<"🐇"/utf8>>}},
                    {{symbol, <<"to">>}, {utf8, To}},
                    {{symbol, <<"reply-to">>}, {utf8, ReplyTo}},
                    {{symbol, <<"correlation-id">>}, {utf8, <<"corr-123">>}},
                    {{symbol, <<"content-type">>}, {symbol, <<"text/plain">>}},
                    {{symbol, <<"content-encoding">>}, {symbol, <<"some encoding">>}},
                    {{symbol, <<"absolute-expiry-time">>}, {timestamp, Now + 100_000}},
                    {{symbol, <<"creation-time">>}, {timestamp, Now}},
                    {{symbol, <<"group-id">>}, {utf8, <<"my group ID">>}},
                    {{symbol, <<"group-sequence">>}, {uint, 16#ff_ff_ff_ff}},
                    {{symbol, <<"reply-to-group-id">>}, {utf8, <<"other group ID">>}}
                   ],
    Filter1 = #{<<"props">> => #filter{descriptor = ?DESCRIPTOR_NAME_PROPERTIES_FILTER,
                                       value = {map, PropsFilter1}}},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        settled, configuration, Filter1),
    ok = amqp10_client:flow_link_credit(Receiver1, 10, never, true),
    receive {amqp10_msg, Receiver1, M1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(M1)),
                ok = amqp10_client:accept_msg(Receiver1, M1)
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(Receiver1, ?LINE),
    ok = detach_link_sync(Receiver1),

    PropsFilter2 = [{{symbol, <<"group-id">>}, {utf8, <<"my group ID">>}}],
    Filter2 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter2}},
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        unsettled, configuration, Filter2),
    {ok, M3} = amqp10_client:get_msg(Receiver2),
    ok = amqp10_client:accept_msg(Receiver2, M3),
    ?assertEqual([<<"m3">>], amqp10_msg:body(M3)),
    ok = detach_link_sync(Receiver2),

    %% Filter is in place, but no message matches.
    PropsFilter3 = [{{symbol, <<"group-id">>}, {utf8, <<"no match">>}}],
    Filter3 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter3}},
    {ok, Receiver3} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 3">>, Address,
                        unsettled, configuration, Filter3),
    receive {amqp10_event, {link, Receiver3, {attached, #'v1_0.attach'{}}}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver3, 10, never, true),
    ok = assert_credit_exhausted(Receiver3, ?LINE),
    ok = detach_link_sync(Receiver3),

    %% Wrong type should fail validation in the server.
    %% RabbitMQ should exclude this filter in its reply attach frame because
    %% "the sending endpoint [RabbitMQ] sets the filter actually in place".
    %% Hence, no filter expression is actually in place and we should receive the remaining message.
    PropsFilter4 = [{{symbol, <<"group-id">>}, {uint, 3}}],
    Filter4 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter4}},
    {ok, Receiver4} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 4">>, Address,
                        unsettled, configuration, Filter4),
    receive {amqp10_event,
             {link, Receiver4,
              {attached, #'v1_0.attach'{
                            source = #'v1_0.source'{filter = {map, ActualFilter}}}}}} ->
                ?assertEqual([], ActualFilter)
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, M2} = amqp10_client:get_msg(Receiver4),
    ok = amqp10_client:accept_msg(Receiver4, M2),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2)),
    ok = detach_link_sync(Receiver4),

    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

property_application_properties_section(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{notify_with_performative => true},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair, QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true}}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_application_properties(
             #{<<"k1">> => -2,
               <<"k2">> => 10,
               <<"k3">> => false,
               <<"k4">> => true,
               <<"k5">> => <<"hey">>},
             amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_application_properties(
             #{<<"k2">> => 10.1},
             amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:new(<<"t3">>, <<"m3">>)),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_application_properties(
             #{<<"k2">> => 10.0},
             amqp10_msg:new(<<"t4">>, <<"m4">>))),

    ok = wait_for_accepts(4),
    ok = detach_link_sync(Sender),
    flush(sent),

    AppPropsFilter0 = [{{utf8, <<"k5">>}, {symbol, <<"no match">>}}],
    Filter0 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter0}},
    {ok, Receiver0} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 0">>, Address,
                        unsettled, configuration, Filter0),
    receive {amqp10_event, {link, Receiver0, {attached, #'v1_0.attach'{}}}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver0, 10, never, true),
    ok = assert_credit_exhausted(Receiver0, ?LINE),
    ok = detach_link_sync(Receiver0),

    AppPropsFilter1 = [
                       {{utf8, <<"k1">>}, {int, -2}},
                       {{utf8, <<"k5">>}, {symbol, <<"hey">>}},
                       {{utf8, <<"k4">>}, {boolean, true}},
                       {{utf8, <<"k3">>}, false}
                      ],
    Filter1 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter1}},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        settled, configuration, Filter1),
    receive {amqp10_event,
             {link, Receiver1,
              {attached, #'v1_0.attach'{
                            source = #'v1_0.source'{filter = {map, ActualFilter1}}}}}} ->
                ?assertMatch(
                   {described, _Type, {map, [
                                             {{utf8, <<"k1">>}, {int, -2}},
                                             {{utf8, <<"k5">>}, {symbol, <<"hey">>}},
                                             {{utf8, <<"k4">>}, true},
                                             {{utf8, <<"k3">>}, false}
                                            ]}},
                   proplists:get_value({symbol, ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER}, ActualFilter1))
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver1, 10, never, true),
    receive {amqp10_msg, Receiver1, M1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(M1)),
                ok = amqp10_client:accept_msg(Receiver1, M1)
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(Receiver1, ?LINE),
    ok = detach_link_sync(Receiver1),

    %% Due to simple type matching [filtex-v1.0-wd09 §4.1.1]
    %% we expect integer 10 to also match number 10.0.
    AppPropsFilter2 = [{{utf8, <<"k2">>}, {uint, 10}}],
    Filter2 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter2}},
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        unsettled, configuration, Filter2),
    {ok, M4} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m4">>], amqp10_msg:body(M4)),
    ok = amqp10_client:accept_msg(Receiver2, M4),
    ok = detach_link_sync(Receiver2),

    %% A reference field value of NULL should always match. [filtex-v1.0-wd09 §4.1.1]
    AppPropsFilter3 = [{{utf8, <<"k2">>}, null}],
    Filter3 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter3}},
    {ok, Receiver3} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 3">>, Address,
                        unsettled, configuration, Filter3),
    {ok, M2} = amqp10_client:get_msg(Receiver3),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2)),
    ok = amqp10_client:accept_msg(Receiver3, M2),
    ok = detach_link_sync(Receiver3),

    %% Wrong type should fail validation in the server.
    %% RabbitMQ should exclude this filter in its reply attach frame because
    %% "the sending endpoint [RabbitMQ] sets the filter actually in place".
    %% Hence, no filter expression is actually in place and we should receive all remaining messages.
    AppPropsFilter4 = [{{symbol, <<"k2">>}, {uint, 10}}],
    Filter4 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter4}},
    {ok, Receiver4} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 4">>, Address,
                        unsettled, configuration, Filter4),
    receive {amqp10_event,
             {link, Receiver4,
              {attached, #'v1_0.attach'{
                            source = #'v1_0.source'{filter = {map, ActualFilter4}}}}}} ->
                ?assertEqual([], ActualFilter4)
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, M3} = amqp10_client:get_msg(Receiver4),
    ?assertEqual([<<"m3">>], amqp10_msg:body(M3)),
    ok = amqp10_client:accept_msg(Receiver4, M3),
    ok = detach_link_sync(Receiver4),

    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Test filter expressions matching multiple message sections.
property_multiple_sections(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection, Session, LinkPair} = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {array, symbol, [{symbol, <<"subject">>}]}
                                  }}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{subject => <<"The Subject">>},
             amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_application_properties(
             #{<<"The Key">> => -123},
             amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{subject => <<"The Subject">>},
             amqp10_msg:set_application_properties(
               #{<<"The Key">> => -123},
               amqp10_msg:new(<<"t3">>, <<"m3">>)))),

    ok = wait_for_accepts(3),
    ok = detach_link_sync(Sender),
    flush(sent),

    PropsFilter = [{{symbol, <<"subject">>}, {utf8, <<"The Subject">>}}],
    AppPropsFilter = [{{utf8, <<"The Key">>}, {byte, -123}}],

    Filter1 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter},
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter}},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        unsettled, configuration, Filter1),
    {ok, M3} = amqp10_client:get_msg(Receiver1),
    ?assertEqual([<<"m3">>], amqp10_msg:body(M3)),
    ok = amqp10_client:accept_msg(Receiver1, M3),
    ok = detach_link_sync(Receiver1),

    Filter2 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter}},
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        unsettled, configuration, Filter2),
    {ok, M2} = amqp10_client:get_msg(Receiver2),
    ok = amqp10_client:accept_msg(Receiver2, M2),
    ?assertEqual([<<"m2">>], amqp10_msg:body(M2)),
    ok = detach_link_sync(Receiver2),

    Filter3 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter}},
    {ok, Receiver3} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 3">>, Address,
                        unsettled, configuration, Filter3),
    {ok, M1} = amqp10_client:get_msg(Receiver3),
    ?assertEqual([<<"m1">>], amqp10_msg:body(M1)),
    ok = amqp10_client:accept_msg(Receiver3, M1),
    ok = detach_link_sync(Receiver3),

    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Filter a small subset from many messages.
%% We test here that flow control still works correctly.
property_filter_few_messages_from_many(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection, Session, LinkPair} = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => {array, symbol, [{symbol, <<"group-id">>}]}
                                  }}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{group_id => <<"my group ID">>},
             amqp10_msg:new(<<"t1">>, <<"first msg">>))),
    ok = send_messages(Sender, 1000, false),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{group_id => <<"my group ID">>},
             amqp10_msg:new(<<"t2">>, <<"last msg">>))),
    ok = wait_for_accepts(1002),
    ok = detach_link_sync(Sender),
    flush(sent),

    %% Our filter should cause us to receive only the first and
    %% last message out of the 1002 messages from the queue.
    PropsFilter = [{{symbol, <<"group-id">>}, {utf8, <<"my group ID">>}}],
    Filter = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter}},
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address,
                       unsettled, configuration, Filter),

    ok = amqp10_client:flow_link_credit(Receiver, 2, never),
    receive {amqp10_msg, Receiver, M1} ->
                ?assertEqual([<<"first msg">>], amqp10_msg:body(M1)),
                ok = amqp10_client:accept_msg(Receiver, M1)
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver, M2} ->
                ?assertEqual([<<"last msg">>], amqp10_msg:body(M2)),
                ok = amqp10_client:accept_msg(Receiver, M2)
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = detach_link_sync(Receiver),

    {ok, #{message_count := 1000}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

property_string_modifier(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection, Session, LinkPair} = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair, QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true,
                                   <<"x-filter-field-names">> => property_field_names()
                                  }}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{to => <<"abc 1">>,
               reply_to => <<"abc 2">>,
               subject => <<"abc 3">>,
               group_id => <<"abc 4">>,
               reply_to_group_id => <<"abc 5">>,
               message_id => {utf8, <<"abc 6">>},
               correlation_id => <<"abc 7">>,
               group_sequence => 16#ff_ff_ff_ff},
             amqp10_msg:set_application_properties(
               #{<<"k1">> => <<"abc 8">>,
                 <<"k2">> => <<"abc 9">>,
                 <<"k3">> => 3},
               amqp10_msg:new(<<"t1">>, <<"m1">>)))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_application_properties(
             #{<<"k1">> => <<"abc">>},
             amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{subject => <<"&Hello">>,
               reply_to_group_id => <<"xyz 5">>},
             amqp10_msg:new(<<"t3">>, <<"m3">>))),

    ok = wait_for_accepts(3),
    ok = detach_link_sync(Sender),
    flush(sent),

    AppPropsFilter00 = [{{utf8, <<"k1">>}, {utf8, <<"&p:abz">>}}],
    Filter00 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter00}},
    {ok, Receiver00} = amqp10_client:attach_receiver_link(
                         Session, <<"receiver 00">>, Address,
                         settled, configuration, Filter00),
    ok = amqp10_client:flow_link_credit(Receiver00, 5, never, true),
    ok = assert_credit_exhausted(Receiver00, ?LINE),
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver00),

    AppPropsFilter01 = [{{utf8, <<"k3">>}, {utf8, <<"&s:3">>}}],
    Filter01 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter01}},
    {ok, Receiver01} = amqp10_client:attach_receiver_link(
                         Session, <<"receiver 01">>, Address,
                         settled, configuration, Filter01),
    ok = amqp10_client:flow_link_credit(Receiver01, 5, never, true),
    ok = assert_credit_exhausted(Receiver01, ?LINE),
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver01),

    PropsFilter1 = [
                    {{symbol, <<"to">>}, {utf8, <<"&p:abc ">>}},
                    {{symbol, <<"reply-to">>}, {utf8, <<"&p:abc">>}},
                    {{symbol, <<"subject">>}, {utf8, <<"&p:ab">>}},
                    {{symbol, <<"group-id">>}, {utf8, <<"&p:a">>}},
                    {{symbol, <<"reply-to-group-id">>}, {utf8, <<"&s: 5">>}},
                    {{symbol, <<"correlation-id">>}, {utf8, <<"&s:abc 7">>}},
                    {{symbol, <<"message-id">>}, {utf8, <<"&p:abc 6">>}}
                   ],
    AppPropsFilter1 = [
                       {{utf8, <<"k1">>}, {utf8, <<"&s: 8">>}},
                       {{utf8, <<"k2">>}, {utf8, <<"&p:abc ">>}}
                      ],
    Filter1 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter1},
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter1}},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        settled, configuration, Filter1),
    ok = amqp10_client:flow_link_credit(Receiver1, 10, never),
    receive {amqp10_msg, Receiver1, M1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(M1)),
                ok = amqp10_client:accept_msg(Receiver1, M1)
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver1),

    %% '&&" is the escape prefix for case-sensitive matching of a string starting with ‘&’
    PropsFilter2 = [{{symbol, <<"subject">>}, {utf8, <<"&&Hello">>}}],
    Filter2 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter2}},
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        settled, configuration, Filter2),
    {ok, M3} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m3">>], amqp10_msg:body(M3)),
    ok = detach_link_sync(Receiver2),

    %% m2
    {ok, #{message_count := 1}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Consumers filter messages by application-properties.
jms_application_properties(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),

    {_, Session, LinkPair} = Init = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair, QName,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                   <<"x-filter-enabled">> => true
                                  }}),

    {ok, R1} = amqp10_client:attach_receiver_link(
                 Session, <<"r1">>, Address, unsettled, none,
                 jms_selector(<<"color = 'red' AND weight > 10">>)),
    {ok, R2} = amqp10_client:attach_receiver_link(
                 Session, <<"r2">>, Address, unsettled, none,
                 jms_selector(<<"color = 'purple' OR color = 'green'">>)),
    {ok, R3} = amqp10_client:attach_receiver_link(
                 Session, <<"r3">>, Address, unsettled, none,
                 jms_selector(<<"name LIKE 'prod%x'">>)),
    {ok, R4} = amqp10_client:attach_receiver_link(
                 Session, <<"r4">>, Address, unsettled, none,
                 jms_selector(<<"price BETWEEN 24.0 AND 26.0">>)),
    {ok, R5} = amqp10_client:attach_receiver_link(
                 Session, <<"r5">>, Address, unsettled, none,
                 jms_selector(<<"category IN ('grocery', 'food', 'edible')">>)),
    {ok, R6} = amqp10_client:attach_receiver_link(
                 Session, <<"r6">>, Address, unsettled, none,
                 jms_selector(<<"description IS NULL AND price * 2 > 20.0">>)),
    {ok, R7} = amqp10_client:attach_receiver_link(
                 Session, <<"r7">>, Address, unsettled, none,
                 jms_selector(<<"NOT available AND discount IS NULL">>)),

    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_application_properties(
                     #{<<"color">> => <<"red">>,
                       <<"weight">> => 5},
                     amqp10_msg:new(<<"t1">>, <<"m1">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_application_properties(
                     #{<<"color">> => <<"green">>,
                       <<"weight">> => 20},
                     amqp10_msg:new(<<"t2">>, <<"m2">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_application_properties(
                     #{<<"color">> => <<"red">>,
                       <<"weight">> => 20},
                     amqp10_msg:new(<<"t3">>, <<"m3">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_application_properties(
                     #{<<"name">> => <<"product-x">>,
                       <<"price">> => 18.5,
                       <<"category">> => <<"electronics">>,
                       <<"available">> => true},
                     amqp10_msg:new(<<"t4">>, <<"m4">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_application_properties(
                     #{<<"name">> => <<"service-y">>,
                       <<"price">> => 25.0,
                       <<"category">> => <<"service">>,
                       <<"description">> => <<"Premium service">>,
                       <<"available">> => false},
                     amqp10_msg:new(<<"t5">>, <<"m5">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_application_properties(
                     #{<<"name">> => <<"product-z">>,
                       <<"price">> => 8.75,
                       <<"category">> => <<"food">>,
                       <<"available">> => true,
                       <<"discount">> => 0.25},
                     amqp10_msg:new(<<"t6">>, <<"m6">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_application_properties(
                     #{<<"name">> => <<"item-special">>,
                       <<"price">> => 12.0,
                       <<"available">> => true},
                     amqp10_msg:new(<<"t7">>, <<"m7">>))),
    ok = amqp10_client:send_msg(
           Sender, amqp10_msg:set_application_properties(
                     #{<<"name">> => <<"clearance-item">>,
                       <<"price">> => 5.0,
                       <<"available">> => false},
                     amqp10_msg:new(<<"t8">>, <<"m8">>))),
    ok = wait_for_accepts(8),
    ok = detach_link_sync(Sender),
    flush(sent),

    ok = amqp10_client:flow_link_credit(R1, 10, never, true),
    receive {amqp10_msg, R1, M3} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3)),
                ok = amqp10_client:accept_msg(R1, M3)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R1, ?LINE),

    ok = amqp10_client:flow_link_credit(R2, 10, never, true),
    receive {amqp10_msg, R2, M2} ->
                ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2)),
                ok = amqp10_client:accept_msg(R2, M2)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R2, ?LINE),

    ok = amqp10_client:flow_link_credit(R3, 10, never, true),
    receive {amqp10_msg, R3, M4} ->
                ?assertEqual(<<"m4">>, amqp10_msg:body_bin(M4)),
                ok = amqp10_client:accept_msg(R3, M4)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R3, ?LINE),

    ok = amqp10_client:flow_link_credit(R4, 10, never, true),
    receive {amqp10_msg, R4, M5} ->
                ?assertEqual(<<"m5">>, amqp10_msg:body_bin(M5)),
                ok = amqp10_client:accept_msg(R4, M5)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R4, ?LINE),

    ok = amqp10_client:flow_link_credit(R5, 10, never, true),
    receive {amqp10_msg, R5, M6} ->
                ?assertEqual(<<"m6">>, amqp10_msg:body_bin(M6)),
                ok = amqp10_client:accept_msg(R5, M6)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R5, ?LINE),

    ok = amqp10_client:flow_link_credit(R6, 10, never, true),
    receive {amqp10_msg, R6, M7} ->
                ?assertEqual(<<"m7">>, amqp10_msg:body_bin(M7)),
                ok = amqp10_client:accept_msg(R6, M7)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R6, ?LINE),

    ok = amqp10_client:flow_link_credit(R7, 10, never, true),
    receive {amqp10_msg, R7, M8} ->
                ?assertEqual(<<"m8">>, amqp10_msg:body_bin(M8)),
                ok = amqp10_client:accept_msg(R7, M8)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R7, ?LINE),

    lists:foreach(fun(R) ->
                          ok = detach_link_sync(R) end,
                  [R1, R2, R3, R4, R5, R6, R7]),

    ?assertMatch({ok, #{message_count := 1}}, % m1
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

mutually_exclusive_queue_args(Config) ->
    {_, _, LinkPair} = Init = init(Config),
    {error, Resp} = rabbitmq_amqp_client:declare_queue(
                      LinkPair, <<"invalid args">>,
                      #{arguments => #{<<"x-queue-type">> => {utf8, <<"quorum">>},
                                       <<"x-filter-enabled">> => true,
                                       <<"x-single-active-consumer">> => true
                                      }}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(
       #'v1_0.amqp_value'{
          content = {utf8, <<"queue arguments x-filter-enabled and x-single-active-consumer "
                             "are mutually exclusive for quorum queue 'invalid args' in vhost '/'">>}},
       amqp10_msg:body(Resp)),
    ok = close(Init).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

jms_selector(String) ->
    #{<<"jms-selector">> => #filter{descriptor = ?DESCRIPTOR_NAME_SELECTOR_FILTER,
                                    value = {utf8, String}}}.

assert_no_msg_received(Line) ->
    receive {amqp10_msg, _, _} = Msg ->
                ct:fail({received_unexpected_msg, Line, Msg})
    after 10 ->
              ok
    end.

assert_credit_exhausted(Receiver, Line) ->
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 9000 -> ct:fail({missing_credit_exhausted, Line})
    end.

property_field_names() ->
    L = [<<"message-id">>,
         <<"user-id">>,
         <<"to">>,
         <<"subject">>,
         <<"reply-to">>,
         <<"correlation-id">>,
         <<"content-type">>,
         <<"content-encoding">>,
         <<"absolute-expiry-time">>,
         <<"creation-time">>,
         <<"group-id">>,
         <<"group-sequence">>,
         <<"reply-to-group-id">>],
    {list, [{symbol, Name} || Name <- L]}.
