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
         close/1,
         flush/1,
         wait_for_credit/1,
         wait_for_accepts/1,
         detach_link_sync/1,
         end_session_sync/1]).

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
    receive {amqp10_event, {link, ReceiverBlue, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(ReceiverRed, 10, never, true),
    receive {amqp10_msg, ReceiverRed, M3} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3)),
                ok = amqp10_client:accept_msg(ReceiverRed, M3)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, ReceiverRed, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

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
    receive {amqp10_event, {link, ReceiverBlue, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(ReceiverRed, 10, never, true),
    receive {amqp10_msg, ReceiverRed, M3} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3)),
                ok = amqp10_client:accept_msg(ReceiverRed, M3)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, ReceiverRed, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

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
    receive {amqp10_event, {link, ReceiverBlue, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(ReceiverRed, 10, never, true),
    Msg3a = receive {amqp10_msg, ReceiverRed, M3a} ->
                        ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3a)),
                        M3a
            after 5000 -> ct:fail({missing_msg, ?LINE})
            end,
    receive {amqp10_event, {link, ReceiverRed, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

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
    receive {amqp10_event, {link, ReceiverBlue, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(ReceiverRed, 10, never, true),
    receive {amqp10_msg, ReceiverRed, M3b} ->
                ?assertEqual(<<"m3">>, amqp10_msg:body_bin(M3b)),
                ok = amqp10_client:accept_msg(ReceiverBlue, M3b)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, ReceiverRed, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

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
    receive {amqp10_event, {link, Receiver1, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
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
    receive {amqp10_event, {link, ReceiverDead, credit_exhausted}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

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
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    receive {amqp10_msg, Receiver, UnexpectedMsg} ->
                ct:fail({unexpected_msg, UnexpectedMsg})
    after 5 -> ok
    end,

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
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = detach_link_sync(Receiver),

    ?assertMatch({ok, #{message_count := 1}}, % m1
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, PolicyName).

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
    receive {amqp10_event, {link, R1, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(R2, 10, never, true),
    receive {amqp10_msg, R2, M2} ->
                ?assertEqual(<<"m2">>, amqp10_msg:body_bin(M2)),
                ok = amqp10_client:accept_msg(R2, M2)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, R2, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(R3, 10, never, true),
    receive {amqp10_msg, R3, M4} ->
                ?assertEqual(<<"m4">>, amqp10_msg:body_bin(M4)),
                ok = amqp10_client:accept_msg(R3, M4)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, R3, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(R4, 10, never, true),
    receive {amqp10_msg, R4, M5} ->
                ?assertEqual(<<"m5">>, amqp10_msg:body_bin(M5)),
                ok = amqp10_client:accept_msg(R4, M5)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, R4, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(R5, 10, never, true),
    receive {amqp10_msg, R5, M6} ->
                ?assertEqual(<<"m6">>, amqp10_msg:body_bin(M6)),
                ok = amqp10_client:accept_msg(R5, M6)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, R5, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(R6, 10, never, true),
    receive {amqp10_msg, R6, M7} ->
                ?assertEqual(<<"m7">>, amqp10_msg:body_bin(M7)),
                ok = amqp10_client:accept_msg(R6, M7)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, R6, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:flow_link_credit(R7, 10, never, true),
    receive {amqp10_msg, R7, M8} ->
                ?assertEqual(<<"m8">>, amqp10_msg:body_bin(M8)),
                ok = amqp10_client:accept_msg(R7, M8)
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, R7, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    lists:foreach(fun(R) ->
                          ok = detach_link_sync(R) end,
                  [R1, R2, R3, R4, R5, R6, R7]),

    ?assertMatch({ok, #{message_count := 1}}, % m1
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),
    ok = close(Init).

jms_selector(String) ->
    #{<<"jms-selector">> => #filter{descriptor = ?DESCRIPTOR_NAME_SELECTOR_FILTER,
                                    value = {utf8, String}}}.


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
