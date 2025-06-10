%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

%% Test suite for §4 Property Filter Expressions of
%% AMQP Filter Expressions Version 1.0 Working Draft 09
%% filtering from a stream.
-module(amqp_filter_prop_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_client/include/amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_filter.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/1]).
-import(amqp_utils,
        [init/1,
         connection_config/1,
         flush/1,
         wait_for_credit/1,
         wait_for_accepts/1,
         send_messages/3,
         detach_link_sync/1,
         end_session_sync/1,
         wait_for_session_end/1,
         close_connection_sync/1]).

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       properties_section,
       application_properties_section,
       multiple_sections,
       filter_few_messages_from_many,
       string_modifier
      ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:merge_app_env(
      Config, {rabbit, [{stream_tick_interval, 1000}]}).

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
    rabbit_ct_helpers:run_teardown_steps(Config,
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

properties_section(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(Stream),

    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{notify_with_performative => true},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  Stream,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}}),
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
    Filter1 = #{<<"from start">> => #filter{descriptor = <<"rabbitmq:stream-offset-spec">>,
                                            value = {symbol, <<"first">>}},
                <<"props">> => #filter{descriptor = ?DESCRIPTOR_NAME_PROPERTIES_FILTER,
                                       value = {map, PropsFilter1}}},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        settled, configuration, Filter1),
    ok = amqp10_client:flow_link_credit(Receiver1, 10, never),
    receive {amqp10_msg, Receiver1, R1M1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(R1M1))
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver1),

    PropsFilter2 = [{{symbol, <<"group-id">>}, {utf8, <<"my group ID">>}}],
    Filter2 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter2}},
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        unsettled, configuration, Filter2),
    {ok, R2M1} = amqp10_client:get_msg(Receiver2),
    {ok, R2M2} = amqp10_client:get_msg(Receiver2),
    ok = amqp10_client:accept_msg(Receiver2, R2M1),
    ok = amqp10_client:accept_msg(Receiver2, R2M2),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R2M1)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R2M2)),
    ok = detach_link_sync(Receiver2),

    %% Filter is in place, but no message matches.
    PropsFilter3 = [{{symbol, <<"group-id">>}, {utf8, <<"no match">>}}],
    Filter3 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter3}},
    {ok, Receiver3} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 3">>, Address,
                        unsettled, configuration, Filter3),
    receive {amqp10_event, {link, Receiver3, {attached, #'v1_0.attach'{}}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver3, 10, never),
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver3),

    %% Wrong type should fail validation in the server.
    %% RabbitMQ should exclude this filter in its reply attach frame because
    %% "the sending endpoint [RabbitMQ] sets the filter actually in place".
    %% Hence, no filter expression is actually in place and we should receive all messages.
    PropsFilter4 = [{{symbol, <<"group-id">>}, {uint, 3}}],
    Filter4 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter4}},
    {ok, Receiver4} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 4">>, Address,
                        unsettled, configuration, Filter4),
    receive {amqp10_event,
             {link, Receiver4,
              {attached, #'v1_0.attach'{
                            source = #'v1_0.source'{filter = {map, ActualFilter}}}}}} ->
                ?assertMatch([{{symbol,<<"rabbitmq:stream-offset-spec">>}, _}],
                             ActualFilter)
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, R4M1} = amqp10_client:get_msg(Receiver4),
    {ok, R4M2} = amqp10_client:get_msg(Receiver4),
    {ok, R4M3} = amqp10_client:get_msg(Receiver4),
    ok = amqp10_client:accept_msg(Receiver4, R4M1),
    ok = amqp10_client:accept_msg(Receiver4, R4M2),
    ok = amqp10_client:accept_msg(Receiver4, R4M3),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R4M1)),
    ?assertEqual([<<"m2">>], amqp10_msg:body(R4M2)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R4M3)),
    ok = detach_link_sync(Receiver4),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, Stream),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

application_properties_section(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(Stream),
    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{notify_with_performative => true},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  Stream,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}}),
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
    Filter0 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter0}},
    {ok, Receiver0} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 0">>, Address,
                        unsettled, configuration, Filter0),
    receive {amqp10_event, {link, Receiver0, {attached, #'v1_0.attach'{}}}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver0, 10, never),
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver0),

    AppPropsFilter1 = [
                       {{utf8, <<"k1">>}, {int, -2}},
                       {{utf8, <<"k5">>}, {symbol, <<"hey">>}},
                       {{utf8, <<"k4">>}, {boolean, true}},
                       {{utf8, <<"k3">>}, false}
                      ],
    Filter1 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter1}},
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
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver1, 10, never),
    receive {amqp10_msg, Receiver1, R1M1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(R1M1))
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver1),

    %% Due to simple type matching [filtex-v1.0-wd09 §4.1.1]
    %% we expect integer 10 to also match number 10.0.
    AppPropsFilter2 = [{{utf8, <<"k2">>}, {uint, 10}}],
    Filter2 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter2}},
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        unsettled, configuration, Filter2),
    {ok, R2M1} = amqp10_client:get_msg(Receiver2),
    {ok, R2M2} = amqp10_client:get_msg(Receiver2),
    ok = amqp10_client:accept_msg(Receiver2, R2M1),
    ok = amqp10_client:accept_msg(Receiver2, R2M2),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R2M1)),
    ?assertEqual([<<"m4">>], amqp10_msg:body(R2M2)),
    ok = detach_link_sync(Receiver2),

    %% A reference field value of NULL should always match. [filtex-v1.0-wd09 §4.1.1]
    AppPropsFilter3 = [{{utf8, <<"k2">>}, null}],
    Filter3 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter3}},
    {ok, Receiver3} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 3">>, Address,
                        unsettled, configuration, Filter3),
    {ok, R3M1} = amqp10_client:get_msg(Receiver3),
    {ok, R3M2} = amqp10_client:get_msg(Receiver3),
    {ok, R3M3} = amqp10_client:get_msg(Receiver3),
    ok = amqp10_client:accept_msg(Receiver3, R3M1),
    ok = amqp10_client:accept_msg(Receiver3, R3M2),
    ok = amqp10_client:accept_msg(Receiver3, R3M3),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R3M1)),
    ?assertEqual([<<"m2">>], amqp10_msg:body(R3M2)),
    ?assertEqual([<<"m4">>], amqp10_msg:body(R3M3)),
    ok = detach_link_sync(Receiver3),

    %% Wrong type should fail validation in the server.
    %% RabbitMQ should exclude this filter in its reply attach frame because
    %% "the sending endpoint [RabbitMQ] sets the filter actually in place".
    %% Hence, no filter expression is actually in place and we should receive all messages.
    AppPropsFilter4 = [{{symbol, <<"k2">>}, {uint, 10}}],
    Filter4 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter4}},
    {ok, Receiver4} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 4">>, Address,
                        unsettled, configuration, Filter4),
    receive {amqp10_event,
             {link, Receiver4,
              {attached, #'v1_0.attach'{
                            source = #'v1_0.source'{filter = {map, ActualFilter4}}}}}} ->
                ?assertMatch([{{symbol,<<"rabbitmq:stream-offset-spec">>}, _}],
                             ActualFilter4)
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, R4M1} = amqp10_client:get_msg(Receiver4),
    {ok, R4M2} = amqp10_client:get_msg(Receiver4),
    {ok, R4M3} = amqp10_client:get_msg(Receiver4),
    {ok, R4M4} = amqp10_client:get_msg(Receiver4),
    ok = amqp10_client:accept_msg(Receiver4, R4M1),
    ok = amqp10_client:accept_msg(Receiver4, R4M2),
    ok = amqp10_client:accept_msg(Receiver4, R4M3),
    ok = amqp10_client:accept_msg(Receiver4, R4M4),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R4M1)),
    ?assertEqual([<<"m2">>], amqp10_msg:body(R4M2)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R4M3)),
    ?assertEqual([<<"m4">>], amqp10_msg:body(R4M4)),
    ok = detach_link_sync(Receiver4),

    %% Complex filter (too many properties to filter on) should fail validation in the server.
    %% RabbitMQ should exclude this filter in its reply attach frame because
    %% "the sending endpoint [RabbitMQ] sets the filter actually in place".
    %% Hence, no filter expression is actually in place and we should receive all messages.
    AppPropsFilter5 = [{{utf8, integer_to_binary(N)}, {uint, 1}} ||
                       N <- lists:seq(1, 17)],
    Filter5 = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter5}},
    {ok, Receiver5} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 5">>, Address,
                        unsettled, configuration, Filter5),
    receive {amqp10_event,
             {link, Receiver5,
              {attached, #'v1_0.attach'{
                            source = #'v1_0.source'{filter = {map, ActualFilter5}}}}}} ->
                ?assertMatch([{{symbol,<<"rabbitmq:stream-offset-spec">>}, _}],
                             ActualFilter5)
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, R5M1} = amqp10_client:get_msg(Receiver5),
    {ok, R5M2} = amqp10_client:get_msg(Receiver5),
    {ok, R5M3} = amqp10_client:get_msg(Receiver5),
    {ok, R5M4} = amqp10_client:get_msg(Receiver5),
    ok = amqp10_client:accept_msg(Receiver5, R5M1),
    ok = amqp10_client:accept_msg(Receiver5, R5M2),
    ok = amqp10_client:accept_msg(Receiver5, R5M3),
    ok = amqp10_client:accept_msg(Receiver5, R5M4),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R5M1)),
    ?assertEqual([<<"m2">>], amqp10_msg:body(R5M2)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R5M3)),
    ?assertEqual([<<"m4">>], amqp10_msg:body(R5M4)),
    ok = detach_link_sync(Receiver5),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, Stream),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Test filter expressions matching multiple message sections.
multiple_sections(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(Stream),
    {Connection, Session, LinkPair} = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  Stream,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}}),
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
    Filter1 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter},
                <<"rabbitmq:stream-offset-spec">> => <<"first">>},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        unsettled, configuration, Filter1),
    {ok, R1M1} = amqp10_client:get_msg(Receiver1),
    {ok, R1M3} = amqp10_client:get_msg(Receiver1),
    ok = amqp10_client:accept_msg(Receiver1, R1M1),
    ok = amqp10_client:accept_msg(Receiver1, R1M3),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R1M1)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R1M3)),
    ok = detach_link_sync(Receiver1),

    AppPropsFilter = [{{utf8, <<"The Key">>}, {byte, -123}}],
    Filter2 = #{?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter},
                <<"rabbitmq:stream-offset-spec">> => <<"first">>},
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        unsettled, configuration, Filter2),
    {ok, R2M2} = amqp10_client:get_msg(Receiver2),
    {ok, R2M3} = amqp10_client:get_msg(Receiver2),
    ok = amqp10_client:accept_msg(Receiver2, R2M2),
    ok = amqp10_client:accept_msg(Receiver2, R2M3),
    ?assertEqual([<<"m2">>], amqp10_msg:body(R2M2)),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R2M3)),
    ok = detach_link_sync(Receiver2),

    Filter3 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter},
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter},
                <<"rabbitmq:stream-offset-spec">> => <<"first">>},
    {ok, Receiver3} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 3">>, Address,
                        unsettled, configuration, Filter3),
    {ok, R3M3} = amqp10_client:get_msg(Receiver3),
    ok = amqp10_client:accept_msg(Receiver3, R3M3),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R3M3)),
    ok = detach_link_sync(Receiver3),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, Stream),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Filter a small subset from many messages.
%% We test here that flow control still works correctly.
filter_few_messages_from_many(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(Stream),
    {Connection, Session, LinkPair} = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  Stream,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}}),
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
    %% last message out of the 1002 messages in the stream.
    PropsFilter = [{{symbol, <<"group-id">>}, {utf8, <<"my group ID">>}}],
    Filter = #{<<"rabbitmq:stream-offset-spec">> => <<"first">>,
               ?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter}},
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address,
                       unsettled, configuration, Filter),

    ok = amqp10_client:flow_link_credit(Receiver, 2, never),
    receive {amqp10_msg, Receiver, M1} ->
                ?assertEqual([<<"first msg">>], amqp10_msg:body(M1)),
                ok = amqp10_client:accept_msg(Receiver, M1)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver, M2} ->
                ?assertEqual([<<"last msg">>], amqp10_msg:body(M2)),
                ok = amqp10_client:accept_msg(Receiver, M2)
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = detach_link_sync(Receiver),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, Stream),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

string_modifier(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(Stream),
    {Connection, Session, LinkPair} = init(Config),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair,
                  Stream,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}}),
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
                 <<"k2">> => <<"abc 9">>},
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

    PropsFilter1 = [
                    {{symbol, <<"to">>}, {utf8, <<"&p:abc ">>}},
                    {{symbol, <<"reply-to">>}, {utf8, <<"&p:abc">>}},
                    {{symbol, <<"subject">>}, {utf8, <<"&p:ab">>}},
                    {{symbol, <<"group-id">>}, {utf8, <<"&p:a">>}},
                    {{symbol, <<"reply-to-group-id">>}, {utf8, <<"&s:5">>}},
                    {{symbol, <<"correlation-id">>}, {utf8, <<"&s:abc 7">>}},
                    {{symbol, <<"message-id">>}, {utf8, <<"&p:abc 6">>}}
                   ],
    AppPropsFilter1 = [
                       {{utf8, <<"k1">>}, {utf8, <<"&s: 8">>}},
                       {{utf8, <<"k2">>}, {utf8, <<"&p:abc ">>}}
                      ],
    Filter1 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter1},
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter1},
                <<"rabbitmq:stream-offset-spec">> => <<"first">>},
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 1">>, Address,
                        settled, configuration, Filter1),
    ok = amqp10_client:flow_link_credit(Receiver1, 10, never),
    receive {amqp10_msg, Receiver1, R1M1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(R1M1))
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver1),

    %% Same filters as before except for subject which shouldn't match anymore.
    PropsFilter2 = lists:keyreplace(
                     {symbol, <<"subject">>}, 1, PropsFilter1,
                     {{symbol, <<"subject">>}, {utf8, <<"&s:xxxxxxxxxxxxxx">>}}),
    Filter2 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter2},
                ?DESCRIPTOR_NAME_APPLICATION_PROPERTIES_FILTER => {map, AppPropsFilter1},
                <<"rabbitmq:stream-offset-spec">> => <<"first">>},
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 2">>, Address,
                        settled, configuration, Filter2),
    receive {amqp10_event, {link, Receiver2, attached}} -> ok
    after 30000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:flow_link_credit(Receiver2, 10, never),
    ok = assert_no_msg_received(?LINE),
    ok = detach_link_sync(Receiver2),

    PropsFilter3 = [{{symbol, <<"reply-to-group-id">>}, {utf8, <<"&s: 5">>}}],
    Filter3 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter3},
                <<"rabbitmq:stream-offset-spec">> => <<"first">>},
    {ok, Receiver3} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 3">>, Address,
                        settled, configuration, Filter3),
    ok = amqp10_client:flow_link_credit(Receiver3, 10, never),
    receive {amqp10_msg, Receiver3, R3M1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(R3M1))
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver3, R3M3} ->
                ?assertEqual([<<"m3">>], amqp10_msg:body(R3M3))
    after 30000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = detach_link_sync(Receiver3),

    %% '&&" is the escape prefix for case-sensitive matching of a string starting with ‘&’
    PropsFilter4 = [{{symbol, <<"subject">>}, {utf8, <<"&&Hello">>}}],
    Filter4 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter4},
                <<"rabbitmq:stream-offset-spec">> => <<"first">>},
    {ok, Receiver4} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 4">>, Address,
                        settled, configuration, Filter4),
    {ok, R4M3} = amqp10_client:get_msg(Receiver4),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R4M3)),
    ok = detach_link_sync(Receiver4),

    %% Starting the reference field value with & is invalid without using a valid modifier
    %% prefix is invalid.
    %% RabbitMQ should exclude this filter in its reply attach frame because
    %% "the sending endpoint [RabbitMQ] sets the filter actually in place".
    %% Hence, no filter expression is actually in place and we should receive all messages.
    PropsFilter5 = [{{symbol, <<"subject">>}, {utf8, <<"&Hello">>}}],
    Filter5 = #{?DESCRIPTOR_NAME_PROPERTIES_FILTER => {map, PropsFilter5},
                <<"rabbitmq:stream-offset-spec">> => <<"first">>},
    {ok, Receiver5} = amqp10_client:attach_receiver_link(
                        Session, <<"receiver 5">>, Address,
                        settled, configuration, Filter5),
    {ok, R5M1} = amqp10_client:get_msg(Receiver5),
    ?assertEqual([<<"m1">>], amqp10_msg:body(R5M1)),
    {ok, R5M2} = amqp10_client:get_msg(Receiver5),
    ?assertEqual([<<"m2">>], amqp10_msg:body(R5M2)),
    {ok, R5M3} = amqp10_client:get_msg(Receiver5),
    ?assertEqual([<<"m3">>], amqp10_msg:body(R5M3)),
    ok = detach_link_sync(Receiver5),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, Stream),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

assert_no_msg_received(Line) ->
    receive {amqp10_msg, _, _} = Msg ->
                ct:fail({received_unexpected_msg, Line, Msg})
    after 10 ->
              ok
    end.
