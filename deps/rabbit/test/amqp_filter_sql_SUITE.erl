%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

%% Test suite for SQL expressions filtering from a stream.
-module(amqp_filter_sql_SUITE).

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
       multiple_sections,
       filter_few_messages_from_many,
       invalid_filters
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

multiple_sections(Config) ->
    Stream = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(Stream),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(
                  LinkPair, Stream,
                  #{arguments => #{<<"x-queue-type">> => {utf8, <<"stream">>}}}),
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ok = wait_for_credit(Sender),

    Now = erlang:system_time(millisecond),
    To = rabbitmq_amqp_address:exchange(<<"some exchange">>, <<"routing key">>),
    ReplyTo = rabbitmq_amqp_address:queue(<<"some queue">>),

    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:new(<<"t1">>, <<"m1">>)),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_headers(
             #{priority => 200},
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
               amqp10_msg:set_application_properties(
                 #{<<"k1">> => -3,
                   <<"k2">> => false,
                   <<"k3">> => true,
                   <<"k4">> => <<"hey👋"/utf8>>},
                 amqp10_msg:new(<<"t2">>, <<"m2">>))))),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_properties(
             #{group_id => <<"my group ID">>},
             amqp10_msg:set_application_properties(
               #{<<"k1">> => -4},
               amqp10_msg:new(<<"t3">>, <<"m3">>)))),

    ok = wait_for_accepts(3),
    ok = detach_link_sync(Sender),
    flush(sent),

    Filter1 = filter(<<"k1 <= -3">>),
    {ok, R1} = amqp10_client:attach_receiver_link(
                 Session, <<"receiver 1">>, Address,
                 settled, configuration, Filter1),
    ok = amqp10_client:flow_link_credit(R1, 10, never, true),
    receive {amqp10_msg, R1, R1M2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(R1M2))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, R1, R1M3} ->
                ?assertEqual([<<"m3">>], amqp10_msg:body(R1M3))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R1, ?LINE),
    ok = detach_link_sync(R1),

    Filter2 = filter(
                <<"header.priority = 200 AND "
                  "properties.message-id = 999 AND "
                  "properties.user-id = 'guest' AND "
                  "properties.to LIKE '/exch_nges/some=%20exchange/rout%' ESCAPE '=' AND "
                  "properties.subject = '🐇' AND "
                  "properties.reply-to LIKE '/queues/some%' AND "
                  "properties.correlation-id IN ('corr-345', 'corr-123') AND "
                  "properties.content-type = 'text/plain' AND "
                  "properties.content-encoding = 'some encoding' AND "
                  "properties.absolute-expiry-time > 0 AND "
                  "properties.creation-time > 0 AND "
                  "properties.group-id IS NOT NULL AND "
                  "properties.group-sequence = 4294967295 AND "
                  "properties.reply-to-group-id = 'other group ID' AND "
                  "k1 < 0 AND "
                  "NOT k2 AND "
                  "k3 AND "
                  "k4 NOT LIKE 'hey' AND "
                  "k5 IS NULL"
                  /utf8>>),
    {ok, R2} = amqp10_client:attach_receiver_link(
                 Session, <<"receiver 2">>, Address,
                 settled, configuration, Filter2),
    ok = amqp10_client:flow_link_credit(R2, 10, never, true),
    receive {amqp10_msg, R2, R2M2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(R2M2))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R2, ?LINE),
    ok = detach_link_sync(R2),

    Filter3 = filter(<<"absent IS NULL">>),
    {ok, R3} = amqp10_client:attach_receiver_link(
                 Session, <<"receiver 3">>, Address,
                 settled, configuration, Filter3),
    ok = amqp10_client:flow_link_credit(R3, 10, never, true),
    receive {amqp10_msg, R3, R3M1} ->
                ?assertEqual([<<"m1">>], amqp10_msg:body(R3M1))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, R3, R3M2} ->
                ?assertEqual([<<"m2">>], amqp10_msg:body(R3M2))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, R3, R3M3} ->
                ?assertEqual([<<"m3">>], amqp10_msg:body(R3M3))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    ok = assert_credit_exhausted(R3, ?LINE),
    ok = detach_link_sync(R3),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, Stream),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = end_session_sync(Session),
    ok = close_connection_sync(Connection).

%% Filter a small subset from many messages.
%% We test here that flow control still works correctly.
%% see amqp_filter_prop_SUITE
filter_few_messages_from_many(Config) ->
    todo.

%% TODO
%% * lexer error
%% * parser error
%% * mutually exclusive with AMQP property filters
invalid_filters(Config) ->
    todo.

filter(SQL)
  when is_binary(SQL) ->
    #{<<"from start">> => #filter{descriptor = <<"rabbitmq:stream-offset-spec">>,
                                  value = {symbol, <<"first">>}},
      ?FILTER_NAME_SQL => #filter{descriptor = ?DESCRIPTOR_NAME_SELECTOR_FILTER,
                                  value = {utf8, SQL}}}.

assert_credit_exhausted(Receiver, Line) ->
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 9000 -> ct:fail({missing_credit_exhausted, Line})
    end.
