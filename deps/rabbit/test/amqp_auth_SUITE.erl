%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(amqp_auth_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/1]).
-import(event_recorder,
        [assert_event_type/2,
         assert_event_prop/2]).
-import(amqp_utils,
        [flush/1,
         wait_for_credit/1,
         close_connection_sync/1]).

all() ->
    [
     {group, address_v1},
     {group, address_v2}
    ].

groups() ->
    [
     {address_v1, [shuffle],
      [
       %% authz
       v1_attach_target_queue,
       v1_attach_source_exchange,
       v1_send_to_topic,
       v1_send_to_topic_using_subject,
       v1_attach_source_topic,
       v1_attach_target_internal_exchange,

       %% limits
       v1_vhost_queue_limit
      ]
     },
     {address_v2, [shuffle],
      [
       %% authz
       attach_source_queue,
       attach_target_exchange,
       attach_target_topic_exchange,
       attach_target_queue,
       target_per_message_exchange,
       target_per_message_internal_exchange,
       target_per_message_topic,

       %% authn
       authn_failure_event,
       sasl_anonymous_success,
       sasl_plain_success,
       sasl_anonymous_failure,
       sasl_plain_failure,
       sasl_none_failure,
       vhost_absent,

       %% limits
       vhost_connection_limit,
       user_connection_limit,

       %% AMQP Management operations against HTTP API v2
       declare_exchange,
       delete_exchange,
       declare_queue,
       declare_queue_dlx_queue,
       declare_queue_dlx_exchange,
       declare_queue_vhost_queue_limit,
       delete_queue,
       purge_queue,
       bind_queue_source,
       bind_queue_destination,
       bind_exchange_source,
       bind_exchange_destination,
       bind_to_topic_exchange,
       unbind_queue_source,
       unbind_queue_target,
       unbind_from_topic_exchange
      ]
     }
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config0) ->
    PermitV1 = case Group of
                   address_v1 -> true;
                   address_v2 -> false
               end,
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit,
                          [{permit_deprecated_features,
                            #{amqp_address_v1 => PermitV1}}]}),
    Config = rabbit_ct_helpers:run_setup_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    Vhost = <<"test vhost">>,
    User = <<"test user">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
    ok = rabbit_ct_broker_helpers:add_user(Config, User),
    [{test_vhost, Vhost},
     {test_user, User}] ++ Config.

end_per_group(_Group, Config) ->
    ok = rabbit_ct_broker_helpers:delete_user(Config, ?config(test_user, Config)),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?config(test_vhost, Config)),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    ok = set_permissions(Config, <<>>, <<>>, <<"^some vhost permission">>),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    delete_all_queues(Config),
    ok = clear_permissions(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

v1_attach_target_queue(Config) ->
    QName = <<"test queue">>,
    %% This target address means RabbitMQ will create a queue
    %% requiring configure access on the queue.
    %% We will also need write access to the default exchange to send to this queue.
    TargetAddress = <<"/queue/", QName/binary>>,
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session_sync(Connection),
    {ok, _Sender1} = amqp10_client:attach_sender_link(
                       Session1, <<"test-sender-1">>, TargetAddress),
    ExpectedErr1 = error_unauthorized(
                     <<"configure access to queue 'test queue' in vhost "
                       "'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session1, {ended, ExpectedErr1}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    %% Give the user configure permissions on the queue.
    ok = set_permissions(Config, QName, <<>>, <<>>),
    {ok, Session2} = amqp10_client:begin_session_sync(Connection),
    {ok, _Sender2} = amqp10_client:attach_sender_link(
                       Session2, <<"test-sender-2">>, TargetAddress),
    ExpectedErr2 = error_unauthorized(
                     <<"write access to exchange 'amq.default' in vhost "
                       "'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session2, {ended, ExpectedErr2}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    %% Give the user configure permissions on the queue and
    %% write access to the default exchange.
    ok = set_permissions(Config, QName, <<"amq\.default">>, <<>>),
    {ok, Session3} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender3} = amqp10_client:attach_sender_link(
                      Session3, <<"test-sender-3">>, TargetAddress),
    receive {amqp10_event, {link, Sender3, attached}} -> ok
    after 5000 -> flush(missing_attached),
                  ct:fail("missing ATTACH from server")
    end,

    ok = close_connection_sync(Connection).

v1_attach_source_exchange(Config) ->
    %% This source address means RabbitMQ will create a queue with a generated name
    %% prefixed with amq.gen requiring configure access on the queue.
    %% The queue is bound to the fanout exchange requiring write access on the queue
    %% and read access on the fanout exchange.
    %% To consume from the queue, we will also need read access on the queue.
    SourceAddress = <<"/exchange/amq.fanout/ignored">>,
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session_sync(Connection),
    {ok, _Recv1} = amqp10_client:attach_receiver_link(
                     Session1, <<"receiver-1">>, SourceAddress),
    receive
        {amqp10_event,
         {session, Session1,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
              description = {utf8, <<"configure access to queue 'amq.gen", _/binary>>}}}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    %% Give the user configure permissions on the queue.
    ok = set_permissions(Config, <<"^amq\.gen">>, <<>>, <<>>),
    {ok, Session2} = amqp10_client:begin_session_sync(Connection),
    {ok, _Recv2} = amqp10_client:attach_receiver_link(
                     Session2, <<"receiver-2">>, SourceAddress),
    receive
        {amqp10_event,
         {session, Session2,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
              description = {utf8, <<"write access to queue 'amq.gen", _/binary>>}}}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    %% Give the user configure and write permissions on the queue.
    ok = set_permissions(Config, <<"^amq\.gen">>, <<"^amq\.gen">>, <<>>),
    {ok, Session3} = amqp10_client:begin_session_sync(Connection),
    {ok, _Recv3} = amqp10_client:attach_receiver_link(
                     Session3, <<"receiver-3">>, SourceAddress),
    ExpectedErr1 = error_unauthorized(
                     <<"read access to exchange 'amq.fanout' in vhost "
                       "'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session3, {ended, ExpectedErr1}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    %% Give the user configure and write permissions on the queue, and read access on the exchange.
    ok = set_permissions(Config, <<"^amq\.gen">>, <<"^amq\.gen">>, <<"amq\.fanout">>),
    {ok, Session4} = amqp10_client:begin_session_sync(Connection),
    {ok, _Recv4} = amqp10_client:attach_receiver_link(
                     Session4, <<"receiver-4">>, SourceAddress),
    receive
        {amqp10_event,
         {session, Session4,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
              description = {utf8, <<"read access to queue 'amq.gen", _/binary>>}}}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    %% Give the user configure, write, and read permissions on the queue,
    %% and read access on the exchange.
    ok = set_permissions(Config, <<"^amq\.gen">>, <<"^amq\.gen">>, <<"^(amq\.gen|amq\.fanout)">>),
    {ok, Session5} = amqp10_client:begin_session_sync(Connection),
    {ok, Recv5} = amqp10_client:attach_receiver_link(
                    Session5, <<"receiver-5">>, SourceAddress),
    receive {amqp10_event, {link, Recv5, attached}} -> ok
    after 5000 -> flush(missing_attached),
                  ct:fail("missing ATTACH from server")
    end,

    ok = close_connection_sync(Connection).

v1_send_to_topic(Config) ->
    TargetAddresses = [<<"/topic/test vhost.test user.a.b">>,
                       <<"/exchange/amq.topic/test vhost.test user.a.b">>],
    lists:foreach(fun(Address) ->
                          ok = send_to_topic(Address, Config)
                  end, TargetAddresses).

send_to_topic(TargetAddress, Config) ->
    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, Vhost),
    ok = set_topic_permissions(Config, <<"amq.topic">>, <<"^$">>, <<"^$">>),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender1} = amqp10_client:attach_sender_link_sync(
                      Session1, <<"sender-1">>, TargetAddress),
    ok = wait_for_credit(Sender1),
    Msg1 = amqp10_msg:new(<<255>>, <<1>>, true),
    ok = amqp10_client:send_msg(Sender1, Msg1),

    ExpectedErr = error_unauthorized(
                    <<"write access to topic 'test vhost.test user.a.b' in exchange "
                      "'amq.topic' in vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session1, {ended, ExpectedErr}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    ok = set_topic_permissions(Config, <<"amq.topic">>, <<"^{vhost}\.{username}\.a\.b$">>, <<"^$">>),
    {ok, Session2} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender2} = amqp10_client:attach_sender_link_sync(
                      Session2, <<"sender-2">>, TargetAddress),
    ok = wait_for_credit(Sender2),
    Dtag = <<0, 0>>,
    Msg2 = amqp10_msg:new(Dtag, <<2>>, false),
    ok = amqp10_client:send_msg(Sender2, Msg2),
    %% We expect RELEASED since no queue is bound.
    receive {amqp10_disposition, {released, Dtag}} -> ok
    after 5000 -> ct:fail(released_timeout)
    end,

    ok = amqp10_client:detach_link(Sender2),
    ok = close_connection_sync(Connection).

v1_send_to_topic_using_subject(Config) ->
    TargetAddress = <<"/exchange/amq.topic">>,
    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, Vhost),
    ok = set_topic_permissions(Config, <<"amq.topic">>, <<"^\.a$">>, <<"^$">>),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(
                     Session, <<"sender">>, TargetAddress),
    ok = wait_for_credit(Sender),

    Dtag1 = <<"dtag 1">>,
    Msg1a = amqp10_msg:new(Dtag1, <<"m1">>, false),
    Msg1b = amqp10_msg:set_properties(#{subject => <<".a">>}, Msg1a),
    ok = amqp10_client:send_msg(Sender, Msg1b),
    %% We have sufficient authorization, but expect RELEASED since no queue is bound.
    receive {amqp10_disposition, {released, Dtag1}} -> ok
    after 5000 -> ct:fail(released_timeout)
    end,

    Dtag2 = <<"dtag 2">>,
    Msg2a = amqp10_msg:new(Dtag2, <<"m2">>, false),
    %% We don't have sufficient authorization.
    Msg2b = amqp10_msg:set_properties(#{subject => <<".a.b">>}, Msg2a),
    ok = amqp10_client:send_msg(Sender, Msg2b),
    ExpectedErr = error_unauthorized(
                    <<"write access to topic '.a.b' in exchange 'amq.topic' in "
                      "vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    ok = close_connection_sync(Connection).

v1_attach_source_topic(Config) ->
    %% These source addresses mean RabbitMQ will bind a queue to the default topic
    %% exchange with binding key 'test vhost.test user.a.b'.
    %% Therefore, we need read access to that topic.
    %% We also test variable expansion in topic permission patterns.
    SourceAddresses = [<<"/topic/test vhost.test user.a.b">>,
                       <<"/exchange/amq.topic/test vhost.test user.a.b">>],
    lists:foreach(fun(Address) ->
                          ok = attach_source_topic0(Address, Config)
                  end, SourceAddresses).

attach_source_topic0(SourceAddress, Config) ->
    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, Vhost),
    ok = set_topic_permissions(Config, <<"amq.topic">>, <<"^$">>, <<"^$">>),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session1} = amqp10_client:begin_session_sync(Connection),
    {ok, _Recv1} = amqp10_client:attach_receiver_link(
                     Session1, <<"receiver-1">>, SourceAddress),
    ExpectedErr = error_unauthorized(
                    <<"read access to topic 'test vhost.test user.a.b' in exchange "
                      "'amq.topic' in vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session1, {ended, ExpectedErr}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    ok = set_topic_permissions(Config, <<"amq.topic">>, <<"^$">>, <<"^{vhost}\.{username}\.a\.b$">>),
    {ok, Session2} = amqp10_client:begin_session_sync(Connection),
    {ok, Recv2} = amqp10_client:attach_receiver_link(
                    Session2, <<"receiver-2">>, SourceAddress),
    receive {amqp10_event, {link, Recv2, attached}} -> ok
    after 5000 -> flush(missing_attached),
                  ct:fail("missing ATTACH from server")
    end,

    ok = close_connection_sync(Connection).

v1_attach_target_internal_exchange(Config) ->
    XName = <<"test exchange">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'exchange.declare_ok'{} =  amqp_channel:call(Ch, #'exchange.declare'{internal = true,
                                                                          exchange = XName}),

    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf = OpnConf0#{sasl := anon},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/", XName/binary, "/some-routing-key">>,
    {ok, _} = amqp10_client:attach_sender_link(
                Session, <<"test-sender">>, Address),
    ExpectedErr = error_unauthorized(
                    <<"forbidden to publish to internal exchange 'test exchange' in vhost '/'">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    ok = amqp10_client:close_connection(Connection),
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = XName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

attach_source_queue(Config) ->
    {Conn, Session, LinkPair} = init_pair(Config),
    QName = <<"🍿"/utf8>>,
    Address = rabbitmq_amqp_address:queue(QName),

    %% missing read permission to queue
    ok = set_permissions(Config, QName, <<>>, <<>>),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),

    {ok, _Recv} = amqp10_client:attach_receiver_link(Session, <<"receiver">>, Address),
    ExpectedErr = error_unauthorized(
                    <<"read access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event,
             {session, Session,
              {ended, ExpectedErr}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,
    ok = close_connection_sync(Conn).

attach_target_exchange(Config) ->
    XName = <<"amq.fanout">>,
    Address1 = rabbitmq_amqp_address:exchange(XName),
    Address2 = rabbitmq_amqp_address:exchange(XName, <<"some-key">>),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),

    {ok, Session1} = amqp10_client:begin_session_sync(Connection),
    {ok, _} = amqp10_client:attach_sender_link(Session1, <<"test-sender">>, Address1),
    ExpectedErr = error_unauthorized(
                    <<"write access to exchange '", XName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session1, {ended, ExpectedErr}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, Session2} = amqp10_client:begin_session_sync(Connection),
    {ok, _} = amqp10_client:attach_sender_link(Session2, <<"test-sender">>, Address2),
    receive {amqp10_event, {session, Session2, {ended, ExpectedErr}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:close_connection(Connection).

attach_target_topic_exchange(Config) ->
    TargetAddress = rabbitmq_amqp_address:exchange(
                      <<"amq.topic">>, <<"test vhost.test user.a.b">>),
    ok = send_to_topic(TargetAddress, Config).

attach_target_queue(Config) ->
    {Conn, Session, LinkPair} = init_pair(Config),
    QName = <<"🍿"/utf8>>,
    Address = rabbitmq_amqp_address:queue(QName),

    %% missing write permission to default exchange
    ok = set_permissions(Config, QName, <<>>, <<>>),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),

    {ok, _} = amqp10_client:attach_sender_link(Session, <<"sender">>, Address),
    ExpectedErr = error_unauthorized(
                    <<"write access to exchange 'amq.default' ",
                      "in vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:close_connection(Conn).

target_per_message_exchange(Config) ->
    TargetAddress = null,
    To1 = rabbitmq_amqp_address:exchange(<<"amq.fanout">>),
    To2 = rabbitmq_amqp_address:queue(<<"q1">>),
    %% missing write permission to default exchange
    ok = set_permissions(Config, <<>>, <<"amq.fanout">>, <<>>),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session, <<"sender">>, TargetAddress),
    ok = wait_for_credit(Sender),

    %% We have sufficient authorization, but expect RELEASED since no queue is bound.
    Tag1 = <<"dtag 1">>,
    Msg1 = amqp10_msg:set_properties(#{to => To1}, amqp10_msg:new(Tag1, <<"m1">>)),
    ok = amqp10_client:send_msg(Sender, Msg1),
    receive {amqp10_disposition, {released, Tag1}} -> ok
    after 5000 -> ct:fail(released_timeout)
    end,

    %% We don't have sufficient authorization.
    Tag2 = <<"dtag 2">>,
    Msg2 = amqp10_msg:set_properties(#{to => To2}, amqp10_msg:new(Tag2, <<"m2">>)),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ExpectedErr = error_unauthorized(
                    <<"write access to exchange 'amq.default' in "
                      "vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = close_connection_sync(Connection).

target_per_message_internal_exchange(Config) ->
    XName = <<"my internal exchange">>,
    XProps = #{internal => true},
    TargetAddress = null,
    To = rabbitmq_amqp_address:exchange(XName),

    ok = set_permissions(Config, XName, XName, <<>>),
    {Conn1, Session1, LinkPair1} = init_pair(Config),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair1, XName, XProps),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session1, <<"sender">>, TargetAddress),
    ok = wait_for_credit(Sender),

    Tag = <<"tag">>,
    Msg = amqp10_msg:set_properties(#{to => To}, amqp10_msg:new(Tag, <<"msg">>, true)),
    ok = amqp10_client:send_msg(Sender, Msg),
    ExpectedErr = error_unauthorized(
                    <<"forbidden to publish to internal exchange '", XName/binary, "' in vhost 'test vhost'">>),
    receive {amqp10_event, {session, Session1, {ended, ExpectedErr}}} -> ok
    after 5000 -> flush(missing_event),
                  ct:fail({missing_event, ?LINE})
    end,
    ok = close_connection_sync(Conn1),

    Init = {_, _, LinkPair2} = init_pair(Config),
    ok = rabbitmq_amqp_client:delete_exchange(LinkPair2, XName),
    ok = cleanup_pair(Init).

target_per_message_topic(Config) ->
    TargetAddress = null,
    To1 = rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<".a">>),
    To2 = rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<".a.b">>),
    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, Vhost),
    ok = set_topic_permissions(Config, <<"amq.topic">>, <<"^\.a$">>, <<"^$">>),

    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session, <<"sender">>, TargetAddress),
    ok = wait_for_credit(Sender),

    %% We have sufficient authorization, but expect RELEASED since no queue is bound.
    Tag1 = <<"dtag 1">>,
    Msg1 = amqp10_msg:set_properties(#{to => To1}, amqp10_msg:new(Tag1, <<"m1">>)),
    ok = amqp10_client:send_msg(Sender, Msg1),
    receive {amqp10_disposition, {released, Tag1}} -> ok
    after 5000 -> ct:fail(released_timeout)
    end,

    %% We don't have sufficient authorization.
    Tag2 = <<"dtag 2">>,
    Msg2 = amqp10_msg:set_properties(#{to => To2}, amqp10_msg:new(Tag2, <<"m2">>)),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ExpectedErr = error_unauthorized(
                    <<"write access to topic '.a.b' in exchange 'amq.topic' in "
                      "vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = close_connection_sync(Connection).

authn_failure_event(Config) ->
    ok = event_recorder:start(Config),

    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Vhost = ?config(test_vhost, Config),
    User = ?config(test_user, Config),
    OpnConf = #{address => Host,
                port => Port,
                container_id => <<"my container">>,
                sasl => {plain, User, <<"wrong password">>},
                hostname => <<"vhost:", Vhost/binary>>},

    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, sasl_auth_failure}}} -> ok
    after 5000 -> flush(missing_closed),
                  ct:fail("did not receive sasl_auth_failure")
    end,

    [E | _] = event_recorder:get_events(Config),
    ok = event_recorder:stop(Config),

    assert_event_type(user_authentication_failure, E),
    assert_event_prop([{name, <<"test user">>},
                       {auth_mechanism, <<"PLAIN">>},
                       {ssl, false},
                       {protocol, {1, 0}}],
                      E).

sasl_anonymous_success(Config) ->
    Mechanism = anon,
    ok = sasl_success(Mechanism, Config).

sasl_plain_success(Config) ->
    Mechanism = {plain, <<"guest">>, <<"guest">>},
    ok = sasl_success(Mechanism, Config).

sasl_success(Mechanism, Config) ->
    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf = OpnConf0#{sasl := Mechanism},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, opened}} -> ok
    after 5000 -> ct:fail(missing_opened)
    end,
    ok = amqp10_client:close_connection(Connection).

sasl_anonymous_failure(Config) ->
    App = rabbit,
    Par = anonymous_login_user,
    {ok, Default} = rpc(Config, application, get_env, [App, Par]),
    %% Prohibit anonymous login.
    ok = rpc(Config, application, set_env, [App, Par, none]),

    Mechanism = anon,
    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf = OpnConf0#{sasl := Mechanism},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, Reason}}} ->
                ?assertEqual({sasl_not_supported, Mechanism}, Reason)
    after 5000 -> ct:fail(missing_closed)
    end,

    ok = rpc(Config, application, set_env, [App, Par, Default]).

sasl_plain_failure(Config) ->
    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf = OpnConf0#{sasl := {plain, <<"guest">>, <<"wrong password">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, Reason}}} ->
                ?assertEqual(sasl_auth_failure, Reason)
    after 5000 -> ct:fail(missing_closed)
    end.

%% Skipping SASL is disallowed in RabbitMQ.
sasl_none_failure(Config) ->
    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf = OpnConf0#{sasl := none},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, _Reason}}} -> ok
    after 5000 -> ct:fail(missing_closed)
    end.

vhost_absent(Config) ->
    OpnConf = connection_config(Config, <<"this vhost does not exist">>),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, _}}} -> ok
    after 5000 -> ct:fail(missing_closed)
    end.

vhost_connection_limit(Config) ->
    Vhost = proplists:get_value(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, Vhost, max_connections, 1),

    OpnConf = connection_config(Config),
    {ok, C1} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C1, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, C2} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C2, {closed, _}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf1 = OpnConf0#{sasl := anon},
    {ok, C3} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, C3, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, C4} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, C4, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    [ok = close_connection_sync(C) || C <- [C1, C3, C4]],
    ok = rabbit_ct_broker_helpers:clear_vhost_limit(Config, 0, Vhost).

user_connection_limit(Config) ->
    DefaultUser = <<"guest">>,
    Limit = max_connections,
    ok = rabbit_ct_broker_helpers:set_user_limits(Config, DefaultUser, #{Limit => 0}),
    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf = OpnConf0#{sasl := anon},
    {ok, C1} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C1, {closed, _}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, C2} = amqp10_client:open_connection(connection_config(Config)),
    receive {amqp10_event, {connection, C2, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = close_connection_sync(C2),
    ok = rabbit_ct_broker_helpers:clear_user_limits(Config, DefaultUser, Limit).

v1_vhost_queue_limit(Config) ->
    Vhost = proplists:get_value(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, Vhost, max_queues, 0),
    QName = <<"q1">>,
    ok = set_permissions(Config, QName, <<>>, <<>>),

    OpnConf1 = connection_config(Config),
    {ok, C1} = amqp10_client:open_connection(OpnConf1),
    {ok, Session1} = amqp10_client:begin_session_sync(C1),
    TargetAddress = <<"/queue/", QName/binary>>,
    {ok, _Sender1} = amqp10_client:attach_sender_link(
                       Session1, <<"test-sender-1">>, TargetAddress),
    ExpectedErr = amqp_error(
                    ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED,
                    <<"cannot declare queue 'q1': queue limit in vhost 'test vhost' (0) is reached">>),
    receive {amqp10_event, {session, Session1, {ended, ExpectedErr}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive expected error")
    end,

    OpnConf2 = connection_config(Config, <<"/">>),
    OpnConf3 = OpnConf2#{sasl := anon},
    {ok, C2} = amqp10_client:open_connection(OpnConf3),
    {ok, Session2} = amqp10_client:begin_session_sync(C2),
    {ok, Sender2} = amqp10_client:attach_sender_link(
                      Session2, <<"test-sender-2">>, TargetAddress),
    receive {amqp10_event, {link, Sender2, attached}} -> ok
    after 5000 -> flush(missing_attached),
                  ct:fail("missing ATTACH from server")
    end,

    ok = close_connection_sync(C1),
    ok = close_connection_sync(C2),
    ok = rabbit_ct_broker_helpers:clear_vhost_limit(Config, 0, Vhost).

declare_exchange(Config) ->
    {Conn, _Session, LinkPair} = init_pair(Config),
    XName = <<"📮"/utf8>>,
    ExpectedErr = error_unauthorized(
                    <<"configure access to exchange '", XName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{})),
    ok = close_connection_sync(Conn).

delete_exchange(Config) ->
    {Conn, Session1, LinkPair1} = init_pair(Config),
    XName = <<"📮"/utf8>>,
    ok = set_permissions(Config, XName, <<>>, <<>>),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair1, XName, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = amqp10_client:end_session(Session1),

    ok = clear_permissions(Config),

    {ok, Session2} = amqp10_client:begin_session_sync(Conn),
    {ok, LinkPair2} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session2, <<"pair 2">>),
    ExpectedErr = error_unauthorized(
                    <<"configure access to exchange '", XName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:delete_exchange(LinkPair2, XName)),
    ok = close_connection_sync(Conn),

    ok = set_permissions(Config, XName, <<>>, <<>>),
    Init = {_, _, LinkPair3} = init_pair(Config),
    ok = rabbitmq_amqp_client:delete_exchange(LinkPair3, XName),
    ok = cleanup_pair(Init).

declare_queue(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    QName = <<"🍿"/utf8>>,
    ExpectedErr = error_unauthorized(
                    <<"configure access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{})),
    ok = close_connection_sync(Conn).

declare_queue_dlx_queue(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    QName = <<"🍿"/utf8>>,
    DlxName = <<"📥"/utf8>>,
    QProps = #{arguments => #{<<"x-dead-letter-exchange">> => {utf8, DlxName}}},
    %% missing read permission to queue
    ok = set_permissions(Config, QName, DlxName, <<>>),
    ExpectedErr = error_unauthorized(
                    <<"read access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps)),
    ok = close_connection_sync(Conn).

declare_queue_dlx_exchange(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    QName = <<"🍿"/utf8>>,
    DlxName = <<"📥"/utf8>>,
    QProps = #{arguments => #{<<"x-dead-letter-exchange">> => {utf8, DlxName}}},
    %% missing write permission to dead letter exchange
    ok = set_permissions(Config, QName, <<>>, QName),
    ExpectedErr = error_unauthorized(
                    <<"write access to exchange '", DlxName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps)),
    ok = close_connection_sync(Conn).

declare_queue_vhost_queue_limit(Config) ->
    QName = <<"🍿"/utf8>>,
    ok = set_permissions(Config, QName, <<>>, <<>>),
    Vhost = proplists:get_value(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, Vhost, max_queues, 0),

    Init = {_, _, LinkPair} = init_pair(Config),
    {error, Resp} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(
       #'v1_0.amqp_value'{
          content = {utf8, <<"cannot declare queue '", QName/binary, "': queue limit in vhost 'test vhost' (0) is reached">>}},
       amqp10_msg:body(Resp)),

    ok = cleanup_pair(Init),
    ok = rabbit_ct_broker_helpers:clear_vhost_limit(Config, 0, Vhost).

delete_queue(Config) ->
    {Conn, Session1, LinkPair1} = init_pair(Config),
    QName = <<"🍿"/utf8>>,
    ok = set_permissions(Config, QName, <<>>, <<>>),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = amqp10_client:end_session(Session1),

    ok = clear_permissions(Config),

    {ok, Session2} = amqp10_client:begin_session_sync(Conn),
    {ok, LinkPair2} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session2, <<"pair 2">>),
    ExpectedErr = error_unauthorized(
                    <<"configure access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:delete_queue(LinkPair2, QName)),
    ok = close_connection_sync(Conn).

purge_queue(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    QName = <<"🍿"/utf8>>,
    %% missing read permission to queue
    ok = set_permissions(Config, QName, <<>>, <<>>),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ExpectedErr = error_unauthorized(
                    <<"read access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:purge_queue(LinkPair, QName)),
    ok = close_connection_sync(Conn).

bind_queue_source(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    %% missing read permission to source exchange
    ok = set_permissions(Config, QName, QName, QName),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),

    XName = <<"amq.direct">>,
    ExpectedErr = error_unauthorized(
                    <<"read access to exchange '", XName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, <<"key">>, #{})),
    ok = close_connection_sync(Conn).

bind_queue_destination(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    QName = <<"my 🐇"/utf8>>,
    XName = <<"amq.direct">>,
    %% missing write permission to destination queue
    ok = set_permissions(Config, QName, <<>>, XName),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),

    ExpectedErr = error_unauthorized(
                    <<"write access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, <<"key">>, #{})),
    ok = close_connection_sync(Conn).

bind_exchange_source(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    SrcXName = <<"amq.fanout">>,
    DstXName = <<"amq.direct">>,
    %% missing read permission to source exchange
    ok = set_permissions(Config, <<>>, DstXName, <<>>),

    ExpectedErr = error_unauthorized(
                    <<"read access to exchange '", SrcXName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:bind_exchange(LinkPair, DstXName, SrcXName, <<"key">>, #{})),
    ok = close_connection_sync(Conn).

bind_exchange_destination(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    SrcXName = <<"amq.fanout">>,
    DstXName = <<"amq.direct">>,
    %% missing write permission to destination exchange
    ok = set_permissions(Config, <<>>, <<>>, SrcXName),

    ExpectedErr = error_unauthorized(
                    <<"write access to exchange '", DstXName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:bind_exchange(LinkPair, DstXName, SrcXName, <<"key">>, #{})),
    ok = close_connection_sync(Conn).

bind_to_topic_exchange(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    SrcXName = <<"amq.topic">>,
    DstXName = <<"amq.direct">>,
    Topic = <<"a.b.🐇"/utf8>>,

    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, Vhost),
    %% missing read permission to Topic
    ok = set_topic_permissions(Config, SrcXName, <<".*">>, <<"wrong.topic">>),

    ExpectedErr = error_unauthorized(
                    <<"read access to topic '", Topic/binary,
                      "' in exchange 'amq.topic' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:bind_exchange(LinkPair, DstXName, SrcXName, Topic, #{})),
    ok = close_connection_sync(Conn).

unbind_queue_source(Config) ->
    {Conn, Session1, LinkPair1} = init_pair(Config),
    QName = BindingKey = atom_to_binary(?FUNCTION_NAME),
    XName = <<"amq.direct">>,
    ok = set_permissions(Config, QName, QName, XName),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair1, QName, XName, BindingKey, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = amqp10_client:end_session(Session1),

    %% remove read permission to source exchange
    ok = set_permissions(Config, QName, QName, <<"^$">>),

    {ok, Session2} = amqp10_client:begin_session_sync(Conn),
    {ok, LinkPair2} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session2, <<"pair 2">>),
    ExpectedErr = error_unauthorized(
                    <<"read access to exchange '", XName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:unbind_queue(LinkPair2, QName, XName, BindingKey, #{})),
    ok = close_connection_sync(Conn).

unbind_queue_target(Config) ->
    {Conn, Session1, LinkPair1} = init_pair(Config),
    QName = BindingKey = atom_to_binary(?FUNCTION_NAME),
    XName = <<"amq.direct">>,
    ok = set_permissions(Config, QName, QName, XName),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair1, QName, XName, BindingKey, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = amqp10_client:end_session(Session1),

    %% remove write permission to destination queue
    ok = set_permissions(Config, QName, <<"^$">>, XName),

    {ok, Session2} = amqp10_client:begin_session_sync(Conn),
    {ok, LinkPair2} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session2, <<"pair 2">>),
    ExpectedErr = error_unauthorized(
                    <<"write access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:unbind_queue(LinkPair2, QName, XName, BindingKey, #{})),
    ok = close_connection_sync(Conn).

unbind_from_topic_exchange(Config) ->
    Init = {_, _, LinkPair1} = init_pair(Config),
    SrcXName = <<"amq.topic">>,
    DstXName = <<"amq.direct">>,
    Topic = <<"a.b.🐇"/utf8>>,

    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, Vhost),
    ok = set_topic_permissions(Config, SrcXName, <<"^$">>, Topic),
    ok = rabbitmq_amqp_client:bind_exchange(LinkPair1, DstXName, SrcXName, Topic, #{}),

    %% remove Topic read permission
    ok = set_topic_permissions(Config, SrcXName, <<"^$">>, <<"^$">>),
    %% Start a new connection since topic permissions are cached by the AMQP session process.
    ok = cleanup_pair(Init),
    {Conn, _, LinkPair2} = init_pair(Config),

    ExpectedErr = error_unauthorized(
                    <<"read access to topic '", Topic/binary,
                      "' in exchange 'amq.topic' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:unbind_exchange(LinkPair2, DstXName, SrcXName, Topic, #{})),

    ok = close_connection_sync(Conn).

init_pair(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"mgmt link pair">>),
    {Connection, Session, LinkPair}.

cleanup_pair({Connection, Session, LinkPair}) ->
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

connection_config(Config) ->
    Vhost = ?config(test_vhost, Config),
    connection_config(Config, Vhost).

connection_config(Config, Vhost) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    User = Password = ?config(test_user, Config),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, User, Password},
      hostname => <<"vhost:", Vhost/binary>>}.

set_permissions(Config, ConfigurePerm, WritePerm, ReadPerm) ->
    ok = rabbit_ct_broker_helpers:set_permissions(Config,
                                                  ?config(test_user, Config),
                                                  ?config(test_vhost, Config),
                                                  ConfigurePerm,
                                                  WritePerm,
                                                  ReadPerm).

set_topic_permissions(Config, Exchange, WritePat, ReadPat) ->
    ok = rpc(Config,
             rabbit_auth_backend_internal,
             set_topic_permissions,
             [?config(test_user, Config),
              ?config(test_vhost, Config),
              Exchange,
              WritePat,
              ReadPat,
              <<"acting-user">>]).

clear_permissions(Config) ->
    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config, User, Vhost),
    ok = rpc(Config,
             rabbit_auth_backend_internal,
             clear_topic_permissions,
             [User, Vhost, <<"acting-user">>]).

error_unauthorized(Description) ->
    amqp_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Description).

amqp_error(Condition, Description)
  when is_binary(Description) ->
    #'v1_0.error'{
       condition = Condition,
       description = {utf8, Description}}.

delete_all_queues(Config) ->
    Qs = rpc(Config, rabbit_amqqueue, list, []),
    [{ok, _QLen} = rpc(Config, rabbit_amqqueue, delete, [Q, false, false, <<"fake-user">>])
     || Q <- Qs].
