%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

-module(auth_SUITE).

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

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle],
      [
       attach_target_queue,
       attach_source_exchange,
       send_to_topic,
       send_to_topic_using_subject,
       attach_source_topic,
       attach_target_internal_exchange,
       authn_failure_event
      ]
     }
    ].

init_per_suite(Config) ->
    application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config0) ->
    Config = rabbit_ct_helpers:run_setup_steps(
               Config0,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    Vhost = <<"test vhost">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
    [{test_vhost, Vhost} | Config].

end_per_group(_Group, Config) ->
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, ?config(test_vhost, Config)),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config0) ->
    User = <<"test user">>,
    ok = rabbit_ct_broker_helpers:add_user(Config0, User),
    Config = [{test_user, User} | Config0],
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    delete_all_queues(Config),
    ok = rabbit_ct_broker_helpers:delete_user(Config, ?config(test_user, Config)),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

attach_target_queue(Config) ->
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

attach_source_exchange(Config) ->
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

send_to_topic(Config) ->
    TargetAddresses = [<<"/topic/test vhost.test user.a.b">>,
                       <<"/exchange/amq.topic/test vhost.test user.a.b">>],
    lists:foreach(fun(Address) ->
                          ok = send_to_topic0(Address, Config)
                  end, TargetAddresses).

send_to_topic0(TargetAddress, Config) ->
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

send_to_topic_using_subject(Config) ->
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

attach_source_topic(Config) ->
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

attach_target_internal_exchange(Config) ->
    XName = <<"test exchange">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'exchange.declare_ok'{} =  amqp_channel:call(Ch, #'exchange.declare'{internal = true,
                                                                          exchange = XName}),

    OpnConf = connection_config(Config, <<"/">>),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    Address = <<"/exchange/", XName/binary, "/some-routing-key">>,
    {ok, _} = amqp10_client:attach_sender_link(
                Session, <<"test-sender">>, Address),
    ExpectedErr = error_unauthorized(
                    <<"attach to internal exchange 'test exchange' in vhost '/' is forbidden">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after 5000 -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,

    ok = amqp10_client:close_connection(Connection),
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = XName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

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
                       {protocol, {'AMQP', {1, 0}}}],
                      E).

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

error_unauthorized(Description)
  when is_binary(Description) ->
    #'v1_0.error'{
       condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
       description = {utf8, Description}}.

% before we can send messages we have to wait for credit from the server
wait_for_credit(Sender) ->
    receive
        {amqp10_event, {link, Sender, credited}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_credit timed out"),
              ct:fail(credited_timeout)
    end.

flush(Prefix) ->
    receive Msg ->
                ct:pal("~ts flushed: ~p~n", [Prefix, Msg]),
                flush(Prefix)
    after 1 ->
              ok
    end.

delete_all_queues(Config) ->
    Qs = rpc(Config, rabbit_amqqueue, list, []),
    [{ok, _QLen} = rpc(Config, rabbit_amqqueue, delete, [Q, false, false, <<"fake-user">>])
     || Q <- Qs].

close_connection_sync(Connection)
  when is_pid(Connection) ->
    ok = amqp10_client:close_connection(Connection),
    receive {amqp10_event, {connection, Connection, {closed, normal}}} -> ok
    after 5000 -> flush(missing_closed),
                  ct:fail("missing CLOSE from server")
    end.
