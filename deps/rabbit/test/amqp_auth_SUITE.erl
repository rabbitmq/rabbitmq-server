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

-define(TIMEOUT, 30_000).

-import(rabbit_ct_broker_helpers,
        [rpc/4]).
-import(rabbit_ct_helpers,
        [eventually/1]).
-import(event_recorder,
        [assert_event_type/2,
         assert_event_prop/2]).
-import(amqp_utils,
        [web_amqp/1,
         flush/1,
         wait_for_credit/1,
         end_session_sync/1,
         close_connection_sync/1]).

all() ->
    [
     {group, address_v2}
    ].

groups() ->
    [
     {address_v2, [shuffle],
      [
       %% authz
       attach_source_queue,
       attach_source_queue_dynamic_exclusive,
       attach_source_queue_dynamic_volatile,
       attach_target_exchange,
       attach_target_topic_exchange,
       attach_target_queue,
       attach_target_queue_dynamic_exchange_write,
       attach_target_queue_dynamic_queue_configure,
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
       declare_exchange_alternate_exchange_allowed,
       declare_exchange_alternate_exchange_read,
       declare_exchange_alternate_exchange_write,
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
       unbind_from_topic_exchange,
       list_bindings_allowed,
       list_bindings_exchange_destination,
       list_bindings_source_read,
       list_bindings_destination_write,

       %% Passive topology operations
       passive_queue_declare,
       passive_exchange_declare,
       get_queue
      ]
     }
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config0) ->
    Config = rabbit_ct_helpers:run_setup_steps(
               Config0,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    case Config of
        _ when is_list(Config) ->
            Vhost = <<"test vhost">>,
            User = <<"test user">>,
            ok = rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
            ok = rabbit_ct_broker_helpers:add_user(Config, User),
            [{test_vhost, Vhost},
             {test_user, User}] ++ Config;
        {skip, _} = Skip ->
            Skip
    end.

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
    after ?TIMEOUT -> flush(missing_ended),
                  ct:fail("did not receive AMQP_ERROR_UNAUTHORIZED_ACCESS")
    end,
    ok = close_connection_sync(Conn).

attach_source_queue_dynamic_exclusive(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% missing configure permission to queue
    ok = set_permissions(Config, <<>>, <<".*">>, <<".*">>),

    Source = #{address => undefined,
               dynamic => true,
               capabilities => [<<"temporary-queue">>],
               durable => none},
    AttachArgs = #{name => <<"my link">>,
                   role => {receiver, Source, self()},
                   snd_settle_mode => unsettled,
                   rcv_settle_mode => first,
                   filter => #{}},
    {ok, _Recv} = amqp10_client:attach_link(Session, AttachArgs),
    receive {amqp10_event,
             {session, Session,
              {ended, Error}}} ->
                #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                              description = {utf8, Description}} = Error,
                ?assertEqual(
                   match,
                   re:run(Description,
                          <<"^configure access to queue 'amq\.dyn-.*' in vhost "
                            "'test vhost' refused for user 'test user'$">>,
                          [{capture, none}]))
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,
    ok = close_connection_sync(Connection).

attach_source_queue_dynamic_volatile(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% missing read permission on volatile queue
    ok = set_permissions(Config, <<".*">>, <<".*">>, <<>>),

    Source = #{address => undefined,
               durable => none,
               expiry_policy => <<"link-detach">>,
               dynamic => true,
               capabilities => [<<"rabbitmq:volatile-queue">>]},
    AttachArgs = #{name => <<"receiver">>,
                   role => {receiver, Source, self()},
                   snd_settle_mode => settled,
                   rcv_settle_mode => first},
    {ok, _Recv} = amqp10_client:attach_link(Session, AttachArgs),
    receive {amqp10_event,
             {session, Session,
              {ended, Error}}} ->
                #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                              description = {utf8, Description}} = Error,
                ?assertEqual(
                   match,
                   re:run(Description,
                          <<"^read access to queue 'amq\.rabbitmq\.reply-to\..*' in vhost "
                            "'test vhost' refused for user 'test user'$">>,
                          [{capture, none}]))
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,
    ok = close_connection_sync(Connection).

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
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,

    {ok, Session2} = amqp10_client:begin_session_sync(Connection),
    {ok, _} = amqp10_client:attach_sender_link(Session2, <<"test-sender">>, Address2),
    receive {amqp10_event, {session, Session2, {ended, ExpectedErr}}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:close_connection(Connection).

attach_target_topic_exchange(Config) ->
    TargetAddress = rabbitmq_amqp_address:exchange(
                      <<"amq.topic">>, <<"test vhost.test user.a.b">>),
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
    after ?TIMEOUT -> flush(missing_ended),
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
    after ?TIMEOUT -> ct:fail(released_timeout)
    end,

    ok = amqp10_client:detach_link(Sender2),
    ok = close_connection_sync(Connection).

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
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:close_connection(Conn).

attach_target_queue_dynamic_exchange_write(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% missing write permission to default exchange
    ok = set_permissions(Config, <<".*">>, <<>>, <<".*">>),

    Target = #{address => undefined,
               dynamic => true,
               capabilities => [<<"temporary-queue">>]},
    AttachArgs = #{name => <<"my link">>,
                   role => {sender, Target},
                   snd_settle_mode => mixed,
                   rcv_settle_mode => first},
    {ok, _Recv} = amqp10_client:attach_link(Session, AttachArgs),
    ExpectedErr = error_unauthorized(
                    <<"write access to exchange 'amq.default' ",
                      "in vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,
    ok = close_connection_sync(Connection).

attach_target_queue_dynamic_queue_configure(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),

    %% missing configure permission to queue
    ok = set_permissions(Config, <<>>, <<".*">>, <<".*">>),

    Target = #{address => undefined,
               dynamic => true,
               capabilities => [<<"temporary-queue">>]},
    AttachArgs = #{name => <<"my link">>,
                   role => {sender, Target},
                   snd_settle_mode => mixed,
                   rcv_settle_mode => first},
    {ok, _Recv} = amqp10_client:attach_link(Session, AttachArgs),
    receive {amqp10_event,
             {session, Session,
              {ended, Error}}} ->
                #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                              description = {utf8, Description}} = Error,
                ?assertEqual(
                   match,
                   re:run(Description,
                          <<"^configure access to queue 'amq\.dyn-.*' in vhost "
                            "'test vhost' refused for user 'test user'$">>,
                          [{capture, none}]))
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,
    ok = close_connection_sync(Connection).

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
    after ?TIMEOUT -> ct:fail(released_timeout)
    end,

    %% We don't have sufficient authorization.
    Tag2 = <<"dtag 2">>,
    Msg2 = amqp10_msg:set_properties(#{to => To2}, amqp10_msg:new(Tag2, <<"m2">>)),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ExpectedErr = error_unauthorized(
                    <<"write access to exchange 'amq.default' in "
                      "vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,

    ok = close_connection_sync(Connection).

target_per_message_internal_exchange(Config) ->
    XName = <<"my internal exchange">>,
    XProps = #{internal => true},
    TargetAddress = null,
    To = rabbitmq_amqp_address:exchange(XName),

    ok = set_permissions(Config, XName, XName, <<>>),
    {_, Session, LinkPair} = Init = init_pair(Config),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, XProps),
    {ok, Sender} = amqp10_client:attach_sender_link_sync(Session, <<"sender">>, TargetAddress),
    ok = wait_for_credit(Sender),

    Tag = <<"tag">>,
    Msg = amqp10_msg:set_properties(#{to => To}, amqp10_msg:new(Tag, <<"msg">>, true)),
    ok = amqp10_client:send_msg(Sender, Msg),
    ExpectedErr = error_unauthorized(
                    <<"forbidden to publish to internal exchange '",
                      XName/binary, "' in vhost 'test vhost'">>),
    receive {amqp10_event, {link, Sender, {detached, ExpectedErr}}} -> ok
    after ?TIMEOUT -> flush(missing_event),
                      ct:fail({missing_event, ?LINE})
    end,

    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
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
    after ?TIMEOUT -> ct:fail(released_timeout)
    end,

    %% We don't have sufficient authorization.
    Tag2 = <<"dtag 2">>,
    Msg2 = amqp10_msg:set_properties(#{to => To2}, amqp10_msg:new(Tag2, <<"m2">>)),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ExpectedErr = error_unauthorized(
                    <<"write access to topic '.a.b' in exchange 'amq.topic' in "
                      "vhost 'test vhost' refused for user 'test user'">>),
    receive {amqp10_event, {session, Session, {ended, ExpectedErr}}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,

    ok = close_connection_sync(Connection).

authn_failure_event(Config) ->
    ok = event_recorder:start(Config),

    User = ?config(test_user, Config),
    OpnConf0 = connection_config(Config),
    OpnConf = maps:update(sasl, {plain, User, <<"wrong password">>}, OpnConf0),

    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, sasl_auth_failure}}} -> ok
    after ?TIMEOUT -> flush(missing_closed),
                  ct:fail("did not receive sasl_auth_failure")
    end,

    [E | _] = event_recorder:get_events(Config),
    ok = event_recorder:stop(Config),

    Proto = case web_amqp(Config) of
                true -> {'Web AMQP', {1, 0}};
                false -> {1, 0}
            end,
    assert_event_type(user_authentication_failure, E),
    assert_event_prop([{name, <<"test user">>},
                       {auth_mechanism, <<"PLAIN">>},
                       {ssl, false},
                       {protocol, Proto}],
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
    after ?TIMEOUT -> ct:fail(missing_opened)
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
    after ?TIMEOUT -> ct:fail(missing_closed)
    end,

    ok = rpc(Config, application, set_env, [App, Par, Default]).

sasl_plain_failure(Config) ->
    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf = OpnConf0#{sasl := {plain, <<"guest">>, <<"wrong password">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, Reason}}} ->
                ?assertEqual(sasl_auth_failure, Reason)
    after ?TIMEOUT -> ct:fail(missing_closed)
    end.

%% Skipping SASL is disallowed in RabbitMQ.
sasl_none_failure(Config) ->
    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf = OpnConf0#{sasl := none},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, _Reason}}} -> ok
    after ?TIMEOUT -> ct:fail(missing_closed)
    end.

vhost_absent(Config) ->
    OpnConf = connection_config(Config, <<"this vhost does not exist">>),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection, {closed, _}}} -> ok
    after ?TIMEOUT -> ct:fail(missing_closed)
    end.

vhost_connection_limit(Config) ->
    Vhost = proplists:get_value(test_vhost, Config),
    ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, Vhost, max_connections, 1),

    OpnConf = connection_config(Config),
    {ok, C1} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C1, opened}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,
    {ok, C2} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C2, {closed, _}}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,

    OpnConf0 = connection_config(Config, <<"/">>),
    OpnConf1 = OpnConf0#{sasl := anon},
    {ok, C3} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, C3, opened}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,
    {ok, C4} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, C4, opened}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
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
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,

    {ok, C2} = amqp10_client:open_connection(connection_config(Config)),
    receive {amqp10_event, {connection, C2, opened}} -> ok
    after ?TIMEOUT -> ct:fail({missing_event, ?LINE})
    end,

    ok = close_connection_sync(C2),
    ok = rabbit_ct_broker_helpers:clear_user_limits(Config, DefaultUser, Limit).

declare_exchange(Config) ->
    {Conn, _Session, LinkPair} = init_pair(Config),
    XName = <<"📮"/utf8>>,
    ExpectedErr = error_unauthorized(
                    <<"configure access to exchange '", XName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{})),
    ok = close_connection_sync(Conn).

declare_exchange_alternate_exchange_allowed(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    XName = <<"x.1">>,
    AeName = <<"ae.1">>,
    XProps = #{arguments => #{<<"alternate-exchange">> => {utf8, AeName}}},
    %% configure and read on the declared exchange, write on the alternate exchange
    ok = set_permissions(Config, XName, AeName, XName),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, XProps),
    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ok = close_connection_sync(Conn).

declare_exchange_alternate_exchange_read(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    XName = <<"x.1">>,
    AeName = <<"ae.1">>,
    XProps = #{arguments => #{<<"alternate-exchange">> => {utf8, AeName}}},
    %% missing read permission to the declared exchange
    ok = set_permissions(Config, XName, AeName, <<>>),
    ExpectedErr = error_unauthorized(
                    <<"read access to exchange '", XName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:declare_exchange(LinkPair, XName, XProps)),
    ok = close_connection_sync(Conn).

declare_exchange_alternate_exchange_write(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    XName = <<"x.1">>,
    AeName = <<"ae.1">>,
    XProps = #{arguments => #{<<"alternate-exchange">> => {utf8, AeName}}},
    %% missing write permission to the alternate exchange
    ok = set_permissions(Config, XName, <<>>, XName),
    ExpectedErr = error_unauthorized(
                    <<"write access to exchange '", AeName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:declare_exchange(LinkPair, XName, XProps)),
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

list_bindings_allowed(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    QName = BindingKey = atom_to_binary(?FUNCTION_NAME),
    XName = <<"amq.direct">>,
    ok = set_permissions(Config, QName, QName, XName),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, BindingKey, #{}),
    {ok, Bindings} = rabbitmq_amqp_client:list_bindings(
                       LinkPair, XName, {queue, QName}, BindingKey),
    ?assertMatch([#{source := XName,
                    destination := {queue, QName},
                    binding_key := BindingKey}],
                 Bindings),
    ok = close_connection_sync(Conn).

list_bindings_exchange_destination(Config) ->
    {Conn, _, LinkPair} = init_pair(Config),
    DstXName = BindingKey = atom_to_binary(?FUNCTION_NAME),
    SrcXName = <<"amq.direct">>,
    ok = set_permissions(Config, DstXName, DstXName, SrcXName),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, DstXName, #{}),
    ok = rabbitmq_amqp_client:bind_exchange(LinkPair, DstXName, SrcXName, BindingKey, #{}),
    {ok, Bindings} = rabbitmq_amqp_client:list_bindings(
                       LinkPair, SrcXName, {exchange, DstXName}, BindingKey),
    ?assertMatch([#{source := SrcXName,
                    destination := {exchange, DstXName},
                    binding_key := BindingKey}],
                 Bindings),
    ok = close_connection_sync(Conn).

list_bindings_source_read(Config) ->
    {Conn, Session1, LinkPair1} = init_pair(Config),
    QName = BindingKey = atom_to_binary(?FUNCTION_NAME),
    XName = <<"amq.direct">>,
    ok = set_permissions(Config, QName, QName, XName),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair1, QName, XName, BindingKey, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = amqp10_client:end_session(Session1),

    %% remove the read permission to the source exchange
    ok = set_permissions(Config, QName, QName, <<"^$">>),

    {ok, Session2} = amqp10_client:begin_session_sync(Conn),
    {ok, LinkPair2} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session2, <<"pair 2">>),
    ExpectedErr = error_unauthorized(
                    <<"read access to exchange '", XName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:list_bindings(LinkPair2, XName, {queue, QName}, BindingKey)),
    ok = close_connection_sync(Conn).

list_bindings_destination_write(Config) ->
    {Conn, Session1, LinkPair1} = init_pair(Config),
    QName = BindingKey = atom_to_binary(?FUNCTION_NAME),
    XName = <<"amq.direct">>,
    ok = set_permissions(Config, QName, QName, XName),
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair1, QName, XName, BindingKey, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = amqp10_client:end_session(Session1),

    %% remove the write permission to the destination queue
    ok = set_permissions(Config, QName, <<"^$">>, XName),

    {ok, Session2} = amqp10_client:begin_session_sync(Conn),
    {ok, LinkPair2} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session2, <<"pair 2">>),
    ExpectedErr = error_unauthorized(
                    <<"write access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:list_bindings(LinkPair2, XName, {queue, QName}, BindingKey)),
    ok = close_connection_sync(Conn).

passive_queue_declare(Config) ->
    QName = <<"🍿"/utf8>>,
    ok = set_permissions(Config, QName, <<>>, <<>>),
    {Conn1, Session1, LinkPair1} = init_pair(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = amqp10_client:end_session(Session1),
    ok = close_connection_sync(Conn1),

    %% No permissions: passive declare is refused.
    ok = set_permissions(Config, <<>>, <<>>, <<>>),
    {Conn091, Ch1} = open_amqpl_channel(Config),
    monitor(process, Ch1),
    _ = (catch amqp_channel:call(Ch1, #'queue.declare'{queue = QName, passive = true})),
    assert_channel_down(Ch1),

    %% Passive declare permission checks are a special case, by design:
    %% any permission (`configure`, `write`, or `read`) would do.
    lists:foreach(
      fun({ConfigP, WriteP, ReadP}) ->
              ok = set_permissions(Config, ConfigP, WriteP, ReadP),
              {ok, Ch} = amqp_connection:open_channel(Conn091),
              #'queue.declare_ok'{queue = QName} =
                  amqp_channel:call(Ch, #'queue.declare'{queue = QName, passive = true}),
              ok = amqp_channel:close(Ch)
      end, [{QName, <<>>,  <<>>},
            {<<>>,  QName, <<>>},
            {<<>>,  <<>>,  QName}]),

    ok = amqp_connection:close(Conn091).

passive_exchange_declare(Config) ->
    XName = <<"amq.direct">>,

    %% No permissions of any kind? Then the passive declare is refused.
    ok = set_permissions(Config, <<>>, <<>>, <<>>),
    {Conn091, Ch1} = open_amqpl_channel(Config),
    monitor(process, Ch1),
    _ = (catch amqp_channel:call(Ch1, #'exchange.declare'{exchange = XName, passive = true})),
    assert_channel_down(Ch1),

    %% Any of `configure`, `write`, or `read` alone grants the permission.
    lists:foreach(
      fun({ConfigP, WriteP, ReadP}) ->
              ok = set_permissions(Config, ConfigP, WriteP, ReadP),
              {ok, Ch} = amqp_connection:open_channel(Conn091),
              #'exchange.declare_ok'{} =
                  amqp_channel:call(Ch, #'exchange.declare'{exchange = XName, passive = true}),
              ok = amqp_channel:close(Ch)
      end, [{XName, <<>>,  <<>>},
            {<<>>,  XName, <<>>},
            {<<>>,  <<>>,  XName}]),

    ok = amqp_connection:close(Conn091).

get_queue(Config) ->
    QName = <<"🍿"/utf8>>,
    ok = set_permissions(Config, QName, <<>>, <<>>),
    {Conn1, Session1, LinkPair1} = init_pair(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair1, QName, #{}),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair1),
    ok = amqp10_client:end_session(Session1),
    ok = close_connection_sync(Conn1),

    %% No permissions: refused on `configure' (tried last).
    ok = set_permissions(Config, <<>>, <<>>, <<>>),
    {Conn2, _, LinkPair2} = init_pair(Config),
    ExpectedErr = error_unauthorized(
                    <<"configure access to queue '", QName/binary,
                      "' in vhost 'test vhost' refused for user 'test user'">>),
    ?assertEqual({error, {session_ended, ExpectedErr}},
                 rabbitmq_amqp_client:get_queue(LinkPair2, QName)),
    ok = close_connection_sync(Conn2),

    %% Any of `configure`, `write`, or `read` alone grantts the permission.
    lists:foreach(
      fun({ConfigP, WriteP, ReadP}) ->
              ok = set_permissions(Config, ConfigP, WriteP, ReadP),
              {Conn, _, LinkPair} = init_pair(Config),
              {ok, #{}} = rabbitmq_amqp_client:get_queue(LinkPair, QName),
              ok = close_connection_sync(Conn)
      end, [{QName, <<>>,  <<>>},
            {<<>>,  QName, <<>>},
            {<<>>,  <<>>,  QName}]).

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
    Cfg = amqp_utils:connection_config(Config),
    User = Password = ?config(test_user, Config),
    Cfg#{hostname => <<"vhost:", Vhost/binary>>,
         sasl := {plain, User, Password}}.

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

open_amqpl_channel(Config) ->
    User = ?config(test_user, Config),
    Vhost = ?config(test_vhost, Config),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, Vhost, User, User),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {Conn, Ch}.

assert_channel_down(Ch) ->
    receive {'DOWN', _, process, Ch,
             {shutdown, {server_initiated_close, 403, _}}} ->
                ok
    after ?TIMEOUT ->
              ct:fail(did_not_receive_access_refused)
    end.
