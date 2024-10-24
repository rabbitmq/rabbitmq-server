%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(topic_permission_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     {group, sequential_tests}
    ].

groups() ->
    [
     {sequential_tests, [],
      [
       amqp_x_cc_annotation,
       amqpl_cc_headers,
       amqpl_bcc_headers,
       topic_permission_database_access,
       topic_permission_checks
      ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

amqp_x_cc_annotation(Config) ->
    ok = set_topic_permissions(Config, "^a", ".*"),

    QName1 = <<"queue 1">>,
    QName2 = <<"queue 2">>,
    {Connection, Session1, LinkPair} = amqp_utils:init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName1, #{}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName2, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName1, <<"amq.topic">>, <<"a.1">>, #{}),
    ok = rabbitmq_amqp_client:bind_queue(LinkPair, QName2, <<"amq.topic">>, <<"a.2">>, #{}),

    {ok, Sender1} = amqp10_client:attach_sender_link(
                      Session1,
                      <<"sender 1">>,
                      rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<"a.1">>)),
    ok = amqp_utils:wait_for_credit(Sender1),
    {ok, Receiver1} = amqp10_client:attach_receiver_link(
                        Session1, <<"receiver 1">>, rabbitmq_amqp_address:queue(QName1), settled),
    {ok, Receiver2} = amqp10_client:attach_receiver_link(
                        Session1, <<"receiver 2">>, rabbitmq_amqp_address:queue(QName2), settled),
    %% We have permissions to send to both topics.
    %% Therefore, m1 should be sent to both queues.
    ok = amqp10_client:send_msg(Sender1, amqp10_msg:set_message_annotations(
                                           #{<<"x-cc">> => {list, [{utf8, <<"a.2">>}]}},
                                           amqp10_msg:new(<<"t1">>, <<"m1">>, true))),
    {ok, Msg1} = amqp10_client:get_msg(Receiver1),
    {ok, Msg2} = amqp10_client:get_msg(Receiver2),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg1)),
    ?assertEqual([<<"m1">>], amqp10_msg:body(Msg2)),
    ok = amqp_utils:detach_link_sync(Sender1),
    ok = amqp_utils:detach_link_sync(Receiver1),
    ok = amqp_utils:detach_link_sync(Receiver2),

    {ok, Session2} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender2} = amqp10_client:attach_sender_link(
                      Session2,
                      <<"sender 2">>,
                      rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<"x.1">>)),
    ok = amqp_utils:wait_for_credit(Sender2),
    ok = amqp10_client:send_msg(Sender2, amqp10_msg:set_message_annotations(
                                           #{<<"x-cc">> => {list, [{utf8, <<"a.2">>}]}},
                                           amqp10_msg:new(<<"t2">>, <<"m2">>, true))),
    receive
        {amqp10_event,
         {session, Session2,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
              description = {utf8, Description1}}}}} ->
            ?assertEqual(
               <<"write access to topic 'x.1' in exchange 'amq.topic' in vhost '/' refused for user 'guest'">>,
               Description1)
    after 5000 -> amqp_utils:flush(missing_ended),
                  ct:fail({missing_event, ?LINE})
    end,

    {ok, Session3} = amqp10_client:begin_session_sync(Connection),
    {ok, Sender3} = amqp10_client:attach_sender_link(
                      Session3,
                      <<"sender 3">>,
                      rabbitmq_amqp_address:exchange(<<"amq.topic">>, <<"a.1">>)),
    ok = amqp_utils:wait_for_credit(Sender3),
    ok = amqp10_client:send_msg(Sender3, amqp10_msg:set_message_annotations(
                                           #{<<"x-cc">> => {list, [{utf8, <<"x.2">>}]}},
                                           amqp10_msg:new(<<"t3">>, <<"m3">>, true))),
    receive
        {amqp10_event,
         {session, Session3,
          {ended,
           #'v1_0.error'{
              condition = ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
              description = {utf8, Description2}}}}} ->
            ?assertEqual(
               <<"write access to topic 'x.2' in exchange 'amq.topic' in vhost '/' refused for user 'guest'">>,
               Description2)
    after 5000 -> amqp_utils:flush(missing_ended),
                  ct:fail({missing_event, ?LINE})
    end,

    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName1),
    {ok, #{message_count := 0}} = rabbitmq_amqp_client:delete_queue(LinkPair, QName2),
    ok = amqp_utils:end_session_sync(Session1),
    ok = amqp10_client:close_connection(Connection),
    ok = clear_topic_permissions(Config).

amqpl_cc_headers(Config) ->
    amqpl_headers(<<"CC">>, Config).

amqpl_bcc_headers(Config) ->
    amqpl_headers(<<"BCC">>, Config).

amqpl_headers(Header, Config) ->
    QName1 = <<"q1">>,
    QName2 = <<"q2">>,
    Ch1 = rabbit_ct_client_helpers:open_channel(Config),

    ok = set_topic_permissions(Config, "^a", ".*"),

    #'queue.declare_ok'{} = amqp_channel:call(Ch1, #'queue.declare'{queue = QName1}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch1, #'queue.declare'{queue = QName2}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch1, #'queue.bind'{queue = QName1,
                                                              exchange = <<"amq.topic">>,
                                                              routing_key = <<"a.1">>}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch1, #'queue.bind'{queue = QName2,
                                                              exchange = <<"amq.topic">>,
                                                              routing_key = <<"a.2">>}),

    amqp_channel:call(Ch1, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch1, self()),

    %% We have permissions to send to both topics.
    %% Therefore, m1 should be sent to both queues.
    amqp_channel:call(
      Ch1,
      #'basic.publish'{exchange = <<"amq.topic">>,
                       routing_key = <<"a.1">>},
      #amqp_msg{payload = <<"m1">>,
                props = #'P_basic'{headers = [{Header, array, [{longstr, <<"a.2">>}]}]}}),
    receive #'basic.ack'{} -> ok
    after 5000 -> ct:fail({missing_confirm, ?LINE})
    end,

    monitor(process, Ch1),
    amqp_channel:call(
      Ch1,
      #'basic.publish'{exchange = <<"amq.topic">>,
                       routing_key = <<"x.1">>},
      #amqp_msg{payload = <<"m2">>,
                props = #'P_basic'{headers = [{Header, array, [{longstr, <<"a.2">>}]}]}}),
    ok = assert_channel_down(
           Ch1,
           <<"ACCESS_REFUSED - write access to topic 'x.1' in exchange "
             "'amq.topic' in vhost '/' refused for user 'guest'">>),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config),
    monitor(process, Ch2),
    amqp_channel:call(
      Ch2,
      #'basic.publish'{exchange = <<"amq.topic">>,
                       routing_key = <<"a.1">>},
      #amqp_msg{payload = <<"m3">>,
                props = #'P_basic'{headers = [{Header, array, [{longstr, <<"x.2">>}]}]}}),
    ok = assert_channel_down(
           Ch2,
           <<"ACCESS_REFUSED - write access to topic 'x.2' in exchange "
             "'amq.topic' in vhost '/' refused for user 'guest'">>),

    Ch3 = rabbit_ct_client_helpers:open_channel(Config),
    ?assertEqual(#'queue.delete_ok'{message_count = 1},
                 amqp_channel:call(Ch3, #'queue.delete'{queue = QName1})),
    ?assertEqual(#'queue.delete_ok'{message_count = 1},
                 amqp_channel:call(Ch3, #'queue.delete'{queue = QName2})),
    ok = rabbit_ct_client_helpers:close_channel(Ch3),
    ok = clear_topic_permissions(Config).

topic_permission_database_access(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, ?MODULE, clear_tables, []),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, topic_permission_database_access1, [Config]).

topic_permission_database_access1(_Config) ->
    rabbit_vhost:add(<<"/">>, <<"acting-user">>),
    rabbit_vhost:add(<<"other-vhost">>, <<"acting-user">>),
    rabbit_auth_backend_internal:add_user(<<"guest">>, <<"guest">>, <<"acting-user">>),
    rabbit_auth_backend_internal:add_user(<<"dummy">>, <<"dummy">>, <<"acting-user">>),

    ok = rabbit_auth_backend_internal:set_topic_permissions(
           <<"guest">>, <<"/">>, <<"amq.topic">>, "^a", "^a", <<"acting-user">>
          ),
    1 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    0 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"/">>)),
    0 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"other-vhost">>)),
    1 = length(rabbit_auth_backend_internal:list_topic_permissions()),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"other-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    ),
    2 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"other-vhost">>)),
    2 = length(rabbit_auth_backend_internal:list_topic_permissions()),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"topic1">>, "^a", "^a", <<"acting-user">>
    ),
    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"topic2">>, "^a", "^a", <<"acting-user">>
    ),

    4 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    3 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_user_vhost_topic_permissions(<<"guest">>,<<"other-vhost">>)),
    4 = length(rabbit_auth_backend_internal:list_topic_permissions()),

    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"other-vhost">>,
                                                         <<"acting-user">>),
    0 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),
    3 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"/">>, <<"topic1">>,
                                                         <<"acting-user">>),
    2 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    rabbit_auth_backend_internal:clear_topic_permissions(<<"guest">>, <<"/">>,
                                                         <<"acting-user">>),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),


    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"non-existing-user">>, <<"other-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    )),

    {error, {no_such_vhost, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"non-existing-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    )),

    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"non-existing-user">>, <<"non-existing-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    )),

    {error, {no_such_user, _}} = (catch rabbit_auth_backend_internal:list_user_topic_permissions(
        <<"non-existing-user">>
    )),

    {error, {no_such_vhost, _}} = (catch rabbit_auth_backend_internal:list_vhost_topic_permissions(
        <<"non-existing-vhost">>
    )),

    {error, {invalid_regexp, _, _}} = (catch rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"amq.topic">>, "[", "^a", <<"acting-user">>
    )),
    ok.

topic_permission_checks(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, ?MODULE, clear_tables, []),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, topic_permission_checks1, [Config]).

topic_permission_checks1(_Config) ->
    rabbit_vhost:add(<<"/">>, <<"">>),
    rabbit_vhost:add(<<"other-vhost">>, <<"">>),

    rabbit_auth_backend_internal:add_user(<<"guest">>, <<"guest">>, <<"acting-user">>),
    rabbit_auth_backend_internal:add_user(<<"dummy">>, <<"dummy">>, <<"acting-user">>),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"/">>, <<"amq.topic">>, "^a", "^a", <<"acting-user">>
    ),
    1 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    0 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),

    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"other-vhost">>, <<"amq.topic">>, ".*", ".*", <<"acting-user">>
    ),
    2 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"guest">>)),
    0 = length(rabbit_auth_backend_internal:list_user_topic_permissions(<<"dummy">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"/">>)),
    1 = length(rabbit_auth_backend_internal:list_vhost_topic_permissions(<<"other-vhost">>)),

    User = #auth_user{username = <<"guest">>},
    Topic = #resource{name = <<"amq.topic">>, virtual_host = <<"/">>,
        kind = topic},
    Context = #{routing_key => <<"a.b.c">>},
    Permissions = [write, read],
    %% user has access to exchange, routing key matches
    [true = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic,
        Perm,
        Context
    ) || Perm <- Permissions],
    %% user has access to exchange, routing key does not match
    [false = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic,
        Perm,
        #{routing_key => <<"x.y.z">>}
    ) || Perm <- Permissions],
    %% user has access to exchange but not on this vhost
    %% let pass when there's no match
    [true = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic#resource{virtual_host = <<"fancyvhost">>},
        Perm,
        Context
    ) || Perm <- Permissions],
    %% user does not have access to exchange
    %% let pass when there's no match
    [true = rabbit_auth_backend_internal:check_topic_access(
        #auth_user{username = <<"dummy">>},
        Topic,
        Perm,
        Context
    ) || Perm <- Permissions],

    %% expand variables
    rabbit_auth_backend_internal:set_topic_permissions(
        <<"guest">>, <<"other-vhost">>, <<"amq.topic">>,
        "services.{vhost}.accounts.{username}.notifications",
        "services.{vhost}.accounts.{username}.notifications", <<"acting-user">>
    ),
    %% routing key OK
    [true = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic#resource{virtual_host = <<"other-vhost">>},
        Perm,
        #{routing_key   => <<"services.other-vhost.accounts.guest.notifications">>,
          variable_map  => #{
              <<"username">> => <<"guest">>,
              <<"vhost">>    => <<"other-vhost">>
          }
        }
    ) || Perm <- Permissions],
    %% routing key KO
    [false = rabbit_auth_backend_internal:check_topic_access(
        User,
        Topic#resource{virtual_host = <<"other-vhost">>},
        Perm,
        #{routing_key   => <<"services.default.accounts.dummy.notifications">>,
          variable_map  => #{
              <<"username">> => <<"guest">>,
              <<"vhost">>    => <<"other-vhost">>
          }
        }
    ) || Perm <- Permissions],

    ok.

clear_tables() ->
    ok = rabbit_db_vhost:clear(),
    ok = rabbit_db_user:clear().

set_topic_permissions(Config, WritePat, ReadPat) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_auth_backend_internal, set_topic_permissions,
           [<<"guest">>, <<"/">>, <<"amq.topic">>, WritePat, ReadPat, <<"acting-user">>]).

clear_topic_permissions(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_auth_backend_internal, clear_topic_permissions,
           [<<"guest">>, <<"/">>, <<"acting-user">>]).

assert_channel_down(Ch, Reason) ->
    receive {'DOWN', _MonitorRef, process, Ch,
             {shutdown,
              {server_initiated_close, 403, Reason}}} ->
                ok
    after 5000 ->
              ct:fail({did_not_receive, Reason})
    end.
