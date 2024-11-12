%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(system_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
     {group, amqp_1_0},
     {group, amqp_0_9_1}
    ].

groups() ->
    [
     {amqp_1_0, [shuffle],
      shared_tests() ++
      [
       amqp_1_0_amqp_connection,
       amqp_1_0_queue_created,
       headers_exchange
      ]},
     {amqp_0_9_1, [],
      shared_tests() ++
      [
       amqp_0_9_1_amqp_connection,
       amqp_0_9_1_queue_created,
       unregister
      ]}
    ].

shared_tests() ->
    [
     authentication_success,
     authentication_failure,
     audit_queue,
     audit_exchange,
     audit_exchange_internal_parameter,
     audit_binding,
     audit_vhost,
     audit_vhost_deletion,
     audit_channel,
     audit_connection,
     audit_direct_connection,
     audit_consumer,
     audit_parameter,
     audit_policy,
     audit_vhost_limit,
     audit_user,
     audit_user_password,
     audit_user_tags,
     audit_permission,
     audit_topic_permission,
     resource_alarm
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(rabbitmq_amqp_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config,
                {rabbitmq_event_exchange, [{protocol, Group}]}),
    Config2 = rabbit_ct_helpers:set_config(
                Config1, [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(
      Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).   

%% -------------------------------------------------------------------
%% Testsuite cases 
%% -------------------------------------------------------------------

amqp_1_0_queue_created(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Headers = queue_created(QName, Config),
    ?assertEqual({longstr, QName},
                 rabbit_misc:table_lookup(Headers, <<"x-opt-name">>)),
    ?assertEqual({table, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
                 rabbit_misc:table_lookup(Headers, <<"x-opt-arguments">>)).

amqp_0_9_1_queue_created(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Headers = queue_created(QName,Config),
    ?assertEqual({longstr, QName},
                 rabbit_misc:table_lookup(Headers, <<"name">>)),
    {array, QArgs} = rabbit_misc:table_lookup(Headers, <<"arguments">>),
    %% Ideally, instead of a longstr containing the formatted Erlang term,
    %% we should expect a table.
    ?assertEqual(<<"{<<\"x-queue-type\">>,longstr,<<\"classic\">>}">>,
                 proplists:get_value(longstr, QArgs)).

queue_created(QName, Config) ->
    Ch = declare_event_queue(Config, <<"queue.created">>),

    Now = os:system_time(second),
    #'queue.declare_ok'{} = amqp_channel:call(
                              Ch, #'queue.declare'{
                                     queue = QName,
                                     exclusive = true,
                                     arguments = [{<<"x-queue-type">>, longstr, <<"classic">>}]
                                    }),

    receive
        {#'basic.deliver'{routing_key = Key},
         #amqp_msg{props = #'P_basic'{headers = Headers,
                                      timestamp = TS}}} ->
            %% timestamp is within the last 5 seconds
            ?assert(((TS - Now) =< 5)),
            ?assertEqual(<<"queue.created">>, Key),
            rabbit_ct_client_helpers:close_channel(Ch),
            Headers
    end.

authentication_success(Config) ->
    Ch = declare_event_queue(Config, <<"user.#">>),
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),

    receive
        {#'basic.deliver'{routing_key = Key},
         #amqp_msg{props = #'P_basic'{headers = Headers}}} ->
            <<"user.authentication.success">> = Key,
            {Vhost, PeerHost, Ssl} =
            case group_name(Config) of
                amqp_0_9_1 ->
                    {<<"vhost">>, <<"peer_host">>, <<"ssl">>};
                amqp_1_0 ->
                    {<<"x-opt-vhost">>, <<"x-opt-peer-host">>, <<"x-opt-ssl">>}
            end,
            undefined = rabbit_misc:table_lookup(Headers, Vhost),
            {longstr, _PeerHost} = rabbit_misc:table_lookup(Headers, PeerHost),
            {bool, false} = rabbit_misc:table_lookup(Headers, Ssl)
    after 5000 -> missing_deliver
    end,

    ok = amqp_connection:close(Conn2),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

authentication_failure(Config) ->
    Ch = declare_event_queue(Config, <<"user.authentication.*">>),
    {error, _} = rabbit_ct_client_helpers:open_unmanaged_connection(
                   Config, 0, <<"fake user">>, <<"fake password">>),

    receive
        {#'basic.deliver'{routing_key = Key},
         #amqp_msg{props = #'P_basic'{headers = Headers}}} ->
            ?assertEqual(<<"user.authentication.failure">>, Key),
            User = case group_name(Config) of
                       amqp_0_9_1 -> <<"name">>;
                       amqp_1_0 -> <<"x-opt-name">>
                   end,
            ?assertEqual({longstr, <<"fake user">>},
                         rabbit_misc:table_lookup(Headers, User))
    after 5000 -> missing_deliver
    end,

    ok = rabbit_ct_client_helpers:close_channel(Ch).

audit_queue(Config) ->
    Ch = declare_event_queue(Config, <<"queue.*">>),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    receive_user_in_event(<<"queue.created">>, Config),

    #'queue.delete_ok'{} =
        amqp_channel:call(Ch, #'queue.delete'{queue = Q}),

    receive_user_in_event(<<"queue.deleted">>, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_exchange(Config) ->
    Ch = declare_event_queue(Config, <<"exchange.*">>),

    X = <<"exchange.audited">>,
    #'exchange.declare_ok'{} =
        amqp_channel:call(Ch, #'exchange.declare'{exchange = X,
                                                  type = <<"topic">>}),

    receive_user_in_event(<<"exchange.created">>, Config),

    #'exchange.delete_ok'{} =
        amqp_channel:call(Ch, #'exchange.delete'{exchange = X}),

    receive_user_in_event(<<"exchange.deleted">>, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_binding(Config) ->
    Ch = declare_event_queue(Config, <<"binding.*">>),
    %% The binding to the event exchange itself is the first queued event
    receive_user_in_event(<<"binding.created">>, Config),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),

    #'queue.bind_ok'{} =
        amqp_channel:call(Ch, #'queue.bind'{queue = Q,
                                            exchange = <<"amq.direct">>,
                                            routing_key = <<"test">>}),
    receive_user_in_event(<<"binding.created">>, Config),

    #'queue.unbind_ok'{} =
        amqp_channel:call(Ch, #'queue.unbind'{queue = Q,
                                              exchange = <<"amq.direct">>,
                                              routing_key = <<"test">>}),
    receive_user_in_event(<<"binding.deleted">>, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_vhost(Config) ->
    Node = atom_to_binary(rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    Ch = declare_event_queue(Config, <<"vhost.*">>),
    User = <<"Bugs Bunny">>,

    rabbit_ct_broker_helpers:add_vhost(Config, 0, <<"test-vhost">>, User),
    Headers = receive_user_in_event(<<"vhost.created">>, User, Config),

    Key = case group_name(Config) of
              amqp_0_9_1 -> <<"cluster_state">>;
              amqp_1_0 -> <<"x-opt-cluster-state">>
          end,
    ?assertEqual({table, [{Node, longstr, <<"running">>}]},
                 rabbit_misc:table_lookup(Headers, Key)),

    rabbit_ct_broker_helpers:delete_vhost(Config, 0, <<"test-vhost">>, User),
    receive_user_in_event(<<"vhost.deleted">>, User, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_vhost_deletion(Config) ->
    Ch = declare_event_queue(Config, <<"queue.*">>),
    ConnUser = proplists:get_value(rmq_username, Config),
    User = <<"Bugs Bunny">>,
    Vhost = <<"test-vhost">>,

    rabbit_ct_broker_helpers:add_vhost(Config, 0, Vhost, User),
    rabbit_ct_broker_helpers:set_full_permissions(Config, ConnUser, Vhost),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, Vhost),
    {ok, Ch2} = amqp_connection:open_channel(Conn),

    %% The user that creates the queue is the connection one, not the vhost creator
    #'queue.declare_ok'{queue = _Q} = amqp_channel:call(Ch2, #'queue.declare'{}),
    receive_user_in_event(<<"queue.created">>, ConnUser, Config),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch2),

    %% Validate that the user deleting the queue is the one used to delete the vhost,
    %% not the original user that created the queue (the connection one)
    rabbit_ct_broker_helpers:delete_vhost(Config, 0, Vhost, User),
    receive_user_in_event(<<"queue.deleted">>, User, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_channel(Config) ->
    Ch = declare_event_queue(Config, <<"channel.*">>),

    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Ch2} = amqp_connection:open_channel(Conn),
    receive_user_in_event(<<"channel.created">>, Config),

    rabbit_ct_client_helpers:close_channel(Ch2),
    receive_user_in_event(<<"channel.closed">>, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_connection(Config) ->
    Ch = declare_event_queue(Config, <<"connection.*">>),

    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    receive_user_in_event(<<"connection.created">>, Config),

    %% Username is not available in connection_close
    rabbit_ct_client_helpers:close_connection(Conn),
    Headers = receive_event(<<"connection.closed">>, user_key(Config), undefined),
    case group_name(Config) of
        amqp_0_9_1 ->
            ?assert(lists:keymember(<<"client_properties">>, 1, Headers));
        amqp_1_0 ->
            {table, ClientProps} = rabbit_misc:table_lookup(Headers, <<"x-opt-client-properties">>),
            ?assertEqual({longstr, <<"Erlang">>},
                         rabbit_misc:table_lookup(ClientProps, <<"platform">>)),
            {table, Caps} = rabbit_misc:table_lookup(ClientProps, <<"capabilities">>),
            ?assertEqual({bool, true},
                         rabbit_misc:table_lookup(Caps, <<"basic.nack">>)),
            ?assertEqual({bool, true},
                         rabbit_misc:table_lookup(Caps, <<"connection.blocked">>))
    end,

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_direct_connection(Config) ->
    Ch = declare_event_queue(Config, <<"connection.*">>),

    Conn = rabbit_ct_client_helpers:open_unmanaged_connection_direct(Config),
    receive_user_in_event(<<"connection.created">>, Config),

    rabbit_ct_client_helpers:close_connection(Conn),
    receive_event(<<"connection.closed">>, user_key(Config), undefined),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_consumer(Config) ->
    Ch = declare_event_queue(Config, <<"consumer.*">>),
    receive_user_in_event(<<"consumer.created">>, Config),

    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true},
                           self()),
    CTag = receive #'basic.consume_ok'{consumer_tag = C} -> C end,
    receive_user_in_event(<<"consumer.created">>, Config),

    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
    receive_user_in_event(<<"consumer.deleted">>, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_exchange_internal_parameter(Config) ->
    Ch = declare_event_queue(Config, <<"parameter.*">>),

    X = <<"exchange.audited-for-parameters">>,
    #'exchange.declare_ok'{} =
        amqp_channel:call(Ch, #'exchange.declare'{exchange = X,
                                                  type = <<"topic">>}),
    #'exchange.delete_ok'{} =
        amqp_channel:call(Ch, #'exchange.delete'{exchange = X}),

    %% Exchange deletion sets and clears a runtime parameter which acts as a
    %% kind of lock:
    receive_user_in_event(<<"parameter.set">>, Config),
    receive_user_in_event(<<"parameter.cleared">>, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_parameter(Config) ->
    Ch = declare_event_queue(Config, <<"parameter.*">>),
    VHost = proplists:get_value(rmq_vhost, Config),
    User = <<"Bugs Bunny">>,

    ok = rabbit_ct_broker_helpers:set_parameter(
           Config, 0, VHost, <<"vhost-limits">>, <<"limits">>,
           [{<<"max-connections">>, 200}], User),
    receive_user_in_event(<<"parameter.set">>, User, Config),

    ok = rabbit_ct_broker_helpers:clear_parameter(
           Config, 0, VHost, <<"vhost-limits">>, <<"limits">>, User),
    receive_user_in_event(<<"parameter.cleared">>, User, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_policy(Config) ->
    Ch = declare_event_queue(Config, <<"policy.*">>),
    User = <<"Bugs Bunny">>,

    rabbit_ct_broker_helpers:set_policy(Config, 0, <<".*">>, <<"all">>, <<"queues">>,
                                        [{<<"max-length-bytes">>, 10000}], User),
    receive_user_in_event(<<"policy.set">>, User, Config),

    ok = rabbit_ct_broker_helpers:clear_policy(Config, 0, <<".*">>, User),
    receive_user_in_event(<<"policy.cleared">>, User, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_vhost_limit(Config) ->
    Ch = declare_event_queue(Config, <<"vhost.limits.*">>),
    VHost = proplists:get_value(rmq_vhost, Config),
    User = <<"Bugs Bunny">>,

    ok = rabbit_ct_broker_helpers:set_parameter(
           Config, 0, VHost, <<"vhost-limits">>, <<"limits">>,
           [{<<"max-connections">>, 200}], User),
    receive_user_in_event(<<"vhost.limits.set">>, User, Config),

    ok = rabbit_ct_broker_helpers:clear_parameter(
           Config, 0, VHost, <<"vhost-limits">>, <<"limits">>, User),
    receive_user_in_event(<<"vhost.limits.cleared">>, User, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_user(Config) ->
    Ch = declare_event_queue(Config, <<"user.*">>),
    ActingUser = <<"Bugs Bunny">>,
    User = <<"Wabbit">>,

    rabbit_ct_broker_helpers:add_user(Config, 0, User, User, ActingUser),
    receive_user_in_event(<<"user.created">>, ActingUser, Config),

    rabbit_ct_broker_helpers:delete_user(Config, 0, User, ActingUser),
    receive_user_in_event(<<"user.deleted">>, ActingUser, Config),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_user_password(Config) ->
    Ch = declare_event_queue(Config, <<"user.password.*">>),
    ActingUser = <<"Bugs Bunny">>,
    User = <<"Wabbit">>,

    rabbit_ct_broker_helpers:add_user(Config, 0, User, User, ActingUser),
    rabbit_ct_broker_helpers:change_password(Config, 0, User, <<"pass">>, ActingUser),
    receive_user_in_event(<<"user.password.changed">>, ActingUser, Config),

    rabbit_ct_broker_helpers:clear_password(Config, 0, User, ActingUser),
    receive_user_in_event(<<"user.password.cleared">>, ActingUser, Config),
    rabbit_ct_broker_helpers:delete_user(Config, 0, User, ActingUser),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_user_tags(Config) ->
    Ch = declare_event_queue(Config, <<"user.tags.*">>),
    ActingUser = <<"Bugs Bunny">>,
    User = <<"Wabbit">>,

    rabbit_ct_broker_helpers:add_user(Config, 0, User, User, ActingUser),
    rabbit_ct_broker_helpers:set_user_tags(Config, 0, User, [management], ActingUser),
    receive_user_in_event(<<"user.tags.set">>, ActingUser, Config),

    rabbit_ct_broker_helpers:delete_user(Config, 0, User, ActingUser),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_permission(Config) ->
    Ch = declare_event_queue(Config, <<"permission.*">>),
    VHost = proplists:get_value(rmq_vhost, Config),
    ActingUser = <<"Bugs Bunny">>,
    User = <<"Wabbit">>,

    rabbit_ct_broker_helpers:add_user(Config, 0, User, User, ActingUser),
    rabbit_ct_broker_helpers:set_permissions(Config, 0, User, VHost, <<".*">>,
                                             <<".*">>, <<".*">>, ActingUser),
    receive_user_in_event(<<"permission.created">>, ActingUser, Config),

    rabbit_ct_broker_helpers:clear_permissions(Config, 0, User, VHost, ActingUser),
    receive_user_in_event(<<"permission.deleted">>, ActingUser, Config),
    rabbit_ct_broker_helpers:delete_user(Config, 0, User, ActingUser),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

audit_topic_permission(Config) ->
    Ch = declare_event_queue(Config, <<"topic.permission.*">>),
    VHost = proplists:get_value(rmq_vhost, Config),
    ActingUser = <<"Bugs Bunny">>,
    User = <<"Wabbit">>,

    rabbit_ct_broker_helpers:add_user(Config, 0, User, User, ActingUser),
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_auth_backend_internal, set_topic_permissions,
      [User, VHost, <<"amq.topic">>, "^a", "^a", ActingUser]),
    receive_user_in_event(<<"topic.permission.created">>, ActingUser, Config),

    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_auth_backend_internal, clear_topic_permissions,
      [User, VHost, ActingUser]),
    receive_user_in_event(<<"topic.permission.deleted">>, ActingUser, Config),
    rabbit_ct_broker_helpers:delete_user(Config, 0, User, ActingUser),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

resource_alarm(Config) ->
    Ch = declare_event_queue(Config, <<"alarm.*">>),

    Source = disk,
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_alarm, set_alarm,
                                 [{{resource_limit, Source, Node}, []}]),
    receive_event(<<"alarm.set">>),

    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_alarm, clear_alarm,
                                 [{resource_limit, Source, Node}]),
    receive_event(<<"alarm.cleared">>),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

unregister(Config) ->
    X = rabbit_misc:r(<<"/">>, exchange, <<"amq.rabbitmq.event">>),

    ?assertMatch({ok, _},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange,
                                              lookup, [X])),

    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange_type_event,
                                 unregister, []),

    ?assertEqual({error, not_found},
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange,
                                              lookup, [X])),
    ok.

%% Test the plugin publishing internally with AMQP 0.9.1 while the client uses AMQP 1.0.
amqp_0_9_1_amqp_connection(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection1, Session, LinkPair} = amqp_init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName,#{}),
    ok = rabbitmq_amqp_client:bind_queue(
           LinkPair, QName, <<"amq.rabbitmq.event">>, <<"connection.*">>, #{}),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address, settled),

    OpnConf0 = amqp_connection_config(Config),
    OpnConf = maps:update(container_id, <<"2nd container">>, OpnConf0),
    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection2, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertMatch(#{<<"x-routing-key">> := <<"connection.created">>},
                 amqp10_msg:message_annotations(Msg)),
    ?assertMatch(#{<<"container_id">> := <<"2nd container">>},
                 amqp10_msg:application_properties(Msg)),
    ok = amqp10_client:close_connection(Connection2),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection1).

%% Test the plugin publishing internally with AMQP 1.0 and the client using AMQP 1.0.
amqp_1_0_amqp_connection(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    {Connection1, Session, LinkPair} = amqp_init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName,#{}),
    ok = rabbitmq_amqp_client:bind_queue(
           LinkPair, QName, <<"amq.rabbitmq.event">>, <<"connection.*">>, #{}),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address, settled),

    Now = os:system_time(millisecond),
    OpnConf0 = amqp_connection_config(Config),
    OpnConf = maps:update(container_id, <<"2nd container">>, OpnConf0),
    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, Connection2, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ?assertEqual(<<>>, iolist_to_binary(amqp10_msg:body(Msg))),
    MsgAnns = amqp10_msg:message_annotations(Msg),
    ?assertMatch(#{<<"x-routing-key">> := <<"connection.created">>,
                   <<"x-opt-container-id">> := <<"2nd container">>,
                   <<"x-opt-channel-max">> := ChannelMax}
                   when is_integer(ChannelMax),
                        MsgAnns),
    %% We expect to receive event properties that have complex types.
    ClientProps = maps:get(<<"x-opt-client-properties">>, MsgAnns),
    OtpRelease = integer_to_binary(?OTP_RELEASE),
    ?assertMatch(#{
                   {symbol, <<"version">>} := {utf8, _Version},
                   {symbol, <<"product">>} := {utf8, <<"AMQP 1.0 client">>},
                   {symbol, <<"platform">>} := {utf8, <<"Erlang/OTP ", OtpRelease/binary>>}
                  },
                 maps:from_list(ClientProps)),
    FormattedPid = maps:get(<<"x-opt-pid">>, MsgAnns),

    %% The formatted Pid should include the RabbitMQ node name:
    ?assertMatch({match, _},
                 re:run(FormattedPid, <<"rmq-ct-system_SUITE">>)),

    #{creation_time := CreationTime} = amqp10_msg:properties(Msg),
    ?assert(is_integer(CreationTime)),
    ?assert(CreationTime > Now - 5000),
    ?assert(CreationTime < Now + 5000),

    ok = amqp10_client:close_connection(Connection2),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection1).

%% Test that routing on specific event properties works.
headers_exchange(Config) ->
    XName = <<"my headers exchange">>,
    QName = atom_to_binary(?FUNCTION_NAME),
    Address = rabbitmq_amqp_address:queue(QName),
    OpnConf = amqp_connection_config(Config),
    {Connection, Session, LinkPair} = amqp_init(Config),

    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{type => <<"headers">>}),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    ok = rabbitmq_amqp_client:bind_queue(
           LinkPair, QName, XName, <<>>,
           #{<<"x-opt-container-id">> => {utf8, <<"client-2">>},
             <<"x-match">> => {utf8, <<"any-with-x">>}}),
    ok = rabbitmq_amqp_client:bind_exchange(
           LinkPair, XName, <<"amq.rabbitmq.event">>, <<"connection.created">>, #{}),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, Address, settled),

    %% Open two connections.
    OpnConf1 = maps:update(container_id, <<"client-1">>, OpnConf),
    {ok, Connection1} = amqp10_client:open_connection(OpnConf1),
    receive {amqp10_event, {connection, Connection1, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    OpnConf2 = maps:update(container_id, <<"client-2">>, OpnConf),
    {ok, Connection2} = amqp10_client:open_connection(OpnConf2),
    receive {amqp10_event, {connection, Connection2, opened}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Thanks to routing via headers exchange on event property
    %% x-opt-container-id = client-2
    %% we should only receive the second connection.created event.
    ok = amqp10_client:flow_link_credit(Receiver, 2, never, true),
    receive {amqp10_msg, Receiver, Msg} ->
                ?assertMatch(#{<<"x-routing-key">> := <<"connection.created">>,
                               <<"x-opt-container-id">> := <<"client-2">>},
                             amqp10_msg:message_annotations(Msg))
    after 5000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_event, {link, Receiver, credit_exhausted}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:close_connection(Connection1),
    ok = amqp10_client:close_connection(Connection2),
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

declare_event_queue(Config, RoutingKey) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = <<"amq.rabbitmq.event">>,
                                        routing_key = RoutingKey}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true},
                           self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    Ch.

user_key(Config) ->
    case group_name(Config) of
        amqp_0_9_1 ->
            <<"user_who_performed_action">>;
        amqp_1_0 ->
            <<"x-opt-user-who-performed-action">>
    end.

group_name(Config) ->
    GroupProps = proplists:get_value(tc_group_properties, Config),
    proplists:get_value(name, GroupProps).

receive_user_in_event(Event, Config) ->
    User = proplists:get_value(rmq_username, Config),
    receive_user_in_event(Event, User, Config).

receive_user_in_event(Event, User, Config) ->
    Key = user_key(Config),
    Value = {longstr, User},
    receive_event(Event, Key, Value).

receive_event(Event, Key, Value) ->
    receive
        {#'basic.deliver'{routing_key = RoutingKey},
         #amqp_msg{props = #'P_basic'{headers = Headers}}} ->
            ?assertEqual(Event, RoutingKey),
            ?assertEqual(Value, rabbit_misc:table_lookup(Headers, Key)),
            Headers
    after
        10_000 ->
            throw({receive_event_timeout, Event, Key, Value})
    end.

receive_event(Event) ->
    receive
        {#'basic.deliver'{routing_key = RoutingKey},
         #amqp_msg{props = #'P_basic'{}}} ->
            ?assertEqual(Event, RoutingKey)
    after
        10_000 ->
            throw({receive_event_timeout, Event})
    end.

amqp_init(Config) ->
    OpnConf = amqp_connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {Connection, Session, LinkPair}.

amqp_connection_config(Config) ->
    Host = proplists:get_value(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>}}.
