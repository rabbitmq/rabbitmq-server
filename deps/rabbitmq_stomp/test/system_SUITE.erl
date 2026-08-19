%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(system_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

-define(QUEUE, <<"TestQueue">>).
-define(QUEUE_QQ, <<"TestQueueQQ">>).
-define(DESTINATION, <<"/amq/queue/TestQueue">>).
-define(DESTINATION_QQ, <<"/amq/queue/TestQueueQQ">>).
-define(AUTHZ_TOPIC, <<"TestUnsubscribeAuthzTopic">>).
-define(AUTHZ_TOPIC_DESTINATION, <<"/topic/TestUnsubscribeAuthzTopic">>).
-define(AUTHZ_SUBSCRIPTION_ID, <<"authz-subscription">>).
%% Stands in for another connection's durable subscription queue: it matches
%% the same configure pattern, so a configure check alone would not protect it.
-define(AUTHZ_BYSTANDER_QUEUE, <<"stomp-subscription-bystander">>).
-define(AUTHZ_USER, <<"stomp-authz-user">>).
-define(AUTHZ_PASSWORD, <<"pass">>).
%% Least privilege for a durable topic subscriber: its own subscription queues.
-define(AUTHZ_CONFIGURE, <<"^stomp-subscription-.*">>).
-define(UNSUBSCRIBE_QUEUE, <<"TestUnsubscribeQueue">>).
-define(UNSUBSCRIBE_QUEUE_QQ, <<"TestUnsubscribeQueueQQ">>).
-define(UNSUBSCRIBE_STREAM, <<"TestUnsubscribeStream">>).
-define(MULTIACK_QUEUE_A, <<"TestMultiAckQueueA">>).
-define(MULTIACK_QUEUE_B, <<"TestMultiAckQueueB">>).
-define(UNSUBSCRIBE_DESTINATION, <<"/amq/queue/TestUnsubscribeQueue">>).
-define(UNSUBSCRIBE_DESTINATION_QQ, <<"/amq/queue/TestUnsubscribeQueueQQ">>).
-define(UNSUBSCRIBE_STREAM_DESTINATION, <<"/amq/queue/TestUnsubscribeStream">>).
-define(MULTIACK_DESTINATION_A, <<"/amq/queue/TestMultiAckQueueA">>).
-define(MULTIACK_DESTINATION_B, <<"/amq/queue/TestMultiAckQueueB">>).

all() ->
    [{group, version_to_group_name(V)} || V <- ?SUPPORTED_VERSIONS] ++
    [{group, unsubscribe_authz}].

groups() ->
    Tests = [
        publish_no_dest_error,
        publish_unauthorized_error,
        declare_with_authorised_dlx,
        declare_with_restricted_dlx,
        declare_without_dlx,
        subscribe_error,
        subscribe,
        subscribe_with_x_priority,
        unsubscribe_ack,
        unsubscribe_multiack_prune,
        multiack_prune_is_connection_wide,
        unsubscribe_transaction_ack,
        unsubscribe_transaction_multiack_prune,
        transaction_ack_then_nack_same_delivery,
        unsubscribe_nack_individual,
        unsubscribe_nack_discard,
        unsubscribe_ack_stream,
        subscribe_ack,
        ack_auto_delivery_errors,
        reused_subscription_id_keeps_ack_mode,
        send,
        delete_queue_subscribe,
        temp_destination_queue,
        temp_destination_in_send,
        blank_destination_in_send,
        stream_filtering,
        transaction_limit,
        global_counters
    ],

    AuthzTests = [durable_unsubscribe_ignores_frame_queue_name,
                  durable_unsubscribe_requires_configure_permission],

    [{version_to_group_name(V), [sequence], Tests}
     || V <- ?SUPPORTED_VERSIONS] ++
    [{unsubscribe_authz, [], AuthzTests}].

version_to_group_name(V) ->
    list_to_atom(re:replace("version_" ++ V,
                            "\\.",
                            "_",
                            [global, {return, list}])).

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(unsubscribe_authz, Config) ->
    rabbit_ct_helpers:set_config(
      Config, [{version, lists:last(?SUPPORTED_VERSIONS)}]);
init_per_group(Group, Config) ->
    Suffix = string:sub_string(atom_to_list(Group), 9),
    Version = re:replace(Suffix, "_", ".", [global, {return, list}]),
    rabbit_ct_helpers:set_config(Config, [{version, Version}]).

end_per_group(_Group, Config) -> Config.

init_per_testcase(TestCase, Config) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{
        node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)
    }),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Client} = rabbit_stomp_client:connect(Version, StompPort),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {amqp_connection, Connection},
        {amqp_channel, Channel},
        {stomp_client, Client}
      ]),
    init_per_testcase0(TestCase, Config1).

end_per_testcase(TestCase, Config) ->
    cleanup_per_testcase0(TestCase, Config),
    Connection = ?config(amqp_connection, Config),
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:disconnect(Client),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    end_per_testcase0(TestCase, Config).

init_per_testcase0(publish_unauthorized_error, Config) ->
    Channel = ?config(amqp_channel, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = <<"RestrictedQueue">>,
                                                    durable     = true,
                                                    auto_delete = true}),

    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, add_user,
                                 [<<"user">>, <<"pass">>, <<"acting-user">>]),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, set_permissions, [
        <<"user">>, <<"/">>, <<"nothing">>, <<"nothing">>, <<"nothing">>, <<"acting-user">>]),
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, ClientFoo} = rabbit_stomp_client:connect(Version, "user", "pass", StompPort),
    rabbit_ct_helpers:set_config(Config, [{client_foo, ClientFoo}]);
init_per_testcase0(TestCase, Config)
  when TestCase =:= declare_with_authorised_dlx;
       TestCase =:= declare_with_restricted_dlx;
       TestCase =:= declare_without_dlx ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, add_user,
                                 [<<"stompuser">>, <<"pass">>, <<"acting-user">>]),
    %% configure, write and read confined to the stomp.* namespace
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, set_permissions,
                                 [<<"stompuser">>, <<"/">>,
                                  <<"^stomp\\.">>, <<"^stomp\\.">>, <<"^stomp\\.">>,
                                  <<"acting-user">>]),
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, ClientFoo} = rabbit_stomp_client:connect(Version, "stompuser", "pass", StompPort),
    rabbit_ct_helpers:set_config(Config, [{client_foo, ClientFoo}]);
init_per_testcase0(TestCase, Config)
  when TestCase =:= durable_unsubscribe_ignores_frame_queue_name;
       TestCase =:= durable_unsubscribe_requires_configure_permission ->
    Channel = ?config(amqp_channel, Config),
    %% a queue this connection never subscribes to
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel,
                          #'queue.declare'{queue       = ?AUTHZ_BYSTANDER_QUEUE,
                                           durable     = true,
                                           auto_delete = false}),
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_auth_backend_internal, add_user,
      [?AUTHZ_USER, ?AUTHZ_PASSWORD, <<"acting-user">>]),
    ok = rabbit_ct_broker_helpers:set_permissions(
           Config, ?AUTHZ_USER, ?config(rmq_vhost, Config),
           ?AUTHZ_CONFIGURE, <<".*">>, <<".*">>),
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(
                  Config, 0, tcp_port_stomp),
    {ok, AuthzClient} = rabbit_stomp_client:connect(
                          Version,
                          binary_to_list(?AUTHZ_USER),
                          binary_to_list(?AUTHZ_PASSWORD),
                          StompPort),
    rabbit_ct_helpers:set_config(
      Config, [{authz_client, AuthzClient},
               {authz_sub_queue, authz_subscription_queue(Config)}]);
init_per_testcase0(_, Config) ->
    Config.

end_per_testcase0(TestCase, Config)
  when TestCase =:= durable_unsubscribe_ignores_frame_queue_name;
       TestCase =:= durable_unsubscribe_requires_configure_permission ->
    AuthzClient = ?config(authz_client, Config),
    catch rabbit_stomp_client:disconnect(AuthzClient),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_auth_backend_internal, delete_user,
           [?AUTHZ_USER, <<"acting-user">>]),
    _ = [delete_queue_if_present(QRes, Config)
         || QRes <- [authz_bystander_queue(Config),
                     ?config(authz_sub_queue, Config)]],
    Config;
end_per_testcase0(publish_unauthorized_error, Config) ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:disconnect(ClientFoo),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, delete_user,
                                 [<<"user">>, <<"acting-user">>]),
    Config;
end_per_testcase0(TestCase, Config)
  when TestCase =:= declare_with_authorised_dlx;
       TestCase =:= declare_with_restricted_dlx;
       TestCase =:= declare_without_dlx ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:disconnect(ClientFoo),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, delete_user,
                                 [<<"stompuser">>, <<"acting-user">>]),
    Config;
end_per_testcase0(_, Config) ->
    Config.

cleanup_per_testcase0(_, Config) ->
    _ = [delete_test_queue(Q, Config)
         || Q <- [?UNSUBSCRIBE_QUEUE, ?UNSUBSCRIBE_QUEUE_QQ, ?UNSUBSCRIBE_STREAM,
                  ?MULTIACK_QUEUE_A, ?MULTIACK_QUEUE_B]],
    Config.

delete_test_queue(Queue, Config) ->
    VHost = ?config(rmq_vhost, Config),
    QName = rabbit_misc:r(VHost, queue, Queue),
    case rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_amqqueue, lookup, [QName]) of
        {ok, _} ->
            Connection = ?config(amqp_connection, Config),
            case amqp_connection:open_channel(Connection) of
                {ok, Channel} ->
                    _ = catch amqp_channel:call(
                                Channel, #'queue.delete'{queue = Queue}),
                    _ = catch amqp_channel:close(Channel),
                    Config;
                _ ->
                    Config
            end;
        _ ->
            Config
    end.
transaction_limit(Config) ->
    Client = ?config(stomp_client, Config),
    %% Open 16 transactions (the limit)
    lists:foreach(fun(I) ->
        TxId = integer_to_binary(I),
        rabbit_stomp_client:send(Client, 'BEGIN',
            [{<<"transaction">>, TxId}])
    end, lists:seq(1, 16)),

    %% The 17th should fail
    rabbit_stomp_client:send(Client, 'BEGIN',
        [{<<"transaction">>, <<"17">>}]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, 'ERROR'),
    <<"Transaction limit exceeded">> = maps:get(<<"message">>, Hdrs),
    ok.

global_counters(Config) ->
    Version = ?config(version, Config),
    ProtoVer = stomp_proto_ver(Version),
    Dest = iolist_to_binary(["/topic/counter-test-", Version]),

    C0 = get_global_counters(Config, ProtoVer),
    Pubs0 = maps:get(publishers, C0, 0),
    Cons0 = maps:get(consumers, C0, 0),
    Recv0 = maps:get(messages_received_total, C0, 0),
    Routed0 = maps:get(messages_routed_total, C0, 0),

    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE',
      [{<<"destination">>, Dest}, {<<"id">>, <<"counter-sub">>}]),

    rabbit_stomp_client:send(
      Client, 'SEND', [{<<"destination">>, Dest}], ["hello"]),

    {ok, Client1, _Hdrs, _Body} = stomp_receive(Client, 'MESSAGE'),

    C1 = get_global_counters(Config, ProtoVer),
    ?assertEqual(Pubs0 + 1, maps:get(publishers, C1)),
    ?assertEqual(Cons0 + 1, maps:get(consumers, C1)),
    ?assertEqual(Recv0 + 1, maps:get(messages_received_total, C1)),
    ?assertEqual(Routed0 + 1, maps:get(messages_routed_total, C1)),

    rabbit_stomp_client:send(
      Client1, 'UNSUBSCRIBE', [{<<"id">>, <<"counter-sub">>}]),

    rabbit_ct_helpers:await_condition(
      fun() -> maps:get(consumers, get_global_counters(Config, ProtoVer)) =:= Cons0 end,
      5_000),

    ok.

get_global_counters(Config, ProtoVer) ->
    maps:get(#{protocol => ProtoVer},
             rabbit_ct_broker_helpers:rpc(
               Config, 0, rabbit_global_counters, overview, [])).

stomp_proto_ver("1.0") -> 'STOMP 1.0';
stomp_proto_ver("1.1") -> 'STOMP 1.1';
stomp_proto_ver("1.2") -> 'STOMP 1.2'.

publish_no_dest_error(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send(
      Client, 'SEND', [{<<"destination">>, <<"/exchange/non-existent">>}], ["hello"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, 'ERROR'),
    <<"not_found">> = maps:get(<<"message">>, Hdrs),
    ok.

publish_unauthorized_error(Config) ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:send(
      ClientFoo, 'SEND', [{<<"destination">>, <<"/amq/queue/RestrictedQueue">>}], ["hello"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(ClientFoo, 'ERROR'),
    <<"access_refused">> = maps:get(<<"message">>, Hdrs),
    ok.

declare_with_authorised_dlx(Config) ->
    ClientFoo = ?config(client_foo, Config),
    subscribe_with_dlx(ClientFoo, <<"/queue/stomp.authorised">>, <<"stomp.target">>),
    {ok, _Client1, _Hdrs, _} = stomp_receive(ClientFoo, 'RECEIPT'),
    ok.

declare_with_restricted_dlx(Config) ->
    ClientFoo = ?config(client_foo, Config),
    subscribe_with_dlx(ClientFoo, <<"/queue/stomp.restricted">>, <<"restricted.x">>),
    {ok, _Client1, Hdrs, _} = stomp_receive(ClientFoo, 'ERROR'),
    <<"access_refused">> = maps:get(<<"message">>, Hdrs),
    ok.

declare_without_dlx(Config) ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:send(
      ClientFoo, 'SUBSCRIBE',
      [{<<"destination">>, <<"/queue/stomp.plain">>},
       {<<"id">>, <<"s0">>},
       {<<"receipt">>, <<"r0">>}]),
    {ok, _Client1, _Hdrs, _} = stomp_receive(ClientFoo, 'RECEIPT'),
    ok.

subscribe_with_dlx(Client, Destination, DLX) ->
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE',
      [{<<"destination">>, Destination},
       {<<"id">>, <<"s0">>},
       {<<"receipt">>, <<"r0">>},
       {<<"x-dead-letter-exchange">>, DLX}]).

subscribe_error(Config) ->
    Client = ?config(stomp_client, Config),
    %% SUBSCRIBE to missing queue
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION}]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, 'ERROR'),
    <<"not_found">> = maps:get(<<"message">>, Hdrs),
    ok.

subscribe(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION}, {<<"receipt">>, <<"foo">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, _Client2, _, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),
    ok.

subscribe_with_x_priority(Config) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    Channel = ?config(amqp_channel, Config),
    ClientA = ?config(stomp_client, Config),
    #'queue.declare_ok'{} =
    amqp_channel:call(Channel, #'queue.declare'{queue     = ?QUEUE_QQ,
                                                durable   = true,
                                                arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                                             {<<"x-single-active-consumer">>, bool, true}
                                                            ]}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      ClientA, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION_QQ}, {<<"receipt">>, <<"foo">>}]),
    {ok, _ClientA1, _, _} = stomp_receive(ClientA, 'RECEIPT'),

    %% subscribe with a higher priority and wait for receipt
    {ok, ClientB} = rabbit_stomp_client:connect(Version, StompPort),
    rabbit_stomp_client:send(
      ClientB, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION_QQ},
                              {<<"receipt">>, <<"foo">>},
                              {<<"x-priority">>, <<"10">>}
                             ]),
    {ok, ClientB1, _, _} = stomp_receive(ClientB, 'RECEIPT'),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE_QQ},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    %% ClientB should receive the message since it has a higher priority
    {ok, _ClientB2, _, [<<"hello">>]} = stomp_receive(ClientB1, 'MESSAGE'),
    #'queue.delete_ok'{} =
        amqp_channel:call(Channel, #'queue.delete'{queue = ?QUEUE_QQ}),
    ok.

unsubscribe_ack(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?UNSUBSCRIBE_QUEUE,
                                    durable = true}),
    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client">>},
                            {<<"prefetch-count">>, <<"1">>},
                            {<<"id">>, <<"subscription-id">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>,
                              routing_key = ?UNSUBSCRIBE_QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, Client2, Hdrs1, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"goodbye">>}),

    rabbit_stomp_client:send(
      Client2, 'UNSUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                               {<<"id">>, <<"subscription-id">>},
                               {<<"receipt">>, <<"rcpt2">>}]),

    {ok, Client3, _, _} = stomp_receive(Client2, 'RECEIPT'),

    rabbit_stomp_client:send(
      Client3, 'ACK', [{rabbit_stomp_util:ack_header_name(Version),
                        maps:get(
                          rabbit_stomp_util:msg_header_name(Version), Hdrs1)},
                       {<<"receipt">>, <<"rcpt3">>}]),

    {ok, _Client4, _, _} = stomp_receive(Client3, 'RECEIPT'),
    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE, 1, 0, 0),
    ok.

unsubscribe_multiack_prune(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?UNSUBSCRIBE_QUEUE,
                                    durable = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client">>},
                            {<<"prefetch-count">>, <<"5">>},
                            {<<"id">>, <<"subscription-id">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    Method = #'basic.publish'{exchange = <<"">>,
                              routing_key = ?UNSUBSCRIBE_QUEUE},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"one">>}),
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"two">>}),
    {ok, Client2, Headers1, [<<"one">>]} = stomp_receive(Client1, 'MESSAGE'),
    {ok, Client3, Headers2, [<<"two">>]} = stomp_receive(Client2, 'MESSAGE'),
    MessageHeader = rabbit_stomp_util:msg_header_name(Version),
    AckHeader = rabbit_stomp_util:ack_header_name(Version),
    Ack1 = {AckHeader, maps:get(MessageHeader, Headers1)},
    Ack2 = {AckHeader, maps:get(MessageHeader, Headers2)},

    rabbit_stomp_client:send(
      Client3, 'UNSUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                               {<<"id">>, <<"subscription-id">>},
                               {<<"receipt">>, <<"rcpt2">>}]),
    {ok, Client4, _, _} = stomp_receive(Client3, 'RECEIPT'),

    %% A multiple-at-once ack of the first delivery leaves the second one pending
    rabbit_stomp_client:send(Client4, 'ACK', [Ack1, {<<"receipt">>, <<"rcpt3">>}]),
    {ok, Client5, _, _} = stomp_receive(Client4, 'RECEIPT'),
    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE, 0, 1, 0),
    rabbit_stomp_client:send(Client5, 'ACK', [Ack2, {<<"receipt">>, <<"rcpt4">>}]),
    {ok, Client6, _, _} = stomp_receive(Client5, 'RECEIPT'),
    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE, 0, 0, 0),
    rabbit_stomp_client:send(Client6, 'ACK', [Ack2]),
    {ok, _Client7, ErrorHeaders, _} = stomp_receive(Client6, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),
    ok.

unsubscribe_transaction_ack(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    %% Exercise the same transition against a quorum queue.
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue     = ?UNSUBSCRIBE_QUEUE_QQ,
                                                    durable   = true,
                                                    arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION_QQ},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client">>},
                            {<<"prefetch-count">>, <<"1">>},
                            {<<"id">>, <<"subscription-id">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    Method = #'basic.publish'{exchange = <<"">>,
                              routing_key = ?UNSUBSCRIBE_QUEUE_QQ},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),
    {ok, Client2, Hdrs1, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),
    AckHeader = {rabbit_stomp_util:ack_header_name(Version),
                 maps:get(
                   rabbit_stomp_util:msg_header_name(Version), Hdrs1)},

    rabbit_stomp_client:send(
      Client2, 'UNSUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION_QQ},
                               {<<"id">>, <<"subscription-id">>},
                               {<<"receipt">>, <<"rcpt2">>}]),
    Client3 = stomp_receive_receipt(Client2, <<"rcpt2">>),

    rabbit_stomp_client:send(
      Client3, 'BEGIN', [{<<"transaction">>, <<"abort-me">>}, {<<"receipt">>, <<"rcpt3">>}]),
    Client4 = stomp_receive_receipt(Client3, <<"rcpt3">>),
    rabbit_stomp_client:send(
      Client4, 'ACK', [AckHeader,
                       {<<"transaction">>, <<"abort-me">>},
                       {<<"receipt">>, <<"rcpt4">>}]),
    Client5 = stomp_receive_receipt(Client4, <<"rcpt4">>),
    rabbit_stomp_client:send(
      Client5, 'ABORT', [{<<"transaction">>, <<"abort-me">>}, {<<"receipt">>, <<"rcpt5">>}]),
    Client6 = stomp_receive_receipt(Client5, <<"rcpt5">>),

    %% ABORT leaves the message held by the canceled consumer.
    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE_QQ, 0, 1, 1, 30_000),
    rabbit_stomp_client:send(
      Client6, 'BEGIN', [{<<"transaction">>, <<"commit-me">>}, {<<"receipt">>, <<"rcpt6">>}]),
    Client7 = stomp_receive_receipt(Client6, <<"rcpt6">>),
    rabbit_stomp_client:send(
      Client7, 'ACK', [AckHeader,
                       {<<"transaction">>, <<"commit-me">>},
                       {<<"receipt">>, <<"rcpt7">>}]),
    Client8 = stomp_receive_receipt(Client7, <<"rcpt7">>),
    rabbit_stomp_client:send(
      Client8, 'COMMIT', [{<<"transaction">>, <<"commit-me">>}, {<<"receipt">>, <<"rcpt8">>}]),
    _Client9 = stomp_receive_receipt(Client8, <<"rcpt8">>),

    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE_QQ, 0, 0, 0, 30_000),
    ok.

unsubscribe_transaction_multiack_prune(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?UNSUBSCRIBE_QUEUE,
                                    durable = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client">>},
                            {<<"prefetch-count">>, <<"2">>},
                            {<<"id">>, <<"subscription-id">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    Method = #'basic.publish'{exchange = <<"">>,
                              routing_key = ?UNSUBSCRIBE_QUEUE},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"one">>}),
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"two">>}),
    {ok, Client2, Headers1, [<<"one">>]} = stomp_receive(Client1, 'MESSAGE'),
    {ok, Client3, Headers2, [<<"two">>]} = stomp_receive(Client2, 'MESSAGE'),
    MessageHeader = rabbit_stomp_util:msg_header_name(Version),
    AckHeader = rabbit_stomp_util:ack_header_name(Version),
    Ack1 = {AckHeader, maps:get(MessageHeader, Headers1)},
    Ack2 = {AckHeader, maps:get(MessageHeader, Headers2)},

    rabbit_stomp_client:send(
      Client3, 'UNSUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                               {<<"id">>, <<"subscription-id">>},
                               {<<"receipt">>, <<"rcpt2">>}]),
    Client4 = stomp_receive_receipt(Client3, <<"rcpt2">>),
    rabbit_stomp_client:send(
      Client4, 'BEGIN', [{<<"transaction">>, <<"tx">>}, {<<"receipt">>, <<"rcpt3">>}]),
    Client5 = stomp_receive_receipt(Client4, <<"rcpt3">>),
    rabbit_stomp_client:send(
      Client5, 'ACK', [Ack2, {<<"transaction">>, <<"tx">>},
                       {<<"receipt">>, <<"rcpt4">>}]),
    Client6 = stomp_receive_receipt(Client5, <<"rcpt4">>),
    rabbit_stomp_client:send(
      Client6, 'ACK', [Ack1, {<<"transaction">>, <<"tx">>},
                       {<<"receipt">>, <<"rcpt5">>}]),
    Client7 = stomp_receive_receipt(Client6, <<"rcpt5">>),
    rabbit_stomp_client:send(
      Client7, 'COMMIT', [{<<"transaction">>, <<"tx">>}, {<<"receipt">>, <<"rcpt6">>}]),
    Client8 = stomp_receive_receipt(Client7, <<"rcpt6">>),
    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE, 0, 0, 0),

    rabbit_stomp_client:send(
      Client8, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                             {<<"receipt">>, <<"rcpt7">>},
                             {<<"id">>, <<"roundtrip">>}]),
    Client9 = stomp_receive_receipt(Client8, <<"rcpt7">>),

    %% An ack op is redundant only when an earlier action in the same
    %% commit removed it.
    rabbit_stomp_client:send(
      Client9, 'BEGIN', [{<<"transaction">>, <<"tx2">>},
                         {<<"receipt">>, <<"rcpt8">>}]),
    Client10 = stomp_receive_receipt(Client9, <<"rcpt8">>),
    rabbit_stomp_client:send(
      Client10, 'ACK', [Ack1, {<<"transaction">>, <<"tx2">>},
                        {<<"receipt">>, <<"rcpt9">>}]),
    Client11 = stomp_receive_receipt(Client10, <<"rcpt9">>),
    rabbit_stomp_client:send(
      Client11, 'COMMIT', [{<<"transaction">>, <<"tx2">>}]),
    {ok, _Client12, ErrorHeaders, _} = stomp_receive(Client11, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),
    ok.

transaction_ack_then_nack_same_delivery(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?UNSUBSCRIBE_QUEUE,
                                    durable = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client-individual">>},
                            {<<"id">>, <<"subscription-id">>}]),
    Client1 = stomp_receive_receipt(Client, <<"rcpt1">>),

    Method = #'basic.publish'{exchange = <<"">>,
                              routing_key = ?UNSUBSCRIBE_QUEUE},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),
    {ok, Client2, Headers, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),
    AckHeader = {rabbit_stomp_util:ack_header_name(Version),
                 maps:get(
                   rabbit_stomp_util:msg_header_name(Version), Headers)},

    rabbit_stomp_client:send(
      Client2, 'BEGIN', [{<<"transaction">>, <<"tx">>},
                         {<<"receipt">>, <<"rcpt2">>}]),
    Client3 = stomp_receive_receipt(Client2, <<"rcpt2">>),
    rabbit_stomp_client:send(
      Client3, 'ACK', [AckHeader, {<<"transaction">>, <<"tx">>},
                       {<<"receipt">>, <<"rcpt3">>}]),
    Client4 = stomp_receive_receipt(Client3, <<"rcpt3">>),
    rabbit_stomp_client:send(
      Client4, 'NACK', [AckHeader, {<<"transaction">>, <<"tx">>},
                        {<<"requeue">>, <<"true">>},
                        {<<"receipt">>, <<"rcpt4">>}]),
    Client5 = stomp_receive_receipt(Client4, <<"rcpt4">>),
    rabbit_stomp_client:send(
      Client5, 'COMMIT', [{<<"transaction">>, <<"tx">>}]),

    %% The ACK settled the delivery, so the NACK has nothing left to reject.
    {ok, Client6, ErrorHeaders, _} = stomp_receive(Client5, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),
    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE, 0, 0, 1),

    rabbit_stomp_client:send(
      Client6, 'SEND', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                        {<<"receipt">>, <<"rcpt5">>}], ["roundtrip"]),
    _Client7 = stomp_receive_receipt(Client6, <<"rcpt5">>),
    ok.

unsubscribe_nack_individual(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?UNSUBSCRIBE_QUEUE,
                                    durable = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client-individual">>},
                            {<<"prefetch-count">>, <<"1">>},
                            {<<"id">>, <<"subscription-id">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    Method = #'basic.publish'{exchange = <<"">>,
                              routing_key = ?UNSUBSCRIBE_QUEUE},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),
    {ok, Client2, Headers, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),
    NackHeader = {rabbit_stomp_util:ack_header_name(Version),
                  maps:get(
                    rabbit_stomp_util:msg_header_name(Version), Headers)},

    rabbit_stomp_client:send(
      Client2, 'UNSUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                               {<<"id">>, <<"subscription-id">>},
                               {<<"receipt">>, <<"rcpt2">>}]),
    {ok, Client3, _, _} = stomp_receive(Client2, 'RECEIPT'),
    rabbit_stomp_client:send(
      Client3, 'NACK', [NackHeader,
                        {<<"requeue">>, <<"true">>},
                        {<<"receipt">>, <<"rcpt3">>}]),
    {ok, Client4, _, _} = stomp_receive(Client3, 'RECEIPT'),
    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE, 1, 0, 0),

    rabbit_stomp_client:send(Client4, 'NACK', [NackHeader]),
    {ok, _Client5, ErrorHeaders, _} = stomp_receive(Client4, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),
    ok.

unsubscribe_nack_discard(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?UNSUBSCRIBE_QUEUE,
                                    durable = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client-individual">>},
                            {<<"prefetch-count">>, <<"1">>},
                            {<<"id">>, <<"subscription-id">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    Method = #'basic.publish'{exchange = <<"">>,
                              routing_key = ?UNSUBSCRIBE_QUEUE},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),
    {ok, Client2, Headers, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),
    NackHeader = {rabbit_stomp_util:ack_header_name(Version),
                  maps:get(
                    rabbit_stomp_util:msg_header_name(Version), Headers)},

    rabbit_stomp_client:send(
      Client2, 'UNSUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                               {<<"id">>, <<"subscription-id">>},
                               {<<"receipt">>, <<"rcpt2">>}]),
    {ok, Client3, _, _} = stomp_receive(Client2, 'RECEIPT'),
    rabbit_stomp_client:send(
      Client3, 'NACK', [NackHeader,
                        {<<"requeue">>, <<"false">>},
                        {<<"receipt">>, <<"rcpt3">>}]),
    {ok, _Client4, _, _} = stomp_receive(Client3, 'RECEIPT'),
    ok = await_queue_state(Config, ?UNSUBSCRIBE_QUEUE, 0, 0, 0),
    ok.

unsubscribe_ack_stream(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel,
          #'queue.declare'{queue = ?UNSUBSCRIBE_STREAM,
                           durable = true,
                           arguments = [{<<"x-queue-type">>, longstr,
                                         <<"stream">>}]}),
    Method = #'basic.publish'{exchange = <<"">>,
                              routing_key = ?UNSUBSCRIBE_STREAM},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_STREAM_DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client">>},
                            {<<"prefetch-count">>, <<"1">>},
                            {<<"x-stream-offset">>, <<"first">>},
                            {<<"id">>, <<"subscription-id">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),
    {ok, Client2, Headers, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),
    AckHeader = {rabbit_stomp_util:ack_header_name(Version),
                 maps:get(
                   rabbit_stomp_util:msg_header_name(Version), Headers)},

    rabbit_stomp_client:send(
      Client2, 'UNSUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_STREAM_DESTINATION},
                               {<<"id">>, <<"subscription-id">>},
                               {<<"receipt">>, <<"rcpt2">>}]),
    {ok, Client3, _, _} = stomp_receive(Client2, 'RECEIPT'),
    %% A stream ack is accepted even though the consumer is gone.
    rabbit_stomp_client:send(
      Client3, 'ACK', [AckHeader, {<<"receipt">>, <<"rcpt3">>}]),
    {ok, Client4, _, _} = stomp_receive(Client3, 'RECEIPT'),
    rabbit_stomp_client:send(Client4, 'ACK', [AckHeader]),
    {ok, _Client5, ErrorHeaders, _} = stomp_receive(Client4, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),
    ok.

%% This multi-ack behavior is a known RabbitMQ deviation from the spec:
%% https://stomp.github.io/stomp-specification-1.2.html#SUBSCRIBE_ack_Header
%% introduced in `300dcee219`.
%%
%% It won't affect a significant majority of the users but these tests document the behavior for
%% the core team's own understanding.
multiack_prune_is_connection_wide(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?MULTIACK_QUEUE_A,
                                    durable = true}),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?MULTIACK_QUEUE_B,
                                    durable = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?MULTIACK_DESTINATION_B},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client-individual">>},
                            {<<"id">>, <<"sub-b">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),
    rabbit_stomp_client:send(
      Client1, 'SUBSCRIBE', [{<<"destination">>, ?MULTIACK_DESTINATION_A},
                             {<<"receipt">>, <<"rcpt2">>},
                             {<<"ack">>, <<"client">>},
                             {<<"id">>, <<"sub-a">>}]),
    {ok, Client2, _, _} = stomp_receive(Client1, 'RECEIPT'),

    PublishB = #'basic.publish'{exchange = <<"">>,
                                routing_key = ?MULTIACK_QUEUE_B},
    amqp_channel:call(Channel, PublishB, #amqp_msg{props = #'P_basic'{},
                                                   payload = <<"one">>}),
    {ok, Client3, HeadersB, [<<"one">>]} = stomp_receive(Client2, 'MESSAGE'),
    PublishA = #'basic.publish'{exchange = <<"">>,
                                routing_key = ?MULTIACK_QUEUE_A},
    amqp_channel:call(Channel, PublishA, #amqp_msg{props = #'P_basic'{},
                                                   payload = <<"two">>}),
    {ok, Client4, HeadersA, [<<"two">>]} = stomp_receive(Client3, 'MESSAGE'),
    MessageHeader = rabbit_stomp_util:msg_header_name(Version),
    AckHeader = rabbit_stomp_util:ack_header_name(Version),
    AckB = {AckHeader, maps:get(MessageHeader, HeadersB)},
    AckA = {AckHeader, maps:get(MessageHeader, HeadersA)},

    rabbit_stomp_client:send(Client4, 'ACK', [AckA, {<<"receipt">>, <<"rcpt3">>}]),
    {ok, Client5, _, _} = stomp_receive(Client4, 'RECEIPT'),
    ok = await_queue_state(Config, ?MULTIACK_QUEUE_B, 0, 0, 1),
    rabbit_stomp_client:send(Client5, 'ACK', [AckB]),
    {ok, _Client6, ErrorHeaders, _} = stomp_receive(Client5, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),
    ok.

%% An UNSUBSCRIBE frame of a topic subscription with a durable header set to `true`
%% must delete the queue recorded on the subscription,
%% not the one named in the frame's x-queue-name header.
durable_unsubscribe_ignores_frame_queue_name(Config) ->
    Client = ?config(authz_client, Config),
    Bystander = authz_bystander_queue(Config),
    SubQueue = ?config(authz_sub_queue, Config),
    Client1 = authz_subscribe(Client),
    ?assertMatch({ok, _}, lookup_queue(SubQueue, Config)),
    ?assertMatch({ok, _}, lookup_queue(Bystander, Config)),
    %% the ID alone identifies the subscription
    rabbit_stomp_client:send(
      Client1, 'UNSUBSCRIBE',
      [{<<"destination">>, ?AUTHZ_TOPIC_DESTINATION},
       {<<"id">>, ?AUTHZ_SUBSCRIPTION_ID},
       {<<"durable">>, <<"true">>},
       {<<"x-queue-name">>, ?AUTHZ_BYSTANDER_QUEUE},
       {<<"receipt">>, <<"rcpt2">>}]),
    {Frame, _Client2} = rabbit_stomp_client:recv(Client1),
    ?assertMatch(#stomp_frame{command = 'RECEIPT',
                              headers = #{<<"receipt-id">> := <<"rcpt2">>}},
                 Frame),

    ?assertMatch({ok, _}, lookup_queue(Bystander, Config)),
    ?awaitMatch({error, not_found}, lookup_queue(SubQueue, Config), 30_000),
    ok.

%% Deleting the subscription's queue requires configure permission on it, so a
%% permission revoked after the subscription was created must take effect.
durable_unsubscribe_requires_configure_permission(Config) ->
    Client = ?config(authz_client, Config),
    Bystander = authz_bystander_queue(Config),
    SubQueue = ?config(authz_sub_queue, Config),
    Client1 = authz_subscribe(Client),
    ?assertMatch({ok, _}, lookup_queue(SubQueue, Config)),
    ok = rabbit_ct_broker_helpers:set_permissions(
           Config, ?AUTHZ_USER, ?config(rmq_vhost, Config),
           <<"^$">>, <<".*">>, <<".*">>),
    rabbit_stomp_client:send(
      Client1, 'UNSUBSCRIBE',
      [{<<"destination">>, ?AUTHZ_TOPIC_DESTINATION},
       {<<"id">>, ?AUTHZ_SUBSCRIPTION_ID},
       {<<"durable">>, <<"true">>},
       {<<"x-queue-name">>, ?AUTHZ_BYSTANDER_QUEUE},
       {<<"receipt">>, <<"rcpt2">>}]),
    {Frame, _Client2} = rabbit_stomp_client:recv(Client1),
    ?assertMatch(#stomp_frame{command = 'ERROR',
                              headers = #{<<"message">> := <<"access_refused">>}},
                 Frame),
    ?assertMatch({ok, _}, lookup_queue(SubQueue, Config)),
    ?assertMatch({ok, _}, lookup_queue(Bystander, Config)),
    ok.

%% auto-delete:false, so the queue outlives its consumer.
authz_subscribe(Client) ->
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE',
      [{<<"destination">>, ?AUTHZ_TOPIC_DESTINATION},
       {<<"durable">>, <<"true">>},
       {<<"auto-delete">>, <<"false">>},
       {<<"id">>, ?AUTHZ_SUBSCRIPTION_ID},
       {<<"receipt">>, <<"rcpt1">>}]),
    {ok, Client1, ReceiptHeaders, _} = stomp_receive(Client, 'RECEIPT'),
    ?assertEqual(<<"rcpt1">>, maps:get(<<"receipt-id">>, ReceiptHeaders)),
    Client1.

%% The queue name SUBSCRIBE derives, computed the way the processor does.
authz_subscription_queue(Config) ->
    QNameBin = rabbit_ct_broker_helpers:rpc(
                 Config, 0, rabbit_stomp_util, subscription_queue_name,
                 [?AUTHZ_TOPIC, ?AUTHZ_SUBSCRIPTION_ID,
                  #stomp_frame{headers = #{}}]),
    rabbit_misc:r(?config(rmq_vhost, Config), queue, QNameBin).

authz_bystander_queue(Config) ->
    rabbit_misc:r(?config(rmq_vhost, Config), queue, ?AUTHZ_BYSTANDER_QUEUE).

lookup_queue(QRes, Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_amqqueue, lookup, [QRes]).

delete_queue_if_present(QRes, Config) ->
    case lookup_queue(QRes, Config) of
        {ok, _} ->
            rabbit_ct_broker_helpers:rpc(
              Config, 0, rabbit_amqqueue, delete_with,
              [QRes, false, false, <<"acting-user">>]);
        _ ->
            ok
    end.

subscribe_ack(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION},
                            {<<"receipt">>,     <<"foo">>},
                            {<<"ack">>,         <<"client">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, _Client2, Headers, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),
    false = (Version == "1.2") xor is_map_key(?HEADER_ACK, Headers),

    MsgHeader = rabbit_stomp_util:msg_header_name(Version),
    AckValue  = maps:get(MsgHeader, Headers),
    AckHeader = rabbit_stomp_util:ack_header_name(Version),

    rabbit_stomp_client:send(Client, 'ACK', [{AckHeader, AckValue}]),
    #'basic.get_empty'{} =
        amqp_channel:call(Channel, #'basic.get'{queue = ?QUEUE}),
    ok.

ack_auto_delivery_errors(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel,
          #'queue.declare'{queue       = ?QUEUE,
                           durable     = true,
                           auto_delete = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"auto">>},
                            {<<"id">>, <<"auto">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    Method = #'basic.publish'{exchange = <<>>, routing_key = ?QUEUE},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),
    {ok, Client2, Headers, [<<"hello">>]} =
        stomp_receive(Client1, 'MESSAGE'),
    MessageId = maps:get(?HEADER_MESSAGE_ID, Headers),
    rabbit_stomp_client:send(
      Client2, 'ACK',
      [{rabbit_stomp_util:ack_header_name(Version), MessageId}]),
    {ok, Client3, ErrorHeaders, _} = stomp_receive(Client2, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),

    rabbit_stomp_client:send(
      Client3, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION},
                              {<<"receipt">>, <<"rcpt2">>},
                              {<<"id">>, <<"roundtrip">>}]),
    {ok, _Client4, _, _} = stomp_receive(Client3, 'RECEIPT'),
    ok.

reused_subscription_id_keeps_ack_mode(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?MULTIACK_QUEUE_A,
                                    durable = true}),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?MULTIACK_QUEUE_B,
                                    durable = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?MULTIACK_DESTINATION_B},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client-individual">>},
                            {<<"id">>, <<"sub-b">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),
    rabbit_stomp_client:send(
      Client1, 'SUBSCRIBE', [{<<"destination">>, ?MULTIACK_DESTINATION_A},
                             {<<"receipt">>, <<"rcpt2">>},
                             {<<"ack">>, <<"client-individual">>},
                             {<<"id">>, <<"reused">>}]),
    {ok, Client2, _, _} = stomp_receive(Client1, 'RECEIPT'),

    PublishB = #'basic.publish'{exchange = <<>>,
                                routing_key = ?MULTIACK_QUEUE_B},
    amqp_channel:call(Channel, PublishB,
                      #amqp_msg{props = #'P_basic'{}, payload = <<"one">>}),
    {ok, Client3, HeadersB, [<<"one">>]} =
        stomp_receive(Client2, 'MESSAGE'),
    PublishA = #'basic.publish'{exchange = <<>>,
                                routing_key = ?MULTIACK_QUEUE_A},
    amqp_channel:call(Channel, PublishA,
                      #amqp_msg{props = #'P_basic'{}, payload = <<"two">>}),
    {ok, Client4, HeadersA, [<<"two">>]} =
        stomp_receive(Client3, 'MESSAGE'),
    MessageHeader = rabbit_stomp_util:msg_header_name(Version),
    AckHeader = rabbit_stomp_util:ack_header_name(Version),
    AckValueB = maps:get(MessageHeader, HeadersB),
    AckValueA = maps:get(MessageHeader, HeadersA),

    ForgedAckValue = ack_value_with_consumer(AckValueB, AckValueA),
    rabbit_stomp_client:send(
      Client4, 'ACK', [{AckHeader, ForgedAckValue}]),
    {ok, Client5, ErrorHeaders, _} = stomp_receive(Client4, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),

    rabbit_stomp_client:send(
      Client5, 'UNSUBSCRIBE', [{<<"destination">>, ?MULTIACK_DESTINATION_A},
                                {<<"id">>, <<"reused">>},
                                {<<"receipt">>, <<"rcpt3">>}]),
    {ok, Client6, _, _} = stomp_receive(Client5, 'RECEIPT'),
    rabbit_stomp_client:send(
      Client6, 'SUBSCRIBE', [{<<"destination">>, ?MULTIACK_DESTINATION_A},
                              {<<"receipt">>, <<"rcpt4">>},
                              {<<"ack">>, <<"client">>},
                              {<<"id">>, <<"reused">>}]),
    {ok, Client7, _, _} = stomp_receive(Client6, 'RECEIPT'),

    rabbit_stomp_client:send(
      Client7, 'ACK', [{AckHeader, AckValueA}, {<<"receipt">>, <<"rcpt5">>}]),
    {ok, Client8, _, _} = stomp_receive(Client7, 'RECEIPT'),
    ok = await_queue_state(Config, ?MULTIACK_QUEUE_A, 0, 0, 1),
    ok = await_queue_state(Config, ?MULTIACK_QUEUE_B, 0, 1, 1),
    rabbit_stomp_client:send(
      Client8, 'ACK', [{AckHeader, AckValueB}, {<<"receipt">>, <<"rcpt6">>}]),
    {ok, Client9, _, _} = stomp_receive(Client8, 'RECEIPT'),
    ok = await_queue_state(Config, ?MULTIACK_QUEUE_B, 0, 0, 1),

    amqp_channel:call(Channel, PublishA,
                      #amqp_msg{props = #'P_basic'{}, payload = <<"three">>}),
    {ok, Client10, HeadersA1, [<<"three">>]} =
        stomp_receive(Client9, 'MESSAGE'),
    amqp_channel:call(Channel, PublishA,
                      #amqp_msg{props = #'P_basic'{}, payload = <<"four">>}),
    {ok, Client11, HeadersA2, [<<"four">>]} =
        stomp_receive(Client10, 'MESSAGE'),
    AckValueA1 = maps:get(MessageHeader, HeadersA1),
    AckValueA2 = maps:get(MessageHeader, HeadersA2),
    rabbit_stomp_client:send(
      Client11, 'ACK', [{AckHeader, AckValueA2}, {<<"receipt">>, <<"rcpt7">>}]),
    {ok, Client12, _, _} = stomp_receive(Client11, 'RECEIPT'),
    ok = await_queue_state(Config, ?MULTIACK_QUEUE_A, 0, 0, 1),
    rabbit_stomp_client:send(
      Client12, 'ACK', [{AckHeader, AckValueA1}]),
    {ok, _Client13, ErrorHeaders2, _} = stomp_receive(Client12, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders2)),
    ok.

send(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION}, {<<"receipt">>, <<"foo">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    %% send from stomp
    rabbit_stomp_client:send(
      Client1, 'SEND', [{<<"destination">>, ?DESTINATION}], ["hello"]),

    {ok, _Client2, _, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),
    ok.

delete_queue_subscribe(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Channel, #'queue.declare'{queue   = ?UNSUBSCRIBE_QUEUE,
                                    durable = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION},
                            {<<"receipt">>, <<"bah">>},
                            {<<"ack">>, <<"client-individual">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},
    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),
    {ok, Client2, MessageHeaders, [<<"hello">>]} =
        stomp_receive(Client1, 'MESSAGE'),
    AckHeader =
        {rabbit_stomp_util:ack_header_name(Version),
         maps:get(
           rabbit_stomp_util:msg_header_name(Version), MessageHeaders)},

    %% delete queue while subscribed
    #'queue.delete_ok'{} =
        amqp_channel:call(Channel, #'queue.delete'{queue = ?QUEUE}),

    {ok, Client3, Headers, _} = stomp_receive(Client2, 'ERROR'),

    ?DESTINATION = maps:get(<<"subscription">>, Headers),

    %% RabbitMQ keeps the session open after this server-cancel ERROR so an
    %% in-flight delivery can still be settled.
    rabbit_stomp_client:send(
      Client3, 'ACK', [AckHeader, {<<"receipt">>, <<"rcpt2">>}]),
    {ok, Client4, _, _} = stomp_receive(Client3, 'RECEIPT'),

    %% A broker round trip proves the late settlement did not close the shared
    %% AMQP channel after the ACK receipt was sent.
    rabbit_stomp_client:send(
      Client4, 'SUBSCRIBE', [{<<"destination">>, ?UNSUBSCRIBE_DESTINATION},
                             {<<"receipt">>, <<"rcpt3">>},
                             {<<"id">>, <<"roundtrip">>}]),
    {ok, _Client5, _, _} = stomp_receive(Client4, 'RECEIPT'),
    ok.

temp_destination_queue(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),
    rabbit_stomp_client:send( Client, 'SEND', [{<<"destination">>, ?DESTINATION},
                                               {<<"reply-to">>, <<"/temp-queue/foo">>}],
                                              ["ping"]),
    amqp_channel:call(Channel,#'basic.consume'{queue  = ?QUEUE, no_ack = true}),
    receive #'basic.consume_ok'{consumer_tag = _Tag} -> ok end,
    ReplyTo = receive {#'basic.deliver'{delivery_tag = _DTag},
             #'amqp_msg'{payload = <<"ping">>,
                         props   = #'P_basic'{reply_to = RT}}} -> RT
    end,
    ok = amqp_channel:call(Channel,
                           #'basic.publish'{routing_key = ReplyTo},
                           #amqp_msg{payload = <<"pong">>}),
    {ok, Client1, Headers, [<<"pong">>]} =
        stomp_receive(Client, 'MESSAGE'),
    MessageId = maps:get(?HEADER_MESSAGE_ID, Headers),
    rabbit_stomp_client:send(
      Client1, 'ACK',
      [{rabbit_stomp_util:ack_header_name(Version), MessageId}]),
    {ok, Client2, ErrorHeaders, _} = stomp_receive(Client1, 'ERROR'),
    ?assertEqual(<<"Message not found">>,
                 maps:get(<<"message">>, ErrorHeaders)),
    rabbit_stomp_client:send(
      Client2, 'SEND', [{<<"destination">>, ?DESTINATION},
                         {<<"receipt">>, <<"roundtrip">>}], ["still alive"]),
    {ok, _Client3, _, _} = stomp_receive(Client2, 'RECEIPT'),
    ok.

temp_destination_in_send(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send( Client, 'SEND', [{<<"destination">>, <<"/temp-queue/foo">>}],
                                              ["poing"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, 'ERROR'),
    <<"Invalid destination">> = maps:get(<<"message">>, Hdrs),
    ok.

blank_destination_in_send(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send( Client, 'SEND', [{<<"destination">>, <<"">>}],
                                              ["poing"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, 'ERROR'),
    <<"Invalid destination">> = maps:get(<<"message">>, Hdrs),
    ok.

stream_filtering(Config) ->
    Version = ?config(version, Config),
    Client = ?config(stomp_client, Config),
    Stream = <<(atom_to_binary(?FUNCTION_NAME))/binary, $-,
               (integer_to_binary(rand:uniform(10000)))/binary>>,
    %% subscription just to create the stream from STOMP
    SubDestination = <<"/topic/stream-queue-test">>,
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE',
      [{<<"destination">>, SubDestination},
       {<<"receipt">>, <<"foo">>},
       {<<"x-queue-name">>, Stream},
       {<<"x-queue-type">>, <<"stream">>},
       {?HEADER_X_STREAM_FILTER_SIZE_BYTES, <<"32">>},
       {<<"durable">>, <<"true">>},
       {<<"auto-delete">>, <<"false">>},
       {<<"id">>, <<"1234">>},
       {<<"prefetch-count">>, <<"1">>},
       {<<"ack">>, <<"client">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),
    rabbit_stomp_client:send(
      Client1, 'UNSUBSCRIBE', [{<<"destination">>, SubDestination},
                               {<<"id">>, <<"1234">>},
                               {<<"receipt">>, <<"bar">>}]),
    {ok, Client2, _, _} = stomp_receive(Client1, 'RECEIPT'),

    %% we are going to publish several waves of messages with and without filter values.
    %% we will then create subscriptions with various filter options
    %% and make sure we receive only what we asked for and not all the messages.

    StreamDestination = <<"/amq/queue/", Stream/binary>>,
    %% logic to publish a wave of messages with or without a filter value
    WaveCount = 1000,
    Publish =
    fun(C, FilterValue) ->
            lists:foldl(fun(Seq, C0) ->
                                Headers0 = [{<<"destination">>, StreamDestination},
                                            {<<"receipt">>, integer_to_binary(Seq)}],
                                Headers = case FilterValue of
                                              undefined ->
                                                  Headers0;
                                              _ ->
                                                  [{<<"x-stream-filter-value">>, FilterValue}] ++ Headers0
                                          end,
                                rabbit_stomp_client:send(
                                  C0, 'SEND', Headers, ["hello"]),
                                {ok, C1, _, _} = stomp_receive(C0, 'RECEIPT'),
                                C1
                        end, C, lists:seq(1, WaveCount))
    end,
    %% publishing messages with the "apple" filter value
    Client3 = Publish(Client2, <<"apple">>),
    %% publishing messages with no filter value
    Client4 = Publish(Client3, undefined),
    %% publishing messages with the "orange" filter value
    Client5 = Publish(Client4, <<"orange">>),

    %% filtering on "apple"
    rabbit_stomp_client:send(
      Client5, 'SUBSCRIBE',
      [{<<"destination">>, StreamDestination},
       {<<"id">>, <<"0">>},
       {<<"ack">>, <<"client">>},
       {<<"prefetch-count">>, <<"1">>},
       {<<"x-stream-filter">>, <<"apple">>},
       {<<"x-stream-offset">>, <<"first">>}]),
    {Client6, AppleMessages} = stomp_receive_messages(Client5, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleMessages) < WaveCount * 3),
    %% client-side filtering
    AppleFilteredMessages =
    lists:filter(fun(H) ->
                         maps:get(<<"x-stream-filter-value">>, H, undefined) =:= <<"apple">>
                 end, AppleMessages),
    %% we should have only the "apple" messages
    ?assert(length(AppleFilteredMessages) =:= WaveCount),
    rabbit_stomp_client:send(
      Client6, 'UNSUBSCRIBE', [{<<"destination">>, StreamDestination},
                               {<<"id">>, <<"0">>},
                               {<<"receipt">>, <<"bar">>}]),
    {ok, Client7, _, _} = stomp_receive(Client6, 'RECEIPT'),

    %% filtering on "apple" and "orange"
    rabbit_stomp_client:send(
      Client7, 'SUBSCRIBE',
      [{<<"destination">>, StreamDestination},
       {<<"id">>, <<"0">>},
       {<<"ack">>, <<"client">>},
       {<<"prefetch-count">>, <<"1">>},
       {<<"x-stream-filter">>, <<"apple,orange">>},
       {<<"x-stream-offset">>, <<"first">>}]),
    {Client8, AppleOrangeMessages} = stomp_receive_messages(Client7, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleOrangeMessages) < WaveCount * 3),
    %% client-side filtering
    AppleOrangeFilteredMessages =
    lists:filter(fun(H) ->
                         maps:get(<<"x-stream-filter-value">>, H, undefined) =:= <<"apple">> orelse
                         maps:get(<<"x-stream-filter-value">>, H, undefined) =:= <<"orange">>
                 end, AppleOrangeMessages),
    %% we should have only the "apple" and "orange" messages
    ?assert(length(AppleOrangeFilteredMessages) =:= WaveCount * 2),
    rabbit_stomp_client:send(
      Client8, 'UNSUBSCRIBE', [{<<"destination">>, StreamDestination},
                                {<<"id">>, <<"0">>},
                                {<<"receipt">>, <<"bar">>}]),
    {ok, Client9, _, _} = stomp_receive(Client8, 'RECEIPT'),

    %% filtering on "apple" and messages without a filter value
    rabbit_stomp_client:send(
      Client9, 'SUBSCRIBE',
      [{<<"destination">>, StreamDestination},
       {<<"id">>, <<"0">>},
       {<<"ack">>, <<"client">>},
       {<<"prefetch-count">>, <<"1">>},
       {<<"x-stream-filter">>, <<"apple">>},
       {<<"x-stream-match-unfiltered">>, <<"true">>},
       {<<"x-stream-offset">>, <<"first">>}]),
    {Client10, AppleUnfilteredMessages} = stomp_receive_messages(Client9, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleUnfilteredMessages) < WaveCount * 3),
    %% client-side filtering
    AppleUnfilteredFilteredMessages =
    lists:filter(fun(H) ->
                         maps:get(<<"x-stream-filter-value">>, H, undefined) =:= <<"apple">> orelse
                         maps:get(<<"x-stream-filter-value">>, H, undefined) =:= undefined
                 end, AppleUnfilteredMessages),
    %% we should have only the "apple" messages and messages without a filter value
    ?assert(length(AppleUnfilteredFilteredMessages) =:= WaveCount * 2),
    rabbit_stomp_client:send(
      Client10, 'UNSUBSCRIBE', [{<<"destination">>, StreamDestination},
                               {<<"id">>, <<"0">>},
                               {<<"receipt">>, <<"bar">>}]),
    {ok, _, _, _} = stomp_receive(Client10, 'RECEIPT'),

    Channel = ?config(amqp_channel, Config),
    #'queue.delete_ok'{} = amqp_channel:call(Channel,
                                             #'queue.delete'{queue = Stream}),
    ok.

stomp_receive_messages(Client, Version) ->
    stomp_receive_messages(Client, [], Version).

stomp_receive_messages(Client, Acc, Version) ->
    try rabbit_stomp_client:recv(Client) of
        {#stomp_frame{command = 'MESSAGE',
                      headers = Headers}, Client1} ->
        MsgHeader = rabbit_stomp_util:msg_header_name(Version),
        AckValue  = maps:get(MsgHeader, Headers),
        AckHeader = rabbit_stomp_util:ack_header_name(Version),
        rabbit_stomp_client:send(Client1, 'ACK', [{AckHeader, AckValue}]),
        stomp_receive_messages(Client1, [Headers] ++ Acc, Version)
    catch
      error:{badmatch, {error, timeout}} ->
            {Client, Acc}
    end.

stomp_receive(Client, Command) ->
    {#stomp_frame{command     = Command,
                  headers     = Hdrs,
                  body_iolist_rev = Body},   Client1} =
    rabbit_stomp_client:recv(Client),
    {ok, Client1, Hdrs, Body}.

stomp_receive_receipt(Client, ReceiptId) ->
    {ok, Client1, Hdrs, _} = stomp_receive(Client, 'RECEIPT'),
    ?assertEqual(ReceiptId, maps:get(<<"receipt-id">>, Hdrs)),
    Client1.

ack_value_with_consumer(ConsumerAckValue, DeliveryAckValue) ->
    {ok, {ConsumerTag, _ConsumerSession, _ConsumerDeliveryTag}} =
        rabbit_stomp_util:parse_message_id(ConsumerAckValue),
    {ok, {_DeliveryConsumerTag, SessionId, DeliveryTag}} =
        rabbit_stomp_util:parse_message_id(DeliveryAckValue),
    iolist_to_binary(
      [ConsumerTag, ?MESSAGE_ID_SEPARATOR, SessionId,
       ?MESSAGE_ID_SEPARATOR, integer_to_list(DeliveryTag)]).

await_queue_state(Config, Queue, Ready, Unacknowledged, Consumers) ->
    await_queue_state(Config, Queue, Ready, Unacknowledged, Consumers, 5_000).

await_queue_state(Config, Queue, Ready, Unacknowledged, Consumers, Timeout) ->
    VHost = ?config(rmq_vhost, Config),
    QName = rabbit_misc:r(VHost, queue, Queue),
    rabbit_ct_helpers:await_condition(
      fun() ->
              case rabbit_ct_broker_helpers:rpc(
                     Config, 0, rabbit_amqqueue, lookup, [QName]) of
                  {ok, Q} ->
                      Info = rabbit_ct_broker_helpers:rpc(
                               Config, 0, rabbit_amqqueue, info,
                               [Q, [messages_ready,
                                    messages_unacknowledged,
                                    consumers]]),
                      lists:sort(Info) =:=
                          lists:sort([{messages_ready, Ready},
                                      {messages_unacknowledged, Unacknowledged},
                                      {consumers, Consumers}]);
                  _ ->
                      false
              end
      end, Timeout).
