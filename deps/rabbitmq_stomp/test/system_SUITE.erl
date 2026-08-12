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

all() ->
    [{group, version_to_group_name(V)} || V <- ?SUPPORTED_VERSIONS].

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
        subscribe_ack,
        send,
        delete_queue_subscribe,
        temp_destination_queue,
        temp_destination_in_send,
        blank_destination_in_send,
        stream_filtering,
        transaction_limit,
        global_counters
    ],

    %% Not a `sequence` group: one failing case must not auto_skip the other.
    AuthzTests = [durable_unsubscribe_ignores_frame_queue_name,
                  durable_unsubscribe_requires_configure_permission],

    [{version_to_group_name(V), [sequence],
      Tests ++ [{group, unsubscribe_authz}]}
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
    Config;
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
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),
    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION},
                            {<<"receipt">>, <<"rcpt1">>},
                            {<<"ack">>, <<"client">>},
                            {<<"id">>, <<"subscription-id">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, Client2, Hdrs1, [<<"hello">>]} = stomp_receive(Client1, 'MESSAGE'),

    rabbit_stomp_client:send(
      Client2, 'UNSUBSCRIBE', [{<<"destination">>, ?DESTINATION},
                               {<<"id">>, <<"subscription-id">>}]),

    rabbit_stomp_client:send(
      Client2, 'ACK', [{rabbit_stomp_util:ack_header_name(Version),
                        maps:get(
                          rabbit_stomp_util:msg_header_name(Version), Hdrs1)},
                       {<<"receipt">>, <<"rcpt2">>}]),

    {ok, _Client3, Hdrs2, _Body2} = stomp_receive(Client2, 'ERROR'),
    ?assertEqual(<<"Subscription not found">>,
                 maps:get(<<"message">>, Hdrs2)),
    ok.

%% A durable UNSUBSCRIBE must delete the queue this connection subscribed to,
%% not the one x-queue-name names on the UNSUBSCRIBE frame.
durable_unsubscribe_ignores_frame_queue_name(Config) ->
    Client = ?config(authz_client, Config),
    Bystander = authz_bystander_queue(Config),
    SubQueue = ?config(authz_sub_queue, Config),
    Client1 = authz_subscribe(Client),
    ?assertMatch({ok, _}, lookup_queue(SubQueue, Config)),
    ?assertMatch({ok, _}, lookup_queue(Bystander, Config)),
    %% the id alone resolves the subscription the delete targets
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
    %% a misdirected delete would land before the RECEIPT, so check it first
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
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION}, {<<"receipt">>, <<"bah">>}]),
    {ok, Client1, _, _} = stomp_receive(Client, 'RECEIPT'),

    %% delete queue while subscribed
    #'queue.delete_ok'{} =
        amqp_channel:call(Channel, #'queue.delete'{queue = ?QUEUE}),

    {ok, _Client2, Headers, _} = stomp_receive(Client1, 'ERROR'),

    ?DESTINATION = maps:get(<<"subscription">>, Headers),

    % server closes connection
    ok.

temp_destination_queue(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
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
    {ok, _Client1, _, [<<"pong">>]} = stomp_receive(Client, 'MESSAGE'),
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
