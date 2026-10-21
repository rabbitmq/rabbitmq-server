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
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

-define(QUEUE, <<"TestQueue">>).
-define(QUEUE_QQ, <<"TestQueueQQ">>).
-define(DESTINATION, "/amq/queue/TestQueue").
-define(DESTINATION_QQ, "/amq/queue/TestQueueQQ").

all() ->
    [{group, version_to_group_name(V)} || V <- ?SUPPORTED_VERSIONS].

groups() ->
    Tests = [
        publish_no_dest_error,
        publish_unauthorized_error,
        subscribe_error,
<<<<<<< HEAD
=======
        subscribe_to_exclusive_queue_of_another_connection,
        subscribe_to_own_exclusive_queue,
        reserved_queue_names_are_refused,
        queue_name_cr_lf_is_stripped,
        queue_name_of_only_cr_lf_is_refused,
        malformed_headers_are_refused,
        error_frame_with_non_latin1_text_case1,
        error_frame_with_non_latin1_text_case2,
        error_frame_with_non_latin1_text_case3,
        error_frame_with_utf8_encoded_text,
        empty_queue_name_is_ignored_for_transient_subscription,
        stream_offset_is_refused_for_classic_queue,
        subscribe_binding_failure_keeps_queue_messages,
>>>>>>> a75f102 (STOMP: refuse SUBSCRIBE to another connection's exclusive queue)
        subscribe,
        subscribe_with_x_priority,
        unsubscribe_ack,
        subscribe_ack,
        publisher_headers_do_not_override_server_headers,
        send,
        delete_queue_subscribe,
        temp_destination_queue,
        temp_destination_in_send,
        blank_destination_in_send,
        stream_filtering
    ],

    [{version_to_group_name(V), [sequence], Tests}
     || V <- ?SUPPORTED_VERSIONS].

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

init_per_group(Group, Config) ->
    Suffix = string:sub_string(atom_to_list(Group), 9),
    Version = re:replace(Suffix, "_", ".", [global, {return, list}]),
    rabbit_ct_helpers:set_config(Config, [{version, Version}]).

end_per_group(_Group, Config) -> Config.

init_per_testcase(TestCase, Config) ->
<<<<<<< HEAD
=======
    case rabbit_ct_helpers:is_mixed_versions() andalso
         lists:member(TestCase, tests_with_new_behavior()) of
        true ->
            {skip, "Should not run in mixed version environments"};
        false ->
            init_per_testcase1(TestCase, Config)
    end.

tests_with_new_behavior() ->
    [subscribe_to_exclusive_queue_of_another_connection,
     reserved_queue_names_are_refused,
     queue_name_cr_lf_is_stripped,
     queue_name_of_only_cr_lf_is_refused,
     malformed_headers_are_refused,
     empty_queue_name_is_ignored_for_transient_subscription,
     error_frame_with_non_latin1_text_case1,
     error_frame_with_non_latin1_text_case2,
     error_frame_with_non_latin1_text_case3,
     error_frame_with_utf8_encoded_text,
     stream_offset_is_refused_for_classic_queue,
     commit_of_failed_transaction_has_no_receipt,
     send_unroutable_with_receipt,
     global_counters_after_close,
     consumer_counter_after_server_cancel,
     stream_readers_closed_with_connection,
     temp_queue_uses_amq_gen_prefix].

init_per_testcase1(TestCase, Config) ->
>>>>>>> a75f102 (STOMP: refuse SUBSCRIBE to another connection's exclusive queue)
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
init_per_testcase0(_, Config) ->
    Config.

end_per_testcase0(publish_unauthorized_error, Config) ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:disconnect(ClientFoo),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, delete_user,
                                 [<<"user">>, <<"acting-user">>]),
    Config;
end_per_testcase0(_, Config) ->
    Config.

publish_no_dest_error(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send(
      Client, "SEND", [{"destination", "/exchange/non-existent"}], ["hello"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "not_found" = proplists:get_value("message", Hdrs),
    ok.

publish_unauthorized_error(Config) ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:send(
      ClientFoo, "SEND", [{"destination", "/amq/queue/RestrictedQueue"}], ["hello"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(ClientFoo, "ERROR"),
    "access_refused" = proplists:get_value("message", Hdrs),
    ok.

subscribe_error(Config) ->
    Client = ?config(stomp_client, Config),
    %% SUBSCRIBE to missing queue
    rabbit_stomp_client:send(
<<<<<<< HEAD
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "not_found" = proplists:get_value("message", Hdrs),
=======
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION}]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, 'ERROR'),
    <<"not_found">> = maps:get(<<"message">>, Hdrs),
    ok.

subscribe_to_exclusive_queue_of_another_connection(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, <<"/amq/queue/", QName/binary>>},
                            {<<"id">>, <<"s">>}]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, 'ERROR', 30_000),
    ?assertEqual(<<"resource_locked">>, maps:get(<<"message">>, Hdrs)),
    ok.

subscribe_to_own_exclusive_queue(Config) ->
    Client = ?config(stomp_client, Config),
    Name = <<"own-exclusive-", (integer_to_binary(rand:uniform(1_000_000)))/binary>>,
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, <<"/queue/", Name/binary>>},
                            {<<"id">>, <<"s1">>}]),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, <<"/amq/queue/", Name/binary>>},
                            {<<"id">>, <<"s2">>}]),
    rabbit_stomp_client:send(
      Client, 'SEND', [{<<"destination">>, <<"/queue/", Name/binary>>}], ["hello"]),
    {ok, _Client1, _, [<<"hello">>]} = stomp_receive(Client, 'MESSAGE', 30_000),
    ok.

reserved_queue_names_are_refused(Config) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    Dest = iolist_to_binary(["/topic/reserved-queue-names-", Version]),
    Frames = [{'SEND', [{<<"destination">>, <<"/queue/amq.reserved">>}]},
              {'SUBSCRIBE', [{<<"destination">>, Dest}, {<<"id">>, <<"s">>},
                             {<<"x-queue-name">>, <<"amq.reserved">>}]}],
    lists:foreach(
      fun({Command, Headers}) ->
              {ok, Client} = rabbit_stomp_client:connect(Version, StompPort),
              rabbit_stomp_client:send(Client, Command, Headers, ["hello"]),
              {ok, _, Hdrs, _} = stomp_receive(Client, 'ERROR'),
              ?assertEqual({Command, <<"access_refused">>},
                           {Command, maps:get(<<"message">>, Hdrs)})
      end, Frames),
    VHost = ?config(rmq_vhost, Config),
    ?assertEqual({error, not_found},
                 lookup_queue(rabbit_misc:r(VHost, queue, <<"amq.reserved">>), Config)).

%% STOMP 1.2 is the first version that escapes CR in header values.
queue_name_cr_lf_is_stripped(Config) ->
    queue_name_cr_lf_is_stripped(?config(version, Config), Config).

queue_name_cr_lf_is_stripped("1.2", Config) ->
    Client = ?config(stomp_client, Config),
    VHost = ?config(rmq_vhost, Config),
    [begin
         rabbit_stomp_client:send(
           Client, 'SEND', [{<<"destination">>, Dest},
                            {<<"receipt">>, <<"r">>}], ["hello"]),
         stomp_receive_receipt(Client, <<"r">>),
         QName = rabbit_misc:r(VHost, queue, Stripped),
         ?assertMatch({ok, _}, lookup_queue(QName, Config)),
         rabbit_stomp_client:send(
           Client, 'SUBSCRIBE', [{<<"destination">>, Dest},
                                 {<<"id">>, Stripped}]),
         {ok, _, _, Body} = stomp_receive(Client, 'MESSAGE'),
         ?assertEqual(<<"hello">>, iolist_to_binary(lists:reverse(Body))),
         rabbit_stomp_client:send(
           Client, 'UNSUBSCRIBE', [{<<"id">>, Stripped},
                                   {<<"receipt">>, <<"u">>}]),
         stomp_receive_receipt(Client, <<"u">>),
         delete_queue_if_present(QName, Config)
     end || {Dest, Stripped} <- [{<<"/queue/cr-lf-\r\nstripped">>, <<"cr-lf-stripped">>},
                                 {<<"cr-lf-\r\nbare">>, <<"cr-lf-bare">>}]],
    ok;
queue_name_cr_lf_is_stripped(_Version, _Config) ->
    ok.

queue_name_of_only_cr_lf_is_refused(Config) ->
    queue_name_of_only_cr_lf_is_refused(?config(version, Config), Config).

queue_name_of_only_cr_lf_is_refused("1.2", Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send(
      Client, 'SEND', [{<<"destination">>, <<"/queue/\r\n">>}], ["hello"]),
    {ok, _, Hdrs, Body} = stomp_receive(Client, 'ERROR'),
    ?assertEqual(<<"Invalid destination">>, maps:get(<<"message">>, Hdrs)),
    ?assertNotEqual(nomatch, binary:match(iolist_to_binary(lists:reverse(Body)),
                                          <<"Destination cannot be blank">>));
queue_name_of_only_cr_lf_is_refused(_Version, _Config) ->
    ok.

malformed_headers_are_refused(Config) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    Topic = iolist_to_binary(["/topic/malformed-headers-", Version]),
    StreamQueueName = iolist_to_binary(["malformed-headers-stream-", Version]),
    StreamQueue = <<"/queue/", StreamQueueName/binary>>,
    Cases = [{'SEND', [{<<"destination">>, Topic}, {<<"x-message-ttl">>, <<"60s">>}],
              <<"precondition_failed">>},
             {'SUBSCRIBE', [{<<"destination">>, Topic}, {<<"id">>, <<"s">>},
                            {<<"x-priority">>, <<"abc">>}],
              <<"precondition_failed">>},
             {'SUBSCRIBE', [{<<"destination">>, Topic}, {<<"id">>, <<"s">>},
                            {<<"x-stream-offset">>, <<"offset=abc">>}],
              <<"precondition_failed">>},
             {'SUBSCRIBE', [{<<"destination">>, Topic}, {<<"durable">>, <<"True">>}],
              <<"Missing Header">>},
             {'SUBSCRIBE', [{<<"destination">>, Topic}, {<<"id">>, <<"s">>},
                            {<<"durable">>, <<"true">>}, {<<"x-queue-name">>, <<>>}],
              <<"Invalid x-queue-name">>},
             {'SUBSCRIBE', [{<<"destination">>, Topic}, {<<"id">>, <<"s">>},
                            {<<"durable">>, <<"true">>}, {<<"x-queue-name">>, <<" ">>}],
              <<"Invalid x-queue-name">>},
             {'SUBSCRIBE', [{<<"destination">>, Topic}, {<<"id">>, <<"/temp-queue/x">>}],
              <<"Invalid id">>},
             {'SUBSCRIBE', [{<<"destination">>, StreamQueue}, {<<"id">>, <<"s">>},
                            {<<"ack">>, <<"client">>}, {<<"prefetch-count">>, <<"10">>},
                            {<<"durable">>, <<"true">>}, {<<"auto-delete">>, <<"false">>},
                            {<<"x-queue-type">>, <<"stream">>},
                            {<<"x-stream-offset">>, <<"offset=abc">>}],
              <<"precondition_failed">>},
             {'UNSUBSCRIBE', [{<<"destination">>, Topic}, {<<"durable">>, <<"True">>}],
              <<"Missing Header">>}],
    lists:foreach(
      fun({Command, Headers, Expected}) ->
              {ok, Client} = rabbit_stomp_client:connect(Version, StompPort),
              rabbit_stomp_client:send(Client, Command, Headers, ["hello"]),
              {ok, _, Hdrs, _} = stomp_receive(Client, 'ERROR'),
              ?assertEqual({Command, Headers, Expected},
                           {Command, Headers, maps:get(<<"message">>, Hdrs)})
      end, Cases),
    ?assertEqual({error, not_found},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_amqqueue, lookup,
                   [rabbit_misc:r(<<"/">>, queue, StreamQueueName)])).

error_frame_with_non_latin1_text_case1(Config) ->
    error_frame_with_non_latin1_text(Config, "amériques.canada.québec").

error_frame_with_non_latin1_text_case2(Config) ->
    error_frame_with_non_latin1_text(Config, "भारत.दिल्ली").

error_frame_with_non_latin1_text_case3(Config) ->
    error_frame_with_non_latin1_text(Config, "ไทย.กรุงเทพ").

error_frame_with_non_latin1_text(Config, Name) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    Exchange = unicode:characters_to_binary(Name),
    {ok, Client} = rabbit_stomp_client:connect(Version, StompPort),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, <<"/exchange/", Exchange/binary>>},
                            {<<"id">>, <<"s">>}]),
    {ok, _, Hdrs, Body} = stomp_receive(Client, 'ERROR'),
    ?assertEqual(<<"not_found">>, maps:get(<<"message">>, Hdrs)),
    ?assertNotEqual(nomatch, binary:match(iolist_to_binary(Body), Exchange)).

error_frame_with_utf8_encoded_text(Config) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    Channel = ?config(amqp_channel, Config),
    Exchange = <<"lrx-utf8-", (list_to_binary(Version))/binary>>,
    Key = unicode:characters_to_binary("日本"),
    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, #'exchange.declare'{exchange = Exchange,
                                                              type = <<"x-local-random">>}),
    try
        {ok, Client} = rabbit_stomp_client:connect(Version, StompPort),
        rabbit_stomp_client:send(
          Client, 'SUBSCRIBE', [{<<"destination">>, <<"/exchange/", Exchange/binary, "/", Key/binary>>},
                                {<<"id">>, <<"s">>}]),
        {ok, _, Hdrs, Body} = stomp_receive(Client, 'ERROR'),
        ?assertEqual(<<"precondition_failed">>, maps:get(<<"message">>, Hdrs)),
        ?assertNotEqual(nomatch, binary:match(iolist_to_binary(lists:reverse(Body)), Key))
    after
        #'exchange.delete_ok'{} = amqp_channel:call(Channel, #'exchange.delete'{exchange = Exchange})
    end.

empty_queue_name_is_ignored_for_transient_subscription(Config) ->
    Version = ?config(version, Config),
    Client = ?config(stomp_client, Config),
    VHost = ?config(rmq_vhost, Config),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, iolist_to_binary(["/topic/empty-queue-name-", Version])},
                            {<<"id">>, <<"s">>}, {<<"x-queue-name">>, <<>>},
                            {<<"receipt">>, <<"r">>}]),
    stomp_receive_receipt(Client, <<"r">>),
    ?assertEqual({error, not_found}, lookup_queue(rabbit_misc:r(VHost, queue, <<>>), Config)).

stream_offset_is_refused_for_classic_queue(Config) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    Channel = ?config(amqp_channel, Config),
    Queue = iolist_to_binary(["classic-with-offset-", Version]),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue = Queue, durable = true}),
    {ok, Client} = rabbit_stomp_client:connect(Version, StompPort),
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, <<"/amq/queue/", Queue/binary>>},
                            {<<"id">>, <<"s">>}, {<<"x-stream-offset">>, <<"first">>}]),
    {ok, _, Hdrs, _} = stomp_receive(Client, 'ERROR'),
    ?assertEqual(<<"precondition_failed">>, maps:get(<<"message">>, Hdrs)),
    #'queue.delete_ok'{} = amqp_channel:call(Channel, #'queue.delete'{queue = Queue}),
    ok.

subscribe_binding_failure_keeps_queue_messages(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue   = ?BIND_FAIL_QUEUE,
                                                    durable = true}),
    [amqp_channel:cast(Channel,
                       #'basic.publish'{exchange    = <<>>,
                                        routing_key = ?BIND_FAIL_QUEUE},
                       #amqp_msg{payload = <<"msg">>})
     || _ <- lists:seq(1, 10)],
    ?awaitMatch(#'queue.declare_ok'{message_count = 10},
                amqp_channel:call(Channel, #'queue.declare'{queue   = ?BIND_FAIL_QUEUE,
                                                            durable = true}),
                30_000),

    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE',
      [{<<"destination">>, <<"/exchange/missing-x/key">>},
       {<<"id">>, <<"bind-fail">>},
       {<<"x-queue-name">>, ?BIND_FAIL_QUEUE},
       {<<"durable">>, <<"true">>},
       {<<"auto-delete">>, <<"false">>},
       {<<"exclusive">>, <<"false">>},
       {<<"ack">>, <<"auto">>}]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, 'ERROR'),
    <<"not_found">> = maps:get(<<"message">>, Hdrs),

    {ok, Channel1} = amqp_connection:open_channel(?config(amqp_connection, Config)),
    ?assertMatch(#'queue.declare_ok'{message_count = 10},
                 amqp_channel:call(Channel1, #'queue.declare'{queue   = ?BIND_FAIL_QUEUE,
                                                              durable = true})),
>>>>>>> a75f102 (STOMP: refuse SUBSCRIBE to another connection's exclusive queue)
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
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}, {"receipt", "foo"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, _Client2, _, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),
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
      ClientA, "SUBSCRIBE", [{"destination", ?DESTINATION_QQ}, {"receipt", "foo"}]),
    {ok, _ClientA1, _, _} = stomp_receive(ClientA, "RECEIPT"),

    %% subscribe with a higher priority and wait for receipt
    {ok, ClientB} = rabbit_stomp_client:connect(Version, StompPort),
    rabbit_stomp_client:send(
      ClientB, "SUBSCRIBE", [{"destination", ?DESTINATION_QQ},
                             {"receipt", "foo"},
                             {"x-priority", 10}
                            ]),
    {ok, ClientB1, _, _} = stomp_receive(ClientB, "RECEIPT"),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE_QQ},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    %% ClientB should receive the message since it has a higher priority
    {ok, _ClientB2, _, [<<"hello">>]} = stomp_receive(ClientB1, "MESSAGE"),
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
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION},
                            {"receipt", "rcpt1"},
                            {"ack", "client"},
                            {"id", "subscription-id"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, Client2, Hdrs1, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),

    rabbit_stomp_client:send(
      Client2, "UNSUBSCRIBE", [{"destination", ?DESTINATION},
                              {"id", "subscription-id"}]),

    rabbit_stomp_client:send(
      Client2, "ACK", [{rabbit_stomp_util:ack_header_name(Version),
                        proplists:get_value(
                          rabbit_stomp_util:msg_header_name(Version), Hdrs1)},
                       {"receipt", "rcpt2"}]),

    {ok, _Client3, Hdrs2, _Body2} = stomp_receive(Client2, "ERROR"),
    ?assertEqual("Subscription not found",
                 proplists:get_value("message", Hdrs2)),
    ok.

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
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION},
                            {"receipt",     "foo"},
                            {"ack",         "client"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, _Client2, Headers, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),
    false = (Version == "1.2") xor proplists:is_defined(?HEADER_ACK, Headers),

    MsgHeader = rabbit_stomp_util:msg_header_name(Version),
    AckValue  = proplists:get_value(MsgHeader, Headers),
    AckHeader = rabbit_stomp_util:ack_header_name(Version),

    rabbit_stomp_client:send(Client, "ACK", [{AckHeader, AckValue}]),
    #'basic.get_empty'{} =
        amqp_channel:call(Channel, #'basic.get'{queue = ?QUEUE}),
    ok.

%% The server-generated `message-id`, `ack`, `subscription` and
%% `redelivered` headers of a MESSAGE frame take precedence over the headers a
%% publisher set.
publisher_headers_do_not_override_server_headers(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),

    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION},
                            {"id",          "precedence-test"},
                            {"receipt",     "foo"},
                            {"ack",         "client"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},
    PublisherHeaders =
        [{<<"message-id">>, longstr, <<"publisher-message-id">>},
         {<<"ack">>, longstr, <<"publisher-ack">>},
         {<<"subscription">>, longstr, <<"publisher-subscription">>},
         {<<"redelivered">>, longstr, <<"publisher-redelivered">>},
         {<<"destination">>, longstr, <<"publisher-destination">>}],
    amqp_channel:call(Channel, Method,
                      #amqp_msg{props = #'P_basic'{headers = PublisherHeaders},
                                payload = <<"hello">>}),

    {ok, Client2, Headers, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),
    MsgHeader = rabbit_stomp_util:msg_header_name(Version),
    AckHeader = rabbit_stomp_util:ack_header_name(Version),
    AckValue = proplists:get_value(MsgHeader, Headers),

    "precedence-test" = proplists:get_value(?HEADER_SUBSCRIPTION, Headers),
    "false" = proplists:get_value(?HEADER_REDELIVERED, Headers),

    %% `ack` is only generated for STOMP 1.2; under the other versions the
    %% ack token is carried by `message-id` and a publisher's `ack` header
    %% is an ordinary application header.
    %%
    %% N.B. `rabbit_stomp_frame` keeps the first of two headers of the same name.
    ServerGenerated = [?HEADER_MESSAGE_ID, ?HEADER_SUBSCRIPTION,
                       ?HEADER_REDELIVERED, ?HEADER_DESTINATION |
                       case Version of
                           "1.2" -> [?HEADER_ACK];
                           _     -> []
                       end],
    ?assertEqual([], [Name || {Name, Value} <- Headers,
                              lists:member(Name, ServerGenerated),
                              lists:prefix("publisher-", Value)]),

    %% An ack token that is syntactically valid but names another session
    {ok, {ConsumerTag, _SessionId, DeliveryTag}} =
        rabbit_stomp_util:parse_message_id(AckValue),
    OtherSessionAckValue =
        binary_to_list(ConsumerTag) ++ ?MESSAGE_ID_SEPARATOR ++
        "other-session" ++ ?MESSAGE_ID_SEPARATOR ++
        integer_to_list(DeliveryTag),
    rabbit_stomp_client:send(Client2, "ACK",
                             [{AckHeader, OtherSessionAckValue}]),
    {ok, Client3, _, _} = stomp_receive(Client2, "ERROR"),

    %% The delivery's own ack token still works afterwards.
    rabbit_stomp_client:send(Client3, "ACK", [{AckHeader, AckValue}]),
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
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}, {"receipt", "foo"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% send from stomp
    rabbit_stomp_client:send(
      Client1, "SEND", [{"destination", ?DESTINATION}], ["hello"]),

    {ok, _Client2, _, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),
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
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}, {"receipt", "bah"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% delete queue while subscribed
    #'queue.delete_ok'{} =
        amqp_channel:call(Channel, #'queue.delete'{queue = ?QUEUE}),

    {ok, _Client2, Headers, _} = stomp_receive(Client1, "ERROR"),

    ?DESTINATION = proplists:get_value("subscription", Headers),

    % server closes connection
    ok.

temp_destination_queue(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    durable     = true,
                                                    auto_delete = true}),
    rabbit_stomp_client:send( Client, "SEND", [{"destination", ?DESTINATION},
                                               {"reply-to", "/temp-queue/foo"}],
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
    {ok, _Client1, _, [<<"pong">>]} = stomp_receive(Client, "MESSAGE"),
    ok.

temp_destination_in_send(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send( Client, "SEND", [{"destination", "/temp-queue/foo"}],
                                              ["poing"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "Invalid destination" = proplists:get_value("message", Hdrs),
    ok.

blank_destination_in_send(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send( Client, "SEND", [{"destination", ""}],
                                              ["poing"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "Invalid destination" = proplists:get_value("message", Hdrs),
    ok.

stream_filtering(Config) ->
    Version = ?config(version, Config),
    Client = ?config(stomp_client, Config),
    Stream = atom_to_list(?FUNCTION_NAME) ++ "-" ++ integer_to_list(rand:uniform(10000)),
    %% subscription just to create the stream from STOMP
    SubDestination = "/topic/stream-queue-test",
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE",
      [{"destination", SubDestination},
       {"receipt", "foo"},
       {"x-queue-name", Stream},
       {"x-queue-type", "stream"},
       {?HEADER_X_STREAM_FILTER_SIZE_BYTES, "32"},
       {"durable", "true"},
       {"auto-delete", "false"},
       {"id", "1234"},
       {"prefetch-count", "1"},
       {"ack", "client"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),
    rabbit_stomp_client:send(
      Client1, "UNSUBSCRIBE", [{"destination", SubDestination},
                               {"id", "1234"},
                               {"receipt", "bar"}]),
    {ok, Client2, _, _} = stomp_receive(Client1, "RECEIPT"),

    %% we are going to publish several waves of messages with and without filter values.
    %% we will then create subscriptions with various filter options
    %% and make sure we receive only what we asked for and not all the messages.

    StreamDestination = "/amq/queue/" ++ Stream,
    %% logic to publish a wave of messages with or without a filter value
    WaveCount = 1000,
    Publish =
    fun(C, FilterValue) ->
            lists:foldl(fun(Seq, C0) ->
                                Headers0 = [{"destination", StreamDestination},
                                            {"receipt", integer_to_list(Seq)}],
                                Headers = case FilterValue of
                                              undefined ->
                                                  Headers0;
                                              _ ->
                                                  [{"x-stream-filter-value", FilterValue}] ++ Headers0
                                          end,
                                rabbit_stomp_client:send(
                                  C0, "SEND", Headers, ["hello"]),
                                {ok, C1, _, _} = stomp_receive(C0, "RECEIPT"),
                                C1
                        end, C, lists:seq(1, WaveCount))
    end,
    %% publishing messages with the "apple" filter value
    Client3 = Publish(Client2, "apple"),
    %% publishing messages with no filter value
    Client4 = Publish(Client3, undefined),
    %% publishing messages with the "orange" filter value
    Client5 = Publish(Client4, "orange"),

    %% filtering on "apple"
    rabbit_stomp_client:send(
      Client5, "SUBSCRIBE",
      [{"destination", StreamDestination},
       {"id", "0"},
       {"ack", "client"},
       {"prefetch-count", "1"},
       {"x-stream-filter", "apple"},
       {"x-stream-offset", "first"}]),
    {Client6, AppleMessages} = stomp_receive_messages(Client5, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleMessages) < WaveCount * 3),
    %% client-side filtering
    AppleFilteredMessages =
    lists:filter(fun(H) ->
                         proplists:get_value("x-stream-filter-value", H) =:= "apple"
                 end, AppleMessages),
    %% we should have only the "apple" messages
    ?assert(length(AppleFilteredMessages) =:= WaveCount),
    rabbit_stomp_client:send(
      Client6, "UNSUBSCRIBE", [{"destination", StreamDestination},
                               {"id", "0"},
                               {"receipt", "bar"}]),
    {ok, Client7, _, _} = stomp_receive(Client6, "RECEIPT"),

    %% filtering on "apple" and "orange"
    rabbit_stomp_client:send(
      Client7, "SUBSCRIBE",
      [{"destination", StreamDestination},
       {"id", "0"},
       {"ack", "client"},
       {"prefetch-count", "1"},
       {"x-stream-filter", "apple,orange"},
       {"x-stream-offset", "first"}]),
    {Client8, AppleOrangeMessages} = stomp_receive_messages(Client7, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleOrangeMessages) < WaveCount * 3),
    %% client-side filtering
    AppleOrangeFilteredMessages =
    lists:filter(fun(H) ->
                         proplists:get_value("x-stream-filter-value", H) =:= "apple" orelse
                         proplists:get_value("x-stream-filter-value", H) =:= "orange"
                 end, AppleOrangeMessages),
    %% we should have only the "apple" and "orange" messages
    ?assert(length(AppleOrangeFilteredMessages) =:= WaveCount * 2),
    rabbit_stomp_client:send(
      Client8, "UNSUBSCRIBE", [{"destination", StreamDestination},
                               {"id", "0"},
                               {"receipt", "bar"}]),
    {ok, Client9, _, _} = stomp_receive(Client8, "RECEIPT"),

    %% filtering on "apple" and messages without a filter value
    rabbit_stomp_client:send(
      Client9, "SUBSCRIBE",
      [{"destination", StreamDestination},
       {"id", "0"},
       {"ack", "client"},
       {"prefetch-count", "1"},
       {"x-stream-filter", "apple"},
       {"x-stream-match-unfiltered", "true"},
       {"x-stream-offset", "first"}]),
    {Client10, AppleUnfilteredMessages} = stomp_receive_messages(Client9, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleUnfilteredMessages) < WaveCount * 3),
    %% client-side filtering
    AppleUnfilteredFilteredMessages =
    lists:filter(fun(H) ->
                         proplists:get_value("x-stream-filter-value", H) =:= "apple" orelse
                         proplists:get_value("x-stream-filter-value", H) =:= undefined
                 end, AppleUnfilteredMessages),
    %% we should have only the "apple" messages and messages without a filter value
    ?assert(length(AppleUnfilteredFilteredMessages) =:= WaveCount * 2),
    rabbit_stomp_client:send(
      Client10, "UNSUBSCRIBE", [{"destination", StreamDestination},
                               {"id", "0"},
                               {"receipt", "bar"}]),
    {ok, _, _, _} = stomp_receive(Client10, "RECEIPT"),

    Channel = ?config(amqp_channel, Config),
    #'queue.delete_ok'{} = amqp_channel:call(Channel,
                                             #'queue.delete'{queue = list_to_binary(Stream)}),
    ok.

stomp_receive_messages(Client, Version) ->
    stomp_receive_messages(Client, [], Version).

stomp_receive_messages(Client, Acc, Version) ->
    try rabbit_stomp_client:recv(Client) of
        {#stomp_frame{command = "MESSAGE",
                      headers = Headers}, Client1} ->
        MsgHeader = rabbit_stomp_util:msg_header_name(Version),
        AckValue  = proplists:get_value(MsgHeader, Headers),
        AckHeader = rabbit_stomp_util:ack_header_name(Version),
        rabbit_stomp_client:send(Client1, "ACK", [{AckHeader, AckValue}]),
        stomp_receive_messages(Client1, [Headers] ++ Acc, Version)
    catch
      error:{badmatch, {error, timeout}} ->
            {Client, Acc}
    end.

stomp_receive(Client, Command) ->
    {#stomp_frame{command     = Command,
                  headers     = Hdrs,
                  body_iolist = Body},   Client1} =
    rabbit_stomp_client:recv(Client),
    {ok, Client1, Hdrs, Body}.
