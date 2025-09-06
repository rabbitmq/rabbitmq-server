%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(direct_reply_to_amqp_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(amqp_utils,
        [init/1, init/2,
         connection_config/1, connection_config/2,
         flush/1,
         wait_for_credit/1,
         wait_for_accepts/1,
         send_messages/3,
         receive_messages/2,
         detach_link_sync/1,
         end_session_sync/1,
         close_connection_sync/1]).

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [],
      [
       responder_attaches_queue_target,
       many_replies,
       many_volatile_queues_same_session
      ]},
     {cluster_size_3, [shuffle],
      [
       rpc_new_to_old_node,
       rpc_old_to_new_node
      ]}
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
    Nodes = case Group of
                cluster_size_1 -> 1;
                cluster_size_3 -> 3
            end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, Nodes},
                         {rmq_nodename_suffix, Suffix}]),
    Config2 = rabbit_ct_helpers:run_setup_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config2, 'rabbitmq_4.2.0') of
        ok ->
            Config2;
        {skip, _} = Skip ->
            Skip
    end.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% Responder attaches to a "/queues/amq.rabbitmq.reply-to.<suffix>" target.
responder_attaches_queue_target(Config) ->
    RequestQueue = atom_to_binary(?FUNCTION_NAME),
    AddrRequestQueue = rabbitmq_amqp_address:queue(RequestQueue),

    {ConnResponder, SessionResponder, LinkPairResponder} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPairResponder, RequestQueue, #{}),

    OpnConfRequester0 = connection_config(Config),
    OpnConfRequester = OpnConfRequester0#{container_id := <<"requester">>,
                                          notify_with_performative => true},
    {ok, ConnRequester} = amqp10_client:open_connection(OpnConfRequester),
    {ok, SessionRequester} = amqp10_client:begin_session_sync(ConnRequester),
    {ok, ReceiverRequester} = amqp10_client:attach_link(SessionRequester, attach_args()),
    AddrVolQ = receive {amqp10_event, {link, ReceiverRequester, {attached, Attach}}} ->
                           #'v1_0.attach'{
                              source = #'v1_0.source'{
                                          address = {utf8, AddressVolatileQueue},
                                          dynamic = true}} = Attach,
                           AddressVolatileQueue
               after 9000 -> ct:fail({missing_event, ?LINE})
               end,
    ok = amqp10_client:flow_link_credit(ReceiverRequester, 3, never),

    {ok, SenderRequester} = amqp10_client:attach_sender_link_sync(
                              SessionRequester, <<"sender requester">>, AddrRequestQueue),
    ok = wait_for_credit(SenderRequester),

    RpcId = <<"RPC message ID">>,
    ok = amqp10_client:send_msg(
           SenderRequester,
           amqp10_msg:set_properties(
             #{message_id => RpcId,
               reply_to => AddrVolQ},
             amqp10_msg:new(<<"t1">>, <<"request-1">>))),
    receive {amqp10_disposition, {accepted, <<"t1">>}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, ReceiverResponder} = amqp10_client:attach_receiver_link(
                                SessionResponder, <<"receiver responder">>,
                                AddrRequestQueue, unsettled),
    {ok, RequestMsg} = amqp10_client:get_msg(ReceiverResponder),
    ok = amqp10_client:accept_msg(ReceiverResponder, RequestMsg),
    ?assertEqual(<<"request-1">>, amqp10_msg:body_bin(RequestMsg)),
    #{message_id := RpcId,
      reply_to := ReplyToAddr} = amqp10_msg:properties(RequestMsg),
    ?assertMatch(<<"/queues/amq.rabbitmq.reply-to.", _/binary>>, ReplyToAddr),

    %% The metadata store should store only the request queue.
    ?assertEqual(1, rabbit_ct_broker_helpers:rpc(Config, rabbit_db_queue, count, [])),

    {ok, #{queue := ReplyQ}} = rabbitmq_amqp_address:to_map(ReplyToAddr),
    ?assertMatch({ok, #{vhost := <<"/">>,
                        durable := false,
                        type := <<"rabbit_volatile_queue">>,
                        message_count := 0,
                        consumer_count := 1}},
                 rabbitmq_amqp_client:get_queue(LinkPairResponder, ReplyQ)),

    {ok, SenderResponder1} = amqp10_client:attach_sender_link_sync(
                               SessionResponder, <<"sender responder unsettled">>,
                               ReplyToAddr, unsettled),
    {ok, SenderResponder2} = amqp10_client:attach_sender_link_sync(
                               SessionResponder, <<"sender responder settled">>,
                               ReplyToAddr, settled),
    ok = wait_for_credit(SenderResponder1),
    ok = wait_for_credit(SenderResponder2),
    flush(attached),
    ?assertMatch(#{publishers := 3,
                   consumers := 2},
                 maps:get(#{protocol => amqp10}, get_global_counters(Config))),

    %% Multiple responders stream back multiple replies for the single request.
    %% "AMQP is commonly used in publish/subscribe systems where copies of a single
    %% original message are distributed to zero or many subscribers. AMQP permits
    %% zero or multiple responses to a message with the reply-to property set,
    %% which can be correlated using the correlation-id property."
    %% [http-over-amqp-v1.0-wd06]
    ok = amqp10_client:send_msg(
           SenderResponder1,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 1">>,
               correlation_id => RpcId},
             amqp10_msg:new(<<1>>, <<"reply-1">>))),
    receive {amqp10_disposition, {accepted, <<1>>}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:send_msg(
           SenderResponder1,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 2">>,
               to => ReplyToAddr,
               correlation_id => RpcId},
             amqp10_msg:new(<<2>>, <<"reply-2">>))),
    receive {amqp10_disposition, {accepted, <<2>>}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:send_msg(
           SenderResponder2,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 3">>,
               to => ReplyToAddr,
               correlation_id => RpcId},
             amqp10_msg:new(<<3>>, <<"reply-3">>, true))),

    receive {amqp10_msg, ReceiverRequester, ReplyMsg1} ->
                ?assertEqual(<<"reply-1">>,
                             amqp10_msg:body_bin(ReplyMsg1)),
                ?assertMatch(#{message_id := <<"reply 1">>,
                               correlation_id := RpcId},
                             amqp10_msg:properties(ReplyMsg1))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverRequester, ReplyMsg2} ->
                ?assertEqual(<<"reply-2">>,
                             amqp10_msg:body_bin(ReplyMsg2)),
                ?assertMatch(#{message_id := <<"reply 2">>,
                               correlation_id := RpcId},
                             amqp10_msg:properties(ReplyMsg2))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverRequester, ReplyMsg3} ->
                ?assertEqual(<<"reply-3">>,
                             amqp10_msg:body_bin(ReplyMsg3)),
                ?assertMatch(#{message_id := <<"reply 3">>,
                               correlation_id := RpcId},
                             amqp10_msg:properties(ReplyMsg3))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,

    %% RabbitMQ should drop the 4th reply due to insufficient link credit.
    ok = amqp10_client:send_msg(
           SenderResponder2,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 4">>,
               to => ReplyToAddr,
               correlation_id => RpcId},
             amqp10_msg:new(<<4>>, <<"reply-4">>, true))),
    receive {amqp10_msg, _, _} ->
                ct:fail({unxpected_msg, ?LINE})
    after 5 -> ok
    end,

    %% Test drain
    flush(pre_drain),
    ok = amqp10_client:flow_link_credit(ReceiverRequester, 100_000, never, true),
    receive {amqp10_event, {link, ReceiverRequester, credit_exhausted}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    %% RabbitMQ should also drop the 5th reply due to insufficient link credit.
    ok = amqp10_client:send_msg(
           SenderResponder2,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 5">>,
               to => ReplyToAddr,
               correlation_id => RpcId},
             amqp10_msg:new(<<5>>, <<"reply-5">>, true))),
    receive {amqp10_msg, _, _} ->
                ct:fail({unxpected_msg, ?LINE})
    after 5 -> ok
    end,

    %% When the requester detaches, the volatile queue is gone.
    ok = detach_link_sync(ReceiverRequester),
    flush(detached),
    ?assertMatch(#{publishers := 3,
                   consumers := 1},
                 maps:get(#{protocol => amqp10}, get_global_counters(Config))),
    %% Therefore, HTTP GET on that queue should return 404.
    {error, Resp} = rabbitmq_amqp_client:get_queue(LinkPairResponder, ReplyQ),
    ?assertMatch(#{subject := <<"404">>}, amqp10_msg:properties(Resp)),
    %% Also, RabbitMQ should refuse attaching to the volatile queue target.
    {ok, SenderResponder3} = amqp10_client:attach_sender_link_sync(
                               SessionResponder, <<"sender responder 3">>,
                               ReplyToAddr),
    receive {amqp10_event, {link, SenderResponder3, {detached, Error1}}} ->
                ?assertMatch(
                   #'v1_0.error'{
                      condition = ?V_1_0_AMQP_ERROR_NOT_FOUND,
                      description = {utf8, <<"no queue 'amq.rabbitmq.reply-to.", _/binary>>}},
                   Error1)
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    %% RabbitMQ should also refuse attaching to the volatile queue target
    %% when the requester ends the session.
    ok = end_session_sync(SessionRequester),
    {ok, SenderResponder4} = amqp10_client:attach_sender_link_sync(
                               SessionResponder, <<"sender responder 4">>,
                               ReplyToAddr),
    receive {amqp10_event, {link, SenderResponder4, {detached, Error2}}} ->
                ?assertMatch(
                   #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_NOT_FOUND},
                   Error2)
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPairResponder, RequestQueue),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPairResponder),

    ok = close_connection_sync(ConnResponder),
    ok = close_connection_sync(ConnRequester),

    Counters = get_global_counters(Config),
    ?assertMatch(#{messages_delivered_total := 3},
                 maps:get(#{protocol => amqp10,
                            queue_type => rabbit_volatile_queue}, Counters)),
    ?assertMatch(#{messages_dead_lettered_maxlen_total := 2},
                 maps:get(#{dead_letter_strategy => disabled,
                            queue_type => rabbit_volatile_queue}, Counters)),
    ?assertMatch(#{publishers := 0,
                   consumers := 0,
                   %% RabbitMQ received 6 msgs in total (1 request + 5 replies)
                   messages_received_total := 6},
                 maps:get(#{protocol => amqp10}, Counters)).

%% Test that responder can send many messages to requester.
%% Load test the volatile queue.
many_replies(Config) ->
    Num = 3000,
    RequestQueue = atom_to_binary(?FUNCTION_NAME),
    AddrRequestQueue = rabbitmq_amqp_address:queue(RequestQueue),

    {ConnResponder, SessionResponder, LinkPair} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, RequestQueue, #{}),

    OpnConfRequester0 = connection_config(Config),
    OpnConfRequester = OpnConfRequester0#{container_id := <<"requester">>,
                                          notify_with_performative => true},
    {ok, ConnRequester} = amqp10_client:open_connection(OpnConfRequester),
    {ok, SessionRequester} = amqp10_client:begin_session_sync(ConnRequester),
    {ok, ReceiverRequester} = amqp10_client:attach_link(SessionRequester, attach_args()),
    AddrVolQ = receive {amqp10_event, {link, ReceiverRequester, {attached, Attach}}} ->
                           #'v1_0.attach'{
                              source = #'v1_0.source'{
                                          address = {utf8, AddressVolatileQueue},
                                          dynamic = true}} = Attach,
                           AddressVolatileQueue
               after 9000 -> ct:fail({missing_event, ?LINE})
               end,
    ok = amqp10_client:flow_link_credit(ReceiverRequester, Num, never),

    {ok, SenderRequester} = amqp10_client:attach_sender_link_sync(
                              SessionRequester, <<"sender requester">>, AddrRequestQueue),
    ok = wait_for_credit(SenderRequester),

    ok = amqp10_client:send_msg(
           SenderRequester,
           amqp10_msg:set_properties(
             #{reply_to => AddrVolQ},
             amqp10_msg:new(<<"t1">>, <<"request-1">>))),
    receive {amqp10_disposition, {accepted, <<"t1">>}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    {ok, ReceiverResponder} = amqp10_client:attach_receiver_link(
                                SessionResponder, <<"receiver responder">>,
                                AddrRequestQueue, unsettled),
    {ok, RequestMsg} = amqp10_client:get_msg(ReceiverResponder),
    ok = amqp10_client:accept_msg(ReceiverResponder, RequestMsg),
    #{reply_to := ReplyToAddr} = amqp10_msg:properties(RequestMsg),

    {ok, SenderResponder} = amqp10_client:attach_sender_link_sync(
                              SessionResponder, <<"sender responder">>, ReplyToAddr),
    ok = wait_for_credit(SenderResponder),
    flush(attached),

    ok = send_messages(SenderResponder, Num, true),
    Msgs = receive_messages(ReceiverRequester, Num),
    receive {amqp10_event, {link, ReceiverRequester, credit_exhausted}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    lists:foldl(fun(Msg, N) ->
                        Bin = integer_to_binary(N),
                        ?assertEqual(Bin, amqp10_msg:body_bin(Msg)),
                        N - 1
                end, Num, Msgs),

    [ok = detach_link_sync(R) || R <- [ReceiverRequester, SenderRequester,
                                       ReceiverResponder, SenderResponder]],
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, RequestQueue),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = close_connection_sync(ConnResponder),
    ok = close_connection_sync(ConnRequester).

%% In contrast to AMQP 0.9.1, we expect RabbitMQ to allow for multiple volatile queues
%% on the same AMQP 1.0 session because for example a JMS app can create multiple
%% temporary queues on the same session:
%% https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/session#createTemporaryQueue()
%% https://jakarta.ee/specifications/messaging/3.1/apidocs/jakarta.messaging/jakarta/jms/jmscontext#createTemporaryQueue()
many_volatile_queues_same_session(Config) ->
    RequestQueue = atom_to_binary(?FUNCTION_NAME),
    AddrRequestQueue = rabbitmq_amqp_address:queue(RequestQueue),

    {ConnResponder, SessionResponder, LinkPair} = init(Config),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, RequestQueue, #{}),

    OpnConfRequester0 = connection_config(Config),
    OpnConfRequester = OpnConfRequester0#{container_id := <<"requester">>,
                                          notify_with_performative => true},
    {ok, ConnRequester} = amqp10_client:open_connection(OpnConfRequester),
    {ok, SessionRequester} = amqp10_client:begin_session_sync(ConnRequester),
    {ok, Receiver1Requester} = amqp10_client:attach_link(SessionRequester, attach_args(<<"r1">>)),
    {ok, Receiver2Requester} = amqp10_client:attach_link(SessionRequester, attach_args(<<"r2">>)),
    AddrVolQ1 = receive {amqp10_event, {link, Receiver1Requester, {attached, Attach1}}} ->
                            #'v1_0.attach'{
                               source = #'v1_0.source'{
                                           address = {utf8, AddressVolatileQueue1},
                                           dynamic = true}} = Attach1,
                            AddressVolatileQueue1
                after 9000 -> ct:fail({missing_event, ?LINE})
                end,
    AddrVolQ2 = receive {amqp10_event, {link, Receiver2Requester, {attached, Attach2}}} ->
                            #'v1_0.attach'{
                               source = #'v1_0.source'{
                                           address = {utf8, AddressVolatileQueue2},
                                           dynamic = true}} = Attach2,
                            AddressVolatileQueue2
                after 9000 -> ct:fail({missing_event, ?LINE})
                end,
    ok = amqp10_client:flow_link_credit(Receiver1Requester, 1, never),
    ok = amqp10_client:flow_link_credit(Receiver2Requester, 1, never),

    {ok, SenderRequester} = amqp10_client:attach_sender_link_sync(
                              SessionRequester, <<"sender requester">>, AddrRequestQueue),
    ok = wait_for_credit(SenderRequester),

    ok = amqp10_client:send_msg(
           SenderRequester,
           amqp10_msg:set_properties(
             #{message_id => <<"RPC receiver 1">>,
               reply_to => AddrVolQ1},
             amqp10_msg:new(<<"t1">>, <<"request-1">>))),
    ok = amqp10_client:send_msg(
           SenderRequester,
           amqp10_msg:set_properties(
             #{message_id => <<"RPC receiver 2">>,
               reply_to => AddrVolQ2},
             amqp10_msg:new(<<"t2">>, <<"request-2">>))),
    ok = wait_for_accepts(2),

    {ok, ReceiverResponder} = amqp10_client:attach_receiver_link(
                                SessionResponder, <<"receiver responder">>,
                                AddrRequestQueue, settled),
    {ok, RequestMsg1} = amqp10_client:get_msg(ReceiverResponder),
    {ok, RequestMsg2} = amqp10_client:get_msg(ReceiverResponder),
    #{message_id := Id1,
      reply_to := ReplyToAddr1} = amqp10_msg:properties(RequestMsg1),
    #{message_id := Id2,
      reply_to := ReplyToAddr2} = amqp10_msg:properties(RequestMsg2),

    ?assertMatch(<<"/queues/amq.rabbitmq.reply-to.", _/binary>>, ReplyToAddr1),
    ?assertMatch(<<"/queues/amq.rabbitmq.reply-to.", _/binary>>, ReplyToAddr2),
    ?assertNotEqual(ReplyToAddr1, ReplyToAddr2),
    %% The metadata store should store only the request queue.
    ?assertEqual(1, rabbit_ct_broker_helpers:rpc(Config, rabbit_db_queue, count, [])),

    {ok, SenderResponder} = amqp10_client:attach_sender_link_sync(
                              SessionResponder, <<"sender responder">>,
                              null, mixed),
    ok = wait_for_credit(SenderResponder),
    flush(attached),

    ok = amqp10_client:send_msg(
           SenderResponder,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 1">>,
               to => ReplyToAddr1,
               correlation_id => Id1},
             amqp10_msg:new(<<1>>, <<"reply-1">>, true))),
    ok = amqp10_client:send_msg(
           SenderResponder,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 2">>,
               to => ReplyToAddr2,
               correlation_id => Id2},
             amqp10_msg:new(<<2>>, <<"reply-2">>, false))),
    ok = wait_for_accepts(1),

    receive {amqp10_msg, Receiver2Requester, ReplyMsg2} ->
                ?assertEqual(<<"reply-2">>,
                             amqp10_msg:body_bin(ReplyMsg2)),
                ?assertMatch(#{message_id := <<"reply 2">>,
                               correlation_id := <<"RPC receiver 2">>},
                             amqp10_msg:properties(ReplyMsg2))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, Receiver1Requester, ReplyMsg1} ->
                ?assertEqual(<<"reply-1">>,
                             amqp10_msg:body_bin(ReplyMsg1)),
                ?assertMatch(#{message_id := <<"reply 1">>,
                               correlation_id := <<"RPC receiver 1">>},
                             amqp10_msg:properties(ReplyMsg1))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,

    [ok = detach_link_sync(R) || R <- [Receiver1Requester, Receiver2Requester, SenderRequester,
                                       ReceiverResponder, SenderResponder]],
    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, RequestQueue),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = close_connection_sync(ConnResponder),
    ok = close_connection_sync(ConnRequester).

%% "new" and "old" refers to new and old RabbitMQ versions in mixed version tests.
rpc_new_to_old_node(Config) ->
    rpc(0, 1, Config).

rpc_old_to_new_node(Config) ->
    rpc(1, 0, Config).

rpc(RequesterNode, ResponderNode, Config) ->
    RequestQueue = atom_to_binary(?FUNCTION_NAME),
    AddrRequestQueue = rabbitmq_amqp_address:queue(RequestQueue),

    {ConnResponder, SessionResponder, _} = init(ResponderNode, Config),

    OpnConfRequester0 = connection_config(RequesterNode, Config),
    OpnConfRequester = OpnConfRequester0#{container_id := <<"requester">>,
                                          notify_with_performative => true},
    {ok, ConnRequester} = amqp10_client:open_connection(OpnConfRequester),
    {ok, SessionRequester} = amqp10_client:begin_session_sync(ConnRequester),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(
                                SessionRequester, <<"link pair requester">>),

    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, RequestQueue, #{}),

    {ok, ReceiverRequester} = amqp10_client:attach_link(SessionRequester, attach_args()),
    AddrVolQ = receive {amqp10_event, {link, ReceiverRequester, {attached, Attach}}} ->
                           #'v1_0.attach'{
                              source = #'v1_0.source'{
                                          address = {utf8, AddressVolatileQueue},
                                          dynamic = true}} = Attach,
                           AddressVolatileQueue
               after 9000 -> ct:fail({missing_event, ?LINE})
               end,
    ok = amqp10_client:flow_link_credit(ReceiverRequester, 2, never),

    {ok, SenderRequester} = amqp10_client:attach_sender_link_sync(
                              SessionRequester, <<"sender requester">>, AddrRequestQueue),
    ok = wait_for_credit(SenderRequester),

    RpcId = <<"RPC message ID">>,
    ok = amqp10_client:send_msg(
           SenderRequester,
           amqp10_msg:set_properties(
             #{message_id => RpcId,
               reply_to => AddrVolQ},
             amqp10_msg:new(<<"t1">>, <<"request-1">>))),
    receive {amqp10_disposition, {accepted, <<"t1">>}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = rabbit_ct_broker_helpers:await_metadata_store_consistent(Config, ResponderNode),
    {ok, ReceiverResponder} = amqp10_client:attach_receiver_link(
                                SessionResponder, <<"receiver responder">>,
                                RequestQueue, unsettled),
    {ok, RequestMsg} = amqp10_client:get_msg(ReceiverResponder),
    ?assertEqual(<<"request-1">>, amqp10_msg:body_bin(RequestMsg)),

    #{message_id := RpcId,
      reply_to := ReplyToAddr} = amqp10_msg:properties(RequestMsg),

    ?assertMatch(<<"/queues/amq.rabbitmq.reply-to.", _/binary>>, ReplyToAddr),

    {ok, SenderResponderAnon} = amqp10_client:attach_sender_link_sync(
                                  SessionResponder,
                                  <<"sender responder anonymous terminus">>,
                                  null, unsettled),
    ok = wait_for_credit(SenderResponderAnon),
    flush(attached),

    %% Responder streams back two replies for the single request.
    ok = amqp10_client:send_msg(
           SenderResponderAnon,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 1">>,
               to => ReplyToAddr,
               correlation_id => RpcId},
             amqp10_msg:new(<<1>>, <<"reply-1">>))),
    receive {amqp10_disposition, {accepted, <<1>>}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,
    ok = amqp10_client:send_msg(
           SenderResponderAnon,
           amqp10_msg:set_properties(
             #{message_id => <<"reply 2">>,
               to => ReplyToAddr,
               correlation_id => RpcId},
             amqp10_msg:new(<<2>>, <<"reply-2">>))),
    receive {amqp10_disposition, {accepted, <<2>>}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    ok = amqp10_client:accept_msg(ReceiverResponder, RequestMsg),

    %% The metadata store should store only the request queue.
    ?assertEqual(1, rabbit_ct_broker_helpers:rpc(Config, rabbit_db_queue, count, [])),

    receive {amqp10_msg, ReceiverRequester, ReplyMsg1} ->
                ?assertEqual(<<"reply-1">>,
                             amqp10_msg:body_bin(ReplyMsg1)),
                ?assertMatch(#{message_id := <<"reply 1">>,
                               correlation_id := RpcId},
                             amqp10_msg:properties(ReplyMsg1))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,
    receive {amqp10_msg, ReceiverRequester, ReplyMsg2} ->
                ?assertEqual(<<"reply-2">>,
                             amqp10_msg:body_bin(ReplyMsg2)),
                ?assertMatch(#{message_id := <<"reply 2">>,
                               correlation_id := RpcId},
                             amqp10_msg:properties(ReplyMsg2))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, RequestQueue),
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),

    ok = close_connection_sync(ConnResponder),
    ok = close_connection_sync(ConnRequester).

attach_args() ->
    attach_args(<<"receiver requester">>).

attach_args(Name) ->
    Source = #{address => undefined,
               durable => none,
               expiry_policy => <<"link-detach">>,
               dynamic => true,
               capabilities => [<<"rabbitmq:volatile-queue">>]},
    #{name => Name,
      role => {receiver, Source, self()},
      snd_settle_mode => settled,
      rcv_settle_mode => first}.

get_global_counters(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, rabbit_global_counters, overview, []).
