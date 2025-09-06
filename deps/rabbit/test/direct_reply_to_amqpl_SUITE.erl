%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(direct_reply_to_amqpl_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_ct_helpers,
        [eventually/1]).
-import(amqp_utils,
        [init/1,
         close/1,
         connection_config/1,
         wait_for_credit/1,
         end_session_sync/1,
         close_connection_sync/1]).

-define(TIMEOUT, 9000).

%% This is the pseudo queue that is specially interpreted by RabbitMQ.
-define(REPLY_QUEUE, <<"amq.rabbitmq.reply-to">>).

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_1_ff_disabled},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      cluster_size_1_common() ++
      [
       amqpl_amqp_amqpl,
       amqp_amqpl_amqp
      ]},

     %% Delete this group when feature flag rabbitmq_4.2.0 becomes required.
     {cluster_size_1_ff_disabled, [],
      cluster_size_1_common() ++
      [
       enable_ff % must run last
      ]},

     {cluster_size_3, [shuffle],
      [
       rpc_new_to_old_node,
       rpc_old_to_new_node
      ]}
    ].

cluster_size_1_common() ->
    [
     trace,
     failure_ack_mode,
     failure_multiple_consumers,
     failure_reuse_consumer_tag,
     failure_publish
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
                cluster_size_3 ->
                    3;
                _ ->
                    1
            end,
    Config1 = case Group of
                  cluster_size_1_ff_disabled ->
                      rabbit_ct_helpers:merge_app_env(
                        Config,
                        {rabbit,
                         [{forced_feature_flags_on_init,
                           {rel, [], ['rabbitmq_4.2.0']}}]});
                  _ ->
                      Config
              end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config1, "", "-"),
    Config2 = rabbit_ct_helpers:set_config(
                Config1, [{rmq_nodes_count, Nodes},
                          {rmq_nodename_suffix, Suffix}]),
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

%% Test enabling the feature flag while a client consumes from the volatile queue.
%% Delete this test case when feature flag rabbitmq_4.2.0 becomes required.
enable_ff(Config) ->
    RequestQueue = <<"request queue">>,
    RequestPayload = <<"my request">>,
    CorrelationId = <<"my correlation ID">>,
    RequesterCh = rabbit_ct_client_helpers:open_channel(Config),
    ResponderCh = rabbit_ct_client_helpers:open_channel(Config),

    amqp_channel:subscribe(RequesterCh,
                           #'basic.consume'{queue = ?REPLY_QUEUE,
                                            no_ack = true},
                           self()),
    CTag = receive #'basic.consume_ok'{consumer_tag = CTag0} -> CTag0
           end,

    #'queue.declare_ok'{} = amqp_channel:call(
                              RequesterCh,
                              #'queue.declare'{queue = RequestQueue}),
    #'confirm.select_ok'{} = amqp_channel:call(RequesterCh, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(RequesterCh, self()),

    %% Send the request.
    amqp_channel:cast(
      RequesterCh,
      #'basic.publish'{routing_key = RequestQueue},
      #amqp_msg{props = #'P_basic'{reply_to = ?REPLY_QUEUE,
                                   correlation_id = CorrelationId},
                payload = RequestPayload}),
    receive #'basic.ack'{} -> ok
    end,

    %% Receive the request.
    {#'basic.get_ok'{},
     #amqp_msg{props = #'P_basic'{reply_to = ReplyTo,
                                  correlation_id = CorrelationId},
               payload = RequestPayload}
    } = amqp_channel:call(ResponderCh, #'basic.get'{queue = RequestQueue}),

    ?assertEqual(#'queue.declare_ok'{queue = ReplyTo,
                                     message_count = 0,
                                     consumer_count = 1},
                 amqp_channel:call(ResponderCh,
                                   #'queue.declare'{queue = ReplyTo})),

    %% Send the first reply.
    amqp_channel:cast(
      ResponderCh,
      #'basic.publish'{routing_key = ReplyTo},
      #amqp_msg{props = #'P_basic'{correlation_id = CorrelationId},
                payload = <<"reply 1">>}),

    %% Receive the frst reply.
    receive {#'basic.deliver'{consumer_tag = CTag,
                              redelivered = false,
                              exchange = <<>>,
                              routing_key = ReplyTo},
             #amqp_msg{payload = P1,
                       props = #'P_basic'{correlation_id = CorrelationId}}} ->
                ?assertEqual(<<"reply 1">>, P1)
    after ?TIMEOUT -> ct:fail({missing_reply, ?LINE})
    end,

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.2.0'),

    ?assertEqual(#'queue.declare_ok'{queue = ReplyTo,
                                     message_count = 0,
                                     consumer_count = 1},
                 amqp_channel:call(ResponderCh,
                                   #'queue.declare'{queue = ReplyTo})),

    %% Send the second reply.
    amqp_channel:cast(
      ResponderCh,
      #'basic.publish'{routing_key = ReplyTo},
      #amqp_msg{props = #'P_basic'{correlation_id = CorrelationId},
                payload = <<"reply 2">>}),

    %% Receive the second reply.
    receive {#'basic.deliver'{consumer_tag = CTag},
             #amqp_msg{payload = P2,
                       props = #'P_basic'{correlation_id = CorrelationId}}} ->
                ?assertEqual(<<"reply 2">>, P2)
    after ?TIMEOUT -> ct:fail({missing_reply, ?LINE})
    end,

    %% Requester cancels consumption.
    ?assertMatch(#'basic.cancel_ok'{consumer_tag = CTag},
                 amqp_channel:call(RequesterCh, #'basic.cancel'{consumer_tag = CTag})),

    %% Responder checks again if the requester is still there.
    %% This time, the requester and its queue should be gone.
    try amqp_channel:call(ResponderCh, #'queue.declare'{queue = ReplyTo}) of
        _ ->
            ct:fail("expected queue.declare to fail")
    catch exit:Reason ->
              ?assertMatch(
                 {{_, {_, _, <<"NOT_FOUND - no queue '",
                               ReplyTo:(byte_size(ReplyTo))/binary,
                               "' in vhost '/'">>}}, _},
                 Reason)
    end,

    %% Clean up.
    #'queue.delete_ok'{} = amqp_channel:call(RequesterCh,
                                             #'queue.delete'{queue = RequestQueue}),
    ok = rabbit_ct_client_helpers:close_channel(RequesterCh).

%% Test case for
%% https://github.com/rabbitmq/rabbitmq-server/discussions/11662
trace(Config) ->
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_on"]),

    Node = atom_to_binary(rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    TraceQueue = <<"trace-queue">>,
    RequestQueue = <<"request-queue">>,
    RequestPayload = <<"my-request">>,
    ReplyPayload = <<"my-reply">>,
    CorrelationId = <<"my-correlation-id">>,
    Qs = [RequestQueue, TraceQueue],
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    RequesterCh = rabbit_ct_client_helpers:open_channel(Config),
    ResponderCh = rabbit_ct_client_helpers:open_channel(Config),

    [#'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = Q0}) || Q0 <- Qs],
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{
                                  queue = TraceQueue,
                                  exchange = <<"amq.rabbitmq.trace">>,
                                  %% We subscribe only to messages entering RabbitMQ.
                                  routing_key = <<"publish.#">>}),

    %% There is no need to declare this pseudo queue first.
    amqp_channel:subscribe(RequesterCh,
                           #'basic.consume'{queue = ?REPLY_QUEUE,
                                            no_ack = true},
                           self()),
    CTag = receive #'basic.consume_ok'{consumer_tag = CTag0} -> CTag0
           end,
    #'confirm.select_ok'{} = amqp_channel:call(RequesterCh, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(RequesterCh, self()),

    %% Send the request.
    amqp_channel:cast(
      RequesterCh,
      #'basic.publish'{routing_key = RequestQueue},
      #amqp_msg{props = #'P_basic'{reply_to = ?REPLY_QUEUE,
                                   correlation_id = CorrelationId},
                payload = RequestPayload}),
    receive #'basic.ack'{} -> ok
    after ?TIMEOUT -> ct:fail(confirm_timeout)
    end,

    %% Receive the request.
    {#'basic.get_ok'{},
     #amqp_msg{props = #'P_basic'{reply_to = ReplyTo,
                                  correlation_id = CorrelationId},
               payload = RequestPayload}
    } = amqp_channel:call(ResponderCh, #'basic.get'{queue = RequestQueue}),

    %% Send the reply.
    amqp_channel:cast(
      ResponderCh,
      #'basic.publish'{routing_key = ReplyTo},
      #amqp_msg{props = #'P_basic'{correlation_id = CorrelationId},
                payload = ReplyPayload}),

    %% Receive the reply.
    receive {#'basic.deliver'{consumer_tag = CTag},
             #amqp_msg{payload = ReplyPayload,
                       props = #'P_basic'{correlation_id = CorrelationId}}} ->
                ok
    after ?TIMEOUT -> ct:fail(missing_reply)
    end,

    %% 2 messages should have entered RabbitMQ:
    %% 1. the RPC request
    %% 2. the RPC reply

    {#'basic.get_ok'{routing_key = <<"publish.">>},
     #amqp_msg{props = #'P_basic'{headers = RequestHeaders},
               payload = RequestPayload}
    } = amqp_channel:call(Ch, #'basic.get'{queue = TraceQueue}),
    ?assertMatch(#{
                   <<"exchange_name">> := <<>>,
                   <<"routing_keys">> := [RequestQueue],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Node,
                   <<"vhost">> := <<"/">>,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"correlation_id">> := CorrelationId},
                   <<"routed_queues">> := [RequestQueue]
                  },
                 rabbit_misc:amqp_table(RequestHeaders)),

    {#'basic.get_ok'{routing_key = <<"publish.">>},
     #amqp_msg{props = #'P_basic'{headers = ResponseHeaders},
               payload = ReplyPayload}
    } = amqp_channel:call(Ch, #'basic.get'{queue = TraceQueue}),
    ?assertMatch(#{
                   <<"exchange_name">> := <<>>,
                   <<"routing_keys">> := [<<"amq.rabbitmq.reply-to.", _/binary>>],
                   <<"connection">> := <<"127.0.0.1:", _/binary>>,
                   <<"node">> := Node,
                   <<"vhost">> := <<"/">>,
                   <<"user">> := <<"guest">>,
                   <<"properties">> := #{<<"correlation_id">> := CorrelationId},
                   <<"routed_queues">> := [<<"amq.rabbitmq.reply-to.", _/binary>>]
                  },
                 rabbit_misc:amqp_table(ResponseHeaders)),

    [#'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q0}) || Q0 <- Qs],
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_off"]).

%% A consumer must consume in no-ack mode.
failure_ack_mode(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Consume = #'basic.consume'{queue = ?REPLY_QUEUE,
                               no_ack = false},
    try amqp_channel:subscribe(Ch, Consume, self()) of
        _ ->
            ct:fail("expected subscribe in ack mode to fail")
    catch exit:Reason ->
              ?assertMatch(
                 {{_, {_, _, <<"PRECONDITION_FAILED - reply consumer cannot acknowledge">>}}, _},
                 Reason)
    end,
    ok = rabbit_ct_client_helpers:close_connection(Conn).

%% In AMQP 0.9.1 there can be at most one reply consumer per channel.
failure_multiple_consumers(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Consume = #'basic.consume'{queue = ?REPLY_QUEUE,
                               no_ack = true},
    amqp_channel:subscribe(Ch, Consume, self()),
    receive #'basic.consume_ok'{} -> ok
    end,

    try amqp_channel:subscribe(Ch, Consume, self()) of
        _ ->
            ct:fail("expected second subscribe to fail")
    catch exit:Reason ->
              ?assertMatch(
                 {{_, {_, _, <<"PRECONDITION_FAILED - reply consumer already set">>}}, _},
                 Reason)
    end,
    ok = rabbit_ct_client_helpers:close_connection(Conn).

%% Reusing the same consumer tag should fail.
failure_reuse_consumer_tag(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Ctag = <<"my-tag">>,

    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Ch, #'queue.declare'{exclusive = true}),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q,
                                                consumer_tag = Ctag}, self()),
    receive #'basic.consume_ok'{} -> ok
    end,

    try amqp_channel:subscribe(Ch, #'basic.consume'{queue = ?REPLY_QUEUE,
                                                    consumer_tag = Ctag,
                                                    no_ack = true}, self()) of
        _ ->
            ct:fail("expected reusing consumer tag to fail")
    catch exit:Reason ->
              ?assertMatch(
                 {{_, {connection_closing,
                       {_, _, <<"NOT_ALLOWED - attempt to reuse consumer tag 'my-tag'">>}
                      }}, _},
                 Reason)
    end.

%% Publishing with reply_to header set but without consuming from the pseudo queue should fail.
failure_publish(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),

    Ref = monitor(process, Ch),
    amqp_channel:cast(
      Ch,
      #'basic.publish'{routing_key = <<"some request queue">>},
      #amqp_msg{props = #'P_basic'{reply_to = ?REPLY_QUEUE,
                                   correlation_id = <<"some correlation ID">>},
                payload = <<"some payload">>}),

    receive {'DOWN', Ref, process, Ch, Reason} ->
                ?assertMatch(
                   {_, {_, _, <<"PRECONDITION_FAILED - fast reply consumer does not exist">>}},
                   Reason)
    after ?TIMEOUT ->
              ct:fail("expected channel error")
    end,
    ok = rabbit_ct_client_helpers:close_connection(Conn).

%% Test that Direct Reply-To works when the requester is an AMQP 0.9.1 client
%% and the responder is an AMQP 1.0 client.
amqpl_amqp_amqpl(Config) ->
    RequestQ = atom_to_binary(?FUNCTION_NAME),
    AddrRequestQ = rabbitmq_amqp_address:queue(RequestQ),
    Id = <<"🥕"/utf8>>,
    RequestPayload = <<"request payload">>,
    ReplyPayload = <<"reply payload">>,

    Chan = rabbit_ct_client_helpers:open_channel(Config),
    amqp_channel:subscribe(Chan, #'basic.consume'{queue = ?REPLY_QUEUE,
                                                  no_ack = true}, self()),
    CTag = receive #'basic.consume_ok'{consumer_tag = CTag0} -> CTag0
           end,

    %% Send the request via AMQP 0.9.1
    #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{queue = RequestQ}),
    amqp_channel:cast(Chan,
                      #'basic.publish'{routing_key = RequestQ},
                      #amqp_msg{props = #'P_basic'{reply_to = ?REPLY_QUEUE,
                                                   message_id = Id},
                                payload = RequestPayload}),

    %% Receive the request via AMQP 1.0.
    {_, Session, LinkPair} = Init = init(Config),
    {ok, Receiver} = amqp10_client:attach_receiver_link(
                       Session, <<"receiver">>, AddrRequestQ),
    {ok, RequestMsg} = amqp10_client:get_msg(Receiver, ?TIMEOUT),
    ?assertEqual(RequestPayload, amqp10_msg:body_bin(RequestMsg)),
    #{message_id := Id,
      reply_to := ReplyToAddr} = amqp10_msg:properties(RequestMsg),

    %% AMQP 1.0 responder checks whether the AMQP 0.9.1 requester is still there.
    {ok, #{queue := ReplyQ}} = rabbitmq_amqp_address:to_map(ReplyToAddr),
    ?assertMatch({ok, #{vhost := <<"/">>,
                        durable := false,
                        type := <<"rabbit_volatile_queue">>,
                        message_count := 0,
                        consumer_count := 1}},
                 rabbitmq_amqp_client:get_queue(LinkPair, ReplyQ)),

    %% Send the reply via AMQP 1.0.
    {ok, Sender} = amqp10_client:attach_sender_link_sync(
                     Session, <<"sender">>, ReplyToAddr),
    ok = wait_for_credit(Sender),
    ok = amqp10_client:send_msg(
           Sender,
           amqp10_msg:set_headers(
             #{priority => 3,
               durable => true},
             amqp10_msg:set_properties(
               #{message_id => <<"reply ID">>,
                 correlation_id => Id},
               amqp10_msg:set_application_properties(
                 #{<<"my key">> => <<"my val">>},
                 amqp10_msg:new(<<1>>, ReplyPayload, true))))),

    %% Receive the reply via AMQP 0.9.1
    receive {Deliver, #amqp_msg{payload = ReplyPayload,
                                props = #'P_basic'{headers = Headers} = Props}} ->
                ?assertMatch(#'basic.deliver'{
                                consumer_tag = CTag,
                                redelivered = false,
                                exchange = <<>>,
                                routing_key = <<"amq.rabbitmq.reply-to.", _/binary>>},
                             Deliver),
                ?assertMatch(#'P_basic'{
                                message_id = <<"reply ID">>,
                                correlation_id = Id,
                                priority = 3,
                                delivery_mode = 2},
                             Props),
                ?assertEqual({value, {<<"my key">>, longstr, <<"my val">>}},
                             lists:keysearch(<<"my key">>, 1, Headers))
    after ?TIMEOUT -> ct:fail(missing_reply)
    end,

    %% AMQP 0.9.1 requester cancels consumption.
    ?assertMatch(#'basic.cancel_ok'{consumer_tag = CTag},
                 amqp_channel:call(Chan, #'basic.cancel'{consumer_tag = CTag})),

    %% This time, when the AMQP 1.0 responder checks whether the AMQP 0.9.1 requester
    %% is still there, an error should be returned.
    {error, Resp} = rabbitmq_amqp_client:get_queue(LinkPair, ReplyQ),
    ?assertMatch(#{subject := <<"404">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"queue '", ReplyQ/binary, "' in vhost '/' not found">>}},
                 amqp10_msg:body(Resp)),

    #'queue.delete_ok'{} = amqp_channel:call(Chan, #'queue.delete'{queue = RequestQ}),
    ok = close(Init),
    ok = rabbit_ct_client_helpers:close_channel(Chan).

%% Test that Direct Reply-To works when the requester is an AMQP 1.0 client
%% and the responder is an AMQP 0.9.1 client.
amqp_amqpl_amqp(Config) ->
    RequestQ = atom_to_binary(?FUNCTION_NAME),
    AddrRequestQ = rabbitmq_amqp_address:queue(RequestQ),
    Id = <<"🥕"/utf8>>,
    RequestPayload = <<"request payload">>,
    ReplyPayload = <<"reply payload">>,

    Chan = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{queue = RequestQ}),

    OpnConf0 = connection_config(Config),
    OpnConf = OpnConf0#{container_id := <<"requester">>,
                        notify_with_performative => true},
    {ok, Conn} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Conn),
    Source = #{address => undefined,
               durable => none,
               expiry_policy => <<"link-detach">>,
               dynamic => true,
               capabilities => [<<"rabbitmq:volatile-queue">>]},
    AttachArgs = #{name => <<"receiver">>,
                   role => {receiver, Source, self()},
                   snd_settle_mode => settled,
                   rcv_settle_mode => first},
    {ok, Receiver} = amqp10_client:attach_link(Session, AttachArgs),
    AddrReplyQ = receive {amqp10_event, {link, Receiver, {attached, Attach}}} ->
                             #'v1_0.attach'{
                                source = #'v1_0.source'{
                                            address = {utf8, Addr},
                                            dynamic = true}} = Attach,
                             Addr
                 after 9000 -> ct:fail({missing_event, ?LINE})
                 end,
    ok = amqp10_client:flow_link_credit(Receiver, 1, never),

    %% Send the request via AMQP 1.0
    {ok, SenderRequester} = amqp10_client:attach_sender_link_sync(
                              Session, <<"sender">>, AddrRequestQ),
    ok = wait_for_credit(SenderRequester),
    ok = amqp10_client:send_msg(
           SenderRequester,
           amqp10_msg:set_properties(
             #{message_id => Id,
               reply_to => AddrReplyQ},
             amqp10_msg:new(<<"t1">>, RequestPayload))),
    receive {amqp10_disposition, {accepted, <<"t1">>}} -> ok
    after 9000 -> ct:fail({missing_event, ?LINE})
    end,

    %% Receive the request via AMQP 0.9.1
    {#'basic.get_ok'{},
     #amqp_msg{props = #'P_basic'{reply_to = ReplyQ,
                                  message_id = Id},
               payload = RequestPayload}
    } = amqp_channel:call(Chan, #'basic.get'{queue = RequestQ}),

    %% Test what the docs state:
    %% "If the RPC server is going to perform some expensive computation it might wish
    %% to check if the client has gone away. To do this the server can declare the
    %% generated reply name first on a disposable channel in order to determine whether
    %% it still exists."
    ?assertEqual(#'queue.declare_ok'{queue = ReplyQ,
                                     message_count = 0,
                                     consumer_count = 1},
                 amqp_channel:call(Chan, #'queue.declare'{queue = ReplyQ})),

    %% Send the reply via AMQP 0.9.1
    amqp_channel:cast(
      Chan,
      #'basic.publish'{routing_key = ReplyQ},
      #amqp_msg{props = #'P_basic'{message_id = <<"msg ID reply">>,
                                   correlation_id = Id},
                payload = ReplyPayload}),

    %% Receive the reply via AMQP 1.0
    receive {amqp10_msg, Receiver, Reply} ->
                ?assertEqual(ReplyPayload,
                             amqp10_msg:body_bin(Reply)),
                ?assertMatch(#{message_id := <<"msg ID reply">>,
                               correlation_id := Id},
                             amqp10_msg:properties(Reply))
    after 9000 -> ct:fail({missing_msg, ?LINE})
    end,

    ok = end_session_sync(Session),
    ok = close_connection_sync(Conn),
    #'queue.delete_ok'{} = amqp_channel:call(Chan, #'queue.delete'{queue = RequestQ}),

    %% AMQP 0.9.1 responder checks again if the AMQP 1.0 requester is still there.
    %% This time, the requester and its queue should be gone.
    try amqp_channel:call(Chan, #'queue.declare'{queue = ReplyQ}) of
        _ ->
            ct:fail("expected queue.declare to fail")
    catch exit:Reason ->
              ?assertMatch(
                 {{_, {_, _, <<"NOT_FOUND - no queue '",
                               ReplyQ:(byte_size(ReplyQ))/binary,
                               "' in vhost '/'">>}}, _},
                 Reason)
    end.

%% "new" and "old" refers to new and old RabbitMQ versions in mixed version tests.
rpc_new_to_old_node(Config) ->
    rpc(0, 1, Config).

rpc_old_to_new_node(Config) ->
    rpc(1, 0, Config).

rpc(RequesterNode, ResponderNode, Config) ->
    RequestQueue = <<"request queue">>,
    RequestPayload = <<"my request">>,
    CorrelationId = <<"my correlation ID">>,
    RequesterCh = rabbit_ct_client_helpers:open_channel(Config, RequesterNode),
    ResponderCh = rabbit_ct_client_helpers:open_channel(Config, ResponderNode),

    %% There is no need to declare this pseudo queue first.
    amqp_channel:subscribe(RequesterCh,
                           #'basic.consume'{queue = ?REPLY_QUEUE,
                                            no_ack = true},
                           self()),
    CTag = receive #'basic.consume_ok'{consumer_tag = CTag0} -> CTag0
           end,

    ?assertEqual(#'queue.declare_ok'{queue = ?REPLY_QUEUE,
                                     message_count = 0,
                                     consumer_count = 1},
                 amqp_channel:call(RequesterCh,
                                   #'queue.declare'{queue = ?REPLY_QUEUE})),

    #'queue.declare_ok'{} = amqp_channel:call(
                              RequesterCh,
                              #'queue.declare'{queue = RequestQueue}),
    #'confirm.select_ok'{} = amqp_channel:call(RequesterCh, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(RequesterCh, self()),

    %% Send the request.
    amqp_channel:cast(
      RequesterCh,
      #'basic.publish'{routing_key = RequestQueue},
      #amqp_msg{props = #'P_basic'{reply_to = ?REPLY_QUEUE,
                                   correlation_id = CorrelationId},
                payload = RequestPayload}),
    receive #'basic.ack'{} -> ok
    end,

    ok = wait_for_queue_declared(RequestQueue, ResponderNode, Config),
    %% Receive the request.
    {#'basic.get_ok'{},
     #amqp_msg{props = #'P_basic'{reply_to = ReplyTo,
                                  correlation_id = CorrelationId},
               payload = RequestPayload}
    } = amqp_channel:call(ResponderCh, #'basic.get'{queue = RequestQueue}),

    %% Test what the docs state:
    %% "If the RPC server is going to perform some expensive computation it might wish
    %% to check if the client has gone away. To do this the server can declare the
    %% generated reply name first on a disposable channel in order to determine whether
    %% it still exists."
    ?assertEqual(#'queue.declare_ok'{queue = ReplyTo,
                                     message_count = 0,
                                     consumer_count = 1},
                 amqp_channel:call(ResponderCh,
                                   #'queue.declare'{queue = ReplyTo})),

    %% Send the reply.
    amqp_channel:cast(
      ResponderCh,
      #'basic.publish'{routing_key = ReplyTo},
      #amqp_msg{props = #'P_basic'{correlation_id = CorrelationId},
                payload = <<"reply 1">>}),

    %% Let's assume the RPC server sends multiple replies for a single request.
    %% Setting the reply address in CC should work.
    amqp_channel:cast(
      ResponderCh,
      #'basic.publish'{routing_key = <<"nowhere">>},
      #amqp_msg{props = #'P_basic'{headers = [{<<"CC">>, array, [{longstr, ReplyTo}]}],
                                   correlation_id = CorrelationId},
                payload = <<"reply 2">>}),

    %% Receive the frst reply.
    receive {#'basic.deliver'{consumer_tag = CTag,
                              redelivered = false,
                              exchange = <<>>,
                              routing_key = ReplyTo},
             #amqp_msg{payload = P1,
                       props = #'P_basic'{correlation_id = CorrelationId}}} ->
                ?assertEqual(<<"reply 1">>, P1)
    after ?TIMEOUT -> ct:fail({missing_reply, ?LINE})
    end,

    %% Receive the second reply.
    receive {#'basic.deliver'{consumer_tag = CTag},
             #amqp_msg{payload = P2,
                       props = #'P_basic'{correlation_id = CorrelationId}}} ->
                ?assertEqual(<<"reply 2">>, P2)
    after ?TIMEOUT -> ct:fail({missing_reply, ?LINE})
    end,

    %% The requester sends a reply to itself.
    %% (Odd, but should work.)
    amqp_channel:cast(
      RequesterCh,
      #'basic.publish'{routing_key = ReplyTo},
      #amqp_msg{props = #'P_basic'{correlation_id = CorrelationId},
                payload = <<"reply 3">>}),
    %% We expect to receive a publisher confirm.
    receive #'basic.ack'{} -> ok
    end,

    receive {#'basic.deliver'{consumer_tag = CTag},
             #amqp_msg{payload = P3,
                       props = #'P_basic'{correlation_id = CorrelationId}}} ->
                ?assertEqual(<<"reply 3">>, P3)
    after ?TIMEOUT -> ct:fail({missing_reply, ?LINE})
    end,

    %% Requester cancels consumption.
    ?assertMatch(#'basic.cancel_ok'{consumer_tag = CTag},
                 amqp_channel:call(RequesterCh, #'basic.cancel'{consumer_tag = CTag})),

    %% Send a final reply.
    amqp_channel:cast(
      ResponderCh,
      #'basic.publish'{routing_key = ReplyTo},
      #amqp_msg{props = #'P_basic'{correlation_id = CorrelationId},
                payload = <<"reply 4">>}),

    %% The final reply shouldn't be delivered since the requester cancelled consumption.
    receive {#'basic.deliver'{}, #amqp_msg{}} ->
                ct:fail("did not expect delivery after cancellation")
    after 100 -> ok
    end,

    %% Responder checks again if the requester is still there.
    %% This time, the requester and its queue should be gone.
    try amqp_channel:call(ResponderCh, #'queue.declare'{queue = ReplyTo}) of
        _ ->
            ct:fail("expected queue.declare to fail")
    catch exit:Reason ->
              ?assertMatch(
                 {{_, {_, _, <<"NOT_FOUND - no queue '",
                               ReplyTo:(byte_size(ReplyTo))/binary,
                               "' in vhost '/'">>}}, _},
                 Reason)
    end,

    %% Clean up.
    #'queue.delete_ok'{} = amqp_channel:call(RequesterCh,
                                             #'queue.delete'{queue = RequestQueue}),
    ok = rabbit_ct_client_helpers:close_channel(RequesterCh).

wait_for_queue_declared(Queue, Node, Config) ->
    eventually(
      ?_assert(
         begin
             Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
             #'queue.declare_ok'{} = amqp_channel:call(
                                       Ch, #'queue.declare'{queue = Queue,
                                                            passive = true}),
             rabbit_ct_client_helpers:close_channel(Ch),
             true
         end)).
