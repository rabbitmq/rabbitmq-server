%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqpl_direct_reply_to_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_ct_helpers, [eventually/1]).

-define(TIMEOUT, 30_000).

%% This is the pseudo queue that is specially interpreted by RabbitMQ.
-define(REPLY_QUEUE, <<"amq.rabbitmq.reply-to">>).

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       trace,
       failure_ack_mode,
       failure_multiple_consumers,
       failure_reuse_consumer_tag,
       failure_publish
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
    rabbit_ct_helpers:run_setup_steps(
      Config1,
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

%% Test case for
%% https://github.com/rabbitmq/rabbitmq-server/discussions/11662
trace(Config) ->
    {ok, _} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["trace_on"]),

    Node = atom_to_binary(rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    TraceQueue = <<"tests.amqpl_direct_reply_to.trace.tracing">>,
    RequestQueue = <<"tests.amqpl_direct_reply_to.trace.requests">>,
    RequestPayload = <<"my request">>,
    ReplyPayload = <<"my reply">>,
    CorrelationId = <<"my correlation ID">>,
    Qs = [RequestQueue, TraceQueue],
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    RequesterCh = rabbit_ct_client_helpers:open_channel(Config, 0),
    ResponderCh = rabbit_ct_client_helpers:open_channel(Config, 0),

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
    %% (This is a bit unusual but should work.)
    amqp_channel:cast(
      ResponderCh,
      #'basic.publish'{routing_key = ReplyTo},
      #amqp_msg{props = #'P_basic'{correlation_id = CorrelationId},
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
    %% (Really odd, but should work.)
    amqp_channel:cast(
      RequesterCh,
      #'basic.publish'{routing_key = ReplyTo},
      #amqp_msg{props = #'P_basic'{correlation_id = CorrelationId},
                payload = <<"reply 3">>}),

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
