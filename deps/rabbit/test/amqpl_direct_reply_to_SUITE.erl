%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(amqpl_direct_reply_to_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all,
          export_all]).

-import(rabbit_ct_helpers, [eventually/1]).

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [shuffle],
      [
       trace
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
    %% This is the pseudo queue that is specially interpreted by RabbitMQ.
    ReplyQueue = <<"amq.rabbitmq.reply-to">>,
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
                           #'basic.consume'{queue = ReplyQueue,
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
      #amqp_msg{props = #'P_basic'{reply_to = ReplyQueue,
                                   correlation_id = CorrelationId},
                payload = RequestPayload}),
    receive #'basic.ack'{} -> ok
    after 5000 -> ct:fail(confirm_timeout)
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
    after 5000 -> ct:fail(missing_reply)
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

%% "new" and "old" refers to new and old RabbitMQ versions in mixed version tests.
rpc_new_to_old_node(Config) ->
    rpc(0, 1, Config).

rpc_old_to_new_node(Config) ->
    rpc(1, 0, Config).

rpc(RequesterNode, ResponderNode, Config) ->
    RequestQueue = <<"tests.amqpl_direct_reply_to.rpc.requests">>,
    %% This is the pseudo queue that is specially interpreted by RabbitMQ.
    ReplyQueue = <<"amq.rabbitmq.reply-to">>,
    RequestPayload = <<"my request">>,
    ReplyPayload = <<"my reply">>,
    CorrelationId = <<"my correlation ID">>,
    RequesterCh = rabbit_ct_client_helpers:open_channel(Config, RequesterNode),
    ResponderCh = rabbit_ct_client_helpers:open_channel(Config, ResponderNode),

    %% There is no need to declare this pseudo queue first.
    amqp_channel:subscribe(RequesterCh,
                           #'basic.consume'{queue = ReplyQueue,
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
      #amqp_msg{props = #'P_basic'{reply_to = ReplyQueue,
                                   correlation_id = CorrelationId},
                payload = RequestPayload}),
    receive #'basic.ack'{} -> ok
    after 5000 -> ct:fail(confirm_timeout)
    end,

    ok = wait_for_queue_declared(RequestQueue, ResponderNode, Config),
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
    after 5000 -> ct:fail(missing_reply)
    end.

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
