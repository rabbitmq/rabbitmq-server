%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_queue_interceptor_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).

all() ->
    [
     {group, cluster_size_1}
    ].

groups() ->
    [
     {cluster_size_1, [],
      [registry_validation,
       classic_queue,
       quorum_queue]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config0, [{rmq_nodename_suffix, Testcase}]),
    Config = rabbit_ct_helpers:run_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    %% The test interceptor callbacks live in this suite module, which must be
    %% on the broker node's code path and loaded for add/1's function_exported/3
    %% check to pass.
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config, ?MODULE),
    {module, ?MODULE} = rabbit_ct_broker_helpers:rpc(
                          Config, 0, code, ensure_loaded, [?MODULE]),
    Config.

end_per_testcase(Testcase, Config0) ->
    Config = rabbit_ct_helpers:testcase_finished(Config0, Testcase),
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

%% Test interceptor callbacks. Run on the broker node and forward every
%% queue-boundary event to the test process identified by the config map.
intercept_enqueue(QName, _Message, #{reply_to := Pid}) ->
    Pid ! {enqueue_event, QName},
    ok.

intercept_settle(QName, Op, CTag, MsgIds, #{reply_to := Pid}) ->
    Pid ! {settle_event, QName, Op, CTag, MsgIds},
    ok.

registry_validation(Config) ->
    %% A module that implements at least one stage is accepted, then removed.
    ?assertEqual(ok, add(Config, [{?MODULE, #{}}])),
    ?assertEqual(ok, remove(Config, [{?MODULE, #{}}])),
    %% A module that implements neither stage is rejected.
    ok = try
             add(Config, [{nonexistent_queue_interceptor, #{}}]),
             ct:fail(expected_validation_error)
         catch
             _:_ ->
                 ok
         end,
    %% With no interceptors registered the gate is closed and both stages are
    %% harmless no-ops.
    ?assertEqual(false,
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_queue_interceptor, enabled, [])),
    QName = rabbit_misc:r(<<"/">>, queue, <<"no-such-queue">>),
    ?assertEqual(ok,
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_queue_interceptor, enqueue,
                   [QName, undefined])),
    ?assertEqual(ok,
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_queue_interceptor, settle,
                   [QName, complete, <<"ctag">>, [0]])),
    ok.

classic_queue(Config) ->
    enqueue_and_settle(Config, <<"classic">>).

quorum_queue(Config) ->
    enqueue_and_settle(Config, <<"quorum">>).

enqueue_and_settle(Config, QueueType) ->
    %% Entry hook: a published message is captured at the queue boundary.
    assert_enqueue(Config, QueueType),
    %% Exit hook: acknowledging, requeueing and discarding each fire once,
    %% with the matching settlement reason.
    assert_settle_op(Config, QueueType, ack, complete),
    assert_settle_op(Config, QueueType, {nack, true}, requeue),
    assert_settle_op(Config, QueueType, {reject, false}, discard),
    ok.

assert_enqueue(Config, QueueType) ->
    QName = iolist_to_binary(
              [atom_to_binary(?FUNCTION_NAME), $-, QueueType]),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    declare_queue(Ch, QName, QueueType),

    %% The interceptor must be registered before publishing for enqueue to fire.
    ok = add(Config, [{?MODULE, #{reply_to => self()}}]),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName},
                      #amqp_msg{payload = <<"msg">>}),
    receive
        {enqueue_event, EventQName} ->
            ?assertEqual(QName, EventQName#resource.name)
    after 5000 ->
              ct:fail(missing_enqueue_event)
    end,

    ok = remove(Config, [{?MODULE, #{reply_to => self()}}]),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

assert_settle_op(Config, QueueType, ClientAction, ExpectedOp) ->
    QName = iolist_to_binary(
              [atom_to_binary(?FUNCTION_NAME), $-, QueueType, $-,
               op_suffix(ClientAction)]),
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    declare_queue(Ch, QName, QueueType),
    %% Publish before registering, so only the settlement (not the enqueue) is
    %% observed by this scenario.
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName},
                      #amqp_msg{payload = <<"msg">>}),

    ok = add(Config, [{?MODULE, #{reply_to => self()}}]),

    %% basic.get (rather than a subscription) keeps the test deterministic: a
    %% requeued message is not redelivered into this process' mailbox.
    DeliveryTag = basic_get(Ch, QName, 100),
    Method = case ClientAction of
                 ack ->
                     #'basic.ack'{delivery_tag = DeliveryTag};
                 {nack, Requeue} ->
                     #'basic.nack'{delivery_tag = DeliveryTag, requeue = Requeue};
                 {reject, Requeue} ->
                     #'basic.reject'{delivery_tag = DeliveryTag, requeue = Requeue}
             end,
    ok = amqp_channel:cast(Ch, Method),

    receive
        {settle_event, EventQName, Op, _CTag, MsgIds} ->
            ?assertEqual(QName, EventQName#resource.name),
            ?assertEqual(ExpectedOp, Op),
            ?assertMatch([_], MsgIds)
    after 5000 ->
              ct:fail({missing_settle_event, ExpectedOp})
    end,

    ok = remove(Config, [{?MODULE, #{reply_to => self()}}]),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

declare_queue(Ch, QName, QueueType) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(
          Ch, #'queue.declare'{queue = QName,
                               durable = true,
                               arguments = [{<<"x-queue-type">>, longstr, QueueType}]}).

op_suffix(ack) -> <<"ack">>;
op_suffix({nack, _}) -> <<"nack">>;
op_suffix({reject, _}) -> <<"reject">>.

basic_get(_Ch, QName, 0) ->
    ct:fail({no_message, QName});
basic_get(Ch, QName, Retries) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = QName, no_ack = false}) of
        {#'basic.get_ok'{delivery_tag = DeliveryTag}, _Content} ->
            DeliveryTag;
        #'basic.get_empty'{} ->
            timer:sleep(50),
            basic_get(Ch, QName, Retries - 1)
    end.

add(Config, Interceptors) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_queue_interceptor, add, [Interceptors]).

remove(Config, Interceptors) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_queue_interceptor, remove, [Interceptors]).
