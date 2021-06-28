%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(priority_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, single_node}
    ].

groups() ->
    [
     {single_node, [], [
                         ackfold,
                         drop,
                         {overflow_reject_publish, [], [reject]},
                         {overflow_reject_publish_dlx, [], [reject]},
                         dropwhile_fetchwhile,
                         info_head_message_timestamp,
                         matching,
                         purge,
                         requeue,
                         resume,
                         simple_order,
                         straight_through,
                         invoke,
                         gen_server2_stats,
                         negative_max_priorities,
                         max_priorities_above_hard_limit
                        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(single_node, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 1},
        {rmq_nodename_suffix, Suffix}
    ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_group(overflow_reject_publish, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {overflow, <<"reject-publish">>}
      ]);
init_per_group(overflow_reject_publish_dlx, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {overflow, <<"reject-publish-dlx">>}
      ]).

end_per_group(overflow_reject_publish, _Config) ->
    ok;
end_per_group(overflow_reject_publish_dlx, _Config) ->
    ok;
end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:setup_steps(),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:teardown_steps(),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% The BQ API is used in all sorts of places in all sorts of
%% ways. Therefore we have to jump through a few different hoops
%% in order to integration-test it.
%%
%% * start/1, stop/0, init/3, terminate/2, delete_and_terminate/2
%%   - starting and stopping rabbit. durable queues / persistent msgs needed
%%     to test recovery
%%
%% * publish/5, drain_confirmed/1, fetch/2, ack/2, is_duplicate/2, msg_rates/1,
%%   needs_timeout/1, timeout/1, invoke/3, resume/1 [0]
%%   - regular publishing and consuming, with confirms and acks and durability
%%
%% * publish_delivered/4    - publish with acks straight through
%% * discard/3              - publish without acks straight through
%% * dropwhile/2            - expire messages without DLX
%% * fetchwhile/4           - expire messages with DLX
%% * ackfold/4              - reject messages with DLX
%% * requeue/2              - reject messages without DLX
%% * drop/2                 - maxlen messages without DLX
%% * purge/1                - issue AMQP queue.purge
%% * purge_acks/1           - mirror queue explicit sync with unacked msgs
%% * fold/3                 - mirror queue explicit sync
%% * depth/1                - mirror queue implicit sync detection
%% * len/1, is_empty/1      - info items
%% * handle_pre_hibernate/1 - hibernation
%%
%% * set_ram_duration_target/2, ram_duration/1, status/1
%%   - maybe need unit testing?
%%
%% [0] publish enough to get credit flow from msg store

simple_order(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"simple_order-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    get_all(Ch, Q, do_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    publish(Ch, Q, [2, 3, 1, 2, 3, 1, 2, 3, 1]),
    get_all(Ch, Q, no_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    publish(Ch, Q, [3, 1, 2, 3, 1, 2, 3, 1, 2]),
    get_all(Ch, Q, do_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

matching(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"matching-queue">>,
    declare(Ch, Q, 5),
    %% We round priority down, and 0 is the default
    publish(Ch, Q, [undefined, 0, 5, 10, undefined]),
    get_all(Ch, Q, do_ack, [5, 10, undefined, 0, undefined]),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

resume(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"resume-queue">>,
    declare(Ch, Q, 5),
    amqp_channel:call(Ch, #'confirm.select'{}),
    publish_many(Ch, Q, 10000),
    amqp_channel:wait_for_confirms(Ch),
    amqp_channel:call(Ch, #'queue.purge'{queue = Q}), %% Assert it exists
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

straight_through(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"straight_through-queue">>,
    declare(Ch, Q, 3),
    [begin
         consume(Ch, Q, Ack),
         [begin
              publish1(Ch, Q, P),
              assert_delivered(Ch, Ack, P)
          end || P <- [1, 2, 3]],
         cancel(Ch)
     end || Ack <- [do_ack, no_ack]],
    get_empty(Ch, Q),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

max_priorities_above_hard_limit(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"max_priorities_above_hard_limit">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       %% Note that lower values (e.g. 300) will overflow the byte type here.
       %% However, values >= 256 would still be rejected when used by
       %% other clients
       declare(Ch, Q, 3000)),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

negative_max_priorities(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"negative_max_priorities">>,
    ?assertExit(
       {{shutdown, {server_initiated_close, 406, _}}, _},
       declare(Ch, Q, -10)),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.


invoke(Config) ->
    %% Synthetic test to check the invoke callback, as the bug tested here
    %% is only triggered with a race condition.
    %% When mirroring is stopped, the backing queue of rabbit_amqqueue_process
    %% changes from rabbit_mirror_queue_master to rabbit_priority_queue,
    %% which shouldn't receive any invoke call. However, there might
    %% be pending messages so the priority queue receives the
    %% `run_backing_queue` cast message sent to the old master.
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    Q = <<"invoke-queue">>,
    declare(Ch, Q, 3),
    Pid = queue_pid(Config, A, rabbit_misc:r(<<"/">>, queue, Q)),
    rabbit_ct_broker_helpers:rpc(
      Config, A, gen_server, cast,
      [Pid,
       {run_backing_queue, ?MODULE, fun(_, _) -> ok end}]),
    Pid2 = queue_pid(Config, A, rabbit_misc:r(<<"/">>, queue, Q)),
    Pid = Pid2,
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.


gen_server2_stats(Config) ->
    %% Synthetic test to check the invoke callback, as the bug tested here
    %% is only triggered with a race condition.
    %% When mirroring is stopped, the backing queue of rabbit_amqqueue_process
    %% changes from rabbit_mirror_queue_master to rabbit_priority_queue,
    %% which shouldn't receive any invoke call. However, there might
    %% be pending messages so the priority queue receives the
    %% `run_backing_queue` cast message sent to the old master.
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, A),
    Q = <<"gen_server2_stats_queue">>,
    declare(Ch, Q, 3),
    Pid = queue_pid(Config, A, rabbit_misc:r(<<"/">>, queue, Q)),
    Metrics = rabbit_ct_broker_helpers:rpc(
                Config, A, rabbit_core_metrics, get_gen_server2_stats,
                [Pid]),
    true = is_number(Metrics),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

dropwhile_fetchwhile(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"dropwhile_fetchwhile-queue">>,
    [begin
         declare(Ch, Q, Args ++ arguments(3)),
         publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
         timer:sleep(10),
         get_empty(Ch, Q),
         delete(Ch, Q)
     end ||
        Args <- [[{<<"x-message-ttl">>, long, 1}],
                 [{<<"x-message-ttl">>,          long,    1},
                  {<<"x-dead-letter-exchange">>, longstr, <<"amq.fanout">>}]
                ]],
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

ackfold(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"ackfolq-queue1">>,
    Q2 = <<"ackfold-queue2">>,
    declare(Ch, Q,
            [{<<"x-dead-letter-exchange">>, longstr, <<>>},
             {<<"x-dead-letter-routing-key">>, longstr, Q2}
             | arguments(3)]),
    declare(Ch, Q2, none),
    publish(Ch, Q, [1, 2, 3]),
    [_, _, DTag] = get_all(Ch, Q, manual_ack, [3, 2, 1]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DTag,
                                        multiple     = true,
                                        requeue      = false}),
    timer:sleep(100),
    get_all(Ch, Q2, do_ack, [3, 2, 1]),
    delete(Ch, Q),
    delete(Ch, Q2),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

requeue(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"requeue-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [1, 2, 3]),
    [_, _, DTag] = get_all(Ch, Q, manual_ack, [3, 2, 1]),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DTag,
                                        multiple     = true,
                                        requeue      = true}),
    get_all(Ch, Q, do_ack, [3, 2, 1]),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

drop(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"drop-queue">>,
    declare(Ch, Q, [{<<"x-max-length">>, long, 4} | arguments(3)]),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    %% We drop from the head, so this is according to the "spec" even
    %% if not likely to be what the user wants.
    get_all(Ch, Q, do_ack, [2, 1, 1, 1]),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

reject(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    XOverflow = ?config(overflow, Config),
    Q = <<"reject-queue-", XOverflow/binary>>,
    declare(Ch, Q, [{<<"x-max-length">>, long, 4},
                    {<<"x-overflow">>, longstr, XOverflow}
                    | arguments(3)]),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    %% First 4 messages are published, all others are discarded.
    get_all(Ch, Q, do_ack, [3, 2, 1, 1]),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

purge(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"purge-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [1, 2, 3]),
    amqp_channel:call(Ch, #'queue.purge'{queue = Q}),
    get_empty(Ch, Q),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

info_head_message_timestamp(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, info_head_message_timestamp1, [Config]).

info_head_message_timestamp1(_Config) ->
    QName = rabbit_misc:r(<<"/">>, queue,
      <<"info_head_message_timestamp-queue">>),
    Q0 = rabbit_amqqueue:pseudo_queue(QName, self()),
    Q1 = amqqueue:set_arguments(Q0, [{<<"x-max-priority">>, long, 2}]),
    PQ = rabbit_priority_queue,
    BQS1 = PQ:init(Q1, new, fun(_, _) -> ok end),
    %% The queue is empty: no timestamp.
    true = PQ:is_empty(BQS1),
    '' = PQ:info(head_message_timestamp, BQS1),
    %% Publish one message with timestamp 1000.
    Msg1 = #basic_message{
      id = msg1,
      content = #content{
        properties = #'P_basic'{
          priority = 1,
          timestamp = 1000
        }},
      is_persistent = false
    },
    BQS2 = PQ:publish(Msg1, #message_properties{size = 0}, false, self(),
      noflow, BQS1),
    1000 = PQ:info(head_message_timestamp, BQS2),
    %% Publish a higher priority message with no timestamp.
    Msg2 = #basic_message{
      id = msg2,
      content = #content{
        properties = #'P_basic'{
          priority = 2
        }},
      is_persistent = false
    },
    BQS3 = PQ:publish(Msg2, #message_properties{size = 0}, false, self(),
      noflow, BQS2),
    '' = PQ:info(head_message_timestamp, BQS3),
    %% Consume message with no timestamp.
    {{Msg2, _, _}, BQS4} = PQ:fetch(false, BQS3),
    1000 = PQ:info(head_message_timestamp, BQS4),
    %% Consume message with timestamp 1000, but do not acknowledge it
    %% yet. The goal is to verify that the unacknowledged message's
    %% timestamp is returned.
    {{Msg1, _, AckTag}, BQS5} = PQ:fetch(true, BQS4),
    1000 = PQ:info(head_message_timestamp, BQS5),
    %% Ack message. The queue is empty now.
    {[msg1], BQS6} = PQ:ack([AckTag], BQS5),
    true = PQ:is_empty(BQS6),
    '' = PQ:info(head_message_timestamp, BQS6),
    PQ:delete_and_terminate(a_whim, BQS6),
    passed.

ram_duration(_Config) ->
    QName = rabbit_misc:r(<<"/">>, queue, <<"ram_duration-queue">>),
    Q0 = rabbit_amqqueue:pseudo_queue(QName, self()),
    Q1 = amqqueue:set_arguments(Q0, [{<<"x-max-priority">>, long, 5}]),
    PQ = rabbit_priority_queue,
    BQS1 = PQ:init(Q1, new, fun(_, _) -> ok end),
    {_Duration1, BQS2} = PQ:ram_duration(BQS1),
    BQS3 = PQ:set_ram_duration_target(infinity, BQS2),
    BQS4 = PQ:set_ram_duration_target(1, BQS3),
    {_Duration2, BQS5} = PQ:ram_duration(BQS4),
    PQ:delete_and_terminate(a_whim, BQS5),
    passed.

%%----------------------------------------------------------------------------

declare(Ch, Q, Args) when is_list(Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           arguments = Args});
declare(Ch, Q, Max) ->
    declare(Ch, Q, arguments(Max)).

delete(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish(Ch, Q, Ps) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [publish1(Ch, Q, P) || P <- Ps],
    amqp_channel:wait_for_confirms(Ch).

publish_payload(Ch, Q, PPds) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [publish1(Ch, Q, P, Pd) || {P, Pd} <- PPds],
    amqp_channel:wait_for_confirms(Ch).

publish_many(_Ch, _Q, 0) -> ok;
publish_many( Ch,  Q, N) -> publish1(Ch, Q, rand:uniform(5)),
                            publish_many(Ch, Q, N - 1).

publish1(Ch, Q, P) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = priority2bin(P)}).

publish1(Ch, Q, P, Pd) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = Pd}).

props(undefined) -> #'P_basic'{delivery_mode = 2};
props(P)         -> #'P_basic'{priority      = P,
                               delivery_mode = 2}.

consume(Ch, Q, Ack) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue        = Q,
                                                no_ack       = Ack =:= no_ack,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end.

cancel(Ch) ->
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = <<"ctag">>}).

assert_delivered(Ch, Ack, P) ->
    PBin = priority2bin(P),
    receive
        {#'basic.deliver'{delivery_tag = DTag}, #amqp_msg{payload = PBin2}} ->
            PBin = PBin2,
            maybe_ack(Ch, Ack, DTag)
    end.

get_all(Ch, Q, Ack, Ps) ->
    DTags = get_partial(Ch, Q, Ack, Ps),
    get_empty(Ch, Q),
    DTags.

get_partial(Ch, Q, Ack, Ps) ->
    [get_ok(Ch, Q, Ack, priority2bin(P)) || P <- Ps].

get_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = Q}).

get_ok(Ch, Q, Ack, PBin) ->
    {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = PBin2}} =
        amqp_channel:call(Ch, #'basic.get'{queue  = Q,
                                           no_ack = Ack =:= no_ack}),
    ?assertEqual(PBin, PBin2),
    maybe_ack(Ch, Ack, DTag).

get_payload(Ch, Q, Ack, Ps) ->
    [get_ok(Ch, Q, Ack, P) || P <- Ps].

get_without_ack(Ch, Q) ->
    {#'basic.get_ok'{}, _} =
        amqp_channel:call(Ch, #'basic.get'{queue  = Q, no_ack = false}).

maybe_ack(Ch, do_ack, DTag) ->
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag}),
    DTag;
maybe_ack(_Ch, _, DTag) ->
    DTag.

arguments(none) -> [];
arguments(Max)  -> [{<<"x-max-priority">>, byte, Max}].

priority2bin(undefined) -> <<"undefined">>;
priority2bin(Int)       -> list_to_binary(integer_to_list(Int)).

%%----------------------------------------------------------------------------


queue_pid(Config, Nodename, Q) ->
    Info = rabbit_ct_broker_helpers:rpc(
             Config, Nodename,
             rabbit_amqqueue, info_all, [<<"/">>, [name, pid]]),
    [Pid] = [P || [{name, Q1}, {pid, P}] <- Info, Q =:= Q1],
    Pid.

%%----------------------------------------------------------------------------
