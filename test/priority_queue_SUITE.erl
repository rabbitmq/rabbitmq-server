%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(priority_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, cluster_size_2},
      {group, cluster_size_3}
    ].

groups() ->
    [
      {cluster_size_2, [], [
          {parallel_tests, [parallel], [
              ackfold,
              drop,
              dropwhile_fetchwhile,
              info_head_message_timestamp,
              matching,
              mirror_queue_sync,
              mirror_queue_sync_priority_above_max,
              mirror_queue_sync_priority_above_max_pending_ack,
              purge,
              requeue,
              resume,
              simple_order,
              straight_through
            ]},
          {non_parallel_tests, [], [
              recovery %% Restart RabbitMQ.
            ]}
        ]},
      {cluster_size_3, [], [
          {parallel_tests, [parallel], [
              mirror_queue_auto_ack
            ]}
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

init_per_group(cluster_size_2, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 2}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_group(cluster_size_3, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 3}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_group(_, Config) ->
    Config.

end_per_group(ClusterSizeGroup, Config)
when ClusterSizeGroup =:= cluster_size_2
orelse ClusterSizeGroup =:= cluster_size_3 ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
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

recovery(Config) ->
    {Conn, Ch} = open(Config),
    Q = <<"recovery-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    amqp_connection:close(Conn),

    %% TODO This terminates the automatically open connection and breaks
    %% coverage.
    rabbit_ct_broker_helpers:restart_broker(Config, 0),

    {Conn2, Ch2} = open(Config),
    get_all(Ch2, Q, do_ack, [3, 3, 3, 2, 2, 2, 1, 1, 1]),
    delete(Ch2, Q),
    amqp_connection:close(Conn2),
    passed.

simple_order(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
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
    passed.

matching(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"matching-queue">>,
    declare(Ch, Q, 5),
    %% We round priority down, and 0 is the default
    publish(Ch, Q, [undefined, 0, 5, 10, undefined]),
    get_all(Ch, Q, do_ack, [5, 10, undefined, 0, undefined]),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    passed.

resume(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"resume-queue">>,
    declare(Ch, Q, 5),
    amqp_channel:call(Ch, #'confirm.select'{}),
    publish_many(Ch, Q, 10000),
    amqp_channel:wait_for_confirms(Ch),
    amqp_channel:call(Ch, #'queue.purge'{queue = Q}), %% Assert it exists
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    passed.

straight_through(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
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
    passed.

dropwhile_fetchwhile(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
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
    passed.

ackfold(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
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
    passed.

requeue(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
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
    passed.

drop(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"drop-queue">>,
    declare(Ch, Q, [{<<"x-max-length">>, long, 4} | arguments(3)]),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3, 1, 2, 3]),
    %% We drop from the head, so this is according to the "spec" even
    %% if not likely to be what the user wants.
    get_all(Ch, Q, do_ack, [2, 1, 1, 1]),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    passed.

purge(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"purge-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [1, 2, 3]),
    amqp_channel:call(Ch, #'queue.purge'{queue = Q}),
    get_empty(Ch, Q),
    delete(Ch, Q),
    rabbit_ct_client_helpers:close_channel(Ch),
    passed.

info_head_message_timestamp(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, info_head_message_timestamp1, [Config]).

info_head_message_timestamp1(_Config) ->
    QName = rabbit_misc:r(<<"/">>, queue,
      <<"info_head_message_timestamp-queue">>),
    Q0 = rabbit_amqqueue:pseudo_queue(QName, self()),
    Q = Q0#amqqueue{arguments = [{<<"x-max-priority">>, long, 2}]},
    PQ = rabbit_priority_queue,
    BQS1 = PQ:init(Q, new, fun(_, _) -> ok end),
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
    Q = Q0#amqqueue{arguments = [{<<"x-max-priority">>, long, 5}]},
    PQ = rabbit_priority_queue,
    BQS1 = PQ:init(Q, new, fun(_, _) -> ok end),
    {_Duration1, BQS2} = PQ:ram_duration(BQS1),
    BQS3 = PQ:set_ram_duration_target(infinity, BQS2),
    BQS4 = PQ:set_ram_duration_target(1, BQS3),
    {_Duration2, BQS5} = PQ:ram_duration(BQS4),
    PQ:delete_and_terminate(a_whim, BQS5),
    passed.

mirror_queue_sync(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Q = <<"mirror_queue_sync-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [1, 2, 3]),
    ok = rabbit_ct_broker_helpers:set_ha_policy(Config, 0,
      <<"^mirror_queue_sync-queue$">>, <<"all">>),
    publish(Ch, Q, [1, 2, 3, 1, 2, 3]),
    %% master now has 9, slave 6.
    get_partial(Ch, Q, manual_ack, [3, 3, 3, 2, 2, 2]),
    %% So some but not all are unacked at the slave
    Nodename0 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_broker_helpers:control_action(sync_queue, Nodename0,
      [binary_to_list(Q)], [{"-p", "/"}]),
    wait_for_sync(Config, Nodename0, rabbit_misc:r(<<"/">>, queue, Q)),
    passed.

mirror_queue_sync_priority_above_max(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    %% Tests synchronisation of slaves when priority is higher than max priority.
    %% This causes an infinity loop (and test timeout) before rabbitmq-server-795
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    Q = <<"mirror_queue_sync_priority_above_max-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [5, 5, 5]),
    ok = rabbit_ct_broker_helpers:set_ha_policy(Config, A,
      <<".*">>, <<"all">>),
    rabbit_ct_broker_helpers:control_action(sync_queue, A,
      [binary_to_list(Q)], [{"-p", "/"}]),
    wait_for_sync(Config, A, rabbit_misc:r(<<"/">>, queue, Q)),
    delete(Ch, Q),
    passed.

mirror_queue_sync_priority_above_max_pending_ack(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    %% Tests synchronisation of slaves when priority is higher than max priority
    %% and there are pending acks.
    %% This causes an infinity loop (and test timeout) before rabbitmq-server-795
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    Q = <<"mirror_queue_sync_priority_above_max_pending_ack-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [5, 5, 5]),
    %% Consume but 'forget' to acknowledge
    get_without_ack(Ch, Q),
    get_without_ack(Ch, Q),
    ok = rabbit_ct_broker_helpers:set_ha_policy(Config, A,
      <<".*">>, <<"all">>),
    rabbit_ct_broker_helpers:control_action(sync_queue, A,
      [binary_to_list(Q)], [{"-p", "/"}]),
    wait_for_sync(Config, A, rabbit_misc:r(<<"/">>, queue, Q)),
    synced_msgs(Config, A, rabbit_misc:r(<<"/">>, queue, Q), 3),
    synced_msgs(Config, B, rabbit_misc:r(<<"/">>, queue, Q), 3),
    delete(Ch, Q),
    passed.

mirror_queue_auto_ack(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    %% Check correct use of AckRequired in the notifications to the slaves.
    %% If slaves are notified with AckRequired == true when it is false,
    %% the slaves will crash with the depth notification as they will not
    %% match the master delta.
    %% Bug rabbitmq-server 687
    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    Q = <<"mirror_queue_auto_ack-queue">>,
    declare(Ch, Q, 3),
    publish(Ch, Q, [1, 2, 3]),
    ok = rabbit_ct_broker_helpers:set_ha_policy(Config, A,
      <<".*">>, <<"all">>),
    get_partial(Ch, Q, no_ack, [3, 2, 1]),

    %% Retrieve slaves
    SPids = slave_pids(Config, A, rabbit_misc:r(<<"/">>, queue, Q)),
    [{SNode1, _SPid1}, {SNode2, SPid2}] = nodes_and_pids(SPids),

    %% Restart one of the slaves so `request_depth` is triggered
    rabbit_ct_broker_helpers:restart_node(Config, SNode1),

    %% The alive slave must have the same pid after its neighbour is restarted
    timer:sleep(3000), %% ugly but we can't know when the `depth` instruction arrives
    Slaves = nodes_and_pids(slave_pids(Config, A, rabbit_misc:r(<<"/">>, queue, Q))),
    SPid2 = proplists:get_value(SNode2, Slaves),

    delete(Ch, Q),
    passed.

%%----------------------------------------------------------------------------

open(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    {Conn, Ch}.

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

publish_many(_Ch, _Q, 0) -> ok;
publish_many( Ch,  Q, N) -> publish1(Ch, Q, random:uniform(5)),
                            publish_many(Ch, Q, N - 1).

publish1(Ch, Q, P) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = priority2bin(P)}).

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
    [get_ok(Ch, Q, Ack, P) || P <- Ps].

get_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = Q}).

get_ok(Ch, Q, Ack, P) ->
    PBin = priority2bin(P),
    {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = PBin2}} =
        amqp_channel:call(Ch, #'basic.get'{queue  = Q,
                                           no_ack = Ack =:= no_ack}),
    PBin = PBin2,
    maybe_ack(Ch, Ack, DTag).

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

wait_for_sync(Config, Nodename, Q) ->
    case synced(Config, Nodename, Q) of
        true  -> ok;
        false -> timer:sleep(100),
                 wait_for_sync(Config, Nodename, Q)
    end.

synced(Config, Nodename, Q) ->
    Info = rabbit_ct_broker_helpers:rpc(Config, Nodename,
      rabbit_amqqueue, info_all, [<<"/">>, [name, synchronised_slave_pids]]),
    [SSPids] = [Pids || [{name, Q1}, {synchronised_slave_pids, Pids}] <- Info,
                        Q =:= Q1],
    length(SSPids) =:= 1.

synced_msgs(Config, Nodename, Q, Expected) ->
    Info = rabbit_ct_broker_helpers:rpc(Config, Nodename,
      rabbit_amqqueue, info_all, [<<"/">>, [name, messages]]),
    [M] = [M || [{name, Q1}, {messages, M}] <- Info, Q =:= Q1],
    M =:= Expected.

nodes_and_pids(SPids) ->
    lists:zip([node(S) || S <- SPids], SPids).

slave_pids(Config, Nodename, Q) ->
    Info = rabbit_ct_broker_helpers:rpc(Config, Nodename,
      rabbit_amqqueue, info_all, [<<"/">>, [name, slave_pids]]),
    [SPids] = [SPids || [{name, Q1}, {slave_pids, SPids}] <- Info,
                        Q =:= Q1],
    SPids.

%%----------------------------------------------------------------------------
