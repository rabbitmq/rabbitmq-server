%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(queue_alarm_SUITE).

%% Exercises the queue federation link's response to a downstream
%% resource alarm. A connection.blocked from the downstream direct
%% connection must cause new deliveries to be buffered on the link, and
%% a subsequent connection.unblocked must drain the buffer in FIFO order
%% into the downstream queue.
%%
%% Queue federation only pulls from upstream while the downstream queue
%% is empty and has at least one active consumer at priority >= 0 (see
%% rabbit_federation_queue:consumer_state_changed/3). Each test case
%% therefore subscribes a downstream consumer first, then blocks the
%% downstream node so the forwarding path must buffer, publishes to
%% upstream, and finally clears the alarm and asserts that the buffered
%% deliveries reach the subscriber in publish order.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(UPSTREAM_NAME, <<"upstream-node">>).
-define(UPSTREAM_Q,    <<"fed-alarm.upstream.q">>).
-define(DOWNSTREAM_Q,  <<"fed-alarm.downstream.q">>).
-define(POLICY_NAME,   <<"fed-alarm-policy">>).
-define(UPSTREAM_SET,  <<"fed-alarm-upstream-set">>).
-define(MSG_COUNT,     50).

all() ->
    [
     {group, alarm}
    ].

groups() ->
    [
     {alarm, [], [
                  alarm_buffers_and_drains_in_order,
                  alarm_buffers_no_ack_and_drains_in_order
                 ]}
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        %% Two unclustered nodes: 0 = downstream, 1 = upstream. Only the
        %% downstream is alarmed during the test, so publishing to the
        %% upstream remains possible while the link is blocked.
        {rmq_nodes_count, 2},
        {rmq_nodes_clustered, false}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    cleanup(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

alarm_buffers_and_drains_in_order(Config) ->
    do_alarm_case(<<"on-confirm">>, Config).

alarm_buffers_no_ack_and_drains_in_order(Config) ->
    do_alarm_case(<<"no-ack">>, Config).

do_alarm_case(AckMode, Config) ->
    UpstreamUri = rabbit_ct_broker_helpers:node_uri(Config, 1),
    setup_federation(Config, UpstreamUri, AckMode),

    {UpConn, UpCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                       Config, 1),
    {DownConn, DownCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                           Config, 0),

    amqp_channel:call(UpCh, #'queue.declare'{queue = ?UPSTREAM_Q,
                                             durable = true}),
    amqp_channel:call(DownCh, #'queue.declare'{queue = ?DOWNSTREAM_Q,
                                               durable = true}),

    %% Subscribe on the downstream first: queue federation only pulls
    %% from upstream while the downstream queue has an active local
    %% consumer at priority >= 0 (see
    %% rabbit_federation_queue:consumer_state_changed/3). no_ack is used
    %% so the consumer captures deliveries directly without adding an
    %% ack round-trip to the assertions.
    #'basic.qos_ok'{} =
        amqp_channel:call(DownCh, #'basic.qos'{prefetch_count = ?MSG_COUNT}),
    amqp_channel:subscribe(DownCh,
                           #'basic.consume'{queue = ?DOWNSTREAM_Q,
                                            no_ack = true},
                           self()),
    receive
        #'basic.consume_ok'{} -> ok
    after 10_000 ->
              ct:fail(subscribe_timeout)
    end,

    await_running_link(Config, ?DOWNSTREAM_Q, ?UPSTREAM_Q),

    while_downstream_blocked(
      Config,
      fun() ->
              publish_n(UpCh, <<"">>, ?UPSTREAM_Q, ?MSG_COUNT),

              %% Nothing should reach our subscriber while the alarm is
              %% active: federation buffers on the link.
              expect_no_delivery(300)
      end),

    %% Once the alarm clears the drain path must deliver every buffered
    %% message to the subscriber, in publish order.
    Received = collect_deliveries(?MSG_COUNT, 30_000),
    Expected = [payload(N) || N <- lists:seq(1, ?MSG_COUNT)],
    ?assertEqual(Expected, Received),

    rabbit_ct_client_helpers:close_connection_and_channel(DownConn, DownCh),
    rabbit_ct_client_helpers:close_connection_and_channel(UpConn, UpCh),
    ok.

%% -------------------------------------------------------------------
%% Federation setup
%% -------------------------------------------------------------------

setup_federation(Config, UpstreamUri, AckMode) ->
    ok = rabbit_ct_broker_helpers:set_parameter(
           Config, 0, <<"federation-upstream">>, ?UPSTREAM_NAME,
           [{<<"uri">>, UpstreamUri},
            {<<"ack-mode">>, AckMode},
            {<<"prefetch-count">>, ?MSG_COUNT * 2}]),
    ok = rabbit_ct_broker_helpers:set_parameter(
           Config, 0, <<"federation-upstream-set">>, ?UPSTREAM_SET,
           [[{<<"upstream">>, ?UPSTREAM_NAME},
             {<<"queue">>, ?UPSTREAM_Q}]]),
    rabbit_ct_broker_helpers:set_policy(
      Config, 0, ?POLICY_NAME, <<"^fed-alarm.downstream.q$">>, <<"queues">>,
      [{<<"federation-upstream-set">>, ?UPSTREAM_SET}]).

await_running_link(Config, DownQ, UpQ) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Status = rabbit_ct_broker_helpers:rpc(
                         Config, 0, rabbit_federation_status, status, []),
              lists:any(
                fun(Entry) ->
                        proplists:get_value(queue, Entry) =:= DownQ andalso
                            proplists:get_value(upstream_queue, Entry) =:= UpQ andalso
                            proplists:get_value(status, Entry) =:= running
                end, Status)
      end, 30_000).

%% -------------------------------------------------------------------
%% Alarm helper (adapted from amqp091_alarm_SUITE)
%% -------------------------------------------------------------------

conserve_resources(Pid, Source, {_, Conserve, _AlarmedNode}) ->
    case Conserve of
        true  -> Pid ! {block, Source};
        false -> Pid ! {unblock, Source}
    end,
    ok.

while_downstream_blocked(Config, Fun) when is_function(Fun, 0) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    OrigLimit = rabbit_ct_broker_helpers:rpc(
                  Config, 0, vm_memory_monitor,
                  get_vm_memory_high_watermark, []),
    ok = rabbit_ct_broker_helpers:add_code_path_to_node(Node, ?MODULE),
    [] = rabbit_ct_broker_helpers:rpc(
           Config, 0, rabbit_alarm, register,
           [self(), {?MODULE, conserve_resources, []}]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0, vm_memory_monitor,
           set_vm_memory_high_watermark, [0]),
    Source = receive
                 {block, S} -> S
             after
                 15_000 -> ct:fail(alarm_set_timeout)
             end,
    try
        Fun()
    after
        ok = rabbit_ct_broker_helpers:rpc(
               Config, 0, vm_memory_monitor,
               set_vm_memory_high_watermark, [OrigLimit]),
        receive
            {unblock, Source} -> ok
        after
            15_000 -> ct:fail(alarm_clear_timeout)
        end
    end.

%% -------------------------------------------------------------------
%% Message helpers
%% -------------------------------------------------------------------

payload(N) ->
    integer_to_binary(N).

publish_n(Ch, X, Key, N) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [amqp_channel:cast(Ch,
                       #'basic.publish'{exchange = X, routing_key = Key},
                       #amqp_msg{props = #'P_basic'{delivery_mode = 2},
                                 payload = payload(I)})
     || I <- lists:seq(1, N)],
    true = amqp_channel:wait_for_confirms(Ch, 30),
    ok.

expect_no_delivery(Timeout) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = P}} ->
            ct:fail("unexpected delivery while alarm active: ~tp", [P])
    after Timeout ->
              ok
    end.

collect_deliveries(N, Timeout) ->
    collect_deliveries(N, Timeout, []).

collect_deliveries(0, _Timeout, Acc) ->
    lists:reverse(Acc);
collect_deliveries(N, Timeout, Acc) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload = P}} ->
            collect_deliveries(N - 1, Timeout, [P | Acc])
    after Timeout ->
              ct:fail("did not receive ~b more deliveries in time; got ~tp",
                      [N, lists:reverse(Acc)])
    end.

cleanup(Config) ->
    rabbit_ct_broker_helpers:clear_policy(Config, 0, ?POLICY_NAME),
    rabbit_ct_broker_helpers:clear_parameter(
      Config, 0, <<"federation-upstream-set">>, ?UPSTREAM_SET),
    rabbit_ct_broker_helpers:clear_parameter(
      Config, 0, <<"federation-upstream">>, ?UPSTREAM_NAME),
    delete_queue(Config, 0, ?DOWNSTREAM_Q),
    delete_queue(Config, 1, ?UPSTREAM_Q),
    %% Drain any lingering deliveries so the next testcase's
    %% collect_deliveries does not observe them.
    flush_deliveries().

delete_queue(Config, Node, Name) ->
    R = rabbit_misc:r(<<"/">>, queue, Name),
    case rabbit_ct_broker_helpers:rpc(
           Config, Node, rabbit_amqqueue, lookup, [R]) of
        {ok, Q} ->
            _ = rabbit_ct_broker_helpers:rpc(
                  Config, Node, rabbit_amqqueue, delete,
                  [Q, false, false, <<"acting-user">>]),
            ok;
        {error, not_found} ->
            ok
    end.

flush_deliveries() ->
    receive
        {#'basic.deliver'{}, _} -> flush_deliveries();
        #'basic.consume_ok'{} -> flush_deliveries();
        #'basic.cancel_ok'{} -> flush_deliveries()
    after 0 ->
              ok
    end.
