%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(exchange_alarm_SUITE).

%% Exercises the exchange federation link's response to a downstream
%% resource alarm. A connection.blocked from the downstream direct
%% connection must cause new deliveries to be buffered on the link, and
%% a subsequent connection.unblocked must drain the buffer in FIFO order
%% into the downstream exchange.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(UPSTREAM_NAME, <<"upstream-node">>).
-define(UPSTREAM_X,    <<"fed-alarm.upstream">>).
-define(DOWNSTREAM_X,  <<"fed-alarm.downstream">>).
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
                  alarm_buffers_no_ack_and_drains_in_order,
                  downstream_death_while_buffering_restarts_link,
                  deliveries_during_a_drain_keep_their_place
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

%% The buffer is drained one delivery per callback, so the link keeps answering
%% its mailbox while a long buffer empties. That means a delivery can arrive
%% mid-drain, and it has to be buffered behind the rest rather than forwarded
%% straight through, or it would overtake them.
deliveries_during_a_drain_keep_their_place(Config) ->
    First = 200,
    Second = 50,
    UpstreamUri = rabbit_ct_broker_helpers:node_uri(Config, 1),
    setup_federation(Config, UpstreamUri, <<"on-confirm">>),

    {UpConn, UpCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                       Config, 1),
    {DownConn, DownCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                           Config, 0),

    declare_exchange(UpCh, ?UPSTREAM_X),
    declare_exchange(DownCh, ?DOWNSTREAM_X),
    amqp_channel:call(DownCh, #'queue.declare'{queue = ?DOWNSTREAM_Q,
                                               durable = true}),
    amqp_channel:call(DownCh, #'queue.bind'{queue = ?DOWNSTREAM_Q,
                                            exchange = ?DOWNSTREAM_X,
                                            routing_key = <<"k">>}),

    await_running_link(Config, ?DOWNSTREAM_X, ?UPSTREAM_X),
    await_upstream_binding(Config),

    while_downstream_blocked(
      Config,
      fun() -> publish_range(UpCh, ?UPSTREAM_X, <<"k">>, 1, First) end),

    %% The alarm has cleared and the link is now draining the first batch one
    %% delivery at a time. Publish the second batch into that window.
    publish_range(UpCh, ?UPSTREAM_X, <<"k">>, First + 1, First + Second),

    ?awaitMatch(N when N =:= First + Second,
                message_count(Config, 0, ?DOWNSTREAM_Q),
                60_000),
    Received = drain_queue(DownCh, ?DOWNSTREAM_Q, First + Second),
    ?assertEqual([payload(N) || N <- lists:seq(1, First + Second)], Received),

    rabbit_ct_client_helpers:close_connection_and_channel(DownConn, DownCh),
    rabbit_ct_client_helpers:close_connection_and_channel(UpConn, UpCh),
    ok.

%% A link holding buffered deliveries must not survive the loss of its
%% downstream channel. credit_flow:peer_down/1 clears the credit_flow block, so
%% the next delivery would be forwarded ahead of everything already buffered,
%% and neither bump_credit nor connection.unblocked can arrive on a dead
%% channel to drain the rest. Restarting redelivers them instead.
downstream_death_while_buffering_restarts_link(Config) ->
    UpstreamUri = rabbit_ct_broker_helpers:node_uri(Config, 1),
    setup_federation(Config, UpstreamUri, <<"on-confirm">>),

    {UpConn, UpCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                       Config, 1),
    {DownConn, DownCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                           Config, 0),

    declare_exchange(UpCh, ?UPSTREAM_X),
    declare_exchange(DownCh, ?DOWNSTREAM_X),
    amqp_channel:call(DownCh, #'queue.declare'{queue = ?DOWNSTREAM_Q,
                                               durable = true}),
    amqp_channel:call(DownCh, #'queue.bind'{queue = ?DOWNSTREAM_Q,
                                            exchange = ?DOWNSTREAM_X,
                                            routing_key = <<"k">>}),

    await_running_link(Config, ?DOWNSTREAM_X, ?UPSTREAM_X),
    await_upstream_binding(Config),

    while_downstream_blocked(
      Config,
      fun() ->
              publish_n(UpCh, ?UPSTREAM_X, <<"k">>, ?MSG_COUNT),
              ?assertEqual(0, message_count(Config, 0, ?DOWNSTREAM_Q)),
              %% Buffered on the link. Take its downstream connection away
              %% cleanly, which is the case that used to leave it wedged.
              ok = close_downstream_link_connections(Config)
      end),

    %% The link restarts, and because on-confirm never acked the buffered
    %% deliveries the upstream redelivers them, so none are lost.
    ?awaitMatch(?MSG_COUNT, message_count(Config, 0, ?DOWNSTREAM_Q), 60_000),
    await_running_link(Config, ?DOWNSTREAM_X, ?UPSTREAM_X),

    rabbit_ct_client_helpers:close_connection_and_channel(DownConn, DownCh),
    rabbit_ct_client_helpers:close_connection_and_channel(UpConn, UpCh),
    ok.

%% The only direct connections on the downstream node are federation's.
close_downstream_link_connections(Config) ->
    Pids = rabbit_ct_broker_helpers:rpc(
             Config, 0, rabbit_direct, list_local, []),
    ?assertNotEqual([], Pids),
    [rabbit_ct_broker_helpers:rpc(Config, 0, amqp_connection, close, [Pid])
     || Pid <- Pids],
    ok.

do_alarm_case(AckMode, Config) ->
    UpstreamUri = rabbit_ct_broker_helpers:node_uri(Config, 1),
    setup_federation(Config, UpstreamUri, AckMode),

    %% Set up an exchange on both sides plus a downstream queue bound to
    %% the downstream exchange, so we can observe messages arriving after
    %% the link forwards them.
    {UpConn, UpCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                       Config, 1),
    {DownConn, DownCh} = rabbit_ct_client_helpers:open_connection_and_channel(
                           Config, 0),

    declare_exchange(UpCh, ?UPSTREAM_X),
    declare_exchange(DownCh, ?DOWNSTREAM_X),
    amqp_channel:call(DownCh, #'queue.declare'{queue = ?DOWNSTREAM_Q,
                                               durable = true}),
    amqp_channel:call(DownCh, #'queue.bind'{queue = ?DOWNSTREAM_Q,
                                            exchange = ?DOWNSTREAM_X,
                                            routing_key = <<"k">>}),

    await_running_link(Config, ?DOWNSTREAM_X, ?UPSTREAM_X),
    await_upstream_binding(Config),

    while_downstream_blocked(
      Config,
      fun() ->
              %% The link is blocked, so anything published to the
              %% upstream exchange must not reach the downstream queue.
              publish_n(UpCh, ?UPSTREAM_X, <<"k">>, ?MSG_COUNT),

              %% Nothing should have been forwarded while the alarm is
              %% active. Check repeatedly so a transient false negative
              %% has time to show up.
              lists:foreach(
                fun(_) ->
                        ?assertEqual(0, message_count(Config, 0, ?DOWNSTREAM_Q)),
                        timer:sleep(100)
                end, lists:seq(1, 5))
      end),

    %% Once the alarm is cleared the drain path must deliver every
    %% buffered message, in the order it was published.
    ?awaitMatch(
       ?MSG_COUNT,
       message_count(Config, 0, ?DOWNSTREAM_Q),
       30_000),
    Received = drain_queue(DownCh, ?DOWNSTREAM_Q, ?MSG_COUNT),
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
             {<<"exchange">>, ?UPSTREAM_X}]]),
    rabbit_ct_broker_helpers:set_policy(
      Config, 0, ?POLICY_NAME, <<"^fed-alarm.downstream$">>, <<"exchanges">>,
      [{<<"federation-upstream-set">>, ?UPSTREAM_SET}]).

%% The link creates the upstream binding asynchronously, after the downstream
%% bind commits, and reporting `running' does not imply it exists yet: go/1
%% passes ensure_upstream_bindings/2 whatever rabbit_binding:list_for_source/1
%% returned for the downstream exchange, which is empty if the link started
%% first. Until it exists, ?UPSTREAM_X is a `direct' exchange with no matching
%% binding, so every publish is silently unroutable, confirms still succeed,
%% and the case fails 30s later looking like a drain bug.
await_upstream_binding(Config) ->
    Resource = rabbit_misc:r(<<"/">>, exchange, ?UPSTREAM_X),
    rabbit_ct_helpers:await_condition(
      fun() ->
              [] =/= rabbit_ct_broker_helpers:rpc(
                       Config, 1, rabbit_binding, list_for_source, [Resource])
      end, 30_000).

await_running_link(Config, DownX, UpX) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Status = rabbit_ct_broker_helpers:rpc(
                         Config, 0, rabbit_federation_status, status, []),
              lists:any(
                fun(Entry) ->
                        proplists:get_value(exchange, Entry) =:= DownX andalso
                            proplists:get_value(upstream_exchange, Entry) =:= UpX andalso
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
    %% Everything from here on is inside the try, so that an abort while
    %% waiting for the alarm cannot leave the node pinned at a watermark of 0
    %% and break the next testcase with {badmatch, [memory]}.
    try
        ok = rabbit_ct_broker_helpers:rpc(
               Config, 0, vm_memory_monitor,
               set_vm_memory_high_watermark, [0]),
        Source = receive
                     {block, S} -> S
                 after
                     15_000 -> ct:fail(alarm_set_timeout)
                 end,
        Fun(),
        receive
            {unblock, Source} -> ok
        after
            0 -> ok
        end
    after
        ok = rabbit_ct_broker_helpers:rpc(
               Config, 0, vm_memory_monitor,
               set_vm_memory_high_watermark, [OrigLimit]),
        %% Waited for here rather than asserted, because raising from an
        %% `after' block would replace whichever assertion actually failed.
        receive
            {unblock, _} -> ok
        after
            15_000 -> ct:pal("alarm did not clear within 15s")
        end
    end.

%% -------------------------------------------------------------------
%% Message helpers
%% -------------------------------------------------------------------

declare_exchange(Ch, Name) ->
    amqp_channel:call(Ch, #'exchange.declare'{exchange = Name,
                                              type = <<"direct">>,
                                              durable = true}).

payload(N) ->
    integer_to_binary(N).

publish_n(Ch, X, Key, N) ->
    publish_range(Ch, X, Key, 1, N).

publish_range(Ch, X, Key, From, To) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [amqp_channel:cast(Ch,
                       #'basic.publish'{exchange = X, routing_key = Key},
                       #amqp_msg{props = #'P_basic'{delivery_mode = 2},
                                 payload = payload(I)})
     || I <- lists:seq(From, To)],
    true = amqp_channel:wait_for_confirms(Ch, 30),
    ok.

message_count(Config, Node, QueueName) ->
    Resource = rabbit_misc:r(<<"/">>, queue, QueueName),
    case rabbit_ct_broker_helpers:rpc(
           Config, Node, rabbit_amqqueue, lookup, [Resource]) of
        {ok, Q} ->
            Info = rabbit_ct_broker_helpers:rpc(
                     Config, Node, rabbit_amqqueue, info, [Q, [messages]]),
            proplists:get_value(messages, Info, 0);
        {error, not_found} ->
            0
    end.

drain_queue(Ch, Q, N) ->
    [begin
         {#'basic.get_ok'{delivery_tag = Tag}, #amqp_msg{payload = P}} =
             amqp_channel:call(Ch, #'basic.get'{queue = Q, no_ack = false}),
         amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Tag}),
         P
     end || _ <- lists:seq(1, N)].

cleanup(Config) ->
    rabbit_ct_broker_helpers:clear_policy(Config, 0, ?POLICY_NAME),
    rabbit_ct_broker_helpers:clear_parameter(
      Config, 0, <<"federation-upstream-set">>, ?UPSTREAM_SET),
    rabbit_ct_broker_helpers:clear_parameter(
      Config, 0, <<"federation-upstream">>, ?UPSTREAM_NAME),
    delete_queue(Config, 0, ?DOWNSTREAM_Q),
    delete_exchange(Config, 0, ?DOWNSTREAM_X),
    delete_exchange(Config, 1, ?UPSTREAM_X),
    ok.

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

delete_exchange(Config, Node, Name) ->
    R = rabbit_misc:r(<<"/">>, exchange, Name),
    _ = rabbit_ct_broker_helpers:rpc(
          Config, Node, rabbit_exchange, delete,
          [R, false, <<"acting-user">>]),
    ok.
