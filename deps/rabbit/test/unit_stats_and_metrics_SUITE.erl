%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_stats_and_metrics_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          channel_statistics
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% -------------------------------------------------------------------
%% Statistics.
%% -------------------------------------------------------------------

channel_statistics(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, channel_statistics1, [Config]).

channel_statistics1(_Config) ->
    application:set_env(rabbit, collect_statistics, fine),

    %% ATM this just tests the queue / exchange stats in channels. That's
    %% by far the most complex code though.

    %% Set up a channel and queue
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{}),
    QName = receive #'queue.declare_ok'{queue = Q0} -> Q0
            after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
            end,
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    X = rabbit_misc:r(<<"/">>, exchange, <<"">>),

    dummy_event_receiver:start(self(), [node()], [channel_stats]),

    %% Check stats empty
    Check1 = fun() ->
                 [] = ets:match(channel_queue_metrics, {Ch, QRes}),
                 [] = ets:match(channel_exchange_metrics, {Ch, X}),
                 [] = ets:match(channel_queue_exchange_metrics,
                                {Ch, {QRes, X}})
             end,
    test_ch_metrics(Check1, ?TIMEOUT),

    %% Publish and get a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{}, <<"">>)),
    rabbit_channel:do(Ch, #'basic.get'{queue = QName}),

    %% Check the stats reflect that
    Check2 = fun() ->
                     [{{Ch, QRes}, 1, 0, 0, 0, 0, 0, 0, 0}] = ets:lookup(
                                                                channel_queue_metrics,
                                                                {Ch, QRes}),
                     [{{Ch, X}, 1, 0, 0, 0, 0}] = ets:lookup(
                                                 channel_exchange_metrics,
                                                 {Ch, X}),
                     [{{Ch, {QRes, X}}, 1, 0}] = ets:lookup(
                                                   channel_queue_exchange_metrics,
                                                   {Ch, {QRes, X}})
             end,
    test_ch_metrics(Check2, ?TIMEOUT),

    %% Check the stats are marked for removal on queue deletion.
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName}),
    Check3 = fun() ->
                     [{{Ch, QRes}, 1, 0, 0, 0, 0, 0, 0, 1}] = ets:lookup(
                                                                channel_queue_metrics,
                                                                {Ch, QRes}),
                 [{{Ch, X}, 1, 0, 0, 0, 0}] = ets:lookup(
                                             channel_exchange_metrics,
                                             {Ch, X}),
                 [{{Ch, {QRes, X}}, 1, 1}] = ets:lookup(
                                               channel_queue_exchange_metrics,
                                               {Ch, {QRes, X}})
             end,
    test_ch_metrics(Check3, ?TIMEOUT),

    %% Check the garbage collection removes stuff.
    force_metric_gc(),
    Check4 = fun() ->
                 [] = ets:lookup(channel_queue_metrics, {Ch, QRes}),
                 [{{Ch, X}, 1, 0, 0, 0, 0}] = ets:lookup(
                                             channel_exchange_metrics,
                                             {Ch, X}),
                 [] = ets:lookup(channel_queue_exchange_metrics,
                                 {Ch, {QRes, X}})
             end,
    test_ch_metrics(Check4, ?TIMEOUT),

    rabbit_channel:shutdown(Ch),
    dummy_event_receiver:stop(),
    passed.

force_metric_gc() ->
    timer:sleep(300),
    rabbit_core_metrics_gc ! start_gc,
    gen_server:call(rabbit_core_metrics_gc, test).

test_ch_metrics(Fun, Timeout) when Timeout =< 0 ->
    Fun();
test_ch_metrics(Fun, Timeout) ->
    try
        Fun()
    catch
        _:{badmatch, _} ->
            timer:sleep(1000),
            test_ch_metrics(Fun, Timeout - 1000)
    end.

test_spawn() ->
    {Writer, _Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    ok = rabbit_channel:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.
