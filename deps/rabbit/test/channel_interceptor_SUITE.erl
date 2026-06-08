%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(channel_interceptor_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          register_interceptor,
          register_interceptor_failing_with_amqp_error,
          register_interceptor_crashing_with_amqp_error_exception,
          register_failing_interceptors,
          multiple_interceptors_ordered_by_priority,
          reject_interceptors_with_same_priority_for_same_operation,
          priority_overridden_by_config
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

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

register_interceptor(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, register_interceptor1, [Config, dummy_interceptor]).

register_interceptor1(Config, Interceptor) ->
    PredefinedChannels = rabbit_channel:list(),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, 0),

    QName = <<"register_interceptor-q">>,
    amqp_channel:call(Ch1, #'queue.declare'{queue = QName, durable = true}),

    [ChannelProc] = rabbit_channel:list() -- PredefinedChannels,

    [{interceptors, []}] = rabbit_channel:info(ChannelProc, [interceptors]),

    check_send_receive(Ch1, QName, <<"bar">>, <<"bar">>),

    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor">>,
                                  Interceptor),
    [{interceptors, [{Interceptor, undefined}]}] =
      rabbit_channel:info(ChannelProc, [interceptors]),

    check_send_receive(Ch1, QName, <<"bar">>, <<"">>),

    ok = rabbit_registry:unregister(channel_interceptor,
                                  <<"dummy interceptor">>),
    [{interceptors, []}] = rabbit_channel:info(ChannelProc, [interceptors]),

    check_send_receive(Ch1, QName, <<"bar">>, <<"bar">>),
    passed.

register_interceptor_failing_with_amqp_error(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, register_interceptor_failing_with_amqp_error1,
      [Config, dummy_interceptor]).

register_interceptor_failing_with_amqp_error1(Config, Interceptor) ->
    PredefinedChannels = rabbit_channel:list(),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, 0),

    [ChannelProc] = rabbit_channel:list() -- PredefinedChannels,

    [{interceptors, []}] = rabbit_channel:info(ChannelProc, [interceptors]),

    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor">>,
                                  Interceptor),
    [{interceptors, [{Interceptor, undefined}]}] =
      rabbit_channel:info(ChannelProc, [interceptors]),

    Q1 = <<"succeeding-q">>,
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch1, #'queue.declare'{queue = Q1, durable = true}),

    Q2 = <<"failing-with-amqp-error-q">>,
    try
        amqp_channel:call(Ch1, #'queue.declare'{queue = Q2, durable = true})
    catch
      _:Reason ->
          ?assertMatch(
              {{shutdown, {_, _, <<"PRECONDITION_FAILED - operation not allowed">>}}, _},
              Reason)
    end,

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, 0),
    %% After the error, the old channel process will terminate
    %% asynchronously and may still be on the channel list.
    %% Wait until there is exactly one live channel.
    ?awaitMatch(
      [P] when P =/= ChannelProc,
      rabbit_channel:list() -- PredefinedChannels,
      10000),
    [ChannelProc1] = rabbit_channel:list() -- PredefinedChannels,

    ok = rabbit_registry:unregister(channel_interceptor,
                                  <<"dummy interceptor">>),
    [{interceptors, []}] = rabbit_channel:info(ChannelProc1, [interceptors]),

    #'queue.declare_ok'{} =
        amqp_channel:call(Ch2, #'queue.declare'{queue = Q2, durable = true}),

    #'queue.delete_ok'{} = amqp_channel:call(Ch2, #'queue.delete' {queue = Q1}),
    #'queue.delete_ok'{} = amqp_channel:call(Ch2, #'queue.delete' {queue = Q2}),

    passed.

register_interceptor_crashing_with_amqp_error_exception(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, register_interceptor_crashing_with_amqp_error_exception1,
      [Config, dummy_interceptor]).

register_interceptor_crashing_with_amqp_error_exception1(Config, Interceptor) ->
    PredefinedChannels = rabbit_channel:list(),

    Ch1 = rabbit_ct_client_helpers:open_channel(Config, 0),

    [ChannelProc] = rabbit_channel:list() -- PredefinedChannels,

    [{interceptors, []}] = rabbit_channel:info(ChannelProc, [interceptors]),

    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor">>,
                                  Interceptor),
    [{interceptors, [{Interceptor, undefined}]}] =
      rabbit_channel:info(ChannelProc, [interceptors]),

    Q1 = <<"succeeding-q">>,
    #'queue.declare_ok'{} =
        amqp_channel:call(Ch1, #'queue.declare'{queue = Q1, durable = true}),

    Q2 = <<"crashing-with-amqp-exception-q">>,
    try
        amqp_channel:call(Ch1, #'queue.declare'{queue = Q2, durable = true})
    catch
      _:Reason ->
          ?assertMatch(
              {{shutdown, {_, _, <<"PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'crashing-with-amqp-exception-q' in vhost '/': received 'false' but current is 'true'">>}}, _},
              Reason)
    end,

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, 0),
    %% After the error, the old channel process will terminate
    %% asynchronously and may still be on the channel list.
    %% Wait until there is exactly one live channel.
    ?awaitMatch(
      [P] when P =/= ChannelProc,
      rabbit_channel:list() -- PredefinedChannels,
      10000),
    [ChannelProc1] = rabbit_channel:list() -- PredefinedChannels,

    ok = rabbit_registry:unregister(channel_interceptor,
                                  <<"dummy interceptor">>),
    [{interceptors, []}] = rabbit_channel:info(ChannelProc1, [interceptors]),

    #'queue.declare_ok'{} =
        amqp_channel:call(Ch2, #'queue.declare'{queue = Q2, durable = true}),

    #'queue.delete_ok'{} = amqp_channel:call(Ch2, #'queue.delete' {queue = Q1}),
    #'queue.delete_ok'{} = amqp_channel:call(Ch2, #'queue.delete' {queue = Q2}),

    passed.

register_failing_interceptors(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, register_interceptor1, [Config, failing_dummy_interceptor]).

multiple_interceptors_ordered_by_priority(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, multiple_interceptors_ordered_by_priority1, [Config]).

multiple_interceptors_ordered_by_priority1(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    QName = <<"multiple-interceptors-q">>,
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                    durable = true}),

    ok = application:set_env(rabbit, channel_interceptor_priorities,
                             [{dummy_interceptor_priority_1, 1},
                              {dummy_interceptor_priority_2, 2},
                              {dummy_interceptor_priority_3, 3}]),

    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 3">>, dummy_interceptor_priority_3),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 2">>, dummy_interceptor_priority_2),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 1">>, dummy_interceptor_priority_1),

    %% Interceptors run in ascending priority order regardless of registration order,
    %% so the payload becomes <<"foo1">>, then <<"foo12">>, then <<"foo123">>.
    check_send_receive(Ch, QName, <<"foo">>, <<"foo123">>),

    ok = rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 1">>),
    ok = rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 2">>),
    ok = rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 3">>),
    ok = application:unset_env(rabbit, channel_interceptor_priorities),

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    passed.

reject_interceptors_with_same_priority_for_same_operation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, reject_interceptors_with_same_priority_for_same_operation1, [Config]).

reject_interceptors_with_same_priority_for_same_operation1(_Config) ->
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 1">>,
                                  dummy_interceptor_priority_1),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 1 conflict">>,
                                  dummy_interceptor_priority_1_conflict),
    try
        %% Initialising interceptors must fail: two interceptors with the same
        %% priority handle the same AMQP operation.
        rabbit_channel_interceptor:init(self())
    catch
        exit:{amqp_error, internal_error, _, _} -> ok
    after
        rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 1">>),
        rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 1 conflict">>)
    end,
    passed.

priority_overridden_by_config(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, priority_overridden_by_config1, [Config]).

priority_overridden_by_config1(Config) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    QName = <<"priority-override-q">>,
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                    durable = true}),

    %% priority_1 (config priority=1) runs before priority_3 (config priority=3),
    %% so the result is <<"foo13">>.
    ok = application:set_env(rabbit, channel_interceptor_priorities,
                             [{dummy_interceptor_priority_1, 1},
                              {dummy_interceptor_priority_3, 3}]),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 1">>, dummy_interceptor_priority_1),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 3">>, dummy_interceptor_priority_3),
    check_send_receive(Ch, QName, <<"foo">>, <<"foo13">>),

    %% Reconfigure priority_3 to run first (priority=0). Now the result is <<"foo31">>.
    ok = application:set_env(rabbit, channel_interceptor_priorities,
                             [{dummy_interceptor_priority_1, 1},
                              {dummy_interceptor_priority_3, 0}]),
    rabbit_channel:refresh_interceptors(),
    check_send_receive(Ch, QName, <<"foo">>, <<"foo31">>),

    ok = application:unset_env(rabbit, channel_interceptor_priorities),
    ok = rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 1">>),
    ok = rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 3">>),

    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    passed.

check_send_receive(Ch1, QName, Send, Receive) ->
    amqp_channel:call(Ch1,
                        #'basic.publish'{routing_key = QName},
                        #amqp_msg{payload = Send}),

    {#'basic.get_ok'{}, #amqp_msg{payload = Receive}} =
        amqp_channel:call(Ch1, #'basic.get'{queue = QName,
                                              no_ack = true}).
