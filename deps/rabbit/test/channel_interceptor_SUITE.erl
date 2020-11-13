%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(channel_interceptor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          register_interceptor,
          register_failing_interceptors
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
    amqp_channel:call(Ch1, #'queue.declare'{queue = QName}),

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

register_failing_interceptors(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, register_interceptor1, [Config, failing_dummy_interceptor]).

check_send_receive(Ch1, QName, Send, Receive) ->
    amqp_channel:call(Ch1,
                        #'basic.publish'{routing_key = QName},
                        #amqp_msg{payload = Send}),

    {#'basic.get_ok'{}, #amqp_msg{payload = Receive}} =
        amqp_channel:call(Ch1, #'basic.get'{queue = QName,
                                              no_ack = true}).
