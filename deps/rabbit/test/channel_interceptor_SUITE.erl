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
          conflicting_interceptors_close_network_connections_gracefully,
          conflicting_interceptors_close_direct_connections_gracefully
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

conflicting_interceptors_close_network_connections_gracefully(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, conflicting_interceptors_close_network_connections_gracefully1, [Config]).

conflicting_interceptors_close_network_connections_gracefully1(Config) ->
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor">>,
                                  dummy_interceptor),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"conflicting dummy interceptor">>,
                                  dummy_interceptor_conflicting),
    try
        Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
        ?assert(is_pid(Conn)),
        ?assert(is_process_alive(Conn)),
        ConnMRef = erlang:monitor(process, Conn),
        Result = try amqp_connection:open_channel(Conn)
                 catch _:_ -> {error, channel_open_failed}
                 end,
        ?assertMatch({error, _}, Result),
        receive
            {'DOWN', ConnMRef, process, Conn, ConnExitReason} ->
                ?assertMatch(
                    {shutdown, {server_initiated_close, _, _}},
                    ConnExitReason)
        after 10000 ->
            ct:fail("Connection process did not terminate")
        end
    after
        ok = rabbit_registry:unregister(channel_interceptor,
                                        <<"dummy interceptor">>),
        ok = rabbit_registry:unregister(channel_interceptor,
                                        <<"conflicting dummy interceptor">>)
    end,
    %% Verify the server still accepts new connections and channels
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    ?assert(is_pid(Conn2)),
    {ok, Ch} = amqp_connection:open_channel(Conn2),
    ?assert(is_pid(Ch)),
    ok = amqp_connection:close(Conn2),
    passed.

conflicting_interceptors_close_direct_connections_gracefully(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, conflicting_interceptors_close_direct_connections_gracefully1, [Config]).

conflicting_interceptors_close_direct_connections_gracefully1(Config) ->
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor">>,
                                  dummy_interceptor),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"conflicting dummy interceptor">>,
                                  dummy_interceptor_conflicting),
    try
        Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
        Params = #amqp_params_direct{node = Node,
                                     virtual_host = <<"/">>,
                                     username = <<"guest">>,
                                     password = <<"guest">>},
        {ok, Conn} = amqp_connection:start(Params),
        ?assert(is_pid(Conn)),
        ?assert(is_process_alive(Conn)),
        %% open_channel must return {error, _} gracefully, not crash.
        %% If the connection process crashes (e.g. badmatch in
        %% amqp_channels_manager), this call will throw and fail the test.
        Result = amqp_connection:open_channel(Conn),
        ?assertMatch({error, _}, Result),
        ?assert(is_process_alive(Conn)),
        ok = amqp_connection:close(Conn)
    after
        ok = rabbit_registry:unregister(channel_interceptor,
                                        <<"dummy interceptor">>),
        ok = rabbit_registry:unregister(channel_interceptor,
                                        <<"conflicting dummy interceptor">>)
    end,
    %% Verify the server still accepts new direct connections and channels
    Node2 = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Params2 = #amqp_params_direct{node = Node2,
                                  virtual_host = <<"/">>,
                                  username = <<"guest">>,
                                  password = <<"guest">>},
    {ok, Conn2} = amqp_connection:start(Params2),
    ?assert(is_pid(Conn2)),
    {ok, Ch} = amqp_connection:open_channel(Conn2),
    ?assert(is_pid(Ch)),
    ok = amqp_connection:close(Conn2),
    passed.

check_send_receive(Ch1, QName, Send, Receive) ->
    amqp_channel:call(Ch1,
                        #'basic.publish'{routing_key = QName},
                        #amqp_msg{payload = Send}),

    {#'basic.get_ok'{}, #amqp_msg{payload = Receive}} =
        amqp_channel:call(Ch1, #'basic.get'{queue = QName,
                                              no_ack = true}).
