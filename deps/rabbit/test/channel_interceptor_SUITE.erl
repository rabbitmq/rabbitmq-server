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
<<<<<<< HEAD
          register_failing_interceptors
=======
          register_failing_interceptors,
          conflicting_interceptors_close_network_connections_gracefully,
          conflicting_interceptors_close_direct_connections_gracefully,
          multiple_interceptors_ordered_by_priority,
          reject_interceptors_with_same_priority_for_same_operation,
          set_priorities_rejects_conflict_without_committing,
          set_priorities_rejects_unknown_interceptor,
<<<<<<< HEAD
          priority_overridden_by_config
>>>>>>> d500ce152a (add core tests for handling multiple channel interceptors)
=======
          priority_overridden_by_config,
          list_skips_unloaded_interceptor,
          warn_unknown_priorities_logs_a_warning
>>>>>>> b29b6e6dd2 (Polishing #16046)
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
      ?MODULE, register_interceptor_amqp_error1,
      [Config, dummy_interceptor, <<"failing-with-amqp-error-q">>,
       fun(Reason) ->
               ?assertMatch(
                   {{shutdown, {_, _, <<"PRECONDITION_FAILED - operation not allowed">>}}, _},
                   Reason)
       end]).

register_interceptor_crashing_with_amqp_error_exception(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, register_interceptor_amqp_error1,
      [Config, dummy_interceptor, <<"crashing-with-amqp-exception-q">>,
       fun(Reason) ->
               ?assertMatch(
                   {{shutdown, {_, _, <<"PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'crashing-with-amqp-exception-q' in vhost '/': received 'false' but current is 'true'">>}}, _},
                   Reason)
       end]).

register_interceptor_amqp_error1(Config, Interceptor, Q2, AssertReason) ->
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

    try
        amqp_channel:call(Ch1, #'queue.declare'{queue = Q2, durable = true})
    catch
      _:Reason ->
          AssertReason(Reason)
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

<<<<<<< HEAD
=======
conflicting_interceptors_close_network_connections_gracefully(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, conflicting_interceptors_close_network_connections_gracefully1, [Config]).

conflicting_interceptors_close_network_connections_gracefully1(Config) ->
    with_conflicting_interceptors(fun() ->
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
    end),
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
    with_conflicting_interceptors(fun() ->
        Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
        Params = #amqp_params_direct{node = Node,
                                     virtual_host = <<"/">>,
                                     username = <<"guest">>,
                                     password = <<"guest">>},
        {ok, Conn} = amqp_connection:start(Params),
        ?assert(is_pid(Conn)),
        ?assert(is_process_alive(Conn)),
        %% `open_channel` must return `{error, _}` gracefully, not crash.
        %% If the connection process crashes (e.g. badmatch in
        %% `amqp_channels_manager`), this call will throw and fail the test.
        Result = amqp_connection:open_channel(Conn),
        ?assertMatch({error, _}, Result),
        ?assert(is_process_alive(Conn)),
        ok = amqp_connection:close(Conn)
    end),
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
        ?assertExit({amqp_error, internal_error, _, _},
                    rabbit_channel_interceptor:init(self()))
    catch
        exit:{amqp_error, internal_error, _, _} -> ok
    after
        rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 1">>),
        rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 1 conflict">>)
    end,
    passed.

set_priorities_rejects_conflict_without_committing(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_priorities_rejects_conflict_without_committing1, [Config]).

set_priorities_rejects_conflict_without_committing1(_Config) ->
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 1">>,
                                  dummy_interceptor_priority_1),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor priority 1 conflict">>,
                                  dummy_interceptor_priority_1_conflict),
    Before = application:get_env(rabbit, channel_interceptor_priorities, []),
    try
        %% Placing both interceptors at the same priority makes them handle the
        %% same operation ambiguously, so the merged configuration is rejected
        %% and nothing is committed.
        ?assertMatch(
            {error, _},
            rabbit_channel_interceptor:set_priorities(
                [{dummy_interceptor_priority_1, 5},
                 {dummy_interceptor_priority_1_conflict, 5}])),
        ?assertEqual(
            Before,
            application:get_env(rabbit, channel_interceptor_priorities, [])),

        %% A non-conflicting configuration is accepted and committed.
        ?assertEqual(
            ok,
            rabbit_channel_interceptor:set_priorities(
                [{dummy_interceptor_priority_1, 5},
                 {dummy_interceptor_priority_1_conflict, 6}])),
        ?assertEqual(
            {dummy_interceptor_priority_1, 5},
            lists:keyfind(dummy_interceptor_priority_1, 1,
                          application:get_env(rabbit, channel_interceptor_priorities, [])))
    after
        application:set_env(rabbit, channel_interceptor_priorities, Before),
        rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 1">>),
        rabbit_registry:unregister(channel_interceptor, <<"dummy interceptor priority 1 conflict">>)
    end,
    passed.

set_priorities_rejects_unknown_interceptor(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_priorities_rejects_unknown_interceptor1, [Config]).

set_priorities_rejects_unknown_interceptor1(_Config) ->
    Before = application:get_env(rabbit, channel_interceptor_priorities, []),
    try
        %% A name that matches no registered channel interceptor is rejected
        %% and nothing is committed, so a typo cannot report success while
        %% leaving the intended interceptor untouched.
        ?assertMatch(
            {error, _},
            rabbit_channel_interceptor:set_priorities(
                [{no_such_interceptor, 7}])),
        ?assertEqual(
            Before,
            application:get_env(rabbit, channel_interceptor_priorities, []))
    after
        application:set_env(rabbit, channel_interceptor_priorities, Before)
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

<<<<<<< HEAD
>>>>>>> d500ce152a (add core tests for handling multiple channel interceptors)
=======
list_skips_unloaded_interceptor(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, list_skips_unloaded_interceptor1, [Config]).

list_skips_unloaded_interceptor1(_Config) ->
    %% `register/3` rejects unloaded modules, so `lookup_all/1` is stubbed instead.
    ok = meck:new(rabbit_registry, [passthrough]),
    meck:expect(rabbit_registry, lookup_all,
                fun(channel_interceptor) ->
                        [{<<"dummy interceptor">>, dummy_interceptor},
                         {<<"unloaded interceptor">>, no_such_channel_interceptor_module}];
                   (Class) ->
                        meck:passthrough([Class])
                end),
    try
        ?assertEqual(
            [dummy_interceptor],
            [proplists:get_value(name, L) || L <- rabbit_channel_interceptor:list()])
    after
        meck:unload(rabbit_registry)
    end,
    passed.

warn_unknown_priorities_logs_a_warning(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, warn_unknown_priorities_logs_a_warning1, [Config]).

warn_unknown_priorities_logs_a_warning1(_Config) ->
    Before = application:get_env(rabbit, channel_interceptor_priorities, []),
    HandlerId = warn_unknown_priorities_test_handler,
    ok = logger:add_handler(HandlerId, ?MODULE, #{config => #{pid => self()}}),
    try
        ok = application:set_env(rabbit, channel_interceptor_priorities,
                                 [{no_such_channel_interceptor_module, 1}]),
        ok = rabbit_channel_interceptor:warn_unknown_priorities(),
        receive
            {log_event, #{msg := {Format, Args}}} ->
                Text = unicode:characters_to_list(io_lib:format(Format, Args)),
                ?assert(string:find(Text, "no_such_channel_interceptor_module") =/= nomatch)
        after 5000 ->
            ct:fail("Expected a warning about an unknown interceptor priority")
        end
    after
        logger:remove_handler(HandlerId),
        application:set_env(rabbit, channel_interceptor_priorities, Before)
    end,
    passed.

%% logger handler callback used by warn_unknown_priorities_logs_a_warning1/1
log(#{meta := #{mfa := {rabbit_channel_interceptor, warn_unknown_priorities, _}}} = LogEvent,
    #{config := #{pid := Pid}}) ->
    Pid ! {log_event, LogEvent};
log(_LogEvent, _Config) ->
    ok.

with_conflicting_interceptors(Fun) ->
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"dummy interceptor">>,
                                  dummy_interceptor),
    ok = rabbit_registry:register(channel_interceptor,
                                  <<"conflicting dummy interceptor">>,
                                  dummy_interceptor_conflicting),
    try
        Fun()
    after
        ok = rabbit_registry:unregister(channel_interceptor,
                                        <<"dummy interceptor">>),
        ok = rabbit_registry:unregister(channel_interceptor,
                                        <<"conflicting dummy interceptor">>)
    end.

>>>>>>> b29b6e6dd2 (Polishing #16046)
check_send_receive(Ch1, QName, Send, Receive) ->
    amqp_channel:call(Ch1,
                        #'basic.publish'{routing_key = QName},
                        #amqp_msg{payload = Send}),

    {#'basic.get_ok'{}, #amqp_msg{payload = Receive}} =
        amqp_channel:call(Ch1, #'basic.get'{queue = QName,
                                              no_ack = true}).
