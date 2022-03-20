%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("amqp_client.hrl").
-include("amqp_client_internal.hrl").

-compile(export_all).

-define(UNAUTHORIZED_USER, <<"test_user_no_perm">>).

%% The latch constant defines how many processes are spawned in order
%% to run certain functionality in parallel. It follows the standard
%% countdown latch pattern.
-define(LATCH, 100).

%% The wait constant defines how long a consumer waits before it
%% unsubscribes
-define(WAIT, 10000).

%% How to long wait for a process to die after an expected failure
-define(PROCESS_EXIT_TIMEOUT, 5000).

all() ->
    [
      {group, direct_connection_tests},
      {group, network_connection_tests}
    ].

-define(COMMON_PARALLEL_TEST_CASES, [
    simultaneous_close,
    basic_recover,
    basic_consume,
    consume_notification,
    basic_nack,
    large_content,
    lifecycle,
    no_vhost,
    nowait_exchange_declare,
    channel_repeat_open_close,
    channel_multi_open_close,
    basic_ack,
    basic_ack_call,
    channel_lifecycle,
    queue_unbind,
    sync_method_serialization,
    async_sync_method_serialization,
    sync_async_method_serialization,
    rpc,
    rpc_client,
    confirm,
    confirm_barrier,
    confirm_select_before_wait,
    confirm_barrier_timeout,
    confirm_barrier_die_timeout,
    default_consumer,
    subscribe_nowait,
    non_existent_exchange,
    non_existent_user,
    invalid_password,
    non_existent_vhost,
    no_permission,
    channel_writer_death,
    command_invalid_over_channel,
    named_connection,
    {teardown_loop, [{repeat, 100}, parallel], [teardown]},
    {bogus_rpc_loop, [{repeat, 100}, parallel], [bogus_rpc]},
    {hard_error_loop, [{repeat, 100}, parallel], [hard_error]}
  ]).
-define(COMMON_NON_PARALLEL_TEST_CASES, [
    basic_qos, %% Not parallel because it's time-based, or has mocks
    connection_failure,
    channel_death,
    safe_call_timeouts
  ]).

groups() ->
    [
      {direct_connection_tests, [], [
          {parallel_tests, [parallel], [
              basic_get_direct,
              no_user,
              no_password
              | ?COMMON_PARALLEL_TEST_CASES]},
          {non_parallel_tests, [], ?COMMON_NON_PARALLEL_TEST_CASES}
        ]},
      {network_connection_tests, [], [
          {parallel_tests, [parallel], [
              basic_get_ipv4,
              basic_get_ipv6,
              basic_get_ipv4_ssl,
              basic_get_ipv6_ssl,
              pub_and_close,
              channel_tune_negotiation,
              shortstr_overflow_property,
              shortstr_overflow_field,
              command_invalid_over_channel0
              | ?COMMON_PARALLEL_TEST_CASES]},
          {non_parallel_tests, [], [
              connection_blocked_network
              | ?COMMON_NON_PARALLEL_TEST_CASES]}
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++ [
        fun ensure_amqp_client_srcdir/1,
        fun create_unauthorized_user/1
      ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, [
        fun delete_unauthorized_user/1
      ] ++ rabbit_ct_broker_helpers:teardown_steps()).

ensure_amqp_client_srcdir(Config) ->
    case rabbit_ct_helpers:get_config(Config, rabbitmq_run_cmd) of
        undefined ->
            rabbit_ct_helpers:ensure_application_srcdir(Config,
                                                        amqp_client, amqp_client);
        _ -> Config
    end.

create_unauthorized_user(Config) ->
    Cmd = ["add_user", ?UNAUTHORIZED_USER, ?UNAUTHORIZED_USER],
    case rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd) of
        {ok, _} -> rabbit_ct_helpers:set_config(
                  Config,
                  [{rmq_unauthorized_username, ?UNAUTHORIZED_USER},
                   {rmq_unauthorized_password, ?UNAUTHORIZED_USER}]);
        _       -> {skip, "Failed to create unauthorized user"}
    end.

delete_unauthorized_user(Config) ->
    Cmd = ["delete_user", ?UNAUTHORIZED_USER],
    rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd),
    Config.

%% -------------------------------------------------------------------
%% Groups.
%% -------------------------------------------------------------------

init_per_group(direct_connection_tests, Config) ->
    rabbit_ct_helpers:set_config(Config, {amqp_client_conn_type, direct});
init_per_group(network_connection_tests, Config) ->
    rabbit_ct_helpers:set_config(Config, {amqp_client_conn_type, network});
init_per_group(Group, Config)
  when Group =:= parallel_tests
  orelse Group =:= non_parallel_tests
  orelse Group =:= teardown_loop
  orelse Group =:= bogus_rpc_loop
  orelse Group =:= hard_error_loop ->
    case ?config(amqp_client_conn_type, Config) of
        undefined -> rabbit_ct_helpers:set_config(
                       Config, {amqp_client_conn_type, network});
        _         -> Config
    end.

end_per_group(_, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

init_per_testcase(Test, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Test),
    {Username, Password} = case Test of
        no_user           -> {none,
                              none};
        no_password       -> {?config(rmq_username, Config),
                              none};
        non_existent_user -> {<<"no-user">>,
                              <<"no-user">>};
        invalid_password  -> {?config(rmq_username, Config),
                              <<"bad">>};
        no_permission     -> {?config(rmq_unauthorized_username, Config),
                              ?config(rmq_unauthorized_password, Config)};
        _                 -> {?config(rmq_username, Config),
                              ?config(rmq_password, Config)}
    end,
    VHost = case Test of
        no_vhost           -> <<"/noexist">>;
        non_existent_vhost -> <<"oops">>;
        _                  -> ?config(rmq_vhost, Config)
    end,
    Host = case Test of
        basic_get_ipv4     -> "127.0.0.1";
        basic_get_ipv6     -> "::1";
        basic_get_ipv4_ssl -> "127.0.0.1";
        basic_get_ipv6_ssl -> "::1";
        _                  -> ?config(rmq_hostname, Config)
    end,
    {Port, SSLOpts} = if
        Test =:= basic_get_ipv4_ssl orelse
        Test =:= basic_get_ipv6_ssl ->
            CertsDir = ?config(rmq_certsdir, Config),
            % Compute SNI from the server's certificate directly.
            {ok, PemBin} = file:read_file(filename:join([CertsDir,
                                                         "client",
                                                         "cert.pem"])),
            [{_, DerCert, _}] = public_key:pem_decode(PemBin),
            [CN] = rabbit_cert_info:subject_items(DerCert,
                                                  ?'id-at-commonName'),
            {
              rabbit_ct_broker_helpers:get_node_config(Config, 0,
                tcp_port_amqp_tls),
              [
                {cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                {certfile, filename:join([CertsDir, "client", "cert.pem"])},
                {keyfile, filename:join([CertsDir, "client", "key.pem"])},
                {verify, verify_peer},
                {server_name_indication, CN}
              ]
            };
        true ->
            {
              rabbit_ct_broker_helpers:get_node_config(Config, 0,
                tcp_port_amqp),
              none
            }
    end,
    ChannelMax = case Test of
        channel_tune_negotiation -> 10;
        _                        -> ?config(rmq_channel_max, Config)
    end,
    ConnParams = case ?config(amqp_client_conn_type, Config) of
        direct ->
            #amqp_params_direct{
              username     = Username,
              password     = Password,
              node         = rabbit_ct_broker_helpers:get_node_config(Config,
                               0, nodename),
              virtual_host = VHost};
        network ->
            #amqp_params_network{
              username     = Username,
              password     = Password,
              host         = Host,
              port         = Port,
              virtual_host = VHost,
              channel_max  = ChannelMax,
              ssl_options  = SSLOpts}
    end,
    rabbit_ct_helpers:set_config(Config,
                                 {amqp_client_conn_params, ConnParams}).

end_per_testcase(Test, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Test).

%% -------------------------------------------------------------------

basic_get_direct(Config)   -> basic_get(Config).
basic_get_ipv4(Config)     -> basic_get(Config).
basic_get_ipv6(Config)     -> basic_get(Config).
basic_get_ipv4_ssl(Config) -> basic_get(Config).
basic_get_ipv6_ssl(Config) -> basic_get(Config).

basic_get(Config) ->
    case new_connection(Config) of
        {ok, Connection} ->
            {ok, Channel} = amqp_connection:open_channel(Connection),
            Payload = <<"foobar">>,
            {ok, Q} = setup_publish(Channel, Payload),
            get_and_assert_equals(Channel, Q, Payload),
            get_and_assert_empty(Channel, Q),
            teardown(Connection, Channel);
        {error, Posix}
          when Posix =:= eaddrnotavail orelse Posix =:= enetunreach ->
            {skip, inet:format_error(Posix)}
    end.

named_connection(Config) ->
    ConnName = <<"Custom Name">>,
    Params = ?config(amqp_client_conn_params, Config),
    {ok, Connection} = amqp_connection:start(Params, ConnName),
    ConnName = amqp_connection:connection_name(Connection),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Payload = <<"foobar">>,
    {ok, Q} = setup_publish(Channel, Payload),
    get_and_assert_equals(Channel, Q, Payload),
    get_and_assert_empty(Channel, Q),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

safe_call_timeouts(Config) ->
    Params = ?config(amqp_client_conn_params, Config),
    safe_call_timeouts_test(Params).

safe_call_timeouts_test(Params = #amqp_params_network{}) ->
    TestConnTimeout = 2000,
    TestCallTimeout = 1000,

    Params1 = Params#amqp_params_network{connection_timeout = TestConnTimeout},

    %% Normal connection
    amqp_util:update_call_timeout(TestCallTimeout),

    {ok, Connection1} = amqp_connection:start(Params1),
    ?assertEqual(TestConnTimeout + ?CALL_TIMEOUT_DEVIATION, amqp_util:call_timeout()),

    ?assertEqual(ok, amqp_connection:close(Connection1)),
    wait_for_death(Connection1),

    %% Failing connection
    amqp_util:update_call_timeout(TestCallTimeout),

    ok = meck:new(amqp_network_connection, [passthrough]),
    ok = meck:expect(amqp_network_connection, connect,
            fun(_AmqpParams, _SIF, _TypeSup, _State) ->
                timer:sleep(TestConnTimeout),
                {error, test_connection_timeout}
            end),

    ?assertEqual({error, test_connection_timeout}, amqp_connection:start(Params1)),

    ?assertEqual(TestConnTimeout + ?CALL_TIMEOUT_DEVIATION, amqp_util:call_timeout()),

    meck:unload(amqp_network_connection);

safe_call_timeouts_test(Params = #amqp_params_direct{}) ->
    TestCallTimeout = 30000,
    NetTicktime0 = net_kernel:get_net_ticktime(),
    amqp_util:update_call_timeout(TestCallTimeout),

    %% 1. NetTicktime >= DIRECT_OPERATION_TIMEOUT (120s)
    NetTicktime1 = 140,
    net_kernel:set_net_ticktime(NetTicktime1, 1),
    wait_until_net_ticktime(NetTicktime1),

    {ok, Connection1} = amqp_connection:start(Params),
    ?assertEqual((NetTicktime1 * 1000) + ?CALL_TIMEOUT_DEVIATION,
        amqp_util:call_timeout()),

    ?assertEqual(ok, amqp_connection:close(Connection1)),
    wait_for_death(Connection1),

    %% Reset call timeout
    amqp_util:update_call_timeout(TestCallTimeout),

    %% 2. Transitioning NetTicktime >= DIRECT_OPERATION_TIMEOUT (120s)
    NetTicktime2 = 120,
    net_kernel:set_net_ticktime(NetTicktime2, 1),
    ?assertEqual({ongoing_change_to, NetTicktime2}, net_kernel:get_net_ticktime()),

    {ok, Connection2} = amqp_connection:start(Params),
    ?assertEqual((NetTicktime2 * 1000) + ?CALL_TIMEOUT_DEVIATION,
        amqp_util:call_timeout()),

    wait_until_net_ticktime(NetTicktime2),

    ?assertEqual(ok, amqp_connection:close(Connection2)),
    wait_for_death(Connection2),

    %% Reset call timeout
    amqp_util:update_call_timeout(TestCallTimeout),

    %% 3. NetTicktime < DIRECT_OPERATION_TIMEOUT (120s)
    NetTicktime3 = 60,
    net_kernel:set_net_ticktime(NetTicktime3, 1),
    wait_until_net_ticktime(NetTicktime3),

    {ok, Connection3} = amqp_connection:start(Params),
    ?assertEqual((?DIRECT_OPERATION_TIMEOUT + ?CALL_TIMEOUT_DEVIATION),
        amqp_util:call_timeout()),

    net_kernel:set_net_ticktime(NetTicktime0, 1),
    wait_until_net_ticktime(NetTicktime0),
    ?assertEqual(ok, amqp_connection:close(Connection3)),
    wait_for_death(Connection3),

    %% Failing direct connection
    amqp_util:update_call_timeout(_LowCallTimeout = 1000),

    ok = meck:new(amqp_direct_connection, [passthrough]),
    ok = meck:expect(amqp_direct_connection, connect,
            fun(_AmqpParams, _SIF, _TypeSup, _State) ->
                timer:sleep(2000),
                {error, test_connection_timeout}
            end),

    ?assertEqual({error, test_connection_timeout}, amqp_connection:start(Params)),

    ?assertEqual((?DIRECT_OPERATION_TIMEOUT + ?CALL_TIMEOUT_DEVIATION),
        amqp_util:call_timeout()),

    meck:unload(amqp_direct_connection).

%% -------------------------------------------------------------------

simultaneous_close(Config) ->
    {ok, Connection} = new_connection(Config),
    %% We pick a high channel number, to avoid any conflict with other
    %% tests running in parallel.
    ChannelNumber = case ?config(rmq_channel_max, Config) of
        0 -> ?MAX_CHANNEL_NUMBER;
        N -> N
    end,
    {ok, Channel1} = amqp_connection:open_channel(Connection, ChannelNumber),

    %% Publish to non-existent exchange and immediately close channel
    amqp_channel:cast(Channel1, #'basic.publish'{exchange = <<"does-not-exist">>,
                                                 routing_key = <<"a">>},
                               #amqp_msg{payload = <<"foobar">>}),
    try amqp_channel:close(Channel1) of
        ok      -> wait_for_death(Channel1);
        closing -> wait_for_death(Channel1)
    catch
        exit:{noproc, _}                                              -> ok;
        exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _}}, _} -> ok
    end,

    %% Channel2 (opened with the exact same number as Channel1)
    %% should not receive a close_ok (which is intended for Channel1)
    {ok, Channel2} = amqp_connection:open_channel(Connection, ChannelNumber),

    %% Make sure Channel2 functions normally
    #'exchange.declare_ok'{} =
        amqp_channel:call(Channel2,
          #'exchange.declare'{exchange = <<"simultaneous_close">>}),
    #'exchange.delete_ok'{} =
        amqp_channel:call(Channel2,
          #'exchange.delete'{exchange = <<"simultaneous_close">>}),

    teardown(Connection, Channel2).

%% -------------------------------------------------------------------

basic_qos(Config) ->
    [NoQos, Qos] = [basic_qos_test(Config, Prefetch) || Prefetch <- [0,1]],
    ExpectedRatio = (1+1) / (1+50/5),
    FudgeFactor = 2, %% account for timing variations
    ct:pal(?LOW_IMPORTANCE,
      "QOS=0 -> ~p (noqos)~n"
      "QOS=1 -> ~p (qos)~n"
      "qos / noqos < ~p * ~p = ~p < ~p = ~p~n",
      [NoQos, Qos, ExpectedRatio, FudgeFactor, Qos / NoQos, ExpectedRatio * FudgeFactor, Qos / NoQos < ExpectedRatio * FudgeFactor]),
    true = Qos / NoQos < ExpectedRatio * FudgeFactor.

basic_qos_test(Config, Prefetch) ->
    {ok, Connection} = new_connection(Config),
    Messages = 100,
    Workers = [5, 50],
    Parent = self(),
    {ok, Chan} = amqp_connection:open_channel(Connection),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Chan, #'queue.declare'{}),
    Kids = [spawn(
            fun() ->
                {ok, Channel} = amqp_connection:open_channel(Connection),
                amqp_channel:call(Channel,
                                  #'basic.qos'{prefetch_count = Prefetch}),
                amqp_channel:call(Channel,
                                  #'basic.consume'{queue = Q}),
                Parent ! finished,
                sleeping_consumer(Channel, Sleep, Parent)
            end) || Sleep <- Workers],
    latch_loop(length(Kids)),
    spawn(fun() -> {ok, Channel} = amqp_connection:open_channel(Connection),
                   producer_loop(Channel, Q, Messages)
          end),
    {Res, _} = timer:tc(erlang, apply, [fun latch_loop/1, [Messages]]),
    [Kid ! stop || Kid <- Kids],
    latch_loop(length(Kids)),
    teardown(Connection, Chan),
    Res.

sleeping_consumer(Channel, Sleep, Parent) ->
    receive
        stop ->
            do_stop(Channel, Parent);
        #'basic.consume_ok'{} ->
            sleeping_consumer(Channel, Sleep, Parent);
        #'basic.cancel_ok'{}  ->
            exit(unexpected_cancel_ok);
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _Content} ->
            Parent ! finished,
            receive stop -> do_stop(Channel, Parent)
            after Sleep -> ok
            end,
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = DeliveryTag}),
            sleeping_consumer(Channel, Sleep, Parent)
    end.

do_stop(Channel, Parent) ->
    Parent ! finished,
    amqp_channel:close(Channel),
    wait_for_death(Channel),
    exit(normal).

producer_loop(Channel, _RoutingKey, 0) ->
    amqp_channel:close(Channel),
    wait_for_death(Channel),
    ok;

producer_loop(Channel, RoutingKey, N) ->
    Publish = #'basic.publish'{exchange = <<>>, routing_key = RoutingKey},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = <<>>}),
    producer_loop(Channel, RoutingKey, N - 1).

%% -------------------------------------------------------------------

basic_recover(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(
                        Connection, {amqp_direct_consumer, [self()]}),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{}),
    #'basic.consume_ok'{consumer_tag = Tag} =
        amqp_channel:call(Channel, #'basic.consume'{queue = Q}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = <<"foobar">>}),
    receive
        {#'basic.deliver'{consumer_tag = Tag}, _} ->
            %% no_ack set to false, but don't send ack
            ok
    end,
    BasicRecover = #'basic.recover'{requeue = true},
    amqp_channel:cast(Channel, BasicRecover),
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag2}, _} ->
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = DeliveryTag2})
    end,
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

basic_consume(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    X = <<"basic_consume">>,
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),
    RoutingKey = <<"key">>,
    Parent = self(),
    [spawn_link(fun () ->
                        consume_loop(Channel, X, RoutingKey, Parent, <<Tag:32>>)
                end) || Tag <- lists:seq(1, ?LATCH)],
    latch_loop(?LATCH),
    Publish = #'basic.publish'{exchange = X, routing_key = RoutingKey},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = <<"foobar">>}),
    latch_loop(?LATCH),
    amqp_channel:call(Channel, #'exchange.delete'{exchange = X}),
    teardown(Connection, Channel).

consume_loop(Channel, X, RoutingKey, Parent, Tag) ->
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{}),
    #'queue.bind_ok'{} =
        amqp_channel:call(Channel, #'queue.bind'{queue = Q,
                                                 exchange = X,
                                                 routing_key = RoutingKey}),
    #'basic.consume_ok'{} =
        amqp_channel:call(Channel,
                          #'basic.consume'{queue = Q, consumer_tag = Tag}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    Parent ! finished,
    receive {#'basic.deliver'{}, _} -> ok end,
    #'basic.cancel_ok'{} =
        amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
    receive #'basic.cancel_ok'{consumer_tag = Tag} -> ok end,
    Parent ! finished.

%% -------------------------------------------------------------------

consume_notification(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{}),
    #'basic.consume_ok'{consumer_tag = CTag} = ConsumeOk =
        amqp_channel:call(Channel, #'basic.consume'{queue = Q}),
    receive ConsumeOk -> ok end,
    #'queue.delete_ok'{} =
        amqp_channel:call(Channel, #'queue.delete'{queue = Q}),
    receive #'basic.cancel'{consumer_tag = CTag} -> ok end,
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

basic_nack(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{}),

    Payload = <<"m1">>,

    amqp_channel:call(Channel,
                      #'basic.publish'{exchange = <<>>, routing_key = Q},
                      #amqp_msg{payload = Payload}),

    #'basic.get_ok'{delivery_tag = Tag} =
        get_and_assert_equals(Channel, Q, Payload, false),

    amqp_channel:call(Channel, #'basic.nack'{delivery_tag = Tag,
                                             multiple     = false,
                                             requeue      = false}),

    get_and_assert_empty(Channel, Q),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

large_content(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{}),
    F = list_to_binary([rand:uniform(256)-1 || _ <- lists:seq(1, 1000)]),
    Payload = list_to_binary([F || _ <- lists:seq(1, 1000)]),
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    get_and_assert_equals(Channel, Q, Payload),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

lifecycle(Config) ->
    {ok, Connection} = new_connection(Config),
    X = <<"lifecycle">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel,
                      #'exchange.declare'{exchange = X,
                                          type = <<"topic">>}),
    Parent = self(),
    [spawn(fun () -> queue_exchange_binding(Channel, X, Parent, Tag) end)
     || Tag <- lists:seq(1, ?LATCH)],
    latch_loop(?LATCH),
    amqp_channel:call(Channel, #'exchange.delete'{exchange = X}),
    teardown(Connection, Channel).

queue_exchange_binding(Channel, X, Parent, Tag) ->
    receive
        nothing -> ok
    after (?LATCH - Tag rem 7) * 10 ->
        ok
    end,
    Q = list_to_binary(rabbit_misc:format("lifecycle.a.b.c.~b", [Tag])),
    Binding = <<"lifecycle.a.b.c.*">>,
    #'queue.declare_ok'{queue = Q1}
        = amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    Q = Q1,
    Route = #'queue.bind'{queue = Q,
                          exchange = X,
                          routing_key = Binding},
    amqp_channel:call(Channel, Route),
    amqp_channel:call(Channel, #'queue.delete'{queue = Q}),
    Parent ! finished.

%% -------------------------------------------------------------------

no_user(Config)     -> no_something(Config).
no_password(Config) -> no_something(Config).

no_something(Config) ->
    {ok, Connection} = new_connection(Config),
    amqp_connection:close(Connection),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

no_vhost(Config) ->
    {error, not_allowed} = new_connection(Config),
    ok.

%% -------------------------------------------------------------------

nowait_exchange_declare(Config) ->
    {ok, Connection} = new_connection(Config),
    X = <<"nowait_exchange_declare">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    ok = amqp_channel:call(Channel, #'exchange.declare'{exchange = X,
                                                        type = <<"topic">>,
                                                        nowait = true}),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

channel_repeat_open_close(Config) ->
    {ok, Connection} = new_connection(Config),
    lists:foreach(
        fun(_) ->
            {ok, Ch} = amqp_connection:open_channel(Connection),
            ok = amqp_channel:close(Ch)
        end, lists:seq(1, 50)),
    amqp_connection:close(Connection),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

channel_multi_open_close(Config) ->
    {ok, Connection} = new_connection(Config),
    [spawn_link(
        fun() ->
            try amqp_connection:open_channel(Connection) of
                {ok, Ch}           -> try amqp_channel:close(Ch) of
                                          ok                 -> ok;
                                          closing            -> ok
                                      catch
                                          exit:{noproc, _} -> ok;
                                          exit:{normal, _} -> ok
                                      end;
                closing            -> ok
            catch
                exit:{noproc, _} -> ok;
                exit:{normal, _} -> ok
            end
        end) || _ <- lists:seq(1, 50)],
    erlang:yield(),
    amqp_connection:close(Connection),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

basic_ack(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    {#'basic.get_ok'{delivery_tag = Tag}, _}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = false}),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

basic_ack_call(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    {#'basic.get_ok'{delivery_tag = Tag}, _}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = false}),
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = Tag}),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

channel_lifecycle(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:close(Channel),
    {ok, Channel2} = amqp_connection:open_channel(Connection),
    teardown(Connection, Channel2).

%% -------------------------------------------------------------------

queue_unbind(Config) ->
    {ok, Connection} = new_connection(Config),
    X = <<"queue_unbind-eggs">>,
    Q = <<"queue_unbind-foobar">>,
    Key = <<"quay">>,
    Payload = <<"foobar">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),
    amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    Bind = #'queue.bind'{queue = Q,
                         exchange = X,
                         routing_key = Key},
    amqp_channel:call(Channel, Bind),
    Publish = #'basic.publish'{exchange = X, routing_key = Key},
    amqp_channel:call(Channel, Publish, Msg = #amqp_msg{payload = Payload}),
    get_and_assert_equals(Channel, Q, Payload),
    Unbind = #'queue.unbind'{queue = Q,
                             exchange = X,
                             routing_key = Key},
    amqp_channel:call(Channel, Unbind),
    amqp_channel:call(Channel, Publish, Msg),
    get_and_assert_empty(Channel, Q),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

%% This is designed to exercise the internal queuing mechanism
%% to ensure that sync methods are properly serialized
sync_method_serialization(Config) ->
    abstract_method_serialization_test(
        "sync_method_serialization", Config,
        fun (_, _) -> ok end,
        fun (Channel, _, _, _, Count) ->
                Q = fmt("sync_method_serialization-~p", [Count]),
                #'queue.declare_ok'{queue = Q1} =
                    amqp_channel:call(Channel,
                                      #'queue.declare'{queue     = Q,
                                                       exclusive = true}),
                Q = Q1
        end,
        fun (_, _, _, _, _) -> ok end).

%% This is designed to exercise the internal queuing mechanism
%% to ensure that sending async methods and then a sync method is serialized
%% properly
async_sync_method_serialization(Config) ->
    abstract_method_serialization_test(
        "async_sync_method_serialization", Config,
        fun (Channel, _X) ->
                #'queue.declare_ok'{queue = Q} =
                    amqp_channel:call(Channel, #'queue.declare'{}),
                Q
        end,
        fun (Channel, X, Payload, _, _) ->
                %% The async methods
                ok = amqp_channel:call(Channel,
                                       #'basic.publish'{exchange = X,
                                                        routing_key = <<"a">>},
                                       #amqp_msg{payload = Payload})
        end,
        fun (Channel, X, _, Q, _) ->
                %% The sync method
                #'queue.bind_ok'{} =
                    amqp_channel:call(Channel,
                                      #'queue.bind'{exchange = X,
                                                    queue = Q,
                                                    routing_key = <<"a">>}),
                %% No message should have been routed
                #'queue.declare_ok'{message_count = 0} =
                    amqp_channel:call(Channel,
                                      #'queue.declare'{queue = Q,
                                                       passive = true})
        end).

%% This is designed to exercise the internal queuing mechanism
%% to ensure that sending sync methods and then an async method is serialized
%% properly
sync_async_method_serialization(Config) ->
    abstract_method_serialization_test(
        "sync_async_method_serialization", Config,
        fun (_, _) -> ok end,
        fun (Channel, X, _Payload, _, _) ->
                %% The sync methods (called with cast to resume immediately;
                %% the order should still be preserved)
                #'queue.declare_ok'{queue = Q} =
                    amqp_channel:call(Channel,
                                      #'queue.declare'{exclusive = true}),
                amqp_channel:cast(Channel, #'queue.bind'{exchange = X,
                                                         queue = Q,
                                                         routing_key= <<"a">>}),
                Q
        end,
        fun (Channel, X, Payload, _, MultiOpRet) ->
                #'confirm.select_ok'{} = amqp_channel:call(
                                           Channel, #'confirm.select'{}),
                ok = amqp_channel:call(Channel,
                                       #'basic.publish'{exchange = X,
                                                        routing_key = <<"a">>},
                                       #amqp_msg{payload = Payload}),
                %% All queues must have gotten this message
                true = amqp_channel:wait_for_confirms(Channel),
                lists:foreach(
                    fun (Q) ->
                            #'queue.declare_ok'{message_count = 1} =
                                amqp_channel:call(
                                  Channel, #'queue.declare'{queue   = Q,
                                                            passive = true})
                    end, lists:flatten(MultiOpRet))
        end).

abstract_method_serialization_test(Test, Config,
                                   BeforeFun, MultiOpFun, AfterFun) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    X = list_to_binary(Test),
    Payload = list_to_binary(["x" || _ <- lists:seq(1, 1000)]),
    OpsPerProcess = 20,
    #'exchange.declare_ok'{} =
        amqp_channel:call(Channel, #'exchange.declare'{exchange = X,
                                                       type = <<"topic">>}),
    BeforeRet = BeforeFun(Channel, X),
    Parent = self(),
    [spawn(fun () -> Ret = [MultiOpFun(Channel, X, Payload, BeforeRet, I)
                            || _ <- lists:seq(1, OpsPerProcess)],
                   Parent ! {finished, Ret}
           end) || I <- lists:seq(1, ?LATCH)],
    MultiOpRet = latch_loop(?LATCH),
    AfterFun(Channel, X, Payload, BeforeRet, MultiOpRet),
    amqp_channel:call(Channel, #'exchange.delete'{exchange = X}),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

teardown(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    true = is_process_alive(Channel),
    true = is_process_alive(Connection),
    teardown(Connection, Channel),
    false = is_process_alive(Channel),
    false = is_process_alive(Connection).

%% -------------------------------------------------------------------

%% This tests whether RPC over AMQP produces the same result as invoking the
%% same argument against the same underlying gen_server instance.
rpc(Config) ->
    {ok, Connection} = new_connection(Config),
    Fun = fun(X) -> X + 1 end,
    RPCHandler = fun(X) -> term_to_binary(Fun(binary_to_term(X))) end,
    Q = <<"rpc-test">>,
    Server = amqp_rpc_server:start(Connection, Q, RPCHandler),
    Client = amqp_rpc_client:start(Connection, Q),
    Input = 1,
    %% give both server and client a moment to initialise,
    %% set up their topology and so on
    timer:sleep(2000),
    Reply = amqp_rpc_client:call(Client, term_to_binary(Input)),
    Expected = Fun(Input),
    DecodedReply = binary_to_term(Reply),
    Expected = DecodedReply,
    amqp_rpc_client:stop(Client),
    amqp_rpc_server:stop(Server),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'queue.delete'{queue = Q}),
    teardown(Connection, Channel).

%% This tests if the RPC continues to generate valid correlation ids
%% over a series of requests.
rpc_client(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Q = <<"rpc-client-test">>,
    Latch = 255, % enough requests to tickle bad correlation ids
    %% Start a server to return correlation ids to the client.
    Server = spawn_link(fun() ->
                                rpc_correlation_server(Channel, Q)
                        end),
    %% Generate a series of RPC requests on the same client.
    Client = amqp_rpc_client:start(Connection, Q),
    Parent = self(),
    [spawn(fun() ->
                   Reply = amqp_rpc_client:call(Client, <<>>),
                   Parent ! {finished, Reply}
           end) || _ <- lists:seq(1, Latch)],
    %% Verify that the correlation ids are valid UTF-8 strings.
    CorrelationIds = latch_loop(Latch),
    [<<_/binary>> = DecodedId
     || DecodedId <- [unicode:characters_to_binary(Id, utf8)
                      || Id <- CorrelationIds]],
    %% Cleanup.
    Server ! stop,
    amqp_rpc_client:stop(Client),
    amqp_channel:call(Channel, #'queue.delete'{queue = Q}),
    teardown(Connection, Channel).

%% Consumer of RPC requests that replies with the CorrelationId.
rpc_correlation_server(Channel, Q) ->
    ok = amqp_channel:register_return_handler(Channel, self()),
    #'queue.declare_ok'{queue = Q} =
      amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    #'basic.consume_ok'{} =
      amqp_channel:call(Channel,
                        #'basic.consume'{queue = Q,
                                         consumer_tag = <<"server">>}),
    ok = rpc_client_consume_loop(Channel),
    #'basic.cancel_ok'{} =
      amqp_channel:call(Channel,
                        #'basic.cancel'{consumer_tag = <<"server">>}),
    ok = amqp_channel:unregister_return_handler(Channel).

rpc_client_consume_loop(Channel) ->
    receive
        stop ->
            ok;
        {#'basic.deliver'{delivery_tag = DeliveryTag},
         #amqp_msg{props = Props}} ->
            #'P_basic'{correlation_id = CorrelationId,
                       reply_to = Q} = Props,
            Properties = #'P_basic'{correlation_id = CorrelationId},
            Publish = #'basic.publish'{exchange = <<>>,
                                       routing_key = Q,
                                       mandatory = true},
            amqp_channel:call(
              Channel, Publish, #amqp_msg{props = Properties,
                                          payload = CorrelationId}),
            amqp_channel:call(
              Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
            rpc_client_consume_loop(Channel);
        _ ->
            rpc_client_consume_loop(Channel)
    after 5000 ->
            exit(no_request_received)
    end.

%% -------------------------------------------------------------------

%% Test for the network client
%% Sends a bunch of messages and immediately closes the connection without
%% closing the channel. Then gets the messages back from the queue and expects
%% all of them to have been sent.
pub_and_close(Config) ->
    {ok, Connection1} = new_connection(Config),
    Payload = <<"eggs">>,
    NMessages = 50000,
    {ok, Channel1} = amqp_connection:open_channel(Connection1),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel1, #'queue.declare'{}),
    %% Send messages
    pc_producer_loop(Channel1, <<>>, Q, Payload, NMessages),
    %% Close connection without closing channels
    amqp_connection:close(Connection1),
    %% Get sent messages back and count them
    {ok, Connection2} = new_connection(Config),
    {ok, Channel2} = amqp_connection:open_channel(
                         Connection2, {amqp_direct_consumer, [self()]}),
    amqp_channel:call(Channel2, #'basic.consume'{queue = Q, no_ack = true}),
    receive #'basic.consume_ok'{} -> ok end,
    true = (pc_consumer_loop(Channel2, Payload, 0) == NMessages),
    %% Make sure queue is empty
    #'queue.declare_ok'{queue = Q, message_count = NRemaining} =
        amqp_channel:call(Channel2, #'queue.declare'{queue   = Q,
                                                     passive = true}),
    true = (NRemaining == 0),
    amqp_channel:call(Channel2, #'queue.delete'{queue = Q}),
    teardown(Connection2, Channel2).

pc_producer_loop(_, _, _, _, 0) -> ok;
pc_producer_loop(Channel, X, Key, Payload, NRemaining) ->
    Publish = #'basic.publish'{exchange = X, routing_key = Key},
    ok = amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    pc_producer_loop(Channel, X, Key, Payload, NRemaining - 1).

pc_consumer_loop(Channel, Payload, NReceived) ->
    receive
        {#'basic.deliver'{},
         #amqp_msg{payload = DeliveredPayload}} ->
            case DeliveredPayload of
                Payload ->
                    pc_consumer_loop(Channel, Payload, NReceived + 1);
                _ ->
                    exit(received_unexpected_content)
            end
    after 1000 ->
        NReceived
    end.

%% -------------------------------------------------------------------

channel_tune_negotiation(Config) ->
    {ok, Connection} = new_connection(Config),
    amqp_connection:close(Connection),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

confirm(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Channel, self()),
    {ok, Q} = setup_publish(Channel),
    {#'basic.get_ok'{}, _}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = false}),
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 2000 ->
                 exit(did_not_receive_pub_ack)
         end,
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

confirm_barrier(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    [amqp_channel:call(
        Channel,
        #'basic.publish'{routing_key = <<"whoosh-confirm_barrier">>},
        #amqp_msg{payload = <<"foo">>})
     || _ <- lists:seq(1, 1000)], %% Hopefully enough to get a multi-ack
    true = amqp_channel:wait_for_confirms(Channel),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

confirm_select_before_wait(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    try amqp_channel:wait_for_confirms(Channel) of
        _ -> exit(success_despite_lack_of_confirm_mode)
    catch
        not_in_confirm_mode -> ok
    end,
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

confirm_barrier_timeout(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    [amqp_channel:call(
        Channel,
        #'basic.publish'{routing_key = <<"whoosh-confirm_barrier_timeout">>},
        #amqp_msg{payload = <<"foo">>})
     || _ <- lists:seq(1, 1000)],
    case amqp_channel:wait_for_confirms(Channel, 0) of
        true    -> ok;
        timeout -> ok
    end,
    true = amqp_channel:wait_for_confirms(Channel),
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

confirm_barrier_die_timeout(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    [amqp_channel:call(
        Channel,
        #'basic.publish'{routing_key = <<"whoosh-confirm_barrier_die_timeout">>},
        #amqp_msg{payload = <<"foo">>})
     || _ <- lists:seq(1, 1000)],
    try amqp_channel:wait_for_confirms_or_die(Channel, 0) of
        true -> ok
    catch
        exit:timeout -> ok
    end,
    amqp_connection:close(Connection),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

default_consumer(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_selective_consumer:register_default_consumer(Channel, self()),

    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{}),
    Pid = spawn(fun () -> receive
                          after 10000 -> ok
                          end
                end),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q}, Pid),
    erlang:monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', _, process, _, _} ->
            io:format("little consumer died out~n")
    end,
    Payload = <<"for the default consumer">>,
    amqp_channel:call(Channel,
                      #'basic.publish'{exchange = <<>>, routing_key = Q},
                      #amqp_msg{payload = Payload}),

    receive
        {#'basic.deliver'{}, #'amqp_msg'{payload = Payload}} ->
            ok
    after 1000 ->
            exit('default_consumer_didnt_work')
    end,
    teardown(Connection, Channel).

%% -------------------------------------------------------------------

subscribe_nowait(Config) ->
    {ok, Conn} = new_connection(Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, Q} = setup_publish(Ch),
    CTag = <<"ctag">>,
    amqp_selective_consumer:register_default_consumer(Ch, self()),
    ok = amqp_channel:call(Ch, #'basic.consume'{queue        = Q,
                                                consumer_tag = CTag,
                                                nowait       = true}),
    ok = amqp_channel:call(Ch, #'basic.cancel' {consumer_tag = CTag,
                                                nowait       = true}),
    ok = amqp_channel:call(Ch, #'basic.consume'{queue        = Q,
                                                consumer_tag = CTag,
                                                nowait       = true}),
    receive
        #'basic.consume_ok'{} ->
            exit(unexpected_consume_ok);
        {#'basic.deliver'{delivery_tag = DTag}, _Content} ->
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag})
    end,
    teardown(Conn, Ch).

%% -------------------------------------------------------------------

%% connection.blocked, connection.unblocked

connection_blocked_network(Config) ->
    {ok, Connection} = new_connection(Config),
    X = <<"amq.direct">>,
    K = Payload = <<"x">>,
    clear_resource_alarm(memory, Config),
    timer:sleep(1000),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Parent = self(),
    Child = spawn_link(
              fun() ->
                      receive
                          #'connection.blocked'{} -> ok
                      end,
                      clear_resource_alarm(memory, Config),
                      receive
                          #'connection.unblocked'{} -> ok
                      end,
                      Parent ! ok
              end),
    amqp_connection:register_blocked_handler(Connection, Child),
    set_resource_alarm(memory, Config),
    Publish = #'basic.publish'{exchange = X,
                               routing_key = K},
    amqp_channel:call(Channel, Publish,
                      #amqp_msg{payload = Payload}),
    timer:sleep(1000),
    receive
        ok ->
            clear_resource_alarm(memory, Config),
            clear_resource_alarm(disk, Config),
            ok
    after 10000 ->
        clear_resource_alarm(memory, Config),
        clear_resource_alarm(disk, Config),
        exit(did_not_receive_connection_blocked)
    end,
    amqp_connection:close(Connection),
    wait_for_death(Connection).

%% -------------------------------------------------------------------
%% Negative test cases.
%% -------------------------------------------------------------------

non_existent_exchange(Config) ->
    {ok, Connection} = new_connection(Config),
    X = <<"test-non_existent_exchange">>,
    RoutingKey = <<"a-non_existent_exchange">>,
    Payload = <<"foobar">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, OtherChannel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),

    %% Deliberately mix up the routingkey and exchange arguments
    Publish = #'basic.publish'{exchange = RoutingKey, routing_key = X},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    wait_for_death(Channel),

    %% Make sure Connection and OtherChannel still serve us and are not dead
    {ok, _} = amqp_connection:open_channel(Connection),
    amqp_channel:call(OtherChannel, #'exchange.delete'{exchange = X}),
    amqp_connection:close(Connection).

%% -------------------------------------------------------------------

bogus_rpc(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    %% Deliberately bind to a non-existent queue
    Bind = #'queue.bind'{exchange    = <<"amq.topic">>,
                         queue       = <<"does-not-exist">>,
                         routing_key = <<>>},
    try amqp_channel:call(Channel, Bind) of
        _ -> exit(expected_to_exit)
    catch
        exit:{{shutdown, {server_initiated_close, Code, _}},_} ->
            ?NOT_FOUND = Code
    end,
    wait_for_death(Channel),
    true = is_process_alive(Connection),
    amqp_connection:close(Connection).

%% -------------------------------------------------------------------

hard_error(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, OtherChannel} = amqp_connection:open_channel(Connection),
    OtherChannelMonitor = erlang:monitor(process, OtherChannel),
    Qos = #'basic.qos'{prefetch_size = 10000000},
    try amqp_channel:call(Channel, Qos) of
        _ -> exit(expected_to_exit)
    catch
        exit:{{shutdown, {connection_closing,
                          {server_initiated_close, ?NOT_IMPLEMENTED, _}}}, _} ->
            ok
    end,
    receive
        {'DOWN', OtherChannelMonitor, process, OtherChannel, OtherExit} ->
            {shutdown,
             {connection_closing,
              {server_initiated_close, ?NOT_IMPLEMENTED, _}}} = OtherExit
    end,
    wait_for_death(Channel),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

non_existent_user(Config) ->
    {error, {auth_failure, _}} = new_connection(Config).

%% -------------------------------------------------------------------

invalid_password(Config) ->
    {error, {auth_failure, _}} = new_connection(Config).

%% -------------------------------------------------------------------

non_existent_vhost(Config) ->
    {error, not_allowed} = new_connection(Config).

%% -------------------------------------------------------------------

no_permission(Config) ->
    {error, not_allowed} = new_connection(Config).

%% -------------------------------------------------------------------

%% An error in a channel should result in the death of the entire connection.
%% The death of the channel is caused by an error in generating the frames
%% (writer dies)
channel_writer_death(Config) ->
    ConnType = ?config(amqp_client_conn_type, Config),
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Publish = #'basic.publish'{routing_key = <<>>, exchange = <<>>},
    QoS = #'basic.qos'{prefetch_count = 0},
    Message = #amqp_msg{props = <<>>, payload = <<>>},
    amqp_channel:cast(Channel, Publish, Message),
    try
        Ret = amqp_channel:call(Channel, QoS),
        throw({unexpected_success, Ret})
    catch
        exit:{{function_clause,
               [{rabbit_channel, check_user_id_header, _, _} | _]}, _}
        when ConnType =:= direct -> ok;

        exit:{{infrastructure_died, {unknown_properties_record, <<>>}}, _}
        when ConnType =:= network -> ok
    end,
    wait_for_death(Channel),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

%% The connection should die if the underlying connection is prematurely
%% closed. For a network connection, this means that the TCP socket is
%% closed. For a direct connection (remotely only, of course), this means that
%% the RabbitMQ node appears as down.
connection_failure(Config) ->
    {ok, Connection} = new_connection(Config),
    case amqp_connection:info(Connection, [type, amqp_params]) of
        [{type, direct}, {amqp_params, Params}]  ->
            case Params#amqp_params_direct.node of
                N when N == node() ->
                    amqp_connection:close(Connection);
                N ->
                    true = erlang:disconnect_node(N),
                    net_adm:ping(N)
            end;
        [{type, network}, {amqp_params, _}] ->
            [{sock, Sock}] = amqp_connection:info(Connection, [sock]),
            close_remote_socket(Config, Sock)
    end,
    wait_for_death(Connection).

%% We obtain the socket for the remote end of the connection by
%% looking through open ports and comparing sockname/peername values.
%% This is necessary because we cannot close our own socket to test
%% connection failures; a close is expected.
%%
%% We need to convert the IPv4 to IPv6 because the server side
%% will use the IPv6 format due to it being enabled for other tests.
close_remote_socket(Config, Socket) when is_port(Socket) ->
    {ok, {IPv4, Port}} = inet:sockname(Socket),
    IPv6 = inet:ipv4_mapped_ipv6_address(IPv4),
    rabbit_ct_broker_helpers:rpc(Config, 0,
        ?MODULE, close_remote_socket1, [{ok, {IPv4, Port}}, {ok, {IPv6, Port}}]).

close_remote_socket1(SockNameIPv4, SockNameIPv6) ->
    AllPorts = [{P, erlang:port_info(P)} || P <- erlang:ports()],
    [RemoteSocket] = [
        P
    || {P, I} <- AllPorts,
        I =/= undefined,
        proplists:get_value(name, I) =:= "tcp_inet",
        inet:peername(P) =:= SockNameIPv4 orelse
        inet:peername(P) =:= SockNameIPv6],
    ok = gen_tcp:close(RemoteSocket).

%% -------------------------------------------------------------------

%% An error in the channel process should result in the death of the entire
%% connection. The death of the channel is caused by making a call with an
%% invalid message to the channel process
channel_death(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    try
        Ret = amqp_channel:call(Channel, {bogus_message, 123}),
        throw({unexpected_success, Ret})
    catch
        exit:{{unknown_method_name, bogus_message}, _} -> ok
    end,
    wait_for_death(Channel),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

%% Attempting to send a shortstr longer than 255 bytes in a property field
%% should fail - this only applies to the network case
shortstr_overflow_property(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    SentString = << <<"k">> || _ <- lists:seq(1, 340)>>,
    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    PBasic = #'P_basic'{content_type = SentString},
    AmqpMsg = #amqp_msg{payload = <<"foobar">>, props = PBasic},
    QoS = #'basic.qos'{prefetch_count = 0},
    amqp_channel:cast(Channel, Publish, AmqpMsg),
    try
        Ret = amqp_channel:call(Channel, QoS),
        throw({unexpected_success, Ret})
    catch
        exit:{{infrastructure_died, content_properties_shortstr_overflow}, _} -> ok
    end,
    wait_for_death(Channel),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

%% Attempting to send a shortstr longer than 255 bytes in a method's field
%% should fail - this only applies to the network case
shortstr_overflow_field(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    SentString = << <<"k">> || _ <- lists:seq(1, 340)>>,
    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
    try
        Ret = amqp_channel:call(
                Channel, #'basic.consume'{queue = Q,
                                          no_ack = true,
                                          consumer_tag = SentString}),
        throw({unexpected_success, Ret})
    catch
        exit:{{infrastructure_died, method_field_shortstr_overflow}, _} -> ok
    end,
    wait_for_death(Channel),
    wait_for_death(Connection).

%% -------------------------------------------------------------------

%% Simulates a #'connection.open'{} method received on non-zero channel. The
%% connection is expected to send a '#connection.close{}' to the server with
%% reply code command_invalid
command_invalid_over_channel(Config) ->
    {ok, Connection} = new_connection(Config),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    MonitorRef = erlang:monitor(process, Connection),
    case amqp_connection:info(Connection, [type]) of
        [{type, direct}]  -> Channel ! {send_command, #'connection.open'{}};
        [{type, network}] -> gen_server:cast(Channel,
                                 {method, #'connection.open'{}, none, noflow})
    end,
    assert_down_with_error(MonitorRef, command_invalid),
    false = is_process_alive(Channel).

%% -------------------------------------------------------------------

%% Simulates a #'basic.ack'{} method received on channel zero. The connection
%% is expected to send a '#connection.close{}' to the server with reply code
%% command_invalid - this only applies to the network case
command_invalid_over_channel0(Config) ->
    {ok, Connection} = new_connection(Config),
    MonitorRef = erlang:monitor(process, Connection),
    gen_server:cast(Connection, {method, #'basic.ack'{}, none, noflow}),
    assert_down_with_error(MonitorRef, command_invalid).

%% -------------------------------------------------------------------
%% Helpers.
%% -------------------------------------------------------------------

new_connection(Config) ->
    Params = ?config(amqp_client_conn_params, Config),
    amqp_connection:start(Params).

setup_publish(Channel) ->
    setup_publish(Channel, <<"foobar">>).

setup_publish(Channel, Payload) ->
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
    ok = amqp_channel:call(Channel, #'basic.publish'{exchange    = <<>>,
                                                     routing_key = Q},
                           #amqp_msg{payload = Payload}),
    {ok, Q}.

teardown(Connection, Channel) ->
    amqp_channel:close(Channel),
    wait_for_death(Channel),
    ?assertEqual(ok, amqp_connection:close(Connection)),
    wait_for_death(Connection).

wait_for_death(Pid) ->
    Ref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _Reason} ->
            ok
    after ?PROCESS_EXIT_TIMEOUT ->
            exit({timed_out_waiting_for_process_death, Pid})
    end.

latch_loop() ->
    latch_loop(?LATCH, []).

latch_loop(Latch) ->
    latch_loop(Latch, []).

latch_loop(0, Acc) ->
    Acc;
latch_loop(Latch, Acc) ->
    receive
        finished        -> latch_loop(Latch - 1, Acc);
        {finished, Ret} -> latch_loop(Latch - 1, [Ret | Acc])
    after ?LATCH * ?WAIT -> exit(waited_too_long)
    end.

get_and_assert_empty(Channel, Q) ->
    #'basic.get_empty'{}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = true}).

get_and_assert_equals(Channel, Q, Payload) ->
    get_and_assert_equals(Channel, Q, Payload, true).

get_and_assert_equals(Channel, Q, Payload, NoAck) ->
    {GetOk = #'basic.get_ok'{}, Content}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = NoAck}),
    #amqp_msg{payload = Payload2} = Content,
    Payload = Payload2,
    GetOk.

assert_down_with_error(MonitorRef, CodeAtom) ->
    receive
        {'DOWN', MonitorRef, process, _, Reason} ->
            {shutdown, {server_misbehaved, Code, _}} = Reason,
            CodeAtom = ?PROTOCOL:amqp_exception(Code)
    after 2000 ->
        exit(did_not_die)
    end.

wait_until_net_ticktime(NetTicktime) ->
    case net_kernel:get_net_ticktime() of
        NetTicktime -> ok;
        {ongoing_change_to, NetTicktime} ->
            timer:sleep(1000),
            wait_until_net_ticktime(NetTicktime);
        _ ->
            throw({error, {net_ticktime_not_set, NetTicktime}})
    end.

set_resource_alarm(Resource, Config)
  when Resource =:= memory orelse Resource =:= disk ->
    SrcDir = ?config(amqp_client_srcdir, Config),
    Nodename = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Cmd = [{"RABBITMQ_NODENAME=~s", [Nodename]},
           "set-resource-alarm",
           {"SOURCE=~s", [Resource]}],
    {ok, _} = case os:getenv("RABBITMQ_RUN") of
        false -> rabbit_ct_helpers:make(Config, SrcDir, Cmd);
        Run -> rabbit_ct_helpers:exec([Run | Cmd])
    end.

clear_resource_alarm(Resource, Config)
  when Resource =:= memory orelse Resource =:= disk ->
    SrcDir = ?config(amqp_client_srcdir, Config),
    Nodename = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Cmd = [{"RABBITMQ_NODENAME=~s", [Nodename]},
           "clear-resource-alarm",
           {"SOURCE=~s", [Resource]}],
    {ok, _} = case os:getenv("RABBITMQ_RUN") of
        false -> rabbit_ct_helpers:make(Config, SrcDir, Cmd);
        Run -> rabbit_ct_helpers:exec([Run | Cmd])
    end.

fmt(Fmt, Args) -> list_to_binary(rabbit_misc:format(Fmt, Args)).
