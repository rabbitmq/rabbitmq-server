%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_exchange_type_modulus_hash_SUITE).

-compile(export_all).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [],
      [
       routed_to_zero_queue_test,
       routed_to_one_queue_test,
       routed_to_many_queue_test,
       stable_routing_across_restarts_test,
       weighted_routing_test
      ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
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
    TestCaseName = rabbit_ct_helpers:config_to_testcase_name(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, {test_resource_name,
                                                    re:replace(TestCaseName, "/", "-", [global, {return, list}])}),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

routed_to_zero_queue_test(Config) ->
    ok = route(Config, [], 5, 0).

routed_to_one_queue_test(Config) ->
    ok = route(Config, [<<"q1">>, <<"q2">>, <<"q3">>], 1, 1).

routed_to_many_queue_test(Config) ->
    ok = route(Config, [<<"q1">>, <<"q2">>, <<"q3">>], 5, 5).

route(Config, Queues, PublishCount, ExpectedRoutedCount) ->
    {Conn, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    B = rabbit_ct_helpers:get_config(Config, test_resource_name),
    XNameBin = erlang:list_to_binary("x-" ++ B),

    #'exchange.declare_ok'{} = amqp_channel:call(Chan,
                                                 #'exchange.declare'{
                                                    exchange = XNameBin,
                                                    type = <<"x-modulus-hash">>,
                                                    durable = true,
                                                    auto_delete = true}),
    [begin
         #'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{
                                                            queue = Q,
                                                            durable = true,
                                                            exclusive = true}),
         #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{
                                                         queue = Q,
                                                         exchange = XNameBin})
     end
     || Q <- Queues],

    amqp_channel:call(Chan, #'confirm.select'{}),
    [amqp_channel:call(Chan,
                       #'basic.publish'{exchange = XNameBin,
                                        routing_key = rnd()},
                       #amqp_msg{props = #'P_basic'{},
                                 payload = <<>>}) ||
     _ <- lists:duplicate(PublishCount, const)],
    amqp_channel:wait_for_confirms_or_die(Chan),

    Count = lists:foldl(
              fun(Q, Acc) ->
                      #'queue.declare_ok'{message_count = M} = amqp_channel:call(
                                                                 Chan,
                                                                 #'queue.declare'{
                                                                    queue = Q,
                                                                    durable = true,
                                                                    exclusive = true}),
                      Acc + M
              end, 0, Queues),
    ?assertEqual(ExpectedRoutedCount, Count),

    amqp_channel:call(Chan, #'exchange.delete'{exchange = XNameBin}),
    [amqp_channel:call(Chan, #'queue.delete'{queue = Q}) || Q <- Queues],
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Chan).

stable_routing_across_restarts_test(Config) ->
    {Conn1, Chan1} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    XNameBin = atom_to_binary(?FUNCTION_NAME),
    NumQs = 40,
    NumMsgs = 500,

    #'exchange.declare_ok'{} = amqp_channel:call(Chan1,
                                                 #'exchange.declare'{
                                                    exchange = XNameBin,
                                                    type = <<"x-modulus-hash">>,
                                                    durable = true}),
    Queues = [erlang:list_to_binary("q-" ++ integer_to_list(I)) || I <- lists:seq(1, NumQs)],
    [begin
         #'queue.declare_ok'{} = amqp_channel:call(Chan1, #'queue.declare'{
                                                             queue = Q,
                                                             durable = true}),
         #'queue.bind_ok'{} = amqp_channel:call(Chan1, #'queue.bind'{
                                                          queue = Q,
                                                          exchange = XNameBin,
                                                          %% The routing key shouldn't matter
                                                          routing_key = rnd()})
     end
     || Q <- Queues],

    RoutingKeys = [rnd() || _ <- lists:seq(1, NumMsgs)],

    amqp_channel:call(Chan1, #'confirm.select'{}),
    [amqp_channel:call(Chan1,
                       #'basic.publish'{exchange = XNameBin,
                                        routing_key = RK},
                       #amqp_msg{payload = RK})
     || RK <- RoutingKeys],
    amqp_channel:wait_for_confirms_or_die(Chan1),

    Map1 = consume_all(Chan1, Queues),

    %% Assert at least two queues got messages
    NonEmptyQueues1 = maps:filter(fun(_Q, Msgs) -> length(Msgs) > 0 end, Map1),
    ?assert(maps:size(NonEmptyQueues1) >= 2),

    %% Assert all messages routed
    ?assertEqual(NumMsgs, lists:sum([length(Msgs) || Msgs <- maps:values(Map1)])),

    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn1, Chan1),
    %% Restart node
    ok = rabbit_ct_broker_helpers:restart_node(Config, 0),
    {Conn2, Chan2} = rabbit_ct_client_helpers:open_connection_and_channel(Config),

    amqp_channel:call(Chan2, #'confirm.select'{}),
    %% Publish the same messages again.
    [amqp_channel:call(Chan2,
                       #'basic.publish'{exchange = XNameBin,
                                        routing_key = RK},
                       #amqp_msg{payload = RK})
     || RK <- RoutingKeys],
    amqp_channel:wait_for_confirms_or_die(Chan2),

    Map2 = consume_all(Chan2, Queues),

    %% Assert the same messages ended up in the same queues,
    %% i.e. that routing was stable.
    ?assertEqual(Map1, Map2),

    amqp_channel:call(Chan2, #'exchange.delete'{exchange = XNameBin}),
    [amqp_channel:call(Chan2, #'queue.delete'{queue = Q}) || Q <- Queues],
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn2, Chan2).

weighted_routing_test(Config) ->
    {Conn, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    XNameBin = atom_to_binary(?FUNCTION_NAME),
    Queues = [<<"q1">>, <<"q2">>, <<"q3">>],
    NumMsgs = 600,

    #'exchange.declare_ok'{} = amqp_channel:call(Chan,
                                                 #'exchange.declare'{
                                                    exchange = XNameBin,
                                                    type = <<"x-modulus-hash">>,
                                                    durable = true}),

    [#'queue.declare_ok'{} = amqp_channel:call(Chan, #'queue.declare'{queue = Q,
                                                                      durable = true})
     || Q <- Queues],

    %% Bind q1 once
    #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{queue = <<"q1">>,
                                                               exchange = XNameBin}),

    %% Bind q2 twice
    #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{queue = <<"q2">>,
                                                               exchange = XNameBin,
                                                               routing_key = <<"a">>}),
    #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{queue = <<"q2">>,
                                                               exchange = XNameBin,
                                                               routing_key = <<"b">>}),

    %% Bind q3 three times
    #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{queue = <<"q3">>,
                                                               exchange = XNameBin,
                                                               routing_key = <<"a">>}),
    #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{queue = <<"q3">>,
                                                               exchange = XNameBin,
                                                               routing_key = <<"b">>}),
    #'queue.bind_ok'{} = amqp_channel:call(Chan, #'queue.bind'{queue = <<"q3">>,
                                                               exchange = XNameBin,
                                                               routing_key = <<"c">>}),

    amqp_channel:call(Chan, #'confirm.select'{}),
    [amqp_channel:call(Chan,
                       #'basic.publish'{exchange = XNameBin,
                                        routing_key = integer_to_binary(I)},
                       #amqp_msg{})
     || I <- lists:seq(1, NumMsgs)],
    amqp_channel:wait_for_confirms_or_die(Chan),

    Counts = lists:foldl(
               fun(Q, Acc) ->
                       #'queue.declare_ok'{message_count = M} = amqp_channel:call(
                                                                  Chan,
                                                                  #'queue.declare'{queue = Q,
                                                                                   durable = true}),
                       maps:put(Q, M, Acc)
               end, #{}, Queues),

    C1 = maps:get(<<"q1">>, Counts),
    C2 = maps:get(<<"q2">>, Counts),
    C3 = maps:get(<<"q3">>, Counts),
    ct:pal("q1: ~b, q2: ~b, q3: ~b", [C1, C2, C3]),

    ?assertEqual(NumMsgs, C1 + C2 + C3),
    %% Assert weighted distribution
    ?assert(C1 < C2),
    ?assert(C2 < C3),

    amqp_channel:call(Chan, #'exchange.delete'{exchange = XNameBin}),
    [amqp_channel:call(Chan, #'queue.delete'{queue = Q}) || Q <- Queues],
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Chan).

consume_all(Chan, Queues) ->
    lists:foldl(fun(Q, Map) ->
                        Msgs = consume_queue(Chan, Q, []),
                        maps:put(Q, Msgs, Map)
                end, #{}, Queues).

consume_queue(Chan, Q, L) ->
    case amqp_channel:call(Chan, #'basic.get'{queue = Q,
                                              no_ack = true}) of
        #'basic.get_empty'{} ->
            L;
        {#'basic.get_ok'{}, #amqp_msg{payload = Payload}} ->
            consume_queue(Chan, Q, L ++ [Payload])
    end.

rnd() ->
    integer_to_binary(rand:uniform(10_000_000)).
