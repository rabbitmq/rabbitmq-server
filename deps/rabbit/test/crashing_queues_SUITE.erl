%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(crashing_queues_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, cluster_size_2}
    ].

groups() ->
    [
      {cluster_size_2, [], [
          crashing_durable,
          give_up_after_repeated_crashes,
          crashing_transient
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
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 2}
      ]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(crashing_transient = Testcase, Config) ->
    case rabbit_ct_broker_helpers:configured_metadata_store(Config) of
        mnesia ->
            init_per_testcase0(Testcase, Config);
        _ ->
            {skip, "Transient queues not supported by Khepri"}
    end;
init_per_testcase(Testcase, Config) ->
    init_per_testcase0(Testcase, Config).

init_per_testcase0(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_broker_helpers:teardown_steps()).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

crashing_durable(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ChA = rabbit_ct_client_helpers:open_channel(Config, A),
    ConnB = rabbit_ct_client_helpers:open_connection(Config, B),
    QName = <<"crashing-q">>,
    amqp_channel:call(ChA, #'confirm.select'{}),
    test_queue_failure(A, ChA, ConnB, 1,
                       #'queue.declare'{queue = QName, durable = true}),
    ok.

crashing_transient(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ChA = rabbit_ct_client_helpers:open_channel(Config, A),
    ConnB = rabbit_ct_client_helpers:open_connection(Config, B),
    QName = <<"crashing-q">>,
    amqp_channel:call(ChA, #'confirm.select'{}),
    test_queue_failure(A, ChA, ConnB, 0,
                       #'queue.declare'{queue = QName, durable = false}),
    ok.

test_queue_failure(Node, Ch, RaceConn, MsgCount, Decl) ->
    #'queue.declare_ok'{queue = QName} = amqp_channel:call(Ch, Decl),
    try
        publish(Ch, QName, transient),
        publish(Ch, QName, durable),
        Racer = spawn_declare_racer(RaceConn, Decl),
        QRes = rabbit_misc:r(<<"/">>, queue, QName),
        rabbit_amqqueue:kill_queue(Node, QRes),
        assert_message_count(MsgCount, Ch, QName),
        stop_declare_racer(Racer)
    after
        amqp_channel:call(Ch, #'queue.delete'{queue = QName})
    end.

give_up_after_repeated_crashes(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ChA = rabbit_ct_client_helpers:open_channel(Config, A),
    ChB = rabbit_ct_client_helpers:open_channel(Config, B),
    QName = <<"give_up_after_repeated_crashes-q">>,
    amqp_channel:call(ChA, #'confirm.select'{}),
    amqp_channel:call(ChA, #'queue.declare'{queue   = QName,
                                            durable = true}),
    rabbit_amqqueue_control:await_state(A, QName, running),
    publish(ChA, QName, durable),
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    rabbit_amqqueue:kill_queue_hard(A, QRes),
    {'EXIT', _} = (catch amqp_channel:call(
                           ChA, #'queue.declare'{queue   = QName,
                                                 durable = true})),
    rabbit_amqqueue_control:await_state(A, QName, crashed),
    amqp_channel:call(ChB, #'queue.delete'{queue = QName}),
    amqp_channel:call(ChB, #'queue.declare'{queue   = QName,
                                            durable = true}),
    rabbit_amqqueue_control:await_state(A, QName, running),

    %% Since it's convenient, also test absent queue status here.
    rabbit_ct_broker_helpers:stop_node(Config, B),
    rabbit_amqqueue_control:await_state(A, QName, down),
    ok.


publish(Ch, QName, DelMode) ->
    Publish = #'basic.publish'{exchange = <<>>, routing_key = QName},
    Msg = #amqp_msg{props = #'P_basic'{delivery_mode = del_mode(DelMode)}},
    amqp_channel:cast(Ch, Publish, Msg),
    amqp_channel:wait_for_confirms(Ch).

del_mode(transient) -> 1;
del_mode(durable)   -> 2.

spawn_declare_racer(Conn, Decl) ->
    Self = self(),
    spawn_link(fun() -> declare_racer_loop(Self, Conn, Decl) end).

stop_declare_racer(Pid) ->
    Pid ! stop,
    MRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    end.

declare_racer_loop(Parent, Conn, Decl) ->
    receive
        stop -> unlink(Parent)
    after 0 ->
            %% Catch here because we might happen to catch the queue
            %% while it is in the middle of recovering and thus
            %% explode with NOT_FOUND because crashed. Doesn't matter,
            %% we are only in this loop to try to fool the recovery
            %% code anyway.
            try
                case amqp_connection:open_channel(Conn) of
                    {ok, Ch} -> amqp_channel:call(Ch, Decl);
                    closing  -> ok;
                    {error, _} -> ok
                end
            catch
                exit:_ ->
                    ok
            end,
            declare_racer_loop(Parent, Conn, Decl)
    end.

lookup(Node, QName) ->
    {ok, Q} = rpc:call(Node, rabbit_amqqueue, lookup,
                       [rabbit_misc:r(<<"/">>, queue, QName)]),
    Q.

assert_message_count(Count, Ch, QName) ->
    #'queue.declare_ok'{message_count = Count} =
        amqp_channel:call(Ch, #'queue.declare'{queue   = QName,
                                               passive = true}).
