%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
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
          crashing_unmirrored,
          crashing_mirrored,
          give_up_after_repeated_crashes
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

init_per_testcase(Testcase, Config) ->
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
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

crashing_unmirrored(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ChA = rabbit_ct_client_helpers:open_channel(Config, A),
    ConnB = rabbit_ct_client_helpers:open_connection(Config, B),
    QName = <<"crashing_unmirrored-q">>,
    amqp_channel:call(ChA, #'confirm.select'{}),
    test_queue_failure(A, ChA, ConnB, 1, 0,
                       #'queue.declare'{queue = QName, durable = true}),
    test_queue_failure(A, ChA, ConnB, 0, 0,
                       #'queue.declare'{queue = QName, durable = false}),
    ok.

crashing_mirrored(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    rabbit_ct_broker_helpers:set_ha_policy(Config, A, <<".*">>, <<"all">>),
    ChA = rabbit_ct_client_helpers:open_channel(Config, A),
    ConnB = rabbit_ct_client_helpers:open_connection(Config, B),
    QName = <<"crashing_mirrored-q">>,
    amqp_channel:call(ChA, #'confirm.select'{}),
    test_queue_failure(A, ChA, ConnB, 2, 1,
                       #'queue.declare'{queue = QName, durable = true}),
    ok.

test_queue_failure(Node, Ch, RaceConn, MsgCount, SlaveCount, Decl) ->
    #'queue.declare_ok'{queue = QName} = amqp_channel:call(Ch, Decl),
    try
        publish(Ch, QName, transient),
        publish(Ch, QName, durable),
        Racer = spawn_declare_racer(RaceConn, Decl),
        kill_queue(Node, QName),
        assert_message_count(MsgCount, Ch, QName),
        assert_slave_count(SlaveCount, Node, QName),
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
    await_state(A, QName, running),
    publish(ChA, QName, durable),
    kill_queue_hard(A, QName),
    {'EXIT', _} = (catch amqp_channel:call(
                           ChA, #'queue.declare'{queue   = QName,
                                                 durable = true})),
    await_state(A, QName, crashed),
    amqp_channel:call(ChB, #'queue.delete'{queue = QName}),
    amqp_channel:call(ChB, #'queue.declare'{queue   = QName,
                                            durable = true}),
    await_state(A, QName, running),

    %% Since it's convenient, also test absent queue status here.
    rabbit_ct_broker_helpers:stop_node(Config, B),
    await_state(A, QName, down),
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
                    closing  -> ok
                end
            catch
                exit:_ ->
                    ok
            end,
            declare_racer_loop(Parent, Conn, Decl)
    end.

await_state(Node, QName, State) ->
    await_state(Node, QName, State, 30000).

await_state(Node, QName, State, Time) ->
    case state(Node, QName) of
        State ->
            ok;
        Other ->
            case Time of
                0 -> exit({timeout_awaiting_state, State, Other});
                _ -> timer:sleep(100),
                     await_state(Node, QName, State, Time - 100)
            end
    end.

state(Node, QName) ->
    V = <<"/">>,
    Res = rabbit_misc:r(V, queue, QName),
    Infos = rpc:call(Node, rabbit_amqqueue, info_all, [V, [name, state]]),
    case Infos of
        []                               -> undefined;
        [[{name,  Res}, {state, State}]] -> State
    end.

kill_queue_hard(Node, QName) ->
    case kill_queue(Node, QName) of
        crashed -> ok;
        _NewPid -> timer:sleep(100),
                   kill_queue_hard(Node, QName)
    end.

kill_queue(Node, QName) ->
    Pid1 = queue_pid(Node, QName),
    exit(Pid1, boom),
    await_new_pid(Node, QName, Pid1).

queue_pid(Node, QName) ->
    Q = lookup(Node, QName),
    QPid = amqqueue:get_pid(Q),
    State = amqqueue:get_state(Q),
    #resource{virtual_host = VHost} = amqqueue:get_name(Q),
    case State of
        crashed ->
            case rabbit_amqqueue_sup_sup:find_for_vhost(VHost, Node) of
                {error, {queue_supervisor_not_found, _}} -> {error, no_sup};
                {ok, SPid} ->
                    case sup_child(Node, SPid) of
                       {ok, _}           -> QPid;   %% restarting
                       {error, no_child} -> crashed %% given up
                    end
            end;
        _       -> QPid
    end.

sup_child(Node, Sup) ->
    case rpc:call(Node, supervisor2, which_children, [Sup]) of
        [{_, Child, _, _}]              -> {ok, Child};
        []                              -> {error, no_child};
        {badrpc, {'EXIT', {noproc, _}}} -> {error, no_sup}
    end.

lookup(Node, QName) ->
    {ok, Q} = rpc:call(Node, rabbit_amqqueue, lookup,
                       [rabbit_misc:r(<<"/">>, queue, QName)]),
    Q.

await_new_pid(Node, QName, OldPid) ->
    case queue_pid(Node, QName) of
        OldPid -> timer:sleep(10),
                  await_new_pid(Node, QName, OldPid);
        New    -> New
    end.

assert_message_count(Count, Ch, QName) ->
    #'queue.declare_ok'{message_count = Count} =
        amqp_channel:call(Ch, #'queue.declare'{queue   = QName,
                                               passive = true}).

assert_slave_count(Count, Node, QName) ->
    Q = lookup(Node, QName),
    [{_, Pids}] = rpc:call(Node, rabbit_amqqueue, info, [Q, [slave_pids]]),
    RealCount = case Pids of
                    '' -> 0;
                    _  -> length(Pids)
                end,
    case RealCount of
        Count ->
            ok;
        _ when RealCount < Count ->
            timer:sleep(10),
            assert_slave_count(Count, Node, QName);
        _ ->
            exit({too_many_slaves, Count, RealCount})
    end.
