%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(classic_queue_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([nowarn_export_all, export_all]).

-import(rabbit_ct_broker_helpers,
        [get_node_config/3,
         rpc/4,
         rpc/5]).

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {cluster_size_1, [], [
                           classic_queue_flow_control_enabled,
                           classic_queue_flow_control_disabled,
                           expires_counts_node_downtime,
                           expires_counts_idle_time_before_shutdown,
                           expires_resumes_remaining_time_after_restart,
                           expires_not_counted_while_consumer_attached,
                           expires_counts_node_downtime_priority_queue
                           ]
     },
     {cluster_size_3, [], [
                           leader_locator_client_local,
                           leader_locator_balanced,
                           locator_deprecated,
                           expires_policy_removed_before_restart
                          ]
     }].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    %% Remove when queue_master_locator is removed entirely.
    Config = rabbit_ct_helpers:merge_app_env(
                Config0,
                {rabbit,
                 [{permit_deprecated_features, #{queue_master_locator => true}}]}),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Nodes = case Group of
                cluster_size_1 -> 1;
                cluster_size_3 -> 3
            end,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [
                                            {rmq_nodename_suffix, Group},
                                            {rmq_nodes_count, Nodes},
                                            {rmq_nodes_clustered, true},
                                            {tcp_ports_base, {skip_n_nodes, 3}}
                                           ]),
    Config2 = rabbit_ct_helpers:run_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    Config2.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_client_helpers:teardown_steps() ++
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(T, Config) ->
    rabbit_ct_helpers:testcase_started(Config, T).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

classic_queue_flow_control_enabled(Config) ->
    FlowEnabled = true,
    VerifyFun =
        fun(QPid, ConnPid) ->
                %% Only 2+2 messages reach the message queue of the classic queue.
                %% (before the credits of the connection and channel processes run out)
                ?awaitMatch(4, proc_info(QPid, message_queue_len), 1000),
                ?assertMatch({0, _}, gen_server2_queue(QPid)),

                %% The connection gets into flow state
                ?assertEqual(
                   [{state, flow}],
                   rabbit_ct_broker_helpers:rpc(Config, rabbit_reader, info, [ConnPid, [state]])),

                Dict = proc_info(ConnPid, dictionary),
                ?assertMatch([_|_], proplists:get_value(credit_blocked, Dict)),
                ok
        end,
    flow_control(Config, FlowEnabled, VerifyFun).

classic_queue_flow_control_disabled(Config) ->
    FlowEnabled = false,
    VerifyFun =
        fun(QPid, ConnPid) ->
                %% All published messages will end up in the message
                %% queue of the suspended classic queue process
                ?awaitMatch(100, proc_info(QPid, message_queue_len), 1000),
                ?assertMatch({0, _}, gen_server2_queue(QPid)),

                %% The connection dos not get into flow state
                ?assertEqual(
                   [{state, running}],
                   rabbit_ct_broker_helpers:rpc(Config, rabbit_reader, info, [ConnPid, [state]])),

                Dict = proc_info(ConnPid, dictionary),
                ?assertMatch([], proplists:get_value(credit_blocked, Dict, []))
        end,
    flow_control(Config, FlowEnabled, VerifyFun).

expires_counts_node_downtime(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    declare_expiring_queue(Config, QName, 5000, []),
    restart_node_after(Config, 7000),
    ?awaitMatch({error, not_found}, lookup_queue(Config, QName), 5000).

expires_counts_idle_time_before_shutdown(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    declare_expiring_queue(Config, QName, 10000, []),
    timer:sleep(7000),
    restart_node_after(Config, 4000),
    ?awaitMatch({error, not_found}, lookup_queue(Config, QName), 3000).

expires_resumes_remaining_time_after_restart(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    declare_expiring_queue(Config, QName, 60000, []),
    restart_node_after(Config, 1000),
    ?assertMatch({ok, _}, lookup_queue(Config, QName)).

expires_not_counted_while_consumer_attached(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{} =
        declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"classic">>},
                            {<<"x-expires">>, long, 10000}]),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = QName}, self()),
    receive #'basic.consume_ok'{} -> ok after 5000 -> ct:fail(no_consume_ok) end,
    timer:sleep(8000),
    restart_node_after(Config, 3000),
    ?assertMatch({ok, _}, lookup_queue(Config, QName)).

expires_counts_node_downtime_priority_queue(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    declare_expiring_queue(Config, QName, 5000,
                           [{<<"x-max-priority">>, byte, 5}]),
    restart_node_after(Config, 7000),
    ?awaitMatch({error, not_found}, lookup_queue(Config, QName), 5000).

expires_policy_removed_before_restart(Config) ->
    QName = atom_to_binary(?FUNCTION_NAME),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{} =
        declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    ok = rabbit_ct_client_helpers:close_connection(Conn),

    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 1, <<"expires">>, QName, <<"queues">>, [{<<"expires">>, 12000}]),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 1, <<"expires">>),
    timer:sleep(8000),

    ok = rabbit_ct_broker_helpers:stop_node(Config, 0),
    ok = rabbit_ct_broker_helpers:set_policy(
           Config, 1, <<"expires">>, QName, <<"queues">>, [{<<"expires">>, 12000}]),
    timer:sleep(5000),
    ok = rabbit_ct_broker_helpers:start_node(Config, 0),

    ?assertMatch({ok, _}, lookup_queue(Config, QName)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, 1, <<"expires">>).

declare_expiring_queue(Config, QName, Expires, Args) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{} =
        declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"classic">>},
                            {<<"x-expires">>, long, Expires} | Args]),
    ok = rabbit_ct_client_helpers:close_connection(Conn).

restart_node_after(Config, Downtime) ->
    ok = rabbit_ct_broker_helpers:stop_node(Config, 0),
    timer:sleep(Downtime),
    ok = rabbit_ct_broker_helpers:start_node(Config, 0).

lookup_queue(Config, QName) ->
    rpc(Config, rabbit_amqqueue, lookup, [rabbit_misc:r(<<"/">>, queue, QName)]).

flow_control(Config, FlowEnabled, VerifyFun) ->
    OrigCredit = set_default_credit(Config, {2, 1}),
    OrigFlow = set_flow_control(Config, FlowEnabled),

    ConnsBefore = rabbit_ct_broker_helpers:rpc(Config, rabbit_networking, local_connections, []),
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    QueueName = atom_to_binary(?FUNCTION_NAME),
    declare(Ch, QueueName, [{<<"x-queue-type">>, longstr,  <<"classic">>}]),
    QPid = get_queue_pid(Config, QueueName),
    try
        sys:suspend(QPid),

        %% Publish 100 messages without publisher confirms
        publish_many(Ch, QueueName, 100),

        [ConnPid] = rabbit_ct_broker_helpers:rpc(Config, rabbit_networking, local_connections, []) -- ConnsBefore,

        VerifyFun(QPid, ConnPid),
        ok
    after
        sys:resume(QPid),
        delete_queues(Ch, [QueueName]),
        set_default_credit(Config, OrigCredit),
        set_flow_control(Config, OrigFlow),
        rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch)
    end.

leader_locator_client_local(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = <<"q1">>,

    [begin
         Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
         ?assertEqual({'queue.declare_ok', Q, 0, 0},
                      declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>},
                                      {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
         {ok, Leader0} = ?awaitMatch(
                            {ok, _},
                            rabbit_ct_broker_helpers:rpc(Config,
                                                         Server,
                                                         rabbit_amqqueue,
                                                         lookup,
                                                         [rabbit_misc:r(<<"/">>, queue, Q)]),
                            5000),
         Leader = amqqueue:qnode(Leader0),
         ?assertEqual(Server, Leader),
         ?assertMatch(#'queue.delete_ok'{},
                      amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
     end || Server <- Servers].

leader_locator_balanced(Config) ->
    test_leader_locator(Config, <<"x-queue-leader-locator">>, [<<"balanced">>]).

%% This test can be delted once we remove x-queue-master-locator support
locator_deprecated(Config) ->
    test_leader_locator(Config, <<"x-queue-master-locator">>, [<<"least-leaders">>,
                                                               <<"random">>,
                                                               <<"min-masters">>]).

test_leader_locator(Config, Argument, Strategies) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Qs = [<<"q1">>, <<"q2">>, <<"q3">>],

    [begin
         Leaders = [begin
                        ?assertEqual({'queue.declare_ok', Q, 0, 0},
                                     declare(Ch, Q,
                                             [{<<"x-queue-type">>, longstr, <<"classic">>},
                                              {Argument, longstr, Strategy}])),

                        {ok, Leader0} = ?awaitMatch(
                                           {ok, _},
                                           rabbit_ct_broker_helpers:rpc(Config,
                                                                        Server,
                                                                        rabbit_amqqueue,
                                                                        lookup,
                                                                        [rabbit_misc:r(<<"/">>, queue, Q)]),
                                           5000),
                        Leader = amqqueue:qnode(Leader0),
                        Leader
                    end || Q <- Qs],
         ?assertEqual(3, sets:size(sets:from_list(Leaders))),

         [?assertMatch(#'queue.delete_ok'{},
                       amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
          || Q <- Qs]
     end || Strategy <- Strategies ].

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

delete_queues(Ch, Qs) ->
    [?assertMatch(#'queue.delete_ok'{},
                  amqp_channel:call(Ch, #'queue.delete'{queue = Q}))
     || Q <- Qs].

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].


publish(Ch, QName, Payload) ->
    amqp_channel:cast(Ch,
                      #'basic.publish'{exchange    = <<>>,
                                       routing_key = QName},
                      #amqp_msg{payload = Payload}).

publish_many(Ch, QName, Count) ->
    [publish(Ch, QName, integer_to_binary(I))
     || I <- lists:seq(1, Count)].

proc_info(Pid, Info) ->
    case rabbit_misc:process_info(Pid, Info) of
        {Info, Value} ->
            Value;
        Error ->
            {error, Error}
    end.

gen_server2_queue(Pid) ->
    Status = sys:get_status(Pid),
    {status, Pid,_Mod,
     [_Dict, _SysStatus, _Parent, _Dbg,
      [{header, _},
       {data, Data}|_]]} = Status,
    proplists:get_value("Queued messages", Data).

set_default_credit(Config, Value) ->
    Key = credit_flow_default_credit,
    OrigValue = rabbit_ct_broker_helpers:rpc(Config, persistent_term, get, [Key]),
    ok = rabbit_ct_broker_helpers:rpc(Config, persistent_term, put, [Key, Value]),
    OrigValue.

set_flow_control(Config, Value) when is_boolean(Value) ->
    Key = classic_queue_flow_control,
    {ok, OrigValue} = rabbit_ct_broker_helpers:rpc(Config, application, get_env, [rabbit, Key]),
    rabbit_ct_broker_helpers:rpc(Config, application, set_env, [rabbit, Key, Value]),
    OrigValue.

get_queue_pid(Config, QueueName) ->
    {ok, QRec} = rabbit_ct_broker_helpers:rpc(
                   Config, 0, rabbit_amqqueue, lookup, [QueueName, <<"/">>]),
    amqqueue:get_pid(QRec).
