%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

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
                           classic_queue_flow_control_disabled
                           ]
     },
     {cluster_size_3, [], [
                           leader_locator_client_local,
                           leader_locator_balanced,
                           locator_deprecated
                          ]
     }].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
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
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, 'rabbitmq_4.0.0') of
        ok ->
            rabbit_ct_helpers:testcase_started(Config, T);
        Skip ->
            Skip
    end.

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

flow_control(Config, FlowEnabled, VerifyFun) ->
    OrigCredit = set_default_credit(Config, {2, 1}),
    OrigFlow = set_flow_control(Config, FlowEnabled),

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QueueName = atom_to_binary(?FUNCTION_NAME),
    declare(Ch, QueueName, [{<<"x-queue-type">>, longstr,  <<"classic">>}]),
    QPid = get_queue_pid(Config, QueueName),
    try
        sys:suspend(QPid),

        %% Publish 100 messages without publisher confirms
        publish_many(Ch, QueueName, 100),

        [ConnPid] = rabbit_ct_broker_helpers:rpc(Config, rabbit_networking, local_connections, []),

        VerifyFun(QPid, ConnPid),
        ok
    after
        sys:resume(QPid),
        delete_queues(Ch, [QueueName]),
        set_default_credit(Config, OrigCredit),
        set_flow_control(Config, OrigFlow),
        rabbit_ct_client_helpers:close_channel(Ch)
    end.

leader_locator_client_local(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Q = <<"q1">>,

    [begin
         Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
         ?assertEqual({'queue.declare_ok', Q, 0, 0},
                      declare(Ch, Q, [{<<"x-queue-type">>, longstr, <<"classic">>},
                                      {<<"x-queue-leader-locator">>, longstr, <<"client-local">>}])),
         {ok, Leader0} = rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_amqqueue, lookup, [rabbit_misc:r(<<"/">>, queue, Q)]),
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
                        ?assertMatch({'queue.declare_ok', Q, 0, 0},
                                     declare(Ch, Q,
                                             [{<<"x-queue-type">>, longstr, <<"classic">>},
                                              {Argument, longstr, Strategy}])),

                        {ok, Leader0} = rabbit_ct_broker_helpers:rpc(Config, Server, rabbit_amqqueue, lookup, [rabbit_misc:r(<<"/">>, queue, Q)]),
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
