%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
-module(quorum_queue_policy_repair_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-compile([nowarn_export_all, export_all]).


all() ->
    [
     {group, all}
    ].

groups() ->
    [
     {all, [], [repair_policy]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000}]}),
    rabbit_ct_helpers:run_setup_steps(Config1, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    ClusterSize = 3,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    rabbit_ct_helpers:run_steps(Config1,
                                [fun merge_app_env/1 ] ++
                                    rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1, [{queue_name, Q}]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

% Tests that, if the process of a QQ is dead in the moment of declaring a policy
% that affects such queue, when the process is made available again, the policy
% will eventually get applied. (https://github.com/rabbitmq/rabbitmq-server/issues/7863)
repair_policy(Config) ->
    [Server0, Server1, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    
    QQ = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QQ, 0, 0},
                 declare(Ch, QQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}])),
    ExpectedMaxLength1 = 10,
    Priority1 = 1,
    ok = rabbit_ct_broker_helpers:rpc(
        Config,
        0, 
        rabbit_policy, 
        set, 
        [
            <<"/">>, 
            <<QQ/binary, "_1">>,
            QQ, 
            [{<<"max-length">>, ExpectedMaxLength1}], 
            Priority1, 
            <<"quorum_queues">>, 
            <<"acting-user">>
        ]),
    
    % Wait for the policy to apply
    timer:sleep(3000),

    % Check the policy has been applied
    %   Insert MaxLength1 + some messages but after consuming all messages only 
    %   MaxLength1 are retrieved.
    %   Checking twice to ensure consistency
    %   
    %   Once
    publish_many(Ch, QQ, ExpectedMaxLength1 + 1),
    timer:sleep(3000),
    Msgs0 = consume_all(Ch, QQ),
    ExpectedMaxLength1 = length(Msgs0),
    %   Twice
    publish_many(Ch, QQ, ExpectedMaxLength1 + 10),
    timer:sleep(3000),
    Msgs1 = consume_all(Ch, QQ),
    ExpectedMaxLength1 = length(Msgs1),

    % Set higher priority policy, allowing more messages
    ExpectedMaxLength2 = 20,
    Priority2 = 2,
    ok = rabbit_ct_broker_helpers:rpc(
        Config,
        0, 
        rabbit_policy, 
        set, 
        [
            <<"/">>, 
            <<QQ/binary, "_2">>,
            QQ, 
            [{<<"max-length">>, ExpectedMaxLength2}], 
            Priority2,
            <<"quorum_queues">>, 
            <<"acting-user">>
        ]),
    
    % Wait for the policy to apply
    timer:sleep(3000),

    % Check the policy has been applied
    %   Insert MaxLength2 + some messages but after consuming all messages only 
    %   MaxLength2 are retrieved.
    %   Checking twice to ensure consistency.
    %   
    %   Once
    publish_many(Ch, QQ, ExpectedMaxLength2 + 1),
    timer:sleep(3000),
    Msgs3 = consume_all(Ch, QQ),
    ExpectedMaxLength2 = length(Msgs3),
    %   Twice
    publish_many(Ch, QQ, ExpectedMaxLength2 + 10),
    timer:sleep(3000),
    Msgs4 = consume_all(Ch, QQ),
    ExpectedMaxLength2 = length(Msgs4),

    % Make the queue process unavailable.
    % Kill the process multiple times until its supervisor stops restarting it.
    lists:foreach(fun(Srv) ->
        KillUntil = fun KillUntil() ->
            case
                rabbit_ct_broker_helpers:rpc(
                    Config,
                    Srv,
                    erlang,
                    whereis,
                    [binary_to_atom(<<"%2F_", QQ/binary>>, utf8)])
            of
                undefined ->
                    ok;
                Pid -> 
                    rabbit_ct_broker_helpers:rpc(
                        Config,
                        Srv,
                        erlang,
                        exit,
                        [Pid, kill]
                    ),
                    % Give some time for the supervisor to restart the process
                    timer:sleep(500),
                    KillUntil()
            end
        end,
        KillUntil()
    end,
    Servers),

    % Add policy with higher priority, allowing even more messages.
    ExpectedMaxLength3 = 30,
    Priority3 = 3,
    ok = rabbit_ct_broker_helpers:rpc(
        Config,
        0, 
        rabbit_policy, 
        set, 
        [
            <<"/">>, 
            <<QQ/binary, "_3">>,
            QQ, 
            [{<<"max-length">>, ExpectedMaxLength3}], 
            Priority3, 
            <<"quorum_queues">>, 
            <<"acting-user">>
        ]),

    % Restart the queue process.
    {ok, Queue} = 
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbit_amqqueue,
            lookup, 
            [{resource, <<"/">>, queue, QQ}]),
    lists:foreach(
        fun(Srv) ->
            rabbit_ct_broker_helpers:rpc(
                Config,
                Srv,
                rabbit_quorum_queue,
                recover,
                [foo, [Queue]]
            )
        end,
        Servers),

    % Wait for the queue to be available again. 
    timer:sleep(3000),

    % Check the policy has been applied
    %   Insert MaxLength3 + some messages but after consuming all messages only 
    %   MaxLength3 are retrieved.
    %   Checking twice to ensure consistency.
    %   
    %   Once
    publish_many(Ch, QQ, ExpectedMaxLength3 + 1),
    timer:sleep(3000),
    Msgs5 = consume_all(Ch, QQ),
    ExpectedMaxLength3 = length(Msgs5),
    %   Twice
    publish_many(Ch, QQ, ExpectedMaxLength3 + 10),
    timer:sleep(3000),
    Msgs6 = consume_all(Ch, QQ),
    ExpectedMaxLength3 = length(Msgs6).


declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).
consume_all(Ch, QQ) ->
    Consume = fun C(Acc) ->
        case amqp_channel:call(Ch, #'basic.get'{queue = QQ}) of
            {#'basic.get_ok'{}, Msg} ->
                C([Msg | Acc]);
            _ ->
                Acc
        end
    end,
    Consume([]).


wait_until(Condition) ->
    wait_until(Condition, 60).

wait_until(Condition, 0) ->
    ?assertEqual(true, Condition());
wait_until(Condition, N) ->
    case Condition() of
        true ->
            ok;
        _ ->
            timer:sleep(500),
            wait_until(Condition, N - 1)
    end.

delete_queues() ->
    [rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

publish_many(Ch, Queue, Count) ->
    [publish(Ch, Queue) || _ <- lists:seq(1, Count)].

publish(Ch, Queue) ->
    publish(Ch, Queue, <<"msg">>).

publish(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).

