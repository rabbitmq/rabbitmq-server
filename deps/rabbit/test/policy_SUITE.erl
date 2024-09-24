%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(policy_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
     {group, tests},
     {group, khepri_migration}
    ].

groups() ->
    [
     {tests, [], all_tests()},
     {khepri_migration, [], [
                             from_mnesia_to_khepri
                            ]}
    ].

all_tests() ->
    [
     policy_ttl,
     operator_policy_ttl,
     operator_retroactive_policy_ttl,
     operator_retroactive_policy_publish_ttl,
     queue_type_specific_policies,
     classic_queue_version_policies,
     overflow_policies,
     is_supported_operator_policy_expires,
     is_supported_operator_policy_message_ttl,
     is_supported_operator_policy_max_length,
     is_supported_operator_policy_max_length,
     is_supported_operator_policy_max_in_memory_length,
     is_supported_operator_policy_max_in_memory_bytes,
     is_supported_operator_policy_delivery_limit,
     is_supported_operator_policy_target_group_size,
     is_supported_operator_policy_overflow
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(tests = Group, Config) ->
    init_per_group_common(Group, Config, 2);
init_per_group(khepri_migration = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia}]),
    init_per_group_common(Group, Config, 1).

init_per_group_common(Group, Config, Size) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, Size},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    Name = rabbit_data_coercion:to_binary(Testcase),
    Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
    Policy = rabbit_data_coercion:to_binary(io_lib:format("~p_~p_policy", [Group, Testcase])),
    OpPolicy = rabbit_data_coercion:to_binary(io_lib:format("~p_~p_op_policy", [Group, Testcase])),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Name},
                                            {policy, Policy},
                                            {op_policy, OpPolicy}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    _ = rabbit_ct_broker_helpers:clear_policy(Config, 0, ?config(policy, Config)),
    _ = rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, ?config(op_policy, Config)),
    Config1 = rabbit_ct_helpers:run_steps(Config, rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).
%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

policy_ttl(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_ttl-queue">>,
    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"ttl-policy">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 20}]),

    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 20)),
    timer:sleep(50),
    get_empty(Ch, Q),
    delete(Ch, Q),

    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl-policy">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

operator_policy_ttl(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_ttl-queue">>,
    % Operator policy will override
    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"ttl-policy">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 100000}]),
    rabbit_ct_broker_helpers:set_operator_policy(Config, 0, <<"ttl-policy-op">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 1}]),

    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 50)),
    timer:sleep(50),
    get_empty(Ch, Q),
    delete(Ch, Q),

    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl-policy">>),
    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"ttl-policy-op">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

operator_retroactive_policy_ttl(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_ttl-queue">>,
    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 50)),
    % Operator policy will override
    rabbit_ct_broker_helpers:set_operator_policy(Config, 0, <<"ttl-policy-op">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 1}]),

    %% Old messages are not expired
    timer:sleep(50),
    get_messages(50, Ch, Q),
    delete(Ch, Q),

    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"ttl-policy-op">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

operator_retroactive_policy_publish_ttl(Config) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Q = <<"policy_ttl-queue">>,
    declare(Ch, Q),
    publish(Ch, Q, lists:seq(1, 50)),
    % Operator policy will override
    rabbit_ct_broker_helpers:set_operator_policy(Config, 0, <<"ttl-policy-op">>,
        <<"policy_ttl-queue">>, <<"all">>, [{<<"message-ttl">>, 1}]),

    %% Old messages are not expired, new ones only expire when they get to the head of
    %% the queue
    publish(Ch, Q, lists:seq(1, 25)),
    timer:sleep(50),
    [[<<"policy_ttl-queue">>, <<"75">>]] =
        rabbit_ct_broker_helpers:rabbitmqctl_list(Config, 0, ["list_queues", "--no-table-headers"]),
    get_messages(50, Ch, Q),
    delete(Ch, Q),

    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"ttl-policy-op">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

queue_type_specific_policies(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    ClassicQ = <<"policy_ttl-classic_queue">>,
    QuorumQ = <<"policy_ttl-quorum_queue">>,
    StreamQ = <<"policy_ttl-stream_queue">>,

    %% all policies match ".*" but different values should be applied based on queue type
    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"ttl-policy-classic">>,
        <<".*">>, <<"classic_queues">>, [{<<"message-ttl">>, 20}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"ttl-policy-quorum">>,
        <<".*">>, <<"quorum_queues">>, [{<<"message-ttl">>, 40}]),

    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"ttl-policy-stream">>,
        <<".*">>, <<"streams">>, [{<<"max-age">>, "1h"}]),

    declare(Ch, ClassicQ, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    declare(Ch, QuorumQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    declare(Ch, StreamQ, [{<<"x-queue-type">>, longstr, <<"stream">>}]),
    timer:sleep(1),

    ?assertMatch(20, check_policy_value(Server, ClassicQ, <<"message-ttl">>)),
    ?assertMatch(40, check_policy_value(Server, QuorumQ, <<"message-ttl">>)),
    ?assertMatch("1h", check_policy_value(Server, StreamQ, <<"max-age">>)),

    delete(Ch, ClassicQ),
    delete(Ch, QuorumQ),
    delete(Ch, StreamQ),
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl-policy-classic">>),
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl-policy-quorum">>),
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"ttl-policy-stream">>),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

classic_queue_version_policies(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = <<"policy_queue_version">>,
    declare(Ch, QName),
    QueueVersionOnePolicy = [{<<"queue-version">>, 1}],
    QueueVersionTwoPolicy = [{<<"queue-version">>, 2}],

    Opts = #{config => Config,
             server => Server,
             qname  => QName},

    %% Queue version OperPolicy has precedence always
    verify_policies(QueueVersionOnePolicy, QueueVersionTwoPolicy, QueueVersionTwoPolicy, Opts),
    verify_policies(QueueVersionTwoPolicy, QueueVersionOnePolicy, QueueVersionOnePolicy, Opts),

    delete(Ch, QName),
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"policy">>),
    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"op_policy">>),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

overflow_policies(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = <<"policy_overflow">>,
    declare(Ch, QName),
    DropHead = [{<<"overflow">>, <<"drop-head">>}],
    RejectPub = [{<<"overflow">>, <<"reject-publish">>}],

    Opts = #{config => Config,
             server => Server,
             qname  => QName},

    %% OperPolicy has precedence always
    verify_policies(DropHead, RejectPub, RejectPub, Opts),

    delete(Ch, QName),
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"policy">>),
    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"op_policy">>),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.


%% See supported policies in https://www.rabbitmq.com/docs/parameters#operator-policies
%% This test applies all supported operator policies to all queue types,
%% and later verifies the effective policy definitions.
%% Just those supported by each queue type should be present.

is_supported_operator_policy_expires(Config) ->
    Value = 6000000,
    effective_operator_policy_per_queue_type(
      Config, <<"expires">>, Value, Value, Value, undefined).

is_supported_operator_policy_message_ttl(Config) ->
    Value = 1000,
    effective_operator_policy_per_queue_type(
      Config, <<"message-ttl">>, Value, Value, Value, undefined).

is_supported_operator_policy_max_length(Config) ->
    Value = 500,
    effective_operator_policy_per_queue_type(
      Config, <<"max-length">>, Value, Value, Value, undefined).

is_supported_operator_policy_max_length_bytes(Config) ->
    Value = 1500,
    effective_operator_policy_per_queue_type(
      Config, <<"max-length-bytes">>, Value, Value, Value, Value).

is_supported_operator_policy_max_in_memory_length(Config) ->
    Value = 30,
    effective_operator_policy_per_queue_type(
      Config, <<"max-in-memory-length">>, Value, undefined, Value, undefined).

is_supported_operator_policy_max_in_memory_bytes(Config) ->
    Value = 50000,
    effective_operator_policy_per_queue_type(
      Config, <<"max-in-memory-bytes">>, Value, undefined, Value, undefined).

is_supported_operator_policy_delivery_limit(Config) ->
    Value = 3,
    effective_operator_policy_per_queue_type(
      Config, <<"delivery-limit">>, Value, undefined, Value, undefined).

is_supported_operator_policy_target_group_size(Config) ->
    Value = 5,
    effective_operator_policy_per_queue_type(
      Config, <<"target-group-size">>, Value, undefined, Value, undefined).

is_supported_operator_policy_overflow(Config) ->
    Value = <<"drop-head">>,
    effective_operator_policy_per_queue_type(
      Config, <<"overflow">>, Value, Value, Value, undefined).

effective_operator_policy_per_queue_type(Config, Name, Value, ClassicValue, QuorumValue, StreamValue) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    ClassicQ = <<"classic_queue">>,
    QuorumQ = <<"quorum_queue">>,
    StreamQ = <<"stream_queue">>,

    declare(Ch, ClassicQ, [{<<"x-queue-type">>, longstr, <<"classic">>}]),
    declare(Ch, QuorumQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    declare(Ch, StreamQ, [{<<"x-queue-type">>, longstr, <<"stream">>}]),

    rabbit_ct_broker_helpers:set_operator_policy(
      Config, 0, <<"operator-policy">>, <<".*">>, <<"all">>,
      [{Name, Value}]),

    ?awaitMatch(ClassicValue, check_policy_value(Server, ClassicQ, Name), 30_000),
    ?awaitMatch(QuorumValue, check_policy_value(Server, QuorumQ, Name), 30_000),
    ?awaitMatch(StreamValue, check_policy_value(Server, StreamQ, Name), 30_000),

    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"operator-policy">>),

    delete(Ch, ClassicQ),
    delete(Ch, QuorumQ),
    delete(Ch, StreamQ),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

%%----------------------------------------------------------------------------
from_mnesia_to_khepri(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q)),

    Policy = ?config(policy, Config),
    ok = rabbit_ct_broker_helpers:set_policy(Config, 0, Policy, Q,
                                             <<"queues">>,
                                             [{<<"dead-letter-exchange">>, <<>>},
                                              {<<"dead-letter-routing-key">>, Q}]),
    OpPolicy = ?config(op_policy, Config),
    ok = rabbit_ct_broker_helpers:set_operator_policy(Config, 0, OpPolicy, Q,
                                                      <<"queues">>,
                                                      [{<<"max-length">>, 10000}]),

    Policies0 = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, list, [])),
    Names0 = lists:sort([proplists:get_value(name, Props) || Props <- Policies0]),

    ?assertEqual([Policy], Names0),

    OpPolicies0 = lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, list_op, [])),
    OpNames0 = lists:sort([proplists:get_value(name, Props) || Props <- OpPolicies0]),

    ?assertEqual([OpPolicy], OpNames0),

    case rabbit_ct_broker_helpers:enable_feature_flag(Config, khepri_db) of
        ok ->
            rabbit_ct_helpers:await_condition(
              fun() ->
                      (Policies0 ==
                           lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, list, [])))
                          andalso
                            (OpPolicies0 ==
                                 lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_policy, list_op, [])))
              end);
        Skip ->
            Skip
    end.

%%----------------------------------------------------------------------------
delete_queues() ->
    [{ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

declare(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                                                   durable   = true}).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                                                   durable   = true,
                                                                   arguments = Args}).

delete(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish(Ch, Q, Ps) ->
    amqp_channel:call(Ch, #'confirm.select'{}),
    [publish1(Ch, Q, P) || P <- Ps],
    amqp_channel:wait_for_confirms(Ch).

publish1(Ch, Q, P) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = erlang:md5(term_to_binary(P))}).

publish1(Ch, Q, P, Pd) ->
    amqp_channel:cast(Ch, #'basic.publish'{routing_key = Q},
                      #amqp_msg{props   = props(P),
                                payload = Pd}).

props(undefined) -> #'P_basic'{delivery_mode = 2};
props(P)         -> #'P_basic'{priority      = P,
                               delivery_mode = 2}.

consume(Ch, Q, Ack) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue        = Q,
                                                no_ack       = Ack =:= no_ack,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end.

get_empty(Ch, Q) ->
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = Q}).

get_messages(0, Ch, Q) ->
    get_empty(Ch, Q);
get_messages(Number, Ch, Q) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = Q}) of
        {#'basic.get_ok'{}, _} ->
            get_messages(Number - 1, Ch, Q);
        #'basic.get_empty'{} ->
            exit(failed)
    end.

check_policy_value(Server, QName, Value) ->
    ct:pal("QUEUES ~p",
           [rpc:call(Server, rabbit_amqqueue, list, [])]),
    {ok, Q} = rpc:call(Server, rabbit_amqqueue, lookup, [rabbit_misc:r(<<"/">>, queue, QName)]),
    case rpc:call(Server, rabbit_policy, effective_definition, [Q]) of
        List when is_list(List) -> proplists:get_value(Value, List);
        Any -> Any
    end.

verify_policies(Policy, OperPolicy, VerifyFuns, #{config := Config,
                                                  server := Server,
                                                  qname := QName}) ->
    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"policy">>,
                                        QName, <<"queues">>,
                                        Policy),
    rabbit_ct_broker_helpers:set_operator_policy(Config, 0, <<"op_policy">>,
                                                 QName, <<"queues">>,
                                                 OperPolicy),
    verify_policy(VerifyFuns, Server, QName).

verify_policy([], _, _) ->
    ok;
verify_policy([{HA, Expect} | Tail], Server, QName) ->
    Expect = check_policy_value(Server, QName, HA),
    verify_policy(Tail, Server, QName).


%%----------------------------------------------------------------------------
