%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(policy_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

all() ->
    [
      {group, cluster_size_2}
    ].

groups() ->
    [
<<<<<<< HEAD
     {cluster_size_2, [], [
                           target_count_policy,
                           policy_ttl,
                           operator_policy_ttl,
                           operator_retroactive_policy_ttl,
                           operator_retroactive_policy_publish_ttl,
                           queue_type_specific_policies,
                           classic_queue_version_policies,
                           is_supported_operator_policy_expires,
                           is_supported_operator_policy_message_ttl,
                           is_supported_operator_policy_max_length,
                           is_supported_operator_policy_max_length,
                           is_supported_operator_policy_max_in_memory_length,
                           is_supported_operator_policy_max_in_memory_bytes,
                           is_supported_operator_policy_delivery_limit,
                           is_supported_operator_policy_ha
                          ]}
=======
     {mnesia_store, [], [target_count_policy] ++ all_tests()},
     {khepri_store, [], all_tests()},
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
     is_supported_operator_policy_overflow,
     is_supported_operator_policy_ha
>>>>>>> cffc77d396 (Add overflow as operpolicy)
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
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, 2},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:setup_steps(),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:teardown_steps(),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

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

target_count_policy(Config) ->
    [Server | _] = Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = <<"policy_ha">>,
    declare(Ch, QName),
    BNodes = [atom_to_binary(N) || N <- Nodes],

    AllPolicy = [{<<"ha-mode">>, <<"all">>}],
    ExactlyPolicyOne = [{<<"ha-mode">>, <<"exactly">>},
                        {<<"ha-params">>, 1}],
    ExactlyPolicyTwo = [{<<"ha-mode">>, <<"exactly">>},
                        {<<"ha-params">>, 2}],
    NodesPolicyAll = [{<<"ha-mode">>, <<"nodes">>},
                      {<<"ha-params">>, BNodes}],
    NodesPolicyOne = [{<<"ha-mode">>, <<"nodes">>},
                      {<<"ha-params">>, [hd(BNodes)]}],
    SyncModePolicyAuto = [{<<"ha-mode">>, <<"all">>}, {<<"ha-sync-mode">>, <<"automatic">>}],
    SyncModePolicyMan = [{<<"ha-mode">>, <<"all">>}, {<<"ha-sync-mode">>, <<"manual">>}],

    %% ALL has precedence
    Opts = #{config => Config,
             server => Server,
             qname  => QName},
    verify_policies(AllPolicy, ExactlyPolicyTwo, [{<<"ha-mode">>, <<"all">>}], Opts),

    verify_policies(ExactlyPolicyTwo, AllPolicy, [{<<"ha-mode">>, <<"all">>}], Opts),

    verify_policies(AllPolicy, NodesPolicyAll, [{<<"ha-mode">>, <<"all">>}], Opts),

    verify_policies(NodesPolicyAll, AllPolicy, [{<<"ha-mode">>, <<"all">>}], Opts),

    %% %% Sync mode OperPolicy has precedence
    verify_policies(SyncModePolicyMan, SyncModePolicyAuto, [{<<"ha-sync-mode">>, <<"automatic">>}], Opts),
    verify_policies(SyncModePolicyAuto, SyncModePolicyMan, [{<<"ha-sync-mode">>, <<"manual">>}], Opts),

    %% exactly has precedence over nodes
    verify_policies(ExactlyPolicyTwo, NodesPolicyAll,[{<<"ha-mode">>, <<"exactly">>}, {<<"ha-params">>, 2}], Opts),

    verify_policies(NodesPolicyAll, ExactlyPolicyTwo, [{<<"ha-mode">>, <<"exactly">>}, {<<"ha-params">>, 2}], Opts),

    %% Highest exactly value has precedence
    verify_policies(ExactlyPolicyTwo, ExactlyPolicyOne, [{<<"ha-mode">>, <<"exactly">>}, {<<"ha-params">>, 2}], Opts),

    verify_policies(ExactlyPolicyOne, ExactlyPolicyTwo, [{<<"ha-mode">>, <<"exactly">>}, {<<"ha-params">>, 2}], Opts),

    %% Longest node count has precedence
    SortedNodes = lists:sort(BNodes),
    verify_policies(NodesPolicyAll, NodesPolicyOne, [{<<"ha-mode">>, <<"nodes">>}, {<<"ha-params">>, SortedNodes}], Opts),
    verify_policies(NodesPolicyOne, NodesPolicyAll, [{<<"ha-mode">>, <<"nodes">>}, {<<"ha-params">>, SortedNodes}], Opts),

    delete(Ch, QName),
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"policy">>),
    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"op_policy">>),
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


%% See supported policies in https://www.rabbitmq.com/parameters.html#operator-policies
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

<<<<<<< HEAD
=======
is_supported_operator_policy_target_group_size(Config) ->
    Value = 5,
    effective_operator_policy_per_queue_type(
      Config, <<"target-group-size">>, Value, undefined, Value, undefined).

is_supported_operator_policy_overflow(Config) ->
    Value = <<"drop-head">>,
    effective_operator_policy_per_queue_type(
      Config, <<"overflow">>, Value, Value, Value, undefined).


>>>>>>> cffc77d396 (Add overflow as operpolicy)
is_supported_operator_policy_ha(Config) ->
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
      [{<<"ha-mode">>, <<"exactly">>},
       {<<"ha-params">>, 2},
       {<<"ha-sync-mode">>, <<"automatic">>}]),

    ?awaitMatch(<<"exactly">>, check_policy_value(Server, ClassicQ, <<"ha-mode">>), 30_000),
    ?awaitMatch(2, check_policy_value(Server, ClassicQ, <<"ha-params">>), 30_000),
    ?awaitMatch(<<"automatic">>, check_policy_value(Server, ClassicQ, <<"ha-sync-mode">>), 30_000),
    ?awaitMatch(undefined, check_policy_value(Server, QuorumQ, <<"ha-mode">>), 30_000),
    ?awaitMatch(undefined, check_policy_value(Server, StreamQ, <<"ha-mode">>), 30_000),

    rabbit_ct_broker_helpers:clear_operator_policy(Config, 0, <<"operator-policy">>),

    delete(Ch, ClassicQ),
    delete(Ch, QuorumQ),
    delete(Ch, StreamQ),

    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    passed.

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
