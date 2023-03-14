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

-compile(export_all).

all() ->
    [
      {group, cluster_size_2}
    ].

groups() ->
    [
     {cluster_size_2, [], [
                           target_count_policy,
                           policy_ttl,
                           operator_policy_ttl,
                           operator_retroactive_policy_ttl,
                           operator_retroactive_policy_publish_ttl,
                           queue_type_specific_policies
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

    %% ALL has precedence
    Opts = #{config => Config,
             server => Server,
             qname  => QName},

    verify_policies(AllPolicy, ExactlyPolicyTwo, [{<<"ha-mode">>, <<"all">>}], Opts),

    verify_policies(ExactlyPolicyTwo, AllPolicy, [{<<"ha-mode">>, <<"all">>}], Opts),

    verify_policies(AllPolicy, NodesPolicyAll, [{<<"ha-mode">>, <<"all">>}], Opts),

    verify_policies(NodesPolicyAll, AllPolicy, [{<<"ha-mode">>, <<"all">>}], Opts),

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
    {ok, Q} = rpc:call(Server, rabbit_amqqueue, lookup, [rabbit_misc:r(<<"/">>, queue, QName)]),
    proplists:get_value(Value, rpc:call(Server, rabbit_policy, effective_definition, [Q])).

verify_policies(Policy, OperPolicy, VerifyFuns, #{config := Config,
                                                  server := Server,
                                                  qname := QName}) ->
    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"policy">>,
                                        <<"policy_ha">>, <<"queues">>,
                                        Policy),
    rabbit_ct_broker_helpers:set_operator_policy(Config, 0, <<"op_policy">>,
                                                 <<"policy_ha">>, <<"queues">>,
                                                 OperPolicy),
    verify_policy(VerifyFuns, Server, QName).

verify_policy([], _, _) ->
    ok;
verify_policy([{HA, Expect} | Tail], Server, QName) ->
    Expect = check_policy_value(Server, QName, HA),
    verify_policy(Tail, Server, QName).


%%----------------------------------------------------------------------------
