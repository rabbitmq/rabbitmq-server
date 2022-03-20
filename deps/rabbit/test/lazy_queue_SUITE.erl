%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(lazy_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(QNAME, <<"queue.mode.test">>).
-define(MESSAGE_COUNT, 2000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          declare_args,
          queue_mode_policy,
          publish_consume
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

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = 2,
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, ClusterSize},
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++ [
        fun rabbit_ct_broker_helpers:set_ha_policy_all/1
      ]).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

declare_args(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    LQ = <<"lazy-q">>,
    declare(Ch, LQ, [{<<"x-queue-mode">>, longstr, <<"lazy">>}]),
    assert_queue_mode(A, LQ, lazy),

    DQ = <<"default-q">>,
    declare(Ch, DQ, [{<<"x-queue-mode">>, longstr, <<"default">>}]),
    assert_queue_mode(A, DQ, default),

    DQ2 = <<"default-q2">>,
    declare(Ch, DQ2),
    assert_queue_mode(A, DQ2, default),

    passed.

queue_mode_policy(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    set_ha_mode_policy(Config, A, <<"lazy">>),

    Ch = rabbit_ct_client_helpers:open_channel(Config, A),

    LQ = <<"lazy-q">>,
    declare(Ch, LQ, [{<<"x-queue-mode">>, longstr, <<"lazy">>}]),
    assert_queue_mode(A, LQ, lazy),

    LQ2 = <<"lazy-q-2">>,
    declare(Ch, LQ2),
    assert_queue_mode(A, LQ2, lazy),

    DQ = <<"default-q">>,
    declare(Ch, DQ, [{<<"x-queue-mode">>, longstr, <<"default">>}]),
    assert_queue_mode(A, DQ, default),

    set_ha_mode_policy(Config, A, <<"default">>),

    ok = wait_for_queue_mode(A, LQ,  lazy, 5000),
    ok = wait_for_queue_mode(A, LQ2, default, 5000),
    ok = wait_for_queue_mode(A, DQ,  default, 5000),

    passed.

publish_consume(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, A),
    declare(Ch, ?QNAME),

    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    consume(Ch, ?QNAME, ack),
    [assert_delivered(Ch, ack, P) || P <- lists:seq(1, ?MESSAGE_COUNT)],

    set_ha_mode_policy(Config, A, <<"lazy">>),
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    [assert_delivered(Ch, ack, P) || P <- lists:seq(1, ?MESSAGE_COUNT)],

    set_ha_mode_policy(Config, A, <<"default">>),
    [assert_delivered(Ch, ack, P) || P <- lists:seq(1, ?MESSAGE_COUNT)],

    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    set_ha_mode_policy(Config, A, <<"lazy">>),
    rabbit_ct_client_helpers:publish(Ch, ?QNAME, ?MESSAGE_COUNT),
    set_ha_mode_policy(Config, A, <<"default">>),
    [assert_delivered(Ch, ack, P) || P <- lists:seq(1, ?MESSAGE_COUNT)],

    set_ha_mode_policy(Config, A, <<"lazy">>),
    [assert_delivered(Ch, ack, P) || P <- lists:seq(1, ?MESSAGE_COUNT)],

    cancel(Ch),

    passed.

%%----------------------------------------------------------------------------

declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           arguments = Args}).

consume(Ch, Q, Ack) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue        = Q,
                                                no_ack       = Ack =:= no_ack,
                                                consumer_tag = <<"ctag">>},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = <<"ctag">>} ->
             ok
    end.

cancel(Ch) ->
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = <<"ctag">>}).

assert_delivered(Ch, Ack, Payload) ->
    PBin = payload2bin(Payload),
    receive
        {#'basic.deliver'{delivery_tag = DTag}, #amqp_msg{payload = PBin2}} ->
            PBin = PBin2,
            maybe_ack(Ch, Ack, DTag)
    end.

maybe_ack(Ch, do_ack, DTag) ->
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag}),
    DTag;
maybe_ack(_Ch, _, DTag) ->
    DTag.

payload2bin(Int) -> list_to_binary(integer_to_list(Int)).

set_ha_mode_policy(Config, Node, Mode) ->
    ok = rabbit_ct_broker_helpers:set_ha_policy(Config, Node, <<".*">>, <<"all">>,
      [{<<"queue-mode">>, Mode}]).


wait_for_queue_mode(_Node, _Q, _Mode, Max) when Max < 0 ->
    fail;
wait_for_queue_mode(Node, Q, Mode, Max) ->
    case get_queue_mode(Node, Q) of
        Mode  -> ok;
        _     -> timer:sleep(100),
                 wait_for_queue_mode(Node, Q, Mode, Max - 100)
    end.

assert_queue_mode(Node, Q, Expected) ->
    Actual = get_queue_mode(Node, Q),
    Expected = Actual.

get_queue_mode(Node, Q) ->
    QNameRes = rabbit_misc:r(<<"/">>, queue, Q),
    {ok, AMQQueue} =
        rpc:call(Node, rabbit_amqqueue, lookup, [QNameRes]),
    [{backing_queue_status, Status}] =
        rpc:call(Node, rabbit_amqqueue, info,
                 [AMQQueue, [backing_queue_status]]),
    proplists:get_value(mode, Status).
