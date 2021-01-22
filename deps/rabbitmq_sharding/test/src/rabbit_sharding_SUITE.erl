%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_sharding_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(TEST_X, <<"sharding.test">>).

-import(rabbit_sharding_util, [a2b/1, exchange_bin/1]).
-import(rabbit_ct_broker_helpers, [set_parameter/5, clear_parameter/4,
                                   set_policy/6, clear_policy/3]).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                shard_empty_routing_key_test,
                                shard_queue_creation_test,
                                shard_queue_creation2_test,
                                shard_update_spn_test,
                                shard_decrease_spn_keep_queues_test,
                                shard_update_routing_key_test,
                                shard_basic_consume_interceptor_test,
                                shard_auto_scale_cluster_test,
                                queue_declare_test,
                                shard_queue_master_locator_test
                               ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    inets:start(),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count,     2}
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
    rabbit_ct_helpers:set_config(Config, {test_resource_name,
                                          re:replace(TestCaseName, "/", "-", [global, {return, list}])}).

end_per_testcase(_Testcase, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

shard_empty_routing_key_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, x_declare(?TEST_X)),
              set_policy(Config, 0, <<"3_shard">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3)),
              timer:sleep(1000),
              ?assertEqual(6, length(queues(Config, 0))),

              teardown(Config, Ch,
                       [{?TEST_X, 6}],
                       [<<"3_shard">>])
      end).

shard_queue_creation_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, x_declare(?TEST_X)),
              set_policy(Config, 0, <<"3_shard">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),
              ?assertEqual(6, length(queues(Config, 0))),

              teardown(Config, Ch,
                       [{?TEST_X, 6}],
                       [<<"3_shard">>])
      end).

shard_queue_creation2_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              set_policy(Config, 0, <<"3_shard">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),
              ?assertEqual(0, length(queues(Config, 0))),

              amqp_channel:call(Ch, x_declare(?TEST_X)),

              ?assertEqual(6, length(queues(Config, 0))),

              teardown(Config, Ch,
                       [{?TEST_X, 6}],
                       [<<"3_shard">>])
      end).

%% SPN = Shards Per Node
shard_update_spn_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, x_declare(?TEST_X)),
              set_policy(Config, 0, <<"3_shard">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),
              ?assertEqual(6, length(queues(Config, 0))),

              set_policy(Config, 0, <<"3_shard">>, <<"^sharding">>, <<"exchanges">>, policy_definition(5, <<"1234">>)),
              ?assertEqual(10, length(queues(Config, 0))),

              teardown(Config, Ch,
                       [{?TEST_X, 5}],
                       [<<"3_shard">>])
      end).

shard_decrease_spn_keep_queues_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, x_declare(?TEST_X)),
              set_policy(Config, 0, <<"3_shard">>, <<"^sharding">>, <<"exchanges">>, policy_definition(5, <<"1234">>)),
              ?assertEqual(10, length(queues(Config, 0))),

              set_policy(Config, 0, <<"3_shard">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),
              ?assertEqual(10, length(queues(Config, 0))),

              teardown(Config, Ch,
                       [{?TEST_X, 5}],
                       [<<"3_shard">>])
      end).


%% changes the routing key policy, therefore the queues should be
%% unbound first and then bound with the new routing key.
shard_update_routing_key_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, x_declare(?TEST_X)),
              set_policy(Config, 0, <<"rkey">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),
              timer:sleep(1000),
              Bs = bindings(Config, 0, ?TEST_X),

              set_policy(Config, 0, <<"rkey">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"4321">>)),
              timer:sleep(1000),
              Bs2 = bindings(Config, 0, ?TEST_X),

              ?assert(Bs =/= Bs2),

              teardown(Config, Ch,
                       [{?TEST_X, 1}],
                       [<<"rkey">>])
      end).

%% tests that the interceptor returns queue names
%% sorted by consumer count and then by queue index.
shard_basic_consume_interceptor_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Sh = ?TEST_X,
              amqp_channel:call(Ch, x_declare(Sh)),
              set_policy(Config, 0, <<"three">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),

              start_consumer(Ch, Sh),
              assert_consumers(Config, Sh, 0, 1),
              assert_consumers(Config, Sh, 1, 0),
              assert_consumers(Config, Sh, 2, 0),

              start_consumer(Ch, Sh),
              assert_consumers(Config, Sh, 0, 1),
              assert_consumers(Config, Sh, 1, 1),
              assert_consumers(Config, Sh, 2, 0),

              start_consumer(Ch, Sh),
              assert_consumers(Config, Sh, 0, 1),
              assert_consumers(Config, Sh, 1, 1),
              assert_consumers(Config, Sh, 2, 1),

              start_consumer(Ch, Sh),
              assert_consumers(Config, Sh, 0, 2),
              assert_consumers(Config, Sh, 1, 1),
              assert_consumers(Config, Sh, 2, 1),

              teardown(Config, Ch,
                       [{?TEST_X, 6}],
                       [<<"three">>])
      end).

shard_auto_scale_cluster_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              Sh = ?TEST_X,
              amqp_channel:call(Ch, x_declare(Sh)),
              set_policy(Config, 0, <<"three">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),

              ?assertEqual(6, length(queues(Config, 0))),
              Qs = queues(Config, 0),

              ?assertEqual(6, length(Qs)),
              Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
              ?assertEqual(Nodes, lists:usort(queue_nodes(Qs))),

              teardown(Config, Ch,
                       [{?TEST_X, 6}],
                       [<<"three">>])
      end).

queue_declare_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              amqp_channel:call(Ch, x_declare(?TEST_X)),
              set_policy(Config, 0, <<"declare">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),

              Declare = #'queue.declare'{queue = <<"sharding.test">>,
                                         auto_delete = false,
                                         durable = true},

              #'queue.declare_ok'{queue = Q} =
                  amqp_channel:call(Ch, Declare),

              ?assertEqual(Q, shard_q(Config, 0, xr(?TEST_X), 0)),

              teardown(Config, Ch,
                       [{?TEST_X, 6}],
                       [<<"declare">>])
      end).

shard_queue_master_locator_test(Config) ->
    with_ch(Config,
      fun (Ch) ->
              set_policy(Config, 0, <<"qml">>, <<"^sharding">>, <<"queues">>, [{<<"queue-master-locator">>, <<"client-local">>}]),
              amqp_channel:call(Ch, x_declare(?TEST_X)),
              set_policy(Config, 0, <<"3_shard">>, <<"^sharding">>, <<"exchanges">>, policy_definition(3, <<"1234">>)),

              [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

              NodesOfQueues = [node(amqqueue:get_pid(Q)) || Q <- queues(Config, 0)],
              ?assertEqual(3, length(lists:filter(fun(N) -> N == A end, NodesOfQueues))),
              ?assertEqual(3, length(lists:filter(fun(N) -> N == B end, NodesOfQueues))),

              teardown(Config, Ch,
                       [{?TEST_X, 6}],
                       [<<"3_shard">>, <<"qml">>])
      end).

start_consumer(Ch, Shard) ->
    amqp_channel:call(Ch, #'basic.consume'{queue = Shard}).

assert_consumers(Config, Shard, QInd, Count) ->
    Q0 = qr(shard_q(Config, 0, xr(Shard), QInd)),
    [{consumers, C0}] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_sharding_interceptor, consumer_count, [Q0]),
    ?assertEqual(C0, Count).

queues(Config, NodeIndex) ->
    case rabbit_ct_broker_helpers:rpc(Config, NodeIndex, rabbit_amqqueue, list, [<<"/">>]) of
        {badrpc, _} -> [];
        Qs          -> Qs
    end.

bindings(Config, NodeIndex, XName) ->
    case rabbit_ct_broker_helpers:rpc(Config, NodeIndex, rabbit_binding, list_for_source, [xr(XName)]) of
        {badrpc, _} -> [];
        Bs          -> Bs
    end.

with_ch(Config, Fun) ->
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Fun(Ch),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    cleanup(Config, 0),
    ok.

cleanup(Config) ->
    cleanup(Config, 0).
cleanup(Config, NodeIndex) ->
    [rabbit_ct_broker_helpers:rpc(Config, NodeIndex, rabbit_amqqueue, delete, [Q, false, false, <<"test-user">>])
     || Q <- queues(Config, 0)].

teardown(Config, Ch, Xs, Policies) ->
    [begin
         amqp_channel:call(Ch, x_delete(XName)),
         delete_queues(Config, Ch, XName, N)
     end || {XName, N} <- Xs],
    [clear_policy(Config, 0, Policy) || Policy <- Policies].

delete_queues(Config, Ch, Name, N) ->
    [amqp_channel:call(Ch, q_delete(Config, Name, QInd)) || QInd <- lists:seq(0, N-1)].

x_declare(Name) -> x_declare(Name, <<"x-modulus-hash">>).

x_declare(Name, Type) ->
    #'exchange.declare'{exchange = Name,
                        type     = Type,
                        durable  = true}.

x_delete(Name) ->
    #'exchange.delete'{exchange = Name}.

q_delete(Config, Name, QInd) ->
    #'queue.delete'{queue = shard_q(Config, 0, xr(Name), QInd)}.

shard_q(Config, NodeIndex, X, N) ->
    rabbit_sharding_util:make_queue_name(
      exchange_bin(X), a2b(rabbit_ct_broker_helpers:get_node_config(Config, NodeIndex, nodename)), N).

policy_definition(SPN) ->
    [{<<"shards-per-node">>, SPN}].

policy_definition(SPN, RK) ->
    [{<<"shards-per-node">>, SPN}, {<<"routing-key">>, RK}].

queue_nodes(Qs) ->
    [queue_node(Q) || Q <- Qs].

queue_node(Q) when ?is_amqqueue(Q) ->
    amqqueue:qnode(Q).

xr(Name) -> rabbit_misc:r(<<"/">>, exchange, Name).
qr(Name) -> rabbit_misc:r(<<"/">>, queue, Name).
