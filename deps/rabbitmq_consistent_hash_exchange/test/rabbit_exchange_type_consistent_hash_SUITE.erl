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
%% The Original Code is RabbitMQ Consistent Hash Exchange.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_consistent_hash_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                routing_key_hashing_test,
                                custom_header_hashing_test,
                                message_id_hashing_test,
                                correlation_id_hashing_test,
                                timestamp_hashing_test,
                                other_routing_test,
                                test_binding_queue_cleanup,
                                test_binding_exchange_cleanup,
                                test_bucket_sizes
                               ]}
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
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
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    clean_up_test_topology(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

-define(Qs, [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>]).
%% N.B. lowering this value below 100K increases the probability
%% of failing the Chi squared test in some environments
-define(DEFAULT_SAMPLE_COUNT, 150000).

routing_key_hashing_test(Config) ->
    ok = test_with_rk(Config, ?Qs).

custom_header_hashing_test(Config) ->
    ok = test_with_header(Config, ?Qs).

message_id_hashing_test(Config) ->
    ok = test_with_message_id(Config, ?Qs).

correlation_id_hashing_test(Config) ->
    ok = test_with_correlation_id(Config, ?Qs).

timestamp_hashing_test(Config) ->
    ok = test_with_timestamp(Config, ?Qs).

other_routing_test(Config) ->
    ok = test_binding_with_negative_routing_key(Config),
    ok = test_binding_with_non_numeric_routing_key(Config),
    ok = test_non_supported_property(Config),
    ok = test_mutually_exclusive_arguments(Config),
    ok.


%% -------------------------------------------------------------------
%% Implementation
%% -------------------------------------------------------------------

test_with_rk(Config, Qs) ->
    test0(Config, fun (E) ->
                  #'basic.publish'{exchange = E, routing_key = rnd()}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{}, payload = <<>>}
          end, [], Qs).

test_with_header(Config, Qs) ->
    test0(Config, fun (E) ->
                  #'basic.publish'{exchange = E}
          end,
          fun() ->
                  H = [{<<"hashme">>, longstr, rnd()}],
                  #amqp_msg{props = #'P_basic'{headers = H}, payload = <<>>}
          end, [{<<"hash-header">>, longstr, <<"hashme">>}], Qs).

test_with_correlation_id(Config, Qs) ->
    test0(Config, fun(E) ->
                  #'basic.publish'{exchange = E}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{correlation_id = rnd()}, payload = <<>>}
          end, [{<<"hash-property">>, longstr, <<"correlation_id">>}], Qs).

test_with_message_id(Config, Qs) ->
    test0(Config, fun(E) ->
                  #'basic.publish'{exchange = E}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{message_id = rnd()}, payload = <<>>}
          end, [{<<"hash-property">>, longstr, <<"message_id">>}], Qs).

test_with_timestamp(Config, Qs) ->
    test0(Config, fun(E) ->
                  #'basic.publish'{exchange = E}
          end,
          fun() ->
                  #amqp_msg{props = #'P_basic'{timestamp = rnd_int()}, payload = <<>>}
          end, [{<<"hash-property">>, longstr, <<"timestamp">>}], Qs).

test_mutually_exclusive_arguments(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    process_flag(trap_exit, true),
    Cmd = #'exchange.declare'{
             exchange  = <<"fail">>,
             type      = <<"x-consistent-hash">>,
             arguments = [{<<"hash-header">>, longstr, <<"foo">>},
                          {<<"hash-property">>, longstr, <<"bar">>}]
            },
    ?assertExit(_, amqp_channel:call(Chan, Cmd)),

    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

test_non_supported_property(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    process_flag(trap_exit, true),
    Cmd = #'exchange.declare'{
             exchange  = <<"fail">>,
             type      = <<"x-consistent-hash">>,
             arguments = [{<<"hash-property">>, longstr, <<"app_id">>}]
            },
    ?assertExit(_, amqp_channel:call(Chan, Cmd)),

    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

rnd() ->
    list_to_binary(integer_to_list(rnd_int())).

rnd_int() ->
    rand:uniform(10000000).

test0(Config, MakeMethod, MakeMsg, DeclareArgs, [Q1, Q2, Q3, Q4] = Queues) ->
    test0(Config, MakeMethod, MakeMsg, DeclareArgs, [Q1, Q2, Q3, Q4] = Queues, ?DEFAULT_SAMPLE_COUNT).

test0(Config, MakeMethod, MakeMsg, DeclareArgs, [Q1, Q2, Q3, Q4] = Queues, IterationCount) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'confirm.select_ok'{} = amqp_channel:call(Chan, #'confirm.select'{}),

    CHX = <<"e">>,

    clean_up_test_topology(Config, CHX, Queues),

    #'exchange.declare_ok'{} =
        amqp_channel:call(Chan,
                          #'exchange.declare' {
                            exchange = CHX,
                            type = <<"x-consistent-hash">>,
                            auto_delete = true,
                            arguments = DeclareArgs
                          }),
    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' {queue = Q,
                                                exchange = CHX,
                                                routing_key = <<"1">>})
     || Q <- [Q1, Q2]],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' {queue = Q,
                                                exchange = CHX,
                                                routing_key = <<"2">>})
     || Q <- [Q3, Q4]],

    [amqp_channel:call(Chan,
                       MakeMethod(CHX),
                       MakeMsg()) || _ <- lists:duplicate(IterationCount, const)],
    amqp_channel:wait_for_confirms(Chan, 5000),
    timer:sleep(500),

    Counts =
        [begin
             #'queue.declare_ok'{message_count = M} =
                 amqp_channel:call(Chan, #'queue.declare' {queue     = Q,
                                                           exclusive = true}),
             M
         end || Q <- Queues],
    IterationCount = lists:sum(Counts), %% All messages got routed

    %% Chi-square test
    %% H0: routing keys are not evenly distributed according to weight
    Expected = [IterationCount div 6, IterationCount div 6, (IterationCount div 6) * 2, (IterationCount div 6) * 2],
    Obs = lists:zip(Counts, Expected),
    Chi = lists:sum([((O - E) * (O - E)) / E || {O, E} <- Obs]),
    ct:pal("Chi-square test for 3 degrees of freedom is ~p, p = 0.01 is 11.35, observations (counts, expected): ~p",
           [Chi, Obs]),
    %% N.B. we need at least 100K iterations to reliably pass this test in multiple different
    %%      environments
    ?assert(Chi < 11.35),

    clean_up_test_topology(Config, CHX, Queues),
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

test_binding_with_negative_routing_key(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
    X = <<"bind-fail">>,
    amqp_channel:call(Chan, #'exchange.delete' {exchange = X}),

    Declare1 = #'exchange.declare'{exchange = X,
                                   type = <<"x-consistent-hash">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Chan, Declare1),
    Q = <<"test-queue">>,
    Declare2 = #'queue.declare'{queue = Q},
    #'queue.declare_ok'{} = amqp_channel:call(Chan, Declare2),
    process_flag(trap_exit, true),
    Cmd = #'queue.bind'{exchange = <<"bind-fail">>,
                        routing_key = <<"-1">>},
    ?assertExit(_, amqp_channel:call(Chan, Cmd)),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch2, #'queue.delete'{queue = Q}),

    rabbit_ct_client_helpers:close_channel(Chan),
    rabbit_ct_client_helpers:close_channel(Ch2),
    ok.

test_binding_with_non_numeric_routing_key(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),
    X = <<"bind-fail">>,
    amqp_channel:call(Chan, #'exchange.delete' {exchange = X}),

    Declare1 = #'exchange.declare'{exchange = X,
                                   type = <<"x-consistent-hash">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Chan, Declare1),
    Q = <<"test-queue">>,
    Declare2 = #'queue.declare'{queue = Q},
    #'queue.declare_ok'{} = amqp_channel:call(Chan, Declare2),
    process_flag(trap_exit, true),
    Cmd = #'queue.bind'{exchange = <<"bind-fail">>,
                        routing_key = <<"not-a-number">>},
    ?assertExit(_, amqp_channel:call(Chan, Cmd)),

    Ch2 = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch2, #'queue.delete'{queue = Q}),

    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

test_binding_queue_cleanup(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    X = <<"test_binding_cleanup">>,
    amqp_channel:call(Chan, #'exchange.delete' {exchange = X}),

    Declare = #'exchange.declare'{exchange = X,
                                  type = <<"x-consistent-hash">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Chan, Declare),

    Queues = [<<"q1">>, <<"q2">>, <<"q3">>, <<"q4">>, <<"q5">>, <<"q6">>],
    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' {queue = Q,
                                                exchange = X,
                                                routing_key = <<"3">>})
     || Q <- Queues],

    ?assertMatch([{_, _, 18}],
                 count_buckets_of_exchange(Config, X)),
    Q1 = <<"q1">>,
    ?assertMatch([_],
                 count_buckets_for_pair(Config, X, Q1)),
    ?assertMatch(18,
                 count_hash_buckets(Config)),

    amqp_channel:call(Chan, #'queue.delete' {queue = Q1}),

    ?assertMatch([{_, _, 15}],
                 count_buckets_of_exchange(Config, X)),
    ?assertMatch([],
                 count_buckets_for_pair(Config, X, Q1)),
    ?assertMatch(15,
                 count_hash_buckets(Config)),

    amqp_channel:call(Chan, #'exchange.delete' {exchange = X}),

    [amqp_channel:call(Chan, #'queue.delete' {queue = Q}) || Q <- Queues],
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

test_binding_exchange_cleanup(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    X = <<"test_binding_cleanup">>,
    amqp_channel:call(Chan, #'exchange.delete' {exchange = X}),

    Declare = #'exchange.declare'{exchange = X,
                                  type = <<"x-consistent-hash">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Chan, Declare),

    Queues = [<<"q1">>, <<"q2">>, <<"q3">>, <<"q4">>, <<"q5">>, <<"q6">>],
    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    [#'queue.bind_ok'{} =
         amqp_channel:call(Chan, #'queue.bind' {queue = Q,
                                                exchange = X,
                                                routing_key = <<"3">>})
     || Q <- Queues],

    ?assertEqual(1,
                 count_all_hash_buckets(Config)),
    ?assertEqual(6,
                 count_all_binding_buckets(Config)),
    ?assertEqual(18,
                 count_all_queue_buckets(Config)),

    {'exchange.delete_ok'} = amqp_channel:call(Chan, #'exchange.delete' {exchange = X}),

    ?assertEqual(0,
                 count_all_hash_buckets(Config)),
    ?assertEqual(0,
                 count_all_binding_buckets(Config)),
    ?assertEqual(0,
                 count_all_queue_buckets(Config)),

    [amqp_channel:call(Chan, #'queue.delete' {queue = Q}) || Q <- Queues],
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

test_bucket_sizes(Config) ->
    Chan = rabbit_ct_client_helpers:open_channel(Config, 0),

    X = <<"test_bucket_sizes">>,
    amqp_channel:call(Chan, #'exchange.delete' {exchange = X}),

    Declare = #'exchange.declare'{exchange = X,
                                  type = <<"x-consistent-hash">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Chan, Declare),

    Queues = [<<"q1">>, <<"q2">>, <<"q3">>],
    [#'queue.declare_ok'{} =
         amqp_channel:call(Chan, #'queue.declare' {
                             queue = Q, exclusive = true }) || Q <- Queues],
    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' {queue = <<"q1">>,
                                               exchange = X,
                                               routing_key = <<"3">>}),
    ?assertMatch([{_, _, 3}], count_buckets_of_exchange(Config, X)),
    ?assertMatch(3, count_all_queue_buckets(Config)),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' {queue = <<"q2">>,
                                               exchange = X,
                                               routing_key = <<"1">>}),
    ?assertMatch([{_, _, 4}], count_buckets_of_exchange(Config, X)),
    ?assertMatch(4, count_all_queue_buckets(Config)),

    #'queue.bind_ok'{} =
        amqp_channel:call(Chan, #'queue.bind' {queue = <<"q3">>,
                                               exchange = X,
                                               routing_key = <<"2">>}),
    ?assertMatch([{_, _, 6}], count_buckets_of_exchange(Config, X)),
    ?assertMatch(6, count_all_queue_buckets(Config)),

    amqp_channel:call(Chan, #'exchange.delete' {exchange = X}),
    [amqp_channel:call(Chan, #'queue.delete' {queue = Q}) || Q <- Queues],
    rabbit_ct_client_helpers:close_channel(Chan),
    ok.

%%
%% Helpers
%%

count_buckets_of_exchange(Config, X) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ets, lookup,
      [rabbit_exchange_type_consistent_hash_bucket_count,
       rabbit_misc:r(<<"/">>, exchange, X)]).

count_buckets_for_pair(Config, X, Q) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ets, lookup,
      [rabbit_exchange_type_consistent_hash_binding_bucket,
       {rabbit_misc:r(<<"/">>, exchange, X),
        rabbit_misc:r(<<"/">>, queue, Q)}]).

count_hash_buckets(Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ets, info,
      [rabbit_exchange_type_consistent_hash_bucket_queue, size]).

count_all_hash_buckets(Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ets, info,
      [rabbit_exchange_type_consistent_hash_bucket_count, size]).

count_all_binding_buckets(Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ets, info,
      [rabbit_exchange_type_consistent_hash_binding_bucket, size]).

count_all_queue_buckets(Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ets, info,
      [rabbit_exchange_type_consistent_hash_bucket_queue, size]).

clean_up_test_topology(Config) ->
    clean_up_test_topology(Config, <<"e">>, ?Qs).

clean_up_test_topology(Config, X, Qs) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),

    amqp_channel:call(Ch, #'exchange.delete' {exchange = X}),
    [amqp_channel:call(Ch, #'queue.delete' {queue = Q}) || Q <- Qs],

    rabbit_ct_client_helpers:close_channel(Ch).
