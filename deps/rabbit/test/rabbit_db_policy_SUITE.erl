%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_policy_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(VHOST, <<"/">>).

all() ->
    [
     {group, all_tests}
    ].

groups() ->
    [
     {all_tests, [], all_tests()}
    ].

all_tests() ->
    [
     update,
     update_retries_on_concurrent_change,
     update_retries_on_concurrent_change_exchange
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_exchange, clear, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_queue, clear, []),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_exchange, clear, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_queue, clear, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

update(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, update1, [Config]).

update1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange = #exchange{name = XName, durable = true},
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange)),
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Queue = amqqueue:new(QName, none, true, false, none, [], ?VHOST, #{},
                         rabbit_classic_queue),
    ?assertEqual({created, Queue}, rabbit_db_queue:create_or_get(Queue)),
    ?assertMatch(
       {[{_, _}], [{_, _}]},
       rabbit_db_policy:update(?VHOST,
                               fun(X) -> #{exchange => X,
                                           update_function =>
                                               fun(X0) ->
                                                       X0#exchange{policy = new_policy}
                                               end}
                               end,
                               fun(Q) -> #{queue => Q,
                                           update_function =>
                                               fun(Q0) ->
                                                       amqqueue:set_policy(Q0, random_policy)
                                               end}
                               end)),
    passed.

%% update/3 decides the new policy from a snapshot read before its own
%% transaction. If a different concurrent write lands in the window
%% between that read and the transaction, the payload_version guard must
%% cause a retry rather than silently overwriting the concurrent write.
update_retries_on_concurrent_change(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, update_retries_on_concurrent_change1, [Config]).

update_retries_on_concurrent_change1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue-race">>),
    Queue = amqqueue:new(QName, none, true, false, none, [], ?VHOST, #{},
                         rabbit_classic_queue),
    ?assertEqual({created, Queue}, rabbit_db_queue:create_or_get(Queue)),

    %% Simulate a concurrent write landing in the window between
    %% update/3's own snapshot read and its transaction: bump the
    %% queue's version via a completely different field, from inside
    %% the caller-supplied GetUpdatedQueueFun, exactly once.
    %%
    %% update_function below reapplies PrecomputedArgs -- a value
    %% derived from Q0, the snapshot read *before* the bump -- the same
    %% way the real rabbit_policy:get_updated_queue/2 precomputes
    %% Decorators outside the transaction and blindly writes it back.
    %% Without the payload_version guard forcing a retry (which
    %% recomputes PrecomputedArgs from the now-current, post-bump
    %% queue), this blind reapplication is exactly what would silently
    %% clobber the concurrent write.
    Invocations = counters:new(1, []),
    Bumped = counters:new(1, []),
    GetUpdatedQueueFun =
        fun(Q0) ->
                counters:add(Invocations, 1, 1),
                PrecomputedArgs = amqqueue:get_arguments(Q0),
                case counters:get(Bumped, 1) of
                    0 ->
                        counters:add(Bumped, 1, 1),
                        _ = rabbit_db_queue:update(
                              QName,
                              fun(Q) ->
                                      amqqueue:set_arguments(
                                        Q, [{<<"x-concurrent">>, long, 1}])
                              end);
                    _ ->
                        ok
                end,
                #{queue => Q0,
                  update_function =>
                      fun(Q) ->
                              Q1 = amqqueue:set_policy(Q, new_policy),
                              amqqueue:set_arguments(Q1, PrecomputedArgs)
                      end}
        end,
    {[], [{_, NewQ}]} = rabbit_db_policy:update(
                           ?VHOST, fun(_X) -> no_change end, GetUpdatedQueueFun),

    %% Neither the concurrent write nor this policy update was dropped.
    ?assertEqual(new_policy, amqqueue:get_policy(NewQ)),
    ?assertEqual([{<<"x-concurrent">>, long, 1}], amqqueue:get_arguments(NewQ)),
    %% GetUpdatedQueueFun ran again for the retry: once for the attempt
    %% that hit the version mismatch, and again for the one that saw the
    %% bump and succeeded.
    ?assert(counters:get(Invocations, 1) >= 2),
    passed.

%% Same race as update_retries_on_concurrent_change/1, on the exchange
%% side of update/3 instead of the queue side.
update_retries_on_concurrent_change_exchange(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, update_retries_on_concurrent_change_exchange1,
               [Config]).

update_retries_on_concurrent_change_exchange1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange-race">>),
    Exchange = #exchange{name = XName, durable = true},
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange)),

    Invocations = counters:new(1, []),
    Bumped = counters:new(1, []),
    GetUpdatedExchangeFun =
        fun(X0) ->
                counters:add(Invocations, 1, 1),
                PrecomputedArgs = X0#exchange.arguments,
                case counters:get(Bumped, 1) of
                    0 ->
                        counters:add(Bumped, 1, 1),
                        _ = rabbit_db_exchange:update(
                              XName,
                              fun(X) ->
                                      X#exchange{arguments =
                                                     [{<<"x-concurrent">>, long, 1}]}
                              end);
                    _ ->
                        ok
                end,
                #{exchange => X0,
                  update_function =>
                      fun(X) ->
                              X#exchange{policy = new_policy,
                                         arguments = PrecomputedArgs}
                      end}
        end,
    {[{_, NewX}], []} = rabbit_db_policy:update(
                           ?VHOST, GetUpdatedExchangeFun, fun(_Q) -> no_change end),

    ?assertEqual(new_policy, NewX#exchange.policy),
    ?assertEqual([{<<"x-concurrent">>, long, 1}], NewX#exchange.arguments),
    ?assert(counters:get(Invocations, 1) >= 2),
    passed.
