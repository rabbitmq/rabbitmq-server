%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_topic_exchange_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
     set,
     delete,
     delete_all_for_exchange,
     match
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
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_topic_exchange, clear, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

set(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set1, [Config]).

set1(_Config) ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    RoutingKey = <<"a.b.c">>,
    Binding = #binding{source = Src, key = RoutingKey, destination = Dst, args = #{}},
    ?assertEqual([], rabbit_db_topic_exchange:match(Src, RoutingKey)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding)),
    ?assertEqual([Dst], rabbit_db_topic_exchange:match(Src, RoutingKey)),
    passed.

delete(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config]).

delete1(_Config) ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Dst3= rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    Dsts = lists:sort([Dst1, Dst2, Dst3]),
    RoutingKey = <<"a.b.c">>,
    Binding1 = #binding{source = Src, key = RoutingKey, destination = Dst1, args = #{}},
    Binding2 = #binding{source = Src, key = RoutingKey, destination = Dst2, args = #{}},
    Binding3 = #binding{source = Src, key = RoutingKey, destination = Dst3, args = #{}},
    ?assertEqual(ok, rabbit_db_topic_exchange:delete([Binding1])),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding1)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding2)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding3)),
    ?assertEqual(Dsts, lists:sort(rabbit_db_topic_exchange:match(Src, RoutingKey))),
    ?assertEqual(ok, rabbit_db_topic_exchange:delete([Binding1, Binding2])),
    ?assertEqual([Dst3], rabbit_db_topic_exchange:match(Src, RoutingKey)),
    passed.

delete_all_for_exchange(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_all_for_exchange1, [Config]).

delete_all_for_exchange1(_Config) ->
    Src1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    Src2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Dsts = lists:sort([Dst1, Dst2]),
    RoutingKey = <<"a.b.c">>,
    ?assertEqual(ok, rabbit_db_topic_exchange:delete_all_for_exchange(Src1)),
    set(Src1, RoutingKey, Dst1),
    set(Src1, RoutingKey, Dst2),
    set(Src2, RoutingKey, Dst1),
    ?assertEqual(Dsts, lists:sort(rabbit_db_topic_exchange:match(Src1, RoutingKey))),
    ?assertEqual([Dst1], rabbit_db_topic_exchange:match(Src2, RoutingKey)),
    ?assertEqual(ok, rabbit_db_topic_exchange:delete_all_for_exchange(Src1)),
    ?assertEqual([], rabbit_db_topic_exchange:match(Src1, RoutingKey)),
    ?assertEqual([Dst1], rabbit_db_topic_exchange:match(Src2, RoutingKey)),
    passed.

match(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, match1, [Config]).

match1(_Config) ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Dst3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    Dst4 = rabbit_misc:r(?VHOST, queue, <<"test-queue4">>),
    Dst5 = rabbit_misc:r(?VHOST, queue, <<"test-queue5">>),
    Dst6 = rabbit_misc:r(?VHOST, queue, <<"test-queue6">>),
    set(Src, <<"a.b.c">>, Dst1),
    set(Src, <<"a.*.c">>, Dst2),
    set(Src, <<"*.#">>, Dst3),
    set(Src, <<"#">>, Dst4),
    set(Src, <<"#.#">>, Dst5),
    set(Src, <<"a.*">>, Dst6),
    Dsts1 = lists:sort([Dst1, Dst2, Dst3, Dst4, Dst5]),
    ?assertEqual(Dsts1, lists:usort(rabbit_db_topic_exchange:match(Src, <<"a.b.c">>))),
    Dsts2 = lists:sort([Dst3, Dst4, Dst5, Dst6]),
    ?assertEqual(Dsts2, lists:usort(rabbit_db_topic_exchange:match(Src, <<"a.b">>))),
    Dsts3 = lists:sort([Dst4, Dst5]),
    ?assertEqual(Dsts3, lists:usort(rabbit_db_topic_exchange:match(Src, <<"">>))),
    Dsts4 = lists:sort([Dst3, Dst4, Dst5]),
    ?assertEqual(Dsts4, lists:usort(rabbit_db_topic_exchange:match(Src, <<"zen.rabbit">>))),
    passed.

set(Src, RoutingKey, Dst) ->
    Binding = #binding{source = Src, key = RoutingKey, destination = Dst, args = #{}},
    ok = rabbit_db_topic_exchange:set(Binding).
