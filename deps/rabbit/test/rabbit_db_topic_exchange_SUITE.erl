%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_topic_exchange_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([nowarn_export_all, export_all]).

-define(VHOST, <<"/">>).

all() ->
    [
     {group, tests}
    ].

groups() ->
    [
     {tests, [shuffle], all_tests()}
    ].

all_tests() ->
    [
     set,
     delete,
     delete_all_for_exchange,
     match,
     match_return_binding_keys_many_destinations,
     match_return_binding_keys_single_destination
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
    ?assertEqual({[], #{}}, rabbit_db_topic_exchange:match(Src, RoutingKey, #{})),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding)),
    ?assertEqual({[Dst], #{}}, rabbit_db_topic_exchange:match(Src, RoutingKey, #{})),
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
    {Actual, #{}} = rabbit_db_topic_exchange:match(Src, RoutingKey, #{}),
    ?assertEqual(Dsts, lists:sort(Actual)),
    ?assertEqual(ok, rabbit_db_topic_exchange:delete([Binding1, Binding2])),
    ?assertEqual({[Dst3], #{}}, rabbit_db_topic_exchange:match(Src, RoutingKey, #{})),
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
    {Actual, #{}} = rabbit_db_topic_exchange:match(Src1, RoutingKey, #{}),
    ?assertEqual(Dsts, lists:sort(Actual)),
    ?assertEqual({[Dst1], #{}}, rabbit_db_topic_exchange:match(Src2, RoutingKey, #{})),
    ?assertEqual(ok, rabbit_db_topic_exchange:delete_all_for_exchange(Src1)),
    ?assertEqual({[], #{}}, rabbit_db_topic_exchange:match(Src1, RoutingKey, #{})),
    ?assertEqual({[Dst1], #{}}, rabbit_db_topic_exchange:match(Src2, RoutingKey, #{})),
    passed.

match(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, ?MODULE, match_0, [Config]).

match_0(_Config) ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Dst3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    Dst4 = rabbit_misc:r(?VHOST, queue, <<"test-queue4">>),
    Dst5 = rabbit_misc:r(?VHOST, queue, <<"test-queue5">>),
    Dst6 = rabbit_misc:r(?VHOST, queue, <<"test-queue6">>),
    Dst7 = rabbit_misc:r(?VHOST, queue, <<"test-queue7">>),
    set(Src, <<"a.b.c">>, Dst1),
    set(Src, <<"a.*.c">>, Dst2),
    set(Src, <<"*.#">>, Dst3),
    set(Src, <<"#">>, Dst4),
    set(Src, <<"#.#">>, Dst5),
    set(Src, <<"a.*">>, Dst6),
    set(Src, <<"a.b.#">>, Dst7),
    {Dsts1, Qs1} = rabbit_db_topic_exchange:match(Src, <<"a.b.c">>, #{}),
    ?assertEqual(lists:sort([Dst1, Dst2, Dst3, Dst4, Dst5, Dst7]),
                 lists:usort(Dsts1)),
    {Dsts2, Qs2} = rabbit_db_topic_exchange:match(Src, <<"a.b">>, #{}),
    ?assertEqual(lists:sort([Dst3, Dst4, Dst5, Dst6, Dst7]),
                 lists:usort(Dsts2)),
    {Dsts3, Qs3} = rabbit_db_topic_exchange:match(Src, <<"">>, #{}),
    ?assertEqual(lists:sort([Dst4, Dst5]),
                 lists:usort(Dsts3)),
    {Dsts4, Qs4} = rabbit_db_topic_exchange:match(Src, <<"zen.rabbit">>, #{}),
    ?assertEqual(lists:sort([Dst3, Dst4, Dst5]),
                 lists:usort(Dsts4)),
    ?assert(lists:all(fun(Qs) -> maps:size(Qs) =:= 0 end, [Qs1, Qs2, Qs3, Qs4])),
    passed.

match_return_binding_keys_many_destinations(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, ?MODULE, match_return_binding_keys_many_destinations0, []).

match_return_binding_keys_many_destinations0() ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"mqtt-subscription-q1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"mqtt-subscription-q2">>),
    Dst3 = rabbit_misc:r(?VHOST, queue, <<"mqtt-subscription-q3">>),
    Dst4 = rabbit_misc:r(?VHOST, queue, <<"mqtt-subscription-q4">>),
    Dst5 = rabbit_misc:r(?VHOST, queue, <<"mqtt-subscription-q5">>),
    Dst6 = rabbit_misc:r(?VHOST, queue, <<"mqtt-subscription-q6">>),
    Dst7 = rabbit_misc:r(?VHOST, queue, <<"mqtt-subscription-q7">>),
    Dst8 = rabbit_misc:r(?VHOST, exchange, <<"e1">>),
    Dst9 = rabbit_misc:r(?VHOST, exchange, <<"e2">>),
    set(Src, <<"a.b.c">>, Dst1),
    set(Src, <<"a.*.c">>, Dst2),
    set(Src, <<"*.#">>, Dst3),
    set(Src, <<"#">>, Dst4),
    set(Src, <<"#.#">>, Dst5),
    set(Src, <<"a.*">>, Dst6),
    set(Src, <<"a.b.#">>, Dst7),
    set(Src, <<"a.b.c">>, Dst8),
    set(Src, <<"#">>, Dst9),

    Mod = rabbit_feature_flags,
    ok = meck:new(Mod),
    ok = meck:expect(Mod, is_enabled, [mqtt_v5], true),
    Opts = #{return_binding_keys => true},

    {Exchanges1, Qs1} = rabbit_db_topic_exchange:match(Src, <<"a.b.c">>, Opts),
    ?assertMatch(#{Dst1 := [<<"a.b.c">>],
                   Dst2 := [<<"a.*.c">>],
                   Dst3 := [<<"*.#">>],
                   Dst4 := [<<"#">>],
                   Dst5 := [<<"#.#">> | _],
                   Dst7 := [<<"a.b.#">>]},
                 Qs1),
    ?assertEqual(6, map_size(Qs1)),
    ?assertEqual(lists:sort([Dst8, Dst9]),
                 lists:sort(Exchanges1)),

    {Exchanges2, Qs2} = rabbit_db_topic_exchange:match(Src, <<"a.b">>, Opts),
    ?assertMatch(#{Dst3 := [<<"*.#">>],
                   Dst4 := [<<"#">>],
                   Dst5 := [<<"#.#">> | _],
                   Dst6 := [<<"a.*">>],
                   Dst7 := [<<"a.b.#">>]},
                 Qs2),
    ?assertEqual(5, map_size(Qs2)),
    ?assertEqual([Dst9], Exchanges2),

    {Exchanges3, Qs3} = rabbit_db_topic_exchange:match(Src, <<"">>, Opts),
    ?assertMatch(#{Dst4 := [<<"#">>],
                   Dst5 := [<<"#.#">> | _]},
                 Qs3),
    ?assertEqual(2, map_size(Qs3)),
    ?assertEqual([Dst9], Exchanges3),

    {Exchanges4, Qs4} = rabbit_db_topic_exchange:match(Src, <<"zen.rabbit">>, Opts),
    ?assertMatch(#{Dst3 := [<<"*.#">>],
                   Dst4 := [<<"#">>],
                   Dst5 := [<<"#.#">> | _]},
                 Qs4),
    ?assertEqual(3, map_size(Qs4)),
    ?assertEqual([Dst9], Exchanges4),

    ?assert(meck:validate(Mod)),
    ok = meck:unload(Mod),
    passed.

match_return_binding_keys_single_destination(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, ?MODULE, match_return_binding_keys_single_destination0, []).

match_return_binding_keys_single_destination0() ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst = rabbit_misc:r(?VHOST, queue, <<"mqtt-subscription-myclientqos1">>),
    [set(Src, BKey, Dst) ||
     BKey <- [<<"a.b.c">>, <<"a.*.c">>, <<"*.#">>, <<"#">>, <<"#.#">>, <<"a.*">>, <<"a.b.#">>]],

    Mod = rabbit_feature_flags,
    ok = meck:new(Mod),
    ok = meck:expect(Mod, is_enabled, [mqtt_v5], true),
    Opts = #{return_binding_keys => true},

    {[], Qs} = rabbit_db_topic_exchange:match(Src, <<"a.b.c">>, Opts),
    ?assertEqual(1, map_size(Qs)),
    BindingKeys = maps:get(Dst, Qs),
    ?assertEqual(lists:sort([<<"a.b.c">>, <<"a.*.c">>, <<"*.#">>, <<"#">>, <<"#.#">>, <<"a.b.#">>]),
                 lists:usort(BindingKeys)),

    ?assert(meck:validate(Mod)),
    ok = meck:unload(Mod),
    passed.

set(Src, RoutingKey, Dst) ->
    Binding = #binding{source = Src, key = RoutingKey, destination = Dst, args = #{}},
    ok = rabbit_db_topic_exchange:set(Binding).
