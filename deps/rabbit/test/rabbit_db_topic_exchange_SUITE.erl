%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_topic_exchange_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([nowarn_export_all, export_all]).

-define(VHOST, <<"/">>).

all() ->
    [
     {group, mnesia_store}
    ].

groups() ->
    [
     {mnesia_store, [], mnesia_tests()},
     {benchmarks, [], benchmarks()}
    ].

mnesia_tests() ->
    [
     set,
     delete,
     delete_all_for_exchange,
     match,
     match_return_binding_keys_many_destinations,
     match_return_binding_keys_single_destination,
     build_key_from_topic_trie_binding_record,
     build_key_from_deletion_events,
     build_key_from_binding_deletion_event,
     build_multiple_key_from_deletion_events
    ].

benchmarks() ->
    [
     match_benchmark
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mnesia_store = Group, Config0) ->
    Config = rabbit_ct_helpers:set_config(Config0, [{metadata_store, mnesia}]),
    init_per_group_common(Group, Config);
init_per_group(Group, Config) ->
    init_per_group_common(Group, Config).

init_per_group_common(Group, Config) ->
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
    XName = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    {ok, X} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, lookup, [XName]),
    Config1 = rabbit_ct_helpers:set_config(Config, [{exchange_name, XName},
                                                    {exchange, X}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

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
    Binding = #binding{source = Src, key = RoutingKey, destination = Dst},
    ?assertEqual([], rabbit_db_topic_exchange:match(Src, RoutingKey, #{})),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding)),
    ?assertEqual([Dst], rabbit_db_topic_exchange:match(Src, RoutingKey, #{})),
    passed.

delete(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config]).

delete1(_Config) ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Dst3= rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    RoutingKey = <<"a.b.c">>,
    Binding1 = #binding{source = Src, key = RoutingKey, destination = Dst1},
    Binding2 = #binding{source = Src, key = RoutingKey, destination = Dst2},
    Binding3 = #binding{source = Src, key = RoutingKey, destination = Dst3},
    ?assertEqual(ok, rabbit_db_topic_exchange:delete([Binding1])),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding1)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding2)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding3)),
    ?assertEqual(lists:sort([Dst1, Dst2, Dst3]),
                 lists:sort(rabbit_db_topic_exchange:match(Src, RoutingKey, #{}))),
    ?assertEqual(ok, rabbit_db_topic_exchange:delete([Binding1, Binding2])),
    ?assertEqual([Dst3], rabbit_db_topic_exchange:match(Src, RoutingKey, #{})),
    passed.

delete_all_for_exchange(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_all_for_exchange1, [Config]).

delete_all_for_exchange1(_Config) ->
    Src1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    Src2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    RoutingKey = <<"a.b.c">>,
    ?assertEqual(ok, rabbit_db_topic_exchange:delete_all_for_exchange(Src1)),
    set(Src1, RoutingKey, Dst1),
    set(Src1, RoutingKey, Dst2),
    set(Src2, RoutingKey, Dst1),
    ?assertEqual(lists:sort([Dst1, Dst2]),
                 lists:sort(rabbit_db_topic_exchange:match(Src1, RoutingKey, #{}))),
    ?assertEqual([Dst1], rabbit_db_topic_exchange:match(Src2, RoutingKey, #{})),
    ?assertEqual(ok, rabbit_db_topic_exchange:delete_all_for_exchange(Src1)),
    ?assertEqual([], rabbit_db_topic_exchange:match(Src1, RoutingKey, #{})),
    ?assertEqual([Dst1], rabbit_db_topic_exchange:match(Src2, RoutingKey, #{})),
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
    ?assertEqual(lists:sort([Dst1, Dst2, Dst3, Dst4, Dst5, Dst7]),
                 lists:usort(rabbit_db_topic_exchange:match(Src, <<"a.b.c">>, #{}))),
    ?assertEqual(lists:sort([Dst3, Dst4, Dst5, Dst6, Dst7]),
                 lists:usort(rabbit_db_topic_exchange:match(Src, <<"a.b">>, #{}))),
    ?assertEqual(lists:sort([Dst4, Dst5]),
                 lists:usort(rabbit_db_topic_exchange:match(Src, <<"">>, #{}))),
    ?assertEqual(lists:sort([Dst3, Dst4, Dst5]),
                 lists:usort(rabbit_db_topic_exchange:match(Src, <<"zen.rabbit">>, #{}))),
    passed.

match_return_binding_keys_many_destinations(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, ?MODULE, match_return_binding_keys_many_destinations0, []).

match_return_binding_keys_many_destinations0() ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"q1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"q2">>),
    Dst3 = rabbit_misc:r(?VHOST, queue, <<"q3">>),
    Dst4 = rabbit_misc:r(?VHOST, queue, <<"q4">>),
    Dst5 = rabbit_misc:r(?VHOST, queue, <<"q5">>),
    Dst6 = rabbit_misc:r(?VHOST, queue, <<"q6">>),
    Dst7 = rabbit_misc:r(?VHOST, queue, <<"q7">>),
    Dst8 = rabbit_misc:r(?VHOST, exchange, <<"e1">>),
    Dst9 = rabbit_misc:r(?VHOST, exchange, <<"e2">>),
    [set(Src, BindingKey, Dst, true) ||
     {BindingKey, Dst} <- [{<<"a.b.c">>, Dst1},
                           {<<"a.*.c">>, Dst2},
                           {<<"*.#">>, Dst3},
                           {<<"#">>, Dst4},
                           {<<"#.#">>, Dst5},
                           {<<"a.*">>, Dst6},
                           {<<"a.b.#">>, Dst7},
                           {<<"a.b.c">>, Dst8},
                           {<<"#">>, Dst9}]],
    Opts = #{return_binding_keys => true},
    ?assertEqual(lists:sort(
                   [{Dst1, <<"a.b.c">>},
                    {Dst2, <<"a.*.c">>},
                    {Dst3, <<"*.#">>},
                    {Dst4, <<"#">>},
                    {Dst5, <<"#.#">>},
                    {Dst7, <<"a.b.#">>},
                    Dst8,
                    Dst9]),
                 lists:usort(rabbit_db_topic_exchange:match(Src, <<"a.b.c">>, Opts))),

    ?assertEqual(lists:sort(
                   [{Dst3, <<"*.#">>},
                    {Dst4, <<"#">>},
                    {Dst5, <<"#.#">>},
                    {Dst6, <<"a.*">>},
                    {Dst7, <<"a.b.#">>},
                    Dst9]),
                 lists:usort(rabbit_db_topic_exchange:match(Src, <<"a.b">>, Opts))),

    ?assertEqual(lists:sort(
                   [{Dst4, <<"#">>},
                    {Dst5, <<"#.#">>},
                    Dst9]),
                 lists:usort(rabbit_db_topic_exchange:match(Src, <<"">>, Opts))),

    ?assertEqual(lists:sort(
                   [{Dst3, <<"*.#">>},
                    {Dst4, <<"#">>},
                    {Dst5, <<"#.#">>},
                    Dst9]),
                 lists:usort(rabbit_db_topic_exchange:match(Src, <<"zen.rabbit">>, Opts))),
    passed.

match_return_binding_keys_single_destination(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, ?MODULE, match_return_binding_keys_single_destination0, []).

match_return_binding_keys_single_destination0() ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst = rabbit_misc:r(?VHOST, queue, <<"q">>),
    [set(Src, BKey, Dst, true) ||
     BKey <- [<<"a.b.c">>, <<"a.*.c">>, <<"*.#">>, <<"#">>, <<"#.#">>, <<"a.*">>, <<"a.b.#">>]],
    Expected = lists:sort(
                 [{Dst, <<"a.b.c">>},
                  {Dst, <<"a.*.c">>},
                  {Dst, <<"*.#">>},
                  {Dst, <<"#">>},
                  {Dst, <<"#.#">>},
                  {Dst, <<"a.b.#">>}]),
    Actual = lists:usort(
               rabbit_db_topic_exchange:match(
                 Src, <<"a.b.c">>, #{return_binding_keys => true})),
    ?assertEqual(Expected, Actual),
    passed.

set(Src, BindingKey, Dst) ->
    set(Src, BindingKey, Dst, []).

set(Src, BindingKey, Dst, _ReturnBindingKeys = true) ->
    set(Src, BindingKey, Dst, [{<<"x-binding-key">>, longstr, BindingKey}]);
set(Src, BindingKey, Dst, Args)
  when is_list(Args) ->
    Binding = #binding{source = Src, key = BindingKey, destination = Dst, args = Args},
    ok = rabbit_db_topic_exchange:set(Binding).

%% ---------------------------------------------------------------------------
%% Functional tests
%% ---------------------------------------------------------------------------

build_key_from_topic_trie_binding_record(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, build_key_from_topic_trie_binding_record1, [Config]).

build_key_from_topic_trie_binding_record1(Config) ->
    XName = ?config(exchange_name, Config),
    X = ?config(exchange, Config),
    QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
    RK = <<"a.b.c.d.e.f">>,
    ok = rabbit_exchange_type_topic:add_binding(none, X, #binding{source = XName,
                                                                  destination = QName,
                                                                  key = RK,
                                                                  args = []}),
    SplitRK = rabbit_db_topic_exchange:split_topic_key(RK),
    [TopicTrieBinding] = ets:tab2list(rabbit_topic_trie_binding),
    ?assertEqual(SplitRK, rabbit_db_topic_exchange:trie_binding_to_key(TopicTrieBinding)),
    passed.

build_key_from_deletion_events(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, build_key_from_deletion_events1, [Config]).

build_key_from_deletion_events1(Config) ->
    XName = ?config(exchange_name, Config),
    X = ?config(exchange, Config),
    QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
    RK = <<"a.b.c.d.e.f">>,
    Binding = #binding{source = XName,
                       destination = QName,
                       key = RK,
                       args = []},
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding),
    SplitRK = rabbit_db_topic_exchange:split_topic_key(RK),
    Tables = [rabbit_topic_trie_binding, rabbit_topic_trie_edge],
    subscribe_to_mnesia_changes(Tables),
    rabbit_exchange_type_topic:remove_bindings(none, X, [Binding]),
    Records = receive_delete_events(7),
    unsubscribe_to_mnesia_changes(Tables),
    ?assertMatch([{_, SplitRK}],
                 rabbit_db_topic_exchange:trie_records_to_key(Records)),
    passed.

build_key_from_binding_deletion_event(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, build_key_from_binding_deletion_event1, [Config]).

build_key_from_binding_deletion_event1(Config) ->
    XName = ?config(exchange_name, Config),
    X = ?config(exchange, Config),
    QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
    RK = <<"a.b.c.d.e.f">>,
    Binding0 = #binding{source = XName,
                        destination = QName,
                        key = RK,
                        args = [some_args]},
    Binding = #binding{source = XName,
                       destination = QName,
                       key = RK,
                       args = []},
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding0),
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding),
    SplitRK = rabbit_db_topic_exchange:split_topic_key(RK),
    Tables = [rabbit_topic_trie_binding, rabbit_topic_trie_edge],
    subscribe_to_mnesia_changes(Tables),
    rabbit_exchange_type_topic:remove_bindings(none, X, [Binding]),
    Records = receive_delete_events(7),
    unsubscribe_to_mnesia_changes(Tables),
    ?assertMatch([{_, SplitRK}],
                 rabbit_db_topic_exchange:trie_records_to_key(Records)),
    passed.

build_multiple_key_from_deletion_events(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, build_multiple_key_from_deletion_events1, [Config]).

build_multiple_key_from_deletion_events1(Config) ->
    XName = ?config(exchange_name, Config),
    X = ?config(exchange, Config),
    QName = rabbit_misc:r(<<"/">>, queue, <<"q1">>),
    RK0 = <<"a.b.c.d.e.f">>,
    RK1 = <<"a.b.c.d">>,
    RK2 = <<"a.b.c.g.e.f">>,
    RK3 = <<"hare.rabbit.ho">>,
    Binding0 = #binding{source = XName, destination = QName, key = RK0, args = []},
    Binding1 = #binding{source = XName, destination = QName, key = RK1, args = []},
    Binding2 = #binding{source = XName, destination = QName, key = RK2, args = []},
    Binding3 = #binding{source = XName, destination = QName, key = RK3, args = []},
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding0),
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding1),
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding2),
    ok = rabbit_exchange_type_topic:add_binding(none, X, Binding3),
    SplitRK0 = rabbit_db_topic_exchange:split_topic_key(RK0),
    SplitRK1 = rabbit_db_topic_exchange:split_topic_key(RK1),
    SplitRK2 = rabbit_db_topic_exchange:split_topic_key(RK2),
    SplitRK3 = rabbit_db_topic_exchange:split_topic_key(RK3),
    Tables = [rabbit_topic_trie_binding, rabbit_topic_trie_edge],
    subscribe_to_mnesia_changes(Tables),
    rabbit_exchange_type_topic:delete(none, X),
    Records = receive_delete_events(7),
    unsubscribe_to_mnesia_changes(Tables),
    RKs = lists:sort([SplitRK0, SplitRK1, SplitRK2, SplitRK3]),
    ?assertMatch(
       RKs,
       lists:sort([RK || {_, RK} <- rabbit_db_topic_exchange:trie_records_to_key(Records)])),
    passed.

%% ---------------------------------------------------------------------------
%% Benchmarks
%% ---------------------------------------------------------------------------

match_benchmark(Config) ->
    %% run the benchmark with Mnesia first
    MnesiaResults = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, match_benchmark1, [Config]),

    %% migrate to Khepri
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {T, ok} = timer:tc(fun() ->
                               rabbit_ct_broker_helpers:enable_feature_flag(Config, Servers, khepri_db)
                       end),
    ct:pal("~p: time to migrate to Khepri: ~.2fs", [?FUNCTION_NAME, T/1000000]),

    %% run the same same benchmark with Khepri enabled
    KhepriResults = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, match_benchmark1, [Config]),

    %% print all the results first
    maps:foreach(fun(Test, KhepriResult) ->
                      MnesiaResult = maps:get(Test, MnesiaResults),
                      ct:pal("~p: Test: ~p, Mnesia: ~.2fus, Khepri: ~.2fus", [?FUNCTION_NAME, Test, MnesiaResult, KhepriResult])
              end, KhepriResults),

    %% fail the test if needed
    maps:foreach(fun(Test, KhepriResult) ->
                      MnesiaResult = maps:get(Test, MnesiaResults),
                      ?assert(KhepriResult < MnesiaResult * 1.5, "Khepri can't be significantly slower than Mnesia")
              end, KhepriResults).

match_benchmark1(_Config) ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    Dst2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Dst3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    Dst4 = rabbit_misc:r(?VHOST, queue, <<"test-queue4">>),
    Dst5 = rabbit_misc:r(?VHOST, queue, <<"test-queue5">>),
    Dst6 = rabbit_misc:r(?VHOST, queue, <<"test-queue6">>),

    SimpleTopics = [list_to_binary("a.b." ++ integer_to_list(N)) || N <- lists:seq(1,1000)],
    Bindings = [#binding{source = Src, key = RoutingKey, destination = Dst1, args = #{}} || RoutingKey <- SimpleTopics],
    BindingRes = [rabbit_db_topic_exchange:set(Binding) || Binding <- Bindings],
    ?assertMatch([ok], lists:uniq(BindingRes)),
    ok = rabbit_db_topic_exchange:set(#binding{source = Src, key = <<"a.b.*">>, destination = Dst2, args = #{}}),
    ok = rabbit_db_topic_exchange:set(#binding{source = Src, key = <<"a.b.*">>, destination = Dst3, args = #{}}),
    ok = rabbit_db_topic_exchange:set(#binding{source = Src, key = <<"a.#">>, destination = Dst4, args = #{}}),
    ok = rabbit_db_topic_exchange:set(#binding{source = Src, key = <<"*.b.42">>, destination = Dst5, args = #{}}),
    ok = rabbit_db_topic_exchange:set(#binding{source = Src, key = <<"#">>, destination = Dst6, args = #{}}),

    {Tany, _} = timer:tc(fun() ->
                              [rabbit_db_topic_exchange:match(Src, <<"foo">>) || _ <- lists:seq(1, 100)]
                      end),
    ?assertMatch([Dst6], rabbit_db_topic_exchange:match(Src, <<"foo">>)),

    {Tbar, _} = timer:tc(fun() ->
                              [rabbit_db_topic_exchange:match(Src, <<"a.b.bar">>) || _ <- lists:seq(1, 100)]
                      end),
    ?assertEqual(lists:sort([Dst2,Dst3,Dst4,Dst6]), lists:sort(rabbit_db_topic_exchange:match(Src, <<"a.b.bar">>))),

    {Tbaz, _} = timer:tc(fun() ->
                              [rabbit_db_topic_exchange:match(Src, <<"baz.b.42">>) || _ <- lists:seq(1, 100)]
                      end),
    ?assertEqual(lists:sort([Dst5,Dst6]), lists:sort(rabbit_db_topic_exchange:match(Src, <<"baz.b.42">>))),

    {Tsimple, Rsimple} = timer:tc(fun() ->
                                          [rabbit_db_topic_exchange:match(Src, RoutingKey)
                                           || RoutingKey <- SimpleTopics, RoutingKey =/= <<"a.b.123">>]
                                  end),
    ?assertEqual([Dst1,Dst2,Dst3,Dst4,Dst6], lists:sort(lists:uniq(hd(Rsimple)))),

    #{
      "average time to match `foo`" => Tany/100,
      "average time to match `a.b.bar`" => Tbar/100,
      "average time to match `baz.b.42`" => Tbaz/100,
      "average time to match a simple topic" => Tsimple/length(SimpleTopics)
     }.

subscribe_to_mnesia_changes([Table | Rest]) ->
    case mnesia:subscribe({table, Table, detailed}) of
        {ok, _} -> subscribe_to_mnesia_changes(Rest);
        Error -> Error
    end;
subscribe_to_mnesia_changes([]) ->
    ok.

unsubscribe_to_mnesia_changes([Table | Rest]) ->
    case mnesia:unsubscribe({table, Table, detailed}) of
        {ok, _} -> unsubscribe_to_mnesia_changes(Rest);
        Error   -> Error
    end;
unsubscribe_to_mnesia_changes([]) ->
    ok.

receive_delete_events(Num) ->
    receive_delete_events(Num, []).

receive_delete_events(0, Evts) ->
    receive
        {mnesia_table_event, {delete, _, Record, _, _}} ->
            receive_delete_events(0, [Record | Evts])
    after 0 ->
            Evts
    end;
receive_delete_events(N, Evts) ->
    receive
        {mnesia_table_event, {delete, _, Record, _, _}} ->
            receive_delete_events(N - 1, [Record | Evts])
    after 10000 ->
            Evts
    end.
