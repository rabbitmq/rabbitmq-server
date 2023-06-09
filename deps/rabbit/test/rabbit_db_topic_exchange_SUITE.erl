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
    Binding = #binding{source = Src, key = RoutingKey, destination = Dst},
    ?assertEqual(#{}, rabbit_db_topic_exchange:match(Src, RoutingKey)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding)),
    ?assertEqual(ok, rabbit_db_topic_exchange:set(Binding)),
    ?assertEqual(#{Dst => #{}}, rabbit_db_topic_exchange:match(Src, RoutingKey)),
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
    ?assertEqual(maps:from_keys([Dst1, Dst2, Dst3], #{}),
                 rabbit_db_topic_exchange:match(Src, RoutingKey)),
    ?assertEqual(ok, rabbit_db_topic_exchange:delete([Binding1, Binding2])),
    ?assertEqual(#{Dst3 => #{}},
                 rabbit_db_topic_exchange:match(Src, RoutingKey)),
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
    ?assertEqual(maps:from_keys([Dst1, Dst2], #{}),
                 rabbit_db_topic_exchange:match(Src1, RoutingKey)),
    ?assertEqual(#{Dst1 => #{}},
                 rabbit_db_topic_exchange:match(Src2, RoutingKey)),
    ?assertEqual(ok, rabbit_db_topic_exchange:delete_all_for_exchange(Src1)),
    ?assertEqual(#{}, rabbit_db_topic_exchange:match(Src1, RoutingKey)),
    ?assertEqual(#{Dst1 => #{}},
                 rabbit_db_topic_exchange:match(Src2, RoutingKey)),
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
    ?assertEqual(maps:from_keys([Dst1, Dst2, Dst3, Dst4, Dst5, Dst7], #{}),
                 rabbit_db_topic_exchange:match(Src, <<"a.b.c">>)),
    ?assertEqual(maps:from_keys([Dst3, Dst4, Dst5, Dst6, Dst7], #{}),
                 rabbit_db_topic_exchange:match(Src, <<"a.b">>)),
    ?assertEqual(maps:from_keys([Dst4, Dst5], #{}),
                 rabbit_db_topic_exchange:match(Src, <<"">>)),
    ?assertEqual(maps:from_keys([Dst3, Dst4, Dst5], #{}),
                 rabbit_db_topic_exchange:match(Src, <<"zen.rabbit">>)),
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
    ?assertEqual(#{Dst1 => #{<<"a.b.c">> => true},
                   Dst2 => #{<<"a.*.c">> => true},
                   Dst3 => #{<<"*.#">> => true},
                   Dst4 => #{<<"#">> => true},
                   Dst5 => #{<<"#.#">> => true},
                   Dst7 => #{<<"a.b.#">> => true},
                   Dst8 => #{<<"a.b.c">> => true},
                   Dst9 => #{<<"#">> => true}},
                 rabbit_db_topic_exchange:match(Src, <<"a.b.c">>)),

    ?assertEqual(#{Dst3 => #{<<"*.#">> => true},
                   Dst4 => #{<<"#">> => true},
                   Dst5 => #{<<"#.#">> => true},
                   Dst6 => #{<<"a.*">> => true},
                   Dst7 => #{<<"a.b.#">> => true},
                   Dst9 => #{<<"#">> => true}},
                 rabbit_db_topic_exchange:match(Src, <<"a.b">>)),

    ?assertEqual(#{Dst4 => #{<<"#">> => true},
                   Dst5 => #{<<"#.#">> => true},
                   Dst9 => #{<<"#">> => true}},
                 rabbit_db_topic_exchange:match(Src, <<"">>)),

    ?assertEqual(#{Dst3 => #{<<"*.#">> => true},
                   Dst4 => #{<<"#">> => true},
                   Dst5 => #{<<"#.#">> => true},
                   Dst9 => #{<<"#">> => true}},
                 rabbit_db_topic_exchange:match(Src, <<"zen.rabbit">>)),
    passed.

match_return_binding_keys_single_destination(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, ?MODULE, match_return_binding_keys_single_destination0, []).

match_return_binding_keys_single_destination0() ->
    Src = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Dst = rabbit_misc:r(?VHOST, queue, <<"q">>),
    [set(Src, BKey, Dst, true) ||
     BKey <- [<<"a.b.c">>, <<"a.*.c">>, <<"*.#">>, <<"#">>, <<"#.#">>, <<"a.*">>, <<"a.b.#">>]],
    ?assertEqual(#{Dst => #{<<"a.b.c">> => true,
                            <<"a.*.c">> => true,
                            <<"*.#">> => true,
                            <<"#">> => true,
                            <<"#.#">> => true,
                            <<"a.b.#">> => true}},
                 rabbit_db_topic_exchange:match(Src, <<"a.b.c">>)),
    passed.

set(Src, BindingKey, Dst) ->
    set(Src, BindingKey, Dst, []).

set(Src, BindingKey, Dst, _ReturnBindingKeys = true) ->
    set(Src, BindingKey, Dst, [{<<"x-binding-key">>, longstr, BindingKey}]);
set(Src, BindingKey, Dst, Args)
  when is_list(Args) ->
    Binding = #binding{source = Src, key = BindingKey, destination = Dst, args = Args},
    ok = rabbit_db_topic_exchange:set(Binding).
