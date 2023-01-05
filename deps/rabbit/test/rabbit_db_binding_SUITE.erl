%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_binding_SUITE).

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
     create,
     exists,
     delete,
     auto_delete,
     get_all,
     get_all_by_vhost,
     get_all_for_source,
     get_all_for_destination,
     get_all_for_source_and_destination,
     get_all_for_source_and_destination_reverse,
     fold,
     match,
     match_routing_key
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
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_exchange, clear, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_binding, clear, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

create(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, create1, [Config]).

create1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertMatch({error, {resources_missing, [_, _]}},
                 rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({error, {resources_missing, [_]}},
                 rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch({error, too_bad},
                 rabbit_db_binding:create(Binding, fun(_, _) -> {error, too_bad} end)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    passed.

exists(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, exists1, [Config]).

exists1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertMatch({error, {resources_missing, [{not_found, _}, {not_found, _}]}},
                 rabbit_db_binding:exists(Binding)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertEqual(false, rabbit_db_binding:exists(Binding)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual(true, rabbit_db_binding:exists(Binding)),
    passed.

delete(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config]).

delete1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, auto_delete = false, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, auto_delete = false, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual(ok, rabbit_db_binding:delete(Binding, fun(_, _) -> ok end)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    Ret = rabbit_db_binding:delete(Binding, fun(_, _) -> ok end),
    ?assertMatch({ok, _}, Ret),
    {ok, Deletions} = Ret,
    ?assertMatch({#exchange{}, not_deleted, [#binding{}], none},
                 dict:fetch(XName1, Deletions)),
    ?assertEqual(false, rabbit_db_binding:exists(Binding)),
    passed.

auto_delete(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config]).

auto_delete1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, auto_delete = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, auto_delete = false, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual(ok, rabbit_db_binding:delete(Binding, fun(_, _) -> ok end)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    Ret = rabbit_db_binding:delete(Binding, fun(_, _) -> ok end),
    ?assertMatch({ok, _}, Ret),
    {ok, Deletions} = Ret,
    ?assertMatch({#exchange{}, deleted, [#binding{}], none},
                 dict:fetch(XName1, Deletions)),
    ?assertEqual(false, rabbit_db_binding:exists(Binding)),
    passed.

get_all(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all1, [Config]).

get_all1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual([], rabbit_db_binding:get_all()),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([Binding], rabbit_db_binding:get_all()),
    passed.

get_all_by_vhost(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all_by_vhost1, [Config]).

get_all_by_vhost1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual([], rabbit_db_binding:get_all(?VHOST)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([Binding], rabbit_db_binding:get_all(?VHOST)),
    ?assertEqual([], rabbit_db_binding:get_all(<<"other-vhost">>)),
    passed.

get_all_for_source(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, get_all_for_source1, [Config]).

get_all_for_source1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual([], rabbit_db_binding:get_all_for_source(XName1)),
    ?assertEqual([], rabbit_db_binding:get_all_for_source(XName2)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([Binding], rabbit_db_binding:get_all_for_source(XName1)),
    ?assertEqual([], rabbit_db_binding:get_all_for_source(XName2)),
    passed.

get_all_for_destination(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, get_all_for_destination1, [Config]).

get_all_for_destination1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual([], rabbit_db_binding:get_all_for_destination(XName1)),
    ?assertEqual([], rabbit_db_binding:get_all_for_destination(XName2)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([], rabbit_db_binding:get_all_for_destination(XName1)),
    ?assertEqual([Binding], rabbit_db_binding:get_all_for_destination(XName2)),
    passed.

get_all_for_source_and_destination(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, get_all_for_source_and_destination1, [Config]).

get_all_for_source_and_destination1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual([], rabbit_db_binding:get_all(XName1, XName2, false)),
    ?assertEqual([], rabbit_db_binding:get_all(XName2, XName1, false)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([Binding], rabbit_db_binding:get_all(XName1, XName2, false)),
    ?assertEqual([], rabbit_db_binding:get_all(XName1, XName1, false)),
    ?assertEqual([], rabbit_db_binding:get_all(XName2, XName1, false)),
    ?assertEqual([], rabbit_db_binding:get_all(XName2, XName2, false)),
    passed.

get_all_for_source_and_destination_reverse(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, get_all_for_source_and_destination_reverse1, [Config]).

get_all_for_source_and_destination_reverse1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual([], rabbit_db_binding:get_all(XName1, XName2, true)),
    ?assertEqual([], rabbit_db_binding:get_all(XName2, XName1, true)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([Binding], rabbit_db_binding:get_all(XName1, XName2, true)),
    ?assertEqual([], rabbit_db_binding:get_all(XName1, XName1, true)),
    ?assertEqual([], rabbit_db_binding:get_all(XName2, XName1, true)),
    ?assertEqual([], rabbit_db_binding:get_all(XName2, XName2, true)),
    passed.

fold(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, fold1, [Config]).

fold1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertEqual([], rabbit_db_binding:fold(fun(B, Acc) -> [B | Acc] end, [])),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([Binding], rabbit_db_binding:fold(fun(B, Acc) -> [B | Acc] end, [])),
    passed.

match(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, match1, [Config]).

match1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2,
                       args = #{foo => bar}},
    ?assertEqual([], rabbit_db_binding:match(XName1, fun(#binding{args = Args}) ->
                                                             maps:get(foo, Args) =:= bar
                                                     end)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([XName2],
                 rabbit_db_binding:match(XName1, fun(#binding{args = Args}) ->
                                                         maps:get(foo, Args) =:= bar
                                                 end)),
    ?assertEqual([],
                 rabbit_db_binding:match(XName1, fun(#binding{args = Args}) ->
                                                         maps:is_key(headers, Args)
                                                 end)),
    passed.

match_routing_key(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, match1, [Config]).

match_routing_key1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true, decorators = {[], []}},
    Exchange2 = #exchange{name = XName2, durable = true, decorators = {[], []}},
    Binding = #binding{source = XName1, key = <<"*.*">>, destination = XName2,
                       args = #{foo => bar}},
    ?assertEqual([], rabbit_db_binding:match_routing_key(XName1, [<<"a.b.c">>], false)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertEqual([], rabbit_db_binding:match_routing_key(XName1, [<<"a.b.c">>], false)),
    ?assertEqual([XName2], rabbit_db_binding:match_routing_key(XName1, [<<"a.b">>], false)),
    passed.
