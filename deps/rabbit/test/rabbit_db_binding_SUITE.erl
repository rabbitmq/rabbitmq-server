%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_binding_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         create/1, create1/1,
         exists/1, exists1/1,
         delete_v1/1, delete_v2/1, delete1/2,
         auto_delete_v1/1, auto_delete_v2/1, auto_delete1/2,
         get_all/1, get_all1/1,
         get_all_by_vhost/1, get_all_by_vhost1/1,
         get_all_for_source/1, get_all_for_source1/1,
         get_all_for_destination/1, get_all_for_destination1/1,
         get_all_for_source_and_destination/1,
         get_all_for_source_and_destination1/1,
         get_all_for_source_and_destination_reverse/1,
         get_all_for_source_and_destination_reverse1/1,
         fold/1, fold1/1,
         match/1, match1/1,
         match_routing_key/1, match_routing_key1/1,
         tie_binding_to_dest_with_keep_while_cond/1,
         tie_binding_to_dest_with_keep_while_cond1/1
        ]).

-define(VHOST, <<"/">>).

all() ->
    [
     {group, all_tests},
     {group, feature_flags}
    ].

groups() ->
    [
     {all_tests, [], all_tests()},
     {feature_flags, [],
      [delete_v1,
       delete_v2,
       auto_delete_v1,
       auto_delete_v2,
       tie_binding_to_dest_with_keep_while_cond]}
    ].

all_tests() ->
    [
     create,
     exists,
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
    Config2 = case Group of
                  feature_flags ->
                      rabbit_ct_helpers:merge_app_env(
                        Config1,
                        {rabbit, [{forced_feature_flags_on_init, []}]});
                  _ ->
                      Config1
              end,
    rabbit_ct_helpers:run_steps(Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    case lists:member({name, feature_flags}, ?config(tc_group_properties, Config)) of
        true ->
            ok = rabbit_ct_broker_helpers:stop_broker(Config, 0),
            ok = rabbit_ct_broker_helpers:reset_node(Config, 0),
            ok = rabbit_ct_broker_helpers:start_broker(Config, 0);
        false ->
            ok
    end,
    Config.

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

delete_v1(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config, v1]).

delete_v2(Config) ->
    Ret = rabbit_ct_broker_helpers:enable_feature_flag(
            Config, tie_binding_to_dest_with_keep_while_cond),
    case Ret of
        ok ->
            passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config, v2]);
        {skip, _} = Skip ->
            Skip
    end.

delete1(_Config, Version) ->
    case Version of
        v1 ->
            ?assertNot(
               rabbit_feature_flags:is_enabled(
                 tie_binding_to_dest_with_keep_while_cond));
        v2 ->
            ?assert(
               rabbit_feature_flags:is_enabled(
                 tie_binding_to_dest_with_keep_while_cond))
    end,
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
    ?assertMatch({#exchange{}, not_deleted, [#binding{}]},
                 rabbit_binding:fetch_deletion(XName1, Deletions)),
    ?assertEqual(false, rabbit_db_binding:exists(Binding)),
    passed.

auto_delete_v1(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, auto_delete1, [Config, v1]).

auto_delete_v2(Config) ->
    Ret = rabbit_ct_broker_helpers:enable_feature_flag(
            Config, tie_binding_to_dest_with_keep_while_cond),
    case Ret of
        ok ->
            passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, auto_delete1, [Config, v2]);
        {skip, _} = Skip ->
            Skip
    end.

auto_delete1(_Config, Version) ->
    case Version of
        v1 ->
            ?assertNot(
               rabbit_feature_flags:is_enabled(
                 tie_binding_to_dest_with_keep_while_cond));
        v2 ->
            ?assert(
               rabbit_feature_flags:is_enabled(
                 tie_binding_to_dest_with_keep_while_cond))
    end,
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
    ?assertMatch({#exchange{}, deleted, [#binding{}]},
                 rabbit_binding:fetch_deletion(XName1, Deletions)),
    ?assertEqual(
       {error, {resources_missing, [{not_found, XName1}]}},
       rabbit_db_binding:exists(Binding)),
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

tie_binding_to_dest_with_keep_while_cond(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0,
               ?MODULE, tie_binding_to_dest_with_keep_while_cond1, [Config]).

tie_binding_to_dest_with_keep_while_cond1(_Config) ->
    %% The testcase uses `rabbit_db_*' modules directly to test specifically
    %% the content of the metadata store and how it behaves.
    %%
    %% It creates two exchanges, one with auto_delete=false, the other one with
    %% auto_delete=true. It then creates a queue and binds it to the two
    %% exchanges. It finally deletes the queue and check what happens with the
    %% bindings and the exchanges.
    %%
    %% This scenario is executed three times:
    %% 1. with the `tie_binding_to_dest_with_keep_while_cond' feature flag
    %%    disabled
    %% 2. with the feature flag enabled in the middle
    %% 3. with the feature flag already eabled
    %% The behaviour should be identical in all cases.

    %% Prepare resource records.
    NamePrefix = atom_to_binary(?FUNCTION_NAME),

    XName1 = rabbit_misc:r(?VHOST, exchange, <<NamePrefix/binary, "-x1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<NamePrefix/binary, "-x2">>),
    XPath1 = rabbit_db_exchange:khepri_exchange_path(XName1),
    XPath2 = rabbit_db_exchange:khepri_exchange_path(XName2),
    Exchange1 = #exchange{
                   name = XName1,
                   durable = true, auto_delete = true, decorators = {[], []}},
    Exchange2 = #exchange{
                   name = XName2,
                   durable = true, auto_delete = false, decorators = {[], []}},
    QName = rabbit_misc:r(?VHOST, queue, <<NamePrefix/binary, "-q">>),
    QPath = rabbit_db_queue:khepri_queue_path(QName),
    Queue = amqqueue:new(QName, none, true, false, none, [], ?VHOST, #{}),
    Binding1 = #binding{
                  source = XName1, key = <<"">>,
                  destination = QName, args = #{}},
    Binding2 = #binding{
                  source = XName2, key = <<"">>,
                  destination = QName, args = #{}},
    BPath1 = rabbit_db_binding:khepri_route_path(Binding1),
    BPath2 = rabbit_db_binding:khepri_route_path(Binding2),

    %% 1. Scenario with the feature flag disabled.
    ?assertNot(
       rabbit_feature_flags:is_enabled(
         tie_binding_to_dest_with_keep_while_cond)),

    ?assertMatch(
       {new, #exchange{}},
       rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch(
       {new, #exchange{}},
       rabbit_db_exchange:create_or_get(Exchange2)),

    ?assertMatch({created, Queue}, rabbit_db_queue:create_or_get(Queue)),

    ?assertMatch(ok, rabbit_db_binding:create(Binding1, fun(_, _) -> ok end)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding2, fun(_, _) -> ok end)),

    {ok, KWCS1} = khepri_machine:get_keep_while_conds_state(
                    rabbit_khepri:get_store_id(), #{}),
    ?assertNotMatch(#{XPath1 := _}, KWCS1),
    ?assertNotMatch(#{XPath2 := _}, KWCS1),
    ?assertNotMatch(#{BPath1 := _}, KWCS1),
    ?assertNotMatch(#{BPath2 := _}, KWCS1),

    Deletions1 = rabbit_db_queue:delete(QName, false),
    ?assertMatch(
       #{XName1 := _,
         XName2 := _},
       Deletions1),

    ?assertNot(rabbit_khepri:exists(QPath)),
    ?assertNot(rabbit_khepri:exists(XPath1)),
    ?assert(rabbit_khepri:exists(XPath2)),
    ?assertNot(rabbit_khepri:exists(BPath1)),
    ?assertNot(rabbit_khepri:exists(BPath2)),

    %% 2. Scenario with the feature flag enabled in the process.

    ?assertMatch(
       {new, #exchange{}},
       rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch(
       {existing, #exchange{}},
       rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch({created, Queue}, rabbit_db_queue:create_or_get(Queue)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding1, fun(_, _) -> ok end)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding2, fun(_, _) -> ok end)),

    ?assertEqual(
       ok,
       rabbit_feature_flags:enable(
         tie_binding_to_dest_with_keep_while_cond)),

    {ok, KWCS2} = khepri_machine:get_keep_while_conds_state(
                    rabbit_khepri:get_store_id(), #{}),
    ?assertMatch(#{XPath1 := _}, KWCS2),
    ?assertNotMatch(#{XPath2 := _}, KWCS2),
    ?assertMatch(#{BPath1 := _}, KWCS2),
    ?assertMatch(#{BPath2 := _}, KWCS2),

    Deletions2 = rabbit_db_queue:delete(QName, false),
    ?assertEqual(Deletions1, Deletions2),

    ?assertNot(rabbit_khepri:exists(QPath)),
    ?assertNot(rabbit_khepri:exists(XPath1)),
    ?assert(rabbit_khepri:exists(XPath2)),
    ?assertNot(rabbit_khepri:exists(BPath1)),
    ?assertNot(rabbit_khepri:exists(BPath2)),

    %% 3. Scenario with the feature flag already enabled.
    ?assert(
       rabbit_feature_flags:is_enabled(tie_binding_to_dest_with_keep_while_cond)),

    ?assertMatch(
       {new, #exchange{}},
       rabbit_db_exchange:create_or_get(Exchange1)),
    ?assertMatch(
       {existing, #exchange{}},
       rabbit_db_exchange:create_or_get(Exchange2)),
    ?assertMatch({created, Queue}, rabbit_db_queue:create_or_get(Queue)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding1, fun(_, _) -> ok end)),
    ?assertMatch(ok, rabbit_db_binding:create(Binding2, fun(_, _) -> ok end)),

    {ok, KWCS3} = khepri_machine:get_keep_while_conds_state(
                    rabbit_khepri:get_store_id(), #{}),
    ?assertMatch(#{XPath1 := _}, KWCS3),
    ?assertNotMatch(#{XPath2 := _}, KWCS3),
    ?assertMatch(#{BPath1 := _}, KWCS3),
    ?assertMatch(#{BPath2 := _}, KWCS3),

    Deletions3 = rabbit_db_queue:delete(QName, false),
    ?assertEqual(Deletions1, Deletions3),

    ?assertNot(rabbit_khepri:exists(QPath)),
    ?assertNot(rabbit_khepri:exists(XPath1)),
    ?assert(rabbit_khepri:exists(XPath2)),
    ?assertNot(rabbit_khepri:exists(BPath1)),
    ?assertNot(rabbit_khepri:exists(BPath2)),

    passed.
