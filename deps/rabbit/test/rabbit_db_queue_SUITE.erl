%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_queue_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("amqqueue.hrl").

-compile(export_all).

-define(VHOST, <<"/">>).

all() ->
    [
     {group, all_tests},
     {group, mnesia_store}
    ].

groups() ->
    [
     {all_tests, [], all_tests()},
     {mnesia_store, [], mnesia_tests()}
    ].

all_tests() ->
    [
     create_or_get,
     get,
     get_many,
     get_all,
     get_all_by_vhost,
     get_all_by_type,
     get_all_by_type_and_node,
     list,
     count,
     count_by_vhost,
     set,
     set_many,
     delete,
     update,
     exists,
     get_all_durable,
     get_all_durable_by_type,
     filter_all_durable,
     get_durable,
     get_many_durable,
     update_durable,
     foreach_durable,
     internal_delete
    ].

mnesia_tests() ->
    [
     set_dirty,
     foreach_transient,
     delete_transient,
     update_in_mnesia_tx,
     get_durable_in_mnesia_tx
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

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
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_queue, clear, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

create_or_get(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, create_or_get1, [Config]).

create_or_get1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    ?assertEqual({created, Q}, rabbit_db_queue:create_or_get(Q)),
    ?assertEqual({existing, Q}, rabbit_db_queue:create_or_get(Q)),
    %% TODO absent
    passed.

get(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get1, [Config]).

get1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    ok = rabbit_db_queue:set(Q),
    ?assertEqual({ok, Q}, rabbit_db_queue:get(QName)),
    ?assertEqual({error, not_found},
                 rabbit_db_queue:get(rabbit_misc:r(?VHOST, queue, <<"test-queue2">>))),
    passed.

get_many(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_many1, [Config]).

get_many1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    ok = rabbit_db_queue:set(Q),
    ?assertEqual([Q], rabbit_db_queue:get_many([QName])),
    ?assertEqual([Q], rabbit_db_queue:get_many([QName, QName2])),
    ?assertEqual([], rabbit_db_queue:get_many([QName2])),
    ok = rabbit_db_queue:set(Q2),
    ?assertEqual(lists:sort([Q, Q2]), lists:sort(rabbit_db_queue:get_many([QName, QName2]))),
    passed.

get_all(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all1, [Config]).

get_all1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    All = lists:sort([Q, Q2]),
    ?assertEqual([], rabbit_db_queue:get_all()),
    set_list([Q, Q2]),
    ?assertEqual(All, lists:sort(rabbit_db_queue:get_all())),
    passed.

get_all_by_vhost(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all_by_vhost1, [Config]).

get_all_by_vhost1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    All = lists:sort([Q, Q2]),
    ?assertEqual([], rabbit_db_queue:get_all(?VHOST)),
    ?assertEqual([], rabbit_db_queue:get_all(<<"some-vhost">>)),
    set_list([Q, Q2]),
    ?assertEqual(All, lists:sort(rabbit_db_queue:get_all(?VHOST))),
    ?assertEqual([], rabbit_db_queue:get_all(<<"some-vhost">>)),
    passed.

get_all_by_type(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all_by_type1, [Config]).

get_all_by_type1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    QName3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    QName4 = rabbit_misc:r(?VHOST, queue, <<"test-queue4">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_quorum_queue),
    Q3 = new_queue(QName3, rabbit_quorum_queue),
    Q4 = new_queue(QName4, rabbit_stream_queue),
    Quorum = lists:sort([Q2, Q3]),
    ?assertEqual([], rabbit_db_queue:get_all_by_type(rabbit_classic_queue)),
    ?assertEqual([], lists:sort(rabbit_db_queue:get_all_by_type(rabbit_quorum_queue))),
    ?assertEqual([], rabbit_db_queue:get_all_by_type(rabbit_stream_queue)),
    set_list([Q, Q2, Q3, Q4]),
    ?assertEqual([Q], rabbit_db_queue:get_all_by_type(rabbit_classic_queue)),
    ?assertEqual(Quorum, lists:sort(rabbit_db_queue:get_all_by_type(rabbit_quorum_queue))),
    ?assertEqual([Q4], rabbit_db_queue:get_all_by_type(rabbit_stream_queue)),
    passed.

get_all_by_type_and_node(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all_by_type_and_node1, [Config]).

get_all_by_type_and_node1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    QName3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    QName4 = rabbit_misc:r(?VHOST, queue, <<"test-queue4">>),
    Pid = spawn(fun() -> ok end),
    Q = new_queue(QName, rabbit_classic_queue, Pid),
    Q2 = new_queue(QName2, rabbit_quorum_queue),
    Q3 = new_queue(QName3, rabbit_quorum_queue, Pid),
    Q4 = new_queue(QName4, rabbit_stream_queue, Pid),
    Node = node(),
    ?assertEqual([], rabbit_db_queue:get_all_by_type_and_node(?VHOST, rabbit_classic_queue, Node)),
    ?assertEqual([], lists:sort(rabbit_db_queue:get_all_by_type_and_node(?VHOST, rabbit_quorum_queue, Node))),
    ?assertEqual([], rabbit_db_queue:get_all_by_type_and_node(?VHOST, rabbit_stream_queue, Node)),
    set_list([Q, Q2, Q3, Q4]),
    ?assertEqual([Q], rabbit_db_queue:get_all_by_type_and_node(?VHOST, rabbit_classic_queue, Node)),
    ?assertEqual([], rabbit_db_queue:get_all_by_type_and_node(<<"other-vhost">>, rabbit_classic_queue, Node)),
    ?assertEqual([Q3], lists:sort(rabbit_db_queue:get_all_by_type_and_node(?VHOST, rabbit_quorum_queue, Node))),
    ?assertEqual([Q4], rabbit_db_queue:get_all_by_type_and_node(?VHOST, rabbit_stream_queue, Node)),
    passed.

list(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, list1, [Config]).

list1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    All = lists:sort([QName, QName2]),
    ?assertEqual([], rabbit_db_queue:list()),
    set_list([Q, Q2]),
    ?assertEqual(All, lists:sort(rabbit_db_queue:list())),
    passed.

count(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, count1, [Config]).

count1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    QName3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    QName4 = rabbit_misc:r(?VHOST, queue, <<"test-queue4">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_quorum_queue),
    Q3 = new_queue(QName3, rabbit_quorum_queue),
    Q4 = new_queue(QName4, rabbit_stream_queue),
    ?assertEqual(0, rabbit_db_queue:count()),
    set_list([Q, Q2, Q3, Q4]),
    ?assertEqual(4, rabbit_db_queue:count()),
    passed.

count_by_vhost(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, count_by_vhost1, [Config]).

count_by_vhost1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    QName3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    QName4 = rabbit_misc:r(?VHOST, queue, <<"test-queue4">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_quorum_queue),
    Q3 = new_queue(QName3, rabbit_quorum_queue),
    Q4 = new_queue(QName4, rabbit_stream_queue),
    ?assertEqual(0, rabbit_db_queue:count(?VHOST)),
    set_list([Q, Q2, Q3, Q4]),
    ?assertEqual(4, rabbit_db_queue:count(?VHOST)),
    ?assertEqual(0, rabbit_db_queue:count(<<"other-vhost">>)),
    passed.

set(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set1, [Config]).

set1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    ?assertEqual({ok, Q}, rabbit_db_queue:get(QName)),
    passed.

set_many(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set_many1, [Config]).

set_many1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    QName3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    Q3 = new_queue(QName3, rabbit_classic_queue),
    ?assertEqual(ok, rabbit_db_queue:set_many([])),
    ?assertEqual(ok, rabbit_db_queue:set_many([Q1, Q2, Q3])),
    ?assertEqual({ok, Q1}, rabbit_db_queue:get_durable(QName1)),
    ?assertEqual({ok, Q2}, rabbit_db_queue:get_durable(QName2)),
    ?assertEqual({ok, Q3}, rabbit_db_queue:get_durable(QName3)),
    passed.

delete(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config]).

delete1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    ?assertEqual({ok, Q}, rabbit_db_queue:get(QName)),
    %% TODO Can we handle the deletions outside of rabbit_db_queue? Probably not because
    %% they should be done in a single transaction, but what a horrid API to have!
    Dict = rabbit_db_queue:delete(QName, normal),
    ?assertEqual(0, dict:size(Dict)),
    ?assertEqual(ok, rabbit_db_queue:delete(QName, normal)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get(QName)),
    passed.

update(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, update1, [Config]).

update1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Pid = spawn(fun() -> ok end),
    Q2 = amqqueue:set_pid(Q, Pid),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    ?assertEqual({ok, Q}, rabbit_db_queue:get(QName)),
    ?assertEqual(Q2, rabbit_db_queue:update(QName, fun(_) -> Q2 end)),
    ?assertEqual({ok, Q2}, rabbit_db_queue:get(QName)),
    ?assertEqual(not_found, rabbit_db_queue:update(QName2, fun(_) -> Q2 end)),
    passed.

update_decorators(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, update_decorators1, [Config]).

update_decorators1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    ?assertEqual({ok, Q}, rabbit_db_queue:get(QName)),
    ?assertEqual(undefined, amqqueue:get_decorators(Q)),
    %% Not really testing we set a decorator, but at least the field is being updated
    ?assertEqual(ok, rabbit_db_queue:update_decorators(QName)),
    {ok, Q1} = rabbit_db_queue:get(QName),
    ?assertEqual([], amqqueue:get_decorators(Q1)),
    passed.

exists(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, exists1, [Config]).

exists1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    ?assertEqual(false, rabbit_db_queue:exists(QName)),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    ?assertEqual(true, rabbit_db_queue:exists(QName)),
    passed.

get_all_durable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all_durable1, [Config]).

get_all_durable1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    QName3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    Q3 = new_queue(QName3, rabbit_classic_queue),
    All = lists:sort([Q1, Q2, Q3]),
    ?assertEqual([], rabbit_db_queue:get_all_durable()),
    set_list(All),
    ?assertEqual(All, lists:sort(rabbit_db_queue:get_all_durable())),
    passed.

get_all_durable_by_type(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all_durable_by_type1, [Config]).

get_all_durable_by_type1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    QName3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    QName4 = rabbit_misc:r(?VHOST, queue, <<"test-queue4">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_quorum_queue),
    Q3 = new_queue(QName3, rabbit_stream_queue),
    Q4 = new_queue(QName4, rabbit_classic_queue),
    All = lists:sort([Q1, Q2, Q3]),
    ok = rabbit_db_queue:set_dirty(Q4),
    ?assertEqual([], rabbit_db_queue:get_all_durable_by_type(rabbit_classic_queue)),
    set_list(All),
    ?assertEqual([Q1], rabbit_db_queue:get_all_durable_by_type(rabbit_classic_queue)),
    ?assertEqual([Q2], rabbit_db_queue:get_all_durable_by_type(rabbit_quorum_queue)),
    ?assertEqual([Q3], rabbit_db_queue:get_all_durable_by_type(rabbit_stream_queue)),
    passed.

filter_all_durable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, filter_all_durable1, [Config]).

filter_all_durable1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    QName3 = rabbit_misc:r(?VHOST, queue, <<"test-queue3">>),
    Q1 = new_queue(QName1, rabbit_quorum_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    Q3 = new_queue(QName3, rabbit_classic_queue),
    All = lists:sort([Q2, Q3]),
    ?assertEqual([], rabbit_db_queue:filter_all_durable(
                       fun(Q) ->
                               amqqueue:get_type(Q) =:= rabbit_classic_queue
                       end)),
    set_list([Q1, Q2, Q3]),
    ?assertEqual(All, lists:sort(rabbit_db_queue:filter_all_durable(
                                   fun(Q) ->
                                           amqqueue:get_type(Q) =:= rabbit_classic_queue
                                   end))),
    passed.

get_durable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_durable1, [Config]).

get_durable1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    ok = rabbit_db_queue:set(Q1),
    ok = rabbit_db_queue:set_dirty(Q2),
    ?assertEqual({ok, Q1}, rabbit_db_queue:get_durable(QName1)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get_durable(QName2)),
    passed.

get_many_durable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_many_durable1, [Config]).

get_many_durable1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    ok = rabbit_db_queue:set(Q1),
    ok = rabbit_db_queue:set_dirty(Q2),
    ?assertEqual([Q1], rabbit_db_queue:get_many_durable([QName1])),
    ?assertEqual([], rabbit_db_queue:get_many_durable([QName2])),
    passed.

update_durable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, update_durable1, [Config]).

update_durable1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    ?assertEqual(ok, rabbit_db_queue:set(Q1)),
    ?assertEqual(ok, rabbit_db_queue:set_dirty(Q2)),
    ?assertEqual(ok, rabbit_db_queue:update_durable(
                       fun(Q0) ->
                               amqqueue:set_policy(Q0, my_policy)
                       end,
                       fun(Q0) when ?is_amqqueue(Q0) -> true end)),
    {ok, Q0} = rabbit_db_queue:get_durable(QName1),
    ?assertMatch(my_policy, amqqueue:get_policy(Q0)),
    passed.

foreach_durable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, foreach_durable1, [Config]).

foreach_durable1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    ?assertEqual(ok, rabbit_db_queue:set(Q1)),
    ?assertEqual(ok, rabbit_db_queue:foreach_durable(
                       fun(Q0) ->
                               rabbit_db_queue:internal_delete(amqqueue:get_name(Q0), true, normal)
                       end,
                       fun(Q0) when ?is_amqqueue(Q0) -> true end)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get(QName1)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get_durable(QName1)),
    passed.

foreach_transient(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, foreach_transient1, [Config]).

foreach_transient1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_classic_queue),
    ?assertEqual(ok, rabbit_db_queue:set(Q1)),
    ?assertEqual(ok, rabbit_db_queue:set_dirty(Q2)),
    ?assertEqual(ok, rabbit_db_queue:foreach_transient(
                       fun(Q0) ->
                               rabbit_db_queue:internal_delete(amqqueue:get_name(Q0), true, normal)
                       end)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get(QName1)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get_durable(QName1)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get(QName2)),
    passed.

delete_transient(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_transient1, [Config]).

delete_transient1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue1">>),
    QName2 = rabbit_misc:r(?VHOST, queue, <<"test-queue2">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = new_queue(QName2, rabbit_quorum_queue),
    ?assertEqual(ok, rabbit_db_queue:set_dirty(Q1)),
    ?assertEqual(ok, rabbit_db_queue:set_dirty(Q2)),
    ?assertMatch({[QName1], _},
                 rabbit_db_queue:delete_transient(
                   fun(Q0) when ?is_amqqueue(Q0) ->
                           amqqueue:get_type(Q0) == rabbit_classic_queue
                   end)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get(QName1)),
    ?assertMatch({ok, _}, rabbit_db_queue:get(QName2)),
    passed.

set_dirty(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set_dirty1, [Config]).

set_dirty1(_Config) ->
    QName1 = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q1 = new_queue(QName1, rabbit_classic_queue),
    Q2 = amqqueue:set_decorators(Q1, []),
    ok = rabbit_db_queue:set_dirty(Q1),
    ?assertEqual({ok, Q2}, rabbit_db_queue:get(QName1)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get_durable(QName1)),
    passed.

internal_delete(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, internal_delete1, [Config]).

internal_delete1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    ?assertEqual(ok, rabbit_db_queue:foreach_durable(
                       fun(Q0) -> rabbit_db_queue:internal_delete(amqqueue:get_name(Q0),
                                                                  false, normal) end,
                       fun(Q0) when ?is_amqqueue(Q0) -> true end)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get(QName)),
    ?assertEqual({error, not_found}, rabbit_db_queue:get_durable(QName)),
    passed.

update_in_mnesia_tx(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, update_in_mnesia_tx1, [Config]).

update_in_mnesia_tx1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    Pid = spawn(fun() -> ok end),
    ?assertEqual({atomic, not_found},
                 mnesia:transaction(fun() ->
                                            rabbit_db_queue:update_in_mnesia_tx(
                                              QName,
                                              fun(Q0) -> amqqueue:set_pid(Q0, Pid) end)
                                    end)),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    {atomic, Q1} =
        mnesia:transaction(fun() ->
                                   rabbit_db_queue:update_in_mnesia_tx(
                                     QName,
                                     fun(Q0) -> amqqueue:set_pid(Q0, Pid) end)
                           end),
    ?assertEqual(Pid, amqqueue:get_pid(Q1)),
    ?assertEqual({ok, Q1}, rabbit_db_queue:get(QName)),
    ?assertEqual({ok, Q1}, rabbit_db_queue:get_durable(QName)),
    passed.

get_durable_in_mnesia_tx(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_durable_in_mnesia_tx1, [Config]).

get_durable_in_mnesia_tx1(_Config) ->
    QName = rabbit_misc:r(?VHOST, queue, <<"test-queue">>),
    Q = new_queue(QName, rabbit_classic_queue),
    ?assertEqual({atomic, {error, not_found}},
                 mnesia:transaction(fun() ->
                                            rabbit_db_queue:get_durable_in_mnesia_tx(QName)
                                    end)),
    ?assertEqual(ok, rabbit_db_queue:set(Q)),
    ?assertEqual({atomic, {ok, Q}},
                 mnesia:transaction(fun() ->
                                            rabbit_db_queue:get_durable_in_mnesia_tx(QName)
                                    end)),
    passed.

set_list(Qs) ->
    [?assertEqual(ok, rabbit_db_queue:set(Q)) || Q <- Qs].

new_queue(QName, Type) ->
    new_queue(QName, Type, none).

new_queue(#resource{virtual_host = VHost} = QName, Type, Pid) ->
    amqqueue:new(QName, Pid, true, false, none, [], VHost, #{}, Type).
