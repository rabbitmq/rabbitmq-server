%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_exchange_SUITE).

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
     create_or_get,
     get,
     get_many,
     get_all,
     get_all_by_vhost,
     get_all_durable,
     list,
     count,
     update,
     set,
     peek_serial,
     next_serial,
     delete_serial,
     delete,
     delete_if_unused,
     exists,
     match,
     recover
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
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_exchange, clear, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_binding, clear, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

create_or_get(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, create_or_get1, [Config]).

create_or_get1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange0 = #exchange{name = XName, durable = true},
    Exchange = rabbit_exchange_decorator:set(Exchange0),
    ?assertMatch({new, Exchange}, rabbit_db_exchange:create_or_get(Exchange0)),
    ?assertEqual({existing, Exchange}, rabbit_db_exchange:create_or_get(Exchange0)),
    passed.

get(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get1, [Config]).

get1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange0 = #exchange{name = XName, durable = true},
    Exchange = rabbit_exchange_decorator:set(Exchange0),
    ?assertEqual({error, not_found}, rabbit_db_exchange:get(XName)),
    ?assertEqual({new, Exchange}, rabbit_db_exchange:create_or_get(Exchange0)),
    ?assertEqual({ok, Exchange}, rabbit_db_exchange:get(XName)),
    passed.

get_many(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_many1, [Config]).

get_many1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange0 = #exchange{name = XName, durable = true},
    Exchange = rabbit_exchange_decorator:set(Exchange0),
    ?assertEqual([], rabbit_db_exchange:get_many([XName])),
    ?assertEqual({new, Exchange}, rabbit_db_exchange:create_or_get(Exchange0)),
    ?assertEqual([Exchange], rabbit_db_exchange:get_many([XName])),
    passed.

get_all(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all1, [Config]).

get_all1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1_0 = #exchange{name = XName1, durable = true},
    Exchange2_0 = #exchange{name = XName2, durable = true},
    Exchange1 = rabbit_exchange_decorator:set(Exchange1_0),
    Exchange2 = rabbit_exchange_decorator:set(Exchange2_0),
    All = lists:sort([Exchange1, Exchange2]),
    ?assertEqual([], rabbit_db_exchange:get_all()),
    create([Exchange1_0, Exchange2_0]),
    ?assertEqual(All, lists:sort(rabbit_db_exchange:get_all())),
    passed.

get_all_by_vhost(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all_by_vhost1, [Config]).

get_all_by_vhost1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1_0 = #exchange{name = XName1, durable = true},
    Exchange2_0 = #exchange{name = XName2, durable = true},
    Exchange1 = rabbit_exchange_decorator:set(Exchange1_0),
    Exchange2 = rabbit_exchange_decorator:set(Exchange2_0),
    All = lists:sort([Exchange1, Exchange2]),
    ?assertEqual([], rabbit_db_exchange:get_all(?VHOST)),
    create([Exchange1_0, Exchange2_0]),
    ?assertEqual(All, lists:sort(rabbit_db_exchange:get_all(?VHOST))),
    ?assertEqual([], lists:sort(rabbit_db_exchange:get_all(<<"other-vhost">>))),
    passed.

get_all_durable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, get_all_durable1, [Config]).

get_all_durable1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1_0 = #exchange{name = XName1, durable = true},
    Exchange2_0 = #exchange{name = XName2, durable = true},
    All = lists:sort([Exchange1_0, Exchange2_0]),
    ?assertEqual([], rabbit_db_exchange:get_all_durable()),
    create([Exchange1_0, Exchange2_0]),
    ?assertEqual(All, lists:sort(rabbit_db_exchange:get_all_durable())),
    passed.

list(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, list1, [Config]).

list1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1_0 = #exchange{name = XName1, durable = true},
    Exchange2_0 = #exchange{name = XName2, durable = true},
    All = lists:sort([XName1, XName2]),
    ?assertEqual([], rabbit_db_exchange:list()),
    create([Exchange1_0, Exchange2_0]),
    ?assertEqual(All, lists:sort(rabbit_db_exchange:list())),
    passed.

count(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, count1, [Config]).

count1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1_0 = #exchange{name = XName1, durable = true},
    Exchange2_0 = #exchange{name = XName2, durable = true},
    ?assertEqual(0, rabbit_db_exchange:count()),
    create([Exchange1_0, Exchange2_0]),
    ?assertEqual(2, rabbit_db_exchange:count()),
    passed.

update(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, update1, [Config]).

update1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange = #exchange{name = XName, durable = true},
    ?assertEqual(ok,
                 rabbit_db_exchange:update(XName, fun(X) -> X#exchange{type = topic} end)),
    create([Exchange]),
    ?assertEqual(ok,
                 rabbit_db_exchange:update(XName, fun(X) -> X#exchange{type = topic} end)),
    {ok, Exchange0} = rabbit_db_exchange:get(XName),
    ?assertEqual(topic, Exchange0#exchange.type),
    passed.

set(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set1, [Config]).

set1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange = #exchange{name = XName, durable = true},
    ?assertEqual(ok, rabbit_db_exchange:set([Exchange])),
    ?assertEqual({error, not_found}, rabbit_db_exchange:get(XName)),
    ?assertEqual([Exchange], rabbit_db_exchange:get_all_durable()),
    passed.

peek_serial(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, peek_serial1, [Config]).

peek_serial1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    ?assertEqual(1, rabbit_db_exchange:peek_serial(XName)),
    ?assertEqual(1, rabbit_db_exchange:peek_serial(XName)),
    ?assertEqual(1, rabbit_db_exchange:next_serial(XName)),
    ?assertEqual(2, rabbit_db_exchange:peek_serial(XName)),
    ?assertEqual(2, rabbit_db_exchange:peek_serial(XName)),
    passed.

next_serial(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, next_serial1, [Config]).

next_serial1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    ?assertEqual(1, rabbit_db_exchange:next_serial(XName)),
    ?assertEqual(2, rabbit_db_exchange:next_serial(XName)),
    ?assertEqual(3, rabbit_db_exchange:next_serial(XName)),
    passed.

delete_serial(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_serial1, [Config]).

delete_serial1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    ?assertEqual(1, rabbit_db_exchange:next_serial(XName)),
    ?assertEqual(2, rabbit_db_exchange:next_serial(XName)),
    ?assertEqual(ok, rabbit_db_exchange:delete_serial(XName)),
    ?assertEqual(1, rabbit_db_exchange:peek_serial(XName)),
    passed.

delete(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config]).

delete1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange0 = #exchange{name = XName, durable = true},
    ?assertMatch({error, not_found}, rabbit_db_exchange:delete(XName, false)),
    create([Exchange0]),
    ?assertMatch({ok, #exchange{name = XName}}, rabbit_db_exchange:get(XName)),
    ?assertMatch([#exchange{name = XName}], rabbit_db_exchange:get_all_durable()),
    ?assertMatch({deleted, #exchange{name = XName}, [], _},
                 rabbit_db_exchange:delete(XName, false)),
    ?assertEqual({error, not_found}, rabbit_db_exchange:get(XName)),
    ?assertEqual([], rabbit_db_exchange:get_all_durable()),
    passed.

delete_if_unused(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_if_unused1, [Config]).

delete_if_unused1(_Config) ->
    XName1 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange1">>),
    XName2 = rabbit_misc:r(?VHOST, exchange, <<"test-exchange2">>),
    Exchange1 = #exchange{name = XName1, durable = true},
    Exchange2 = #exchange{name = XName2, durable = true},
    Binding = #binding{source = XName1, key = <<"">>, destination = XName2, args = #{}},
    ?assertMatch({error, not_found}, rabbit_db_exchange:delete(XName1, true)),
    create([Exchange1, Exchange2]),
    ?assertEqual(ok, rabbit_db_binding:create(Binding, fun(_, _) -> ok end)),
    ?assertMatch({ok, #exchange{name = XName1}}, rabbit_db_exchange:get(XName1)),
    ?assertMatch([#exchange{}, #exchange{}], rabbit_db_exchange:get_all_durable()),
    ?assertMatch({error, in_use}, rabbit_db_exchange:delete(XName1, true)),
    ?assertMatch({ok, #exchange{name = XName1}}, rabbit_db_exchange:get(XName1)),
    ?assertMatch([#exchange{}, #exchange{}], rabbit_db_exchange:get_all_durable()),
    passed.

exists(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, exists1, [Config]).

exists1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange = #exchange{name = XName, durable = true},
    ?assertEqual(false, rabbit_db_exchange:exists(XName)),
    create([Exchange]),
    ?assertEqual(true, rabbit_db_exchange:exists(XName)),
    passed.

match(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, match1, [Config]).

match1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange = #exchange{name = XName, durable = true, type = topic},
    Pattern = #exchange{durable = true, type = topic, _ = '_'},
    ?assertEqual([], rabbit_db_exchange:match(Pattern)),
    create([Exchange]),
    ?assertMatch([#exchange{name = XName}], rabbit_db_exchange:match(Pattern)),
    passed.

recover(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, recover1, [Config]).

recover1(_Config) ->
    XName = rabbit_misc:r(?VHOST, exchange, <<"test-exchange">>),
    Exchange = #exchange{name = XName, durable = true},
    ?assertEqual(ok, rabbit_db_exchange:set([Exchange])),
    ?assertEqual({error, not_found}, rabbit_db_exchange:get(XName)),
    ?assertEqual([Exchange], rabbit_db_exchange:get_all_durable()),
    ?assertMatch([Exchange], rabbit_db_exchange:recover(?VHOST)),
    ?assertMatch({ok, #exchange{name = XName}}, rabbit_db_exchange:get(XName)),
    ?assertEqual([Exchange], rabbit_db_exchange:get_all_durable()),
    passed.

create(Exchanges) ->
    [?assertMatch({new, #exchange{}}, rabbit_db_exchange:create_or_get(Exchange))
     || Exchange <- Exchanges].
