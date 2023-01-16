%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_policy_SUITE).

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
     update
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
