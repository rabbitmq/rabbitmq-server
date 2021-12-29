%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_connection_tracking_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          exchange_count,
          queue_count,
          connection_count,
          connection_lookup
        ]}
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
    rabbit_ct_helpers:testcase_finished(Config, Testcase).


%% ---------------------------------------------------------------------------
%% Count functions for management only API purposes
%% ---------------------------------------------------------------------------

exchange_count(Config) ->
    %% Default exchanges == 7
    ?assertEqual(7, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, count, [])).

queue_count(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:call(Ch, #'queue.declare'{ queue = <<"my-queue">> }),

    ?assertEqual(1, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, count, [])),

    amqp_channel:call(Ch, #'queue.delete'{ queue = <<"my-queue">> }),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

%% connection_count/1 has been failing on Travis. This seems a legit failure, as the registering
%% of connections in the tracker is async. `rabbit_connection_tracking_handler` receives a rabbit
%% event with `connection_created`, which then forwards as a cast to `rabbit_connection_tracker`
%% for register. We should wait a reasonable amount of time for the counter to increase before
%% failing.
connection_count(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),

    rabbit_ct_helpers:await_condition(
      fun() ->
              rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, count, []) == 1
      end, 30000),

    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

connection_lookup(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),

    %% Let's wait until the connection is registered, otherwise this test could fail in a slow
    %% machine as connection tracking is asynchronous
    rabbit_ct_helpers:await_condition(
      fun() ->
              rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, count, []) == 1
      end, 30000),

    [Connection] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, list, []),
    ?assertMatch(Connection, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking,
                                                          lookup,
                                                          [Connection#tracked_connection.name])),

    rabbit_ct_client_helpers:close_connection(Conn),
    ok.
