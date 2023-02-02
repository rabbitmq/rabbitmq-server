%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_maintenance_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

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
     setup_schema,
     set_and_get,
     set_and_get_consistent
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
%% Test Cases
%% ---------------------------------------------------------------------------

setup_schema(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, setup_schema1, [Config]).

setup_schema1(_Config) ->
    ?assertEqual(ok, rabbit_db_maintenance:setup_schema()),
    passed.

set_and_get(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, set_and_get1, [Config]).

set_and_get1(_Config) ->
    ?assertEqual(true, rabbit_db_maintenance:set(ready)),
    ?assertEqual(ready, rabbit_db_maintenance:get(node())),
    ?assertEqual(undefined, rabbit_db_maintenance:get('another-node')),
    passed.

set_and_get_consistent(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(
               Config, 0, ?MODULE, set_and_get_consistent1, [Config]).

set_and_get_consistent1(_Config) ->
    ?assertEqual(true, rabbit_db_maintenance:set(ready)),
    ?assertEqual(ready, rabbit_db_maintenance:get_consistent(node())),
    ?assertEqual(undefined, rabbit_db_maintenance:get_consistent('another-node')),
    passed.
