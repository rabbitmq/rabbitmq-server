%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_msup_SUITE).

-include_lib("rabbit_common/include/rabbit.hrl").
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
     create_tables,
     create_or_update,
     find_mirror,
     delete,
     delete_all,
     update_all
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
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_db_msup, clear, []),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% ---------------------------------------------------------------------------
%% Test Cases
%% ---------------------------------------------------------------------------

create_tables(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, create_tables1, [Config]).

create_tables1(_Config) ->
    ?assertEqual(ok, rabbit_db_msup:create_tables()),
    passed.

create_or_update(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, create_or_update1, [Config]).

create_or_update1(_Config) ->
    Overall = spawn(fun() -> ok end),
    Spec = #{id => id, start => {m, f, args}},
    ?assertEqual(start,
                 rabbit_db_msup:create_or_update(group, Overall, undefined, Spec, id)),
    passed.

find_mirror(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, find_mirror1, [Config]).

find_mirror1(_Config) ->
    Overall = spawn(fun() -> ok end),
    Spec = #{id => id, start => {m, f, args}},
    ?assertEqual(start, rabbit_db_msup:create_or_update(group, Overall, undefined,
                                                        Spec, id)),
    ?assertEqual({ok, Overall}, rabbit_db_msup:find_mirror(group, id)),
    passed.

delete(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete1, [Config]).

delete1(_Config) ->
    Overall = spawn(fun() -> ok end),
    Spec = #{id => id, start => {m, f, args}},
    ?assertEqual(start, rabbit_db_msup:create_or_update(group, Overall, undefined,
                                                        Spec, id)),
    ?assertEqual(ok, rabbit_db_msup:delete(group, id)),
    ?assertEqual({error, not_found}, rabbit_db_msup:find_mirror(group, id)),
    passed.

delete_all(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_all1, [Config]).

delete_all1(_Config) ->
    Overall = spawn(fun() -> ok end),
    Spec = #{id => id, start => {m, f, args}},
    ?assertEqual(start, rabbit_db_msup:create_or_update(group, Overall, undefined,
                                                        Spec, id)),
    ?assertEqual(ok, rabbit_db_msup:delete_all(group)),
    ?assertEqual({error, not_found}, rabbit_db_msup:find_mirror(group, id)),
    passed.

update_all(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, update_all1, [Config]).

update_all1(_Config) ->
    OldOverall = spawn(fun() -> ok end),
    Overall = spawn(fun() -> ok end),
    Spec = #{id => id, start => {m, f, args}},
    ?assertEqual(start, rabbit_db_msup:create_or_update(group, OldOverall, undefined,
                                                        Spec, id)),
    ?assertEqual({ok, OldOverall}, rabbit_db_msup:find_mirror(group, id)),
    ?assertEqual([Spec], rabbit_db_msup:update_all(Overall, OldOverall)),
    ?assertEqual({ok, Overall}, rabbit_db_msup:find_mirror(group, id)),
    passed.
