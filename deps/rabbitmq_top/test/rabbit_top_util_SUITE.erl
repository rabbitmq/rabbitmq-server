%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_top_util_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     {group, unit_tests}
    ].

groups() ->
    [
     {unit_tests, [parallel],
      [
       sort_by_param_known_process_keys,
       sort_by_param_known_ets_table_keys,
       sort_by_param_missing_uses_default,
       sort_by_param_unknown_uses_default,
       sort_by_param_unknown_does_not_create_atoms
      ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, _Config) ->
    ok.

%%
%% Tests
%%

sort_by_param_known_process_keys(_Config) ->
    KnownKeys = [
        {<<"reduction_delta">>,   reduction_delta},
        {<<"memory">>,            memory},
        {<<"message_queue_len">>, message_queue_len},
        {<<"reductions">>,        reductions},
        {<<"status">>,            status},
        {<<"buffer_len">>,        buffer_len},
        {<<"pid">>,               pid}
    ],
    [?assertEqual(Atom, rabbit_top_util:sort_by_param(req(Bin), some_default))
     || {Bin, Atom} <- KnownKeys].

sort_by_param_known_ets_table_keys(_Config) ->
    KnownKeys = [
        {<<"size">>,                  size},
        {<<"name">>,                  name},
        {<<"owner">>,                 owner},
        {<<"owner_name">>,            owner_name},
        {<<"type">>,                  type},
        {<<"protection">>,            protection},
        {<<"node">>,                  node},
        {<<"keypos">>,                keypos},
        {<<"compressed">>,            compressed},
        {<<"named_table">>,           named_table},
        {<<"write_concurrency">>,     write_concurrency},
        {<<"read_concurrency">>,      read_concurrency},
        {<<"decentralized_counters">>, decentralized_counters}
    ],
    [?assertEqual(Atom, rabbit_top_util:sort_by_param(req(Bin), some_default))
     || {Bin, Atom} <- KnownKeys].

sort_by_param_missing_uses_default(_Config) ->
    ?assertEqual(my_default, rabbit_top_util:sort_by_param(#{qs => <<>>}, my_default)).

sort_by_param_unknown_uses_default(_Config) ->
    ?assertEqual(my_default, rabbit_top_util:sort_by_param(req(<<"unknown_column">>), my_default)).

sort_by_param_unknown_does_not_create_atoms(_Config) ->
    Key = <<"nonexistent_sort_key_for_rabbitmq_top_12345">>,
    %% Confirm the atom does not exist before the call.
    ?assertError(badarg, binary_to_existing_atom(Key, utf8)),
    %% An unknown sort key must not create a new atom.
    _ = rabbit_top_util:sort_by_param(req(Key), reduction_delta),
    %% The atom must still not exist after the call.
    ?assertError(badarg, binary_to_existing_atom(Key, utf8)).

%%
%% Helpers
%%

req(SortValue) ->
    #{qs => <<"sort=", SortValue/binary>>}.
