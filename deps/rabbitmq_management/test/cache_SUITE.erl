%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(cache_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-compile(export_all).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               name,
                               fetch,
                               fetch_cached,
                               fetch_stale,
                               fetch_stale_after_expiry,
                               fetch_throws,
                               fetch_cached_with_same_args,
                               fetch_cached_with_different_args_invalidates_cache
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) -> Config.

end_per_group(_, Config) -> Config.

init_per_testcase(_Testcase, Config) ->
    {ok, P} = rabbit_mgmt_db_cache:start_link(banana),
    rabbit_ct_helpers:set_config(Config, {sut, P}).

end_per_testcase(_Testcase, Config) ->
    P = ?config(sut, Config),
    _ = gen_server:stop(P),
    Config.

-define(DEFAULT_CACHE_TIME, 5000).

%% tests

name(Config) ->
    ct:pal(?LOW_IMPORTANCE, "Priv: ~p", [?config(priv_dir, Config)]),
    rabbit_mgmt_db_cache_banana = rabbit_mgmt_db_cache:process_name(banana).

fetch_new_key(_Config) ->
    {error, key_not_found} = rabbit_mgmt_db_cache:fetch(this_is_not_the_key_you_are_looking_for,
                                           fun() -> 123 end).

fetch(_Config) ->
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun() -> 123 end).

fetch_cached(_Config) ->
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun() ->
                                                           timer:sleep(100),
                                                           123 end),
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun() -> 321 end).

fetch_stale(Config) ->
    P = ?config(sut, Config),
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun() -> 123 end),
    ok = gen_server:call(P, purge_cache),
    {ok, 321} = rabbit_mgmt_db_cache:fetch(banana, fun() -> 321 end).

fetch_stale_after_expiry(_Config) ->
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun() -> 123 end), % expire quickly
    timer:sleep(500),
    {ok, 321} = rabbit_mgmt_db_cache:fetch(banana, fun() -> 321 end).

fetch_throws(_Config) ->
    {error, {throw, banana_face}} =
        rabbit_mgmt_db_cache:fetch(banana, fun() -> throw(banana_face) end),
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun() -> 123 end).

fetch_cached_with_same_args(_Config) ->
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun(_) ->
                                                           timer:sleep(100),
                                                           123
                                                   end, [42]),
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun(_) -> 321 end, [42]).

fetch_cached_with_different_args_invalidates_cache(_Config) ->
    {ok, 123} = rabbit_mgmt_db_cache:fetch(banana, fun(_) ->
                                                           timer:sleep(100),
                                                           123
                                                   end, [42]),
    {ok, 321} = rabbit_mgmt_db_cache:fetch(banana, fun(_) ->
                                                           timer:sleep(100),
                                                           321 end, [442]),
    {ok, 321} = rabbit_mgmt_db_cache:fetch(banana, fun(_) -> 456 end, [442]).
