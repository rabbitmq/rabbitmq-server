%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
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
                               fetch_invalid_key,
                               fetch,
                               fetch_cached,
                               fetch_stale,
                               fetch_stale_after_expiry,
                               fetch_throws
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
    ct:pal("Priv: ~p", [?config(priv_dir, Config)]),
    rabbit_mgmt_db_cache_banana = rabbit_mgmt_db_cache:process_name(banana).

fetch_invalid_key(_Config) ->
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
