%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_clear_cache_command_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile(export_all).

-define(CLEAR_CACHE_CMD, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ClearAuthBackendCacheCommand').

all() ->
    [
      {group, non_parallel_tests},
      {group, cluster_size_2}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               clear_cache
                              ]},
     {cluster_size_2, [], [
                               clear_cache
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).


setup_env(Config, Nodename) ->
    rpc(Config, Nodename, application, set_env, 
        [rabbit, auth_backends, [rabbit_auth_backend_cache]]),
    Config.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_2, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of 
        true -> {skip, "cluster size 2 isn't mixed versions compatible"};
        false -> init_per_multinode_group(cluster_size_2, Config, 2)
    end;
init_per_group(Group, Config) ->
    init_per_multinode_group(Group, Config, 1).

init_per_multinode_group(_Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
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

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------


clear_cache(Config) ->
    F = user_login_authentication,
    A = [<<"guest">>, [{password, <<"guest">>}]],
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [ setup_env(Config, Nodename) || Nodename <- Nodes],
    
    [ ok = ensure_cache_entries(Config, Node, {F, A}) || Node <- Nodes],
    ?CLEAR_CACHE_CMD:run([], #{node => lists:last(Nodes)}),
    [ rabbit_ct_helpers:await_condition_with_retries(fun () -> 
            case has_cache_entry(Config, Node, {F, A}) of 
                {error, not_found} -> true;
                _ -> false
            end 
        end, 20) || Node <- Nodes].

ensure_cache_entries(Config, Nodename, {F, A}) ->
    {ok, AuthRespOk} = rpc(Config, Nodename, rabbit_auth_backend_internal, F, A),
    {ok, AuthRespOk} = rpc(Config, Nodename, rabbit_auth_backend_cache, F, A),
    ok = has_cache_entry(Config, Nodename, {F, A}).

rpc(Config, N, M, F, A) ->
    rabbit_ct_broker_helpers:rpc(Config, N, M, F, A).

has_cache_entry(Config, Node, {F, A}) ->
    {ok, AuthCache} = rpc(Config, Node, application, get_env, 
        [rabbitmq_auth_backend_cache, cache_module]),
    case rpc(Config, Node, AuthCache, get, [{F, A}]) of
        {ok, _} -> ok;
        {error, not_found} = E -> E
    end.