%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(peer_discovery_classic_config_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(rabbit_ct_broker_helpers, [
    cluster_members_online/2
]).

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
     {group, non_parallel},
     {group, cluster_size_3},
     {group, cluster_size_5},
     {group, cluster_size_7}
    ].

groups() ->
    [
     {non_parallel, [], [
                         no_nodes_configured
                        ]},
     {cluster_size_3, [], [
                           successful_discovery,
                           successful_discovery_with_a_subset_of_nodes_coming_online
                          ]},
     {cluster_size_5, [], [
                           successful_discovery,
                           successful_discovery_with_a_subset_of_nodes_coming_online
                          ]},
     {cluster_size_7, [], [
                           successful_discovery,
                           successful_discovery_with_a_subset_of_nodes_coming_online
                          ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 5}}
    ].

-define(TIMEOUT, 120_000).

%%
%% Setup/teardown.
%%

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_3 = Group, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}, {group, Group}]);
init_per_group(cluster_size_5 = Group, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 5}, {group, Group}]);
init_per_group(cluster_size_7 = Group, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 7}, {group, Group}]);
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(successful_discovery = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    N = ?config(rmq_nodes_count, Config),
    NodeNames = [
      list_to_atom(rabbit_misc:format("~ts-~ts-~b", [Testcase, ?config(group, Config), I]))
      || I <- lists:seq(1, N)
    ],
    Config2 = rabbit_ct_helpers:set_config(Config1, [
        {rmq_nodename_suffix, Testcase},
        %% note: this must not include the host part
        {rmq_nodes_count, NodeNames},
        {rmq_nodes_clustered, false}
      ]),
    NodeNamesWithHostname = [rabbit_nodes:make({Name, "localhost"}) || Name <- NodeNames],
    Config3 = rabbit_ct_helpers:merge_app_env(Config2,
      {rabbit, [
          {cluster_nodes, {NodeNamesWithHostname, disc}},
          {cluster_formation, [
              {internal_lock_retries, 10}
          ]}
      ]}),
    rabbit_ct_helpers:run_steps(Config3,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(successful_discovery_with_a_subset_of_nodes_coming_online = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),

    N = ?config(rmq_nodes_count, Config),
    NodeNames = [
      list_to_atom(rabbit_misc:format("~ts-~ts-~b", [Testcase, ?config(group, Config), I]))
      || I <- lists:seq(1, N)
    ],
    Config2 = rabbit_ct_helpers:set_config(Config1, [
        {rmq_nodename_suffix, Testcase},
        %% We remove the first node in the list: it will be considered by peer
        %% discovery (see `cluster_nodes' below), but it won't be started.
        %% note: this must not include the host part
        {rmq_nodes_count, tl(NodeNames)},
        {rmq_nodes_clustered, false}
      ]),
    NodeNamesWithHostname = [rabbit_nodes:make({Name, "localhost"}) || Name <- NodeNames],
    %% reduce retry time since we know one node on the list does
    %% not exist and not just unreachable
    %% We no longer test non-existing nodes, it just times out
    %% constantly in CI
    %% To compare, this suite takes ~23min in my machine with
    %% unreachable nodes vs ~6min without them
    Config3 = rabbit_ct_helpers:merge_app_env(Config2,
      {rabbit, [
          {cluster_nodes, {NodeNamesWithHostname, disc}},
          {cluster_formation, [
              {internal_lock_retries, 10}
          ]}
      ]}),
    rabbit_ct_helpers:run_steps(Config3,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(no_nodes_configured = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_count, 2},
        {rmq_nodes_clustered, false}
      ]),
    Config3 = rabbit_ct_helpers:merge_app_env(Config2,
      {rabbit, [
          {cluster_nodes, {[], disc}},
          {cluster_formation, [
              {internal_lock_retries, 10}
          ]}
      ]}),
    rabbit_ct_helpers:run_steps(Config3,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).


end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).


%%
%% Test cases
%%
successful_discovery(Config) ->
  N = length(?config(rmq_nodes_count, Config)),
  ?awaitMatch(
     {M1, M2} when length(M1) =:= N; length(M2) =:= N,
                   {cluster_members_online(Config, 0),
                    cluster_members_online(Config, 1)},
                   ?TIMEOUT).

successful_discovery_with_a_subset_of_nodes_coming_online() ->
    [{timetrap, {minutes, 15}}].

successful_discovery_with_a_subset_of_nodes_coming_online(Config) ->
  N = length(?config(rmq_nodes_count, Config)),
  ?awaitMatch(
     {M1, M2} when length(M1) =:= N; length(M2) =:= N,
                   {cluster_members_online(Config, 0),
                    cluster_members_online(Config, 1)},
                   ?TIMEOUT).

no_nodes_configured(Config) ->
  ?awaitMatch(
     M when length(M) < 2,
            cluster_members_online(Config, 0),
            ?TIMEOUT).
