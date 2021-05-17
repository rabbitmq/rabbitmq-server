%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(peer_discovery_classic_config_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(rabbit_ct_broker_helpers, [
    cluster_members_online/2
]).

-compile(export_all).

all() ->
    [
     {group, non_parallel}
    ].

groups() ->
    [
     {non_parallel, [], [
                         successful_discovery,
                         successful_discovery_with_a_subset_of_nodes_coming_online,
                         no_nodes_configured
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

init_per_testcase(successful_discovery = Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),

    N = 3,
    NodeNames = [
      list_to_atom(rabbit_misc:format("~s-~b", [Testcase, I]))
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

    N = 2,
    NodeNames = [
      list_to_atom(rabbit_misc:format("~s-~b", [Testcase, I]))
      || I <- lists:seq(1, N)
    ],
    Config2 = rabbit_ct_helpers:set_config(Config1, [
        {rmq_nodename_suffix, Testcase},
        %% note: this must not include the host part
        {rmq_nodes_count, NodeNames},
        {rmq_nodes_clustered, false}
      ]),
    NodeNamesWithHostname = [rabbit_nodes:make({Name, "localhost"}) || Name <- [nonexistent | NodeNames]],
    %% reduce retry time since we know one node on the list does
    %% not exist and not just unreachable
    Config3 = rabbit_ct_helpers:merge_app_env(Config2,
      {rabbit, [
          {cluster_formation, [
              {discovery_retry_limit, 10},
              {discovery_retry_interval, 200}
          ]},
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
  ?awaitMatch(
     {M1, M2} when length(M1) =:= 3; length(M2) =:= 3,
                   {cluster_members_online(Config, 0),
                    cluster_members_online(Config, 1)},
                   ?TIMEOUT).

successful_discovery_with_a_subset_of_nodes_coming_online(Config) ->
  ?awaitMatch(
     {M1, M2} when length(M1) =:= 2; length(M2) =:= 2,
                   {cluster_members_online(Config, 0),
                    cluster_members_online(Config, 1)},
                   ?TIMEOUT).

no_nodes_configured(Config) ->
  ?awaitMatch(
     M when length(M) < 2,
            cluster_members_online(Config, 0),
            ?TIMEOUT).
