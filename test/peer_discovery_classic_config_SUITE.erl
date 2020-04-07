%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(peer_discovery_classic_config_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

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


%%
%% Setup/teardown.
%%

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

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
              {randomized_startup_delay_range, {1, 10}}
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
              {randomized_startup_delay_range, {1, 10}}
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
              {randomized_startup_delay_range, {1, 10}}
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
    Condition = fun() ->
                    3 =:= length(cluster_members_online(Config, 0)) andalso
                    3 =:= length(cluster_members_online(Config, 1))
                end,
    await_cluster(Config, Condition, [1, 2]).

successful_discovery_with_a_subset_of_nodes_coming_online(Config) ->
    Condition = fun() ->
                    2 =:= length(cluster_members_online(Config, 0)) andalso
                    2 =:= length(cluster_members_online(Config, 1))
                end,
    await_cluster(Config, Condition, [1]).

no_nodes_configured(Config) ->
    Condition = fun() -> length(cluster_members_online(Config, 0)) < 2 end,
    await_cluster(Config, Condition, [1]).

reset_and_restart_node(Config, I) when is_integer(I) andalso I >= 0 ->
    Name = rabbit_ct_broker_helpers:get_node_config(Config, I, nodename),
    rabbit_control_helper:command(stop_app, Name),
    rabbit_ct_broker_helpers:reset_node(Config, Name),
    rabbit_control_helper:command(start_app, Name).

await_cluster(Config, Condition, Nodes) ->
    try
        rabbit_ct_helpers:await_condition(Condition, 30000)
    catch
        exit:{test_case_failed, _} ->
            ct:pal(?LOW_IMPORTANCE, "Possible dead-lock; resetting/restarting these nodes: ~p", [Nodes]),
            [reset_and_restart_node(Config, N) || N <- Nodes],
            rabbit_ct_helpers:await_condition(Condition, 30000)
    end.
