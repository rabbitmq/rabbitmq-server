%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_cluster_formation_sort_nodes_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2,

         sort_single_node/1,
         sort_by_cluster_size/1,
         sort_by_start_time/1,
         sort_by_node_name/1,
         cluster_size_has_precedence_over_start_time/1,
         start_time_has_precedence_over_node_name/1,
         failed_in_ci_1/1,
         failed_in_ci_2/1]).

all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    [
     {parallel_tests, [parallel],
      [
       sort_single_node,
       sort_by_cluster_size,
       sort_by_start_time,
       sort_by_node_name,
       cluster_size_has_precedence_over_start_time,
       start_time_has_precedence_over_node_name,
       failed_in_ci_1,
       failed_in_ci_2
      ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

sort_single_node(_Config) ->
    NodesAndProps = [{a, [a], 100, true}],
    ?assertEqual(
       NodesAndProps,
       rabbit_peer_discovery:sort_nodes_and_props(NodesAndProps)).

sort_by_cluster_size(_Config) ->
    NodesAndProps = [{a, [a], 100, true},
                     {a, [a, a], 100, true}],
    ?assertEqual(
       [{a, [a, a], 100, true},
        {a, [a], 100, true}],
       rabbit_peer_discovery:sort_nodes_and_props(NodesAndProps)).

sort_by_start_time(_Config) ->
    NodesAndProps = [{a, [a], 20, true},
                     {a, [a], 10, true}],
    ?assertEqual(
       [{a, [a], 10, true},
        {a, [a], 20, true}],
       rabbit_peer_discovery:sort_nodes_and_props(NodesAndProps)).

sort_by_node_name(_Config) ->
    NodesAndProps = [{b, [b], 100, true},
                     {a, [a], 100, true}],
    ?assertEqual(
       [{a, [a], 100, true},
        {b, [b], 100, true}],
       rabbit_peer_discovery:sort_nodes_and_props(NodesAndProps)).

cluster_size_has_precedence_over_start_time(_Config) ->
    NodesAndProps = [{a, [a], 100, true},
                     {b, [b, c], 90, true}],
    ?assertEqual(
       [{b, [b, c], 90, true},
        {a, [a], 100, true}],
       rabbit_peer_discovery:sort_nodes_and_props(NodesAndProps)).

start_time_has_precedence_over_node_name(_Config) ->
    NodesAndProps = [{a, [a], 100, true},
                     {b, [b], 90, true}],
    ?assertEqual(
       [{b, [b], 90, true},
        {a, [a], 100, true}],
       rabbit_peer_discovery:sort_nodes_and_props(NodesAndProps)).

failed_in_ci_1(_Config) ->
    NodesAndProps = [{'successful_discovery-cluster_size_7-7@localhost',
                      ['successful_discovery-cluster_size_7-7@localhost'],
                      1699635835018, true},
                     {'successful_discovery-cluster_size_7-6@localhost',
                      ['successful_discovery-cluster_size_7-6@localhost'],
                      1699635835006, true},
                     {'successful_discovery-cluster_size_7-5@localhost',
                      ['successful_discovery-cluster_size_7-5@localhost'],
                      1699635835019, true},
                     {'successful_discovery-cluster_size_7-4@localhost',
                      ['successful_discovery-cluster_size_7-4@localhost'],
                      1699635835007, true},
                     {'successful_discovery-cluster_size_7-3@localhost',
                      ['successful_discovery-cluster_size_7-3@localhost'],
                      1699635835006, true},
                     {'successful_discovery-cluster_size_7-2@localhost',
                      ['successful_discovery-cluster_size_7-2@localhost'],
                      1699635835013, true},
                     {'successful_discovery-cluster_size_7-1@localhost',
                      ['successful_discovery-cluster_size_7-1@localhost'],
                      1699635835011, true}],
    ?assertEqual(
       [{'successful_discovery-cluster_size_7-3@localhost',
         ['successful_discovery-cluster_size_7-3@localhost'],
         1699635835006, true},
        {'successful_discovery-cluster_size_7-6@localhost',
         ['successful_discovery-cluster_size_7-6@localhost'],
         1699635835006, true},
        {'successful_discovery-cluster_size_7-4@localhost',
         ['successful_discovery-cluster_size_7-4@localhost'],
         1699635835007, true},
        {'successful_discovery-cluster_size_7-1@localhost',
         ['successful_discovery-cluster_size_7-1@localhost'],
         1699635835011, true},
        {'successful_discovery-cluster_size_7-2@localhost',
         ['successful_discovery-cluster_size_7-2@localhost'],
         1699635835013, true},
        {'successful_discovery-cluster_size_7-7@localhost',
         ['successful_discovery-cluster_size_7-7@localhost'],
         1699635835018, true},
        {'successful_discovery-cluster_size_7-5@localhost',
         ['successful_discovery-cluster_size_7-5@localhost'],
         1699635835019, true}],
       rabbit_peer_discovery:sort_nodes_and_props(NodesAndProps)).

failed_in_ci_2(_Config) ->
    NodesAndProps = [{'successful_discovery-cluster_size_7-7@localhost',
                      ['successful_discovery-cluster_size_7-1@localhost',
                       'successful_discovery-cluster_size_7-7@localhost',
                       'successful_discovery-cluster_size_7-2@localhost'],
                      1699635835018, true},
                     {'successful_discovery-cluster_size_7-6@localhost',
                      ['successful_discovery-cluster_size_7-6@localhost'],
                      1699635835006, true},
                     {'successful_discovery-cluster_size_7-5@localhost',
                      ['successful_discovery-cluster_size_7-5@localhost'],
                      1699635835019, true},
                     {'successful_discovery-cluster_size_7-4@localhost',
                      ['successful_discovery-cluster_size_7-4@localhost'],
                      1699635835007, true},
                     {'successful_discovery-cluster_size_7-3@localhost',
                      ['successful_discovery-cluster_size_7-3@localhost'],
                      1699635835006, true},
                     {'successful_discovery-cluster_size_7-2@localhost',
                      ['successful_discovery-cluster_size_7-1@localhost',
                       'successful_discovery-cluster_size_7-7@localhost',
                       'successful_discovery-cluster_size_7-2@localhost'],
                      1699635835013, true},
                     {'successful_discovery-cluster_size_7-1@localhost',
                      ['successful_discovery-cluster_size_7-1@localhost',
                       'successful_discovery-cluster_size_7-7@localhost',
                       'successful_discovery-cluster_size_7-2@localhost'],
                      1699635835011, true}],
    ?assertEqual(
       [{'successful_discovery-cluster_size_7-1@localhost',
         ['successful_discovery-cluster_size_7-1@localhost',
          'successful_discovery-cluster_size_7-7@localhost',
          'successful_discovery-cluster_size_7-2@localhost'],
         1699635835011, true},
        {'successful_discovery-cluster_size_7-2@localhost',
         ['successful_discovery-cluster_size_7-1@localhost',
          'successful_discovery-cluster_size_7-7@localhost',
          'successful_discovery-cluster_size_7-2@localhost'],
         1699635835013, true},
        {'successful_discovery-cluster_size_7-7@localhost',
         ['successful_discovery-cluster_size_7-1@localhost',
          'successful_discovery-cluster_size_7-7@localhost',
          'successful_discovery-cluster_size_7-2@localhost'],
         1699635835018, true},
        {'successful_discovery-cluster_size_7-3@localhost',
         ['successful_discovery-cluster_size_7-3@localhost'],
         1699635835006, true},
        {'successful_discovery-cluster_size_7-6@localhost',
         ['successful_discovery-cluster_size_7-6@localhost'],
         1699635835006, true},
        {'successful_discovery-cluster_size_7-4@localhost',
         ['successful_discovery-cluster_size_7-4@localhost'],
         1699635835007, true},
        {'successful_discovery-cluster_size_7-5@localhost',
         ['successful_discovery-cluster_size_7-5@localhost'],
         1699635835019, true}],
       rabbit_peer_discovery:sort_nodes_and_props(NodesAndProps)).
