%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(clustering_utils).

-export([
         assert_cluster_status/2,
         assert_clustered/1,
         assert_not_clustered/1
        ]).

-define(LOOP_RECURSION_DELAY, 100).

assert_cluster_status({All, Disc, Running}, Nodes) ->
    assert_cluster_status({All, Running, All, Disc, Running}, Nodes);
assert_cluster_status(Status0, Nodes) ->
    Status = sort_cluster_status(Status0),
    AllNodes = case Status of
                   {undef, undef, All, _, _} ->
                       %% Support mixed-version clusters
                       All;
                   {All, _, _, _, _} ->
                       All
               end,
    wait_for_cluster_status(Status, AllNodes, Nodes).

wait_for_cluster_status(Status, AllNodes, Nodes) ->
    Max = 10000 / ?LOOP_RECURSION_DELAY,
    wait_for_cluster_status(0, Max, Status, AllNodes, Nodes).

wait_for_cluster_status(N, Max, Status, _AllNodes, Nodes) when N >= Max ->
    erlang:error({cluster_status_max_tries_failed,
                  [{nodes, Nodes},
                   {expected_status, Status},
                   {max_tried, Max},
                   {status, sort_cluster_status(cluster_status(hd(Nodes)))}]});
wait_for_cluster_status(N, Max, Status, AllNodes, Nodes) ->
    case lists:all(fun (Node) ->
                            verify_status_equal(Node, Status, AllNodes)
                   end, Nodes) of
        true  -> ok;
        false -> timer:sleep(?LOOP_RECURSION_DELAY),
                 wait_for_cluster_status(N + 1, Max, Status, AllNodes, Nodes)
    end.

verify_status_equal(Node, Status, AllNodes) ->
    NodeStatus = sort_cluster_status(cluster_status(Node)),
    IsClustered = case rpc:call(Node, rabbit_db_cluster, is_clustered, []) of
                      {badrpc, {'EXIT', {undef, _}}} ->
                          rpc:call(Node, rabbit_mnesia, is_clustered, []);
                      Ret ->
                          Ret
                  end,
    (AllNodes =/= [Node]) =:= IsClustered andalso equal(Status, NodeStatus).

equal({_, _, A, B, C}, {undef, undef, A, B, C}) ->
    true;
equal({_, _, _, _, _}, {undef, undef, _, _, _}) ->
    false;
equal(Status0, Status1) ->
    Status0 == Status1.

cluster_status(Node) ->
    AllMembers = rpc:call(Node, rabbit_nodes, list_members, []),
    RunningMembers = rpc:call(Node, rabbit_nodes, list_running, []),

    AllDbNodes = case rpc:call(Node, rabbit_db_cluster, members, []) of
                     {badrpc, {'EXIT', {undef, _}}} ->
                         rpc:call(Node, rabbit_mnesia, cluster_nodes, [all]);
                     Ret ->
                         Ret
                 end,
    DiscDbNodes = rpc:call(Node, rabbit_mnesia, cluster_nodes, [disc]),
    RunningDbNodes = rpc:call(Node, rabbit_mnesia, cluster_nodes, [running]),

    {AllMembers,
     RunningMembers,
     AllDbNodes,
     DiscDbNodes,
     RunningDbNodes}.

sort_cluster_status({{badrpc, {'EXIT', {undef, _}}}, {badrpc, {'EXIT', {undef, _}}}, AllM, DiscM, RunningM}) ->
    {undef, undef, lists:sort(AllM), lists:sort(DiscM), lists:sort(RunningM)};
sort_cluster_status({All, Running, AllM, DiscM, RunningM}) ->
    {lists:sort(All), lists:sort(Running), lists:sort(AllM), lists:sort(DiscM), lists:sort(RunningM)}.

assert_clustered(Nodes) ->
    assert_cluster_status({Nodes, Nodes, Nodes, Nodes, Nodes}, Nodes).

assert_not_clustered(Node) ->
    assert_cluster_status({[Node], [Node], [Node], [Node], [Node]}, [Node]).

