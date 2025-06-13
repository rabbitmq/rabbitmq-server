%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(clustering_utils).

-export([
         assert_status/2,
         assert_cluster_status/2,
         assert_clustered/1,
         assert_not_clustered/1
        ]).

-define(LOOP_RECURSION_DELAY, 100).

assert_status(Tuple, Nodes) ->
    assert_cluster_status(Tuple, Nodes, fun verify_status_equal/3).

assert_cluster_status(Tuple, Nodes) ->
    assert_cluster_status(Tuple, Nodes, fun verify_cluster_status_equal/3).

assert_cluster_status({All, Running}, Nodes, VerifyFun) ->
    assert_cluster_status({All, Running, All, All, Running}, Nodes, VerifyFun);
assert_cluster_status({All, Disc, Running}, Nodes, VerifyFun) ->
    assert_cluster_status({All, Running, All, Disc, Running}, Nodes, VerifyFun);
assert_cluster_status(Status0, Nodes, VerifyFun) ->
    Status = sort_cluster_status(Status0),
    AllNodes = case Status of
                   {undef, undef, All, _, _} ->
                       %% Support mixed-version clusters
                       All;
                   {All, _, _, _, _} ->
                       All
               end,
    wait_for_cluster_status(Status, AllNodes, Nodes, VerifyFun).

wait_for_cluster_status(Status, AllNodes, Nodes, VerifyFun) ->
    Max = 10000 / ?LOOP_RECURSION_DELAY,
    wait_for_cluster_status(0, Max, Status, AllNodes, Nodes, VerifyFun).

wait_for_cluster_status(N, Max, Status, _AllNodes, Nodes, _VerifyFun) when N >= Max ->
    erlang:error({cluster_status_max_tries_failed,
                  [{nodes, Nodes},
                   {expected_status, Status},
                   {max_tried, Max},
                   {status, [{Node, sort_cluster_status(cluster_status(Node))} || Node <- Nodes]}]});
wait_for_cluster_status(N, Max, Status, AllNodes, Nodes, VerifyFun) ->
    case lists:all(fun (Node) ->
                           VerifyFun(Node, Status, AllNodes)
                   end, Nodes) of
        true  -> ok;
        false -> timer:sleep(?LOOP_RECURSION_DELAY),
                 wait_for_cluster_status(N + 1, Max, Status, AllNodes, Nodes, VerifyFun)
    end.

verify_status_equal(Node, Status, _AllNodes) ->
    NodeStatus = sort_cluster_status(cluster_status(Node)),
    equal(Status, NodeStatus).

verify_cluster_status_equal(Node, Status, AllNodes) ->
    NodeStatus = sort_cluster_status(cluster_status(Node)),
    %% To be compatible with mixed version clusters in 3.11.x we use here
    %% rabbit_mnesia:is_clustered/0 instead of rabbit_db_cluster:is_clustered/0
    IsClustered0 = rpc:call(Node, rabbit_db_cluster, is_clustered, []),
    IsClustered = case maybe_undef(IsClustered0) of
                      undef -> rpc:call(Node, rabbit_mnesia, is_clustered, []);
                      _     -> IsClustered0
                  end,
    ((AllNodes =/= [Node]) =:= IsClustered andalso equal(Status, NodeStatus)).

equal({_, _, A, B, C}, {undef, undef, A, B, C}) ->
    true;
equal({_, _, _, _, _}, {undef, undef, _, _, _}) ->
    false;
equal(Status0, Status1) ->
    Status0 == Status1.

cluster_status(Node) ->
    %% To be compatible with mixed version clusters in 3.11.x we use here
    %% rabbit_nodes:all/0 instead of rabbit_nodes:list_members/0 and
    %% rabbit_nodes:all_running/0 instead of rabbit_nodes:list_running/0
    %% which are part of the new API.
    AllMembers0 = rpc:call(Node, rabbit_nodes, list_members, []),
    AllMembers = case maybe_undef(AllMembers0) of
                     undef -> rpc:call(Node, rabbit_nodes, all, []);
                     _     -> AllMembers0
                 end,
    RunningMembers0 = rpc:call(Node, rabbit_nodes, list_running, []),
    RunningMembers = case maybe_undef(RunningMembers0) of
                         undef -> rpc:call(Node, rabbit_nodes, all_running, []);
                         _     -> RunningMembers0
                     end,

    %% To be compatible with mixed version clusters in 3.11.x we use here
    %% rabbit_mnesia:cluster_nodes/1 instead of rabbit_db_cluster:members/0
    AllDbNodes0 = rpc:call(Node, rabbit_db_cluster, members, []),
    AllDbNodes = case maybe_undef(AllDbNodes0) of
                     undef -> rpc:call(Node, rabbit_mnesia, cluster_nodes, [all]);
                     _     -> AllDbNodes0
                 end,
    {DiscDbNodes, RunningDbNodes} =
    case rpc:call(Node, rabbit_khepri, is_enabled, []) of
        true ->
            {AllMembers, RunningMembers};
        _ ->
            {rpc:call(Node, rabbit_mnesia, cluster_nodes, [disc]),
             rpc:call(Node, rabbit_mnesia, cluster_nodes, [running])}
    end,

    {AllMembers,
     RunningMembers,
     AllDbNodes,
     DiscDbNodes,
     RunningDbNodes}.

sort_cluster_status({All, Running, AllM, DiscM, RunningM}) ->
    {maybe_sort(All), maybe_sort(Running), maybe_sort(AllM), maybe_sort(DiscM), maybe_sort(RunningM)}.

maybe_sort({badrpc, {'EXIT', {undef, _}}}) ->
    undef;
maybe_sort({badrpc, nodedown}) ->
    nodedown;
maybe_sort({badrpc, Reason}) ->
    Reason;
maybe_sort(List) ->
    lists:sort(List).

maybe_undef({badrpc, {'EXIT', {undef, _}}}) ->
    undef;
maybe_undef(Any) ->
    Any.

assert_clustered(Nodes) ->
    assert_cluster_status({Nodes, Nodes, Nodes, Nodes, Nodes}, Nodes).

assert_not_clustered(Node) ->
    assert_cluster_status({[Node], [Node], [Node], [Node], [Node]}, [Node]).
