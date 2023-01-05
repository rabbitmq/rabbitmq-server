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
                   {status, sort_cluster_status(cluster_status(hd(Nodes)))}]});
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
    IsClustered = rpc:call(Node, rabbit_db_cluster, is_clustered, []),
    ((AllNodes =/= [Node]) =:= IsClustered andalso equal(Status, NodeStatus)).

equal({_, _, A, B, C}, {undef, undef, A, B, C}) ->
    true;
equal({_, _, _, _, _}, {undef, undef, _, _, _}) ->
    false;
equal(Status0, Status1) ->
    Status0 == Status1.

cluster_status(Node) ->
    {AllNodes, DiscNodes, RunningNodes} =
    try
        erpc:call(Node, rabbit_db_cluster, current_or_last_status, [])
    catch
        error:{exception, undef,
               [{rabbit_db_cluster, current_or_last_status, _, _} | _]} ->
            erpc:call(Node, rabbit_mnesia, cluster_status, [status])
    end,
    {rpc:call(Node, rabbit_nodes, list_members, []),
     rpc:call(Node, rabbit_nodes, list_running, []),
     AllNodes,
     DiscNodes,
     RunningNodes}.

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

assert_clustered(Nodes) ->
    assert_cluster_status({Nodes, Nodes, Nodes, Nodes, Nodes}, Nodes).

assert_not_clustered(Node) ->
    assert_cluster_status({[Node], [Node], [Node], [Node], [Node]}, [Node]).
