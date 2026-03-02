%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_quorum_queue_prop_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(ITERATIONS, 10000).

all() ->
    [
     merge_member_uids_no_change_when_in_sync,
     merge_member_uids_preserves_existing_uids,
     merge_member_uids_prunes_stale_nodes,
     merge_member_uids_adds_gathered_uids,
     merge_member_uids_result_is_subset_of_ra,
     merge_member_uids_repaired_means_complete,
     merge_member_uids_ok_tuple_means_incomplete
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

%% -------------------------------------------------------------------
%% Generators
%% -------------------------------------------------------------------

node_gen() ->
    ?LET(N, range(1, 20),
         list_to_atom("node" ++ integer_to_list(N))).

uid_gen() ->
    non_empty(binary()).

node_list_gen() ->
    ?LET(Nodes, non_empty(list(node_gen())),
         lists:usort(Nodes)).

uid_map_gen(Nodes) ->
    ?LET(UIDs, vector(length(Nodes), uid_gen()),
         maps:from_list(lists:zip(Nodes, UIDs))).

%% Generate a test scenario for rabbitmq/rabbitmq#14241.
%% AddedNodesUids is a subset of nodes in RaNodes but not in PreviousUidsMap,
%% simulating partial or full success of `gather_node_uids/2`.
scenario_gen() ->
    ?LET({AllNodes, PrevNodes}, {node_list_gen(), node_list_gen()},
    ?LET(PreviousUidsMap, uid_map_gen(PrevNodes),
    ?LET(SuccessNodes, sublist_gen(AllNodes -- PrevNodes),
    ?LET(AddedNodesUids, uid_map_gen(SuccessNodes),
         {AllNodes, PreviousUidsMap, AddedNodesUids})))).

sublist_gen([]) ->
    [];
sublist_gen(List) ->
    ?LET(Bools, vector(length(List), boolean()),
         [E || {E, true} <- lists:zip(List, Bools)]).

%% Generate a scenario where RaNodes matches the previous map's keys exactly.
in_sync_scenario_gen() ->
    ?LET(Nodes, node_list_gen(),
    ?LET(UidsMap, uid_map_gen(Nodes),
         {Nodes, UidsMap, #{}})).

%% Generate a scenario where all new nodes report success
full_success_scenario_gen() ->
    ?LET({AllNodes, PrevNodes}, {node_list_gen(), node_list_gen()},
    begin
        NodesToAdd = AllNodes -- PrevNodes,
        ?LET(PreviousUidsMap, uid_map_gen(PrevNodes),
        ?LET(AddedNodesUids, uid_map_gen(NodesToAdd),
             {AllNodes, PreviousUidsMap, AddedNodesUids}))
    end).

%% Generate a scenario where at least one new node reports a failure.
partial_failure_scenario_gen() ->
    ?LET({AllNodes, PrevNodes}, {node_list_gen(), node_list_gen()},
    begin
        NodesToAdd = AllNodes -- PrevNodes,
        case NodesToAdd of
            [] ->
                %% No nodes to add, force at least one
                ExtraNode = 'node99',
                AllNodes2 = lists:usort([ExtraNode | AllNodes]),
                NodesToAdd2 = AllNodes2 -- PrevNodes,
                ?LET(PreviousUidsMap, uid_map_gen(PrevNodes),
                ?LET(SuccessNodes, strict_sublist_gen(NodesToAdd2),
                ?LET(AddedNodesUids, uid_map_gen(SuccessNodes),
                     {AllNodes2, PreviousUidsMap, AddedNodesUids})));
            _ ->
                ?LET(PreviousUidsMap, uid_map_gen(PrevNodes),
                ?LET(SuccessNodes, strict_sublist_gen(NodesToAdd),
                ?LET(AddedNodesUids, uid_map_gen(SuccessNodes),
                     {AllNodes, PreviousUidsMap, AddedNodesUids})))
        end
    end).

strict_sublist_gen([]) ->
    [];
strict_sublist_gen([_Single]) ->
    [];
strict_sublist_gen(List) ->
    ?SUCHTHAT(Sub, sublist_gen(List),
              length(Sub) < length(List)).

%% -------------------------------------------------------------------
%% Properties
%% -------------------------------------------------------------------

merge_member_uids_no_change_when_in_sync(Config) ->
    run(Config, fun prop_no_change_when_in_sync/1).

prop_no_change_when_in_sync(_Config) ->
    ?FORALL({RaNodes, PreviousUidsMap, AddedNodesUids}, in_sync_scenario_gen(),
        ok =:= rabbit_quorum_queue:merge_member_uids(
                  RaNodes, PreviousUidsMap, AddedNodesUids)).

merge_member_uids_preserves_existing_uids(Config) ->
    run(Config, fun prop_preserves_existing_uids/1).

prop_preserves_existing_uids(_Config) ->
    ?FORALL({RaNodes, PreviousUidsMap, AddedNodesUids}, scenario_gen(),
        begin
            Result = rabbit_quorum_queue:merge_member_uids(
                       RaNodes, PreviousUidsMap, AddedNodesUids),
            case Result of
                ok ->
                    true;
                {_, NewNodes} ->
                    %% For every node in both RaNodes and PreviousUidsMap,
                    %% its UID must be preserved
                    Kept = maps:with(RaNodes, PreviousUidsMap),
                    maps:fold(fun(Node, UId, Acc) ->
                                      Acc andalso maps:get(Node, NewNodes) =:= UId
                              end, true, Kept)
            end
        end).

merge_member_uids_prunes_stale_nodes(Config) ->
    run(Config, fun prop_prunes_stale_nodes/1).

prop_prunes_stale_nodes(_Config) ->
    ?FORALL({RaNodes, PreviousUidsMap, AddedNodesUids}, scenario_gen(),
        begin
            Result = rabbit_quorum_queue:merge_member_uids(
                       RaNodes, PreviousUidsMap, AddedNodesUids),
            case Result of
                ok ->
                    true;
                {_, NewNodes} ->
                    %% No node outside RaNodes appears in the result
                    lists:all(fun(N) -> lists:member(N, RaNodes) end,
                              maps:keys(NewNodes))
            end
        end).

merge_member_uids_adds_gathered_uids(Config) ->
    run(Config, fun prop_adds_gathered_uids/1).

prop_adds_gathered_uids(_Config) ->
    ?FORALL({RaNodes, PreviousUidsMap, AddedNodesUids}, scenario_gen(),
        begin
            Result = rabbit_quorum_queue:merge_member_uids(
                       RaNodes, PreviousUidsMap, AddedNodesUids),
            case Result of
                ok ->
                    true;
                {_, NewNodes} ->
                    %% All successfully gathered UIDs are in the result
                    maps:fold(fun(Node, UId, Acc) ->
                                      Acc andalso maps:get(Node, NewNodes) =:= UId
                              end, true, AddedNodesUids)
            end
        end).

merge_member_uids_result_is_subset_of_ra(Config) ->
    run(Config, fun prop_result_is_subset_of_ra/1).

prop_result_is_subset_of_ra(_Config) ->
    ?FORALL({RaNodes, PreviousUidsMap, AddedNodesUids}, scenario_gen(),
        begin
            Result = rabbit_quorum_queue:merge_member_uids(
                       RaNodes, PreviousUidsMap, AddedNodesUids),
            case Result of
                ok ->
                    true;
                {_, NewNodes} ->
                    lists:all(fun(N) -> lists:member(N, RaNodes) end,
                              maps:keys(NewNodes))
            end
        end).

merge_member_uids_repaired_means_complete(Config) ->
    run(Config, fun prop_repaired_means_complete/1).

prop_repaired_means_complete(_Config) ->
    ?FORALL({RaNodes, PreviousUidsMap, AddedNodesUids}, scenario_gen(),
        begin
            Result = rabbit_quorum_queue:merge_member_uids(
                       RaNodes, PreviousUidsMap, AddedNodesUids),
            case Result of
                {repaired, NewNodes} ->
                    lists:sort(maps:keys(NewNodes)) =:= lists:sort(RaNodes);
                _ ->
                    true
            end
        end).

merge_member_uids_ok_tuple_means_incomplete(Config) ->
    run(Config, fun prop_ok_tuple_means_incomplete/1).

prop_ok_tuple_means_incomplete(_Config) ->
    ?FORALL({RaNodes, PreviousUidsMap, AddedNodesUids}, partial_failure_scenario_gen(),
        begin
            Result = rabbit_quorum_queue:merge_member_uids(
                       RaNodes, PreviousUidsMap, AddedNodesUids),
            case Result of
                {ok, NewNodes} ->
                    lists:sort(maps:keys(NewNodes)) =/= lists:sort(RaNodes);
                _ ->
                    true
            end
        end).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

run(_Config, PropFun) ->
    Property = fun() -> PropFun([]) end,
    rabbit_ct_proper_helpers:run_proper(Property, [], ?ITERATIONS).
