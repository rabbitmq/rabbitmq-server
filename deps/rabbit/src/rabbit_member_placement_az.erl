%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_member_placement_az).
-behaviour(rabbit_member_placement).

%% Callback implementation.
-export([select_members/7]).

-include_lib("kernel/include/logger.hrl").

%% Exported so they can be mocked in unit tests.
-export([node/0,
         nodes/0,
         node_tags_for_nodes/2]).

%% Diagnostic functions.
-export([queues_not_fully_az_covered/0,
         queues_not_fully_az_covered/1,
         member_distribution_per_az/0,
         member_distribution_per_az/1]).

-spec select_members(pos_integer(), [node(), ...], [node()],
                     non_neg_integer(), non_neg_integer(),
                     function(), binary()) -> {[node(), ...], function()}.
select_members(Size, AllNodes, RunningNodes, QueueCount, QueueCountStartRandom,
               GetQueues, TagKey) ->
    NodeToAZ = ?MODULE:node_tags_for_nodes(AllNodes, TagKey),
    case lists:any(fun(V) -> V =/= undefined end, maps:values(NodeToAZ)) of
        false ->
            ?LOG_WARNING(
               "Quorum queue member-placement-tag '~ts' is set but no cluster node has "
               "this tag. Falling back to default member placement.", [TagKey]),
            rabbit_queue_location:select_members_balanced_fallback(
              Size, AllNodes, RunningNodes, QueueCount, QueueCountStartRandom, GetQueues);
        true ->
            select_members_az_aware(Size, AllNodes, RunningNodes, QueueCount,
                                    QueueCountStartRandom, GetQueues, NodeToAZ)
    end.

select_members_az_aware(Size, AllNodes, RunningNodes, QueueCount, QueueCountStartRandom,
                        GetQueues0, NodeToAZ) ->
    AZCounts = #{AZ => 0 || AZ <- lists:uniq(maps:values(NodeToAZ))},
    CandidateNodes = running_first(AllNodes, RunningNodes),
    case QueueCount >= QueueCountStartRandom of
        true ->
            PickFun = fun(Pool) -> hd(shuffle(Pool)) end,
            Selected = select_az_aware(Size, CandidateNodes, RunningNodes,
                                       NodeToAZ, AZCounts, PickFun),
            {Selected, GetQueues0};
        false ->
            Queues = GetQueues0(),
            Counters = rabbit_queue_location:build_node_counters(
                         #{N => 0 || N <- AllNodes}, Queues),
            PickFun = fun(Pool) -> pick_balanced(Pool, Counters) end,
            Selected = select_az_aware(Size, CandidateNodes, RunningNodes,
                                       NodeToAZ, AZCounts, PickFun),
            {Selected, fun() -> Queues end}
    end.

select_az_aware(0, _, _, _, _, _) ->
    [];
select_az_aware(_, [], _, _, _, _) ->
    [];
select_az_aware(Remaining, CandidateNodes, RunningNodes, NodeToAZ, AZCounts, PickFun) ->
    Pool = az_aware_candidates(CandidateNodes, RunningNodes, NodeToAZ, AZCounts),
    Node = PickFun(Pool),
    NodeAZ = maps:get(Node, NodeToAZ, undefined),
    NewAZCounts = maps:update_with(NodeAZ, fun(C) -> C + 1 end, AZCounts),
    [Node | select_az_aware(Remaining - 1, lists:delete(Node, CandidateNodes),
                            RunningNodes, NodeToAZ, NewAZCounts, PickFun)].

%% Pick the pool of candidate nodes for the next AZ-aware slot.
%% Prefers nodes in the least-populated AZs; prefers running nodes within that pool.
%% Falls back to any remaining node if all nodes in min-AZs are already selected.
az_aware_candidates(CandidateNodes, RunningNodes, NodeToAZ, AZCounts) ->
    MinCount = lists:min(maps:values(AZCounts)),
    MinAZs = [AZ || AZ := C <- AZCounts, C =:= MinCount],
    InMinAZs = [N || N <- CandidateNodes,
                     lists:member(maps:get(N, NodeToAZ, undefined), MinAZs)],
    case InMinAZs of
        [] ->
            %% All nodes from the minimum AZs are already selected; use any remaining node.
            CandidateNodes;
        _ ->
            case [N || N <- InMinAZs, lists:member(N, RunningNodes)] of
                [] -> InMinAZs;
                Running -> Running
            end
    end.

pick_balanced(Pool, Counters) ->
    hd(lists:sort(fun(A, B) -> maps:get(A, Counters, 0) =< maps:get(B, Counters, 0) end, Pool)).

%% Running nodes first so that AZ-aware selection prefers them within each AZ.
running_first(Nodes, RunningNodes) ->
    [N || N <- Nodes, lists:member(N, RunningNodes)]
    ++ [N || N <- Nodes, not lists:member(N, RunningNodes)].

-spec queues_not_fully_az_covered() -> [#{queue        := rabbit_types:r(queue),
                                          member_nodes := [node()],
                                          member_azs   := [binary() | undefined],
                                          missing_azs  := [binary()]}].
%% Uses each queue's own effective tag key (policy, then queue argument, then
%% cluster-wide configuration), matching the precedence used by the actual
%% member placement logic in rabbit_queue_location.
queues_not_fully_az_covered() ->
    Nodes = rabbit_nodes:list_members(),
    lists:filtermap(
      fun(Q) ->
              case amqqueue:get_type(Q) =:= rabbit_quorum_queue andalso
                   rabbit_queue_location:placement_tag_key(Q) of
                  undefined -> false;
                  false -> false;
                  TagKey -> queue_az_coverage(Q, Nodes, TagKey)
              end
      end, rabbit_amqqueue:list()).

-spec queues_not_fully_az_covered(binary() | undefined) ->
    [#{queue        := rabbit_types:r(queue),
       member_nodes := [node()],
       member_azs   := [binary() | undefined],
       missing_azs  := [binary()]}].
queues_not_fully_az_covered(undefined) ->
    [];
queues_not_fully_az_covered(TagKey) ->
    Nodes = rabbit_nodes:list_members(),
    lists:filtermap(
      fun(Q) ->
              amqqueue:get_type(Q) =:= rabbit_quorum_queue andalso
              queue_az_coverage(Q, Nodes, TagKey)
      end, rabbit_amqqueue:list()).

%% Returns `{true, Coverage}' if Q is missing members in one or more AZs,
%% `false' otherwise.
queue_az_coverage(Q, Nodes, TagKey) ->
    NodeToAZ = ?MODULE:node_tags_for_nodes(Nodes, TagKey),
    AllAZs = lists:usort([AZ || _ := AZ <- NodeToAZ, AZ =/= undefined]),
    MemberNodes = rabbit_quorum_queue:get_replicas(Q),
    MemberAZs = lists:usort([maps:get(N, NodeToAZ, undefined)
                             || N <- MemberNodes, maps:is_key(N, NodeToAZ)]),
    case AllAZs -- MemberAZs of
        [] ->
            false;
        MissingAZs ->
            {true, #{queue => amqqueue:get_name(Q),
                     member_nodes => MemberNodes,
                     member_azs => MemberAZs,
                     missing_azs => MissingAZs}}
    end.

%% Uses each queue's own effective tag key (policy, then queue argument, then
%% cluster-wide configuration), matching the precedence used by the actual
%% member placement logic in rabbit_queue_location. Since different queues
%% may resolve to different tag keys, nodes with no members are not included.
-spec member_distribution_per_az() -> #{binary() | undefined => #{node() => non_neg_integer()}}.
member_distribution_per_az() ->
    Nodes = rabbit_nodes:list_running(),
    lists:foldl(
      fun(Q, Acc) ->
              case amqqueue:get_type(Q) =:= rabbit_quorum_queue andalso
                   rabbit_queue_location:placement_tag_key(Q) of
                  undefined -> Acc;
                  false -> Acc;
                  TagKey ->
                      NodeToAZ = ?MODULE:node_tags_for_nodes(Nodes, TagKey),
                      add_replicas_to_az_counts(Q, NodeToAZ, Acc)
              end
      end, #{}, rabbit_amqqueue:list()).

-spec member_distribution_per_az(binary() | undefined) ->
    #{binary() | undefined => #{node() => non_neg_integer()}}.
member_distribution_per_az(undefined) ->
    #{};
member_distribution_per_az(TagKey) ->
    Nodes = rabbit_nodes:list_running(),
    NodeToAZ = ?MODULE:node_tags_for_nodes(Nodes, TagKey),
    Acc0 = maps:fold(
             fun(Node, AZ, Acc) ->
                     maps:update_with(AZ, fun(M) -> M#{Node => 0} end, #{Node => 0}, Acc)
             end, #{}, NodeToAZ),
    lists:foldl(
      fun(Q, Acc) ->
              case amqqueue:get_type(Q) of
                  rabbit_quorum_queue -> add_replicas_to_az_counts(Q, NodeToAZ, Acc);
                  _ -> Acc
              end
      end, Acc0, rabbit_amqqueue:list()).

add_replicas_to_az_counts(Q, NodeToAZ, Acc) ->
    lists:foldl(
      fun(N, Acc2) ->
              AZ = maps:get(N, NodeToAZ, undefined),
              maps:update_with(
                AZ,
                fun(M) -> maps:update_with(N, fun(C) -> C + 1 end, 1, M) end,
                #{N => 1},
                Acc2)
      end, Acc, rabbit_quorum_queue:get_replicas(Q)).

%% Returns #{Node => TagValue | undefined} for all Nodes.
%% Uses rabbit_db_node_metadata (stored in Khepri) to retrieve node tags without RPCs.
-spec node_tags_for_nodes([node()], binary()) -> #{node() => binary() | undefined}.
node_tags_for_nodes(Nodes, TagKey) ->
    maps:from_list([
      begin
          Metadata = rabbit_db_node_metadata:get(N),
          Tags = maps:get(node_tags, Metadata, []),
          {N, proplists:get_value(TagKey, Tags, undefined)}
      end || N <- Nodes]).

-spec node() -> node().
node() -> erlang:node().

-spec nodes() -> [node()].
nodes() -> erlang:nodes().

%% OTP29 added rand:shuffle but we need to support older OTP versions
shuffle(L0) when is_list(L0) ->
    L1 = [{rand:uniform(), E} || E <- L0],
    L = lists:keysort(1, L1),
    [E || {_, E} <- L].
