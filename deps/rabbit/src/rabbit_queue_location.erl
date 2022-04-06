%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_location).

-include("amqqueue.hrl").

-export([select_leader_and_followers/2]).

select_leader_and_followers(Q, Size)
  when (?amqqueue_is_quorum(Q) orelse ?amqqueue_is_stream(Q)) andalso is_integer(Size) ->
    QueueType = amqqueue:get_type(Q),
    GetQueues0 = get_queues_for_type(QueueType),
    {AllNodes, _DiscNodes, RunningNodes} = rabbit_mnesia:cluster_nodes(status),
    {Replicas, GetQueues} = select_replicas(Size, AllNodes, RunningNodes, GetQueues0),
    LeaderLocator = leader_locator(
                      rabbit_queue_type_util:args_policy_lookup(
                        <<"queue-leader-locator">>,
                        fun (PolVal, _ArgVal) ->
                                PolVal
                        end, Q)),
    Leader = leader_node(LeaderLocator, Replicas, RunningNodes, GetQueues),
    Followers = lists:delete(Leader, Replicas),
    {Leader, Followers}.

select_replicas(Size, AllNodes, _, Fun)
  when length(AllNodes) =< Size ->
    {AllNodes, Fun};
select_replicas(Size, _, RunningNodes, Fun)
  when length(RunningNodes) =:= Size ->
    {RunningNodes, Fun};
select_replicas(Size, AllNodes, RunningNodes, GetQueues) ->
    %% Select nodes in the following order:
    %% 1. local node (to have data locality for declaring client)
    %% 2. running nodes
    %% 3. nodes with least replicas (to have a "balanced" RabbitMQ cluster).
    Local = node(),
    true = lists:member(Local, AllNodes),
    true = lists:member(Local, RunningNodes),
    Counters0 = maps:from_list([{Node, 0} || Node <- lists:delete(Local, AllNodes)]),
    Queues = GetQueues(),
    Counters = lists:foldl(fun(Q, Acc) ->
                                   #{nodes := Nodes} = amqqueue:get_type_state(Q),
                                   lists:foldl(fun(N, A)
                                                     when is_map_key(N, A) ->
                                                       maps:update_with(N, fun(C) -> C+1 end, A);
                                                  (_, A) ->
                                                       A
                                               end, Acc, Nodes)
                           end, Counters0, Queues),
    L0 = maps:to_list(Counters),
    L1 = lists:sort(fun({N0, C0}, {N1, C1}) ->
                            case {lists:member(N0, RunningNodes),
                                  lists:member(N1, RunningNodes)} of
                                {true, false} ->
                                    true;
                                {false, true} ->
                                    false;
                                _ ->
                                    C0 =< C1
                            end
                    end, L0),
    {L2, _} = lists:split(Size - 1, L1),
    L = lists:map(fun({N, _}) -> N end, L2),
    {[Local | L], fun() -> Queues end}.

leader_locator(undefined) -> <<"client-local">>;
leader_locator(Val) -> Val.

leader_node(<<"client-local">>, _, _, _) ->
    node();
leader_node(<<"random">>, Nodes0, RunningNodes, _) ->
    Nodes = potential_leaders(Nodes0, RunningNodes),
    lists:nth(rand:uniform(length(Nodes)), Nodes);
leader_node(<<"least-leaders">>, Nodes0, RunningNodes, GetQueues)
  when is_function(GetQueues, 0) ->
    Nodes = potential_leaders(Nodes0, RunningNodes),
    Counters0 = maps:from_list([{N, 0} || N <- Nodes]),
    Counters = lists:foldl(fun(Q, Acc) ->
                                   case amqqueue:get_pid(Q) of
                                       {RaName, LeaderNode}
                                         when is_atom(RaName), is_atom(LeaderNode), is_map_key(LeaderNode, Acc) ->
                                           maps:update_with(LeaderNode, fun(C) -> C+1 end, Acc);
                                       StreamLeaderPid
                                         when is_pid(StreamLeaderPid), is_map_key(node(StreamLeaderPid), Acc) ->
                                           maps:update_with(node(StreamLeaderPid), fun(C) -> C+1 end, Acc);
                                       _ ->
                                           Acc
                                   end
                           end, Counters0, GetQueues()),
    {Node, _} = hd(lists:keysort(2, maps:to_list(Counters))),
    Node.

potential_leaders(Nodes, AllRunningNodes) ->
    RunningNodes = lists:filter(fun(N) ->
                                        lists:member(N, AllRunningNodes)
                                end, Nodes),
    case rabbit_maintenance:filter_out_drained_nodes_local_read(RunningNodes) of
        [] ->
            %% All running nodes are drained. Let's place the leader on a drained node
            %% respecting the requested queue-leader-locator streategy.
            RunningNodes;
        Filtered ->
            Filtered
    end.

%% Return a function so that queues are fetched lazily (i.e. only when needed,
%% and at most once when no amqqueue migration is going on).
get_queues_for_type(QueueType) ->
    fun() -> rabbit_amqqueue:list_with_possible_retry(
               fun() ->
                       mnesia:dirty_match_object(rabbit_queue,
                                                 amqqueue:pattern_match_on_type(QueueType))
               end)
    end.
