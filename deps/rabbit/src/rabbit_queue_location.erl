%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_location).

-include("amqqueue.hrl").

-export([queue_leader_locators/0,
         select_leader_and_followers/2]).

-define(QUEUE_LEADER_LOCATORS_DEPRECATED, [<<"random">>, <<"least-leaders">>]).
-define(QUEUE_LEADER_LOCATORS, [<<"client-local">>, <<"balanced">>] ++ ?QUEUE_LEADER_LOCATORS_DEPRECATED).
-define(QUEUE_COUNT_START_RANDOM_SELECTION, 1_000).

-type queue_leader_locator() :: binary().

-spec queue_leader_locators() ->
    [queue_leader_locator()].
queue_leader_locators() ->
    ?QUEUE_LEADER_LOCATORS.

-spec select_leader_and_followers(amqqueue:amqqueue(), pos_integer()) ->
    {Leader :: node(), Followers :: [node()]}.
select_leader_and_followers(Q, Size)
  when (?amqqueue_is_quorum(Q) orelse ?amqqueue_is_stream(Q)) andalso is_integer(Size) ->
    AllNodes = rabbit_nodes:list_members(),
    RunningNodes = rabbit_nodes:filter_running(AllNodes),
    true = lists:member(node(), AllNodes),
    QueueType = amqqueue:get_type(Q),
    GetQueues0 = get_queues_for_type(QueueType),
    %% TODO do we always need the queue count? it can be expensive, check if it can be skipped!
    %% for example, for random
    QueueCount = rabbit_amqqueue:count(),
    QueueCountStartRandom = application:get_env(rabbit, queue_count_start_random_selection,
                                                ?QUEUE_COUNT_START_RANDOM_SELECTION),
    {Replicas, GetQueues} = select_replicas(Size, AllNodes, RunningNodes,
                                            QueueCount, QueueCountStartRandom, GetQueues0),
    LeaderLocator = leader_locator(Q),
    Leader = leader_node(LeaderLocator, Replicas, RunningNodes,
                         QueueCount, QueueCountStartRandom, GetQueues),
    Followers = lists:delete(Leader, Replicas),
    {Leader, Followers}.

-spec leader_locator(amqqueue:amqqueue()) ->
    queue_leader_locator().
leader_locator(Q) ->
    L = case rabbit_queue_type_util:args_policy_lookup(
               <<"queue-leader-locator">>,
               fun (PolVal, _ArgVal) -> PolVal end,
               Q) of
            undefined ->
                application:get_env(rabbit, queue_leader_locator, undefined);
            Val ->
                Val
        end,
    leader_locator0(L).

leader_locator0(<<"client-local">>) ->
    <<"client-local">>;
leader_locator0(<<"balanced">>) ->
    <<"balanced">>;
%% 'random' and 'least-leaders' are deprecated
leader_locator0(<<"random">>) ->
    <<"balanced">>;
leader_locator0(<<"least-leaders">>) ->
    <<"balanced">>;
leader_locator0(_) ->
    %% default
    <<"client-local">>.

-spec select_replicas(pos_integer(), [node(),...], [node(),...],
                      non_neg_integer(), non_neg_integer(), function()) ->
    {[node(),...], function()}.
select_replicas(Size, AllNodes, _, _, _, Fun)
  when length(AllNodes) =< Size ->
    {AllNodes, Fun};
%% Select nodes in the following order:
%% 1.   Local node to have data locality for declaring client.
%% 2.   Running nodes.
%% 3.1. If there are many queues: Randomly to avoid expensive calculation of counting replicas
%%      per node. Random replica selection is good enough for most use cases.
%% 3.2. If there are few queues: Nodes with least replicas to have a "balanced" RabbitMQ cluster.
select_replicas(Size, AllNodes, RunningNodes, QueueCount, QueueCountStartRandom, GetQueues)
  when QueueCount >= QueueCountStartRandom ->
    L0 = shuffle(lists:delete(node(), AllNodes)),
    L1 = lists:sort(fun(X, _Y) ->
                            lists:member(X, RunningNodes)
                    end, L0),
    {L, _} = lists:split(Size - 1, L1),
    {[node() | L], GetQueues};
select_replicas(Size, AllNodes, RunningNodes, _, _, GetQueues) ->
    Counters0 = maps:from_list([{N, 0} || N <- lists:delete(node(), AllNodes)]),
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
    {[node() | L], fun() -> Queues end}.

-spec leader_node(queue_leader_locator(), [node(),...], [node(),...],
                  non_neg_integer(), non_neg_integer(), function()) ->
    node().
leader_node(<<"client-local">>, _, _, _, _, _) ->
    node();
leader_node(<<"balanced">>, Nodes0, RunningNodes, QueueCount, QueueCountStartRandom, _)
  when QueueCount >= QueueCountStartRandom ->
    Nodes = potential_leaders(Nodes0, RunningNodes),
    lists:nth(rand:uniform(length(Nodes)), Nodes);
leader_node(<<"balanced">>, Nodes0, RunningNodes, _, _, GetQueues)
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

potential_leaders(Replicas, RunningNodes) ->
    case lists:filter(fun(R) ->
                              lists:member(R, RunningNodes)
                      end, Replicas) of
        [] ->
            Replicas;
        RunningReplicas ->
            case rabbit_maintenance:filter_out_drained_nodes_local_read(RunningReplicas) of
                [] ->
                    RunningReplicas;
                Filtered ->
                    Filtered
            end
    end.

%% Return a function so that queues are fetched lazily (i.e. only when needed,
%% and at most once when no amqqueue migration is going on).
get_queues_for_type(QueueType) ->
    fun () -> rabbit_amqqueue:list_by_type(QueueType) end.

shuffle(L0) when is_list(L0) ->
    L1 = lists:map(fun(E) -> {rand:uniform(), E} end, L0),
    L = lists:keysort(1, L1),
    lists:map(fun({_, E}) -> E end, L).
