%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_queue_location).

-include("amqqueue.hrl").

-export([queue_leader_locators/0,
         select_leader_and_followers/2]).

-define(QUEUES_LIMIT_FOR_LEAST_REPLICAS_SELECTION, 1_000).
-define(QUEUE_LEADER_LOCATORS, [<<"client-local">>, <<"random">>, <<"least-leaders">>]).
-define(DEFAULT_QUEUE_LEADER_LOCATOR, <<"client-local">>).

-type queue_leader_locator() :: nonempty_binary().

-spec queue_leader_locators() ->
    [queue_leader_locator()].
queue_leader_locators() ->
    ?QUEUE_LEADER_LOCATORS.

-spec select_leader_and_followers(amqqueue:amqqueue(), pos_integer()) ->
    {Leader :: node(), Followers :: [node()]}.
select_leader_and_followers(Q, Size)
  when (?amqqueue_is_quorum(Q) orelse ?amqqueue_is_stream(Q)) andalso is_integer(Size) ->
    QueueType = amqqueue:get_type(Q),
    GetQueues0 = get_queues_for_type(QueueType),
    {AllNodes, _DiscNodes, RunningNodes} = rabbit_mnesia:cluster_nodes(status),
    {Replicas, GetQueues} = select_replicas(Size, AllNodes, RunningNodes, GetQueues0),
    LeaderLocator = leader_locator(Q),
    Leader = leader_node(LeaderLocator, Replicas, RunningNodes, GetQueues),
    Followers = lists:delete(Leader, Replicas),
    {Leader, Followers}.

-spec select_replicas(pos_integer(), [node(),...], [node(),...], function()) ->
    {[node(),...], function()}.
select_replicas(Size, AllNodes, _, Fun)
  when length(AllNodes) =< Size ->
    {AllNodes, Fun};
select_replicas(Size, AllNodes, RunningNodes, GetQueues) ->
    %% Select nodes in the following order:
    %% 1.   Local node to have data locality for declaring client.
    %% 2.   Running nodes.
    %% 3.1. If there are few queues: Nodes with least replicas to have a "balanced" RabbitMQ cluster.
    %% 3.2. If there are many queues: Randomly to avoid expensive calculation of counting replicas
    %%      per node. Random replica selection is good enough for most use cases.
    Local = node(),
    true = lists:member(Local, AllNodes),
    RemoteNodes = lists:delete(Local, AllNodes),
    case rabbit_amqqueue:count() of
        Count when Count =< ?QUEUES_LIMIT_FOR_LEAST_REPLICAS_SELECTION ->
            Counters0 = maps:from_list([{N, 0} || N <- RemoteNodes]),
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
            {[Local | L], fun() -> Queues end};
        _ ->
            L0 = shuffle(RemoteNodes),
            L1 = lists:sort(fun(X, _Y) ->
                                    lists:member(X, RunningNodes)
                            end, L0),
            {L, _} = lists:split(Size - 1, L1),
            {[Local | L], GetQueues}
    end.

-spec leader_locator(amqqueue:amqqueue()) ->
    queue_leader_locator().
leader_locator(Q) ->
    case rabbit_queue_type_util:args_policy_lookup(
           <<"queue-leader-locator">>,
           fun (PolVal, _ArgVal) -> PolVal end,
           Q) of
        undefined ->
            case application:get_env(rabbit, queue_leader_locator) of
                {ok, Locator} ->
                    case lists:member(Locator, ?QUEUE_LEADER_LOCATORS) of
                        true ->
                            Locator;
                        false ->
                            ?DEFAULT_QUEUE_LEADER_LOCATOR
                    end;
                undefined ->
                    ?DEFAULT_QUEUE_LEADER_LOCATOR
            end;
        Val ->
            Val
    end.

-spec leader_node(queue_leader_locator(), [node(),...], [node(),...], function()) ->
    node().
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

potential_leaders(Replicas, RunningNodes) ->
    case lists:filter(fun(R) ->
                              lists:member(R, RunningNodes)
                      end, Replicas) of
        [] ->
            Replicas;
        RunningReplicas ->
            case rabbit_maintenance:filter_out_drained_nodes_local_read(RunningReplicas) of
                [] ->
                    %% All selected replica nodes are drained. Let's place the leader on a
                    %% drained node respecting the requested queue-leader-locator streategy.
                    RunningReplicas;
                Filtered ->
                    Filtered
            end
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

shuffle(L0) when is_list(L0) ->
    L1 = lists:map(fun(E) -> {rand:uniform(), E} end, L0),
    L = lists:keysort(1, L1),
    lists:map(fun({_, E}) -> E end, L).
