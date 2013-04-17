%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_autoheal).

-export([init/0, maybe_start/1, node_down/2, handle_msg/3]).

%% The named process we are running in.
-define(SERVER, rabbit_node_monitor).

%%----------------------------------------------------------------------------

%% In order to autoheal we want to:
%%
%% * Find the winning partition
%% * Stop all nodes in other partitions
%% * Wait for them all to be stopped
%% * Start them again
%%
%% To keep things simple, we assume all nodes are up. We don't start
%% unless all nodes are up, and if a node goes down we abandon the
%% whole process. To further keep things simple we also defer the
%% decision as to the winning node to the "leader" - arbitrarily
%% selected as the first node in the cluster.
%%
%% To coordinate the restarting nodes we pick a special node from the
%% winning partition - the "winner". Restarting nodes then stop, tell
%% the winner they have done so, and wait for it to tell them it is
%% safe to start again.
%%
%% The winner and the leader are not necessarily the same node! Since
%% the leader may end up restarting, we also make sure that it does
%% not announce its decision (and thus cue other nodes to restart)
%% until it has seen a request from every node that has experienced a
%% partition.

%%----------------------------------------------------------------------------

init() -> not_healing.

maybe_start(not_healing) ->
    case enabled() andalso rabbit_node_monitor:all_nodes_up() of
        true  -> [Leader | _] = lists:usort(rabbit_mnesia:cluster_nodes(all)),
                 rabbit_log:info("Autoheal: leader is ~p~n", [Leader]),
                 send(Leader, {request_winner, node()}),
                 case node() of
                     Leader -> not_healing;
                     _      -> wait_for_winner
                 end;
        false -> not_healing
    end;
maybe_start(State) ->
    State.

enabled() ->
    {ok, autoheal} =:= application:get_env(rabbit, cluster_partition_handling).

node_down(_Node, {wait_for, _Nodes, _Notify} = Autoheal) ->
    Autoheal;
node_down(_Node, not_healing) ->
    not_healing;
node_down(Node, _State) ->
    rabbit_log:info("Autoheal: aborting - ~p went down~n", [Node]),
    not_healing.

handle_msg({request_winner, Node},
           not_healing, Partitions) ->
    case rabbit_node_monitor:all_nodes_up() of
        false -> not_healing;
        true  -> Nodes = rabbit_mnesia:cluster_nodes(all),
                 Partitioned =
                     [N || N <- Nodes -- [node()],
                           P <- [begin
                                     {_, R} = rpc:call(N, rabbit_node_monitor,
                                                       partitions, []),
                                     R
                                 end],
                           is_list(P) andalso length(P) > 0],
                 Partitioned1 = case Partitions of
                                    [] -> Partitioned;
                                    _  -> [node() | Partitioned]
                                end,
                 rabbit_log:info(
                   "Autoheal leader start; partitioned nodes are ~p~n",
                   [Partitioned1]),
                 handle_msg({request_winner, Node},
                            {wait_for_winner_reqs, Partitioned1, Partitioned1},
                            Partitions)
    end;

handle_msg({request_winner, Node},
           {wait_for_winner_reqs, [Node], Notify}, Partitions) ->
    AllPartitions = all_partitions(Partitions),
    Winner = select_winner(AllPartitions),
    rabbit_log:info("Autoheal request winner from ~p~n"
                    "  Partitions were determined to be ~p~n"
                    "  Winner is ~p~n", [Node, AllPartitions, Winner]),
    [send(N, {winner, Winner}) || N <- Notify],
    wait_for_winner;

handle_msg({request_winner, Node},
           {wait_for_winner_reqs, Nodes, Notify}, _Partitions) ->
    rabbit_log:info("Autoheal request winner from ~p~n", [Node]),
    {wait_for_winner_reqs, Nodes -- [Node], Notify};

handle_msg({winner, Winner},
           wait_for_winner, Partitions) ->
    case lists:member(Winner, Partitions) of
        false -> case node() of
                     Winner -> rabbit_log:info(
                                 "Autoheal: waiting for nodes to stop: ~p~n",
                                 [Partitions]),
                               {wait_for, Partitions, Partitions};
                     _      -> rabbit_log:info(
                                 "Autoheal: nothing to do~n", []),
                               not_healing
                 end;
        true  -> restart_me(Winner),
                 restarting
    end;

handle_msg({winner, _Winner}, State, _Partitions) ->
    %% ignore, we already cancelled the autoheal process
    State;

handle_msg({node_stopped, Node},
           {wait_for, [Node], Notify}, _Partitions) ->
    rabbit_log:info("Autoheal: final node has stopped, starting...~n",[]),
    [{rabbit_outside_app_process, N} ! autoheal_safe_to_start || N <- Notify],
    not_healing;

handle_msg({node_stopped, Node},
           {wait_for, WaitFor, Notify}, _Partitions) ->
    {wait_for, WaitFor -- [Node], Notify};

handle_msg({node_stopped, _Node}, State, _Partitions) ->
    %% ignore, we already cancelled the autoheal process
    State.

%%----------------------------------------------------------------------------

send(Node, Msg) -> {?SERVER, Node} ! {autoheal_msg, Msg}.

select_winner(AllPartitions) ->
    {_, [Winner | _]} =
        hd(lists:reverse(
             lists:sort([{partition_value(P), P} || P <- AllPartitions]))),
    Winner.

partition_value(Partition) ->
    Connections = [Res || Node <- Partition,
                          Res <- [rpc:call(Node, rabbit_networking,
                                           connections_local, [])],
                          is_list(Res)],
    {length(lists:append(Connections)), length(Partition)}.

restart_me(Winner) ->
    rabbit_log:warning(
      "Autoheal: we were selected to restart; winner is ~p~n", [Winner]),
    rabbit_node_monitor:run_outside_applications(
      fun () ->
              MRef = erlang:monitor(process, {?SERVER, Winner}),
              rabbit:stop(),
              send(Winner, {node_stopped, node()}),
              receive
                  {'DOWN', MRef, process, {?SERVER, Winner}, _Reason} -> ok;
                  autoheal_safe_to_start                              -> ok
              end,
              erlang:demonitor(MRef, [flush]),
              rabbit:start()
      end).

%% We have our local understanding of what partitions exist; but we
%% only know which nodes we have been partitioned from, not which
%% nodes are partitioned from each other.
all_partitions(PartitionedWith) ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    Partitions = [{node(), PartitionedWith} |
                  [rpc:call(Node, rabbit_node_monitor, partitions, [])
                   || Node <- Nodes -- [node()]]],
    all_partitions(Partitions, [Nodes]).

all_partitions([], Partitions) ->
    Partitions;
all_partitions([{Node, CantSee} | Rest], Partitions) ->
    {[Containing], Others} =
        lists:partition(fun (Part) -> lists:member(Node, Part) end, Partitions),
    A = Containing -- CantSee,
    B = Containing -- A,
    Partitions1 = case {A, B} of
                      {[], _}  -> Partitions;
                      {_,  []} -> Partitions;
                      _        -> [A, B | Others]
                  end,
    all_partitions(Rest, Partitions1).
