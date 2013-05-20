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
%% safe to start again. The winner and the leader are not necessarily
%% the same node.
%%
%% Possible states:
%%
%% not_healing
%%   - the default
%%
%% {winner_waiting, OutstandingStops, Notify}
%%   - we are the winner and are waiting for all losing nodes to stop
%%   before telling them they can restart
%%
%% restarting
%%   - we are restarting. Of course the node monitor immediately dies
%%   then so this state does not last long. We therefore send the
%%   autoheal_safe_to_start message to the rabbit_outside_app_process
%%   instead.

%%----------------------------------------------------------------------------

init() -> not_healing.

maybe_start(not_healing) ->
    case enabled() of
        true  -> [Leader | _] = lists:usort(rabbit_mnesia:cluster_nodes(all)),
                 send(Leader, {request_start, node()}),
                 rabbit_log:info("Autoheal request sent to ~p~n", [Leader]),
                 not_healing;
        false -> not_healing
    end;
maybe_start(State) ->
    State.

enabled() ->
    {ok, autoheal} =:= application:get_env(rabbit, cluster_partition_handling).

node_down(_Node, {winner_waiting, _Nodes, _Notify} = Autoheal) ->
    Autoheal;
node_down(_Node, not_healing) ->
    not_healing;
node_down(Node, _State) ->
    rabbit_log:info("Autoheal: aborting - ~p went down~n", [Node]),
    not_healing.

%% By receiving this message we become the leader
%% TODO should we try to debounce this?
handle_msg({request_start, Node},
           not_healing, Partitions) ->
    rabbit_log:info("Autoheal request received from ~p~n", [Node]),
    case rabbit_node_monitor:all_rabbit_nodes_up() of
        false -> not_healing;
        true  -> AllPartitions = all_partitions(Partitions),
                 {Winner, Losers} = make_decision(AllPartitions),
                 rabbit_log:info("Autoheal decision~n"
                                 "  * Partitions: ~p~n"
                                 "  * Winner:     ~p~n"
                                 "  * Losers:     ~p~n",
                                 [AllPartitions, Winner, Losers]),
                 send(Winner, {become_winner, Losers}),
                 [send(L, {winner_is, Winner}) || L <- Losers],
                 not_healing
    end;

handle_msg({request_start, Node},
           State, _Partitions) ->
    rabbit_log:info("Autoheal request received from ~p when in state ~p; "
                    "ignoring~n", [Node, State]),
    State;

handle_msg({become_winner, Losers},
           not_healing, _Partitions) ->
    rabbit_log:info("Autoheal: I am the winner, waiting for ~p to stop~n",
                    [Losers]),
    {winner_waiting, Losers, Losers};

handle_msg({become_winner, Losers},
           {winner_waiting, WaitFor, Notify}, _Partitions) ->
    rabbit_log:info("Autoheal: I am the winner, waiting additionally for "
                    "~p to stop~n", [Losers]),
    {winner_waiting, lists:usort(Losers ++ WaitFor),
     lists:usort(Losers ++ Notify)};

handle_msg({winner_is, Winner},
           not_healing, _Partitions) ->
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
      end),
    restarting;

%% This is the winner receiving its last notification that a node has
%% stopped - all nodes can now start again
handle_msg({node_stopped, Node},
           {winner_waiting, [Node], Notify}, _Partitions) ->
    rabbit_log:info("Autoheal: final node has stopped, starting...~n",[]),
    [{rabbit_outside_app_process, N} ! autoheal_safe_to_start || N <- Notify],
    not_healing;

handle_msg({node_stopped, Node},
           {winner_waiting, WaitFor, Notify}, _Partitions) ->
    {winner_waiting, WaitFor -- [Node], Notify};

handle_msg(_, restarting, _Partitions) ->
    %% ignore, we can contribute no further
    restarting;

handle_msg({node_stopped, _Node}, State, _Partitions) ->
    %% ignore, we already cancelled the autoheal process
    State.

%%----------------------------------------------------------------------------

send(Node, Msg) -> {?SERVER, Node} ! {autoheal_msg, Msg}.

make_decision(AllPartitions) ->
    Sorted = lists:sort([{partition_value(P), P} || P <- AllPartitions]),
    [[Winner | _] | Rest] = lists:reverse([P || {_, P} <- Sorted]),
    {Winner, lists:append(Rest)}.

partition_value(Partition) ->
    Connections = [Res || Node <- Partition,
                          Res <- [rpc:call(Node, rabbit_networking,
                                           connections_local, [])],
                          is_list(Res)],
    {length(lists:append(Connections)), length(Partition)}.

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
