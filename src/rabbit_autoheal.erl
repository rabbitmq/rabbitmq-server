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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_autoheal).

-export([init/0, maybe_start/1, rabbit_down/2, node_down/2, handle_msg/3]).

%% The named process we are running in.
-define(SERVER, rabbit_node_monitor).

-define(AUTOHEAL_INITIAL_PAUSE, 1000).

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
%% winning partition - the "winner". Restarting nodes then stop, and
%% wait for it to tell them it is safe to start again. The winner
%% determines that a node has stopped just by seeing if its rabbit app
%% stops - if a node stops for any other reason it just gets a message
%% it will ignore, and otherwise we carry on.
%%
%% The winner and the leader are not necessarily the same node.
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
%% {leader_waiting, OutstandingStops}
%%   - we are the leader, and have already assigned the winner and losers.
%%     We are neither but need to ignore further requests to autoheal.
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


%% This is the winner receiving its last notification that a node has
%% stopped - all nodes can now start again
rabbit_down(Node, {winner_waiting, [Node], Notify}) ->
    rabbit_log:info("Autoheal: final node has stopped, starting...~n",[]),
    winner_finish(Notify);

rabbit_down(Node, {winner_waiting, WaitFor, Notify}) ->
    {winner_waiting, WaitFor -- [Node], Notify};

rabbit_down(Node, {leader_waiting, [Node]}) ->
    not_healing;

rabbit_down(Node, {leader_waiting, WaitFor}) ->
    {leader_waiting, WaitFor -- [Node]};

rabbit_down(_Node, State) ->
    %% ignore, we already cancelled the autoheal process
    State.

node_down(_Node, not_healing) ->
    not_healing;

node_down(Node, {winner_waiting, _, Notify}) ->
    abort([Node], Notify);

node_down(Node, _State) ->
    rabbit_log:info("Autoheal: aborting - ~p went down~n", [Node]),
    not_healing.

%% By receiving this message we become the leader
%% TODO should we try to debounce this?
handle_msg({request_start, Node},
           not_healing, Partitions) ->
    rabbit_log:info("Autoheal request received from ~p~n", [Node]),
    rabbit_node_monitor:ping_all(),
    case rabbit_node_monitor:all_rabbit_nodes_up() of
        false -> not_healing;
        true  -> AllPartitions = all_partitions(Partitions),
                 {Winner, Losers} = make_decision(AllPartitions),
                 rabbit_log:info("Autoheal decision~n"
                                 "  * Partitions: ~p~n"
                                 "  * Winner:     ~p~n"
                                 "  * Losers:     ~p~n",
                                 [AllPartitions, Winner, Losers]),
                 [send(L, {winner_is, Winner}) || L <- Losers],
                 Continue = fun(Msg) ->
                                    handle_msg(Msg, not_healing, Partitions)
                            end,
                 case node() =:= Winner of
                     true  -> Continue({become_winner, Losers});
                     false -> send(Winner, {become_winner, Losers}), %% [0]
                              case lists:member(node(), Losers) of
                                  true  -> Continue({winner_is, Winner});
                                  false -> {leader_waiting, Losers}
                              end
                 end
    end;
%% [0] If we are a loser we will never receive this message - but it
%% won't stick in the mailbox as we are restarting anyway

handle_msg({request_start, Node},
           State, _Partitions) ->
    rabbit_log:info("Autoheal request received from ~p when in state ~p; "
                    "ignoring~n", [Node, State]),
    State;

handle_msg({become_winner, Losers},
           not_healing, _Partitions) ->
    rabbit_log:info("Autoheal: I am the winner, waiting for ~p to stop~n",
                    [Losers]),
    %% Since an autoheal will kick off after a partition heals it is
    %% possible that the partition has healed for the leader but we
    %% are marginally behind. In filter_already_down_losers/2 we will
    %% abort the autoheal if any nodes are down, and it's possible
    %% that we still think that some nodes are down but that they will
    %% come back in the very near future. So sleep briefly to give a
    %% chance of avoiding such a needlessly aborted autoheal.
    timer:sleep(?AUTOHEAL_INITIAL_PAUSE),
    filter_already_down_losers(Losers, Losers);

handle_msg({become_winner, Losers},
           {winner_waiting, WaitFor, Notify}, _Partitions) ->
    rabbit_log:info("Autoheal: I am the winner, waiting additionally for "
                    "~p to stop~n", [Losers]),
    filter_already_down_losers(lists:usort(Losers ++ WaitFor),
                               lists:usort(Losers ++ Notify));

handle_msg({winner_is, Winner},
           not_healing, _Partitions) ->
    rabbit_log:warning(
      "Autoheal: we were selected to restart; winner is ~p~n", [Winner]),
    rabbit_node_monitor:run_outside_applications(
      fun () ->
              MRef = erlang:monitor(process, {?SERVER, Winner}),
              rabbit:stop(),
              receive
                  {'DOWN', MRef, process, {?SERVER, Winner}, _Reason} -> ok;
                  autoheal_safe_to_start                              -> ok
              end,
              erlang:demonitor(MRef, [flush]),
              rabbit:start()
      end),
    restarting;

handle_msg(_, restarting, _Partitions) ->
    %% ignore, we can contribute no further
    restarting.

%%----------------------------------------------------------------------------

send(Node, Msg) -> {?SERVER, Node} ! {autoheal_msg, Msg}.

abort(Down, Notify) ->
    rabbit_log:info("Autoheal: aborting - ~p down~n", [Down]),
    %% Make sure any nodes waiting for us start - it won't necessarily
    %% heal the partition but at least they won't get stuck.
    winner_finish(Notify).

winner_finish(Notify) ->
    [{rabbit_outside_app_process, N} ! autoheal_safe_to_start || N <- Notify],
    not_healing.

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
                  rabbit_node_monitor:partitions(Nodes -- [node()])],
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

%% We could have received and ignored DOWN messages from some losers
%% before becoming the winner - check for already down nodes.
filter_already_down_losers(WantStopped, Notify) ->
    Down = WantStopped -- rabbit_node_monitor:alive_nodes(WantStopped),
    case Down of
        [] ->
            Running = rabbit_node_monitor:alive_rabbit_nodes(WantStopped),
            AlreadyStopped = WantStopped -- Running,
            case AlreadyStopped of
                [] -> ok;
                _  -> rabbit_log:info(
                        "Autoheal: ~p already down~n", [AlreadyStopped])
            end,
            case Running of
                [] -> rabbit_log:info(
                        "Autoheal: final node has stopped, starting...~n",[]),
                      winner_finish(Notify);
                _  -> {winner_waiting, Running, Notify}
            end;
        _ ->
            abort(Down, Notify)
    end.
