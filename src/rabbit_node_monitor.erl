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

-module(rabbit_node_monitor).

-behaviour(gen_server).

-export([start_link/0]).
-export([running_nodes_filename/0,
         cluster_status_filename/0, prepare_cluster_status_files/0,
         write_cluster_status/1, read_cluster_status/0,
         update_cluster_status/0, reset_cluster_status/0]).
-export([notify_node_up/0, notify_joined_cluster/0, notify_left_cluster/1]).
-export([partitions/0, subscribe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(RABBIT_UP_RPC_TIMEOUT, 2000).
-define(RABBIT_DOWN_PING_INTERVAL, 1000).

-record(state, {monitors, partitions, subscribers, down_ping_timer, autoheal}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).

-spec(running_nodes_filename/0 :: () -> string()).
-spec(cluster_status_filename/0 :: () -> string()).
-spec(prepare_cluster_status_files/0 :: () -> 'ok').
-spec(write_cluster_status/1 :: (rabbit_mnesia:cluster_status()) -> 'ok').
-spec(read_cluster_status/0 :: () -> rabbit_mnesia:cluster_status()).
-spec(update_cluster_status/0 :: () -> 'ok').
-spec(reset_cluster_status/0 :: () -> 'ok').

-spec(notify_node_up/0 :: () -> 'ok').
-spec(notify_joined_cluster/0 :: () -> 'ok').
-spec(notify_left_cluster/1 :: (node()) -> 'ok').

-spec(partitions/0 :: () -> {node(), [node()]}).
-spec(subscribe/1 :: (pid()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% Start
%%----------------------------------------------------------------------------

start_link() -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%----------------------------------------------------------------------------
%% Cluster file operations
%%----------------------------------------------------------------------------

%% The cluster file information is kept in two files.  The "cluster
%% status file" contains all the clustered nodes and the disc nodes.
%% The "running nodes file" contains the currently running nodes or
%% the running nodes at shutdown when the node is down.
%%
%% We strive to keep the files up to date and we rely on this
%% assumption in various situations. Obviously when mnesia is offline
%% the information we have will be outdated, but it cannot be
%% otherwise.

running_nodes_filename() ->
    filename:join(rabbit_mnesia:dir(), "nodes_running_at_shutdown").

cluster_status_filename() ->
    rabbit_mnesia:dir() ++ "/cluster_nodes.config".

prepare_cluster_status_files() ->
    rabbit_mnesia:ensure_mnesia_dir(),
    Corrupt = fun(F) -> throw({error, corrupt_cluster_status_files, F}) end,
    RunningNodes1 = case try_read_file(running_nodes_filename()) of
                        {ok, [Nodes]} when is_list(Nodes) -> Nodes;
                        {ok, Other}                       -> Corrupt(Other);
                        {error, enoent}                   -> []
                    end,
    ThisNode = [node()],
    %% The running nodes file might contain a set or a list, in case
    %% of the legacy file
    RunningNodes2 = lists:usort(ThisNode ++ RunningNodes1),
    {AllNodes1, WantDiscNode} =
        case try_read_file(cluster_status_filename()) of
            {ok, [{AllNodes, DiscNodes0}]} ->
                {AllNodes, lists:member(node(), DiscNodes0)};
            {ok, [AllNodes0]} when is_list(AllNodes0) ->
                {legacy_cluster_nodes(AllNodes0),
                 legacy_should_be_disc_node(AllNodes0)};
            {ok, Files} ->
                Corrupt(Files);
            {error, enoent} ->
                {legacy_cluster_nodes([]), true}
        end,
    AllNodes2 = lists:usort(AllNodes1 ++ RunningNodes2),
    DiscNodes = case WantDiscNode of
                    true  -> ThisNode;
                    false -> []
                end,
    ok = write_cluster_status({AllNodes2, DiscNodes, RunningNodes2}).

write_cluster_status({All, Disc, Running}) ->
    ClusterStatusFN = cluster_status_filename(),
    Res = case rabbit_file:write_term_file(ClusterStatusFN, [{All, Disc}]) of
              ok ->
                  RunningNodesFN = running_nodes_filename(),
                  {RunningNodesFN,
                   rabbit_file:write_term_file(RunningNodesFN, [Running])};
              E1 = {error, _} ->
                  {ClusterStatusFN, E1}
          end,
    case Res of
        {_, ok}           -> ok;
        {FN, {error, E2}} -> throw({error, {could_not_write_file, FN, E2}})
    end.

read_cluster_status() ->
    case {try_read_file(cluster_status_filename()),
          try_read_file(running_nodes_filename())} of
        {{ok, [{All, Disc}]}, {ok, [Running]}} when is_list(Running) ->
            {All, Disc, Running};
        {Stat, Run} ->
            throw({error, {corrupt_or_missing_cluster_files, Stat, Run}})
    end.

update_cluster_status() ->
    {ok, Status} = rabbit_mnesia:cluster_status_from_mnesia(),
    write_cluster_status(Status).

reset_cluster_status() ->
    write_cluster_status({[node()], [node()], [node()]}).

%%----------------------------------------------------------------------------
%% Cluster notifications
%%----------------------------------------------------------------------------

notify_node_up() ->
    Nodes = rabbit_mnesia:cluster_nodes(running) -- [node()],
    gen_server:abcast(Nodes, ?SERVER,
                      {node_up, node(), rabbit_mnesia:node_type()}),
    %% register other active rabbits with this rabbit
    DiskNodes = rabbit_mnesia:cluster_nodes(disc),
    [gen_server:cast(?SERVER, {node_up, N, case lists:member(N, DiskNodes) of
                                               true  -> disc;
                                               false -> ram
                                           end}) || N <- Nodes],
    ok.

notify_joined_cluster() ->
    Nodes = rabbit_mnesia:cluster_nodes(running) -- [node()],
    gen_server:abcast(Nodes, ?SERVER,
                      {joined_cluster, node(), rabbit_mnesia:node_type()}),
    ok.

notify_left_cluster(Node) ->
    Nodes = rabbit_mnesia:cluster_nodes(running),
    gen_server:abcast(Nodes, ?SERVER, {left_cluster, Node}),
    ok.

%%----------------------------------------------------------------------------
%% Server calls
%%----------------------------------------------------------------------------

partitions() ->
    gen_server:call(?SERVER, partitions, infinity).

subscribe(Pid) ->
    gen_server:cast(?SERVER, {subscribe, Pid}).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    %% We trap exits so that the supervisor will not just kill us. We
    %% want to be sure that we are not going to be killed while
    %% writing out the cluster status files - bad things can then
    %% happen.
    process_flag(trap_exit, true),
    {ok, _} = mnesia:subscribe(system),
    {ok, #state{monitors    = pmon:new(),
                subscribers = pmon:new(),
                partitions  = [],
                autoheal    = not_healing}}.

handle_call(partitions, _From, State = #state{partitions = Partitions}) ->
    {reply, {node(), Partitions}, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

%% Note: when updating the status file, we can't simply write the
%% mnesia information since the message can (and will) overtake the
%% mnesia propagation.
handle_cast({node_up, Node, NodeType},
            State = #state{monitors = Monitors}) ->
    case pmon:is_monitored({rabbit, Node}, Monitors) of
        true  -> {noreply, State};
        false -> rabbit_log:info("rabbit on node ~p up~n", [Node]),
                 {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
                 write_cluster_status({add_node(Node, AllNodes),
                                       case NodeType of
                                           disc -> add_node(Node, DiscNodes);
                                           ram  -> DiscNodes
                                       end,
                                       add_node(Node, RunningNodes)}),
                 ok = handle_live_rabbit(Node),
                 {noreply, State#state{
                             monitors = pmon:monitor({rabbit, Node}, Monitors)}}
    end;
handle_cast({joined_cluster, Node, NodeType}, State) ->
    {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
    write_cluster_status({add_node(Node, AllNodes),
                          case NodeType of
                              disc -> add_node(Node, DiscNodes);
                              ram  -> DiscNodes
                          end,
                          RunningNodes}),
    {noreply, State};
handle_cast({left_cluster, Node}, State) ->
    {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
    write_cluster_status({del_node(Node, AllNodes), del_node(Node, DiscNodes),
                          del_node(Node, RunningNodes)}),
    {noreply, State};
handle_cast({subscribe, Pid}, State = #state{subscribers = Subscribers}) ->
    {noreply, State#state{subscribers = pmon:monitor(Pid, Subscribers)}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, {rabbit, Node}, _Reason},
            State = #state{monitors = Monitors, subscribers = Subscribers}) ->
    rabbit_log:info("rabbit on node ~p down~n", [Node]),
    {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
    write_cluster_status({AllNodes, DiscNodes, del_node(Node, RunningNodes)}),
    ok = handle_dead_rabbit(Node),
    [P ! {node_down, Node} || P <- pmon:monitored(Subscribers)],
    {noreply, handle_dead_rabbit_state(
                Node,
                State#state{monitors = pmon:erase({rabbit, Node}, Monitors)})};

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state{subscribers = Subscribers}) ->
    {noreply, State#state{subscribers = pmon:erase(Pid, Subscribers)}};

handle_info({mnesia_system_event,
             {inconsistent_database, running_partitioned_network, Node}},
            State = #state{partitions = Partitions,
                           monitors   = Monitors}) ->
    %% We will not get a node_up from this node - yet we should treat it as
    %% up (mostly).
    State1 = case pmon:is_monitored({rabbit, Node}, Monitors) of
                 true  -> State;
                 false -> State#state{
                            monitors = pmon:monitor({rabbit, Node}, Monitors)}
             end,
    ok = handle_live_rabbit(Node),
    State2 = case application:get_env(rabbit, cluster_partition_handling) of
                 {ok, autoheal} -> case all_nodes_up() of
                                       true  -> autoheal(State1);
                                       false -> State1
                                   end;
                 _              -> State1
             end,
    Partitions1 = ordsets:to_list(
                    ordsets:add_element(Node, ordsets:from_list(Partitions))),
    {noreply, State2#state{partitions = Partitions1}};

handle_info({autoheal_request_winner, Node},
            State = #state{autoheal   = not_healing,
                           partitions = Partitions}) ->
    case all_nodes_up() of
        false -> {noreply, State};
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
                 Autoheal = {wait_for_winner_reqs, Partitioned1, Partitioned1},
                 handle_info({autoheal_request_winner, Node},
                             State#state{autoheal = Autoheal})
    end;

handle_info({autoheal_request_winner, Node},
            State = #state{autoheal   = {wait_for_winner_reqs, [Node], Notify},
                           partitions = Partitions}) ->
    AllPartitions = all_partitions(Partitions),
    io:format("all partitions: ~p~n", [AllPartitions]),
    Winner = autoheal_select_winner(AllPartitions),
    rabbit_log:info("Autoheal request winner from ~p~n"
                    "  Partitions were determined to be ~p~n"
                    "  Winner is ~p~n", [Node, AllPartitions, Winner]),
    [{?MODULE, N} ! {autoheal_winner, Winner} || N <- Notify],
    {noreply, State#state{autoheal = wait_for_winner}};

handle_info({autoheal_request_winner, Node},
            State = #state{autoheal = {wait_for_winner_reqs, Nodes, Notify}}) ->
    rabbit_log:info("Autoheal request winner from ~p~n", [Node]),
    {noreply, State#state{autoheal = {wait_for_winner_reqs,
                                      Nodes -- [Node], Notify}}};

handle_info({autoheal_winner, Winner},
            State = #state{autoheal   = wait_for_winner,
                           partitions = Partitions}) ->
    io:format("got winner ~p~n", [[Winner, Partitions]]),
    case lists:member(Winner, Partitions) of
        false -> case node() of
                     Winner -> rabbit_log:info(
                                 "Autoheal: waiting for nodes to stop: ~p~n",
                                 [Partitions]),
                               {noreply,
                                State#state{autoheal = {wait_for, Partitions,
                                                        Partitions}}};
                     _      -> rabbit_log:info(
                                 "Autoheal: nothing to do~n", []),
                               {noreply, State#state{autoheal = not_healing}}
                 end;
        true  -> autoheal_restart(Winner),
                 {noreply, State}
    end;

handle_info({autoheal_winner, _Winner}, State) ->
    %% ignore, we already cancelled the autoheal process
    {noreply, State};

handle_info({autoheal_node_stopped, Node},
            State = #state{autoheal = {wait_for, [Node], Notify}}) ->
    rabbit_log:info("Autoheal: final node has stopped, starting...~n",[]),
    [{rabbit_outside_app_process, N} ! autoheal_safe_to_start || N <- Notify],
    {noreply, State#state{autoheal = not_healing}};

handle_info({autoheal_node_stopped, Node},
            State = #state{autoheal = {wait_for, WaitFor, Notify}}) ->
    {noreply, State#state{autoheal = {wait_for, WaitFor -- [Node], Notify}}};

handle_info({autoheal_node_stopped, _Node}, State) ->
    %% ignore, we already cancelled the autoheal process
    {noreply, State};

handle_info(ping_nodes, State) ->
    %% We ping nodes when some are down to ensure that we find out
    %% about healed partitions quickly. We ping all nodes rather than
    %% just the ones we know are down for simplicity; it's not expensive
    %% to ping the nodes that are up, after all.
    State1 = State#state{down_ping_timer = undefined},
    Self = self(),
    %% all_nodes_up() both pings all the nodes and tells us if we need to again.
    %%
    %% We ping in a separate process since in a partition it might
    %% take some noticeable length of time and we don't want to block
    %% the node monitor for that long.
    spawn_link(fun () ->
                       case all_nodes_up() of
                           true  -> ok;
                           false -> Self ! ping_again
                       end
               end),
    {noreply, State1};

handle_info(ping_again, State) ->
    {noreply, ensure_ping_timer(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    rabbit_misc:stop_timer(State, #state.down_ping_timer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Functions that call the module specific hooks when nodes go up/down
%%----------------------------------------------------------------------------

%% TODO: This may turn out to be a performance hog when there are lots
%% of nodes.  We really only need to execute some of these statements
%% on *one* node, rather than all of them.
handle_dead_rabbit(Node) ->
    ok = rabbit_networking:on_node_down(Node),
    ok = rabbit_amqqueue:on_node_down(Node),
    ok = rabbit_alarm:on_node_down(Node),
    ok = rabbit_mnesia:on_node_down(Node),
    case application:get_env(rabbit, cluster_partition_handling) of
        {ok, pause_minority} ->
            case majority() of
                true  -> ok;
                false -> await_cluster_recovery()
            end;
        {ok, ignore} ->
            ok;
        {ok, autoheal} ->
            ok;
        {ok, Term} ->
            rabbit_log:warning("cluster_partition_handling ~p unrecognised, "
                               "assuming 'ignore'~n", [Term]),
            ok
    end,
    ok.

majority() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    length(alive_nodes(Nodes)) / length(Nodes) > 0.5.

all_nodes_up() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    length(alive_nodes(Nodes)) =:= length(Nodes).

%% mnesia:system_info(db_nodes) (and hence
%% rabbit_mnesia:cluster_nodes(running)) does not give reliable results
%% when partitioned.
alive_nodes() -> alive_nodes(rabbit_mnesia:cluster_nodes(all)).

alive_nodes(Nodes) -> [N || N <- Nodes, pong =:= net_adm:ping(N)].

alive_rabbit_nodes() ->
    [N || N <- alive_nodes(), rabbit_nodes:is_process_running(N, rabbit)].

await_cluster_recovery() ->
    rabbit_log:warning("Cluster minority status detected - awaiting recovery~n",
                       []),
    Nodes = rabbit_mnesia:cluster_nodes(all),
    run_outside_applications(fun () ->
                                     rabbit:stop(),
                                     wait_for_cluster_recovery(Nodes)
                             end).

run_outside_applications(Fun) ->
    spawn(fun () ->
                  %% If our group leader is inside an application we are about
                  %% to stop, application:stop/1 does not return.
                  group_leader(whereis(init), self()),
                  %% Ensure only one such process at a time, will
                  %% exit(badarg) (harmlessly) if one is already running
                  register(rabbit_outside_app_process, self()),
                  Fun()
          end).

wait_for_cluster_recovery(Nodes) ->
    case majority() of
        true  -> rabbit:start();
        false -> timer:sleep(?RABBIT_DOWN_PING_INTERVAL),
                 wait_for_cluster_recovery(Nodes)
    end.

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
autoheal(State = #state{autoheal = not_healing}) ->
    [Leader | _] = lists:usort(rabbit_mnesia:cluster_nodes(all)),
    rabbit_log:info("Autoheal: leader is ~p~n", [Leader]),
    {?MODULE, Leader} ! {autoheal_request_winner, node()},
    State#state{autoheal = case node() of
                               Leader -> not_healing;
                               _      -> wait_for_winner
                           end};
autoheal(State) ->
    State.

autoheal_select_winner(AllPartitions) ->
    {_, [Winner | _]} =
        hd(lists:reverse(
             lists:sort([{autoheal_value(P), P} || P <- AllPartitions]))),
    Winner.

autoheal_value(Partition) ->
    Connections = [Res || Node <- Partition,
                          Res <- [rpc:call(Node, rabbit_networking,
                                           connections_local, [])],
                          is_list(Res)],
    {length(lists:append(Connections)), length(Partition)}.

autoheal_restart(Winner) ->
    rabbit_log:warning(
      "Autoheal: we were selected to restart; winner is ~p~n", [Winner]),
    run_outside_applications(
      fun () ->
              MRef = erlang:monitor(process, {?MODULE, Winner}),
              rabbit:stop(),
              {?MODULE, Winner} ! {autoheal_node_stopped, node()},
              receive
                  {'DOWN', MRef, process, {?MODULE, Winner}, _Reason} -> ok;
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

handle_dead_rabbit_state(Node, State = #state{partitions = Partitions,
                                              autoheal   = Autoheal}) ->
    %% If we have been partitioned, and we are now in the only remaining
    %% partition, we no longer care about partitions - forget them. Note
    %% that we do not attempt to deal with individual (other) partitions
    %% going away. It's only safe to forget anything about partitions when
    %% there are no partitions.
    Partitions1 = case Partitions -- (Partitions -- alive_rabbit_nodes()) of
                      [] -> [];
                      _  -> Partitions
                  end,
    Autoheal1 = case Autoheal of
                    {wait_for, _Nodes, _Notify} ->
                        Autoheal;
                    not_healing ->
                        not_healing;
                    _ ->
                        rabbit_log:info(
                          "Autoheal: aborting - ~p went down~n", [Node]),
                        not_healing
                end,
    ensure_ping_timer(State#state{partitions = Partitions1,
                                  autoheal   = Autoheal1}).

ensure_ping_timer(State) ->
    rabbit_misc:ensure_timer(
      State, #state.down_ping_timer, ?RABBIT_DOWN_PING_INTERVAL, ping_nodes).

handle_live_rabbit(Node) ->
    ok = rabbit_alarm:on_node_up(Node),
    ok = rabbit_mnesia:on_node_up(Node).

%%--------------------------------------------------------------------
%% Internal utils
%%--------------------------------------------------------------------

try_read_file(FileName) ->
    case rabbit_file:read_term_file(FileName) of
        {ok, Term}      -> {ok, Term};
        {error, enoent} -> {error, enoent};
        {error, E}      -> throw({error, {cannot_read_file, FileName, E}})
    end.

legacy_cluster_nodes(Nodes) ->
    %% We get all the info that we can, including the nodes from
    %% mnesia, which will be there if the node is a disc node (empty
    %% list otherwise)
    lists:usort(Nodes ++ mnesia:system_info(db_nodes)).

legacy_should_be_disc_node(DiscNodes) ->
    DiscNodes == [] orelse lists:member(node(), DiscNodes).

add_node(Node, Nodes) -> lists:usort([Node | Nodes]).

del_node(Node, Nodes) -> Nodes -- [Node].
