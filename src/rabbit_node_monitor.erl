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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_node_monitor).

-behaviour(gen_server).

-export([start_link/0]).
-export([running_nodes_filename/0,
         cluster_status_filename/0, prepare_cluster_status_files/0,
         write_cluster_status/1, read_cluster_status/0,
         update_cluster_status/0, reset_cluster_status/0]).
-export([notify_node_up/0, notify_joined_cluster/0, notify_left_cluster/1]).
-export([partitions/0, partitions/1, subscribe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

 %% Utils
-export([all_rabbit_nodes_up/0, run_outside_applications/1]).

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

-spec(partitions/0 :: () -> [node()]).
-spec(partitions/1 :: ([node()]) -> [{node(), [node()]}]).
-spec(subscribe/1 :: (pid()) -> 'ok').

-spec(all_rabbit_nodes_up/0 :: () -> boolean()).
-spec(run_outside_applications/1 :: (fun (() -> any())) -> pid()).

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
    {AllNodes1, DiscNodes} =
        case try_read_file(cluster_status_filename()) of
            {ok, [{AllNodes, DiscNodes0}]} ->
                {AllNodes, DiscNodes0};
            {ok, [AllNodes0]} when is_list(AllNodes0) ->
                {legacy_cluster_nodes(AllNodes0), legacy_disc_nodes(AllNodes0)};
            {ok, Files} ->
                Corrupt(Files);
            {error, enoent} ->
                LegacyNodes = legacy_cluster_nodes([]),
                {LegacyNodes, LegacyNodes}
        end,
    AllNodes2 = lists:usort(AllNodes1 ++ RunningNodes2),
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

partitions(Nodes) ->
    {Replies, _} = gen_server:multi_call(Nodes, ?SERVER, partitions, infinity),
    Replies.

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
    net_kernel:monitor_nodes(true),
    {ok, _} = mnesia:subscribe(system),
    {ok, #state{monitors    = pmon:new(),
                subscribers = pmon:new(),
                partitions  = [],
                autoheal    = rabbit_autoheal:init()}}.

handle_call(partitions, _From, State = #state{partitions = Partitions}) ->
    {reply, Partitions, State};

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

handle_info({nodedown, Node}, State) ->
    ok = handle_dead_node(Node),
    {noreply, State};

handle_info({mnesia_system_event,
             {inconsistent_database, running_partitioned_network, Node}},
            State = #state{partitions = Partitions,
                           monitors   = Monitors,
                           autoheal   = AState}) ->
    %% We will not get a node_up from this node - yet we should treat it as
    %% up (mostly).
    State1 = case pmon:is_monitored({rabbit, Node}, Monitors) of
                 true  -> State;
                 false -> State#state{
                            monitors = pmon:monitor({rabbit, Node}, Monitors)}
             end,
    ok = handle_live_rabbit(Node),
    Partitions1 = ordsets:to_list(
                    ordsets:add_element(Node, ordsets:from_list(Partitions))),
    {noreply, State1#state{partitions = Partitions1,
                           autoheal   = rabbit_autoheal:maybe_start(AState)}};

handle_info({autoheal_msg, Msg}, State = #state{autoheal   = AState,
                                                partitions = Partitions}) ->
    AState1 = rabbit_autoheal:handle_msg(Msg, AState, Partitions),
    {noreply, State#state{autoheal = AState1}};

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
    ok.

handle_dead_node(_Node) ->
    %% In general in rabbit_node_monitor we care about whether the
    %% rabbit application is up rather than the node; we do this so
    %% that we can respond in the same way to "rabbitmqctl stop_app"
    %% and "rabbitmqctl stop" as much as possible.
    %%
    %% However, for pause_minority mode we can't do this, since we
    %% depend on looking at whether other nodes are up to decide
    %% whether to come back up ourselves - if we decide that based on
    %% the rabbit application we would go down and never come back.
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
    end.

await_cluster_recovery() ->
    rabbit_log:warning("Cluster minority status detected - awaiting recovery~n",
                       []),
    Nodes = rabbit_mnesia:cluster_nodes(all),
    run_outside_applications(fun () ->
                                     rabbit:stop(),
                                     wait_for_cluster_recovery(Nodes)
                             end),
    ok.

run_outside_applications(Fun) ->
    spawn(fun () ->
                  %% If our group leader is inside an application we are about
                  %% to stop, application:stop/1 does not return.
                  group_leader(whereis(init), self()),
                  %% Ensure only one such process at a time, the
                  %% exit(badarg) is harmless if one is already running
                  try register(rabbit_outside_app_process, self()) of
                      true           -> Fun()
                  catch error:badarg -> ok
                  end
          end).

wait_for_cluster_recovery(Nodes) ->
    case majority() of
        true  -> rabbit:start();
        false -> timer:sleep(?RABBIT_DOWN_PING_INTERVAL),
                 wait_for_cluster_recovery(Nodes)
    end.

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
    ensure_ping_timer(
      State#state{partitions = Partitions1,
                  autoheal   = rabbit_autoheal:node_down(Node, Autoheal)}).

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

legacy_disc_nodes(AllNodes) ->
    case AllNodes == [] orelse lists:member(node(), AllNodes) of
        true  -> [node()];
        false -> []
    end.

add_node(Node, Nodes) -> lists:usort([Node | Nodes]).

del_node(Node, Nodes) -> Nodes -- [Node].

%%--------------------------------------------------------------------

%% mnesia:system_info(db_nodes) (and hence
%% rabbit_mnesia:cluster_nodes(running)) does not give reliable
%% results when partitioned. So we have a small set of replacement
%% functions here. "rabbit" in a function's name implies we test if
%% the rabbit application is up, not just the node.

majority() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    length(alive_nodes(Nodes)) / length(Nodes) > 0.5.

all_nodes_up() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    length(alive_nodes(Nodes)) =:= length(Nodes).

all_rabbit_nodes_up() ->
    Nodes = rabbit_mnesia:cluster_nodes(all),
    length(alive_rabbit_nodes(Nodes)) =:= length(Nodes).

alive_nodes(Nodes) -> [N || N <- Nodes, pong =:= net_adm:ping(N)].

alive_rabbit_nodes() -> alive_rabbit_nodes(rabbit_mnesia:cluster_nodes(all)).

alive_rabbit_nodes(Nodes) ->
    [N || N <- alive_nodes(Nodes), rabbit:is_running(N)].
