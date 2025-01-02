%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_node_monitor).

-behaviour(gen_server).

-export([start_link/0]).
-export([running_nodes_filename/0,
         cluster_status_filename/0, coordination_filename/0,
         stream_filename/0,
         quorum_filename/0, default_quorum_filename/0,
         classic_filename/0,
         prepare_cluster_status_files/0,
         write_cluster_status/1, read_cluster_status/0,
         update_cluster_status/0, reset_cluster_status/0]).
-export([notify_node_up/0, notify_joined_cluster/0, notify_left_cluster/1]).
-export([partitions/0, partitions/1, status/1, subscribe/1]).
-export([pause_partition_guard/0]).
-export([global_sync/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

 %% Utils
-export([all_rabbit_nodes_up/0, run_outside_applications/2, ping_all/0,
         alive_nodes/1, alive_rabbit_nodes/1]).

-define(SERVER, ?MODULE).
-define(NODE_REPLY_TIMEOUT, 5000).
-define(RABBIT_UP_RPC_TIMEOUT, 2000).
-define(RABBIT_DOWN_PING_INTERVAL, 1000).
-define(NODE_DISCONNECTION_TIMEOUT, 1000).

-record(state, {monitors, partitions, subscribers, down_ping_timer,
                keepalive_timer, autoheal, guid, node_guids}).

%%----------------------------------------------------------------------------
%% Start
%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

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

-spec running_nodes_filename() -> string().

running_nodes_filename() ->
    filename:join(rabbit:data_dir(), "nodes_running_at_shutdown").

-spec cluster_status_filename() -> string().

cluster_status_filename() ->
    filename:join(rabbit:data_dir(), "cluster_nodes.config").

coordination_filename() ->
    filename:join(rabbit:data_dir(), "coordination").

stream_filename() ->
    filename:join(rabbit:data_dir(), "stream").

quorum_filename() ->
    ra_env:data_dir().

default_quorum_filename() ->
    filename:join(rabbit:data_dir(), "quorum").

classic_filename() ->
    filename:join(rabbit:data_dir(), "msg_stores").

-spec prepare_cluster_status_files() -> 'ok' | no_return().

prepare_cluster_status_files() ->
    rabbit_db:ensure_dir_exists(),
    RunningNodes1 = case try_read_file(running_nodes_filename()) of
                        {ok, [Nodes]} when is_list(Nodes) -> Nodes;
                        {ok, Other}                       -> corrupt_cluster_status_files(Other);
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
                corrupt_cluster_status_files(Files);
            {error, enoent} ->
                LegacyNodes = legacy_cluster_nodes([]),
                {LegacyNodes, LegacyNodes}
        end,
    AllNodes2 = lists:usort(AllNodes1 ++ RunningNodes2),
    ok = write_cluster_status({AllNodes2, DiscNodes, RunningNodes2}).

-spec corrupt_cluster_status_files(any()) -> no_return().

corrupt_cluster_status_files(F) ->
    throw({error, corrupt_cluster_status_files, F}).

-spec write_cluster_status(rabbit_mnesia:cluster_status()) -> 'ok'.

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

-spec read_cluster_status() -> rabbit_mnesia:cluster_status().

read_cluster_status() ->
    case {try_read_file(cluster_status_filename()),
          try_read_file(running_nodes_filename())} of
        {{ok, [{All, Disc}]}, {ok, [Running]}} when is_list(Running) ->
            {All, Disc, Running};
        {Stat, Run} ->
            throw({error, {corrupt_or_missing_cluster_files, Stat, Run}})
    end.

-spec update_cluster_status() -> 'ok'.

update_cluster_status() ->
    {ok, Status} = rabbit_mnesia:cluster_status_from_mnesia(),
    write_cluster_status(Status).

-spec reset_cluster_status() -> 'ok'.

reset_cluster_status() ->
    write_cluster_status({[node()], [node()], [node()]}).

%%----------------------------------------------------------------------------
%% Cluster notifications
%%----------------------------------------------------------------------------

-spec notify_node_up() -> 'ok'.

notify_node_up() ->
    gen_server:cast(?SERVER, notify_node_up).

-spec notify_joined_cluster() -> 'ok'.

notify_joined_cluster() ->
    NewMember = node(),
    Nodes = alive_rabbit_nodes(rabbit_nodes:list_consistent_members()) -- [NewMember],
    gen_server:abcast(Nodes, ?SERVER,
                      {joined_cluster, node(), rabbit_db_cluster:node_type()}),

    ok.

-spec notify_left_cluster(node()) -> 'ok'.

notify_left_cluster(Node) ->
    Nodes = alive_rabbit_nodes(),
    gen_server:abcast(Nodes, ?SERVER, {left_cluster, Node}),
    ok.

%%----------------------------------------------------------------------------
%% Server calls
%%----------------------------------------------------------------------------

-spec partitions() -> [node()].

partitions() ->
    gen_server:call(?SERVER, partitions, infinity).

-spec partitions([node()]) -> [{node(), [node()]}].

partitions(Nodes) ->
    {Replies, _} = gen_server:multi_call(Nodes, ?SERVER, partitions, ?NODE_REPLY_TIMEOUT),
    Replies.

-spec status([node()]) -> {[{node(), [node()]}], [node()]}.

status(Nodes) ->
    gen_server:multi_call(Nodes, ?SERVER, status, infinity).

-spec subscribe(pid()) -> 'ok'.

subscribe(Pid) ->
    gen_server:cast(?SERVER, {subscribe, Pid}).

%%----------------------------------------------------------------------------
%% pause_minority/pause_if_all_down safety
%%----------------------------------------------------------------------------

%% If we are in a minority and pause_minority mode then a) we are
%% going to shut down imminently and b) we should not confirm anything
%% until then, since anything we confirm is likely to be lost.
%%
%% The same principles apply to a node which isn't part of the preferred
%% partition when we are in pause_if_all_down mode.
%%
%% We could confirm something by having an HA queue see the pausing
%% state (and fail over into it) before the node monitor stops us, or
%% by using unmirrored queues and just having them vanish (and
%% confirming messages as thrown away).
%%
%% So we have channels call in here before issuing confirms, to do a
%% lightweight check that we have not entered a pausing state.

-spec pause_partition_guard() -> 'ok' | 'pausing'.

pause_partition_guard() ->
    case get(pause_partition_guard) of
        not_pause_mode ->
            ok;
        undefined ->
            {ok, M} = application:get_env(rabbit, cluster_partition_handling),
            case M of
                pause_minority ->
                    pause_minority_guard([], ok);
                {pause_if_all_down, PreferredNodes, _} ->
                    pause_if_all_down_guard(PreferredNodes, [], ok);
                _ ->
                    put(pause_partition_guard, not_pause_mode),
                    ok
            end;
        {minority_mode, Nodes, LastState} ->
            pause_minority_guard(Nodes, LastState);
        {pause_if_all_down_mode, PreferredNodes, Nodes, LastState} ->
            pause_if_all_down_guard(PreferredNodes, Nodes, LastState)
    end.

pause_minority_guard(LastNodes, LastState) ->
    case nodes() of
        LastNodes -> LastState;
        _         -> NewState = case majority() of
                                    false -> pausing;
                                    true  -> ok
                                end,
                     put(pause_partition_guard,
                         {minority_mode, nodes(), NewState}),
                     NewState
    end.

pause_if_all_down_guard(PreferredNodes, LastNodes, LastState) ->
    case nodes() of
        LastNodes -> LastState;
        _         -> NewState = case in_preferred_partition(PreferredNodes) of
                                    false -> pausing;
                                    true  -> ok
                                end,
                     put(pause_partition_guard,
                         {pause_if_all_down_mode, PreferredNodes, nodes(),
                          NewState}),
                     NewState
    end.

%%----------------------------------------------------------------------------
%% "global" hang workaround.
%%----------------------------------------------------------------------------

%% This code works around a possible inconsistency in the "global"
%% state, causing global:sync/0 to never return.
%%
%%     1. A process is spawned.
%%     2. If after 10", global:sync() didn't return, the "global"
%%        state is parsed.
%%     3. If it detects that a sync is blocked for more than 10",
%%        the process sends fake nodedown/nodeup events to the two
%%        nodes involved (one local, one remote).
%%     4. Both "global" instances restart their synchronisation.
%%     5. global:sync() finally returns.
%%
%% FIXME: Remove this workaround, once we got rid of the change to
%% "dist_auto_connect" and fixed the bugs uncovered.

global_sync() ->
    Pid = spawn(fun workaround_global_hang/0),
    ok = global:sync(),
    Pid ! global_sync_done,
    ok.

workaround_global_hang() ->
    receive
        global_sync_done ->
            ok
    after 10_000 ->
            find_blocked_global_peers()
    end.

find_blocked_global_peers() ->
    Snapshot1 = snapshot_global_dict(),
    timer:sleep(10_000),
    Snapshot2 = snapshot_global_dict(),
    logger:debug("global's sync tags 10s ago: ~p~n"
                 "global's sync tags now: ~p",
                 [Snapshot1, Snapshot2]),
    find_blocked_global_peers1(Snapshot2, Snapshot1).

snapshot_global_dict() ->
    {status, _Pid, _Mod, [PDict | _]} = sys:get_status(global_name_server),
    [E || {{sync_tag_his, _}, _} = E <- PDict].

find_blocked_global_peers1([{{sync_tag_his, Peer}, _} = Item | Rest],
  OlderSnapshot) ->
    case lists:member(Item, OlderSnapshot) of
        true  -> unblock_global_peer(Peer);
        false -> ok
    end,
    find_blocked_global_peers1(Rest, OlderSnapshot);
find_blocked_global_peers1([], _) ->
    ok.

unblock_global_peer(PeerNode) ->
    ThisNode = node(),

    PeerState = erpc:call(PeerNode, sys, get_state, [global_name_server]),
    ThisState = sys:get_state(global_name_server),
    PeerToThisCid = connection_id(PeerState, ThisNode),
    ThisToPeerCid = connection_id(ThisState, PeerNode),

    logger:info(
      "global hang workaround: faking nodedown / nodeup between peer node ~s "
      "(connection ID to us: ~p) and our node ~s (connection ID to peer: ~p)",
      [PeerNode, PeerToThisCid, ThisNode, ThisToPeerCid]),
    logger:debug(
      "peer global state: ~tp~nour global state: ~tp",
      [erpc:call(PeerNode, sys, get_status, [global_name_server]),
       sys:get_status(global_name_server)]),

    {ThisDownMsg, ThisUpMsg} = messages(ThisToPeerCid, PeerNode),
    {PeerDownMsg, PeerUpMsg} = messages(PeerToThisCid, ThisNode),
    {global_name_server, ThisNode} ! ThisDownMsg,
    {global_name_server, PeerNode} ! PeerDownMsg,
    {global_name_server, ThisNode} ! ThisUpMsg,
    {global_name_server, PeerNode} ! PeerUpMsg,
    ok.

connection_id(State, Node) ->
    case element(3, State) of
        #{{connection_id, Node} := ConnectionId} ->
            ConnectionId;
        _ ->
            undefined
    end.

%% The nodedown and nodeup message format handled by global differs due to
%% https://github.com/erlang/otp/commit/9274e89857d294f85702e0d5d42fb196e8e12d6a
messages(undefined, Node) ->
    %% OTP < 25.1
    {{nodedown, Node},
     {nodeup, Node}};
messages(ConnectionId, Node) ->
    %% OTP >= 25.1
    Cid = #{connection_id => ConnectionId},
    {{nodedown, Node, Cid},
     {nodeup, Node, Cid}}.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    %% We trap exits so that the supervisor will not just kill us. We
    %% want to be sure that we are not going to be killed while
    %% writing out the cluster status files - bad things can then
    %% happen.
    process_flag(trap_exit, true),
    _ = net_kernel:monitor_nodes(true, [nodedown_reason]),
    Monitors = case rabbit_khepri:is_enabled() of
                   true ->
                       startup_log(),
                       pmon:new();
                   false ->
                       {ok, _} = mnesia:subscribe(system),

                       %% If the node has been restarted, Mnesia can trigger a
                       %% system notification before the monitor subscribes to
                       %% receive them. To avoid autoheal blocking due to the
                       %% inconsistent database event never arriving, we being
                       %% monitoring all running nodes as early as possible.
                       %% The rest of the monitoring ops will only be
                       %% triggered when notifications arrive.
                       Nodes = possibly_partitioned_nodes(),
                       startup_log(Nodes),
                       lists:foldl(
                         fun(Node, Monitors0) ->
                                 pmon:monitor({rabbit, Node}, Monitors0)
                         end, pmon:new(), Nodes)
               end,
    {ok, ensure_keepalive_timer(#state{monitors    = Monitors,
                                       subscribers = pmon:new(),
                                       partitions  = [],
                                       guid        = erlang:system_info(creation),
                                       node_guids  = maps:new(),
                                       autoheal    = rabbit_autoheal:init()})}.

handle_call(partitions, _From, State = #state{partitions = Partitions}) ->
    {reply, Partitions, State};

handle_call(status, _From, State = #state{partitions = Partitions}) ->
    {reply, [{partitions, Partitions},
             {nodes,      [node() | nodes()]}], State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(notify_node_up, State = #state{guid = GUID}) ->
    Nodes = rabbit_nodes:list_reachable() -- [node()],
    gen_server:abcast(Nodes, ?SERVER,
                      {node_up, node(), rabbit_db_cluster:node_type(), GUID}),
    %% register other active rabbits with this rabbit
    DiskNodes = rabbit_db_cluster:disc_members(),
    [gen_server:cast(?SERVER, {node_up, N, case lists:member(N, DiskNodes) of
                                               true  -> disc;
                                               false -> ram
                                           end}) || N <- Nodes],
    {noreply, State};

%%----------------------------------------------------------------------------
%% Partial partition detection
%%
%% Every node generates a GUID each time it starts, and announces that
%% GUID in 'node_up', with 'announce_guid' sent by return so the new
%% node knows the GUIDs of the others. These GUIDs are sent in all the
%% partial partition related messages to ensure that we ignore partial
%% partition messages from before we restarted (to avoid getting stuck
%% in a loop).
%%
%% When one node gets nodedown from another, it then sends
%% 'check_partial_partition' to all the nodes it still thinks are
%% alive. If any of those (intermediate) nodes still see the "down"
%% node as up, they inform it that this has happened. The original
%% node (in 'ignore', 'pause_if_all_down' or 'autoheal' mode) will then
%% disconnect from the intermediate node to "upgrade" to a full
%% partition.
%%
%% In pause_minority mode it will instead immediately pause until all
%% nodes come back. This is because the contract for pause_minority is
%% that nodes should never sit in a partitioned state - if it just
%% disconnected, it would become a minority, pause, realise it's not
%% in a minority any more, and come back, still partitioned (albeit no
%% longer partially).
%%
%% UPDATE: The GUID is actually not a GUID anymore - it is the value
%% returned by erlang:system_info(creation). This prevent false-positives
%% in a situation when a node is restarted (Erlang VM is up) but the rabbit
%% app is not yet up. The GUID was only generated and announced upon rabbit
%% startup; creation is available immediately. Therefore we can tell that
%% the node was restarted, before it announces the new value.
%% ----------------------------------------------------------------------------

handle_cast({node_up, Node, NodeType, GUID},
            State = #state{guid       = MyGUID,
                           node_guids = GUIDs}) ->
    cast(Node, {announce_guid, node(), MyGUID}),
    GUIDs1 = maps:put(Node, GUID, GUIDs),
    handle_cast({node_up, Node, NodeType}, State#state{node_guids = GUIDs1});

handle_cast({announce_guid, Node, GUID}, State = #state{node_guids = GUIDs}) ->
    {noreply, State#state{node_guids = maps:put(Node, GUID, GUIDs)}};

handle_cast({check_partial_partition, Node, Rep, NodeGUID, MyGUID, RepGUID},
            State = #state{guid       = MyGUID,
                           node_guids = GUIDs}) ->
    case lists:member(Node, rabbit_mnesia:cluster_nodes(running)) andalso
        maps:find(Node, GUIDs) =:= {ok, NodeGUID} of
        true  -> spawn_link( %%[1]
                   fun () ->
                           case rpc:call(Node, erlang, system_info, [creation]) of
                               {badrpc, _} -> ok;
                               NodeGUID ->
                                   rabbit_log:warning("Received a 'DOWN' message"
                                                      " from ~tp but still can"
                                                      " communicate with it ",
                                                      [Node]),
                                   cast(Rep, {partial_partition,
                                                         Node, node(), RepGUID});
                                _ ->
                                   rabbit_log:warning("Node ~tp was restarted", [Node]),
                                   ok
                           end
                   end),
                 ok;
        false -> ok
    end,
    {noreply, State};
%% [1] We checked that we haven't heard the node go down - but we
%% really should make sure we can actually communicate with
%% it. Otherwise there's a race where we falsely detect a partial
%% partition.
%%
%% Now of course the rpc:call/4 may take a long time to return if
%% connectivity with the node is actually interrupted - but that's OK,
%% we only really want to do something in a timely manner if
%% connectivity is OK. However, of course as always we must not block
%% the node monitor, so we do the check in a separate process.

handle_cast({check_partial_partition, _Node, _Reporter,
             _NodeGUID, _GUID, _ReporterGUID}, State) ->
    {noreply, State};

handle_cast({partial_partition, NotReallyDown, Proxy, MyGUID},
            State = #state{guid = MyGUID}) ->
    FmtBase = "Partial partition detected:~n"
        " * We saw DOWN from ~ts~n"
        " * We can still see ~ts which can see ~ts~n",
    ArgsBase = [NotReallyDown, Proxy, NotReallyDown],
    case application:get_env(rabbit, cluster_partition_handling) of
        {ok, pause_minority} ->
            rabbit_log:error(
              FmtBase ++ " * pause_minority mode enabled~n"
              "We will therefore pause until the *entire* cluster recovers",
              ArgsBase),
            await_cluster_recovery(fun all_nodes_up/0),
            {noreply, State};
        {ok, {pause_if_all_down, PreferredNodes, _}} ->
            case in_preferred_partition(PreferredNodes) of
                true  -> rabbit_log:error(
                           FmtBase ++ "We will therefore intentionally "
                           "disconnect from ~ts", ArgsBase ++ [Proxy]),
                         upgrade_to_full_partition(Proxy);
                false -> rabbit_log:info(
                           FmtBase ++ "We are about to pause, no need "
                           "for further actions", ArgsBase)
            end,
            {noreply, State};
        {ok, _} ->
            rabbit_log:error(
              FmtBase ++ "We will therefore intentionally disconnect from ~ts",
              ArgsBase ++ [Proxy]),
            upgrade_to_full_partition(Proxy),
            {noreply, State}
    end;

handle_cast({partial_partition, _GUID, _Reporter, _Proxy}, State) ->
    {noreply, State};

%% Sometimes it appears the Erlang VM does not give us nodedown
%% messages reliably when another node disconnects from us. Therefore
%% we are told just before the disconnection so we can reciprocate.
handle_cast({partial_partition_disconnect, Other}, State) ->
    rabbit_log:error("Partial partition disconnect from ~ts", [Other]),
    disconnect(Other),
    {noreply, State};

%% Note: when updating the status file, we can't simply write the
%% mnesia information since the message can (and will) overtake the
%% mnesia propagation.
handle_cast({node_up, Node, NodeType},
            State = #state{monitors = Monitors}) ->
    rabbit_log:info("rabbit on node ~tp up", [Node]),
    case rabbit_khepri:is_enabled() of
        true ->
            ok;
        false ->
            {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
            write_cluster_status({add_node(Node, AllNodes),
                                  case NodeType of
                                      disc -> add_node(Node, DiscNodes);
                                      ram  -> DiscNodes
                                  end,
                                  add_node(Node, RunningNodes)})
    end,
    ok = handle_live_rabbit(Node),
    Monitors1 = case pmon:is_monitored({rabbit, Node}, Monitors) of
                    true ->
                        Monitors;
                    false ->
                        pmon:monitor({rabbit, Node}, Monitors)
                end,
    {noreply, maybe_autoheal(State#state{monitors = Monitors1})};

handle_cast({joined_cluster, Node, NodeType}, State) ->
    case rabbit_khepri:is_enabled() of
        true ->
            ok;
        false ->
            {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
            write_cluster_status({add_node(Node, AllNodes),
                                  case NodeType of
                                      disc -> add_node(Node, DiscNodes);
                                      ram  -> DiscNodes
                                  end,
                                  RunningNodes})
    end,
    rabbit_log:debug("Node '~tp' has joined the cluster", [Node]),
    rabbit_event:notify(node_added, [{node, Node}]),
    {noreply, State};

handle_cast({left_cluster, Node}, State) ->
    case rabbit_khepri:is_enabled() of
        true ->
            ok;
        false ->
            {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
            write_cluster_status(
              {del_node(Node, AllNodes), del_node(Node, DiscNodes),
               del_node(Node, RunningNodes)})
    end,
    rabbit_event:notify(node_deleted, [{node, Node}]),
    {noreply, State};

handle_cast({subscribe, Pid}, State = #state{subscribers = Subscribers}) ->
    {noreply, State#state{subscribers = pmon:monitor(Pid, Subscribers)}};

handle_cast(keepalive, State) ->
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, {rabbit, Node}, _Reason},
            State = #state{monitors = Monitors, subscribers = Subscribers}) ->
    rabbit_log:info("rabbit on node ~tp down", [Node]),
    case rabbit_khepri:is_enabled() of
        true ->
            ok;
        false ->
            {AllNodes, DiscNodes, RunningNodes} = read_cluster_status(),
            write_cluster_status(
              {AllNodes, DiscNodes, del_node(Node, RunningNodes)})
    end,
    _ = [P ! {node_down, Node} || P <- pmon:monitored(Subscribers)],
    {noreply, handle_dead_rabbit(
                Node,
                State#state{monitors = pmon:erase({rabbit, Node}, Monitors)})};

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state{subscribers = Subscribers}) ->
    {noreply, State#state{subscribers = pmon:erase(Pid, Subscribers)}};

handle_info({nodedown, Node, Info}, State) ->
    rabbit_log:info("node ~tp down: ~tp",
                    [Node, proplists:get_value(nodedown_reason, Info)]),
    case rabbit_khepri:is_enabled() of
        true  -> {noreply, State};
        false -> handle_nodedown_using_mnesia(Node, State)
    end;

handle_info({nodeup, Node, _Info}, State) ->
    rabbit_log:info("node ~tp up", [Node]),
    {noreply, State};

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
    Partitions1 = lists:usort([Node | Partitions]),
    {noreply, maybe_autoheal(State1#state{partitions = Partitions1})};

handle_info({autoheal_msg, Msg}, State = #state{autoheal   = AState,
                                                partitions = Partitions}) ->
    AState1 = rabbit_autoheal:handle_msg(Msg, AState, Partitions),
    {noreply, State#state{autoheal = AState1}};

handle_info(ping_down_nodes, State) ->
    %% We ping nodes when some are down to ensure that we find out
    %% about healed partitions quickly. We ping all nodes rather than
    %% just the ones we know are down for simplicity; it's not expensive
    %% to ping the nodes that are up, after all.
    State1 = State#state{down_ping_timer = undefined},
    Self = self(),
    %% We ping in a separate process since in a partition it might
    %% take some noticeable length of time and we don't want to block
    %% the node monitor for that long.
    spawn_link(fun () ->
                       ping_all(),
                       case all_nodes_up() of
                           true  -> ok;
                           false -> Self ! ping_down_nodes_again
                       end
               end),
    {noreply, State1};

handle_info(ping_down_nodes_again, State) ->
    {noreply, ensure_ping_timer(State)};

handle_info(ping_up_nodes, State) ->
    %% In this case we need to ensure that we ping "quickly" -
    %% i.e. only nodes that we know to be up.
    [cast(N, keepalive) || N <- alive_nodes() -- [node()]],
    {noreply, ensure_keepalive_timer(State#state{keepalive_timer = undefined})};

handle_info({'EXIT', _, _} = Info, State = #state{autoheal = AState0}) ->
    AState = rabbit_autoheal:process_down(Info, AState0),
    {noreply, State#state{autoheal = AState}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = rabbit_misc:stop_timer(State, #state.down_ping_timer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------
%% Functions that call the module specific hooks when nodes go up/down
%%----------------------------------------------------------------------------

handle_nodedown_using_mnesia(Node, State = #state{guid       = MyGUID,
                                                  node_guids = GUIDs}) ->
    Check = fun (N, CheckGUID, DownGUID) ->
                    cast(N, {check_partial_partition,
                             Node, node(), DownGUID, CheckGUID, MyGUID})
            end,
    _ = case maps:find(Node, GUIDs) of
        {ok, DownGUID} -> Alive = rabbit_mnesia:cluster_nodes(running)
                              -- [node(), Node],
                          [case maps:find(N, GUIDs) of
                               {ok, CheckGUID} -> Check(N, CheckGUID, DownGUID);
                               error           -> ok
                           end || N <- Alive];
        error          -> ok
    end,
    {noreply, handle_dead_node(Node, State)}.

handle_dead_node(Node, State = #state{autoheal = Autoheal}) ->
    %% In general in rabbit_node_monitor we care about whether the
    %% rabbit application is up rather than the node; we do this so
    %% that we can respond in the same way to "rabbitmqctl stop_app"
    %% and "rabbitmqctl stop" as much as possible.
    %%
    %% However, for pause_minority and pause_if_all_down modes we can't do
    %% this, since we depend on looking at whether other nodes are up
    %% to decide whether to come back up ourselves - if we decide that
    %% based on the rabbit application we would go down and never come
    %% back.
    case application:get_env(rabbit, cluster_partition_handling) of
        {ok, pause_minority} ->
            case majority([Node]) of
                true  -> ok;
                false -> await_cluster_recovery(fun majority/0)
            end,
            State;
        {ok, {pause_if_all_down, PreferredNodes, HowToRecover}} ->
            case in_preferred_partition(PreferredNodes, [Node]) of
                true  -> ok;
                false -> await_cluster_recovery(
                           fun in_preferred_partition/0)
            end,
            case HowToRecover of
                autoheal -> State#state{autoheal =
                              rabbit_autoheal:node_down(Node, Autoheal)};
                _        -> State
            end;
        {ok, ignore} ->
            State;
        {ok, autoheal} ->
            State#state{autoheal = rabbit_autoheal:node_down(Node, Autoheal)};
        {ok, Term} ->
            rabbit_log:warning("cluster_partition_handling ~tp unrecognised, "
                               "assuming 'ignore'", [Term]),
            State
    end.

await_cluster_recovery(Condition) ->
    rabbit_log:warning("Cluster minority/secondary status detected - "
                       "awaiting recovery", []),
    run_outside_applications(fun () ->
                                     rabbit:stop(),
                                     wait_for_cluster_recovery(Condition)
                             end, false),
    ok.

run_outside_applications(Fun, WaitForExistingProcess) ->
    spawn_link(fun () ->
                       %% Ignore exit messages from the monitor - the link is needed
                       %% to ensure the monitor detects abnormal exits from this process
                       %% and can reset the 'restarting' status on the autoheal, avoiding
                       %% a deadlock. The monitor is restarted when rabbit does, so messages
                       %% in the other direction should be ignored.
                       process_flag(trap_exit, true),
                       %% If our group leader is inside an application we are about
                       %% to stop, application:stop/1 does not return.
                       group_leader(whereis(init), self()),
                       register_outside_app_process(Fun, WaitForExistingProcess)
               end).

register_outside_app_process(Fun, WaitForExistingProcess) ->
    %% Ensure only one such process at a time, the exit(badarg) is
    %% harmless if one is already running.
    %%
    %% If WaitForExistingProcess is false, the given fun is simply not
    %% executed at all and the process exits.
    %%
    %% If WaitForExistingProcess is true, we wait for the end of the
    %% currently running process before executing the given function.
    try register(rabbit_outside_app_process, self()) of
        true ->
            do_run_outside_app_fun(Fun)
    catch
        error:badarg when WaitForExistingProcess ->
            MRef = erlang:monitor(process, rabbit_outside_app_process),
            receive
                {'DOWN', MRef, _, _, _} ->
                    %% The existing process exited, let's try to
                    %% register again.
                    register_outside_app_process(Fun, WaitForExistingProcess)
            end;
        error:badarg ->
            ok
    end.

do_run_outside_app_fun(Fun) ->
    try
        Fun()
    catch _:E:Stacktrace ->
            rabbit_log:error(
              "rabbit_outside_app_process:~n~tp~n~tp",
              [E, Stacktrace])
    end.

wait_for_cluster_recovery(Condition) ->
    ping_all(),
    case Condition() of
        true  -> rabbit:start();
        false -> timer:sleep(?RABBIT_DOWN_PING_INTERVAL),
                 wait_for_cluster_recovery(Condition)
    end.

handle_dead_rabbit(Node, State) ->
    %% TODO: This may turn out to be a performance hog when there are
    %% lots of nodes.  We really only need to execute some of these
    %% statements on *one* node, rather than all of them.
    ok = rabbit_amqqueue:on_node_down(Node),
    ok = rabbit_alarm:on_node_down(Node),
    ok = rabbit_quorum_queue_periodic_membership_reconciliation:on_node_down(Node),
    State1 = case rabbit_khepri:is_enabled() of
                 true  -> State;
                 false -> on_node_down_using_mnesia(Node, State)
             end,
    ensure_ping_timer(State1).

on_node_down_using_mnesia(Node, State = #state{partitions = Partitions,
                                               autoheal   = Autoheal}) ->
    ok = rabbit_mnesia:on_node_down(Node),
    %% If we have been partitioned, and we are now in the only remaining
    %% partition, we no longer care about partitions - forget them. Note
    %% that we do not attempt to deal with individual (other) partitions
    %% going away. It's only safe to forget anything about partitions when
    %% there are no partitions.
    Down = Partitions -- alive_rabbit_nodes(),
    NoLongerPartitioned = rabbit_mnesia:cluster_nodes(running),
    Partitions1 = case Partitions -- Down -- NoLongerPartitioned of
                      [] -> [];
                      _  -> Partitions
                  end,
    State#state{partitions = Partitions1,
                autoheal   = rabbit_autoheal:rabbit_down(Node, Autoheal)}.

ensure_ping_timer(State) ->
    rabbit_misc:ensure_timer(
      State, #state.down_ping_timer, ?RABBIT_DOWN_PING_INTERVAL,
      ping_down_nodes).

ensure_keepalive_timer(State) ->
    {ok, Interval} = application:get_env(rabbit, cluster_keepalive_interval),
    rabbit_misc:ensure_timer(
      State, #state.keepalive_timer, Interval, ping_up_nodes).

handle_live_rabbit(Node) ->
    ok = rabbit_amqqueue:on_node_up(Node),
    ok = rabbit_alarm:on_node_up(Node),
    case rabbit_khepri:is_enabled() of
        true  -> ok;
        false -> on_node_up_using_mnesia(Node)
    end,
    ok = rabbit_vhosts:on_node_up(Node),
    ok = rabbit_quorum_queue_periodic_membership_reconciliation:on_node_up(Node).

on_node_up_using_mnesia(Node) ->
    ok = rabbit_mnesia:on_node_up(Node).

maybe_autoheal(State) ->
    case rabbit_khepri:is_enabled() of
        true  -> State;
        false -> maybe_autoheal1(State)
    end.

maybe_autoheal1(State = #state{partitions = []}) ->
    State;

maybe_autoheal1(State = #state{autoheal = AState}) ->
    case all_nodes_up() of
        true  -> State#state{autoheal = rabbit_autoheal:maybe_start(AState)};
        false -> State
    end.

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

cast(Node, Msg) -> gen_server:cast({?SERVER, Node}, Msg).

upgrade_to_full_partition(Proxy) ->
    cast(Proxy, {partial_partition_disconnect, node()}),
    disconnect(Proxy).

%% When we call this, it's because we want to force Mnesia to detect a
%% partition. But if we just disconnect_node/1 then Mnesia won't
%% detect a very short partition. So we want to force a slightly
%% longer disconnect. Unfortunately we don't have a way to blacklist
%% individual nodes; the best we can do is turn off auto-connect
%% altogether. If Node is not already part of the connected nodes, then
%% there's no need to repeat disabling dist_auto_connect and executing
%% disconnect_node/1, which can result in application_controller
%% timeouts and crash node monitor process. This also implies that
%% the already disconnected node was already processed. In an
%% unstable network, if we get consecutive 'up' and 'down' messages,
%% then we expect disconnect_node/1 to be executed.
disconnect(Node) ->
    case lists:member(Node, nodes()) of
        true ->
            application:set_env(kernel, dist_auto_connect, never),
            erlang:disconnect_node(Node),
            timer:sleep(?NODE_DISCONNECTION_TIMEOUT),
            application:unset_env(kernel, dist_auto_connect);
        false ->
            ok
    end.

%%--------------------------------------------------------------------

%% mnesia:system_info(db_nodes) does not return all nodes
%% when partitioned, just those that we are sharing Mnesia state
%% with. So we have a small set of replacement functions
%% here. "rabbit" in a function's name implies we test if the rabbit
%% application is up, not just the node.

%% As we use these functions to decide what to do in pause_minority or
%% pause_if_all_down states, they *must* be fast, even in the case where
%% TCP connections are timing out. So that means we should be careful
%% about whether we connect to nodes which are currently disconnected.

majority() ->
    majority([]).

majority(NodesDown) ->
    Nodes = rabbit_nodes:list_members(),
    AliveNodes = alive_nodes(Nodes) -- NodesDown,
    length(AliveNodes) / length(Nodes) > 0.5.

in_preferred_partition() ->
    {ok, {pause_if_all_down, PreferredNodes, _}} =
        application:get_env(rabbit, cluster_partition_handling),
    in_preferred_partition(PreferredNodes).

in_preferred_partition(PreferredNodes) ->
    in_preferred_partition(PreferredNodes, []).

in_preferred_partition(PreferredNodes, NodesDown) ->
    Nodes = rabbit_nodes:list_members(),
    RealPreferredNodes = [N || N <- PreferredNodes, lists:member(N, Nodes)],
    AliveNodes = alive_nodes(RealPreferredNodes) -- NodesDown,
    RealPreferredNodes =:= [] orelse AliveNodes =/= [].

all_nodes_up() ->
    Nodes = rabbit_nodes:list_members(),
    length(alive_nodes(Nodes)) =:= length(Nodes).

-spec all_rabbit_nodes_up() -> boolean().

all_rabbit_nodes_up() ->
    Nodes = rabbit_nodes:list_members(),
    length(alive_rabbit_nodes(Nodes)) =:= length(Nodes).

alive_nodes() -> rabbit_nodes:list_reachable().

-spec alive_nodes([node()]) -> [node()].

alive_nodes(Nodes) -> rabbit_nodes:filter_reachable(Nodes).

alive_rabbit_nodes() ->
    alive_rabbit_nodes(rabbit_nodes:list_members()).

-spec alive_rabbit_nodes([node()]) -> [node()].

alive_rabbit_nodes(Nodes) ->
    ok = ping(Nodes),
    rabbit_nodes:filter_running(Nodes).

%% This one is allowed to connect!

-spec ping_all() -> 'ok'.

ping_all() ->
    ping(rabbit_nodes:list_members()).

ping(Nodes) ->
    _ = [net_adm:ping(N) || N <- Nodes],
    ok.

possibly_partitioned_nodes() ->
    alive_rabbit_nodes() -- rabbit_mnesia:cluster_nodes(running).

startup_log() ->
    rabbit_log:info("Starting rabbit_node_monitor (partition handling strategy unapplicable with Khepri)", []).

startup_log(Nodes) ->
    {ok, M} = application:get_env(rabbit, cluster_partition_handling),
    startup_log(Nodes, M).

startup_log([], PartitionHandling) ->
    rabbit_log:info("Starting rabbit_node_monitor (in ~tp mode)", [PartitionHandling]);
startup_log(Nodes, PartitionHandling) ->
    rabbit_log:info("Starting rabbit_node_monitor (in ~tp mode), might be partitioned from ~tp",
                    [PartitionHandling, Nodes]).
