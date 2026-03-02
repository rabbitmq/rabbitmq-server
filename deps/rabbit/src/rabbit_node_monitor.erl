%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_node_monitor).

-include_lib("kernel/include/logger.hrl").


-behaviour(gen_server).

-export([start_link/0]).
-export([running_nodes_filename/0,
         cluster_status_filename/0, coordination_filename/0,
         stream_filename/0,
         quorum_filename/0, default_quorum_filename/0,
         classic_filename/0]).
-export([notify_node_up/0, notify_joined_cluster/0, notify_left_cluster/1]).
-export([global_sync/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

 %% Utils
-export([all_rabbit_nodes_up/0, ping_all/0,
         alive_nodes/1, alive_rabbit_nodes/1]).

-define(SERVER, ?MODULE).
-define(NODE_REPLY_TIMEOUT, 5000).
-define(RABBIT_UP_RPC_TIMEOUT, 2000).
-define(RABBIT_DOWN_PING_INTERVAL, 1000).
-define(NODE_DISCONNECTION_TIMEOUT, 1000).

-record(state, {monitors, down_ping_timer,
                keepalive_timer, guid}).

%%----------------------------------------------------------------------------
%% Start
%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%----------------------------------------------------------------------------
%% Cluster file operations
%%----------------------------------------------------------------------------

-spec running_nodes_filename() -> string().
%% @deprecated.

running_nodes_filename() ->
    filename:join(rabbit:data_dir(), "nodes_running_at_shutdown").

-spec cluster_status_filename() -> string().
%% @deprecated.

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
    ?LOG_DEBUG("global's sync tags 10s ago: ~p~n"
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

    ?LOG_INFO(
      "global hang workaround: faking nodedown / nodeup between peer node ~s "
      "(connection ID to us: ~p) and our node ~s (connection ID to peer: ~p)",
      [PeerNode, PeerToThisCid, ThisNode, ThisToPeerCid]),
    ?LOG_DEBUG(
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
    startup_log(),
    Monitors = pmon:new(),
    {ok, ensure_keepalive_timer(#state{monitors    = Monitors,
                                       guid        = erlang:system_info(creation)})}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(notify_node_up, State) ->
    do_notify_node_up(State),
    {noreply, State};

handle_cast({node_up, Node, NodeType, _GUID}, State) ->
    handle_cast({node_up, Node, NodeType}, State);

handle_cast({node_up, Node, _NodeType},
            State = #state{monitors = Monitors}) ->
    ?LOG_INFO("rabbit on node ~tp up", [Node]),
    ok = handle_live_rabbit(Node),
    Monitors1 = case pmon:is_monitored({rabbit, Node}, Monitors) of
                    true ->
                        Monitors;
                    false ->
                        pmon:monitor({rabbit, Node}, Monitors)
                end,
    {noreply, State#state{monitors = Monitors1}};

handle_cast({joined_cluster, Node, _NodeType}, State) ->
    ?LOG_DEBUG("Node '~tp' has joined the cluster", [Node]),
    rabbit_event:notify(node_added, [{node, Node}]),
    {noreply, State};

handle_cast({left_cluster, Node}, State) ->
    rabbit_event:notify(node_deleted, [{node, Node}]),
    {noreply, State};

handle_cast(keepalive, State) ->
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, {rabbit, Node}, _Reason},
            State = #state{monitors = Monitors}) ->
    ?LOG_INFO("rabbit on node ~tp down", [Node]),
    {noreply, handle_dead_rabbit(
                Node,
                State#state{monitors = pmon:erase({rabbit, Node}, Monitors)})};

handle_info({nodedown, Node, Info}, State) ->
    ?LOG_INFO("node ~tp down: ~tp",
                    [Node, proplists:get_value(nodedown_reason, Info)]),
    {noreply, State};

handle_info({nodeup, Node, _Info}, State) ->
    ?LOG_INFO("node ~tp up", [Node]),
    %% We notify that `rabbit' is up here too (in addition to the message sent
    %% explicitly by a boot step. That's because nodes may go down then up
    %% during a network partition, and with Khepri, nodes are not restarted,
    %% and thus the boot steps are not executed.
    do_notify_node_up(State),
    {noreply, State};

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

do_notify_node_up(#state{guid = GUID}) ->
    Nodes = rabbit_nodes:list_reachable() -- [node()],
    gen_server:abcast(Nodes, ?SERVER,
                      {node_up, node(), rabbit_db_cluster:node_type(), GUID}),
    %% register other active rabbits with this rabbit
    _ = [gen_server:cast(
           ?SERVER,
           {node_up, N, disc}) || N <- Nodes],
    ok.

handle_dead_rabbit(Node, State) ->
    %% TODO: This may turn out to be a performance hog when there are
    %% lots of nodes.  We really only need to execute some of these
    %% statements on *one* node, rather than all of them.
    ok = rabbit_amqqueue:on_node_down(Node),
    ok = rabbit_alarm:on_node_down(Node),
    ensure_ping_timer(State).

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
    ok = rabbit_vhosts:on_node_up(Node).

%%--------------------------------------------------------------------
%% Internal utils
%%--------------------------------------------------------------------

cast(Node, Msg) -> gen_server:cast({?SERVER, Node}, Msg).

%%--------------------------------------------------------------------

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

startup_log() ->
    ?LOG_INFO("Starting rabbit_node_monitor", []).
