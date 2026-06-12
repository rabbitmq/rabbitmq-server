%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_amqp_sole_conn).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("include/rabbit_khepri.hrl").
-include_lib("amqp10_common/include/amqp10_sole_conn.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(RA_CLUSTER_NAME, rabbitmq_amqp10_sole_conn).
-define(STORE_ID, ?RA_CLUSTER_NAME).
-define(RA_FRIENDLY_NAME, "AMQP Sole Conn Enforcement").
-define(RA_SYSTEM, coordination).
-define(TRIGGER_ID, amqp10_sole_conn_kill_connection).
-define(DEFAULT_COMMAND_OPTIONS, #{reply_from => local}).
-define(RPC_TIMEOUT, 30_000).
-define(ALIVENESS_RPC_TIMEOUT, 1_000).
-define(JOIN_MAX_ATTEMPTS, 5).
-define(JOIN_RETRY_BACKOFF, 1_000).
-define(RESIZE_POLL_INTERVAL, 500).
-define(BOOTSTRAP_LOCK, {?MODULE, bootstrap}).

%% defaults for configuration parameters
-define(TICK_INTERVAL, 30_000).
%% TODO: same default as metadata store, reconsider for this store
-define(SNAPSHOT_INTERVAL, 50_000).
-define(LEADER_WAIT_RETRY_TIMEOUT, 300_000).
-define(DEFAULT_KHEPRI_TIMEOUT, 30_000).

-rabbit_boot_step({?MODULE,
                   [{description, "AMQP 1.0 sole connection enforcement"},
                    {mfa,         {?MODULE, recover, []}},
                    {requires,    database},
                    {enables,     pre_flight}]}).

%% supervisor and gen_server callbacks
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% lifecycle and store management
-export([forget_node/1,
         recover/0,
         ensure_running/0,
         stop/0,
         is_resizing/0,
         get_ra_system/0,
         get_store_id/0]).

%% public API
-export([acquire/5,
         refuse_connection_error/0,
         close_existing_connection_error/0,
         is_feature_enabled/0]).

%% CLI
-export([status/0,
         force_delete/2,
         start/0,
         wipe/0]).

%% for testing
-export([conn/2,
         try_put/3,
         conn_path/2]).

-type vhost() :: binary().
-type container_id() :: binary().
-type username() :: binary().
-type wipe_status() :: ok | not_started | {error, term()}.
-type start_status() :: ok | {error, term()}.

-record(conn, {pid :: pid(),
               username :: username()}).
%% gen_server state
-record(state, {resizer_pid :: pid() | undefined}).
%% --------------------------------------------------------------
%% gen_server callbacks
%% --------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:send_after(tick_interval(), self(), cluster_tick),
    {ok, #state{resizer_pid = undefined}}.

handle_info(cluster_tick, State = #state{resizer_pid = ResizerPid}) ->
    ?LOG_DEBUG("sole_conn cluster tick"),
    erlang:send_after(tick_interval(), self(), cluster_tick),
    case is_leader() of
        true when ResizerPid =:= undefined ->
            ?LOG_DEBUG("leader, spawning resizing process"),
            %% We are the leader and no resize is currently running. Start one.
            {Pid, _MonitorRef} = spawn_monitor(fun maybe_resize_cluster/0),
            {noreply, State#state{resizer_pid = Pid}};
        true ->
            %% We are the leader but a resize is already running. Skip this tick.
            ?LOG_DEBUG("Skipping sole_conn cluster resize tick, previous run still in progress"),
            {noreply, State};
        false ->
            ?LOG_DEBUG("not the leader, no resizing"),
            %% We are not the leader. Do nothing.
            {noreply, State}
    end;
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State = #state{resizer_pid = Pid}) ->
    %% The resizing process finished or crashed. Clear the tracker so the next tick can run.
    {noreply, State#state{resizer_pid = undefined}};
handle_info(Message, State) ->
    {stop, {unhandled_info, Message}, State}.

handle_call(is_resizing, _From, State = #state{resizer_pid = ResizerPid}) ->
    {reply, ResizerPid =/= undefined, State};
handle_call(Request, _From, State) ->
    {stop, {unhandled_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {unhandled_cast, Request}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------
%% Lifecycle and store management
%% --------------------------------------------------------------

-spec forget_node(node()) -> ok | {error, term()}.
forget_node(Node) when is_atom(Node) ->
    %% Check if the store was ever bootstrapped locally
    case ra_directory:uid_of(get_ra_system(), get_store_id()) of
        undefined ->
            %% The store was never used on this node (lazy init), safe to skip
            ok;
        _ ->
            %% Evict the node using the unified logic
            evict_node(Node)
    end.

is_leader() ->
    case ra_leaderboard:lookup_leader(get_store_id()) of
        {_StoreId, Node} when Node =:= node() -> true;
        _ -> false
    end.

maybe_resize_cluster() ->
    case rabbit:is_running() of
        true ->
            StoreId = get_store_id(),
            Options = #{favor => low_latency, timeout => default_timeout()},
            case khepri_cluster:members(StoreId, Options) of
                {ok, Members} ->
                    %% Extract the nodes currently in the Khepri cluster
                    MemberNodes = [Node || {_, Node} <- Members],
                    %% Get the state of the broader RabbitMQ cluster
                    Present = rabbit_nodes:list_running(),
                    RabbitNodes = rabbit_nodes:list_members(),
                    %% Explicitly compute nodes that are allowed to be added
                    %% They are members of the cluster and running, which is
                    %% necessary to bootstrap the Khepri store
                    AddableNodes = [N || N <- RabbitNodes, lists:member(N, Present)],
                    %% Calculate nodes to add
                    case AddableNodes -- MemberNodes of
                        [] ->
                            ok;
                        [New | _] ->
                            ?LOG_INFO("~ts: Expanding sole_conn Khepri cluster to "
                                      "running node ~w", [?MODULE, New]),
                           try
                                erpc:cast(New, ?MODULE, ensure_running, [])
                            catch
                                Class:Reason ->
                                    ?LOG_WARNING("~ts: Failed to cast ensure_running to node ~w. "
                                                 "Error: ~p:~p",
                                                 [?MODULE, New, Class, Reason])
                            end
                    end,
                    %% Calculate nodes to add
                    case MemberNodes -- RabbitNodes of
                        [] ->
                            ok;
                        [Old | _] when length(RabbitNodes) > 0 ->
                            %% This should be rare, as forget_cluster_node shrinks
                            %% this cluster as well
                            ?LOG_INFO("~ts: RabbitMQ node ~w was formally removed from the cluster, "
                                      "evicting it from the sole_conn Khepri cluster",
                                      [?MODULE, Old]),
                            _ = evict_node(Old),
                            ok;
                        _ ->
                            ok
                    end;
                _ ->
                    %% Failed to read local members, retry next tick
                    ok
            end;
        false ->
            ok
    end.

evict_node(Node) ->
    StoreId = get_store_id(),
    %% Check if the node we want to evict is currently reachable
    case net_adm:ping(Node) of
        pong ->
            %% Node is online. Safely stop our gen_server first.
            try
                erpc:cast(Node, ?MODULE, stop, [])
            catch
                Class:Reason ->
                    ?LOG_DEBUG("~ts: Could not stop sole_conn gen_server on node ~w. "
                               "Error: ~p:~p",
                               [?MODULE, Node, Class, Reason])
            end,
            %% Ask Khepri to cleanly reset the store on that node via RPC.
            %% This removes it from the quorum, deletes RA data,
            %% and clears Khepri memory caches.
            ?LOG_INFO("~ts: Target node ~w is reachable, executing Khepri reset",
                      [?MODULE, Node]),
            try erpc:call(Node, khepri_cluster, reset, [StoreId], ?RPC_TIMEOUT) of
                ok ->
                    ok;
                {error, _} = Err ->
                    Err
            catch
                error:{erpc, timeout} ->
                    {error, timeout};
                error:{erpc, RpcReason} ->
                    {error, RpcReason}
            end;
        pang ->
            %% Node is offline, we can ask it to reset itself.
            %% We must forcibly shrink the quorum via the Raft leader.
            ?LOG_INFO("~ts: Target node ~w is unreachable, forcefully removing "
                      "from Raft quorum",
                      [?MODULE, Node]),
            ExpectedMembers = [{StoreId, N} || N <- rabbit_nodes:list_members()],
            ToRemove = {StoreId, Node},
            case ra:members(ExpectedMembers) of
                {ok, Members, Leader} ->
                    case lists:member(ToRemove, Members) of
                        true ->
                            %% ra:remove_member safely evicts the dead node
                            %% from the consensus group
                            case ra:remove_member(Leader, ToRemove) of
                                {ok, _, _} ->
                                    ok;
                                {timeout, _} ->
                                    {error, timeout};
                                {error, _} = Err ->
                                    Err
                            end;
                        false ->
                            %% The node is already gone from the Raft quorum
                            ok
                    end;
                {timeout, _} ->
                    {error, timeout};
                {error, _} = Err ->
                    Err
            end
    end.

stop() ->
    ?LOG_DEBUG("Stopping sole_conn gen_server and "
               "removing from supervision tree on ~p", [node()]),
    _ = rabbit_sup:stop_child(?MODULE),
    ok.

-spec is_resizing() -> boolean().
is_resizing() ->
    gen_server:call(?MODULE, is_resizing).

init_schema() ->
    _ = khepri_adv:put(get_store_id(),
                       kill_connection_sproc_path(),
                       fun kill_connection_sproc/1,
                       default_command_options()),

    EventFilter = khepri_evf:tree(kill_connection_sproc_trigger_pattern(),
                                  #{on_actions => [update]}),

    Opts = #{where => all_members, timeout => default_timeout()},
    ok = khepri:register_trigger(
           get_store_id(),
           ?TRIGGER_ID,
           EventFilter,
           kill_connection_sproc_path(),
           Opts).

%% --------------------------------------------------------------
%% CLI
%% --------------------------------------------------------------

-spec status() -> [[{binary(), term()}]] | {error, term()}.
status() ->
    case is_feature_enabled() of
        false ->
            {error, sole_conn_feature_flag_not_enabled};
        true ->
            status0()
    end.

status0() ->
    case members() of
        {ok, Members} ->
            [begin
                 %% Securely call ra:key_metrics/1 on the remote node
                 MetricsResult = try
                                     erpc:call(N, ra, key_metrics, [ServerId], ?RPC_TIMEOUT)
                                 catch
                                     _:Err -> {error, Err}
                                 end,
                 case MetricsResult of
                     #{state := RaftState,
                       membership := Membership,
                       commit_index := Commit,
                       term := Term,
                       last_index := Last,
                       last_applied := LastApplied,
                       last_written_index := LastWritten,
                       snapshot_index := SnapIdx} ->
                         %% Optionally fetch the Khepri machine version, failing gracefully to 0
                         MacVer = try
                                      erpc:call(N, khepri_machine, version, [], 1000)
                                  catch _:_ ->
                                            0
                                  end,
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, RaftState},
                          {<<"Membership">>, Membership},
                          {<<"Last Log Index">>, Last},
                          {<<"Last Written">>, LastWritten},
                          {<<"Last Applied">>, LastApplied},
                          {<<"Commit Index">>, Commit},
                          {<<"Snapshot Index">>, SnapIdx},
                          {<<"Term">>, Term},
                          {<<"Machine Version">>, MacVer}];
                     {error, ErrReason} ->
                         [{<<"Node Name">>, N},
                          {<<"Raft State">>, rabbit_misc:format("~p", [ErrReason])},
                          {<<"Membership">>, <<>>},
                          {<<"Last Log Index">>, <<>>},
                          {<<"Last Written">>, <<>>},
                          {<<"Last Applied">>, <<>>},
                          {<<"Commit Index">>, <<>>},
                          {<<"Snapshot Index">>, <<>>},
                          {<<"Term">>, <<>>},
                          {<<"Machine Version">>, <<>>}]
                 end
             end || {_, N} = ServerId <- Members];
        {error, {no_more_servers_to_try, _}} ->
            {error, sole_conn_not_started_or_available};
        {error, _} = Err ->
            Err
    end.

-spec force_delete(vhost(), container_id()) ->
    ok | {error, any()}.
force_delete(VHost, ContainerId) ->
    case is_feature_enabled() of
        false ->
            {error, sole_conn_feature_flag_not_enabled};
        true ->
            force_delete0(VHost, ContainerId)
    end.

force_delete0(VHost, ContainerId) ->
    case whereis(?MODULE) of
        undefined ->
            {error, sole_conn_not_started_or_available};
        _Pid ->
            Path = conn_path(VHost, ContainerId),
            Options = #{timeout => default_timeout()},
            case khepri_adv:delete(get_store_id(), Path, Options) of
                {ok, Map} when map_size(Map) =:= 0 ->
                    %% The path matched no tree node, there was nothing to delete.
                    {error, not_found};
                {ok, _Map} ->
                    ok;
                {error, _} = Err ->
                    Err
            end
    end.

%% Last-resort operator command (meant to be run via `rabbitmqctl eval`) to
%% wipe the Khepri store on every cluster member, e.g. after the store got
%% into a state it cannot recover from on its own.
-spec wipe() -> [{node(), wipe_status()}].
wipe() ->
    Nodes = rabbit_nodes:list_members(),
    ?LOG_WARNING("~ts: wiping Khepri store on nodes ~w as requested by an operator",
                 [?MODULE, Nodes]),
    global:set_lock(?BOOTSTRAP_LOCK),
    try
        %% For each node, wait for its resizer process to settle and stop its
        %% gen_server (disabling the cluster-resizing tick) right after,
        %% before moving to the next node.
        lists:foreach(fun disable_tick/1, Nodes),
        [{Node, reset_remote_store(Node)} || Node <- Nodes]
    after
        global:del_lock(?BOOTSTRAP_LOCK)
    end.

disable_tick(Node) ->
    Deadline = erlang:monotonic_time(millisecond) + ?RPC_TIMEOUT,
    wait_for_resize_to_settle(Node, Deadline),
    stop_remote_gen_server(Node).

wait_for_resize_to_settle(Node, Deadline) ->
    case is_resizing_remote(Node) of
        true ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    ?LOG_WARNING("~ts: Node ~w is still resizing the sole_conn "
                                 "Khepri cluster after waiting, proceeding with "
                                 "wipe anyway", [?MODULE, Node]);
                false ->
                    timer:sleep(?RESIZE_POLL_INTERVAL),
                    wait_for_resize_to_settle(Node, Deadline)
            end;
        false ->
            ok
    end.

is_resizing_remote(Node) ->
    try erpc:call(Node, ?MODULE, is_resizing, [], ?ALIVENESS_RPC_TIMEOUT) of
        Result -> Result
    catch
        %% Not running, unreachable, or otherwise gone: nothing to wait for.
        _:_ -> false
    end.

stop_remote_gen_server(Node) ->
    try
        _ = erpc:call(Node, ?MODULE, stop, [], ?RPC_TIMEOUT)
    catch
        Class:Reason ->
            ?LOG_WARNING("~ts: Could not stop sole_conn gen_server on node ~w "
                         "before wipe. Error: ~p:~p",
                         [?MODULE, Node, Class, Reason])
    end,
    ok.

-spec reset_remote_store(node()) -> wipe_status().
reset_remote_store(Node) ->
    StoreId = get_store_id(),
    RaSystem = get_ra_system(),
    try
        case erpc:call(Node, ra_directory, uid_of, [RaSystem, StoreId], ?RPC_TIMEOUT) of
            undefined ->
                %% The store was never bootstrapped on that node, nothing to reset.
                not_started;
            _ ->
                case erpc:call(Node, khepri_cluster, reset, [StoreId], ?RPC_TIMEOUT) of
                    ok ->
                        ok;
                    {error, _} = Err ->
                        Err
                end
        end
    catch
        error:{erpc, timeout} ->
            {error, timeout};
        error:{erpc, RpcReason} ->
            {error, RpcReason};
        Class:Reason ->
            {error, {Class, Reason}}
    end.

%% Operator command (meant to be run via `rabbitmqctl eval`) to eagerly
%% bootstrap the Khepri store on every listed cluster member, instead of
%% relying on the store being lazily started by the first `acquire' call on
%% each node. Best-effort like wipe/0: one node failing to start does not
%% stop the others from being attempted.
-spec start() -> [{node(), start_status()}].
start() ->
    case is_feature_enabled() of
        false ->
            ?LOG_WARNING("~ts: refusing to start the sole_conn Khepri store: "
                         "feature flag 'rabbitmq_4.4.0' is not enabled cluster-wide "
                         "(cluster is mid-upgrade)", [?MODULE]),
            [];
        true ->
            Nodes = rabbit_nodes:list_members(),
            ?LOG_INFO("~ts: starting the sole_conn Khepri store on nodes ~w "
                      "as requested by an operator",
                      [?MODULE, Nodes]),
            [{Node, start_remote(Node)} || Node <- Nodes]
    end.

-spec start_remote(node()) -> start_status().
start_remote(Node) ->
    try
        ok = erpc:call(Node, ?MODULE, ensure_running, [], ?RPC_TIMEOUT)
    catch
        error:{erpc, timeout} ->
            {error, timeout};
        error:{erpc, RpcReason} ->
            {error, RpcReason};
        Class:Reason ->
            {error, {Class, Reason}}
    end.

%% --------------------------------------------------------------
%% Public API
%% --------------------------------------------------------------

recover() ->
    LocalServerId = {get_store_id(), node()},
    %% We ask RA to passively check the disk and restart the Khepri state machine
    ?LOG_DEBUG("Trying to restart local sole_conn RA server on ~p", [node()]),

    case ra:restart_server(get_ra_system(), LocalServerId) of
        {error, Reason} when Reason == not_started;
                             Reason == name_not_registered ->
            ?LOG_DEBUG("~p, will start on demand", [Reason]),
            %% First boot, do nothing and wait until the first `acquire`
            ok;
        _ ->
            ?LOG_DEBUG("Restarted local sole_conn RA server on ~p", [node()]),
            %% Khepri instance restarted
            %% We can now safely start our gen_server to manage it.
            rabbit_sup:start_child(?MODULE)
    end.

ensure_running() ->
    case whereis(?MODULE) of
        undefined ->
            ?LOG_DEBUG("sole_conn not running on ~p, "
                       "trying to acquire bootstrap lock", [node()]),
            global:set_lock(?BOOTSTRAP_LOCK),
            try
                case whereis(?MODULE) of
                    undefined ->
                        start_local_store();
                    _Pid ->
                        ?LOG_DEBUG("sole_conn has started on ~p, skipping bootstrap sequence",
                                   [node()]),
                        ok
                end
            after
                %% Lock is released even if an exception occurs
                global:del_lock(?BOOTSTRAP_LOCK)
            end;
        _ ->
            ok
    end.

start_local_store() ->
    ?LOG_DEBUG("Starting sole_conn bootstrap sequence on ~p",
               [node()]),
    RetryTimeout = leader_wait_retry_timeout(),
    StoreId = get_store_id(),

    RaServerConfig = make_ra_server_config(),
    ?LOG_DEBUG("Starting ~ts Khepri store", [?RA_FRIENDLY_NAME]),
    {ok, _} = khepri:start(?RA_SYSTEM, RaServerConfig),

    %% Check if we just booted a virgin node or recovered data
    case khepri:is_empty(StoreId, #{timeout => default_timeout()}) of
        true ->
            %% Virgin bootstrap
            OtherNodes = rabbit_nodes:list_running() -- [node()],
            ?LOG_DEBUG("Other nodes in cluster: ~p", [OtherNodes]),
            case find_active_peer(OtherNodes) of
                undefined ->
                    %% Virgin Cluster
                    ?LOG_DEBUG("No active peer, starting new cluster"),
                    ok = khepri_cluster:wait_for_leader(StoreId, RetryTimeout),
                    ?LOG_DEBUG("Started new cluster, initializing schema"),
                    init_schema(),
                    ?LOG_DEBUG("Schema initialized");
                PeerNode ->
                    %% Existing Cluster
                    ?LOG_DEBUG("Trying to join active peer: ~p", [PeerNode]),
                    ok = join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout),

                    ?LOG_DEBUG("Joined existing cluster, waiting for effective behaviour"),
                    ok = khepri_cluster:wait_for_effective_behaviour(
                           StoreId, process_based_keep_while, RetryTimeout),
                    ?LOG_DEBUG("Local store ready")
            end;
        false ->
            %% Recovery
            %% The node already has data, meaning it was part of a cluster.
            %% It natively rejoins the Raft consensus group.
            ?LOG_DEBUG("sole_conn store recovered from disk. Skipping discovery. "
                       "Waiting for effective behaviour."),
            ok = khepri_cluster:wait_for_effective_behaviour(
                   StoreId, process_based_keep_while, RetryTimeout),
            ?LOG_DEBUG("Local store ready")
    end,

    %% Start the gen_server. This registers the local process
    %% name, which allows subsequent calls to bypass this setup, and
    %% lets other nodes discover us via find_active_peer/1.
    ok = rabbit_sup:start_child(?MODULE),
    ok.

make_ra_server_config() ->
    SnapshotInterval = snapshot_interval(),
    #{cluster_name => ?RA_CLUSTER_NAME,
      friendly_name => ?RA_FRIENDLY_NAME,
      metrics_labels => #{ra_system => ?RA_SYSTEM, module => ?MODULE},
      min_recovery_checkpoint_interval => 4096,
      machine_config => #{snapshot_interval => SnapshotInterval}}.

join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout) ->
    join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout, ?JOIN_MAX_ATTEMPTS).

join_active_peer(_StoreId, PeerNode, _RaServerConfig, _RetryTimeout, 0) ->
    ?LOG_ERROR("Giving up joining active peer ~p after repeated failures", [PeerNode]),
    erlang:error({failed_to_join_peer, PeerNode});
join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout, AttemptsLeft) ->
    try join_or_evict_ghost_and_retry(StoreId, PeerNode, RetryTimeout) of
        ok ->
            ok;
        {error, Reason} ->
            retry_join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout,
                                   AttemptsLeft, Reason)
    catch
        %% The local Ra server can end up stopped (while its Ra system and
        %% server config are still known) if it crashed between being
        %% restarted (as part of a failed join attempt) and the eviction of
        %% our stale ghost identity from the remote peer: until that ghost is
        %% evicted, the remote cluster may still address Raft messages to it.
        error:?khepri_exception(ra_server_not_running_but_props_available, _) = Reason ->
            retry_join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout,
                                   AttemptsLeft, Reason)
    end.

retry_join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout, AttemptsLeft, Reason) ->
    ?LOG_WARNING("Failed to join active peer ~p (~p), restarting local store "
                 "and retrying (~b attempt(s) left)",
                 [PeerNode, Reason, AttemptsLeft - 1]),
    timer:sleep(?JOIN_RETRY_BACKOFF),
    %% Bring the local Ra server back up (it is a no-op if it is already
    %% running) before retrying the join.
    {ok, _} = khepri:start(?RA_SYSTEM, RaServerConfig),
    join_active_peer(StoreId, PeerNode, RaServerConfig, RetryTimeout, AttemptsLeft - 1).

join_or_evict_ghost_and_retry(StoreId, PeerNode, RetryTimeout) ->
    case khepri_cluster:join(StoreId, PeerNode) of
        ok ->
            ok;
        {error, _Reason} ->
            %% A violent crash may have wiped our local metadata,
            %% but the remote cluster still remembers our old ghost identity.
            %% We must forcibly evict our ghost from the active peer and retry.
            ?LOG_DEBUG("Join failed, attempting to evict ghost "
                       "identity from ~p",
                       [PeerNode]),
            TargetRaftNode = {StoreId, PeerNode},
            GhostIdentity = {StoreId, node()},

            %% Ask the active peer's RA server to remove our old identity
            _ = erpc:call(PeerNode, ra, remove_member,
                          [TargetRaftNode, GhostIdentity, RetryTimeout]),

            %% Retry the join now that the cluster views us as a clean slate
            ?LOG_DEBUG("Ghost evicted. Retrying join..."),
            khepri_cluster:join(StoreId, PeerNode)
    end.

get_ra_system() ->
    ?RA_SYSTEM.

get_store_id() ->
    ?STORE_ID.

%% --------------------------------------------------------------
%% Public API
%% --------------------------------------------------------------

-spec acquire(none | enforcement_policy(), vhost(), container_id(), username(), pid()) ->
    ok | {error, refuse_connection | close_existing}.
acquire(none, _, _, _, _) ->
    ok;
acquire(Plcy, VHost, ContainerId, Username, ConnPid) ->
    ensure_running(),
    do_acquire(Plcy, VHost, ContainerId, Username, ConnPid).

refuse_connection_error() ->
    %% the error field of close MUST have an error with the condition field
    %% of error being invalid-field and the info field of error having
    %% the symbol key invalid-field taking the symbol value container-id.
    %% [sole conn 3.2.1]
    amqp_error(
      ?V_1_0_AMQP_ERROR_INVALID_FIELD,
      <<"The container-id is already bound to an "
        "active exclusive connection.">>,
      {?V_1_0_AMQP_ERROR_INVALID_FIELD, {symbol, <<"container-id">>}}).

close_existing_connection_error() ->
    %% "The existing connection MUST be closed with the error field of
    %% close having the condition field of error being resource-locked.
    %% Further the info field of error MUST contain the symbol key
    %% sole-connection-enforcement taking the boolean value true"
    %% [sole conn 3.2.1]
    amqp_error(?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
               <<"Connection closed because another "
                 "connection with the same container-id "
                 "was established (sole connection "
                 "enforcement).">>,
               {?SOLE_CONN_ENFORCEMENT, {boolean, true}}).

is_feature_enabled() ->
    rabbit_feature_flags:is_enabled('rabbitmq_4.4.0').

do_acquire(refuse_connection = Plcy, VHost, ContainerId, Username, ConnPid) ->
    Path = conn_path(VHost, ContainerId),

    Opts = default_create_options(ConnPid),
    Payload = #conn{pid = ConnPid, username = Username},
    case khepri_adv:create(get_store_id(), Path, Payload, Opts) of
        {ok, _} ->
            %% no node yet, accept
            %% node should clean itself when the connection is closed
            ok;
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingConn}}}} ->
            %% Only the same user may take over a dead connection's lease;
            %% a different user is refused outright, aliveness notwithstanding.
            case same_user(ExistingConn, Username) of
                true ->
                    case check_conn(ExistingConn) of
                        true ->
                            {error, refuse_connection};
                        _ ->
                            case try_put(Path, ExistingConn, Payload) of
                                ok ->
                                    ok;
                                _ ->
                                    {error, refuse_connection}
                            end
                    end;
                false ->
                    {error, refuse_connection}
            end;
        {error, Reason} ->
            ?LOG_INFO("Unexpected Khepri error for connection '~ts' "
                      "in vhost ~ts (policy ~ts): ~p. Refusing connection.",
                      [ContainerId, VHost, Plcy, Reason]),
            {error, refuse_connection}
    end;
do_acquire(close_existing = Plcy, VHost, ContainerId, Username, ConnPid) ->
    Path = conn_path(VHost, ContainerId),
    Opts = default_create_options(ConnPid),
    Payload = #conn{pid = ConnPid, username = Username},
    case khepri_adv:create(get_store_id(), Path, Payload, Opts) of
        {ok, _} ->
            ok;
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingConn}}}} ->
            %% A different user may not close and replace someone else's
            %% connection: that would be a container ID hijack.
            case same_user(ExistingConn, Username) of
                true ->
                    case try_put(Path, ExistingConn, Payload) of
                        ok ->
                            ok;
                        _ ->
                            {error, refuse_connection}
                    end;
                false ->
                    {error, refuse_connection}
            end;
        {error, Reason} ->
            ?LOG_INFO("Unexpected Khepri error for connection '~ts' "
                      "in vhost ~ts (policy ~ts): ~p. Refusing connection.",
                      [ContainerId, VHost, Plcy, Reason]),
            {error, refuse_connection}
    end.

%% --------------------------------------------------------------
%% Internals
%% --------------------------------------------------------------

%% Iterates through peer nodes and checks if the amqp10_sole_conn process is alive.
find_active_peer([]) ->
    undefined;
find_active_peer([Node | Rest]) ->
    %% Use a fast RPC call with a 1-second timeout to avoid hanging the client
    %% if a peer is unresponsive.
    try erpc:call(Node, erlang, whereis, [?MODULE], 1000) of
        Pid when is_pid(Pid) ->
            Node;
        _ ->
            find_active_peer(Rest)
    catch
        error:{erpc, _Reason} ->
            %% Node is unreachable or timed out, move on to the next one
            find_active_peer(Rest)
    end.

default_command_options() ->
    maps:merge(?DEFAULT_COMMAND_OPTIONS, #{timeout => default_timeout()}).

default_create_options(Pid) ->
    maps:merge(default_command_options(), #{keep_while => Pid}).

same_user(#conn{username = ExistingUsername}, Username) ->
    ExistingUsername =:= Username.

check_conn(#conn{pid = Pid}) ->
    Node = node(Pid),
    case Node =:= node() of
        true ->
            is_process_alive(Pid);
        false ->
            try erpc:call(Node, erlang, is_process_alive, [Pid],
                          ?ALIVENESS_RPC_TIMEOUT) of
                Result ->
                    Result
            catch
                error:{erpc, _Reason} ->
                    %% If the RPC times out, the node is down, or unreachable,
                    %% we assume the process is dead to allow the new connection.
                    false
            end
    end.

try_put(Path,
        #conn{pid = ExistingPid} = ExistingConn,
        #conn{pid = NewPid} = NewConn) ->
    Opts = default_create_options(NewPid),
    case khepri:compare_and_swap(get_store_id(), Path, ExistingConn, NewConn,
                                 Opts) of
        ok ->
            ok;
        {error, Error} ->
            ?LOG_WARNING("Unexpected Khepri error for connection '~p', "
                         "old conn ~p, new conn ~p. Error is ~p.",
                         [Path, ExistingPid, NewPid, Error]),
            error
    end.

kill_connection_sproc(#khepri_trigger{type = tree,
                                      event = #{change := update,
                                                old_node_props := #{data := #conn{pid = Pid}}}}) ->
    exit(Pid, sole_conn_enforcement),
    ok;
kill_connection_sproc(Props) ->
    ?LOG_WARNING("Unexpected event for sole_conn stored procedure, "
                 "connection will not be instructed to close. Event: ~p",
                 Props),
    ok.

amqp_error(Cond, Desc, Info) ->
    #'v1_0.error'{
       condition = Cond,
       description = {utf8, Desc},
       info = {map, [Info]}}.

%% Retrieves the Khepri members safely, even if the local store is offline
members() ->
    StoreId = get_store_id(),
    LocalServerId = {StoreId, node()},
    case whereis(?MODULE) of
        undefined ->
            %% The local store is not running (lazy init hasn't occurred).
            %% Query the other reachable RabbitMQ nodes to find the Raft leader.
            ExpectedMembers = [{StoreId, N} || N <- rabbit_nodes:list_reachable()],
            OtherMembers = lists:delete(LocalServerId, ExpectedMembers),
            case ra:members(OtherMembers) of
                {ok, Members, _Leader} ->
                    {ok, Members};
                Err ->
                    Err
            end;
        _Pid ->
            %% The local store is running, we can use the Khepri API directly
            khepri_cluster:members(StoreId, #{timeout => default_timeout()})
    end.


%% for testing
conn(Pid, Username) ->
    #conn{pid = Pid, username = Username}.

%% --------------------------------------------------------------
%% Khepri paths
%% --------------------------------------------------------------

conn_path(VHost, ContainerId)
  when ?IS_KHEPRI_PATH_CONDITION(VHost) andalso
       ?IS_KHEPRI_PATH_CONDITION(ContainerId) ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(VHost, [amqp10_sole_conn, ContainerId]).

kill_connection_sproc_path() ->
    ?RABBITMQ_KHEPRI_ROOT_PATH([amqp10_sole_conn, kill_connection]).

kill_connection_sproc_trigger_pattern() ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(?KHEPRI_WILDCARD_STAR_STAR,
                                [amqp10_sole_conn,
                                 ?KHEPRI_WILDCARD_STAR_STAR]).

%% --------------------------------------------------------------
%% Configuration parameters for the module
%% --------------------------------------------------------------

%% TODO: add the parameters to the cuttlefish schema

tick_interval() ->
    application:get_env(rabbit, amqp10_sole_conn_tick_interval,
                        ?TICK_INTERVAL).

snapshot_interval() ->
    application:get_env(rabbit, amqp10_sole_conn_snapshot_interval,
                        ?SNAPSHOT_INTERVAL).

leader_wait_retry_timeout() ->
    application:get_env(rabbit, amqp10_sole_conn_leader_wait_retry_timeout,
                        ?LEADER_WAIT_RETRY_TIMEOUT).

default_timeout() ->
    application:get_env(rabbit, amqp10_sole_conn_default_timeout,
                        ?DEFAULT_KHEPRI_TIMEOUT).
