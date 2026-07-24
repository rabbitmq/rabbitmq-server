-module(rabbit_amqp_sole_conn_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile([nowarn_export_all, export_all]).

-define(SOLE_CONN_MOD, rabbit_amqp_sole_conn).
-define(STORE_ID, rabbit_amqp_sole_conn:get_store_id()).
-define(VH, <<"/">>).
-define(CID1, <<"id-1">>).
-define(CID2, <<"id-2">>).
-define(CID3, <<"id-3">>).
-define(USER, <<"user-1">>).
-define(NODE_COUNT, 3).

-define(LOGFMT_CONFIG, #{legacy_header => false,
                         single_line => false,
                         template => [time, " ", pid, ": ", msg, "\n"]}).

all() ->
    [{group, default_group}].

groups() ->
    [{default_group, [], [
        lazy_cluster_formation,
        cluster_should_grow_with_tick,
        cluster_should_shrink_with_tick,
        node_rejoins_cluster_after_graceful_shutdown,
        node_rejoins_cluster_after_abrupt_shutdown,
        forget_node_should_remove_node,
        status_should_return_ra_metrics,
        wipe_should_reset_store_on_all_nodes,
        start_should_bootstrap_store_on_all_nodes,
        conflict_should_resolve_when_existing_conns_node_is_unreachable
    ]}].

init_per_suite(Config) ->
    basic_logger_config(),
    ok = start_epmd(),
    case net_kernel:start([?MODULE, shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    Config.

end_per_suite(Config) ->
    net_kernel:stop(),
    Config.

init_per_group(default_group, Config) ->
    Config.

end_per_group(default_group, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    Nodes = start_n_nodes(Testcase, ?NODE_COUNT, Config0),
    ct:pal("Started peer nodes ~p", [Nodes]),

    Config1 = [{peer_nodes, Nodes} | Config0],

    lists:foreach(
      fun({Node, _Peer}) ->
              setup_mocks(Config1, Node)
      end, Nodes),
    maybe_set_tick_interval(Testcase, Config1),
    Config1.

end_per_testcase(_Testcase, Config) ->
    Nodes = ?config(peer_nodes, Config),
    lists:foreach(
        fun({Node, _Peer}) ->
            %% Stop the gen_server safely
            call(Config, Node, erlang, apply, [fun() -> 
                case whereis(?SOLE_CONN_MOD) of
                    undefined -> ok;
                    Pid -> gen_server:stop(Pid)
                end
            end, []]),
            
            call(Config, Node, khepri, stop, [?STORE_ID]),
            
            try 
                call(Config, Node, meck, unload, [rabbit_nodes]) 
            catch 
                _:_ -> ok 
            end,
            
            try
                call(Config, Node, meck, unload, [rabbit_sup])
            catch
                _:_ -> ok
            end,

            try
                call(Config, Node, meck, unload, [rabbit_feature_flags])
            catch
                _:_ -> ok
            end,

            %% Defensive cleanup in case a testcase-local mock was left
            %% behind by a failed assertion before it could unload it.
            try
                call(Config, Node, meck, unload, [erpc])
            catch
                _:_ -> ok
            end
        end, Nodes),
    lists:foreach(
        fun({Node, _Peer}) ->
            call(Config, Node, application, stop, [khepri]),
            call(Config, Node, application, stop, [ra]),
            ok = stop_erlang_node(Config, Node)
        end, Nodes),
    Config.

maybe_set_tick_interval(Testcase, Config)
  when Testcase == cluster_should_grow_with_tick
       orelse Testcase == cluster_should_shrink_with_tick ->
    Nodes = ?config(peer_nodes, Config),
    lists:foreach(
      fun({_Node, Peer}) ->
              ok = peer:call(Peer, application,
                             set_env,
                             [rabbit, amqp10_sole_conn_tick_interval, 1000])
      end, Nodes),
    ok;
maybe_set_tick_interval(_, _) ->
    ok.

%% -------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------

lazy_cluster_formation(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = [N || {N, _Peer} <- Nodes],
    
    ct:pal("Triggering acquire/4 on Node 1 (~p)", [Node1]),
    Pid1 = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),
    
    %% Verify Node 1 is the sole member
    Members1 = kh_members(Config, Node1),
    ?assertEqual(1, length(Members1)),
    
    ct:pal("Triggering acquire/4 on Node 2 (~p)", [Node2]),
    Pid2 = spawn_disposable(Config, Node2),
    ok = acq_ref_conn(Config, Node2, ?VH, ?CID2, ?USER, Pid2),
    
    %% Verify Node 2 joined the cluster
    Members2 = kh_members(Config, Node2),
    ?assertEqual(2, length(Members2)),
    
    ct:pal("Triggering acquire/4 on Node 3 (~p)", [Node3]),
    Pid3 = spawn_disposable(Config, Node3),
    ok = acq_ref_conn(Config, Node3, ?VH, ?CID3, ?USER, Pid3),
    
    %% Verify all 3 nodes are in the cluster
    Members3 = kh_members(Config, Node3),
    ?assertEqual(3, length(Members3)),

    %% Simulate a conflict with a connection on node 1
    Pid4 = spawn_disposable(Config, Node3),
    {error, refuse_connection} = acq_ref_conn(Config, Node3, ?VH, ?CID1, ?USER, Pid4),

    %% Cleanup the dummy processes
    kill_disposable(Config, Node1, Pid1),
    kill_disposable(Config, Node2, Pid2),
    kill_disposable(Config, Node3, Pid3),
    kill_disposable(Config, Node3, Pid4),
    ok.

cluster_should_grow_with_tick(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = [N || {N, _Peer} <- Nodes],

    ct:pal("Triggering acquire/4 on Node 1 (~p)", [Node1]),
    Pid1 = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),

    %% Verify at least one member
    Members1 = kh_members(Config, Node1),
    ?assert(length(Members1) > 0),

    %% Wait until all the nodes join
    rabbit_ct_helpers:eventually(?_assert(length(kh_members(Config, Node1)) =:= ?NODE_COUNT),
                                 1000, 10),

    %% Check nodes are working as expected
    Pid2 = spawn_disposable(Config, Node2),
    ok = acq_ref_conn(Config, Node2, ?VH, ?CID2, ?USER, Pid2),
    Pid3 = spawn_disposable(Config, Node3),
    ok = acq_ref_conn(Config, Node3, ?VH, ?CID3, ?USER, Pid3),

    %% Simulate a conflict with a connection on node 1
    Pid4 = spawn_disposable(Config, Node3),
    {error, refuse_connection} = acq_ref_conn(Config, Node3, ?VH, ?CID1, ?USER, Pid4),

    %% Cleanup the dummy processes
    kill_disposable(Config, Node1, Pid1),
    kill_disposable(Config, Node2, Pid2),
    kill_disposable(Config, Node3, Pid3),
    kill_disposable(Config, Node3, Pid4),
    ok.

cluster_should_shrink_with_tick(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = [N || {N, _Peer} <- Nodes],

    %% Bootstrap and grow the cluster to 3 nodes
    ct:pal("Triggering acquire/4 on Node 1 (~p)", [Node1]),
    Pid1 = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),

    %% Wait until all nodes join
    rabbit_ct_helpers:eventually(?_assertEqual(?NODE_COUNT, length(kh_members(Config, Node1))),
                                 1000, 10),

    %% Check node 2 is working as expected
    Pid2 = spawn_disposable(Config, Node2),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID2, ?USER, Pid2),

    %% Simulate 'rabbitmqctl forget_cluster_node' for Node 3
    ct:pal("Simulating formal removal of Node 3 (~p)", [Node3]),
    ReducedNodes = [Node1, Node2],
    lists:foreach(
        fun({Node, _Peer}) ->
            %% Dynamically update the mock to pretend Node 3 was permanently removed
            call(Config, Node, meck, expect, [rabbit_nodes, list_members, fun() -> ReducedNodes end]),
            call(Config, Node, meck, expect, [rabbit_nodes, list_running, fun() -> ReducedNodes end])
        end, Nodes),

    %% Wait for the leader's tick to detect the change and evict Node 3
    ct:pal("Waiting for tick to evict Node 3"),
    rabbit_ct_helpers:eventually(?_assertEqual(2, length(kh_members(Config, Node1))),
                                 1000, 10),

    %% Verify Node 3 is actually gone from Khepri
    CurrentMembers = kh_members(Config, Node1),
    CurrentNodes = [N || {_, N} <- CurrentMembers],
    ?assertNot(lists:member(Node3, CurrentNodes)),

    %% Check we can still detect a conflict
    Pid3 = spawn_disposable(Config, Node2),
    {error, refuse_connection} = acq_ref_conn(Config, Node1, ?VH, ?CID2, ?USER, Pid3),

    %% Cleanup
    kill_disposable(Config, Node1, Pid1),
    kill_disposable(Config, Node2, Pid2),
    kill_disposable(Config, Node2, Pid3),
    ok.

node_rejoins_cluster_after_graceful_shutdown(Config0) ->
    Nodes = ?config(peer_nodes, Config0),
    [Node1, Node2, Node3] = [N || {N, _Peer} <- Nodes],

    %% Form the initial cluster and write data
    Pid1 = spawn_disposable(Config0, Node1),
    ok = acq_ref_conn(Config0, Node1, ?VH, ?CID1, ?USER, Pid1),
    Pid2 = spawn_disposable(Config0, Node2),
    ok = acq_ref_conn(Config0, Node2, ?VH, ?CID2, ?USER, Pid2),
    Pid3 = spawn_disposable(Config0, Node3),
    ok = acq_ref_conn(Config0, Node3, ?VH, ?CID3, ?USER, Pid3),

    ct:pal("Gracefully flushing Node 3 RA metadata before VM kill"),
    call(Config0, Node3, application, stop, [khepri]),
    call(Config0, Node3, application, stop, [ra]),

    %% Stop Node 3 to simulate a crash/shutdown
    ct:pal("Stopping Node 3 (~p)", [Node3]),
    stop_erlang_node(Config0, Node3),

    %% Restart Node 3 (which re-uses the same on-disk DataDir)
    ct:pal("Restarting Node 3 (~p)", [Node3]),
    NewPeer3 = restart_node(Node3, Config0),

    %% Update the Config so call/5 uses the new control PID for Node 3
    NewNodes = lists:keyreplace(Node3, 1, Nodes, {Node3, NewPeer3}),
    Config1 = lists:keyreplace(peer_nodes, 1, Config0, {peer_nodes, NewNodes}),

    %% Execute the boot step recovery natively
    ct:pal("Executing recover/0 on Node 3"),
    ok = call(Config1, Node3, ?SOLE_CONN_MOD, recover, []),

    %% Verify Node 3 is fully operational and rejoined
    %% Check that the gen_server successfully started
    RecoveredPid = call(Config1, Node3, erlang, whereis, [?SOLE_CONN_MOD]),
    ?assert(is_pid(RecoveredPid)),

    RecoveredMembers = kh_members(Config1, Node3),
    ct:pal("Members: ~p", [RecoveredMembers]),
    ?assertEqual(?NODE_COUNT, length(RecoveredMembers)),

    %% Simulate a conflict with a connection on node 1
    Pid4 = spawn_disposable(Config1, Node3),
    {error, refuse_connection} = acq_ref_conn(Config1, Node3, ?VH, ?CID1, ?USER, Pid4),

    %% Cleanup the dummy processes
    kill_disposable(Config1, Node1, Pid1),
    kill_disposable(Config1, Node2, Pid2),
    %% (Pid3 was naturally killed when Node3 was stopped)
    kill_disposable(Config1, Node3, Pid4),
    ok.

node_rejoins_cluster_after_abrupt_shutdown(Config0) ->
    Nodes = ?config(peer_nodes, Config0),
    [Node1, Node2, Node3] = [N || {N, _Peer} <- Nodes],

    %% Form the initial cluster and write data
    Pid1 = spawn_disposable(Config0, Node1),
    ok = acq_ref_conn(Config0, Node1, ?VH, ?CID1, ?USER, Pid1),
    Pid2 = spawn_disposable(Config0, Node2),
    ok = acq_ref_conn(Config0, Node2, ?VH, ?CID2, ?USER, Pid2),
    Pid3 = spawn_disposable(Config0, Node3),
    ok = acq_ref_conn(Config0, Node3, ?VH, ?CID3, ?USER, Pid3),

    %% Stop Node 3 to simulate a crash/shutdown
    ct:pal("Stopping Node 3 (~p)", [Node3]),
    stop_erlang_node(Config0, Node3),

    %% Restart Node 3 (which re-uses the same on-disk DataDir)
    ct:pal("Restarting Node 3 (~p)", [Node3]),
    NewPeer3 = restart_node(Node3, Config0),

    %% Update the Config so call/5 uses the new control PID for Node 3
    NewNodes = lists:keyreplace(Node3, 1, Nodes, {Node3, NewPeer3}),
    Config1 = lists:keyreplace(peer_nodes, 1, Config0, {peer_nodes, NewNodes}),

    %% Execute the boot step recovery natively
    ct:pal("Executing recover/0 on Node 3"),
    ok = call(Config1, Node3, ?SOLE_CONN_MOD, recover, []),

    %% gen_server should not have been restarted
    ?assertEqual(
       undefined,
       call(Config1, Node3, erlang, whereis, [?SOLE_CONN_MOD])
    ),

    CurrentMembers = kh_members(Config1, Node1),
    ct:pal("Members: ~p", [CurrentMembers]),

    %% Make sure restarted node works.
    %% The lazily-triggered join of the just-restarted node can transiently
    %% crash if the remote cluster still addresses Raft messages to its
    %% not-yet-evicted ghost identity (see rabbit_amqp_sole_conn:start_local_store/0);
    %% retry until it succeeds instead of failing on the first attempt.
    Pid4 = spawn_disposable(Config1, Node3),
    rabbit_ct_helpers:eventually(
      ?_assertEqual(ok, acq_ref_conn(Config1, Node3, ?VH, ?CID3, ?USER, Pid4)),
      1000, 15),
    %% Make sure the system detects a conflict
    {error, refuse_connection} = acq_ref_conn(Config1, Node1, ?VH, ?CID3, ?USER, Pid1),

    %% Cleanup the dummy processes
    kill_disposable(Config1, Node1, Pid1),
    kill_disposable(Config1, Node2, Pid2),
    %% (Pid3 was naturally killed when Node3 was stopped)
    kill_disposable(Config1, Node3, Pid4),
    ok.

forget_node_should_remove_node(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = [N || {N, _Peer} <- Nodes],

    %% Form the initial cluster and write data
    Pid1 = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),
    Pid2 = spawn_disposable(Config, Node2),
    ok = acq_ref_conn(Config, Node2, ?VH, ?CID2, ?USER, Pid2),
    Pid3 = spawn_disposable(Config, Node3),
    ok = acq_ref_conn(Config, Node3, ?VH, ?CID3, ?USER, Pid3),

    ?assertEqual(?NODE_COUNT, length(kh_members(Config, Node1))),

    %% Simulate 'rabbitmqctl forget_cluster_node' for Node 3
    ct:pal("Simulating formal removal of Node 3 (~p)", [Node3]),
    ReducedNodes = [Node1, Node2],
    lists:foreach(
      fun({Node, _Peer}) ->
              %% Dynamically update the mock to pretend Node 3 was permanently removed
              call(Config, Node, meck, expect, [rabbit_nodes, list_members, fun() -> ReducedNodes end]),
              call(Config, Node, meck, expect, [rabbit_nodes, list_running, fun() -> ReducedNodes end])
      end, Nodes),

    ok = call(Config, Node1, ?SOLE_CONN_MOD, forget_node, [Node3]),

    ?assertEqual(?NODE_COUNT - 1, length(kh_members(Config, Node1))),

    %% no gen_server on node 3
    %% the stop is a cast, so we poll
    rabbit_ct_helpers:eventually(
      ?_assertEqual(
         undefined,
         call(Config, Node3, erlang, whereis, [?SOLE_CONN_MOD])
        ),
      1000, 10
     ),

    %% The tree node from the connection on node 3 should still be in the store
    %% so we should detect a conflict
    Pid4 = spawn_disposable(Config, Node1),
    {error, refuse_connection} = acq_ref_conn(Config, Node1, ?VH, ?CID3, ?USER, Pid4),

    %% Cleanup the dummy processes
    kill_disposable(Config, Node1, Pid1),
    kill_disposable(Config, Node2, Pid2),
    kill_disposable(Config, Node3, Pid3),
    kill_disposable(Config, Node3, Pid4),
    ok.

status_should_return_ra_metrics(Config) ->
    PeerNodes = ?config(peer_nodes, Config),
    Nodes = [Node1, Node2, Node3] = [N || {N, _Peer} <- PeerNodes],

    [begin
         ?assertEqual(
            {error, sole_conn_not_started_or_available},
            call(Config, Node, ?SOLE_CONN_MOD, status, [])
           )
     end || Node <- Nodes],


    %% Initialize the store on one node with a request
    Pid1 = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),

    ExtractNodeName = fun(Metrics) ->
                              proplists:get_value(<<"Node Name">>, Metrics)
                      end,
    ExtractNodeNames = fun(Status) ->
                               lists:sort([ExtractNodeName(NodeMetrics) || NodeMetrics <- Status])
                       end,
    GetNodeNames = fun(N) ->
                           ExtractNodeNames(call(Config, N, ?SOLE_CONN_MOD, status, []))
                   end,

    S1 = GetNodeNames(Node1),
    ?assertEqual(1, length(S1)),
    ?assertEqual([Node1], S1),
    %% Same result from other nodes
    ?assertEqual(S1, GetNodeNames(Node2)),
    ?assertEqual(S1, GetNodeNames(Node3)),

    %% Initialize the store on the second node
    Pid2 = spawn_disposable(Config, Node2),
    ok = acq_ref_conn(Config, Node2, ?VH, ?CID2, ?USER, Pid2),

    S2 = GetNodeNames(Node1),
    ?assertEqual(2, length(S2)),

    ?assertEqual(lists:sort(Nodes -- [Node3]), S2),
    %% Same status even from other nodes
    ?assertEqual(S2, GetNodeNames(Node2)),
    ?assertEqual(S2, GetNodeNames(Node3)),

    %% Initialize the store on the third node
    Pid3 = spawn_disposable(Config, Node3),
    ok = acq_ref_conn(Config, Node3, ?VH, ?CID3, ?USER, Pid3),

    S3 = GetNodeNames(Node1),
    ?assertEqual(3, length(S3)),

    ?assertEqual(lists:sort(Nodes), S3),
    %% Same status even from other nodes
    ?assertEqual(S3, GetNodeNames(Node2)),
    ?assertEqual(S3, GetNodeNames(Node3)),

    FinalStatus = call(Config, Node1, ?SOLE_CONN_MOD, status, []),
    %% Check there is only one leader
    RaftStates = [proplists:get_value(<<"Raft State">>, NodeMetrics) || NodeMetrics <- FinalStatus],
    LeaderCount = length([S || S <- RaftStates, S =:= leader]),
    ?assertEqual(1, LeaderCount),

    %% Cleanup the dummy processes
    kill_disposable(Config, Node1, Pid1),
    kill_disposable(Config, Node2, Pid2),
    kill_disposable(Config, Node3, Pid3),

    ok.

wipe_should_reset_store_on_all_nodes(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = AllNodes = [N || {N, _Peer} <- Nodes],

    %% Form the initial cluster and write data
    Pid1 = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),
    Pid2 = spawn_disposable(Config, Node2),
    ok = acq_ref_conn(Config, Node2, ?VH, ?CID2, ?USER, Pid2),
    Pid3 = spawn_disposable(Config, Node3),
    ok = acq_ref_conn(Config, Node3, ?VH, ?CID3, ?USER, Pid3),

    ?assertEqual(?NODE_COUNT, length(kh_members(Config, Node1))),

    ct:pal("Wiping the sole_conn Khepri store from Node 1 (~p)", [Node1]),
    WipeResult = call(Config, Node1, ?SOLE_CONN_MOD, wipe, []),
    ?assertEqual(
       lists:sort([{N, ok} || N <- AllNodes]),
       lists:sort(WipeResult)),

    %% The wiped-out connections' pids are no longer relevant
    kill_disposable(Config, Node1, Pid1),
    kill_disposable(Config, Node2, Pid2),
    kill_disposable(Config, Node3, Pid3),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 undefined,
                 call(Config, Node, erlang, whereis, [?SOLE_CONN_MOD])),
              ?assertEqual(
                 undefined,
                 call(Config, Node, ra_directory, uid_of, [coordination, ?STORE_ID]))
      end, AllNodes),

    %% The system must be able to bootstrap from scratch again, using the
    %% eager start/0 operator command rather than waiting for the first
    %% acquire/5 call to trigger the lazy bootstrap on each node
    ct:pal("Starting the sole_conn Khepri store from Node 1 (~p)", [Node1]),
    StartResult = call(Config, Node1, ?SOLE_CONN_MOD, start, []),
    ?assertEqual(
       lists:sort([{N, ok} || N <- AllNodes]),
       lists:sort(StartResult)),

    lists:foreach(
      fun(Node) ->
              ?assert(is_pid(call(Config, Node, erlang, whereis, [?SOLE_CONN_MOD])))
      end, AllNodes),
    ?assertEqual(?NODE_COUNT, length(kh_members(Config, Node1))),

    %% Check the restarted store is functional
    Pid1b = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1b),
    Pid2b = spawn_disposable(Config, Node2),
    ok = acq_ref_conn(Config, Node2, ?VH, ?CID2, ?USER, Pid2b),
    Pid3b = spawn_disposable(Config, Node3),
    ok = acq_ref_conn(Config, Node3, ?VH, ?CID3, ?USER, Pid3b),

    %% Cleanup the dummy processes
    kill_disposable(Config, Node1, Pid1b),
    kill_disposable(Config, Node2, Pid2b),
    kill_disposable(Config, Node3, Pid3b),
    ok.

start_should_bootstrap_store_on_all_nodes(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node1, Node2, Node3] = AllNodes = [N || {N, _Peer} <- Nodes],

    %% Nothing has called acquire yet, so the store was never bootstrapped
    %% on any node
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 undefined,
                 call(Config, Node, erlang, whereis, [?SOLE_CONN_MOD]))
      end, AllNodes),

    ct:pal("Starting the sole_conn Khepri store from Node 1 (~p)", [Node1]),
    StartResult = call(Config, Node1, ?SOLE_CONN_MOD, start, []),
    ?assertEqual(
       lists:sort([{N, ok} || N <- AllNodes]),
       lists:sort(StartResult)),

    lists:foreach(
      fun(Node) ->
              ?assert(is_pid(call(Config, Node, erlang, whereis, [?SOLE_CONN_MOD])))
      end, AllNodes),
    ?assertEqual(?NODE_COUNT, length(kh_members(Config, Node1))),

    %% Check the store is functional on every node
    Pid1 = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),
    Pid2 = spawn_disposable(Config, Node2),
    ok = acq_ref_conn(Config, Node2, ?VH, ?CID2, ?USER, Pid2),
    Pid3 = spawn_disposable(Config, Node3),
    ok = acq_ref_conn(Config, Node3, ?VH, ?CID3, ?USER, Pid3),

    %% Simulate a conflict with a connection on node 1
    Pid4 = spawn_disposable(Config, Node3),
    {error, refuse_connection} = acq_ref_conn(Config, Node3, ?VH, ?CID1, ?USER, Pid4),

    %% Cleanup the dummy processes
    kill_disposable(Config, Node1, Pid1),
    kill_disposable(Config, Node2, Pid2),
    kill_disposable(Config, Node3, Pid3),
    kill_disposable(Config, Node3, Pid4),
    ok.

conflict_should_resolve_when_existing_conns_node_is_unreachable(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node1, Node2, _Node3] = [N || {N, _Peer} <- Nodes],

    %% Acquire a lease on Node1
    Pid1 = spawn_disposable(Config, Node1),
    ok = acq_ref_conn(Config, Node1, ?VH, ?CID1, ?USER, Pid1),

    %% Simulate what a real network partition looks like from Node2's point
    %% of view: the cross-node liveness check for Pid1 (on Node1) fails,
    %% even though Pid1 is genuinely still alive. This is exactly what
    %% check_conn/1 sees when the node hosting the existing connection
    %% becomes unreachable.
    call(Config, Node2, meck, new, [erpc, [passthrough, unstick, no_link]]),
    call(Config, Node2, meck, expect,
         [erpc, call,
          fun(Node, erlang, is_process_alive, [Pid], _Timeout)
                when Node =:= Node1, Pid =:= Pid1 ->
                  erlang:error({erpc, noconnection});
             (N, M, F, A, T) ->
                  meck:passthrough([N, M, F, A, T])
          end]),

    ?assert(call(Config, Node1, erlang, is_process_alive, [Pid1])),

    %% A conflicting acquire from Node2 must succeed: Node2 cannot verify
    %% Pid1 is alive, so it treats the existing lease as dead and takes over
    Pid2 = spawn_disposable(Config, Node2),
    ?assertEqual(ok, acq_ref_conn(Config, Node2, ?VH, ?CID1, ?USER, Pid2)),

    call(Config, Node2, meck, unload, [erpc]),

    %% Cleanup the dummy processes
    kill_disposable(Config, Node1, Pid1),
    kill_disposable(Config, Node2, Pid2),
    ok.

%% --------------------------------------------------------------
%% Internal Helpers
%% --------------------------------------------------------------

start_epmd() ->
    RootDir = code:root_dir(),
    ErtsVersion = erlang:system_info(version),
    ErtsDir = lists:flatten(io_lib:format("erts-~ts", [ErtsVersion])),
    EpmdPath0 = filename:join([RootDir, ErtsDir, "bin", "epmd"]),
    EpmdPath = case os:type() of
                   {win32, _} -> EpmdPath0 ++ ".exe";
                   _          -> EpmdPath0
               end,
    Port = erlang:open_port(
             {spawn_executable, EpmdPath},
             [{args, ["-daemon"]}]),
    erlang:port_close(Port),
    ok.

start_n_nodes(Prefix, Count, Config) ->
    Nodes = [begin
                 Name = list_to_atom(lists:flatten(
                                       io_lib:format("~s-~s-~b",
                                                     [?MODULE, Prefix, I]))),
                 {ok, Peer, Node} = peer:start(#{name => Name, connection => standard_io}),
                 {Node, Peer}
             end || I <- lists:seq(1, Count)],

    lists:foreach(
        fun({Node, Peer}) ->
            ok = init_node({Node, Peer}, Config)
        end, Nodes),
    Nodes.

init_node({Node, Peer}, Config) ->
    PrivDir = ?config(priv_dir, Config),
    CodePath = code:get_path(),

    %% Setup a unique DataDir for this specific peer
    DataDir = filename:join(PrivDir, rabbit_misc:format("data-~ts", [Node])),
    filelib:ensure_dir(filename:join(DataDir, "dummy")),

    peer:call(Peer, code, add_pathsz, [CodePath]),
    peer:call(Peer, ?MODULE, setup_node, [], infinity),
    %% Load the rabbit application and set the env variables
    case peer:call(Peer, application, load, [rabbit]) of
        ok                           -> ok;
        {error, {already_loaded, _}} -> ok
    end,
    ct:pal("Using data_dir ~p for node ~p", [DataDir, Node]),
    ok = peer:call(Peer, application, set_env, [rabbit, data_dir, DataDir]),

    %% Start the dependencies
    {ok, _} = peer:call(Peer, application, ensure_all_started, [khepri]),
    ok = peer:call(Peer, rabbit_ra_systems, ensure_ra_system_started, [coordination]),
    ok.



%% Restarts an existing node using its exact previous short-name and DataDir,
%% and re-establishes the test mocks for the new VM.
restart_node(Node, Config) ->
    %% peer:start/1 expects a short name (e.g. 'node_1'), not the full node address.
    %% We split the existing atom 'node_1@hostname' to extract just the short name.
    [ShortName, _Host] = string:split(atom_to_list(Node), "@"),
    Name = list_to_atom(ShortName),

    {ok, Peer, Node} = peer:start(#{name => Name, connection => standard_io}),
    ok = init_node({Node, Peer}, Config),

    setup_mocks(Config, Node),
    Peer.

stop_erlang_node(Config, Node) ->
    Nodes = ?config(peer_nodes, Config),
    case proplists:get_value(Node, Nodes) of
        undefined -> ok;
        Peer ->
            case is_process_alive(Peer) of
                true ->
                    peer:stop(Peer);
                false ->
                    try
                        erpc:cast(Node, erlang, halt, [])
                    catch
                        _:_ -> ok
                    end
            end
    end.

setup_mocks(Config, Node) ->
    %% Mock rabbit_sup to bypass RabbitMQ boot
    call(Config, Node, meck, new, [rabbit_sup, [passthrough, no_link]]),
    call(Config, Node, meck, expect,
         [rabbit_sup, start_child,
          fun(?SOLE_CONN_MOD) ->
                  gen_server:start({local, ?SOLE_CONN_MOD},
                                   ?SOLE_CONN_MOD, [], []),
                  ok
          end]),
    call(Config, Node, meck, expect,
         [rabbit_sup, stop_child,
          fun(?SOLE_CONN_MOD) ->
                  gen_server:stop(?SOLE_CONN_MOD),
                  ok
          end]),

    NodeNames = node_names(Config),
    %% Mock rabbit_nodes to return our 3 peers
    call(Config, Node, meck, new, [rabbit_nodes, [passthrough, no_link]]),
    call(Config, Node, meck, expect, [rabbit_nodes, list_running,
                                      fun() -> NodeNames end]),
    call(Config, Node, meck, expect, [rabbit_nodes, list_members,
                                      fun() -> NodeNames end]),
    call(Config, Node, meck, expect, [rabbit_nodes, list_reachable,
                                      fun() -> NodeNames end]),
    call(Config, Node, meck, expect, [rabbit, is_running,
                                      fun() -> true end]),

    %% assume the feature flag is enabled
    call(Config, Node, meck, new, [rabbit_feature_flags, [passthrough, no_link]]),
    call(Config, Node, meck, expect, [rabbit_feature_flags, is_enabled,
                                      fun(_) -> true end]),
    ok.

node_names(Config) ->
    Nodes = ?config(peer_nodes, Config),
    [Node || {Node, _Peer} <- Nodes].

kh_members(Config, Node) ->
    {ok, Members} = call(Config, Node, khepri_cluster, members, [?STORE_ID]),
    Members.

acq_ref_conn(Config, Node, VH, CID, Username, Pid) ->
    call(Config, Node, ?SOLE_CONN_MOD, acquire,
         [refuse_connection, VH, CID, Username, Pid]).

call(Config, Node, Module, Func, Args) ->
    Nodes = ?config(peer_nodes, Config),
    case proplists:get_value(Node, Nodes) of
        undefined ->
            case Node =:= node() of
                true -> erlang:apply(Module, Func, Args);
                false -> erlang:error({unknown_node, Node})
            end;
        Peer ->
            case is_process_alive(Peer) of
                true ->
                    peer:call(Peer, Module, Func, Args, infinity);
                false ->
                    %% Fallback control channel (TCP Distribution)
                    %% Used when a test dynamically restarts a node.
                    erpc:call(Node, Module, Func, Args, infinity)
            end
    end.

%% Forces logger to debug level, disables burst limits, and applies a clean single-line
%% formatting template to make Common Test HTML reports readable and verbose.
basic_logger_config() ->
    _ = logger:set_primary_config(level, debug),
    HandlerIds = [HandlerId ||
                  HandlerId <- logger:get_handler_ids(),
                  HandlerId =:= default orelse
                  HandlerId =:= cth_log_redirect],
    lists:foreach(
      fun(HandlerId) ->
              ok = logger:set_handler_config(
                    HandlerId, formatter,
                    {logger_formatter, ?LOGFMT_CONFIG}),
              ok = logger:update_handler_config(
                    HandlerId, config, #{burst_limit_enable => false}),
              _ = logger:add_handler_filter(
                    HandlerId, progress,
                    {fun logger_filters:progress/2,stop}),
              _ = logger:remove_handler_filter(
                    HandlerId, remote_gl)
      end, HandlerIds),
    ok.

%% Configures peer nodes to format logs cleanly for standard_io capture,
%% and sets strict Khepri timeouts so cluster tests fail fast instead of hanging.
setup_node() ->
    basic_logger_config(),
    
    %% Set strict timeouts for Khepri so we don't wait forever during network splits
    ok = application:set_env(
           khepri, default_timeout, 5000, [{persistent, true}]),
    ok.

spawn_disposable(Config, Node) ->
    call(Config, Node, erlang, spawn, [fun() -> receive die -> ok end end]).

kill_disposable(Config, Node, Pid) ->
    call(Config, Node, erlang, exit, [Pid, kill]).
