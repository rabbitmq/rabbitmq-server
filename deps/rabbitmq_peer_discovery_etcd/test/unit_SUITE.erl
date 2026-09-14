%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2007-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved. All rights reserved.
%%

-module(unit_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("rabbit_peer_discovery_etcd.hrl").

-import(rabbit_data_coercion, [to_binary/1]).


all() ->
    [
     {group, unit}
    ].

groups() ->
    [
     {unit, [], [
                    registration_value_test,
                    extract_nodes_case1_test,
                    filter_nodes_test,
                    node_key_base_test,
                    node_key_test,
                    lock_key_base_test,
                    lock_call_timeout_test,
                    clear_lease_keepalive_test,
                    keepalive_halted_info_test,
                    connected_enter_keeps_existing_node_lease_test,
                    node_key_lease_is_healthy_test,
                    connected_enter_regrants_when_keepalive_dead_test,
                    recover_unlock_releases_lock_lease_test,
                    connected_enter_survives_lease_grant_failure_test,
                    connected_enter_discards_fresh_lease_on_put_failure_test,
                    register_replies_with_put_error_on_fresh_lease_test,
                    keepalive_halted_schedules_retry_for_registered_node_key_test,
                    keepalive_halted_skips_retry_for_unregistered_node_key_test,
                    retry_node_key_lease_state_timeout_repeats_state_test,
                    lock_success_monitors_owner_test,
                    lock_owner_down_releases_lock_test,
                    lock_lease_lost_aborts_owner_test,
                    recover_lock_owner_down_stops_lease_keepalive_test,
                    recover_lock_lease_lost_aborts_owner_test,
                    lock_lease_lost_flushes_pending_owner_down_test,
                    terminate_stops_lease_keepalives_test,
                    register_persists_intent_across_client_restart_test,
                    unregister_clears_persisted_registration_intent_test,
                    init_seeds_node_key_registered_from_persisted_intent_test
                ]}
    ].

end_per_testcase(_TestCase, _Config) ->
    %% guards against a failed meck-based assertion above leaving a mock loaded
    catch meck:unload(),
    %% guards against one test's registration intent leaking into the next,
    %% since it is stored in a persistent_term rather than process state
    _ = persistent_term:erase({rabbitmq_peer_discovery_etcd_v3_client, node_key_registered}),
    ok.

%%
%% Test cases
%%

registration_value_test(_Config) ->
    LeaseID = 8488283859587364900,
    TTL     = 61,
    Input   = #statem_data{
        node_key_lease_id = LeaseID,
        node_key_ttl_in_seconds = TTL
    },
    Expected = registration_value_of(LeaseID, TTL),
    ?assertEqual(Expected, rabbitmq_peer_discovery_etcd_v3_client:registration_value(Input)).


extract_nodes_case1_test(_Config) ->
    Input    = registration_value_of(8488283859587364900, 61),
    Expected = node(),
    CreatedRev = ?LINE,
    ?assertEqual({true, {CreatedRev, Expected}},
                 rabbitmq_peer_discovery_etcd_v3_client:extract_node(
                   {CreatedRev, Input})),

    ?assertEqual(false,
                 rabbitmq_peer_discovery_etcd_v3_client:extract_node(
                   {CreatedRev, <<"{}">>})).

filter_nodes_test(_Config) ->
    Input    = [node(), undefined, undefined, {error, reason1}, {error, {another, reason}}],
    Expected = [node()],

    ?assertEqual(Expected, lists:filter(fun rabbitmq_peer_discovery_etcd_v3_client:filter_node/1, Input)).

node_key_base_test(_Config) ->
    Expected = <<"/rabbitmq/discovery/prefffix/clusters/cluster-a/nodes">>,
    Input = #statem_data{
        cluster_name = "cluster-a",
        key_prefix = "prefffix"
    },
    ?assertEqual(Expected, rabbitmq_peer_discovery_etcd_v3_client:node_key_base(Input)).

node_key_test(_Config) ->
    Expected = to_binary(rabbit_misc:format("/rabbitmq/discovery/prefffix/clusters/cluster-a/nodes/~ts", [node()])),
    Input = #statem_data{
        cluster_name = "cluster-a",
        key_prefix = "prefffix"
    },
    ?assertEqual(Expected, rabbitmq_peer_discovery_etcd_v3_client:node_key(Input)).

lock_key_base_test(_Config) ->
    Expected = <<"/rabbitmq/locks/prefffix/clusters/cluster-b/registration">>,
    Input = #statem_data{
        cluster_name = "cluster-b",
        key_prefix = "prefffix"
    },
    ?assertEqual(Expected, rabbitmq_peer_discovery_etcd_v3_client:lock_key_base(Input)).

lock_call_timeout_test(_Config) ->
    ?assertEqual(45000 + 20000, rabbitmq_peer_discovery_etcd_v3_client:lock_call_timeout(45)),
    ?assertEqual(30000 + 20000, rabbitmq_peer_discovery_etcd_v3_client:lock_call_timeout(30)),
    ?assertEqual(15000, rabbitmq_peer_discovery_etcd_v3_client:lock_call_timeout(undefined)).

clear_lease_keepalive_test(_Config) ->
    NodeData = #statem_data{node_key_lease_id = 1, node_lease_keepalive_pid = self()},
    ?assertEqual(
        NodeData#statem_data{node_key_lease_id = undefined, node_lease_keepalive_pid = undefined},
        rabbitmq_peer_discovery_etcd_v3_client:clear_lease_keepalive(1, NodeData)),

    LockData = #statem_data{lock_lease_id = 2, lock_lease_keepalive_pid = self()},
    ?assertEqual(
        LockData#statem_data{lock_lease_id = undefined, lock_lease_keepalive_pid = undefined},
        rabbitmq_peer_discovery_etcd_v3_client:clear_lease_keepalive(2, LockData)),

    %% a halt for a lease matching neither field is a no-op, not a crash
    UnrelatedData = #statem_data{node_key_lease_id = 1, lock_lease_id = 2},
    ?assertEqual(
        UnrelatedData,
        rabbitmq_peer_discovery_etcd_v3_client:clear_lease_keepalive(3, UnrelatedData)).

%% exercises connected/3 and recover/3 directly: the bug covered here is a
%% function_clause crash on an unmatched info message, not a defect in
%% the pure clear_lease_keepalive/2 helper
keepalive_halted_info_test(_Config) ->
    Data = #statem_data{lock_lease_id = 2, lock_lease_keepalive_pid = self()},
    Event = #{event => 'KeepAliveHalted', lease_id => 2, reason => <<"lease not found">>},

    ?assertMatch(
        {keep_state, #statem_data{lock_lease_id = undefined, lock_lease_keepalive_pid = undefined}},
        rabbitmq_peer_discovery_etcd_v3_client:connected(info, Event, Data)),

    ?assertMatch(
        {keep_state, #statem_data{lock_lease_id = undefined, lock_lease_keepalive_pid = undefined}},
        rabbitmq_peer_discovery_etcd_v3_client:recover(info, Event, Data)).

%% eetcd_lease keeps renewing the node-key lease across a reconnect on its
%% own, so state_enter must leave an already-held lease alone
connected_enter_keeps_existing_node_lease_test(_Config) ->
    Data = #statem_data{node_key_lease_id = 1, node_lease_keepalive_pid = self()},
    ?assertEqual(
        keep_state_and_data,
        rabbitmq_peer_discovery_etcd_v3_client:connected(enter, recover, Data)).

%% node_key_lease_id alone does not prove the keep-alive is still running,
%% since it can exit without sending KeepAliveHalted
node_key_lease_is_healthy_test(_Config) ->
    DeadPid = spawn(fun() -> ok end),
    ct:sleep(10),
    ?assertNot(is_process_alive(DeadPid)),

    ?assertNot(rabbitmq_peer_discovery_etcd_v3_client:node_key_lease_is_healthy(
        #statem_data{node_key_lease_id = undefined, node_lease_keepalive_pid = self()})),
    ?assertNot(rabbitmq_peer_discovery_etcd_v3_client:node_key_lease_is_healthy(
        #statem_data{node_key_lease_id = 1, node_lease_keepalive_pid = DeadPid})),
    ?assert(rabbitmq_peer_discovery_etcd_v3_client:node_key_lease_is_healthy(
        #statem_data{node_key_lease_id = 1, node_lease_keepalive_pid = self()})).

%% counterpart of connected_enter_keeps_existing_node_lease_test/1: a
%% node_key_lease_id left over from a dead keep-alive must not be treated
%% as still held, so connected(enter) must attempt, and apply, a fresh grant
connected_enter_regrants_when_keepalive_dead_test(_Config) ->
    DeadPid = spawn(fun() -> ok end),
    ct:sleep(10),
    ?assertNot(is_process_alive(DeadPid)),

    FreshKeepalivePid = spawn(fun keep_alive_stub/0),
    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, grant, fun(_Name, _TTL) -> {ok, #{'ID' => 999}} end),
    meck:expect(eetcd_lease, keep_alive, fun(_Name, _LeaseID) -> {ok, FreshKeepalivePid} end),

    Data = #statem_data{
        node_key_lease_id = 1,
        node_lease_keepalive_pid = DeadPid,
        connection_name = ?MODULE,
        node_key_ttl_in_seconds = 60
    },
    {keep_state, FinalData} = rabbitmq_peer_discovery_etcd_v3_client:connected(enter, recover, Data),
    ?assertEqual(999, FinalData#statem_data.node_key_lease_id),
    ?assertEqual(FreshKeepalivePid, FinalData#statem_data.node_lease_keepalive_pid),
    exit(FreshKeepalivePid, kill).

%% the connection can drop between a successful {lock, Node} and the
%% matching unlock/2 call; recover/3 must stop the lock lease's
%% keep-alive so the lease lapses at its TTL instead of renewing forever
recover_unlock_releases_lock_lease_test(_Config) ->
    Data = #statem_data{lock_lease_id = 2, lock_lease_keepalive_pid = self()},
    Tag = make_ref(),
    ?assertMatch(
        {keep_state, #statem_data{lock_lease_id = undefined, lock_lease_keepalive_pid = undefined}},
        rabbitmq_peer_discovery_etcd_v3_client:recover({call, {self(), Tag}}, {unlock, <<"some-key">>}, Data)),
    receive
        {Tag, Reply} -> ?assertEqual({error, not_connected}, Reply)
    after 1000 ->
        ?assert(false)
    end,
    receive
        {'$gen_cast', close} -> ok
    after 1000 ->
        ?assert(false)
    end.

%% connected(enter) must survive a failed eetcd_lease:grant/2, leave the
%% lease fields unhealthy, and schedule a retry rather than waiting for
%% an unrelated reconnect
connected_enter_survives_lease_grant_failure_test(_Config) ->
    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, grant, fun(_Name, _TTL) -> {error, mocked_grant_failure} end),

    Data = #statem_data{
        connection_name = ?MODULE,
        cluster_name = "cluster-a",
        key_prefix = "prefffix",
        node_key_ttl_in_seconds = 60
    },
    ?assertMatch(
        {keep_state_and_data, [{state_timeout, _, retry_node_key_lease}]},
        rabbitmq_peer_discovery_etcd_v3_client:connected(enter, recover, Data)).

%% a node key lease freshly granted by connected(enter)'s repair path,
%% whose PUT then fails, must be discarded rather than kept, or this node
%% would stay silently missing from etcd-based peer discovery
connected_enter_discards_fresh_lease_on_put_failure_test(_Config) ->
    FreshKeepalivePid = spawn(fun keep_alive_stub/0),
    StaleKeepalivePid = spawn(fun() -> ok end),
    ct:sleep(10),
    ?assertNot(is_process_alive(StaleKeepalivePid)),

    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, grant, fun(_Name, _TTL) -> {ok, #{'ID' => 999}} end),
    meck:expect(eetcd_lease, keep_alive, fun(_Name, _LeaseID) -> {ok, FreshKeepalivePid} end),
    meck:expect(eetcd_lease, revoke, fun(_Ctx, _LeaseID) -> {ok, #{}} end),
    meck:new(eetcd_kv, [passthrough]),
    meck:expect(eetcd_kv, put, fun(_Ctx, _Key, _Value) -> {error, mocked_put_failure} end),

    Data = #statem_data{
        connection_name = ?MODULE,
        cluster_name = "cluster-a",
        key_prefix = "prefffix",
        node_key_ttl_in_seconds = 60,
        node_key_registered = true,
        node_key_lease_id = 1,
        node_lease_keepalive_pid = StaleKeepalivePid
    },
    {keep_state, FinalData, [{state_timeout, _, retry_node_key_lease}]} =
        rabbitmq_peer_discovery_etcd_v3_client:connected(enter, recover, Data),
    ?assertEqual(undefined, FinalData#statem_data.node_key_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.node_lease_keepalive_pid),
    ?assertNot(rabbitmq_peer_discovery_etcd_v3_client:node_key_lease_is_healthy(FinalData)),
    exit(FreshKeepalivePid, kill).

%% when register/1 grants a fresh node key lease and the subsequent PUT
%% fails, it must reply with the PUT's own error instead of ok, but must
%% still record that registration was requested and schedule a retry, or
%% this, the very first registration attempt, is never repaired
register_replies_with_put_error_on_fresh_lease_test(_Config) ->
    FreshKeepalivePid = spawn(fun keep_alive_stub/0),

    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, grant, fun(_Name, _TTL) -> {ok, #{'ID' => 999}} end),
    meck:expect(eetcd_lease, keep_alive, fun(_Name, _LeaseID) -> {ok, FreshKeepalivePid} end),
    meck:expect(eetcd_lease, revoke, fun(_Ctx, _LeaseID) -> {ok, #{}} end),
    meck:new(eetcd_kv, [passthrough]),
    meck:expect(eetcd_kv, put, fun(_Ctx, _Key, _Value) -> {error, mocked_put_failure} end),

    Data0 = #statem_data{
        connection_name = ?MODULE,
        cluster_name = "cluster-a",
        key_prefix = "prefffix",
        node_key_ttl_in_seconds = 60,
        node_key_registered = false
    },
    Tag = make_ref(),
    {keep_state, FinalData, [{state_timeout, _, retry_node_key_lease}]} =
        rabbitmq_peer_discovery_etcd_v3_client:connected({call, {self(), Tag}}, register, Data0),
    receive
        {Tag, Reply} -> ?assertEqual({error, mocked_put_failure}, Reply)
    after 1000 ->
        ?assert(false)
    end,
    ?assert(FinalData#statem_data.node_key_registered),
    %% the freshly granted lease must also be discarded, not just left unregistered
    ?assertEqual(undefined, FinalData#statem_data.node_key_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.node_lease_keepalive_pid),
    ?assertNot(rabbitmq_peer_discovery_etcd_v3_client:node_key_lease_is_healthy(FinalData)),
    exit(FreshKeepalivePid, kill).

%% a KeepAliveHalted for the node key lease while the node is registered
%% must schedule a retry, or the node stays absent from discovery until
%% an unrelated reconnect happens to repair it
keepalive_halted_schedules_retry_for_registered_node_key_test(_Config) ->
    Data = #statem_data{
        node_key_lease_id = 1,
        node_lease_keepalive_pid = self(),
        node_key_registered = true
    },
    Event = #{event => 'KeepAliveHalted', lease_id => 1, reason => <<"lease not found">>},
    {keep_state, FinalData, [{state_timeout, _, retry_node_key_lease}]} =
        rabbitmq_peer_discovery_etcd_v3_client:connected(info, Event, Data),
    ?assertEqual(undefined, FinalData#statem_data.node_key_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.node_lease_keepalive_pid).

%% a halt for a node key lease that was never successfully registered has
%% nothing to repair, so no retry needs to be scheduled
keepalive_halted_skips_retry_for_unregistered_node_key_test(_Config) ->
    Data = #statem_data{
        node_key_lease_id = 1,
        node_lease_keepalive_pid = self(),
        node_key_registered = false
    },
    Event = #{event => 'KeepAliveHalted', lease_id => 1, reason => <<"lease not found">>},
    ?assertEqual(
        {keep_state, Data#statem_data{node_key_lease_id = undefined, node_lease_keepalive_pid = undefined}},
        rabbitmq_peer_discovery_etcd_v3_client:connected(info, Event, Data)).

%% the state_timeout set by the retry actions above must re-run
%% connected(enter,...) via repeat_state, not merely keep the state
retry_node_key_lease_state_timeout_repeats_state_test(_Config) ->
    Data = #statem_data{node_key_lease_id = 1, node_lease_keepalive_pid = self()},
    ?assertEqual(
        {repeat_state, Data},
        rabbitmq_peer_discovery_etcd_v3_client:connected(state_timeout, retry_node_key_lease, Data)).

%% a successfully acquired lock must monitor its caller, so the lock can
%% be released if that caller dies before calling unlock/1
lock_success_monitors_owner_test(_Config) ->
    %% gen_statem:reply/2 replies to whichever process is in From, so the
    %% owner being monitored must be the calling (this test) process
    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, grant, fun(_Ctx, _TTL) -> {ok, #{'ID' => 42}} end),
    meck:expect(eetcd_lease, keep_alive, fun(_Conn, _LeaseID) -> {ok, self()} end),
    meck:new(eetcd_lock, [passthrough]),
    meck:expect(eetcd_lock, lock, fun(_Ctx, _Key, _LeaseID) -> {ok, #{key => <<"owner-key">>}} end),

    Data = #statem_data{connection_name = ?MODULE, lock_ttl_in_seconds = 30},
    Tag = make_ref(),
    Self = self(),
    {keep_state, FinalData} = rabbitmq_peer_discovery_etcd_v3_client:connected({call, {Self, Tag}}, {lock, node()}, Data),
    ?assertEqual(Self, FinalData#statem_data.lock_owner_pid),
    ?assert(is_reference(FinalData#statem_data.lock_owner_monitor)),
    receive
        {Tag, Reply} -> ?assertEqual({ok, <<"owner-key">>}, Reply)
    after 1000 ->
        ?assert(false)
    end,
    demonitor(FinalData#statem_data.lock_owner_monitor, [flush]).

%% if the lock owner dies without calling unlock/1, the lock and its
%% lease must be released, or the independently supervised keep-alive
%% worker renews it, and holds the lock, forever
lock_owner_down_releases_lock_test(_Config) ->
    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, revoke, fun(_Ctx, _LeaseID) -> {ok, #{}} end),

    Owner = spawn(fun() -> ok end),
    OwnerMonitor = monitor(process, Owner),
    ct:sleep(10),
    receive
        {'DOWN', OwnerMonitor, process, Owner, _Reason} -> ok
    after 1000 ->
        ?assert(false)
    end,

    Data = #statem_data{
        connection_name = ?MODULE,
        lock_lease_id = 42,
        lock_lease_keepalive_pid = self(),
        lock_owner_pid = Owner,
        lock_owner_monitor = OwnerMonitor
    },
    {keep_state, FinalData} = rabbitmq_peer_discovery_etcd_v3_client:connected(
        info, {'DOWN', OwnerMonitor, process, Owner, noproc}, Data),
    ?assertEqual(undefined, FinalData#statem_data.lock_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.lock_lease_keepalive_pid),
    ?assertEqual(undefined, FinalData#statem_data.lock_owner_pid),
    ?assertEqual(undefined, FinalData#statem_data.lock_owner_monitor),
    receive
        {'$gen_cast', close} -> ok
    after 1000 ->
        ?assert(false)
    end.

%% losing the lock lease's keep-alive while it is held means mutual
%% exclusion is gone, so the owner must be aborted rather than left to
%% continue as if it still held the lock
lock_lease_lost_aborts_owner_test(_Config) ->
    process_flag(trap_exit, true),
    Owner = spawn_link(fun keep_alive_stub/0),
    OwnerMonitor = monitor(process, Owner),

    Data = #statem_data{
        lock_lease_id = 42,
        lock_lease_keepalive_pid = self(),
        lock_owner_pid = Owner,
        lock_owner_monitor = OwnerMonitor
    },
    Event = #{event => 'KeepAliveHalted', lease_id => 42, reason => <<"lease not found">>},
    {keep_state, FinalData} = rabbitmq_peer_discovery_etcd_v3_client:connected(info, Event, Data),
    ?assertEqual(undefined, FinalData#statem_data.lock_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.lock_lease_keepalive_pid),
    ?assertEqual(undefined, FinalData#statem_data.lock_owner_pid),
    ?assertEqual(undefined, FinalData#statem_data.lock_owner_monitor),
    receive
        {'EXIT', Owner, {etcd_lock_lease_lost, 42}} -> ok
    after 1000 ->
        ?assert(false)
    end,
    process_flag(trap_exit, false).

%% recover/3 releases the lock lease when the owner dies while disconnected
%% (see recover_unlock_releases_lock_lease_test above for the unlock/2
%% counterpart), but must also stop the lock lease's keep-alive, or the
%% independently supervised eetcd worker keeps renewing a lock nobody owns
recover_lock_owner_down_stops_lease_keepalive_test(_Config) ->
    Owner = spawn(fun() -> ok end),
    OwnerMonitor = monitor(process, Owner),
    ct:sleep(10),
    receive
        {'DOWN', OwnerMonitor, process, Owner, _Reason} -> ok
    after 1000 ->
        ?assert(false)
    end,

    Data = #statem_data{
        lock_lease_id = 42,
        lock_lease_keepalive_pid = self(),
        lock_owner_pid = Owner,
        lock_owner_monitor = OwnerMonitor
    },
    {keep_state, FinalData} = rabbitmq_peer_discovery_etcd_v3_client:recover(
        info, {'DOWN', OwnerMonitor, process, Owner, noproc}, Data),
    ?assertEqual(undefined, FinalData#statem_data.lock_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.lock_lease_keepalive_pid),
    ?assertEqual(undefined, FinalData#statem_data.lock_owner_pid),
    ?assertEqual(undefined, FinalData#statem_data.lock_owner_monitor),
    receive
        {'$gen_cast', close} -> ok
    after 1000 ->
        ?assert(false)
    end.

%% recover/3's generic KeepAliveHalted clause must abort the lock owner
%% when the halted lease is the lock's own, the same as connected/3 does
%% (see lock_lease_lost_aborts_owner_test above); otherwise a lock owner
%% that is still running while disconnected proceeds as if it still held
%% mutual exclusion
recover_lock_lease_lost_aborts_owner_test(_Config) ->
    process_flag(trap_exit, true),
    Owner = spawn_link(fun keep_alive_stub/0),
    OwnerMonitor = monitor(process, Owner),

    Data = #statem_data{
        lock_lease_id = 42,
        lock_lease_keepalive_pid = self(),
        lock_owner_pid = Owner,
        lock_owner_monitor = OwnerMonitor
    },
    Event = #{event => 'KeepAliveHalted', lease_id => 42, reason => <<"lease not found">>},
    {keep_state, FinalData} = rabbitmq_peer_discovery_etcd_v3_client:recover(info, Event, Data),
    ?assertEqual(undefined, FinalData#statem_data.lock_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.lock_lease_keepalive_pid),
    ?assertEqual(undefined, FinalData#statem_data.lock_owner_pid),
    ?assertEqual(undefined, FinalData#statem_data.lock_owner_monitor),
    receive
        {'EXIT', Owner, {etcd_lock_lease_lost, 42}} -> ok
    after 1000 ->
        ?assert(false)
    end,
    process_flag(trap_exit, false).

%% a lock owner dying concurrently with its lease being lost can already
%% have a 'DOWN' message queued when connected/3 clears lock_owner_monitor;
%% maybe_demonitor/1 must flush it, or a real gen_statem later crashes with
%% function_clause on that now-unmatched, stale 'DOWN'
lock_lease_lost_flushes_pending_owner_down_test(_Config) ->
    Owner = spawn(fun() -> ok end),
    OwnerMonitor = monitor(process, Owner),
    ct:sleep(10),

    Data = #statem_data{
        lock_lease_id = 42,
        lock_lease_keepalive_pid = self(),
        lock_owner_pid = Owner,
        lock_owner_monitor = OwnerMonitor
    },
    Event = #{event => 'KeepAliveHalted', lease_id => 42, reason => <<"lease not found">>},
    {keep_state, _FinalData} = rabbitmq_peer_discovery_etcd_v3_client:connected(info, Event, Data),
    receive
        {'DOWN', OwnerMonitor, process, Owner, _} ->
            ?assert(false)
    after 0 ->
        ok
    end.

%% terminate/3 must stop both tracked lease keep-alives, or the
%% independently supervised eetcd workers survive it and can renew a node
%% key lease or a lock lease that this client no longer tracks
terminate_stops_lease_keepalives_test(_Config) ->
    Data = #statem_data{
        node_lease_keepalive_pid = self(),
        lock_lease_keepalive_pid = self()
    },
    ok = rabbitmq_peer_discovery_etcd_v3_client:terminate(normal, connected, Data),
    receive
        {'$gen_cast', close} -> ok
    after 1000 ->
        ?assert(false)
    end,
    receive
        {'$gen_cast', close} -> ok
    after 1000 ->
        ?assert(false)
    end.

%% rabbitmq_peer_discovery_etcd_sup stops and re-attaches this client with
%% fresh statem_data on every node boot, so a successful register/1 call
%% must persist that registration was requested outside process state, or
%% the replacement process (simulated here by init/1) never knows to repair
%% a registration the old process's terminate/3 is about to let expire
register_persists_intent_across_client_restart_test(_Config) ->
    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, grant, fun(_Name, _TTL) -> {ok, #{'ID' => 999}} end),
    meck:expect(eetcd_lease, keep_alive, fun(_Name, _LeaseID) -> {ok, self()} end),
    meck:new(eetcd_kv, [passthrough]),
    meck:expect(eetcd_kv, put, fun(_Ctx, _Key, _Value) -> {ok, #{}} end),

    Data0 = #statem_data{
        connection_name = ?MODULE,
        cluster_name = "cluster-a",
        key_prefix = "prefffix",
        node_key_ttl_in_seconds = 60,
        node_key_registered = false
    },
    Tag = make_ref(),
    {keep_state, _FinalData} = rabbitmq_peer_discovery_etcd_v3_client:connected(
        {call, {self(), Tag}}, register, Data0),
    receive
        {Tag, Reply} -> ?assertEqual(ok, Reply)
    after 1000 ->
        ?assert(false)
    end,

    NewData = init_new_client_statem_data(),
    ?assert(NewData#statem_data.node_key_registered).

%% unregister/1 must clear the persisted intent too, or a client started
%% after this node unregisters wrongly believes registration is still wanted
unregister_clears_persisted_registration_intent_test(_Config) ->
    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, revoke, fun(_Ctx, _LeaseID) -> {ok, #{}} end),
    meck:new(eetcd_kv, [passthrough]),
    meck:expect(eetcd_kv, delete, fun(_Ctx, _Key) -> {ok, #{}} end),

    Data0 = #statem_data{connection_name = ?MODULE, node_key_registered = true},
    Tag = make_ref(),
    {keep_state, _} = rabbitmq_peer_discovery_etcd_v3_client:connected(
        {call, {self(), Tag}}, unregister, Data0),
    receive
        {Tag, ok} -> ok
    after 1000 ->
        ?assert(false)
    end,

    NewData = init_new_client_statem_data(),
    ?assertNot(NewData#statem_data.node_key_registered).

%% init/1 with no prior registration must not claim one is wanted
init_seeds_node_key_registered_from_persisted_intent_test(_Config) ->
    NewData = init_new_client_statem_data(),
    ?assertNot(NewData#statem_data.node_key_registered).

%%
%% Helpers
%%

init_new_client_statem_data() ->
    %% init/1 under test starts credentials_obfuscation and eetcd with
    %% application:ensure_started/1, which (unlike ensure_all_started/1)
    %% does not also start their own dependencies
    {ok, _} = application:ensure_all_started(credentials_obfuscation),
    {ok, _} = application:ensure_all_started(eetcd),
    {ok, recover, NewData, _Actions} =
        rabbitmq_peer_discovery_etcd_v3_client:init(#{endpoints => ["localhost:2379"]}),
    NewData.

%% stands in for the pid eetcd_lease:keep_alive/2 would return: stays
%% alive and tolerates the gen_server:cast(Pid, close) a discard sends it
keep_alive_stub() ->
    receive
        _ -> keep_alive_stub()
    end.

registration_value_of(LeaseID, TTL) ->
    to_binary(rabbit_json:encode(#{
        <<"node">> => to_binary(node()),
        <<"lease_id">> => LeaseID,
        <<"ttl">> => TTL
    })).
