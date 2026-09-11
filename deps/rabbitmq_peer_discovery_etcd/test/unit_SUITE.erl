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
                    register_replies_with_put_error_on_fresh_lease_test
                ]}
    ].

end_per_testcase(_TestCase, _Config) ->
    %% guards against a failed meck-based assertion above leaving a mock loaded
    catch meck:unload(),
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

%% connected(enter) must survive a failed eetcd_lease:grant/2 and leave
%% the lease fields unhealthy so the next reconnect retries
connected_enter_survives_lease_grant_failure_test(_Config) ->
    meck:new(eetcd_lease, [passthrough]),
    meck:expect(eetcd_lease, grant, fun(_Name, _TTL) -> {error, mocked_grant_failure} end),

    Data = #statem_data{
        connection_name = ?MODULE,
        cluster_name = "cluster-a",
        key_prefix = "prefffix",
        node_key_ttl_in_seconds = 60
    },
    ?assertEqual(
        keep_state_and_data,
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
    {keep_state, FinalData} = rabbitmq_peer_discovery_etcd_v3_client:connected(enter, recover, Data),
    ?assertEqual(undefined, FinalData#statem_data.node_key_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.node_lease_keepalive_pid),
    ?assertNot(rabbitmq_peer_discovery_etcd_v3_client:node_key_lease_is_healthy(FinalData)),
    exit(FreshKeepalivePid, kill).

%% when register/1 grants a fresh node key lease and the subsequent PUT
%% fails, it must reply with the PUT's own error instead of ok
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
    {keep_state, FinalData} = rabbitmq_peer_discovery_etcd_v3_client:connected({call, {self(), Tag}}, register, Data0),
    receive
        {Tag, Reply} -> ?assertEqual({error, mocked_put_failure}, Reply)
    after 1000 ->
        ?assert(false)
    end,
    ?assertNot(FinalData#statem_data.node_key_registered),
    %% the freshly granted lease must also be discarded, not just left unregistered
    ?assertEqual(undefined, FinalData#statem_data.node_key_lease_id),
    ?assertEqual(undefined, FinalData#statem_data.node_lease_keepalive_pid),
    ?assertNot(rabbitmq_peer_discovery_etcd_v3_client:node_key_lease_is_healthy(FinalData)),
    exit(FreshKeepalivePid, kill).

%%
%% Helpers
%%

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
