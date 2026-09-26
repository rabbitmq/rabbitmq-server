%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbitmq_peer_discovery_etcd_v3_client).

%% API
-export([]).


-behaviour(gen_statem).

-export([start_link/1, start/1, stop/0]).
-export([init/1, callback_mode/0, terminate/3]).
-export([register/1, register/0, unregister/1, unregister/0, list_nodes/0, list_nodes/1]).
-export([lock/0, lock/1, lock/2, unlock/0, unlock/1, unlock/2]).
-export([recover/3, connected/3, disconnected/3]).

%% for tests
-export([extract_node/1, filter_node/1, registration_value/1, node_key_base/1, node_key/1, lock_key_base/1, lock_call_timeout/1, cached_lock_call_timeout/0, clear_lease_keepalive/2, node_key_lease_is_healthy/1]).

-import(rabbit_data_coercion, [to_binary/1, to_list/1]).

-compile(nowarn_unused_function).

-include("rabbit_peer_discovery_etcd.hrl").
-include_lib("kernel/include/logger.hrl").

%%
%% API
%%

-define(ETCD_CONN_NAME, ?MODULE).
%% 60s by default matches the default heartbeat timeout.
%% We add 1s for state machine bookkeeping and...
-define(DEFAULT_NODE_KEY_LEASE_TTL, 61).
%% ...don't allow node lease key TTL to be lower than this
%% as overly low values can cause annoying timeouts in etcd client operations
-define(MINIMUM_NODE_KEY_LEASE_TTL, 15).
%% default randomized delay range was 5s to 60s, so this value
%% produces a comparable delay
-define(DEFAULT_LOCK_WAIT_TTL, 70).
%% don't allow lock lease TTL to be lower than this
%% as overly low values can cause annoying timeouts in etcd client operations
-define(MINIMUM_LOCK_WAIT_TTL, 30).

-define(CALL_TIMEOUT, 15000).
%% margin for granting a lease and starting its keep-alive before the
%% wait for the lock, or the node key put, begins.
-define(LOCK_CALL_TIMEOUT_BUFFER, 20000).
-define(REGISTER_CALL_TIMEOUT, ?CALL_TIMEOUT + ?LOCK_CALL_TIMEOUT_BUFFER).
%% lock_ttl_in_seconds never changes after init/1, so it is cached here
%% rather than read back via sys:get_state/2, which can itself time out
%% while the statem is busy inside a slow {lock, Node}/{unlock, _} handler.
-define(LOCK_TTL_PTERM_KEY, {?MODULE, lock_ttl_in_seconds}).
%% survives the stop-and-restart the plugin supervisor performs on every
%% boot, so a fresh process still knows to repair a registration an
%% earlier one had requested; see connected(enter, ...) below.
-define(NODE_KEY_REGISTERED_PTERM_KEY, {?MODULE, node_key_registered}).
-define(NODE_KEY_LEASE_RETRY_INTERVAL, 5000).

start(Conf) ->
    gen_statem:start({local, ?MODULE}, ?MODULE, Conf, []).

start_link(Conf) ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, Conf, []).

stop() ->
    gen_statem:stop(?MODULE).

init(Args) ->
    ok = application:ensure_started(credentials_obfuscation),
    ok = application:ensure_started(eetcd),
    Settings = normalize_settings(Args),
    Endpoints = maps:get(endpoints, Settings),
    Username = maps:get(etcd_username, Settings, undefined),
    Password = maps:get(etcd_password, Settings, undefined),
    TLSOpts = maps:get(ssl_options, Settings, []),
    Actions = [{next_event, internal, start}],
    LockTTL = erlang:max(
        ?MINIMUM_LOCK_WAIT_TTL,
        maps:get(lock_wait_time, Settings, ?DEFAULT_LOCK_WAIT_TTL)
    ),
    persistent_term:put(?LOCK_TTL_PTERM_KEY, LockTTL),
    {ok, recover, #statem_data{
        endpoints = Endpoints,
        tls_options = TLSOpts,
        username = Username,
        obfuscated_password = obfuscate(Password),
        key_prefix = maps:get(etcd_prefix, Settings, <<"rabbitmq">>),
        node_key_ttl_in_seconds = erlang:max(
            ?MINIMUM_NODE_KEY_LEASE_TTL,
            maps:get(etcd_node_ttl, Settings, ?DEFAULT_NODE_KEY_LEASE_TTL)
        ),
        cluster_name = maps:get(cluster_name, Settings, <<"default">>),
        lock_ttl_in_seconds = LockTTL,
        node_key_registered = persistent_term:get(?NODE_KEY_REGISTERED_PTERM_KEY, false)
    }, Actions}.

callback_mode() -> [state_functions, state_enter].

terminate(Reason, State, #statem_data{connection_monitor = Ref,
                                       node_lease_keepalive_pid = NodeKAPid,
                                       lock_lease_keepalive_pid = LockKAPid}) ->
    ?LOG_DEBUG("etcd v3 API client will terminate in state ~tp, reason: ~tp",
                     [State, Reason]),
    maybe_demonitor(Ref),
    %% these keep-alives are independently supervised eetcd workers that
    %% otherwise outlive this gen_statem, so they must be stopped here.
    %% Their leases are left to expire rather than revoked, so a
    %% registration this process requested stays visible for a
    %% replacement process to repair.
    maybe_stop_lease_keepalive(NodeKAPid),
    maybe_stop_lease_keepalive(LockKAPid),
    _ = do_disconnect(?ETCD_CONN_NAME),
    ?LOG_DEBUG("etcd v3 API client has disconnected"),
    ?LOG_DEBUG("etcd v3 API client: total number of connections to etcd is ~tp", [length(eetcd_conn_sup:info())]),
    ok.

register() ->
    register(?MODULE).

register(ServerRef) ->
    gen_statem:call(ServerRef, register, ?REGISTER_CALL_TIMEOUT).

unregister() ->
    ?MODULE:unregister(?MODULE).

unregister(ServerRef) ->
    gen_statem:call(ServerRef, unregister, ?CALL_TIMEOUT).

list_nodes() ->
    list_nodes(?MODULE).

list_nodes(ServerRef) ->
    gen_statem:call(ServerRef, list_keys, ?CALL_TIMEOUT).

lock() ->
    lock(?MODULE, node()).

lock(Node) ->
    lock(?MODULE, Node).

lock(ServerRef, Node) ->
    %% the lock request can block for as long as lock_ttl_in_seconds,
    %% which can exceed ?CALL_TIMEOUT, so the call timeout scales with it.
    gen_statem:call(ServerRef, {lock, Node}, cached_lock_call_timeout()).

unlock() ->
    unlock(?MODULE, node()).

unlock(LockKey) ->
    unlock(?MODULE, LockKey).

unlock(ServerRef, LockKey) ->
    gen_statem:call(ServerRef, {unlock, LockKey}, cached_lock_call_timeout()).

cached_lock_call_timeout() ->
    lock_call_timeout(persistent_term:get(?LOCK_TTL_PTERM_KEY, undefined)).

lock_call_timeout(TTL) when is_integer(TTL) ->
    (TTL * 1000) + ?LOCK_CALL_TIMEOUT_BUFFER;
lock_call_timeout(_) ->
    ?CALL_TIMEOUT.

%%
%% States
%%

recover(enter, _PrevState, #statem_data{endpoints = Endpoints}) ->
    ?LOG_DEBUG("etcd v3 API client has entered recovery state, endpoints: ~ts",
                     [string:join(Endpoints, ",")]),
    keep_state_and_data;
recover(internal, start, Data = #statem_data{endpoints = Endpoints, connection_monitor = Ref}) ->
    ?LOG_DEBUG("etcd v3 API client will attempt to connect, endpoints: ~ts",
                     [string:join(Endpoints, ",")]),
    maybe_demonitor(Ref),
    case connect(?ETCD_CONN_NAME, Endpoints, Data) of
        {ok, Pid} ->
            ?LOG_DEBUG("etcd v3 API client connection: ~tp", [Pid]),
            ?LOG_DEBUG("etcd v3 API client: total number of connections to etcd is ~tp", [length(eetcd_conn_sup:info())]),
            {next_state, connected, Data#statem_data{
                connection_name = ?ETCD_CONN_NAME,
                connection_pid = Pid,
                connection_monitor = monitor(process, Pid)
            }};
        {error, Errors} ->
            [?LOG_ERROR("etcd peer discovery: failed to connect to endpoint ~tp: ~tp", [Endpoint, Err]) || {Endpoint, Err} <- Errors],
            _ = ensure_disconnected(?ETCD_CONN_NAME, Data),
            Actions = [{state_timeout, reconnection_interval(), recover}],
            {keep_state, reset_statem_data(Data), Actions}
    end;
recover(state_timeout, _PrevState, Data) ->
    ?LOG_DEBUG("etcd peer discovery: connection entered a reconnection delay state"),
    _ = ensure_disconnected(?ETCD_CONN_NAME, Data),
    {next_state, recover, reset_statem_data(Data)};
%% a lock lease keep-alive halt can race with the disconnect that lands
%% us here; the owner must still be aborted, as in the matching clause
%% in connected/3.
recover(info, #{event := 'KeepAliveHalted', lease_id := LeaseID, reason := Reason},
        Data = #statem_data{lock_lease_id = LeaseID}) ->
    log_keepalive_halted(LeaseID, Reason, Data),
    #statem_data{lock_owner_pid = OwnerPid, lock_owner_monitor = OwnerMonitor} = Data,
    case OwnerPid of
        undefined ->
            ok;
        _ ->
            ?LOG_ERROR("etcd peer discovery: lost the registration lock's lease "
                       "(held by ~tp) while disconnected, aborting the owner "
                       "since mutual exclusion can no longer be guaranteed", [OwnerPid]),
            maybe_demonitor(OwnerMonitor),
            exit(OwnerPid, {etcd_lock_lease_lost, LeaseID})
    end,
    NewData = clear_lease_keepalive(LeaseID, Data),
    {keep_state, NewData#statem_data{lock_owner_pid = undefined, lock_owner_monitor = undefined}};
%% same race for the node key lease; see the matching clause in connected/3.
recover(info, #{event := 'KeepAliveHalted', lease_id := LeaseID, reason := Reason}, Data) ->
    log_keepalive_halted(LeaseID, Reason, Data),
    {keep_state, clear_lease_keepalive(LeaseID, Data)};
%% rabbit_peer_discovery calls unlock/2 exactly once with no retry, so if
%% the connection drops before it arrives, the lock lease's keep-alive
%% (which survives reconnects on its own) must be stopped here or it
%% renews the lock forever.
recover({call, From}, {unlock, _GeneratedKey}, Data = #statem_data{lock_lease_keepalive_pid = KAPid,
                                                                    lock_owner_monitor = OwnerMonitor}) ->
    ?LOG_ERROR("etcd v3 API: client received an unlock call while not connected, will stop the lock lease keep-alive so its lease can expire"),
    maybe_stop_lease_keepalive(KAPid),
    maybe_demonitor(OwnerMonitor),
    gen_statem:reply(From, {error, not_connected}),
    {keep_state, Data#statem_data{
        lock_lease_id = undefined,
        lock_lease_keepalive_pid = undefined,
        lock_owner_pid = undefined,
        lock_owner_monitor = undefined
    }};
%% the lock owner can die while disconnected too; the keep-alive survives
%% on its own and must be stopped here or it renews a lock nobody owns.
recover(info, {'DOWN', MonRef, process, _Pid, _Reason}, Data = #statem_data{
                                                            lock_owner_monitor = MonRef,
                                                            lock_lease_keepalive_pid = KAPid
                                                          }) ->
    ?LOG_WARNING("etcd v3 API: lock owner process died while disconnected, clearing lock ownership state"),
    maybe_stop_lease_keepalive(KAPid),
    {keep_state, Data#statem_data{
        lock_lease_id = undefined,
        lock_lease_keepalive_pid = undefined,
        lock_owner_pid = undefined,
        lock_owner_monitor = undefined
    }};
recover({call, From}, Req, _Data) ->
    ?LOG_ERROR("etcd v3 API: client received a call ~tp while not connected, will do nothing", [Req]),
    gen_statem:reply(From, {error, not_connected}),
    keep_state_and_data.


%% eetcd_lease renews the node-key lease across a reconnect on its own, so
%% a fresh lease is only granted here when node_key_lease_is_healthy/1
%% says the old one is gone. register/0,1 runs once, at boot, so a freshly
%% granted lease needs the node key re-PUT here, or a previously
%% registered node silently disappears from list_nodes/0,1.
connected(enter, _PrevState, Data) ->
    case node_key_lease_is_healthy(Data) of
        true ->
            ?LOG_INFO("etcd peer discovery: successfully connected to etcd, node key lease is still held"),
            keep_state_and_data;
        false ->
            ?LOG_INFO("etcd peer discovery: successfully connected to etcd"),
            StaleLeaseID = Data#statem_data.node_key_lease_id,
            StaleKeepalivePid = Data#statem_data.node_lease_keepalive_pid,
            case acquire_node_key_lease_grant(Data) of
                {ok, NewData} ->
                    case Data#statem_data.node_key_registered of
                        true ->
                            case put_node_key(NewData) of
                                ok ->
                                    revoke_stale_node_key_lease(StaleLeaseID, StaleKeepalivePid, NewData),
                                    {keep_state, NewData};
                                {error, _} ->
                                    %% register/1 won't run again on its own to repair
                                    %% this, so schedule a retry
                                    FinalData = revoke_fresh_node_key_lease(NewData),
                                    {keep_state, FinalData, [node_key_lease_retry_action()]}
                            end;
                        false ->
                            {keep_state, NewData}
                    end;
                {error, _} ->
                    {keep_state_and_data, [node_key_lease_retry_action()]}
            end
    end;
%% repeat_state re-executes connected(enter, ...) above, retrying the grant/put.
connected(state_timeout, retry_node_key_lease, Data) ->
    {repeat_state, Data};
connected(info, {'DOWN', ConnRef, process, ConnPid, Reason}, Data = #statem_data{
                                                               connection_pid = ConnPid,
                                                               connection_monitor = ConnRef
                                                             }) ->
    ?LOG_DEBUG("etcd peer discovery: connection to etcd ~tp is down: ~tp", [ConnPid, Reason]),
    maybe_demonitor(ConnRef),
    {next_state, recover, reset_statem_data(Data)};
%% the lock owner did not call unlock/1 before dying, so the lock must be
%% released here or its keep-alive renews it indefinitely.
connected(info, {'DOWN', MonRef, process, _Pid, _Reason}, Data = #statem_data{
                                                              lock_owner_monitor = MonRef,
                                                              connection_name = Conn,
                                                              lock_lease_id = LockLeaseID,
                                                              lock_lease_keepalive_pid = KAPid
                                                            }) ->
    ?LOG_WARNING("etcd peer discovery: lock owner process died without releasing the lock, will release it now"),
    maybe_revoke_lease(LockLeaseID, eetcd_kv:new(Conn)),
    maybe_stop_lease_keepalive(KAPid),
    {keep_state, Data#statem_data{
        lock_lease_id = undefined,
        lock_lease_keepalive_pid = undefined,
        lock_owner_pid = undefined,
        lock_owner_monitor = undefined
    }};
%% eetcd_lease's keep-alive process sends this info message, unsolicited,
%% whenever it stops renewing a lease; without a matching clause it
%% crashes the gen_statem with a function_clause error. A halt for a
%% registered node key must schedule a retry, or the node stays absent
%% from discovery until an unrelated reconnect repairs it via
%% connected(enter, ...) above.
connected(info, #{event := 'KeepAliveHalted', lease_id := LeaseID, reason := Reason},
          Data = #statem_data{node_key_lease_id = LeaseID}) ->
    log_keepalive_halted(LeaseID, Reason, Data),
    NewData = clear_lease_keepalive(LeaseID, Data),
    case Data#statem_data.node_key_registered of
        true ->
            {keep_state, NewData, [node_key_lease_retry_action()]};
        false ->
            {keep_state, NewData}
    end;
%% a halt for the lock lease means mutual exclusion is gone, so the
%% owner is aborted rather than merely notified.
connected(info, #{event := 'KeepAliveHalted', lease_id := LeaseID, reason := Reason},
          Data = #statem_data{lock_lease_id = LeaseID}) ->
    log_keepalive_halted(LeaseID, Reason, Data),
    #statem_data{lock_owner_pid = OwnerPid, lock_owner_monitor = OwnerMonitor} = Data,
    case OwnerPid of
        undefined ->
            ok;
        _ ->
            ?LOG_ERROR("etcd peer discovery: lost the registration lock's lease "
                       "(held by ~tp) while it was still held, aborting the owner "
                       "since mutual exclusion can no longer be guaranteed", [OwnerPid]),
            maybe_demonitor(OwnerMonitor),
            exit(OwnerPid, {etcd_lock_lease_lost, LeaseID})
    end,
    NewData = clear_lease_keepalive(LeaseID, Data),
    {keep_state, NewData#statem_data{lock_owner_pid = undefined, lock_owner_monitor = undefined}};
connected(info, #{event := 'KeepAliveHalted', lease_id := LeaseID, reason := Reason}, Data) ->
    log_keepalive_halted(LeaseID, Reason, Data),
    {keep_state, clear_lease_keepalive(LeaseID, Data)};
connected({call, From}, {lock, _Node}, Data = #statem_data{connection_name = Conn, lock_ttl_in_seconds = TTL}) ->
    case eetcd_lease:grant(eetcd_kv:new(Conn), TTL) of
        {ok, #{'ID' := LeaseID}} ->
            Key = lock_key_base(Data),
            ?LOG_DEBUG("etcd peer discovery: granted a lease ~tp for registration lock ~ts with TTL = ~tp", [LeaseID, Key, TTL]),
            %% eetcd_lock does no session management of its own, so the keep-alive
            %% must start now, before the wait, or the lease can expire while
            %% eetcd_lock:lock/3 is still blocked on a contested lock.
            case eetcd_lease:keep_alive(Conn, LeaseID) of
                {ok, KeepalivePid} ->
                    case eetcd_lock:lock(lock_context(Conn, Data), Key, LeaseID) of
                        {ok, #{key := GeneratedKey}} ->
                            ?LOG_DEBUG("etcd peer discovery: successfully acquired a lock, lock owner key: ~ts", [GeneratedKey]),
                            %% monitored so the lock is released if the owner dies
                            %% before calling unlock/1
                            {OwnerPid, _Tag} = From,
                            OwnerMonitor = monitor(process, OwnerPid),
                            gen_statem:reply(From, {ok, GeneratedKey}),
                            {keep_state, Data#statem_data{
                                lock_lease_id = LeaseID,
                                lock_lease_keepalive_pid = KeepalivePid,
                                lock_owner_pid = OwnerPid,
                                lock_owner_monitor = OwnerMonitor
                            }};
                        {error, _} = Error ->
                            ?LOG_DEBUG("etcd peer discovery: failed to acquire a lock using key ~ts: ~tp", [Key, Error]),
                            maybe_stop_lease_keepalive(KeepalivePid),
                            maybe_revoke_lease(LeaseID, eetcd_kv:new(Conn)),
                            reply_and_retain_state(From, Error)
                    end;
                {error, _} = Error ->
                    ?LOG_DEBUG("etcd peer discovery: failed to start a lease keep-alive for registration lock ~ts: ~tp", [Key, Error]),
                    maybe_revoke_lease(LeaseID, eetcd_kv:new(Conn)),
                    reply_and_retain_state(From, Error)
            end;
        {error, _} = Error ->
            ?LOG_DEBUG("etcd peer discovery: failed to get a lease for registration lock: ~tp", [Error]),
            reply_and_retain_state(From, Error)
    end;
connected({call, From}, {unlock, GeneratedKey}, Data = #statem_data{connection_name = Conn,
                                                                     lock_lease_id = LockLeaseID,
                                                                     lock_lease_keepalive_pid = KAPid,
                                                                     lock_owner_monitor = OwnerMonitor}) ->
    Ctx = unlock_context(Conn, Data),
    case eetcd_lock:unlock(Ctx, GeneratedKey) of
        {ok, _} ->
            ?LOG_DEBUG("etcd peer discovery: successfully released lock, lock owner key: ~ts", [GeneratedKey]),
            maybe_revoke_lease(LockLeaseID, eetcd_kv:new(Conn)),
            maybe_stop_lease_keepalive(KAPid),
            maybe_demonitor(OwnerMonitor),
            gen_statem:reply(From, ok),
            {keep_state, Data#statem_data{
                lock_lease_id = undefined,
                lock_lease_keepalive_pid = undefined,
                lock_owner_pid = undefined,
                lock_owner_monitor = undefined
            }};
        {error, _} = Error ->
            ?LOG_DEBUG("etcd peer discovery: failed to release registration lock, lock owner key: ~ts, error ~tp",
                             [GeneratedKey, Error]),
            %% the lock key is attached to this lease, so revoking it releases
            %% the lock when the unlock RPC itself failed
            maybe_revoke_lease(LockLeaseID, eetcd_kv:new(Conn)),
            maybe_stop_lease_keepalive(KAPid),
            maybe_demonitor(OwnerMonitor),
            gen_statem:reply(From, Error),
            {keep_state, Data#statem_data{
                lock_lease_id = undefined,
                lock_lease_keepalive_pid = undefined,
                lock_owner_pid = undefined,
                lock_owner_monitor = undefined
            }}
    end;
connected({call, From}, register, Data0) ->
    %% node_key_registered is set as soon as registration is requested,
    %% not only once the put succeeds, so a transient grant or put
    %% failure is retried the same way a later keep-alive halt would be;
    %% see ?NODE_KEY_REGISTERED_PTERM_KEY above.
    persistent_term:put(?NODE_KEY_REGISTERED_PTERM_KEY, true),
    StaleLeaseID = Data0#statem_data.node_key_lease_id,
    StaleKeepalivePid = Data0#statem_data.node_lease_keepalive_pid,
    {Data1, Regranted} = case node_key_lease_is_healthy(Data0) of
               true -> {Data0#statem_data{node_key_registered = true}, false};
               false ->
                   case acquire_node_key_lease_grant(Data0) of
                       {ok, Data0Regranted} -> {Data0Regranted#statem_data{node_key_registered = true}, true};
                       {error, _} -> {Data0#statem_data{node_key_registered = true}, true}
                   end
           end,
    case node_key_lease_is_healthy(Data1) of
        true ->
            case put_node_key(Data1) of
                ok ->
                    Data = Data1,
                    case Regranted of
                        true ->
                            revoke_stale_node_key_lease(StaleLeaseID, StaleKeepalivePid, Data);
                        false ->
                            ok
                    end,
                    gen_statem:reply(From, ok),
                    {keep_state, Data};
                {error, _} = Error ->
                    %% only discard the lease if it was freshly granted; a
                    %% still-healthy pre-existing one did not cause this failure
                    FinalData = case Regranted of
                        true -> revoke_fresh_node_key_lease(Data1);
                        false -> Data1
                    end,
                    gen_statem:reply(From, Error),
                    {keep_state, FinalData, [node_key_lease_retry_action()]}
            end;
        false ->
            ?LOG_ERROR("etcd peer discovery: could not acquire a lease for node key ~tp, registration skipped", [node_key(Data1)]),
            gen_statem:reply(From, {error, no_node_key_lease}),
            {keep_state, Data1, [node_key_lease_retry_action()]}
    end;
connected({call, From}, unregister, Data = #statem_data{connection_name = Conn}) ->
    unregister(Conn, Data),
    persistent_term:put(?NODE_KEY_REGISTERED_PTERM_KEY, false),
    gen_statem:reply(From, ok),
    {keep_state, Data#statem_data{
        node_key_lease_id = undefined,
        node_lease_keepalive_pid = undefined,
        node_key_registered = false
    }};
connected({call, From}, list_keys, Data = #statem_data{connection_name = Conn}) ->
    Prefix = node_key_base(Data),
    C1 = eetcd_kv:new(Conn),
    C2 = eetcd_kv:with_prefix(eetcd_kv:with_key(C1, Prefix)),
    ?LOG_DEBUG("etcd peer discovery: will use prefix ~ts to query for node keys", [Prefix]),
    {ok, #{kvs := Result}} = eetcd_kv:get(C2),
    ?LOG_DEBUG("etcd peer discovery returned keys: ~tp", [Result]),
    Values = [{maps:get(create_revision, M), maps:get(value, M)} || M <- Result],
    ?LOG_DEBUG("etcd peer discovery: listing node keys returned ~b results",
                     [length(Values)]),
    ParsedNodes = lists:filtermap(fun extract_node/1, Values),
    ?LOG_INFO("etcd peer discovery: successfully extracted nodes: ~0tp",
                    [ParsedNodes]),
    gen_statem:reply(From, lists:usort(ParsedNodes)),
    keep_state_and_data.


disconnected(enter, _PrevState, _Data) ->
    ?LOG_INFO("etcd peer discovery: successfully disconnected from etcd"),
    keep_state_and_data.


%%
%% Implementation
%%

acquire_node_key_lease_grant(Data = #statem_data{connection_name = Name,
                                                  node_key_ttl_in_seconds = TTL}) ->
    case eetcd_lease:grant(Name, TTL) of
        {ok, #{'ID' := LeaseID}} ->
            case eetcd_lease:keep_alive(Name, LeaseID) of
                {ok, KeepalivePid} ->
                    ?LOG_DEBUG("etcd peer discovery: acquired a lease ~tp for node key ~ts with TTL = ~tp", [LeaseID, node_key(Data), TTL]),
                    {ok, Data#statem_data{
                        node_key_lease_id = LeaseID,
                        node_lease_keepalive_pid = KeepalivePid
                    }};
                {error, _} = Error ->
                    ?LOG_ERROR("etcd peer discovery: failed to start a lease keep-alive for node key ~ts: ~tp", [node_key(Data), Error]),
                    maybe_revoke_lease(LeaseID, eetcd_kv:new(Name)),
                    Error
            end;
        {error, _} = Error ->
            ?LOG_ERROR("etcd peer discovery: failed to acquire a lease for node key ~ts: ~tp", [node_key(Data), Error]),
            Error
    end.

%% Shared by the register call handler and connected(enter)'s recovery
%% path so both re-establish the node key under a lease the same way.
put_node_key(Data = #statem_data{connection_name = Conn}) ->
    Key = node_key(Data),
    Ctx = registration_context(Conn, Data),
    case eetcd_kv:put(Ctx, Key, registration_value(Data)) of
        {ok, _} ->
            ?LOG_DEBUG("etcd peer discovery: put key ~tp, done with registration", [Key]),
            ok;
        {error, Reason} = Error ->
            ?LOG_ERROR("etcd peer discovery: put key ~tp failed: ~p", [Key, Reason]),
            Error
    end.

%% only called after the replacement's key PUT has succeeded; revoking
%% before the PUT would leave the node briefly absent from discovery.
revoke_stale_node_key_lease(undefined, _StaleKeepalivePid, _Data) ->
    ok;
revoke_stale_node_key_lease(StaleLeaseID, StaleKeepalivePid, #statem_data{connection_name = Conn}) ->
    maybe_stop_lease_keepalive(StaleKeepalivePid),
    maybe_revoke_lease(StaleLeaseID, eetcd_kv:new(Conn)).

%% discards a lease whose node key PUT failed; unlike
%% revoke_stale_node_key_lease/3, it never had a key under it, so
%% revoking right away opens no visibility window.
revoke_fresh_node_key_lease(Data = #statem_data{connection_name = Conn,
                                                 node_key_lease_id = LeaseID,
                                                 node_lease_keepalive_pid = KeepalivePid}) ->
    maybe_stop_lease_keepalive(KeepalivePid),
    maybe_revoke_lease(LeaseID, eetcd_kv:new(Conn)),
    Data#statem_data{
        node_key_lease_id = undefined,
        node_lease_keepalive_pid = undefined
    }.

node_key_lease_retry_action() ->
    {state_timeout, ?NODE_KEY_LEASE_RETRY_INTERVAL, retry_node_key_lease}.

%% node_key_lease_id alone is not reliable evidence that the lease is
%% still being renewed: the keep-alive worker can die silently without
%% sending KeepAliveHalted, so is_process_alive/1 must also be checked.
node_key_lease_is_healthy(#statem_data{node_key_lease_id = undefined}) ->
    false;
node_key_lease_is_healthy(#statem_data{node_lease_keepalive_pid = Pid}) when is_pid(Pid) ->
    is_process_alive(Pid);
node_key_lease_is_healthy(_Data) ->
    false.

registration_context(ConnName, #statem_data{node_key_lease_id = LeaseID}) ->
    Ctx1 = eetcd_kv:new(ConnName),
    eetcd_kv:with_lease(Ctx1, LeaseID).

unregistration_context(ConnName, _Data) ->
    eetcd_kv:new(ConnName).

lock_context(ConnName, #statem_data{lock_ttl_in_seconds = LeaseTTL}) ->
    %% LeaseTT is in seconds, eetcd_lock:with_timeout/2 expects milliseconds
    eetcd_lock:with_timeout(eetcd_lock:new(ConnName), LeaseTTL * 1000).

unlock_context(ConnName, #statem_data{lock_ttl_in_seconds = Timeout}) ->
    %% caps the timeout here using the lock TTL value, it makes more
    %% sense than picking an arbitrary number. MK.
    eetcd_lock:with_timeout(eetcd_lock:new(ConnName), Timeout * 1000).

node_key_base(#statem_data{cluster_name = ClusterName, key_prefix = Prefix}) ->
    to_binary(rabbit_misc:format("/rabbitmq/discovery/~ts/clusters/~ts/nodes", [Prefix, ClusterName])).

node_key(Data) ->
    to_binary(rabbit_misc:format("~ts/~ts", [node_key_base(Data), node()])).

lock_key_base(#statem_data{key_prefix = Prefix, cluster_name = ClusterName}) ->
    Key = rabbit_misc:format("/rabbitmq/locks/~ts/clusters/~ts/registration",
                             [Prefix, ClusterName]),
    to_binary(Key).

%% This value is not used and merely
%% provides additional context to the operator.
registration_value(#statem_data{node_key_lease_id = LeaseID, node_key_ttl_in_seconds = TTL}) ->
    to_binary(rabbit_json:encode(#{
        <<"node">>     => to_binary(node()),
        <<"lease_id">> => LeaseID,
        <<"ttl">>      => TTL
    })).

extract_node({CreatedRev, Payload}) ->
    case rabbit_json:try_decode(Payload) of
        {error, _Error} ->
            ?LOG_ERROR("etcd peer discovery: failed to extract node name from etcd value ~tp",
                             [Payload]),
            false;
        {ok, Map} ->
            case maps:get(<<"node">>, Map, undefined) of
                undefined ->
                    false;
                Node ->
                    {true, {CreatedRev, rabbit_data_coercion:to_atom(Node)}}
            end
    end.

filter_node(undefined)  -> false;
filter_node({error, _}) -> false;
filter_node(_Other)     -> true.


error_is_already_started({_Endpoint, already_started}) ->
    true;
error_is_already_started({_Endpoint, _}) ->
    false.

connect(Name, Endpoints, Data) ->
    case eetcd_conn:lookup(Name) of
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, eetcd_conn_unavailable} ->
            do_connect(Name, Endpoints, Data)
    end.

do_connect(Name, Endpoints, Data = #statem_data{username = Username}) ->
    Opts = connection_options(Data),
    case Username of
        undefined -> ?LOG_INFO("etcd peer discovery: will connect to etcd without authentication (no credentials configured)");
        _         -> ?LOG_INFO("etcd peer discovery: will connect to etcd as user '~ts'", [Username])
    end,
    case eetcd:open(Name, Endpoints, Opts) of
        {ok, Pid} -> {ok, Pid};
        {error, Errors0} ->
            Errors = case is_list(Errors0) of
                         true  -> Errors0;
                         false -> [Errors0]
                     end,
            ?LOG_DEBUG("etcd peer discovery: connection errors: ~tp",
                             [Errors]),
            ?LOG_DEBUG("etcd peer discovery: are all connection errors benign?: ~tp",
                             [lists:all(fun error_is_already_started/1, Errors)]),
            %% If all errors are already_started we can ignore them.
            %% eetcd registers connections under a name
            case lists:all(fun error_is_already_started/1, Errors) of
                true ->
                    eetcd_conn:lookup(Name);
                false ->
                    {error, Errors}
            end
    end.

obfuscate(undefined) -> undefined;
obfuscate(Password) ->
    credentials_obfuscation:encrypt(to_binary(Password)).

deobfuscate(undefined) -> undefined;
deobfuscate(Password) ->
    credentials_obfuscation:decrypt(Password).

unregister(Conn, Data = #statem_data{node_key_lease_id = LeaseID, node_lease_keepalive_pid = KAPid}) ->
    Ctx = unregistration_context(Conn, Data),
    Key = node_key(Data),
    _ = eetcd_kv:delete(Ctx, Key),
    ?LOG_DEBUG("etcd peer discovery: deleted key ~ts, done with unregistration", [Key]),
    maybe_revoke_lease(LeaseID, Ctx),
    maybe_stop_lease_keepalive(KAPid),
    ?LOG_DEBUG("etcd peer discovery: revoked a lease ~tp for node key ~ts", [LeaseID, Key]),
    ok.

reply_and_retain_state(From, Value) ->
    gen_statem:reply(From, Value),
    keep_state_and_data.

%% A halt for a lease already cleared by us (a normal unlock/unregister)
%% is expected; a halt for a lease still recorded as held means this
%% node just lost a lock or its registration without knowing it.
log_keepalive_halted(LeaseID, Reason, Data) ->
    case clear_lease_keepalive(LeaseID, Data) of
        Data ->
            ?LOG_DEBUG("etcd peer discovery: lease ~tp keep-alive halted: ~tp", [LeaseID, Reason]);
        _ ->
            ?LOG_WARNING("etcd peer discovery: lease ~tp keep-alive halted unexpectedly: ~tp", [LeaseID, Reason])
    end.

%% Clears whichever lease/keep-alive pid fields match the halted lease.
%% LeaseID may legitimately match neither field, e.g. when the halt
%% event for a lease already cleared by us arrives after the fact.
clear_lease_keepalive(LeaseID, Data = #statem_data{node_key_lease_id = LeaseID}) ->
    Data#statem_data{node_key_lease_id = undefined, node_lease_keepalive_pid = undefined};
clear_lease_keepalive(LeaseID, Data = #statem_data{lock_lease_id = LeaseID}) ->
    Data#statem_data{lock_lease_id = undefined, lock_lease_keepalive_pid = undefined};
clear_lease_keepalive(_LeaseID, Data) ->
    Data.

%% eetcd_lease:revoke/2 crashes the gen_statem with badarg on an
%% undefined lease id, which can happen once clear_lease_keepalive/2 has
%% already cleared it in response to an earlier KeepAliveHalted event.
maybe_revoke_lease(undefined, _Ctx) ->
    ok;
maybe_revoke_lease(LeaseID, Ctx) ->
    _ = eetcd_lease:revoke(Ctx, LeaseID),
    ok.

maybe_stop_lease_keepalive(undefined) ->
    ok;
maybe_stop_lease_keepalive(KeepalivePid) ->
    %% the keep-alive gen_server does not trap exits, so exit(Pid, normal)
    %% would be a no-op; use its documented `close' message instead
    %% (see eetcd_lease:close/0).
    gen_server:cast(KeepalivePid, close).

maybe_demonitor(undefined) ->
    true;
maybe_demonitor(Ref) when is_reference(Ref) ->
    %% flush drops a 'DOWN' already queued for Ref, which would otherwise
    %% match no clause once the corresponding fields are cleared
    erlang:demonitor(Ref, [flush]).

reset_statem_data(Data0 = #statem_data{endpoints = Es, connection_monitor = Ref}) when Es =/= undefined ->
    maybe_demonitor(Ref),
    Data0#statem_data{
        connection_pid = undefined,
        connection_monitor = undefined
    }.

ensure_disconnected(Name, #statem_data{connection_monitor = Ref}) ->
    maybe_demonitor(Ref),
    do_disconnect(Name).

do_disconnect(Name) ->
    try
        eetcd:close(Name)
    catch _:_ ->
        ok
    end.

reconnection_interval() ->
    3000.

normalize_settings(Map) when is_map(Map) ->
    Endpoints = maps:get(endpoints, Map, []),
    LegacyEndpoints = case maps:get(etcd_host, Map, undefined) of
        undefined -> [];
        Hostname ->
            Port = maps:get(etcd_port, Map, 2379),
            [rabbit_misc:format("~ts:~tp", [Hostname, Port])]
    end,

    AllEndpoints = Endpoints ++ LegacyEndpoints,
    maps:merge(maps:without([etcd_prefix, lock_wait_time], Map),
               #{endpoints => AllEndpoints}).

connection_options(#statem_data{tls_options = TlsOpts,
                                username = Username,
                                obfuscated_password = Password}) ->
    Opts0 = case TlsOpts of
                [] ->
                    ?LOG_INFO("etcd v3 API client is configured to use plain TCP (without TLS)"),
                    [{transport, tcp}];
                _ ->
                    ?LOG_INFO("etcd v3 API client is configured to use TLS"),
                    [{transport, tls},
                     {tls_opts, TlsOpts}]
            end,
    Opts = [{mode, random} | Opts0],
    case Username =:= undefined orelse
         Password =:= undefined of
        true ->
            Opts;
        false ->
            [{name, Username},
             {password, to_list(deobfuscate(Password))}] ++ Opts
    end.
