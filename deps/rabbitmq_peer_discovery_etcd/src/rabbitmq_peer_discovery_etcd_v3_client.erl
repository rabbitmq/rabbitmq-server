%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
-export([extract_node/1, filter_node/1, registration_value/1, node_key_base/1, node_key/1, lock_key_base/1]).

-import(rabbit_data_coercion, [to_binary/1, to_list/1]).

-compile(nowarn_unused_function).

-include("rabbit_peer_discovery_etcd.hrl").

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

start(Conf) ->
    gen_statem:start({local, ?MODULE}, ?MODULE, Conf, []).

start_link(Conf) ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, Conf, []).

stop() ->
    gen_statem:stop(?MODULE).

init(Args) ->
    ok = application:ensure_started(eetcd),
    Settings = normalize_settings(Args),
    Endpoints = maps:get(endpoints, Settings),
    Username = maps:get(etcd_username, Settings, undefined),
    Password = maps:get(etcd_password, Settings, undefined),
    TLSOpts = maps:get(ssl_options, Settings, []),
    Actions = [{next_event, internal, start}],
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
        lock_ttl_in_seconds = erlang:max(
            ?MINIMUM_LOCK_WAIT_TTL,
            maps:get(lock_wait_time, Settings, ?DEFAULT_LOCK_WAIT_TTL)
        )
    }, Actions}.

callback_mode() -> [state_functions, state_enter].

terminate(Reason, State, Data) ->
    rabbit_log:debug("etcd v3 API client will terminate in state ~tp, reason: ~tp",
                     [State, Reason]),
    _ = disconnect(?ETCD_CONN_NAME, Data),
    rabbit_log:debug("etcd v3 API client has disconnected"),
    rabbit_log:debug("etcd v3 API client: total number of connections to etcd is ~tp", [length(eetcd_conn_sup:info())]),
    ok.

register() ->
    register(?MODULE).

register(ServerRef) ->
    gen_statem:call(ServerRef, register, ?CALL_TIMEOUT).

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
    gen_statem:call(ServerRef, {lock, Node}, ?CALL_TIMEOUT).

unlock() ->
    unlock(?MODULE, node()).

unlock(LockKey) ->
    unlock(?MODULE, LockKey).

unlock(ServerRef, LockKey) ->
    gen_statem:call(ServerRef, {unlock, LockKey}, ?CALL_TIMEOUT).

%%
%% States
%%

recover(enter, _PrevState, #statem_data{endpoints = Endpoints}) ->
    rabbit_log:debug("etcd v3 API client has entered recovery state, endpoints: ~ts",
                     [string:join(Endpoints, ",")]),
    keep_state_and_data;
recover(internal, start, Data = #statem_data{endpoints = Endpoints, connection_monitor = Ref}) ->
    rabbit_log:debug("etcd v3 API client will attempt to connect, endpoints: ~ts",
                     [string:join(Endpoints, ",")]),
    maybe_demonitor(Ref),
    {Transport, TransportOpts} = pick_transport(Data),
    case Transport of
        tcp -> rabbit_log:info("etcd v3 API client is configured to connect over plain TCP, without using TLS");
        tls -> rabbit_log:info("etcd v3 API client is configured to use TLS")
    end,
    ConnName = ?ETCD_CONN_NAME,
    case connect(ConnName, Endpoints, Transport, TransportOpts, Data) of
        {ok, Pid} ->
            rabbit_log:debug("etcd v3 API client connection: ~tp", [Pid]),
            rabbit_log:debug("etcd v3 API client: total number of connections to etcd is ~tp", [length(eetcd_conn_sup:info())]),
            {next_state, connected, Data#statem_data{
                connection_name = ConnName,
                connection_pid = Pid,
                connection_monitor = monitor(process, Pid)
            }};
        {error, Errors} ->
            [rabbit_log:error("etcd peer discovery: failed to connect to endpoint ~tp: ~tp", [Endpoint, Err]) || {Endpoint, Err} <- Errors],
            _ = ensure_disconnected(?ETCD_CONN_NAME, Data),
            Actions = [{state_timeout, reconnection_interval(), recover}],
            {keep_state, reset_statem_data(Data), Actions}
    end;
recover(state_timeout, _PrevState, Data) ->
    rabbit_log:debug("etcd peer discovery: connection entered a reconnection delay state"),
    _ = ensure_disconnected(?ETCD_CONN_NAME, Data),
    {next_state, recover, reset_statem_data(Data)};
recover({call, From}, Req, _Data) ->
    rabbit_log:error("etcd v3 API: client received a call ~tp while not connected, will do nothing", [Req]),
    gen_statem:reply(From, {error, not_connected}),
    keep_state_and_data.


connected(enter, _PrevState, Data) ->
    rabbit_log:info("etcd peer discovery: successfully connected to etcd"),

    {keep_state, acquire_node_key_lease_grant(Data)};
connected(info, {'DOWN', ConnRef, process, ConnPid, Reason}, Data = #statem_data{
                                                               connection_pid = ConnPid,
                                                               connection_monitor = ConnRef
                                                             }) ->
    rabbit_log:debug("etcd peer discovery: connection to etcd ~tp is down: ~tp", [ConnPid, Reason]),
    maybe_demonitor(ConnRef),
    {next_state, recover, reset_statem_data(Data)};
connected({call, From}, {lock, _Node}, Data = #statem_data{connection_name = Conn, lock_ttl_in_seconds = TTL}) ->
    case eetcd_lease:grant(eetcd_kv:new(Conn), TTL) of
        {ok, #{'ID' := LeaseID}} ->
            Key = lock_key_base(Data),
            rabbit_log:debug("etcd peer discovery: granted a lease ~tp for registration lock ~ts with TTL = ~tp", [LeaseID, Key, TTL]),
            case eetcd_lock:lock(lock_context(Conn, Data), Key, LeaseID) of
                {ok, #{key := GeneratedKey}} ->
                    rabbit_log:debug("etcd peer discovery: successfully acquired a lock, lock owner key: ~ts", [GeneratedKey]),
                    reply_and_retain_state(From, {ok, GeneratedKey});
                {error, _} = Error ->
                    rabbit_log:debug("etcd peer discovery: failed to acquire a lock using key ~ts: ~tp", [Key, Error]),
                    reply_and_retain_state(From, Error)
            end;
        {error, _} = Error ->
            rabbit_log:debug("etcd peer discovery: failed to get a lease for registration lock: ~tp", [Error]),
            reply_and_retain_state(From, Error)
    end;
connected({call, From}, {unlock, GeneratedKey}, Data = #statem_data{connection_name = Conn}) ->
    Ctx = unlock_context(Conn, Data),
    case eetcd_lock:unlock(Ctx, GeneratedKey) of
        {ok, _} ->
            rabbit_log:debug("etcd peer discovery: successfully released lock, lock owner key: ~ts", [GeneratedKey]),
            reply_and_retain_state(From, ok);
        {error, _} = Error ->
            rabbit_log:debug("etcd peer discovery: failed to release registration lock, lock owner key: ~ts, error ~tp",
                             [GeneratedKey, Error]),
            reply_and_retain_state(From, Error)
    end;
connected({call, From}, register, Data = #statem_data{connection_name = Conn}) ->
    Ctx = registration_context(Conn, Data),
    Key = node_key(Data),
    eetcd_kv:put(Ctx, Key, registration_value(Data)),
    rabbit_log:debug("etcd peer discovery: put key ~tp, done with registration", [Key]),
    gen_statem:reply(From, ok),
    keep_state_and_data;
connected({call, From}, unregister, Data = #statem_data{connection_name = Conn}) ->
    unregister(Conn, Data),
    gen_statem:reply(From, ok),
    {keep_state, Data#statem_data{
        node_key_lease_id = undefined
    }};
connected({call, From}, list_keys, Data = #statem_data{connection_name = Conn}) ->
    Prefix = node_key_base(Data),
    C1 = eetcd_kv:new(Conn),
    C2 = eetcd_kv:with_prefix(eetcd_kv:with_key(C1, Prefix)),
    rabbit_log:debug("etcd peer discovery: will use prefix ~ts to query for node keys", [Prefix]),
    {ok, #{kvs := Result}} = eetcd_kv:get(C2),
    rabbit_log:debug("etcd peer discovery returned keys: ~tp", [Result]),
    Values = [{maps:get(create_revision, M), maps:get(value, M)} || M <- Result],
    rabbit_log:debug("etcd peer discovery: listing node keys returned ~b results",
                     [length(Values)]),
    ParsedNodes = lists:filtermap(fun extract_node/1, Values),
    rabbit_log:info("etcd peer discovery: successfully extracted nodes: ~0tp",
                    [ParsedNodes]),
    gen_statem:reply(From, lists:usort(ParsedNodes)),
    keep_state_and_data.


disconnected(enter, _PrevState, _Data) ->
    rabbit_log:info("etcd peer discovery: successfully disconnected from etcd"),
    keep_state_and_data.


%%
%% Implementation
%%

acquire_node_key_lease_grant(Data = #statem_data{connection_name = Name, node_key_ttl_in_seconds = TTL}) ->
    %% acquire a lease for TTL
    {ok, #{'ID' := LeaseID}} = eetcd_lease:grant(Name, TTL),
    {ok, KeepalivePid} = eetcd_lease:keep_alive(Name, LeaseID),
    rabbit_log:debug("etcd peer discovery: acquired a lease ~tp for node key ~ts with TTL = ~tp", [LeaseID, node_key(Data), TTL]),
    Data#statem_data{
        node_key_lease_id = LeaseID,
        node_lease_keepalive_pid = KeepalivePid
    }.

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
            rabbit_log:error("etcd peer discovery: failed to extract node name from etcd value ~tp",
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

connect(Name, Endpoints, Transport, TransportOpts, Data) ->
    case eetcd_conn:lookup(Name) of
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, eetcd_conn_unavailable} ->
            do_connect(Name, Endpoints, Transport, TransportOpts, Data)
    end.

do_connect(Name, Endpoints, Transport, TransportOpts, Data = #statem_data{username = Username}) ->
    case Username of
        undefined -> rabbit_log:info("etcd peer discovery: will connect to etcd without authentication (no credentials configured)");
        _         -> rabbit_log:info("etcd peer discovery: will connect to etcd as user '~ts'", [Username])
    end,
    case eetcd:open(Name, Endpoints, connection_options(Data), Transport, TransportOpts) of
        {ok, Pid} -> {ok, Pid};
        {error, Errors0} ->
            Errors = case is_list(Errors0) of
                         true  -> Errors0;
                         false -> [Errors0]
                     end,
            rabbit_log:debug("etcd peer discovery: connection errors: ~tp",
                             [Errors]),
            rabbit_log:debug("etcd peer discovery: are all connection errors benign?: ~tp",
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

connection_options(#statem_data{username = Username, obfuscated_password = Password}) ->
    SharedOpts = [{mode, random}],
    case {Username, Password} of
        {undefined, _} -> SharedOpts;
        {_, undefined} -> SharedOpts;
        {UVal, PVal}   ->
            [{name, UVal}, {password, to_list(deobfuscate(PVal))}] ++ SharedOpts
    end.


obfuscate(undefined) -> undefined;
obfuscate(Password) ->
    credentials_obfuscation:encrypt(to_binary(Password)).

deobfuscate(undefined) -> undefined;
deobfuscate(Password) ->
    credentials_obfuscation:decrypt({encrypted, to_binary(Password)}).

disconnect(ConnName, #statem_data{connection_monitor = Ref}) ->
    maybe_demonitor(Ref),
    do_disconnect(ConnName).

unregister(Conn, Data = #statem_data{node_key_lease_id = LeaseID, node_lease_keepalive_pid = KAPid}) ->
    Ctx = unregistration_context(Conn, Data),
    Key = node_key(Data),
    eetcd_kv:delete(Ctx, Key),
    rabbit_log:debug("etcd peer discovery: deleted key ~ts, done with unregistration", [Key]),
    eetcd_lease:revoke(Ctx, LeaseID),
    exit(KAPid, normal),
    rabbit_log:debug("etcd peer discovery: revoked a lease ~tp for node key ~ts", [LeaseID, Key]),
    ok.

reply_and_retain_state(From, Value) ->
    gen_statem:reply(From, Value),
    keep_state_and_data.

maybe_demonitor(undefined) ->
    true;
maybe_demonitor(Ref) when is_reference(Ref) ->
    erlang:demonitor(Ref).

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

pick_transport(#statem_data{tls_options = []}) ->
    {tcp, []};
pick_transport(#statem_data{tls_options = Opts}) ->
    {tls, Opts}.
