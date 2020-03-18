%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% Copyright (c) 2020 VMware, Inc. or its affiliates. All rights reserved.
%%
-module(rabbitmq_peer_discovery_etcd_v3_client).

%% API
-export([]).


-behaviour(gen_statem).

-export([start_link/1]).
-export([init/1, callback_mode/0, terminate/3]).
-export([recover/3, connected/3]).

-import(rabbit_data_coercion, [to_binary/1]).

-compile(nowarn_unused_function).

-record(statem_data, {
    endpoints,
    connection_name,
    connection_pid,
    connection_monitor,
    key_prefix,
    cluster_name,
    lock_timeout,
    node_key_lease_id,
    node_key_ttl
}).

%%
%% API
%%

-define(ETCD_CONN_NAME, ?MODULE).
%% 60s by default matches the default heartbeat timeout.
%% We add 1s for state machine bookkeeping and
-define(DEFAULT_NODE_KEY_LEASE_TTL, 61000).

start_link(Conf) ->
  gen_statem:start_link({local, ?MODULE}, ?MODULE, Conf, []).

init(Args) ->
    ok = application:ensure_started(eetcd),
    Settings = normalize_settings(Args),
    Endpoints = maps:get(endpoints, Settings),
    Actions = [{next_event, internal, start}],
    {ok, recover, #statem_data{
        endpoints = Endpoints,
        key_prefix = maps:get(etcd_prefix, Settings, undefined),

        node_key_ttl = maps:get(etcd_node_ttl, Settings, ?DEFAULT_NODE_KEY_LEASE_TTL)
    }, Actions}.

callback_mode() -> [state_functions, state_enter].

terminate(Reason, State, _Data) ->
    rabbit_log:debug("etcd v3 API client will terminate in state ~p, reason: ~p",
                     [State, Reason]).



%%
%% States
%%

recover(enter, _PrevState, #statem_data{endpoints = Endpoints}) ->
    rabbit_log:debug("etcd v3 API client has entered recovery state, endpoints: ~s",
                     [string:join(Endpoints, ",")]),
    keep_state_and_data;
recover(internal, start, Data = #statem_data{endpoints = Endpoints, connection_monitor = Ref}) ->
    rabbit_log:debug("etcd v3 API client will attempt to connect, endpoints: ~s",
                     [string:join(Endpoints, ",")]),
    maybe_demonitor(Ref),
    Transport = tcp,
    TransportOpts = [],
    ConnName = ?ETCD_CONN_NAME,
    case connect(ConnName, Endpoints, Transport, TransportOpts) of
        {ok, Pid} ->
            rabbit_log:debug("etcd v3 API client connection: ~p", [Pid]),
            rabbit_log:debug("etcd v3 API client: total number of connections to etcd is ~p", [length(eetcd_conn_sup:info())]),
            {next_state, connected, Data#statem_data{
                connection_name = ConnName,
                connection_pid = Pid,
                connection_monitor = monitor(process, Pid)
            }};
        {error, Errors} ->
            [rabbit_log:error("etcd peer discovery: failed to connect to endpoint ~p: ~p", [Endpoint, Err]) || {Endpoint, Err} <- Errors],
            ensure_disconnected(?ETCD_CONN_NAME, Data),
            Actions = [{state_timeout, reconnection_interval(), recover}],
            {keep_state, reset_statem_data(Data), Actions}
    end;
recover(state_timeout, _PrevState, Data) ->
    rabbit_log:debug("etcd peer discovery: connection entered a reconnection delay state"),
    ensure_disconnected(?ETCD_CONN_NAME, Data),
    {next_state, recover, reset_statem_data(Data)}.


connected(enter, _PrevState, Data) ->
    rabbit_log:info("etcd peer discovery: successfully connected to etcd"),

    {keep_state, acquire_node_key_lease_grant(Data)};
connected(info, {'DOWN', ConnRef, process, ConnPid, Reason}, Data = #statem_data{
                                                               connection_pid = ConnPid,
                                                               connection_monitor = ConnRef
                                                             }) ->
    rabbit_log:debug("etcd peer discovery: connection to etcd ~p is down: ~p", [ConnPid, Reason]),
    maybe_demonitor(ConnRef),
    {next_state, recover, reset_statem_data(Data)};
connected({call, From}, lock, #statem_data{connection_name = _Conn}) ->
    gen_statem:reply(From, ok),
    keep_state_and_data;
connected({call, From}, unlock, #statem_data{connection_name = _Conn}) ->
    gen_statem:reply(From, ok),
    keep_state_and_data;
connected({call, From}, register, Data = #statem_data{connection_name = Conn}) ->
    Ctx = registration_context(Conn, Data),
    Key = node_key(Data),
    eetcd_kv:put(Ctx, Key, registration_value(Data)),
    rabbit_log:debug("", [Key]),
    gen_statem:reply(From, ok),
    keep_state_and_data;
connected({call, From}, unregister, #statem_data{connection_name = _Conn}) ->
    gen_statem:reply(From, ok),
    keep_state_and_data.


%%
%% Implementation
%%

acquire_node_key_lease_grant(Data = #statem_data{connection_name = Name, node_key_ttl = TTL}) ->
    %% acquire a lease for TTL
    {ok, #{'ID' := LeaseID}} = eetcd_lease:grant(Name, TTL),
    rabbit_log:debug("etcd peer discovery: acquired a lease ~p for node key ~s with TTL = ~p", [LeaseID, node_key(Data), TTL]),
    Data#statem_data{
        node_key_lease_id = LeaseID
    }.

registration_context(ConnName, #statem_data{node_key_lease_id = LeaseID}) ->
    Ctx1 = eetcd_kv:new(ConnName),
    eetcd_kv:with_lease(Ctx1, LeaseID).

node_key(#statem_data{key_prefix = Prefix}) ->
    Key = rabbit_misc:format("/rabbitmq/discovery/~s/clusters/~s/nodes/~p",
                             [Prefix, rabbit_nodes:persistent_cluster_id(), node()]),
    to_binary(Key).

%% This value is not used and merely
%% provides additional context to the operator.
registration_value(#statem_data{node_key_lease_id = LeaseID, node_key_ttl = TTL}) ->
    to_binary(rabbit_json:encode(#{
        <<"node">>     => to_binary(node()),
        <<"lease_id">> => LeaseID,
        <<"ttl">>      => TTL
    })).

error_is_already_started({_Endpoint, already_started}) ->
    true;
error_is_already_started({_Endpoint, _}) ->
    false.

connect(Name, Endpoints, Transport, TransportOpts) ->
    case eetcd_conn:lookup(Name) of
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, eetcd_conn_unavailable} ->
            do_connect(Name, Endpoints, Transport, TransportOpts)
    end.

do_connect(Name, Endpoints, Transport, TransportOpts) ->
    case eetcd:open(Name, Endpoints, [{mode, random}], Transport, TransportOpts) of
        {ok, Pid} -> {ok, Pid};
        {error, Errors} ->
            rabbit_log:debug("etcd peer discovery: connection errors: ~p, all benign?: ~p",
                             [Errors, lists:all(fun error_is_already_started/1, Errors)]),
            %% If all errors are already_started we can ignore them.
            %% eetcd registers connections under a name
            case lists:all(fun error_is_already_started/1, Errors) of
                true ->
                    eetcd_conn:lookup(Name);
                false ->
                    {error, Errors}
            end
    end.

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
            [rabbit_misc:format("~s:~p", [Hostname, Port])]
    end,

    AllEndpoints = Endpoints ++ LegacyEndpoints,
    maps:merge(maps:without([etcd_prefix, etcd_node_ttl, lock_wait_time], Map),
               #{endpoints => AllEndpoints}).
