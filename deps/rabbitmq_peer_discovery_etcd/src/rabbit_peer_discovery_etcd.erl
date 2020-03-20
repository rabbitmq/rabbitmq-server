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
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2020 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbit_peer_discovery_etcd).
-behaviour(rabbit_peer_discovery_backend).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_peer_discovery_common/include/rabbit_peer_discovery.hrl").
-include("rabbit_peer_discovery_etcd.hrl").

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).

-define(ETCD_CLIENT, rabbitmq_peer_discovery_etcd_v3_client).

%%
%% API
%%

init() ->
    %% We cannot start this plugin yet since it depends on the rabbit app,
    %% which is in the process of being started by the time this function is called
    application:load(rabbitmq_peer_discovery_common),
    application:load(rabbitmq_peer_discovery_etcd),

    %% Here we start the client very early on, before plugins have initialized.
    %% We need to do it conditionally, however.
    NoOp = fun() -> ok end,
    Run  = fun(_) ->
            rabbit_log:debug("Peer discovery etcd: initialising..."),
            application:ensure_all_started(eetcd),
            Formation = application:get_env(rabbit, cluster_formation, []),
            Opts = maps:from_list(proplists:get_value(peer_discovery_etcd, Formation, [])),
            {ok, Pid} = rabbitmq_peer_discovery_etcd_v3_client:start_link(Opts),
            %% unlink so that this supervisor's lifecycle does not affect RabbitMQ core
            unlink(Pid),
            rabbit_log:debug("etcd peer discovery: v3 client pid: ~p", [whereis(rabbitmq_peer_discovery_etcd_v3_client)])
           end,
    rabbit_peer_discovery_util:maybe_backend_configured(?BACKEND_CONFIG_KEY, NoOp, NoOp, Run),

    ok.


-spec list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.

list_nodes() ->
    Fun0 = fun() -> {ok, {[], disc}} end,
    Fun1 = fun() ->
                   rabbit_log:warning("Peer discovery backend is set to ~s "
                                      "but final config does not contain "
                                      "rabbit.cluster_formation.peer_discovery_etcd. "
                                      "Cannot discover any nodes because etcd cluster details are not configured!",
                                      [?MODULE]),
                   {ok, {[], disc}}
           end,
    Fun2 = fun(_Proplist) ->
                   %% error logging will be done by the client
                   Nodes = rabbitmq_peer_discovery_etcd_v3_client:list_nodes(),
                   {ok, {Nodes, disc}}
           end,
    rabbit_peer_discovery_util:maybe_backend_configured(?BACKEND_CONFIG_KEY, Fun0, Fun1, Fun2).


-spec supports_registration() -> boolean().

supports_registration() ->
    true.


-spec register() -> ok | {error, string()}.

register() ->
    Result = ?ETCD_CLIENT:register(),
    rabbit_log:info("Registered node with etcd"),
    Result.


-spec unregister() -> ok | {error, string()}.
unregister() ->
    %% This backend unregisters on plugin (etcd v3 client) deactivation
    %% because by the time unregistration happens, the plugin and thus the client
    %% it provides are already gone. MK.
    ok.

-spec post_registration() -> ok | {error, Reason :: string()}.

post_registration() ->
    ok.

-spec lock(Node :: atom()) -> {ok, Data :: term()} | {error, Reason :: string()}.

lock(Node) when is_atom(Node) ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    Now = erlang:system_time(seconds),
%%    EndTime = Now + get_config_key(lock_wait_time, M),
%%    lock(atom_to_list(Node) ++ " - " ++ generate_unique_string(), Now, EndTime),
    {ok, {to, do}}.


-spec unlock(Data :: term()) -> ok.

unlock({_UniqueId, _TRef}) ->
%%    stop_lock_ttl_updater(TRef),
%%    case delete_etcd_lock_key(UniqueId) of
%%        {ok, _} ->
%%            ok;
%%        {error, _} = Err ->
%%            Err
%%    end
    ok.

%%
%% Implementation
%%

%%-spec get_config_key(Key :: atom(), Map :: #{atom() => peer_discovery_config_value()})
%%             -> peer_discovery_config_value().
%%
%%get_config_key(Key, Map) ->
%%    ?CONFIG_MODULE:get(Key, ?CONFIG_MAPPING, Map).
%%
%%%% @doc Update etcd, setting a key for this node with a TTL of etcd_node_ttl
%%%% @end
%%-spec set_etcd_node_key(Map :: #{atom() => peer_discovery_config_value()})
%%                       -> {ok, any()} | {error, string()}.
%%set_etcd_node_key(Map) ->
%%  Interval = get_config_key(etcd_node_ttl, Map),
%%  etcd_put(node_path(Map), [{ttl, Interval}], [{value, enabled}], Map).
%%
%%%% @doc Part of etcd path that allows us to distinguish different
%%%% cluster using the same etcd server.
%%%% @end
%%-spec cluster_name_path_part(Map :: #{atom() => peer_discovery_config_value()}) -> string().
%%cluster_name_path_part(Map) ->
%%    case get_config_key(cluster_name, Map) of
%%        "undefined" -> "default";
%%        Value       -> Value
%%    end.
%%
%%%% @doc Return a list of path segments that are the base path for all
%%%% etcd keys related to current cluster.
%%%% @end
%%-spec base_path(Map :: #{atom() => peer_discovery_config_value()}) -> string().
%%base_path(Map) ->
%%  Segments = [v2, keys, get_config_key(etcd_prefix, Map), cluster_name_path_part(Map)],
%%  rabbit_peer_discovery_httpc:build_path(Segments).
%%
%%%% @doc Return a list of path segments that are the base path for all
%%%% etcd keys related to current cluster.
%%%% @end
%%-spec base_path() -> string().
%%base_path() ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    base_path(M).
%%
%%%% @doc Returns etcd path under which nodes should be registered.
%%%% @end
%%-spec nodes_path(Map :: #{atom() => peer_discovery_config_value()}) -> string().
%%nodes_path(Map) ->
%%    base_path(Map) ++ "/nodes".
%%
%%%% @doc Returns etcd path under which current node should be registered
%%%% @end
%%-spec node_path(Map :: #{atom() => peer_discovery_config_value()}) -> string().
%%node_path(Map) ->
%%  nodes_path(Map) ++ "/" ++ atom_to_list(node()).
%%
%%%% @doc Return the list of erlang nodes
%%%% @end
%%%%
%%-spec extract_nodes(list(), list()) -> [node()].
%%extract_nodes([], Nodes) -> Nodes;
%%extract_nodes([H|T], Nodes) ->
%%  M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%  ToAppend = case get_node_from_key(maps:get(<<"key">>, H), M) of
%%                 {error, none} -> [];
%%                 Name          -> [Name]
%%             end,
%%  extract_nodes(T, lists:append(Nodes, ToAppend)).
%%
%%%% @doc Return the list of erlang nodes
%%%% @end
%%%%
%%-spec extract_nodes(list()) -> [node()].
%%extract_nodes([]) -> [];
%%extract_nodes(Nodes) ->
%%  Dir = maps:get(<<"node">>, Nodes),
%%  case maps:get(<<"nodes">>, Dir, undefined) of
%%    undefined -> [];
%%    Values    -> extract_nodes(Values, [])
%%  end.
%%
%%
%%%% @doc Given an etcd key, return the erlang node name
%%%% @end
%%%%
%%-spec get_node_from_key(binary(), Map :: #{atom() => peer_discovery_config_value()}) -> node() | rabbit_types:error('none').
%%get_node_from_key(<<"/", V/binary>>, Map) -> get_node_from_key(V, Map);
%%get_node_from_key(V, _Map) ->
%%  %% Nodes path is /v2/keys/{etcd-prefix}/{cluster-name}/nodes and
%%  %% etcd returns node keys as /{etcd-prefix}/{cluster-name}/nodes/{nodename}.
%%  %%
%%  %% Note that both the prefix and the cluster name might contain slashes,
%%  %% and so will the resulting key. It is, however, significantly less
%%  %% likely that any of those will contains a /nodes/{name} sequence,
%%  %% so that's what we extract.
%%  %%
%%  %% See rabbitmq/rabbitmq-peer-discovery-etcd#14 for details.
%%  case re:run(V, <<"/nodes/([^/]+)$">>, [{capture, all_but_first, binary}]) of
%%      nomatch          -> {error, none};
%%      {match, [Name]} ->
%%          ?UTIL_MODULE:node_name(Name)
%%  end.
%%
%%-spec etcd_delete(Path, Query, Map)
%%                 -> {ok, term()} | {error, string()} when
%%      Path :: string(),
%%      Query :: list(),
%%      Map :: #{atom() => peer_discovery_config_value()}.
%%etcd_delete(Path, Query, Map) ->
%%    ?UTIL_MODULE:stringify_error(
%%      ?HTTPC_MODULE:delete(get_config_key(etcd_scheme, Map),
%%                           get_config_key(etcd_host, Map),
%%                           get_config_key(etcd_port, Map),
%%                           Path, Query, "")).
%%
%%-spec etcd_get(Path, Query, Map)
%%              -> {ok, term()} | {error, string()} when
%%      Path :: string(),
%%      Query :: list(),
%%      Map :: #{atom() => peer_discovery_config_value()}.
%%etcd_get(Path, Query, Map) ->
%%    ?UTIL_MODULE:stringify_error(
%%      ?HTTPC_MODULE:get(get_config_key(etcd_scheme, Map),
%%                        get_config_key(etcd_host, Map),
%%                        get_config_key(etcd_port, Map),
%%                        Path, Query)).
%%
%%-spec etcd_put(Path, Query, Body, Map) -> {ok, any()} | {error, string()} when
%%      Path :: string(),
%%      Query :: list(),
%%      Body :: list(),
%%      Map :: #{atom() => peer_discovery_config_value()}.
%%etcd_put(Path, Query, Body, Map) ->
%%    ?UTIL_MODULE:stringify_error(
%%      ?HTTPC_MODULE:put(get_config_key(etcd_scheme, Map),
%%                        get_config_key(etcd_host, Map),
%%                        get_config_key(etcd_port, Map),
%%                        Path, Query, ?HTTPC_MODULE:build_query(Body))).
%%
%%
%%-spec update_node_key() -> {ok, any()} | {error, string()}.
%%update_node_key() ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    set_etcd_node_key(M).
%%
%%%% @doc
%%%% Tries to acquire the lock. Will retry until the lock is finally
%%%% granted or time is up.
%%%% @end
%%-spec lock(string(), pos_integer(), pos_integer()) -> ok | {error, string()}.
%%lock(_, Now, EndTime) when EndTime < Now ->
%%    {error, "Acquiring the lock taking too long, bailing out"};
%%lock(UniqueId, _, EndTime) ->
%%    case try_insert_lock_key(UniqueId) of
%%        true ->
%%            TRef = start_lock_ttl_updater(UniqueId),
%%            {ok, {UniqueId, TRef}};
%%        false ->
%%            wait_for_lock_release(),
%%            lock(UniqueId, erlang:system_time(seconds), EndTime);
%%        {error, Reason} ->
%%            {error, lists:flatten(io_lib:format("Error while acquiring the lock, reason: ~p", [Reason]))}
%%    end.
%%
%%%% @doc Tries to acquire a lock in etcd. This can either succeed, fail
%%%% because somebody else is holding the lock, or completely file due
%%%% to some I/O error.
%%%% @end
%%-spec try_insert_lock_key(string()) -> boolean() | {error, term()}.
%%try_insert_lock_key(UniqueId) ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    Ttl = get_config_key(etcd_node_ttl, M),
%%    case set_etcd_lock_key(UniqueId, Ttl) of
%%        {ok, _} ->
%%            true;
%%        %% Precondition failed
%%        {error, "412"} ->
%%            false;
%%        {error, _} = Err ->
%%            Err
%%    end.
%%
%%%% @doc Orders etcd to create startup lock key if it doesn't exist already.
%%%% @end
%%-spec set_etcd_lock_key(string(), non_neg_integer()) -> {ok, term()} | {error, string()}.
%%set_etcd_lock_key(UniqueId, Ttl) ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    rabbit_log:debug("Will set a lock key with Etcd, path: ~p", [startup_lock_path()]),
%%    etcd_put(startup_lock_path(),
%%             [{ttl, Ttl}, {'prevExist', "false"}],
%%             [{value, UniqueId}],
%%             M).
%%
%%%% @doc Returns etcd path for startup lock
%%%% @end
%%-spec startup_lock_path() -> string().
%%startup_lock_path() ->
%%    base_path() ++ "/startup_lock".
%%
%%%% @doc Generate random string. We are using it for compare-and-change
%%%% operations in etcd.
%%%% @end
%%-spec generate_unique_string() -> string().
%%generate_unique_string() ->
%%    [ $a - 1 + rand:uniform(26) || _ <- lists:seq(1, 32) ].
%%
%%-spec start_lock_ttl_updater(string()) -> timer:tref().
%%start_lock_ttl_updater(UniqueId) ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    Interval = get_config_key(etcd_node_ttl, M),
%%    rabbit_log:debug("Starting startup lock refresher"),
%%    {ok, TRef} = timer:apply_interval(Interval * 500, ?MODULE,
%%                                      lock_ttl_update_callback, [UniqueId]),
%%    TRef.
%%
%%-spec stop_lock_ttl_updater(timer:tref()) -> ok.
%%stop_lock_ttl_updater(TRef) ->
%%    timer:cancel(TRef),
%%    rabbit_log:debug("Stopped startup lock refresher"),
%%    ok.
%%
%%-spec wait_for_lock_release() -> ok.
%%wait_for_lock_release() ->
%%    %% XXX Try to use etcd wait feature, but we somehow need to know
%%    %% the index from the last lock attempt operation.
%%    timer:sleep(1000).
%%
%%%% @doc Delete startup lock in etcd, but only if we are the holder of that lock.
%%%% @end
%%-spec delete_etcd_lock_key(string()) -> {ok, term()} | {error, string()}.
%%delete_etcd_lock_key(UniqueId) ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    etcd_delete(startup_lock_path(),
%%                [{'prevExist', "true"}, {'prevValue', UniqueId}],
%%                M).
%%
%%-spec lock_ttl_update_callback(string()) -> string().
%%lock_ttl_update_callback(UniqueId) ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    _ = refresh_etcd_lock_ttl(UniqueId, get_config_key(etcd_node_ttl, M)),
%%    UniqueId.
%%
%%%% @doc Refresh startup lock TTL in etcd, but only if we are the holder of that lock.
%%%% @end
%%-spec refresh_etcd_lock_ttl(string(), non_neg_integer()) -> {ok, term()} | {error, string()}.
%%refresh_etcd_lock_ttl(UniqueId, Ttl) ->
%%    M = ?CONFIG_MODULE:config_map(?BACKEND_CONFIG_KEY),
%%    etcd_put(startup_lock_path(),
%%             [],
%%             [{ttl, Ttl}, {'prevExist', true}, {'prevValue', UniqueId}, {refresh, true}],
%%             M).
