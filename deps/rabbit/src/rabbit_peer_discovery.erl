%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_peer_discovery).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

%%
%% API
%%

-export([maybe_init/0, maybe_create_cluster/1, discover_cluster_nodes/0,
         backend/0, node_type/0,
         normalize/1, format_discovered_nodes/1, log_configured_backend/0,
         register/0, unregister/0, maybe_register/0, maybe_unregister/0,
         lock/0, unlock/1, discovery_retries/0]).
-export([append_node_prefix/1, node_prefix/0, locking_retry_timeout/0,
         lock_acquisition_failure_mode/0]).

-ifdef(TEST).
-export([maybe_create_cluster/3]).
-endif.

-type create_cluster_callback() :: fun((node(),
                                        rabbit_db_cluster:node_type())
                                       -> ok).

-define(DEFAULT_BACKEND,   rabbit_peer_discovery_classic_config).

%% what node type is used by default for this node when joining
%% a new cluster as a virgin node
-define(DEFAULT_NODE_TYPE, disc).

%% default node prefix to attach to discovered hostnames
-define(DEFAULT_PREFIX, "rabbit").

%% default discovery retries and interval.
-define(DEFAULT_DISCOVERY_RETRY_COUNT, 10).
-define(DEFAULT_DISCOVERY_RETRY_INTERVAL_MS, 500).

-define(NODENAME_PART_SEPARATOR, "@").

-spec backend() -> atom().

backend() ->
  case application:get_env(rabbit, cluster_formation) of
    {ok, Proplist} ->
      proplists:get_value(peer_discovery_backend, Proplist, ?DEFAULT_BACKEND);
    undefined      ->
      ?DEFAULT_BACKEND
  end.



-spec node_type() -> rabbit_types:node_type().

node_type() ->
  case application:get_env(rabbit, cluster_formation) of
    {ok, Proplist} ->
      proplists:get_value(node_type, Proplist, ?DEFAULT_NODE_TYPE);
    undefined      ->
      ?DEFAULT_NODE_TYPE
  end.

-spec locking_retry_timeout() -> {Retries :: integer(), Timeout :: integer()}.

locking_retry_timeout() ->
    case application:get_env(rabbit, cluster_formation) of
        {ok, Proplist} ->
            Retries = proplists:get_value(lock_retry_limit, Proplist, 10),
            Timeout = proplists:get_value(lock_retry_timeout, Proplist, 30000),
            {Retries, Timeout};
        undefined ->
            {10, 30000}
    end.

-spec lock_acquisition_failure_mode() -> ignore | fail.

lock_acquisition_failure_mode() ->
    case application:get_env(rabbit, cluster_formation) of
        {ok, Proplist} ->
            proplists:get_value(lock_acquisition_failure_mode, Proplist, fail);
        undefined      ->
            fail
  end.

-spec log_configured_backend() -> ok.

log_configured_backend() ->
  rabbit_log:info("Configured peer discovery backend: ~ts", [backend()]).

maybe_init() ->
    Backend = backend(),
    _ = code:ensure_loaded(Backend),
    case erlang:function_exported(Backend, init, 0) of
        true  ->
            rabbit_log:debug("Peer discovery backend supports initialisation"),
            case Backend:init() of
                ok ->
                    rabbit_log:debug("Peer discovery backend initialisation succeeded"),
                    ok;
                {error, Error} ->
                    rabbit_log:warning("Peer discovery backend initialisation failed: ~tp.", [Error]),
                    ok
            end;
        false ->
            rabbit_log:debug("Peer discovery backend does not support initialisation"),
            ok
    end.

maybe_create_cluster(CreateClusterCallback) ->
    {Retries, Timeout} = locking_retry_timeout(),
    maybe_create_cluster(Retries, Timeout, CreateClusterCallback).

maybe_create_cluster(0, _, CreateClusterCallback)
  when is_function(CreateClusterCallback, 2) ->
    case lock_acquisition_failure_mode() of
        ignore ->
            ?LOG_WARNING(
               "Peer discovery: Could not acquire a peer discovery lock, "
               "out of retries", [],
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
            run_peer_discovery(CreateClusterCallback),
            maybe_register();
        fail ->
            exit(cannot_acquire_startup_lock)
    end;
maybe_create_cluster(Retries, Timeout, CreateClusterCallback)
  when is_function(CreateClusterCallback, 2) ->
    LockResult = lock(),
    ?LOG_DEBUG(
       "Peer discovery: rabbit_peer_discovery:lock/0 returned ~tp",
       [LockResult],
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
    case LockResult of
        not_supported ->
            run_peer_discovery(CreateClusterCallback),
            maybe_register();
        {ok, Data} ->
            try
                run_peer_discovery(CreateClusterCallback),
                maybe_register()
            after
                unlock(Data)
            end;
        {error, _Reason} ->
            timer:sleep(Timeout),
            maybe_create_cluster(
              Retries - 1, Timeout, CreateClusterCallback)
    end.

-spec run_peer_discovery(CreateClusterCallback) -> Ret when
      CreateClusterCallback :: create_cluster_callback(),
      Ret :: ok | {Nodes, NodeType},
      Nodes :: [node()],
      NodeType :: rabbit_db_cluster:node_type().

run_peer_discovery(CreateClusterCallback) ->
    {RetriesLeft, DelayInterval} = discovery_retries(),
    run_peer_discovery_with_retries(
      RetriesLeft, DelayInterval, CreateClusterCallback).

-spec run_peer_discovery_with_retries(
        Retries, DelayInterval, CreateClusterCallback) -> ok when
      CreateClusterCallback :: create_cluster_callback(),
      Retries :: non_neg_integer(),
      DelayInterval :: non_neg_integer().

run_peer_discovery_with_retries(
  0, _DelayInterval, _CreateClusterCallback) ->
    ok;
run_peer_discovery_with_retries(
  RetriesLeft, DelayInterval, CreateClusterCallback) ->
    FindBadNodeNames = fun
        (Name, BadNames) when is_atom(Name) -> BadNames;
        (Name, BadNames)                    -> [Name | BadNames]
    end,
    {DiscoveredNodes0, NodeType} =
        case discover_cluster_nodes() of
            {error, Reason} ->
                RetriesLeft1 = RetriesLeft - 1,
                ?LOG_ERROR(
                   "Peer discovery: Failed to discover nodes: ~tp. "
                   "Will retry after a delay of ~b ms, ~b retries left...",
                   [Reason, DelayInterval, RetriesLeft1],
                   #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
                timer:sleep(DelayInterval),
                run_peer_discovery_with_retries(
                  RetriesLeft1, DelayInterval, CreateClusterCallback);
            {ok, {Nodes, Type} = Config}
              when is_list(Nodes) andalso
                   (Type == disc orelse Type == disk orelse Type == ram) ->
                case lists:foldr(FindBadNodeNames, [], Nodes) of
                    []       -> Config;
                    BadNames -> e({invalid_cluster_node_names, BadNames})
                end;
            {ok, {_, BadType}} when BadType /= disc andalso BadType /= ram ->
                e({invalid_cluster_node_type, BadType});
            {ok, _} ->
                e(invalid_cluster_nodes_conf)
        end,
    DiscoveredNodes = lists:usort(DiscoveredNodes0),
    ?LOG_INFO(
       "Peer discovery: All discovered existing cluster peers: ~ts",
       [format_discovered_nodes(DiscoveredNodes)],
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
    Peers = rabbit_nodes:nodes_excl_me(DiscoveredNodes),
    case Peers of
        [] ->
            ?LOG_INFO(
               "Peer discovery: Discovered no peer nodes to cluster with. "
               "Some discovery backends can filter nodes out based on a "
               "readiness criteria. "
               "Enabling debug logging might help troubleshoot.",
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
            CreateClusterCallback(none, disc);
        _  ->
            ?LOG_INFO(
               "Peer discovery: Peer nodes we can cluster with: ~ts",
               [format_discovered_nodes(Peers)],
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
            join_discovered_peers(Peers, NodeType, CreateClusterCallback)
    end.

-spec e(any()) -> no_return().

e(Tag) -> throw({error, {Tag, error_description(Tag)}}).

error_description({invalid_cluster_node_names, BadNames}) ->
    "In the 'cluster_nodes' configuration key, the following node names "
        "are invalid: " ++ lists:flatten(io_lib:format("~tp", [BadNames]));
error_description({invalid_cluster_node_type, BadType}) ->
    "In the 'cluster_nodes' configuration key, the node type is invalid "
        "(expected 'disc' or 'ram'): " ++
        lists:flatten(io_lib:format("~tp", [BadType]));
error_description(invalid_cluster_nodes_conf) ->
    "The 'cluster_nodes' configuration key is invalid, it must be of the "
        "form {[Nodes], Type}, where Nodes is a list of node names and "
        "Type is either 'disc' or 'ram'".

%% Attempts to join discovered, reachable and compatible (in terms of Mnesia
%% internal protocol version and such) cluster peers in order.
join_discovered_peers(TryNodes, NodeType, CreateClusterCallback) ->
    {RetriesLeft, DelayInterval} = discovery_retries(),
    join_discovered_peers_with_retries(
      TryNodes, NodeType, RetriesLeft, DelayInterval, CreateClusterCallback).

join_discovered_peers_with_retries(
  TryNodes, _NodeType, 0, _DelayInterval, CreateClusterCallback) ->
    ?LOG_INFO(
       "Peer discovery: Could not successfully contact any node of: ~ts "
       "(as in Erlang distribution). "
       "Starting as a blank standalone node...",
       [string:join(lists:map(fun atom_to_list/1, TryNodes), ",")],
       #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
    init_single_node(CreateClusterCallback);
join_discovered_peers_with_retries(
  TryNodes, NodeType, RetriesLeft, DelayInterval, CreateClusterCallback) ->
    case find_reachable_peer_to_cluster_with(TryNodes) of
        {ok, Node} ->
            ?LOG_INFO(
               "Peer discovery: Node '~ts' selected for auto-clustering",
               [Node],
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
            create_cluster(Node, NodeType, CreateClusterCallback);
        none ->
            RetriesLeft1 = RetriesLeft - 1,
            ?LOG_INFO(
               "Peer discovery: Trying to join discovered peers failed. "
               "Will retry after a delay of ~b ms, ~b retries left...",
               [DelayInterval, RetriesLeft1],
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
            timer:sleep(DelayInterval),
            join_discovered_peers_with_retries(
              TryNodes, NodeType, RetriesLeft1, DelayInterval,
              CreateClusterCallback)
    end.

find_reachable_peer_to_cluster_with([]) ->
    none;
find_reachable_peer_to_cluster_with([Node | Nodes]) when Node =/= node() ->
    case rabbit_db_cluster:check_compatibility(Node) of
        ok ->
            {ok, Node};
        Error ->
            ?LOG_WARNING(
               "Peer discovery: Could not auto-cluster with node ~ts: ~0p",
               [Node, Error],
               #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
            find_reachable_peer_to_cluster_with(Nodes)
    end;
find_reachable_peer_to_cluster_with([Node | Nodes]) when Node =:= node() ->
    find_reachable_peer_to_cluster_with(Nodes).

init_single_node(CreateClusterCallback) ->
    IsVirgin = rabbit_db:is_virgin_node(),
    rabbit_db_cluster:ensure_feature_flags_are_in_sync([], IsVirgin),
    CreateClusterCallback(none, disc),
    ok.

create_cluster(RemoteNode, NodeType, CreateClusterCallback) ->
    %% We want to synchronize feature flags first before we update the cluster
    %% membership. This is needed to ensure the local list of Mnesia tables
    %% matches the rest of the cluster for example, in case a feature flag
    %% adds or removes tables.
    %%
    %% For instance, a feature flag may remove a table (so it's gone from the
    %% cluster). If we were to wait for that table locally before
    %% synchronizing feature flags, we would wait forever; indeed the feature
    %% flag being disabled before sync, `rabbit_table:definitions()' would
    %% return the old table.
    %%
    %% Feature flags need to be synced before any change to Mnesia membership.
    %% If enabling feature flags fails, Mnesia could remain in an inconsistent
    %% state that prevents later joining the nodes.
    IsVirgin = rabbit_db:is_virgin_node(),
    rabbit_db_cluster:ensure_feature_flags_are_in_sync([RemoteNode], IsVirgin),
    CreateClusterCallback(RemoteNode, NodeType),
    rabbit_node_monitor:notify_joined_cluster(),
    ok.

%% This module doesn't currently sanity-check the return value of
%% `Backend:list_nodes()`. Therefore, it could return something invalid:
%% thus the `{œk, any()} in the spec.
%%
%% `rabbit_mnesia:init_from_config()` does some verifications.

-spec discover_cluster_nodes() ->
    {ok, {Nodes :: [node()], NodeType :: rabbit_types:node_type()} | any()} |
    {error, Reason :: string()}.

discover_cluster_nodes() ->
    Backend = backend(),
    normalize(Backend:list_nodes()).


-spec maybe_register() -> ok.

maybe_register() ->
  Backend = backend(),
  case Backend:supports_registration() of
    true  ->
      register(),
      Backend:post_registration();
    false ->
      rabbit_log:info("Peer discovery backend ~ts does not support registration, skipping registration.", [Backend]),
      ok
  end.


-spec maybe_unregister() -> ok.

maybe_unregister() ->
  Backend = backend(),
  case Backend:supports_registration() of
    true  ->
      unregister();
    false ->
      rabbit_log:info("Peer discovery backend ~ts does not support registration, skipping unregistration.", [Backend]),
      ok
  end.

-spec discovery_retries() -> {Retries :: integer(), Interval :: integer()}.

discovery_retries() ->
    case application:get_env(rabbit, cluster_formation) of
        {ok, Proplist} ->
            Retries  = proplists:get_value(discovery_retry_limit,    Proplist, ?DEFAULT_DISCOVERY_RETRY_COUNT),
            Interval = proplists:get_value(discovery_retry_interval, Proplist, ?DEFAULT_DISCOVERY_RETRY_INTERVAL_MS),
            {Retries, Interval};
        undefined ->
            {?DEFAULT_DISCOVERY_RETRY_COUNT, ?DEFAULT_DISCOVERY_RETRY_INTERVAL_MS}
    end.

-spec register() -> ok.

register() ->
  Backend = backend(),
  rabbit_log:info("Will register with peer discovery backend ~ts", [Backend]),
  case Backend:register() of
    ok             -> ok;
    {error, Error} ->
      rabbit_log:error("Failed to register with peer discovery backend ~ts: ~tp",
        [Backend, Error]),
      ok
  end.


-spec unregister() -> ok.

unregister() ->
  Backend = backend(),
  rabbit_log:info("Will unregister with peer discovery backend ~ts", [Backend]),
  case Backend:unregister() of
    ok             -> ok;
    {error, Error} ->
      rabbit_log:error("Failed to unregister with peer discovery backend ~ts: ~tp",
        [Backend, Error]),
      ok
  end.

-spec lock() -> {ok, Data :: term()} | not_supported | {error, Reason :: string()}.

lock() ->
    Backend = backend(),
    rabbit_log:info("Will try to lock with peer discovery backend ~ts", [Backend]),
    case Backend:lock(node()) of
        {error, Reason} = Error ->
            rabbit_log:error("Failed to lock with peer discovery backend ~ts: ~tp",
                             [Backend, Reason]),
            Error;
        Any ->
            Any
    end.

-spec unlock(Data :: term()) -> ok | {error, Reason :: string()}.

unlock(Data) ->
    Backend = backend(),
    rabbit_log:info("Will try to unlock with peer discovery backend ~ts", [Backend]),
    case Backend:unlock(Data) of
        {error, Reason} = Error ->
            rabbit_log:error("Failed to unlock with peer discovery backend ~ts: ~tp, "
                             "lock data: ~tp",
                             [Backend, Reason, Data]),
            Error;
        Any ->
            Any
    end.

%%
%% Implementation
%%

-spec normalize(Nodes :: [node()] |
                {Nodes :: [node()],
                 NodeType :: rabbit_types:node_type()} |
                {ok, Nodes :: [node()]} |
                {ok, {Nodes :: [node()],
                      NodeType :: rabbit_types:node_type()}} |
                {error, Reason :: string()}) ->
    {ok, {Nodes :: [node()], NodeType :: rabbit_types:node_type()}} |
    {error, Reason :: string()}.

normalize(Nodes) when is_list(Nodes) ->
  {ok, {Nodes, disc}};
normalize({Nodes, NodeType}) when is_list(Nodes) andalso is_atom(NodeType) ->
  {ok, {Nodes, NodeType}};
normalize({ok, Nodes}) when is_list(Nodes) ->
  {ok, {Nodes, disc}};
normalize({ok, {Nodes, NodeType}}) when is_list(Nodes) andalso is_atom(NodeType) ->
  {ok, {Nodes, NodeType}};
normalize({error, Reason}) ->
  {error, Reason}.

-spec format_discovered_nodes(Nodes :: list()) -> string().

format_discovered_nodes(Nodes) ->
  %% NOTE: in OTP 21 string:join/2 is deprecated but still available.
  %%       Its recommended replacement is not a drop-in one, though, so
  %%       we will not be switching just yet.
  string:join(lists:map(fun rabbit_data_coercion:to_list/1, Nodes), ", ").



-spec node_prefix() -> string().

node_prefix() ->
    case string:tokens(atom_to_list(node()), ?NODENAME_PART_SEPARATOR) of
        [Prefix, _] -> Prefix;
        [_]         -> ?DEFAULT_PREFIX
    end.



-spec append_node_prefix(Value :: binary() | string()) -> string().

append_node_prefix(Value) when is_binary(Value) orelse is_list(Value) ->
    Val = rabbit_data_coercion:to_list(Value),
    Hostname = case string:tokens(Val, ?NODENAME_PART_SEPARATOR) of
                   [_ExistingPrefix, HN] -> HN;
                   [HN]                  -> HN
               end,
    string:join([node_prefix(), Hostname], ?NODENAME_PART_SEPARATOR).
