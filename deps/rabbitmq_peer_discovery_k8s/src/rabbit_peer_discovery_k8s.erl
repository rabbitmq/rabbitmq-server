%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_peer_discovery_k8s).
-behaviour(rabbit_peer_discovery_backend).

-export([init/0, list_nodes/0, supports_registration/0, register/0,
         unregister/0, post_registration/0, lock/1, unlock/1, node/0,
         retry_strategy/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-ifdef(TEST).
-compile([node/0]).
-endif.

init() ->
    Formation = application:get_env(rabbit, cluster_formation, []),
    Opts = proplists:get_value(peer_discovery_k8s, Formation, []),
    DeprecatedOpts = [ {K, V} || {K, V} <- Opts, not lists:member(K, configuration_options()) ],
    case DeprecatedOpts of
        [] -> ok;
        _ -> ?LOG_WARNING("Peer discovery: ignoring deprecated configuration options: ~w",
                          [proplists:get_keys(DeprecatedOpts)],
                          #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
             ok
    end,
    case proplists:get_value(discovery_retry_limit, Formation, undefined) of
        undefined -> ok;
        _ -> ?LOG_WARNING("Peer discovery: ignoring cluster_formation.discovery_retry_limit option "
                          "(will retry forever)",
                          [], #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
             ok
    end,
    ok.

-spec list_nodes() -> {ok, {Nodes :: [node()] | node(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.

list_nodes() ->
    Formation = application:get_env(rabbit, cluster_formation, []),
    Opts = proplists:get_value(peer_discovery_k8s, Formation, []),
    SeedNode = proplists:get_value(seed_node, Opts, undefined),
    SeedNodeOrdinal = integer_to_list(proplists:get_value(ordinal_start, Opts, 0)),
    seed_node(SeedNode, SeedNodeOrdinal).

seed_node(undefined, SeedNodeOrdinal) ->
    Nodename = atom_to_list(?MODULE:node()),
    try
        [[], Prefix, StatefulSetName, MyPodId, Domain] = re:split(
                                                           Nodename,
                                                           "^([^@]+@)([^.]*-)([0-9]+)",
                                                           [{return, list}]),
        _ = list_to_integer(MyPodId),
        SeedNode = list_to_atom(lists:flatten(Prefix ++ StatefulSetName ++ SeedNodeOrdinal ++ Domain)),
        {ok, {SeedNode, disc}}
    catch error:_ ->
              ?LOG_WARNING("Peer discovery: Failed to parse my node (~s). "
                           "Perhaps you are trying to deploy RabbitMQ without a StatefulSet?",
                           [Nodename],
                           #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
              {error, lists:flatten(io_lib:format("my nodename (~s) doesn't seem to have the expected -ID suffix "
                                                  "like StatefulSet pods should", [?MODULE:node()]))}
    end;
seed_node(SeedNode, _SeedNodeOrdinal) ->
    % elp:ignore atoms_exhaustion
    {ok, {list_to_atom(SeedNode), disc}}.

node() ->
    erlang:node().

supports_registration() -> false.
register() -> ok.
unregister() -> ok.
post_registration() -> ok.
lock(_) -> not_supported.
unlock(_) -> ok.
retry_strategy() -> unlimited.

configuration_options() ->
    [ordinal_start, seed_node].
