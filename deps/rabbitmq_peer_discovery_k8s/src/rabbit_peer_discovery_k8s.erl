%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_peer_discovery_k8s).
-behaviour(rabbit_peer_discovery_backend).

-export([init/0, list_nodes/0, supports_registration/0, register/0,
         unregister/0, post_registration/0, lock/1, unlock/1, node/0]).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-ifdef(TEST).
-compile([node/0]).
-endif.

-spec list_nodes() -> {ok, {Nodes :: [node()] | node(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.

list_nodes() ->
    Nodename = atom_to_list(?MODULE:node()),
    try
        [[], Prefix, StatefulSetName, MyPodId, Domain] = re:split(
                                                           Nodename,
                                                           "([^@]+@)([^.]*-)([0-9]+)",
                                                           [{return, list}]),
        _ = list_to_integer(MyPodId),
        NodeToClusterWith = list_to_atom(lists:flatten(Prefix ++ StatefulSetName ++ "0" ++ Domain)),
        {ok, {NodeToClusterWith, disc}}
    catch error:_ ->
              ?LOG_WARNING("Peer discovery: Failed to parse my node (~s). "
                           "Perhaps you are trying to deploy RabbitMQ without a StatefulSet?",
                           [Nodename],
                           #{domain => ?RMQLOG_DOMAIN_PEER_DISC}),
              {error, lists:flatten(io_lib:format("my nodename (~s) doesn't seem to be have an -ID suffix "
                                                  "like StatefulSet pods should", [?MODULE:node()]))}
    end.

node() ->
    erlang:node().

supports_registration() -> false.
init() -> ok.
register() -> ok.
unregister() -> ok.
post_registration() -> ok.
lock(_) -> not_supported.
unlock(_) -> ok.
