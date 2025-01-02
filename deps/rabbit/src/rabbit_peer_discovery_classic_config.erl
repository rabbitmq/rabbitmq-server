%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_peer_discovery_classic_config).
-behaviour(rabbit_peer_discovery_backend).

-export([list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).

%%
%% API
%%

-spec list_nodes() -> {ok, {Nodes :: [node()], rabbit_types:node_type()}} |
                      {error, Reason :: string()}.

list_nodes() ->
    case application:get_env(rabbit, cluster_nodes, {[], disc}) of
        {Nodes, NodeType} ->
            check_local_node(Nodes),
            check_duplicates(Nodes),
            {ok, {add_this_node(Nodes), NodeType}};
        Nodes when is_list(Nodes) ->
            check_local_node(Nodes),
            check_duplicates(Nodes),
            {ok, {add_this_node(Nodes), disc}}
    end.

add_this_node(Nodes) ->
    ThisNode = node(),
    case lists:member(ThisNode, Nodes) of
        true  -> Nodes;
        false -> [ThisNode | Nodes]
    end.

check_duplicates(Nodes) ->
    case (length(lists:usort(Nodes)) == length(Nodes)) of
        true ->
            ok;
        false ->
            rabbit_log:warning("Classic peer discovery backend: list of "
                               "nodes contains duplicates ~0tp",
                               [Nodes])
    end.

check_local_node(Nodes) ->
    case lists:member(node(), Nodes) of
        true ->
            ok;
        false ->
            rabbit_log:warning("Classic peer discovery backend: list of "
                               "nodes does not contain the local node ~0tp",
                               [Nodes])
    end.

-spec lock(Nodes :: [node()]) ->
    {ok, {{ResourceId :: string(), LockRequesterId :: node()}, Nodes :: [node()]}} |
    {error, Reason :: string()}.

lock(Nodes) ->
  Node = node(),
  case lists:member(Node, Nodes) of
    false when Nodes =/= [] ->
      rabbit_log:warning("Local node ~ts is not part of configured nodes ~tp. "
                      "This might lead to incorrect cluster formation.", [Node, Nodes]);
    _ -> ok
  end,
  LockId = rabbit_nodes:lock_id(Node),
  Retries = rabbit_nodes:lock_retries(),
  case global:set_lock(LockId, Nodes, Retries) of
    true ->
      {ok, {LockId, Nodes}};
    false ->
      {error, io_lib:format("Acquiring lock taking too long, bailing out after ~b retries", [Retries])}
  end.

-spec unlock({{ResourceId :: string(), LockRequesterId :: node()}, Nodes :: [node()]}) ->
    ok.

unlock({LockId, Nodes}) ->
  global:del_lock(LockId, Nodes),
  ok.

-spec supports_registration() -> boolean().

supports_registration() ->
  false.

-spec register() -> ok.

register() ->
    ok.

-spec unregister() -> ok.

unregister() ->
    ok.

-spec post_registration() -> ok.

post_registration() ->
    ok.
