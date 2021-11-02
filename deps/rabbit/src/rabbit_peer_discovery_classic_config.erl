%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_peer_discovery_classic_config).
-behaviour(rabbit_peer_discovery_backend).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).

%%
%% API
%%

-spec list_nodes() -> {ok, {Nodes :: [node()], rabbit_types:node_type()}} |
                      {error, Reason :: string()}.

list_nodes() ->
    case application:get_env(rabbit, cluster_nodes, {[], disc}) of
        {_Nodes, _NodeType} = Pair -> {ok, Pair};
        Nodes when is_list(Nodes)  -> {ok, {Nodes, disc}}
    end.

-spec lock(Node :: node()) -> {ok, {{ResourceId :: string(), LockRequesterId :: node()}, Nodes :: [node()]}} |
                              {error, Reason :: string()}.

lock(Node) ->
  {ok, {Nodes, _NodeType}} = list_nodes(),
  case lists:member(Node, Nodes) of
    false when Nodes =/= [] ->
      rabbit_log:warning("Local node ~s is not part of configured nodes ~p. "
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
