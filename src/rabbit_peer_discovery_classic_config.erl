%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_peer_discovery_classic_config).
-behaviour(rabbit_peer_discovery_backend).

-include("rabbit.hrl").

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

-spec supports_registration() -> boolean().

supports_registration() ->
    %% If we don't have any nodes configured, skip randomized delay and similar operations
    %% as we don't want to delay startup for no reason. MK.
    has_any_peer_nodes_configured().

-spec register() -> ok.

register() ->
    ok.

-spec unregister() -> ok.

unregister() ->
    ok.

-spec post_registration() -> ok.

post_registration() ->
    ok.

-spec lock(Node :: atom()) -> not_supported.

lock(_Node) ->
    not_supported.

-spec unlock(Data :: term()) -> ok.

unlock(_Data) ->
    ok.

%%
%% Helpers
%%

has_any_peer_nodes_configured() ->
    case application:get_env(rabbit, cluster_nodes, []) of
        {[], _NodeType} ->
            false;
        {Nodes, _NodeType} when is_list(Nodes) ->
            true;
        [] ->
            false;
        Nodes when is_list(Nodes) ->
            true
    end.
