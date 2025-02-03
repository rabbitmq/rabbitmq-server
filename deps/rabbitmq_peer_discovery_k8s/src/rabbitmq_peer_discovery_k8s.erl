%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% This module exists as an alias for rabbit_peer_discovery_k8s.
%% Some users assume that the discovery module is the same as plugin
%% name. This module tries to fill the naming gap between module and plugin names.
-module(rabbitmq_peer_discovery_k8s).
-behaviour(rabbit_peer_discovery_backend).

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1, retry_strategy/0]).

-define(DELEGATE, rabbit_peer_discovery_k8s).

%%
%% API
%%

-spec init() -> ok | {error, Reason :: string()}.
init() ->
    ?DELEGATE:init().

-spec list_nodes() -> {ok, {Nodes :: [node()] | node(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.

list_nodes() ->
    ?DELEGATE:list_nodes().

-spec supports_registration() -> boolean().
supports_registration() ->
    ?DELEGATE:supports_registration().

-spec register()   -> ok | {error, Reason :: string()}.
register() ->
    ?DELEGATE:register().

-spec unregister() -> ok | {error, Reason :: string()}.
unregister() ->
    ?DELEGATE:unregister().

-spec post_registration()   -> ok | {error, Reason :: string()}.
post_registration() ->
    ?DELEGATE:post_registration().

-spec lock(Nodes :: [node()]) -> {ok, Data :: term()} | not_supported | {error, Reason :: string()}.
lock(Node) ->
    ?DELEGATE:lock(Node).

-spec unlock(Data :: term()) -> ok.
unlock(Data) ->
    ?DELEGATE:unlock(Data).

-spec retry_strategy() -> limited | unlimited.
retry_strategy() ->
    ?DELEGATE:retry_strategy().

