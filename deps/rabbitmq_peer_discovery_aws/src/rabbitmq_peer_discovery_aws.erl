%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module exists as an alias for rabbit_peer_discovery_aws.
%% Some users assume that the discovery module is the same as plugin
%% name. This module tries to fill the naming gap between module and plugin names.
-module(rabbitmq_peer_discovery_aws).
-behaviour(rabbit_peer_discovery_backend).

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).

-define(DELEGATE, rabbit_peer_discovery_aws).

%%
%% API
%%

init() ->
    ?DELEGATE:init().

-spec list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} |
                      {error, Reason :: string()}.
list_nodes() ->
    ?DELEGATE:list_nodes().

-spec supports_registration() -> boolean().
supports_registration() ->
    ?DELEGATE:supports_registration().


-spec register() -> ok.
register() ->
    ?DELEGATE:register().

-spec unregister() -> ok.
unregister() ->
    ?DELEGATE:unregister().

-spec post_registration() -> ok | {error, Reason :: string()}.
post_registration() ->
    ?DELEGATE:post_registration().

-spec lock(Node :: node()) -> {ok, {ResourceId :: string(), LockRequesterId :: node()}} | {error, Reason :: string()}.
lock(Node) ->
    ?DELEGATE:lock(Node).

-spec unlock({{ResourceId :: string(), LockRequestedId :: atom()}, Nodes :: [atom()]}) -> 'ok'.
unlock(Data) ->
    ?DELEGATE:unlock(Data).
