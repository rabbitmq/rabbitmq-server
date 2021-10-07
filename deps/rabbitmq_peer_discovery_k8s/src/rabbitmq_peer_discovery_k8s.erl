%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates. All rights reserved.
%%

%% This module exists as an alias for rabbit_peer_discovery_k8s.
%% Some users assume that the discovery module is the same as plugin
%% name. This module tries to fill the naming gap between module and plugin names.
-module(rabbitmq_peer_discovery_k8s).
-behaviour(rabbit_peer_discovery_backend).

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1, send_event/3, generate_v1_event/7]).

-define(DELEGATE, rabbit_peer_discovery_k8s).

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

generate_v1_event(Namespace, Name, Type, Message, Reason, Timestamp, HostName) ->
    ?DELEGATE:generate_v1_event(Namespace, Name, Type, Message, Reason, Timestamp, HostName).

%% @doc Perform a HTTP POST request to K8s to send and k8s v1.Event
%% @end
%%
-spec send_event(term(),term(), term()) -> {ok, term()} | {error, term()}.
send_event(Type, Reason, Message) ->
    ?DELEGATE:send_event(Type, Reason, Message).
