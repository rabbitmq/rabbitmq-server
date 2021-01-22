%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbitmq_peer_discovery_consul).
-behaviour(rabbit_peer_discovery_backend).

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).
-export([send_health_check_pass/0]).
-export([session_ttl_update_callback/1]).

-define(DELEGATE, rabbit_peer_discovery_consul).

%%
%% API
%%

init() ->
    ?DELEGATE:init().

-spec list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.
list_nodes() ->
    ?DELEGATE:list_nodes().

-spec supports_registration() -> boolean().
supports_registration() ->
    ?DELEGATE:supports_registration().

-spec register() -> ok | {error, Reason :: string()}.
register() ->
    ?DELEGATE:register().

-spec unregister() -> ok | {error, Reason :: string()}.
unregister() ->
    ?DELEGATE:unregister().

-spec post_registration() -> ok.
post_registration() ->
    ?DELEGATE:post_registration().

-spec lock(Node :: atom()) -> {ok, Data :: term()} | {error, Reason :: string()}.
lock(Node) ->
    ?DELEGATE:lock(Node).

-spec unlock({SessionId :: string(), TRef :: timer:tref()}) -> ok.
unlock(Data) ->
    ?DELEGATE:unlock(Data).

-spec send_health_check_pass() -> ok.
send_health_check_pass() ->
    ?DELEGATE:send_health_check_pass().

-spec session_ttl_update_callback(string()) -> string().
session_ttl_update_callback(SessionId) ->
    ?DELEGATE:session_ttl_update_callback(SessionId).
