%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2020 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbitmq_peer_discovery_etcd).
-behaviour(rabbit_peer_discovery_backend).

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).

-define(DELEGATE, rabbit_peer_discovery_etcd).

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

-spec lock(Node :: atom()) -> not_supported.
lock(Node) ->
    ?DELEGATE:lock(Node).

-spec unlock(Data :: term()) -> ok.
unlock(Data) ->
    ?DELEGATE:unlock(Data).
