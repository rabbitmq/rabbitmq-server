%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbit_peer_discovery_etcd).
-behaviour(rabbit_peer_discovery_backend).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_peer_discovery_common/include/rabbit_peer_discovery.hrl").
-include("rabbit_peer_discovery_etcd.hrl").

-export([init/0, list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).

-define(ETCD_CLIENT, rabbitmq_peer_discovery_etcd_v3_client).

%%
%% API
%%

init() ->
    %% We cannot start this plugin yet since it depends on the rabbit app,
    %% which is in the process of being started by the time this function is called
    application:load(rabbitmq_peer_discovery_common),
    application:load(rabbitmq_peer_discovery_etcd),

    %% Here we start the client very early on, before plugins have initialized.
    %% We need to do it conditionally, however.
    NoOp = fun() -> ok end,
    Run  = fun(_) ->
            _ = rabbit_log:debug("Peer discovery etcd: initialising..."),
            application:ensure_all_started(eetcd),
            Formation = application:get_env(rabbit, cluster_formation, []),
            Opts = maps:from_list(proplists:get_value(peer_discovery_etcd, Formation, [])),
            {ok, Pid} = rabbitmq_peer_discovery_etcd_v3_client:start_link(Opts),
            %% unlink so that this supervisor's lifecycle does not affect RabbitMQ core
            unlink(Pid),
            _ = rabbit_log:debug("etcd peer discovery: v3 client pid: ~p", [whereis(rabbitmq_peer_discovery_etcd_v3_client)])
           end,
    rabbit_peer_discovery_util:maybe_backend_configured(?BACKEND_CONFIG_KEY, NoOp, NoOp, Run),

    ok.


-spec list_nodes() -> {ok, {Nodes :: list(), NodeType :: rabbit_types:node_type()}} | {error, Reason :: string()}.

list_nodes() ->
    Fun0 = fun() -> {ok, {[], disc}} end,
    Fun1 = fun() ->
                   _ = rabbit_log:warning("Peer discovery backend is set to ~s "
                                      "but final config does not contain "
                                      "rabbit.cluster_formation.peer_discovery_etcd. "
                                      "Cannot discover any nodes because etcd cluster details are not configured!",
                                      [?MODULE]),
                   {ok, {[], disc}}
           end,
    Fun2 = fun(_Proplist) ->
                   %% error logging will be done by the client
                   Nodes = rabbitmq_peer_discovery_etcd_v3_client:list_nodes(),
                   {ok, {Nodes, disc}}
           end,
    rabbit_peer_discovery_util:maybe_backend_configured(?BACKEND_CONFIG_KEY, Fun0, Fun1, Fun2).


-spec supports_registration() -> boolean().

supports_registration() ->
    true.


-spec register() -> ok | {error, string()}.

register() ->
    Result = ?ETCD_CLIENT:register(),
    _ = rabbit_log:info("Registered node with etcd"),
    Result.


-spec unregister() -> ok | {error, string()}.
unregister() ->
    %% This backend unregisters on plugin (etcd v3 client) deactivation
    %% because by the time unregistration happens, the plugin and thus the client
    %% it provides are already gone. MK.
    ok.

-spec post_registration() -> ok | {error, Reason :: string()}.

post_registration() ->
    ok.

-spec lock(Node :: atom()) -> {ok, Data :: term()} | {error, Reason :: string()}.

lock(Node) when is_atom(Node) ->
    case rabbitmq_peer_discovery_etcd_v3_client:lock(Node) of
        {ok, GeneratedKey} -> {ok, GeneratedKey};
        {error, _} = Error -> Error
    end.


-spec unlock(Data :: term()) -> ok.

unlock(GeneratedKey) ->
    rabbitmq_peer_discovery_etcd_v3_client:unlock(GeneratedKey).
