%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_internal_event_handler).

-include_lib("kernel/include/logger.hrl").


-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2]).

-import(rabbit_misc, [pget/2]).

-define(STATE, []).
%% Above the 120 second shutdown timeout of a retainer.
-define(DELETE_TIMEOUT, 150_000).

init([]) ->
    {ok, ?STATE}.

handle_event({event, vhost_created, Info, _, _}, ?STATE) ->
    Name = pget(name, Info),
    try rabbit_mqtt_retainer_sup:start_child_for_vhost(Name)
    catch Class:Reason ->
              ?LOG_ERROR("Failed to start MQTT retained message store for vhost '~ts': ~tp:~tp",
                         [Name, Class, Reason])
    end,
    {ok, ?STATE};
handle_event({event, vhost_deleted, Info, _, _}, ?STATE) ->
    Name = pget(name, Info),
    _ = spawn(fun() -> delete_retained_msg_store_cluster_wide(Name) end),
    {ok, ?STATE};
handle_event({event, maintenance_connections_closed, _Info, _, _}, ?STATE) ->
    {ok, NConnections} = rabbit_mqtt:close_local_client_connections(maintenance),
    ?LOG_WARNING("Closed ~b local (Web) MQTT client connections", [NConnections]),
    {ok, ?STATE};
handle_event(_Event, ?STATE) ->
    {ok, ?STATE}.

handle_call(_Request, ?STATE) ->
    {ok, ok, ?STATE}.

handle_info(_Info, State) ->
    {ok, State}.

delete_retained_msg_store_cluster_wide(VHost) ->
    Nodes = rabbit_nodes:list_running(),
    Results = erpc:multicall(Nodes, rabbit_mqtt_retainer_sup, delete_child_for_vhost,
                             [VHost], ?DELETE_TIMEOUT),
    lists:foreach(
      fun({_Node, {ok, ok}}) ->
              ok;
         ({Node, Error}) ->
              ?LOG_WARNING("Failed to delete MQTT retained message store for vhost '~ts' "
                           "on node ~ts: ~tp", [VHost, Node, Error])
      end, lists:zip(Nodes, Results)).
