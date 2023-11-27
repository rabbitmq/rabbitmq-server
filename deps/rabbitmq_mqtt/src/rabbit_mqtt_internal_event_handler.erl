%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_mqtt_internal_event_handler).

-behaviour(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2]).

-import(rabbit_misc, [pget/2]).

-define(STATE, []).

init([]) ->
    {ok, ?STATE}.

handle_event({event, vhost_created, Info, _, _}, ?STATE) ->
    Name = pget(name, Info),
    rabbit_mqtt_retainer_sup:start_child_for_vhost(Name),
    {ok, ?STATE};
handle_event({event, vhost_deleted, Info, _, _}, ?STATE) ->
    Name = pget(name, Info),
    rabbit_mqtt_retainer_sup:delete_child_for_vhost(Name),
    {ok, ?STATE};
handle_event({event, maintenance_connections_closed, _Info, _, _}, ?STATE) ->
    {ok, NConnections} = rabbit_mqtt:close_local_client_connections(maintenance),
    rabbit_log:warning("Closed ~b local (Web) MQTT client connections", [NConnections]),
    {ok, ?STATE};
handle_event(_Event, ?STATE) ->
    {ok, ?STATE}.

handle_call(_Request, ?STATE) ->
    {ok, ok, ?STATE}.

handle_info(_Info, State) ->
    {ok, State}.
