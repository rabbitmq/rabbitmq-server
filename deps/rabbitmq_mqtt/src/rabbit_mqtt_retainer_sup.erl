%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retainer_sup).
-behaviour(supervisor2).

-export([start_link/1, init/1, start_child/2,start_child/1, child_for_vhost/1,
         delete_child/1]).

-define(ENCODING, utf8).

-spec start_child(binary()) -> supervisor2:startchild_ret().
-spec start_child(term(), binary()) -> supervisor2:startchild_ret().

start_link(SupName) ->
  supervisor2:start_link(SupName, ?MODULE, []).

child_for_vhost(VHost) when is_binary(VHost) ->
  case rabbit_mqtt_retainer_sup:start_child(VHost) of
    {ok, Pid}                       -> Pid;
    {error, {already_started, Pid}} -> Pid
  end.

start_child(VHost) when is_binary(VHost) ->
  start_child(rabbit_mqtt_retainer:store_module(), VHost).

start_child(RetainStoreMod, VHost) ->
  supervisor2:start_child(?MODULE,

    {vhost_to_atom(VHost),
      {rabbit_mqtt_retainer, start_link, [RetainStoreMod, VHost]},
      permanent, 60, worker, [rabbit_mqtt_retainer]}).

delete_child(VHost) ->
  Id = vhost_to_atom(VHost),
  ok = supervisor2:terminate_child(?MODULE, Id),
  ok = supervisor2:delete_child(?MODULE, Id).

init([]) ->
  Mod = rabbit_mqtt_retainer:store_module(),
  rabbit_log:info("MQTT retained message store: ~p",
    [Mod]),
  {ok, {{one_for_one, 5, 5}, child_specs(Mod, rabbit_vhost:list_names())}}.

child_specs(Mod, VHosts) ->
  %% see start_child/2
  [{vhost_to_atom(V),
      {rabbit_mqtt_retainer, start_link, [Mod, V]},
      permanent, infinity, worker, [rabbit_mqtt_retainer]} || V <- VHosts].

vhost_to_atom(VHost) ->
    %% we'd like to avoid any conversion here because
    %% this atom isn't meant to be human-readable, only
    %% unique. This makes sure we don't get noisy process restarts
    %% with really unusual vhost names used by various HTTP API test suites
    rabbit_data_coercion:to_atom(VHost, latin1).
