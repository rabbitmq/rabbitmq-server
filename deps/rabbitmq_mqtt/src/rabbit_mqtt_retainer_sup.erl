%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_retainer_sup).
-behaviour(supervisor).

-export([start_link/1, init/1, start_child/2,start_child/1, child_for_vhost/1,
         delete_child/1]).

-define(ENCODING, utf8).

-spec start_child(binary()) -> supervisor:startchild_ret().
-spec start_child(term(), binary()) -> supervisor:startchild_ret().

start_link(SupName) ->
  supervisor:start_link(SupName, ?MODULE, []).

child_for_vhost(VHost) when is_binary(VHost) ->
  case rabbit_mqtt_retainer_sup:start_child(VHost) of
    {ok, Pid}                       -> Pid;
    {error, {already_started, Pid}} -> Pid
  end.

start_child(VHost) when is_binary(VHost) ->
  start_child(rabbit_mqtt_retainer:store_module(), VHost).

start_child(RetainStoreMod, VHost) ->
  supervisor:start_child(?MODULE,
    #{
      id       => vhost_to_atom(VHost),
      start    => {rabbit_mqtt_retainer, start_link, [RetainStoreMod, VHost]},
      restart  => permanent,
      shutdown => 60,
      type     => worker,
      modules  => [rabbit_mqtt_retainer]
    }).

delete_child(VHost) ->
  Id = vhost_to_atom(VHost),
  ok = supervisor:terminate_child(?MODULE, Id),
  ok = supervisor:delete_child(?MODULE, Id).

init([]) ->
  Mod = rabbit_mqtt_retainer:store_module(),
  rabbit_log:info("MQTT retained message store: ~p~n", [Mod]),
  Flags = #{
        strategy => one_for_all,
        period => 5,
        intensity => 5
  },
  {ok, {Flags, child_specs(Mod, rabbit_vhost:list_names())}}.

child_specs(Mod, VHosts) ->
  %% see start_child/2
  [child_spec(V, Mod) || V <- VHosts].

child_spec(VHost, Mod) ->
  #{
    id => vhost_to_atom(VHost),
    start => {rabbit_mqtt_retainer, start_link, [Mod, VHost]},
    restart => permanent,
    shutdown => infinity,
    type => worker,
    modules => [rabbit_mqtt_retainer]
  }.

vhost_to_atom(VHost) ->
    %% we'd like to avoid any conversion here because
    %% this atom isn't meant to be human-readable, only
    %% unique. This makes sure we don't get noisy process restarts
    %% with really unusual vhost names used by various HTTP API test suites
    rabbit_data_coercion:to_atom(VHost, latin1).
